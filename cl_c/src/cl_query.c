/*
 * The query interface 
 *
 *
 * Citrusleaf, 2012
 * All rights reserved
 */

#include <sys/types.h>
#include <stdio.h>
#include <errno.h> //errno
#include <stdlib.h> //fprintf
#include <unistd.h> // close
#include <string.h>
#include <pthread.h>
#include <fcntl.h>
#include <assert.h>


#include "citrusleaf/citrusleaf.h"
#include "citrusleaf/cl_cluster.h"
#include "citrusleaf/citrusleaf-internal.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/proto.h"
#include "citrusleaf/cf_socket.h"
#include "citrusleaf/cf_vector.h"

#define EXTRA_CHECKS 1

// work item given to each node
typedef struct {
    // these sections are the same for the same query
    cl_cluster				*asc; 
    const char				*ns;
 	const uint8_t			*query_buf;
 	size_t					query_sz;
 	const cl_mr_job			*mr_job; 
    citrusleaf_get_many_cb	cb; 
    void					*udata;
   	cf_queue				*node_complete_q;	// used to synchronize work from all nodes are finished
    
   	cl_mr_state				*mr_state;				// 0 if no map reduce, created by cl_mapreduce
   	
    // different for each node
    char					node_name[NODE_NAME_SIZE];    
} query_work;

#define	N_MAX_QUERY_THREADS 5
static cf_atomic32  query_initialized = 0;
static cf_queue     *g_query_q         = 0;
static pthread_t    g_query_th[N_MAX_QUERY_THREADS];
static query_work	g_null_work;


// query range field layout: contains - numranges, binname, start, end
// 
// generic field header
// 0   4 size = size of data only
// 4   1 field_type = CL_MSG_FIELD_TYPE_INDEX_RANGE
//
// numranges
// 5   1 numranges (max 255 ranges) 
//
// binname 
// 6   1 binnamelen b
// 7   b binname
// 
// particle (start & end)
// +b    1 particle_type
// +b+1  4 start_particle_size x
// +b+5  x start_particle_data
// +b+5+x      4 end_particle_size y
// +b+5+x+y+4   y end_particle_data
//
// repeat "numranges" times from "binname"
static int query_compile_range_field(cf_vector *range_v, uint8_t *buf, int *sz_p)
{
	int sz = 0;
		
	// numranges
	sz += 1;
	if (buf) {
		*buf++ = cf_vector_size(range_v);
	}
	
	// iterate through each ranage	
	for (uint i=0; i<cf_vector_size(range_v); i++) {
		cl_query_range *range = (cl_query_range *)cf_vector_getp(range_v,i);
		
		// binname size
		int binnamesz = strlen(range->bin_name);
		sz += 1;
		if (buf) {
			*buf++ = binnamesz;
		}
		
		// binname
		sz += binnamesz;
		if (buf) {
			memcpy(buf,range->bin_name,binnamesz);
			buf += binnamesz;
		}
		
		// particle type
		sz += 1;
		if (buf) {
			*buf++ = range->start_obj.type;
		}
		
		// start particle len
		// particle len will be in network order 
		sz += 4;
		size_t psz = 0;
		cl_object_get_size(&range->start_obj,&psz);
		if (buf) {
			uint32_t ss = psz; 
			*((uint32_t *)buf) = ntohl(ss);
			// fprintf(stderr, "*** ss %ld buf %ld\n",ss,*((uint32_t *)buf));
			buf += sizeof(uint32_t);
		} 
		
		// start particle data
		sz += psz;
		if (buf) {
			//fprintf(stderr, "*** buf %ld\n",*((uint64_t *)buf));
			cl_object_to_buf(&range->start_obj,buf);
			//fprintf(stderr, "*** buf %ld\n",*((uint64_t *)buf));
			buf += psz;
		}
		
		// end particle len
		// particle len will be in network order 
		sz += 4;
		psz = 0;
		cl_object_get_size(&range->end_obj,&psz);
		if (buf) {
			uint32_t ss = psz; 
			*((uint32_t *)buf) = ntohl(ss);
			// fprintf(stderr, "*** ss %ld buf %ld\n",ss,*((uint32_t *)buf));
			buf += sizeof(uint32_t);
		} 
		
		// end particle data
		sz += psz;
		if (buf) {
			//fprintf(stderr, "*** buf %ld\n",*((uint64_t *)buf));
			cl_object_to_buf(&range->end_obj,buf);
			//fprintf(stderr, "*** buf %ld\n",*((uint64_t *)buf));
			buf += psz;
		}
	}		
	
	*sz_p = sz;
}

// generic field header
// 0   4 size = size of data only
// 4   1 field_type = CL_MSG_FIELD_TYPE_INDEX_RANGE
//
// numbins
// 5   1 binnames (max 255 binnames) 
//
// binnames 
// 6   1 binnamelen b
// 7   b binname
// 
// numbins times


static int query_compile_binnames_field(cf_vector *binnames, uint8_t *buf, int *sz_p)
{
	int sz = 0;
		
	// numbins
	sz += 1;
	if (buf) {
		*buf++ = cf_vector_size(binnames);
	}
	
	// iterate through each biname	
	for (uint i=0; i<cf_vector_size(binnames); i++) {
		char *binname = (char *)cf_vector_getp(binnames, i);
		
		// binname size
		int binnamesz = strlen(binname);
		sz += 1;
		if (buf) {
			*buf++ = binnamesz;
		}
		
		// binname
		sz += binnamesz;
		if (buf) {
			memcpy(buf, binname, binnamesz);
			buf += binnamesz;
		}
		//fprintf(stderr, "Packing binname %s of size %d \n",binname, binnamesz);
	}		
	*sz_p = sz;
}

//
// Both query and mr_job are allowed to be null
// if the query is null, then you run the MR job over the entire set or namespace
// If the job is null, just run the query

static int query_compile (const char *ns, const cl_query *query, const cl_mr_state *mr_state, const cl_mr_job *mr_job,
                         uint8_t **buf_r, size_t *buf_sz_r) {
		
	if (ns==NULL) 		return -1;

#ifdef EXTRA_CHECKS
	if (query) {
		if ( ! query->indexname) { 
			fprintf(stderr, "query compile internal error: query but no indexname\n");
			return(-1);
		}
		if ( ! query->ranges ) {
			fprintf(stderr, "query compile internal error: query but no indexname\n");
			return(-1);
		}
	}
	if (mr_job) {
		if ( ! mr_job->package ) {
			fprintf(stderr, "query compile internal error: job but no package\n");
			return(-1);
		}
		if ( ! mr_state ) {
			fprintf(stderr, "query compile internal error: job but no state\n");
			return(-1);
		}
		if ( ! mr_state->generation) {
			fprintf(stderr, "query compile internal error: state but no generation\n");
			return(-1);
		}
	}
#endif	
    
    // calculating buffer size & n_fields
   	int 	n_fields = 0; 
    size_t  msg_sz  = sizeof(as_msg);
    
    // namespace field 
    n_fields++;
    int     ns_len  = strlen(ns);
    msg_sz += ns_len  + sizeof(cl_msg_field); 

    int     iname_len = 0;
	int		setname_len = 0;
    int 	range_sz = 0;
	int		num_bins = 0;
	if (query) {
		// indexname field
		n_fields++;
		iname_len  = strlen(query->indexname);
		msg_sz += strlen(query->indexname) + sizeof(cl_msg_field);
		
		if (query->setname) {
		    n_fields++;
			setname_len = strlen(query->setname);
			msg_sz += setname_len + sizeof(cl_msg_field);
		}

		// query field    
		n_fields++;
		range_sz = 0; 
		query_compile_range_field(query->ranges, NULL, &range_sz);
		msg_sz += range_sz + sizeof(cl_msg_field);

		// bin field	
		if (query->binnames) {
			n_fields++;
			num_bins = 0;
			query_compile_binnames_field(query->binnames, NULL, &num_bins);
			msg_sz += num_bins + sizeof(cl_msg_field);
		}
	}
	
	// TODO filter field
	// TODO orderby field
	// TODO limit field
	
	// mrj package name field
	int 	package_len = 0;
	int 	gen_len = 0;	
	int 	mapper_len = 0;
	int 	maparg_len = 0;
	int 	reducer_len = 0;
	int 	rdcarg_len = 0;
	int 	finalizer_len = 0;
	int 	fnzarg_len = 0;
	if (mr_job) {
		n_fields++;
		package_len = strlen(mr_job->package); 
		msg_sz += package_len  + sizeof(cl_msg_field);
		
		n_fields++;
		gen_len = strlen(mr_state->generation); 
		msg_sz += gen_len  + sizeof(cl_msg_field);
	
		if (mr_job->map_fname) {
			n_fields++;
			mapper_len = strlen(mr_job->map_fname); 
			msg_sz += mapper_len  + sizeof(cl_msg_field);
			
			if (mr_job->map_argc > 0) {
				n_fields++;
				sproc_compile_arg_field(mr_job->map_argk, mr_job->map_argv, mr_job->map_argc, NULL, &maparg_len); 
				msg_sz += maparg_len + sizeof(cl_msg_field);
			}
		}

		if (mr_job->rdc_fname) {
			n_fields++;
			reducer_len = strlen(mr_job->rdc_fname); 
			msg_sz += reducer_len  + sizeof(cl_msg_field);
			
			if (mr_job->rdc_argc > 0) {
				n_fields++;
				sproc_compile_arg_field(mr_job->rdc_argk, mr_job->rdc_argv, mr_job->rdc_argc, NULL, &rdcarg_len); 
				msg_sz += rdcarg_len + sizeof(cl_msg_field);
			}
		}

		if (mr_job->fnz_fname) {
			n_fields++;
			finalizer_len = strlen(mr_job->fnz_fname); 
			msg_sz += finalizer_len  + sizeof(cl_msg_field);
			
			if (mr_job->fnz_argc > 0) {
				n_fields++;
				sproc_compile_arg_field(mr_job->fnz_argk, mr_job->fnz_argv, mr_job->fnz_argc, NULL, &fnzarg_len); 
				msg_sz += fnzarg_len + sizeof(cl_msg_field);
			}
		}
	}

	// get a buffer to write to.
    uint8_t *buf; uint8_t *mbuf = 0;
    if ((*buf_r) && (msg_sz > *buf_sz_r)) { 
        mbuf   = buf = malloc(msg_sz); if (!buf) return(-1);
        *buf_r = buf;
    } else buf = *buf_r;
    *buf_sz_r  = msg_sz;
    memset(buf, 0, msg_sz);  // NOTE: this line is debug - shouldn't be required
    
    // write the headers
    int	info1      = CL_MSG_INFO1_READ;
    int info2      = 0;
    int info3      = 0;
    buf = cl_write_header(buf, msg_sz, info1, info2, info3, 0, 0, 0, 
    		n_fields, 0);
        
    // now write the fields
    cl_msg_field *mf = (cl_msg_field *) buf;
    cl_msg_field *mf_tmp = mf;
    if (ns) {
        mf->type = CL_MSG_FIELD_TYPE_NAMESPACE;
        mf->field_sz = ns_len + 1;
        memcpy(mf->data, ns, ns_len);
        mf_tmp = cl_msg_field_get_next(mf);
        cl_msg_swap_field(mf);
        mf = mf_tmp;
    }
    
    if (query->indexname) {
        mf->type = CL_MSG_FIELD_TYPE_INDEX_NAME;
        mf->field_sz = iname_len + 1;
        memcpy(mf->data, query->indexname, iname_len);
        mf_tmp = cl_msg_field_get_next(mf);
        cl_msg_swap_field(mf);
        mf = mf_tmp;
		if (cf_debug_enabled()) {
			fprintf(stderr,"adding indexname %d %s\n",iname_len+1, query->indexname);
		}
    }

	if (query->setname) {
        mf->type = CL_MSG_FIELD_TYPE_SET;
        mf->field_sz = setname_len + 1;
        memcpy(mf->data, query->setname, setname_len);
        mf_tmp = cl_msg_field_get_next(mf);
        cl_msg_swap_field(mf);
        mf = mf_tmp;
		if (cf_debug_enabled()) {
			fprintf(stderr,"adding setname %d %s\n",setname_len+1, query->setname);
		}
    }

    if (query->ranges) {
        mf->type = CL_MSG_FIELD_TYPE_INDEX_RANGE;
        mf->field_sz = range_sz + 1;
		query_compile_range_field(query->ranges, mf->data, &range_sz);
        mf_tmp = cl_msg_field_get_next(mf);
        cl_msg_swap_field(mf);
        mf = mf_tmp;
    }
	
	if (query->binnames) {
        mf->type = CL_MSG_FIELD_TYPE_QUERY_BINLIST;
        mf->field_sz = num_bins + 1;
		query_compile_binnames_field(query->binnames, mf->data, &num_bins);
        mf_tmp = cl_msg_field_get_next(mf);
        cl_msg_swap_field(mf);
        mf = mf_tmp;
    }

    if (package_len) {
        mf->type = CL_MSG_FIELD_TYPE_SPROC_PACKAGE;
        mf->field_sz = package_len + 1;
        memcpy(mf->data, mr_job->package, package_len);
        mf_tmp = cl_msg_field_get_next(mf);
        cl_msg_swap_field(mf);
        mf = mf_tmp;
		if (cf_debug_enabled()) {
			fprintf(stderr,"adding package %s\n", mr_job->package);
		}
    }

    if (gen_len) {
        mf->type = CL_MSG_FIELD_TYPE_SPROC_PACKAGE_GEN;
        mf->field_sz = gen_len + 1;
        memcpy(mf->data, mr_state->generation, gen_len);
        mf_tmp = cl_msg_field_get_next(mf);
        cl_msg_swap_field(mf);
        mf = mf_tmp;
		if (cf_debug_enabled()) {
			fprintf(stderr,"adding generation %s\n", mr_state->generation);
		}
    }

	// map
	if (mapper_len) {
        mf->type = CL_MSG_FIELD_TYPE_SPROC_MAP;
        mf->field_sz = mapper_len + 1;
        memcpy(mf->data, mr_job->map_fname, mapper_len);
        mf_tmp = cl_msg_field_get_next(mf);
        cl_msg_swap_field(mf);
        mf = mf_tmp;
	}

	if (maparg_len) {
        mf->type = CL_MSG_FIELD_TYPE_SPROC_MAP_ARG;
        mf->field_sz = maparg_len + 1;
		sproc_compile_arg_field(mr_job->map_argk, mr_job->map_argv, mr_job->map_argc, mf->data, &maparg_len); 
        mf_tmp = cl_msg_field_get_next(mf);
        cl_msg_swap_field(mf);
        mf = mf_tmp;
	}

	// reduce
	if (reducer_len) {
        mf->type = CL_MSG_FIELD_TYPE_SPROC_REDUCE;
        mf->field_sz = reducer_len + 1;
        memcpy(mf->data, mr_job->rdc_fname, reducer_len);
        mf_tmp = cl_msg_field_get_next(mf);
        cl_msg_swap_field(mf);
        mf = mf_tmp;
	}

	if (rdcarg_len) {
        mf->type = CL_MSG_FIELD_TYPE_SPROC_REDUCE_ARG;
        mf->field_sz = rdcarg_len + 1;
		sproc_compile_arg_field(mr_job->rdc_argk, mr_job->rdc_argv, mr_job->rdc_argc, mf->data, &rdcarg_len); 
        mf_tmp = cl_msg_field_get_next(mf);
        cl_msg_swap_field(mf);
        mf = mf_tmp;
	}

	// finalize
	if (finalizer_len) {
        mf->type = CL_MSG_FIELD_TYPE_SPROC_FINALIZE;
        mf->field_sz = finalizer_len + 1;
        memcpy(mf->data, mr_job->fnz_fname, finalizer_len);
        mf_tmp = cl_msg_field_get_next(mf);
        cl_msg_swap_field(mf);
        mf = mf_tmp;
	}

	if (fnzarg_len > 0) {
        mf->type = CL_MSG_FIELD_TYPE_SPROC_FINALIZE_ARG;
        mf->field_sz = fnzarg_len + 1;
		sproc_compile_arg_field(mr_job->fnz_argk, mr_job->fnz_argv, mr_job->fnz_argc, mf->data, &fnzarg_len); 
        mf_tmp = cl_msg_field_get_next(mf);
        cl_msg_swap_field(mf);
        mf = mf_tmp;
	}

    if (!buf) { 
    	if (mbuf) {
    		free(mbuf); 
    	}
    	return(-1); 
    }

    return(0);    
}

#define STACK_BUF_SZ (1024 * 16) // provide a safe number for your system
                                 // - linux tends to have 8M stacks these days
#define STACK_BINS 100

// 
// this is an actual instance of a query, running on a query thread
//

static int do_query_monte(cl_cluster_node *node, const char *ns, const uint8_t *query_buf, size_t query_sz,  cl_mr_state *mr_state,
                          citrusleaf_get_many_cb cb, void *udata, bool isnbconnect) {

	uint8_t		rd_stack_buf[STACK_BUF_SZ];	
	uint8_t		*rd_buf = rd_stack_buf;
	size_t		rd_buf_sz = 0;
    
	as_msg 		msg;    

    int fd = cl_cluster_node_fd_get(node, false, isnbconnect);
    if (fd == -1) { 
        fprintf(stderr,"do query monte: cannot get fd for node %s ",node->name);
    	return(-1); 
    }
    
    // send it to the cluster - non blocking socket, but we're blocking
    if (0 != cf_socket_write_forever(fd, (uint8_t *) query_buf, (size_t) query_sz)) { return(-1); }

    cl_proto         proto;
    bool done = false;
    int rv;
    
    do {
	
    	// multiple CL proto per response
        // Now turn around and read a fine cl_pro - that's the first 8 bytes that has types and lenghts
        if ((rv = cf_socket_read_forever(fd, (uint8_t *) &proto,
                                         sizeof(cl_proto) ) ) ) {
            fprintf(stderr, "network error: errno %d fd %d\n",rv, fd);
            return(-1);
        }
        cl_proto_swap(&proto);

        if (proto.version != CL_PROTO_VERSION) {
            fprintf(stderr, "network error: received protocol message of wrong version %d\n",proto.version);
            return(-1);
        }
        if ((proto.type != CL_PROTO_TYPE_CL_MSG) &&
            (proto.type != CL_PROTO_TYPE_CL_MSG_COMPRESSED)) {
            fprintf(stderr, "network error: received incorrect message version %d\n",proto.type);
            return(-1);
        }
        
        // second read for the remainder of the message - expect this to cover lots of data, many lines
        //
        // if there's no error
        rd_buf_sz =  proto.sz;
        if (rd_buf_sz > 0) {
            if (rd_buf_sz > sizeof(rd_stack_buf))
                rd_buf = malloc(rd_buf_sz);
            else
                rd_buf = rd_stack_buf;
            if (rd_buf == NULL)        return (-1);

            if ((rv = cf_socket_read_forever(fd, rd_buf, rd_buf_sz))) {
                fprintf(stderr, "network error: errno %d fd %d\n",rv, fd);
                if (rd_buf != rd_stack_buf)    { free(rd_buf); }
                return(-1);
            }
        }

        // process all the cl_msg in this proto
        uint8_t *buf = rd_buf;
        uint pos = 0;
        cl_bin stack_bins[STACK_BINS];
        cl_bin *bins;
        
        while (pos < rd_buf_sz) {
            uint8_t *buf_start = buf;
            cl_msg *msg = (cl_msg *) buf;
            cl_msg_swap_header(msg);
            buf += sizeof(cl_msg);
            
            if (msg->header_sz != sizeof(cl_msg)) {
                fprintf(stderr, "received cl msg of unexpected size: expecting %zd found %d, internal error\n",
                    sizeof(cl_msg),msg->header_sz);
                return(-1);
            }

            // parse through the fields
            cf_digest *keyd = 0;
            char ns_ret[33] = {0};
            char *set_ret = NULL;
            cl_msg_field *mf = (cl_msg_field *)buf;
            for (int i=0;i<msg->n_fields;i++) {
                cl_msg_swap_field(mf);
                if (mf->type == CL_MSG_FIELD_TYPE_KEY)
                    fprintf(stderr, "read: found a key - unexpected\n");
                else if (mf->type == CL_MSG_FIELD_TYPE_DIGEST_RIPE) {
                    keyd = (cf_digest *) mf->data;
                }
                else if (mf->type == CL_MSG_FIELD_TYPE_NAMESPACE) {
                    memcpy(ns_ret, mf->data, cl_msg_field_get_value_sz(mf));
                    ns_ret[ cl_msg_field_get_value_sz(mf) ] = 0;
                }
                else if (mf->type == CL_MSG_FIELD_TYPE_SET) {
                    uint32_t set_name_len = cl_msg_field_get_value_sz(mf);
                    set_ret = (char *)malloc(set_name_len + 1);
                    memcpy(set_ret, mf->data, set_name_len);
                    set_ret[ set_name_len ] = '\0';
                }
                mf = cl_msg_field_get_next(mf);
            }
            buf = (uint8_t *) mf;
            if (msg->n_ops > STACK_BINS) {
                bins = malloc(sizeof(cl_bin) * msg->n_ops);
            }
            else {
                bins = stack_bins;
            }
            if (bins == NULL) {
                if (set_ret) {
                    free(set_ret);
                }
                return (-1);
            }

            // parse through the bins/ops
            cl_msg_op *op = (cl_msg_op *)buf;
            for (int i=0;i<msg->n_ops;i++) {

                cl_msg_swap_op(op);

#ifdef DEBUG_VERBOSE
                fprintf(stderr, "op receive: %p size %d op %d ptype %d pversion %d namesz %d \n",
                    op,op->op_sz, op->op, op->particle_type, op->version, op->name_sz);
#endif            

#ifdef DEBUG_VERBOSE
                dump_buf("individual op (host order)", (uint8_t *) op, op->op_sz + sizeof(uint32_t));
#endif    

                cl_set_value_particular(op, &bins[i]);
                op = cl_msg_op_get_next(op);
            }
            buf = (uint8_t *) op;
            
            if (msg->info3 & CL_MSG_INFO3_LAST)    {
#ifdef DEBUG                
                fprintf(stderr, "received final message\n");
#endif                
                done = true;
            }

            // if there's a map-reduce on this query, callback into the mr system
            // (which ends up accumulating into the mr state), or just return the responses now)
            //
            if ((msg->n_ops || (msg->info1 & CL_MSG_INFO1_NOBINDATA))) {
            	
				if (mr_state) {
#ifdef USE_LUA_MR
					cl_mr_state_row(mr_state, ns_ret, keyd, set_ret, msg->generation, 
						msg->record_ttl, bins, msg->n_ops, false /*islast*/, cb, udata);
#endif
				}
				else if (cb) {
					// got one good value? call it a success!
					// (Note:  In the key exists case, there is no bin data.)
					(*cb) ( ns_ret, keyd, set_ret, msg->generation, msg->record_ttl, bins, msg->n_ops, false /*islast*/, udata);
				}
                rv = 0;
            }
//            else
//                fprintf(stderr, "received message with no bins, signal of an error\n");

            if (bins != stack_bins) {
                free(bins);
                bins = 0;
            }

            if (set_ret) {
                free(set_ret);
                set_ret = NULL;
            }

            // don't have to free object internals. They point into the read buffer, where
            // a pointer is required
            pos += buf - buf_start;
            
        }
        
        if (rd_buf && (rd_buf != rd_stack_buf))    {
            free(rd_buf);
            rd_buf = 0;
        }

    } while ( done == false );

    cl_cluster_node_fd_put(node, fd, false);
    
    goto Final;
    
Final:    
    
#ifdef DEBUG_VERBOSE    
    fprintf(stderr, "exited loop: rv %d\n", rv );
#endif    
    
    return(rv);
}

static void *query_worker_fn(void *dummy) {
    while (1) {
        query_work work;
        if (0 != cf_queue_pop(g_query_q, &work, CF_QUEUE_FOREVER)) {
            fprintf(stderr, "queue pop failed\n");
        }
        
		if (cf_debug_enabled()) {
			fprintf(stderr, "query_worker_fn: getting one work item\n");
		}
        // a NULL structure is the condition that we should exit. See shutdown()
        if( 0==memcmp(&work,&g_null_work,sizeof(query_work))) { 
        	pthread_exit(NULL); 
        }

		// query if the node is still around
		cl_cluster_node *node = cl_cluster_node_get_byname(work.asc, work.node_name);
		int an_int = CITRUSLEAF_FAIL_UNAVAILABLE;
		if (node) {
        	an_int = do_query_monte(node, work.ns, work.query_buf, work.query_sz, work.mr_state, work.cb, work.udata, work.asc->nbconnect);
        }
                                    
        cf_queue_push(work.node_complete_q, (void *)&an_int);
    }
}


cl_rv citrusleaf_query(cl_cluster *asc, const char *ns, const cl_query *query, const cl_mr_job *mr_job,
		citrusleaf_get_many_cb cb, void *udata) 
{
    query_work work;
        
    work.asc = asc;
    work.ns = ns;
    work.mr_job = mr_job;
    work.mr_state = 0;

	// This must be before the compile because the 
#ifdef USE_LUA_MR
	if (mr_job) {
		
		// make sure it's in the cache before you do a "get" with the same package name
		if (0 != citrusleaf_sproc_package_get_and_create( asc, mr_job->package, CL_SCRIPT_LANG_LUA ) ) {
			return CITRUSLEAF_FAIL_CLIENT;
		}

		work.mr_state = cl_mr_state_get( mr_job );
		if (! work.mr_state) {
			return CITRUSLEAF_FAIL_CLIENT;
		}
	}
#else
	if (mr_job) {
        fprintf(stderr,"MR job called but Aerospike client not compiled w/ Lua/MR capability\n");
		return CITRUSLEAF_FAIL_CLIENT;
	}
#endif

	uint8_t		wr_stack_buf[STACK_BUF_SZ];
	uint8_t		*wr_buf = wr_stack_buf;
	size_t		wr_buf_sz = sizeof(wr_stack_buf);
    
	// compile the query - a good place to fail    
    int rv = query_compile(ns, query, work.mr_state, mr_job, &wr_buf, &wr_buf_sz);
    if (rv) {
        fprintf(stderr,"do query monte: query compile failed: ");
#ifdef USE_LUA_MR
        if (work.mr_state) cl_mr_state_put( work.mr_state );
#endif
        return (rv);
    }
    work.query_buf = wr_buf;
    work.query_sz = wr_buf_sz;
    
    // shared between threads
    work.cb = cb;
    work.udata = udata;    
    work.node_complete_q = cf_queue_create(sizeof(int),true);

	char *node_names = NULL;	
	int	n_nodes = 0;
	cl_cluster_get_node_names(asc, &n_nodes, &node_names);
	if (n_nodes == 0) {
		fprintf(stderr, "citrusleaf query nodes: don't have any nodes?\n");
#ifdef USE_LUA_MR
		if (work.mr_state) cl_mr_state_put( work.mr_state );
#endif
		cf_queue_destroy(work.node_complete_q);
		if (wr_buf && (wr_buf != wr_stack_buf)) { free(wr_buf); wr_buf = 0; }
		return CITRUSLEAF_FAIL_CLIENT;
	}
	
	
    // dispatch work to the worker queue to allow the transactions in parallel
    // note: if a new node is introduced in the middle, it is NOT taken care of
	char *nptr = node_names;
    for (int i=0;i<n_nodes;i++) {        
        // fill in per-request specifics
        strcpy(work.node_name,nptr);
        cf_queue_push(g_query_q, &work);
		nptr+=NODE_NAME_SIZE;					
    }
	free(node_names); node_names = 0;
    
    // wait for the work to complete from all the nodes.
    int retval = 0;
    for (int i=0;i<n_nodes;i++) {
        int z;
        cf_queue_pop(work.node_complete_q, &z, CF_QUEUE_FOREVER);
        if (z != 0)
            retval = z;
    }

    // do the final reduce, big operation, then done
    if ((retval == 0) && work.mr_state) {
#ifdef USE_LUA_MR    
    	retval = cl_mr_state_done(work.mr_state, work.cb, work.udata);
    	
    	cl_mr_state_put(work.mr_state);
#endif
    }

    if (wr_buf && (wr_buf != wr_stack_buf)) { free(wr_buf); wr_buf = 0; }
    
    cf_queue_destroy(work.node_complete_q);
    if (retval != 0)  {
    	return( CITRUSLEAF_FAIL_CLIENT );
    }
    return 0;
}

cl_query *citrusleaf_query_create(const char *indexname, const char *setname)
{
	cl_query *query = malloc(sizeof(cl_query));
	if (query==NULL) {
		return NULL;
	}
	memset(query,0,sizeof(cl_query));
	if (indexname) memcpy(query->indexname, indexname, strlen(indexname));
	if (setname)   memcpy(query->setname,   setname,   strlen(setname));

	return query;	
}

static void cl_range_destroy(cl_query_range *range)
{
	citrusleaf_object_free(&range->start_obj);
	citrusleaf_object_free(&range->end_obj);
}

static void cl_filter_destroy(cl_query_filter *filter)
{
	citrusleaf_object_free(&filter->compare_obj);
}

void citrusleaf_query_destroy(cl_query *query)
{
	if (query->binnames) {
		cf_vector_destroy(query->binnames);
	}

	if (query->ranges) {
		for (uint i=0; i<cf_vector_size(query->ranges); i++) {
			cl_query_range *range = (cl_query_range *)cf_vector_getp(query->ranges, i);
			cl_range_destroy(range);
		}
		cf_vector_destroy(query->ranges);
	}
	
	if (query->filters) {
		for (uint i=0; i<cf_vector_size(query->filters); i++) {
			cl_query_filter *filter = (cl_query_filter *)cf_vector_getp(query->filters, i);
			cl_filter_destroy(filter);
		}
		cf_vector_destroy(query->filters);
	}
	
	if (query->orderbys) {
		cf_vector_destroy(query->orderbys);
	}

	free(query);
}

cl_rv citrusleaf_query_add_binname(cl_query *query, const char *binname)
{
	if ( !query->binnames ) {
		query->binnames = cf_vector_create(CL_BINNAME_SIZE, 5, 0);
		if (query->binnames==NULL) {
			return CITRUSLEAF_FAIL_CLIENT;
		}
	}
	cf_vector_append(query->binnames, (void *)binname);	
	//fprintf(stderr,"Appending binname %s\n",binname);
	return CITRUSLEAF_OK;	
}

static cl_rv add_range_generic (cl_query *query, const char *binname, cl_query_range *range)
{
	if ( !query->ranges) {
		query->ranges = cf_vector_create(sizeof(cl_query_range),5,0);
		if (query->ranges==NULL) {
			return CITRUSLEAF_FAIL_CLIENT;
		}
	}
	strcpy(range->bin_name,binname);
	cf_vector_append(query->ranges,(void *)range);
	return CITRUSLEAF_OK;
}

cl_rv citrusleaf_query_add_range_numeric (cl_query *query, const char *binname, int64_t start, int64_t end)
{
	cl_query_range range;
	citrusleaf_object_init_int(&range.start_obj,start);
	citrusleaf_object_init_int(&range.end_obj,end);

	return add_range_generic(query,binname, &range);	
}

cl_rv citrusleaf_query_add_range_string (cl_query *query, const char *binname, const char *start, const char *end)
{
	cl_query_range range;
	citrusleaf_object_init_str(&range.start_obj,start);
	citrusleaf_object_init_str(&range.end_obj,end);

	return add_range_generic(query,binname, &range);	
}

cl_rv citrusleaf_query_add_filter_numeric (cl_query *query, const char *binname, int64_t comparer, cl_query_filter_op op)
{
	return CITRUSLEAF_OK;
}

cl_rv citrusleaf_query_add_filter_string (cl_query *query, const char *binname, const char* comparer, cl_query_filter_op op)
{
	return CITRUSLEAF_OK;
}

cl_rv citrusleaf_query_add_orderby (cl_query *query, const char *binname, cl_query_orderby_op op)
{
	return CITRUSLEAF_OK;
}

cl_rv citrusleaf_query_set_limit (cl_query *query, uint64_t limit )
{
	query->limit = limit;
	return CITRUSLEAF_OK;	
}


int citrusleaf_query_init() {
    if (1 == cf_atomic32_incr(&query_initialized)) {

		if (cf_debug_enabled()) {
			fprintf(stderr, "query_init: creating %d threads\n",N_MAX_QUERY_THREADS);
		}
				
	    memset(&g_null_work,0,sizeof(query_work));

        // create dispatch queue
        g_query_q = cf_queue_create(sizeof(query_work), true);
        
        // create thread pool
        for (int i=0;i<N_MAX_QUERY_THREADS;i++) {
            pthread_create(&g_query_th[i], 0, query_worker_fn, 0);
		}
    }
    return(0);    
}

void citrusleaf_query_shutdown() {

    for( int i=0; i<N_MAX_QUERY_THREADS; i++) {
    	cf_queue_push(g_query_q,&g_null_work);
    }

    for( int i=0; i<N_MAX_QUERY_THREADS; i++) {
    	pthread_join(g_query_th[i],NULL);
    }
    cf_queue_destroy(g_query_q);
}
