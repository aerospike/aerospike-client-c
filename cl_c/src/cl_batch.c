/*
 * The batch interface has some cleverness, it makes parallel requests
 * under the covers to different servers
 *
 *
 * Brian Bulkowski, 2011
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
#include <zlib.h>


#include "citrusleaf/citrusleaf.h"
#include "citrusleaf/cl_cluster.h"
#include "citrusleaf/citrusleaf-internal.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/proto.h"
#include "citrusleaf/cf_socket.h"


//
// Decompresses a compressed CL msg
// The buffer passed in is the space *after* the header, just the compressed data
//
//
// returns -1 if it can't be decompressed for some reason
//


static int
batch_decompress(uint8_t *in_buf, size_t in_sz, uint8_t **out_buf, size_t *out_sz) 
{
	z_stream 	strm;

	uint64_t now = cf_getms();
	
	strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    int rv = inflateInit(&strm);
    if (rv != Z_OK)
        return (-1);

    
    // first 8 bytes are the inflated size, allows efficient alloc (round up: the buf likes that)
    size_t  	b_sz_alloc = *(uint64_t *)in_buf;
    uint8_t 	*b = malloc(b_sz_alloc);
    if (0 == b) {
    	fprintf(stderr, "batch_decompress: could not malloc %"PRIu64" bytes\n",b_sz_alloc);
    	inflateEnd(&strm);
    	return(-1);
    }
    
    strm.avail_in = in_sz - 8;
    strm.next_in = in_buf + 8;
    
	strm.avail_out = b_sz_alloc; // round up: seems to like that
	strm.next_out = b;


//	fprintf(stderr, "before deflate: in_buf %p in_sz %"PRIu64" outbuf %p out_sz %"PRIu64"\n",
//		strm.next_in, strm.avail_in, strm.next_out, strm.avail_out);

	rv = inflate(&strm, Z_FINISH);
	
//	fprintf(stderr, "after deflate: rv %d in_buf %p in_sz %"PRIu64" outbuf %p out_sz %"PRIu64" ms %"PRIu64"\n",
//		rv, strm.next_in, strm.avail_in, strm.next_out, strm.avail_out, cf_getms() - now);

	if (rv != Z_STREAM_END) {
		fprintf(stderr, "could not deflate data: zlib error %d (check zlib.h)\n",rv);
		free(b);
		inflateEnd(&strm);
		return(-1);
	}
    	
	inflateEnd(&strm);
    
    *out_buf = b;
    *out_sz = b_sz_alloc - strm.avail_out;
	
	return(0);	
}


static uint8_t *write_fields_batch_digests(uint8_t *buf, char *ns, int ns_len, cf_digest *digests, cl_cluster_node **nodes, int n_digests, int n_my_digests, cl_cluster_node *my_node, char *mrjids, char *imatchs, map_args_t *margs, int marg_sz) {
    printf("write_fields_batch_digests: ns: %p mrjid: %s imatch: %s\n",
           ns, mrjids, imatchs);
    printf("write_fields_batch_digests: margs: %p\n", margs);

	cl_msg_field *mf = (cl_msg_field *) buf;
	cl_msg_field *mf_tmp = mf;
	if (ns) { printf("writing NS\n");
		mf->type = CL_MSG_FIELD_TYPE_NAMESPACE;
		mf->field_sz = ns_len + 1;
		memcpy(mf->data, ns, ns_len);
		mf_tmp = cl_msg_field_get_next(mf);
		cl_msg_swap_field(mf);
		mf = mf_tmp;
	}
    if (imatchs) { printf("writing imatch\n");
	    mf->type = CL_MSG_FIELD_TYPE_SECONDARY_INDEX_ID;
        int ilen = strlen(imatchs);
	    mf->field_sz = ilen + 1;
	    memcpy(mf->data, imatchs, ilen);
	    mf_tmp = cl_msg_field_get_next(mf);
	    cl_msg_swap_field(mf);
	    mf = mf_tmp;
    }
    if (mrjids) { printf("writing MRJID\n");
	    mf->type = CL_MSG_FIELD_TYPE_MAP_REDUCE_JOB_ID;
        int mlen = strlen(mrjids);
	    mf->field_sz = mlen + 1;
	    memcpy(mf->data, mrjids, mlen);
	    mf_tmp = cl_msg_field_get_next(mf);
	    cl_msg_swap_field(mf);
	    mf = mf_tmp;
    }

	mf->type = CL_MSG_FIELD_TYPE_SECONDARY_INDEX_SINGLE;
    printf("writing DIGESTS n_digests: %d n_my_digests: %d\n",
           n_digests, n_my_digests);
	int digest_sz = sizeof(cf_digest) * n_my_digests;
	mf->field_sz = digest_sz;
	uint8_t *b = mf->data;
	for (int i = 0; i < n_digests; i++) {
		memcpy(b, &digests[i], sizeof(cf_digest));
        //printf("B: %lu\n", *(uint64_t *)&digests[i]);
		b += sizeof(cf_digest);
	}
	mf_tmp = cl_msg_field_get_next(mf);
	cl_msg_swap_field(mf);		
	mf = mf_tmp;

    if (margs) { printf("writing MARGS\n");
	    mf->type        = CL_MSG_FIELD_TYPE_MAP_REDUCE_ARG;
	    mf->field_sz    = marg_sz + 1;
	    memcpy(mf->data, &margs->argc, sizeof(int));
	    uint8_t *b      = mf->data;
	    b              += sizeof(int);
        for (int i = 0; i < margs->argc; i++) {
            int klen = strlen(margs->kargv[i]);
		    memcpy(b, &klen, sizeof(int));
		    b += sizeof(int); //printf("writing: i: %d klen: %d\n", i, klen);
        }
        for (int i = 0; i < margs->argc; i++) {
            int vlen = strlen(margs->vargv[i]);
		    memcpy(b, &vlen, sizeof(int));
		    b += sizeof(int); //printf("writing: i: %d vlen: %d\n", i, vlen);
        }
        for (int i = 0; i < margs->argc; i++) {
            int klen = strlen(margs->kargv[i]);
            //printf("writn KARGV[%d]: len: %d %s\n", i, klen, margs->kargv[i]);
		    memcpy(b, margs->kargv[i], klen);
		    b += klen;
        }
        for (int i = 0; i < margs->argc; i++) {
            int vlen = strlen(margs->vargv[i]);
            //printf("writn VARGV[%d]: len: %d %s\n", i, vlen, margs->vargv[i]);
		    memcpy(b, margs->vargv[i], vlen);
		    b += vlen;
        }
	    mf_tmp = cl_msg_field_get_next(mf);
	    cl_msg_swap_field(mf);
	    mf = mf_tmp;
    }
	return ( (uint8_t *) mf_tmp );
}

static uint8_t *write_fields_lua_func_register(
                               uint8_t *buf, char *ns, int ns_len,
                               char *lua_mapf, int lmflen,
                               char *lua_rdcf, int lrflen,
                               char *lua_fnzf, int lfflen, int reg_mrjid) {
printf("write_fields_lua_func_register lua_mapf(%s) lua_rdcf,(%s) lua_fnzf(%s)\n reg_mrjid: %d", lua_mapf, lua_rdcf, lua_fnzf, reg_mrjid);
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
	mf->type = CL_MSG_FIELD_TYPE_LUA_MAP_FUNCTION_REGISTER;
	mf->field_sz = lmflen + 1;
	memcpy(mf->data, lua_mapf, lmflen);
	mf_tmp = cl_msg_field_get_next(mf);
	cl_msg_swap_field(mf);
	mf = mf_tmp;

	mf->type = CL_MSG_FIELD_TYPE_LUA_REDUCE_FUNCTION_REGISTER;
	mf->field_sz = lrflen + 1;
	memcpy(mf->data, lua_rdcf, lrflen);
	mf_tmp = cl_msg_field_get_next(mf);
	cl_msg_swap_field(mf);
	mf = mf_tmp;

	mf->type = CL_MSG_FIELD_TYPE_LUA_FINALIZE_FUNCTION_REGISTER;
	mf->field_sz = lfflen + 1;
	memcpy(mf->data, lua_fnzf, lfflen);
	mf_tmp = cl_msg_field_get_next(mf);
	cl_msg_swap_field(mf);
	mf = mf_tmp;

	mf->type = CL_MSG_FIELD_TYPE_MAP_REDUCE_ID;
	mf->field_sz = sizeof(int);
	memcpy(mf->data, &reg_mrjid, sizeof(int));
	mf_tmp = cl_msg_field_get_next(mf);
	cl_msg_swap_field(mf);
	mf = mf_tmp;

	return ( (uint8_t *) mf_tmp );
}



static int batch_compile(uint info1, uint info2, uint info3, char *ns, cf_digest *digests, cl_cluster_node **nodes, int n_digests, cl_cluster_node *my_node, int n_my_digests, cl_bin *values, cl_operator operator, cl_operation *operations, int n_values,  uint8_t **buf_r, size_t *buf_sz_r, const cl_write_parameters *cl_w_p, char *lua_mapf, int lmflen, char *lua_rdcf, int lrflen, char *lua_fnzf, int lfflen, int mrjid, int imatch, map_args_t *margs, int reg_mrjid) {
    printf("batch_compile: n_values: %d\n", n_values);
	int		ns_len = ns ? strlen(ns) : 0;
	int		i;
	
    int   marg_sz = 0;
    char *mrjids  = NULL; char mrjbuf[32];
    char *imatchs = NULL; char imbuf [32];
	if (mrjid)        { sprintf(mrjbuf, "%d", mrjid);       mrjids  = mrjbuf; }
	if (imatch != -1) { sprintf(imbuf,  "%d", imatch);      imatchs = imbuf; }

	// determine the size
	size_t	msg_sz = sizeof(as_msg); // header
	if (ns)      msg_sz += ns_len          + sizeof(cl_msg_field); // fields
	if (mrjids)  msg_sz += strlen(mrjids)  + sizeof(cl_msg_field);
	if (imatchs) msg_sz += strlen(imatchs) + sizeof(cl_msg_field);
    if (margs) {
	    marg_sz = sizeof(cl_msg_field) + sizeof(int); // argc(int)
        for (int i = 0; i < margs->argc; i++) {
	        int klen  = strlen(margs->kargv[i]);
	        marg_sz   += (sizeof(int) + klen); // Length(int) + string_arg
	        int vlen  = strlen(margs->vargv[i]);
	        marg_sz   += (sizeof(int) + vlen); // Length(int) + string_arg
        }
	    msg_sz += marg_sz;
    }
    if (reg_mrjid) msg_sz += sizeof(cl_msg_field) + sizeof(int);

printf("msg_sz: %d marg_sz: %d sizeof(cl_msg_field): %d\n", msg_sz, marg_sz, sizeof(cl_msg_field));

    if (n_my_digests) {
	    msg_sz += sizeof(cl_msg_field) + 1 + (sizeof(cf_digest) * n_my_digests);
	    // ops
	    for (i = 0; i < n_values; i++) {
		    msg_sz += sizeof(cl_msg_op) + strlen(values[i].bin_name);
            if (0 != cl_value_to_op_get_size(&values[i], &msg_sz)) {
                fprintf(stderr, "illegal parameter: bad type %d write op %d\n",
                        values[i].object.type,i);
                return(-1);
            }
	    }
    }
    if (lmflen) {
        if (!reg_mrjid) assert(!"registering a map-reduce job requires an id");
	    msg_sz += sizeof(cl_msg_field) + 1 + lmflen;
	    msg_sz += sizeof(cl_msg_field) + 1 + lrflen;
	    msg_sz += sizeof(cl_msg_field) + 1 + lfflen;
    }
	
	// size too small? malloc!
	uint8_t	*buf;
	uint8_t *mbuf = 0;
	if ((*buf_r) && (msg_sz > *buf_sz_r)) {
		mbuf   = buf = malloc(msg_sz); if (!buf) return(-1);
		*buf_r = buf;
	} else buf = *buf_r;
	*buf_sz_r = msg_sz;
	memset(buf, 0, msg_sz);	// debug - shouldn't be required
	uint32_t generation = 0; // lay in some parameters
	if (cl_w_p) {
		if (cl_w_p->unique) {
			info2 |= CL_MSG_INFO2_WRITE_UNIQUE;
		}
		else if (cl_w_p->use_generation) {
			info2 |= CL_MSG_INFO2_GENERATION;
			generation = cl_w_p->generation;
		}
		else if (cl_w_p->use_generation_gt) {
			info2 |= CL_MSG_INFO2_GENERATION_GT;
			generation = cl_w_p->generation;
		}
		else if (cl_w_p->use_generation_dup) {
			info2 |= CL_MSG_INFO2_GENERATION_DUP;
			generation = cl_w_p->generation;
		}
	}
	
	uint32_t record_ttl      = cl_w_p ? cl_w_p->record_ttl : 0;
	uint32_t transaction_ttl = cl_w_p ? cl_w_p->timeout_ms : 0;
	
	// lay out the header - currently always 3
	//   digest array, lua_mapf, and the ns
	int n_fields;
    if (n_my_digests) {
        n_fields = 1 + (ns ? 1 : 0) + (imatchs ? 1 : 0);
        if (mrjid)  n_fields++;
        if (margs)  n_fields++;
    } else {
        if (lmflen) n_fields = 4 + (ns ? 1 : 0);
        else        n_fields = 0;
    }
printf("n_fields: %d\n", n_fields);

	buf = cl_write_header(buf, msg_sz, info1, info2, info3, generation, record_ttl, transaction_ttl, n_fields, n_values);
		
	// now the fields
	if (n_my_digests) {
	    buf = write_fields_batch_digests(buf, ns, ns_len, digests, nodes,
                                        n_digests, n_my_digests, my_node,
                                        mrjids, imatchs, margs, marg_sz);
    } else if (lmflen) {
	    buf = write_fields_lua_func_register(buf, ns, ns_len,
                                             lua_mapf, lmflen,
                                             lua_rdcf, lrflen,
                                             lua_fnzf, lfflen, reg_mrjid);
    } else assert(!"batch_compile() define n_my_digests OR lmflen");

	if (!buf) { if (mbuf) free(mbuf); return(-1); }

	// lay out the ops
	if (n_values) {
		cl_msg_op *op = (cl_msg_op *) buf;
		cl_msg_op *op_tmp;
		for (i = 0; i< n_values;i++) {
			if (values) { cl_value_to_op( &values[i], operator, 0, op);
			} else {      cl_value_to_op(0,0,&operations[i],op); }
	
			op_tmp = cl_msg_op_get_next(op);
			cl_msg_swap_op(op);
			op = op_tmp;
		}
	}
	return(0);	
}





#define STACK_BUF_SZ (1024 * 16) // provide a safe number for your system - linux tends to have 8M stacks these days
#define STACK_BINS 100

#define HACK_MAX_RESULT_CODE 100

static int do_batch_monte(cl_cluster *asc, int info1, int info2, int info3, char *ns, cf_digest *digests, cl_cluster_node **nodes, int n_digests, cl_bin *bins, cl_operator operator, cl_operation *operations, int n_ops, cl_cluster_node *node, int n_node_digests, citrusleaf_get_many_cb cb, void *udata, char *lua_mapf, int lmflen, char *lua_rdcf, int lrflen, char *lua_fnzf, int lfflen, int mrjid, int imatch, map_args_t *margs, int reg_mrjid) {

printf("do_batch_monte: n_digests: %d n_node_digests: %d\n", n_digests, n_node_digests);

	int rv = -1;
	uint8_t		rd_stack_buf[STACK_BUF_SZ];	
	uint8_t		*rd_buf = 0;
	size_t		rd_buf_sz = 0;
	uint8_t		wr_stack_buf[STACK_BUF_SZ];
	uint8_t		*wr_buf = wr_stack_buf;
	size_t		wr_buf_sz = sizeof(wr_stack_buf);

	rv = batch_compile(info1, info2, info3, ns, digests, nodes, n_digests,
                       node, n_node_digests, bins, operator, operations,
                       n_ops, &wr_buf, &wr_buf_sz, 0,
                       lua_mapf, lmflen, lua_rdcf, lrflen, lua_fnzf, lfflen,
                       mrjid, imatch, margs, reg_mrjid);
	if (rv != 0) {
		fprintf(stderr, " do batch monte: batch compile failed: some kind of intermediate error\n");
		return (rv);
	}

	int fd = cl_cluster_node_fd_get(node, false);
	if (fd == -1) { return(-1); }
	
	// send it to the cluster - non blocking socket, but we're blocking
	if (0 != cf_socket_write_forever(fd, wr_buf, wr_buf_sz)) { return(-1); }

	cl_proto 		proto;
	bool done = false;
	
	do { // multiple CL proto per response
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
			if (rd_buf == NULL)		return (-1);

			if ((rv = cf_socket_read_forever(fd, rd_buf, rd_buf_sz))) {
				fprintf(stderr, "network error: errno %d fd %d\n",rv, fd);
				if (rd_buf != rd_stack_buf)	{ free(rd_buf); }
				return(-1);
			}
		}
		if (proto.type == CL_PROTO_TYPE_CL_MSG_COMPRESSED) {
			uint8_t *new_rd_buf   = NULL;
			size_t  new_rd_buf_sz = 0;
			rv = batch_decompress(rd_buf, rd_buf_sz, &new_rd_buf, &new_rd_buf_sz);
			if (rv != 0) {
				fprintf(stderr, "could not decompress compressed message: error %d\n",rv);
				if (rd_buf != rd_stack_buf)	{ free(rd_buf); }
			}				
				
			if (rd_buf != rd_stack_buf)	{ free(rd_buf); }
			rd_buf = new_rd_buf;
			rd_buf_sz = new_rd_buf_sz;
			
			// also re-touch the proto - not certain if this matters
			proto.sz = rd_buf_sz;
			proto.type = CL_PROTO_TYPE_CL_MSG;

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
			
//ALCHEMY RUSS HACK
            if (msg->result_code >= HACK_MAX_RESULT_CODE && cb) {
				(*cb) (NULL, NULL, NULL, 0, 0, NULL, 1, true, (void *)msg->result_code);
				done = true;
                rv = 0;
            }

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
			
			if (msg->info3 & CL_MSG_INFO3_LAST)	{
#ifdef DEBUG				
				fprintf(stderr, "received final message\n");
#endif				
				done = true;
			}

			if (cb && (msg->n_ops || (msg->info1 & CL_MSG_INFO1_NOBINDATA))) {
				// got one good value? call it a success!
				// (Note:  In the key exists case, there is no bin data.)
				(*cb) ( ns_ret, keyd, set_ret, msg->generation, msg->record_ttl, bins, msg->n_ops, false /*islast*/, udata);
				rv = 0;
			}
//			else
//				fprintf(stderr, "received message with no bins, signal of an error\n");

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
		
		if (rd_buf && (rd_buf != rd_stack_buf))	{
			free(rd_buf);
			rd_buf = 0;
		}

	} while ( done == false );

	if (wr_buf != wr_stack_buf) {
		free(wr_buf);
		wr_buf = 0;
	}
	
	cl_cluster_node_fd_put(node, fd, false);
	
	goto Final;
	
Final:	
	
#ifdef DEBUG_VERBOSE	
	fprintf(stderr, "exited loop: rv %d\n", rv );
#endif	
	
	return(rv);
}

static cf_atomic32 batch_initialized = 0;

#define N_BATCH_THREADS 6
static cf_queue *g_batch_q = 0;
static pthread_t g_batch_th[N_BATCH_THREADS];


//
// These externally visible functions are exposed through citrusleaf.h
//

typedef struct {
	
	// these sections are the same for the same query
	cl_cluster 	*asc; 
    int          info1;
	int          info2;
	int          info3;
	char 		*ns;
	cf_digest 	*digests; 
	cl_cluster_node **nodes;
	int 		n_digests; 
	bool 		get_key;
	cl_bin 		*bins;         // Bins. If this is used, 'operation' should be null, and 'operator' should be the operation to be used on the bins
	cl_operator     operator;      // Operator.  The single operator used on all the bins, if bins is non-null
	cl_operation    *operations;   // Operations.  Set of operations (bins + operators).  Should be used if bins is not used.
	int		n_ops;          // Number of operations (count of elements in 'bins' or count of elements in 'operations', depending on which is used. 
	citrusleaf_get_many_cb cb; 
	void *udata;

	cf_queue *complete_q;
	
	// this is different for every work
	cl_cluster_node *my_node;				
	int				my_node_digest_count;
	
	int 			index; // debug only
	
    unsigned int  mrjid;
    char         *lua_mapf;
    int           lmflen;
    char         *lua_rdcf;
    int           lrflen;
    char         *lua_fnzf;
    int           lfflen;

    int           imatch;
    map_args_t   *margs;
    int           reg_mrjid;
} digest_work;


static void *
batch_worker_fn(void *dummy)
{
	do {
		digest_work work;
		if (0 != cf_queue_pop(g_batch_q, &work, CF_QUEUE_FOREVER)) {
			fprintf(stderr, "queue pop failed\n");
		}
		
		/* See function citrusleaf_batch_shutdown() for more details */
		if(!work.digests && !work.lua_mapf) { pthread_exit(NULL); }

		int an_int = do_batch_monte(work.asc, work.info1, work.info2, work.info3, work.ns, work.digests, work.nodes, work.n_digests, work.bins, work.operator, work.operations, work.n_ops, work.my_node, work.my_node_digest_count, work.cb, work.udata, work.lua_mapf, work.lmflen, work.lua_rdcf, work.lrflen, work.lua_fnzf, work.lfflen, work.mrjid, work.imatch, work.margs, work.reg_mrjid);
		
		cf_queue_push(work.complete_q, (void *) &an_int);
		
	} while (1);
}

static cl_rv citrusleaf_sik_traversal(cl_cluster *asc, char *ns, const cf_digest *digests, int n_digests, cl_bin *bins, int n_bins, bool get_key, citrusleaf_get_many_cb cb, void *udata, unsigned int mrjid, char *lua_mapf, char *lua_rdcf, char *lua_fnzf, int imatch, map_args_t *margs, int reg_mrjid) {
    int lmflen = lua_mapf ? strlen(lua_mapf) : 0;
    int lrflen = lua_rdcf ? strlen(lua_rdcf) : 0;
    int lfflen = lua_fnzf ? strlen(lua_fnzf) : 0;

    printf("citrusleaf_sik_traversal: mrjid: %d "\
           "lmflen: %d lrflen: %d lfflen: %d\n",
           mrjid, lmflen, lrflen, lfflen);

	int	n_nodes = cf_vector_size(&asc->node_v);
printf("n_nodes: %d\n", n_nodes);
	cl_cluster_node **nodes = malloc(sizeof(cl_cluster_node *) * n_nodes);
	if (!nodes) { fprintf(stderr, " allocation failed "); return(-1); }
    for (uint i = 0; i < n_nodes; i++) {
        nodes[i] = cf_vector_pointer_get(&asc->node_v, i);
    }

    printf("citrusleaf_sik_traversal: n_digests: %d n_bins: %d\n",
           n_digests, n_bins);

	// Note:  The digest exists case does not retrieve bin data.
	digest_work work;
	work.asc        = asc;
	work.info1      = CL_MSG_INFO1_READ;
	work.info2      = 0;
	work.info3      = 0;
	work.ns         = ns;
	work.digests    = (cf_digest *)digests;
	work.nodes      = nodes;
	work.n_digests  = n_digests;
	work.get_key    = get_key;
	work.bins       = bins;
	work.operator   = CL_OP_READ;
	work.operations = 0;
	work.n_ops      = n_bins;
	work.cb         = cb;
	work.udata      = udata;
    work.mrjid      = mrjid;
    work.lua_mapf   = lua_mapf;
    work.lmflen     = lmflen;
    work.lua_rdcf   = lua_rdcf;
    work.lrflen     = lrflen;
    work.lua_fnzf   = lua_fnzf;
    work.lfflen     = lfflen;
    work.imatch     = imatch;
    work.margs      = margs;
    work.reg_mrjid  = reg_mrjid;
	work.complete_q = cf_queue_create(sizeof(int),true);

	// dispatch work to the worker queue to allow the transactions in parallel
	for (int i=0;i<n_nodes;i++) {
		// fill in per-request specifics
		work.my_node = nodes[i];
printf("%d: n_nodes: %d my_node: %p\n", i, n_nodes, work.my_node);
		work.my_node_digest_count = n_digests;
		work.index = i;
		// dispatch - copies data
		cf_queue_push(g_batch_q, &work);
	}
	
	// wait for the work to complete
	int retval = 0;
	for (int i=0;i<n_nodes;i++) {
		int z;
		cf_queue_pop(work.complete_q, &z, CF_QUEUE_FOREVER);
		if (z != 0) retval = z;
	}
	
	// free and return what needs freeing and putting
	cf_queue_destroy(work.complete_q);
	//for (int i=0;i<n_digests;i++) { cl_cluster_node_put(nodes[i]); }
	free(nodes);

	if (retval != 0) return( CITRUSLEAF_FAIL_CLIENT );
	else             return 0;
}

int   CurrentMRJid      = -1;
char *CurrentLuaMapFunc = NULL;
char *CurrentLuaRdcFunc = NULL;
char *CurrentLuaFnzFunc = NULL;

cl_rv citrusleaf_register_lua_function(cl_cluster *asc, char *ns, citrusleaf_get_many_cb cb, char *lua_mapf, char *lua_rdcf, char *lua_fnzf, int reg_mrjid) {
    CurrentLuaMapFunc = lua_mapf;
    CurrentLuaRdcFunc = lua_rdcf;
    CurrentLuaFnzFunc = lua_fnzf;
    return citrusleaf_sik_traversal(asc, ns, NULL, 0, NULL, 0, 0, cb, NULL, 0, lua_mapf, lua_rdcf, lua_fnzf, -1, NULL, reg_mrjid);
}
cl_rv citrusleaf_get_sik_digest(cl_cluster *asc, char *ns, const cf_digest *digests, int n_digests, cl_bin *bins, int n_bins, bool get_key, citrusleaf_get_many_cb cb, void *udata, int imatch) {
    return citrusleaf_sik_traversal(asc, ns, digests, n_digests, bins, n_bins, get_key, cb, udata, 0, NULL, NULL, NULL, imatch, NULL, 0);
}
cl_rv citrusleaf_run_mr_sik_digest(cl_cluster *asc, char *ns, const cf_digest *digests, int n_digests, cl_bin *bins, int n_bins, bool get_key, citrusleaf_get_many_cb cb, void *udata, int mrjid, int imatch, map_args_t *margs) {
    CurrentMRJid = mrjid;
    return citrusleaf_sik_traversal(asc, ns, digests, n_digests, bins, n_bins, get_key, cb, udata, mrjid, NULL, NULL, NULL, imatch, margs, 0);
}



#define MAX_NODES 32

static cl_rv
do_get_exists_many_digest(cl_cluster *asc, char *ns, const cf_digest *digests, int n_digests, cl_bin *bins, int n_bins, bool get_key, bool get_bin_data, citrusleaf_get_many_cb cb, void *udata)
{
	// fast path: if there's only one node, or the number of digests is super short, just dispatch to the server directly

	// TODO FAST PATH

	//
	// allocate the digest-node array, and populate it
	// 
	cl_cluster_node **nodes = malloc( sizeof(cl_cluster_node *)  * n_digests);
	if (!nodes) {
		fprintf(stderr, " allocation failed ");
		return(-1);
	}
	
	// loop through all digests and determine a node
	for (int i=0;i<n_digests;i++) {
		
		nodes[i] = cl_cluster_node_get(asc, ns, &digests[i], true/*write, but that's Ok*/);
		
		// not sure if this is required - it looks like cluster_node_get automatically calls random?
		// it's certainly safer though
		if (nodes[i] == 0) {
			fprintf(stderr, "index %d: no specific node, getting random\n",i);
			nodes[i] = cl_cluster_node_get_random(asc);
		}
		if (nodes[i] == 0) {
			fprintf(stderr, "index %d: can't get any node\n", i);
			free(nodes);
			return(-1);
		}
			
	}

	// find unique set
	cl_cluster_node *unique_nodes[MAX_NODES];
	int				unique_nodes_count[MAX_NODES];
	int 			n_nodes = 0;
	for (int i=0;i<n_digests;i++) {
		// look to see if nodes[i] is in the unique list
		int j;
		for (j=0;j<n_nodes;j++) {
			if (unique_nodes[j] == nodes[i]) {
				unique_nodes_count[j]++;
				break;
			}
		}
		// not found, insert in nodes list
		if (j == n_nodes) {
			unique_nodes[n_nodes] = nodes[i];
			unique_nodes_count[n_nodes] = 1;
			n_nodes++;
		}
	}
	
	// 
	// Note:  The digest exists case does not retrieve bin data.
	//
	digest_work work;
	work.asc = asc;
	work.info1 = CL_MSG_INFO1_READ | (get_bin_data ? 0 : CL_MSG_INFO1_NOBINDATA);
	work.info2 = 0;
	work.ns = ns;
	work.digests = (cf_digest *) digests; // discarding const to make compiler happy
	work.nodes = nodes;
	work.n_digests = n_digests;
	work.get_key = get_key;
	work.bins = bins;
	work.operator = CL_OP_READ;
	work.operations = 0;
	work.n_ops = n_bins;
	work.cb = cb;
	work.udata = udata;
	
	work.complete_q = cf_queue_create(sizeof(int),true);
	//
	// dispatch work to the worker queue to allow the transactions in parallel
	//
	for (int i=0;i<n_nodes;i++) {
		
		// fill in per-request specifics
		work.my_node = unique_nodes[i];
		work.my_node_digest_count = unique_nodes_count[i];
		work.index = i;
		
//		fprintf(stderr,"dispatching work: index %d node %p n_digeset %d\n",i,work.my_node,work.my_node_digest_count); 
		
		// dispatch - copies data
		cf_queue_push(g_batch_q, &work);
	}
	
	// wait for the work to complete
	int retval = 0;
	for (int i=0;i<n_nodes;i++) {
		int z;
		cf_queue_pop(work.complete_q, &z, CF_QUEUE_FOREVER);
		if (z != 0)
			retval = z;
	}
	
	// free and return what needs freeing and putting
	cf_queue_destroy(work.complete_q);
	for (int i=0;i<n_digests;i++) {
		cl_cluster_node_put(nodes[i]);	
	}
	free(nodes);
	if (retval != 0) {
		return( CITRUSLEAF_FAIL_CLIENT );
	} 
	else {
		return ( 0 );
	}

}

cl_rv
citrusleaf_get_many_digest(cl_cluster *asc, char *ns, const cf_digest *digests, int n_digests, cl_bin *bins, int n_bins, bool get_key, citrusleaf_get_many_cb cb, void *udata)
{
printf("IN citrusleaf_get_many_digest\n");
	return do_get_exists_many_digest(asc, ns, digests, n_digests, bins, n_bins, get_key, true, cb, udata);
}


cl_rv
citrusleaf_exists_many_digest(cl_cluster *asc, char *ns, const cf_digest *digests, int n_digests, cl_bin *bins, int n_bins, bool get_key, citrusleaf_get_many_cb cb, void *udata)
{
	return do_get_exists_many_digest(asc, ns, digests, n_digests, bins, n_bins, get_key, false, cb, udata);
}


//
// Initializes the shared thread pool and whatever queues
//

int
citrusleaf_batch_init()
{
	if (1 == cf_atomic32_incr(&batch_initialized)) {

		// create dispatch queue
		g_batch_q = cf_queue_create(sizeof(digest_work), true);
		
		// create thread pool
		for (int i=0;i<N_BATCH_THREADS;i++)
			pthread_create(&g_batch_th[i], 0, batch_worker_fn, 0);

	}
	
	return(0);	
}

/*
* This function is used to close the batch threads gracefully. The earlier plan was to use pthread_cancel
* with pthread_join. When the cancellation request comes, the thread is waiting on a cond variable. (see batch_worker_fn)
* The pthread_cond_wait is a cancellation point. If a thread that's blocked on a condition variable is canceled,
* the thread reacquires the mutex that's guarding the condition variable, so that the thread's cleanup handlers run
* in the same state as the critical code before and after the call to this function. If some other thread owns the lock,
* the canceled thread blocks until the mutex is available. So the first thread, holds the mutex, gets unlocked on the cond
* variable and then dies. The case maybe that the mutex might be released or not. When the next thread gets the cancellation
* request, it waits on the mutex to get free which may never happen and it blocks itself forever. This is why we use our own
* thread cleanup routine. We push NULL work items in the queue. When we pop them, we signal the thread to exit. We join on the
* thread thereafter.
*/
void citrusleaf_batch_shutdown() {

        int i;
        digest_work work;
        memset(&work,0,sizeof(digest_work));
        for(i=0;i<N_BATCH_THREADS;i++) {
                cf_queue_push(g_batch_q,&work);
        }
        for(i=0;i<N_BATCH_THREADS;i++) {
                pthread_join(g_batch_th[i],NULL);
        }
}

