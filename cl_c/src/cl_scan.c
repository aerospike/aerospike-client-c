/*
 * The scan interface 
 *
 *
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
#include <zlib.h>


#include "citrusleaf/citrusleaf.h"
#include "citrusleaf/cl_cluster.h"
#include "citrusleaf/citrusleaf-internal.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/proto.h"
#include "citrusleaf/cf_socket.h"


//
// Omnibus internal function that the externals can map to which returns many results
//
// This function is a bit different from the single-type one, because this one
// has to read multiple proto-messages, and multiple cl_msg within them. So it really
// does read just 8 byte, then the second header each time. More system calls, but much
// cleaner.

#define STACK_BINS 100


static int
do_scan_monte(cl_cluster *asc, char *node_name, uint operation_info, uint operation_info2, const char *ns, const char *set, 
	cl_bin *bins, int n_bins, uint8_t scan_pct, 
	citrusleaf_get_many_cb cb, void *udata, cl_scan_parameters *scan_opt)
{
	int rv = -1;

	uint8_t		rd_stack_buf[STACK_BUF_SZ];	
	uint8_t		*rd_buf = 0;
	size_t		rd_buf_sz = 0;
	uint8_t		wr_stack_buf[STACK_BUF_SZ];
	uint8_t		*wr_buf = wr_stack_buf;
	size_t		wr_buf_sz = sizeof(wr_stack_buf);

	cl_scan_param_field	scan_param_field;

	if (scan_opt) {
		scan_param_field.scan_pct = scan_pct>100? 100:scan_pct;
		scan_param_field.byte1 = (scan_opt->priority<<4) | (scan_opt->fail_on_cluster_change<<3);
	}

	// we have a single namespace and/or set to get
	if (cl_compile(operation_info, operation_info2, 0, ns, set, 0, 0, 0, 0, 0, 0, &wr_buf, &wr_buf_sz, 0, NULL, 0,
			scan_opt ? &scan_param_field : NULL)) {
		return(rv);
	}
	
#ifdef DEBUG_VERBOSE
	dump_buf("sending request to cluster:", wr_buf, wr_buf_sz);
#endif

	int fd;
	cl_cluster_node *node = 0;

	// Get an FD from a cluster
	if (node_name) {
		node = cl_cluster_node_get_byname(asc,node_name);
		// grab a reservation
		if (node)
			cf_client_rc_reserve(node);
	} else {
		pthread_mutex_lock(&asc->LOCK);
		node = cl_cluster_node_get_random(asc);
		pthread_mutex_unlock(&asc->LOCK);
	}
	if (!node) {
#ifdef DEBUG
		cf_debug("warning: no healthy nodes in cluster, failing");
#endif			
		return(-1);
	}
	fd = cl_cluster_node_fd_get(node, false, asc->nbconnect);
	if (fd == -1) {
#ifdef DEBUG			
		cf_debug("warning: node %s has no file descriptors, retrying transaction", node->name);
#endif
		return(-1);
	}
	
	// send it to the cluster - non blocking socket, but we're blocking
	if (0 != cf_socket_write_forever(fd, wr_buf, wr_buf_sz)) {
#ifdef DEBUG			
		cf_debug("Citrusleaf: write timeout or error when writing header to server - %d fd %d errno %d", rv, fd, errno);
#endif
		return(-1);
	}

	cl_proto 		proto;
	bool done = false;
	
	do { // multiple CL proto per response
		
		// Now turn around and read a fine cl_pro - that's the first 8 bytes that has types and lengths
		if ((rv = cf_socket_read_forever(fd, (uint8_t *) &proto, sizeof(cl_proto) ) ) ) {
			cf_error("network error: errno %d fd %d",rv, fd);
			return(-1);
		}
#ifdef DEBUG_VERBOSE
		dump_buf("read proto header from cluster", (uint8_t *) &proto, sizeof(cl_proto));
#endif	
		cl_proto_swap(&proto);

		if (proto.version != CL_PROTO_VERSION) {
			cf_error("network error: received protocol message of wrong version %d", proto.version);
			return(-1);
		}
		if (proto.type != CL_PROTO_TYPE_CL_MSG) {
			cf_error("network error: received incorrect message version %d", proto.type);
			return(-1);
		}
		
		// second read for the remainder of the message - expect this to cover lots of data, many lines
		//
		// if there's no error
		rd_buf_sz =  proto.sz;
		if (rd_buf_sz > 0) {
                                                         
//            cf_debug("message read: size %u",(uint)proto.sz);

			if (rd_buf_sz > sizeof(rd_stack_buf))
				rd_buf = malloc(rd_buf_sz);
			else
				rd_buf = rd_stack_buf;
			if (rd_buf == NULL) 		return (-1);

			if ((rv = cf_socket_read_forever(fd, rd_buf, rd_buf_sz))) {
				cf_error("network error: errno %d fd %d", rv, fd);
				if (rd_buf != rd_stack_buf)	{ free(rd_buf); }
				return(-1);
			}
// this one's a little much: printing the entire body before printing the other bits			
#ifdef DEBUG_VERBOSE
			dump_buf("read msg body header (multiple msgs)", rd_buf, rd_buf_sz);
#endif	
		}
		
		// process all the cl_msg in this proto
		uint8_t *buf = rd_buf;
		uint pos = 0;
		cl_bin stack_bins[STACK_BINS];
		cl_bin *bins_local;
		
		while (pos < rd_buf_sz) {

#ifdef DEBUG_VERBOSE
			dump_buf("individual message header", buf, sizeof(cl_msg));
#endif	
			
			uint8_t *buf_start = buf;
			cl_msg *msg = (cl_msg *) buf;
			cl_msg_swap_header(msg);
			buf += sizeof(cl_msg);
			
			if (msg->header_sz != sizeof(cl_msg)) {
				cf_error("received cl msg of unexpected size: expecting %zd found %d, internal error",
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
				if (mf->type == CL_MSG_FIELD_TYPE_KEY) {
					cf_error("read: found a key - unexpected");
				}
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

#ifdef DEBUG_VERBOSE
			cf_debug("message header fields: nfields %u nops %u", msg->n_fields, msg->n_ops);
#endif


			if (msg->n_ops > STACK_BINS) {
				bins_local = malloc(sizeof(cl_bin) * msg->n_ops);
			}
			else {
				bins_local = stack_bins;
			}
			if (bins_local == NULL) {
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
				cf_debug("op receive: %p size %d op %d ptype %d pversion %d namesz %d",
					op,op->op_sz, op->op, op->particle_type, op->version, op->name_sz);				
#endif			

#ifdef DEBUG_VERBOSE
				dump_buf("individual op (host order)", (uint8_t *) op, op->op_sz + sizeof(uint32_t));
#endif	

				cl_set_value_particular(op, &bins_local[i]);
				op = cl_msg_op_get_next(op);
			}
			buf = (uint8_t *) op;
			
			if (msg->result_code != CL_RESULT_OK) {
				// Special case - if we scan a set name that doesn't exist on a
				// node, it will return "not found" - we unify this with the
				// case where OK is returned and no callbacks were made. [AKG]
				if (msg->result_code == CL_RESULT_NOTFOUND) {
					msg->result_code = CL_RESULT_OK;
				}
				rv = (int)msg->result_code;
				done = true;
			}
			else if (msg->info3 & CL_MSG_INFO3_LAST)	{
#ifdef DEBUG
				cf_debug("received final message");
#endif
				done = true;
			}
			else if ((msg->n_ops) || (operation_info & CL_MSG_INFO1_NOBINDATA)) {
				// got one good value? call it a success!
				(*cb) ( ns_ret, keyd, set_ret, msg->generation, msg->record_ttl, bins_local, msg->n_ops, false /*islast*/, udata);
				rv = 0;
			}
//			else
//				cf_debug("received message with no bins, signal of an error");

			if (bins_local != stack_bins) {
				free(bins_local);
				bins_local = 0;
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
	cl_cluster_node_put(node);
	node = 0;
	
#ifdef DEBUG_VERBOSE	
	cf_debug("exited loop: rv %d", rv );
#endif	
	
	return(rv);
}


extern cl_rv
citrusleaf_scan(cl_cluster *asc, char *ns, char *set, cl_bin *bins, int n_bins, bool get_key, citrusleaf_get_many_cb cb, void *udata, bool nobindata)
{
	if (n_bins != 0) {
		cf_error("citrusleaf get many: does not yet support bin-specific requests");
	}

	uint info=0;
	if (nobindata == true) {
		info = (CL_MSG_INFO1_READ | CL_MSG_INFO1_NOBINDATA);
	} else {
		info = CL_MSG_INFO1_READ; 
	}

	return( do_scan_monte( asc, NULL, info, 0, ns, set, bins,n_bins, 100, cb, udata, NULL ) );
}

extern cl_rv
citrusleaf_scan_node (cl_cluster *asc, char *node_name, char *ns, char *set, cl_bin *bins, int n_bins, bool nobindata, uint8_t scan_pct,
		citrusleaf_get_many_cb cb, void *udata, cl_scan_parameters *scan_param)
{

	if (n_bins != 0) {
		cf_error("citrusleaf get many: does not yet support bin-specific requests");
	}

	uint info=0;
	if (nobindata == true) {
		info = (CL_MSG_INFO1_READ | CL_MSG_INFO1_NOBINDATA);
	} else {
		info = CL_MSG_INFO1_READ; 
	}

	cl_scan_parameters default_scan_param;
	if (scan_param == NULL) {
		cl_scan_parameters_set_default(&default_scan_param);
		scan_param = &default_scan_param;
	}
		
	return( do_scan_monte( asc, node_name, info, 0, ns, set, bins, n_bins, scan_pct, cb, udata, scan_param ) ); 
}

cf_vector *
citrusleaf_scan_all_nodes (cl_cluster *asc, char *ns, char *set, cl_bin *bins, int n_bins, bool nobindata, uint8_t scan_pct,
		citrusleaf_get_many_cb cb, void *udata, cl_scan_parameters *scan_param)
{
	char *node_names = NULL;	
	int	n_nodes = 0;
	cl_cluster_get_node_names(asc, &n_nodes, &node_names);

	if (n_nodes == 0) {
		cf_error("citrusleaf scan all nodes: don't have any nodes?");
		return NULL;
	}

	cf_vector *rsp_v = cf_vector_create(sizeof(cl_node_response), n_nodes, 0);
	if (rsp_v == NULL) {
		cf_error("citrusleaf scan all nodes: cannot allocate for response array for %d nodes", n_nodes);
		free(node_names);
		return NULL;
	}
	 
	if (scan_param && scan_param->concurrent_nodes) {
		cf_error("citrusleaf scan all nodes: concurrent node scanning not yet supported");
	} else {
		char *nptr = node_names;
		for (int i=0;i< n_nodes; i++) {
			cl_rv r = citrusleaf_scan_node (asc, nptr, ns, set, bins, n_bins, nobindata, scan_pct,
				cb, udata, scan_param);
			cl_node_response resp_s;
			resp_s.node_response = r;
			memcpy(resp_s.node_name,nptr,NODE_NAME_SIZE);			
			cf_vector_append(rsp_v, (void *)&resp_s);
			nptr+=NODE_NAME_SIZE;					
		}
	}
	free(node_names);
	return rsp_v;
}

