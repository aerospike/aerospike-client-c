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
#include <time.h> // for job ID


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
	if (cl_compile(operation_info, operation_info2, 0/*info3*/, ns, set, 
			0/*key*/, 0/*digest*/, 0/*values*/, 0/*op*/, 0/*operations*/, 
			0/*n_values*/, &wr_buf, &wr_buf_sz, 0/*w_p*/, NULL/*d_ret*/, 0/*trid*/,
			scan_opt ? &scan_param_field : NULL, 0/*sproc*/)) {
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
		node = cl_cluster_node_get_random(asc);
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
		cl_bin *bins;
		
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
				cf_debug("op receive: %p size %d op %d ptype %d pversion %d namesz %d",
					op,op->op_sz, op->op, op->particle_type, op->version, op->name_sz);				
#endif			

#ifdef DEBUG_VERBOSE
				dump_buf("individual op (host order)", (uint8_t *) op, op->op_sz + sizeof(uint32_t));
#endif	

				cl_set_value_particular(op, &bins[i]);
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
				(*cb) ( ns_ret, keyd, set_ret, msg->generation, msg->record_ttl, bins, msg->n_ops, false /*islast*/, udata);
				rv = 0;
			}
//			else
//				cf_debug("received message with no bins, signal of an error");

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



// For now, adding sproc scan implementation here:

static inline uint64_t
create_job_uid()
{
	return (uint64_t)time(NULL); // TODO - add random number in high 32 bits
}

const int ATTEMPT_MILLISEC = 500;
const uint8_t DISCONNECTED_JOB = 1 << 2;

//------------------------------------------------
// Do a no-retries transaction, where no returned
// msg body is expected.
//
// (May later generalize this for use elsewhere...)
//
static cl_rv
try_transaction_once(int fd, uint8_t *wr_buf, size_t wr_buf_sz)
{
	// Send packet to server node.
	int wr_result = cf_socket_write_timeout(fd, wr_buf, wr_buf_sz,
			cf_getms() + ATTEMPT_MILLISEC, ATTEMPT_MILLISEC);

	if (wr_result != 0) {
		fprintf(stderr, "try trans: socket write error %d\n", wr_result);
		return wr_result == ETIMEDOUT ?
				CITRUSLEAF_FAIL_TIMEOUT : CITRUSLEAF_FAIL_CLIENT;
	}

	// Read the reply into msg.
	as_msg msg;
	int rd_result = cf_socket_read_timeout(fd, (uint8_t *)&msg, sizeof(as_msg),
			cf_getms() + ATTEMPT_MILLISEC, ATTEMPT_MILLISEC);

	if (rd_result != 0) {
		fprintf(stderr, "try trans: socket read error %d\n", rd_result);
		return rd_result == ETIMEDOUT ?
				CITRUSLEAF_FAIL_TIMEOUT : CITRUSLEAF_FAIL_CLIENT;
	}

	cl_proto_swap(&msg.proto);
	cl_msg_swap_header(&msg.m);

	size_t rd_buf_sz = msg.proto.sz - msg.m.header_sz;

	// We don't expect a message body - if there is one, read it and dump it...
	if (rd_buf_sz > 0) {
		fprintf(stderr, "try trans: dumping unexpected msg body\n");

		uint8_t rd_stack_buf[STACK_BUF_SZ];
		uint8_t *rd_buf;

		if (rd_buf_sz > sizeof(rd_stack_buf)) {
			rd_buf = malloc(rd_buf_sz);

			if (! rd_buf) {
				fprintf(stderr, "try trans: can't alloc\n");
				return CITRUSLEAF_FAIL_CLIENT;
			}
		}
		else {
			rd_buf = rd_stack_buf;
		}

		rd_result = cf_socket_read_timeout(fd, rd_buf, rd_buf_sz,
					cf_getms() + ATTEMPT_MILLISEC, ATTEMPT_MILLISEC);

		if (rd_buf != rd_stack_buf) {
			free(rd_buf);
		}

		if (rd_result != 0) {
			fprintf(stderr, "try trans: socket read error %d\n", rd_result);
			return rd_result == ETIMEDOUT ?
					CITRUSLEAF_FAIL_TIMEOUT : CITRUSLEAF_FAIL_CLIENT;
		}
	}

	// The msg result is the thing to return here...
	return msg.m.result_code;
}

//------------------------------------------------
// Tell a particular node to start a sproc job.
// (Won't keep connection to node open since we
// don't expect per-record results sent back.)
//
static cl_rv
start_sproc_job(cl_cluster *asc, const char *node_name, char *ns, char *set,
		cl_sproc_def *sproc_def, cl_scan_parameters *scan_p, uint64_t job_uid)
{
	uint8_t wr_stack_buf[STACK_BUF_SZ];
	uint8_t *wr_buf = wr_stack_buf;
	size_t wr_buf_sz = sizeof(wr_stack_buf);

	cl_scan_param_field	scan_param_field;

	scan_param_field.scan_pct = 100;
	scan_param_field.byte1 = (scan_p->priority << 4) |
			(scan_p->fail_on_cluster_change << 3) | DISCONNECTED_JOB;

	if (cl_compile(0/*info1*/, CL_MSG_INFO2_WRITE, 0/*info3*/, ns, set,
			0/*key*/, 0/*digest*/, 0/*values*/, 0/*op*/, 0/*operations*/,
			0/*n_values*/, &wr_buf, &wr_buf_sz, 0/*w_p*/, NULL/*d_ret*/,
			job_uid, &scan_param_field, sproc_def)) {
		fprintf(stderr, "start sproc job %s: fail cl_compile\n", node_name);
		return CITRUSLEAF_FAIL_CLIENT;
	}

	// Get the specified node. No other node will do...
	cl_cluster_node *node_p = cl_cluster_node_get_byname(asc, node_name);
	cl_rv rv = CITRUSLEAF_FAIL_CLIENT;

	if (node_p) {
		cf_client_rc_reserve(node_p);

		// Get an open socket to the node.
		int fd = cl_cluster_node_fd_get(node_p, false, asc->nbconnect);

		if (fd != -1) {
			// Try starting the job on the node.
			rv = try_transaction_once(fd, wr_buf, wr_buf_sz);
			cl_cluster_node_fd_put(node_p, fd, false);
		}
		else {
			fprintf(stderr, "start node tscan %s: can't get fd\n", node_name);
		}

		cl_cluster_node_put(node_p);
	}
	else {
		fprintf(stderr, "start node tscan %s: no such node\n", node_name);
	}

	if (wr_buf != wr_stack_buf) {
		free(wr_buf);
	}

	return rv;
}

//------------------------------------------------
// Execute specified sproc job on all server
// nodes. A job-UID is assigned and returned, to
// be used for future management of this job. If
// job is expected to generate & return results
// per key, cb must be passed in this call.
//
cf_vector *
citrusleaf_sproc_execute_all_nodes(cl_cluster *asc, char *ns, char *set,
		const char *package_name, const char *sproc_name,
		const cl_sproc_params *sproc_params, citrusleaf_get_many_cb cb,
		void *udata, cl_scan_parameters *scan_p, uint64_t *job_uid_p)
{
	// Caller must retrieve the job ID. TODO - really ???
	if (! job_uid_p) {
		fprintf(stderr, "citrusleaf sproc execute all nodes: null job_uid_p\n");
		return NULL;
	}

	// Use default scan parameters if none are provided. Note that client-
	// related parameters are ignored.
	cl_scan_parameters scan_params;

	if (! scan_p) {
		cl_scan_parameters_set_default(&scan_params);
		scan_p = &scan_params;
	}

	// TODO - if there's a cb, we'll do the multi-threaded, stay-connected ops.
	if (cb) {
		fprintf(stderr, "citrusleaf sproc execute all nodes: cb unsupported\n");
		return NULL;
	}

	// Get all the node names.
	char *node_names = NULL;
	int	n_nodes = 0;

	cl_cluster_get_node_names(asc, &n_nodes, &node_names);

	if (n_nodes == 0) {
		fprintf(stderr, "citrusleaf sproc execute all nodes: no nodes?\n");
		return NULL;
	}

	// Create the result vector.
	cf_vector *vec_p = cf_vector_create(sizeof(cl_node_response), n_nodes, 0);

	if (! vec_p) {
		fprintf(stderr, "citrusleaf sproc execute all nodes: can't alloc\n");
		free(node_names);
		return NULL;
	}

	// Assign a job ID.
	*job_uid_p = create_job_uid();

	// Wrap sproc-related params.
	cl_sproc_def sproc_def;

	sproc_def.package = package_name;
	sproc_def.fname = sproc_name;
	sproc_def.params = sproc_params;

	// In series, try to start this job on every node.
	const char *node_name = node_names;

	for (int n = 0; n < n_nodes; n++) {
		cl_node_response response;

		response.node_response = start_sproc_job(asc, node_name, ns, set,
				&sproc_def, scan_p, *job_uid_p);

		strcpy(response.node_name, node_name);
		node_name += NODE_NAME_SIZE;

		cf_vector_append(vec_p, (void *)&response);
	}

	free(node_names);

	return vec_p;
}
