/******************************************************************************
 * Copyright 2008-2013 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to 
 * deal in the Software without restriction, including without limitation the 
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 * sell copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *****************************************************************************/

#include <sys/types.h>
#include <stdio.h>
#include <errno.h> //errno
#include <stdlib.h> //fprintf
#include <unistd.h> // close
#include <string.h>
#include <pthread.h>
#include <fcntl.h>
#include <zlib.h>

#include <aerospike/as_cluster.h>

#include <citrusleaf/cf_socket.h>
#include <citrusleaf/cf_proto.h>

#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/cl_types.h>

#include "internal.h"


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

	// uint64_t now = cf_getms();
	
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
    	cf_error("batch_decompress: could not malloc %"PRIu64" bytes", b_sz_alloc);
    	inflateEnd(&strm);
    	return(-1);
    }
    
    strm.avail_in = (uInt)in_sz - 8;
    strm.next_in = in_buf + 8;
    
	strm.avail_out = (uInt)b_sz_alloc; // round up: seems to like that
	strm.next_out = b;

	rv = inflate(&strm, Z_FINISH);

	if (rv != Z_STREAM_END) {
		cf_error("could not deflate data: zlib error %d (check zlib.h)", rv);
		free(b);
		inflateEnd(&strm);
		return(-1);
	}
    	
	inflateEnd(&strm);
    
    *out_buf = b;
    *out_sz = b_sz_alloc - strm.avail_out;
	
	return(0);	
}


static uint8_t *
write_fields_batch_digests(uint8_t *buf, char *ns, int ns_len, cf_digest *digests, as_node **nodes, int n_digests, 
	int n_my_digests, as_node *my_node )
{
	
	// lay out the fields
	cl_msg_field *mf = (cl_msg_field *) buf;
	cl_msg_field *mf_tmp = mf;
	
	if (ns) {
		mf->type = CL_MSG_FIELD_TYPE_NAMESPACE;
		mf->field_sz = ns_len + 1;
		memcpy(mf->data, ns, ns_len);
		mf_tmp = cl_msg_field_get_next(mf);
		cl_msg_swap_field_to_be(mf);
		mf = mf_tmp;
	}

	mf->type = CL_MSG_FIELD_TYPE_DIGEST_RIPE_ARRAY;
	int digest_sz = sizeof(cf_digest) * n_my_digests;
	mf->field_sz = digest_sz + 1;
	uint8_t *b = mf->data;
	for (int i=0;i<n_digests;i++) {
		if (nodes[i] == my_node) { 
			memcpy(b, &digests[i], sizeof(cf_digest));
			b += sizeof(cf_digest);
		}
	}
		
	mf_tmp = cl_msg_field_get_next(mf);
	cl_msg_swap_field_to_be(mf);
	mf = mf_tmp;

	return ( (uint8_t *) mf_tmp );
}


static int
batch_compile(uint info1, uint info2, char *ns, cf_digest *digests, as_node **nodes, int n_digests, as_node *my_node, int n_my_digests, cl_bin *values, cl_operator operator, cl_operation *operations, int n_values,  
	uint8_t **buf_r, size_t *buf_sz_r, const cl_write_parameters *cl_w_p)
{
	// I hate strlen
	int		ns_len = ns ? (int)strlen(ns) : 0;
	int		i;
	
	// determine the size
	size_t	msg_sz = sizeof(as_msg); // header
	// fields
	if (ns) msg_sz += ns_len + sizeof(cl_msg_field);
	msg_sz += sizeof(cl_msg_field) + 1 + (sizeof(cf_digest) * n_my_digests);
	// ops
	for (i=0;i<n_values;i++) {
		msg_sz += sizeof(cl_msg_op) + strlen(values[i].bin_name);

        if (0 != cl_value_to_op_get_size(&values[i], &msg_sz)) {
            cf_error("illegal parameter: bad type %d write op %d", values[i].object.type, i);
            return(-1);
        }
	}
	
	// size too small? malloc!
	uint8_t	*buf;
	uint8_t *mbuf = 0;
	if ((*buf_r) && (msg_sz > *buf_sz_r)) {
		mbuf = buf = malloc(msg_sz);
		if (!buf) 			return(-1);
		*buf_r = buf;
	}
	else
		buf = *buf_r;
	
	*buf_sz_r = msg_sz;
	
	// debug - shouldn't be required
	memset(buf, 0, msg_sz);
	
	// lay in some parameters
	uint info3 = 0;
	uint32_t generation = 0;
	if (cl_w_p) {
		if (cl_w_p->unique) {
			info2 |= CL_MSG_INFO2_CREATE_ONLY;
		}
		else if (cl_w_p->unique_bin) {
			info2 |= CL_MSG_INFO2_BIN_CREATE_ONLY;
		}
		else if (cl_w_p->update_only) {
			info3 |= CL_MSG_INFO3_UPDATE_ONLY;
		}
		else if (cl_w_p->create_or_replace) {
			info3 |= CL_MSG_INFO3_CREATE_OR_REPLACE;
		}
		else if (cl_w_p->replace_only) {
			info3 |= CL_MSG_INFO3_REPLACE_ONLY;
		}
		else if (cl_w_p->bin_replace_only) {
			info3 |= CL_MSG_INFO3_BIN_REPLACE_ONLY;
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
	
	uint32_t record_ttl = cl_w_p ? cl_w_p->record_ttl : 0;
	uint32_t transaction_ttl = cl_w_p ? cl_w_p->timeout_ms : 0;
	
	// lay out the header - currently always 2, the digest array and the ns
	int n_fields = 2;
	buf = cl_write_header(buf, msg_sz, info1, info2, info3, generation, record_ttl, transaction_ttl, n_fields, n_values);
		
	// now the fields
	buf = write_fields_batch_digests(buf, ns, ns_len, digests, nodes, n_digests,n_my_digests, my_node);
	if (!buf) {
		if (mbuf)	free(mbuf);
		return(-1);
	}

	// lay out the ops
	if (n_values) {

		cl_msg_op *op = (cl_msg_op *) buf;
		cl_msg_op *op_tmp;
		for (i = 0; i< n_values;i++) {
			if( values ){
				cl_value_to_op( &values[i], operator, 0, op);
			}else{
				cl_value_to_op(0,0,&operations[i],op);
			}
	
			op_tmp = cl_msg_op_get_next(op);
			cl_msg_swap_op_to_be(op);
			op = op_tmp;
		}
	}
	return(0);	
}





#define STACK_BUF_SZ (1024 * 16) // provide a safe number for your system - linux tends to have 8M stacks these days
#define STACK_BINS 100

//
// do_batch_monte(as_cluster *asc, int info1, int info2, const char *ns, const cf_digest *digests, const cf_node *nodes, int n_digests,
//					cf_node node, citrusleaf_get_many_cb cb, void *udata)
//
// asc - cluster to send to 
// info1 - INFO1 options
// info2 - INFO2 options
// ns - namespace for all the digests
// digests - array of digests to fetch
// nodes - array of nodes for those digests
// n_digests - size of the two preceeding arrays
// node - node of this particular request (thus will send a subset of digests)
// cb - callback that gets called back MULTITHREADED when data arrives
// udata - user data for the callback
//

static int
do_batch_monte(as_cluster *asc, int info1, int info2, char *ns, cf_digest *digests, as_node **nodes, 
	int n_digests, cl_bin *bins, cl_operator operator, cl_operation *operations, int n_ops,
	as_node *node, int n_node_digests, citrusleaf_get_many_cb cb, void *udata)
{
	int rv = -1;

	uint8_t		rd_stack_buf[STACK_BUF_SZ];	
	uint8_t		*rd_buf = 0;
	size_t		rd_buf_sz = 0;
	uint8_t		wr_stack_buf[STACK_BUF_SZ];
	uint8_t		*wr_buf = wr_stack_buf;
	size_t		wr_buf_sz = sizeof(wr_stack_buf);

	// we have a list of many keys
//	if (0 == bins && CL_MSG_INFO1_READ == info1) info1 |= CL_MSG_INFO1_GET_ALL;
	rv = batch_compile(info1, info2, ns, digests, nodes, n_digests, node, n_node_digests, bins, operator, operations, n_ops, 
		&wr_buf, &wr_buf_sz, 0);
	if (rv != 0) {
		cf_error("do batch monte: batch compile failed: some kind of intermediate error");
		return (rv);
	}
	
#ifdef DEBUG_VERBOSE
	dump_buf("sending request to cluster:", wr_buf, wr_buf_sz);
#endif	

	int fd;
	rv = as_node_get_connection(node, &fd);
	if (rv) {
		return rv;
	}
	
	// send it to the cluster - non blocking socket, but we're blocking
	if ((rv = cf_socket_write_forever(fd, wr_buf, wr_buf_sz))) {
#ifdef DEBUG			
		cf_debug("Citrusleaf: write timeout or error when writing header to server - %d fd %d errno %d", rv, fd, errno);
#endif
		cf_close(fd);
		return(-1);
	}

	cl_proto 		proto;
	bool done = false;
	
	do { // multiple CL proto per response
		
		// Now turn around and read a fine cl_pro - that's the first 8 bytes that has types and lenghts
		if ((rv = cf_socket_read_forever(fd, (uint8_t *) &proto, sizeof(cl_proto) ) ) ) {
			cf_error("network error: errno %d fd %d", rv, fd);
			cf_close(fd);
			return(-1);
		}
#ifdef DEBUG_VERBOSE
		dump_buf("read proto header from cluster", (uint8_t *) &proto, sizeof(cl_proto));
#endif	
		cl_proto_swap_from_be(&proto);

		if (proto.version != CL_PROTO_VERSION) {
			cf_error("network error: received protocol message of wrong version %d", proto.version);
			cf_close(fd);
			return(-1);
		}
		if ((proto.type != CL_PROTO_TYPE_CL_MSG) && (proto.type != CL_PROTO_TYPE_CL_MSG_COMPRESSED)) {
			cf_error("network error: received incorrect message version %d", proto.type);
			cf_close(fd);
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
			if (rd_buf == NULL) {
				cf_close(fd);
				return (-1);
			}

			if ((rv = cf_socket_read_forever(fd, rd_buf, rd_buf_sz))) {
				cf_error("network error: errno %d fd %d", rv, fd);
				if (rd_buf != rd_stack_buf)	{ free(rd_buf); }
				cf_close(fd);
				return(-1);
			}
// this one's a little much: printing the entire body before printing the other bits			
#ifdef DEBUG_VERBOSE
			dump_buf("read msg body header (multiple msgs)", rd_buf, rd_buf_sz);
#endif	
		}
		
		if (proto.type == CL_PROTO_TYPE_CL_MSG_COMPRESSED) {
			
			uint8_t *new_rd_buf   = NULL;
			size_t  new_rd_buf_sz = 0;
			
			rv = batch_decompress(rd_buf, rd_buf_sz, &new_rd_buf, &new_rd_buf_sz);
			if (rv != 0) {
				cf_error("could not decompress compressed message: error %d", rv);
				if (rd_buf != rd_stack_buf)	{ free(rd_buf); }
				cf_close(fd);
				return -1;
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
		cl_bin *bins_local;
		
		while (pos < rd_buf_sz) {

#ifdef DEBUG_VERBOSE
			dump_buf("individual message header", buf, sizeof(cl_msg));
#endif	
			
			uint8_t *buf_start = buf;
			cl_msg *msg = (cl_msg *) buf;
			cl_msg_swap_header_from_be(msg);
			buf += sizeof(cl_msg);
			
			if (msg->header_sz != sizeof(cl_msg)) {
				cf_error("received cl msg of unexpected size: expecting %zd found %d, internal error",
					sizeof(cl_msg),msg->header_sz);
				cf_close(fd);
				return(-1);
			}

			// parse through the fields
			cf_digest *keyd = 0;
			char ns_ret[33] = {0};
			char *set_ret = NULL;
			cl_msg_field *mf = (cl_msg_field *)buf;
			for (int i=0;i<msg->n_fields;i++) {
				cl_msg_swap_field_from_be(mf);
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
			cf_debug("message header fields: nfields %u nops %u",msg->n_fields,msg->n_ops);
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
				cf_close(fd);
				return (-1);
			}

			// parse through the bins/ops
			cl_msg_op *op = (cl_msg_op *)buf;
			for (int i=0;i<msg->n_ops;i++) {

				cl_msg_swap_op_from_be(op);

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
			
			// Keep processing batch on OK and NOTFOUND return codes.
			// All other return codes indicate a error has occurred and the batch was aborted.
			if (msg->result_code != CL_RESULT_OK && msg->result_code != CL_RESULT_NOTFOUND) {
				rv = (int)msg->result_code;
				done = true;
			}

			if (msg->info3 & CL_MSG_INFO3_LAST)	{
#ifdef DEBUG				
				cf_debug("received final message");
#endif				
				done = true;
			}

			if (cb && ! done) {
				(*cb)(ns_ret, keyd, set_ret, NULL, msg->result_code, msg->generation,
						cf_server_void_time_to_ttl(msg->record_ttl),
						msg->n_ops != 0 ? bins_local : NULL, msg->n_ops, udata);
				rv = 0;
			}

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

	// We should close the connection fd in case of error
	// to throw away any unread data on connection socket.
	// Instead if we put back fd into pull the subsequent
	// call will read stale data.

	if (rv == 0) {
		as_node_put_connection(node, fd);
	} else {
		cf_close(fd);
	}

#ifdef DEBUG_VERBOSE	
	cf_debug("exited loop: rv %d", rv );
#endif	
	
	return(rv);
}


//
// These externally visible functions are exposed through citrusleaf.h
//

typedef struct {
	
	// these sections are the same for the same query
	as_cluster 	*asc;
    int          info1;
	int          info2;
	char 		*ns;
	cf_digest 	*digests; 
	as_node **nodes;
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
	as_node *my_node;				
	int				my_node_digest_count;
	
	int 			index; // debug only
	
} digest_work;

typedef struct {
	int result;
	as_node* my_node;
} work_complete;

static void *
batch_worker_fn(void* pv_asc)
{
	as_cluster* asc = (as_cluster*)pv_asc;

	while (true) {
		digest_work work;

		if (0 != cf_queue_pop(asc->batch_q, &work, CF_QUEUE_FOREVER)) {
			cf_error("queue pop failed");
		}

		// This is how batch shutdown signals we're done.
		if (! work.asc) {
			break;
		}

		work_complete wc;

		wc.my_node = work.my_node;
		wc.result = do_batch_monte( work.asc, work.info1, work.info2, work.ns,
				work.digests, work.nodes, work.n_digests, work.bins,
				work.operator, work.operations, work.n_ops, work.my_node,
				work.my_node_digest_count, work.cb, work.udata );

		cf_queue_push(work.complete_q, (void *) &wc);
	}

	return NULL;
}


#define MAX_NODES 128


cl_rv
citrusleaf_batch_read(as_cluster *asc, char *ns, const cf_digest *digests, int n_digests,
		cl_bin *bins, int n_bins, bool get_bin_data, citrusleaf_get_many_cb cb, void *udata)
{
	// fast path: if there's only one node, or the number of digests is super short, just dispatch to the server directly

	// TODO FAST PATH

	//
	// allocate the digest-node array, and populate it
	// 
	as_node **nodes = malloc( sizeof(as_node *)  * n_digests);
	if (!nodes) {
		cf_error("allocation failed");
		return(-1);
	}
	
	// loop through all digests and determine a node
	as_partition_table* table = as_cluster_get_partition_table(asc, ns);
	
	for (int i = 0; i < n_digests; i++) {
		// Must use write mode to get master paritition since batch doesn't proxy.
		nodes[i] = as_partition_table_get_node(asc, table, &digests[i], true);
		
		if (nodes[i] == 0) {
			cf_error("index %d: can't get any node", i);
			
			// Release previous nodes just reserved.
			for (int j = 0; j < i; j++) {
				as_node_release(nodes[j]);
			}
			free(nodes);
			return(-1);
		}
	}

	// find unique set
	as_node *unique_nodes[MAX_NODES];
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
	work.get_key = false; // we don't use this
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
		
		// dispatch - copies data
		cf_queue_push(asc->batch_q, &work);
	}
	
	// wait for the work to complete
	int retval = 0;
	for (int i=0;i<n_nodes;i++) {
		work_complete wc;
		cf_queue_pop(work.complete_q, &wc, CF_QUEUE_FOREVER);
		if (wc.result != 0) {
			cf_error("Node %d retcode error: %d", i, wc.result);
			// Find all the records we were looking for on this node.
			for (int j = 0; j < n_digests; j++) {
				if (nodes[j] == wc.my_node) {
					cf_error("   rec %d", j);
					cb(ns, &work.digests[j], NULL, NULL, wc.result, 0, 0, NULL, 0, udata);
				}
			}
			retval = wc.result;
		}
	}
	
	// free and return what needs freeing and putting
	cf_queue_destroy(work.complete_q);
	for (int i=0;i<n_digests;i++) {
		as_node_release(nodes[i]);
	}
	free(nodes);
	return retval;
}


void
cl_cluster_batch_init(as_cluster* asc)
{
	// We do this lazily, during the first batch request, so make sure it's only
	// done once.

	// Quicker than pulling a lock, handles everything except first race:
	if (ck_pr_load_32(&asc->batch_initialized) == 1) {
		return;
	}

	// Handle first race - losers must wait for winner to create dispatch queue.
	pthread_mutex_lock(&asc->batch_init_lock);

	if (ck_pr_load_32(&asc->batch_initialized) == 1) {
		// Lost race - another thread got here first.
		pthread_mutex_unlock(&asc->batch_init_lock);
		return;
	}

	// Create dispatch queue.
	asc->batch_q = cf_queue_create(sizeof(digest_work), true);

	// It's now safe to push to the queue.
	ck_pr_store_32(&asc->batch_initialized, 1);

	pthread_mutex_unlock(&asc->batch_init_lock);

	// Create thread pool.
	for (int i = 0; i < AS_NUM_BATCH_THREADS; i++) {
		pthread_create(&asc->batch_threads[i], 0, batch_worker_fn, (void*)asc);
	}
}


void
cl_cluster_batch_shutdown(as_cluster* asc)
{
	// Note - we assume this doesn't race cl_cluster_batch_init(), i.e. that no
	// threads are initiating batch transactions while we're shutting down the
	// cluster.

	// Check whether we ever (lazily) initialized batch machinery.
	if (ck_pr_load_32(&asc->batch_initialized) == 0) {
		return;
	}

	// This tells the worker threads to stop. We do this (instead of using a
	// "running" flag) to allow the workers to "wait forever" on processing the
	// work dispatch queue, which has minimum impact when the queue is empty.
	// This also means all queued requests get processed when shutting down.
	for (int i = 0; i < AS_NUM_BATCH_THREADS; i++) {
		digest_work work;
		work.asc = NULL;
		cf_queue_push(asc->batch_q, &work);
	}

	for (int i = 0; i < AS_NUM_BATCH_THREADS; i++) {
		pthread_join(asc->batch_threads[i], NULL);
	}

	cf_queue_destroy(asc->batch_q);
	asc->batch_q = NULL;
	ck_pr_store_32(&asc->batch_initialized, 0);
}
