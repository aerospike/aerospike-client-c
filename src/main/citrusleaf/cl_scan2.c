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
#include <assert.h>

#include <citrusleaf/cf_byte_order.h>
#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_queue.h>
#include <citrusleaf/cf_socket.h>
#include <citrusleaf/cf_vector.h>
#include <citrusleaf/cf_random.h>
#include <citrusleaf/cf_proto.h>

#include <aerospike/as_aerospike.h>
#include <aerospike/as_list.h>
#include <aerospike/as_module.h>
#include <aerospike/as_msgpack.h>
#include <aerospike/as_serializer.h>
#include <aerospike/as_string.h>

#include <aerospike/mod_lua.h>
#include <aerospike/mod_lua_config.h>

#include <citrusleaf/citrusleaf.h>
#include <aerospike/as_cluster.h>
#include <citrusleaf/as_scan.h>
#include <citrusleaf/cl_udf.h>

#include "internal.h"

extern as_val * citrusleaf_udf_bin_to_val(as_serializer *ser, cl_bin *);

/******************************************************************************
 * MACROS
 *****************************************************************************/

/*
 * Provide a safe number for your system linux tends to have 8M 
 * stacks these days
 */ 
#define STACK_BUF_SZ        (1024 * 16) 
#define STACK_BINS           100

static void __log(const char * file, const int line, const char * fmt, ...) {
    char msg[256] = {0};
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, 256, fmt, ap);
    va_end(ap);
    printf("[%s:%d] %s\n",file,line,msg);
}

#define LOG(__fmt, args...) \
    __log(__FILE__,__LINE__,__fmt, ## args)


/******************************************************************************
 * TYPES
 *****************************************************************************/

/*
 * Work item which gets queued up to each node
 */
typedef struct {
    as_cluster *            asc; 
    const char *            ns;
    char                    node_name[NODE_NAME_SIZE];    
    const uint8_t *         scan_buf;
    size_t                  scan_sz;
    void *                  udata;
    int                     (* callback)(as_val *, void *);
	uint64_t 				job_id;
	udf_execution_type		type;
	cf_queue              * complete_q;
} cl_scan_task;


/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static int scan_compile(const cl_scan * scan, uint8_t **buf_r, size_t *buf_sz_r);

static cl_rv cl_scan_udf_destroy(cl_scan_udf * udf);

cf_vector * cl_scan_execute(as_cluster * cluster, const cl_scan * scan, char *node_name, cl_rv * res, int (* callback)(as_val *, void *), void *udata);

// Creates a message, internally calling cl_compile to pass to the server
static int scan_compile(const cl_scan * scan, uint8_t ** buf_r, size_t * buf_sz_r) {

    if (!scan) return CITRUSLEAF_FAIL_CLIENT;

    //  Prepare udf call to send to the server
    as_call call;
    as_serializer ser;
    as_buffer argbuffer;
    as_buffer_init(&argbuffer);

    if ( scan->udf.type != CL_SCAN_UDF_NONE) {
        as_string file;
        as_string_init(&file, (char *) scan->udf.filename, true /*ismalloc*/);

        as_string func;
        as_string_init(&func, (char *) scan->udf.function, true /*ismalloc*/);

        if (scan->udf.arglist != NULL) {
            /**
             * If the query has a udf w/ arglist,
             * then serialize it.
             */
            as_msgpack_init(&ser);
            as_serializer_serialize(&ser, (as_val *) scan->udf.arglist, &argbuffer);
        }
        call.file = &file;
        call.func = &func;
        call.args = &argbuffer;
    }

    // Prepare to send scan parameters
    cl_scan_param_field     scan_param_field;
    cl_scan_params params = scan->params;
    scan_param_field.scan_pct = params.pct > 100 ? 100 : params.pct;
    scan_param_field.byte1 = (params.priority << 4)  | (params.fail_on_cluster_change << 3);

    // Prepare the msg type to be sent
    uint info;
    info = CL_MSG_INFO1_READ;

    // Pass on to the cl_compile to create the msg
    cl_compile(info /*info1*/, 0, 0, scan->ns /*namespace*/, scan->setname /*setname*/, 0 /*key*/, 0/*digest*/, NULL /*bins*/, 0/*op*/, 0/*operations*/, 0/*n_values*/, buf_r, buf_sz_r, 0 /*w_p*/, NULL /*d_ret*/, scan->job_id, &scan_param_field, scan->udf.type != CL_SCAN_UDF_NONE ? &call : NULL/*udf call*/, scan->udf.type);

    if (scan->udf.arglist) {
        as_serializer_destroy(&ser);
    }
    as_buffer_destroy(&argbuffer);
    return CITRUSLEAF_OK;
}

/**
 * Get a value for a bin of with the given key.
 */
static as_val * scan_response_get(const as_rec * rec, const char * name)  {
    as_val * v = NULL;
    as_serializer ser;
    as_msgpack_init(&ser);
    cl_scan_response_rec * r = (cl_scan_response_rec *) rec;
    for (int i = 0; i < r->n_bins; i++) {
        if (!strcmp(r->bins[i].bin_name, name)) {
            v = citrusleaf_udf_bin_to_val(&ser, &r->bins[i]);
            break;
        }
    }
    as_serializer_destroy(&ser);
    return v;
}

static uint32_t scan_response_ttl(const as_rec * rec) {
    cl_scan_response_rec * r = (cl_scan_response_rec *) rec;
    return r->record_ttl;
}

static uint16_t scan_response_gen(const as_rec * rec) {
    cl_scan_response_rec * r = (cl_scan_response_rec *) rec;
    if (!r) return 0;
    return r->generation;
}

bool scan_response_destroy(as_rec *rec) {
    cl_scan_response_rec * r = (cl_scan_response_rec *) rec;
    if (!r) return false;
    citrusleaf_bins_free(r->bins, r->n_bins);
    //    if (r->bins) free(r->bins);
    if (r->ns)   free(r->ns);
    if (r->set)  free(r->set);
    if (r->ismalloc) free(r);
    rec->data = NULL;
    return true;
}

const as_rec_hooks scan_response_hooks = {
    .get        = scan_response_get,
    .set        = NULL,
    .remove     = NULL,
    .ttl        = scan_response_ttl,
    .gen        = scan_response_gen,
    .destroy    = scan_response_destroy
};

/* 
 * this is an actual instance of the scan, running on a scan thread
 * It reads on the node fd till it finds the last msg, in the meantime calling
 * task->callback on the returned data. The returned data is a bin of name SUCCESS/FAILURE
 * and the value of the bin is the return value from the udf.
 */
static int cl_scan_worker_do(as_node * node, cl_scan_task * task) {

    uint8_t     rd_stack_buf[STACK_BUF_SZ] = {0};    
    uint8_t *   rd_buf = rd_stack_buf;
    size_t      rd_buf_sz = 0;
	
	int fd;
    int rc = as_node_get_connection(node, &fd);
    if (rc) {
        LOG("[ERROR] cl_scan_worker_do: cannot get fd for node %s ",node->name);
        return rc;
    }

    // send it to the cluster - non blocking socket, but we're blocking
    if (0 != cf_socket_write_forever(fd, (uint8_t *) task->scan_buf, (size_t) task->scan_sz)) {
    	cf_close(fd);
        return CITRUSLEAF_FAIL_CLIENT;
    }

    cl_proto  proto;
    bool      done = false;

    do {
        // multiple CL proto per response
        // Now turn around and read a fine cl_proto - that's the first 8 bytes 
        // that has types and lengths
        if ( (rc = cf_socket_read_forever(fd, (uint8_t *) &proto, sizeof(cl_proto) ) ) ) {
            LOG("[ERROR] cl_scan_worker_do: network error: errno %d fd %d node name %s\n", rc, fd, node->name);
            cf_close(fd);
            return CITRUSLEAF_FAIL_CLIENT;
        }
        cl_proto_swap_from_be(&proto);

        if ( proto.version != CL_PROTO_VERSION) {
            LOG("[ERROR] cl_scan_worker_do: network error: received protocol message of wrong version %d from node %s\n", proto.version, node->name);
            cf_close(fd);
            return CITRUSLEAF_FAIL_CLIENT;
        }

        if ( proto.type != CL_PROTO_TYPE_CL_MSG && proto.type != CL_PROTO_TYPE_CL_MSG_COMPRESSED ) {
            LOG("[ERROR] cl_scan_worker_do: network error: received incorrect message version %d from node %s \n",proto.type, node->name);
            cf_close(fd);
            return CITRUSLEAF_FAIL_CLIENT;
        }

        // second read for the remainder of the message - expect this to cover 
        // lots of data, many lines if there's no error
        rd_buf_sz =  proto.sz;
        if (rd_buf_sz > 0) {

            if (rd_buf_sz > sizeof(rd_stack_buf)){
                rd_buf = malloc(rd_buf_sz);
            }
            else {
                rd_buf = rd_stack_buf;
            }

            if (rd_buf == NULL) {
            	cf_close(fd);
            	return CITRUSLEAF_FAIL_CLIENT;
            }

            if ( (rc = cf_socket_read_forever(fd, rd_buf, rd_buf_sz)) ) {
                LOG("[ERROR] cl_scan_worker_do: network error: errno %d fd %d node name %s\n", rc, fd, node->name);
                if ( rd_buf != rd_stack_buf ) free(rd_buf);
                cf_close(fd);
                return CITRUSLEAF_FAIL_CLIENT;
            }
        }

        // process all the cl_msg in this proto
        uint8_t *   buf = rd_buf;
        uint        pos = 0;
        cl_bin      stack_bins[STACK_BINS];
        cl_bin *    bins;

        while (pos < rd_buf_sz) {

            uint8_t *   buf_start = buf;
            cl_msg *    msg = (cl_msg *) buf;

            cl_msg_swap_header_from_be(msg);
            buf += sizeof(cl_msg);

            if ( msg->header_sz != sizeof(cl_msg) ) {
                LOG("[ERROR] cl_scan_worker_do: received cl msg of unexpected size: expecting %zd found %d, internal error\n",
                        sizeof(cl_msg),msg->header_sz);
                cf_close(fd);
                return CITRUSLEAF_FAIL_CLIENT;
            }

            // parse through the fields
            cf_digest       keyd;
            char            ns_ret[33]  = {0};
            char *          set_ret     = NULL;
            cl_msg_field *  mf          = (cl_msg_field *)buf;

            for (int i=0; i < msg->n_fields; i++) {
                cl_msg_swap_field_from_be(mf);
                if (mf->type == CL_MSG_FIELD_TYPE_KEY) {
                    LOG("[ERROR] cl_scan_worker_do: read: found a key - unexpected\n");
                }
                else if (mf->type == CL_MSG_FIELD_TYPE_DIGEST_RIPE) {
                    memcpy(&keyd, mf->data, sizeof(cf_digest));
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
                cf_close(fd);
               return CITRUSLEAF_FAIL_CLIENT;
            }

            // parse through the bins/ops
            cl_msg_op * op = (cl_msg_op *) buf;
            for (int i=0;i<msg->n_ops;i++) {
                cl_msg_swap_op_from_be(op);

#ifdef DEBUG_VERBOSE
                LOG("[DEBUG] cl_scan_worker_do: op receive: %p size %d op %d ptype %d pversion %d namesz %d \n",
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

                rc = (int) msg->result_code;
                done = true;
                if (rc == CITRUSLEAF_FAIL_SCAN_ABORT) {
                    LOG("[INFO] cl_scan_worker_do: Scan successfully aborted at node [%s]\n", node->name);
                }
            }
            else if (msg->info3 & CL_MSG_INFO3_LAST)    {
#ifdef DEBUG_VERBOSE
               if ( cf_debug_enabled() ) {
                    LOG("[INFO] cl_scan_worker_do: Received final message from node [%s], scan complete\n", node->name);
                }
#endif
                done = true;
            }
            else if ((msg->n_ops || (msg->info1 & CL_MSG_INFO1_NOBINDATA))) {

                cl_scan_response_rec rec;
                cl_scan_response_rec *recp = &rec;

                recp->ns         = strdup(ns_ret);
                recp->keyd       = keyd;
                recp->set        = set_ret;
                recp->generation = msg->generation;
                recp->record_ttl = msg->record_ttl;
                recp->bins       = bins;
                recp->n_bins     = msg->n_ops;
                recp->ismalloc   = false;

                as_rec r;
                as_rec *rp = &r;
                rp = as_rec_init(rp, recp, &scan_response_hooks);

                as_val * v = as_rec_get(rp, "SUCCESS");
                if ( v  != NULL && task->callback) {
                    // Got a non null value for the resposne bin,
                    // call callback on it and destroy the record
                    task->callback(v, task->udata);

                    as_rec_destroy(rp);

                }

                rc = CITRUSLEAF_OK;
            }

            // if done free it 
            if (done) {
                citrusleaf_bins_free(bins, msg->n_ops);
                if (bins != stack_bins) {
                    free(bins);
                    bins = 0;
                }

                if (set_ret) {
                    free(set_ret);
                    set_ret = NULL;
                }
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
    as_node_put_connection(node, fd);

#ifdef DEBUG_VERBOSE    
    LOG("[DEBUG] cl_scan_worker_do: exited loop: rc %d\n", rc );
#endif    

    return rc;
}

void * cl_scan_worker(void * pv_asc) {
	as_cluster* asc = (as_cluster*)pv_asc;

	while (true) {
        // Response structure to be pushed in the complete q
        cl_node_response response; 
        memset(&response, 0, sizeof(cl_node_response));

        cl_scan_task task;

        if ( 0 != cf_queue_pop(asc->scan_q, &task, CF_QUEUE_FOREVER) ) {
            LOG("[WARNING] cl_scan_worker: queue pop failed\n");
        }

#ifdef DEBUG_VERBOSE
       if ( cf_debug_enabled() ) {
            LOG("[DEBUG] cl_scan_worker: getting one task item\n");
        }
#endif

        // This is how scan shutdown signals we're done.
        if ( ! task.asc ) {
            break;
        }

        // query if the node is still around
        int rc = CITRUSLEAF_FAIL_UNAVAILABLE;

        as_node * node = as_node_get_by_name(task.asc, task.node_name);
        if ( node ) {
            rc = cl_scan_worker_do(node, &task);
			as_node_release(node);
        }
        else {
            LOG("[INFO] cl_scan_worker: No node found with the name %s\n", task.node_name);
        }
        strncpy(response.node_name, task.node_name, strlen(task.node_name));
        response.node_response = rc;
        response.job_id = task.job_id;
        cf_queue_push(task.complete_q, (void *)&response);
    }

	return NULL;
}

cl_rv cl_scan_params_init(cl_scan_params * oparams, cl_scan_params *iparams) {

    // If there is an input structure use the values from that else use the default ones
    oparams->fail_on_cluster_change = iparams ? iparams->fail_on_cluster_change : false;
    oparams->priority = iparams ? iparams->priority : CL_SCAN_PRIORITY_AUTO;
    //    oparams->threads_per_node = iparams ? iparams->threads_per_node : 1;
    oparams->pct = iparams ? iparams->pct : 100;
    return CITRUSLEAF_OK;
}

cl_rv cl_scan_udf_init(cl_scan_udf * udf, udf_execution_type type, const char * filename, const char * function, as_list * arglist) {
    udf->type        = type;
    udf->filename    = filename == NULL ? NULL : strdup(filename);
    udf->function    = function == NULL ? NULL : strdup(function);
    udf->arglist     = arglist;
    return CITRUSLEAF_OK;
}

cl_rv cl_scan_udf_destroy(cl_scan_udf * udf) {

    udf->type = CL_SCAN_UDF_NONE;

    if ( udf->filename ) {
        free(udf->filename);
        udf->filename = NULL;
    }

    if ( udf->function ) {
        free(udf->function);
        udf->function = NULL;
    }

    if ( udf->arglist ) {
        as_list_destroy(udf->arglist);
        udf->arglist = NULL;
    }

    return CITRUSLEAF_OK;
}

/*
 * Calls a scan on all the nodes in the cluster. This function initializes a background scan.
 * The udf return values are not returned back to the client. 
 */
cf_vector * citrusleaf_udf_scan_background(as_cluster * asc, cl_scan * scan) {
    scan->udf.type = CL_SCAN_UDF_BACKGROUND;
    cl_rv res = CITRUSLEAF_OK;

    // Call cl_scan_execute with a NULL node_name.
	return cl_scan_execute(asc, scan, NULL, &res, NULL, NULL);
}

/*
 * Calls a scan on a specified node in the cluster. This function initializes a background scan.
 * The udf return values are not returned back to the client. 
 */
cl_rv citrusleaf_udf_scan_node_background(as_cluster * asc, cl_scan * scan, char *node_name) {
    scan->udf.type = CL_SCAN_UDF_BACKGROUND;
	cl_node_response resp;
    cl_rv rv = CITRUSLEAF_OK;

    // Call cl_scan_execute with a NULL node_name.
    cf_vector * v = cl_scan_execute(asc, scan, node_name, &rv, NULL, NULL);

    if (v) {
        cf_vector_get(v, 0, &resp);
		rv = resp.node_response;
		cf_vector_destroy(v);
    }
	return rv;
}

/*
 *  Calls a scan on a particular node in the cluster with the given parameters and then applies
 *  the udf on the results. It returns values from the udf. The callback is then applied on those values at the client.
 */
cl_rv citrusleaf_udf_scan_node(as_cluster *asc, cl_scan *scan, char *node_name, int( *callback)(as_val *, void *), void * udata) {
    scan->udf.type = CL_SCAN_UDF_CLIENT_RECORD;
	cl_node_response resp;
    cl_rv rv = CITRUSLEAF_FAIL_CLIENT;

    // If cl_scan_execute returns a non null vector, return the value in the vector, else return a failure
    cf_vector *v = cl_scan_execute(asc, scan, node_name, &rv, callback, udata);
    if (v) {
        cf_vector_get(v, 0, &resp);
		rv = resp.node_response;
		cf_vector_destroy(v);
    }
    return rv;
}

/* Calls a scan of all the nodes in the cluster with the given parameters and then applies the udf on the results.
 * It returns values from the udf. The callback is then applied on those values at the client. 
 */
cf_vector * citrusleaf_udf_scan_all_nodes(as_cluster *asc, cl_scan * scan, int (*callback)(as_val*, void*), void * udata) {
    scan->udf.type = CL_SCAN_UDF_CLIENT_RECORD;
    cl_rv rc = CITRUSLEAF_OK;
    return cl_scan_execute(asc, scan, NULL, &rc, callback, udata);
}

cf_vector * cl_scan_execute(as_cluster * cluster, const cl_scan * scan, char * node_name, cl_rv * res, int (* callback)(as_val *, void *), void * udata) {

    cl_rv           rc                          = CITRUSLEAF_OK;
    uint8_t         wr_stack_buf[STACK_BUF_SZ]  = { 0 };
    uint8_t *       wr_buf                      = wr_stack_buf;
    size_t          wr_buf_sz                   = sizeof(wr_stack_buf);
    int             node_count                  = 0;
    cl_node_response  response;
    rc = scan_compile(scan, &wr_buf, &wr_buf_sz);

    if ( rc != CITRUSLEAF_OK ) {
        LOG("[ERROR] cl_scan_execute: scan compile failed: \n");
        *res = rc;
        return NULL;
    }

    // Setup worker
    cl_scan_task task = {
        .asc                = cluster,
        .ns                 = scan->ns,
        .scan_buf          = wr_buf,
        .scan_sz           = wr_buf_sz,
        .udata              = udata,
        .callback           = callback,
        .job_id                = scan->job_id,
        .type                = scan->udf.type,
    };

    task.complete_q      = cf_queue_create(sizeof(cl_node_response), true);
    cf_vector * result_v = NULL;

    // If node_name is not null, we are executing scan on a particular node
    if (node_name) {
        // Copy the node name in the task and push it in the global scan queue. One task for each node
        strcpy(task.node_name, node_name);
        cf_queue_push(cluster->scan_q, &task);
        node_count = 1;
    }
    else {
        // Node name is NULL, we have to scan all nodes 
        char *node_names    = NULL;    

        // Get a list of the node names, so we can can send work to each node
        as_cluster_get_node_names(cluster, &node_count, &node_names);
        if ( node_count == 0 ) {
            LOG("[ERROR] cl_scan_execute: don't have any nodes?\n");
            *res = CITRUSLEAF_FAIL_CLIENT;
            goto Cleanup;
        }

        // Dispatch work to the worker queue to allow the transactions in parallel
        // NOTE: if a new node is introduced in the middle, it is NOT taken care of
        node_name = node_names;
        for ( int i=0; i < node_count; i++ ) {
            // fill in per-request specifics
            strcpy(task.node_name, node_name);
            cf_queue_push(cluster->scan_q, &task);
            node_name += NODE_NAME_SIZE;                    
        }
        free(node_names);
        node_names = NULL;
    }

    // Wait for the work to complete from all the nodes.
    // For every node, fill in the return value in the result vector
    result_v = cf_vector_create(sizeof(cl_node_response), node_count, 0);
    for ( int i=0; i < node_count; i++ ) {
        // Pop the response structure
        cf_queue_pop(task.complete_q, &response, CF_QUEUE_FOREVER);
        cf_vector_append(result_v, &response);
    }

Cleanup:
    if ( wr_buf && (wr_buf != wr_stack_buf) ) { 
        free(wr_buf); 
        wr_buf = 0;
    }
    cf_queue_destroy(task.complete_q);

    return result_v;
}

/**
 * Allocates and initializes a new cl_scan.
 */
cl_scan * cl_scan_new(const char * ns, const char * setname, uint64_t *job_id) {
    cl_scan * scan = (cl_scan*) malloc(sizeof(cl_scan));
    memset(scan, 0, sizeof(cl_scan));
    return cl_scan_init(scan, ns, setname, job_id);
}

/**
 * Initializes an cl_scan
 */
cl_scan * cl_scan_init(cl_scan * scan, const char * ns, const char * setname, uint64_t *job_id) {
    if ( scan == NULL ) return scan;

    cf_queue * result_queue = cf_queue_create(sizeof(void *), true);
    if ( !result_queue ) {
        scan->res_streamq = NULL;
        return scan;
    }

    scan->res_streamq = result_queue;
    scan->job_id = (cf_get_rand64())/2;
    *job_id = scan->job_id;
    scan->setname = setname == NULL ? NULL : strdup(setname);
    scan->ns = ns == NULL ? NULL : strdup(ns);
    cl_scan_params_init(&scan->params, NULL);
    cl_scan_udf_init(&scan->udf, CL_SCAN_UDF_NONE, NULL, NULL, NULL);

    return scan;
}

void cl_scan_destroy(cl_scan *scan) {

    if ( scan == NULL ) return;

    cl_scan_udf_destroy(&scan->udf);
    if (scan->ns)      free(scan->ns);
    if (scan->setname) free(scan->setname);

    if ( scan->res_streamq ) {
        as_val *val = NULL;
        while (CF_QUEUE_OK == cf_queue_pop (scan->res_streamq, 
                    &val, CF_QUEUE_NOWAIT)) {
            as_val_destroy(val);
            val = NULL;
        }

        cf_queue_destroy(scan->res_streamq);
        scan->res_streamq = NULL;
    }

    free(scan);
    scan = NULL;
}

cl_rv cl_scan_foreach(cl_scan * scan, const char * filename, const char * function, as_list * arglist) {
    return cl_scan_udf_init(&scan->udf, CL_SCAN_UDF_CLIENT_RECORD, filename, function, arglist);
}

cl_rv cl_scan_limit(cl_scan *scan, uint64_t limit) {
    return CITRUSLEAF_OK;    
}

int
cl_cluster_scan_init(as_cluster* asc)
{
	// We do this lazily, during the first scan request, so make sure it's only
	// done once.
	if (ck_pr_fas_32(&asc->scan_initialized, 1) == 1 || asc->scan_q) {
		return 0;
	}

#ifdef DEBUG_VERBOSE
	if (cf_debug_enabled()) {
		LOG("[DEBUG] cl_cluster_scan_init: creating %d threads\n", AS_NUM_SCAN_THREADS);
	}
#endif
	
	// Create dispatch queue.
	asc->scan_q = cf_queue_create(sizeof(cl_scan_task), true);

	// Create thread pool.
	for (int i = 0; i < AS_NUM_SCAN_THREADS; i++) {
		pthread_create(&asc->scan_threads[i], 0, cl_scan_worker, (void*)asc);
	}

	return 0;
}

void
cl_cluster_scan_shutdown(as_cluster* asc)
{
	// Check whether we ever (lazily) initialized scan machinery.
	if (ck_pr_load_32(&asc->scan_initialized) == 0 && ! asc->scan_q) {
		return;
	}

	// This tells the worker threads to stop. We do this (instead of using a
	// "running" flag) to allow the workers to "wait forever" on processing the
	// work dispatch queue, which has minimum impact when the queue is empty.
	// This also means all queued requests get processed when shutting down.
	for (int i = 0; i < AS_NUM_SCAN_THREADS; i++) {
		cl_scan_task task;
		task.asc = NULL;
		cf_queue_push(asc->scan_q, &task);
	}

	for (int i = 0; i < AS_NUM_SCAN_THREADS; i++) {
		pthread_join(asc->scan_threads[i], NULL);
	}

	cf_queue_destroy(asc->scan_q);
	asc->scan_q = NULL;
	ck_pr_store_32(&asc->scan_initialized, 0);
}
