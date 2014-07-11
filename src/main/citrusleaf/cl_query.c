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

#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_byte_order.h>
#include <citrusleaf/cf_proto.h>
#include <citrusleaf/cf_queue.h>
#include <citrusleaf/cf_random.h>
#include <citrusleaf/cf_socket.h>
#include <citrusleaf/cf_vector.h>

#include <aerospike/as_aerospike.h>
#include <aerospike/as_module.h>
#include <aerospike/as_msgpack.h>
#include <aerospike/as_list.h>
#include <aerospike/as_record.h>
#include <aerospike/as_serializer.h>
#include <aerospike/as_string.h>
#include <aerospike/as_udf_context.h>
#include <aerospike/mod_lua.h>
#include <aerospike/mod_lua_config.h>

#include <citrusleaf/citrusleaf.h>
#include <aerospike/as_cluster.h>
#include <citrusleaf/cl_query.h>
#include <citrusleaf/cl_udf.h>

#include "../aerospike/_shim.h"
#include "internal.h"

/******************************************************************************
 * MACROS
 *****************************************************************************/

/*
 * Provide a safe number for your system linux tends to have 8M 
 * stacks these days
 */ 
#define STACK_BUF_SZ        (1024 * 16) 
#define STACK_BINS           100

#define LOG_ENABLED 0

#if LOG_ENABLED == 1

static void __log(const char * file, const int line, const char * fmt, ...) {
    char msg[256] = {0};
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, 256, fmt, ap);
    va_end(ap);
    printf("[%s:%d] %s\n",file,line,msg);
}

#define LOG(__fmt, args...) \
    // __log(__FILE__,__LINE__,__fmt, ## args)

#else
#define LOG(__fmt, args...)
#endif

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
    const uint8_t *         query_buf;
    size_t                  query_sz;
    void *                  udata;
    int                     (* callback)(as_val *, void *);
	cf_queue              * complete_q;
	bool                    abort;
    as_val                * err_val;
} cl_query_task;


/*
 * where indicates start/end condition for the columns of the indexes.
 * Example1: (index on "last_activity" bin) 
 *              WHERE last_activity > start_time AND last_activity < end_time
 * Example2: (index on "last_activity" bin for equality) 
 *              WHERE last_activity = start_time
 * Example3: (compound index on "last_activity","state","age") 
 *              WHERE last_activity > start_time AND last_activity < end_time
 *                    AND state IN ["ca","wa","or"]
 *                    AND age = 28
 */                    
typedef struct query_range {
    char       bin_name[CL_BINNAME_SIZE];
    bool       closedbound;
    bool       isfunction;
    cl_object  start_obj;
    cl_object  end_obj;
} query_range;

/*
 * Filter Indicate condition for the non-indexed columns.
 * Example3: (index on "last_activity","state","age") 
 *              WHERE last_activity > start_time AND last_activity < end_time
 *                    AND state IN ["ca","wa","or"]
 *                    AND age = 28
 */
typedef struct query_filter {
    char        bin_name[CL_BINNAME_SIZE];
    cl_object   compare_obj;
    cl_query_op ftype;
} query_filter;

typedef struct query_orderby_clause {
    char                bin_name[CL_BINNAME_SIZE];
    cl_query_orderby_op ordertype;
} query_orderby;

typedef struct as_query_fail_s { 
    int       rc;
    as_val  * err_val;
} as_query_fail_t;

/******************************************************************************
 * VARIABLES
 *****************************************************************************/

bool            gasq_abort         = false;

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static int query_compile_select(cf_vector *binnames, uint8_t *buf, int *sz_p);

static int query_compile_range(cf_vector *range_v, uint8_t *buf, int *sz_p);

#if 0
static int query_compile_filter(cf_vector *filter_v, uint8_t *buf, int *sz_p)  { return 0; }
static int query_compile_orderby(cf_vector *filter_v, uint8_t *buf, int *sz_p) { return 0; }
static int query_compile_function(cf_vector *range_v, uint8_t *buf, int *sz_p) { return 0; }
#endif

static int query_compile(const cl_query * query, uint8_t **buf_r, size_t *buf_sz_r);

// static int do_query_monte(as_node *node, const char *ns, const uint8_t *query_buf, size_t query_sz, cl_query_cb cb, void *udata, bool isnbconnect, as_stream *);

static cl_rv cl_query_udf_init(cl_query_udf * udf, cl_query_udf_type type, const char * filename, const char * function, as_list * arglist);

static cl_rv cl_query_udf_destroy(cl_query_udf * udf);

// static cl_rv cl_query_execute_sink(as_cluster * cluster, const cl_query * query, as_stream * stream);

static cl_rv cl_query_execute(as_cluster * cluster, const cl_query * query, void * udata, int (* callback)(as_val *, void *), as_val ** err_val);

static void cl_range_destroy(query_range *range) {
    citrusleaf_object_free(&range->start_obj);
    citrusleaf_object_free(&range->end_obj);
}

static void cl_filter_destroy(query_filter *filter) {
    citrusleaf_object_free(&filter->compare_obj);
}

/*
 * query range field layout: contains - numranges, binname, start, end
 * 
 * generic field header
 * 0   4 size = size of data only
 * 4   1 field_type = CL_MSG_FIELD_TYPE_INDEX_RANGE
 *
 * numranges
 * 5   1 numranges (max 255 ranges) 
 *
 * binname 
 * 6   1 binnamelen b
 * 7   b binname
 * 
 * particle (start & end)
 * +b    1 particle_type
 * +b+1  4 start_particle_size x
 * +b+5  x start_particle_data
 * +b+5+x      4 end_particle_size y
 * +b+5+x+y+4   y end_particle_data
 *
 * repeat "numranges" times from "binname"
 */
static int query_compile_range(cf_vector *range_v, uint8_t *buf, int *sz_p) {

    int sz = 0;

    // numranges
    sz += 1;
    if (buf) {
        *buf++ = cf_vector_size(range_v);
    }

    // iterate through each range    
    for (uint i=0; i<cf_vector_size(range_v); i++) {
        query_range *range = (query_range *)cf_vector_getp(range_v,i);

        // binname size
        int binnamesz = (int)strlen(range->bin_name);
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
            uint32_t ss = (uint32_t)psz;
            *((uint32_t *)buf) = cf_swap_to_be32(ss);
            buf += sizeof(uint32_t);
        } 

        // start particle data
        sz += psz;
        if (buf) {
            cl_object_to_buf(&range->start_obj,buf);
            buf += psz;
        }

        // end particle len
        // particle len will be in network order 
        sz += 4;
        psz = 0;
        cl_object_get_size(&range->end_obj,&psz);
        if (buf) {
            uint32_t ss = (uint32_t)psz;
            *((uint32_t *)buf) = cf_swap_to_be32(ss);
            buf += sizeof(uint32_t);
        } 

        // end particle data
        sz += psz;
        if (buf) {
            cl_object_to_buf(&range->end_obj,buf);
            buf += psz;
        }
    }        

    *sz_p = sz;

    // @chris:  mostly because I have no idea what it was supposed to return
    //          and i figured the compiler assumed it returns 
    return 0; 
}

/*
 * Wire Layout
 *
 * Generic field header
 * 0   4 size = size of data only
 * 4   1 field_type = CL_MSG_FIELD_TYPE_INDEX_RANGE
 *
 * numbins
 * 5   1 binnames (max 255 binnames) 
 *
 * binnames 
 * 6   1 binnamelen b
 * 7   b binname
 * 
 * numbins times
 */
static int query_compile_select(cf_vector *binnames, uint8_t *buf, int *sz_p) {
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
        int binnamesz = (int)strlen(binname);
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
    } 
    *sz_p = sz;

    return 0;
}

/*
 * If the query is null, then you run the MR job over the entire set or namespace
 * If the job is null, just run the query
 */
static int query_compile(const cl_query * query, uint8_t ** buf_r, size_t * buf_sz_r) {

    if (!query || !query->ranges) return CITRUSLEAF_FAIL_CLIENT;

    /**
     * If the query has a udf w/ arglist,
     * then serialize it.
     */
    as_buffer argbuffer;
    as_buffer_init(&argbuffer);

    if ( (query->udf.type != AS_UDF_CALLTYPE_NONE) && (query->udf.arglist != NULL) ) {
        as_serializer ser;
        as_msgpack_init(&ser);
        as_serializer_serialize(&ser, (as_val *) query->udf.arglist, &argbuffer);
        as_serializer_destroy(&ser);
    }

    // Calculating buffer size & n_fields
    int n_fields    = 0; 
    size_t msg_sz   = sizeof(as_msg);
    int ns_len      = 0;
    int setname_len = 0;
    int iname_len   = 0;
    int range_sz    = 0;
    int num_bins    = 0;

    if (query) {

        // namespace field 
        if ( !query->ns ) return CITRUSLEAF_FAIL_CLIENT;

        ns_len  = (int)strlen(query->ns);
        if (ns_len) {
            n_fields++;
            msg_sz += ns_len  + sizeof(cl_msg_field); 
        }

        // indexname field
        if ( query->indexname ) {
            iname_len = (int)strlen(query->indexname);
            if (iname_len) {
                n_fields++;
                msg_sz += strlen(query->indexname) + sizeof(cl_msg_field);
            }
        }

        if (query->setname) {
            setname_len = (int)strlen(query->setname);
            if (setname_len) {
                n_fields++;
                msg_sz += setname_len + sizeof(cl_msg_field);
            }
        }

        if (query->job_id) {
            n_fields++;
            msg_sz += sizeof(cl_msg_field) + sizeof(query->job_id);
        }

        // query field    
        n_fields++;
        range_sz = 0; 
        if (query_compile_range(query->ranges, NULL, &range_sz)) {
            return CITRUSLEAF_FAIL_CLIENT;
        }
        msg_sz += range_sz + sizeof(cl_msg_field);

        // bin field    
        if (query->binnames) {
            n_fields++;
            num_bins = 0;
            if ( query_compile_select(query->binnames, NULL, &num_bins) != 0 ) {
                return CITRUSLEAF_FAIL_CLIENT;
            }
            msg_sz += num_bins + sizeof(cl_msg_field);
        }

        // TODO filter field
        // TODO orderby field
        // TODO limit field
        if ( query->udf.type != AS_UDF_CALLTYPE_NONE ) {
            // as_call *udf = (as_call *)query->udf;
            msg_sz += sizeof(cl_msg_field) + strlen(query->udf.filename);
            msg_sz += sizeof(cl_msg_field) + strlen(query->udf.function);
            msg_sz += sizeof(cl_msg_field) + argbuffer.size;
            msg_sz += sizeof(cl_msg_field) + 1;
            n_fields += 4;
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
    int    info1      = CL_MSG_INFO1_READ;
    int info2      = 0;
    int info3      = 0;
    buf = cl_write_header(buf, msg_sz, info1, info2, info3, 0, 0, 0, 
            n_fields, 0);
    // now write the fields
    cl_msg_field *mf = (cl_msg_field *) buf;
    cl_msg_field *mf_tmp = mf;
    if (query->ns) {
        mf->type = CL_MSG_FIELD_TYPE_NAMESPACE;
        mf->field_sz = ns_len + 1;
        memcpy(mf->data, query->ns, ns_len);
        mf_tmp = cl_msg_field_get_next(mf);
        cl_msg_swap_field_to_be(mf);
        mf = mf_tmp;
    }

    if (iname_len) {
        mf->type = CL_MSG_FIELD_TYPE_INDEX_NAME;
        mf->field_sz = iname_len + 1;
        memcpy(mf->data, query->indexname, iname_len);
        mf_tmp = cl_msg_field_get_next(mf);
        cl_msg_swap_field_to_be(mf);
        mf = mf_tmp;
        if (cf_debug_enabled()) {
            LOG("[DEBUG] query_compile: adding indexname %d %s\n",iname_len+1, query->indexname);
        }
    }

    if (setname_len) {
        mf->type = CL_MSG_FIELD_TYPE_SET;
        mf->field_sz = setname_len + 1;
        memcpy(mf->data, query->setname, setname_len);
        mf_tmp = cl_msg_field_get_next(mf);
        cl_msg_swap_field_to_be(mf);
        mf = mf_tmp;
        if (cf_debug_enabled()) {
            LOG("[DEBUG] query_compile: adding setname %d %s\n",setname_len+1, query->setname);
        }
    }

    if (query->ranges) {
        mf->type = CL_MSG_FIELD_TYPE_INDEX_RANGE;
        mf->field_sz = range_sz + 1;
        query_compile_range(query->ranges, mf->data, &range_sz);
        mf_tmp = cl_msg_field_get_next(mf);
        cl_msg_swap_field_to_be(mf);
        mf = mf_tmp;
    }

    if (query->binnames) {
        mf->type = CL_MSG_FIELD_TYPE_QUERY_BINLIST;
        mf->field_sz = num_bins + 1;
        query_compile_select(query->binnames, mf->data, &num_bins);
        mf_tmp = cl_msg_field_get_next(mf);
        cl_msg_swap_field_to_be(mf);
        mf = mf_tmp;
    }

    if (query->job_id) {
        mf->type = CL_MSG_FIELD_TYPE_TRID;
        // Convert the transaction-id to network byte order (big-endian)
        uint64_t trid_nbo = cf_swap_to_be64(query->job_id); //swaps in place
        mf->field_sz = sizeof(trid_nbo) + 1;
        memcpy(mf->data, &trid_nbo, sizeof(trid_nbo));
        mf_tmp = cl_msg_field_get_next(mf);
        cl_msg_swap_field_to_be(mf);
        mf = mf_tmp;
    }

    if ( query->udf.type != AS_UDF_CALLTYPE_NONE ) {
        mf->type = CL_MSG_FIELD_TYPE_UDF_OP;
        mf->field_sz =  1 + 1;
        switch ( query->udf.type ) {
            case AS_UDF_CALLTYPE_RECORD:
                *mf->data = CL_UDF_MSG_VAL_RECORD;
                break;
            case AS_UDF_CALLTYPE_STREAM:
                *mf->data = CL_UDF_MSG_VAL_STREAM;
                break;
            default:
                // should never happen!
                break;
        }

        mf_tmp = cl_msg_field_get_next(mf);
        cl_msg_swap_field_to_be(mf);
        mf = mf_tmp;

        // Append filename to message fields
        int len = 0;
        len = (int)strlen(query->udf.filename) * sizeof(char);
        mf->type = CL_MSG_FIELD_TYPE_UDF_FILENAME;
        mf->field_sz =  len + 1;
        memcpy(mf->data, query->udf.filename, len);

        mf_tmp = cl_msg_field_get_next(mf);
        cl_msg_swap_field_to_be(mf);
        mf = mf_tmp;

        // Append function name to message fields
        len = (int)strlen(query->udf.function) * sizeof(char);
        mf->type = CL_MSG_FIELD_TYPE_UDF_FUNCTION;
        mf->field_sz =  len + 1;
        memcpy(mf->data, query->udf.function, len);

        mf_tmp = cl_msg_field_get_next(mf);
        cl_msg_swap_field_to_be(mf);
        mf = mf_tmp;

        // Append arglist to message fields
        len = argbuffer.size * sizeof(char);
        mf->type = CL_MSG_FIELD_TYPE_UDF_ARGLIST;
        mf->field_sz = len + 1;
        memcpy(mf->data, argbuffer.data, len);

        mf_tmp = cl_msg_field_get_next(mf);
        cl_msg_swap_field_to_be(mf);
        mf = mf_tmp;
    }

    if (!buf) { 
        if (mbuf) {
            free(mbuf); 
        }
        as_buffer_destroy(&argbuffer);
        return CITRUSLEAF_FAIL_CLIENT;
    }

    as_buffer_destroy(&argbuffer);
    return CITRUSLEAF_OK;
}

//
// TODO: hard to put into udf.h because of the type requirements.
// FIX
extern as_val * citrusleaf_udf_bin_to_val(as_serializer *ser, cl_bin *);

/**
 * Get a value for a bin of with the given key.
 */
static as_val * query_response_get(const as_rec * rec, const char * name)  {
    as_val * v = NULL;
    cl_query_response_rec * r = (cl_query_response_rec *) rec->data;

    if ( r == NULL ) return v;

    if ( r->values != NULL ) {
        as_string key;
        v = as_map_get(r->values, (as_val *) as_string_init(&key, (char *)name, false));
    }

    if ( v == NULL ) {
        for (int i = 0; i < r->n_bins; i++) {
            // Raj (todo) remove this stupid linear search from here
            if (!strcmp(r->bins[i].bin_name, name)) {
                as_serializer ser;
                as_msgpack_init(&ser);
                v = citrusleaf_udf_bin_to_val(&ser, &r->bins[i]);
                as_serializer_destroy(&ser);
                break;
            }
        }

        if ( v ) {
            if ( r->values == NULL ) {
                r->values = (as_map *) as_hashmap_new(32);
            }
            as_string * key = as_string_new(strdup(name), true);
            as_map_set(r->values, (as_val *) key, v);
        }
    }

    return v;
}

static uint32_t query_response_ttl(const as_rec * rec) {
    cl_query_response_rec * r = (cl_query_response_rec *) rec->data;
    return r->record_ttl;
}

static uint16_t query_response_gen(const as_rec * rec) {
    cl_query_response_rec * r = (cl_query_response_rec *) rec->data;
    if (!r) return 0;
    return r->generation;
}

bool query_response_destroy(as_rec *rec) {
    cl_query_response_rec * r = (cl_query_response_rec *) rec->data;
    if ( !r ) return false;
    if ( r->bins ) {
        citrusleaf_bins_free(r->bins, r->n_bins);
        if (r->free_bins) free(r->bins);
    }
    if ( r->ns )        free(r->ns);
    if ( r->set )       free(r->set);
    if ( r->values )    {
        as_map_destroy(r->values);
        r->values = NULL;
    }
    if ( r->ismalloc )  free(r);
    rec->data = NULL;
    return true;
}

// Chris(todo) needs addition to the as_rec interface
cf_digest query_response_digest(const as_rec *rec) {
    cl_query_response_rec * r = (cl_query_response_rec *) rec->data;
    return r->keyd;
}

// Chris(todo) needs addition to the as_rec interface
uint64_t query_response_numbins(const as_rec *rec) {
    cl_query_response_rec * r = (cl_query_response_rec *) rec->data;
    if (!r) return 0;
    return r->n_bins;
}

const as_rec_hooks query_response_hooks = {
    .get        = query_response_get,
    .set        = NULL,
    .remove     = NULL,
    .ttl        = query_response_ttl,
    .gen        = query_response_gen,
    .destroy    = query_response_destroy
};

/* 
 * this is an actual instance of a query, running on a query thread
 */
static int cl_query_worker_do(as_node * node, cl_query_task * task) {

    uint8_t     rd_stack_buf[STACK_BUF_SZ] = {0};    
    uint8_t *   rd_buf = rd_stack_buf;
    size_t      rd_buf_sz = 0;

	int fd;
	int rc = as_node_get_connection(node, &fd);
    if (rc) {
        return rc;
    }

    // send it to the cluster - non blocking socket, but we're blocking
    if (0 != cf_socket_write_forever(fd, (uint8_t *) task->query_buf, (size_t) task->query_sz)) {
        LOG("[ERROR] cl_query_worker_do: unable to write to %s ",node->name);
        return CITRUSLEAF_FAIL_CLIENT;
    }

    cl_proto  proto;
    bool      done = false;

    do {
        // multiple CL proto per response
        // Now turn around and read a fine cl_proto - that's the first 8 bytes 
        // that has types and lengths
        if ( (rc = cf_socket_read_forever(fd, (uint8_t *) &proto, sizeof(cl_proto) ) ) ) {
            LOG("[ERROR] cl_query_worker_do: network error: errno %d fd %d\n", rc, fd);
            return CITRUSLEAF_FAIL_CLIENT;
        }
        cl_proto_swap_from_be(&proto);

        if ( proto.version != CL_PROTO_VERSION) {
            LOG("[ERROR] cl_query_worker_do: network error: received protocol message of wrong version %d\n",proto.version);
            return CITRUSLEAF_FAIL_CLIENT;
        }

        if ( proto.type != CL_PROTO_TYPE_CL_MSG && proto.type != CL_PROTO_TYPE_CL_MSG_COMPRESSED ) {
            LOG("[ERROR] cl_query_worker_do: network error: received incorrect message version %d\n",proto.type);
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

            if (rd_buf == NULL) return CITRUSLEAF_FAIL_CLIENT;

            if ( (rc = cf_socket_read_forever(fd, rd_buf, rd_buf_sz)) ) {
                LOG("[ERROR] cl_query_worker_do: network error: errno %d fd %d\n", rc, fd);
                if ( rd_buf != rd_stack_buf ) free(rd_buf);
                return CITRUSLEAF_FAIL_CLIENT;
            }
        }

        // process all the cl_msg in this proto
        uint8_t *   buf = rd_buf;
        uint        pos = 0;
        cl_bin      stack_bins[STACK_BINS];
        cl_bin *    bins;
		cl_object   key;
		citrusleaf_object_init_null(&key);

        while (pos < rd_buf_sz) {

            uint8_t *   buf_start = buf;
            cl_msg *    msg = (cl_msg *) buf;

            cl_msg_swap_header_from_be(msg);
            buf += sizeof(cl_msg);

            if ( msg->header_sz != sizeof(cl_msg) ) {
                LOG("[ERROR] cl_query_worker_do: received cl msg of unexpected size: expecting %zd found %d, internal error\n",
                        sizeof(cl_msg),msg->header_sz);
                return CITRUSLEAF_FAIL_CLIENT;
            }

            // parse through the fields
            cf_digest       keyd;
            char            ns_ret[AS_NAMESPACE_MAX_SIZE] = {0};
            char           	set_ret[AS_SET_MAX_SIZE] = {0};
            cl_msg_field *  mf          = (cl_msg_field *)buf;

            for (int i=0; i < msg->n_fields; i++) {
                cl_msg_swap_field_from_be(mf);
                if (mf->type == CL_MSG_FIELD_TYPE_KEY) {
					uint8_t* flat_key = mf->data;
					uint8_t* flat_val = &flat_key[1];
					switch (flat_key[0]) {
					case CL_INT:
						citrusleaf_object_init_int(&key, cf_swap_from_be64(*(int64_t*)flat_val));
						break;
					case CL_STR:
						// The object value pointer points straight into rd_buf,
						// and relies on shim to copy and null-terminate it.
						citrusleaf_object_init_str2(&key, (const char*)flat_val, cl_msg_field_get_value_sz(mf) - 1);
						break;
					case CL_BLOB:
						// The object value pointer points straight into rd_buf,
						// and relies on shim to copy it.
						citrusleaf_object_init_blob(&key, (const void*)flat_val, cl_msg_field_get_value_sz(mf) - 1);
						break;
					default:
						cf_error("scan: ignoring key with unrecognized type %d", flat_key[0]);
						break;
					}
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
                    memcpy(set_ret, mf->data, set_name_len);
                    set_ret[ set_name_len ] = '\0';
                }
                mf = cl_msg_field_get_next(mf);
            }

            bool free_bins = false;
            buf = (uint8_t *) mf;
            if (msg->n_ops > STACK_BINS) {
                bins = malloc(sizeof(cl_bin) * msg->n_ops);
                free_bins = true;
            }
            else {
                bins = stack_bins;
            }

            if (bins == NULL) {
                return CITRUSLEAF_FAIL_CLIENT;
            }

            // parse through the bins/ops
            cl_msg_op * op = (cl_msg_op *) buf;
            for (int i=0;i<msg->n_ops;i++) {

                cl_msg_swap_op_from_be(op);

#ifdef DEBUG_VERBOSE
                LOG("[DEBUG] cl_query_worker_do: op receive: %p size %d op %d ptype %d pversion %d namesz %d \n",
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
            }
            else if (msg->info3 & CL_MSG_INFO3_LAST)    {

#ifdef DEBUG                
                LOG("[DEBUG] cl_query_worker_do: received final message\n");
#endif                
                done = true;
            }
            else if ((msg->n_ops || (msg->info1 & CL_MSG_INFO1_NOBINDATA))) {

                as_record r;
                as_record * record = &r;

				as_record_inita(record, msg->n_ops);

				askey_from_clkey(&record->key, ns_ret, set_ret, &key);
                memcpy(record->key.digest.value, &keyd, 20);
                record->key.digest.init = true;

                record->ttl = cf_server_void_time_to_ttl(msg->record_ttl);
    			record->gen = msg->generation;

                clbins_to_asrecord(bins, msg->n_ops, record);

                // TODO:
                //      Fix the following block of code. It is really lame 
                //      to check for a bin called "SUCCESS" to determine
                //      whether you have a single value or not.
                // TODO:
                //      Fix how we are to handle errors.... not everything
                //      will be a "SUCCESS"... or will it?
                //
                // got one good value? call it a success!
                // (Note:  In the key exists case, there is no bin data.)
                as_val * v = (as_val *) as_record_get(record, "SUCCESS");

                if ( v  != NULL ) {
                    // I only need this value. The rest of the record is useless.
                    // Need to detach the value from the record (result)
                    // then release the record back to the wild... or wherever
                    // it came from.
					as_val * vp = NULL;
					if ( !v->free ) {
						switch(as_val_type(v)) {
							case AS_INTEGER:
								vp = (as_val *) as_integer_new(as_integer_get((as_integer *)v));
								break;
							case AS_STRING: {
								as_string * s = (as_string *)v;
								vp = (as_val *) as_string_new(as_string_get(s), true);
								s->value = NULL;
								break;
							}
							case AS_BYTES: {
								as_bytes * b = (as_bytes *)v;
								vp = (as_val *) as_bytes_new_wrap(as_bytes_get(b), as_bytes_size(b), true);
								b->value = NULL;
								b->size = 0;
								break;
							}
							default:
								LOG("[WARNING] unknown stack as_val type\n");
								break;
						}
					}
					else {
						vp = as_val_reserve(v);
					}
					task->callback(vp, task->udata);
                }
                else {
                    as_val * v_fail = (as_val *) as_record_get(record, "FAILURE");
                    if(v_fail != NULL) {
                        done = true;
                        rc = CITRUSLEAF_FAIL_UNKNOWN;
                        as_val * vp = NULL;
                        if ( !v_fail->free ) {
                            switch (as_val_type(v_fail)) {

                                case AS_STRING: {
                                    as_string * s = (as_string *)v_fail;
                                    vp = (as_val *) as_string_new(as_string_get(s), true);
                                    s->value = NULL;
                                    break;
                                    }    
                                default:
                                    LOG("[WARNING] unknown stack as_val type\n");
                                    break;
                            }    
                        }    
                        else {
                            vp = as_val_reserve(v_fail);
                        }    
                        task->err_val = vp;

                    }    
                    else {
                        task->callback((as_val *) record, task->udata);
                    }
                }

				as_record_destroy(record);
                if (task->err_val) 
                    rc = CITRUSLEAF_FAIL_UNKNOWN;
                else
                    rc = CITRUSLEAF_OK;
            }

			citrusleaf_bins_free(bins, (int)msg->n_ops);

                if (free_bins) {
                    free(bins);
                    bins = 0;
                }

            // don't have to free object internals. They point into the read buffer, where
            // a pointer is required
            pos += buf - buf_start;
            if (task->abort || gasq_abort) {
                break;
            }

        }

        if (rd_buf && (rd_buf != rd_stack_buf))    {
            free(rd_buf);
            rd_buf = 0;
        }

        // abort requested by the user
        if (task->abort || gasq_abort) {
			cf_close(fd);
            goto Final;
        }
    } while ( done == false );

    as_node_put_connection(node, fd);

    goto Final;

Final:    

#ifdef DEBUG_VERBOSE    
    LOG("[DEBUG] exited loop: rc %d\n", rc );
#endif    

    return rc;
}

static void * cl_query_worker(void * pv_asc) {
	as_cluster* asc = (as_cluster*)pv_asc;

    while (true) {
        cl_query_task task;

        if ( 0 != cf_queue_pop(asc->query_q, &task, CF_QUEUE_FOREVER) ) {
            LOG("[WARNING] cl_query_worker: queue pop failed\n");
        }

#ifdef DEBUG_VERBOSE
        if ( cf_debug_enabled() ) {
            LOG("[DEBUG] cl_query_worker: getting one task item\n");
        }
#endif

        // This is how query shutdown signals we're done.
        if( ! task.asc ) {
            LOG("[DEBUG] cl_query_worker: exiting\n");
            break;
        }

        // query if the node is still around
        as_query_fail_t rc_fail = {
            .rc      = CITRUSLEAF_FAIL_UNAVAILABLE,
            .err_val = NULL
        };

        as_node * node = as_node_get_by_name(task.asc, task.node_name);
        if ( node ) {
            LOG("[DEBUG] cl_query_worker: working\n");
            rc_fail.rc = cl_query_worker_do(node, &task);
			as_node_release(node);
        }
        if (task.err_val) {
            rc_fail.err_val = task.err_val;
        }
        cf_queue_push(task.complete_q, (void *)&rc_fail);
    }

    return NULL;
}




static as_val * queue_stream_read(const as_stream * s) {
    as_val * val = NULL;
    if (CF_QUEUE_EMPTY == cf_queue_pop(as_stream_source(s), &val, CF_QUEUE_NOWAIT)) {
        return NULL;
    }
    // push it back so it can be destroyed
    cf_queue_push(as_stream_source(s), &val);
    return val;
}

// This is a no-op. the queue and its contents are destroyed in cl_query_destroy().
static int queue_stream_destroy(as_stream *s) {
    return 0;
}

static as_stream_status queue_stream_write(const as_stream * s, as_val * val) {
    if (CF_QUEUE_OK != cf_queue_push(as_stream_source(s), &val)) {
        LOG("[ERROR] queue_stream_write: Write to client side stream failed");
        as_val_destroy(val);
        return AS_STREAM_ERR;
    } 
    return AS_STREAM_OK;
}

static const as_stream_hooks queue_stream_hooks = {
    .destroy  = queue_stream_destroy,
    .read     = queue_stream_read,
    .write    = queue_stream_write
};


typedef struct {
    void *  udata;
    bool    (* callback)(as_val *, void *);
} callback_stream_source;

static int callback_stream_destroy(as_stream *s) {
    return 0;
}

static as_stream_status callback_stream_write(const as_stream * s, as_val * val) {
    callback_stream_source * source = (callback_stream_source *) as_stream_source(s);
    source->callback(val, source->udata);
    as_val_destroy(val);
    return AS_STREAM_OK;
}

static const as_stream_hooks callback_stream_hooks = {
    .destroy  = callback_stream_destroy,
    .read     = NULL,
    .write    = callback_stream_write
};

static as_stream * callback_stream_init(as_stream * stream, callback_stream_source * source) {
    as_stream_init(stream, source, &callback_stream_hooks);
    return stream;
}



static cl_rv cl_query_udf_init(cl_query_udf * udf, cl_query_udf_type type, const char * filename, const char * function, as_list * arglist) {
    udf->type        = type;
    udf->filename    = filename == NULL ? NULL : strdup(filename);
    udf->function    = function == NULL ? NULL : strdup(function);
    udf->arglist     = arglist;
    return CITRUSLEAF_OK;
}

static cl_rv cl_query_udf_destroy(cl_query_udf * udf) {

    udf->type = AS_UDF_CALLTYPE_NONE;

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


static int query_aerospike_log(const as_aerospike * as, const char * file, const int line, const int level, const char * msg) {
    switch(level) {
        case 1:
			as_logger_warn(mod_lua.logger, "%s:%d - %s", file, line, msg);
            break;
        case 2:
			as_logger_info(mod_lua.logger, "%s:%d - %s", file, line, msg);
            break;
        case 3:
 			as_logger_debug(mod_lua.logger, "%s:%d - %s", file, line, msg);
            break;
        default:
			as_logger_trace(mod_lua.logger, "%s:%d - %s", file, line, msg);
            break;
    }
    return 0;
}

static const as_aerospike_hooks query_aerospike_hooks = {
    .destroy = NULL,
    .rec_create = NULL,
    .rec_update = NULL,
    .rec_remove = NULL,
    .rec_exists = NULL,
    .log = query_aerospike_log,
};


static cl_rv cl_query_execute(as_cluster * cluster, const cl_query * query, void * udata, int (* callback)(as_val *, void *), as_val ** err_val) {

    cl_rv       rc                          = CITRUSLEAF_OK;
    uint8_t     wr_stack_buf[STACK_BUF_SZ]  = { 0 };
    uint8_t *   wr_buf                      = wr_stack_buf;
    size_t      wr_buf_sz                   = sizeof(wr_stack_buf);

    // compile the query - a good place to fail    
    rc = query_compile(query, &wr_buf, &wr_buf_sz);

    if ( rc != CITRUSLEAF_OK ) {
        LOG("[ERROR] cl_query_execute query compile failed: \n");
        return rc;
    }

    // Setup worker
    cl_query_task task = {
        .asc                = cluster,
        .ns                 = query->ns,
        .query_buf          = wr_buf,
        .query_sz           = wr_buf_sz,
        .udata              = udata,
        .callback           = callback,
		.abort              = false,
        .err_val            = NULL
    };

    char *node_names    = NULL;    
    int   node_count    = 0;

    // Get a list of the node names, so we can can send work to each node
	as_cluster_get_node_names(cluster, &node_count, &node_names);
    if ( node_count == 0 ) {
        LOG("[ERROR] cl_query_execute: don't have any nodes?\n");
        return CITRUSLEAF_FAIL_CLIENT;
    }

	task.complete_q = cf_queue_create(sizeof(as_query_fail_t), true);
    // Dispatch work to the worker queue to allow the transactions in parallel
    // NOTE: if a new node is introduced in the middle, it is NOT taken care of
    char * node_name = node_names;
    for ( int i=0; i < node_count; i++ ) {
        // fill in per-request specifics
        strcpy(task.node_name, node_name);
        cf_queue_push(cluster->query_q, &task);
        node_name += NODE_NAME_SIZE;                    
    }
    free(node_names);
    node_names = NULL;

    // wait for the work to complete from all the nodes.
    rc = CITRUSLEAF_OK;
    for ( int i=0; i < node_count; i++ ) {
        as_query_fail_t node_rc;
        cf_queue_pop(task.complete_q, &node_rc, CF_QUEUE_FOREVER);
        if ( node_rc.rc != 0 ) {
            // Got failure from one node. Trigger abort for all 
            // the ongoing request
            task.abort = true;
            rc = node_rc.rc;
            if ( err_val ) {
                if ( *err_val )
                    as_val_destroy(*err_val);
                *err_val = node_rc.err_val;
            }
            else {
                as_val_destroy(node_rc.err_val);
            }   
        }
    }

    if ( wr_buf && (wr_buf != wr_stack_buf) ) { 
        free(wr_buf); 
        wr_buf = 0;
    }

    // If completely successful, make the callback that signals completion.
    if (rc == CITRUSLEAF_OK) {
    	callback(NULL, udata);
    }

	if (task.complete_q) cf_queue_destroy(task.complete_q);
    return rc;
}

static cl_rv query_where_generic(bool isfunction, cl_query *query, const char *binname, cl_query_op op, va_list list) { 
    query_range range;
    range.isfunction = isfunction;
    int type = va_arg(list, int);
    if ( type == CL_INT ) {
        uint64_t start = 0;
        uint64_t end   = 0;
        switch(op) {
            case CL_EQ:
                start = end = va_arg(list, uint64_t);
                break;
            case CL_LE: range.closedbound = true;
            case CL_LT:
                start = 0;
                end = va_arg(list, uint64_t);
                break;
            case CL_GE: range.closedbound = true;
            case CL_GT:
                start = va_arg(list, uint64_t);
                end = UINT64_MAX;
                break;
            case CL_RANGE:
                start = va_arg(list, uint64_t);
                end = va_arg(list, uint64_t);
                break;
            default: 
                goto Cleanup;
        }
        citrusleaf_object_init_int(&range.start_obj, start);
        citrusleaf_object_init_int(&range.end_obj, end);
    }
    else if (type == CL_STR) {
        char *val = NULL;
        switch(op) {
            case CL_EQ:
                val = va_arg(list, char *);
                citrusleaf_object_init_str(&range.start_obj, val);
                citrusleaf_object_init_str(&range.end_obj, val);
                break;
            case CL_LE: 
            case CL_LT:
            case CL_GE:
            case CL_GT:
            case CL_RANGE:
            default:  
                goto Cleanup;
        }
    } else {
        goto Cleanup;
    }
    va_end(list);
    if (!query->ranges) {
        query->ranges = cf_vector_create(sizeof(query_range),5,0);
        if (query->ranges==NULL) {
            return CITRUSLEAF_FAIL_CLIENT;
        }
    }
    strcpy(range.bin_name, binname);
    cf_vector_append(query->ranges,(void *)&range);
    return CITRUSLEAF_OK;
Cleanup:
    va_end(list);
    return CITRUSLEAF_FAIL_CLIENT;
}




/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * Allocates and initializes a new cl_query.
 */
cl_query * cl_query_new(const char * ns, const char * setname) {
    cl_query * query = malloc(sizeof(cl_query));
    memset(query, 0, sizeof(cl_query));
    return cl_query_init(query, ns, setname);
}

/**
 * Initializes an cl_query
 */
cl_query * cl_query_init(cl_query * query, const char * ns, const char * setname) {
    if ( query == NULL ) return query;

    cf_queue * result_queue = cf_queue_create(sizeof(void *), true);
    if ( !result_queue ) {
        query->res_streamq = NULL;
        return query;
    }

    query->res_streamq = result_queue;
    query->job_id = cf_get_rand64();
    query->setname = setname == NULL ? NULL : strdup(setname);
    query->ns = ns == NULL ? NULL : strdup(ns);

    cl_query_udf_init(&query->udf, AS_UDF_CALLTYPE_NONE, NULL, NULL, NULL);

    return query;
}

void cl_query_destroy(cl_query *query) {

    if ( query == NULL ) return;

    if (query->binnames) {
        cf_vector_destroy(query->binnames);
    }

    if (query->ranges) {
        for (uint i=0; i<cf_vector_size(query->ranges); i++) {
            query_range *range = (query_range *)cf_vector_getp(query->ranges, i);
            cl_range_destroy(range);
        }
        cf_vector_destroy(query->ranges);
    }

    if (query->filters) {
        for (uint i=0; i<cf_vector_size(query->filters); i++) {
            query_filter *filter = (query_filter *)cf_vector_getp(query->filters, i);
            cl_filter_destroy(filter);
        }
        cf_vector_destroy(query->filters);
    }

    if (query->orderbys) {
        cf_vector_destroy(query->orderbys);
    }

    cl_query_udf_destroy(&query->udf);
    if (query->ns)      free(query->ns);
    if (query->setname) free(query->setname);

    if ( query->res_streamq ) {
        as_val *val = NULL;
        while (CF_QUEUE_OK == cf_queue_pop (query->res_streamq, 
                                        &val, CF_QUEUE_NOWAIT)) {
            as_val_destroy(val);
            val = NULL;
        }

        cf_queue_destroy(query->res_streamq);
        query->res_streamq = NULL;
    }

    free(query);
    query = NULL;
}

cl_rv cl_query_select(cl_query *query, const char *binname) {
    if ( !query->binnames ) {
        query->binnames = cf_vector_create(CL_BINNAME_SIZE, 5, 0);
        if (query->binnames==NULL) {
            return CITRUSLEAF_FAIL_CLIENT;
        }
    }
    cf_vector_append(query->binnames, (void *)binname);    
    return CITRUSLEAF_OK;    
}

cl_rv cl_query_where_function(cl_query *query, const char *finame, cl_query_op op, ...) {
    va_list args;
    va_start(args, op);
    cl_rv rv = query_where_generic(true, query, finame, op, args); 
    va_end(args);
    return rv;
}

cl_rv cl_query_where(cl_query *query, const char *binname, cl_query_op op, ...) {
    va_list args;
    va_start(args, op);
    cl_rv rv = query_where_generic(false, query, binname, op, args); 
    va_end(args);
    return rv;
}

cl_rv cl_query_filter(cl_query *query, const char *binname, cl_query_op op, ...) {
    return CITRUSLEAF_OK;
}

cl_rv cl_query_orderby(cl_query *query, const char *binname, cl_query_orderby_op op) {
    return CITRUSLEAF_OK;
}

cl_rv cl_query_aggregate(cl_query * query, const char * filename, const char * function, as_list * arglist) {
    return cl_query_udf_init(&query->udf, AS_UDF_CALLTYPE_STREAM, filename, function, arglist);
}

cl_rv cl_query_foreach(cl_query * query, const char * filename, const char * function, as_list * arglist) {
    return cl_query_udf_init(&query->udf, AS_UDF_CALLTYPE_RECORD, filename, function, arglist);
}

cl_rv cl_query_limit(cl_query *query, uint64_t limit) {
    return CITRUSLEAF_OK;    
}


// This callback will populate an intermediate stream, to be used for the aggregation
static int citrusleaf_query_foreach_callback_stream(as_val * v, void * udata) {
	as_stream * queue_stream = (as_stream *) udata;
    as_stream_write(queue_stream, v == NULL ? AS_STREAM_END : v );
    return 0;
}

// The callback calls the foreach function for each value
static int citrusleaf_query_foreach_callback(as_val * v, void * udata) {
	callback_stream_source * source = (callback_stream_source *) udata;
    source->callback(v, source->udata);
    return 0;
}


cl_rv citrusleaf_query_foreach(as_cluster * cluster, const cl_query * query, void * udata, cl_query_cb foreach, as_val ** err_val) {

    cl_rv rc = CITRUSLEAF_OK;

    callback_stream_source source = {
        .udata      = udata,
        .callback   = foreach
    };

    if ( query->udf.type == AS_UDF_CALLTYPE_STREAM ) {

        // Setup as_aerospike, so we can get log() function.
        // TODO: this should occur only once
        as_aerospike as;
        as_aerospike_init(&as, NULL, &query_aerospike_hooks);

        // stream for results from each node
        as_stream queue_stream;
        as_stream_init(&queue_stream, query->res_streamq, &queue_stream_hooks); 

        // The callback stream provides the ability to write to a callback function
        // when as_stream_write is called.
        as_stream ostream;
        callback_stream_init(&ostream, &source);

        // sink the data from multiple sources into the result stream
        rc = cl_query_execute(cluster, query, &queue_stream, citrusleaf_query_foreach_callback_stream, err_val);

        if ( rc == CITRUSLEAF_OK ) {

        	as_udf_context ctx = {
        		.as = &as,
        		.timer = NULL,
        		.memtracker = NULL
        	};

            // Apply the UDF to the result stream
            as_result   res;
            as_result_init(&res);
            int ret = as_module_apply_stream(&mod_lua, &ctx, query->udf.filename, query->udf.function, &queue_stream, query->udf.arglist, &ostream, &res); //
            if (ret != 0 && err_val) { 
                rc = CITRUSLEAF_FAIL_UDF_LUA_EXECUTION;
                char *rs = as_module_err_string(ret);
                as_val * vp = NULL;
                if (res.value != NULL) {
                    switch (as_val_type(res.value)) {
                        case AS_STRING: {
                            as_string * lua_s   = as_string_fromval(res.value);
                            char *      lua_err  = (char *) as_string_tostring(lua_s);
                            if (lua_err != NULL) {
                                int l_rs_len = (int)strlen(rs);
                                rs = cf_realloc(rs,l_rs_len + strlen(lua_err) + 4);
                                sprintf(&rs[l_rs_len]," : %s",lua_err);
                            }
                            vp = (as_val *) as_string_new(rs, true);
                            break;
                            }    
                        default:
                            LOG("[WARNING] unknown stack as_val type\n");
                            break;
                    }    
                }    
                if (vp != NULL) {
                    *err_val = vp;
                }    
              }    
              as_result_destroy(&res);
        }
    }
    else {
        // sink the data from multiple sources into the result stream
        rc = cl_query_execute(cluster, query, &source, citrusleaf_query_foreach_callback, err_val);
    }

    return rc;
}


int
cl_cluster_query_init(as_cluster* asc)
{
	// We do this lazily, during the first query request, so make sure it's only
	// done once.
	if (ck_pr_fas_32(&asc->query_initialized, 1) == 1 || asc->query_q) {
		return 0;
	}

	if (cf_debug_enabled()) {
		LOG("[DEBUG] cl_cluster_query_init: creating %d threads\n", NUM_QUERY_THREADS);
	}

	// Create dispatch queue.
	asc->query_q = cf_queue_create(sizeof(cl_query_task), true);

	// Create thread pool.
	for (int i = 0; i < AS_NUM_QUERY_THREADS; i++) {
		pthread_create(&asc->query_threads[i], 0, cl_query_worker, (void*)asc);
	}

	return 0;
}

void
cl_cluster_query_shutdown(as_cluster* asc)
{
	// Check whether we ever (lazily) initialized query machinery.
	if (ck_pr_load_32(&asc->query_initialized) == 0 && ! asc->query_q) {
		return;
	}

	// This tells the worker threads to stop. We do this (instead of using a
	// "running" flag) to allow the workers to "wait forever" on processing the
	// work dispatch queue, which has minimum impact when the queue is empty.
	// This also means all queued requests get processed when shutting down.
	for (int i = 0; i < AS_NUM_QUERY_THREADS; i++) {
		cl_query_task task;
		task.asc = NULL;
		cf_queue_push(asc->query_q, &task);
	}

	for (int i = 0; i < AS_NUM_QUERY_THREADS; i++) {
		pthread_join(asc->query_threads[i], NULL);
	}

	cf_queue_destroy(asc->query_q);
	asc->query_q = NULL;
	ck_pr_store_32(&asc->query_initialized, 0);
}
