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
#include <asm/byteorder.h> // 64-bit swap macro

#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_socket.h>
#include <citrusleaf/cf_vector.h>
#include <citrusleaf/cf_random.h>
#include <citrusleaf/cf_proto.h>

#include <aerospike/as_aerospike.h>
#include <aerospike/as_module.h>
#include <aerospike/as_msgpack.h>
#include <aerospike/as_list.h>
#include <aerospike/as_serializer.h>
#include <aerospike/as_string.h>
#include <aerospike/mod_lua.h>
#include <aerospike/mod_lua_config.h>

#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/cl_cluster.h>
#include <citrusleaf/cl_query.h>
#include <citrusleaf/cl_udf.h>

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
#define N_MAX_QUERY_THREADS  5

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


/******************************************************************************
 * TYPES
 *****************************************************************************/

/*
 * Work item which gets queued up to each node
 */
typedef struct {
    cl_cluster *            asc; 
    const char *            ns;
    char                    node_name[NODE_NAME_SIZE];    
    const uint8_t *         query_buf;
    size_t                  query_sz;
    void *                  udata;
    int                     (* callback)(as_val *, void *);
    bool                    isinline;
	cf_queue              * complete_q;
	bool                    abort;
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


/******************************************************************************
 * VARIABLES
 *****************************************************************************/

cf_atomic32     query_initialized  = 0;
cf_queue *      g_query_q          = 0;
pthread_t       g_query_th[N_MAX_QUERY_THREADS];
cl_query_task   g_null_task;
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

// static int do_query_monte(cl_cluster_node *node, const char *ns, const uint8_t *query_buf, size_t query_sz, cl_query_cb cb, void *udata, bool isnbconnect, as_stream *);

static cl_rv cl_query_udf_init(cl_query_udf * udf, cl_query_udf_type type, const char * filename, const char * function, as_list * arglist);

static cl_rv cl_query_udf_destroy(cl_query_udf * udf);

// static cl_rv cl_query_execute_sink(cl_cluster * cluster, const cl_query * query, as_stream * stream);

static cl_rv cl_query_execute(cl_cluster * cluster, const cl_query * query, void * udata, int (* callback)(as_val *, void *), bool istream);

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
            uint32_t ss = psz; 
            *((uint32_t *)buf) = ntohl(ss);
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

    if ( (query->udf.type != AS_QUERY_UDF_NONE) && (query->udf.arglist != NULL) ) {
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

        ns_len  = strlen(query->ns);
        if (ns_len) {
            n_fields++;
            msg_sz += ns_len  + sizeof(cl_msg_field); 
        }

        // indexname field
        if ( query->indexname ) {
            iname_len = strlen(query->indexname);
            if (iname_len) {
                n_fields++;
                msg_sz += strlen(query->indexname) + sizeof(cl_msg_field);
            }
        }

        if (query->setname) {
            setname_len = strlen(query->setname);
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
        if ( query->udf.type != AS_QUERY_UDF_NONE ) {
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
        cl_msg_swap_field(mf);
        mf = mf_tmp;
    }

    if (iname_len) {
        mf->type = CL_MSG_FIELD_TYPE_INDEX_NAME;
        mf->field_sz = iname_len + 1;
        memcpy(mf->data, query->indexname, iname_len);
        mf_tmp = cl_msg_field_get_next(mf);
        cl_msg_swap_field(mf);
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
        cl_msg_swap_field(mf);
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
        cl_msg_swap_field(mf);
        mf = mf_tmp;
    }

    if (query->binnames) {
        mf->type = CL_MSG_FIELD_TYPE_QUERY_BINLIST;
        mf->field_sz = num_bins + 1;
        query_compile_select(query->binnames, mf->data, &num_bins);
        mf_tmp = cl_msg_field_get_next(mf);
        cl_msg_swap_field(mf);
        mf = mf_tmp;
    }

    if (query->job_id) {
        mf->type = CL_MSG_FIELD_TYPE_TRID;
        // Convert the transaction-id to network byte order (big-endian)
        uint64_t trid_nbo = __cpu_to_be64(query->job_id); //swaps in place
        mf->field_sz = sizeof(trid_nbo) + 1;
        memcpy(mf->data, &trid_nbo, sizeof(trid_nbo));
        mf_tmp = cl_msg_field_get_next(mf);
        cl_msg_swap_field(mf);
        mf = mf_tmp;
    }

    if ( query->udf.type != AS_QUERY_UDF_NONE ) {
        mf->type = CL_MSG_FIELD_TYPE_UDF_OP;
        mf->field_sz =  1 + 1;
        switch ( query->udf.type ) {
            case AS_QUERY_UDF_RECORD:
                *mf->data = 0;
                break;
            case AS_QUERY_UDF_STREAM:
                *mf->data = 1;
                break;
            default:
                // should never happen!
                break;
        }

        mf_tmp = cl_msg_field_get_next(mf);
        cl_msg_swap_field(mf);
        mf = mf_tmp;

        // Append filename to message fields
        int len = 0;
        len = strlen(query->udf.filename) * sizeof(char);
        mf->type = CL_MSG_FIELD_TYPE_UDF_FILENAME;
        mf->field_sz =  len + 1;
        memcpy(mf->data, query->udf.filename, len);

        mf_tmp = cl_msg_field_get_next(mf);
        cl_msg_swap_field(mf);
        mf = mf_tmp;

        // Append function name to message fields
        len = strlen(query->udf.function) * sizeof(char);
        mf->type = CL_MSG_FIELD_TYPE_UDF_FUNCTION;
        mf->field_sz =  len + 1;
        memcpy(mf->data, query->udf.function, len);

        mf_tmp = cl_msg_field_get_next(mf);
        cl_msg_swap_field(mf);
        mf = mf_tmp;

        // Append arglist to message fields
        len = argbuffer.size * sizeof(char);
        mf->type = CL_MSG_FIELD_TYPE_UDF_ARGLIST;
        mf->field_sz = len + 1;
        memcpy(mf->data, argbuffer.data, len);

        mf_tmp = cl_msg_field_get_next(mf);
        cl_msg_swap_field(mf);
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
                r->values = as_hashmap_new(32);
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
static int cl_query_worker_do(cl_cluster_node * node, cl_query_task * task) {

    uint8_t     rd_stack_buf[STACK_BUF_SZ] = {0};    
    uint8_t *   rd_buf = rd_stack_buf;
    size_t      rd_buf_sz = 0;

    int fd = cl_cluster_node_fd_get(node, false, task->asc->nbconnect);
    if ( fd == -1 ) { 
        LOG("[ERROR] cl_query_worker_do: do query monte: cannot get fd for node %s ",node->name);
        return CITRUSLEAF_FAIL_CLIENT; 
    }

    // send it to the cluster - non blocking socket, but we're blocking
    if (0 != cf_socket_write_forever(fd, (uint8_t *) task->query_buf, (size_t) task->query_sz)) {
        LOG("[ERROR] cl_query_worker_do: unable to write to %s ",node->name);
        return CITRUSLEAF_FAIL_CLIENT;
    }

    cl_proto  proto;
    int       rc   = CITRUSLEAF_OK;
    bool      done = false;

    do {
        // multiple CL proto per response
        // Now turn around and read a fine cl_proto - that's the first 8 bytes 
        // that has types and lengths
        if ( (rc = cf_socket_read_forever(fd, (uint8_t *) &proto, sizeof(cl_proto) ) ) ) {
            LOG("[ERROR] cl_query_worker_do: network error: errno %d fd %d\n", rc, fd);
            return CITRUSLEAF_FAIL_CLIENT;
        }
        cl_proto_swap(&proto);

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

        while (pos < rd_buf_sz) {

            uint8_t *   buf_start = buf;
            cl_msg *    msg = (cl_msg *) buf;

            cl_msg_swap_header(msg);
            buf += sizeof(cl_msg);

            if ( msg->header_sz != sizeof(cl_msg) ) {
                LOG("[ERROR] cl_query_worker_do: received cl msg of unexpected size: expecting %zd found %d, internal error\n",
                        sizeof(cl_msg),msg->header_sz);
                return CITRUSLEAF_FAIL_CLIENT;
            }

            // parse through the fields
            cf_digest       keyd;
            char            ns_ret[33]  = {0};
            char *          set_ret     = NULL;
            cl_msg_field *  mf          = (cl_msg_field *)buf;

            for (int i=0; i < msg->n_fields; i++) {
                cl_msg_swap_field(mf);
                if (mf->type == CL_MSG_FIELD_TYPE_KEY) {
                    LOG("[INFO] cl_query_worker_do: read: found a key - unexpected\n");
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

            bool free_bins = false;
            buf = (uint8_t *) mf;
            if ((msg->n_ops > STACK_BINS) 
                    || (!task->isinline)) {
                bins = malloc(sizeof(cl_bin) * msg->n_ops);
                free_bins = true;
            }
            else {
                bins = stack_bins;
            }

            if (bins == NULL) {
                if (set_ret) {
                    free(set_ret);
                }
                return CITRUSLEAF_FAIL_CLIENT;
            }

            // parse through the bins/ops
            cl_msg_op * op = (cl_msg_op *) buf;
            for (int i=0;i<msg->n_ops;i++) {

                cl_msg_swap_op(op);

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

                cl_query_response_rec rec;
                cl_query_response_rec *recp = &rec;
                as_rec r;
                as_rec *rp = &r;

                if (!task->isinline) { 
                    recp = malloc(sizeof(cl_query_response_rec));
                    recp->ismalloc   = true;
                    rp = as_rec_new(recp, &query_response_hooks);    
                } else {
                    recp->ismalloc   = false;
                    rp = as_rec_init(rp, recp, &query_response_hooks);
                }

                recp->ns         = strdup(ns_ret);
                recp->keyd       = keyd;
                recp->set        = set_ret;
                recp->generation = msg->generation;
                recp->record_ttl = msg->record_ttl;
                recp->bins       = bins;
                recp->n_bins     = msg->n_ops;
                recp->values     = NULL;
                recp->free_bins  = free_bins;

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
                as_val * v = as_rec_get(rp, "SUCCESS");

                if ( v  != NULL ) {
                    // I only need this value. The rest of the record is useless.
                    // Need to detach the value from the record (result)
                    // then release the record back to the wild... or wherever
                    // it came from.
                    as_val_reserve(v);
                    task->callback(v, task->udata);

                    as_rec_destroy(rp);
                }
                else {
                    task->callback((as_val *) rp, task->udata);
                }
                rc = CITRUSLEAF_OK;
            }

            // if done free stack allocated stuff
            if (done) {
                if (free_bins) {
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
            close(fd);
            goto Final;
        }
    } while ( done == false );

    cl_cluster_node_fd_put(node, fd, false);

    goto Final;

Final:    

#ifdef DEBUG_VERBOSE    
    LOG("[DEBUG] exited loop: rc %d\n", rc );
#endif    

    return rc;
}

static void * cl_query_worker(void * dummy) {
    while (1) {

        cl_query_task task;

        if ( 0 != cf_queue_pop(g_query_q, &task, CF_QUEUE_FOREVER) ) {
            LOG("[WARNING] cl_query_worker: queue pop failed\n");
        }

        if ( cf_debug_enabled() ) {
            LOG("[DEBUG] cl_query_worker: getting one task item\n");
        }

        // a NULL structure is the condition that we should exit. See shutdown()
        if( 0 == memcmp(&task, &g_null_task, sizeof(cl_query_task)) ) { 
            LOG("[DEBUG] cl_query_worker: exiting\n");
            pthread_exit(NULL); 
        }

        // query if the node is still around
        int rc = CITRUSLEAF_FAIL_UNAVAILABLE;

        cl_cluster_node * node = cl_cluster_node_get_byname(task.asc, task.node_name);
        if ( node ) {
            LOG("[DEBUG] cl_query_worker: working\n");
            rc = cl_query_worker_do(node, &task);
        }

        cf_queue_push(task.complete_q, (void *)&rc);
    }
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

static int queue_stream_destroy(as_stream *s) {
    as_val * val = NULL;
    while (CF_QUEUE_EMPTY != cf_queue_pop(as_stream_source(s), &val, CF_QUEUE_NOWAIT)) {
        if (val) as_val_destroy(val);
    }
    cf_queue_destroy(as_stream_source(s));
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
    bool    (* callback)(const as_val *, void *);
} callback_stream_source;

static int callback_stream_destroy(as_stream *s) {
    return 0;
}

static as_stream_status callback_stream_write(const as_stream * s, as_val * val) {
    callback_stream_source * source = (callback_stream_source *) as_stream_source(s);
    source->callback(val, source->udata);
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

    udf->type = AS_QUERY_UDF_NONE;

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
    char l[10] = {'\0'};
    switch(level) {
        case 1:
            strncpy(l,"WARN",10);
            break;
        case 2:
            strncpy(l,"INFO",10);
            break;
        case 3:
            strncpy(l,"DEBUG",10);
            break;
        default:
            strncpy(l,"TRACE",10);
            break;
    }
    LOG("[%s:%d] %s - %s\n", file, line, l, msg);
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


static cl_rv cl_query_execute(cl_cluster * cluster, const cl_query * query, void * udata, int (* callback)(as_val *, void *), bool isinline) {

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
        .isinline           = isinline,
		.abort              = false
    };

    char *node_names    = NULL;    
    int   node_count    = 0;

    // Get a list of the node names, so we can can send work to each node
    cl_cluster_get_node_names(cluster, &node_count, &node_names);
    if ( node_count == 0 ) {
        LOG("[ERROR] cl_query_execute: don't have any nodes?\n");
        return CITRUSLEAF_FAIL_CLIENT;
    }

	task.complete_q = cf_queue_create(sizeof(int), true);
    // Dispatch work to the worker queue to allow the transactions in parallel
    // NOTE: if a new node is introduced in the middle, it is NOT taken care of
    char * node_name = node_names;
    for ( int i=0; i < node_count; i++ ) {
        // fill in per-request specifics
        strcpy(task.node_name, node_name);
        cf_queue_push(g_query_q, &task);
        node_name += NODE_NAME_SIZE;                    
    }
    free(node_names);
    node_names = NULL;

    // wait for the work to complete from all the nodes.
    rc = CITRUSLEAF_OK;
    for ( int i=0; i < node_count; i++ ) {
        int node_rc;
        cf_queue_pop(task.complete_q, &node_rc, CF_QUEUE_FOREVER);
        if ( node_rc != 0 ) {
            // Got failure from one node. Trigger abort for all 
            // the ongoing request
            task.abort = true;
            rc = node_rc;
        }
    }

    if ( wr_buf && (wr_buf != wr_stack_buf) ) { 
        free(wr_buf); 
        wr_buf = 0;
    }

    callback(NULL, udata);

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

    cl_query_udf_init(&query->udf, AS_QUERY_UDF_NONE, NULL, NULL, NULL);

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
    return cl_query_udf_init(&query->udf, AS_QUERY_UDF_STREAM, filename, function, arglist);
}

cl_rv cl_query_foreach(cl_query * query, const char * filename, const char * function, as_list * arglist) {
    return cl_query_udf_init(&query->udf, AS_QUERY_UDF_RECORD, filename, function, arglist);
}

cl_rv cl_query_limit(cl_query *query, uint64_t limit) {
    return CITRUSLEAF_OK;    
}



cl_rv citrusleaf_query_stream(cl_cluster * cluster, const cl_query * query, as_stream * ostream) {

    cl_rv rc = CITRUSLEAF_OK;

    // deserializer
    as_serializer ser;
    as_msgpack_init(&ser);

    if ( query->udf.type == AS_QUERY_UDF_STREAM ) {

        // Setup as_aerospike, so we can get log() function.
        // TODO: this should occur only once
        as_aerospike as;
        as_aerospike_init(&as, NULL, &query_aerospike_hooks);

        // stream for results from each node
        as_stream queue_stream;
        as_stream_init(&queue_stream, query->res_streamq, &queue_stream_hooks); 

        // callback for cl_query_execute()
        int callback(as_val * v, void * udata) {
            as_stream_write(&queue_stream, v == NULL ? AS_STREAM_END : v );
            return 0;
        }

        // sink the data from multiple sources into the result stream
        rc = cl_query_execute(cluster, query, NULL, callback, false);

        if ( rc == CITRUSLEAF_OK ) {
            as_module_apply_stream(&mod_lua, &as, query->udf.filename, query->udf.function, &queue_stream, query->udf.arglist, ostream);
        }
    }
    else {

        // callback for cl_query_execute()
        int callback(as_val * v, void * udata) {
            as_stream_write(ostream, v == NULL ? AS_STREAM_END : v );
            return 0;
        }

        // sink the data from multiple sources into the result stream
        rc = cl_query_execute(cluster, query, NULL, callback, false);
    }

    // cleanup deserializer
    as_serializer_destroy(&ser);

    return rc;
}



cl_rv citrusleaf_query_foreach(cl_cluster * cluster, const cl_query * query, void * udata, cl_query_cb foreach) {

    cl_rv rc = CITRUSLEAF_OK;

    if ( query->udf.type == AS_QUERY_UDF_STREAM ) {

        // Setup as_aerospike, so we can get log() function.
        // TODO: this should occur only once
        as_aerospike as;
        as_aerospike_init(&as, NULL, &query_aerospike_hooks);

        // stream for results from each node
        as_stream queue_stream;
        as_stream_init(&queue_stream, query->res_streamq, &queue_stream_hooks); 

        // The callback stream provides the ability to write to a callback function
        // when as_stream_write is called.
        callback_stream_source source = {
            .udata      = udata,
            .callback   = foreach
        };
        as_stream ostream;
        callback_stream_init(&ostream, &source);

        // This callback will populate an intermediate stream, to be used for the aggregation
        int callback(as_val * v, void * udata) {
            as_stream_write(&queue_stream, v == NULL ? AS_STREAM_END : v );
            return 0;
        }

        // sink the data from multiple sources into the result stream
        rc = cl_query_execute(cluster, query, NULL, callback, true);

        if ( rc == CITRUSLEAF_OK ) {
            // Apply the UDF to the result stream
            as_module_apply_stream(&mod_lua, &as, query->udf.filename, query->udf.function, &queue_stream, query->udf.arglist, &ostream);
        }

    }
    else {

        // The callback calls the foreach function for each value
        int callback(as_val * v, void * udata) {
            foreach(v, udata);
            return 0;
        }

        // sink the data from multiple sources into the result stream
        rc = cl_query_execute(cluster, query, udata, callback, true);
    }

    return rc;
}


int citrusleaf_query_init() {
    if (1 == cf_atomic32_incr(&query_initialized)) {

        if (cf_debug_enabled()) {
            LOG("[DEBUG] citrusleaf_query_init: creating %d threads\n",N_MAX_QUERY_THREADS);
        }

        memset(&g_null_task,0,sizeof(cl_query_task));

        // create dispatch queue
        g_query_q = cf_queue_create(sizeof(cl_query_task), true);


        // create thread pool
        for (int i = 0; i < N_MAX_QUERY_THREADS; i++) {
            pthread_create(&g_query_th[i], 0, cl_query_worker, 0);
        }
    }
    return(0);    
}

void citrusleaf_query_shutdown() {

    if (0 != cf_atomic32_get(query_initialized)) {
	    for( int i=0; i<N_MAX_QUERY_THREADS; i++) {
   	     	cf_queue_push(g_query_q, &g_null_task);
   		}

	    for( int i=0; i<N_MAX_QUERY_THREADS; i++) {
    	    pthread_join(g_query_th[i],NULL);
	    }
    	cf_queue_destroy(g_query_q);
    	cf_atomic32_decr(&query_initialized);
	}
}
