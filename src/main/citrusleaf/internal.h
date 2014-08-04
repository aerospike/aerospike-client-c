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

#pragma once

#include <inttypes.h>
#include <stdbool.h>
#include <netinet/in.h>

#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_ll.h>
#include <citrusleaf/cf_vector.h>
#include <citrusleaf/cf_queue.h>
#include <citrusleaf/cf_digest.h>
#include <citrusleaf/cf_log_internal.h>
#include <citrusleaf/cf_proto.h>

#include <aerospike/as_buffer.h>
#include <aerospike/as_string.h>

#include <citrusleaf/citrusleaf.h>
#include <aerospike/as_cluster.h>
#include <citrusleaf/cl_udf.h>
#include <citrusleaf/cl_scan.h>

/******************************************************************************
 * CONSTANTS
 ******************************************************************************/

#define MAX_PACKAGE_NAME_SIZE 64

// 30-39 RESEVED FOR UDF
#define CL_MSG_FIELD_TYPE_UDF_FILENAME          30
#define CL_MSG_FIELD_TYPE_UDF_FUNCTION          31
#define CL_MSG_FIELD_TYPE_UDF_ARGLIST           32

#pragma GCC diagnostic warning "-Wformat"

#define DO_PRAGMA(x) _Pragma (#x)
#define TODO(x) DO_PRAGMA(message ("TODO - " x))

/******************************************************************************
 * TYPES
 ******************************************************************************/


typedef struct cl_async_work cl_async_work;
typedef struct cl_batch_work cl_batch_work;
typedef struct as_call_s as_call;

struct cl_async_work {
	uint64_t			trid;		//Transaction-id of the submitted work
	uint64_t			deadline;	//Deadline time for this work item
	uint64_t			starttime;	//Start time for this work item
	as_node *			node;		//Node to which the work item was sent
	int					fd;		//FD used to send the command asynchronously
	void *				udata;
};

struct cl_batch_work {
	// these sections are the same for the same query
	as_cluster *			asc; 
    int          			info1;
	int          			info2;
	int          			info3;
	char *					ns;
	cf_digest * 			digests; 
	as_node **				nodes;
	int 					n_digests; 
	bool 					get_key;
	cl_bin *				bins;         // Bins. If this is used, 'operation' should be null, and 'operator' should be the operation to be used on the bins
	cl_operator     		operator;      // Operator.  The single operator used on all the bins, if bins is non-null
	cl_operation * 			operations;   // Operations.  Set of operations (bins + operators).  Should be used if bins is not used.
	int						n_ops;          // Number of operations (count of elements in 'bins' or count of elements in 'operations', depending on which is used. 
	citrusleaf_get_many_cb 	cb; 
	void *					udata;
	// struct mr_state_s * 	mr_state;
	cf_queue *				complete_q;
	// this is different for every work
	as_node *				my_node;
	int						my_node_digest_count;
	int 					index; // debug only
    int           			imatch;

};

struct as_call_s {
    as_string * file;
    as_string * func;
    as_buffer * args;
};

/******************************************************************************
 * VARIABLES
 ******************************************************************************/

extern shash * 	g_cl_async_hashtab;

// For XDS...
extern cf_queue * g_cl_async_q;
extern cf_queue	* g_cl_workitems_freepool_q;

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

int cl_del_node_asyncworkitems(void *key, void *value, void *clnode);


// citrusleaf.c used by cl_batch
int cl_value_to_op_get_size(cl_bin *v, size_t *sz);

uint8_t * cl_write_header(uint8_t *buf, size_t msg_sz, uint info1, uint info2, uint info3, uint32_t generation, uint32_t record_ttl, uint32_t transaction_ttl, uint32_t n_fields, uint32_t n_ops );

int cl_value_to_op(cl_bin *v, cl_operator clOperator, cl_operation *operation, cl_msg_op *op);

void cl_set_value_particular(cl_msg_op *op, cl_bin *value);

int cl_object_get_size(cl_object *obj, size_t *sz);

int cl_object_to_buf (cl_object *obj, uint8_t *data);

void cl_cluster_batch_init();


int cl_do_async_monte(as_cluster *asc, int info1, int info2, const char *ns, const char *set, const cl_object *key,
	const cf_digest *digest, cl_bin **values, cl_operator operator, cl_operation **operations,
	int *n_values, uint32_t *cl_gen, const cl_write_parameters *cl_w_p, uint64_t *trid, void *udata
	);

int do_the_full_monte(as_cluster *asc, int info1, int info2, int info3, const char *ns, const char *set, const cl_object *key,
	const cf_digest *digest, cl_bin **values, cl_operator operator, cl_operation **operations, int *n_values, 
	uint32_t *cl_gen, const cl_write_parameters *cl_w_p, uint64_t *trid, char **setname_r, as_call * call, uint32_t* cl_ttl
	);

int cl_compile(uint info1, uint info2, uint info3, const char *ns, const char *set, const cl_object *key, const cf_digest *digest,
	cl_bin *values, cl_operator operator, cl_operation *operations, int n_values,  
	uint8_t **buf_r, size_t *buf_sz_r, const cl_write_parameters *cl_w_p, cf_digest *d_ret, uint64_t trid, 
	cl_scan_param_field *scan_field, as_call * as_call, uint8_t udf_type
	);

int cl_parse(cl_msg *msg, uint8_t *buf, size_t buf_len, cl_bin **values_r, cl_operation **operations_r, 
	int *n_values_r, uint64_t *trid, char **setname_r
	);

// // Get a map reduce state - the instance - based on the job description
// cl_mr_state * cl_mr_state_get(const cl_mr_job *mrj);
// void cl_mr_state_put(cl_mr_state *mrs);

// // hand a row to the map reduce system
// // call with "islast" on the final bit! important!
// int cl_mr_state_row(cl_mr_state *mr_state, char *ns, cf_digest *keyd, char *set, 
// 	uint32_t generation, uint32_t record_ttl,
// 	cl_bin *bins, int n_bins, bool islast, citrusleaf_get_many_cb cb, void *udata
// 	);

// // All data has been done. Do finalize and any necessary callbacks
// int cl_mr_state_done(cl_mr_state *mr_state, citrusleaf_get_many_cb cb, void *udata);

// int citrusleaf_mr_init(void);
// void citrusleaf_mr_shutdown(void);



// int citrusleaf_query_init();

// void citrusleaf_query_shutdown();


// int sproc_compile_arg_field(char * const*argk, cl_object * const*argv, int argc, uint8_t *buf, int *sz_p);


// int citrusleaf_sproc_package_get(as_cluster *asc, const char *package, cl_script_lang_t lang);

// int citrusleaf_sproc_package_get_with_gen(as_cluster *asc, const char *package_name, char **content, int *content_len, char **gen, cl_script_lang_t lang_t);

#ifdef DEBUG_VERBOSE
void
dump_buf(char *info, uint8_t *buf, size_t buf_len);
#endif
