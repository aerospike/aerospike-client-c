/*
 * The Aerospike C interface. A good, basic library that many clients can be based on.
 *
 * This is the external, public header file
 *
 * this code currently assumes that the server is running in an ASCII-7 based
 * (ie, utf8 or ISO-LATIN-1)
 * character set, as values coming back from the server are UTF-8. We currently
 * don't bother to convert to the character set of the machine we're running on
 * but we advertise these values as 'strings'
 *
 * All rights reserved
 * Brian Bulkowski, 2009
 * CitrusLeaf
 */

#include <inttypes.h>
#include <stdbool.h>
#include <netinet/in.h>
 
// do this both the new skool and old skool way which gives the highest correctness,
// speed, and compatibility
#pragma once

#include "citrusleaf/citrusleaf.h"
#include "citrusleaf/cl_cluster.h"
#include "citrusleaf/proto.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_ll.h"
#include "citrusleaf/cf_vector.h"
#include "citrusleaf/cf_queue.h"
#include "citrusleaf/cf_alloc.h"
#include "citrusleaf/cf_digest.h"

#ifdef __cplusplus
extern "C" {
#endif

// TUNING PARAMETER FOR BATCH
#define N_BATCH_THREADS 20


// citrusleaf.c used by cl_batch
int
cl_value_to_op_get_size(cl_bin *v, size_t *sz);

uint8_t *
cl_write_header(uint8_t *buf, size_t msg_sz, uint info1, uint info2, uint info3, uint32_t generation, uint32_t record_ttl, uint32_t transaction_ttl, uint32_t n_fields, uint32_t n_ops );

int
cl_value_to_op(cl_bin *v, cl_operator clOperator, cl_operation *operation, cl_msg_op *op);

void
cl_set_value_particular(cl_msg_op *op, cl_bin *value);

int
cl_object_get_size(cl_object *obj, size_t *sz);

int
cl_object_to_buf (cl_object *obj, uint8_t *data);

extern shash		  		*g_cl_async_hashtab;

typedef struct cl_async_work {
	uint64_t	trid;		//Transaction-id of the submitted work
	uint64_t	deadline;	//Deadline time for this work item
	uint64_t	starttime;	//Start time for this work item
	cl_cluster_node *node;		//Node to which the work item was sent
	int		fd;		//FD used to send the command asynchronously
	void		*udata;
} cl_async_work;

int cl_del_node_asyncworkitems(void *key, void *value, void *clnode);

struct mr_state_s; // forward definition, specifics in cl_mapreduce.c
struct mr_package_s; // forward definition, specifics in cl_mapreduce.c

typedef struct cl_batch_work {
	
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
	
	struct mr_state_s *mr_state;

	cf_queue *complete_q;
	
	// this is different for every work
	cl_cluster_node *my_node;				
	int				my_node_digest_count;
	
	int 			index; // debug only

    int           imatch;

} cl_batch_work;




//
// Treat the lua state pointer as an exteral, saves us a lot of hassle
// with defines
struct lua_State;

#define MAX_PACKAGE_NAME_SIZE 64

//
// This is the state of a current map reduce job
// It has all the functions, and it has the number of nodes and number of
// responses currently received, and the current LUA object
// (may require a mutex, because you can get responses from multiple
// servers at the same time?)
//
typedef struct mr_state_s {

	// state of the current response - when are we done	
	int			num_nodes;
	int			responses;
	
	char 	package_name[MAX_PACKAGE_NAME_SIZE]; // used to queue state after done
	
	// enough information to find the package -- but don't need to load code
	const cl_mr_job 	*mr_job;

    struct lua_State *lua; // don't want to require the entire lua headers
    
} cl_mr_state;


// Get a map reduce state - the instance - based on the job description
cl_mr_state * cl_mr_state_get(const cl_mr_job *mrj);
void cl_mr_state_put(cl_mr_state *mrs);

// hand a row to the map reduce system
// call with "islast" on the final bit! important!
int
cl_mr_state_row(cl_mr_state *mr_state, char *ns, cf_digest *keyd, char *set, 
	uint32_t generation, uint32_t record_ttl,
	cl_bin *bins, int n_bins, bool islast, citrusleaf_get_many_cb cb, void *udata);
// All data has been done. Do finalize and any necessary callbacks
int
cl_mr_state_done(cl_mr_state *mr_state, citrusleaf_get_many_cb cb, void *udata);

int citrusleaf_mr_init(void);
void citrusleaf_mr_shutdown(void);

// scan fields
// left-to-right bits
// 0-3 priority hint = ClScanningPriority
// 4 = failOnClusterChange
// 5-7 = unused 
// 8-15 = scan_pct
 typedef struct cl_scan_param_field_s {
	uint8_t	byte1;
	uint8_t scan_pct;
} cl_scan_param_field;

// For XDS...
extern cf_queue	   *g_cl_async_q;
extern cf_queue	   *g_cl_workitems_freepool_q;


int
cl_do_async_monte(cl_cluster *asc, int info1, int info2, const char *ns, const char *set, const cl_object *key,
			const cf_digest *digest, cl_bin **values, cl_operator operator, cl_operation **operations,
			int *n_values, uint32_t *cl_gen, const cl_write_parameters *cl_w_p, uint64_t *trid, void *udata);

/*
int
do_the_full_monte(cl_cluster *asc, int info1, int info2, const char *ns, const char *set, const cl_object *key, 
	const cf_digest *digest, cl_bin **values, cl_operator operator, cl_operation **operations, 
	int *n_values, uint32_t *cl_gen, const cl_write_parameters *cl_w_p, uint64_t *trid);

int
do_many_monte(cl_cluster *asc, uint operation_info, uint operation_info2, const char *ns, const char *set, 
	const cf_digest *digests, int n_digests, citrusleaf_get_many_cb cb, void *udata);

int
do_async_monte(cl_cluster *asc, int info1, int info2, const char *ns, const char *set, const cl_object *key, 
	const cf_digest *digest, cl_bin **values, cl_operator operator, cl_operation **operations, 
	int *n_values, uint32_t *cl_gen, const cl_write_parameters *cl_w_p, uint64_t *trid, void *udata);
*/

//
// cl_batch.c
//

int
citrusleaf_batch_init();

void
citrusleaf_batch_shutdown();

int
citrusleaf_query_init();

void
citrusleaf_query_shutdown();

int
cl_compile(uint info1, uint info2, uint info3, const char *ns, const char *set, const cl_object *key, const cf_digest *digest,
	cl_bin *values, cl_operator operator, cl_operation *operations, int n_values,  
	uint8_t **buf_r, size_t *buf_sz_r, const cl_write_parameters *cl_w_p, cf_digest *d_ret, uint64_t trid, cl_scan_param_field *scan_field);

int
cl_parse(cl_msg *msg, uint8_t *buf, size_t buf_len, cl_bin **values_r, cl_operation **operations_r, int *n_values_r, uint64_t *trid);

#ifdef __cplusplus
} // end extern "C"
#endif


