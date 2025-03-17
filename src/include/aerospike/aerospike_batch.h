/*
 * Copyright 2008-2025 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
#pragma once

/**
 * @defgroup batch_operations Batch Operations
 * @ingroup client_operations
 *
 * The Batch API is a collection of APIs that use multiple keys for looking up
 * records in one call.
 */

#include <aerospike/aerospike.h>
#include <aerospike/as_batch.h>
#include <aerospike/as_listener.h>
#include <aerospike/as_error.h>
#include <aerospike/as_key.h>
#include <aerospike/as_list.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>
#include <aerospike/as_vector.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Types
//---------------------------------

#define AS_BATCH_READ 0
#define AS_BATCH_WRITE 1
#define AS_BATCH_APPLY 2
#define AS_BATCH_REMOVE 3
#define AS_BATCH_TXN_VERIFY 4
#define AS_BATCH_TXN_ROLL 5

/**
 * Batch record type. Values: AS_BATCH_READ, AS_BATCH_WRITE, AS_BATCH_APPLY or AS_BATCH_REMOVE
 *
 * @ingroup batch_operations
 */
typedef uint8_t as_batch_type;

/**
 * Batch base request/response record. Used in batch commands where different command types are
 * needed for different keys. All batch record types contain these base fields.
 *
 * @ingroup batch_operations
 */
typedef struct as_batch_base_record_s {
	/**
	 * Requested key.
	 */
	as_key key;
	
	/**
	 * Record result for the requested key. This record will only be populated when the result is
	 * AEROSPIKE_OK or AEROSPIKE_ERR_UDF. If AEROSPIKE_ERR_UDF, use as_record_get_udf_error()
	 * to obtain the error message.
	 */
	as_record record;

	/**
	 * Result code.
	 */
	as_status result;

	/**
	 * Type of batch record.
	 */
	as_batch_type type;

	/**
	 * Does batch sub-command contain a write operation.
	 */
	bool has_write;

	/**
	 * Is it possible that the write command completed even though this error was generated.
	 * This may be the case when a client error occurs (like timeout) after the command was sent
	 * to the server.
	 */
	bool in_doubt;
} as_batch_base_record;

/**
 * Batch key and read only operations with read policy.
 *
 * @relates as_batch_base_record
 * @ingroup batch_operations
 */
typedef struct as_batch_read_record_s {
	as_key key;
	as_record record;
	as_status result;
	as_batch_type type;
	bool has_write;
	bool in_doubt; // Will always be false for reads.

	/**
	 * Optional read policy.
	 */
	const as_policy_batch_read* policy;

	/**
	 * Read operations for this key. ops are mutually exclusive with bin_names.
	 * If defined, the user must call as_operations_destroy() when done with the batch.
	 */
	as_operations* ops;

	/**
	 * Bin names requested for this key. bin_names are mutually exclusive with ops.
	 * If heap defined, the user must free when done with the batch.
	 */
	char** bin_names;
	
	/**
	 * Count of bin names requested for this key.
	 */
	uint32_t n_bin_names;
	
	/**
	 * If true, ignore bin_names and read all bins.
	 * If false and bin_names are set, read specified bin_names.
	 * If false and bin_names are not set, read record header (generation, expiration) only.
	 */
	bool read_all_bins;
} as_batch_read_record;

/**
 * Batch key and read/write operations with write policy.
 *
 * All fields must be allocated on the heap (or global) when an async batch write is run under a
 * transaction. The reason is transactions require an extra async call to add write keys to the
 * transaction monitor record and this extra call causes stack variables to fall out of scope before
 * the async batch is executed.
 *
 * @relates as_batch_base_record
 * @ingroup batch_operations
 */
typedef struct as_batch_write_record_s {
	as_key key;
	as_record record;  // Contains results of operations from ops field.
	as_status result;
	as_batch_type type;
	bool has_write;
	bool in_doubt;

	/**
	 * Optional write policy.
	 */
	const as_policy_batch_write* policy;

	/**
	 * Required read/write operations for this key. The user must call as_operations_destroy()
	 * when done with the batch.
	 */
	as_operations* ops;
} as_batch_write_record;

/**
 * Batch UDF (user defined function) apply.
 *
 * All fields must be allocated on the heap (or global) when an async batch UDF is run under a
 * transaction. The reason is transactions require an extra async call to add write keys to the
 * transaction monitor record and this extra call causes stack variables to fall out of scope before
 * the async batch is executed.
 *
 * @relates as_batch_base_record
 * @ingroup batch_operations
 */
typedef struct as_batch_apply_record_s {
	as_key key;
	as_record record;
	as_status result;
	as_batch_type type;
	bool has_write;
	bool in_doubt;

	/**
	 * Optional apply policy.
	 */
	const as_policy_batch_apply* policy;

	/**
	 * Package or lua module name.
	 * If heap defined, the user must free when done with the batch.
	 */
	const char* module;

	/**
	 * Lua function name.
	 * If heap defined, the user must free when done with the batch.
	 */
	const char* function;

	/**
	 * Optional arguments to lua function.
	 * If defined, the user must call as_arraylist_destroy() when done with the batch.
	 */
	as_list* arglist;
} as_batch_apply_record;

/**
 * Batch delete operation.
 *
 * All fields must be allocated on the heap (or global) when an async batch remove is run under a
 * transaction. The reason is transactions require an extra async call to add write keys to the
 * transaction monitor record and this extra call causes stack variables to fall out of scope before
 * the async batch is executed.
 *
 * @relates as_batch_base_record
 * @ingroup batch_operations
 */
typedef struct as_batch_remove_record_s {
	as_key key;
	as_record record;
	as_status result;
	as_batch_type type;
	bool has_write;
	bool in_doubt;

	/**
	 * Optional remove policy.
	 */
	const as_policy_batch_remove* policy;
} as_batch_remove_record;

/**
 * Batch request/response record union.
 *
 * @relates as_batch_base_record
 * @ingroup batch_operations
 */
typedef union {
	as_batch_base_record base;
	as_batch_read_record read;
	as_batch_write_record write;
	as_batch_apply_record apply;
	as_batch_remove_record remove;
} as_batch_record;

/**
 * List of batch request/response (as_batch_base_record) records. The record types can be
 * as_batch_read_record, as_batch_write_record, as_batch_apply_record or as_batch_remove_record.
 *
 * @ingroup batch_operations
 */
typedef struct as_batch_records_s {
	as_vector list;
} as_batch_records;

/**
 * List of batch request/response (as_batch_base_record) records. The record types can be
 * as_batch_read_record, as_batch_write_record, as_batch_apply_record or as_batch_remove_record.
 *
 * @deprecated Use as_batch_records instead.
 * @ingroup batch_operations
 */
typedef as_batch_records as_batch_read_records;

/**
 * This listener will be called with the results of batch commands for all keys.
 *
 * The `results` argument will be an array of `n` as_batch_result entries. The
 * `results` argument is on the stack and is only available within the context
 * of the listener. To use the data outside of the listener, copy the data.
 *
 * ~~~~~~~~~~{.c}
 * bool my_listener(const as_batch_result* results, uint32_t n, void* udata) {
 *     return true;
 * }
 * ~~~~~~~~~~
 *
 * @param results		The results from the batch request.
 * @param n				The number of results from the batch request.
 * @param udata 		User-data provided to the calling function.
 * @return `true` on success. Otherwise, an error occurred.
 * @ingroup batch_operations
 */
typedef bool (*as_batch_listener)(const as_batch_result* results, uint32_t n, void* udata);

/**
 * This listener will be called with the results of batch commands for all keys.
 *
 * @deprecated Use as_batch_listener instead.
 * @ingroup batch_operations
 */
typedef as_batch_listener aerospike_batch_read_callback;

/**
 * Asynchronous batch user listener.  This function is called once when the batch completes or an
 * error has occurred.
 *
 * @param err			Error structure that is populated if an error occurs. NULL on success.
 * @param records		Record results. Records must be destroyed with as_batch_records_destroy()
 *						when done.
 * @param udata 		User data that is forwarded from asynchronous command function.
 * @param event_loop	Event loop that this command was executed on. Use this event loop when
 *						running nested asynchronous commands when single threaded behavior is
 *						desired for the group of commands.
 * @ingroup batch_operations
 */
typedef void (*as_async_batch_listener)(as_error* err, as_batch_records* records, void* udata,
	as_event_loop* event_loop);

//---------------------------------
// Functions
//---------------------------------

/**
 * Initialize batch records with specified capacity on the stack using alloca().
 *
 * When the batch is no longer needed, then use as_batch_records_destroy() to
 * release the batch and associated resources.
 *
 * @param __records		Batch record list.
 * @param __capacity	Initial capacity of batch record list. List will resize when necessary.
 *
 * @relates as_batch_records
 * @ingroup batch_operations
 */
#define as_batch_records_inita(__records, __capacity) \
	as_vector_inita(&((__records)->list), sizeof(as_batch_record), __capacity);

/**
 * Initialize batch records with specified capacity on the stack using alloca().
 *
 * @deprecated Use as_batch_records_inita() instead.
 * @relates as_batch_records
 * @ingroup batch_operations
 */
#define as_batch_read_inita(__records, __capacity) \
	as_vector_inita(&((__records)->list), sizeof(as_batch_record), __capacity);

/**
 * Initialize batch records with specified capacity on the heap.
 *
 * When the batch is no longer needed, then use as_batch_records_destroy() to
 * release the batch and associated resources.
 *
 * @param records	Batch record list.
 * @param capacity	Initial capacity of batch record list. List will resize when necessary.
 *
 * @relates as_batch_records
 * @ingroup batch_operations
 */
static inline void
as_batch_records_init(as_batch_records* records, uint32_t capacity)
{
	as_vector_init(&records->list, sizeof(as_batch_record), capacity);
}

/**
 * Initialize batch records with specified capacity on the heap.
 *
 * @deprecated Use as_batch_records_init() instead.
 * @relates as_batch_records
 * @ingroup batch_operations
 */
static inline void
as_batch_read_init(as_batch_records* records, uint32_t capacity)
{
	as_vector_init(&records->list, sizeof(as_batch_record), capacity);
}

/**
 * Create batch records on heap with specified list capacity on the heap.
 *
 * When the batch is no longer needed, then use as_batch_records_destroy() to
 * release the batch and associated resources.
 *
 * @param capacity	Initial capacity of batch record list. List will resize when necessary.
 * @return			Batch record list.
 *
 * @relates as_batch_records
 * @ingroup batch_operations
 */
static inline as_batch_records*
as_batch_records_create(uint32_t capacity)
{
	return (as_batch_records*)as_vector_create(sizeof(as_batch_record), capacity);
}

/**
 * Create batch records on heap with specified list capacity on the heap.
 *
 * @deprecated Use as_batch_records_create() instead.
 * @relates as_batch_records
 * @ingroup batch_operations
 */
static inline as_batch_records*
as_batch_read_create(uint32_t capacity)
{
	return (as_batch_records*)as_vector_create(sizeof(as_batch_record), capacity);
}

/**
 * Reserve a new `as_batch_read_record` slot. Capacity will be increased when necessary.
 * Return reference to record. The record is initialized to zeroes.
 *
 * @relates as_batch_records
 * @ingroup batch_operations
 */
static inline as_batch_read_record*
as_batch_read_reserve(as_batch_records* records)
{
	as_batch_read_record* r = (as_batch_read_record*)as_vector_reserve(&records->list);
	r->type = AS_BATCH_READ;
	return r;
}

/**
 * Reserve a new `as_batch_write_record` slot. Capacity will be increased when necessary.
 * Return reference to record. The record is initialized to zeroes.
 *
 * @relates as_batch_records
 * @ingroup batch_operations
 */
static inline as_batch_write_record*
as_batch_write_reserve(as_batch_records* records)
{
	as_batch_write_record* r = (as_batch_write_record*)as_vector_reserve(&records->list);
	r->type = AS_BATCH_WRITE;
	r->has_write = true;
	return r;
}

/**
 * Reserve a new `as_batch_apply_record` slot for UDF. Capacity will be increased when necessary.
 * Return reference to record. The record is initialized to zeroes.
 *
 * @relates as_batch_records
 * @ingroup batch_operations
 */
static inline as_batch_apply_record*
as_batch_apply_reserve(as_batch_records* records)
{
	as_batch_apply_record* r = (as_batch_apply_record*)as_vector_reserve(&records->list);
	r->type = AS_BATCH_APPLY;
	r->has_write = true;
	return r;
}

/**
 * Reserve a new `as_batch_remove_record` slot. Capacity will be increased when necessary.
 * Return reference to record. The record is initialized to zeroes.
 *
 * @relates as_batch_records
 * @ingroup batch_operations
 */
static inline as_batch_remove_record*
as_batch_remove_reserve(as_batch_records* records)
{
	as_batch_remove_record* r = (as_batch_remove_record*)as_vector_reserve(&records->list);
	r->type = AS_BATCH_REMOVE;
	r->has_write = true;
	return r;
}

/**
 * Destroy keys and records in record list. It's the responsility of the caller to
 * free additional user specified fields in the record.
 *
 * @relates as_batch_records
 * @ingroup batch_operations
 */
AS_EXTERN void
as_batch_records_destroy(as_batch_records* records);

/**
 * Destroy keys and records in record list. It's the responsility of the caller to
 * free additional user specified fields in the record.
 *
 * @deprecated Use as_batch_records_destroy() instead.
 * @relates as_batch_records
 * @ingroup batch_operations
 */
static inline void
as_batch_read_destroy(as_batch_records* records)
{
	as_batch_records_destroy(records);
}

/**
 * Read multiple records for specified batch keys in one batch call.
 * This method allows different namespaces/bins to be requested for each key in the batch.
 * The returned records are located in the same batch array.
 *
 * ~~~~~~~~~~{.c}
 * as_batch_records records;
 * as_batch_records_inita(&records, 10);
 *
 * char* bin_names[] = {"bin1", "bin2"};
 * char* ns = "ns";
 * char* set = "set";
 *
 * as_batch_read_record* record = as_batch_read_reserve(&records);
 * as_key_init(&record->key, ns, set, "key1");
 * record->bin_names = bin_names;
 * record->n_bin_names = 2;
 *
 * record = as_batch_read_reserve(&records);
 * as_key_init(&record->key, ns, set, "key2");
 * record->read_all_bins = true;
 *
 * as_status status = aerospike_batch_read(as, &err, NULL, &records);
 * // process results
 * as_batch_records_destroy(&records);
 * ~~~~~~~~~~
 *
 * @param as		Aerospike cluster instance.
 * @param err		Error detail structure that is populated if an error occurs.
 * @param policy	Batch policy configuration parameters, pass in NULL for default.
 * @param records	List of keys and records to retrieve.
 * 					The returned records are located in the same array.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error.
 * @ingroup batch_operations
 */
AS_EXTERN as_status
aerospike_batch_read(
	aerospike* as, as_error* err, const as_policy_batch* policy, as_batch_records* records
	);

/**
 * Asynchronously read multiple records for specified batch keys in one batch call.
 * This method allows different namespaces/bins to be requested for each key in the batch.
 * The returned records are located in the same batch array.
 *
 * ~~~~~~~~~~{.c}
 * void my_listener(as_error* err, as_batch_records* records, void* udata, as_event_loop* loop)
 * {
 * 	   if (err) {
 * 	       fprintf(stderr, "Command failed: %d %s\n", err->code, err->message);
 * 	   }
 * 	   else {
 * 	       as_vector* list = &records->list;
 * 	       for (uint32_t i = 0; i < list->size; i++) {
 * 	           as_batch_read_record* record = as_vector_get(list, i);
 * 		       // Process record
 * 	       }
 *     }
 * 	   // Must free batch records on both success and error conditions because it was created
 * 	   // before calling aerospike_batch_read_async().
 * 	   as_batch_records_destroy(records);
 * }
 *
 * as_batch_records* records = as_batch_records_create(10);
 *
 * // bin_names must point to a static/global array of literal/global strings.
 * char* bin_names[] = {"bin1", "bin2"};
 * char* ns = "ns";
 * char* set = "set";
 *
 * as_batch_read_record* record = as_batch_read_reserve(records);
 * as_key_init(&record->key, ns, set, "key1");
 * record->bin_names = bin_names;
 * record->n_bin_names = 2;
 *
 * record = as_batch_read_reserve(records);
 * as_key_init(&record->key, ns, set, "key2");
 * record->read_all_bins = true;
 *
 * as_status status = aerospike_batch_read_async(as, &err, NULL, records, NULL, my_listener, NULL);
 *
 * if (status != AEROSPIKE_OK) {
 * 	   // Must free batch records on queue error because the listener will not be called.
 * 	   as_batch_records_destroy(records);
 * }
 * ~~~~~~~~~~
 *
 * @param as			Aerospike cluster instance.
 * @param err			Error detail structure that is populated if an error occurs.
 * @param policy		Batch policy configuration parameters, pass in NULL for default.
 * @param records		List of keys and records to retrieve. Returned records are located in the
 *						same list. Must create using as_batch_records_create() (allocates memory on
 *						heap) because the async method returns immediately after queueing command.
 * @param listener 		User function to be called with command results.
 * @param udata 		User data to be forwarded to listener.
 * @param event_loop 	Event loop assigned to run this command. If NULL, an event loop will be
 *						chosen by round-robin.
 *
 * @return AEROSPIKE_OK if async command succesfully queued. Otherwise an error.
 * @ingroup batch_operations
 */
AS_EXTERN as_status
aerospike_batch_read_async(
	aerospike* as, as_error* err, const as_policy_batch* policy, as_batch_records* records,
	as_async_batch_listener listener, void* udata, as_event_loop* event_loop
	);

/**
 * Read/Write multiple records for specified batch keys in one batch call.
 * This method allows different sub-commands for each key in the batch.
 * The returned records are located in the same list.
 *
 * Requires server version 6.0+
 *
 * ~~~~~~~~~~{.c}
 * as_operations ops1;
 * as_operations_inita(&ops1, 1);
 * as_operations_add_write_int64(&ops1, "bin1", 100);
 *
 * as_operations ops2;
 * as_operations_inita(&ops2, 1);
 * as_operations_add_write_int64(&ops2, "bin2", 200);
 *
 * as_batch_records recs;
 * as_batch_records_inita(&recs, 2);
 *
 * as_batch_write_record* r = as_batch_write_reserve(&recs);
 * as_key_init_int64(&r->key, "test", "set", 1);
 * r->ops = &ops1;
 *
 * r = as_batch_write_reserve(&recs);
 * as_key_init_int64(&r->key, "test", "set", 2);
 * r->ops = &ops2;
 * 
 * as_status status = aerospike_batch_write(as, err, NULL, &recs);
 *
 * // Process results. Overall status contains first error, if any.
 * as_operations_destroy(&ops1);
 * as_operations_destroy(&ops2);
 * as_batch_records_destroy(&recs);
 * ~~~~~~~~~~
 *
 * @param as		Aerospike cluster instance.
 * @param err		Error detail structure that is populated if an error occurs.
 * @param policy	Batch policy configuration parameters, pass in NULL for default.
 * @param records	List of batch sub-commands to perform. The returned records are located in the
 *					same list.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error.
 * @ingroup batch_operations
 */
AS_EXTERN as_status
aerospike_batch_write(
	aerospike* as, as_error* err, const as_policy_batch* policy, as_batch_records* records
	);

typedef struct {
	as_operations* ops1;
	as_operations* ops2;
} heap_fields;

/**
 * Asynchronously read/write multiple records for specified batch keys in one batch call.
 * This method allows different sub-commands for each key in the batch.
 * The returned records are located in the same list.
 *
 * All as_batch_record pointer fields must be allocated on the heap (or global) when an async batch
 * write is run under a transaction. The reason is transactions require an extra async call to add
 * write keys to the transaction monitor record and this extra call causes stack variables to fall out
 * of scope before the async batch is executed.
 *
 * Requires server version 6.0+
 *
 * ~~~~~~~~~~{.c}
 * typedef struct {
 *     as_operations* ops1;
 *     as_operations* ops2;
 * } heap_fields;
 *
 * void my_listener(as_error* err, as_batch_records* records, void* udata, as_event_loop* loop)
 * {
 *     if (err) {
 *         fprintf(stderr, "Command failed: %d %s\n", err->code, err->message);
 *     }
 *     else {
 *         as_vector* list = &records->list;
 *         for (uint32_t i = 0; i < list->size; i++) {
 *             as_batch_base_record* r = as_vector_get(list, i);
 *             // Process record
 *         }
 *     }
 *
 *     // Must free batch records on both success and error conditions because it was created
 *     // before calling aerospike_batch_read_async().
 *     as_batch_records_destroy(records);
 *
 *     heap_fields* hf = udata;
 *     as_operations_destroy(hf->ops1);
 *     as_operations_destroy(hf->ops2);
 *     free(hf);
 * }
 *
 * as_operations* ops1 = as_operations_new(2);
 * as_operations_add_write_int64(ops1, bin1, 100);
 * as_operations_add_read(ops1, bin2);
 *
 * as_operations* ops2 = as_operations_new(2);
 * as_operations_add_write_int64(ops2, bin3, 0);
 * as_operations_add_read(ops2, bin4);
 *
 * as_batch_records* recs = as_batch_records_create(2);
 *
 * as_batch_write_record* wr = as_batch_write_reserve(recs);
 * as_key_init_int64(&wr->key, NAMESPACE, SET, 1);
 * wr->ops = ops1;
 *
 * wr = as_batch_write_reserve(recs);
 * as_key_init_int64(&wr->key, NAMESPACE, SET, 6);
 * wr->ops = ops2;
 *
 * heap_fields* hf = malloc(sizeof(heap_fields));
 * hf->ops1 = ops1;
 * hf->ops2 = ops2;
 *
 * as_status status = aerospike_batch_write_async(as, &err, NULL, recs, my_listener, hf, NULL);
 *
 * if (status != AEROSPIKE_OK) {
 *    // Must free batch records on queue error because the listener will not be called.
 *    as_batch_records_destroy(records);
 *    as_operations_destroy(ops1);
 *    as_operations_destroy(ops2);
 *    free(hf);
 * }
 * ~~~~~~~~~~
 *
 * @param as			Aerospike cluster instance.
 * @param err			Error detail structure that is populated if an error occurs.
 * @param policy		Batch policy configuration parameters, pass in NULL for default.
 * @param records		List of keys and records to retrieve. Returned records are located in the
 *						same list. Must create using as_batch_records_create() (allocates memory on
 *						heap) because the async method returns immediately after queueing command.
 * @param listener 		User function to be called with command results.
 * @param udata 		User data to be forwarded to listener.
 * @param event_loop 	Event loop assigned to run this command. If NULL, an event loop will be
 *						chosen by round-robin.
 *
 * @return AEROSPIKE_OK if async command succesfully queued. Otherwise an error.
 * @ingroup batch_operations
 */
AS_EXTERN as_status
aerospike_batch_write_async(
	aerospike* as, as_error* err, const as_policy_batch* policy, as_batch_records* records,
	as_async_batch_listener listener, void* udata, as_event_loop* event_loop
	);

/**
 * Look up multiple records by key, then return all bins.
 *
 * ~~~~~~~~~~{.c}
 * as_batch batch;
 * as_batch_inita(&batch, 3);
 * 
 * as_key_init(as_batch_keyat(&batch,0), "ns", "set", "key1");
 * as_key_init(as_batch_keyat(&batch,1), "ns", "set", "key2");
 * as_key_init(as_batch_keyat(&batch,2), "ns", "set", "key3");
 * 
 * as_status status = aerospike_batch_get(as, &err, NULL, &batch, listener, NULL);
 * // process results
 * as_batch_destroy(&batch);
 * ~~~~~~~~~~
 *
 * @param as			Aerospike cluster instance.
 * @param err			Error detail structure that is populated if an error occurs.
 * @param policy		Batch policy configuration parameters, pass in NULL for default.
 * @param batch			List of keys.
 * @param listener		User function to be called with command results.
 * @param udata 		User data to be forwarded to listener.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error.
 * @ingroup batch_operations
 */
AS_EXTERN as_status
aerospike_batch_get(
	aerospike* as, as_error* err, const as_policy_batch* policy, const as_batch* batch,
	as_batch_listener listener, void* udata
	);

/**
 * Look up multiple records by key, then return specified bins.
 *
 * ~~~~~~~~~~{.c}
 * as_batch batch;
 * as_batch_inita(&batch, 3);
 *
 * as_key_init(as_batch_keyat(&batch,0), "ns", "set", "key1");
 * as_key_init(as_batch_keyat(&batch,1), "ns", "set", "key2");
 * as_key_init(as_batch_keyat(&batch,2), "ns", "set", "key3");
 *
 * const char* bin_filters[] = {"bin1", "bin2"};
 *
 * as_status status = aerospike_batch_get_bins(as, &err, NULL, &batch, bin_filters, 2, listener, NULL);
 * // process results
 * as_batch_destroy(&batch);
 * ~~~~~~~~~~
 *
 * @param as			Aerospike cluster instance.
 * @param err			Error detail structure that is populated if an error occurs.
 * @param policy		Batch policy configuration parameters, pass in NULL for default.
 * @param batch			The batch of keys to read.
 * @param bins			Bin filters.  Only return these bins.
 * @param n_bins		The number of bin filters.
 * @param listener 		User function to be called with command results.
 * @param udata 		User data to be forwarded to listener.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error.
 * @ingroup batch_operations
 */
AS_EXTERN as_status
aerospike_batch_get_bins(
	aerospike* as, as_error* err, const as_policy_batch* policy, const as_batch* batch,
	const char** bins, uint32_t n_bins, as_batch_listener listener, void* udata
	);

/**
 * Look up multiple records by key, then return results from specified read operations.
 *
 * ~~~~~~~~~~{.c}
 * as_batch batch;
 * as_batch_inita(&batch, 3);
 *
 * as_key_init(as_batch_keyat(&batch,0), "ns", "set", "key1");
 * as_key_init(as_batch_keyat(&batch,1), "ns", "set", "key2");
 * as_key_init(as_batch_keyat(&batch,2), "ns", "set", "key3");
 *
 * as_operations ops;
 * as_operations_inita(&ops, 1);
 * as_operations_list_size(&ops, "list", NULL);
 *
 * as_status status = aerospike_batch_get_ops(as, &err, NULL, &batch, &ops, listener, NULL);
 * // process results
 * as_batch_destroy(&batch);
 * as_operations_destroy(&ops);
 * ~~~~~~~~~~
 *
 * @param as			Aerospike cluster instance.
 * @param err			Error detail structure that is populated if an error occurs.
 * @param policy		Batch policy configuration parameters, pass in NULL for default.
 * @param batch			The batch of keys to read.
 * @param ops			Read operations.
 * @param listener 		User function to be called with command results.
 * @param udata 		User data to be forwarded to listener.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error.
 * @ingroup batch_operations
 */
AS_EXTERN as_status
aerospike_batch_get_ops(
	aerospike* as, as_error* err, const as_policy_batch* policy, const as_batch* batch,
	as_operations* ops, as_batch_listener listener, void* udata
	);

/**
 * Test whether multiple records exist in the cluster.
 *
 * ~~~~~~~~~~{.c}
 * as_batch batch;
 * as_batch_inita(&batch, 3);
 * 
 * as_key_init(as_batch_keyat(&batch,0), "ns", "set", "key1");
 * as_key_init(as_batch_keyat(&batch,1), "ns", "set", "key2");
 * as_key_init(as_batch_keyat(&batch,2), "ns", "set", "key3");
 * 
 * as_status status = aerospike_batch_exists(as, &err, NULL, &batch, listener, NULL);
 * // process results
 * as_batch_destroy(&batch);
 * ~~~~~~~~~~
 *
 * @param as			Aerospike cluster instance.
 * @param err			Error detail structure that is populated if an error occurs.
 * @param policy		Batch policy configuration parameters, pass in NULL for default.
 * @param batch			The batch of keys to read.
 * @param listener 		The listener to invoke for each record read.
 * @param udata			The user-data for the listener.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error.
 * @ingroup batch_operations
 */
AS_EXTERN as_status
aerospike_batch_exists(
	aerospike* as, as_error* err, const as_policy_batch* policy, const as_batch* batch,
	as_batch_listener listener, void* udata
	);

/**
 * Perform read/write operations on multiple keys.
 * Requires server version 6.0+
 *
 * ~~~~~~~~~~{.c}
 * as_integer val;
 * as_integer_init(&val, 100);
 *
 * as_operations ops;
 * as_operations_inita(&ops, 3);
 * as_operations_list_append(&ops, bin, NULL, NULL, (as_val*)&val);
 * as_operations_list_size(&ops, bin, NULL);
 * as_operations_list_get_by_index(&ops, bin, NULL, -1, AS_LIST_RETURN_VALUE);
 *
 * as_batch batch;
 * as_batch_inita(&batch, 3);
 * 
 * as_key_init(as_batch_keyat(&batch,0), "ns", "set", "key1");
 * as_key_init(as_batch_keyat(&batch,1), "ns", "set", "key2");
 * as_key_init(as_batch_keyat(&batch,2), "ns", "set", "key3");
 *
 * as_status status = aerospike_batch_operate(as, &err, NULL, NULL, &batch, &ops, listener, NULL);
 * // process results
 * as_operations_destroy(&ops);
 * as_batch_destroy(&batch);
 * ~~~~~~~~~~
 *
 * @param as			Aerospike cluster instance.
 * @param err			Error detail structure that is populated if an error occurs.
 * @param policy		Batch policy configuration parameters, pass in NULL for default.
 * @param policy_write	Write policy configuration parameters, pass in NULL for default.
 * @param batch			List of keys.
 * @param ops			Read/Write operations.
 * @param listener		User function to be called with command results.
 * @param udata 		User data to be forwarded to listener.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error.
 * @ingroup batch_operations
 */
AS_EXTERN as_status
aerospike_batch_operate(
	aerospike* as, as_error* err, const as_policy_batch* policy,
	const as_policy_batch_write* policy_write, const as_batch* batch,
	as_operations* ops, as_batch_listener listener, void* udata
	);

/**
 * Apply UDF (user defined function) on multiple keys.
 * Requires server version 6.0+
 *
 * ~~~~~~~~~~{.c}
 * as_arraylist args;
 * as_arraylist_init(&args, 2, 0);
 * as_arraylist_append_str(&args, "s1");
 * as_arraylist_append_str(&args, "s2");
 *
 * as_batch batch;
 * as_batch_inita(&batch, 3);
 * 
 * as_key_init(as_batch_keyat(&batch,0), "ns", "set", "key1");
 * as_key_init(as_batch_keyat(&batch,1), "ns", "set", "key2");
 * as_key_init(as_batch_keyat(&batch,2), "ns", "set", "key3");
 *
 * as_status status = aerospike_batch_apply(as, &err, NULL, NULL, &batch, "mod", "func",
 *     (as_list*)&args, NULL, NULL);
 *
 * // process results
 * as_arraylist_destroy(&args);
 * as_operations_destroy(&ops);
 * as_batch_destroy(&batch);
 * ~~~~~~~~~~
 *
 * @param as			Aerospike cluster instance.
 * @param err			Error detail structure that is populated if an error occurs.
 * @param policy		Batch policy configuration parameters, pass in NULL for default.
 * @param policy_apply	UDF policy configuration parameters, pass in NULL for default.
 * @param batch			List of keys.
 * @param module		Server package name.
 * @param function		Server user defined function.
 * @param arglist		Server user defined function arguments.
 * @param listener		User function to be called with command results.
 * @param udata 		User data to be forwarded to listener.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error.
 * @ingroup batch_operations
 */
AS_EXTERN as_status
aerospike_batch_apply(
	aerospike* as, as_error* err, const as_policy_batch* policy,
	const as_policy_batch_apply* policy_apply, const as_batch* batch,
	const char* module, const char* function, as_list* arglist,
	as_batch_listener listener, void* udata
	);

/**
 * Remove multiple records.
 * Requires server version 6.0+
 *
 * ~~~~~~~~~~{.c}
 * as_batch batch;
 * as_batch_inita(&batch, 3);
 * 
 * as_key_init(as_batch_keyat(&batch,0), "ns", "set", "key1");
 * as_key_init(as_batch_keyat(&batch,1), "ns", "set", "key2");
 * as_key_init(as_batch_keyat(&batch,2), "ns", "set", "key3");
 *
 * as_status status = aerospike_batch_remove(as, &err, NULL, NULL, &batch, listener, NULL);
 * // process results
 * as_batch_destroy(&batch);
 * ~~~~~~~~~~
 *
 * @param as			Aerospike cluster instance.
 * @param err			Error detail structure that is populated if an error occurs.
 * @param policy		Batch policy configuration parameters, pass in NULL for default.
 * @param policy_remove	Remove policy configuration parameters, pass in NULL for default.
 * @param batch			List of keys.
 * @param listener		User function to be called with command results.
 * @param udata 		User data to be forwarded to listener.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error.
 * @ingroup batch_operations
 */
AS_EXTERN as_status
aerospike_batch_remove(
	aerospike* as, as_error* err, const as_policy_batch* policy,
	const as_policy_batch_remove* policy_remove, const as_batch* batch,
	as_batch_listener listener, void* udata
	);

#ifdef __cplusplus
} // end extern "C"
#endif
