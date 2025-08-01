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
 * @defgroup key_operations Key Operations
 * @ingroup client_operations
 *
 * Aerospike provides a key based API to access and modify data into the 
 * cluster. 
 *
 * The Key API is a collection of APIs that use as_key as for looking up
 * records for accessing and modifying in the cluster. 
 * 
 */

#include <aerospike/aerospike.h>
#include <aerospike/as_listener.h>
#include <aerospike/as_error.h>
#include <aerospike/as_key.h>
#include <aerospike/as_list.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Functions
//---------------------------------

/**
 * Look up a record by key and return all bins.
 *
 * @code
 * as_key key;
 * as_key_init(&key, "ns", "set", "key");
 * 
 * as_record* rec = NULL;
 * if (aerospike_key_get(&as, &err, NULL, &key, &rec) != AEROSPIKE_OK) {
 *     printf("error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 * }
 * else {
 * 	   as_record_destroy(rec);
 * }
 * @endcode
 *
 * @param as			The aerospike instance to use for this operation.
 * @param err			The as_error to be populated if an error occurs.
 * @param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 * @param key			The key of the record.
 * @param rec 			The record to be populated with the data from request. If the record pointer is
 *						preset to NULL, the record will be created and initialized. If the record pointer
 *						is not NULL, the record is assumed to be valid and will be reused. Either way,
 *						the record must be preset.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error.
 *
 * @ingroup key_operations
 */
AS_EXTERN as_status
aerospike_key_get(
	aerospike* as, as_error* err, const as_policy_read* policy, const as_key* key, as_record** rec
	);

/**
 * Asynchronously look up a record by key and return all bins.
 *
 * @code
 * void my_listener(as_error* err, as_record* record, void* udata, as_event_loop* event_loop)
 * {
 *     if (err) {
 *         printf("Command failed: %d %s\n", err->code, err->message);
 *         return;
 * 	   }
 * 	   // Process record bins
 * 	   // Only call as_record_destroy(record) when command is successful (err == NULL) and 
 *     // "as_policy_read.async_heap_rec" is true. Otherwise, the calling function will destroy the
 *     // record when the listener completes.
 * }
 *
 * as_key key;
 * as_key_init(&key, "ns", "set", "key");
 *
 * as_status status = aerospike_key_get_async(&as, &err, NULL, &key, my_listener, NULL, NULL, NULL);
 * @endcode
 *
 * @param as				The aerospike instance to use for this operation.
 * @param err				The as_error to be populated if an error occurs.
 * @param policy			The policy to use for this operation. If NULL, then the default policy will be used.
 * @param key				The key of the record.
 * @param listener			User function to be called with command results.
 * @param udata 			User data to be forwarded to user callback.
 * @param event_loop 		Event loop assigned to run this command. If NULL, an event loop will be chosen by round-robin.
 * @param pipe_listener		Enables command pipelining, if not NULL. The given callback is invoked after the current command
 * 							has been sent to the server. This allows for issuing the next command even before receiving a
 * 							result for the current command.
 *
 * @return AEROSPIKE_OK if async command successfully queued. Otherwise an error.
 *
 * @ingroup key_operations
 */
AS_EXTERN as_status
aerospike_key_get_async(
	aerospike* as, as_error* err, const as_policy_read* policy, const as_key* key,
	as_async_record_listener listener, void* udata, as_event_loop* event_loop,
	as_pipe_listener pipe_listener
	);

/**
 * Read a record's bins given the NULL terminated bins array argument.
 *
 * @code
 * char* select[] = {"bin1", "bin2", "bin3", NULL};
 * 
 * as_key key;
 * as_key_init(&key, "ns", "set", "key");
 * 
 * as_record* rec = NULL;
 * if (aerospike_key_select(&as, &err, NULL, &key, select, &rec) != AEROSPIKE_OK) {
 * 	   printf("error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 * }
 * else {
 *     as_record_destroy(rec);
 * }
 * @endcode
 *
 * @param as			The aerospike instance to use for this operation.
 * @param err			The as_error to be populated if an error occurs.
 * @param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 * @param key			The key of the record.
 * @param bins			The bins to select. A NULL terminated array of NULL terminated strings.
 * @param rec 			The record to be populated with the data from request. If the record pointer is
 *						preset to NULL, the record will be created and initialized. If the record pointer
 *						is not NULL, the record is assumed to be valid and will be reused. Either way,
 *						the record must be preset.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error.
 *
 * @ingroup key_operations
 */
AS_EXTERN as_status
aerospike_key_select(
	aerospike* as, as_error* err, const as_policy_read* policy, const as_key* key,
	const char* bins[], as_record** rec
	);

/**
 * Asynchronously read a record's bins given the NULL terminated bins array argument.
 *
 * @code
 * void my_listener(as_error* err, as_record* record, void* udata, as_event_loop* event_loop)
 * {
 *     if (err) {
 * 	       printf("Command failed: %d %s\n", err->code, err->message);
 * 	       return;
 * 	   }
 * 	   // Process record bins
 * 	   // Only call as_record_destroy(record) when command is successful (err == NULL) and 
 *     // "as_policy_read.async_heap_rec" is true. Otherwise, the calling function will destroy the
 *     // record when the listener completes.
 * }
 *
 * char* select[] = {"bin1", "bin2", "bin3", NULL};
 *
 * as_key key;
 * as_key_init(&key, "ns", "set", "key");
 *
 * as_status status = aerospike_key_select_async(&as, &err, NULL, &key, select, my_listener, NULL, NULL, NULL);
 * @endcode
 *
 * @param as				The aerospike instance to use for this operation.
 * @param err				The as_error to be populated if an error occurs.
 * @param policy			The policy to use for this operation. If NULL, then the default policy will be used.
 * @param key				The key of the record.
 * @param bins				The bins to select. A NULL terminated array of NULL terminated strings.
 * @param listener			User function to be called with command results.
 * @param udata				User data to be forwarded to user callback.
 * @param event_loop		Event loop assigned to run this command. If NULL, an event loop will be chosen by round-robin.
 * @param pipe_listener		Enables command pipelining, if not NULL. The given callback is invoked after the current command
 * 							has been sent to the server. This allows for issuing the next command even before receiving a
 * 							result for the current command.
 *
 * @return AEROSPIKE_OK if async command successfully queued. Otherwise an error.
 *
 * @ingroup key_operations
 */
AS_EXTERN as_status
aerospike_key_select_async(
	aerospike* as, as_error* err, const as_policy_read* policy, const as_key* key, const char* bins[],
	as_async_record_listener listener, void* udata, as_event_loop* event_loop, as_pipe_listener pipe_listener
	);

/**
 * Read a record's bins given the bins array argument and the n_bins count.
 *
 * @code
 * char* select[] = {"bin1", "bin2", "bin3"};
 * 
 * as_key key;
 * as_key_init(&key, "ns", "set", "key");
 * 
 * as_record* rec = NULL;
 * if (aerospike_key_select_bins(&as, &err, NULL, &key, select, 3, &rec) != AEROSPIKE_OK) {
 * 	   printf("error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 * }
 * else {
 *     as_record_destroy(rec);
 * }
 * @endcode
 *
 * @param as			The aerospike instance to use for this operation.
 * @param err			The as_error to be populated if an error occurs.
 * @param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 * @param key			The key of the record.
 * @param bins			The array of bins to select. The array does not need a final NULL entry.
 * @param n_bins		The count of bins to select.
 * @param rec 			The record to be populated with the data from request. If the record pointer is
 *						preset to NULL, the record will be created and initialized. If the record pointer
 *						is not NULL, the record is assumed to be valid and will be reused. Either way,
 *						the record must be preset.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error.
 *
 * @ingroup key_operations
 */
as_status
aerospike_key_select_bins(
	aerospike* as, as_error* err, const as_policy_read* policy, const as_key* key,
	const char* bins[], uint32_t n_bins, as_record** rec
	);

/**
 * Asynchronously read a record's bins given the bins array argument and the n_bins count.
 *
 * @code
 * void my_listener(as_error* err, as_record* record, void* udata, as_event_loop* event_loop)
 * {
 *     if (err) {
 * 	       printf("Command failed: %d %s\n", err->code, err->message);
 * 	       return;
 * 	   }
 * 	   // Process record bins
 * 	   // Only call as_record_destroy(record) when command is successful (err == NULL) and 
 *     // "as_policy_read.async_heap_rec" is true. Otherwise, the calling function will destroy the
 *     // record when the listener completes.
 * }
 *
 * char* select[] = {"bin1", "bin2", "bin3"};
 *
 * as_key key;
 * as_key_init(&key, "ns", "set", "key");
 *
 * as_status status = aerospike_key_select_bins_async(&as, &err, NULL, &key, select, 3, my_listener, NULL, NULL, NULL);
 * @endcode
 *
 * @param as				The aerospike instance to use for this operation.
 * @param err				The as_error to be populated if an error occurs.
 * @param policy			The policy to use for this operation. If NULL, then the default policy will be used.
 * @param key				The key of the record.
 * @param bins				The bins to select. A NULL terminated array of NULL terminated strings.
 * @param n_bins			The count of bins to select.
 * @param listener			User function to be called with command results.
 * @param udata				User data to be forwarded to user callback.
 * @param event_loop		Event loop assigned to run this command. If NULL, an event loop will be chosen by round-robin.
 * @param pipe_listener		Enables command pipelining, if not NULL. The given callback is invoked after the current command
 * 							has been sent to the server. This allows for issuing the next command even before receiving a
 * 							result for the current command.
 *
 * @return AEROSPIKE_OK if async command successfully queued. Otherwise an error.
 *
 * @ingroup key_operations
 */
as_status
aerospike_key_select_bins_async(
	aerospike* as, as_error* err, const as_policy_read* policy, const as_key* key, const char* bins[],
	uint32_t n_bins, as_async_record_listener listener, void* udata, as_event_loop* event_loop,
	as_pipe_listener pipe_listener
	);

/**
 * Check if a record exists in the cluster via its key. The record's metadata 
 * will be populated if the record exists.
 *
 * @code
 * as_key key;
 * as_key_init(&key, "ns", "set", "key");
 * 
 * as_record* rec = NULL;
 * if (aerospike_key_exists(&as, &err, NULL, &key, &rec) != AEROSPIKE_OK) {
 *     printf("error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 * }
 * else {
 * 	   if (rec) {
 * 	       printf("Record exists.");
 * 	       as_record_destroy(rec);
 *     }
 * 	   else {
 * 	       printf("Record doesn't exist.");
 * 	   }
 * }
 * @endcode
 *
 * @param as			The aerospike instance to use for this operation.
 * @param err			The as_error to be populated if an error occurs.
 * @param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 * @param key			The key of the record.
 * @param rec    		The metadata will be populated if the record exists. If the record pointer is
 *						preset to NULL, the record will be created and initialized. If the record pointer
 *						is not NULL, the record is assumed to be valid and will be reused. Either way,
 *						the record must be preset.
 *
 * @return AEROSPIKE_OK if successful. AEROSPIKE_ERR_RECORD_NOT_FOUND if not found. Otherwise an error.
 *
 * @ingroup key_operations
 */
AS_EXTERN as_status
aerospike_key_exists(
	aerospike* as, as_error* err, const as_policy_read* policy, const as_key* key, as_record** rec
	);

/**
 * Asynchronously check if a record exists in the cluster via its key. The record's metadata
 * will be populated if the record exists.
 *
 * @code
 * void my_listener(as_error* err, as_record* record, void* udata, as_event_loop* event_loop)
 * {
 * 	   if (err) {
 * 	       printf("Command failed: %d %s\n", err->code, err->message);
 * 	       return;
 * 	   }
 * 	   if (record) {
 * 	       printf("Record exists.");
 * 	       // Only call as_record_destroy(record) when the record exists, the command is successful 
 *         // (err == NULL) and "as_policy_read.async_heap_rec" is true. Otherwise, the calling
 *         // function will destroy the record when the listener completes.
 * 	   }
 * 	   else {
 * 	       printf("Record doesn't exist.");
 * 	   }
 * }
 *
 * as_key key;
 * as_key_init(&key, "ns", "set", "key");
 *
 * as_status status = aerospike_key_exists_async(&as, &err, NULL, &key, my_listener, NULL, NULL, NULL);
 * @endcode
 *
 * @param as				The aerospike instance to use for this operation.
 * @param err				The as_error to be populated if an error occurs.
 * @param policy			The policy to use for this operation. If NULL, then the default policy will be used.
 * @param key				The key of the record.
 * @param listener			User function to be called with command results.
 * @param udata				User data to be forwarded to user callback.
 * @param event_loop		Event loop assigned to run this command. If NULL, an event loop will be chosen by round-robin.
 * @param pipe_listener		Enables command pipelining, if not NULL. The given callback is invoked after the current command
 * 							has been sent to the server. This allows for issuing the next command even before receiving a
 * 							result for the current command.
 *
 * @return AEROSPIKE_OK if async command successfully queued. Otherwise an error.
 *
 * @ingroup key_operations
 */
AS_EXTERN as_status
aerospike_key_exists_async(
	aerospike* as, as_error* err, const as_policy_read* policy, const as_key* key,
	as_async_record_listener listener, void* udata, as_event_loop* event_loop,
	as_pipe_listener pipe_listener
	);

/**
 * Store a record in the cluster.
 *
 * @code
 * as_key key;
 * as_key_init(&key, "ns", "set", "key");
 *
 * as_record rec;
 * as_record_init(&rec, 2);
 * as_record_set_str(&rec, "bin1", "abc");
 * as_record_set_int64(&rec, "bin2", 123);
 * 
 * if (aerospike_key_put(&as, &err, NULL, &key, &rec) != AEROSPIKE_OK) {
 * 	   printf("error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 * }
 * as_record_destroy(&rec);
 * @endcode
 *
 * @param as			The aerospike instance to use for this operation.
 * @param err			The as_error to be populated if an error occurs.
 * @param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 * @param key			The key of the record.
 * @param rec 			The record containing the data to be written.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error.
 *
 * @ingroup key_operations
 */
AS_EXTERN as_status
aerospike_key_put(
	aerospike* as, as_error* err, const as_policy_write* policy, const as_key* key, as_record* rec
	);

/**
 * Asynchronously store a record in the cluster.
 *
 * @code
 * void my_listener(as_error* err, void* udata, as_event_loop* event_loop)
 * {
 * 	   if (err) {
 * 	       printf("Command failed: %d %s\n", err->code, err->message);
 * 	       return;
 * 	   }
 * 	   printf("Command succeeded\n");
 * }
 *
 * as_key key;
 * as_key_init(&key, "ns", "set", "key");
 *
 * as_record rec;
 * as_record_init(&rec, 2);
 * as_record_set_str(&rec, "bin1", "abc");
 * as_record_set_int64(&rec, "bin2", 123);
 *
 * as_status status = aerospike_key_put_async(&as, &err, NULL, &key, &rec, my_listener, NULL, NULL, NULL);
 * as_record_destroy(&rec);
 * @endcode
 *
 * @param as				The aerospike instance to use for this operation.
 * @param err				The as_error to be populated if an error occurs.
 * @param policy			The policy to use for this operation. If NULL, then the default policy will be used.
 * @param key				The key of the record.
 * @param rec				The record containing the data to be written.
 * @param listener			User function to be called with command results.
 * @param udata				User data to be forwarded to user callback.
 * @param event_loop		Event loop assigned to run this command. If NULL, an event loop will be chosen by round-robin.
 * @param pipe_listener		Enables command pipelining, if not NULL. The given callback is invoked after the current command
 * 							has been sent to the server. This allows for issuing the next command even before receiving a
 * 							result for the current command.
 *
 * @return AEROSPIKE_OK if async command successfully queued. Otherwise an error.
 *
 * @ingroup key_operations
 */
AS_EXTERN as_status
aerospike_key_put_async(
	aerospike* as, as_error* err, const as_policy_write* policy, const as_key* key, as_record* rec,
	as_async_write_listener listener, void* udata, as_event_loop* event_loop, as_pipe_listener pipe_listener
	);

/**
 * Remove a record from the cluster.
 *
 * @code
 * as_key key;
 * as_key_init(&key, "ns", "set", "key");
 *
 * if (aerospike_key_remove(&as, &err, NULL, &key) != AEROSPIKE_OK) {
 * 	   printf("error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 * }
 * @endcode
 *
 * @param as			The aerospike instance to use for this operation.
 * @param err			The as_error to be populated if an error occurs.
 * @param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 * @param key			The key of the record.
 *
 * @return AEROSPIKE_OK if successful and AEROSPIKE_ERR_RECORD_NOT_FOUND if the record was not found. Otherwise an error.
 *
 * @ingroup key_operations
 */
AS_EXTERN as_status
aerospike_key_remove(
	aerospike* as, as_error* err, const as_policy_remove* policy, const as_key* key
	);

/**
 * Asynchronously remove a record from the cluster.
 *
 * @code
 * void my_listener(as_error* err, void* udata, as_event_loop* event_loop)
 * {
 * 	   if (err) {
 * 	       printf("Command failed: %d %s\n", err->code, err->message);
 * 	       return;
 * 	   }
 * 	   printf("Command succeeded\n");
 * }
 *
 * as_key key;
 * as_key_init(&key, "ns", "set", "key");
 *
 * as_status status = aerospike_key_remove(&as, &err, NULL, &key, my_listener, NULL, NULL, NULL);
 * @endcode
 *
 * @param as				The aerospike instance to use for this operation.
 * @param err				The as_error to be populated if an error occurs.
 * @param policy			The policy to use for this operation. If NULL, then the default policy will be used.
 * @param key				The key of the record.
 * @param listener			User function to be called with command results.
 * @param udata				User data to be forwarded to user callback.
 * @param event_loop		Event loop assigned to run this command. If NULL, an event loop will be chosen by round-robin.
 * @param pipe_listener		Enables command pipelining, if not NULL. The given callback is invoked after the current command
 * 							has been sent to the server. This allows for issuing the next command even before receiving a
 * 							result for the current command.
 *
 * @return AEROSPIKE_OK if async command successfully queued. Otherwise an error.
 *
 * @ingroup key_operations
 */
AS_EXTERN as_status
aerospike_key_remove_async(
	aerospike* as, as_error* err, const as_policy_remove* policy, const as_key* key,
	as_async_write_listener listener, void* udata, as_event_loop* event_loop,
	as_pipe_listener pipe_listener
	);

/**
 * Lookup a record by key, then perform specified operations.
 *
 * @code
 * as_key key;
 * as_key_init(&key, "ns", "set", "key");
 *
 * as_operations ops;
 * as_operations_inita(&ops,3);
 * as_operations_add_incr(&ops, "bin1", 456);
 * as_operations_add_append_str(&ops, "bin2", "def");
 * as_operations_add_read(&ops, "bin1")
 *
 * as_record* rec = NULL;
 *
 * if (aerospike_key_operate(&as, &err, NULL, &key, &ops, &rec) != AEROSPIKE_OK) {
 * 	   printf("error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 * }
 * else {
 * 	   as_record_destroy(rec);
 * }
 * as_operations_destroy(&ops);
 * @endcode
 * 
 * @param as			The aerospike instance to use for this operation.
 * @param err			The as_error to be populated if an error occurs.
 * @param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 * @param key			The key of the record.
 * @param ops			The operations to perform on the record.
 * @param rec			The record to be populated with the data from AS_OPERATOR_READ operations.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error.
 *
 * @ingroup key_operations
 */
AS_EXTERN as_status
aerospike_key_operate(
	aerospike* as, as_error* err, const as_policy_operate* policy, const as_key* key,
	const as_operations* ops, as_record** rec
	);

/**
 * Asynchronously lookup a record by key, then perform specified operations.
 *
 * @code
 * void my_listener(as_error* err, as_record* record, void* udata, as_event_loop* event_loop)
 * {
 * 	   if (err) {
 *        printf("Command failed: %d %s\n", err->code, err->message);
 * 	      return;
 * 	   }
 * 	   // Process record bins
 * 	   // Only call as_record_destroy(record) when command is successful (err == NULL) and
 *     // "as_policy_operate.async_heap_rec" is true. Otherwise, the calling function will destroy
 *     // the record when the listener completes.
 * }
 *
 * as_key key;
 * as_key_init(&key, "ns", "set", "key");
 *
 * as_operations ops;
 * as_operations_inita(&ops,3);
 * as_operations_add_incr(&ops, "bin1", 456);
 * as_operations_add_append_str(&ops, "bin2", "def");
 * as_operations_add_read(&ops, "bin1")
 *
 * as_status status = aerospike_key_operate(&as, &err, NULL, &key, &ops, my_listener, NULL, NULL, NULL);
 * as_operations_destroy(&ops);
 * @endcode
 *
 * @param as				The aerospike instance to use for this operation.
 * @param err				The as_error to be populated if an error occurs.
 * @param policy			The policy to use for this operation. If NULL, then the default policy will be used.
 * @param key				The key of the record.
 * @param ops				The operations to perform on the record.
 * @param listener			User function to be called with command results.
 * @param udata				User data to be forwarded to user callback.
 * @param event_loop		Event loop assigned to run this command. If NULL, an event loop will be chosen by round-robin.
 * @param pipe_listener		Enables command pipelining, if not NULL. The given callback is invoked after the current command
 * 							has been sent to the server. This allows for issuing the next command even before receiving a
 * 							result for the current command.
 *
 * @return AEROSPIKE_OK if async command successfully queued. Otherwise an error.
 *
 * @ingroup key_operations
 */
AS_EXTERN as_status
aerospike_key_operate_async(
	aerospike* as, as_error* err, const as_policy_operate* policy, const as_key* key, const as_operations* ops,
	as_async_record_listener listener, void* udata, as_event_loop* event_loop, as_pipe_listener pipe_listener
	);

/**
 * Lookup a record by key, then apply the UDF.
 *
 * @code
 * as_key key;
 * as_key_init(&key, "ns", "set", "key");
 *
 * as_arraylist args;
 * as_arraylist_inita(&args, 2);
 * as_arraylist_append_int64(&args, 1);
 * as_arraylist_append_int64(&args, 2);
 * 
 * as_val * res = NULL;
 * 
 * if (aerospike_key_apply(&as, &err, NULL, &key, "math", "add", &args, &res) != AEROSPIKE_OK) {
 *     printf("error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 * }
 * else {
 * 	   as_val_destroy(res);
 * }
 * 
 * as_arraylist_destroy(&args);
 * @endcode
 *
 *
 * @param as			The aerospike instance to use for this operation.
 * @param err			The as_error to be populated if an error occurs.
 * @param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 * @param key			The key of the record.
 * @param module		The module containing the function to execute.
 * @param function 		The function to execute.
 * @param arglist 		The arguments for the function.
 * @param result 		The return value from the function.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error.
 *
 * @ingroup key_operations
 */
AS_EXTERN as_status
aerospike_key_apply(
	aerospike* as, as_error* err, const as_policy_apply* policy, const as_key* key,
	const char* module, const char* function, as_list* arglist, as_val** result
	);

/**
 * Asynchronously lookup a record by key, then apply the UDF.
 *
 * @code
 * void my_listener(as_error* err, as_val* val, void* udata, as_event_loop* event_loop)
 * {
 * 	   if (err) {
 * 	       printf("Command failed: %d %s\n", err->code, err->message);
 * 	       return;
 * 	    }
 * 	    // Process value.  The calling function will call as_val_destroy().
 * 	    // If the value needs to be preserved, bump up the reference count using as_val_reserve()
 * 	    // and call as_val_destroy() when done with the value.
 * }
 *
 * as_key key;
 * as_key_init(&key, "ns", "set", "key");
 *
 * as_arraylist args;
 * as_arraylist_inita(&args, 2);
 * as_arraylist_append_int64(&args, 1);
 * as_arraylist_append_int64(&args, 2);
 *
 * as_status status = aerospike_key_apply(&as, &err, NULL, &key, "math", "add", &args, my_listener, NULL, NULL, NULL);
 * as_arraylist_destroy(&args);
 * @endcode
 *
 * @param as				The aerospike instance to use for this operation.
 * @param err				The as_error to be populated if an error occurs.
 * @param policy			The policy to use for this operation. If NULL, then the default policy will be used.
 * @param key				The key of the record.
 * @param module			The module containing the function to execute.
 * @param function			The function to execute.
 * @param arglist			The arguments for the function.
 * @param listener			User function to be called with command results.
 * @param udata				User data to be forwarded to user callback.
 * @param event_loop		Event loop assigned to run this command. If NULL, an event loop will be chosen by round-robin.
 * @param pipe_listener		Enables command pipelining, if not NULL. The given callback is invoked after the current command
 * 							has been sent to the server. This allows for issuing the next command even before receiving a
 * 							result for the current command.
 *
 * @return AEROSPIKE_OK if async command successfully queued. Otherwise an error.
 *
 * @ingroup key_operations
 */
AS_EXTERN as_status
aerospike_key_apply_async(
	aerospike* as, as_error* err, const as_policy_apply* policy, const as_key* key,
	const char* module, const char* function, as_list* arglist,
	as_async_value_listener listener, void* udata, as_event_loop* event_loop,
	as_pipe_listener pipe_listener
	);

/**
 * @cond SKIP_DOXYGEN
 * doxygen skips this section till endcond
 */
AS_EXTERN as_status
aerospike_key_put_async_ex(
	aerospike* as, as_error* err, const as_policy_write* policy, const as_key* key, as_record* rec,
	as_async_write_listener listener, void* udata, as_event_loop* event_loop, as_pipe_listener pipe_listener,
	size_t* length, size_t* comp_length
	);

AS_EXTERN as_status
aerospike_key_remove_async_ex(
	aerospike* as, as_error* err, const as_policy_remove* policy, const as_key* key,
	as_async_write_listener listener, void* udata, as_event_loop* event_loop,
	as_pipe_listener pipe_listener, size_t* length
	);
/**
 * @endcond
 */

#ifdef __cplusplus
} // end extern "C"
#endif
