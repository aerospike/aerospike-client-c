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
 * @defgroup index_operations Index Operations
 * @ingroup client_operations
 *
 * The Index API provides the ability to create and remove secondary indexes.
 * 
 * Aerospike currently supports indexing of strings and integers.
 * 
 * ## String Indexes
 * 
 * A string index allows for equality lookups. An equality lookup means that 
 * if you query for an indexed bin with value "abc", then only the records 
 * containing bins with "abc" will be returned.
 *
 * ## Integer Indexes
 *
 * An integer index allows for either equality or range lookups. An equality
 * lookup means that if you query for an indexed bin with value 123, then only 
 * the records containing bins with the value 123 will be returned. A range 
 * lookup means that you can query bins within a range. So, if your range is 
 * (1...100), then all records containing the a value in that range will
 * be returned.
 */

#include <aerospike/aerospike.h>
#include <aerospike/as_bin.h>
#include <aerospike/as_error.h>
#include <aerospike/as_key.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_status.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Macros
//---------------------------------

#define AS_INDEX_NAME_MAX_SIZE 64

//---------------------------------
// Types
//---------------------------------

/**
 * Index Type
 *
 * @ingroup index_operations
 */
typedef enum as_index_type_s {
	AS_INDEX_TYPE_DEFAULT,
	AS_INDEX_TYPE_LIST,
	AS_INDEX_TYPE_MAPKEYS,
	AS_INDEX_TYPE_MAPVALUES
} as_index_type;

/*
 * Type of data which is going to indexed
 */
typedef enum as_index_datatype_s {
	AS_INDEX_STRING,
	AS_INDEX_NUMERIC,
	AS_INDEX_GEO2DSPHERE,
	AS_INDEX_BLOB  // Requires server version 7.0+.
} as_index_datatype;

/**
 * Index Task
 *
 * Task used to poll for long running create index completion.
 *
 * @ingroup index_operations
 */
typedef struct as_index_task_s {
	/**
	 * The aerospike instance to use for this operation.
	 */
	aerospike* as;
	
	/**
	 * The namespace to be indexed.
	 */
	as_namespace ns;
	
	/**
	 * The name of the index.
	 */
	char name[AS_INDEX_NAME_MAX_SIZE];

	/**
	 * Maximum time in milliseconds to wait for info command to return create index status.
	 * Defaults to "as_policy_info.timeout" that is passed to the original create index function.
	 */
	uint32_t socket_timeout;

	/**
	 * Maximum time in milliseconds to wait for create index to complete.
	 * Default: 30000 ms (30 seconds)
	 */
	uint32_t total_timeout;

	/**
	 * Has operation completed.
	 */
	bool done;
} as_index_task;

struct as_cdt_ctx;
struct as_exp;

//---------------------------------
// Functions
//---------------------------------

/**
 * Create secondary index given collection type, data type and context.
 *
 * This asynchronous server call will return before the command is complete.
 * The user can optionally wait for command completion by using a task instance.
 *
 * @code
 * as_cdt_ctx ctx;
 * as_cdt_ctx_init(&ctx, 1);
 * as_cdt_ctx_add_list_rank(&ctx, -1);
 * as_index_task task;
 * if (aerospike_index_create_ctx(&as, &err, &task, NULL, "test", "demo", "bin1",
 *     "idx_test_demo_bin1", AS_INDEX_TYPE_DEFAULT, AS_INDEX_NUMERIC, &ctx) == AEROSPIKE_OK) {
 *     aerospike_index_create_wait(&err, &task, 0);
 * }
 * @endcode
 *
 * @param as			The aerospike instance to use for this operation.
 * @param err			The as_error to be populated if an error occurs.
 * @param task			The optional task data used to poll for completion.
 * @param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 * @param ns			The namespace to be indexed.
 * @param set			The set to be indexed.
 * @param bin_name		The bin name to be indexed.
 * @param index_name	The name of the index.
 * @param itype			The type of index, default or complex type.
 * @param dtype			The data type of index, string or integer.
 * @param ctx			Optional CDT context describing the path to locate the data to be indexed.
 *
 * @return AEROSPIKE_OK if successful. Return AEROSPIKE_ERR_INDEX_FOUND if index exists. Otherwise an error.
 *
 * @ingroup index_operations
 */
AS_EXTERN as_status
aerospike_index_create_ctx(
	aerospike* as, as_error* err, as_index_task* task, const as_policy_info* policy, const char* ns,
	const char* set, const char* bin_name, const char* index_name, as_index_type itype,
	as_index_datatype dtype, struct as_cdt_ctx* ctx
	);

/**
 * Create secondary index on an expression.
 *
 * This asynchronous server call will return before the command is complete.
 * The user can optionally wait for command completion by using a task instance.
 *
 * @code
 * as_exp_build(exp, as_exp_add(as_exp_bin_int("a"), as_exp_bin_int("b")));
 *
 * as_index_task task;
 * if (aerospike_index_create_ctx(&as, &err, &task, NULL, "test", "demo",
 *     "idx_test_demo_bin1", AS_INDEX_TYPE_DEFAULT, AS_INDEX_NUMERIC, exp) == AEROSPIKE_OK) {
 *     aerospike_index_create_wait(&err, &task, 0);
 * }
 * as_exp_destroy(exp);
 * @endcode
 *
 * @param as			The aerospike instance to use for this operation.
 * @param err			The as_error to be populated if an error occurs.
 * @param task			The optional task data used to poll for completion.
 * @param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 * @param ns			The namespace to be indexed.
 * @param set			The set to be indexed.
 * @param index_name	The name of the index.
 * @param itype			The type of index, default or complex type.
 * @param dtype			The data type of index, string or integer.
 * @param exp			The expression to be indexed.
 *
 * @return AEROSPIKE_OK if successful. Return AEROSPIKE_ERR_INDEX_FOUND if index exists. Otherwise an error.
 *
 * @ingroup index_operations
 */
AS_EXTERN as_status
aerospike_index_create_exp(
	aerospike* as, as_error* err, as_index_task* task, const as_policy_info* policy, const char* ns,
	const char* set, const char* index_name, as_index_type itype, as_index_datatype dtype,
	struct as_exp* exp
	);

/**
 * Create secondary index given collection type and data type.
 *
 * This asynchronous server call will return before the command is complete.
 * The user can optionally wait for command completion by using a task instance.
 *
 * @code
 * as_index_task task;
 * if (aerospike_index_create_complex(&as, &err, &task, NULL, "test", "demo", "bin1",
 *     "idx_test_demo_bin1", AS_INDEX_TYPE_DEFAULT, AS_INDEX_NUMERIC) == AEROSPIKE_OK) {
 *     aerospike_index_create_wait(&err, &task, 0);
 * }
* @endcode
 *
 * @param as			The aerospike instance to use for this operation.
 * @param err			The as_error to be populated if an error occurs.
 * @param task			The optional task data used to poll for completion.
 * @param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 * @param ns			The namespace to be indexed.
 * @param set			The set to be indexed.
 * @param bin_name		The bin name to be indexed.
 * @param index_name	The name of the index.
 * @param itype			The type of index, default or complex type.
 * @param dtype			The data type of index, string or integer.
 *
 * @return AEROSPIKE_OK if successful. Return AEROSPIKE_ERR_INDEX_FOUND if index exists. Otherwise an error.
 *
 * @ingroup index_operations
 */
static inline as_status
aerospike_index_create_complex(
	aerospike* as, as_error* err, as_index_task* task,
	const as_policy_info* policy, const char* ns, const char* set,
	const char* bin_name, const char* index_name, as_index_type itype,
	as_index_datatype dtype
	)
{
	return aerospike_index_create_ctx(as, err, task, policy, ns, set, bin_name,
			index_name, itype, dtype, NULL);
}

/**
 * Create secondary index given data type.
 *
 * This asynchronous server call will return before the command is complete.
 * The user can optionally wait for command completion by using a task instance.
 *
 * @code
 * as_index_task task;
 * if (aerospike_index_create(&as, &err, &task, NULL, "test", "demo", "bin1", 
 * 	   "idx_test_demo_bin1", AS_INDEX_NUMERIC) == AEROSPIKE_OK) {
 * 	   aerospike_index_create_wait(&err, &task, 0);
 * }
 * @endcode
 *
 * @param as			The aerospike instance to use for this operation.
 * @param err			The as_error to be populated if an error occurs.
 * @param task			The optional task data used to poll for completion.
 * @param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 * @param ns			The namespace to be indexed.
 * @param set			The set to be indexed.
 * @param bin_name		The bin name to be indexed.
 * @param index_name	The name of the index.
 * @param dtype			The data type of index, string or integer.
 *
 * @return AEROSPIKE_OK if successful. Return AEROSPIKE_ERR_INDEX_FOUND if index exists. Otherwise an error.
 *
 * @ingroup index_operations
 */
static inline as_status
aerospike_index_create(
	aerospike* as, as_error* err, as_index_task* task, const as_policy_info* policy,
	const char* ns, const char* set, const char* bin_name, const char* index_name,
	as_index_datatype dtype
	)
{
	return aerospike_index_create_ctx(as, err, task, policy, ns, set, bin_name, index_name,
			AS_INDEX_TYPE_DEFAULT, dtype, NULL);
}

/**
 * Wait for asynchronous task to complete using given polling interval.
 *
 * @param err			The as_error to be populated if an error occurs.
 * @param task			The task data used to poll for completion.
 * @param interval_ms	The polling interval in milliseconds. If zero, 1000 ms is used.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error.
 *
 * @ingroup index_operations
 */
AS_EXTERN as_status
aerospike_index_create_wait(as_error* err, as_index_task* task, uint32_t interval_ms);

/**
 * Removes (drops) a secondary index.
 *
 * @code
 * if (aerospike_index_remove(&as, &err, NULL, "test", idx_test_demo_bin1") != AEROSPIKE_OK) {
 *     fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 * }
 * @endcode
 *
 * @param as			The aerospike instance to use for this operation.
 * @param err			The as_error to be populated if an error occurs.
 * @param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 * @param ns			The namespace containing the index to be removed.
 * @param index_name	The name of the index to be removed.
 *
 * @return AEROSPIKE_OK if successful or index does not exist. Otherwise an error.
 *
 * @ingroup index_operations
 */
AS_EXTERN as_status
aerospike_index_remove(
	aerospike* as, as_error* err, const as_policy_info* policy,
	const char* ns, const char* index_name
	);

//---------------------------------
// Deprecated Functions
//---------------------------------

/**
 * Create a new secondary index on an integer bin.
 *
 * @deprecated Use aerospike_index_create() instead.
 *
 * @ingroup index_operations
 */
static inline as_status
aerospike_index_integer_create(
	aerospike* as, as_error* err, const as_policy_info* policy, 
	const char* ns, const char* set, const char* bin, const char* name
	)
{
	return aerospike_index_create_complex(as, err, 0, policy, ns, set, bin, name, AS_INDEX_TYPE_DEFAULT, AS_INDEX_NUMERIC);
}

/**
 * Create a new secondary index on a string bin.
 *
 * @deprecated Use aerospike_index_create() instead.
 *
 * @ingroup index_operations
 */
static inline as_status
aerospike_index_string_create(
	aerospike* as, as_error* err, const as_policy_info* policy, 
	const char* ns, const char* set, const char* bin, const char* name
	)
{
	return aerospike_index_create_complex(as, err, 0, policy, ns, set, bin, name, AS_INDEX_TYPE_DEFAULT, AS_INDEX_STRING);
}

#ifdef __cplusplus
} // end extern "C"
#endif
