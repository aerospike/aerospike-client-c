/*
 * Copyright 2008-2022 Aerospike, Inc.
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
 * @defgroup hll_operations HyperLogLog Operations
 * @ingroup client_operations
 *
 * HyperLogLog (HLL) operations.
 *
 * HyperLogLog operations on HLL items nested in lists/maps are not currently
 * supported by the server. The as_cdt_ctx argument in HLL operations must
 * be set to NULL.
 */

#include <aerospike/as_operations.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * HyperLogLog write flags.
 *
 * @ingroup hll_operations
 */
typedef enum as_hll_write_flags_e {
	/**
	 * Default. Allow create or update.
	 */
	AS_HLL_WRITE_DEFAULT = 0,

	/**
	 * If the bin already exists, the operation will be denied.
	 * If the bin does not exist, a new bin will be created.
	 */
	AS_HLL_WRITE_CREATE_ONLY = 1,

	/**
	 * If the bin already exists, the bin will be overwritten.
	 * If the bin does not exist, the operation will be denied.
	 */
	AS_HLL_WRITE_UPDATE_ONLY = 2,

	/**
	 * Do not raise error if operation is denied.
	 */
	AS_HLL_WRITE_NO_FAIL = 4,

	/**
	 * Allow the resulting set to be the minimum of provided index bits.
	 * Also, allow the usage of less precise HLL algorithms when minhash bits
	 * of all participating sets do not match.
	 */
	AS_HLL_WRITE_ALLOW_FOLD = 8
} as_hll_write_flags;

/**
 * HLL operation policy.
 *
 * @ingroup hll_operations
 */
typedef struct as_hll_policy_s {
	uint64_t flags;
} as_hll_policy;

/**
 * @private
 * HLL operation codes.
 */
typedef enum {
	AS_HLL_OP_INIT = 0,
	AS_HLL_OP_ADD = 1,
	AS_HLL_OP_UNION = 2,
	AS_HLL_OP_REFRESH_COUNT = 3,
	AS_HLL_OP_FOLD = 4,
	AS_HLL_OP_COUNT = 50,
	AS_HLL_OP_GET_UNION = 51,
	AS_HLL_OP_UNION_COUNT = 52,
	AS_HLL_OP_INTERSECT_COUNT = 53,
	AS_HLL_OP_SIMILARITY = 54,
	AS_HLL_OP_DESCRIBE = 55,
	AS_HLL_OP_MAY_CONTAIN = 56
} as_hll_op;

/******************************************************************************
 * PRIVATE FUNCTIONS
 *****************************************************************************/

AS_EXTERN bool
as_operations_hll_read(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, uint16_t command
	);

AS_EXTERN bool
as_operations_hll_read_list(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, uint16_t command, as_list* list
	);

/******************************************************************************
 * PUBLIC FUNCTIONS
 *****************************************************************************/

/**
 * Initialize HLL policy to default.
 *
 * @ingroup hll_operations
 */
static inline void
as_hll_policy_init(as_hll_policy* policy)
{
	policy->flags = AS_HLL_WRITE_DEFAULT;
}

/**
 * Set HLL write flags in HLL policy.
 *
 * @ingroup hll_operations
 */
static inline void
as_hll_policy_set_write_flags(as_hll_policy* policy, as_hll_write_flags flags)
{
	policy->flags = flags;
}

/**
 * Create HLL init operation with minhash bits.
 * Server creates a new HLL or resets an existing HLL.
 * Server does not return a value.
 *
 * @param ops				Operations array.
 * @param name				Name of bin.
 * @param ctx				Must set to NULL.
 * @param policy			Write policy. Use NULL for default.
 * @param index_bit_count	Number of index bits. Must be between 4 and 16 inclusive.
 * @param mh_bit_count      Number of min hash bits. Must be between 4 and 51 inclusive.
 *							Also, index_bit_count + mh_bit_count must be <= 64.
 * @ingroup hll_operations
 */
AS_EXTERN bool
as_operations_hll_init_mh(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_hll_policy* policy,
	int index_bit_count, int mh_bit_count
	);

/**
 * Create HLL init operation.
 * Server creates a new HLL or resets an existing HLL.
 * Server does not return a value.
 *
 * @param ops				Operations array.
 * @param name				Name of bin.
 * @param ctx				Must set to NULL.
 * @param policy			Write policy. Use NULL for default.
 * @param index_bit_count	Number of index bits. Must be between 4 and 16 inclusive.
 * @ingroup hll_operations
 */
static inline bool
as_operations_hll_init(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_hll_policy* policy,
	int index_bit_count
	)
{
	return as_operations_hll_init_mh(ops, name, ctx, policy, index_bit_count, -1);
}

/**
 * Create HLL add operation with minhash bits.
 * Server adds values to HLL set. If HLL bin does not exist, use bit counts to create HLL bin.
 * Server returns number of entries that caused HLL to update a register.
 *
 * @param ops				Operations array.
 * @param name				Name of bin.
 * @param ctx				Must set to NULL.
 * @param policy			Write policy. Use NULL for default.
 * @param list				List of values to be added.
 * @param index_bit_count	Number of index bits. Must be between 4 and 16 inclusive.
 * @param mh_bit_count      Number of min hash bits. Must be between 4 and 51 inclusive.
 *							Also, index_bit_count + mh_bit_count must be <= 64.
 * @ingroup hll_operations
 */
AS_EXTERN bool
as_operations_hll_add_mh(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_hll_policy* policy,
	as_list* list, int index_bit_count, int mh_bit_count
	);

/**
 * Create HLL add operation with index bits.
 * Server adds values to HLL set. If HLL bin does not exist, use bit counts to create HLL bin.
 * Server returns number of entries that caused HLL to update a register.
 *
 * @param ops				Operations array.
 * @param name				Name of bin.
 * @param ctx				Must set to NULL.
 * @param policy			Write policy. Use NULL for default.
 * @param list				List of values to be added.
 * @param index_bit_count	Number of index bits. Must be between 4 and 16 inclusive.
 * @ingroup hll_operations
 */
static inline bool
as_operations_hll_add(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_hll_policy* policy,
	as_list* list, int index_bit_count
	)
{
	return as_operations_hll_add_mh(ops, name, ctx, policy, list, index_bit_count, -1);
}

/**
 * Create HLL update operation. This operation assumes HLL bin already exists.
 * Server adds values to HLL set.
 * Server returns number of entries that caused HLL to update a register.
 *
 * @param ops				Operations array.
 * @param name				Name of bin.
 * @param ctx				Must set to NULL.
 * @param policy			Write policy. Use NULL for default.
 * @param list				List of values to be added.
 * @ingroup hll_operations
 */
static inline bool
as_operations_hll_update(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_hll_policy* policy,
	as_list* list
	)
{
	return as_operations_hll_add_mh(ops, name, ctx, policy, list, -1, -1);
}

/**
 * Create HLL set union operation.
 * Server sets union of specified HLL objects with HLL bin.
 * Server does not return a value.
 *
 * @param ops				Operations array.
 * @param name				Name of bin.
 * @param ctx				Must set to NULL.
 * @param policy			Write policy. Use NULL for default.
 * @param list				List of HLL as_bytes objects.
 * @ingroup hll_operations
 */
AS_EXTERN bool
as_operations_hll_set_union(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_hll_policy* policy,
	as_list* list
	);

/**
 * Create HLL refresh operation.
 * Server updates the cached count (if stale) and returns the count.
 *
 * @param ops				Operations array.
 * @param name				Name of bin.
 * @param ctx				Must set to NULL.
 * @ingroup hll_operations
 */
AS_EXTERN bool
as_operations_hll_refresh_count(
	as_operations* ops, const char* name, as_cdt_ctx* ctx
	);

/**
 * Create HLL fold operation.
 * Servers folds index_bit_count to the specified value.
 * This can only be applied when minhash bit count on the HLL bin is 0.
 * Server does not return a value.
 *
 * @param ops				Operations array.
 * @param name				Name of bin.
 * @param ctx				Must set to NULL.
 * @param index_bit_count	Number of index bits. Must be between 4 and 16 inclusive.
 * @ingroup hll_operations
 */
AS_EXTERN bool
as_operations_hll_fold(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int index_bit_count
	);

/**
 * Create HLL get count operation.
 * Server returns estimated number of elements in the HLL bin.
 *
 * @param ops				Operations array.
 * @param name				Name of bin.
 * @param ctx				Must set to NULL.
 * @ingroup hll_operations
 */
static inline bool
as_operations_hll_get_count(
	as_operations* ops, const char* name, as_cdt_ctx* ctx
	)
{
	return as_operations_hll_read(ops, name, ctx, AS_HLL_OP_COUNT);
}

/**
 * Create HLL get union operation.
 * Server returns an HLL object that is the union of all specified HLL objects in the list
 * with the HLL bin.
 *
 * @param ops				Operations array.
 * @param name				Name of bin.
 * @param ctx				Must set to NULL.
 * @param list				List of HLL as_bytes objects.
 * @ingroup hll_operations
 */
static inline bool
as_operations_hll_get_union(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list* list
	)
{
	return as_operations_hll_read_list(ops, name, ctx, AS_HLL_OP_GET_UNION, list);
}

/**
 * Create HLL get union count operation.
 * Server returns estimated number of elements that would be contained by the union of these
 * HLL objects.
 *
 * @param ops				Operations array.
 * @param name				Name of bin.
 * @param ctx				Must set to NULL.
 * @param list				List of HLL as_bytes objects.
 * @ingroup hll_operations
 */
static inline bool
as_operations_hll_get_union_count(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list* list
	)
{
	return as_operations_hll_read_list(ops, name, ctx, AS_HLL_OP_UNION_COUNT, list);
}

/**
 * Create HLL get intersect count operation.
 * Server returns estimated number of elements that would be contained by the intersection of
 * these HLL objects.
 *
 * @param ops				Operations array.
 * @param name				Name of bin.
 * @param ctx				Must set to NULL.
 * @param list				List of HLL as_bytes objects.
 * @ingroup hll_operations
 */
static inline bool
as_operations_hll_get_intersect_count(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list* list
	)
{
	return as_operations_hll_read_list(ops, name, ctx, AS_HLL_OP_INTERSECT_COUNT, list);
}

/**
 * Create HLL get similarity operation.
 * Server returns estimated similarity of these HLL objects. Return type is a double.
 *
 * @param ops				Operations array.
 * @param name				Name of bin.
 * @param ctx				Must set to NULL.
 * @param list				List of HLL as_bytes objects.
 * @ingroup hll_operations
 */
static inline bool
as_operations_hll_get_similarity(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list* list
	)
{
	return as_operations_hll_read_list(ops, name, ctx, AS_HLL_OP_SIMILARITY, list);
}

/**
 * Create HLL describe operation.
 * Server returns index and minhash bit counts used to create HLL bin in a list of integers.
 * The list size is 2.
 *
 * @param ops				Operations array.
 * @param name				Name of bin.
 * @param ctx				Must set to NULL.
 * @ingroup hll_operations
 */
static inline bool
as_operations_hll_describe(
	as_operations* ops, const char* name, as_cdt_ctx* ctx
	)
{
	return as_operations_hll_read(ops, name, ctx, AS_HLL_OP_DESCRIBE);
}

#ifdef __cplusplus
} // end extern "C"
#endif
