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
 * @defgroup exp_operations Expression Operations
 * @ingroup client_operations
 *
 * Expression operations.
 */

#include <aerospike/as_operations.h>
#include <aerospike/as_exp.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * Expression write flags.
 *
 * @ingroup exp_operations
 */
typedef enum as_exp_write_flags_e {
	/**
	 * Default. Allow create or update.
	 */
	AS_EXP_WRITE_DEFAULT = 0,

	/**
	 * If bin does not exist, a new bin will be created.
	 * If bin exists, the operation will be denied.
	 * If bin exists, fail with AEROSPIKE_ERR_BIN_EXISTS
	 * when AS_EXP_WRITE_POLICY_NO_FAIL is not set.
	 */
	AS_EXP_WRITE_CREATE_ONLY = 1,

	/**
	 * If bin exists, the bin will be overwritten.
	 * If bin does not exist, the operation will be denied.
	 * If bin does not exist, fail with AEROSPIKE_ERR_BIN_NOT_FOUND
	 * when AS_EXP_WRITE_POLICY_NO_FAIL is not set.
	 */
	AS_EXP_WRITE_UPDATE_ONLY = 2,

	/**
	 * If expression results in nil value, then delete the bin. Otherwise, return
	 * AEROSPIKE_ERR_OP_NOT_APPLICABLE when AS_EXP_WRITE_POLICY_NO_FAIL is not set.
	 */
	AS_EXP_WRITE_ALLOW_DELETE = 4,

	/**
	 * Do not raise error if operation is denied.
	 */
	AS_EXP_WRITE_POLICY_NO_FAIL = 8,

	/**
	 * Ignore failures caused by the expression resolving to unknown or a non-bin type.
	 */
	AS_EXP_WRITE_EVAL_NO_FAIL = 16
} as_exp_write_flags;

/**
 * Expression read flags.
 *
 * @ingroup exp_operations
 */
typedef enum as_exp_read_flags_e {
	/**
	 * Default.
	 */
	AS_EXP_READ_DEFAULT = 0,

	/**
	 * Ignore failures caused by the expression resolving to unknown or a non-bin type.
	 */
	AS_EXP_READ_EVAL_NO_FAIL = 16
} as_exp_read_flags;

/******************************************************************************
 * PUBLIC FUNCTIONS
 *****************************************************************************/

/**
 * Create operation that performs an expression that writes to a record bin.
 * Requires server version 5.6.0+.
 *
 * @param ops			Operations array.
 * @param name			Name of bin.
 * @param exp			Expression to evaluate.
 * @param flags			Expression write flags. Multiple flags can be specified via "|" operator.
 * @ingroup exp_operations
 */
AS_EXTERN bool
as_operations_exp_write(
	as_operations* ops, const char* name, const as_exp* exp, as_exp_write_flags flags
	);

/**
 * Create operation that performs a read expression.
 * Requires server version 5.6.0+.
 *
 * @param ops			Operations array.
 * @param name			Variable name of read expression result. This name can be used as the
 *						bin name when retrieving bin results from the record.
 * @param exp			Expression to evaluate.
 * @param flags			Expression read flags.
 * @ingroup exp_operations
 */
AS_EXTERN bool
as_operations_exp_read(
	as_operations* ops, const char* name, const as_exp* exp, as_exp_read_flags flags
	);

#ifdef __cplusplus
} // end extern "C"
#endif
