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
#include <aerospike/as_exp_operations.h>
#include <aerospike/as_cdt_internal.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_msgpack.h>
#include <citrusleaf/alloc.h>

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static inline void
pack_exp(as_packer* pk, const as_exp* exp)
{
	if (pk->buffer != NULL) {
		memcpy(pk->buffer + pk->offset, exp->packed, exp->packed_sz);
	}

	pk->offset += exp->packed_sz;
}

static inline bool
as_operations_exp_op(
	as_operator command, as_operations* ops, const char* name, const as_exp* exp,
	uint64_t flags
	)
{
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, 2);
	pack_exp(&pk, exp);
	as_pack_uint64(&pk, flags);
	as_cdt_end(&pk);

	return as_cdt_add_packed(&pk, ops, name, command);
}

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

bool
as_operations_exp_write(
	as_operations* ops, const char* name, const as_exp* exp, as_exp_write_flags flags
	)
{
	return as_operations_exp_op(AS_OPERATOR_EXP_MODIFY, ops, name, exp, flags);
}

bool
as_operations_exp_read(
	as_operations* ops, const char* name, const as_exp* exp, as_exp_read_flags flags
	)
{
	return as_operations_exp_op(AS_OPERATOR_EXP_READ, ops, name, exp, flags);
}
