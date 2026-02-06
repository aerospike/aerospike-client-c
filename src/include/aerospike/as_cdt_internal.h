/*
 * Copyright 2008-2023 Aerospike, Inc.
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

#include <aerospike/as_msgpack.h>
#include <aerospike/as_cdt_ctx.h>
#include <aerospike/as_operations.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define as_cdt_begin() \
	{0};\
	while (true) {

#define as_cdt_end(pk) \
		if (!(pk)->buffer) {\
			(pk)->buffer = cf_malloc((pk)->offset);\
			(pk)->capacity = (pk)->offset;\
			(pk)->offset = 0;\
			(pk)->head = NULL;\
			(pk)->tail = NULL;\
			continue;\
		}\
		break;\
	}

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

void
as_cdt_pack_header(as_packer* pk, as_cdt_ctx* ctx, uint16_t command, uint32_t count);

void
as_cdt_pack_header_flag(as_packer* pk, as_cdt_ctx* ctx, uint16_t command, uint32_t count, uint32_t flag);

void
as_cdt_pack_ctx(as_packer* pk, as_cdt_ctx* ctx);

uint32_t
as_cdt_ctx_pack(const as_cdt_ctx* ctx, as_packer* pk);

bool
as_cdt_add_packed(as_packer* pk, as_operations* ops, const char* name, as_operator op_type);

bool
as_cdt_ctx_from_unpacker(as_cdt_ctx* ctx, as_unpacker* pk);

static inline void
as_pack_string(as_packer* pk, const char* s)
{
	uint32_t len = (uint32_t)strlen(s);
	as_pack_str(pk, (uint8_t*)s, len);
}

static inline void
as_pack_byte_string(as_packer* pk, const uint8_t* b, uint32_t len)
{
	as_pack_str(pk, b, len);
}

bool
as_unpack_str_init(as_unpacker* pk, char* str, uint32_t max);

bool
as_unpack_str_new(as_unpacker* pk, char** str, uint32_t max);

bool
as_unpack_bytes_init(as_unpacker* pk, uint8_t* b, uint32_t max);

bool
as_unpack_bytes_new(as_unpacker* pk, uint8_t** bytes, uint32_t* bytes_size, uint32_t max);

#define as_cmp_error() \
	printf("Line %d\n", __LINE__);\
	return false;

bool
as_val_compare(as_val* v1, as_val* v2);

#ifdef __cplusplus
} // end extern "C"
#endif
