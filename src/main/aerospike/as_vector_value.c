/*
 * Copyright 2008-2026 Aerospike, Inc.
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
#include <aerospike/as_vector_value.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_byte_order.h>
#include <string.h>

//---------------------------------
// Static Functions
//---------------------------------

static as_vector_value*
as_vector_value_new(as_vector_element_type element_type, uint32_t count, const void* data, uint32_t elem_size)
{
	as_vector_value* vec = (as_vector_value*)cf_malloc(sizeof(as_vector_value));
	vec->version = AS_VECTOR_VALUE_VERSION;
	vec->element_type = element_type;
	vec->dimensions = count;
	vec->free_data = true;

	size_t size = (size_t)count * elem_size;
	vec->data = cf_malloc(size > 0 ? size : 1);
	memcpy(vec->data, data, size);
	return vec;
}

//---------------------------------
// Functions
//---------------------------------

as_vector_value*
as_vector_value_new_float16(const uint16_t* data, uint32_t count)
{
	return as_vector_value_new(AS_VECTOR_ELEMENT_TYPE_FLOAT16, count, data, sizeof(uint16_t));
}

as_vector_value*
as_vector_value_new_int32(const int32_t* data, uint32_t count)
{
	return as_vector_value_new(AS_VECTOR_ELEMENT_TYPE_INT32, count, data, sizeof(int32_t));
}

as_vector_value*
as_vector_value_new_float32(const float* data, uint32_t count)
{
	return as_vector_value_new(AS_VECTOR_ELEMENT_TYPE_FLOAT32, count, data, sizeof(float));
}

as_vector_value*
as_vector_value_new_float64(const double* data, uint32_t count)
{
	return as_vector_value_new(AS_VECTOR_ELEMENT_TYPE_FLOAT64, count, data, sizeof(double));
}

void
as_vector_value_destroy(as_vector_value* vec)
{
	if (vec->free_data) {
		cf_free(vec->data);
	}
	cf_free(vec);
}

uint32_t
as_vector_element_type_size(as_vector_element_type type)
{
	switch (type) {
	case AS_VECTOR_ELEMENT_TYPE_FLOAT16:
		return 2;
	case AS_VECTOR_ELEMENT_TYPE_INT32:
		return 4;
	case AS_VECTOR_ELEMENT_TYPE_FLOAT32:
		return 4;
	case AS_VECTOR_ELEMENT_TYPE_FLOAT64:
		return 8;
	default:
		return 0;
	}
}

uint32_t
as_vector_value_write(const as_vector_value* vec, uint8_t* buffer, uint32_t offset)
{
	uint32_t pos = offset;

	// Vector wire format is little-endian to match the server.
	buffer[pos++] = vec->version;
	buffer[pos++] = (uint8_t)vec->element_type;

	uint32_t dims_le = cf_swap_to_le32(vec->dimensions);
	memcpy(&buffer[pos], &dims_le, sizeof(dims_le));
	pos += sizeof(dims_le);

	buffer[pos++] = 0; // reserved
	buffer[pos++] = 0; // reserved

	uint32_t i;

	switch (vec->element_type) {
	case AS_VECTOR_ELEMENT_TYPE_FLOAT16: {
		const uint16_t* src = (const uint16_t*)vec->data;
		for (i = 0; i < vec->dimensions; i++) {
			uint16_t le = cf_swap_to_le16(src[i]);
			memcpy(&buffer[pos], &le, sizeof(le));
			pos += sizeof(le);
		}
		break;
	}
	case AS_VECTOR_ELEMENT_TYPE_INT32: {
		const int32_t* src = (const int32_t*)vec->data;
		for (i = 0; i < vec->dimensions; i++) {
			uint32_t le = cf_swap_to_le32((uint32_t)src[i]);
			memcpy(&buffer[pos], &le, sizeof(le));
			pos += sizeof(le);
		}
		break;
	}
	case AS_VECTOR_ELEMENT_TYPE_FLOAT32: {
		const float* src = (const float*)vec->data;
		for (i = 0; i < vec->dimensions; i++) {
			union { float f; uint32_t u; } conv;
			conv.f = src[i];
			conv.u = cf_swap_to_le32(conv.u);
			memcpy(&buffer[pos], &conv.u, sizeof(conv.u));
			pos += sizeof(conv.u);
		}
		break;
	}
	case AS_VECTOR_ELEMENT_TYPE_FLOAT64: {
		const double* src = (const double*)vec->data;
		for (i = 0; i < vec->dimensions; i++) {
			double le = cf_swap_to_little_float64(src[i]);
			memcpy(&buffer[pos], &le, sizeof(le));
			pos += sizeof(le);
		}
		break;
	}
	default:
		break;
	}

	return pos - offset;
}

bool
as_vector_value_to_bytes(const as_vector_value* vec, as_bytes* bytes)
{
	uint32_t size = as_vector_value_get_wire_size(vec);
	uint8_t* buffer = (uint8_t*)cf_malloc(size);
	as_vector_value_write(vec, buffer, 0);

	as_bytes_init_wrap(bytes, buffer, size, true);
	as_bytes_set_type(bytes, AS_BYTES_VECTOR);
	return true;
}
