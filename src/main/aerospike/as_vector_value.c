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
as_vector_value_alloc(as_vector_element_type element_type, uint8_t version, uint32_t count, uint32_t elem_size)
{
	as_vector_value* vec = (as_vector_value*)cf_malloc(sizeof(as_vector_value));
	vec->version = version;
	vec->element_type = element_type;
	vec->dimensions = count;
	vec->free_data = true;

	size_t size = (size_t)count * elem_size;
	vec->data = cf_malloc(size > 0 ? size : 1);
	return vec;
}

static bool
as_vector_value_is_valid(const as_vector_value* vec)
{
	return vec != NULL &&
		vec->version == AS_VECTOR_VALUE_VERSION &&
		vec->data != NULL &&
		vec->dimensions > 0 &&
		vec->dimensions <= as_vector_element_type_max_dimensions(vec->element_type);
}

static as_vector_value*
as_vector_value_new(as_vector_element_type element_type, uint32_t count, const void* data, uint32_t elem_size)
{
	if (data == NULL || count == 0 ||
			count > as_vector_element_type_max_dimensions(element_type)) {
		return NULL;
	}

	as_vector_value* vec = as_vector_value_alloc(element_type, AS_VECTOR_VALUE_VERSION, count, elem_size);
	memcpy(vec->data, data, (size_t)count * elem_size);
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
	if (! as_vector_value_is_valid(vec)) {
		return 0;
	}

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
	if (! as_vector_value_is_valid(vec) || bytes == NULL) {
		return false;
	}

	uint32_t size = as_vector_value_get_wire_size(vec);
	uint8_t* buffer = (uint8_t*)cf_malloc(size);
	as_vector_value_write(vec, buffer, 0);

	as_bytes_init_wrap(bytes, buffer, size, true);
	as_bytes_set_type(bytes, AS_BYTES_VECTOR);
	return true;
}

as_bytes*
as_vector_value_to_bytes_new(const as_vector_value* vec)
{
	if (! as_vector_value_is_valid(vec)) {
		return NULL;
	}

	uint32_t size = as_vector_value_get_wire_size(vec);
	uint8_t* buffer = (uint8_t*)cf_malloc(size);
	as_vector_value_write(vec, buffer, 0);

	as_bytes* bytes = as_bytes_new_wrap(buffer, size, true);

	if (bytes) {
		as_bytes_set_type(bytes, AS_BYTES_VECTOR);
	}
	else {
		cf_free(buffer);
	}
	return bytes;
}

bool
as_vector_value_append_to_list(as_list* list, const as_vector_value* vec)
{
	if (list == NULL) {
		return false;
	}

	as_bytes* bytes = as_vector_value_to_bytes_new(vec);

	if (bytes == NULL) {
		return false;
	}

	if (as_list_append(list, (as_val*)bytes) != 0) {
		as_bytes_destroy(bytes);
		return false;
	}
	return true;
}

bool
as_vector_value_set_in_map(as_map* map, const as_val* key, const as_vector_value* vec)
{
	if (map == NULL || key == NULL) {
		return false;
	}

	as_bytes* bytes = as_vector_value_to_bytes_new(vec);

	if (bytes == NULL) {
		return false;
	}

	if (as_map_set(map, key, (as_val*)bytes) != 0) {
		as_bytes_destroy(bytes);
		return false;
	}
	return true;
}

as_vector_value*
as_vector_value_from(const uint8_t* buffer, uint32_t offset, uint32_t length)
{
	if (buffer == NULL || length < AS_VECTOR_VALUE_HEADER_SIZE) {
		return NULL;
	}

	uint32_t pos = offset;

	// Vector wire format is little-endian to match the server.
	uint8_t version = buffer[pos++];
	as_vector_element_type element_type = (as_vector_element_type)buffer[pos++];

	uint32_t elem_size = as_vector_element_type_size(element_type);
	if (elem_size == 0) {
		// Unknown/unsupported element type.
		return NULL;
	}

	uint32_t dims_le;
	memcpy(&dims_le, &buffer[pos], sizeof(dims_le));
	pos += sizeof(dims_le);
	uint32_t dimensions = cf_swap_from_le32(dims_le);

	pos += 2; // skip reserved bytes

	if (dimensions == 0 ||
		dimensions > as_vector_element_type_max_dimensions(element_type)) {
		return NULL;
	}

	uint32_t data_size = dimensions * elem_size;

	if (length < AS_VECTOR_VALUE_HEADER_SIZE + data_size) {
		return NULL;
	}

	as_vector_value* vec = as_vector_value_alloc(element_type, version, dimensions, elem_size);

	uint32_t i;

	switch (element_type) {
	case AS_VECTOR_ELEMENT_TYPE_FLOAT16: {
		uint16_t* dst = (uint16_t*)vec->data;
		for (i = 0; i < dimensions; i++) {
			uint16_t le;
			memcpy(&le, &buffer[pos], sizeof(le));
			dst[i] = cf_swap_from_le16(le);
			pos += sizeof(le);
		}
		break;
	}
	case AS_VECTOR_ELEMENT_TYPE_INT32: {
		int32_t* dst = (int32_t*)vec->data;
		for (i = 0; i < dimensions; i++) {
			uint32_t le;
			memcpy(&le, &buffer[pos], sizeof(le));
			dst[i] = (int32_t)cf_swap_from_le32(le);
			pos += sizeof(le);
		}
		break;
	}
	case AS_VECTOR_ELEMENT_TYPE_FLOAT32: {
		float* dst = (float*)vec->data;
		for (i = 0; i < dimensions; i++) {
			union { float f; uint32_t u; } conv;
			memcpy(&conv.u, &buffer[pos], sizeof(conv.u));
			conv.u = cf_swap_from_le32(conv.u);
			dst[i] = conv.f;
			pos += sizeof(conv.u);
		}
		break;
	}
	case AS_VECTOR_ELEMENT_TYPE_FLOAT64: {
		double* dst = (double*)vec->data;
		for (i = 0; i < dimensions; i++) {
			double le;
			memcpy(&le, &buffer[pos], sizeof(le));
			dst[i] = cf_swap_from_little_float64(le);
			pos += sizeof(le);
		}
		break;
	}
	default:
		break;
	}

	return vec;
}

as_vector_value*
as_vector_value_from_bytes(const as_bytes* bytes)
{
	if (bytes == NULL) {
		return NULL;
	}
	return as_vector_value_from(bytes->value, 0, bytes->size);
}
