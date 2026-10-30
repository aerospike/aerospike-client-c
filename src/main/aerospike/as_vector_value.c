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
as_vector_value_alloc(as_vector_element_type element_type, uint32_t count, uint32_t elem_size)
{
	as_vector_value* vec = (as_vector_value*)cf_malloc(sizeof(as_vector_value));
	vec->version = AS_VECTOR_VALUE_VERSION;
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

// Error messages match the server's vector validation messages where one exists.
static bool
as_vector_value_check_dimensions(
	as_error* err, as_vector_element_type element_type, uint32_t dimensions
	)
{
	if (dimensions == 0) {
		as_error_set_message(err, AEROSPIKE_ERR_PARAM, "vector: must have at least 1 dimension");
		return false;
	}

	uint32_t max_dimensions = as_vector_element_type_max_dimensions(element_type);

	if (dimensions > max_dimensions) {
		as_error_update(err, AEROSPIKE_ERR_PARAM,
			"vector: %u dimensions exceed max %u for element type %u", dimensions,
			max_dimensions, (uint32_t)element_type);
		return false;
	}
	return true;
}

static bool
as_vector_value_check_new(
	as_error* err, as_vector_element_type element_type, uint32_t count, const void* data
	)
{
	as_error_reset(err);

	if (data == NULL) {
		as_error_set_message(err, AEROSPIKE_ERR_PARAM, "vector: data is NULL");
		return false;
	}
	return as_vector_value_check_dimensions(err, element_type, count);
}

static as_vector_value*
as_vector_value_new(
	as_error* err, as_vector_element_type element_type, uint32_t count, const void* data,
	uint32_t elem_size
	)
{
	if (! as_vector_value_check_new(err, element_type, count, data)) {
		return NULL;
	}

	as_vector_value* vec = as_vector_value_alloc(element_type, count, elem_size);
	memcpy(vec->data, data, (size_t)count * elem_size);
	return vec;
}

// IEEE 754 binary32 to binary16 with round-ties-to-even. float is binary32 on
// every supported platform. memcpy reads its bit pattern.
static uint16_t
as_vector_float32_to_float16_bits(float v)
{
	uint32_t bits;
	memcpy(&bits, &v, sizeof(bits));

	uint16_t sign = (uint16_t)((bits >> 16) & 0x8000);
	int32_t exponent = (int32_t)((bits >> 23) & 0xff);
	uint32_t mantissa = bits & 0x7fffffu;

	if (exponent == 0xff) {
		// Infinity keeps a zero mantissa. NaN becomes a quiet NaN.
		uint16_t inf_or_nan = mantissa == 0 ? 0x7c00 : 0x7e00;
		return (uint16_t)(sign | inf_or_nan);
	}

	int32_t exp = exponent - 127 + 15;

	if (exp >= 0x1f) {
		return (uint16_t)(sign | 0x7c00);
	}

	if (exp <= 0) {
		// Subnormal binary16, or zero when the value is too small.
		if (exp < -10) {
			return sign;
		}

		uint32_t m = mantissa | 0x800000u;
		uint32_t shift = (uint32_t)(14 - exp);
		uint16_t half_mant = (uint16_t)(m >> shift);
		uint32_t remainder = m & ((1u << shift) - 1u);
		uint32_t halfway = 1u << (shift - 1u);

		if (remainder > halfway || (remainder == halfway && (half_mant & 1u))) {
			half_mant++;
		}
		return (uint16_t)(sign | half_mant);
	}

	uint16_t half_mant = (uint16_t)(mantissa >> 13);
	uint32_t round_bit = (mantissa >> 12) & 1u;
	uint32_t sticky = mantissa & 0xfffu;

	if (round_bit && (sticky || (half_mant & 1u))) {
		half_mant++;

		if (half_mant == 0x400) {
			half_mant = 0;
			exp++;

			if (exp >= 0x1f) {
				return (uint16_t)(sign | 0x7c00);
			}
		}
	}
	return (uint16_t)(sign | (uint16_t)(exp << 10) | half_mant);
}

// IEEE 754 binary16 to binary32. Every binary16 value is exact in binary32.
static float
as_vector_float16_bits_to_float32(uint16_t half)
{
	uint32_t sign = (uint32_t)(half & 0x8000) << 16;
	int32_t exp = (half >> 10) & 0x1f;
	uint32_t mant = half & 0x3ff;
	uint32_t bits;

	if (exp == 0 && mant == 0) {
		bits = sign;
	}
	else if (exp == 0) {
		// Normalize a subnormal binary16.
		int32_t e = 127 - 14;

		while ((mant & 0x400) == 0) {
			mant <<= 1;
			e--;
		}
		mant &= 0x3ff;
		bits = sign | ((uint32_t)e << 23) | (mant << 13);
	}
	else if (exp == 0x1f) {
		bits = sign | 0x7f800000u | (mant << 13);
	}
	else {
		bits = sign | ((uint32_t)(exp + (127 - 15)) << 23) | (mant << 13);
	}

	float v;
	memcpy(&v, &bits, sizeof(v));
	return v;
}

static uint32_t
as_vector_value_get_wire_size(const as_vector_value* vec)
{
	return AS_VECTOR_VALUE_HEADER_SIZE + vec->dimensions * as_vector_element_type_size(vec->element_type);
}

static void
as_vector_value_write(const as_vector_value* vec, uint8_t* buffer)
{
	uint32_t pos = 0;

	// Vector wire format is little-endian to match the server.
	buffer[pos++] = vec->version;
	buffer[pos++] = (uint8_t)vec->element_type;

	uint32_t dims_le = cf_swap_to_le32(vec->dimensions);
	memcpy(&buffer[pos], &dims_le, sizeof(dims_le));
	pos += sizeof(dims_le);

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
}

//---------------------------------
// Functions
//---------------------------------

as_vector_value*
as_vector_value_new_float16(as_error* err, const uint16_t* data, uint32_t count)
{
	return as_vector_value_new(err, AS_VECTOR_ELEMENT_TYPE_FLOAT16, count, data, sizeof(uint16_t));
}

as_vector_value*
as_vector_value_new_float16_from_float(as_error* err, const float* data, uint32_t count)
{
	if (! as_vector_value_check_new(err, AS_VECTOR_ELEMENT_TYPE_FLOAT16, count, data)) {
		return NULL;
	}

	as_vector_value* vec = as_vector_value_alloc(AS_VECTOR_ELEMENT_TYPE_FLOAT16, count,
		sizeof(uint16_t));
	uint16_t* dst = (uint16_t*)vec->data;

	for (uint32_t i = 0; i < count; i++) {
		uint16_t bits = as_vector_float32_to_float16_bits(data[i]);

		// An all-ones exponent is infinity or NaN, which a vector cannot hold.
		if ((bits & 0x7c00) == 0x7c00) {
			as_vector_value_destroy(vec);
			as_error_update(err, AEROSPIKE_ERR_PARAM,
				"vector: element %u (%g) is not finite as float16", i, (double)data[i]);
			return NULL;
		}
		dst[i] = bits;
	}
	return vec;
}

as_status
as_vector_value_get_float16_as_float(
	as_error* err, const as_vector_value* vec, float* out, uint32_t count
	)
{
	as_error_reset(err);

	if (vec == NULL || vec->element_type != AS_VECTOR_ELEMENT_TYPE_FLOAT16) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "vector: element type is not float16");
	}

	if (out == NULL) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "vector: out is NULL");
	}

	if (count < vec->dimensions) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
			"vector: out has %u elements, %u needed", count, vec->dimensions);
	}

	const uint16_t* src = (const uint16_t*)vec->data;

	for (uint32_t i = 0; i < vec->dimensions; i++) {
		out[i] = as_vector_float16_bits_to_float32(src[i]);
	}
	return AEROSPIKE_OK;
}

as_vector_value*
as_vector_value_new_int32(as_error* err, const int32_t* data, uint32_t count)
{
	return as_vector_value_new(err, AS_VECTOR_ELEMENT_TYPE_INT32, count, data, sizeof(int32_t));
}

as_vector_value*
as_vector_value_new_float32(as_error* err, const float* data, uint32_t count)
{
	return as_vector_value_new(err, AS_VECTOR_ELEMENT_TYPE_FLOAT32, count, data, sizeof(float));
}

as_vector_value*
as_vector_value_new_float64(as_error* err, const double* data, uint32_t count)
{
	return as_vector_value_new(err, AS_VECTOR_ELEMENT_TYPE_FLOAT64, count, data, sizeof(double));
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

bool
as_vector_value_to_bytes(const as_vector_value* vec, as_bytes* bytes)
{
	if (! as_vector_value_is_valid(vec) || bytes == NULL) {
		return false;
	}

	uint32_t size = as_vector_value_get_wire_size(vec);
	uint8_t* buffer = (uint8_t*)cf_malloc(size);
	as_vector_value_write(vec, buffer);

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
	as_vector_value_write(vec, buffer);

	as_bytes* bytes = as_bytes_new_wrap(buffer, size, true);

	if (bytes) {
		as_bytes_set_type(bytes, AS_BYTES_VECTOR);
	}
	else {
		cf_free(buffer);
	}
	return bytes;
}

as_vector_value*
as_vector_value_from_bytes(as_error* err, const as_bytes* bytes)
{
	as_error_reset(err);

	if (bytes == NULL || bytes->value == NULL) {
		as_error_set_message(err, AEROSPIKE_ERR_PARAM, "vector: bytes is NULL");
		return NULL;
	}

	const uint8_t* buffer = bytes->value;
	uint32_t length = bytes->size;

	if (length < AS_VECTOR_VALUE_HEADER_SIZE) {
		as_error_update(err, AEROSPIKE_ERR_PARAM,
			"vector: value size %u smaller than %u-byte header", length,
			AS_VECTOR_VALUE_HEADER_SIZE);
		return NULL;
	}

	uint32_t pos = 0;

	// Vector wire format is little-endian to match the server.
	uint8_t version = buffer[pos++];

	if (version != AS_VECTOR_VALUE_VERSION) {
		as_error_update(err, AEROSPIKE_ERR_PARAM,
			"vector: unsupported format version %u (expected %u)", (uint32_t)version,
			AS_VECTOR_VALUE_VERSION);
		return NULL;
	}

	as_vector_element_type element_type = (as_vector_element_type)buffer[pos++];
	uint32_t elem_size = as_vector_element_type_size(element_type);

	if (elem_size == 0) {
		as_error_update(err, AEROSPIKE_ERR_PARAM, "vector: invalid element type %u",
			(uint32_t)element_type);
		return NULL;
	}

	uint32_t dims_le;
	memcpy(&dims_le, &buffer[pos], sizeof(dims_le));
	pos += sizeof(dims_le);
	uint32_t dimensions = cf_swap_from_le32(dims_le);

	if (! as_vector_value_check_dimensions(err, element_type, dimensions)) {
		return NULL;
	}

	uint32_t expected_size = AS_VECTOR_VALUE_HEADER_SIZE + dimensions * elem_size;

	if (length < expected_size) {
		as_error_update(err, AEROSPIKE_ERR_PARAM,
			"vector: value size %u does not match %u (%u dimensions of %u bytes, little-endian)",
			length, expected_size, dimensions, elem_size);
		return NULL;
	}

	as_vector_value* vec = as_vector_value_alloc(element_type, dimensions, elem_size);

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

