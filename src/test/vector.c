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
#include <aerospike/as_bytes.h>
#include <aerospike/as_vector_value.h>
#include <citrusleaf/cf_byte_order.h>
#include <string.h>

#include "test.h"

//---------------------------------
// Test Cases
//---------------------------------

TEST(vector_wire_size, "as_vector_value wire size per element type")
{
	uint16_t f16[3] = {0, 0, 0};
	int32_t i32[3] = {0, 0, 0};
	float f32[3] = {0, 0, 0};
	double f64[3] = {0, 0, 0};

	as_vector_value* v16 = as_vector_value_new_float16(f16, 3);
	as_vector_value* v32i = as_vector_value_new_int32(i32, 3);
	as_vector_value* v32f = as_vector_value_new_float32(f32, 3);
	as_vector_value* v64 = as_vector_value_new_float64(f64, 3);

	assert_int_eq(as_vector_value_get_wire_size(v16), AS_VECTOR_VALUE_HEADER_SIZE + 3 * 2);
	assert_int_eq(as_vector_value_get_wire_size(v32i), AS_VECTOR_VALUE_HEADER_SIZE + 3 * 4);
	assert_int_eq(as_vector_value_get_wire_size(v32f), AS_VECTOR_VALUE_HEADER_SIZE + 3 * 4);
	assert_int_eq(as_vector_value_get_wire_size(v64), AS_VECTOR_VALUE_HEADER_SIZE + 3 * 8);

	as_vector_value_destroy(v16);
	as_vector_value_destroy(v32i);
	as_vector_value_destroy(v32f);
	as_vector_value_destroy(v64);
}

TEST(vector_header, "as_vector_value serializes the fixed header")
{
	float data[4] = {1.5f, -2.25f, 0.0f, 100.0f};
	as_vector_value* vec = as_vector_value_new_float32(data, 4);

	uint8_t buf[64];
	uint32_t written = as_vector_value_write(vec, buf, 0);

	assert_int_eq(written, as_vector_value_get_wire_size(vec));
	assert_int_eq(buf[0], AS_VECTOR_VALUE_VERSION);
	assert_int_eq(buf[1], AS_VECTOR_ELEMENT_TYPE_FLOAT32);

	uint32_t dims;
	memcpy(&dims, &buf[2], sizeof(dims));
	assert_int_eq(cf_swap_from_le32(dims), 4);

	// Reserved bytes must be zero.
	assert_int_eq(buf[6], 0);
	assert_int_eq(buf[7], 0);

	as_vector_value_destroy(vec);
}

TEST(vector_float32_roundtrip, "as_vector_value float32 data round-trips")
{
	float data[4] = {1.5f, -2.25f, 0.0f, 100.0f};
	as_vector_value* vec = as_vector_value_new_float32(data, 4);

	uint8_t buf[64];
	as_vector_value_write(vec, buf, 0);

	for (uint32_t i = 0; i < 4; i++) {
		uint32_t bits;
		memcpy(&bits, &buf[AS_VECTOR_VALUE_HEADER_SIZE + i * 4], sizeof(bits));
		bits = cf_swap_from_le32(bits);

		float out;
		memcpy(&out, &bits, sizeof(out));
		assert(out == data[i]);
	}

	as_vector_value_destroy(vec);
}

TEST(vector_int32_roundtrip, "as_vector_value int32 data round-trips")
{
	int32_t data[4] = {0, -1, 2147483647, -2147483648};
	as_vector_value* vec = as_vector_value_new_int32(data, 4);

	uint8_t buf[64];
	as_vector_value_write(vec, buf, 0);

	assert_int_eq(buf[1], AS_VECTOR_ELEMENT_TYPE_INT32);

	for (uint32_t i = 0; i < 4; i++) {
		uint32_t bits;
		memcpy(&bits, &buf[AS_VECTOR_VALUE_HEADER_SIZE + i * 4], sizeof(bits));
		assert_int_eq((int32_t)cf_swap_from_le32(bits), data[i]);
	}

	as_vector_value_destroy(vec);
}

TEST(vector_float64_roundtrip, "as_vector_value float64 data round-trips")
{
	double data[3] = {3.14159265358979, -0.5, 1e300};
	as_vector_value* vec = as_vector_value_new_float64(data, 3);

	uint8_t buf[64];
	as_vector_value_write(vec, buf, 0);

	assert_int_eq(buf[1], AS_VECTOR_ELEMENT_TYPE_FLOAT64);

	for (uint32_t i = 0; i < 3; i++) {
		double out;
		memcpy(&out, &buf[AS_VECTOR_VALUE_HEADER_SIZE + i * 8], sizeof(out));
		out = cf_swap_from_little_float64(out);
		assert(out == data[i]);
	}

	as_vector_value_destroy(vec);
}

TEST(vector_float16_roundtrip, "as_vector_value float16 raw bit patterns round-trip")
{
	uint16_t data[3] = {0x3C00, 0xC000, 0x0000}; // 1.0, -2.0, 0.0 as IEEE 754 half
	as_vector_value* vec = as_vector_value_new_float16(data, 3);

	uint8_t buf[64];
	as_vector_value_write(vec, buf, 0);

	assert_int_eq(buf[1], AS_VECTOR_ELEMENT_TYPE_FLOAT16);

	for (uint32_t i = 0; i < 3; i++) {
		uint16_t bits;
		memcpy(&bits, &buf[AS_VECTOR_VALUE_HEADER_SIZE + i * 2], sizeof(bits));
		assert_int_eq(cf_swap_from_le16(bits), data[i]);
	}

	as_vector_value_destroy(vec);
}

TEST(vector_write_offset, "as_vector_value_write honors a non-zero offset")
{
	float data[2] = {1.0f, 2.0f};
	as_vector_value* vec = as_vector_value_new_float32(data, 2);

	uint32_t size = as_vector_value_get_wire_size(vec);
	uint8_t buf[64];
	memset(buf, 0xAB, sizeof(buf));

	const uint32_t offset = 5;
	uint32_t written = as_vector_value_write(vec, buf, offset);

	assert_int_eq(written, size);

	// Bytes preceding the offset must be untouched.
	for (uint32_t i = 0; i < offset; i++) {
		assert_int_eq(buf[i], 0xAB);
	}

	// Header must start at the offset.
	assert_int_eq(buf[offset], AS_VECTOR_VALUE_VERSION);

	as_vector_value_destroy(vec);
}

TEST(vector_to_bytes, "as_vector_value_to_bytes produces AS_BYTES_VECTOR")
{
	float data[4] = {1.5f, -2.25f, 0.0f, 100.0f};
	as_vector_value* vec = as_vector_value_new_float32(data, 4);

	uint32_t size = as_vector_value_get_wire_size(vec);
	uint8_t expected[64];
	as_vector_value_write(vec, expected, 0);

	as_bytes bytes;
	assert(as_vector_value_to_bytes(vec, &bytes));

	assert_int_eq(as_bytes_get_type(&bytes), AS_BYTES_VECTOR);
	assert_int_eq(bytes.size, size);
	assert_bytes_eq(bytes.value, bytes.size, expected, size);

	as_bytes_destroy(&bytes);
	as_vector_value_destroy(vec);
}

//---------------------------------
// Test Suite
//---------------------------------

SUITE(vector, "as_vector_value serialization")
{
	suite_add(vector_wire_size);
	suite_add(vector_header);
	suite_add(vector_float32_roundtrip);
	suite_add(vector_int32_roundtrip);
	suite_add(vector_float64_roundtrip);
	suite_add(vector_float16_roundtrip);
	suite_add(vector_write_offset);
	suite_add(vector_to_bytes);
}
