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
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_batch.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_batch.h>
#include <aerospike/as_buffer.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_error.h>
#include <aerospike/as_event.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_exp_operations.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_list_operations.h>
#include <aerospike/as_map_operations.h>
#if AS_EVENT_LIB_DEFINED
#include <aerospike/as_monitor.h>
#endif
#include <aerospike/as_operations.h>
#include <aerospike/as_record.h>
#include <aerospike/as_msgpack.h>
#include <aerospike/as_serializer.h>
#include <aerospike/as_string.h>
#include <aerospike/as_vector_value.h>
#include <citrusleaf/cf_byte_order.h>
#include <float.h>
#include <string.h>

#include "test.h"

extern aerospike* as;

#define NAMESPACE "test"
#define SET "test_vector"

//---------------------------------
// Helpers
//---------------------------------

// Assert that an as_bytes holds the same float32 vector as `expected`.
static void
assert_vector_bytes_eq_float32(atf_test_result* __result__, as_bytes* bytes, const float* expected, uint32_t count)
{
	assert_not_null(bytes);
	assert_int_eq(bytes->type, AS_BYTES_VECTOR);

	as_vector_value* vec = as_vector_value_from_bytes(bytes);
	assert_not_null(vec);
	assert_int_eq(vec->element_type, AS_VECTOR_ELEMENT_TYPE_FLOAT32);
	assert_int_eq(vec->dimensions, count);

	const float* elems = as_vector_value_get_float32(vec);
	assert_not_null(elems);
	for (uint32_t i = 0; i < count; i++) {
		assert(elems[i] == expected[i]);
	}
	as_vector_value_destroy(vec);
}

#if AS_EVENT_LIB_DEFINED
typedef struct vector_async_data_s {
	as_monitor monitor;
	atf_test_result* result;
	float expected[2];
} vector_async_data;

static void
vector_async_get_callback(as_error* err, as_record* rec, void* udata, as_event_loop* event_loop)
{
	vector_async_data* data = (vector_async_data*)udata;
	assert_success_async(&data->monitor, err, data->result);

	assert_async(&data->monitor, rec);
	as_vector_value* vec = as_record_get_vector(rec, "v");
	assert_async(&data->monitor, vec);

	if (vec) {
		assert_int_eq_async(&data->monitor, vec->element_type, AS_VECTOR_ELEMENT_TYPE_FLOAT32);
		assert_int_eq_async(&data->monitor, vec->dimensions, 2);
		const float* values = as_vector_value_get_float32(vec);
		assert_async(&data->monitor, values);
		assert_async(&data->monitor, values[0] == data->expected[0]);
		assert_async(&data->monitor, values[1] == data->expected[1]);
		as_vector_value_destroy(vec);
	}
	as_monitor_notify(&data->monitor);
}

static void
vector_async_put_callback(as_error* err, void* udata, as_event_loop* event_loop)
{
	vector_async_data* data = (vector_async_data*)udata;
	assert_success_async(&data->monitor, err, data->result);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 25);

	as_error get_err;
	as_status status = aerospike_key_get_async(
		as, &get_err, NULL, &key, vector_async_get_callback, data, event_loop, NULL);
	assert_status_async(&data->monitor, status, &get_err);
}
#endif

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

TEST(vector_from_float32, "as_vector_value_from deserializes float32 elements")
{
	float data[4] = {1.5f, -2.25f, 0.0f, 100.0f};
	as_vector_value* vec = as_vector_value_new_float32(data, 4);

	uint8_t buf[64];
	uint32_t size = as_vector_value_write(vec, buf, 0);

	as_vector_value* out = as_vector_value_from(buf, 0, size);
	assert_not_null(out);
	assert_int_eq(out->version, AS_VECTOR_VALUE_VERSION);
	assert_int_eq(out->element_type, AS_VECTOR_ELEMENT_TYPE_FLOAT32);
	assert_int_eq(out->dimensions, 4);

	const float* elems = as_vector_value_get_float32(out);
	assert_not_null(elems);
	for (uint32_t i = 0; i < 4; i++) {
		assert(elems[i] == data[i]);
	}

	// Wrong-type accessors must return NULL.
	assert_null(as_vector_value_get_int32(out));
	assert_null(as_vector_value_get_float64(out));
	assert_null(as_vector_value_get_float16(out));

	as_vector_value_destroy(out);
	as_vector_value_destroy(vec);
}

TEST(vector_from_int32, "as_vector_value_from deserializes int32 elements")
{
	int32_t data[4] = {0, -1, 2147483647, -2147483648};
	as_vector_value* vec = as_vector_value_new_int32(data, 4);

	uint8_t buf[64];
	uint32_t size = as_vector_value_write(vec, buf, 0);

	as_vector_value* out = as_vector_value_from(buf, 0, size);
	assert_not_null(out);
	assert_int_eq(out->element_type, AS_VECTOR_ELEMENT_TYPE_INT32);
	assert_int_eq(out->dimensions, 4);

	const int32_t* elems = as_vector_value_get_int32(out);
	assert_not_null(elems);
	for (uint32_t i = 0; i < 4; i++) {
		assert_int_eq(elems[i], data[i]);
	}

	as_vector_value_destroy(out);
	as_vector_value_destroy(vec);
}

TEST(vector_from_float64, "as_vector_value_from deserializes float64 elements")
{
	double data[3] = {3.14159265358979, -0.5, 1e300};
	as_vector_value* vec = as_vector_value_new_float64(data, 3);

	uint8_t buf[64];
	uint32_t size = as_vector_value_write(vec, buf, 0);

	as_vector_value* out = as_vector_value_from(buf, 0, size);
	assert_not_null(out);
	assert_int_eq(out->element_type, AS_VECTOR_ELEMENT_TYPE_FLOAT64);
	assert_int_eq(out->dimensions, 3);

	const double* elems = as_vector_value_get_float64(out);
	assert_not_null(elems);
	for (uint32_t i = 0; i < 3; i++) {
		assert(elems[i] == data[i]);
	}

	as_vector_value_destroy(out);
	as_vector_value_destroy(vec);
}

TEST(vector_from_float16, "as_vector_value_from deserializes float16 raw bit patterns")
{
	uint16_t data[3] = {0x3C00, 0xC000, 0x0000}; // 1.0, -2.0, 0.0 as IEEE 754 half
	as_vector_value* vec = as_vector_value_new_float16(data, 3);

	uint8_t buf[64];
	uint32_t size = as_vector_value_write(vec, buf, 0);

	as_vector_value* out = as_vector_value_from(buf, 0, size);
	assert_not_null(out);
	assert_int_eq(out->element_type, AS_VECTOR_ELEMENT_TYPE_FLOAT16);
	assert_int_eq(out->dimensions, 3);

	const uint16_t* elems = as_vector_value_get_float16(out);
	assert_not_null(elems);
	for (uint32_t i = 0; i < 3; i++) {
		assert_int_eq(elems[i], data[i]);
	}

	as_vector_value_destroy(out);
	as_vector_value_destroy(vec);
}

TEST(vector_from_offset, "as_vector_value_from honors a non-zero offset")
{
	float data[2] = {1.0f, 2.0f};
	as_vector_value* vec = as_vector_value_new_float32(data, 2);

	const uint32_t offset = 5;
	uint8_t buf[64];
	memset(buf, 0xAB, sizeof(buf));
	uint32_t size = as_vector_value_write(vec, buf, offset);

	as_vector_value* out = as_vector_value_from(buf, offset, size);
	assert_not_null(out);
	assert_int_eq(out->dimensions, 2);

	const float* elems = as_vector_value_get_float32(out);
	assert_not_null(elems);
	assert(elems[0] == 1.0f);
	assert(elems[1] == 2.0f);

	as_vector_value_destroy(out);
	as_vector_value_destroy(vec);
}

TEST(vector_from_bytes_roundtrip, "as_vector_value_from_bytes round-trips as_vector_value_to_bytes")
{
	float data[4] = {1.5f, -2.25f, 0.0f, 100.0f};
	as_vector_value* vec = as_vector_value_new_float32(data, 4);

	as_bytes bytes;
	assert(as_vector_value_to_bytes(vec, &bytes));
	assert_int_eq(as_bytes_get_type(&bytes), AS_BYTES_VECTOR);

	as_vector_value* out = as_vector_value_from_bytes(&bytes);
	assert_not_null(out);
	assert_int_eq(out->element_type, AS_VECTOR_ELEMENT_TYPE_FLOAT32);
	assert_int_eq(out->dimensions, 4);

	const float* elems = as_vector_value_get_float32(out);
	assert_not_null(elems);
	for (uint32_t i = 0; i < 4; i++) {
		assert(elems[i] == data[i]);
	}

	as_vector_value_destroy(out);
	as_bytes_destroy(&bytes);
	as_vector_value_destroy(vec);
}

TEST(vector_from_invalid, "as_vector_value_from rejects malformed buffers")
{
	uint8_t buf[64];
	memset(buf, 0, sizeof(buf));

	// Length shorter than the fixed header.
	assert_null(as_vector_value_from(buf, 0, AS_VECTOR_VALUE_HEADER_SIZE - 1));

	// NULL buffer.
	assert_null(as_vector_value_from(NULL, 0, 32));

	// Unknown element type.
	buf[0] = AS_VECTOR_VALUE_VERSION;
	buf[1] = 0x7F; // not a valid as_vector_element_type
	assert_null(as_vector_value_from(buf, 0, sizeof(buf)));

	// Valid type but dimensions exceed the available length.
	float data[2] = {1.0f, 2.0f};
	as_vector_value* vec = as_vector_value_new_float32(data, 2);
	uint32_t size = as_vector_value_write(vec, buf, 0);
	assert_null(as_vector_value_from(buf, 0, size - 1));
	as_vector_value_destroy(vec);
}

TEST(vector_from_limits, "as_vector_value_from rejects empty and oversized vectors before allocation")
{
	uint8_t buf[AS_VECTOR_VALUE_HEADER_SIZE] = {0};
	buf[0] = AS_VECTOR_VALUE_VERSION;
	buf[1] = AS_VECTOR_ELEMENT_TYPE_FLOAT32;

	// The constructors and server reject empty vectors.
	assert_null(as_vector_value_from(buf, 0, sizeof(buf)));

	uint32_t dimensions = cf_swap_to_le32(
		as_vector_element_type_max_dimensions(AS_VECTOR_ELEMENT_TYPE_FLOAT32) + 1);
	memcpy(&buf[2], &dimensions, sizeof(dimensions));
	assert_null(as_vector_value_from(buf, 0, sizeof(buf)));
}

TEST(vector_from_unknown_version, "as_vector_value_from preserves a future wire version")
{
	float data[2] = {1.0f, 2.0f};
	as_vector_value* vec = as_vector_value_new_float32(data, 2);
	uint8_t buf[64];
	uint32_t size = as_vector_value_write(vec, buf, 0);
	buf[0] = AS_VECTOR_VALUE_VERSION + 1;

	as_vector_value* out = as_vector_value_from(buf, 0, size);
	assert_not_null(out);
	assert_int_eq(out->version, AS_VECTOR_VALUE_VERSION + 1);
	assert(as_vector_value_get_float32(out)[1] == data[1]);

	as_vector_value_destroy(out);
	as_vector_value_destroy(vec);
}

TEST(vector_to_bytes_new, "as_vector_value_to_bytes_new produces a heap AS_BYTES_VECTOR")
{
	float data[3] = {1.5f, -2.25f, 3.14159f};
	as_vector_value* vec = as_vector_value_new_float32(data, 3);

	as_bytes* bytes = as_vector_value_to_bytes_new(vec);
	assert_not_null(bytes);
	assert_int_eq(bytes->type, AS_BYTES_VECTOR);
	assert_int_eq(bytes->size, as_vector_value_get_wire_size(vec));

	assert_vector_bytes_eq_float32(__result__, bytes, data, 3);

	// as_vector_value_to_bytes_new returns a heap as_bytes (an as_val), so
	// as_bytes_destroy() releases both the buffer and the struct.
	as_bytes_destroy(bytes);
	as_vector_value_destroy(vec);
}

TEST(vector_nested_in_list, "vector nested in a list round-trips through msgpack")
{
	float data[3] = {1.5f, -2.25f, 3.14159f};
	as_vector_value* vec = as_vector_value_new_float32(data, 3);

	as_arraylist list;
	as_arraylist_init(&list, 2, 0);
	as_arraylist_append_str(&list, "label");
	assert_true(as_vector_value_append_to_list((as_list*)&list, vec));

	as_serializer ser;
	as_msgpack_init(&ser);

	as_buffer buffer;
	as_buffer_init(&buffer);
	assert_int_eq(as_serializer_serialize(&ser, (as_val*)&list, &buffer), 0);

	as_val* out = NULL;
	assert_int_eq(as_serializer_deserialize(&ser, &buffer, &out), 0);
	assert_not_null(out);
	assert_int_eq(as_val_type(out), AS_LIST);

	as_arraylist* olist = (as_arraylist*)out;
	assert_int_eq(as_arraylist_size(olist), 2);

	as_val* el = as_arraylist_get(olist, 1);
	assert_int_eq(as_val_type(el), AS_BYTES);
	assert_vector_bytes_eq_float32(__result__, (as_bytes*)el, data, 3);

	as_val_destroy(out);
	as_buffer_destroy(&buffer);
	as_serializer_destroy(&ser);
	as_arraylist_destroy(&list);
	as_vector_value_destroy(vec);
}

TEST(vector_nested_in_map, "vector nested in a map round-trips through msgpack")
{
	float data[2] = {9.0f, -0.5f};
	as_vector_value* vec = as_vector_value_new_float32(data, 2);

	as_hashmap map;
	as_hashmap_init(&map, 1);
	assert_true(as_vector_value_set_in_map((as_map*)&map, (as_val*)as_string_new_strdup("k"), vec));

	as_serializer ser;
	as_msgpack_init(&ser);

	as_buffer buffer;
	as_buffer_init(&buffer);
	assert_int_eq(as_serializer_serialize(&ser, (as_val*)&map, &buffer), 0);

	as_val* out = NULL;
	assert_int_eq(as_serializer_deserialize(&ser, &buffer, &out), 0);
	assert_not_null(out);
	assert_int_eq(as_val_type(out), AS_MAP);

	as_string key;
	as_string_init(&key, "k", false);
	as_val* el = as_map_get((as_map*)out, (as_val*)&key);
	as_string_destroy(&key);

	assert_not_null(el);
	assert_int_eq(as_val_type(el), AS_BYTES);
	assert_vector_bytes_eq_float32(__result__, (as_bytes*)el, data, 2);

	as_val_destroy(out);
	as_buffer_destroy(&buffer);
	as_serializer_destroy(&ser);
	as_hashmap_destroy(&map);
	as_vector_value_destroy(vec);
}

TEST(vector_collection_helpers, "vector collection helpers own wrappers and reject invalid arguments")
{
	float data[2] = {1.0f, 2.0f};
	as_vector_value* vec = as_vector_value_new_float32(data, 2);
	assert_not_null(vec);

	assert_false(as_vector_value_append_to_list(NULL, vec));

	as_arraylist list;
	as_arraylist_init(&list, 0, 0);
	assert_false(as_vector_value_append_to_list((as_list*)&list, vec));
	as_arraylist_destroy(&list);

	assert_false(as_vector_value_set_in_map(NULL, NULL, vec));

	as_hashmap map;
	as_hashmap_init(&map, 1);
	as_double invalid_key;
	as_double_init(&invalid_key, 1.0);
	assert_false(as_vector_value_set_in_map((as_map*)&map, (as_val*)&invalid_key, vec));
	as_double_destroy(&invalid_key);
	as_hashmap_destroy(&map);

	as_vector_value_destroy(vec);
}

TEST(vector_element_type_size, "as_vector_element_type_size reports per-type byte sizes")
{
	assert_int_eq(as_vector_element_type_size(AS_VECTOR_ELEMENT_TYPE_FLOAT16), 2);
	assert_int_eq(as_vector_element_type_size(AS_VECTOR_ELEMENT_TYPE_INT32), 4);
	assert_int_eq(as_vector_element_type_size(AS_VECTOR_ELEMENT_TYPE_FLOAT32), 4);
	assert_int_eq(as_vector_element_type_size(AS_VECTOR_ELEMENT_TYPE_FLOAT64), 8);

	// Unknown element type has no defined size.
	assert_int_eq(as_vector_element_type_size((as_vector_element_type)0x7F), 0);
}

TEST(vector_getter_type_safety, "typed accessors only return data for the matching element type")
{
	uint16_t f16[2] = {0x3C00, 0x0000};
	int32_t i32[2] = {1, 2};
	float f32[2] = {1.0f, 2.0f};
	double f64[2] = {1.0, 2.0};

	as_vector_value* v16 = as_vector_value_new_float16(f16, 2);
	as_vector_value* v32i = as_vector_value_new_int32(i32, 2);
	as_vector_value* v32f = as_vector_value_new_float32(f32, 2);
	as_vector_value* v64 = as_vector_value_new_float64(f64, 2);

	// Matching getter returns data; the other three return NULL.
	assert_not_null(as_vector_value_get_float16(v16));
	assert_null(as_vector_value_get_int32(v16));
	assert_null(as_vector_value_get_float32(v16));
	assert_null(as_vector_value_get_float64(v16));

	assert_not_null(as_vector_value_get_int32(v32i));
	assert_null(as_vector_value_get_float16(v32i));
	assert_null(as_vector_value_get_float32(v32i));
	assert_null(as_vector_value_get_float64(v32i));

	assert_not_null(as_vector_value_get_float32(v32f));
	assert_null(as_vector_value_get_float16(v32f));
	assert_null(as_vector_value_get_int32(v32f));
	assert_null(as_vector_value_get_float64(v32f));

	assert_not_null(as_vector_value_get_float64(v64));
	assert_null(as_vector_value_get_float16(v64));
	assert_null(as_vector_value_get_int32(v64));
	assert_null(as_vector_value_get_float32(v64));

	as_vector_value_destroy(v16);
	as_vector_value_destroy(v32i);
	as_vector_value_destroy(v32f);
	as_vector_value_destroy(v64);
}

TEST(vector_null_safety, "accessors and from_bytes tolerate NULL arguments")
{
	assert_null(as_vector_value_get_float16(NULL));
	assert_null(as_vector_value_get_int32(NULL));
	assert_null(as_vector_value_get_float32(NULL));
	assert_null(as_vector_value_get_float64(NULL));

	assert_null(as_vector_value_from_bytes(NULL));
}

TEST(vector_empty, "zero-dimension vectors are rejected")
{
	uint16_t float16_data[1] = {0};
	int32_t int32_data[1] = {0};
	float float32_data[1] = {0};
	double float64_data[1] = {0};

	assert_null(as_vector_value_new_float16(float16_data, 0));
	assert_null(as_vector_value_new_int32(int32_data, 0));
	assert_null(as_vector_value_new_float32(float32_data, 0));
	assert_null(as_vector_value_new_float64(float64_data, 0));
}

TEST(vector_from_trailing_bytes, "as_vector_value_from ignores bytes past the encoded vector")
{
	float data[3] = {1.5f, -2.25f, 3.14159f};
	as_vector_value* vec = as_vector_value_new_float32(data, 3);

	uint8_t buf[64];
	memset(buf, 0xCD, sizeof(buf)); // trailing junk after the vector
	uint32_t size = as_vector_value_write(vec, buf, 0);

	// Report far more available bytes than the vector actually needs.
	as_vector_value* out = as_vector_value_from(buf, 0, sizeof(buf));
	assert_not_null(out);
	assert_int_eq(out->dimensions, 3);

	const float* elems = as_vector_value_get_float32(out);
	assert_not_null(elems);
	for (uint32_t i = 0; i < 3; i++) {
		assert(elems[i] == data[i]);
	}

	// The exact-size decode must agree with the oversized-length decode.
	as_vector_value* exact = as_vector_value_from(buf, 0, size);
	assert_not_null(exact);
	assert_int_eq(exact->dimensions, 3);

	as_vector_value_destroy(exact);
	as_vector_value_destroy(out);
	as_vector_value_destroy(vec);
}

TEST(vector_from_overflow, "as_vector_value_from rejects dimensions that overflow 32-bit size math")
{
	// dimensions * element_size overflows a uint32_t (0x20000000 * 8 == 0x100000000),
	// which would wrap to 0 with 32-bit math and bypass the length check. The 64-bit
	// guard must reject it instead of attempting a multi-gigabyte read.
	uint8_t buf[16];
	memset(buf, 0, sizeof(buf));
	buf[0] = AS_VECTOR_VALUE_VERSION;
	buf[1] = (uint8_t)AS_VECTOR_ELEMENT_TYPE_FLOAT64; // 8 bytes/element

	uint32_t dims = cf_swap_to_le32(0x20000000u);
	memcpy(&buf[2], &dims, sizeof(dims));

	assert_null(as_vector_value_from(buf, 0, sizeof(buf)));
}

TEST(vector_constructor_limits, "constructors enforce the server dimension limits")
{
	assert_int_eq(as_vector_element_type_max_dimensions(AS_VECTOR_ELEMENT_TYPE_FLOAT16), 131072);
	assert_int_eq(as_vector_element_type_max_dimensions(AS_VECTOR_ELEMENT_TYPE_INT32), 65536);
	assert_int_eq(as_vector_element_type_max_dimensions(AS_VECTOR_ELEMENT_TYPE_FLOAT32), 65536);
	assert_int_eq(as_vector_element_type_max_dimensions(AS_VECTOR_ELEMENT_TYPE_FLOAT64), 32768);
	assert_int_eq(as_vector_element_type_max_dimensions((as_vector_element_type)0x7F), 0);

	uint32_t max = as_vector_element_type_max_dimensions(AS_VECTOR_ELEMENT_TYPE_FLOAT32);
	float* data = calloc(max + 1, sizeof(float));

	as_vector_value* valid = as_vector_value_new_float32(data, max);
	assert_not_null(valid);
	assert_null(as_vector_value_new_float32(data, max + 1));
	assert_null(as_vector_value_new_float32(NULL, 1));

	as_vector_value_destroy(valid);
	free(data);
}

TEST(vector_serialization_rejects_invalid_vectors, "serialization rejects vectors the server would reject")
{
	float data[1] = {1.0f};
	as_vector_value* vec = as_vector_value_new_float32(data, 1);
	uint8_t buffer[16];
	as_bytes bytes;

	vec->version = AS_VECTOR_VALUE_VERSION + 1;
	assert_int_eq(as_vector_value_write(vec, buffer, 0), 0);
	assert_false(as_vector_value_to_bytes(vec, &bytes));
	assert_null(as_vector_value_to_bytes_new(vec));

	vec->version = AS_VECTOR_VALUE_VERSION;
	vec->dimensions = as_vector_element_type_max_dimensions(vec->element_type) + 1;
	assert_int_eq(as_vector_value_write(vec, buffer, 0), 0);
	assert_false(as_vector_value_to_bytes(vec, &bytes));
	assert_null(as_vector_value_to_bytes_new(vec));

	as_vector_value_destroy(vec);
}

TEST(vector_nested_int32_in_list, "nested int32 vector preserves element type through msgpack")
{
	int32_t data[3] = {-5, 0, 12345};
	as_vector_value* vec = as_vector_value_new_int32(data, 3);

	as_arraylist list;
	as_arraylist_init(&list, 1, 0);
	assert_true(as_vector_value_append_to_list((as_list*)&list, vec));

	as_serializer ser;
	as_msgpack_init(&ser);

	as_buffer buffer;
	as_buffer_init(&buffer);
	assert_int_eq(as_serializer_serialize(&ser, (as_val*)&list, &buffer), 0);

	as_val* out = NULL;
	assert_int_eq(as_serializer_deserialize(&ser, &buffer, &out), 0);
	assert_not_null(out);
	assert_int_eq(as_val_type(out), AS_LIST);

	as_val* el = as_arraylist_get((as_arraylist*)out, 0);
	assert_int_eq(as_val_type(el), AS_BYTES);
	assert_int_eq(((as_bytes*)el)->type, AS_BYTES_VECTOR);

	as_vector_value* rvec = as_vector_value_from_bytes((as_bytes*)el);
	assert_not_null(rvec);
	assert_int_eq(rvec->element_type, AS_VECTOR_ELEMENT_TYPE_INT32);
	assert_int_eq(rvec->dimensions, 3);

	const int32_t* elems = as_vector_value_get_int32(rvec);
	assert_not_null(elems);
	for (uint32_t i = 0; i < 3; i++) {
		assert_int_eq(elems[i], data[i]);
	}

	as_vector_value_destroy(rvec);
	as_val_destroy(out);
	as_buffer_destroy(&buffer);
	as_serializer_destroy(&ser);
	as_arraylist_destroy(&list);
	as_vector_value_destroy(vec);
}

TEST(vector_server_roundtrip, "vectors round-trip through the server with native bin and collection helpers")
{
	uint16_t float16_data[] = {0x3C00, 0xC000};
	int32_t int32_data[] = {-1, 0, 1};
	float float32_data[] = {1.5f, -2.25f};
	double float64_data[] = {3.14159265358979, -0.5};

	as_vector_value* float16 = as_vector_value_new_float16(float16_data, 2);
	as_vector_value* int32 = as_vector_value_new_int32(int32_data, 3);
	as_vector_value* float32 = as_vector_value_new_float32(float32_data, 2);
	as_vector_value* float64 = as_vector_value_new_float64(float64_data, 2);
	assert_not_null(float16);
	assert_not_null(int32);
	assert_not_null(float32);
	assert_not_null(float64);

	as_arraylist list;
	as_arraylist_init(&list, 1, 0);
	assert_true(as_vector_value_append_to_list((as_list*)&list, float32));

	as_hashmap map;
	as_hashmap_init(&map, 1);
	assert_true(as_vector_value_set_in_map((as_map*)&map, (as_val*)as_string_new_strdup("v"), float64));

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 1);

	as_error err;
	as_error_reset(&err);
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_record record;
	as_record_inita(&record, 6);
	assert_true(as_record_set_vector(&record, "float16", float16));
	assert_true(as_record_set_vector(&record, "int32", int32));
	assert_true(as_record_set_vector(&record, "float32", float32));
	assert_true(as_record_set_vector(&record, "float64", float64));
	assert_true(as_record_set_list(&record, "list", (as_list*)&list));
	assert_true(as_record_set_map(&record, "map", (as_map*)&map));

	status = aerospike_key_put(as, &err, NULL, &key, &record);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&record);

	as_record* result = NULL;
	status = aerospike_key_get(as, &err, NULL, &key, &result);
	assert_int_eq(status, AEROSPIKE_OK);

	as_vector_value* result_float16 = as_record_get_vector(result, "float16");
	as_vector_value* result_int32 = as_record_get_vector(result, "int32");
	as_vector_value* result_float32 = as_record_get_vector(result, "float32");
	as_vector_value* result_float64 = as_record_get_vector(result, "float64");
	assert_not_null(result_float16);
	assert_not_null(result_int32);
	assert_not_null(result_float32);
	assert_not_null(result_float64);
	assert_int_eq(result_float16->element_type, AS_VECTOR_ELEMENT_TYPE_FLOAT16);
	assert_int_eq(result_int32->element_type, AS_VECTOR_ELEMENT_TYPE_INT32);
	assert_int_eq(result_float32->element_type, AS_VECTOR_ELEMENT_TYPE_FLOAT32);
	assert_int_eq(result_float64->element_type, AS_VECTOR_ELEMENT_TYPE_FLOAT64);
	assert_int_eq(as_vector_value_get_float16(result_float16)[0], float16_data[0]);
	assert_int_eq(as_vector_value_get_int32(result_int32)[0], int32_data[0]);
	assert(as_vector_value_get_float32(result_float32)[0] == float32_data[0]);
	assert(as_vector_value_get_float64(result_float64)[0] == float64_data[0]);

	as_list* result_list = as_record_get_list(result, "list");
	assert_not_null(result_list);
	as_bytes* list_bytes = (as_bytes*)as_list_get(result_list, 0);
	assert_not_null(list_bytes);
	assert_int_eq(list_bytes->type, AS_BYTES_VECTOR);
	as_vector_value* list_vector = as_vector_value_from_bytes(list_bytes);
	assert_not_null(list_vector);
	assert_int_eq(list_vector->element_type, AS_VECTOR_ELEMENT_TYPE_FLOAT32);
	assert(as_vector_value_get_float32(list_vector)[1] == float32_data[1]);

	as_string map_key;
	as_string_init(&map_key, "v", false);
	as_map* result_map = as_record_get_map(result, "map");
	assert_not_null(result_map);
	as_bytes* map_bytes = (as_bytes*)as_map_get(result_map, (as_val*)&map_key);
	assert_not_null(map_bytes);
	assert_int_eq(map_bytes->type, AS_BYTES_VECTOR);
	as_vector_value* map_vector = as_vector_value_from_bytes(map_bytes);
	assert_not_null(map_vector);
	assert_int_eq(map_vector->element_type, AS_VECTOR_ELEMENT_TYPE_FLOAT64);
	assert(as_vector_value_get_float64(map_vector)[1] == float64_data[1]);

	as_vector_value_destroy(map_vector);
	as_vector_value_destroy(list_vector);
	as_vector_value_destroy(result_float64);
	as_vector_value_destroy(result_float32);
	as_vector_value_destroy(result_int32);
	as_vector_value_destroy(result_float16);
	as_record_destroy(result);
	as_key_destroy(&key);
	as_vector_value_destroy(float64);
	as_vector_value_destroy(float32);
	as_vector_value_destroy(int32);
	as_vector_value_destroy(float16);
}

//---------------------------------
// Server integration tests (native value API)
//---------------------------------

static void
remove_key(int64_t id)
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, id);
	as_error err;
	aerospike_key_remove(as, &err, NULL, &key);
	as_key_destroy(&key);
}

TEST(vector_native_bin_roundtrip, "set/get vector bins of every element type round-trip through the server")
{
	uint16_t f16[4] = {0x3C00, 0xBC00, 0x4000, 0x0000};
	int32_t i32[4] = {INT32_MIN, -5, 12345, INT32_MAX};
	float f32[4] = {1.5f, -2.25f, 3.14159f, 0.0f};
	double f64[4] = {1.5, -2.25, 3.14159, 0.0};

	as_vector_value* v16 = as_vector_value_new_float16(f16, 4);
	as_vector_value* v32i = as_vector_value_new_int32(i32, 4);
	as_vector_value* v32f = as_vector_value_new_float32(f32, 4);
	as_vector_value* v64 = as_vector_value_new_float64(f64, 4);

	remove_key(10);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 10);

	as_record rec;
	as_record_inita(&rec, 4);
	assert_true(as_record_set_vector(&rec, "f16", v16));
	assert_true(as_record_set_vector(&rec, "i32", v32i));
	assert_true(as_record_set_vector(&rec, "f32", v32f));
	assert_true(as_record_set_vector(&rec, "f64", v64));

	as_error err;
	as_status status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_record* out = NULL;
	status = aerospike_key_get(as, &err, NULL, &key, &out);
	assert_int_eq(status, AEROSPIKE_OK);

	as_vector_value* r16 = as_record_get_vector(out, "f16");
	as_vector_value* r32i = as_record_get_vector(out, "i32");
	as_vector_value* r32f = as_record_get_vector(out, "f32");
	as_vector_value* r64 = as_record_get_vector(out, "f64");
	assert_not_null(r16);
	assert_not_null(r32i);
	assert_not_null(r32f);
	assert_not_null(r64);

	assert_int_eq(r16->element_type, AS_VECTOR_ELEMENT_TYPE_FLOAT16);
	assert_int_eq(r32i->element_type, AS_VECTOR_ELEMENT_TYPE_INT32);
	assert_int_eq(r32f->element_type, AS_VECTOR_ELEMENT_TYPE_FLOAT32);
	assert_int_eq(r64->element_type, AS_VECTOR_ELEMENT_TYPE_FLOAT64);
	assert_int_eq(r16->dimensions, 4);

	for (uint32_t i = 0; i < 4; i++) {
		assert_int_eq(as_vector_value_get_float16(r16)[i], f16[i]);
		assert_int_eq(as_vector_value_get_int32(r32i)[i], i32[i]);
		assert(as_vector_value_get_float32(r32f)[i] == f32[i]);
		assert(as_vector_value_get_float64(r64)[i] == f64[i]);
	}

	as_vector_value_destroy(r16);
	as_vector_value_destroy(r32i);
	as_vector_value_destroy(r32f);
	as_vector_value_destroy(r64);
	as_record_destroy(out);
	as_key_destroy(&key);
	as_vector_value_destroy(v16);
	as_vector_value_destroy(v32i);
	as_vector_value_destroy(v32f);
	as_vector_value_destroy(v64);
}

TEST(vector_special_float_bits_server, "special float bit patterns survive the server bit-exact")
{
	float f32[7] = {NAN, INFINITY, -INFINITY, -0.0f, 0.0f, FLT_MIN, FLT_MAX};
	double f64[7] = {NAN, INFINITY, -INFINITY, -0.0, 0.0, DBL_MIN, DBL_MAX};
	// +Inf, -Inf, NaN, +0, -0, smallest subnormal.
	uint16_t f16[6] = {0x7C00, 0xFC00, 0x7E00, 0x0000, 0x8000, 0x0001};

	as_vector_value* v32 = as_vector_value_new_float32(f32, 7);
	as_vector_value* v64 = as_vector_value_new_float64(f64, 7);
	as_vector_value* v16 = as_vector_value_new_float16(f16, 6);

	remove_key(11);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 11);

	as_record rec;
	as_record_inita(&rec, 3);
	assert_true(as_record_set_vector(&rec, "f32", v32));
	assert_true(as_record_set_vector(&rec, "f64", v64));
	assert_true(as_record_set_vector(&rec, "f16", v16));

	as_error err;
	assert_int_eq(aerospike_key_put(as, &err, NULL, &key, &rec), AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_record* out = NULL;
	assert_int_eq(aerospike_key_get(as, &err, NULL, &key, &out), AEROSPIKE_OK);

	as_vector_value* r32 = as_record_get_vector(out, "f32");
	as_vector_value* r64 = as_record_get_vector(out, "f64");
	as_vector_value* r16 = as_record_get_vector(out, "f16");
	assert_not_null(r32);
	assert_not_null(r64);
	assert_not_null(r16);

	// Compare by raw bit pattern so NaN and -0.0 are exercised exactly.
	const float* e32 = as_vector_value_get_float32(r32);
	const double* e64 = as_vector_value_get_float64(r64);
	const uint16_t* e16 = as_vector_value_get_float16(r16);

	for (uint32_t i = 0; i < 7; i++) {
		uint32_t a, b;
		memcpy(&a, &e32[i], sizeof(a));
		memcpy(&b, &f32[i], sizeof(b));
		assert_int_eq(a, b);

		uint64_t c, d;
		memcpy(&c, &e64[i], sizeof(c));
		memcpy(&d, &f64[i], sizeof(d));
		assert_int_eq(c, d);
	}
	for (uint32_t i = 0; i < 6; i++) {
		assert_int_eq(e16[i], f16[i]);
	}

	as_vector_value_destroy(r32);
	as_vector_value_destroy(r64);
	as_vector_value_destroy(r16);
	as_record_destroy(out);
	as_key_destroy(&key);
	as_vector_value_destroy(v32);
	as_vector_value_destroy(v64);
	as_vector_value_destroy(v16);
}

TEST(vector_single_dimension_server, "a single-dimension vector round-trips through the server")
{
	float data[1] = {42.5f};
	as_vector_value* vec = as_vector_value_new_float32(data, 1);

	remove_key(12);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 12);

	as_record rec;
	as_record_inita(&rec, 1);
	assert_true(as_record_set_vector(&rec, "v", vec));

	as_error err;
	assert_int_eq(aerospike_key_put(as, &err, NULL, &key, &rec), AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_record* out = NULL;
	assert_int_eq(aerospike_key_get(as, &err, NULL, &key, &out), AEROSPIKE_OK);

	as_vector_value* r = as_record_get_vector(out, "v");
	assert_not_null(r);
	assert_int_eq(r->dimensions, 1);
	assert(as_vector_value_get_float32(r)[0] == 42.5f);

	as_vector_value_destroy(r);
	as_record_destroy(out);
	as_key_destroy(&key);
	as_vector_value_destroy(vec);
}

TEST(vector_large_crossing_16bit_server, "a large vector crossing the 16-bit msgpack length boundary round-trips")
{
	// 9000 f64 => 8 + 9000*8 = 72008 bytes, past 65535.
	uint32_t dims = 9000;
	double* data = (double*)malloc(sizeof(double) * dims);
	for (uint32_t i = 0; i < dims; i++) {
		data[i] = i * 0.5;
	}

	as_vector_value* vec = as_vector_value_new_float64(data, dims);
	assert_not_null(vec);
	assert_int_eq(as_vector_value_get_wire_size(vec) > 0xFFFF, true);

	remove_key(13);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 13);

	as_record rec;
	as_record_inita(&rec, 1);
	assert_true(as_record_set_vector(&rec, "v", vec));

	as_error err;
	assert_int_eq(aerospike_key_put(as, &err, NULL, &key, &rec), AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_record* out = NULL;
	assert_int_eq(aerospike_key_get(as, &err, NULL, &key, &out), AEROSPIKE_OK);

	as_vector_value* r = as_record_get_vector(out, "v");
	assert_not_null(r);
	assert_int_eq(r->dimensions, dims);
	const double* elems = as_vector_value_get_float64(r);
	for (uint32_t i = 0; i < dims; i++) {
		assert(elems[i] == data[i]);
	}

	as_vector_value_destroy(r);
	as_record_destroy(out);
	as_key_destroy(&key);
	as_vector_value_destroy(vec);
	free(data);
}

TEST(vector_max_dimensions_server, "a vector at the server dimension limit round-trips")
{
	uint32_t dims = as_vector_element_type_max_dimensions(AS_VECTOR_ELEMENT_TYPE_FLOAT64);
	double* data = (double*)calloc(dims, sizeof(double));
	data[0] = -1.0;
	data[dims - 1] = 1.0;
	as_vector_value* vec = as_vector_value_new_float64(data, dims);
	assert_not_null(vec);

	remove_key(24);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 24);

	as_record rec;
	as_record_inita(&rec, 1);
	assert_true(as_record_set_vector(&rec, "v", vec));

	as_error err;
	assert_int_eq(aerospike_key_put(as, &err, NULL, &key, &rec), AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_record* out = NULL;
	assert_int_eq(aerospike_key_get(as, &err, NULL, &key, &out), AEROSPIKE_OK);
	as_vector_value* result = as_record_get_vector(out, "v");
	assert_not_null(result);
	assert_int_eq(result->dimensions, dims);
	assert(as_vector_value_get_float64(result)[0] == -1.0);
	assert(as_vector_value_get_float64(result)[dims - 1] == 1.0);

	as_vector_value_destroy(result);
	as_record_destroy(out);
	as_key_destroy(&key);
	as_vector_value_destroy(vec);
	free(data);
}

TEST(vector_absent_bin_null, "an absent vector bin reads back as NULL, not an empty vector")
{
	remove_key(14);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 14);

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, "scalar", 42);

	as_error err;
	assert_int_eq(aerospike_key_put(as, &err, NULL, &key, &rec), AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_record* out = NULL;
	assert_int_eq(aerospike_key_get(as, &err, NULL, &key, &out), AEROSPIKE_OK);

	assert_null(as_record_get_vector(out, "missing"));
	assert_int_eq(as_record_get_int64(out, "scalar", -1), 42);

	as_record_destroy(out);
	as_key_destroy(&key);
}

TEST(vector_overwrite_replaces_type_dims, "overwriting a vector bin fully replaces element type and dimensions")
{
	float f32[4] = {1.0f, 2.0f, 3.0f, 4.0f};
	int32_t i32[2] = {9, -9};
	as_vector_value* first = as_vector_value_new_float32(f32, 4);
	as_vector_value* second = as_vector_value_new_int32(i32, 2);

	remove_key(15);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 15);

	as_error err;
	as_record r1;
	as_record_inita(&r1, 1);
	assert_true(as_record_set_vector(&r1, "v", first));
	assert_int_eq(aerospike_key_put(as, &err, NULL, &key, &r1), AEROSPIKE_OK);
	as_record_destroy(&r1);

	as_record r2;
	as_record_inita(&r2, 1);
	assert_true(as_record_set_vector(&r2, "v", second));
	assert_int_eq(aerospike_key_put(as, &err, NULL, &key, &r2), AEROSPIKE_OK);
	as_record_destroy(&r2);

	as_record* out = NULL;
	assert_int_eq(aerospike_key_get(as, &err, NULL, &key, &out), AEROSPIKE_OK);

	as_vector_value* r = as_record_get_vector(out, "v");
	assert_not_null(r);
	assert_int_eq(r->element_type, AS_VECTOR_ELEMENT_TYPE_INT32);
	assert_int_eq(r->dimensions, 2);
	assert_int_eq(as_vector_value_get_int32(r)[0], 9);

	as_vector_value_destroy(r);
	as_record_destroy(out);
	as_key_destroy(&key);
	as_vector_value_destroy(first);
	as_vector_value_destroy(second);
}

TEST(vector_scalar_churn, "a vector bin can be replaced by a scalar and back")
{
	float f32[2] = {0.1f, 0.2f};
	double f64[3] = {9.0, 8.0, 7.0};
	as_vector_value* v1 = as_vector_value_new_float32(f32, 2);
	as_vector_value* v2 = as_vector_value_new_float64(f64, 3);

	remove_key(16);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 16);
	as_error err;

	as_record r1;
	as_record_inita(&r1, 1);
	assert_true(as_record_set_vector(&r1, "b", v1));
	assert_int_eq(aerospike_key_put(as, &err, NULL, &key, &r1), AEROSPIKE_OK);
	as_record_destroy(&r1);

	as_record r2;
	as_record_inita(&r2, 1);
	as_record_set_int64(&r2, "b", 123);
	assert_int_eq(aerospike_key_put(as, &err, NULL, &key, &r2), AEROSPIKE_OK);
	as_record_destroy(&r2);

	as_record* mid = NULL;
	assert_int_eq(aerospike_key_get(as, &err, NULL, &key, &mid), AEROSPIKE_OK);
	assert_null(as_record_get_vector(mid, "b"));
	assert_int_eq(as_record_get_int64(mid, "b", -1), 123);
	as_record_destroy(mid);

	as_record r3;
	as_record_inita(&r3, 1);
	assert_true(as_record_set_vector(&r3, "b", v2));
	assert_int_eq(aerospike_key_put(as, &err, NULL, &key, &r3), AEROSPIKE_OK);
	as_record_destroy(&r3);

	as_record* out = NULL;
	assert_int_eq(aerospike_key_get(as, &err, NULL, &key, &out), AEROSPIKE_OK);
	as_vector_value* r = as_record_get_vector(out, "b");
	assert_not_null(r);
	assert_int_eq(r->element_type, AS_VECTOR_ELEMENT_TYPE_FLOAT64);
	assert_int_eq(r->dimensions, 3);

	as_vector_value_destroy(r);
	as_record_destroy(out);
	as_key_destroy(&key);
	as_vector_value_destroy(v1);
	as_vector_value_destroy(v2);
}

TEST(vector_selective_read_server, "a selective read returns only the requested vector bin")
{
	float wanted[2] = {1.0f, 2.0f};
	int32_t other[3] = {5, 6, 7};
	as_vector_value* vw = as_vector_value_new_float32(wanted, 2);
	as_vector_value* vo = as_vector_value_new_int32(other, 3);

	remove_key(17);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 17);
	as_error err;

	as_record rec;
	as_record_inita(&rec, 2);
	assert_true(as_record_set_vector(&rec, "wanted", vw));
	assert_true(as_record_set_vector(&rec, "other", vo));
	assert_int_eq(aerospike_key_put(as, &err, NULL, &key, &rec), AEROSPIKE_OK);
	as_record_destroy(&rec);

	const char* bins[] = {"wanted", NULL};
	as_record* out = NULL;
	as_status status = aerospike_key_select(as, &err, NULL, &key, bins, &out);
	assert_int_eq(status, AEROSPIKE_OK);

	as_vector_value* r = as_record_get_vector(out, "wanted");
	assert_not_null(r);
	assert_int_eq(r->dimensions, 2);
	assert_null(as_record_get_vector(out, "other"));

	as_vector_value_destroy(r);
	as_record_destroy(out);
	as_key_destroy(&key);
	as_vector_value_destroy(vw);
	as_vector_value_destroy(vo);
}

TEST(vector_operate_write_read, "as_operations_add_write_vector writes a vector that reads back")
{
	double data[3] = {1.1, 2.2, 3.3};
	as_vector_value* vec = as_vector_value_new_float64(data, 3);

	remove_key(18);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 18);
	as_error err;

	as_operations ops;
	as_operations_inita(&ops, 2);
	assert_true(as_operations_add_write_vector(&ops, "v", vec));
	as_operations_add_read(&ops, "v");

	as_record* out = NULL;
	as_status status = aerospike_key_operate(as, &err, NULL, &key, &ops, &out);
	assert_int_eq(status, AEROSPIKE_OK);

	as_vector_value* r = as_record_get_vector(out, "v");
	assert_not_null(r);
	assert_int_eq(r->element_type, AS_VECTOR_ELEMENT_TYPE_FLOAT64);
	assert_int_eq(r->dimensions, 3);
	const double* elems = as_vector_value_get_float64(r);
	for (uint32_t i = 0; i < 3; i++) {
		assert(elems[i] == data[i]);
	}

	as_vector_value_destroy(r);
	as_record_destroy(out);
	as_operations_destroy(&ops);
	as_key_destroy(&key);
	as_vector_value_destroy(vec);
}

TEST(vector_cdt_read_server, "list and map operations return vectors with their particle tag")
{
	float list_data[2] = {1.0f, 2.0f};
	int32_t map_data[3] = {-1, 0, 1};
	as_vector_value* list_vec = as_vector_value_new_float32(list_data, 2);
	as_vector_value* map_vec = as_vector_value_new_int32(map_data, 3);

	remove_key(23);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 23);

	as_arraylist list;
	as_arraylist_init(&list, 1, 0);
	assert_true(as_vector_value_append_to_list((as_list*)&list, list_vec));

	as_hashmap map;
	as_hashmap_init(&map, 1);
	assert_true(as_vector_value_set_in_map((as_map*)&map, (as_val*)as_string_new_strdup("v"), map_vec));

	as_record record;
	as_record_inita(&record, 2);
	assert_true(as_record_set_list(&record, "list", (as_list*)&list));
	assert_true(as_record_set_map(&record, "map", (as_map*)&map));

	as_error err;
	assert_int_eq(aerospike_key_put(as, &err, NULL, &key, &record), AEROSPIKE_OK);
	as_record_destroy(&record);

	as_operations list_ops;
	as_operations_inita(&list_ops, 1);
	assert_true(as_operations_list_get(&list_ops, "list", NULL, 0));

	as_record* list_result = NULL;
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &list_ops, &list_result), AEROSPIKE_OK);
	as_bytes* list_bytes = as_record_get_bytes(list_result, "list");
	assert_vector_bytes_eq_float32(__result__, list_bytes, list_data, 2);
	as_record_destroy(list_result);
	as_operations_destroy(&list_ops);

	as_operations map_ops;
	as_operations_inita(&map_ops, 1);
	assert_true(as_operations_map_get_by_key(&map_ops, "map", NULL,
		(as_val*)as_string_new_strdup("v"), AS_MAP_RETURN_VALUE));

	as_record* map_result = NULL;
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &map_ops, &map_result), AEROSPIKE_OK);
	as_bytes* map_bytes = as_record_get_bytes(map_result, "map");
	assert_not_null(map_bytes);
	assert_int_eq(map_bytes->type, AS_BYTES_VECTOR);
	as_vector_value* decoded = as_vector_value_from_bytes(map_bytes);
	assert_not_null(decoded);
	assert_int_eq(decoded->element_type, AS_VECTOR_ELEMENT_TYPE_INT32);
	assert_int_eq(decoded->dimensions, 3);
	assert_int_eq(as_vector_value_get_int32(decoded)[0], map_data[0]);
	assert_int_eq(as_vector_value_get_int32(decoded)[2], map_data[2]);

	as_vector_value_destroy(decoded);
	as_record_destroy(map_result);
	as_operations_destroy(&map_ops);
	as_key_destroy(&key);
	as_vector_value_destroy(list_vec);
	as_vector_value_destroy(map_vec);
}

#if AS_EVENT_LIB_DEFINED
TEST(vector_async_roundtrip, "async put/get round-trips a vector through the server")
{
	vector_async_data data;
	data.result = __result__;
	data.expected[0] = 1.5f;
	data.expected[1] = -2.25f;
	as_monitor_init(&data.monitor);
	as_monitor_begin(&data.monitor);

	as_vector_value* vec = as_vector_value_new_float32(data.expected, 2);
	assert_not_null(vec);

	remove_key(25);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 25);

	as_record rec;
	as_record_inita(&rec, 1);
	assert_true(as_record_set_vector(&rec, "v", vec));

	as_error err;
	as_status status = aerospike_key_put_async(
		as, &err, NULL, &key, &rec, vector_async_put_callback, &data, NULL, NULL);
	assert_int_eq(status, AEROSPIKE_OK);

	as_key_destroy(&key);
	as_record_destroy(&rec);
	as_vector_value_destroy(vec);
	as_monitor_wait(&data.monitor);
	as_monitor_destroy(&data.monitor);
}
#endif

TEST(vector_batch_read_server, "a batch read returns records that contain vector bins")
{
	float d1[3] = {1.0f, 2.0f, 3.0f};
	int32_t d2[2] = {10, 20};
	as_vector_value* v1 = as_vector_value_new_float32(d1, 3);
	as_vector_value* v2 = as_vector_value_new_int32(d2, 2);

	remove_key(19);
	remove_key(20);

	as_error err;
	as_key k1;
	as_key_init_int64(&k1, NAMESPACE, SET, 19);
	as_key k2;
	as_key_init_int64(&k2, NAMESPACE, SET, 20);

	as_record r1;
	as_record_inita(&r1, 1);
	assert_true(as_record_set_vector(&r1, "embedding", v1));
	assert_int_eq(aerospike_key_put(as, &err, NULL, &k1, &r1), AEROSPIKE_OK);
	as_record_destroy(&r1);

	as_record r2;
	as_record_inita(&r2, 1);
	assert_true(as_record_set_vector(&r2, "embedding", v2));
	assert_int_eq(aerospike_key_put(as, &err, NULL, &k2, &r2), AEROSPIKE_OK);
	as_record_destroy(&r2);

	as_batch_records recs;
	as_batch_records_inita(&recs, 2);

	as_batch_read_record* br1 = as_batch_read_reserve(&recs);
	as_key_init_int64(&br1->key, NAMESPACE, SET, 19);
	br1->read_all_bins = true;

	as_batch_read_record* br2 = as_batch_read_reserve(&recs);
	as_key_init_int64(&br2->key, NAMESPACE, SET, 20);
	br2->read_all_bins = true;

	as_status status = aerospike_batch_read(as, &err, NULL, &recs);
	assert_int_eq(status, AEROSPIKE_OK);

	assert_int_eq(br1->result, AEROSPIKE_OK);
	assert_int_eq(br2->result, AEROSPIKE_OK);

	as_vector_value* rv1 = as_record_get_vector(&br1->record, "embedding");
	as_vector_value* rv2 = as_record_get_vector(&br2->record, "embedding");
	assert_not_null(rv1);
	assert_not_null(rv2);
	assert_int_eq(rv1->element_type, AS_VECTOR_ELEMENT_TYPE_FLOAT32);
	assert_int_eq(rv2->element_type, AS_VECTOR_ELEMENT_TYPE_INT32);
	assert_int_eq(rv1->dimensions, 3);
	assert_int_eq(rv2->dimensions, 2);

	as_vector_value_destroy(rv1);
	as_vector_value_destroy(rv2);
	as_batch_records_destroy(&recs);
	as_key_destroy(&k1);
	as_key_destroy(&k2);
	as_vector_value_destroy(v1);
	as_vector_value_destroy(v2);
}

TEST(vector_dist_exp_compiles, "vector distance expression compiles and packs (client-side only)")
{
	// The server does not yet implement EXP_VECTOR_DIST, so this only verifies
	// that the ported expression macros build a well-formed expression. See the
	// disabled vector_distance_expression_server test below.
	float data[4] = {0.1f, 0.2f, 0.3f, 0.4f};
	as_vector_value* query = as_vector_value_new_float32(data, 4);

	uint32_t qsize;
	const uint8_t* qbytes = as_vector_value_element_bytes(query, &qsize);
	assert_not_null(qbytes);
	assert_int_eq(qsize, 4 * sizeof(float));

	as_exp_build(filter,
		as_exp_cmp_ge(
			as_exp_vector_dist(AS_VECTOR_DISTANCE_COSINE_SIMILARITY, (uint8_t*)qbytes, qsize,
				as_exp_bin_vector("embedding")),
			as_exp_float(0.8)));
	assert_not_null(filter);

	as_exp_destroy(filter);
	as_vector_value_destroy(query);
}

//---------------------------------
// WORK IN PROGRESS - disabled server tests
//
// These are ported to keep the C client in the same state as the Java and Rust
// clients, but are intentionally NOT registered in the suite below (the C test
// framework has no per-test "disabled" flag, so leaving them unregistered is the
// equivalent of Java's @Disabled / Rust's #[ignore]).
//
// - vector_distance_expression_server: the server has no EXP_VECTOR_DIST (op 52)
//   yet; building+sending the expression fails with PARAMETER_ERROR. Re-enable
//   once the server ships the op (and revisit metric semantics).
// - vector_bin_expression_read_server: reading a vector bin through the
//   expression engine currently crashes the node (server rt_bin_translate has no
//   AS_PARTICLE_TYPE_VECTOR arm). Re-enable once the server handles VECTOR on the
//   expression read path.
//---------------------------------

TEST(vector_distance_expression_server, "[disabled] vector distance expression evaluates on the server")
{
	float stored[4] = {0.1f, 0.2f, 0.3f, 0.4f};
	as_vector_value* vec = as_vector_value_new_float32(stored, 4);

	remove_key(21);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 21);
	as_error err;

	as_record rec;
	as_record_inita(&rec, 1);
	assert_true(as_record_set_vector(&rec, "embedding", vec));
	assert_int_eq(aerospike_key_put(as, &err, NULL, &key, &rec), AEROSPIKE_OK);
	as_record_destroy(&rec);

	uint32_t qsize;
	const uint8_t* qbytes = as_vector_value_element_bytes(vec, &qsize);

	as_exp_build(read_exp,
		as_exp_vector_dist(AS_VECTOR_DISTANCE_EUCLIDEAN_SQUARED, (uint8_t*)qbytes, qsize,
			as_exp_bin_vector("embedding")));
	assert_not_null(read_exp);

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_exp_read(&ops, "dist", read_exp, AS_EXP_READ_DEFAULT);

	as_record* out = NULL;
	as_status status = aerospike_key_operate(as, &err, NULL, &key, &ops, &out);
	assert_int_eq(status, AEROSPIKE_OK);

	// Distance from a vector to itself is zero (for L2 / L2-squared).
	assert_double_eq(as_record_get_double(out, "dist", -1.0), 0.0);

	as_record_destroy(out);
	as_operations_destroy(&ops);
	as_exp_destroy(read_exp);
	as_key_destroy(&key);
	as_vector_value_destroy(vec);
}

TEST(vector_bin_expression_read_server, "[disabled] reading a vector bin via an expression must not crash the node")
{
	float stored[3] = {0.5f, -1.5f, 2.0f};
	as_vector_value* vec = as_vector_value_new_float32(stored, 3);

	remove_key(22);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 22);
	as_error err;

	as_record rec;
	as_record_inita(&rec, 1);
	assert_true(as_record_set_vector(&rec, "embedding", vec));
	assert_int_eq(aerospike_key_put(as, &err, NULL, &key, &rec), AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_exp_build(read_exp, as_exp_bin_vector("embedding"));
	assert_not_null(read_exp);

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_exp_read(&ops, "out", read_exp, AS_EXP_READ_DEFAULT);

	as_record* out = NULL;
	as_status status = aerospike_key_operate(as, &err, NULL, &key, &ops, &out);
	assert_int_eq(status, AEROSPIKE_OK);

	as_record_destroy(out);
	as_operations_destroy(&ops);
	as_exp_destroy(read_exp);
	as_key_destroy(&key);
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
	suite_add(vector_from_float32);
	suite_add(vector_from_int32);
	suite_add(vector_from_float64);
	suite_add(vector_from_float16);
	suite_add(vector_from_offset);
	suite_add(vector_from_bytes_roundtrip);
	suite_add(vector_from_invalid);
	suite_add(vector_from_limits);
	suite_add(vector_from_unknown_version);
	suite_add(vector_to_bytes_new);
	suite_add(vector_nested_in_list);
	suite_add(vector_nested_in_map);
	suite_add(vector_collection_helpers);
	suite_add(vector_element_type_size);
	suite_add(vector_getter_type_safety);
	suite_add(vector_null_safety);
	suite_add(vector_empty);
	suite_add(vector_from_trailing_bytes);
	suite_add(vector_from_overflow);
	suite_add(vector_constructor_limits);
	suite_add(vector_serialization_rejects_invalid_vectors);
	suite_add(vector_nested_int32_in_list);
	suite_add(vector_server_roundtrip);
	suite_add(vector_native_bin_roundtrip);
	suite_add(vector_special_float_bits_server);
	suite_add(vector_single_dimension_server);
	suite_add(vector_large_crossing_16bit_server);
	suite_add(vector_max_dimensions_server);
	suite_add(vector_absent_bin_null);
	suite_add(vector_overwrite_replaces_type_dims);
	suite_add(vector_scalar_churn);
	suite_add(vector_selective_read_server);
	suite_add(vector_operate_write_read);
	suite_add(vector_cdt_read_server);
#if AS_EVENT_LIB_DEFINED
	suite_add(vector_async_roundtrip);
#endif
	suite_add(vector_batch_read_server);
	suite_add(vector_dist_exp_compiles);

	// Intentionally not registered (ported WIP, gated like Java @Disabled /
	// Rust #[ignore]); see the "WORK IN PROGRESS" section above:
	//   vector_distance_expression_server
	//   vector_bin_expression_read_server
}
