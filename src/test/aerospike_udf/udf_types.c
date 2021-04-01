/*
 * Copyright 2008-2021 Aerospike, Inc.
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
#include <aerospike/aerospike_udf.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_boolean.h>
#include <aerospike/as_double.h>
#include <aerospike/as_error.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_hashmap_iterator.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_list.h>
#include <aerospike/as_map.h>
#include <aerospike/as_nil.h>
#include <aerospike/as_record.h>
#include <aerospike/as_sleep.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>
#include <aerospike/as_stringmap.h>
#include <aerospike/as_val.h>
#include <aerospike/as_udf.h>

#include "../test.h"
#include "../util/udf.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike * as;

/******************************************************************************
 * MACROS
 *****************************************************************************/
#define NAMESPACE "test"
#define SET "test_udftype"

#define LUA_FILE AS_START_DIR "src/test/lua/udf_types.lua"
#define UDF_FILE "udf_types"

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

static bool
udf_types_pre(atf_suite* suite)
{
	const char * filename = UDF_FILE".lua";

	as_error err;
	as_bytes content;

	info("reading: %s", LUA_FILE);

	if (!udf_readfile(LUA_FILE, &content)) {
		return false;
	}

	info("uploading: %s", filename);
	
	as_status status = aerospike_udf_put(as, &err, NULL, filename, AS_UDF_TYPE_LUA, &content);

	if (status != AEROSPIKE_OK) {
		return false;
	}

	status = aerospike_udf_put_wait(as, &err, NULL, filename, 100);

	if (status != AEROSPIKE_OK) {
		return false;
	}
	as_bytes_destroy(&content);
	return true;
}

static bool
udf_types_post(atf_suite* suite)
{
	const char * filename = UDF_FILE".lua";

	as_error err;
	as_status status = aerospike_udf_remove(as, &err, NULL, filename);

	if (status != AEROSPIKE_OK) {
		return false;
	}

	as_sleep(100);
	return true;
}

TEST(udf_types_nil, "udf_types.get_nil() returns as_nil") {

	as_error err;

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "test");

	as_val * val = NULL;

	aerospike_key_apply(as, &err, NULL, &key, "udf_types", "get_nil", NULL, &val);

	assert_int_eq(err.code, AEROSPIKE_OK);
	assert_int_eq(as_val_type(val), AS_NIL);

	as_val_destroy(val);
	as_key_destroy(&key);
}

TEST(udf_types_true, "udf_types.get_true() returns 1 (as_integer)") {

	as_error err;

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "test");

	as_val * val = NULL;

	aerospike_key_apply(as, &err, NULL, &key, "udf_types", "get_true", NULL, &val);

	assert_int_eq(err.code, AEROSPIKE_OK);
	assert_true(as_val_tobool(val));

	as_val_destroy(val);
	as_key_destroy(&key);
}

TEST(udf_types_false, "udf_types.get_false() returns 0 (as_integer)") {

	as_error err;

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "test");

	as_val * val = NULL;

	aerospike_key_apply(as, &err, NULL, &key, "udf_types", "get_false", NULL, &val);

	assert_int_eq(err.code, AEROSPIKE_OK);
	assert_false(as_val_tobool(val));

	as_val_destroy(val);
	as_key_destroy(&key);
}

TEST(udf_types_integer, "udf_types.get_integer() returns 123 (as_integer)") {

	as_error err;

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "test");

	as_val * val = NULL;

	aerospike_key_apply(as, &err, NULL, &key, "udf_types", "get_integer", NULL, &val);

	assert_int_eq(err.code, AEROSPIKE_OK);
	assert_int_eq(as_val_type(val), AS_INTEGER);

	as_integer * ival = as_integer_fromval(val);
	assert_int_eq(as_integer_get(ival), 123);

	as_val_destroy(val);
	as_key_destroy(&key);
}

TEST(udf_types_string, "udf_types.get_string() returns 'abc' (as_string)") {

	as_error err;

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "test");

	as_val * val = NULL;

	aerospike_key_apply(as, &err, NULL, &key, "udf_types", "get_string", NULL, &val);

	assert_int_eq(err.code, AEROSPIKE_OK);
	assert_int_eq(as_val_type(val), AS_STRING);

	as_string * sval = as_string_fromval(val);
	assert_string_eq(as_string_get(sval), "abc");

	as_val_destroy(val);
	as_key_destroy(&key);
}

bool udf_types_map_foreach(const as_val * key, const as_val * value, void * udata) {
	char * k = as_val_tostring(key);
	char * v = as_val_tostring(value);
	fprintf(stderr, "%s=%s\n", k, v);
	free(k);
	free(v);
	return true;
}

bool udf_types_map_notnil_foreach(const as_val * key, const as_val * value, void * udata) {
	uint32_t * count = (uint32_t *)udata;
	if (as_val_type(value) != AS_NIL) {
		(*count)++;
	}
	return true;
}

TEST(udf_types_map, "udf_types.get_map() returns {a:1, b:2, c:3} (as_map)") {

	as_error err;

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "test");

	as_val * val = NULL;

	aerospike_key_apply(as, &err, NULL, &key, "udf_types", "get_map", NULL, &val);

	assert_int_eq(err.code, AEROSPIKE_OK);
	assert_int_eq(as_val_type(val), AS_MAP);

	as_map * mval = as_map_fromval(val);
	assert_int_eq(as_map_size(mval), 3);
	assert_int_eq(as_stringmap_get_int64(mval,"a"), 1);
	assert_int_eq(as_stringmap_get_int64(mval,"b"), 2);
	assert_int_eq(as_stringmap_get_int64(mval,"c"), 3);

	as_map_foreach(mval, udf_types_map_foreach, NULL);


	as_val_destroy(val);
	as_key_destroy(&key);
}

TEST(udf_types_list, "udf_types.get_list() returns [1,2,3] (as_list)") {

	as_error err;

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "test");

	as_val * val = NULL;

	aerospike_key_apply(as, &err, NULL, &key, "udf_types", "get_list", NULL, &val);

	assert_int_eq(err.code, AEROSPIKE_OK);
	assert_int_eq(as_val_type(val), AS_LIST);

	as_list * lval = as_list_fromval(val);
	assert_int_eq(as_list_size(lval), 3);
	assert_int_eq(as_list_get_int64(lval,0), 1);
	assert_int_eq(as_list_get_int64(lval,1), 2);
	assert_int_eq(as_list_get_int64(lval,2), 3);

	as_val_destroy(val);
	as_key_destroy(&key);
}

TEST(udf_types_bytes, "udf_types.get_bytes() returns 'zyx' (as_bytes)") {

	as_error err;

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "test");

	as_val * val = NULL;
	as_val * val2 = NULL;

	aerospike_key_apply(as, &err, NULL, &key, "udf_types", "get_bytes", NULL, &val);
	assert_int_eq(err.code, AEROSPIKE_OK);
	assert_int_eq(as_val_type(val), AS_BYTES);

	as_bytes * bval = as_bytes_fromval(val);
	assert_int_eq(as_bytes_size(bval), 3);
	assert_int_eq(memcmp(as_bytes_get(bval), "zyx", as_bytes_size(bval)), 0);

	as_arraylist args;
	as_arraylist_inita(&args, 1);
	as_arraylist_append(&args, val);

	aerospike_key_apply(as, &err, NULL, &key, "udf_types", "modify_bytes", (as_list*)&args, &val2);
	assert_int_eq(err.code, AEROSPIKE_OK);
	assert_int_eq(as_val_type(val2), AS_BYTES);

	bval = as_bytes_fromval(val2);
	assert_int_eq(as_bytes_size(bval), 3);
	uint8_t v = 0;
	as_bytes_get_byte(bval, 0, &v);
	assert_int_eq(v, 122);
	as_bytes_get_byte(bval, 1, &v);
	assert_int_eq(v, 135);
	as_bytes_get_byte(bval, 2, &v);
	assert_int_eq(v, 120);
	info("changed second byte to 135 via UDF");

	as_val_destroy(val);
	as_val_destroy(val2);
	as_key_destroy(&key);
}

TEST(udf_types_rec_map, "udf_types.get_rec_map() returns {t: true, f: false, n: nil, i: 123, s: 'abc', l: [1,2,3], b: 'zyx' (as_bytes)} (as_map)") {

	as_error err;

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "test");

	as_val * val = NULL;

	aerospike_key_apply(as, &err, NULL, &key, "udf_types", "get_rec_map", NULL, &val);

	assert_int_eq(err.code, AEROSPIKE_OK);
	assert_int_eq(as_val_type(val), AS_MAP);

	as_map * mval = as_map_fromval(val);

	uint32_t non_nil_count = 0;
	as_map_foreach(mval, udf_types_map_notnil_foreach, &non_nil_count);
	assert_int_eq(non_nil_count, 6);

	as_val* v = as_stringmap_get(mval, "t");
	assert_int_eq(v->type, AS_BOOLEAN);
	as_boolean* b = (as_boolean*)v;
	assert_true(b->value);

	v = as_stringmap_get(mval, "f");
	assert_int_eq(v->type, AS_BOOLEAN);
	b = (as_boolean*)v;
	assert_false(b->value);

	as_map_foreach(mval, udf_types_map_foreach, NULL);

	as_val_destroy(val);
	as_key_destroy(&key);
}

TEST(udf_types_double, "udf_types.write_f(rec, bname, float_binval) == udf_type_read_f(rec, bname, float_add) + float_add")
{
	as_error err;

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "udf_double");

	// remove record to make sure we are clean
	aerospike_key_remove(as, &err, NULL, &key);

	// write a double bin value, list value, map value
	as_arraylist args;
	as_arraylist_inita(&args, 4);
	as_arraylist_append_str(&args, "bin1");
	as_arraylist_append_str(&args, "binlist");
	as_arraylist_append_str(&args, "binmap");
	as_arraylist_append_double(&args,4.4);

	as_val * res = NULL;

	aerospike_key_apply(as, &err, NULL, &key, "udf_types", "write_f", (as_list*)&args, &res);

	assert_int_eq(err.code, AEROSPIKE_OK);
	assert_int_eq(as_val_type(res), AS_NIL);

	as_val_destroy(res);
	as_val_destroy(&args);
	res = NULL;

	// read bin value back via udf and pass it back
	as_arraylist_inita(&args, 1);
	as_arraylist_append_str(&args, "bin1");

	aerospike_key_apply(as, &err, NULL, &key, "udf_types", "read_f", (as_list*)&args, &res);

	assert_int_eq(err.code, AEROSPIKE_OK);
	assert_int_eq(as_val_type(res), AS_DOUBLE);

	double result = as_double_get((as_double *)res);
	info ("double = %lf", result);
	assert_true((result > 4.39 && result < 4.41));

	as_val_destroy(res);
	as_val_destroy(&args);

	// read list value back via udf pass it back
	as_arraylist_inita(&args, 1);
	as_arraylist_append_str(&args, "binlist");

	aerospike_key_apply(as, &err, NULL, &key, "udf_types", "read_f", (as_list*)&args, &res);

	assert_int_eq(err.code, AEROSPIKE_OK);
	assert_int_eq(as_val_type(res), AS_LIST);

	result = as_arraylist_get_double((as_arraylist *)res, 0);

	info ("list[0] = %lf", result);
	assert_true((result> 1.19 && result < 1.21));

	result = as_arraylist_get_double((as_arraylist *)res, 1);

	info ("list[1] = %lf", result);
	assert_true((result> 3.39 && result < 3.41));

	as_val_destroy(res);
	as_val_destroy(&args);

	// read map value back via udf pass it back
	as_arraylist_inita(&args, 1);
	as_arraylist_append_str(&args, "binmap");

	aerospike_key_apply(as, &err, NULL, &key, "udf_types", "read_f", (as_list*)&args, &res);

	assert_int_eq(err.code, AEROSPIKE_OK);
	assert_int_eq(as_val_type(res), AS_MAP);

	as_hashmap_iterator it;
	as_hashmap_iterator_init(&it, (const as_hashmap*)res);

	while (as_hashmap_iterator_has_next(&it)) {
		as_pair* p_pair = (as_pair *)as_hashmap_iterator_next(&it);
		const char* p_key = as_string_val_tostring(as_pair_1(p_pair));
		as_val* p_val = as_pair_2(p_pair);
		if (strcmp (p_key,"\"a\"") == 0) { // AER-4267 should not contain extra ""
			result = as_double_get((const as_double *)p_val);
			info ("map['a'] = %lf", result);
			assert_true((result> 5.59 && result < 5.61));
		} else if (strcmp (p_key,"\"b\"") == 0) {
			result = as_double_get((const as_double *)p_val);
			info ("map['b'] = %lf", result);
			assert_true((result> 7.79 && result < 7.81));
		}
		free ((void *)p_key);
	}

	as_val_destroy(res);
	as_val_destroy(&args);

	as_key_destroy(&key);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/


SUITE(udf_types, "aerospike_udf type tests")
{
	suite_before(udf_types_pre);
	suite_after(udf_types_post);

	suite_add(udf_types_nil);
	suite_add(udf_types_true);
	suite_add(udf_types_false);
	suite_add(udf_types_integer);
	suite_add(udf_types_string);
	suite_add(udf_types_map);
	suite_add(udf_types_list);
	suite_add(udf_types_bytes);
	suite_add(udf_types_rec_map);
	suite_add(udf_types_double);
}
