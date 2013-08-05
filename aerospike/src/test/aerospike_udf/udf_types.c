
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_udf.h>

#include <aerospike/as_error.h>
#include <aerospike/as_status.h>

#include <aerospike/as_record.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_string.h>
#include <aerospike/as_list.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_map.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_val.h>
#include <aerospike/as_udf.h>

#include <time.h>

#include "../test.h"
#include "../util/udf.h"
#include "../util/info_util.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike * as;

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define LUA_FILE "src/test/lua/udf_types.lua"
#define UDF_FILE "udf_types"

#define WAIT_MS(__ms) nanosleep((struct timespec[]){{0, __ms##000000}}, NULL)

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( udf_types_pre , "upload udf_types.lua" ) {

	const char * filename = UDF_FILE".lua";

	as_error err;
	as_bytes content;

	info("reading: %s",LUA_FILE);
	udf_readfile(LUA_FILE, &content);

	info("uploading: %s",filename);
	aerospike_udf_put(as, &err, NULL, filename, AS_UDF_TYPE_LUA, &content);

	assert_int_eq( err.code, AEROSPIKE_OK );

	WAIT_MS(100);

	as_bytes_destroy(&content);
}

TEST( udf_types_post , "upload udf_types.lua" ) {

	const char * filename = UDF_FILE".lua";
	bool exists = false;

	as_error err;

	// remove the file, regardless of whether it is on server or not.
	
	aerospike_udf_remove(as, &err, NULL, filename);

	assert_int_eq( err.code, AEROSPIKE_OK );

	WAIT_MS(100);

}

TEST( udf_types_nil, "udf_types.get_nil() returns as_nil" ) {

	as_error err;

	as_key key;
	as_key_init(&key, "test", "test", "test");

	as_val * val = NULL;

	aerospike_key_apply(as, &err, NULL, &key, "udf_types", "get_nil", NULL, &val);

	assert_int_eq( err.code, AEROSPIKE_OK );
	assert_int_eq( as_val_type(val), AS_NIL );

	as_val_destroy(val);
	as_key_destroy(&key);
}

TEST( udf_types_true, "udf_types.get_true() returns 1 (as_integer)" ) {

	as_error err;

	as_key key;
	as_key_init(&key, "test", "test", "test");

	as_val * val = NULL;

	aerospike_key_apply(as, &err, NULL, &key, "udf_types", "get_true", NULL, &val);

	assert_int_eq( err.code, AEROSPIKE_OK );
	assert_int_eq( as_val_type(val), AS_INTEGER );

	as_integer * ival = as_integer_fromval(val);
	assert_int_eq( as_integer_get(ival), 1);

	as_val_destroy(val);
	as_key_destroy(&key);
}

TEST( udf_types_false, "udf_types.get_false() returns 0 (as_integer)" ) {

	as_error err;

	as_key key;
	as_key_init(&key, "test", "test", "test");

	as_val * val = NULL;

	aerospike_key_apply(as, &err, NULL, &key, "udf_types", "get_false", NULL, &val);

	assert_int_eq( err.code, AEROSPIKE_OK );
	assert_int_eq( as_val_type(val), AS_INTEGER );

	as_integer * ival = as_integer_fromval(val);
	assert_int_eq( as_integer_get(ival), 0);

	as_val_destroy(val);
	as_key_destroy(&key);
}

TEST( udf_types_integer, "udf_types.get_integer() returns 123 (as_integer)" ) {

	as_error err;

	as_key key;
	as_key_init(&key, "test", "test", "test");

	as_val * val = NULL;

	aerospike_key_apply(as, &err, NULL, &key, "udf_types", "get_integer", NULL, &val);

	assert_int_eq( err.code, AEROSPIKE_OK );
	assert_int_eq( as_val_type(val), AS_INTEGER );

	as_integer * ival = as_integer_fromval(val);
	assert_int_eq( as_integer_get(ival), 123);

	as_val_destroy(val);
	as_key_destroy(&key);
}

TEST( udf_types_string, "udf_types.get_string() returns 'abc' (as_string)" ) {

	as_error err;

	as_key key;
	as_key_init(&key, "test", "test", "test");

	as_val * val = NULL;

	aerospike_key_apply(as, &err, NULL, &key, "udf_types", "get_string", NULL, &val);

	assert_int_eq( err.code, AEROSPIKE_OK );
	assert_int_eq( as_val_type(val), AS_STRING );

	as_string * sval = as_string_fromval(val);
	assert_string_eq( as_string_get(sval), "abc");

	as_val_destroy(val);
	as_key_destroy(&key);
}

TEST( udf_types_map, "udf_types.get_map() returns {a:1, b:2, c:3} (as_map)" ) {

	as_error err;

	as_key key;
	as_key_init(&key, "test", "test", "test");

	as_val * val = NULL;

	aerospike_key_apply(as, &err, NULL, &key, "udf_types", "get_map", NULL, &val);

	assert_int_eq( err.code, AEROSPIKE_OK );
	assert_int_eq( as_val_type(val), AS_MAP );

	as_map * mval = as_map_fromval(val);
	assert_int_eq( as_map_size(mval), 3);
	assert_int_eq( as_stringmap_get_int64(mval,"a"), 1);
	assert_int_eq( as_stringmap_get_int64(mval,"b"), 2);
	assert_int_eq( as_stringmap_get_int64(mval,"c"), 3);

	as_val_destroy(val);
	as_key_destroy(&key);
}

TEST( udf_types_list, "udf_types.get_list() returns [1,2,3] (as_list)" ) {

	as_error err;

	as_key key;
	as_key_init(&key, "test", "test", "test");

	as_val * val = NULL;

	aerospike_key_apply(as, &err, NULL, &key, "udf_types", "get_list", NULL, &val);

	assert_int_eq( err.code, AEROSPIKE_OK );
	assert_int_eq( as_val_type(val), AS_LIST );

	as_list * lval = as_list_fromval(val);
	assert_int_eq( as_list_size(lval), 3);
	assert_int_eq( as_list_get_int64(lval,0), 1);
	assert_int_eq( as_list_get_int64(lval,1), 2);
	assert_int_eq( as_list_get_int64(lval,2), 3);

	as_val_destroy(val);
	as_key_destroy(&key);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( udf_types, "aerospike_udf type tests" ) {
	suite_add( udf_types_pre );
	suite_add( udf_types_nil );
	suite_add( udf_types_true );
	suite_add( udf_types_false );
	suite_add( udf_types_integer );
	suite_add( udf_types_string );
	suite_add( udf_types_map );
	suite_add( udf_types_list );
	suite_add( udf_types_post );
}
