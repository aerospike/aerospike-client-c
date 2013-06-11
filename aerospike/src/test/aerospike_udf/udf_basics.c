
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

#define LUA_FILE "src/test/lua/udf_basics.lua"
#define UDF_FILE "udf_basics"

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( udf_basics_1 , "manage udf_basics.lua" ) {

	const char * filename = UDF_FILE".lua";
	bool exists = false;

	as_error err;
	as_error_reset(&err);

	// remove the file, regardless of whether it is on server or not.
	
	aerospike_udf_remove(as, &err, NULL, filename);

    assert_int_eq( err.code, AEROSPIKE_OK );

	// list the files on the server

	as_udf_list list;

	as_udf_list_init(&list);
	
	aerospike_udf_list(as, &err, NULL, &list);
	assert_int_eq( err.code, AEROSPIKE_OK );

	info("files: ")
	for(int i=0; i<list.size; i++) {
		as_udf_file * file = &list.files[i];
		info("- %s", file->name);
		if ( strcmp(file->name, filename) == 0 ) {
			exists = true;
		}
	}

	as_udf_list_destroy(&list);

	// assert_false( exists );

	// upload the file

	as_bytes content;

	info("reading: %s",LUA_FILE);
	udf_readfile(LUA_FILE, &content);

	info("uploading: %s",filename);
    aerospike_udf_put(as, &err, NULL, filename, AS_UDF_TYPE_LUA, &content);

    assert_int_eq( err.code, AEROSPIKE_OK );

	// list the files on the server

	as_udf_list_init(&list);

	aerospike_udf_list(as, &err, NULL, &list);

    assert_int_eq( err.code, AEROSPIKE_OK );

	info("files: ")
	for(int i=0; i<list.size; i++) {
		as_udf_file * file = &list.files[i];
		info("- %s", file->name);
		if ( strcmp(file->name, filename) == 0 ) {
			exists = true;
		}
	}

	as_udf_list_destroy(&list);

	assert_true( exists );

	// dowload the file

	as_udf_file file;
	as_udf_file_init(&file);

	info("downloading: %s", filename);
    aerospike_udf_get(as, &err, NULL, filename, AS_UDF_TYPE_LUA, &file);

    assert_int_eq( err.code, AEROSPIKE_OK );

	info("downaloded: %s size=%d", filename, file.content.size);
    assert_int_eq( file.content.size, content.len );

    as_udf_file_destroy(&file);

    // remove the file

	aerospike_udf_remove(as, &err, NULL, filename);

    assert_int_eq( err.code, AEROSPIKE_OK );

	as_bytes_destroy(&content);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( udf_basics, "aerospike_udf basic tests" ) {
	suite_add( udf_basics_1 );
}
