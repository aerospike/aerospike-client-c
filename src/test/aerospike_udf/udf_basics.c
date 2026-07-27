/*
 * Copyright 2008-2019 Aerospike, Inc.
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

#define LUA_FILE AS_START_DIR "src/test/lua/udf_basics.lua"
#define UDF_FILE "udf_basics"

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( udf_basics_1 , "manage udf_basics.lua" ) {

	const char * filename = UDF_FILE".lua";
	bool exists = false;

	as_error err;
	as_error_reset(&err);

	/* This test uploads, lists, downloads, and then proceeds to remove the udf-lua-file as well. 
         * As a safe-guard, always ensure that the file does not exist in the first place. */ 
 	
	aerospike_udf_remove(as, &err, NULL, filename);

	as_sleep(100);

	// list the files on the server

	as_udf_files files;
	as_udf_files_init(&files, 0);
	
	aerospike_udf_list(as, &err, NULL, &files);
	assert_int_eq( err.code, AEROSPIKE_OK );

	info("files: ")
	for(uint32_t i=0; i<files.size; i++) {
		as_udf_file * file = &files.entries[i];
		info("- %s", file->name);
		if ( strcmp(file->name, filename) == 0 ) {
			exists = true;
		}
	}

	as_udf_files_destroy(&files);

	assert_false( exists );

	// upload the file

	as_bytes content;

	info("reading: %s",LUA_FILE);
	bool b = udf_readfile(LUA_FILE, &content);
	assert_true(b);

	info("uploading: %s",filename);
	aerospike_udf_put(as, &err, NULL, filename, AS_UDF_TYPE_LUA, &content);

	assert_int_eq( err.code, AEROSPIKE_OK );

	aerospike_udf_put_wait(as, &err, NULL, filename, 100);

	// list the files on the server

	as_udf_files_init(&files, 0);

	aerospike_udf_list(as, &err, NULL, &files);

	assert_int_eq( err.code, AEROSPIKE_OK );

	info("files: ")
	for(uint32_t i=0; i<files.size; i++) {
		as_udf_file * file = &files.entries[i];
		info("- %s", file->name);
		if ( strcmp(file->name, filename) == 0 ) {
			exists = true;
		}
	}

	as_udf_files_destroy(&files);

	assert_true( exists );

	// dowload the file

	as_udf_file file;
	as_udf_file_init(&file);

	info("downloading: %s", filename);
	aerospike_udf_get(as, &err, NULL, filename, AS_UDF_TYPE_LUA, &file);

	assert_int_eq( err.code, AEROSPIKE_OK );

	info("downloaded: %s size=%d", filename, file.content.size);
	assert_int_eq( file.content.size, content.size );

	as_udf_file_destroy(&file);

	// remove the file

	info("removing file: %s ", filename);
	aerospike_udf_remove(as, &err, NULL, filename);

	assert_int_eq( err.code, AEROSPIKE_OK );

	as_sleep(100);

	as_bytes_destroy(&content);
}

TEST( udf_basics_2 , "path-prefixed filename is stripped to basename for put, get and remove" ) {

	const char * path_prefixed = LUA_FILE;
	const char * basename_only = UDF_FILE".lua";

	as_error err;
	as_error_reset(&err);

	// Ensure clean state.
	aerospike_udf_remove(as, &err, NULL, basename_only);
	as_sleep(100);

	// Upload the file content.
	as_bytes content;
	bool b = udf_readfile(path_prefixed, &content);
	assert_true(b);

	// udf_put with a path-prefixed filename must succeed (as_basename strips).
	info("udf_put with path-prefixed filename: %s", path_prefixed);
	aerospike_udf_put(as, &err, NULL, path_prefixed, AS_UDF_TYPE_LUA, &content);
	assert_int_eq( err.code, AEROSPIKE_OK );

	aerospike_udf_put_wait(as, &err, NULL, basename_only, 100);

	// udf_get with a path-prefixed filename must succeed.
	as_udf_file file;
	as_udf_file_init(&file);

	info("udf_get with path-prefixed filename: %s", path_prefixed);
	aerospike_udf_get(as, &err, NULL, path_prefixed, AS_UDF_TYPE_LUA, &file);
	assert_int_eq( err.code, AEROSPIKE_OK );

	// file.name must contain the basename, not the full path.
	assert_string_eq( file.name, basename_only );
	assert_int_eq( file.content.size, content.size );

	as_udf_file_destroy(&file);

	// udf_remove with a path-prefixed filename must succeed.
	info("udf_remove with path-prefixed filename: %s", path_prefixed);
	aerospike_udf_remove(as, &err, NULL, path_prefixed);
	assert_int_eq( err.code, AEROSPIKE_OK );

	as_sleep(100);

	// Verify the module is gone.
	as_udf_files files;
	as_udf_files_init(&files, 0);
	aerospike_udf_list(as, &err, NULL, &files);
	assert_int_eq( err.code, AEROSPIKE_OK );

	bool still_exists = false;
	for (uint32_t i = 0; i < files.size; i++) {
		if ( strcmp(files.entries[i].name, basename_only) == 0 ) {
			still_exists = true;
		}
	}
	as_udf_files_destroy(&files);
	assert_false( still_exists );

	as_bytes_destroy(&content);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( udf_basics, "aerospike_udf basic tests" ) {
	suite_add( udf_basics_1 );
	suite_add( udf_basics_2 );
}
