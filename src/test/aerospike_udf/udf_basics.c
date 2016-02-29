/*
 * Copyright 2008-2016 Aerospike, Inc.
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

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike * as;

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define LUA_FILE "src/test/lua/udf_basics.lua"
#define UDF_FILE "udf_basics"

#define WAIT_MS(__ms) nanosleep((struct timespec[]){{0, __ms##000000}}, NULL)

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

	assert_int_eq( err.code, AEROSPIKE_ERR_UDF );

	WAIT_MS(100);

	// list the files on the server

	as_udf_files files;
	as_udf_files_init(&files, 0);
	
	aerospike_udf_list(as, &err, NULL, &files);
	assert_int_eq( err.code, AEROSPIKE_OK );

	info("files: ")
	for(int i=0; i<files.size; i++) {
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
	for(int i=0; i<files.size; i++) {
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

	WAIT_MS(100);

	as_bytes_destroy(&content);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( udf_basics, "aerospike_udf basic tests" ) {
	suite_add( udf_basics_1 );
}
