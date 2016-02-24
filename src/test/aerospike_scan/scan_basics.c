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
#include <aerospike/aerospike_scan.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_info.h>

#include <aerospike/as_error.h>
#include <aerospike/as_status.h>

#include <aerospike/as_record.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_string.h>
#include <aerospike/as_list.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_map.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_stringmap.h>
#include <aerospike/as_val.h>

#include <aerospike/as_cluster.h>
#include <citrusleaf/cf_types.h>

#include "../test.h"
#include "../util/udf.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike * as;

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define LUA_FILE "src/test/aerospike_scan/aerospike_scan_test.lua"

#define NS "test"
#define SET_STRSZ 20
#define NUM_RECS_SET1 100
#define SET1 "sb_set1"
#define NUM_RECS_SET2 50
#define SET2 "sb_set2"
#define NUM_RECS_NULLSET 20

/******************************************************************************
 * TYPES
 *****************************************************************************/
typedef struct scan_check_s {
	bool failed;
	char * set;
	bool nobindata; // flag to be set when you dont expect to get back any bins 
	int count;
	char * bins[10];
	int unique_tcount;
	pthread_t threadids[32];
} scan_check;

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static bool scan_udf_info_callback(const as_error * err, const as_node * node, const char * req, char * res, void * udata) {

	if (!err) {
		goto done;
	}

	if ( err->code != AEROSPIKE_OK ) {
		debug("UDF_CALLBACK Error: (%d) %s - node=%s response=%s\n", err->code, err->message, node ? node->name : "NULL", res);
	}
	else {
		if ( res == NULL || strlen(res) == 0 ) {
			goto done;
		}
		char * start_resp = strchr(res, '\t');

		if ( start_resp == NULL || strlen(start_resp) == 0 ) {
			goto done;
		}
		char print_resp[128];
		as_strncpy(print_resp, start_resp, sizeof(print_resp));
	 	debug("%s", print_resp);
	}
done:
	return true;
}

static int check_bin1(as_record * rec, scan_check * check)
{
	as_val * bin = (as_val *) as_record_get(rec, "bin1");
	if ( !bin ) {
		error("Expected a value in bin('%s'), but got null", "bin1");
		return !(check->failed = true);
	}
	
	as_integer * integer = as_integer_fromval(bin);
	if ( !integer ) {
		error("Expected a integer in bin('%s'), but got type %d", "bin1", as_val_type(bin));
		return !(check->failed = true);
	}

	return !(check->failed = false);
}


static int check_bin2(as_record * rec, scan_check * check)
{
	as_val * bin = (as_val *) as_record_get(rec, "bin2");
	if ( !bin ) {
		error("Expected a value in bin('%s'), but got null", "bin2");
		return !(check->failed = true);
	}
	
	as_string * string = as_string_fromval(bin);
	if ( !string ) {
		error("Expected a string in bin('%s'), but got type %d", "bin2", as_val_type(bin));
		return !(check->failed = true);
	}
	
	char * str = as_string_get(string);
	if ( !str ) {
		error("Expected a string value but it is NULL");
		return !(check->failed = true);
	}

	// Check the string bin
	char expected[SET_STRSZ];
	int64_t bin1 = as_record_get_int64(rec, "bin1", INT64_MIN);
	sprintf(expected, "str-%s-%" PRId64, rec->key.set[0] == '\0' ? "noset" : rec->key.set, bin1);

	if (strcmp(expected, str) != 0) {
		error("Expected '%s' in bin('%s'), but got '%s'", expected, "bin2", str);
		return !(check->failed = true);
	}

	return !(check->failed = false);
}


static bool check_bin3(as_record * rec, scan_check * check)
{
	as_val * bin = (as_val *) as_record_get(rec, "bin3");
	if ( !bin ) {
		error("Expected a value in bin('%s'), but got null", "bin3");
		return !(check->failed = true);
	}

	as_map * map = as_map_fromval(bin);
	if ( !map ) {
		error("Expected a map in bin('%s'), but got type %d", "bin3", as_val_type(bin));
		return !(check->failed = true);
	}

	int sz = as_map_size(map);
	if ( sz != 3 ) {
		error("Expected map size of %d, but got %d", 3, sz);
		return !(check->failed = true);
	} 

	int64_t bin1 = as_record_get_int64(rec, "bin1", INT64_MIN);
	int64_t ival = 0;

	ival = as_stringmap_get_int64(map, "x");
	if ( ival != bin1 ) {
		error("Expected map value '%s'=%ld, but got %ld", "x", bin1, ival);
		return !(check->failed = true);
	}
	
	ival = as_stringmap_get_int64(map, "y");
	if ( ival != bin1+1 ) {
		error("Expected map value '%s'=%ld, but got %ld", "y", bin1+1, ival);
		return !(check->failed = true);
	}

	ival = as_stringmap_get_int64(map, "z");
	if ( ival != bin1+2 ) {
		error("Expected map value '%s'=%ld, but got %ld", "z", bin1+2, ival);
		return !(check->failed = true);
	}

	return !(check->failed = false);
}


static int check_bin4(as_record * rec, scan_check * check)
{
	as_val * bin = (as_val *) as_record_get(rec, "bin4");
	if ( !bin ) {
		error("Expected a value in bin('%s'), but got null", "bin4");
		return !(check->failed = true);
	}

	as_list * list = as_list_fromval(bin);
	if ( !list ) {
		error("Expected a list in bin('%s'), but got type %d", "bin4", as_val_type(bin));
		return !(check->failed = true);
	}

	int sz = as_list_size(list);
	if ( sz < 3 ) {
		error("Expected list size of %d, but got %d", 3, sz);
		return !(check->failed = true);
	}

	for ( int i = 0; i < sz; i++ ) {
		as_val * val = as_list_get(list, i);
		if ( !val ) {
			error("Expecting value at %d, but got null", i);
			return !(check->failed = true);
		}

		as_integer * ival = as_integer_fromval(val);
		if ( !ival ) {
			error("Expecting integer at %d, but got type %d", i, as_val_type(val));
			return !(check->failed = true);
		}
	}

	return !(check->failed = false);
}

static bool scan_check_callback(const as_val * val, void * udata) 
{
	int i;

	// NULL is END OF SCAN
	if ( !val ) {
		return false;
	}
	
	scan_check * check = (scan_check *) udata;

	as_record * rec = as_record_fromval(val);
	if ( !rec ) {
		error("Expected a record, but got type %d", as_val_type(val));
		return !(check->failed = true);
	}

	const char * set = rec->key.set[0] == '\0' ? NULL : rec->key.set;

	check->count++;
	// Find the number of unique threads spawned under the hood.
	// Note that the scan callback will be called in the thread context.
	// As the number of threads is same as node count, they will be limited.
	// A linear search is good enough for this.
	pthread_t cur_thread = pthread_self();
	for (i=0; i<check->unique_tcount; i++) {
		if (check->threadids[i] == cur_thread) {
			break;
		}
	}
	// Found a new thread
	if (i == check->unique_tcount) {
		check->threadids[check->unique_tcount] = cur_thread;
		check->unique_tcount++;
	}

	// Check if we are getting the results only from the set the scan is triggered for
	// If scan is called with NULL set, all the recs will come. So, no checks in this case.
	if ( check->set ) {
		// Do the check only if the rec also have a setname
		if ( !set ) {
			error("Expected set '%s', but got set NULL", check->set);
			return !(check->failed = true);
		}
		else if ( strcmp(check->set, set) != 0) {
			error("Expected set '%s', but got set '%s'", check->set, set);
			return !(check->failed = true);
		}
	}

	// Check that we got the right number of bins
	int numbins = as_record_numbins(rec);

	if ( check->nobindata ) {
		if ( numbins != 0 ) {
			error("Expected 0 bins, but got %d", numbins);
			return !(check->failed = true);
		}
		return !(check->failed = false);
	} 

	// only validate data if in sb_set1 or sb_set2
	if ( check->set && strcmp(set, SET1) != 0 && strcmp(set, SET2) != 0 ) {
		return !(check->failed = false);
	}

	// validate bins
	int nbins = sizeof(check->bins) / sizeof(char *);
	for( int i = 0; check->bins[i] && i < nbins; i++ ) {
		char * bin = check->bins[i];
		if ( strcmp(bin, "bin1") == 0 ) {
			if ( !check_bin1(rec, check) ) {
				error("Failed check of bin1");
				return !(check->failed = true);
			}
		}
		else if ( strcmp(bin, "bin2") == 0 ) {
			if ( !check_bin2(rec, check) ) {
				error("Failed check of bin2");
				return !(check->failed = true);
			}
		}
		else if ( strcmp(bin, "bin3") == 0 ) {
			if ( !check_bin3(rec, check) ) {
				error("Failed check of bin3");
				return !(check->failed = true);
			}
		}
		else if ( strcmp(bin, "bin4") == 0 ) {
			if ( !check_bin4(rec, check) ) {
				error("Failed check of bin4");
				return !(check->failed = true);
			}
		}
		else {
			error("Unknown bin %s", bin);
			return !(check->failed = true);
		}
	}

	return !(check->failed = false);
}

static void insert_data(int numrecs, const char *setname)
{
	as_status rc;
	char strval[SET_STRSZ], strkey[SET_STRSZ];

	as_error err;
	as_error_reset(&err);

	for (int i=0; i<numrecs; i++) {

		sprintf(strval, "str-%s-%d", setname ? setname : "noset", i);
		sprintf(strkey, "key-%s-%d", setname, i);

		// Map bin
        as_hashmap m;
        as_hashmap_init(&m, 8);
		as_stringmap_set_int64((as_map *) &m, "x", i);
		as_stringmap_set_int64((as_map *) &m, "y", i+1);
		as_stringmap_set_int64((as_map *) &m, "z", i+2);

        as_record r;
        as_record_init(&r, 3);
		as_record_set_int64(&r, "bin1", i);
		as_record_set_str(&r, "bin2", strval);
		as_record_set_map(&r, "bin3", (as_map *) &m);

        as_key k;
		as_key_init(&k, NS, setname, strkey);

		rc = aerospike_key_put(as, &err, NULL, &k, &r);
		if (rc != AEROSPIKE_OK) {
			error("digest put failed with error %d", rc);
		}

        as_hashmap_destroy(&m);
		as_key_destroy(&k);
        as_record_destroy(&r);
	}

}


/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( scan_basics_null_set , "full scan (using NULL setname)" ) {

	scan_check check = {
		.failed = false,
		.set = NULL,
		.count = 0,
		.nobindata = false,
		.bins = { NULL }
	};

	as_error err;

	as_scan scan;
	as_scan_init(&scan, NS, "");

	as_status rc = aerospike_scan_foreach(as, &err, NULL, &scan, scan_check_callback, &check);
	
	assert_int_eq( rc, AEROSPIKE_OK );
	assert_false( check.failed );

	// assert_int_eq( scan_data.ret_failed, false );
	// // We should get all the data that we inserted
	// int exp_rec_count = (NUM_RECS_SET1 + NUM_RECS_SET2 + NUM_RECS_NULLSET);
	// assert_true( scan_data.ret_rec_count >= exp_rec_count );
	// info("Got %d records in the scan. Expected atleast %d", scan_data.ret_rec_count, exp_rec_count);

	as_scan_destroy(&scan);
}

TEST( scan_basics_set1 , "scan "SET1"" ) {

	scan_check check = {
		.failed = false,
		.set = SET1,
		.count = 0,
		.nobindata = false,
		.bins = { "bin1", "bin2", "bin3", NULL },
		.unique_tcount = 0
	};

	as_error err;

	as_scan scan;
	as_scan_init(&scan, NS, SET1);

	as_status rc = aerospike_scan_foreach(as, &err, NULL, &scan, scan_check_callback, &check);
	
	assert_int_eq( rc, AEROSPIKE_OK );
	assert_false( check.failed );

	assert_int_eq( check.count, NUM_RECS_SET1 );
	info("Got %d records in the scan. Expected %d", check.count, NUM_RECS_SET1);
	info("Number of threads used = %d", check.unique_tcount);

	as_scan_destroy(&scan);
}

TEST( scan_basics_set1_concurrent , "scan "SET1" concurrently" ) {

	scan_check check = {
		.failed = false,
		.set = SET1,
		.count = 0,
		.nobindata = false,
		.bins = { "bin1", "bin2", "bin3", NULL },
		.unique_tcount = 0
	};

	as_error err;

	as_scan scan;
	as_scan_init(&scan, NS, SET1);
	as_scan_set_concurrent(&scan, true);

	as_status rc = aerospike_scan_foreach(as, &err, NULL, &scan, scan_check_callback, &check);
	
	assert_int_eq( rc, AEROSPIKE_OK );
	assert_false( check.failed );

	assert_int_eq( check.count, NUM_RECS_SET1 );
	info("Got %d records in the concurrent scan. Expected %d", check.count, NUM_RECS_SET1);
	info("Number of threads used = %d", check.unique_tcount);

	as_scan_destroy(&scan);
}

TEST( scan_basics_set1_select , "scan "SET1" and select 'bin1'" ) {

	scan_check check = {
		.failed = false,
		.set = SET1,
		.count = 0,
		.nobindata = false,
		.bins = { "bin1", NULL }
	};

	as_error err;

	as_scan scan;
	as_scan_init(&scan, NS, SET1);

	as_scan_select_inita(&scan, 1);
	as_scan_select(&scan, "bin1");

	as_status rc = aerospike_scan_foreach(as, &err, NULL, &scan, scan_check_callback, &check);
	
	assert_int_eq( rc, AEROSPIKE_OK );
	assert_false( check.failed );

	// assert_int_eq( scan_data.ret_failed, false );
	// assert_int_eq( scan_data.ret_rec_count, NUM_RECS_SET1 );
	// info("Got %d records in the scan. Expected %d", scan_data.ret_rec_count, NUM_RECS_SET1);

	as_scan_destroy(&scan);
}

TEST( scan_basics_set1_nodata , "scan "SET1" with no-bin-data" ) {

	scan_check check = {
		.failed = false,
		.set = SET1,
		.count = 0,
		.nobindata = true,
		.bins = { NULL }
	};

	as_error err;

	as_scan scan;
	as_scan_init(&scan, NS, SET1);
	as_scan_set_nobins(&scan, true);

	as_status rc = aerospike_scan_foreach(as, &err, NULL, &scan, scan_check_callback, &check);
	
	assert_int_eq( rc, AEROSPIKE_OK );
	assert_false( check.failed );

	// assert_int_eq( scan_data.ret_failed, false );
	// assert_int_eq( scan_data.ret_rec_count, NUM_RECS_SET1 );
	// info("Got %d records in the scan. Expected %d", scan_data.ret_rec_count, NUM_RECS_SET1);

	as_scan_destroy(&scan);
}


TEST( scan_basics_background , "scan "SET1" in background to insert a new bin" ) {

	scan_check check = {
		.failed = false,
		.set = SET1,
		.count = 0,
		.nobindata = false,
		.bins = { "bin1", "bin2", "bin3", "bin4", NULL }
	};

	as_error err;

	as_scan scan;
	as_scan_init(&scan, NS, SET1);
	as_scan_apply_each(&scan, "aerospike_scan_test", "scan_insert_bin4", NULL);

	uint64_t scanid = 0;
	as_status udf_rc = aerospike_scan_background(as, &err, NULL, &scan, &scanid);
	
	assert_int_eq( udf_rc, AEROSPIKE_OK );

	aerospike_scan_wait(as, &err, NULL, scanid, 0);

	// See if the above udf ran fine
	as_scan scan2;
	as_scan_init(&scan2, NS, SET1);

	as_status rc = aerospike_scan_foreach(as, &err, NULL, &scan2, scan_check_callback, &check);

	assert_int_eq( rc, AEROSPIKE_OK );
	assert_false( check.failed );

	as_scan_destroy(&scan);
	as_scan_destroy(&scan2);
}

TEST( scan_basics_background_poll_job_status , " Start a UDF scan job in the background and poll for job-status" ) {

	as_error err;

	as_scan scan;
	as_scan_init(&scan, NS, "");

	as_scan_apply_each(&scan, "aerospike_scan_test", "scan_dummy_read_update_rec", NULL);

	uint64_t scanid = 0;
	as_status udf_rc = aerospike_scan_background(as, &err, NULL, &scan, &scanid);
	
	assert_int_eq( udf_rc, AEROSPIKE_OK );

	as_status rc = AEROSPIKE_OK;
	as_error cb_err;
        
	debug("    This is a UDF Background Scan job-polling test .. ");  
	for(int i = 0; i < 5; i++){
		as_error_reset(&cb_err);
		rc = aerospike_info_foreach(as, &cb_err, NULL, "jobs:module=scan", scan_udf_info_callback, NULL);
		assert_int_eq( rc, AEROSPIKE_OK );
		sleep(1);
	}

	as_scan_destroy(&scan);

}

TEST( scan_basics_background_delete_bins , "Apply scan to count num-records in SET1, conditional-delete of bin1, verify that bin1 is gone" ) {

	scan_check check = {
		.failed = false,
		.set = SET1,
		.count = 0,
		.nobindata = false,
		.bins = { "bin1", "bin2", "bin3", NULL },
		.unique_tcount = 0
	};

	as_error err;

	as_scan scan;
	as_scan_init(&scan, NS, SET1);
	as_scan_set_concurrent(&scan, true);

	as_status rc = aerospike_scan_foreach(as, &err, NULL, &scan, scan_check_callback, &check);
	
	assert_int_eq( rc, AEROSPIKE_OK );
	assert_false( check.failed );

	assert_int_eq( check.count, NUM_RECS_SET1 );
	debug("Got %d records in the scan. Expected %d", check.count, NUM_RECS_SET1);

	as_scan scan_del;
	as_scan_init(&scan_del, NS, SET1);
	as_scan_apply_each(&scan_del, "aerospike_scan_test", "scan_delete_bin", NULL);

	uint64_t scanid = 0;
	as_status udf_rc = aerospike_scan_background(as, &err, NULL, &scan_del, &scanid);

	assert_int_eq( udf_rc, AEROSPIKE_OK );

	aerospike_scan_wait(as, &err, NULL, scanid, 0);

	debug("Got done with deletion of bin1 in SET1. "); 

	scan_check check2 = {
		.failed = false,
		.set = SET1,
		.count = 0,
		.nobindata = false,
		.bins = { NULL }, // look for all the bins 
		.unique_tcount = 0
	};
	as_scan scan2;
	as_scan_init(&scan2, NS, SET1);
	as_scan_set_concurrent(&scan2, true);

	rc = aerospike_scan_foreach(as, &err, NULL, &scan2, scan_check_callback, &check2);
	
	assert_int_eq( rc, AEROSPIKE_OK );
	assert_false( check2.failed );  

	assert_int_eq( check2.count,  NUM_RECS_SET1);
	debug("Got %d records in the scan after deletion ", check.count);
	
	as_scan_destroy(&scan);
	as_scan_destroy(&scan_del);
	as_scan_destroy(&scan2);
}

TEST( scan_basics_background_delete_records , "Apply scan to count num-records in SET1, delete some of them and verify the count after deletion" ) {

	scan_check check = {
		.failed = false,
		.set = SET1,
		.count = 0,
		.nobindata = false,
		.bins = { NULL }, // look for all the bins 
		.unique_tcount = 0
	};

	as_error err;

	as_scan scan;
	as_scan_init(&scan, NS, SET1);
	as_scan_set_concurrent(&scan, true);

	as_status rc = aerospike_scan_foreach(as, &err, NULL, &scan, scan_check_callback, &check);
	
	assert_int_eq( rc, AEROSPIKE_OK );
	assert_false( check.failed );

	assert_int_eq( check.count, NUM_RECS_SET1 );
	debug("Got %d records in the scan. Expected %d", check.count, NUM_RECS_SET1);

	as_scan scan_del;
	as_scan_init(&scan_del, NS, SET1);
	as_scan_apply_each(&scan_del, "aerospike_scan_test", "scan_delete_rec", NULL);

	uint64_t scanid = 0;
	as_status udf_rc = aerospike_scan_background(as, &err, NULL, &scan_del, &scanid);

	assert_int_eq( udf_rc, AEROSPIKE_OK );

	aerospike_scan_wait(as, &err, NULL, scanid, 0);

	debug("Got done with deletion of all the records in SET1. ");

	as_scan scan2;
	as_scan_init(&scan2, NS, SET1);
	as_scan_set_concurrent(&scan2, true);
	check.count = 0; // reset the param from previous call and re-use 

	rc = aerospike_scan_foreach(as, &err, NULL, &scan2, scan_check_callback, &check);
	
	assert_int_eq( rc, AEROSPIKE_OK );
	assert_false( check.failed );

	debug("Got %d records in the scan after deletion ", check.count);
	assert_int_eq( check.count, 0 );

	as_scan_destroy(&scan);
	as_scan_destroy(&scan_del);
	as_scan_destroy(&scan2);
}

TEST( scan_basics_background_sameid , "starting two udf scan of "SET2" in background with same scan-id" ) {

	as_error err;

	// insert a new bin using udf
	as_scan scan;
	as_scan_init(&scan, NS, SET2);
	as_scan_apply_each(&scan, "aerospike_scan_test", "scan_noop", NULL);

	uint64_t scanid = 0;
	as_status udf_rc = aerospike_scan_background(as, &err, NULL, &scan, &scanid);
	
	assert_int_eq( udf_rc, AEROSPIKE_OK );

	aerospike_scan_wait(as, &err, NULL, scanid, 0);
 
	as_scan scan2;
	as_scan_init(&scan2, NS, SET2);
	as_scan_apply_each(&scan2, "aerospike_scan_test", "scan_noop", NULL);

	uint64_t scanid2 = scanid;
	as_status udf_rc2 = aerospike_scan_background(as, &err, NULL, &scan2, &scanid2);

	// TODO - don't allow AEROSPIKE_ERR_SERVER when older servers are gone.
    assert_true( udf_rc2 == AEROSPIKE_ERR_SERVER || udf_rc2 == AEROSPIKE_ERR_REQUEST_INVALID );

	aerospike_scan_wait(as, &err, NULL, scanid, 0);

	as_scan_destroy(&scan);
	as_scan_destroy(&scan2);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

/**
 * Inserts records that can be used in the rest of the testcases
 */
static bool before(atf_suite * suite) {

	insert_data(NUM_RECS_SET1, SET1);
	insert_data(NUM_RECS_SET2, SET2);
	insert_data(NUM_RECS_NULLSET, NULL);

	if ( ! udf_put(LUA_FILE) ) {
		error("failure while uploading: %s", LUA_FILE);
		return false;
	}

	if ( ! udf_exists(LUA_FILE) ) {
		error("lua file does not exist: %s", LUA_FILE);
		return false;
	}

    return true;
}

static bool after(atf_suite * suite) {
	if ( ! udf_remove(LUA_FILE) ) {
		error("failure while removing: %s", LUA_FILE);
		return false;
	}

    return true;
}

SUITE( scan_basics, "aerospike_scan basic tests" ) {

	// For the cluster to settle as it is important for the per-node testcases
	sleep(1);
    suite_before( before );
    suite_after( after );

	suite_add( scan_basics_null_set );
	suite_add( scan_basics_set1 );
	suite_add( scan_basics_set1_concurrent );
	suite_add( scan_basics_set1_select );
	suite_add( scan_basics_set1_nodata );
	suite_add( scan_basics_background );
	suite_add( scan_basics_background_sameid );
	suite_add( scan_basics_background_poll_job_status );
	suite_add( scan_basics_background_delete_bins );
	suite_add( scan_basics_background_delete_records );
}
