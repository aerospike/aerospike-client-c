/*
 * Copyright 2008-2023 Aerospike, Inc.
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
#include <aerospike/as_cluster.h>
#include <aerospike/as_status.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_exp_operations.h>
#include <aerospike/as_record.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_string.h>
#include <aerospike/as_list.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_map.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_sleep.h>
#include <aerospike/as_stringmap.h>
#include <aerospike/as_val.h>

#include "../test.h"
#include "../util/udf.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike* as;
extern bool g_has_ttl;

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define LUA_FILE AS_START_DIR "src/test/aerospike_scan/aerospike_scan_test.lua"

#define NS "test"
#define SET_STRSZ 100
#define NUM_RECS_SET1 100
#define SET1 "sb_set1"
#define NUM_RECS_SET2 50
#define SET2 "sb_set2"
#define NUM_RECS_NULLSET 20
#define NUM_RECS_SET3D 100
#define SET3D "sb_set3d"
#define NUM_RECS_SET3 100
#define SET3 "sb_set3"
#define NUM_RECS_SET4D 100
#define SET4D "sb_set4d"
#define NUM_RECS_SET4 100
#define SET4 "sb_set4"
#define NUM_RECS_SET5 50
#define SET5 "sb_set5"
#define NUM_RECS_SET6 10
#define SET6 "sb_set6"

/******************************************************************************
 * TYPES
 *****************************************************************************/
typedef struct scan_check_s {
	bool failed;
	char* set;
	bool allow_null_set;
	bool nobindata; // flag to be set when you dont expect to get back any bins 
	uint32_t count;
	char* bins[10];
} scan_check;

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static bool scan_udf_info_callback(
	const as_error* err, const as_node* node, const char* req, char* res, void* udata
	)
{
	if (!err) {
		goto done;
	}

	if (err->code != AEROSPIKE_OK) {
		debug("UDF_CALLBACK Error: (%d) %s - node=%s response=%s\n", err->code, err->message, node ? node->name : "NULL", res);
	}
	else {
		if (res == NULL || strlen(res) == 0) {
			goto done;
		}
		char* start_resp = strchr(res, '\t');

		if (start_resp == NULL || strlen(start_resp) == 0) {
			goto done;
		}
		char print_resp[128];
		as_strncpy(print_resp, start_resp, sizeof(print_resp));
		debug("%s", print_resp);
	}
done:
	return true;
}

static int check_bin1(as_record* rec, scan_check* check)
{
	as_val* bin = (as_val*) as_record_get(rec, "bin1");
	if (!bin) {
		error("Expected a value in bin('%s'), but got null", "bin1");
		return !(check->failed = true);
	}

	as_integer* integer = as_integer_fromval(bin);
	if (!integer) {
		error("Expected a integer in bin('%s'), but got type %d", "bin1", as_val_type(bin));
		return !(check->failed = true);
	}

	return !(check->failed = false);
}


static int check_bin2(as_record* rec, scan_check* check)
{
	as_val* bin = (as_val*) as_record_get(rec, "bin2");
	if (!bin) {
		error("Expected a value in bin('%s'), but got null", "bin2");
		return !(check->failed = true);
	}

	as_string* string = as_string_fromval(bin);
	if (!string) {
		error("Expected a string in bin('%s'), but got type %d", "bin2", as_val_type(bin));
		return !(check->failed = true);
	}

	char* str = as_string_get(string);
	if (!str) {
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

static bool check_bin3(as_record* rec, scan_check* check)
{
	as_val* bin = (as_val*) as_record_get(rec, "bin3");
	if (!bin) {
		error("Expected a value in bin('%s'), but got null", "bin3");
		return !(check->failed = true);
	}

	as_map * map = as_map_fromval(bin);
	if (!map) {
		error("Expected a map in bin('%s'), but got type %d", "bin3", as_val_type(bin));
		return !(check->failed = true);
	}

	int sz = as_map_size(map);
	if (sz != 3) {
		error("Expected map size of %d, but got %d", 3, sz);
		return !(check->failed = true);
	} 

	int64_t bin1 = as_record_get_int64(rec, "bin1", INT64_MIN);
	int64_t ival = 0;

	ival = as_stringmap_get_int64(map, "x");
	if (ival != bin1) {
		error("Expected map value '%s'=%ld, but got %ld", "x", bin1, ival);
		return !(check->failed = true);
	}

	ival = as_stringmap_get_int64(map, "y");
	if (ival != bin1+1) {
		error("Expected map value '%s'=%ld, but got %ld", "y", bin1+1, ival);
		return !(check->failed = true);
	}

	ival = as_stringmap_get_int64(map, "z");
	if (ival != bin1+2) {
		error("Expected map value '%s'=%ld, but got %ld", "z", bin1+2, ival);
		return !(check->failed = true);
	}

	return !(check->failed = false);
}

static int check_bin4(as_record* rec, scan_check* check)
{
	as_val* bin = (as_val*) as_record_get(rec, "bin4");
	if (!bin) {
		error("Expected a value in bin('%s'), but got null", "bin4");
		return !(check->failed = true);
	}

	as_list* list = as_list_fromval(bin);
	if (!list) {
		error("Expected a list in bin('%s'), but got type %d", "bin4", as_val_type(bin));
		return !(check->failed = true);
	}

	int sz = as_list_size(list);
	if (sz < 3) {
		error("Expected list size of %d, but got %d", 3, sz);
		return !(check->failed = true);
	}

	for (int i = 0; i < sz; i++) {
		as_val* val = as_list_get(list, i);
		if (!val) {
			error("Expecting value at %d, but got null", i);
			return !(check->failed = true);
		}

		as_integer* ival = as_integer_fromval(val);
		if (!ival) {
			error("Expecting integer at %d, but got type %d", i, as_val_type(val));
			return !(check->failed = true);
		}
	}

	return !(check->failed = false);
}

static bool scan_check_callback(const as_val* val, void* udata) 
{
	// NULL is END OF SCAN
	if (!val) {
		return false;
	}

	scan_check* check = (scan_check*) udata;

	as_record* rec = as_record_fromval(val);
	if (!rec) {
		error("Expected a record, but got type %d", as_val_type(val));
		return !(check->failed = true);
	}

	const char* set = rec->key.set[0] == '\0' ? NULL : rec->key.set;

	as_incr_uint32(&check->count);

	// Check if we are getting the results only from the set the scan is triggered for
	// If scan is called with NULL set, all the recs will come. So, no checks in this case.
	if (check->set) {
		// Do the check only if the rec also have a setname
		if (!set) {
			if (!check->allow_null_set) {
				error("Expected set '%s', but got set NULL", check->set);
				return !(check->failed = true);
			}
			else {
				return !(check->failed = false);
			}
		}
		else if (strcmp(check->set, set) != 0) {
			error("Expected set '%s', but got set '%s'", check->set, set);
			return !(check->failed = true);
		}
	}

	// Check that we got the right number of bins
	int numbins = as_record_numbins(rec);

	if (check->nobindata) {
		if (numbins != 0) {
			error("Expected 0 bins, but got %d", numbins);
			return !(check->failed = true);
		}
		return !(check->failed = false);
	} 

	// only validate data if in sb_set1 or sb_set2
	if (set && strcmp(set, SET1) != 0 && strcmp(set, SET2) != 0) {
		return !(check->failed = false);
	}

	// validate bins
	int nbins = sizeof(check->bins) / sizeof(char*);
	for(int i = 0; i < nbins && check->bins[i]; i++) {
		char* bin = check->bins[i];
		if (strcmp(bin, "bin1") == 0) {
			if (!check_bin1(rec, check)) {
				error("Failed check of bin1");
				return !(check->failed = true);
			}
		}
		else if (strcmp(bin, "bin2") == 0) {
			if (!check_bin2(rec, check)) {
				error("Failed check of bin2");
				return !(check->failed = true);
			}
		}
		else if (strcmp(bin, "bin3") == 0) {
			if (!check_bin3(rec, check)) {
				error("Failed check of bin3");
				return !(check->failed = true);
			}
		}
		else if (strcmp(bin, "bin4") == 0) {
			if (!check_bin4(rec, check)) {
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

static void insert_data(int numrecs, const char* setname)
{
	as_status rc;
	char strval[SET_STRSZ], strkey[SET_STRSZ];

	as_error err;
	as_error_reset(&err);

	uint32_t the_ttl = AS_RECORD_NO_EXPIRE_TTL;

	for (int i=0; i<numrecs; i++) {
		if (i == 30) {
			// We change the TTL from never to 100 days
			the_ttl = 100 * 24 * 60 * 60;
		}

		const char* sn = setname ? setname : "noset";
		sprintf(strval, "str-%s-%d", sn, i);
		sprintf(strkey, "key-%s-%d", sn, i);

		// Map bin
		as_hashmap m;
		as_hashmap_init(&m, 8);
		as_stringmap_set_int64((as_map *) &m, "x", i);
		as_stringmap_set_int64((as_map *) &m, "y", i+1);
		as_stringmap_set_int64((as_map *) &m, "z", i+2);

		as_record r;
		as_record_init(&r, i < 10 ? 4 : 3);
		as_record_set_int64(&r, "bin1", i);
		as_record_set_str(&r, "bin2", strval);
		as_record_set_map(&r, "bin3", (as_map *) &m);

		if (i < 10) {
			as_record_set_int64(&r, "otherBin", i);
		}

		r.ttl = the_ttl;

		as_key k;
		as_key_init(&k, NS, setname, strkey);

		as_policy_write p;
		as_policy_write_init(&p);

		p.key = AS_POLICY_KEY_SEND;

		rc = aerospike_key_put(as, &err, &p, &k, &r);

		if (rc != AEROSPIKE_OK) {
			error("digest put failed with error %d", rc);
		}

		as_hashmap_destroy(&m);
		as_key_destroy(&k);
		as_record_destroy(&r);
	}
}

static void insert_data_int_key(int numrecs, const char* setname)
{
	as_status rc;
	char strval[SET_STRSZ], strkey[SET_STRSZ];

	as_error err;
	as_error_reset(&err);

	uint32_t the_ttl = AS_RECORD_NO_EXPIRE_TTL;

	for (int i=0; i<numrecs; i++) {
		if (i == 30) {
			// We change the TTL from never to 100 days
			the_ttl = 100 * 24 * 60 * 60;
		}

		const char* sn = setname ? setname : "noset";
		sprintf(strval, "str-%s-%d", sn, i);
		sprintf(strkey, "key-%s-%d", sn, i);

		// Map bin
		as_hashmap m;
		as_hashmap_init(&m, 8);
		as_stringmap_set_int64((as_map *) &m, "x", i);
		as_stringmap_set_int64((as_map *) &m, "y", i+1);
		as_stringmap_set_int64((as_map *) &m, "z", i+2);

		as_record r;
		as_record_init(&r, i < 10 ? 4 : 3);
		as_record_set_int64(&r, "bin1", i);
		as_record_set_str(&r, "bin2", strval);
		as_record_set_map(&r, "bin3", (as_map *) &m);

		if (i < 10) {
			as_record_set_int64(&r, "otherBin", i);
		}

		r.ttl = the_ttl;

		as_key k;
		as_key_init_int64(&k, NS, setname, i);

		as_policy_write p;
		as_policy_write_init(&p);

		p.key = AS_POLICY_KEY_SEND;

		rc = aerospike_key_put(as, &err, &p, &k, &r);

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

TEST(scan_basics_null_set , "full scan (using NULL setname)")
{
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

	assert_int_eq(rc, AEROSPIKE_OK);
	assert_false(check.failed);

	// assert_int_eq(scan_data.ret_failed, false);
	// // We should get all the data that we inserted
	// int exp_rec_count = (NUM_RECS_SET1 + NUM_RECS_SET2 + NUM_RECS_NULLSET);
	// assert_true(scan_data.ret_rec_count >= exp_rec_count);
	// info("Got %d records in the scan. Expected atleast %d", scan_data.ret_rec_count, exp_rec_count);

	as_scan_destroy(&scan);
}

TEST(scan_basics_set1 , "scan "SET1"")
{
	scan_check check = {
		.failed = false,
		.set = SET1,
		.count = 0,
		.nobindata = false,
		.bins = { "bin1", "bin2", "bin3", NULL }
	};

	as_error err;

	as_scan scan;
	as_scan_init(&scan, NS, SET1);

	as_status rc = aerospike_scan_foreach(as, &err, NULL, &scan, scan_check_callback, &check);

	assert_int_eq(rc, AEROSPIKE_OK);
	assert_false(check.failed);

	assert_int_eq(check.count, NUM_RECS_SET1);
	info("Got %d records in the scan. Expected %d", check.count, NUM_RECS_SET1);

	as_scan_destroy(&scan);
}

TEST(scan_filter_set1 , "scan "SET1" w/ 25 <= bin1 <= 33")
{
	scan_check check = {
		.failed = false,
		.set = SET1,
		.count = 0,
		.nobindata = false,
		.bins = { "bin1", "bin2", "bin3", NULL },
	};

	as_error err;

	as_scan scan;
	as_scan_init(&scan, NS, SET1);

	as_exp_build(filter,
		as_exp_and(
			as_exp_cmp_le(as_exp_bin_int("bin1"), as_exp_int(33)),
			as_exp_cmp_ge(as_exp_bin_int("bin1"), as_exp_int(25))));

	as_policy_scan p;
	as_policy_scan_init(&p);
	p.base.filter_exp = filter;

	as_status rc = aerospike_scan_foreach(as, &err, &p, &scan, scan_check_callback, &check);

	assert_int_eq(rc, AEROSPIKE_OK);
	assert_false(check.failed);

	assert_int_eq(check.count, 9);
	info("Got %d records in the scan. Expected %d", check.count, 9);

	as_scan_destroy(&scan);
	as_exp_destroy(filter);
}

TEST(scan_basics_set1_concurrent , "scan "SET1" concurrently")
{
	scan_check check = {
		.failed = false,
		.set = SET1,
		.count = 0,
		.nobindata = false,
		.bins = { "bin1", "bin2", "bin3", NULL }
	};

	as_error err;

	as_scan scan;
	as_scan_init(&scan, NS, SET1);
	as_scan_set_concurrent(&scan, true);

	as_status rc = aerospike_scan_foreach(as, &err, NULL, &scan, scan_check_callback, &check);

	assert_int_eq(rc, AEROSPIKE_OK);
	assert_false(check.failed);

	assert_int_eq(check.count, NUM_RECS_SET1);
	info("Got %d records in the concurrent scan. Expected %d", check.count, NUM_RECS_SET1);

	as_scan_destroy(&scan);
}

TEST(scan_basics_set1_select , "scan "SET1" and select 'bin1'")
{
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

	assert_int_eq(rc, AEROSPIKE_OK);
	assert_false(check.failed);

	// assert_int_eq(scan_data.ret_failed, false);
	// assert_int_eq(scan_data.ret_rec_count, NUM_RECS_SET1);
	// info("Got %d records in the scan. Expected %d", scan_data.ret_rec_count, NUM_RECS_SET1);

	as_scan_destroy(&scan);
}

TEST(scan_basics_set1_nodata , "scan "SET1" with no-bin-data")
{
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

	assert_int_eq(rc, AEROSPIKE_OK);
	assert_false(check.failed);

	// assert_int_eq(scan_data.ret_failed, false);
	// assert_int_eq(scan_data.ret_rec_count, NUM_RECS_SET1);
	// info("Got %d records in the scan. Expected %d", scan_data.ret_rec_count, NUM_RECS_SET1);

	as_scan_destroy(&scan);
}

TEST(scan_basics_background , "scan "SET1" in background to insert a new bin")
{
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

	assert_int_eq(udf_rc, AEROSPIKE_OK);

	aerospike_scan_wait(as, &err, NULL, scanid, 0);

	// See if the above udf ran fine
	as_scan scan2;
	as_scan_init(&scan2, NS, SET1);

	as_status rc = aerospike_scan_foreach(as, &err, NULL, &scan2, scan_check_callback, &check);

	assert_int_eq(rc, AEROSPIKE_OK);
	assert_false(check.failed);

	as_scan_destroy(&scan);
	as_scan_destroy(&scan2);
}

TEST(scan_basics_background_poll_job_status , "Start a UDF scan job in the background and poll for job-status")
{
	as_error err;

	as_scan scan;
	as_scan_init(&scan, NS, SET1);

	as_scan_apply_each(&scan, "aerospike_scan_test", "scan_dummy_read_update_rec", NULL);

	uint64_t scanid = 0;
	as_status udf_rc = aerospike_scan_background(as, &err, NULL, &scan, &scanid);

	assert_int_eq(udf_rc, AEROSPIKE_OK);

	as_status rc = AEROSPIKE_OK;
	as_error cb_err;

	debug("    This is a UDF Background Scan job-polling test .. ");
	as_node* node = as_node_get_random(as->cluster);
	assert_not_null(node);
	char* cmd;

	if (as->cluster->has_partition_query) {
		// query-show works for both scan and query.
		cmd = "query-show";
	}
	else if (node->features & AS_FEATURES_QUERY_SHOW) {
		// scan-show and query-show are separate.
		cmd = "scan-show";
	}
	else {
		// old job monitor syntax.
		cmd = "jobs:module=scan";
	}
	as_node_release(node);

	for(int i = 0; i < 5; i++){
		as_error_reset(&cb_err);
		rc = aerospike_info_foreach(as, &cb_err, NULL, cmd, scan_udf_info_callback, NULL);
		assert_int_eq(rc, AEROSPIKE_OK);
		as_sleep(1 * 1000);
	}

	as_scan_destroy(&scan);
}

TEST(scan_basics_background_delete_bins , "Apply scan to count num-records in SET1, conditional-delete of bin1, verify that bin1 is gone")
{
	scan_check check = {
		.failed = false,
		.set = SET1,
		.count = 0,
		.nobindata = false,
		.bins = { "bin1", "bin2", "bin3", NULL }
	};

	as_error err;

	as_scan scan;
	as_scan_init(&scan, NS, SET1);
	as_scan_set_concurrent(&scan, true);

	as_status rc = aerospike_scan_foreach(as, &err, NULL, &scan, scan_check_callback, &check);

	assert_int_eq(rc, AEROSPIKE_OK);
	assert_false(check.failed);

	assert_int_eq(check.count, NUM_RECS_SET1);
	debug("Got %d records in the scan. Expected %d", check.count, NUM_RECS_SET1);

	as_scan scan_del;
	as_scan_init(&scan_del, NS, SET1);
	as_scan_apply_each(&scan_del, "aerospike_scan_test", "scan_delete_bin", NULL);

	uint64_t scanid = 0;
	as_status udf_rc = aerospike_scan_background(as, &err, NULL, &scan_del, &scanid);

	assert_int_eq(udf_rc, AEROSPIKE_OK);

	aerospike_scan_wait(as, &err, NULL, scanid, 0);

	debug("Got done with deletion of bin1 in SET1. "); 

	scan_check check2 = {
		.failed = false,
		.set = SET1,
		.count = 0,
		.nobindata = false,
		.bins = { NULL } // look for all the bins
	};
	as_scan scan2;
	as_scan_init(&scan2, NS, SET1);
	as_scan_set_concurrent(&scan2, true);

	rc = aerospike_scan_foreach(as, &err, NULL, &scan2, scan_check_callback, &check2);

	assert_int_eq(rc, AEROSPIKE_OK);
	assert_false(check2.failed);  

	assert_int_eq(check2.count,  NUM_RECS_SET1);
	debug("Got %d records in the scan after deletion ", check.count);

	as_scan_destroy(&scan);
	as_scan_destroy(&scan_del);
	as_scan_destroy(&scan2);
}

TEST(scan_basics_background_delete_records_rec_filter , "scan_basics_background_delete_records_rec_filter")
{
	scan_check check = {
		.failed = false,
		.set = SET3,
		.count = 0,
		.nobindata = false,
		.bins = { NULL } // look for all the bins
	};

	as_error err;

	as_scan scan;
	as_scan_init(&scan, NS, SET3);
	as_scan_set_concurrent(&scan, true);

	as_status rc = aerospike_scan_foreach(as, &err, NULL, &scan, scan_check_callback, &check);

	assert_int_eq(rc, AEROSPIKE_OK);
	assert_false(check.failed);

	assert_int_eq(check.count, NUM_RECS_SET3);
	debug("Got %d records in the scan. Expected %d", check.count, NUM_RECS_SET3);

	as_scan scan_del;
	as_scan_init(&scan_del, NS, SET3);

	as_exp_build(filter,
		as_exp_cmp_ge(as_exp_bin_int("bin1"), as_exp_int(90)));

	as_policy_scan p;
	as_policy_scan_init(&p);
	p.base.filter_exp = filter;

	as_scan_apply_each(&scan_del, "aerospike_scan_test", "scan_delete_rec", NULL);

	uint64_t scanid = 0;
	as_status udf_rc = aerospike_scan_background(as, &err, &p, &scan_del, &scanid);

	assert_int_eq(udf_rc, AEROSPIKE_OK);

	aerospike_scan_wait(as, &err, NULL, scanid, 0);

	debug("Got done with deletion of some of the records in SET3. ");

	as_scan scan2;
	as_scan_init(&scan2, NS, SET3);
	as_scan_set_concurrent(&scan2, true);
	check.count = 0; // reset the param from previous call and re-use 

	rc = aerospike_scan_foreach(as, &err, NULL, &scan2, scan_check_callback, &check);

	assert_int_eq(rc, AEROSPIKE_OK);
	assert_false(check.failed);

	debug("Got %d records in the scan after deletion ", check.count);
	assert_int_eq(check.count, 90);

	as_scan_destroy(&scan);
	as_scan_destroy(&scan_del);
	as_scan_destroy(&scan2);

	as_exp_destroy(filter);
}

TEST(scan_basics_background_delete_records_md_filter , "scan_basics_background_delete_records_md_filter")
{
	scan_check check = {
		.failed = false,
		.set = SET4,
		.count = 0,
		.nobindata = false,
		.bins = { NULL } // look for all the bins
	};

	as_error err;

	as_scan scan;
	as_scan_init(&scan, NS, SET4);
	as_scan_set_concurrent(&scan, true);

	as_status rc = aerospike_scan_foreach(as, &err, NULL, &scan, scan_check_callback, &check);

	assert_int_eq(rc, AEROSPIKE_OK);
	assert_false(check.failed);

	assert_int_eq(check.count, NUM_RECS_SET4);
	debug("Got %d records in the scan. Expected %d", check.count, NUM_RECS_SET4);

	as_scan scan_del;
	as_scan_init(&scan_del, NS, SET4);

	as_exp_build(filter,
		as_exp_cmp_eq(as_exp_int(-1), as_exp_void_time()));

	as_policy_scan p;
	as_policy_scan_init(&p);
	p.base.filter_exp = filter;

	as_scan_apply_each(&scan_del, "aerospike_scan_test", "scan_delete_rec", NULL);

	uint64_t scanid = 0;
	as_status udf_rc = aerospike_scan_background(as, &err, &p, &scan_del, &scanid);

	assert_int_eq(udf_rc, AEROSPIKE_OK);

	aerospike_scan_wait(as, &err, NULL, scanid, 0);

	debug("Got done with deletion of some of the records in SET4. ");

	as_scan scan2;
	as_scan_init(&scan2, NS, SET4);
	as_scan_set_concurrent(&scan2, true);
	check.count = 0; // reset the param from previous call and re-use 

	rc = aerospike_scan_foreach(as, &err, NULL, &scan2, scan_check_callback, &check);

	assert_int_eq(rc, AEROSPIKE_OK);
	assert_false(check.failed);

	debug("Got %d records in the scan after deletion ", check.count);
	assert_int_eq(check.count, 70);

	as_scan_destroy(&scan);
	as_scan_destroy(&scan_del);
	as_scan_destroy(&scan2);
	as_exp_destroy(filter);
}

TEST(scan_basics_background_delete_records , "Apply scan to count num-records in SET1, delete some of them and verify the count after deletion")
{
	scan_check check = {
		.failed = false,
		.set = SET1,
		.count = 0,
		.nobindata = false,
		.bins = { NULL } // look for all the bins
	};

	as_error err;

	as_scan scan;
	as_scan_init(&scan, NS, SET1);
	as_scan_set_concurrent(&scan, true);

	as_status rc = aerospike_scan_foreach(as, &err, NULL, &scan, scan_check_callback, &check);

	assert_int_eq(rc, AEROSPIKE_OK);
	assert_false(check.failed);

	assert_int_eq(check.count, NUM_RECS_SET1);
	debug("Got %d records in the scan. Expected %d", check.count, NUM_RECS_SET1);

	as_scan scan_del;
	as_scan_init(&scan_del, NS, SET1);
	as_scan_apply_each(&scan_del, "aerospike_scan_test", "scan_delete_rec", NULL);

	uint64_t scanid = 0;
	as_status udf_rc = aerospike_scan_background(as, &err, NULL, &scan_del, &scanid);

	assert_int_eq(udf_rc, AEROSPIKE_OK);

	aerospike_scan_wait(as, &err, NULL, scanid, 0);

	debug("Got done with deletion of all the records in SET1. ");

	as_scan scan2;
	as_scan_init(&scan2, NS, SET1);
	as_scan_set_concurrent(&scan2, true);
	check.count = 0; // reset the param from previous call and re-use 

	rc = aerospike_scan_foreach(as, &err, NULL, &scan2, scan_check_callback, &check);

	assert_int_eq(rc, AEROSPIKE_OK);
	assert_false(check.failed);

	debug("Got %d records in the scan after deletion ", check.count);
	assert_int_eq(check.count, 0);

	as_scan_destroy(&scan);
	as_scan_destroy(&scan_del);
	as_scan_destroy(&scan2);
}

TEST(scan_basics_background_sameid , "starting two udf scan of "SET2" in background with same scan-id")
{
	as_error err;

	// insert a new bin using udf
	as_scan scan;
	as_scan_init(&scan, NS, SET2);
	as_scan_apply_each(&scan, "aerospike_scan_test", "scan_noop", NULL);

	uint64_t scanid = 0;
	as_status udf_rc = aerospike_scan_background(as, &err, NULL, &scan, &scanid);

	assert_int_eq(udf_rc, AEROSPIKE_OK);

	aerospike_scan_wait(as, &err, NULL, scanid, 0);
 
	as_scan scan2;
	as_scan_init(&scan2, NS, SET2);
	as_scan_apply_each(&scan2, "aerospike_scan_test", "scan_noop", NULL);

	uint64_t scanid2 = scanid;
	as_status udf_rc2 = aerospike_scan_background(as, &err, NULL, &scan2, &scanid2);

	// TODO - don't allow AEROSPIKE_ERR_SERVER when older servers are gone.
	assert_true(udf_rc2 == AEROSPIKE_ERR_SERVER || udf_rc2 == AEROSPIKE_ERR_REQUEST_INVALID);

	aerospike_scan_wait(as, &err, NULL, scanid, 0);

	as_scan_destroy(&scan);
	as_scan_destroy(&scan2);
}

TEST(scan_operate, "scan operate")
{
	as_error err;
	as_string str;
	as_string_init(&str, "bar", false);

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_write(&ops, "foo", (as_bin_value*)&str);

	as_scan scan;
	as_scan_init(&scan, NS, SET2);
	scan.ops = &ops;

	uint64_t scanid = 0;
	as_status status = aerospike_scan_background(as, &err, NULL, &scan, &scanid);
	assert_int_eq(status, AEROSPIKE_OK);
	as_scan_destroy(&scan);

	aerospike_scan_wait(as, &err, NULL, scanid, 0);

	as_key key;
	as_key_init(&key, NS, SET2, "key-" SET2 "-5");
	const char* bins[2] = {"foo", NULL};

	as_record* rec = NULL;

	status = aerospike_key_select(as, &err, NULL, &key, bins, &rec);
	assert_int_eq(status, AEROSPIKE_OK);

	char* result = as_record_get_str(rec, "foo");
	assert_not_null(result);
	assert_string_eq(result, "bar");

	as_record_destroy(rec);
}

TEST(scan_operate_ttl, "scan operate ttl")
{
	as_error err;
	as_string str;
	as_string_init(&str, "bar", false);
	uint32_t ttl = 123456;
	as_operations ops;
	as_operations_inita(&ops, 1);
	ops.ttl = ttl;
	as_operations_add_touch(&ops);
	as_scan scan;
	as_scan_init(&scan, NS, SET2);
	scan.ops = &ops;

	uint64_t scanid = 0;
	as_status status = aerospike_scan_background(as, &err, NULL, &scan, &scanid);
	assert_int_eq(status, AEROSPIKE_OK);
	as_scan_destroy(&scan);

	aerospike_scan_wait(as, &err, NULL, scanid, 0);

	as_key key;
	as_key_init(&key, NS, SET2, "key-" SET2 "-5");

	as_record* rec = NULL;
	status = aerospike_key_get(as, &err, NULL, &key, &rec);

	assert_int_eq(status, AEROSPIKE_OK);

	// Current ttl should be within 2 seconds of original ttl.
	if (! (rec->ttl >= ttl - 2 && rec->ttl <= ttl)) {
		assert_int_eq(rec->ttl, ttl);
	}
	as_record_destroy(rec);
}

TEST(scan_operate_expop, "scan operate expop")
{
	as_error err;
	as_string str;
	as_string_init(&str, "bar", false);
	const char* binname = "scan-expop";

	as_exp_build(exp, as_exp_cond(
			as_exp_bin_exists(binname), as_exp_add(as_exp_bin_int(binname), as_exp_int(4)),
			as_exp_int(4)));
	assert_not_null(exp);

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_exp_write(&ops, binname, exp, AS_EXP_WRITE_DEFAULT);

	as_scan scan;
	as_scan_init(&scan, NS, SET2);
	scan.ops = &ops;

	uint64_t scanid = 0;
	as_status status = aerospike_scan_background(as, &err, NULL, &scan, &scanid);
	assert_int_eq(status, AEROSPIKE_OK);
	as_scan_destroy(&scan);

	aerospike_scan_wait(as, &err, NULL, scanid, 0);

	as_key key;
	as_key_init(&key, NS, SET2, "key-" SET2 "-5");
	const char* bins[2] = {binname, NULL};

	as_record* rec = NULL;

	status = aerospike_key_select(as, &err, NULL, &key, bins, &rec);
	assert_int_eq(status, AEROSPIKE_OK);

	int64_t result = as_record_get_int64(rec, binname, 0);
	assert_int_eq(result, 4);

	as_record_destroy(rec);
	as_exp_destroy(exp);
}

TEST(scan_filter_set_name, "scan filter set_name")
{
	scan_check check = {
		.failed = false,
		.set = SET5,
		.allow_null_set = true,
		.count = 0,
		.nobindata = false,
		.bins = { "bin1", "bin2", "bin3", NULL },
	};
	as_error err;
	as_scan scan;

	as_scan_init(&scan, NS, NULL);

	as_exp_build(filter,
		as_exp_or(
			as_exp_cmp_eq(as_exp_str(""), as_exp_set_name()),
			as_exp_cmp_eq(as_exp_str(SET5), as_exp_set_name())));

	as_policy_scan p;
	as_policy_scan_init(&p);

	p.base.filter_exp = filter;

	as_status rc = aerospike_scan_foreach(as, &err, &p, &scan, scan_check_callback, &check);

	assert_int_eq(rc, AEROSPIKE_OK);
	assert_false(check.failed);

	int expected = NUM_RECS_SET5 + NUM_RECS_NULLSET;

	assert_int_eq(check.count, expected);
	info("Got %d records in the scan. Expected %d", check.count, expected);

	as_exp_destroy(filter);
	as_scan_destroy(&scan);
}

TEST(scan_filter_rec_ttl, "scan filter record ttl")
{
	scan_check check = {
		.failed = false,
		.set = SET5,
		.count = 0,
		.nobindata = false,
		.bins = { "bin1", "bin2", "bin3", NULL },
	};
	as_error err;
	as_scan scan;

	as_scan_init(&scan, NS, SET5);

	as_exp_build(filter,
		as_exp_cmp_gt(as_exp_ttl(), as_exp_int(1)));

	as_policy_scan p;

	as_policy_scan_init(&p);

	p.base.filter_exp = filter;

	as_status rc = aerospike_scan_foreach(as, &err, &p, &scan, scan_check_callback, &check);

	assert_int_eq(rc, AEROSPIKE_OK);
	assert_false(check.failed);

	assert_int_eq(check.count, 20);
	info("Got %d records in the scan. Expected %d", check.count, 20);

	as_exp_destroy(filter);
	as_scan_destroy(&scan);
}

TEST(scan_filter_rec_str_key, "scan filter record str key")
{
	scan_check check = {
		.failed = false,
		.set = SET6,
		.count = 0,
		.nobindata = false,
		.bins = { "bin1", "bin2", "bin3", NULL },
	};
	as_error err;
	as_scan scan;

	as_scan_init(&scan, NS, SET6);

	as_exp_build(filter,
		as_exp_cmp_regex(0, "^key-.*-0$", as_exp_key_str()));

	as_policy_scan p;

	as_policy_scan_init(&p);

	p.base.filter_exp = filter;

	as_status rc = aerospike_scan_foreach(as, &err, &p, &scan, scan_check_callback, &check);

	assert_int_eq(rc, AEROSPIKE_OK);
	assert_false(check.failed);

	assert_int_eq(check.count, 1);
	info("Got %d records in the scan. Expected %d", check.count, 1);

	as_exp_destroy(filter);
	as_scan_destroy(&scan);
}

TEST(scan_filter_rec_int_key, "scan filter record int key")
{
	scan_check check = {
		.failed = false,
		.set = SET6,
		.count = 0,
		.nobindata = false,
		.bins = { "bin1", "bin2", "bin3", NULL },
	};
	as_error err;
	as_scan scan;

	as_scan_init(&scan, NS, SET6);

	as_exp_build(filter,
		as_exp_cmp_eq(as_exp_int(1), as_exp_key_int()));

	as_policy_scan p;

	as_policy_scan_init(&p);

	p.base.filter_exp = filter;

	as_status rc = aerospike_scan_foreach(as, &err, &p, &scan, scan_check_callback, &check);

	assert_int_eq(rc, AEROSPIKE_OK);
	assert_false(check.failed);

	assert_int_eq(check.count, 1);
	info("Got %d records in the scan. Expected %d", check.count, 1);

	as_exp_destroy(filter);
	as_scan_destroy(&scan);
}

TEST(scan_filter_bin_exists, "scan filter bin exists")
{
	scan_check check = {
		.failed = false,
		.set = SET5,
		.count = 0,
		.nobindata = false,
		.bins = { "bin1", "bin2", "bin3", NULL },
	};
	as_error err;
	as_scan scan;

	as_scan_init(&scan, NS, SET5);

	as_exp_build(filter,
		as_exp_bin_exists("otherBin"));

	as_policy_scan p;

	as_policy_scan_init(&p);

	p.base.filter_exp = filter;

	as_status rc = aerospike_scan_foreach(as, &err, &p, &scan, scan_check_callback, &check);

	assert_int_eq(rc, AEROSPIKE_OK);
	assert_false(check.failed);

	assert_int_eq(check.count, 10);
	info("Got %d records in the scan. Expected %d", check.count, 10);

	as_exp_destroy(filter);
	as_scan_destroy(&scan);
}

TEST(scan_invalid_filter, "scan invalid filter")
{
	scan_check check = {
		.failed = false,
		.set = SET5,
		.count = 0,
		.nobindata = false,
		.bins = { "bin1", "bin2", "bin3", NULL },
	};
	as_error err;
	as_scan scan;

	as_scan_init(&scan, NS, SET5);

	as_exp_build(filter,
		as_exp_add(as_exp_int(5), as_exp_int(5)));

	as_policy_scan p;

	as_policy_scan_init(&p);

	p.base.filter_exp = filter;

	as_status rc = aerospike_scan_foreach(as, &err, &p, &scan,
			scan_check_callback, &check);

	assert_int_eq(rc, AEROSPIKE_ERR_REQUEST_INVALID);

	as_exp_destroy(filter);
	as_scan_destroy(&scan);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

/**
 * Inserts records that can be used in the rest of the testcases
 */
static bool before(atf_suite * suite) {
	as_error err;
	aerospike_truncate(as, &err, NULL, NS, NULL, 0);

	insert_data(NUM_RECS_SET1, SET1);
	insert_data(NUM_RECS_SET2, SET2);
	insert_data(NUM_RECS_NULLSET, NULL);
	insert_data(NUM_RECS_SET3D, SET3D);
	insert_data(NUM_RECS_SET3, SET3);
	insert_data(NUM_RECS_SET4D, SET4D);
	insert_data(NUM_RECS_SET4, SET4);
	insert_data(NUM_RECS_SET5, SET5);

	insert_data(NUM_RECS_SET6, SET6);
	insert_data_int_key(NUM_RECS_SET6, SET6);

	if (! udf_put(LUA_FILE)) {
		error("failure while uploading: %s", LUA_FILE);
		return false;
	}

	if (! udf_exists(LUA_FILE)) {
		error("lua file does not exist: %s", LUA_FILE);
		return false;
	}

	return true;
}

static bool after(atf_suite * suite) {
	if (! udf_remove(LUA_FILE)) {
		error("failure while removing: %s", LUA_FILE);
		return false;
	}

	return true;
}

SUITE(scan_basics, "aerospike_scan basic tests")
{
	if (! g_has_ttl) {
		// Records are inserted with ttl in before().
		return;
	}

	suite_before(before);
	suite_after(after);

	suite_add(scan_basics_null_set);
	suite_add(scan_basics_set1);
	suite_add(scan_filter_set1);
	suite_add(scan_basics_set1_concurrent);
	suite_add(scan_basics_set1_select);
	suite_add(scan_basics_set1_nodata);
	suite_add(scan_basics_background);
	suite_add(scan_basics_background_sameid);
	suite_add(scan_basics_background_poll_job_status);
	suite_add(scan_basics_background_delete_bins);
	suite_add(scan_basics_background_delete_records_rec_filter);
	suite_add(scan_basics_background_delete_records_md_filter);
	suite_add(scan_basics_background_delete_records);
	suite_add(scan_operate);
	suite_add(scan_operate_ttl);
	suite_add(scan_operate_expop);
	suite_add(scan_filter_set_name);
	suite_add(scan_filter_rec_ttl);
	suite_add(scan_filter_rec_str_key);
	suite_add(scan_filter_rec_int_key);
	suite_add(scan_filter_bin_exists);
	suite_add(scan_invalid_filter);
}
