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
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_scan.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_monitor.h>
#include <aerospike/as_stringmap.h>

#include "../test.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike* as;
static as_monitor monitor;

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define NS "test"
#define SET_STRSZ 20
#define NUM_RECS_SET1 100
#define SET1 "asbs1"
#define NUM_RECS_SET2 50
#define SET2 "asbs2"
#define NUM_RECS_NULLSET 20

/******************************************************************************
 * TYPES
 *****************************************************************************/

typedef struct {
	uint32_t count;
	uint32_t max;
} put_counter;

typedef struct scan_check_s {
	bool failed;
	char * set;
	bool nobindata; // flag to be set when you dont expect to get back any bins 
	int count;
	char * bins[10];
} scan_check;

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static void
put_listener(as_error* err, void* udata, as_event_loop* event_loop)
{
	// Exactly one event loop was initialized, so we know we are single threaded here.
	put_counter* c = udata;

	if (++c->count == c->max) {
		as_monitor_notify(&monitor);
	}

	if (err) {
		error("Put failed: %d %s\n", err->code, err->message);
		return;
	}
}

static void
insert_data(uint32_t numrecs, const char *setname)
{
	// Counter can be on stack because we block at end of function.
	put_counter counter = {.count = 0, .max = numrecs};
	
	char strval[SET_STRSZ], strkey[SET_STRSZ];
	as_error err;
	as_status status;
		
	as_monitor_begin(&monitor);
	
	for (int i = 0; i < numrecs; i++) {
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

		status = aerospike_key_put_async(as, &err, NULL, &k, &r, put_listener, &counter, 0, NULL);
		as_hashmap_destroy(&m);
		as_key_destroy(&k);
		as_record_destroy(&r);
		
		if (status != AEROSPIKE_OK) {
			error("Put failed: %d %s\n", err.code, err.message);
			break;
		}
	}
	as_monitor_wait(&monitor);
}

static bool
before(atf_suite* suite)
{
	as_monitor_init(&monitor);
	insert_data(NUM_RECS_SET1, SET1);
	insert_data(NUM_RECS_SET2, SET2);
	insert_data(NUM_RECS_NULLSET, NULL);
	return true;
}

static bool
after(atf_suite* suite)
{
	as_monitor_destroy(&monitor);
	return true;
}

static int
check_bin1(as_record * rec, scan_check * check)
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


static int
check_bin2(as_record * rec, scan_check * check)
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


static bool
check_bin3(as_record * rec, scan_check * check)
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


static int
check_bin4(as_record * rec, scan_check * check)
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

static bool
scan_listener(as_error* err, as_record* rec, void* udata, as_event_loop* event_loop)
{
	scan_check* check = (scan_check*)udata;

	if (err) {
		// Scan error occurred.
		error("Scan failed: %d %s", err->code, err->message);
		as_monitor_notify(&monitor);
		return false;
	}
	
	if (! rec) {
		// Scan has ended.
		info("Scanned %u records", check->count);
		as_monitor_notify(&monitor);
		return false;
	}
	check->count++;
	
	const char* set = rec->key.set[0] == '\0' ? NULL : rec->key.set;
			
	// Check if we are getting the results only from the set the scan is triggered for
	// If scan is called with NULL set, all the recs will come. So, no checks in this case.
	if (check->set) {
		// Do the check only if the rec also have a setname
		if (!set) {
			error("Expected set '%s', but got set NULL", check->set);
			return !(check->failed = true);
		}
		else if (strcmp(check->set, set) != 0) {
			error("Expected set '%s', but got set '%s'", check->set, set);
			return !(check->failed = true);
		}
	}
	
	// Check that we got the right number of bins
	int numbins = as_record_numbins(rec);
	
	if (check->nobindata) {
		if ( numbins != 0 ) {
			error("Expected 0 bins, but got %d", numbins);
			return !(check->failed = true);
		}
		return !(check->failed = false);
	}
	
	// only validate data if in sb_set1 or sb_set2
	if (check->set && strcmp(set, SET1) != 0 && strcmp(set, SET2) != 0) {
		return !(check->failed = false);
	}
	
	// validate bins
	int nbins = sizeof(check->bins) / sizeof(char *);
	for (int i = 0; check->bins[i] && i < nbins; i++) {
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

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST(scan_async_null_set, "full async scan using NULL setname")
{
	scan_check check = {
		.failed = false,
		.set = NULL,
		.count = 0,
		.nobindata = false,
		.bins = { NULL }
	};

	as_scan scan;
	as_scan_init(&scan, NS, NULL);

	as_monitor_begin(&monitor);

	as_error err;
	as_status status = aerospike_scan_async(as, &err, NULL, &scan, 0, scan_listener, &check, 0);
	as_scan_destroy(&scan);
	
	assert_int_eq(status, AEROSPIKE_OK);
	as_monitor_wait(&monitor);
	assert_false(check.failed);
}

TEST(scan_async_set1, "async scan "SET1"")
{
	scan_check check = {
		.failed = false,
		.set = SET1,
		.count = 0,
		.nobindata = false,
		.bins = { "bin1", "bin2", "bin3", NULL },
	};

	as_scan scan;
	as_scan_init(&scan, NS, SET1);

	as_monitor_begin(&monitor);
	
	as_error err;
	as_status status = aerospike_scan_async(as, &err, NULL, &scan, 0, scan_listener, &check, 0);
	as_scan_destroy(&scan);
	
	assert_int_eq(status, AEROSPIKE_OK);
	as_monitor_wait(&monitor);
	assert_false(check.failed);
	assert_int_eq(check.count, NUM_RECS_SET1);
	info("Got %d records in the scan. Expected %d", check.count, NUM_RECS_SET1);
}

TEST(scan_async_set1_concurrent, "async scan "SET1" concurrently")
{
	scan_check check = {
		.failed = false,
		.set = SET1,
		.count = 0,
		.nobindata = false,
		.bins = { "bin1", "bin2", "bin3", NULL },
	};

	as_scan scan;
	as_scan_init(&scan, NS, SET1);
	as_scan_set_concurrent(&scan, true);

	as_monitor_begin(&monitor);
	
	as_error err;
	as_status status = aerospike_scan_async(as, &err, NULL, &scan, 0, scan_listener, &check, 0);
	as_scan_destroy(&scan);
	
	assert_int_eq(status, AEROSPIKE_OK);
	as_monitor_wait(&monitor);
	assert_false(check.failed);
	assert_int_eq( check.count, NUM_RECS_SET1);
	info("Got %d records in the concurrent scan. Expected %d", check.count, NUM_RECS_SET1);
}

TEST(scan_async_set1_select, "scan "SET1" and select 'bin1'")
{
	scan_check check = {
		.failed = false,
		.set = SET1,
		.count = 0,
		.nobindata = false,
		.bins = { "bin1", NULL }
	};

	as_scan scan;
	as_scan_init(&scan, NS, SET1);

	as_scan_select_inita(&scan, 1);
	as_scan_select(&scan, "bin1");

	as_monitor_begin(&monitor);
	
	as_error err;
	as_status status = aerospike_scan_async(as, &err, NULL, &scan, 0, scan_listener, &check, 0);
	as_scan_destroy(&scan);
	
	assert_int_eq(status, AEROSPIKE_OK);
	as_monitor_wait(&monitor);
	assert_false(check.failed);
}

TEST(scan_async_set1_nodata, "scan "SET1" with no-bin-data")
{
	scan_check check = {
		.failed = false,
		.set = SET1,
		.count = 0,
		.nobindata = true,
		.bins = { NULL }
	};

	as_scan scan;
	as_scan_init(&scan, NS, SET1);
	as_scan_set_nobins(&scan, true);

	as_monitor_begin(&monitor);
	
	as_error err;
	as_status status = aerospike_scan_async(as, &err, NULL, &scan, 0, scan_listener, &check, 0);
	as_scan_destroy(&scan);
	
	assert_int_eq(status, AEROSPIKE_OK);
	as_monitor_wait(&monitor);
	assert_false(check.failed);
}

TEST(scan_async_single_node, "scan single node")
{
	scan_check check = {
		.failed = false,
		.set = SET1,
		.count = 0,
		.nobindata = false,
		.bins = { NULL }
	};
	
	as_node* node = as_node_get_random(as->cluster);
	assert_not_null(node);
	
	as_scan scan;
	as_scan_init(&scan, NS, SET1);
	
	as_monitor_begin(&monitor);
	
	as_error err;
	as_status status = aerospike_scan_node_async(as, &err, NULL, &scan, 0, node->name, scan_listener, &check, 0);
	as_node_release(node);
	as_scan_destroy(&scan);

	assert_int_eq(status, AEROSPIKE_OK);
	as_monitor_wait(&monitor);
	assert_false(check.failed);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE(scan_async, "Scan Async Tests")
{
	suite_before(before);
	suite_after(after);

	suite_add(scan_async_null_set);
	suite_add(scan_async_set1);
	suite_add(scan_async_set1_concurrent);
	suite_add(scan_async_set1_select);
	suite_add(scan_async_set1_nodata);
	suite_add(scan_async_single_node);
}
