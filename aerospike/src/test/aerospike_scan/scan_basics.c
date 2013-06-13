
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_scan.h>
#include <aerospike/aerospike_digest.h>

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

#include "../test.h"

#define NUM_RECS 100
#define NS "test"
#define SET "scan_basics_set"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike * as;

/******************************************************************************
 * TYPES
 *****************************************************************************/
typedef struct scan_struct_s {
	bool failed;
	int rec_count;
} scan_struct;

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/
 static int scan_cb_counter(as_val *val, void *udata) 
 {
	scan_struct *scan_data = (scan_struct *)udata;
	as_record *rec = (as_record *)val;

	printf("-----------------------------------------------------------\n");
	scan_data->rec_count++;

	// Check that we got the right number of bins
	int numbins = as_record_numbins(rec);
	if (numbins != 3) {
		error("Number of bins in record got from scan: Expected %d got %d", 3, numbins);
		scan_data->failed = true;
	}

	// We use the value of bin1 to check the values of the rest of the bins
	int intval = as_record_get_int64(rec, "bin1");

	// Check the string bin
	char exp_strval[10];
	sprintf(exp_strval, "str-%d", intval);
	char *actual_strval = as_record_get_str(rec, "bin2");
	if (strcmp(exp_strval, actual_strval) != 0) {
		error("String value: Expected %s but got %s", exp_strval, actual_strval);
		scan_data->failed = true;
	}

	// Check the map bin
	as_map * map = as_record_get_map(rec, "bin3");
	if (map == NULL) {
		error("Did not get the map which I expected");
		scan_data->failed = true;
	} else {
		int sz = as_map_size(map);
		if (sz != 3) {
			error("Hashmap size: Expected %d but got %d", 3, sz);
			scan_data->failed = true;
		} else {
			int val1 = as_stringmap_get_int64(map, "x");
			int val2 = as_stringmap_get_int64(map, "y");
			int val3 = as_stringmap_get_int64(map, "z");
			if (val1 != intval) {
				error("Hashmap value for key %s: Expected %d but got %d", "x", intval, val1);
				scan_data->failed = true;
			}
			if (val2 != intval+1) {
				error("Hashmap value for key %s: Expected %d but got %d", "y", intval+1, val2);
				scan_data->failed = true;
			}
			if (val3 != intval+2) {
				error("Hashmap value for key %s: Expected %d but got %d", "z", intval+2, val3);
				scan_data->failed = true;
			}
		}
	}

	return 0;
 }


/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( scan_basics_1 , "simple scan" ) {

	scan_struct scan_data;
	scan_data.failed = false;
	scan_data.rec_count = 0;

	as_error err;
	as_error_reset(&err);

	as_scan *scan = as_scan_new(NS, SET);
	as_status rc = aerospike_scan_foreach(as, &err, NULL, scan, scan_cb_counter, &scan_data);
	assert_int_eq( rc, AEROSPIKE_OK );

	assert_int_eq( scan_data.failed, false );
	assert_int_eq( scan_data.rec_count, NUM_RECS );

	as_scan_destroy(scan);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

/**
 * Inserts records that can be used in the rest of the testcases
 */
static bool before(atf_suite * suite) {

	as_status rc;
	char strval[10], strkey[10];

	as_error err;
	as_error_reset(&err);

	as_record r;
	as_record_init(&r, 3);

	as_map m;
	as_hashmap_init(&m, 8);

	as_digest d;
	as_digest_init(&d, NULL, NULL);

	for (int i=0; i<NUM_RECS; i++) {
		// Simple integer bin
		as_record_set_int64(&r, "bin1", i);

		// Simple string bin
		sprintf(strval, "str-%d",i);
		as_record_set_str(&r, "bin2", strval);

		// Map bin
		as_stringmap_set_int64(&m, "x", i);
		as_stringmap_set_int64(&m, "y", i+1);
		as_stringmap_set_int64(&m, "z", i+2);
		// as_record_set() will try to destroy the bin value (hashmap) if it already exists
		// To reuse the same hashmap again and again in the loop we should protect it from
		// getting destroyed. The trick is to bump up the ref count.
		as_val_reserve(&m);
		as_record_set_map(&r, "bin3", &m);

		sprintf(strkey, "key%d", i);
		as_digest_init(&d, SET, strkey);
		rc = aerospike_digest_put(as, &err, NULL, NS, &d, &r);
		if (rc != AEROSPIKE_OK) {
			error("digest put failed with error %d", rc);
		}
	}

    return true;
}

static bool after(atf_suite * suite) {
    return true;
}

SUITE( scan_basics, "aerospike_scan basic tests" ) {
    suite_before( before );
    suite_after( after );

	suite_add( scan_basics_1 );
}
