
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_scan.h>
#include <aerospike/aerospike_key.h>

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

#include <citrusleaf/cl_cluster.h>

#include "../test.h"
#include "../util/udf.h"

#define NS "test"
#define SET_STRSZ 20
#define NUM_RECS_SET1 100
#define SET1 "sb_set1"
#define NUM_RECS_SET2 50
#define SET2 "sb_set2"
#define NUM_RECS_NULLSET 20

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike * as;

/******************************************************************************
 * TYPES
 *****************************************************************************/
typedef struct scan_check_s {
	bool failed;
	char * set;
	bool nobindata;
	int count;
	char * bins[10];
} scan_check;

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

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
	sprintf(expected, "str-%s-%ld", rec->key.set[0] == '\0' ? "noset" : rec->key.set, bin1);

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
		.bins = { "bin1", "bin2", "bin3", NULL }
	};

	as_error err;

	as_scan scan;
	as_scan_init(&scan, NS, SET1);

	as_status rc = aerospike_scan_foreach(as, &err, NULL, &scan, scan_check_callback, &check);
	
	assert_int_eq( rc, AEROSPIKE_OK );
	assert_false( check.failed );

	// assert_int_eq( scan_data.ret_failed, false );
	// assert_int_eq( scan_data.ret_rec_count, NUM_RECS_SET1 );
	// info("Got %d records in the scan. Expected %d", scan_data.ret_rec_count, NUM_RECS_SET1);

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

TEST( scan_basics_background , "scan "SET2" in background to insert a new bin" ) {

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

	// We should let the background scan to finish
	sleep(2);

	// See if the above udf ran fine
	as_scan scan2;
	as_scan_init(&scan2, NS, SET1);

	as_status rc = aerospike_scan_foreach(as, &err, NULL, &scan2, scan_check_callback, &check);

	assert_int_eq( rc, AEROSPIKE_OK );
	assert_false( check.failed );

	as_scan_destroy(&scan);
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

	as_scan scan2;
	as_scan_init(&scan2, NS, SET2);
	as_scan_apply_each(&scan2, "aerospike_scan_test", "scan_noop", NULL);

	uint64_t scanid2 = scanid;
	as_status udf_rc2 = aerospike_scan_background(as, &err, NULL, &scan2, &scanid2);
	
	assert_int_eq( udf_rc2, AEROSPIKE_ERR_SERVER );

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

	if (udf_put("src/test/aerospike_scan/aerospike_scan_test.lua") == false) {
		return false;
	}

    return true;
}

static bool after(atf_suite * suite) {
    return true;
}

SUITE( scan_basics, "aerospike_scan basic tests" ) {

	// For the cluster to settle as it is important for the per-node testcases
	sleep(1);
    suite_before( before );
    suite_after( after );

	suite_add( scan_basics_null_set );
	suite_add( scan_basics_set1 );
	suite_add( scan_basics_set1_select );
	suite_add( scan_basics_set1_nodata );
	suite_add( scan_basics_background );
	suite_add( scan_basics_background_sameid );
}
