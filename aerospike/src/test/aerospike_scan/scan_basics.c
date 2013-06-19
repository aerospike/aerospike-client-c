
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
typedef struct scan_struct_s {
	// Variable used to determine how the scan callback will behave
	char *setname;
	bool justcount;
	bool nobindata;
	bool bin4_present;
	int bin4_startval;
	// Variable used to return info to the caller
	bool ret_failed;
	int ret_rec_count;
} scan_struct;

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/
static void scan_data_init(scan_struct *s)
{
	s->ret_failed = false;
	s->ret_rec_count = 0;
	s->justcount = false;
	s->nobindata = false;
	s->bin4_present = false;
	s->bin4_startval = 0;
}

static int check_bin4(as_record *rec, scan_struct *scan_data)
{
	as_val *v = as_record_get(rec, "bin4");
	if (as_val_type(v) != AS_LIST) {
		error("Expecting list type in bin4. But got type %d", as_val_type(v));
		scan_data->ret_failed = true;
		return 0;
	}
	as_list *list = as_list_fromval(v);
	if (list == NULL) {
		error("Did not get list in bin4");
		scan_data->ret_failed = true;
	} else {
		int lsz = as_list_size(list);
		if (lsz < 3) {
			error("Expected atleast %d elements, but the list has only %d elements", 3, lsz);
			scan_data->ret_failed = true;
		}

		for (int i=0; i<lsz; i++) {
			as_val *v = as_list_get(list, i);
			if (as_val_type(v) != AS_INTEGER) {
				error("Expecting integer type in the arraylist. But got type %d", as_val_type(v));
				scan_data->ret_failed = true;
				return 0;
			}
			as_integer *asint = as_integer_fromval(v);
			uint64_t intval = as_integer_toint(asint);

			if (intval != (scan_data->bin4_startval + i)) {
				error("In bin4 list, Expecting %d but got %d", (scan_data->bin4_startval + i), intval);
				scan_data->ret_failed = true;
				return 0;
			}
		}
	}

	return 0;
}

static int scan_cb_counter(as_val *val, void *udata) 
{
	scan_struct *scan_data = (scan_struct *)udata;
	const char *scan_setname = scan_data->setname;

	as_record *rec = (as_record *)val;
	const char *rec_setname = rec->key.set;

	scan_data->ret_rec_count++;

	if (scan_data->justcount == true) {
		return 0;
	}

	// Check if we are getting the results only from the set the scan is triggered for
	// If scan is called with NULL set, all the recs will come. So, no checks in this case.
	if (scan_setname) {
		// Do the check only if the rec also have a setname
		if (rec_setname && strcmp(scan_setname, rec_setname) != 0) {
			error("Scanned for %s set, but got records of set %s", scan_setname, rec_setname);
			scan_data->ret_failed = false;
		}
	}

	// Check that we got the right number of bins
	int numbins = as_record_numbins(rec);

	if (scan_data->nobindata) {
		if (numbins != 0) {
			error("Got %d bins when not expecting any. Scan done with nobindata.", numbins);
			scan_data->ret_failed = true;
		}
		// As there are no bins, no need to progress further
		return 0;
	} 

	if (scan_data->bin4_present) {
		if (numbins != 4) {
			error("Number of bins in record got from scan: Expected %d got %d", 4, numbins);
			scan_data->ret_failed = true;
			return 0;
		}
	} else {
		// We may get 4 bins if the test is run again and again
		if ((numbins != 3) && (numbins != 4)) {
			error("Number of bins in record got from scan: Expected %d got %d", 3, numbins);
			scan_data->ret_failed = true;
		}
	}

	// We use the value of bin1 to check the values of the rest of the bins
	int intval = as_record_get_int64(rec, "bin1", INT64_MAX);

	// Check the string bin
	char exp_strval[SET_STRSZ];
	sprintf(exp_strval, "str-%s-%d", scan_setname ? scan_setname : "noset", intval);
	char *actual_strval = as_record_get_str(rec, "bin2");
	if (strcmp(exp_strval, actual_strval) != 0) {
		error("String value: Expected %s but got %s", exp_strval, actual_strval);
		scan_data->ret_failed = true;
	}

	// Check the map bin
	as_map * map = as_record_get_map(rec, "bin3");
	if (map == NULL) {
		error("Did not get the map which I expected");
		scan_data->ret_failed = true;
	} else {
		int sz = as_map_size(map);
		if (sz != 3) {
			error("Hashmap size: Expected %d but got %d", 3, sz);
			scan_data->ret_failed = true;
		} else {
			int val1 = as_stringmap_get_int64(map, "x");
			int val2 = as_stringmap_get_int64(map, "y");
			int val3 = as_stringmap_get_int64(map, "z");
			if (val1 != intval) {
				error("Hashmap value for key %s: Expected %d but got %d", "x", intval, val1);
				scan_data->ret_failed = true;
			}
			if (val2 != intval+1) {
				error("Hashmap value for key %s: Expected %d but got %d", "y", intval+1, val2);
				scan_data->ret_failed = true;
			}
			if (val3 != intval+2) {
				error("Hashmap value for key %s: Expected %d but got %d", "z", intval+2, val3);
				scan_data->ret_failed = true;
			}
		}
	}

	if (scan_data->bin4_present) {
		return check_bin4(rec, scan_data);
	}

	return 0;
}

static void insert_data(int numrecs, const char *setname)
{
	as_status rc;
	char strval[SET_STRSZ], strkey[SET_STRSZ];

	as_error err;
	as_error_reset(&err);

	as_record r;
	as_record_init(&r, 3);

	as_map m;
	as_hashmap_init(&m, 8);

	as_key k;

	for (int i=0; i<numrecs; i++) {
		// Simple integer bin
		as_record_set_int64(&r, "bin1", i);

		// Simple string bin
		sprintf(strval, "str-%s-%d", setname ? setname : "noset", i);
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

		sprintf(strkey, "key-%s-%d", setname, i);

		as_key_init(&k, NS, setname, strkey);

		rc = aerospike_key_put(as, &err, NULL, &k, &r);
		if (rc != AEROSPIKE_OK) {
			error("digest put failed with error %d", rc);
		}

		as_key_destroy(&k);
	}
}


/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( scan_basics_1 , "full scan (using NULL setname)" ) {

	scan_struct scan_data;
	scan_data_init(&scan_data);
	// The scan is no sure what all records may come for the scan. So, just count.
	// When running under ATF all data in the NS may be returned to this scan
	scan_data.justcount = true;

	as_error err;
	as_error_reset(&err);

	as_scan *scan = as_scan_new(NS, NULL);
	as_status rc = aerospike_scan_foreach(as, &err, NULL, scan, scan_cb_counter, &scan_data);
	assert_int_eq( rc, AEROSPIKE_OK );

	assert_int_eq( scan_data.ret_failed, false );
	// We should get all the data that we inserted
	int exp_rec_count = (NUM_RECS_SET1 + NUM_RECS_SET2 + NUM_RECS_NULLSET);
	assert_true( scan_data.ret_rec_count >= exp_rec_count );
	info("Got %d records in the scan. Expected atleast %d", scan_data.ret_rec_count, exp_rec_count);

	as_scan_destroy(scan);
}

TEST( scan_basics_2 , "simple scan of a specific set" ) {

	scan_struct scan_data;
	scan_data_init(&scan_data);
	scan_data.setname = SET1;

	as_error err;
	as_error_reset(&err);

	as_scan *scan = as_scan_new(NS, SET1);
	as_status rc = aerospike_scan_foreach(as, &err, NULL, scan, scan_cb_counter, &scan_data);
	assert_int_eq( rc, AEROSPIKE_OK );

	assert_int_eq( scan_data.ret_failed, false );
	assert_int_eq( scan_data.ret_rec_count, NUM_RECS_SET1 );
	info("Got %d records in the scan. Expected %d", scan_data.ret_rec_count, NUM_RECS_SET1);

	as_scan_destroy(scan);
}

TEST( scan_basics_3 , "scan of a specific set on a specific node" ) {

	int total_rec_count = 0;
	scan_struct scan_data;
	scan_data_init(&scan_data);
	scan_data.setname = SET2;

	as_error err;
	as_error_reset(&err);

	char node_name[NODE_NAME_SIZE];
	char *node_names = NULL;
	int num_nodes;
	cl_cluster_get_node_names(as->cluster, &num_nodes, &node_names);

	as_scan *scan = as_scan_new(NS, SET2);

	for (int i=0; i<num_nodes; i++) {
		memcpy(node_name, node_names+(NODE_NAME_SIZE*i), NODE_NAME_SIZE);
		as_status rc = aerospike_scan_node_foreach(as, &err, NULL, node_name, scan, scan_cb_counter, &scan_data);
		assert_int_eq( rc, AEROSPIKE_OK );
		assert_int_eq( scan_data.ret_failed, false );
		total_rec_count += scan_data.ret_rec_count;
		debug("Got %d records from %s node", scan_data.ret_rec_count, node_name);
		scan_data.ret_rec_count = 0;
	}

	// When we scan all the nodes, we should have got all the recs of this set and no more
	assert_int_eq( total_rec_count, NUM_RECS_SET2 );
	info("Got %d records in the scan. Expected %d", total_rec_count, NUM_RECS_SET2);

	free(node_names);
	as_scan_destroy(scan);
}

TEST( scan_basics_4 , "simple scan of a specific set with no-bin-data" ) {

	scan_struct scan_data;
	scan_data_init(&scan_data);
	scan_data.nobindata = true;
	scan_data.setname = SET1;

	as_error err;
	as_error_reset(&err);

	as_scan *scan = as_scan_new(NS, SET1);
	scan->no_bins = true;
	as_status rc = aerospike_scan_foreach(as, &err, NULL, scan, scan_cb_counter, &scan_data);
	assert_int_eq( rc, AEROSPIKE_OK );

	assert_int_eq( scan_data.ret_failed, false );
	assert_int_eq( scan_data.ret_rec_count, NUM_RECS_SET1 );
	info("Got %d records in the scan. Expected %d", scan_data.ret_rec_count, NUM_RECS_SET1);

	as_scan_destroy(scan);
}

TEST( scan_basics_5 , "udf scan in background to insert a new bin" ) {

	scan_struct scan_data;
	scan_data_init(&scan_data);
	scan_data.setname = SET2;
	scan_data.bin4_present = true;
	scan_data.bin4_startval = 1;

	as_error err;
	as_error_reset(&err);

	// insert a new bin using udf
	as_scan *scan = as_scan_new(NS, SET2);
	as_scan_foreach(scan, "aerospike_scan_test", "scan_insert_bin4", NULL);
	uint64_t scanid = 123;
	as_status udf_rc = aerospike_scan_background(as, &err, NULL, scan, &scanid);
	assert_int_eq( udf_rc, AEROSPIKE_OK );

	// We should let the background scan to finish
	sleep(2);

	// See if the above udf ran fine
	as_scan *scan2 = as_scan_new(NS, SET2);
	as_status rc = aerospike_scan_foreach(as, &err, NULL, scan2, scan_cb_counter, &scan_data);
	assert_int_eq( rc, AEROSPIKE_OK );
	assert_int_eq( scan_data.ret_failed, false );
	assert_int_eq( scan_data.ret_rec_count, NUM_RECS_SET2 );
	info("Got %d records in the scan. Expected %d", scan_data.ret_rec_count, NUM_RECS_SET2);

	as_scan_destroy(scan);
	as_scan_destroy(scan2);
}

TEST( scan_basics_6 , "udf scan in background per node to insert a new bin" ) {

	int total_rec_count = 0;
	scan_struct scan_data;
	scan_data_init(&scan_data);
	scan_data.setname = SET2;
	scan_data.bin4_present = true;
	scan_data.bin4_startval = 1;

	as_error err;
	as_error_reset(&err);

	char node_name[NODE_NAME_SIZE];
	char *node_names = NULL;
	int num_nodes;
	cl_cluster_get_node_names(as->cluster, &num_nodes, &node_names);

	// insert a new bin using udf
	as_scan *scan = as_scan_new(NS, SET2);
	as_scan_foreach(scan, "aerospike_scan_test", "scan_insert_bin4", NULL);
	uint64_t scanid = 456;
	for (int i=0; i<num_nodes; i++) {
		memcpy(node_name, node_names+(NODE_NAME_SIZE*i), NODE_NAME_SIZE);
		as_status udf_rc = aerospike_scan_node_background(as, &err, NULL, node_name, scan, &scanid);
		assert_int_eq( udf_rc, AEROSPIKE_OK );
		assert_int_eq( scan_data.ret_failed, false );
	}

	// We should let the background scan to finish
	sleep(2);

	// See if the above udf ran fine
	as_scan *scan2 = as_scan_new(NS, SET2);
	for (int i=0; i<num_nodes; i++) {
		memcpy(node_name, node_names+(NODE_NAME_SIZE*i), NODE_NAME_SIZE);
		as_status rc = aerospike_scan_node_foreach(as, &err, NULL, node_name, scan2, scan_cb_counter, &scan_data);
		assert_int_eq( rc, AEROSPIKE_OK );
		assert_int_eq( scan_data.ret_failed, false );
		total_rec_count += scan_data.ret_rec_count;
		debug("Got %d records from %s node", scan_data.ret_rec_count, node_name);
		scan_data.ret_rec_count = 0;
	}
	// When we scan all the nodes, we should have got all the recs of this set and no more
	assert_int_eq( total_rec_count, NUM_RECS_SET2 );
	info("Got %d records in the scan. Expected %d", total_rec_count, NUM_RECS_SET2);

	as_scan_destroy(scan);
	as_scan_destroy(scan2);
}

TEST( scan_basics_7 , "udf scan in foreground to insert a new bin" ) {

	scan_struct scan_data;
	scan_data_init(&scan_data);
	scan_data.setname = SET1;
	scan_data.bin4_present = true;

	as_error err;
	as_error_reset(&err);

	// insert a new bin using udf
	as_scan *scan = as_scan_new(NS, SET2);
	as_scan_foreach(scan, "aerospike_scan_test", "scan_update_bin4", NULL);
	uint64_t scanid = 789;
	as_status udf_rc = aerospike_scan_background(as, &err, NULL, scan, &scanid);
	assert_int_eq( udf_rc, AEROSPIKE_OK );

	// We should let the background scan to finish
	sleep(2);

	// See if the above udf ran fine
	as_scan *scan2 = as_scan_new(NS, SET2);
	as_scan_foreach(scan2, "aerospike_scan_test", "scan_getrec", NULL);
	scan_data.bin4_startval = 4;
	as_status rc = aerospike_scan_foreach(as, &err, NULL, scan2, scan_cb_counter, &scan_data);
	assert_int_eq( rc, AEROSPIKE_OK );
	assert_int_eq( scan_data.ret_failed, false );
	assert_int_eq( scan_data.ret_rec_count, NUM_RECS_SET2 );
	info("Got %d records in the scan. Expected %d", scan_data.ret_rec_count, NUM_RECS_SET2);

	as_scan_destroy(scan);
	as_scan_destroy(scan2);
}

TEST( scan_basics_8 , "starting two udf scan in background with same scan-id" ) {

	scan_struct scan_data;
	scan_data_init(&scan_data);
	scan_data.setname = SET2;
	scan_data.bin4_present = true;
	scan_data.bin4_startval = 1;

	as_error err;
	as_error_reset(&err);

	// insert a new bin using udf
	as_scan *scan = as_scan_new(NS, SET2);
	as_scan_foreach(scan, "aerospike_scan_test", "scan_noop", NULL);
	uint64_t scanid = 564;
	as_status udf_rc = aerospike_scan_background(as, &err, NULL, scan, &scanid);
	assert_int_eq( udf_rc, AEROSPIKE_OK );

	as_scan *scan2 = as_scan_new(NS, SET2);
	as_scan_foreach(scan2, "aerospike_scan_test", "scan_noop", NULL);
	uint64_t scanid2 = 564;
	as_status udf_rc2 = aerospike_scan_background(as, &err, NULL, scan2, &scanid2);
	assert_int_eq( udf_rc2, AEROSPIKE_ERR );

	as_scan_destroy(scan);
	as_scan_destroy(scan2);
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

	suite_add( scan_basics_1 );
	suite_add( scan_basics_2 );
	suite_add( scan_basics_3 );
	suite_add( scan_basics_4 );
	suite_add( scan_basics_5 );
	suite_add( scan_basics_6 );
	// Foreground scan udf is not yet available on the server side
	// suite_add( scan_basics_7 );
	suite_add(scan_basics_8);
}
