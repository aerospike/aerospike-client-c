
#include "../test.h"
#include "../util/udf.h"
#include <citrusleaf/citrusleaf.h>
#include <aerospike/as_types.h>

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define LUA_FILE "src/test/lua/client_record_basics.lua"
#define UDF_FILE "client_record_basics"
extern cl_cluster * cluster;

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( record_basics_exists, LUA_FILE" exists" ) {
    int rc = udf_exists(LUA_FILE);
    assert_int_eq( rc, 0 );
}

/**
 *
 * @TODO FIX Server and/or Client handling of AS_BOOLEAN.
 *
 * This test fails because the function returns a true (Boolean),
 * yet, the server and/or client says it is a string.
 *
 * The database does not actually support Boolean type, so the call should
 * have resulted in a failire (!r.is_success), with a type error.
 *
 */
TEST( record_basics_getboolean, "getboolean() - get a boolean" ) {

    int rc = 0;

    as_result r;
    as_success_init(&r,NULL);

    rc = udf_apply_record("test", "test", "test", UDF_FILE, "getboolean", NULL, &r);

    print_result(rc, &r);

    assert_int_eq( rc, 0 );
    assert_true( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_BOOLEAN );
    assert_true( as_boolean_tobool((as_boolean *) r.value) );

    as_result_destroy(&r);
}

TEST( record_basics_getfloat, "getfloat() - get a float. Will be converted to an integer." ) {

    int rc = 0;

    as_result r;
    as_success_init(&r,NULL);

    rc = udf_apply_record("test", "test", "test", UDF_FILE, "getfloat", NULL, &r);

    print_result(rc, &r);

    assert_int_eq( rc, 0 );
    assert_true( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_INTEGER );
    assert_int_eq( as_integer_toint((as_integer *) r.value), 123 );

    as_result_destroy(&r);
}

TEST( record_basics_getinteger, "getinteger() - get an integer" ) {

    int rc = 0;

    as_result r;
    as_success_init(&r,NULL);

    rc = udf_apply_record("test", "test", "test", UDF_FILE, "getinteger", NULL, &r);

    print_result(rc, &r);

    assert_int_eq( rc, 0 );
    assert_true( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_INTEGER );
    assert_int_eq( as_integer_toint((as_integer *) r.value), 123 );

    as_result_destroy(&r);
}

TEST( record_basics_getstring, "getstring() - get a string" ) {

    int rc = 0;

    as_result r;
    as_success_init(&r,NULL);

    rc = udf_apply_record("test", "test", "test", UDF_FILE, "getstring", NULL, &r);

    print_result(rc, &r);

    assert_int_eq( rc, 0 );
    assert_true( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_STRING );
    assert_string_eq( as_string_tostring((as_string *) r.value), "abc" );

    as_result_destroy(&r);
}

TEST( record_basics_gettable, "gettable() - get a table" ) {

    int rc = 0;

    as_result r;
    as_success_init(&r,NULL);

    rc = udf_apply_record("test", "test", "test", UDF_FILE, "gettable", NULL, &r);

    print_result(rc, &r);

    assert_int_eq( rc, 0 );
    assert_false( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_STRING );
    assert_string_eq( as_string_tostring((as_string *) r.value), "Invalid response. (2)" );

    as_result_destroy(&r);
}

TEST( record_basics_getlist, "getlist() - get a list" ) {

    int rc = 0;

    as_result r;
    as_success_init(&r,NULL);

    rc = udf_apply_record("test", "test", "test", UDF_FILE, "getlist", NULL, &r);

    print_result(rc, &r);

    assert_int_eq( rc, 0 );
    assert_true( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_LIST );
    assert_int_eq( as_list_size((as_list *) r.value), 3 );

    as_result_destroy(&r);
}

TEST( record_basics_getmap, "getmap() - get a map" ) {

    int rc = 0;

    as_result r;
    as_success_init(&r,NULL);

    rc = udf_apply_record("test", "test", "test", UDF_FILE, "getmap", NULL, &r);

    print_result(rc, &r);

    assert_int_eq( rc, 0 );
    assert_true( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_MAP );
    assert_int_eq( as_map_size((as_map *) r.value), 3 );

    as_result_destroy(&r);
}



TEST( record_basics_concat, "concat() - concatenate two strings, return the result" ) {

    int rc = 0;

    as_list * arglist = as_arraylist_new(2, 8);
    as_list_add_string(arglist, "abc");
    as_list_add_string(arglist, "def");

    as_result r;
    as_success_init(&r,NULL);

    rc = udf_apply_record("test", "test", "test", UDF_FILE, "concat", arglist, &r);

    print_result(rc, &r);

    assert_int_eq( rc, 0 );
    assert_true( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_STRING );
    assert_string_eq( as_string_tostring((as_string *) r.value), "abcdef" );

    as_list_destroy(arglist);
    as_result_destroy(&r);
}

TEST( record_basics_add, "add() - add two integer, return the result" ) {

    int rc = 0;

    as_list * arglist = as_arraylist_new(2, 8);
    as_list_add_integer(arglist, 1000);
    as_list_add_integer(arglist, 1);

    as_result r;
    as_success_init(&r,NULL);

    rc = udf_apply_record("test", "test", "test", UDF_FILE, "add", arglist, &r);

    print_result(rc, &r);

    assert_int_eq( rc, 0 );
    assert_true( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_INTEGER );
    assert_int_eq( as_integer_toint((as_integer *) r.value), 1001 );

    as_list_destroy(arglist);
    as_result_destroy(&r);
}

TEST( record_basics_sum, "sum() - UDF calling another UDF should fail" ) {

    int rc = 0;

    as_list * arglist = as_arraylist_new(2, 8);
    as_list_add_integer(arglist, 1000);
    as_list_add_integer(arglist, 1);

    as_result r;
    as_success_init(&r,NULL);

    rc = udf_apply_record("test", "test", "test", UDF_FILE, "sum", arglist, &r);

    print_result(rc, &r);

    assert_int_eq( rc, 100 );

    as_list_destroy(arglist);
    as_result_destroy(&r);
}

TEST( record_basics_diff, "diff() - UDF calling a local function" ) {

    int rc = 0;

    as_list * arglist = as_arraylist_new(2, 8);
    as_list_add_integer(arglist, 1000);
    as_list_add_integer(arglist, 1);

    as_result r;
    as_success_init(&r,NULL);

    rc = udf_apply_record("test", "test", "test", UDF_FILE, "diff", arglist, &r);

    print_result(rc, &r);

    assert_int_eq( rc, 0 );
    assert_true( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_INTEGER );
    assert_int_eq( as_integer_toint((as_integer *) r.value), 999 );

    as_list_destroy(arglist);
    as_result_destroy(&r);
}

TEST( record_basics_difference, "difference() - calling a local function should fail" ) {

    int rc = 0;

    as_list * arglist = as_arraylist_new(2, 8);
    as_list_add_integer(arglist, 1000);
    as_list_add_integer(arglist, 1);

    as_result r;
    as_success_init(&r,NULL);

    rc = udf_apply_record("test", "test", "test", UDF_FILE, "difference", arglist, &r);

    print_result(rc, &r);

    assert_int_eq( rc, 100 );
    assert_false( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_STRING );
    assert_string_eq( as_string_tostring((as_string *) r.value), "function not found" );

    as_list_destroy(arglist);
    as_result_destroy(&r);
}

TEST( record_basics_func_does_not_exist, "calling a non-existent function should fail" ) {

    int rc = 0;

    as_list * arglist = as_arraylist_new(2, 8);
    as_list_add_integer(arglist, 1000);
    as_list_add_integer(arglist, 1);

    as_result r;
    as_success_init(&r,NULL);

    rc = udf_apply_record("test", "test", "test", UDF_FILE, "does_not_exist", arglist, &r);

    print_result(rc, &r);

    assert_int_eq( rc, 100 );
    assert_false( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_STRING );
    assert_string_eq( as_string_tostring((as_string *) r.value), "function not found" );

    as_list_destroy(arglist);
    as_result_destroy(&r);
}


TEST( record_basics_file_does_not_exist, "non-existent UDF file should fail" ) {

    int rc = 0;

    as_list * arglist = as_arraylist_new(2, 8);
    as_list_add_integer(arglist, 1000);
    as_list_add_integer(arglist, 1);

    as_result r;
    as_success_init(&r,NULL);

    rc = udf_apply_record("test", "test", "test", "does_not_exist", "does_not_exist", arglist, &r);

    print_result(rc, &r);

    assert_int_eq( rc, 100 );
    assert_false( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_STRING );
    assert_string_eq( as_string_tostring((as_string *) r.value), "function not found" );

    as_list_destroy(arglist);
    as_result_destroy(&r);
}

TEST( record_delete_replication, "Check to see if the record is getting replicated on a delete from UDF") {
	// Delete record
	cl_object okey;
	citrusleaf_object_init_str(&okey, "test");
	int rc = citrusleaf_delete(cluster, "test", "test", &okey, 0);
	info("Delete returned %d", rc);

	// Insert 3 bins
	cl_bin bins[3];
	strcpy(bins[0].bin_name, "bin1");
	citrusleaf_object_init_str(&bins[0].object, "first string");
	strcpy(bins[1].bin_name, "bin2");
	citrusleaf_object_init_str(&bins[1].object, "second string");
	strcpy(bins[2].bin_name, "bin3");
	citrusleaf_object_init_str(&bins[2].object, "third string");
	rc = citrusleaf_put(cluster, "test", "test", &okey, bins, 3, NULL);
	if (rc) {
		info("Put failed");
		return ;
	}
	info("Put succeeded");

	as_result r;
	as_result_init(&r);
	
	// Apply udf which deletes all the three bins
	rc = udf_apply_record("test", "test", "test", UDF_FILE, "delete", NULL, &r);
	print_result(rc, &r);

	// Get bins.
	// In C client, you get the record from master and replica in alternate calls.
	cl_bin * rsp_bins[2];
	int rsp_n_bins[2], prev;
	int cl_gen, i = 0, j = 0;

	for (j = 0; j<2; j++) {
		int rsp = citrusleaf_get_all(cluster, "test", "test", &okey, &rsp_bins[j], &rsp_n_bins[j], 1000, &cl_gen);  
		info("Bins = %d", rsp_n_bins[j]);
		free(rsp_bins[j]);	
	}
	int master_bins = rsp_n_bins[0];
	int replica_bins = rsp_n_bins[1];
	assert_int_eq ( master_bins, 0 );
	assert_int_eq ( replica_bins, 0 );
}

TEST( record_basics_update_memory, "Memory accounting on updating record through UDFs" ) {

	// delete record, start afresh
	cl_object okey;
    citrusleaf_object_init_str(&okey, "test");
	int rc = citrusleaf_delete(cluster, "test", "test", &okey, 0);

	char * query = "namespace/test";
	as_result r;
	as_result_init(&r);

	// Get used memory before applying udf	
	char ** v_b = get_stats( query, "used-bytes-memory", cluster);
	int i = 0;
	while(v_b[i]) {
		debug("Used memory before - node %d = %ld\n",i,atol(v_b[i]));
		i++;
	}

	// Apply udf
	rc = udf_apply_record("test", "test", "test", UDF_FILE, "update", NULL, &r);
    print_result(rc, &r);
	if(atoi(as_val_tostring(r.value)) == -1) {
		info("update failed");
		return;
	}
	
	// Get namespace used bytes after record update
	char ** v_a = get_stats( query, "used-bytes-memory", cluster);
	i = 0;
	while(v_a[i]) {
		debug("Used memory after - node %d = %ld\n",i,atol(v_a[i]));
		i++;
	}

	// Get replication factor
	char ** v_c = get_stats( query, "repl-factor", cluster);
	int repl_factor = atoi(v_c[0]);
	debug("Replication factor %d\n", repl_factor);
	
	// The difference between the memory usage after and before update should be the record memory
	// for only 'replication factor' number of nodes
	int cluster_size = i;
	int diff = 0, count = 0;
	uint64_t rec_memory = 123;	
	
	for(int i = 0; i < cluster_size; i++) {
		diff = atol(v_a[i]) - atol(v_b[i]);
		if ( diff == rec_memory ) {
			count ++ ;
		}
	}
	assert_int_eq(count, repl_factor);

	// Free memory
	for ( i = 0;i < cluster_size; i++) {
		free(v_a[i]);
		free(v_b[i]);
		free(v_c[i]);
	}
	free(v_a);
	free(v_b);
	free(v_c); 
	as_result_destroy(&r);
}

TEST( record_basics_return_types_test, "Test for validating return types (including nested)") {
	// delete record, start afresh
	cl_object okey;
    citrusleaf_object_init_str(&okey, "test");
	int rc = citrusleaf_delete(cluster, "test", "test", &okey, 0);
	
	int errors = 0;

	// set arglist
	as_list * arglist = NULL;

	as_result res;
	as_result_init(&res);

	/**
	 * NONE
	 */

	arglist = as_arraylist_new(1, 8);	
	as_list_add_string(arglist, "none");

	int rsp = udf_apply_record("test", "test", "test", UDF_FILE, "return_types", arglist, &res); 
	print_result(rsp, &res);
    assert_int_eq( rsp, 0 );

	if ( res.is_success ) {
		assert( as_val_type(res.value) != AS_NIL );
		char *str = as_val_tostring(res.value);
		info("return type test: first return is %s",str);
		free(str);
	}

	as_val_destroy(arglist);

	as_result_destroy(&res);

	/**
	 * STRING
	 */

	arglist = as_arraylist_new(1, 8);	
	as_list_add_string(arglist, "string_primitive");

	as_result_init(&res);

	rsp = udf_apply_record("test", "test", "test", UDF_FILE, "return_types", arglist, &res);
	
    assert_int_eq( rsp, 0 );

	info("string: %s", res.is_success ? "SUCCESS" : "FAILURE");
	if ( res.is_success ) {
		assert( as_val_type(res.value) == AS_STRING );
		assert_string_eq( as_string_tostring((as_string *) res.value), "good" );
	}

	as_val_destroy(arglist);

	as_result_destroy(&res);
	
	/**
	 * POSITIVE INTEGER
	 */
	
	arglist = as_arraylist_new(1, 8);	
	as_list_add_string(arglist, "p_int_primitive");

	rsp = udf_apply_record("test", "test", "test", UDF_FILE, "return_types", arglist, &res);
	
	assert_int_eq( rsp, 0 );

	info("postive integer: %s", res.is_success ? "SUCCESS" : "FAILURE");
	if ( res.is_success ) {
		assert( as_val_type(res.value) == AS_INTEGER );
		assert_int_eq( as_integer_toint((as_integer *) res.value), 5 );
	}

	as_val_destroy(arglist);
	as_val_destroy(res.value);
	as_result_destroy(&res);

	/**
	 * NAGATIVE INTEGER
	 */

	arglist = as_arraylist_new(1, 8);	
	as_list_add_string(arglist, "n_int_primitive");

	as_result_init(&res);

	rsp = udf_apply_record("test", "test", "test", UDF_FILE, "return_types", arglist, &res);
	
	assert_int_eq( rsp, 0 );
	
	info("negative integer: %s", res.is_success ? "SUCCESS" : "FAILURE");
	if ( res.is_success ) {
		assert( as_val_type(res.value) == AS_INTEGER );
		assert_int_eq( as_integer_toint((as_integer *) res.value), -5 );
	}

	as_val_destroy(arglist);

	as_result_destroy(&res);
	
	/**
	 * LIST
	 */

	arglist = as_arraylist_new(1, 8);	
	as_list_add_string(arglist, "bin_array");

	as_result_init(&res);

	rsp = udf_apply_record("test", "test", "test", UDF_FILE, "return_types", arglist, &res);
	
	assert_int_eq( rsp, 0 );

	info("list: %s", res.is_success ? "SUCCESS" : "FAILURE");
	if ( res.is_success ) {
		assert( as_val_type(res.value) == AS_LIST );
		assert_int_eq( as_list_size((as_list *) res.value), 2);
	}

	as_val_destroy(arglist);

	as_result_destroy(&res);

	/**
	 * NESTED LIST
	 */

	arglist = as_arraylist_new(1, 8);	
	as_list_add_string(arglist, "bin_nested_list");
	as_result_init(&res);

	rsp = udf_apply_record("test", "test", "test", UDF_FILE, "return_types", arglist, &res);
	
	assert_int_eq( rsp, 0 );

	info("list: %s", res.is_success ? "SUCCESS" : "FAILURE");
	if ( res.is_success ) {
		assert( as_val_type(res.value) == AS_LIST );
		assert_int_eq( as_list_size((as_list *) res.value), 2);
		as_iterator l1_i;
		as_list_iterator_init(&l1_i, (as_list *)res.value);
		bool l1_string = false;
		bool l1_list = false;
		while ( as_iterator_has_next(&l1_i) ) {
			as_val * l1_v = (as_val *) as_iterator_next(&l1_i);
			if ( as_val_type(l1_v) == AS_STRING ) {
				assert_string_eq( as_string_tostring((as_string *) l1_v), "string_resp");
				l1_string = false;
			}
			else if ( as_val_type(l1_v) == AS_LIST ) {
				assert_int_eq( as_list_size((as_list *) l1_v), 2);
				as_iterator l2_i; 
				as_list_iterator_init(&l2_i, (as_list*) l1_v);
				bool l2_string = false;
				bool l2_integer = false;
				while ( as_iterator_has_next(&l2_i) ) {
					as_val * l2_v = (as_val *) as_iterator_next(&l2_i);
					if ( as_val_type(l2_v) == AS_STRING ) {
						assert_string_eq( as_string_tostring((as_string *) l2_v), "yup");
						l2_string = true;
					}
					else if ( as_val_type(l2_v) == AS_INTEGER ) {
						assert_int_eq( as_integer_toint((as_integer *) l2_v), 1);
						l2_integer = true;
					}

					if ( l2_integer && l2_string ) {
						l1_list = true;
					}
					as_iterator_destroy(&l2_i);
				}
			}
			as_iterator_destroy(&l1_i);
		}
	}
	as_val_destroy(arglist);

	as_result_destroy(&res);

	/**
	 * MAP
	 */

	arglist = as_arraylist_new(1, 8);	
	as_list_add_string(arglist, "bin_map");

	as_result_init(&res);

	rsp = udf_apply_record("test", "test", "test", UDF_FILE, "return_types", arglist, &res);
	
	assert_int_eq( rsp, 0 );

	info("map: %s", res.is_success ? "SUCCESS" : "FAILURE");
	if ( res.is_success ) {
		assert( as_val_type(res.value) == AS_MAP );
		as_map * m1 = (as_map *) res.value;
		assert_int_eq( as_map_size(m1), 4 );
		as_string s;
		as_val * m1_s = as_map_get(m1,(as_val *) as_string_init(&s,"s",false));
		if ( as_val_type(m1_s) == AS_STRING ) {
			assert_string_eq( as_string_tostring((as_string*) m1_s), "abc");
		}

		as_val * m1_i = as_map_get(m1,(as_val *) as_string_init(&s,"i", false));
		if ( as_val_type(m1_i) == AS_INTEGER ) {
			assert_int_eq( as_integer_toint((as_integer *) m1_i), 123 );
		}

		as_val * m1_l = as_map_get(m1,(as_val *) as_string_init(&s,"l",false));
		if ( as_val_type(m1_l) == AS_LIST ) {
			assert_int_eq( as_list_size((as_list *) m1_l), 2 );
		}

		as_val * m1_m = as_map_get(m1,(as_val *) as_string_init(&s,"m",false));
		if ( as_val_type(m1_m) == AS_MAP ) {
			assert_int_eq( as_map_size((as_map *) m1_m), 3 );
			as_map * m2 = (as_map *) m1_m;

			as_val * m2_i = as_map_get(m2,(as_val *) as_string_init(&s,"i",false));
			if ( as_val_type(m2_i) == AS_INTEGER ) {
				assert_int_eq( as_integer_toint((as_integer *) m2_i), 456 );
			}

			as_val * m2_s = as_map_get(m2,(as_val *) as_string_init(&s,"s",false));
			if ( as_val_type(m2_s) == AS_STRING ) {
				assert_string_eq( as_string_tostring((as_string*) m2_s), "def");
			}

			as_val * m2_l = as_map_get(m2,(as_val *) as_string_init(&s,"l",false));
			if ( as_val_type(m2_l) == AS_LIST ) {
				assert_int_eq( as_list_size((as_list *) m2_l), 3 );
			}
		}
	}

	as_val_destroy(arglist);
	as_result_destroy(&res);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

static bool before(atf_suite * suite) {
    int rc = 0;

    rc = udf_put(LUA_FILE);
    if ( rc != 0 ) {
        error("failure while uploading: %s (%d)", LUA_FILE, rc);
        return false;
    }

    rc = udf_exists(LUA_FILE);
    if ( rc != 0 ) {
        error("lua file does not exist: %s (%d)", LUA_FILE, rc);
        return false;
    }

    return true;
}

static bool after(atf_suite * suite) {
    int rc = udf_remove(LUA_FILE);
    if ( rc != 0 ) {
        error("failure while removing: %s (%d)", LUA_FILE, rc);
        return false;
    }

    return true;
}

SUITE( record_basics, "test basics.lua" ) {

    suite_before( before );

    suite_add( record_basics_exists );

    suite_add( record_basics_getboolean );
    suite_add( record_basics_getfloat );
    suite_add( record_basics_getinteger );
    suite_add( record_basics_getstring );
    suite_add( record_basics_gettable );
    suite_add( record_basics_getlist );
    suite_add( record_basics_getmap );
    

    suite_add( record_basics_concat );
    suite_add( record_basics_add );
    suite_add( record_basics_sum );
    suite_add( record_basics_diff );
    suite_add( record_basics_difference );

    suite_add( record_basics_func_does_not_exist );
    suite_add( record_basics_file_does_not_exist );
	suite_add( record_basics_update_memory );
	suite_add( record_delete_replication );
	suite_add( record_basics_return_types_test );

    suite_after( after );
}
