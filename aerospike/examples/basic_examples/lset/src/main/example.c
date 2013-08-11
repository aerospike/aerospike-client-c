/*******************************************************************************
 * Copyright 2008-2013 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/


//==========================================================
// Includes
//

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_lset.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_ldt.h>
#include <aerospike/as_arraylist_iterator.h>

#include "example_utils.h"


//==========================================================
// Constants
//


//==========================================================
// Forward Declarations
//



//==========================================================
// Large Set Data Example
//

int
main(int argc, char* argv[])
{
	// Parse command line arguments.
	if (! example_get_opts(argc, argv, EXAMPLE_BASIC_OPTS)) {
		exit(-1);
	}

	// Connect to the aerospike database cluster.
	aerospike as;

	example_connect_to_aerospike(&as);

	// Start clean.
	example_remove_test_record(&as);

	// Create a lset bin to use.
	as_ldt lset;
	if (! as_ldt_init(&lset, "mylset", AS_LDT_LSET, NULL)) {
		LOG("unable to initialize ldt");
		exit(-1);
	}

	// Push a few values into the set.
	as_error err;
	as_integer ival;
	as_integer_init(&ival, 12345);

	if ( aerospike_lset_add(&as, &err, NULL, &g_key, &lset, (as_val *) &ival) != AEROSPIKE_OK ) {
		LOG("first aerospike_set_add() returned %d - %s", err.code, err.message);
		exit(-1);
	}

	as_string sval;
	as_string_init(&sval, "lset value", false);
	if ( aerospike_lset_add(&as, &err, NULL, &g_key, &lset, (as_val *) &sval) != AEROSPIKE_OK ) {
		LOG("second aerospike_set_add() returned %d - %s", err.code, err.message);
		exit(-1);
	}
	LOG("2 values added to set");

	// Look at the lset size right now
	uint32_t n = 0;
	if ( aerospike_lset_size(&as, &err, NULL, &g_key, &lset, &n) != AEROSPIKE_OK ) {
		LOG("aerospike_lset_size() returned %d - %s", err.code, err.message);
		exit(-1);
	}
	if (n != 2) {
		LOG("lset size not as expected %d not 2",n);
		exit(-1);
	}
	LOG("lset size confirmed to be %d",n);

	// Get all the values back.
	as_ldt lset2;
	as_ldt_init(&lset2, "mylset", AS_LDT_LSET, NULL);

	as_list *list = NULL;

	if ( aerospike_lset_filter(&as, &err, NULL, &g_key, &lset, NULL, NULL, &list) != AEROSPIKE_OK ) {
 		LOG("error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
		as_list_destroy(list);
		exit(-1);
 	}

	// See if the elements match what we expect
	as_arraylist_iterator it;
	as_arraylist_iterator_init(&it, (as_arraylist *)list);
	while ( as_arraylist_iterator_has_next(&it) ) {
		const as_val * val = as_arraylist_iterator_next(&it);
		LOG(" Peek - type = %d value = %s ", as_val_type(val), as_val_tostring(val));
	}
	as_list_destroy(list);
	list = NULL;

	// Add 3 more items into the set
	as_arraylist vals;
	as_arraylist_inita(&vals, 3);
	as_arraylist_append_int64(&vals, 1001);
	as_arraylist_append_int64(&vals, 2002);
	as_arraylist_append_int64(&vals, 3003);

	if ( aerospike_lset_addall(&as, &err, NULL, &g_key, &lset, (as_list *) &vals) != AEROSPIKE_OK ) {
		LOG("aerospike_lset_addall() returned %d - %s", err.code, err.message);
		exit(-1);
	}
	LOG("3 more values added");


	// Get and print all the values back again.
	if ( aerospike_lset_filter(&as, &err, NULL, &g_key, &lset, NULL, NULL, &list) != AEROSPIKE_OK ) {
 		LOG("error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
		as_list_destroy(list);
		exit(-1);
 	}
	as_arraylist_iterator_init(&it, (as_arraylist *)list);
	while ( as_arraylist_iterator_has_next(&it) ) {
		const as_val * val = as_arraylist_iterator_next(&it);
		LOG(" Peek - type = %d value = %s ", as_val_type(val), as_val_tostring(val));
	}
	as_list_destroy(list);
	list = NULL;

	// Check if a specific value exists
	as_boolean exists;
	as_boolean_init(&exists,false);
	if ( aerospike_lset_exists(&as, &err, NULL, &g_key, &lset2, (as_val *)&ival, &exists) != AEROSPIKE_OK ) {
 		LOG("error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 		exit (-1);
	}
	if (as_boolean_get(&exists)) {
 		LOG("not able to find a value which should be in the set");
 		exit (-1);
	}

	// Check if a non-existent value, isn't
	as_boolean_init(&exists,false);
	as_integer_init(&ival, 33333);
	if ( aerospike_lset_exists(&as, &err, NULL, &g_key, &lset2, (as_val *)&ival, &exists) != AEROSPIKE_OK ) {
 		LOG("error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 		exit (-1);
	}
	if (as_boolean_get(&exists)) {
 		LOG("not able to find a value which should be in the set");
 		exit (-1);
	}
	LOG("Existence function checked");

	/*
	// Add 3 more items onto the list
	as_arraylist vals;
	as_arraylist_inita(&vals, 3);
	as_arraylist_append_int64(&vals, 888);
	as_arraylist_append_int64(&vals, 999);
	as_arraylist_append_int64(&vals, 1000);

	if ( aerospike_lset_addall(&as, &err, NULL, &g_key, &lset, (as_list *) &vals) != AEROSPIKE_OK ) {
		LOG("second aerospike_lset_addall() returned %d - %s", err.code, err.message);
		exit(-1);
	}
	LOG("3 more values added");

	// Peek all the values back again
	as_ldt_init(&lset2, "mylset", AS_LDT_LSET, NULL);

	if ( aerospike_lset_filter(&as, &err, NULL, &g_key, &lset, NULL, NULL, &list) != AEROSPIKE_OK ) {
 		LOG("error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
		as_list_destroy(list);
		exit(-1);
 	}

	// See if the elements match what we expect
	as_arraylist_iterator_init(&it, (as_arraylist *)list);
	while ( as_arraylist_iterator_has_next(&it) ) {
		const as_val * val = as_arraylist_iterator_next(&it);
		LOG(" Peek - type = %d value = %s ", as_val_type(val), as_val_tostring(val));
	}
	as_list_destroy(list);
	*/
	// Destroy the Lset
	LOG("Deleting Lset");
	if ( aerospike_lset_destroy(&as, &err, NULL, &g_key, &lset) != AEROSPIKE_OK ) {
 		LOG("error(%d) %s", err.code, err.message);
		exit(-1);
 	}

	// See if we can still do any lset operations
	n = 0;
	if ( aerospike_lset_size(&as, &err, NULL, &g_key, &lset, &n) == AEROSPIKE_OK ) {
		LOG("aerospike_lset_size() did not return error");
		exit(-1);
	}

	// Cleanup and disconnect from the database cluster.
	// Note: deletes example record used to keep database intact
	example_cleanup(&as);

	LOG("lset example successfully completed");

	return 0;
}


//==========================================================
// Helpers
//
