/*******************************************************************************
 * Copyright 2008-2016 by Aerospike.
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

#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_lset.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_arraylist_iterator.h>
#include <aerospike/as_error.h>
#include <aerospike/as_ldt.h>
#include <aerospike/as_list.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>

#include "example_utils.h"


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

	// Create a large set object to use. No need to destroy lset if using
	// as_ldt_init() on stack object.
	as_ldt lset;
	if (! as_ldt_init(&lset, "mylset", AS_LDT_LSET, NULL)) {
		LOG("unable to initialize ldt");
		example_cleanup(&as);
		exit(-1);
	}

	as_error err;
	as_boolean ldt_exists;
	as_boolean_init(&ldt_exists, false);

	// Verify that the LDT is not already there.
	if (aerospike_lset_ldt_exists(&as, &err, NULL, &g_key, &lset,
			&ldt_exists) != AEROSPIKE_OK) {
		int rc = example_handle_udf_error(&err, "first aerospike_lset_ldt_exists()");
		example_cleanup(&as);
		exit(rc);
	}

	if (as_boolean_get(&ldt_exists)) {
		LOG("found ldt that should not be present");
		example_cleanup(&as);
		exit(-1);
	}

	LOG("verified that lset ldt is not present");

	// No need to destroy ival if using as_integer_init() on stack object.
	as_integer ival;
	as_integer_init(&ival, 12345);

	// Add an integer value to the set.
	if (aerospike_lset_add(&as, &err, NULL, &g_key, &lset,
			(const as_val*)&ival) != AEROSPIKE_OK) {
		LOG("first aerospike_set_add() returned %d - %s", err.code,
				err.message);
		example_cleanup(&as);
		exit(-1);
	}

	// No need to destroy sval if using as_string_init() on stack object with
	// free parameter false.
	as_string sval;
	as_string_init(&sval, "lset value", false);

	// Add a string value to the set.
	if (aerospike_lset_add(&as, &err, NULL, &g_key, &lset,
			(const as_val*)&sval) != AEROSPIKE_OK) {
		LOG("second aerospike_set_add() returned %d - %s", err.code,
				err.message);
		example_cleanup(&as);
		exit(-1);
	}

	LOG("2 values added to set");

	uint32_t n_elements = 0;

	// See how many elements we have in the set now.
	if (aerospike_lset_size(&as, &err, NULL, &g_key, &lset, &n_elements)
			!= AEROSPIKE_OK) {
		LOG("aerospike_lset_size() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	if (n_elements != 2) {
		LOG("unexpected lset size %u", n_elements);
		example_cleanup(&as);
		exit(-1);
	}

	LOG("lset size confirmed to be %u", n_elements);

	as_ldt lset2;
	as_ldt_init(&lset2, "mylset", AS_LDT_LSET, NULL);

	as_list* p_list = NULL;

	// Get all the values back.
	if (aerospike_lset_filter(&as, &err, NULL, &g_key, &lset, NULL, NULL,
			&p_list) != AEROSPIKE_OK) {
		LOG("aerospike_lset_filter() returned %d - %s", err.code, err.message);
		as_list_destroy(p_list);
		example_cleanup(&as);
		exit(-1);
	}

	as_arraylist_iterator it;
	as_arraylist_iterator_init(&it, (const as_arraylist*)p_list);

	// See if the elements match what we expect.
	while (as_arraylist_iterator_has_next(&it)) {
		const as_val* p_val = as_arraylist_iterator_next(&it);
		char* p_str = as_val_tostring(p_val);

		LOG("   element - type = %d, value = %s", as_val_type(p_val), p_str);
		free(p_str);
	}

	as_list_destroy(p_list);
	p_list = NULL;

	// Add 3 more items into the set. By using as_arraylist_inita() we avoid
	// some but not all internal heap usage, so we must call
	// as_arraylist_destroy().
	as_arraylist vals;
	as_arraylist_inita(&vals, 3);
	as_arraylist_append_int64(&vals, 1001);
	as_arraylist_append_int64(&vals, 2002);
	as_arraylist_append_int64(&vals, 3003);

	if (aerospike_lset_add_all(&as, &err, NULL, &g_key, &lset,
			(const as_list*)&vals) != AEROSPIKE_OK) {
		LOG("aerospike_lset_addall() returned %d - %s", err.code, err.message);
		as_arraylist_destroy(&vals);
		example_cleanup(&as);
		exit(-1);
	}

	as_arraylist_destroy(&vals);

	LOG("3 more values added");

	// Get and print all the values back again.
	if (aerospike_lset_filter(&as, &err, NULL, &g_key, &lset, NULL, NULL,
			&p_list) != AEROSPIKE_OK) {
		LOG("second aerospike_lset_filter() returned %d - %s", err.code,
				err.message);
		as_list_destroy(p_list);
		example_cleanup(&as);
		exit(-1);
	}

	as_arraylist_iterator_init(&it, (const as_arraylist*)p_list);

	int item_count = 0;

	while (as_arraylist_iterator_has_next(&it)) {
		const as_val* p_val = as_arraylist_iterator_next(&it);
		char* p_str = as_val_tostring(p_val);

		LOG("   element - type = %d, value = %s", as_val_type(p_val), p_str);
		free(p_str);

		item_count++;

		// Make sure it's a value we expect.
		if (as_val_type(p_val) == AS_INTEGER) {
			int64_t myival = as_integer_get((const as_integer*)p_val);

			if (myival != 1001 && myival != 2002 && myival != 3003 &&
					myival != 12345) {
				LOG("unexpected integer value %"PRId64" returned", myival);
				as_list_destroy(p_list);
				example_cleanup(&as);
				exit(-1);
			}
		}
		else if (as_val_type(p_val) == AS_STRING) {
			if (strcmp("lset value", as_string_get((as_string*)p_val)) != 0) {
				LOG("unexpected string value %s returned",
						as_string_get((const as_string*)p_val));
				as_list_destroy(p_list);
				example_cleanup(&as);
				exit(-1);
			}
		}
		else {
			LOG("unexpected value type %d", as_val_type(p_val));
			as_list_destroy(p_list);
			example_cleanup(&as);
			exit(-1);
		}
	}

	if (item_count != 5) {
		LOG("unexpected number of values %d", item_count);
		as_list_destroy(p_list);
		example_cleanup(&as);
		exit(-1);
	}

	as_list_destroy(p_list);
	p_list = NULL;

	// No need to destroy exists if using as_boolean_init() on stack object.
	as_boolean exists;
	as_boolean_init(&exists, false);

	// Check if a specific value exists.
	if (aerospike_lset_exists(&as, &err, NULL, &g_key, &lset2,
			(const as_val*)&ival, &exists) != AEROSPIKE_OK) {
		LOG("aerospike_lset_exists() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	if (! as_boolean_get(&exists)) {
		LOG("not able to find a value which should be in the set");
		example_cleanup(&as);
		exit(-1);
	}

	LOG("value existence checked");

	as_boolean_init(&ldt_exists, false);

	// Verify that the LDT is now present.
	if (aerospike_lset_ldt_exists(&as, &err, NULL, &g_key, &lset,
			&ldt_exists) != AEROSPIKE_OK) {
		LOG("first aerospike_lset_ldt_exists() returned %d - %s", err.code,
				err.message);
		example_cleanup(&as);
		exit(-1);
	}

	if (! as_boolean_get(&ldt_exists)) {
		LOG("did not find ldt that should be be present");
		example_cleanup(&as);
		exit(-1);
	}

	LOG("verified that lset ldt is present");

	// Remove the value from the set.
	if (aerospike_lset_remove(&as, &err, NULL, &g_key, &lset2,
			(const as_val*)&ival) != AEROSPIKE_OK) {
		LOG("aerospike_lset_remove() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	n_elements = 0;

	// See how many elements we have in the set now.
	if (aerospike_lset_size(&as, &err, NULL, &g_key, &lset, &n_elements)
			!= AEROSPIKE_OK) {
		LOG("aerospike_lset_size() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	if (n_elements != 4) {
		LOG("unexpected lset size %u", n_elements);
		example_cleanup(&as);
		exit(-1);
	}

	LOG("one value removed and checked");

	// Destroy the lset.
	if (aerospike_lset_destroy(&as, &err, NULL, &g_key, &lset) !=
			AEROSPIKE_OK) {
		LOG("aerospike_lset_destroy() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	n_elements = 0;

	LOG("lset destroyed");

	// Cleanup and disconnect from the database cluster.
	example_cleanup(&as);

	LOG("lset example successfully completed");

	return 0;
}
