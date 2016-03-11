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
#include <aerospike/aerospike_llist.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_arraylist_iterator.h>
#include <aerospike/as_boolean.h>
#include <aerospike/as_error.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_key.h>
#include <aerospike/as_ldt.h>
#include <aerospike/as_list.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>
#include <aerospike/as_val.h>

#include "example_utils.h"


//==========================================================
// Large List Data Example
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

	// Create a large list object to use. No need to destroy llist if using
	// as_ldt_init() on stack object.
	as_ldt llist;
	if (! as_ldt_init(&llist, "myllist", AS_LDT_LLIST, NULL)) {
		LOG("unable to initialize ldt");
		example_cleanup(&as);
		exit(-1);
	}

	as_error err;
	as_boolean ldt_exists;
	as_boolean_init(&ldt_exists, false);

	// Verify that the LDT is not already there.
	if (aerospike_llist_ldt_exists(&as, &err, NULL, &g_key, &llist,
			&ldt_exists) != AEROSPIKE_OK) {
		int rc = example_handle_udf_error(&err, "first aerospike_llist_ldt_exists()");
		example_cleanup(&as);
		exit(rc);
	}

	if (as_boolean_get(&ldt_exists)) {
		LOG("found ldt that should not be present");
		example_cleanup(&as);
		exit(-1);
	}

	LOG("verified that llist ldt is not present");

	// Add 3 integer values to the list, one per operation.
	int example_values[3] = { 12000, 2000, 22000 };
	int example_ordered[3] = { 2000, 12000, 22000 };

	// No need to destroy ival if using as_integer_init() on stack object.
	as_integer ival;
	as_integer_init(&ival, example_values[0]);

	if (aerospike_llist_add(&as, &err, NULL, &g_key, &llist,
			(const as_val*)&ival) != AEROSPIKE_OK) {
		LOG("first aerospike_llist_add() returned %d - %s", err.code,
				err.message);
		example_cleanup(&as);
		exit(-1);
	}

	// Ok to reuse.
	as_integer_init(&ival, example_values[1]);

	if (aerospike_llist_add(&as, &err, NULL, &g_key, &llist,
			(const as_val*)&ival) != AEROSPIKE_OK) {
		LOG("second aerospike_llist_add() returned %d - %s", err.code,
				err.message);
		example_cleanup(&as);
		exit(-1);
	}

	as_integer_init(&ival, example_values[2]);

	if (aerospike_llist_add(&as, &err, NULL, &g_key, &llist,
			(const as_val*)&ival) != AEROSPIKE_OK) {
		LOG("third aerospike_llist_add() returned %d - %s", err.code,
				err.message);
		example_cleanup(&as);
		exit(-1);
	}

	LOG("3 values added to list");

	uint32_t n_elements = 0;

	// See how many elements we have in the list now.
	if (aerospike_llist_size(&as, &err, NULL, &g_key, &llist, &n_elements) !=
			AEROSPIKE_OK) {
		LOG("aerospike_llist_size() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	if (n_elements != 3) {
		LOG("unexpected llist size %u", n_elements);
		example_cleanup(&as);
		exit(-1);
	}

	LOG("llist size confirmed to be %u", n_elements);

	as_ldt llist2;
	as_ldt_init(&llist2, "myllist", AS_LDT_LLIST, NULL);

	as_list* p_list = NULL;

	// Get all the values back and print them. Make sure they are ordered.
	if (aerospike_llist_filter(&as, &err, NULL, &g_key, &llist, NULL, NULL,
			&p_list) != AEROSPIKE_OK) {
		LOG("aerospike_llist_filter() returned %d - %s", err.code, err.message);
		as_list_destroy(p_list);
		example_cleanup(&as);
		exit(-1);
	}

	as_arraylist_iterator it;
	as_arraylist_iterator_init(&it, (const as_arraylist*)p_list);

	int item_count = 0;

	while (as_arraylist_iterator_has_next(&it)) {
		const as_val* p_val = as_arraylist_iterator_next(&it);
		char* p_str = as_val_tostring(p_val);

		LOG("   element - type = %d, value = %s", as_val_type(p_val), p_str);
		free(p_str);

		// Make sure it's integer type.
		if (as_val_type(p_val) != AS_INTEGER) {
			LOG("unexpected value type %d", as_val_type(p_val));
			as_list_destroy(p_list);
			example_cleanup(&as);
			exit(-1);
		}

		int64_t myival = as_integer_get((const as_integer*)p_val);

		if (myival != example_ordered[item_count]) {
			LOG("unexpected integer value %"PRId64" returned on count %d",
					myival, item_count);
			as_list_destroy(p_list);
			example_cleanup(&as);
			exit(-1);
		}

		item_count++;
	}

	as_list_destroy(p_list);
	p_list = NULL;

	// No need to destroy sval if using as_string_init() on stack object with
	// free parameter false.
	as_string sval;
	as_string_init(&sval, "llist value", false);

	// Should not be able to add string to the llist since first element defines
	// the list type (integer in this case).
	if (aerospike_llist_add(&as, &err, NULL, &g_key, &llist,
			(const as_val*)&sval) == AEROSPIKE_OK) {
		LOG("unexpected success of aerospike_llist_add()");
		example_cleanup(&as);
		exit(-1);
	}

	n_elements = 0;

	// See how many elements we have in the list now.
	if (aerospike_llist_size(&as, &err, NULL, &g_key, &llist, &n_elements) !=
			AEROSPIKE_OK) {
		LOG("aerospike_llist_size() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	if (n_elements != 3) {
		LOG("unexpected llist size %u", n_elements);
		example_cleanup(&as);
		exit(-1);
	}

	LOG("attempting range query from 10000 to 25000");

	// No need to destroy these if using as_integer_init() on stack objects.
	as_integer min_val;
	as_integer_init(&min_val, 10000);
	as_integer max_val;
	as_integer_init(&max_val, 25000);

	// Perform a range query on the list. Let's query for the range that will
	// get us the last two elements in the list (12000 and 22000).
	if (aerospike_llist_range(&as, &err, NULL, &g_key, &llist,
			(const as_val*)&min_val, (const as_val*)&max_val, NULL, NULL,
			&p_list) != AEROSPIKE_OK) {
		LOG("aerospike_llist_range() returned %d - %s", err.code, err.message);
		as_list_destroy(p_list);
		example_cleanup(&as);
		exit(-1);
	}

	// We expect the size of the returned list to be 2, and the elements to be
	// 12000 and 22000.
	uint32_t returned_size = as_list_size(p_list);

	if (returned_size != 2) {
		LOG("range query returned list of size %u, expected 2", returned_size);

		char* p_str = as_val_tostring(p_list);
		LOG("list contents: %s", p_str);
		free(p_str);

		as_list_destroy(p_list);
		example_cleanup(&as);
		exit(-1);
	}

	LOG("range query returned list of expected size 2");

	as_list_destroy(p_list);

	// Remove the last inserted value (22000) from the list.
	// Note that the variable ival still retains the value 22000.
	if (aerospike_llist_remove(&as, &err, NULL, &g_key, &llist2,
			(const as_val*)&ival) != AEROSPIKE_OK) {
		LOG("aerospike_llist_remove() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	n_elements = 0;

	// See how many elements we have in the list now.
	if (aerospike_llist_size(&as, &err, NULL, &g_key, &llist, &n_elements)
			!= AEROSPIKE_OK) {
		LOG("aerospike_llist_size() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	if (n_elements != 2) {
		LOG("unexpected list size %u", n_elements);
		example_cleanup(&as);
		exit(-1);
	}

	LOG("one value removed and checked");

	as_boolean_init(&ldt_exists, false);

	// Verify that the LDT is now present.
	if (aerospike_llist_ldt_exists(&as, &err, NULL, &g_key, &llist,
			&ldt_exists) != AEROSPIKE_OK) {
		LOG("first aerospike_llist_ldt_exists() returned %d - %s", err.code,
				err.message);
		example_cleanup(&as);
		exit(-1);
	}

	if (! as_boolean_get(&ldt_exists)) {
		LOG("did not find ldt that should be present");
		example_cleanup(&as);
		exit(-1);
	}

	LOG("verified that llist ldt is present");

	// Destroy the list.
	if (aerospike_llist_destroy(&as, &err, NULL, &g_key, &llist) !=
			AEROSPIKE_OK) {
		LOG("aerospike_llist_destroy() returned %d - %s", err.code,
				err.message);
		example_cleanup(&as);
		exit(-1);
	}

	n_elements = 0;

	// See if we can still do any list operations.
	if (aerospike_llist_size(&as, &err, NULL, &g_key, &llist, &n_elements) ==
			AEROSPIKE_OK) {
		LOG("aerospike_llist_size() did not return error");
		example_cleanup(&as);
		exit(-1);
	}

	LOG("llist destroyed and checked");

	// Cleanup and disconnect from the database cluster.
	example_cleanup(&as);

	LOG("llist example successfully completed");

	return 0;
}
