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

	as_ldt lset;

	// Create a lset bin to use. No need to destroy as_ldt if using
	// as_ldt_init() on stack object.
	if (! as_ldt_init(&lset, "mylset", AS_LDT_LSET, NULL)) {
		LOG("unable to initialize ldt");
		exit(-1);
	}

	as_error err;

	// No need to destroy as_integer if using as_integer_init() on stack object.
	as_integer ival;
	as_integer_init(&ival, 12345);

	// Add an integer value to the set.
	if (aerospike_lset_add(&as, &err, NULL, &g_key, &lset,
			(const as_val*)&ival) != AEROSPIKE_OK) {
		LOG("first aerospike_set_add() returned %d - %s", err.code,
				err.message);
		exit(-1);
	}

	// No need to destroy as_string if using as_string_init() on stack object.
	as_string sval;
	as_string_init(&sval, "lset value", false);

	// Add a string value to the set.
	if (aerospike_lset_add(&as, &err, NULL, &g_key, &lset,
			(const as_val*)&sval) != AEROSPIKE_OK) {
		LOG("second aerospike_set_add() returned %d - %s", err.code,
				err.message);
		exit(-1);
	}

	LOG("2 values added to set");

	uint32_t n_elements = 0;

	// See how many elements we have in the set now.
	if (aerospike_lset_size(&as, &err, NULL, &g_key, &lset, &n_elements)
			!= AEROSPIKE_OK) {
		LOG("aerospike_lset_size() returned %d - %s", err.code, err.message);
		exit(-1);
	}

	if (n_elements != 2) {
		LOG("unexpected lset size %u", n_elements);
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
		exit(-1);
	}

	// See if the elements match what we expect.
	as_arraylist_iterator it;
	as_arraylist_iterator_init(&it, (const as_arraylist*)p_list);

	while (as_arraylist_iterator_has_next(&it)) {
		const as_val* p_val = as_arraylist_iterator_next(&it);
		LOG("   element - type = %d, value = %s ", as_val_type(p_val),
				as_val_tostring(p_val));
	}

	as_list_destroy(p_list);
	p_list = NULL;

	// Add 3 more items into the set. By using as_arraylist_inita(), we won't
	// need to destroy the as_arraylist if we only use
	// as_arraylist_append_int64().
	as_arraylist vals;
	as_arraylist_inita(&vals, 3);
	as_arraylist_append_int64(&vals, 1001);
	as_arraylist_append_int64(&vals, 2002);
	as_arraylist_append_int64(&vals, 3003);

	if (aerospike_lset_addall(&as, &err, NULL, &g_key, &lset,
			(const as_list*)&vals) != AEROSPIKE_OK) {
		LOG("aerospike_lset_addall() returned %d - %s", err.code, err.message);
		exit(-1);
	}

	LOG("3 more values added");

	// Get and print all the values back again.
	if (aerospike_lset_filter(&as, &err, NULL, &g_key, &lset, NULL, NULL,
			&p_list) != AEROSPIKE_OK) {
		LOG("second aerospike_lset_filter() returned %d - %s", err.code,
				err.message);
		as_list_destroy(p_list);
		exit(-1);
	}

	as_arraylist_iterator_init(&it, (const as_arraylist*)p_list);

	while (as_arraylist_iterator_has_next(&it)) {
		const as_val* p_val = as_arraylist_iterator_next(&it);
		LOG("   element - type = %d, value = %s ", as_val_type(p_val),
				as_val_tostring(p_val));
	}

	as_list_destroy(p_list);
	p_list = NULL;

	// No need to destroy as_boolean if using as_boolean_init() on stack object.
	as_boolean exists;
	as_boolean_init(&exists, false);

	// Check if a specific value exists.
	if (aerospike_lset_exists(&as, &err, NULL, &g_key, &lset2,
			(const as_val*)&ival, &exists) != AEROSPIKE_OK) {
		LOG("aerospike_lset_exists() returned %d - %s", err.code, err.message);
		exit(-1);
	}

	if (as_boolean_get(&exists)) {
		LOG("not able to find a value which should be in the set");
		exit(-1);
	}

	as_boolean_init(&exists, false);
	as_integer_init(&ival, 33333);

	// Check that a value which should not be in the set, really isn't.
	if (aerospike_lset_exists(&as, &err, NULL, &g_key, &lset2,
			(const as_val*)&ival, &exists) != AEROSPIKE_OK) {
		LOG("second aerospike_lset_exists() returned %d - %s", err.code,
				err.message);
		exit(-1);
	}

	if (as_boolean_get(&exists)) {
		LOG("found a value which should not be in the set");
		exit(-1);
	}

	LOG("existence functionality checked");

	// Destroy the lset.
	if (aerospike_lset_destroy(&as, &err, NULL, &g_key, &lset) !=
			AEROSPIKE_OK) {
		LOG("aerospike_lset_destroy() returned %d - %s", err.code, err.message);
		exit(-1);
	}

	n_elements = 0;

	// See if we can still do any lset operations.
	if (aerospike_lset_size(&as, &err, NULL, &g_key, &lset, &n_elements) ==
			AEROSPIKE_OK) {
		LOG("aerospike_lset_size() did not return error");
		exit(-1);
	}

	// Cleanup and disconnect from the database cluster.
	example_cleanup(&as);

	LOG("lset example successfully completed");

	return 0;
}
