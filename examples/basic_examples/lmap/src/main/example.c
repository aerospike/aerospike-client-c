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

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_lmap.h>
#include <aerospike/as_error.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_hashmap_iterator.h>
#include <aerospike/as_ldt.h>
#include <aerospike/as_list.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>

#include "example_utils.h"


//==========================================================
// Large Map Data Example
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

	// Create a large map object to use. No need to destroy lmap if using
	// as_ldt_init() on stack object.
	as_ldt lmap;
	if (! as_ldt_init(&lmap, "mylmap", AS_LDT_LMAP, NULL)) {
		LOG("unable to initialize ldt");
		example_cleanup(&as);
		exit(-1);
	}

	as_error err;
	as_boolean ldt_exists;
	as_boolean_init(&ldt_exists, false);

	// Verify that the LDT is not already there.
	if (aerospike_lmap_ldt_exists(&as, &err, NULL, &g_key, &lmap,
			&ldt_exists) != AEROSPIKE_OK) {
		int rc = example_handle_udf_error(&err, "first aerospike_lmap_ldt_exists()");
		example_cleanup(&as);
		exit(rc);
	}

	if (as_boolean_get(&ldt_exists)) {
		LOG("found ldt that should not be present");
		example_cleanup(&as);
		exit(-1);
	}

	LOG("verified that lmap ldt is not present");

	// No need to destroy ikey if using as_integer_init() on stack object.
	as_integer ikey;
	as_integer_init(&ikey, 12345);

	// No need to destroy sval if using as_string_init() on stack object with
	// free parameter false.
	as_string sval;
	as_string_init(&sval, "lmap value", false);

	// Put a string entry to the lmap.
	if (aerospike_lmap_put(&as, &err, NULL, &g_key, &lmap,
			(const as_val*)&ikey, (as_val *)&sval) != AEROSPIKE_OK) {
		LOG("first aerospike_lmap_put() returned %d - %s", err.code,
				err.message);
		example_cleanup(&as);
		exit(-1);
	}

	// Ok to reuse.
	as_integer_init(&ikey, 345);

	as_integer ival;
	as_integer_init(&ival, 1000);

	// Put an integer entry to the lmap.
	if (aerospike_lmap_put(&as, &err, NULL, &g_key, &lmap,
			(const as_val*)&ikey, (as_val*)&ival) != AEROSPIKE_OK) {
		LOG("second aerospike_lmap_put() returned %d - %s", err.code,
				err.message);
		example_cleanup(&as);
		exit(-1);
	}

	LOG("2 entries added to map");

	uint32_t n_elements = 0;

	// See how many elements we have in the lmap now.
	if (aerospike_lmap_size(&as, &err, NULL, &g_key, &lmap, &n_elements) !=
			AEROSPIKE_OK) {
		LOG("aerospike_lmap_size() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	if (n_elements != 2) {
		LOG("unexpected lmap size %u", n_elements);
		example_cleanup(&as);
		exit(-1);
	}

	LOG("lmap size confirmed to be %u", n_elements);

	as_boolean_init(&ldt_exists, false);

	// Verify that the LDT is now present.
	if (aerospike_lmap_ldt_exists(&as, &err, NULL, &g_key, &lmap,
			&ldt_exists) != AEROSPIKE_OK) {
		LOG("first aerospike_lmap_ldt_exists() returned %d - %s", err.code,
				err.message);
		example_cleanup(&as);
		exit(-1);
	}

	if (! as_boolean_get(&ldt_exists)) {
		LOG("did not find ldt that should be be present");
		example_cleanup(&as);
		exit(-1);
	}

	LOG("verified that lmap ldt is present");

	as_map* p_map = NULL;

	// Get all the entries back.
	if (aerospike_lmap_get_all(&as, &err, NULL, &g_key, &lmap, &p_map) !=
			AEROSPIKE_OK) {
		LOG("aerospike_lmap_filter() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	as_hashmap_iterator it;
	as_hashmap_iterator_init(&it, (const as_hashmap*)p_map);

	// See if the elements match what we expect.
	while (as_hashmap_iterator_has_next(&it)) {
		const as_val* p_val = as_hashmap_iterator_next(&it);
		char* p_str = as_val_tostring(p_val);

		LOG("   element type %d, value %s", as_val_type(p_val), p_str);
		free(p_str);
	}

	as_map_destroy(p_map);
	p_map = NULL;

	as_integer_init(&ikey, 12345);

	// Remove an entry from the map.
	if (aerospike_lmap_remove(&as, &err, NULL, &g_key, &lmap,
			(const as_val*)&ikey) != AEROSPIKE_OK) {
		LOG("aerospike_lmap_remove() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	as_val* p_val = NULL;

	// Make sure we can't get the value any more.
	as_status result = aerospike_lmap_get(&as, &err, NULL, &g_key, &lmap,
			(const as_val*)&ikey, &p_val);

	if (result == AEROSPIKE_OK) {
		// Server version >= 3.4.1 returns empty map if element doesn't exist.
		if (p_val && (as_val_type(p_val) != AS_MAP ||
				as_map_size((as_map*)p_val) != 0)) {
			char* p_str = as_val_tostring(p_val);

			LOG("entry was not successfully removed");
			LOG("   element type %d, value %s", as_val_type(p_val), p_str);
			free(p_str);

			as_val_destroy(p_val);
			example_cleanup(&as);
			exit(-1);
		}
	}
	else if (result != AEROSPIKE_ERR_LARGE_ITEM_NOT_FOUND) {
		LOG("aerospike_lmap_get() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	LOG("entry successfully removed");

	// Destroy the lmap.
	if (aerospike_lmap_destroy(&as, &err, NULL, &g_key, &lmap) !=
			AEROSPIKE_OK) {
		LOG("aerospike_lmap_destroy() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	n_elements = 0;

	// See if we can still do any lmap operations.
	if (aerospike_lmap_size(&as, &err, NULL, &g_key, &lmap, &n_elements) ==
			AEROSPIKE_OK) {
		LOG("aerospike_lmap_size() did not return error");
		example_cleanup(&as);
		exit(-1);
	}

	LOG("lmap successfully destroyed");

	// Cleanup and disconnect from the database cluster.
	example_cleanup(&as);

	LOG("lmap example successfully completed");

	return 0;
}
