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

#include <stddef.h>
#include <stdlib.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_error.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_hashmap_iterator.h>
#include <aerospike/as_nil.h>
#include <aerospike/as_map_operations.h>
#include <aerospike/as_record.h>
#include <aerospike/as_record_iterator.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>

#include "example_utils.h"

//==========================================================
// Map Example
//

static const char* map_bin_name = "mapbin";

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
	
	// Create hashmap of scores.
	as_hashmap scores;
	as_hashmap_init(&scores, 4);
	as_string  mkey1;
	as_integer mval1;
	as_string_init(&mkey1, "Bob", false);
	as_integer_init(&mval1, 55);
	as_hashmap_set(&scores, (as_val*)&mkey1, (as_val*)&mval1);
	as_string  mkey2;
	as_integer mval2;
	as_string_init(&mkey2, "Jim", false);
	as_integer_init(&mval2, 98);
	as_hashmap_set(&scores, (as_val*)&mkey2, (as_val*)&mval2);
	as_string  mkey3;
	as_integer mval3;
	as_string_init(&mkey3, "John", false);
	as_integer_init(&mval3, 76);
	as_hashmap_set(&scores, (as_val*)&mkey3, (as_val*)&mval3);
	as_string  mkey4;
	as_integer mval4;
	as_string_init(&mkey4, "Harry", false);
	as_integer_init(&mval4, 82);
	as_hashmap_set(&scores, (as_val*)&mkey4, (as_val*)&mval4);
	
	// Write scores to server.
	as_map_policy map_policy;
	as_map_policy_init(&map_policy);

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_map_put_items(&ops, map_bin_name, &map_policy, (as_map*)&scores);
	
	as_error err;
	as_record* rec = NULL;
	if (aerospike_key_operate(&as, &err, NULL, &g_key, &ops, &rec) != AEROSPIKE_OK) {
		LOG("aerospike_key_operate() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}
	as_operations_destroy(&ops);
	as_record_destroy(rec);
	
	// Change multiple scores in one call.
	as_operations_inita(&ops, 3);
	
	as_string_init(&mkey1, "Bob", false);
	as_integer_init(&mval1, 25);
	as_operations_add_map_increment(&ops, map_bin_name, &map_policy, (as_val*)&mkey1, (as_val*)&mval1);
	
	as_string_init(&mkey1, "Jim", false);
	as_integer_init(&mval1, 10);
	as_operations_add_map_decrement(&ops, map_bin_name, &map_policy, (as_val*)&mkey1, (as_val*)&mval1);
	
	as_string_init(&mkey1, "Harry", false);
	as_integer_init(&mval1, 4);
	as_operations_add_map_increment(&ops, map_bin_name, &map_policy, (as_val*)&mkey1, (as_val*)&mval1);
	
	rec = NULL;
	if (aerospike_key_operate(&as, &err, NULL, &g_key, &ops, &rec) != AEROSPIKE_OK) {
		LOG("aerospike_key_operate() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}
	as_operations_destroy(&ops);
	as_record_destroy(rec);
	
	// Retrieve keys and values for the top 2 scores.
	as_operations_inita(&ops, 1);
	as_operations_add_map_get_by_rank_range(&ops, map_bin_name, -2, 2, AS_MAP_RETURN_KEY_VALUE);
	
	rec = NULL;
	if (aerospike_key_operate(&as, &err, NULL, &g_key, &ops, &rec) != AEROSPIKE_OK) {
		LOG("aerospike_key_operate() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}
	as_operations_destroy(&ops);
	
	// Operations are returned in same order they are added.  Since there is only one operation in
	// the aerospike_key_operate() call, the results are located in the first returned bin.
	as_list* results = &rec->bins.entries[0].valuep->list;
	
	// Iterate list and print results.
	// Server returns scores lowest to highest with key and value in separate rows.
	// Format: ["Harry", 86, "Jim", 88]
	// Iterate backwards to retrieve highest to lowest order.
	LOG("Top Scores");
	int i = as_list_size(results) - 1;
	
	while (i >= 0) {
		int score = (int)as_list_get_int64(results, i--);
		const char* name = as_list_get_str(results, i--);
		LOG("%s %d", name, score);
	}
	as_record_destroy(rec);

	// Cleanup and disconnect from the database cluster.
	example_cleanup(&as);
	LOG("map example successfully completed");
	return 0;
}
