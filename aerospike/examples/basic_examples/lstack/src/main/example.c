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
#include <aerospike/aerospike_lstack.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_arraylist_iterator.h>
#include <aerospike/as_error.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_list.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>
#include <aerospike/as_ldt.h>

#include "example_utils.h"


//==========================================================
// Constants
//


//==========================================================
// Forward Declarations
//



//==========================================================
// Large Stack Data Example
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

	// Create a large stack bin to use.
	as_ldt lstack;
	if (! as_ldt_init(&lstack, "mystack", AS_LDT_LSTACK, NULL)) {
		LOG("unable to initialize ldt");
		exit(-1);
	}

	// Push a few values onto the stack.
	as_error err;
	as_integer ival;
	as_integer_init(&ival, 123);

	if ( aerospike_lstack_push(&as, &err, NULL, &g_key, &lstack, (as_val *) &ival) != AEROSPIKE_OK ) {
		LOG("first aerospike_lstack_push() returned %d - %s", err.code, err.message);
		exit(-1);
	}

	as_string sval;
	as_string_init(&sval, "string stack value", false);
	if ( aerospike_lstack_push(&as, &err, NULL, &g_key, &lstack, (as_val *) &sval) != AEROSPIKE_OK ) {
		LOG("second aerospike_lstack_push() returned %d - %s", err.code, err.message);
		exit(-1);
	}
	LOG("2 values pushed");

	// Look at the stack size right now
	uint32_t n = 0;
	if ( aerospike_lstack_size(&as, &err, NULL, &g_key, &lstack, &n) != AEROSPIKE_OK ) {
		LOG("aerospike_lstack_size() returned %d - %s", err.code, err.message);
		exit(-1);
	}
	if (n != 2) {
		LOG("stack size not as expected %d not 2",n);
		exit(-1);
	}
	LOG("stack size confirmed to be %d",n);

	// Peek a few values back.
	as_ldt lstack2;
	as_ldt_init(&lstack2, "mystack", AS_LDT_LSTACK, NULL);
	uint32_t peek_count = 3;

	as_list *list = NULL;

	if ( aerospike_lstack_peek(&as, &err, NULL, &g_key, &lstack2, peek_count, &list) != AEROSPIKE_OK ) {
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

	// Push 3 more items onto the list
	as_arraylist vals;
	as_arraylist_inita(&vals, 3);
	as_arraylist_append_int64(&vals, 1000);
	as_arraylist_append_int64(&vals, 2000);
	as_arraylist_append_int64(&vals, 3000);

	if ( aerospike_lstack_pushall(&as, &err, NULL, &g_key, &lstack, (as_list *) &vals) != AEROSPIKE_OK ) {
		LOG("second aerospike_lstack_push() returned %d - %s", err.code, err.message);
		exit(-1);
	}
	LOG("3 more values pushed");

	// Peek all the values back again
	as_ldt_init(&lstack2, "mystack", AS_LDT_LSTACK, NULL);
	peek_count = 10;

	if ( aerospike_lstack_peek(&as, &err, NULL, &g_key, &lstack2, peek_count, &list) != AEROSPIKE_OK ) {
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

	// Set Capacity Size for the LStack
	if ( aerospike_lstack_set_capacity(&as, &err, NULL, &g_key, &lstack,
			10000) != AEROSPIKE_OK ) {
 		LOG("error(%d) %s", err.code, err.message);
		exit(-1);
 	}

	// Get Capacity Size for the LStack
	uint32_t cap_size = 0;
	if ( aerospike_lstack_get_capacity(&as, &err, NULL, &g_key, &lstack,
			&cap_size) != AEROSPIKE_OK ) {
 		LOG("error(%d) %s", err.code, err.message);
		exit(-1);
 	}
	if (cap_size != 10000) {
		LOG("capacity size not as expected %d not 10000",cap_size);
		exit(-1);
	}

	// Destroy the LStack
	LOG("Deleting Lstack");
	if ( aerospike_lstack_destroy(&as, &err, NULL, &g_key, &lstack) != AEROSPIKE_OK ) {
 		LOG("error(%d) %s", err.code, err.message);
		exit(-1);
 	}

	// See if we can still do any lstack operations
	n = 0;
	if ( aerospike_lstack_size(&as, &err, NULL, &g_key, &lstack, &n) == AEROSPIKE_OK ) {
		LOG("aerospike_lstack_push() did not return error");
		exit(-1);
	}

	// Cleanup and disconnect from the database cluster.
	// Note: deletes example record used to keep database intact
	example_cleanup(&as);

	LOG("lstack example successfully completed");

	return 0;
}


//==========================================================
// Helpers
//
