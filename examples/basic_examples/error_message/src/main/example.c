/*******************************************************************************
 * Copyright 2008-2025 by Aerospike.
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
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>

#include "example_utils.h"


//==========================================================
// Error Message Example
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

	// Write a record with an integer bin, then perform an invalid op on it.
	const char* bin_name = "test-bin";
	as_key key;
	as_key_init(&key, g_namespace, g_set, "error-message-key");

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, bin_name, 1);

	as_policy_write policy;
	as_policy_write_init(&policy);
	policy.base.respond_error_message = true;

	as_error err;
	as_status status = aerospike_key_put(&as, &err, &policy, &key, &rec);

	if (status == AEROSPIKE_OK) {
		LOG("write succeeded, attempting invalid append");
	}
	else {
		LOG("write failed unexpectedly: %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	// Append a string to an integer bin to trigger a server-side type error.
	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_append_str(&ops, bin_name, "bad-append");

	as_policy_operate op_policy;
	as_policy_operate_init(&op_policy);
	op_policy.base.respond_error_message = true;

	status = aerospike_key_operate(&as, &err, &op_policy, &key, &ops, NULL);
	as_operations_destroy(&ops);

	if (status == AEROSPIKE_OK) {
		LOG("unexpected success on invalid append");
		example_cleanup(&as);
		exit(-1);
	}

	LOG("operate failed as expected: %d - %s", err.code, err.message);

	// Cleanup and disconnect from the database cluster.
	example_cleanup(&as);

	LOG("error_message example successfully completed");

	return 0;
}

