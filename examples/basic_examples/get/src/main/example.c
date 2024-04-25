/*******************************************************************************
 * Copyright 2008-2018 by Aerospike.
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
#include <aerospike/as_record.h>
#include <aerospike/as_sleep.h>
#include <aerospike/as_status.h>
#include <aerospike/aerospike_batch.h>
#include <aerospike/as_arraylist.h>

#include "example_utils.h"


//==========================================================
// Forward Declarations
//

bool write_record(aerospike* p_as);


//==========================================================
// GET Example
//

static void customize_batch_remove_policy(as_policy_batch_remove *policy)
{
	policy->gen = AS_POLICY_GEN_EQ;
	policy->generation = 42;
}

int
main(int argc, char* argv[])
{
	as_config config;
	as_config_init(&config);
	as_config_add_host(&config, "127.0.0.1", 3000);

	customize_batch_remove_policy(&(config.policies.batch_remove));

	aerospike as;
	aerospike_init(&as, &config);
	as_error err;

	if (aerospike_connect(&as, &err) != AEROSPIKE_OK) {
	  fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
	  exit(1);
	}

	as_batch_records recs;
	as_batch_records_init(&recs, 1);

	as_batch_remove_record *r = as_batch_remove_reserve(&recs);
	as_key_init_int64(&r->key, "test", "demo", 0);

#if 1
	as_policy_batch_remove transaction_level_policy;
	as_policy_batch_remove_init(&transaction_level_policy);
	customize_batch_remove_policy(&transaction_level_policy);
	r->policy = &transaction_level_policy;
#endif

	as_status result = aerospike_batch_write(&as, &err, NULL, &recs);
	if (result == AEROSPIKE_OK) {
		printf("Batch write succeeded\n");
	} else {
		printf("Batch write returned an error: %d\n", result);
	}

	as_batch_records_destroy(&recs);

	aerospike_close(&as, &err);

	return 0;
}


//==========================================================
// Helpers
//

bool
write_record(aerospike* p_as)
{
	as_error err;

	// Create an as_record object with four bins with different value types. By
	// using as_record_inita(), we won't need to destroy the record if we only
	// set bins using as_record_set_int64(), as_record_set_str(), and
	// as_record_set_raw().
	as_record rec;
	as_record_inita(&rec, 4);
	as_record_set_int64(&rec, "test-bin-1", 1111);
	as_record_set_int64(&rec, "test-bin-2", 2222);
	as_record_set_str(&rec, "test-bin-3", "test-bin-3-data");

	static const uint8_t bytes[] = { 1, 2, 3 };
	as_record_set_raw(&rec, "test-bin-4", bytes, 3);

	// Log its contents.
	LOG("as_record object to write to database:");
	example_dump_record(&rec);

	// Write the record to the database.
	if (aerospike_key_put(p_as, &err, NULL, &g_key, &rec) != AEROSPIKE_OK) {
		LOG("aerospike_key_put() returned %d - %s", err.code, err.message);
		return false;
	}

	LOG("write succeeded");

	return true;
}
