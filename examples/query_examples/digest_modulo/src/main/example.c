/*******************************************************************************
 * Copyright 2008-2023 by Aerospike.
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
#include <aerospike/aerospike_query.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_error.h>
#include <aerospike/as_key.h>
#include <aerospike/as_partition.h>
#include <aerospike/as_query.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>

#include "example_utils.h"

//==========================================================
// Constants
//

const char TEST_INDEX_NAME[] = "test-bin-index";
const char DIGEST_MODULO_INDEX_NAME[] = "exp-digest-modulo";

const char PAGE_INDEX_NAME[] = "page-index";
const char PAGE_BIN_INT[] = "binint";
const char PAGE_BIN_STR[] = "binstr";

//==========================================================
// Forward Declarations
//

bool query_cb(const as_val* p_val, void* udata);
void cleanup(aerospike* p_as);
bool insert_records(aerospike* p_as);

//==========================================================
// exp-digest-modulo EXP QUERY Examples
//

int
main(int argc, char* argv[])
{
	// Parse command line arguments.
	if (! example_get_opts(argc, argv, EXAMPLE_MULTI_KEY_OPTS)) {
		exit(-1);
	}

	// Connect to the aerospike database cluster.
	aerospike as;
	example_connect_to_aerospike(&as);

	// Start clean.
	example_remove_test_records(&as);
	example_remove_index(&as, DIGEST_MODULO_INDEX_NAME);

	insert_records(&as);

	as_exp_build(exp, as_exp_cond(as_exp_cmp_eq(as_exp_digest_modulo(100), as_exp_int(1)), as_exp_int(1), as_exp_unknown()));

	// Create an expression index.
	if (! example_create_exp_index(&as, g_set, DIGEST_MODULO_INDEX_NAME, exp)) {
		cleanup(&as);
		exit(-1);
	}

	LOG("creating si: exp-digest-modulo where digest_modulo(100) == 1 ");

	as_query query;
	as_query_init(&query, g_namespace, g_set);

	as_query_where_inita(&query, 1);
	as_query_where_with_exp(&query, NULL, exp, as_integer_equals(1));

	LOG("executing query: where exp-digest-modulo equals 1");

	as_error err;

	uint32_t n_responses = 0;

	// Execute the query. This call blocks - callbacks are made in the scope of
	// this call.
	if (aerospike_query_foreach(&as, &err, NULL, &query, query_cb,
			&n_responses) != AEROSPIKE_OK) {
		LOG("aerospike_query_foreach() returned %d - %s", err.code,
				err.message);
		sleep(1000);
		as_query_destroy(&query);
		cleanup(&as);
		exit(-1);
	}

	LOG("query executed and returned %u", n_responses);

	as_query_destroy(&query);

	// Cleanup and disconnect from the database cluster.
	cleanup(&as);

	LOG("exp-digest-modulo query example successfully completed");
	return 0;
}


//==========================================================
// Query Callback
//

bool
query_cb(const as_val* p_val, void* udata)
{
	if (! p_val) {
		LOG("query callback returned null - query is complete");
		return true;
	}

	// The query didn't use a UDF, so the as_val object should be an as_record.
	as_record* p_rec = as_record_fromval(p_val);

	if (! p_rec) {
		LOG("query callback returned non-as_record object");
		return true;
	}

	uint32_t* n_responses = (uint32_t*)udata;

	as_aaf_uint32(n_responses, 1);

	//	LOG("query callback returned record:");
	example_dump_record(p_rec);

	return true;
}


//==========================================================
// Helpers
//

void
cleanup(aerospike* p_as)
{
	example_remove_test_records(p_as);
	example_remove_index(p_as, TEST_INDEX_NAME);
	example_cleanup(p_as);
}

bool
insert_records(aerospike* p_as)
{
	as_record rec;
	as_record_inita(&rec, 3);

	uint32_t n_keys = 10000;

	for (uint32_t i = 0; i < n_keys; i++) {
		as_error err;

		// No need to destroy a stack as_key object, if we only use
		// as_key_init_int64().
		as_key key;
		as_key_init_int64(&key, g_namespace, g_set, (int64_t)i);

		as_record_set_int64(&rec, "campaign1", i);
		as_record_set_int64(&rec, "campaign2", 100);
		as_record_set_int64(&rec, "campaign3", 100);

		// Write a record to the database.
		if (aerospike_key_put(p_as, &err, NULL, &key, &rec) != AEROSPIKE_OK) {
			LOG("aerospike_key_put() returned %d - %s", err.code, err.message);
			return false;
		}
	}

	LOG("insert succeeded");

	return true;
}