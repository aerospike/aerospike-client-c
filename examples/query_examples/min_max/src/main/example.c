/*******************************************************************************
 * Copyright 2008-2026 by Aerospike.
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
// MIN/MAX aggregation query example.
//
// Demonstrates aerospike_query_min()/aerospike_query_max(): find the
// minimum/maximum value of a scalar bin across a query's entire result set.
//
// NOTE: this requires a server release that supports Top-K queries, same as
// the "topk" example.
//

//==========================================================
// Includes
//

#include <stdint.h>
#include <stdlib.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_query.h>
#include <aerospike/as_error.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_key.h>
#include <aerospike/as_query.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>

#include "example_utils.h"

//==========================================================
// Constants
//

#define N_RECORDS 100

//==========================================================
// Forward Declarations
//

static void cleanup(aerospike* p_as);
static bool insert_records(aerospike* p_as);
static as_status find_min_max_score(aerospike* p_as, as_error* err);

//==========================================================
// MIN/MAX QUERY Example
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

	if (! insert_records(&as)) {
		cleanup(&as);
		exit(-1);
	}

	as_error err;

	if (find_min_max_score(&as, &err) != AEROSPIKE_OK) {
		LOG("find_min_max_score() returned %d - %s", err.code, err.message);
		cleanup(&as);
		exit(-1);
	}

	// Cleanup and disconnect from the database cluster.
	cleanup(&as);

	LOG("min/max query example successfully completed");
	return 0;
}

//==========================================================
// Helpers
//

static void
cleanup(aerospike* p_as)
{
	example_remove_test_records(p_as);
	example_cleanup(p_as);
}

// Insert N_RECORDS records, each with an integer "score" bin holding a
// distinct value 0..N_RECORDS-1.
static bool
insert_records(aerospike* p_as)
{
	as_record rec;
	as_record_inita(&rec, 1);

	for (uint32_t i = 0; i < N_RECORDS; i++) {
		as_error err;

		as_key key;
		as_key_init_int64(&key, g_namespace, g_set, (int64_t)i);

		as_record_set_int64(&rec, "score", (int64_t)i);

		if (aerospike_key_put(p_as, &err, NULL, &key, &rec) != AEROSPIKE_OK) {
			LOG("aerospike_key_put() returned %d - %s", err.code, err.message);
			return false;
		}
	}

	LOG("inserted %d records", N_RECORDS);
	return true;
}

// Find MIN(score) and MAX(score) across every record in the set.
static as_status
find_min_max_score(aerospike* p_as, as_error* err)
{
	as_query min_query;
	as_query_init(&min_query, g_namespace, g_set);

	as_val* min_value = NULL;
	as_status status =
		aerospike_query_min(p_as, err, NULL, &min_query, "score", AS_QUERY_ORDER_BY_INTEGER, &min_value);

	as_query_destroy(&min_query);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	if (min_value) {
		LOG("min(score) = %lld", (long long)as_integer_get((as_integer*)min_value));
		as_val_destroy(min_value);
	}
	else {
		LOG("min(score): no matching record");
	}

	as_query max_query;
	as_query_init(&max_query, g_namespace, g_set);

	as_val* max_value = NULL;
	status =
		aerospike_query_max(p_as, err, NULL, &max_query, "score", AS_QUERY_ORDER_BY_INTEGER, &max_value);

	as_query_destroy(&max_query);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	if (max_value) {
		LOG("max(score) = %lld", (long long)as_integer_get((as_integer*)max_value));
		as_val_destroy(max_value);
	}
	else {
		LOG("max(score): no matching record");
	}

	return AEROSPIKE_OK;
}
