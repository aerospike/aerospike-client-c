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
// Top-K (ORDER BY <bin> LIMIT k) query example.
//
// Demonstrates as_query_order_by()/as_query_top_k(): a foreground query that
// returns only the best k records (by some bin's value), fully sorted, out
// of every record scanned - without the caller having to sort or truncate
// anything on its own.
//
// NOTE: this requires a server release that supports the ORDER_BY/TOP_K
// query fields. Running this example against a server that predates Top-K
// support will fail with a server-side error (the fields will not be
// recognized).
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
#define TOP_K 5

//==========================================================
// Forward Declarations
//

static void cleanup(aerospike* p_as);
static bool insert_records(aerospike* p_as);
static as_status query_top_k_scores(aerospike* p_as, as_error* err);
static as_status query_top_k_names_case_insensitive(aerospike* p_as, as_error* err);
static bool print_record_cb(const as_val* p_val, void* udata);

//==========================================================
// TOP-K QUERY Example
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

	if (query_top_k_scores(&as, &err) != AEROSPIKE_OK) {
		LOG("query_top_k_scores() returned %d - %s", err.code, err.message);
		cleanup(&as);
		exit(-1);
	}

	if (query_top_k_names_case_insensitive(&as, &err) != AEROSPIKE_OK) {
		LOG("query_top_k_names_case_insensitive() returned %d - %s", err.code, err.message);
		cleanup(&as);
		exit(-1);
	}

	// Cleanup and disconnect from the database cluster.
	cleanup(&as);

	LOG("top-k query example successfully completed");
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

// Insert N_RECORDS records, each with an integer "score" bin (a distinct
// value per record, so the Top-K result is deterministic) and a "name" bin
// (a mix of upper/lower case strings, to exercise case-insensitive Top-K).
static bool
insert_records(aerospike* p_as)
{
	as_record rec;
	as_record_inita(&rec, 2);

	static const char* names[] = {"Alice", "bob", "CAROL", "dave", "EVE"};

	for (uint32_t i = 0; i < N_RECORDS; i++) {
		as_error err;

		as_key key;
		as_key_init_int64(&key, g_namespace, g_set, (int64_t)i);

		as_record_set_int64(&rec, "score", (int64_t)i);
		as_record_set_str(&rec, "name", names[i % (sizeof(names) / sizeof(names[0]))]);

		if (aerospike_key_put(p_as, &err, NULL, &key, &rec) != AEROSPIKE_OK) {
			LOG("aerospike_key_put() returned %d - %s", err.code, err.message);
			return false;
		}
	}

	LOG("inserted %d records", N_RECORDS);
	return true;
}

// ORDER BY score DESC LIMIT 5: the top 5 highest-scoring records, in
// descending order, regardless of which node(s) they actually live on.
static as_status
query_top_k_scores(aerospike* p_as, as_error* err)
{
	as_query query;
	as_query_init(&query, g_namespace, g_set);

	as_query_order_by(&query, "score", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_DESCENDING,
			AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_top_k(&query, TOP_K);

	LOG("executing query: order by score desc limit %d", TOP_K);

	as_status status = aerospike_query_foreach(p_as, err, NULL, &query, print_record_cb, NULL);

	as_query_destroy(&query);
	return status;
}

// ORDER BY name ASC LIMIT 5 with case-insensitive comparison: without the
// CASE_INSENSITIVE flag, all-uppercase names would sort before any
// lowercase name (plain byte comparison); with it, comparison folds case
// first, so e.g. "Alice" and "alice" would compare equal up to case.
static as_status
query_top_k_names_case_insensitive(aerospike* p_as, as_error* err)
{
	as_query query;
	as_query_init(&query, g_namespace, g_set);

	as_query_order_by(&query, "name", AS_QUERY_ORDER_BY_STRING, AS_ORDER_ASCENDING,
			AS_QUERY_ORDER_BY_CASE_INSENSITIVE);
	as_query_top_k(&query, TOP_K);

	LOG("executing query: order by name asc (case insensitive) limit %d", TOP_K);

	as_status status = aerospike_query_foreach(p_as, err, NULL, &query, print_record_cb, NULL);

	as_query_destroy(&query);
	return status;
}

static bool
print_record_cb(const as_val* p_val, void* udata)
{
	(void)udata;

	if (! p_val) {
		LOG("query callback returned null - query is complete");
		return true;
	}

	as_record* p_rec = as_record_fromval(p_val);

	if (! p_rec) {
		LOG("query callback returned non-as_record object");
		return true;
	}

	example_dump_record(p_rec);
	return true;
}
