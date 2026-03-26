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
// Includes
//

#include <stddef.h>
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

#define INDEX_NAME "proj-test-index"
#define SET_NAME "proj-test"
#define NUM_RECORDS 20

//==========================================================
// Forward Declarations
//

void cleanup(aerospike* p_as);
bool insert_records(aerospike* p_as);
as_status test_query_select_only(aerospike* p_as);
as_status test_query_nobins_only(aerospike* p_as);
as_status test_query_select_and_nobins(aerospike* p_as);

//==========================================================
// Query Callbacks
//

struct query_stats {
	uint32_t count;
	uint32_t total_bins;
};

static bool
select_cb(const as_val* p_val, void* udata)
{
	if (! p_val) {
		return true;
	}

	struct query_stats* stats = (struct query_stats*)udata;
	as_record* p_rec = as_record_fromval(p_val);

	if (p_rec) {
		uint16_t nbins = as_record_numbins(p_rec);
		stats->count++;
		stats->total_bins += nbins;

		if (stats->count <= 3) {
			LOG("  record %u: %u bins", stats->count, nbins);
			example_dump_record(p_rec);
		}
	}

	return true;
}

//==========================================================
// Main
//

int
main(int argc, char* argv[])
{
	if (! example_get_opts(argc, argv, EXAMPLE_BASIC_OPTS)) {
		exit(-1);
	}

	aerospike as;
	example_connect_to_aerospike(&as);

	example_remove_index(&as, INDEX_NAME);

	if (! example_create_integer_index(&as, SET_NAME, "ibin", INDEX_NAME)) {
		cleanup(&as);
		exit(-1);
	}

	if (! insert_records(&as)) {
		cleanup(&as);
		exit(-1);
	}

	as_status status;

	// Test 1: query with select (bin projection) only.
	LOG("=== Test 1: query with select only ===");
	status = test_query_select_only(&as);

	if (status != AEROSPIKE_OK) {
		LOG("FAIL: test_query_select_only returned %d", status);
		cleanup(&as);
		exit(-1);
	}

	LOG("PASS\n");

	// Test 2: query with no_bins only.
	LOG("=== Test 2: query with no_bins only ===");
	status = test_query_nobins_only(&as);

	if (status != AEROSPIKE_OK) {
		LOG("FAIL: test_query_nobins_only returned %d", status);
		cleanup(&as);
		exit(-1);
	}

	LOG("PASS\n");

	// Test 3: query with both select AND no_bins.
	// Before the server fix this returned AEROSPIKE_ERR_REQUEST_INVALID.
	LOG("=== Test 3: query with select + no_bins (regression test) ===");
	status = test_query_select_and_nobins(&as);

	if (status != AEROSPIKE_OK) {
		LOG("FAIL: test_query_select_and_nobins returned %d", status);
		LOG("This is the regression - server should accept this combination");
		cleanup(&as);
		exit(-1);
	}

	LOG("PASS\n");

	cleanup(&as);
	LOG("query projection example successfully completed");
	return 0;
}

//==========================================================
// Helpers
//

void
cleanup(aerospike* p_as)
{
	as_error err;

	for (uint32_t i = 0; i < NUM_RECORDS; i++) {
		as_key key;
		as_key_init_int64(&key, g_namespace, SET_NAME, (int64_t)i);
		aerospike_key_remove(p_as, &err, NULL, &key);
	}

	example_remove_index(p_as, INDEX_NAME);
	example_cleanup(p_as);
}

bool
insert_records(aerospike* p_as)
{
	for (uint32_t i = 0; i < NUM_RECORDS; i++) {
		as_key key;
		as_key_init_int64(&key, g_namespace, SET_NAME, (int64_t)i);

		as_record rec;
		as_record_inita(&rec, 3);
		as_record_set_int64(&rec, "ibin", (int64_t)i);
		as_record_set_str(&rec, "sbin", "hello");
		as_record_set_int64(&rec, "extra", (int64_t)(i * 10));

		as_error err;

		if (aerospike_key_put(p_as, &err, NULL, &key, &rec) != AEROSPIKE_OK) {
			LOG("aerospike_key_put() returned %d - %s", err.code,
					err.message);
			return false;
		}
	}

	LOG("inserted %u records", NUM_RECORDS);
	return true;
}

//==========================================================
// Test: query with select only - should return selected bins
//

as_status
test_query_select_only(aerospike* p_as)
{
	as_query query;
	as_query_init(&query, g_namespace, SET_NAME);

	as_query_where_inita(&query, 1);
	as_query_where(&query, "ibin", as_integer_range(0, 9));

	as_query_select_inita(&query, 1);
	as_query_select(&query, "sbin");

	struct query_stats stats = { 0, 0 };
	as_error err;

	as_status status = aerospike_query_foreach(p_as, &err, NULL, &query,
			select_cb, &stats);

	if (status != AEROSPIKE_OK) {
		LOG("  aerospike_query_foreach() returned %d - %s", err.code,
				err.message);
		as_query_destroy(&query);
		return status;
	}

	LOG("  returned %u records, total bins across all records: %u",
			stats.count, stats.total_bins);

	if (stats.count == 0) {
		LOG("  ERROR: expected records but got none");
		as_query_destroy(&query);
		return AEROSPIKE_ERR_CLIENT;
	}

	if (stats.total_bins != stats.count) {
		LOG("  ERROR: expected 1 bin per record (selected 'sbin'), got %u total",
				stats.total_bins);
		as_query_destroy(&query);
		return AEROSPIKE_ERR_CLIENT;
	}

	as_query_destroy(&query);
	return AEROSPIKE_OK;
}

//==========================================================
// Test: query with no_bins - should return 0 bins per record
//

as_status
test_query_nobins_only(aerospike* p_as)
{
	as_query query;
	as_query_init(&query, g_namespace, SET_NAME);

	as_query_where_inita(&query, 1);
	as_query_where(&query, "ibin", as_integer_range(0, 9));

	query.no_bins = true;

	struct query_stats stats = { 0, 0 };
	as_error err;

	as_status status = aerospike_query_foreach(p_as, &err, NULL, &query,
			select_cb, &stats);

	if (status != AEROSPIKE_OK) {
		LOG("  aerospike_query_foreach() returned %d - %s", err.code,
				err.message);
		as_query_destroy(&query);
		return status;
	}

	LOG("  returned %u records, total bins across all records: %u",
			stats.count, stats.total_bins);

	if (stats.count == 0) {
		LOG("  ERROR: expected records but got none");
		as_query_destroy(&query);
		return AEROSPIKE_ERR_CLIENT;
	}

	if (stats.total_bins != 0) {
		LOG("  ERROR: expected 0 bins (no_bins=true), got %u total",
				stats.total_bins);
		as_query_destroy(&query);
		return AEROSPIKE_ERR_CLIENT;
	}

	as_query_destroy(&query);
	return AEROSPIKE_OK;
}

//==========================================================
// Test: query with BOTH select AND no_bins.
// This combination previously caused AEROSPIKE_ERR_REQUEST_INVALID
// because the server rejected having both GET_NO_BINS flag and
// read ops in the same message. The server fix skips parsing ops
// when no_bins is set.
//

as_status
test_query_select_and_nobins(aerospike* p_as)
{
	as_query query;
	as_query_init(&query, g_namespace, SET_NAME);

	as_query_where_inita(&query, 1);
	as_query_where(&query, "ibin", as_integer_range(0, 9));

	as_query_select_inita(&query, 2);
	as_query_select(&query, "sbin");
	as_query_select(&query, "extra");

	query.no_bins = true;

	struct query_stats stats = { 0, 0 };
	as_error err;

	as_status status = aerospike_query_foreach(p_as, &err, NULL, &query,
			select_cb, &stats);

	if (status != AEROSPIKE_OK) {
		LOG("  aerospike_query_foreach() returned %d - %s", err.code,
				err.message);
		as_query_destroy(&query);
		return status;
	}

	LOG("  returned %u records, total bins across all records: %u",
			stats.count, stats.total_bins);

	if (stats.count == 0) {
		LOG("  ERROR: expected records but got none");
		as_query_destroy(&query);
		return AEROSPIKE_ERR_CLIENT;
	}

	// no_bins takes precedence over select - expect 0 bins.
	if (stats.total_bins != 0) {
		LOG("  ERROR: expected 0 bins (no_bins takes precedence), got %u",
				stats.total_bins);
		as_query_destroy(&query);
		return AEROSPIKE_ERR_CLIENT;
	}

	as_query_destroy(&query);
	return AEROSPIKE_OK;
}
