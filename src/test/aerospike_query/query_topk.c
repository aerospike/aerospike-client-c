/*
 * Copyright 2008-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_query.h>
#include <aerospike/as_error.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_query.h>
#include <aerospike/as_query_topk.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>
#include <aerospike/as_val.h>

#include "../test.h"

//---------------------------------
// Global Variables
//---------------------------------

extern aerospike* as;

#define NAMESPACE "test"
#define SET "query_topk"

//---------------------------------
// Setter / Serialization Tests
// (pure client-side logic - no server round trip)
//---------------------------------

TEST(query_topk_setters, "as_query_order_by()/as_query_top_k() set fields correctly")
{
	as_query query;
	as_query_init(&query, NAMESPACE, SET);

	assert_false(query.order_by.defined);
	assert_int_eq(query.top_k, 0);

	as_query_order_by(&query, "score", AS_QUERY_ORDER_BY_DOUBLE, AS_ORDER_DESCENDING,
		AS_QUERY_ORDER_BY_CASE_INSENSITIVE);
	as_query_top_k(&query, 25);

	assert_true(query.order_by.defined);
	assert_string_eq(query.order_by.bin, "score");
	assert_int_eq(query.order_by.type, AS_QUERY_ORDER_BY_DOUBLE);
	assert_int_eq(query.order_by.direction, AS_ORDER_DESCENDING);
	assert_int_eq(query.order_by.flags, AS_QUERY_ORDER_BY_CASE_INSENSITIVE);
	assert_int_eq(query.top_k, 25);

	// Overwrite - only one order-by clause supported.
	as_query_order_by(&query, "name", AS_QUERY_ORDER_BY_STRING, AS_ORDER_ASCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);

	assert_string_eq(query.order_by.bin, "name");
	assert_int_eq(query.order_by.type, AS_QUERY_ORDER_BY_STRING);
	assert_int_eq(query.order_by.direction, AS_ORDER_ASCENDING);

	as_query_destroy(&query);
}

TEST(query_topk_serialize, "as_query_to_bytes()/as_query_from_bytes() round-trip order_by/top_k")
{
	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_order_by(&query, "score", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_DESCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_top_k(&query, 7);

	uint8_t* bytes = NULL;
	uint32_t bytes_size = 0;
	assert_true(as_query_to_bytes(&query, &bytes, &bytes_size));
	assert_not_null(bytes);

	as_query query2;
	as_query_init(&query2, NAMESPACE, SET);
	assert_true(as_query_from_bytes(&query2, bytes, bytes_size));
	cf_free(bytes);

	assert_true(query2.order_by.defined);
	assert_string_eq(query2.order_by.bin, "score");
	assert_int_eq(query2.order_by.type, AS_QUERY_ORDER_BY_INTEGER);
	assert_int_eq(query2.order_by.direction, AS_ORDER_DESCENDING);
	assert_int_eq(query2.top_k, 7);

	assert_true(as_query_compare(&query, &query2));

	as_query_destroy(&query);
	as_query_destroy(&query2);
}

TEST(query_topk_compare, "as_query_compare() distinguishes different order_by/top_k")
{
	as_query q1;
	as_query_init(&q1, NAMESPACE, SET);
	as_query_order_by(&q1, "score", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_DESCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_top_k(&q1, 5);

	as_query q2;
	as_query_init(&q2, NAMESPACE, SET);
	as_query_order_by(&q2, "score", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_DESCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_top_k(&q2, 6); // different k

	assert_false(as_query_compare(&q1, &q2));

	as_query_top_k(&q2, 5);
	assert_true(as_query_compare(&q1, &q2));

	as_query_destroy(&q1);
	as_query_destroy(&q2);
}

//---------------------------------
// Client-side validation tests (require a connected `as` client since the
// order_by/top_k checks live in as_query_command_size(), which needs cluster
// capability info (qb->is_new) - but every check ahead of that one is a pure
// parameter check that always runs first, so these fail fast without ever
// touching the network for an invalid combination. See
// as_query_validate_topk() in aerospike_query.c).
//---------------------------------

TEST(query_topk_validate_top_k_without_order_by, "top_k without order_by is rejected")
{
	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_top_k(&query, 5);

	as_error err;
	as_status status = aerospike_query_foreach(as, &err, NULL, &query, NULL, NULL);
	assert_int_eq(status, AEROSPIKE_ERR_PARAM);

	as_query_destroy(&query);
}

TEST(query_topk_validate_order_by_without_top_k, "order_by without top_k is rejected")
{
	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_order_by(&query, "score", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_DESCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);

	as_error err;
	as_status status = aerospike_query_foreach(as, &err, NULL, &query, NULL, NULL);
	assert_int_eq(status, AEROSPIKE_ERR_PARAM);

	as_query_destroy(&query);
}

TEST(query_topk_validate_k_out_of_range, "top_k > 1000 is rejected")
{
	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_order_by(&query, "score", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_DESCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_top_k(&query, 1001);

	as_error err;
	as_status status = aerospike_query_foreach(as, &err, NULL, &query, NULL, NULL);
	assert_int_eq(status, AEROSPIKE_ERR_PARAM);

	as_query_destroy(&query);
}

TEST(query_topk_validate_case_insensitive_wrong_type, "CASE_INSENSITIVE with non-STRING type is rejected")
{
	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_order_by(&query, "score", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_DESCENDING,
		AS_QUERY_ORDER_BY_CASE_INSENSITIVE);
	as_query_top_k(&query, 5);

	as_error err;
	as_status status = aerospike_query_foreach(as, &err, NULL, &query, NULL, NULL);
	assert_int_eq(status, AEROSPIKE_ERR_PARAM);

	as_query_destroy(&query);
}

TEST(query_topk_validate_incompatible_with_aggregation, "order_by/top_k is rejected with an aggregation UDF")
{
	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_order_by(&query, "score", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_DESCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_top_k(&query, 5);
	as_query_apply(&query, "some_module", "some_function", NULL);

	as_error err;
	as_status status = aerospike_query_foreach(as, &err, NULL, &query, NULL, NULL);
	assert_int_eq(status, AEROSPIKE_ERR_PARAM);

	as_query_destroy(&query);
}

TEST(query_topk_validate_background_rejected, "order_by/top_k is rejected on background queries")
{
	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_order_by(&query, "score", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_DESCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_top_k(&query, 5);
	as_query_apply(&query, "some_module", "some_function", NULL);

	as_error err;
	uint64_t query_id = 0;
	as_status status = aerospike_query_background(as, &err, NULL, &query, &query_id);
	assert_int_eq(status, AEROSPIKE_ERR_PARAM);

	as_query_destroy(&query);
}

TEST(query_topk_validate_max_records_inconsistent, "max_records < top_k is rejected")
{
	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_order_by(&query, "score", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_DESCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_top_k(&query, 10);
	query.max_records = 5;

	as_error err;
	as_status status = aerospike_query_foreach(as, &err, NULL, &query, NULL, NULL);
	assert_int_eq(status, AEROSPIKE_ERR_PARAM);

	as_query_destroy(&query);
}

TEST(query_topk_validate_projection_missing_bin, "order_by bin missing from projection is rejected")
{
	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_order_by(&query, "score", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_DESCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_top_k(&query, 5);
	as_query_select_init(&query, 1);
	as_query_select(&query, "some_other_bin");

	as_error err;
	as_status status = aerospike_query_foreach(as, &err, NULL, &query, NULL, NULL);
	assert_int_eq(status, AEROSPIKE_ERR_PARAM);

	as_query_destroy(&query);
}

//---------------------------------
// Merge/comparator tests - drive the as_query_topk_collector directly with
// synthetic per-node batches, bypassing the network entirely. Mirrors what
// each targeted node would send: a batch already sorted/deduped/truncated
// to <= k by the server.
//---------------------------------

static int g_topk_test_count;
static char g_topk_test_last[16][256];

static bool
topk_test_collect_cb(const as_val* val, void* udata)
{
	(void)udata;

	if (! val) {
		return true;
	}

	as_record* rec = as_record_fromval(val);
	as_integer* score = (as_integer*)as_record_get(rec, "score");
	as_string* name = (as_string*)as_record_get(rec, "name");

	if (name) {
		snprintf(g_topk_test_last[g_topk_test_count], sizeof(g_topk_test_last[0]), "digest=%02x name=%s",
			rec->key.digest.value[0], as_string_get(name));
	}
	else {
		snprintf(g_topk_test_last[g_topk_test_count], sizeof(g_topk_test_last[0]), "digest=%02x score=%lld",
			rec->key.digest.value[0], score ? (long long)as_integer_get(score) : -1);
	}
	g_topk_test_count++;
	return true;
}

static void
topk_test_make_int_rec(as_record* rec, uint8_t digest_byte, int64_t score, bool has_bin)
{
	// as_record_init() (heap-allocated bins), not as_record_inita() (stack/alloca'd bins) -
	// the latter's alloca() must happen in the caller's own frame, not a helper, or the
	// bins array becomes a dangling pointer the moment this helper returns.
	as_record_init(rec, 1);
	memset(rec->key.digest.value, 0, AS_DIGEST_VALUE_SIZE);
	rec->key.digest.value[0] = digest_byte;
	rec->key.digest.init = true;

	if (has_bin) {
		as_record_set_int64(rec, "score", score);
	}
}

TEST(query_topk_merge_integer_desc,
	"topk collector merges/dedups/truncates integer-descending batches from multiple nodes")
{
	as_query_order_by_field order_by;
	order_by.type = AS_QUERY_ORDER_BY_INTEGER;
	order_by.direction = AS_ORDER_DESCENDING;
	order_by.flags = AS_QUERY_ORDER_BY_FLAGS_DEFAULT;
	as_strncpy(order_by.bin, "score", sizeof(order_by.bin));
	order_by.defined = true;

	g_topk_test_count = 0;

	as_query_topk_collector* collector =
		as_query_topk_collector_create_sync(&order_by, 3, topk_test_collect_cb, NULL);

	// Node A batch (already sorted desc by score): 50, 30, 10.
	as_record a1; topk_test_make_int_rec(&a1, 0xA1, 50, true);
	as_record a2; topk_test_make_int_rec(&a2, 0xA2, 30, true);
	as_record a3; topk_test_make_int_rec(&a3, 0xA3, 10, true);
	as_query_topk_collect((as_val*)&a1, collector);
	as_query_topk_collect((as_val*)&a2, collector);
	as_query_topk_collect((as_val*)&a3, collector);
	as_record_destroy(&a1);
	as_record_destroy(&a2);
	as_record_destroy(&a3);

	// Node B batch: 40, 20, and one record missing the order-by bin (NIL - must never
	// appear in the final result, regardless of direction).
	as_record b1; topk_test_make_int_rec(&b1, 0xB1, 40, true);
	as_record b2; topk_test_make_int_rec(&b2, 0xB2, 20, true);
	as_record b3; topk_test_make_int_rec(&b3, 0xB3, 0, false);
	as_query_topk_collect((as_val*)&b1, collector);
	as_query_topk_collect((as_val*)&b2, collector);
	as_query_topk_collect((as_val*)&b3, collector);
	as_record_destroy(&b1);
	as_record_destroy(&b2);
	as_record_destroy(&b3);

	// Duplicate digest of a1 with a different score (simulates a migration race) - must be
	// deduped down to a single entry.
	as_record dup; topk_test_make_int_rec(&dup, 0xA1, 999, true);
	as_query_topk_collect((as_val*)&dup, collector);
	as_record_destroy(&dup);

	// Trigger the flush (val == NULL signals every node has reported).
	as_query_topk_collect(NULL, collector);
	as_query_topk_collector_destroy(collector);

	// Expect the true top 3 by score desc: 50 (0xA1), 40 (0xB1), 30 (0xA2).
	assert_int_eq(g_topk_test_count, 3);
	assert_true(strstr(g_topk_test_last[0], "score=50") != NULL);
	assert_true(strstr(g_topk_test_last[1], "score=40") != NULL);
	assert_true(strstr(g_topk_test_last[2], "score=30") != NULL);
}

TEST(query_topk_merge_string_case_insensitive_asc,
	"topk collector applies STRING + CASE_INSENSITIVE ascending ordering")
{
	as_query_order_by_field order_by;
	order_by.type = AS_QUERY_ORDER_BY_STRING;
	order_by.direction = AS_ORDER_ASCENDING;
	order_by.flags = AS_QUERY_ORDER_BY_CASE_INSENSITIVE;
	as_strncpy(order_by.bin, "name", sizeof(order_by.bin));
	order_by.defined = true;

	g_topk_test_count = 0;

	as_query_topk_collector* collector =
		as_query_topk_collector_create_sync(&order_by, 2, topk_test_collect_cb, NULL);

	as_record s1;
	as_record_init(&s1, 1);
	memset(s1.key.digest.value, 0, AS_DIGEST_VALUE_SIZE);
	s1.key.digest.value[0] = 1;
	s1.key.digest.init = true;
	as_record_set_str(&s1, "name", "Bob");

	as_record s2;
	as_record_init(&s2, 1);
	memset(s2.key.digest.value, 0, AS_DIGEST_VALUE_SIZE);
	s2.key.digest.value[0] = 2;
	s2.key.digest.init = true;
	as_record_set_str(&s2, "name", "alice"); // lowercase - must still sort before "Bob".

	as_record s3;
	as_record_init(&s3, 1);
	memset(s3.key.digest.value, 0, AS_DIGEST_VALUE_SIZE);
	s3.key.digest.value[0] = 3;
	s3.key.digest.init = true;
	as_record_set_str(&s3, "name", "Carol"); // Truncated away - k == 2.

	as_query_topk_collect((as_val*)&s1, collector);
	as_query_topk_collect((as_val*)&s2, collector);
	as_query_topk_collect((as_val*)&s3, collector);
	as_record_destroy(&s1);
	as_record_destroy(&s2);
	as_record_destroy(&s3);

	as_query_topk_collect(NULL, collector);
	as_query_topk_collector_destroy(collector);

	assert_int_eq(g_topk_test_count, 2);
	assert_true(strstr(g_topk_test_last[0], "name=alice") != NULL);
	assert_true(strstr(g_topk_test_last[1], "name=Bob") != NULL);
}

//---------------------------------
// Suite
//---------------------------------

SUITE(query_topk, "Top-K (order_by/top_k) query tests")
{
	suite_add(query_topk_setters);
	suite_add(query_topk_serialize);
	suite_add(query_topk_compare);
	suite_add(query_topk_validate_top_k_without_order_by);
	suite_add(query_topk_validate_order_by_without_top_k);
	suite_add(query_topk_validate_k_out_of_range);
	suite_add(query_topk_validate_case_insensitive_wrong_type);
	suite_add(query_topk_validate_incompatible_with_aggregation);
	suite_add(query_topk_validate_background_rejected);
	suite_add(query_topk_validate_max_records_inconsistent);
	suite_add(query_topk_validate_projection_missing_bin);
	suite_add(query_topk_merge_integer_desc);
	suite_add(query_topk_merge_string_case_insensitive_asc);
}
