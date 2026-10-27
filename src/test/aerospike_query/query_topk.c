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
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_query.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_boolean.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_error.h>
#include <aerospike/as_geojson.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_key.h>
#include <aerospike/as_list.h>
#include <aerospike/as_map.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_partition_filter.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_query.h>
#include <aerospike/as_query_topk.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>
#include <aerospike/as_stringmap.h>
#include <aerospike/as_val.h>
#include <aerospike/as_version.h>

#include <math.h>

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

	as_query_then_order_by(&query, "ts", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_ASCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);

	assert_string_eq(query.order_by.bin, "score");
	assert_true(query.then_order_by.defined);
	assert_string_eq(query.then_order_by.bin, "ts");
	assert_int_eq(query.then_order_by.type, AS_QUERY_ORDER_BY_INTEGER);
	assert_int_eq(query.then_order_by.direction, AS_ORDER_ASCENDING);
	assert_false(query.order_by_overflow);

	// A third key is recorded and rejected when the query runs.
	as_query_then_order_by(&query, "extra", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_ASCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);

	assert_true(query.order_by_overflow);
	assert_string_eq(query.then_order_by.bin, "ts");

	// Setting the first key again replaces both keys and keeps k.
	as_query_order_by(&query, "name", AS_QUERY_ORDER_BY_STRING, AS_ORDER_ASCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);

	assert_string_eq(query.order_by.bin, "name");
	assert_int_eq(query.order_by.type, AS_QUERY_ORDER_BY_STRING);
	assert_int_eq(query.order_by.direction, AS_ORDER_ASCENDING);
	assert_false(query.then_order_by.defined);
	assert_false(query.order_by_overflow);
	assert_int_eq(query.top_k, 25);

	as_query_destroy(&query);
}

TEST(query_topk_serialize, "as_query_to_bytes()/as_query_from_bytes() round-trip order_by/top_k")
{
	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_order_by(&query, "score", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_DESCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_then_order_by(&query, "name", AS_QUERY_ORDER_BY_STRING, AS_ORDER_ASCENDING,
		AS_QUERY_ORDER_BY_CASE_INSENSITIVE);
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
	assert_true(query2.then_order_by.defined);
	assert_string_eq(query2.then_order_by.bin, "name");
	assert_int_eq(query2.then_order_by.type, AS_QUERY_ORDER_BY_STRING);
	assert_int_eq(query2.then_order_by.direction, AS_ORDER_ASCENDING);
	assert_int_eq(query2.then_order_by.flags, AS_QUERY_ORDER_BY_CASE_INSENSITIVE);
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

	as_query_then_order_by(&q2, "ts", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_ASCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	assert_false(as_query_compare(&q1, &q2));

	as_query_then_order_by(&q1, "ts", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_ASCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
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
	assert_string_eq(err.message, "topK must be in [1, 1000]; got 1001");

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
	assert_string_eq(err.message, "orderBy flags are only valid with type STRING");

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
	assert_string_eq(err.message, "orderBy is incompatible with aggregate UDFs; use the UDF's own reduce stage");

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
	assert_string_eq(err.message,
		"orderBy/topK is only valid on foreground (SELECT-style) queries; background (UPDATE-style) "
		"queries do not produce a result stream");

	as_query_destroy(&query);
}

TEST(query_topk_validate_max_records, "any max_records is rejected, even one >= top_k")
{
	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_order_by(&query, "score", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_DESCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_top_k(&query, 10);
	query.max_records = 20;

	as_error err;
	as_status status = aerospike_query_foreach(as, &err, NULL, &query, NULL, NULL);
	assert_int_eq(status, AEROSPIKE_ERR_PARAM);
	assert_string_eq(err.message, "Top-K is incompatible with maxRecords");

	as_query_destroy(&query);
}

TEST(query_topk_validate_third_order_key, "a third order key is rejected")
{
	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_order_by(&query, "score", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_DESCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_then_order_by(&query, "ts", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_ASCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_then_order_by(&query, "name", AS_QUERY_ORDER_BY_STRING, AS_ORDER_ASCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_top_k(&query, 5);

	as_error err;
	as_status status = aerospike_query_foreach(as, &err, NULL, &query, NULL, NULL);
	assert_int_eq(status, AEROSPIKE_ERR_PARAM);
	assert_string_eq(err.message, "orderBy supports at most two keys");

	as_query_destroy(&query);
}

TEST(query_topk_then_order_by_first,
	"as_query_then_order_by() before as_query_order_by() sets the first key, which as_query_order_by() replaces")
{
	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_then_order_by(&query, "ts", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_ASCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_top_k(&query, 5);

	assert_true(query.order_by.defined);
	assert_string_eq(query.order_by.bin, "ts");
	assert_int_eq(query.order_by.type, AS_QUERY_ORDER_BY_INTEGER);
	assert_int_eq(query.order_by.direction, AS_ORDER_ASCENDING);
	assert_false(query.then_order_by.defined);

	as_query_order_by(&query, "score", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_DESCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);

	assert_string_eq(query.order_by.bin, "score");
	assert_int_eq(query.order_by.direction, AS_ORDER_DESCENDING);
	assert_false(query.then_order_by.defined);
	assert_int_eq(query.top_k, 5);

	as_query_destroy(&query);
}

TEST(query_topk_setters_invalid_bin_name, "an invalid order-by bin name still sets an order key")
{
	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_order_by(&query, "a_very_long_bin_name", AS_QUERY_ORDER_BY_INTEGER,
		AS_ORDER_DESCENDING, AS_QUERY_ORDER_BY_FLAGS_DEFAULT);

	assert_true(query.order_by.defined);
	assert_string_eq(query.order_by.bin, "");

	as_query_then_order_by(&query, NULL, AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_ASCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);

	assert_true(query.then_order_by.defined);
	assert_string_eq(query.then_order_by.bin, "");

	as_query_destroy(&query);
}

TEST(query_topk_validate_bin_name_too_long, "an order-by bin name longer than 15 bytes is rejected")
{
	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_order_by(&query, "a_very_long_bin_name", AS_QUERY_ORDER_BY_INTEGER,
		AS_ORDER_DESCENDING, AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_top_k(&query, 5);

	as_error err;
	as_status status = aerospike_query_foreach(as, &err, NULL, &query, NULL, NULL);
	assert_int_eq(status, AEROSPIKE_ERR_PARAM);
	assert_string_eq(err.message, "Top-K order-by bin name must be 1-15 UTF-8 bytes without NUL");

	as_query_destroy(&query);
}

TEST(query_topk_validate_bin_name_too_long_without_top_k,
	"an order-by bin name longer than 15 bytes is rejected without top_k")
{
	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_order_by(&query, "a_very_long_bin_name", AS_QUERY_ORDER_BY_INTEGER,
		AS_ORDER_DESCENDING, AS_QUERY_ORDER_BY_FLAGS_DEFAULT);

	as_error err;
	as_status status = aerospike_query_foreach(as, &err, NULL, &query, NULL, NULL);
	assert_int_eq(status, AEROSPIKE_ERR_PARAM);
	assert_string_eq(err.message, "Top-K order-by bin name must be 1-15 UTF-8 bytes without NUL");

	as_query_destroy(&query);
}

TEST(query_topk_validate_second_key_bin_name_invalid,
	"a second order key with a NULL or too-long bin name is rejected")
{
	const char* bin_names[] = {"a_very_long_bin_name", NULL};

	for (uint32_t i = 0; i < sizeof(bin_names) / sizeof(bin_names[0]); i++) {
		as_query query;
		as_query_init(&query, NAMESPACE, SET);
		as_query_order_by(&query, "score", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_DESCENDING,
			AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
		as_query_then_order_by(&query, bin_names[i], AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_ASCENDING,
			AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
		as_query_top_k(&query, 5);

		as_error err;
		as_status status = aerospike_query_foreach(as, &err, NULL, &query, NULL, NULL);
		assert_int_eq(status, AEROSPIKE_ERR_PARAM);
		assert_string_eq(err.message, "Top-K order-by bin name must be 1-15 UTF-8 bytes without NUL");

		as_query_destroy(&query);
	}
}

TEST(query_topk_validate_direction_invalid, "an order key direction other than ASC or DESC is rejected")
{
	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_order_by(&query, "score", AS_QUERY_ORDER_BY_INTEGER, (as_order)2,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_top_k(&query, 5);

	as_error err;
	as_status status = aerospike_query_foreach(as, &err, NULL, &query, NULL, NULL);
	assert_int_eq(status, AEROSPIKE_ERR_PARAM);
	assert_string_eq(err.message, "orderBy direction must be ASC or DESC");

	as_query_order_by(&query, "score", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_DESCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_then_order_by(&query, "ts", AS_QUERY_ORDER_BY_INTEGER, (as_order)2,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);

	status = aerospike_query_foreach(as, &err, NULL, &query, NULL, NULL);
	assert_int_eq(status, AEROSPIKE_ERR_PARAM);
	assert_string_eq(err.message, "orderBy direction must be ASC or DESC");

	as_query_destroy(&query);
}

TEST(query_topk_validate_type_missing, "an order key without a type is rejected")
{
	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_order_by(&query, "score", (as_query_order_by_type)0, AS_ORDER_DESCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_top_k(&query, 5);

	as_error err;
	as_status status = aerospike_query_foreach(as, &err, NULL, &query, NULL, NULL);
	assert_int_eq(status, AEROSPIKE_ERR_PARAM);
	assert_string_eq(err.message,
		"orderBy requires a type declaration (INTEGER, DOUBLE, STRING, or BYTES); Aerospike has no schema");

	as_query_destroy(&query);
}

TEST(query_topk_validate_type_invalid, "an order key type other than INTEGER, DOUBLE, STRING, BYTES is rejected")
{
	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_order_by(&query, "score", (as_query_order_by_type)9, AS_ORDER_DESCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_top_k(&query, 5);

	as_error err;
	as_status status = aerospike_query_foreach(as, &err, NULL, &query, NULL, NULL);
	assert_int_eq(status, AEROSPIKE_ERR_PARAM);
	assert_string_eq(err.message, "orderBy type must be one of INTEGER, DOUBLE, STRING, BYTES; got 9");

	as_query_destroy(&query);
}

TEST(query_topk_validate_flags_wrong_type, "any order-by flag with a non-STRING type is rejected")
{
	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_order_by(&query, "score", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_DESCENDING, 4);
	as_query_top_k(&query, 5);

	as_error err;
	as_status status = aerospike_query_foreach(as, &err, NULL, &query, NULL, NULL);
	assert_int_eq(status, AEROSPIKE_ERR_PARAM);
	assert_string_eq(err.message, "orderBy flags are only valid with type STRING");

	as_query_destroy(&query);
}

TEST(query_topk_validate_second_key_not_projected, "a second order key missing from projection is rejected")
{
	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_order_by(&query, "score", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_DESCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_then_order_by(&query, "ts", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_ASCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_top_k(&query, 5);
	as_query_select_init(&query, 1);
	as_query_select(&query, "score");

	as_error err;
	as_status status = aerospike_query_foreach(as, &err, NULL, &query, NULL, NULL);
	assert_int_eq(status, AEROSPIKE_ERR_PARAM);
	assert_string_eq(err.message,
		"orderBy bin 'ts' is not in projection; add it to setOperations or remove projection");

	as_query_destroy(&query);
}

TEST(query_topk_validate_duplicate_projection_op,
	"an order key produced by more than one projection operation is rejected")
{
	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_order_by(&query, "score", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_DESCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_top_k(&query, 5);

	as_operations* ops = as_operations_new(2);
	as_operations_add_read(ops, "score");
	as_operations_add_read(ops, "score");
	query.ops = ops;

	as_error err;
	as_status status = aerospike_query_foreach(as, &err, NULL, &query, NULL, NULL);
	assert_int_eq(status, AEROSPIKE_ERR_PARAM);
	assert_string_eq(err.message, "orderBy bin 'score' is produced by more than one operation");

	as_query_destroy(&query);
}

TEST(query_topk_validate_resumed_partition_filter,
	"a partition filter that resumes an earlier query is rejected")
{
	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_order_by(&query, "score", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_DESCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_top_k(&query, 5);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 1);

	as_digest* digest = as_key_digest(&key);

	as_partition_filter pf;
	as_partition_filter_set_after(&pf, digest);

	as_error err;
	as_status status = aerospike_query_partitions(as, &err, NULL, &query, &pf, NULL, NULL);
	assert_int_eq(status, AEROSPIKE_ERR_PARAM);
	assert_string_eq(err.message, "Top-K can't resume a partition filter from an earlier query");

	as_key_destroy(&key);
	as_query_destroy(&query);
}

TEST(query_topk_validate_async_rejected, "order_by/top_k is rejected on asynchronous queries")
{
	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_order_by(&query, "score", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_DESCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_top_k(&query, 5);

	as_error err;
	as_status status = aerospike_query_async(as, &err, NULL, &query, NULL, NULL, NULL);
	assert_int_eq(status, AEROSPIKE_ERR_PARAM);
	assert_string_eq(err.message, "Top-K is only supported by synchronous partition queries");

	as_partition_filter pf;
	as_partition_filter_set_all(&pf);

	status = aerospike_query_partitions_async(as, &err, NULL, &query, &pf, NULL, NULL, NULL);
	assert_int_eq(status, AEROSPIKE_ERR_PARAM);
	assert_string_eq(err.message, "Top-K is only supported by synchronous partition queries");

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
	assert_string_eq(err.message,
		"orderBy bin 'score' is not in projection; add it to setOperations or remove projection");

	as_query_destroy(&query);
}

TEST(query_topk_validate_server_support,
	"order_by/top_k is rejected unless every cluster node supports it")
{
	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_order_by(&query, "score", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_DESCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_top_k(&query, 5);

	bool has_order_by = as->cluster->has_order_by;
	as->cluster->has_order_by = false;

	as_error err;
	as_status status = aerospike_query_foreach(as, &err, NULL, &query, NULL, NULL);

	as->cluster->has_order_by = has_order_by;
	as_query_destroy(&query);

	assert_int_eq(status, AEROSPIKE_ERR_PARAM);
	assert_string_eq(err.message, "order_by/top_k is not supported by all nodes in the cluster");
}

//---------------------------------
// Merge/comparator tests - drive the as_query_topk_collector directly with
// synthetic per-node batches, bypassing the network entirely. Mirrors what
// each targeted node would send: a batch already sorted/deduped/truncated
// to <= k by the server.
//---------------------------------

static int g_topk_test_count;
static char g_topk_test_last[16][256];
static uint8_t g_topk_test_digests[16];

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

	g_topk_test_digests[g_topk_test_count] = rec->key.digest.value[0];

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
topk_test_init_rec(as_record* rec, uint8_t digest_byte, uint16_t n_bins)
{
	// as_record_init() (heap-allocated bins), not as_record_inita() (stack/alloca'd bins) -
	// the latter's alloca() must happen in the caller's own frame, not a helper, or the
	// bins array becomes a dangling pointer the moment this helper returns.
	as_record_init(rec, n_bins);
	memset(rec->key.digest.value, 0, AS_DIGEST_VALUE_SIZE);
	rec->key.digest.value[0] = digest_byte;
	rec->key.digest.init = true;
}

static void
topk_test_make_int_rec(as_record* rec, uint8_t digest_byte, int64_t score, bool has_bin)
{
	topk_test_init_rec(rec, digest_byte, 1);

	if (has_bin) {
		as_record_set_int64(rec, "score", score);
	}
}

static void
topk_test_collect_rec(as_query_topk_collector* collector, as_record* rec)
{
	as_query_topk_collect((as_val*)rec, collector);
	as_record_destroy(rec);
}

TEST(query_topk_merge_integer_desc,
	"topk collector merges/dedups/truncates integer-descending batches from multiple nodes")
{
	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_order_by(&query, "score", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_DESCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_top_k(&query, 3);

	g_topk_test_count = 0;

	as_query_topk_collector* collector = as_query_topk_collector_create(&query, topk_test_collect_cb, NULL);
	as_query_destroy(&query);

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
	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_order_by(&query, "name", AS_QUERY_ORDER_BY_STRING, AS_ORDER_ASCENDING,
		AS_QUERY_ORDER_BY_CASE_INSENSITIVE);
	as_query_top_k(&query, 2);

	g_topk_test_count = 0;

	as_query_topk_collector* collector = as_query_topk_collector_create(&query, topk_test_collect_cb, NULL);
	as_query_destroy(&query);

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

TEST(query_topk_merge_double_nan_and_zero,
	"topk collector ranks NaN above every double and -0.0 equal to 0.0")
{
	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_order_by(&query, "score", AS_QUERY_ORDER_BY_DOUBLE, AS_ORDER_DESCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_top_k(&query, 5);

	g_topk_test_count = 0;

	as_query_topk_collector* collector = as_query_topk_collector_create(&query, topk_test_collect_cb, NULL);
	as_query_destroy(&query);

	double scores[] = {1.0, 0.0, NAN, -0.0, 2.0};

	for (uint8_t i = 0; i < 5; i++) {
		as_record rec;
		topk_test_init_rec(&rec, (uint8_t)(i + 1), 1);
		as_record_set_double(&rec, "score", scores[i]);
		topk_test_collect_rec(collector, &rec);
	}

	as_query_topk_collect(NULL, collector);
	as_query_topk_collector_destroy(collector);

	// NaN (3), 2.0 (5), 1.0 (1), then 0.0 (2) and -0.0 (4) tie and rank by digest.
	assert_int_eq(g_topk_test_count, 5);
	assert_int_eq(g_topk_test_digests[0], 3);
	assert_int_eq(g_topk_test_digests[1], 5);
	assert_int_eq(g_topk_test_digests[2], 1);
	assert_int_eq(g_topk_test_digests[3], 2);
	assert_int_eq(g_topk_test_digests[4], 4);
}

TEST(query_topk_merge_two_keys,
	"topk collector breaks first-key ties, including NIL first keys, with the second key")
{
	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_order_by(&query, "score", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_DESCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_then_order_by(&query, "ts", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_ASCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_top_k(&query, 5);

	g_topk_test_count = 0;

	as_query_topk_collector* collector = as_query_topk_collector_create(&query, topk_test_collect_cb, NULL);
	as_query_destroy(&query);

	// Digest, score (0 means no score bin), ts.
	int64_t rows[][3] = {
		{1, 10, 5},
		{2, 10, 3},
		{3, 20, 9},
		{4, 0, 1},
		{5, 0, 0}
	};

	for (uint32_t i = 0; i < 5; i++) {
		as_record rec;
		topk_test_init_rec(&rec, (uint8_t)rows[i][0], 2);

		if (rows[i][1] != 0) {
			as_record_set_int64(&rec, "score", rows[i][1]);
		}
		as_record_set_int64(&rec, "ts", rows[i][2]);
		topk_test_collect_rec(collector, &rec);
	}

	as_query_topk_collect(NULL, collector);
	as_query_topk_collector_destroy(collector);

	assert_int_eq(g_topk_test_count, 5);
	assert_int_eq(g_topk_test_digests[0], 3);
	assert_int_eq(g_topk_test_digests[1], 2);
	assert_int_eq(g_topk_test_digests[2], 1);
	assert_int_eq(g_topk_test_digests[3], 5);
	assert_int_eq(g_topk_test_digests[4], 4);
}

static as_record* g_topk_test_kept;

static bool
topk_test_keep_cb(const as_val* val, void* udata)
{
	(void)udata;

	if (val && ! g_topk_test_kept) {
		g_topk_test_kept = (as_record*)as_val_reserve(val);
	}
	return true;
}

TEST(query_topk_merge_copies_values,
	"topk collector keeps strings past an embedded NUL and copies every bin value type")
{
	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_order_by(&query, "name", AS_QUERY_ORDER_BY_STRING, AS_ORDER_ASCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_top_k(&query, 1);

	g_topk_test_kept = NULL;

	as_query_topk_collector* collector = as_query_topk_collector_create(&query, topk_test_keep_cb, NULL);
	as_query_destroy(&query);

	// Both names read "a" up to the NUL. The higher name gets the lower digest,
	// so a copy that stopped at the NUL would tie and keep the wrong record.
	// Values are stack allocated and stored inline, as the query parser does.
	char higher_name[] = {'a', '\0', 'c'};
	as_string higher;
	as_string_init_wlen(&higher, higher_name, sizeof(higher_name), false);

	as_record r1;
	topk_test_init_rec(&r1, 1, 1);
	as_record_set_string(&r1, "name", &higher);
	topk_test_collect_rec(collector, &r1);

	char lower_name[] = {'a', '\0', 'b'};
	as_string lower;
	as_string_init_wlen(&lower, lower_name, sizeof(lower_name), false);

	as_boolean flag;
	as_boolean_init(&flag, true);

	char geo_json[] = "{\"type\":\"Point\",\"coordinates\":[0,0]}";
	as_geojson geo;
	as_geojson_init(&geo, geo_json, false);

	uint8_t raw[] = {1, 2, 3};
	as_bytes bytes;
	as_bytes_init_wrap(&bytes, raw, sizeof(raw), false);
	as_bytes_set_type(&bytes, AS_BYTES_HLL);

	as_arraylist* list = as_arraylist_new(1, 0);
	as_arraylist_append_int64(list, 7);

	as_hashmap* map = as_hashmap_new(1);
	as_stringmap_set_int64((as_map*)map, "k", 8);

	as_record r2;
	topk_test_init_rec(&r2, 2, 6);
	as_string_init(&r2.key.value.string, "pk", false);
	r2.key.valuep = &r2.key.value;
	as_record_set_string(&r2, "name", &lower);
	as_record_set(&r2, "flag", (as_bin_value*)&flag);
	as_record_set_geojson(&r2, "geo", &geo);
	as_record_set_bytes(&r2, "bytes", &bytes);
	as_record_set_list(&r2, "list", (as_list*)list);
	as_record_set_map(&r2, "map", (as_map*)map);
	topk_test_collect_rec(collector, &r2);

	as_query_topk_collect(NULL, collector);
	as_query_topk_collector_destroy(collector);

	as_record* rec = g_topk_test_kept;
	assert_not_null(rec);
	assert_int_eq(rec->key.digest.value[0], 2);

	char* key = as_string_get((as_string*)rec->key.valuep);
	assert_string_eq(key, "pk");

	as_string* name = as_record_get_string(rec, "name");
	size_t name_len = as_string_len(name);
	char* name_value = as_string_get(name);
	assert_true(name_value != lower_name);
	assert_int_eq(name_len, sizeof(lower_name));

	int name_cmp = memcmp(name_value, lower_name, name_len);
	assert_int_eq(name_cmp, 0);

	as_val* flag_val = (as_val*)as_record_get(rec, "flag");
	assert_int_eq(as_val_type(flag_val), AS_BOOLEAN);
	assert_true(as_boolean_get((as_boolean*)flag_val));

	as_geojson* geo_copy = as_record_get_geojson(rec, "geo");
	char* geo_value = as_geojson_get(geo_copy);
	assert_string_eq(geo_value, geo_json);

	as_bytes* bytes_copy = as_record_get_bytes(rec, "bytes");
	assert_true(bytes_copy->value != raw);
	assert_int_eq(bytes_copy->type, AS_BYTES_HLL);
	assert_bytes_eq(bytes_copy->value, bytes_copy->size, raw, sizeof(raw));

	as_list* list_copy = as_record_get_list(rec, "list");
	assert_int_eq(as_list_get_int64(list_copy, 0), 7);

	as_map* map_copy = as_record_get_map(rec, "map");
	assert_int_eq(as_stringmap_get_int64(map_copy, "k"), 8);

	as_record_destroy(rec);
}

//---------------------------------
// Server integration tests
//---------------------------------

static bool
server_supports_topk(void)
{
	as_node* node = as_node_get_random(as->cluster);

	if (! node) {
		info("skipping Top-K server test; no cluster node available");
		return false;
	}

	bool supported = as_version_compare(&node->version, &as_server_version_8_2_1) >= 0;
	as_node_release(node);

	if (! supported) {
		info("skipping Top-K server test; requires server >= 8.2.1");
	}
	return supported;
}

#define TOPK_INTEGRATION_RECORDS 20
#define TOPK_INTEGRATION_K 5

typedef struct topk_integration_result_s {
	uint32_t count;
	int64_t scores[TOPK_INTEGRATION_K];
} topk_integration_result;

static bool
topk_integration_callback(const as_val* val, void* udata)
{
	if (! val) {
		return true;
	}

	topk_integration_result* result = (topk_integration_result*)udata;

	if (result->count < TOPK_INTEGRATION_K) {
		result->scores[result->count] =
			as_record_get_int64(as_record_fromval(val), "score", -1);
	}
	result->count++;
	return true;
}

static void
topk_integration_remove_records(void)
{
	for (uint32_t i = 0; i < TOPK_INTEGRATION_RECORDS; i++) {
		as_key key;
		as_key_init_int64(&key, NAMESPACE, SET, (int64_t)i);

		as_error err;
		aerospike_key_remove(as, &err, NULL, &key);
	}
}

static bool
topk_integration_put_records(void)
{
	as_record rec;
	as_record_inita(&rec, 1);

	for (uint32_t i = 0; i < TOPK_INTEGRATION_RECORDS; i++) {
		as_record_set_int64(&rec, "score", (int64_t)i);

		as_key key;
		as_key_init_int64(&key, NAMESPACE, SET, (int64_t)i);

		as_error err;
		as_status status = aerospike_key_put(as, &err, NULL, &key, &rec);

		if (status != AEROSPIKE_OK) {
			as_record_destroy(&rec);
			return false;
		}
	}
	as_record_destroy(&rec);
	return true;
}

static as_status
topk_integration_query(const as_policy_query* policy, topk_integration_result* result)
{
	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_order_by(&query, "score", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_DESCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_top_k(&query, TOPK_INTEGRATION_K);

	as_error err;
	as_status status =
		aerospike_query_foreach(as, &err, policy, &query, topk_integration_callback, result);

	as_query_destroy(&query);
	return status;
}

TEST(query_topk_integer_desc_server,
	"server query returns the globally ranked top-k integer values")
{
	if (! server_supports_topk()) {
		return;
	}

	topk_integration_remove_records();
	assert_true(topk_integration_put_records());

	topk_integration_result result = {0};
	as_status status = topk_integration_query(NULL, &result);

	topk_integration_remove_records();

	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(result.count, TOPK_INTEGRATION_K);

	for (uint32_t i = 0; i < TOPK_INTEGRATION_K; i++) {
		assert_int_eq(result.scores[i], TOPK_INTEGRATION_RECORDS - 1 - i);
	}
}

TEST(query_topk_short_query_server, "a short-query hint is accepted and the query still ranks every record")
{
	if (! server_supports_topk()) {
		return;
	}

	topk_integration_remove_records();
	assert_true(topk_integration_put_records());

	as_policy_query policy;
	as_policy_query_init(&policy);
	policy.expected_duration = AS_QUERY_DURATION_SHORT;

	topk_integration_result result = {0};
	as_status status = topk_integration_query(&policy, &result);

	topk_integration_remove_records();

	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(result.count, TOPK_INTEGRATION_K);
	assert_int_eq(result.scores[0], TOPK_INTEGRATION_RECORDS - 1);
}

TEST(query_topk_order_by_without_top_k_server,
	"order_by without top_k is sent to the server, which rejects it")
{
	if (! server_supports_topk()) {
		return;
	}

	as_query query;
	as_query_init(&query, NAMESPACE, SET);
	as_query_order_by(&query, "score", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_DESCENDING,
		AS_QUERY_ORDER_BY_FLAGS_DEFAULT);

	topk_integration_result result = {0};
	as_error err;
	as_status status =
		aerospike_query_foreach(as, &err, NULL, &query, topk_integration_callback, &result);

	as_query_destroy(&query);

	assert_int_eq(status, AEROSPIKE_ERR_REQUEST_INVALID);
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
	suite_add(query_topk_validate_k_out_of_range);
	suite_add(query_topk_validate_case_insensitive_wrong_type);
	suite_add(query_topk_validate_incompatible_with_aggregation);
	suite_add(query_topk_validate_background_rejected);
	suite_add(query_topk_validate_max_records);
	suite_add(query_topk_validate_third_order_key);
	suite_add(query_topk_then_order_by_first);
	suite_add(query_topk_setters_invalid_bin_name);
	suite_add(query_topk_validate_bin_name_too_long);
	suite_add(query_topk_validate_bin_name_too_long_without_top_k);
	suite_add(query_topk_validate_second_key_bin_name_invalid);
	suite_add(query_topk_validate_direction_invalid);
	suite_add(query_topk_validate_type_missing);
	suite_add(query_topk_validate_type_invalid);
	suite_add(query_topk_validate_flags_wrong_type);
	suite_add(query_topk_validate_second_key_not_projected);
	suite_add(query_topk_validate_duplicate_projection_op);
	suite_add(query_topk_validate_resumed_partition_filter);
	suite_add(query_topk_validate_async_rejected);
	suite_add(query_topk_validate_projection_missing_bin);
	suite_add(query_topk_validate_server_support);
	suite_add(query_topk_merge_integer_desc);
	suite_add(query_topk_merge_string_case_insensitive_asc);
	suite_add(query_topk_merge_double_nan_and_zero);
	suite_add(query_topk_merge_two_keys);
	suite_add(query_topk_merge_copies_values);
	suite_add(query_topk_integer_desc_server);
	suite_add(query_topk_short_query_server);
	suite_add(query_topk_order_by_without_top_k_server);
}
