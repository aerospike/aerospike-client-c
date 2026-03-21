/*
 * Copyright 2008-2025 Aerospike, Inc.
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

//==========================================================
// Vector Distance Expression Example
//
// This example demonstrates how to use as_exp_vector_distance()
// to calculate distances between vectors in expressions.
//
// Usage: ./vector_distance_example <host> <port>
//==========================================================

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_query.h>
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_record.h>
#include <aerospike/as_query.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_exp_operations.h>
#include <aerospike/as_ml_vector.h>

#include "example_utils.h"

//==========================================================
// Constants
//

const char* NAMESPACE = "test";
const char* SET = "vector-demo";

//==========================================================
// Helper Functions
//

/**
 * Create a vector in ML vector format
 */
static void create_ml_vector(uint8_t* buffer, float* data, uint32_t count)
{
	uint8_t* p = buffer;

	// Magic number: "VECT" (0x56454354) in native byte order
	*(uint32_t*)p = 0x56454354;
	p += 4;

	// Version: 1 in native byte order
	*(uint32_t*)p = 1;
	p += 4;

	// Element count
	*(uint32_t*)p = count;
	p += 4;

	// Element type: 1 (FLOAT32) in native byte order
	*(uint32_t*)p = 1;
	p += 4;

	// Vector data
	memcpy(p, data, count * sizeof(float));
}

/**
 * Print vector data
 */
static void print_vector(const char* name, float* data, uint32_t count)
{
	printf("%s: [", name);
	for (uint32_t i = 0; i < count; i++) {
		printf("%.2f", data[i]);
		if (i < count - 1) printf(", ");
	}
	printf("]\n");
}

//==========================================================
// Query Callback
//

bool query_callback(const as_val* val, void* udata)
{
	if (!val) {
		LOG("query callback returned null - query is complete");
		return true;
	}

	// The query was called with operations, so the callback will be called
	// with a record
	as_record* rec = as_record_fromval(val);
	if (!rec) {
		LOG("query callback returned non-as_record object");
		return true;
	}

	LOG("query callback returned record:");
	example_dump_record(rec);

	return true;
}

//==========================================================
// Example 1: Using as_exp_vector_distance with as_operations_exp_vector_distance
//

void example_vector_distance_expression_operation(aerospike* as)
{
	LOG("=== Example 1: Vector Distance Expression Operation ===");

	as_error err;
	as_query query;
	as_query_init(&query, NAMESPACE, SET);

	// Set up a range query
	as_query_where_inita(&query, 1);
	as_query_where(&query, "id", as_integer_range(0, 5));

	// Create a query vector
	float query_data[] = {2.5f, 3.5f, 4.5f};
	uint8_t query_vector_data[28]; // 16 bytes header + 12 bytes data (3 floats)
	create_ml_vector(query_vector_data, query_data, 3);

	as_bytes query_vector_bytes;
	as_bytes_init_wrap(&query_vector_bytes, query_vector_data, 28, false);
	query_vector_bytes.type = AS_BYTES_VECTOR;

	print_vector("Query vector", query_data, 3);

	// Create operations with vector distance expression
	as_operations ops;
	as_operations_inita(&ops, 3);

	// Read regular bins
	as_operations_add_read(&ops, "id");
	as_operations_add_read(&ops, "name");

	// Build vector distance expression
	as_exp_build(vector_distance_exp,
		as_exp_vector_distance(
			as_exp_bin_blob("vector-bin"),
			as_exp_bytes(query_vector_bytes.value, query_vector_bytes.size)
		)
	);

	// Use the new as_operations_exp_vector_distance function
		if (!as_operations_exp_vector_distance(&ops, "distance", vector_distance_exp, AS_EXP_READ_DEFAULT)) {
		LOG("Failed to add vector distance expression operation");
		as_query_destroy(&query);
		return;
	}

	// Set operations on query
	query.ops = &ops;

	// Execute query
	LOG("Executing query with vector distance expression...");
	if (aerospike_query_foreach(as, &err, NULL, &query,
			query_callback, NULL) != AEROSPIKE_OK) {
		LOG("Query failed: (%d) %s", err.code, err.message);
	}

	as_exp_destroy(vector_distance_exp);
	as_query_destroy(&query);
}

//==========================================================
// Example 2: Using as_exp_vector_distance with as_operations_exp_read
//

void example_vector_distance_expression_read(aerospike* as)
{
	LOG("=== Example 2: Vector Distance Expression Read ===");

	as_error err;
	as_query query;
	as_query_init(&query, NAMESPACE, SET);

	// Set up a range query
	as_query_where_inita(&query, 1);
	as_query_where(&query, "id", as_integer_range(0, 5));

	// Create a query vector
	float query_data[] = {1.0f, 2.0f, 3.0f};
	uint8_t query_vector_data[28];
	create_ml_vector(query_vector_data, query_data, 3);

	as_bytes query_vector_bytes;
	as_bytes_init_wrap(&query_vector_bytes, query_vector_data, 28, false);
	query_vector_bytes.type = AS_BYTES_VECTOR;

	print_vector("Query vector", query_data, 3);

	// Create operations
	as_operations ops;
	as_operations_inita(&ops, 3);

	// Read regular bins
	as_operations_add_read(&ops, "id");
	as_operations_add_read(&ops, "name");

	// Build vector distance expression
	as_exp_build(vector_distance_exp,
		as_exp_vector_distance(
			as_exp_bin_blob("vector-bin"),
			as_exp_bytes(query_vector_bytes.value, query_vector_bytes.size)
		)
	);

	// Use as_operations_exp_read (alternative method)
	if (!as_operations_exp_read(&ops, "distance", vector_distance_exp, AS_EXP_READ_DEFAULT)) {
		LOG("Failed to add vector distance expression read");
		as_query_destroy(&query);
		return;
	}

	// Set operations on query
	query.ops = &ops;

	// Execute query
	LOG("Executing query with vector distance expression...");
	if (aerospike_query_foreach(as, &err, NULL, &query,
			query_callback, NULL) != AEROSPIKE_OK) {
		LOG("Query failed: (%d) %s", err.code, err.message);
	}

	as_exp_destroy(vector_distance_exp);
	as_query_destroy(&query);
}

//==========================================================
// Example 3: Using vector distance in a filter expression
//

void example_vector_distance_filter(aerospike* as)
{
	LOG("=== Example 3: Vector Distance in Filter Expression ===");

	as_error err;
	as_query query;
	as_query_init(&query, NAMESPACE, SET);

	// Create a query vector
	float query_data[] = {2.0f, 2.0f, 2.0f};
	uint8_t query_vector_data[28];
	create_ml_vector(query_vector_data, query_data, 3);

	as_bytes query_vector_bytes;
	as_bytes_init_wrap(&query_vector_bytes, query_vector_data, 28, false);
	query_vector_bytes.type = AS_BYTES_VECTOR;

	print_vector("Query vector", query_data, 3);

	// Build a filter expression: only return records where distance < 2.0
	as_exp_build(filter_exp,
		as_exp_cmp_lt(
			as_exp_vector_distance(
				as_exp_bin_blob("vector-bin"),
				as_exp_bytes(query_vector_bytes.value, query_vector_bytes.size)
			),
			as_exp_float(2.0)
		)
	);

	// Set filter on query policy
	as_policy_query qpol;
	as_policy_query_init(&qpol);
	qpol.base.filter_exp = filter_exp;

	// Create operations to read results
	as_operations ops;
	as_operations_inita(&ops, 2);
	as_operations_add_read(&ops, "id");
	as_operations_add_read(&ops, "name");
	query.ops = &ops;

	// Execute query with filter
	LOG("Executing query with vector distance filter (distance < 2.0)...");
	if (aerospike_query_foreach(as, &err, &qpol, &query,
			query_callback, NULL) != AEROSPIKE_OK) {
		LOG("Query failed: (%d) %s", err.code, err.message);
	}

	as_exp_destroy(filter_exp);
	as_query_destroy(&query);
}

//==========================================================
// Example 4: Comparing distances between two stored vectors
//

void example_vector_distance_between_bins(aerospike* as)
{
	LOG("=== Example 4: Distance Between Two Stored Vectors ===");

	as_error err;
	as_query query;
	as_query_init(&query, NAMESPACE, SET);

	// Set up a range query
	as_query_where_inita(&query, 1);
	as_query_where(&query, "id", as_integer_range(0, 5));

	// Create operations
	as_operations ops;
	as_operations_inita(&ops, 1);

	// Calculate distance between two vector bins in the same record
	as_exp_build(vector_distance_exp,
		as_exp_vector_distance(
			as_exp_bin_blob("vector-bin"),
			as_exp_bin_blob("vector-bin-2")
		)
	);

	if (!as_operations_exp_vector_distance(&ops, "bin-distance", vector_distance_exp, AS_EXP_READ_DEFAULT)) {
		LOG("Failed to add vector distance expression operation");
		as_query_destroy(&query);
		return;
	}

	// Set operations on query
	query.ops = &ops;

	// Execute query
	printf("\nExecuting query to calculate distance between two bins...\n");
	if (aerospike_query_foreach(as, &err, NULL, &query,
			query_callback, NULL) != AEROSPIKE_OK) {
		printf("Query failed: (%d) %s\n", err.code, err.message);
	}

	as_exp_destroy(vector_distance_exp);
	as_query_destroy(&query);
}

//==========================================================
// Data Setup
//

bool insert_test_data(aerospike* as)
{
	LOG("=== Inserting Test Data ===");

	as_error err;
	as_policy_write wpol;
	as_policy_write_init(&wpol);

	// Insert 5 records with different vectors
	for (uint32_t i = 0; i < 5; i++) {
		as_key key;
		as_key_init_int64(&key, NAMESPACE, SET, (int64_t)i);

		as_record rec;
		as_record_inita(&rec, 4);

		// Set integer bins
		as_record_set_int64(&rec, "id", (int64_t)i);

		char name_buf[32];
		snprintf(name_buf, sizeof(name_buf), "record-%u", i);
		as_record_set_str(&rec, "name", name_buf);

		// Create vector 1
		float vec1_data[] = {
			(float)(i + 1.0),
			(float)(i + 2.0),
			(float)(i + 3.0)
		};
		uint8_t* vec1_buffer = (uint8_t*)malloc(28);
		create_ml_vector(vec1_buffer, vec1_data, 3);
		as_bytes* vec1_bytes = as_bytes_new(0);
		as_bytes_init_wrap(vec1_bytes, vec1_buffer, 28, true);
		vec1_bytes->type = AS_BYTES_VECTOR;
		as_record_set_bytes(&rec, "vector-bin", vec1_bytes);

		// Create vector 2 (for example 4)
		float vec2_data[] = {
			(float)(i + 0.5),
			(float)(i + 1.5),
			(float)(i + 2.5)
		};
		uint8_t* vec2_buffer = (uint8_t*)malloc(28);
		create_ml_vector(vec2_buffer, vec2_data, 3);
		as_bytes* vec2_bytes = as_bytes_new(0);
		as_bytes_init_wrap(vec2_bytes, vec2_buffer, 28, true);
		vec2_bytes->type = AS_BYTES_VECTOR;
		as_record_set_bytes(&rec, "vector-bin-2", vec2_bytes);

		print_vector("Inserting", vec1_data, 3);

		if (aerospike_key_put(as, &err, &wpol, &key, &rec) != AEROSPIKE_OK) {
			LOG("Failed to insert record %u: (%d) %s", i, err.code, err.message);
			return false;
		}
	}

	LOG("Test data inserted successfully.");
	return true;
}

//==========================================================
// Main
//

int main(int argc, char* argv[])
{
	if (argc != 3) {
		LOG("Usage: %s <host> <port>", argv[0]);
		return -1;
	}

	const char* host = argv[1];
	int port = atoi(argv[2]);

	// Initialize Aerospike client
	as_config config;
	as_config_init(&config);
	as_config_add_host(&config, host, port);

	aerospike as;
	aerospike_init(&as, &config);

	as_error err;
	if (aerospike_connect(&as, &err) != AEROSPIKE_OK) {
		LOG("Failed to connect to Aerospike: %s", err.message);
		aerospike_destroy(&as);
		return -1;
	}

	LOG("Connected to Aerospike at %s:%d", host, port);

	// Insert test data
	if (!insert_test_data(&as)) {
		aerospike_close(&as, &err);
		aerospike_destroy(&as);
		return -1;
	}

	// Run examples
	example_vector_distance_expression_operation(&as);
	example_vector_distance_expression_read(&as);
	example_vector_distance_filter(&as);
	example_vector_distance_between_bins(&as);

	// Cleanup
	aerospike_close(&as, &err);
	aerospike_destroy(&as);

	LOG("=== All Examples Completed ===");
	return 0;
}

