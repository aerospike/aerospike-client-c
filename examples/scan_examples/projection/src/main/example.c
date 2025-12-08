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
// Includes
//

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <math.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_scan.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_record.h>
#include <aerospike/as_scan.h>
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
const char* SET = "demo1";

//==========================================================
// Forward Declarations
//

bool scan_operations_callback(const as_val* val, void* udata);
void example_scan_with_read_operations(aerospike* as);
void example_scan_with_expression_read_operations(aerospike* as);
bool verify_records(aerospike* p_as);
void example_scan_with_write_operations(aerospike* as);
void example_scan_with_vector_distance_operations(aerospike* as);
void cleanup_example(aerospike* as);
bool insert_records(aerospike* p_as);

//==========================================================
// Scan Operations Example
//

int
main(int argc, char* argv[])
{
	// Parse command line arguments.
	if (argc != 3) {
		printf("Usage: %s <host> <port>\n", argv[0]);
		return -1;
	}

	const char* host = argv[1];
	int port = atoi(argv[2]);

	// Initialize Aerospike client.
	as_config config;
	as_config_init(&config);
	as_config_add_host(&config, host, port);

	aerospike as;
	aerospike_init(&as, &config);

	as_error err;
	if (aerospike_connect(&as, &err) != AEROSPIKE_OK) {
		printf("Failed to connect to Aerospike: %s\n", err.message);
		aerospike_destroy(&as);
		return -1;
	}

	printf("Connected to Aerospike at %s:%d\n", host, port);

	insert_records(&as);

	printf("Records inserted\n");

	// Verify inserted records
	if (! verify_records(&as)) {
		LOG("Record verification failed!");
		cleanup_example(&as);
		aerospike_close(&as, &err);
		return -1;
	}

	// Run examples
	// example_scan_with_read_operations(&as);
	// example_scan_with_expression_read_operations(&as);
	example_scan_with_vector_distance_operations(&as);

	// Cleanup and disconnect
	cleanup_example(&as);
	aerospike_close(&as, &err);
	aerospike_destroy(&as);

	printf("Scan operations example completed successfully.\n");
	return 0;
}

//==========================================================
// Example Functions
//

void
example_scan_with_read_operations(aerospike* as)
{
	printf("\n--- Scan with Read Operations Example ---\n");

	as_error err;
	as_scan scan;
	as_scan_init(&scan, NAMESPACE, SET);

	// Create operations to read specific bins and perform calculations
	as_operations ops;
	as_operations_inita(&ops, 3);

	// Read existing bins
	as_operations_add_read(&ops, "test-bin-1");
	as_operations_add_read(&ops, "test-bin-2");
	as_operations_add_read(&ops, "test-bin-3");

	// Set operations on scan
	scan.ops = &ops;

	printf("Executing foreground scan with read operations...\n");

	// Execute scan with operations
	as_status status = aerospike_scan_foreach(as, &err, NULL, &scan, scan_operations_callback, NULL);

	if (status != AEROSPIKE_OK) {
		printf("Scan failed: (%d) %s\n", err.code, err.message);
	} else {
		printf("Scan with read operations completed successfully.\n");
	}

	as_operations_destroy(&ops);
	as_scan_destroy(&scan);
}

void
example_scan_with_expression_read_operations(aerospike* as)
{
	printf("\n--- Scan with Expression Read Operations Example ---\n");

	as_error err;
	as_scan scan;
	as_scan_init(&scan, NAMESPACE, SET);

	// Create operations using expressions to read bin values
	as_operations ops;
	as_operations_inita(&ops, 3);

	// Create expressions to read bin values
	// Expression to read test-bin-1
	as_exp_build(exp1, as_exp_bin_int("test-bin-1"));
	if (!as_operations_exp_read(&ops, "result-bin-1", exp1, AS_EXP_READ_DEFAULT)) {
		printf("Failed to add expression read operation for test-bin-1\n");
	}

	// Expression to read test-bin-2
	as_exp_build(exp2, as_exp_bin_int("test-bin-2"));
	if (!as_operations_exp_read(&ops, "result-bin-2", exp2, AS_EXP_READ_DEFAULT)) {
		printf("Failed to add expression read operation for test-bin-2\n");
	}

	// Expression to read test-bin-3
	as_exp_build(exp3, as_exp_bin_int("test-bin-3"));
	if (!as_operations_exp_read(&ops, "result-bin-3", exp3, AS_EXP_READ_DEFAULT)) {
		printf("Failed to add expression read operation for test-bin-3\n");
	}

	// Set operations on scan
	scan.ops = &ops;

	printf("Executing scan with expression read operations...\n");

	// Execute scan with operations
	as_status status = aerospike_scan_foreach(as, &err, NULL, &scan, scan_operations_callback, NULL);

	if (status != AEROSPIKE_OK) {
		printf("Scan failed: (%d) %s\n", err.code, err.message);
	} else {
		printf("Scan with expression read operations completed successfully.\n");
	}

	// Clean up expressions
	as_exp_destroy(exp1);
	as_exp_destroy(exp2);
	as_exp_destroy(exp3);

	as_operations_destroy(&ops);
	as_scan_destroy(&scan);
}

void
example_scan_with_write_operations(aerospike* as)
{
	printf("\n--- Scan with Write Operations Example ---\n");

	as_error err;
	as_scan scan;
	as_scan_init(&scan, NAMESPACE, SET);

	// Create operations to modify records
	as_operations ops;
	as_operations_inita(&ops, 2);

	// Increment a counter bin
	as_operations_add_incr(&ops, "counter", 1);

	// Set a timestamp bin
	as_operations_add_write_int64(&ops, "last_scanned", (int64_t)time(NULL));

	// Set operations on scan
	scan.ops = &ops;

	printf("Executing background scan with write operations...\n");

	// Execute background scan with operations
	uint64_t scan_id = 0;
	as_status status = aerospike_scan_background(as, &err, NULL, &scan, &scan_id);

	if (status != AEROSPIKE_OK) {
		printf("Background scan failed: (%d) %s\n", err.code, err.message);
	} else {
		printf("Background scan with write operations started successfully (ID: %lu).\n", scan_id);

		// Wait for scan to complete
		printf("Waiting for background scan to complete...\n");
		status = aerospike_scan_wait(as, &err, NULL, scan_id, 1000);

		if (status == AEROSPIKE_OK) {
			printf("Background scan completed successfully.\n");
		} else {
			printf("Error waiting for scan: (%d) %s\n", err.code, err.message);
		}
	}

	as_operations_destroy(&ops);
	as_scan_destroy(&scan);
}

void
cleanup_example(aerospike* as)
{
	// This function could be used to clean up any test data
	// For this example, we'll leave the data as-is
	printf("\nExample cleanup completed.\n");
}

//==========================================================
// Callback Functions
//

bool
scan_operations_callback(const as_val* p_val, void* udata)
{
	if (! p_val) {
		LOG("scan callback returned null - scan is complete");
		return true;
	}

	// The scan didn't use a UDF, so the as_val object should be an as_record.
	as_record* p_rec = as_record_fromval(p_val);

	if (! p_rec) {
		LOG("scan callback returned non-as_record object");
		return true;
	}

	LOG("scan callback returned record:");
	example_dump_record(p_rec);

	return true;
}


//==========================================================
// Helper Functions
//

bool
insert_records(aerospike* p_as)
{
	// Create an as_record object with up to four bins (3 integer + 1 vector). By using
	// as_record_inita(), we won't need to destroy the record if we only set
	// bins using as_record_set_int64() and as_record_set_bytes().
	as_record rec;
	as_record_inita(&rec, 4);

	// Re-using rec, write records into the database such that each record's key
	// and (test-bin) value is based on the loop index.
	for (uint32_t i = 0; i < 2; i++) {
		as_error err;

		// Set up a default as_policy_write object.
		as_policy_write wpol;
		as_policy_write_init(&wpol);

		// No need to destroy a stack as_key object, if we only use
		// as_key_init_int64().
		as_key key;
		as_key_init_int64(&key, NAMESPACE, SET, (int64_t)i);

		// In general it's ok to reset a bin value - all as_record_set_... calls
		// destroy any previous value.

		// Write three integer bins
		as_record_set_int64(&rec, "test-bin-1", (int64_t)i);
		as_record_set_int64(&rec, "test-bin-2", (int64_t)(100 + i));
		as_record_set_int64(&rec, "test-bin-3", (int64_t)(1000 + i));

		// Create and insert a vector bin
		// Create a sample 3D vector with different values for each record
		float vector_data[] = {
			(float)(i + 1.0),      // x component
			(float)(i + 2.0),      // y component
			(float)(i + 3.0)       // z component
		};

		as_vector vector;
		as_status status = as_ml_vector_init_float32(&vector, vector_data, 3);

		if (status != AEROSPIKE_OK) {
			LOG("Failed to initialize ML vector: %d", status);
			return false;
		}

		// Serialize the ML vector for storage as bytes (with ML vector header)
		as_bytes* vector_bytes = as_bytes_new(0);

		status = as_ml_vector_serialize(&vector, AS_ML_VECTOR_FLOAT32, vector_bytes);

		if (status != AEROSPIKE_OK) {
			LOG("Failed to serialize ML vector: %d", status);
			as_vector_destroy(&vector);
			as_bytes_destroy(vector_bytes);
			return false;
		}

		// Store as bytes with ML vector format (contains magic number, version, type, data)
		as_record_set_bytes(&rec, "vector-bin", vector_bytes);

		printf("Inserting record %u with vector [%.1f, %.1f, %.1f]\n",
			   i, vector_data[0], vector_data[1], vector_data[2]);

		// Cleanup vector resources
		as_vector_destroy(&vector);
		// Note: vector_bytes memory will be managed by the record

		// If we want the key to be returned in the scan callback, we must
		// store it with the record in the database. AS_POLICY_KEY_SEND
		// causes the key to be stored.

		// Write a record to the database.
		if (aerospike_key_put(p_as, &err, &wpol, &key, &rec) != AEROSPIKE_OK) {
			LOG("aerospike_key_put() returned %d - %s", err.code, err.message);
			return false;
		}

		// Note: vector_bytes will be cleaned up by as_record when it's destroyed
	}

	LOG("insert succeeded");

	return true;
}

//==========================================================
// Record Verification
//

bool
verify_records(aerospike* p_as)
{
	LOG("Verifying inserted records...");

	for (uint32_t i = 0; i < 2; i++) {
		as_error err;
		as_key key;
		as_record* p_rec = NULL;

		// Initialize key for record retrieval
		as_key_init_int64(&key, NAMESPACE, SET, (int64_t)i);

		// Read the record from the database
		if (aerospike_key_get(p_as, &err, NULL, &key, &p_rec) != AEROSPIKE_OK) {
			LOG("Failed to retrieve record %u: %d - %s", i, err.code, err.message);
			return false;
		}

		if (! p_rec) {
			LOG("Record %u not found", i);
			return false;
		}

		printf("\n--- Verifying Record %u ---\n", i);

		// Verify integer bins
		as_bin_value* bin1 = as_record_get(p_rec, "test-bin-1");
		as_bin_value* bin2 = as_record_get(p_rec, "test-bin-2");
		as_bin_value* bin3 = as_record_get(p_rec, "test-bin-3");
		as_bin_value* vector_bin = as_record_get(p_rec, "vector-bin");

		if (! bin1 || ! bin2 || ! bin3 || ! vector_bin) {
			LOG("Missing bins in record %u", i);
			as_record_destroy(p_rec);
			return false;
		}

		// Verify integer values
		int64_t val1 = as_integer_get(&bin1->integer);
		int64_t val2 = as_integer_get(&bin2->integer);
		int64_t val3 = as_integer_get(&bin3->integer);

		printf("Integer bins: %ld, %ld, %ld\n", val1, val2, val3);

		if (val1 != (int64_t)i || val2 != (int64_t)(100 + i) || val3 != (int64_t)(1000 + i)) {
			LOG("Integer bin values don't match expected values for record %u", i);
			as_record_destroy(p_rec);
			return false;
		}

		as_bytes* vector_bytes = &vector_bin->bytes;

		if (! vector_bytes || ! vector_bytes->value) {
			LOG("Vector bin is not bytes type for record %u", i);
			as_record_destroy(p_rec);
			return false;
		}

		printf("Vector bin: type=%d, size=%u bytes\n", vector_bytes->type, vector_bytes->size);

		// Debug: Show first few bytes of vector data
		printf("First 16 bytes: ");
		for (int j = 0; j < 16 && j < vector_bytes->size; j++) {
			printf("%02x ", vector_bytes->value[j]);
		}
		printf("\n");

		// Deserialize and verify vector data
		as_vector retrieved_vector;
		as_ml_vector_element_type element_type;
		as_status status = as_ml_vector_deserialize(vector_bytes, &retrieved_vector, &element_type);

		if (status != AEROSPIKE_OK) {
			LOG("Failed to deserialize vector for record %u: %d", i, status);
			as_record_destroy(p_rec);
			return false;
		}

		printf("Deserialized vector: type=%d, count=%u, element_size=%u\n",
			   element_type, retrieved_vector.size, (uint32_t)retrieved_vector.item_size);

		// Verify vector contents
		if (element_type != AS_ML_VECTOR_FLOAT32 || retrieved_vector.size != 3) {
			LOG("Vector metadata doesn't match for record %u", i);
			as_vector_destroy(&retrieved_vector);
			as_record_destroy(p_rec);
			return false;
		}

		float* vector_data = (float*)retrieved_vector.list;
		float expected_x = (float)(i + 1.0);
		float expected_y = (float)(i + 2.0);
		float expected_z = (float)(i + 3.0);

		printf("Vector data: [%.1f, %.1f, %.1f]\n", vector_data[0], vector_data[1], vector_data[2]);
		printf("Expected:    [%.1f, %.1f, %.1f]\n", expected_x, expected_y, expected_z);

		if (fabs(vector_data[0] - expected_x) > 0.001 ||
			fabs(vector_data[1] - expected_y) > 0.001 ||
			fabs(vector_data[2] - expected_z) > 0.001) {
			LOG("Vector data doesn't match expected values for record %u", i);
			as_vector_destroy(&retrieved_vector);
			as_record_destroy(p_rec);
			return false;
		}

		printf("✓ Record %u verified successfully\n", i);

		// Cleanup
		as_vector_destroy(&retrieved_vector);
		as_record_destroy(p_rec);
	}

	LOG("All records verified successfully!");
	return true;
}

void
example_scan_with_vector_distance_operations(aerospike* as)
{
	printf("\n--- Scan with Vector Distance Operations Example ---\n");

	as_error err;
	as_scan scan;
	as_scan_init(&scan, NAMESPACE, SET);

	// Create operations with vector distance calculation
	as_operations ops;
	as_operations_inita(&ops, 1);

	// Create a sample query vector for distance calculation
	// This will calculate distances from stored vectors to this query vector
	float query_data[] = {2.5f, 3.5f, 4.5f};
	as_vector query_vector;

	// Initialize the ML vector using the existing utilities
	as_status status = as_ml_vector_init_float32(&query_vector, query_data, 3);
	if (status != AEROSPIKE_OK) {
		printf("Failed to initialize ML vector: %d\n", status);
		as_operations_destroy(&ops);
		as_scan_destroy(&scan);
		return;
	}

	// Add vector distance operation
	if (! as_operations_add_vector_distance(&ops, "vector-bin", &query_vector, AS_ML_VECTOR_FLOAT32)) {
		printf("Failed to add vector distance operation\n");
		as_vector_destroy(&query_vector);
		as_operations_destroy(&ops);
		as_scan_destroy(&scan);
		return;
	}

	// Set operations on scan
	scan.ops = &ops;

	// Execute scan with operations
	status = aerospike_scan_foreach(as, &err, NULL, &scan, scan_operations_callback, NULL);

	if (status != AEROSPIKE_OK) {
		printf("Scan failed: (%d) %s\n", err.code, err.message);
	} else {
		printf("Scan with vector distance operations completed successfully.\n");
	}

	// Cleanup
	as_vector_destroy(&query_vector);
	as_operations_destroy(&ops);
	as_scan_destroy(&scan);
}
