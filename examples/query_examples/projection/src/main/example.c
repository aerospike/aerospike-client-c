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

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

#include <aerospike/as_arraylist.h>
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_query.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_index.h>
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_list_operations.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_record.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_query.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>

#include "example_utils.h"

//==========================================================
// Constants
//

const char* NAMESPACE = "test";
const char* SET = "demo1";
const char* INDEX_NAME = "test-bin-1-index";

//==========================================================
// Forward Declarations
//

bool query_operations_callback(const as_val* val, void* udata);
bool query_respond_all_ops_callback(const as_val* val, void* udata);
void example_query_with_read_operations(aerospike* as);
void example_query_with_expression_read_operations(aerospike* as);
void example_query_with_read_ops_respond_all(aerospike* as);
void example_query_with_increment_operations(aerospike* as);
bool verify_records(aerospike* p_as);
bool insert_records(aerospike* p_as);
void cleanup_example(aerospike* p_as);

//==========================================================
// Query Operations Callback
//

bool
query_operations_callback(const as_val* val, void* udata)
{
	if (!val) {
		LOG("query callback returned null - query is complete");
		return true;
	}

	// Foreground query callback: each matching row is delivered as a record.
	as_record* rec = as_record_fromval(val);
	if (!rec) {
		LOG("query callback returned non-as_record object");
		return true;
	}

	LOG("query callback returned record:");
	example_dump_record(rec);

	return true;
}

static bool
respond_all_ops_print_bin(const char* name, const as_val* value, void* udata)
{
	(void)udata;

	if (!value) {
		LOG("  bin \"%s\": (null value pointer)", name);
		return true;
	}

	if (value->type == AS_NIL) {
		LOG("  bin \"%s\": AS_NIL / UNDEF (empty op result — e.g. read of missing bin)", name);
		return true;
	}

	as_integer* ival = as_integer_fromval(value);
	if (ival) {
		LOG("  bin \"%s\": integer %" PRId64, name, as_integer_get(ival));
		return true;
	}

	as_bytes* b = as_bytes_fromval(value);
	if (b) {
		LOG("  bin \"%s\": raw particle_type=%u size=%u (often CDT error wire blob)",
			name, (unsigned)b->type, (unsigned)b->size);
		if (b->value && b->size > 0) {
			uint32_t n = b->size > 48 ? 48 : b->size;
			char hex[3 * 48 + 1];
			size_t pos = 0;
			for (uint32_t i = 0; i < n && pos + 3 <= sizeof(hex); i++) {
				pos += (size_t)snprintf(hex + pos, sizeof(hex) - pos, "%02x ", b->value[i]);
			}
			LOG("    first %u bytes: %s", n, hex);
		}
		return true;
	}

	char* s = as_val_tostring(value);
	LOG("  bin \"%s\": %s", name, s ? s : "(as_val_tostring failed)");
	if (s) {
		free(s);
	}
	return true;
}

bool
query_respond_all_ops_callback(const as_val* val, void* udata)
{
	(void)udata;

	if (!val) {
		LOG("respond_all_ops callback: null — query complete");
		return true;
	}

	as_record* rec = as_record_fromval(val);
	if (!rec) {
		LOG("respond_all_ops callback: non-record value");
		return true;
	}

	LOG("respond_all_ops row (%u bins, in op order):", (unsigned)as_record_numbins(rec));
	as_record_foreach(rec, respond_all_ops_print_bin, NULL);
	return true;
}

//==========================================================
// Query Examples
//

void
example_query_with_read_operations(aerospike* as)
{
	LOG("query with read operations");

	as_error err;

	as_query query;
	as_query_init(&query, NAMESPACE, SET);

	as_query_where_inita(&query, 1);
	as_query_where(&query, "test-bin-1", as_integer_range(0, 10));

	as_operations ops;
	as_operations_inita(&ops, 5);
	as_operations_add_read(&ops, "test-bin-1");
	as_operations_add_read(&ops, "test-bin-2");
	as_operations_add_read(&ops, "test-bin-3");
	 as_operations_add_read(&ops, "missing-bin");
	if (!as_operations_add_list_get_by_index(&ops, "list-bin", 0, AS_LIST_RETURN_VALUE)) {
		LOG("as_operations_add_list_get_by_index() failed");
		as_query_destroy(&query);
		return;
	}

	query.ops = &ops;

	if (aerospike_query_foreach(as, &err, NULL, &query, query_respond_all_ops_callback, NULL) !=
			AEROSPIKE_OK) {
		LOG("aerospike_query_foreach() returned %d - %s", err.code, err.message);
		as_query_destroy(&query);
		return;
	}

	as_query_destroy(&query);
}

void
example_query_with_select_bins_read_operations(aerospike* as)
{
	LOG("query with bin projection");

	as_error err;
	as_query query;
	as_query_init(&query, NAMESPACE, SET);

	as_query_select_inita(&query, 3);
	as_query_select(&query, "test-bin-1");
	as_query_select(&query, "test-bin-2");
	as_query_select(&query, "test-bin-3");

	as_query_where_inita(&query, 1);
	as_query_where(&query, "test-bin-1", as_integer_range(0, 10));

	if (aerospike_query_foreach(as, &err, NULL, &query, query_operations_callback, NULL) != AEROSPIKE_OK) {
		LOG("aerospike_query_foreach() returned %d - %s", err.code, err.message);
		as_query_destroy(&query);
		return;
	}

	as_query_destroy(&query);
	LOG("query with read operations completed");
}

void
example_query_with_expression_read_operations(aerospike* as)
{
	LOG("query with expression read operations");

	as_error err;

	as_query query;
	as_query_init(&query, NAMESPACE, SET);

	as_query_where_inita(&query, 1);
	as_query_where(&query, "test-bin-1", as_integer_range(0, 10));

	as_operations ops;
	as_operations_inita(&ops, 1);

	if (!as_operations_add_list_get_by_index(&ops, "list-bin", 0, AS_LIST_RETURN_VALUE)) {
		LOG("as_operations_add_list_get_by_index() failed");
		as_query_destroy(&query);
		return;
	}

	query.ops = &ops;

	if (aerospike_query_foreach(as, &err, NULL, &query, query_operations_callback, NULL) != AEROSPIKE_OK) {
		LOG("aerospike_query_foreach() returned %d - %s", err.code, err.message);
		as_query_destroy(&query);
		return;
	}

	as_query_destroy(&query);
	LOG("query with expression read operations completed");
}

void
example_query_with_read_ops_respond_all(aerospike* as)
{
	LOG("foreground query with as_query.ops (read) and policy.respond_all_ops = true");
	LOG("  ops: 3x read (existing bins), read missing bin, list-get-by-index on integer bin (type error)");
	LOG("  expect: one NIL/empty bin for missing-bin; error payload for list op on non-list");

	as_error err;
	as_policy_query qpol;
	as_policy_query_init(&qpol);
	qpol.respond_all_ops = true;

	as_query query;
	as_query_init(&query, NAMESPACE, SET);

	as_query_where_inita(&query, 1);
	as_query_where(&query, "test-bin-1", as_integer_range(0, 10));

	as_operations ops;
	as_operations_inita(&ops, 5);
	as_operations_add_read(&ops, "test-bin-1");
	as_operations_add_read(&ops, "test-bin-2");
	as_operations_add_read(&ops, "test-bin-3");
	 as_operations_add_read(&ops, "missing-bin");
	if (!as_operations_add_list_get_by_index(&ops, "list-bin", 0, AS_LIST_RETURN_VALUE)) {
		LOG("as_operations_add_list_get_by_index() failed");
		as_query_destroy(&query);
		return;
	}

	query.ops = &ops;

	if (aerospike_query_foreach(as, &err, &qpol, &query, query_respond_all_ops_callback, NULL) !=
			AEROSPIKE_OK) {
		LOG("aerospike_query_foreach() returned %d - %s", err.code, err.message);
		as_query_destroy(&query);
		return;
	}

	as_query_destroy(&query);
	LOG("query with read ops and respond_all_ops completed");
}

void
example_query_with_increment_operations(aerospike* as)
{
	LOG("query with increment via background ops, then read back with foreach");

	as_error err;
	as_query query;
	as_query_init(&query, NAMESPACE, SET);

	as_query_where_inita(&query, 1);
	as_query_where(&query, "test-bin-1", as_integer_range(0, 10));

	printf("--- Query with Increment Operations Example ---\n");

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_incr(&ops, "test-bin-3", 1000);

	query.ops = &ops;

	uint64_t query_id = 0;
	if (aerospike_query_background(as, &err, NULL, &query, &query_id) != AEROSPIKE_OK) {
		LOG("aerospike_query_background() returned %d - %s", err.code, err.message);
		as_query_destroy(&query);
		return;
	}

	if (aerospike_query_wait(as, &err, NULL, &query, query_id, 0) != AEROSPIKE_OK) {
		LOG("aerospike_query_wait() returned %d - %s", err.code, err.message);
		as_query_destroy(&query);
		return;
	}

	as_query_destroy(&query);

	as_query_init(&query, NAMESPACE, SET);
	as_query_select_inita(&query, 3);
	as_query_select(&query, "test-bin-1");
	as_query_select(&query, "test-bin-2");
	as_query_select(&query, "test-bin-3");
	as_query_where_inita(&query, 1);
	as_query_where(&query, "test-bin-1", as_integer_range(0, 10));

	LOG("records after background increment (+1000 on test-bin-3):");
	if (aerospike_query_foreach(as, &err, NULL, &query, query_operations_callback, NULL) != AEROSPIKE_OK) {
		LOG("aerospike_query_foreach() returned %d - %s", err.code, err.message);
		as_query_destroy(&query);
		return;
	}

	as_query_destroy(&query);
	LOG("query with increment operations completed");
}

//==========================================================
// Record Management
//

bool
insert_records(aerospike* p_as)
{
	// Create an as_record object with five bins (3 integer + CDT list + vector). By using
	// as_record_inita(), we won't need to destroy the record if we only set
	// bins using as_record_set_int64(), as_record_set_list(), and as_record_set_bytes().
	as_record rec;
	as_record_inita(&rec, 5);

	// Re-using rec, write records into the database such that each record's key
	// and (test-bin) value is based on the loop index.
	for (uint32_t i = 0; i < 5; i++) {
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

		// CDT list bin (heap: record owns it; replaced each iteration via set_list)
		as_arraylist* list_bin = as_arraylist_new(4, 0);
		if (!list_bin) {
			LOG("as_arraylist_new() failed");
			return false;
		}
		as_arraylist_append_int64(list_bin, (int64_t)(10 + i));
		as_arraylist_append_int64(list_bin, (int64_t)(20 + i));
		as_arraylist_append_int64(list_bin, (int64_t)(30 + i));
		as_record_set_list(&rec, "list-bin", (as_list*)list_bin);

		// Create and insert a vector bin in ML vector format
		// Format: magic(4) + version(4) + count(4) + type(4) + data
		uint8_t* vector_data = (uint8_t*)malloc(28); // 16 bytes header + 12 bytes data (3 floats)
		uint8_t* p = vector_data;

		// Magic number: "VECT" (0x56454354) in native byte order
		*(uint32_t*)p = 0x56454354;
		p += 4;

		// Version: 1 in native byte order
		*(uint32_t*)p = 1;
		p += 4;

		// Element count: 3 in native byte order
		*(uint32_t*)p = 3;
		p += 4;

		// Element type: 1 (FLOAT32) in native byte order
		*(uint32_t*)p = 1;
		p += 4;

		// Vector data: different for each record
		float vector_floats[] = {
			(float)(i + 1.0),  // x component
			(float)(i + 2.0),  // y component
			(float)(i + 3.0)   // z component
		};
		memcpy(p, vector_floats, 12);

		// Create as_bytes for the vector
		as_bytes* vector_bytes = as_bytes_new(0);
		as_bytes_init_wrap(vector_bytes, vector_data, 28, true); // true = free on destroy
		vector_bytes->type = AS_BYTES_VECTOR;

		// Store the vector bin
		as_record_set_bytes(&rec, "vector-bin", vector_bytes);

		printf("Inserting record %u with bins [%d, %d, %d], list [%d,%d,%d], vector [%.1f, %.1f, %.1f]\n",
			   i, (int)i, (int)(100 + i), (int)(1000 + i),
			   (int)(10 + i), (int)(20 + i), (int)(30 + i),
			   vector_floats[0], vector_floats[1], vector_floats[2]);

		// Write a record to the database.
		if (aerospike_key_put(p_as, &err, &wpol, &key, &rec) != AEROSPIKE_OK) {
			LOG("aerospike_key_put() returned %d - %s", err.code, err.message);
			return false;
		}
	}

	LOG("insert succeeded");

	return true;
}

bool
verify_records(aerospike* p_as)
{
	LOG("Verifying inserted records...");

	for (uint32_t i = 0; i < 5; i++) {
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

		if (!p_rec) {
			LOG("Record %u not found", i);
			return false;
		}

		printf("\n--- Verifying Record %u ---\n", i);

		// Verify integer bins
		as_bin_value* bin1 = as_record_get(p_rec, "test-bin-1");
		as_bin_value* bin2 = as_record_get(p_rec, "test-bin-2");
		as_bin_value* bin3 = as_record_get(p_rec, "test-bin-3");
		as_list* lst = as_record_get_list(p_rec, "list-bin");
		as_bin_value* vector_bin = as_record_get(p_rec, "vector-bin");

		if (!bin1 || !bin2 || !bin3 || !lst || !vector_bin) {
			LOG("Missing bins in record %u", i);
			as_record_destroy(p_rec);
			return false;
		}

		// Verify integer values
		int64_t val1 = as_integer_get(&bin1->integer);
		int64_t val2 = as_integer_get(&bin2->integer);
		int64_t val3 = as_integer_get(&bin3->integer);

		printf("Integer bins: %" PRId64 ", %" PRId64 ", %" PRId64 "\n", val1, val2, val3);

		if (val1 != (int64_t)i || val2 != (int64_t)(100 + i) || val3 != (int64_t)(1000 + i)) {
			LOG("Integer bin values don't match expected values for record %u", i);
			as_record_destroy(p_rec);
			return false;
		}

		if (as_list_size(lst) != 3) {
			LOG("list-bin wrong size in record %u", i);
			as_record_destroy(p_rec);
			return false;
		}
		if (as_list_get_int64(lst, 0) != (int64_t)(10 + i) ||
			as_list_get_int64(lst, 1) != (int64_t)(20 + i) ||
			as_list_get_int64(lst, 2) != (int64_t)(30 + i)) {
			LOG("list-bin values don't match for record %u", i);
			as_record_destroy(p_rec);
			return false;
		}
		printf("List bin: [%" PRId64 ", %" PRId64 ", %" PRId64 "]\n",
			   as_list_get_int64(lst, 0), as_list_get_int64(lst, 1), as_list_get_int64(lst, 2));

		// Verify vector bin
		as_bytes* vector_bytes = &vector_bin->bytes;
		if (!vector_bytes || !vector_bytes->value) {
			LOG("Vector bin is not bytes type for record %u", i);
			as_record_destroy(p_rec);
			return false;
		}

		printf("Vector bin: type=%d, size=%u bytes\n", vector_bytes->type, vector_bytes->size);

		// Verify vector format (basic check)
		if (vector_bytes->size >= 16) {
			uint32_t* header = (uint32_t*)vector_bytes->value;
			uint32_t magic = header[0];
			uint32_t version = header[1];
			uint32_t count = header[2];
			uint32_t type = header[3];

			printf("Vector header: magic=0x%08x, version=%u, count=%u, type=%u\n",
				   magic, version, count, type);

			if (magic == 0x56454354 && version == 1 && count == 3 && type == 1) {
				float* data = (float*)(vector_bytes->value + 16);
				printf("Vector data: [%.1f, %.1f, %.1f]\n", data[0], data[1], data[2]);
				printf("Expected:    [%.1f, %.1f, %.1f]\n",
					   (float)(i + 1.0), (float)(i + 2.0), (float)(i + 3.0));
			} else {
				LOG("Invalid vector header for record %u", i);
				as_record_destroy(p_rec);
				return false;
			}
		} else {
			LOG("Vector bin too small for record %u", i);
			as_record_destroy(p_rec);
			return false;
		}

		printf("✓ Record %u verified successfully\n", i);

		// Cleanup
		as_record_destroy(p_rec);
	}

	LOG("All records verified successfully!");
	return true;
}

void
cleanup_example(aerospike* p_as)
{
	as_error err;

	// Drop the index
	aerospike_index_remove(p_as, &err, NULL, NAMESPACE, INDEX_NAME);

	// Clean up records
	for (uint32_t i = 0; i < 5; i++) {
		as_key key;
		as_key_init_int64(&key, NAMESPACE, SET, (int64_t)i);
		aerospike_key_remove(p_as, &err, NULL, &key);
	}

	LOG("Example cleanup completed.");
}

//==========================================================
// Main
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

	// Start clean (uses -n / -s from CLI; use -n test -s demo1 to match this example).
	example_remove_test_records(&as);

	insert_records(&as);

	printf("Records inserted\n");

	// Create index for queries
	as_error err;
	as_index_task task;
	as_status istat = aerospike_index_create(&as, &err, &task, NULL, NAMESPACE, SET,
			"test-bin-1", INDEX_NAME, AS_INDEX_NUMERIC);
	switch (istat) {
	case AEROSPIKE_OK:
		if (aerospike_index_create_wait(&err, &task, 0) != AEROSPIKE_OK) {
			LOG("aerospike_index_create_wait() returned %d - %s", err.code, err.message);
			cleanup_example(&as);
			aerospike_close(&as, &err);
			return -1;
		}
		break;
	case AEROSPIKE_ERR_INDEX_FOUND:
		LOG("index already exists");
		break;
	default:
		LOG("aerospike_index_create() returned %d - %s", err.code, err.message);
		cleanup_example(&as);
		aerospike_close(&as, &err);
		return -1;
	}

	// Verify inserted records
	if (!verify_records(&as)) {
		LOG("Record verification failed!");
		cleanup_example(&as);
		aerospike_close(&as, &err);
		return -1;
	}

	// Run examples
	example_query_with_read_operations(&as);
	example_query_with_select_bins_read_operations(&as);
	example_query_with_expression_read_operations(&as);
	example_query_with_read_ops_respond_all(&as);
	example_query_with_increment_operations(&as);

	// Cleanup and disconnect
	cleanup_example(&as);
	aerospike_close(&as, &err);
	aerospike_destroy(&as);

	LOG("query projection example successfully completed");

	return 0;
}
