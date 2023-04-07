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

//==========================================================
// Forward Declarations
//

bool query_cb(const as_val* p_val, void* udata);
void cleanup(aerospike* p_as);
bool insert_records(aerospike* p_as);
as_status query_pages(aerospike* p_as, as_error* err);
as_status query_terminate_resume(aerospike* p_as, as_error* err);
as_status query_terminate_resume_with_serialization(aerospike* p_as, as_error* err);

//==========================================================
// SIMPLE QUERY Examples
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
	example_remove_index(&as, TEST_INDEX_NAME);

	// Create a numeric secondary index on test-bin.
	if (! example_create_integer_index(&as, "test-bin", TEST_INDEX_NAME)) {
		cleanup(&as);
		exit(-1);
	}

	if (! insert_records(&as)) {
		cleanup(&as);
		exit(-1);
	}

	if (! example_read_test_records(&as)) {
		cleanup(&as);
		exit(-1);
	}

	as_error err;

	// Create an as_query object.
	as_query query;
	as_query_init(&query, g_namespace, g_set);

	// Generate an as_query.where condition. Note that as_query_destroy() takes
	// care of destroying all the query's member objects if necessary. However
	// using as_query_where_inita() does avoid internal heap usage.
	as_query_where_inita(&query, 1);
	as_query_where(&query, "test-bin", as_integer_equals(7));

	LOG("executing query: where test-bin = 7");

	// Execute the query. This call blocks - callbacks are made in the scope of
	// this call.
	if (aerospike_query_foreach(&as, &err, NULL, &query, query_cb, NULL) !=
			AEROSPIKE_OK) {
		LOG("aerospike_query_foreach() returned %d - %s", err.code,
				err.message);
		as_query_destroy(&query);
		cleanup(&as);
		exit(-1);
	}

	LOG("query executed");

	as_query_destroy(&query);

	// Run query pages.
	if (query_pages(&as, &err) != AEROSPIKE_OK) {
		LOG("query_pages() returned %d - %s", err.code, err.message);
		cleanup(&as);
		exit(-1);
	}

	// Run query terminate/resume.
	if (query_terminate_resume(&as, &err) != AEROSPIKE_OK) {
		LOG("query_terminate_resume() returned %d - %s", err.code, err.message);
		cleanup(&as);
		exit(-1);
	}

	// Run query terminate/resume with serialization.
	if (query_terminate_resume_with_serialization(&as, &err) != AEROSPIKE_OK) {
		LOG("query_terminate_resume_with_serialization() returned %d - %s", err.code, err.message);
		cleanup(&as);
		exit(-1);
	}

	// Cleanup and disconnect from the database cluster.
	cleanup(&as);

	LOG("simple query example successfully completed");
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

	LOG("query callback returned record:");
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
	// Create an as_record object with one (integer value) bin. By using
	// as_record_inita(), we won't need to destroy the record if we only set
	// bins using as_record_set_int64().
	as_record rec;
	as_record_inita(&rec, 1);

	// Re-using rec, write records into the database such that each record's key
	// and (test-bin) value is based on the loop index.
	for (uint32_t i = 0; i < g_n_keys; i++) {
		as_error err;

		// No need to destroy a stack as_key object, if we only use
		// as_key_init_int64().
		as_key key;
		as_key_init_int64(&key, g_namespace, g_set, (int64_t)i);

		// In general it's ok to reset a bin value - all as_record_set_... calls
		// destroy any previous value.
		as_record_set_int64(&rec, "test-bin", (int64_t)i);

		// Write a record to the database.
		if (aerospike_key_put(p_as, &err, NULL, &key, &rec) != AEROSPIKE_OK) {
			LOG("aerospike_key_put() returned %d - %s", err.code, err.message);
			return false;
		}
	}

	LOG("insert succeeded");

	return true;
}

//==========================================================
// Query Pages
//

struct counter {
	uint32_t count;
	uint32_t max;
};

static bool
pquery_cb2(const as_val* val, void* udata)
{
	if (! val) {
		// Query complete.
		return true;
	}

	struct counter* c = udata;
	as_incr_uint32(&c->count);
	return true;
}

static as_status
insert_records_for_query_page(aerospike* p_as, as_error* err, const char* set, uint32_t size)
{
	// Write records that belong to the specified partition.
	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, "bin1", 55);

	as_status status;

	for (uint32_t i = 0; i < size; i++) {
		as_key key;
		as_key_init_int64(&key, g_namespace, set, (int64_t)i);

		status = as_key_set_digest(err, &key);

		if (status != AEROSPIKE_OK) {
			return status;
		}

		status = aerospike_key_put(p_as, err, NULL, &key, &rec);

		if (status != AEROSPIKE_OK) {
			return status;
		}
	}
	return AEROSPIKE_OK;
}

as_status
query_pages(aerospike* p_as, as_error* err)
{
	const char* set = "querypage";
	uint32_t total_size = 190;
	uint32_t page_size = 100;

	LOG("write records for query pagination");
	as_status status = insert_records_for_query_page(p_as, err, set, total_size);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	LOG("records written: %u", total_size);

	struct counter c;
	c.count = 0;

	as_query query;
	as_query_init(&query, g_namespace, set);
	as_query_set_paginate(&query, true);
	query.max_records = page_size;

	// Query 3 pages of records.
	for (int i = 1; i <= 3 && ! as_query_is_done(&query); i++) {
		c.count = 0;

		LOG("query page: %d", i);
		status = aerospike_query_foreach(p_as, err, NULL, &query, pquery_cb2, &c);

		if (status != AEROSPIKE_OK) {
			as_query_destroy(&query);
			return status;
		}
		LOG("records returned: %u", c.count);
	}

	as_query_destroy(&query);
	return AEROSPIKE_OK;
}

//==========================================================
// Query Terminate and Resume
//

struct page_counter {
	as_spinlock lock;
	uint32_t count;
	uint32_t max;
};

static void
page_counter_init(struct page_counter* c, uint32_t max)
{
	as_spinlock_init(&c->lock);
	c->count = 0;
	c->max = max;
}

static void
page_counter_reset(struct page_counter* c)
{
	c->count = 0;
}

static bool
query_terminate_cb(const as_val* val, void* udata)
{
	if (! val) {
		// Query complete.
		return true;
	}

	struct page_counter* c = udata;
	bool rv;

	as_spinlock_lock(&c->lock);
	if (c->count < c->max) {
		c->count++;
		rv = true;
	}
	else {
		// Since we are terminating the query here, the query last digest
		// will not be set and the current record will be returned again
		// if the query resumes at a later time.
		rv = false;
	}
	as_spinlock_unlock(&c->lock);
	return rv;
}

static bool
query_resume_cb(const as_val* val, void* udata)
{
	if (! val) {
		// Query complete.
		return true;
	}

	struct page_counter* c = udata;
	as_incr_uint32(&c->count);
	return true;
}

as_status
query_terminate_resume(aerospike* p_as, as_error* err)
{
	const char* set = "queryresume";
	uint32_t total_size = 200;

	LOG("write records for query terminate/resume");
	as_status status = insert_records_for_query_page(p_as, err, set, total_size);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	LOG("records written: %u", total_size);
	LOG("start query terminate");

	struct page_counter c;
	page_counter_init(&c, 50);

	as_query query;
	as_query_init(&query, g_namespace, set);
	as_query_set_paginate(&query, true);

	// Start query. Query will be terminated early in callback.
	status = aerospike_query_foreach(p_as, err, NULL, &query, query_terminate_cb, &c);

	if (status != AEROSPIKE_OK) {
		as_query_destroy(&query);
		return status;
	}

	LOG("terminate records returned: %u", c.count);
	LOG("start query resume");

	// Store completion status of all partitions.
	as_partitions_status* parts_all = as_partitions_status_reserve(query.parts_all);

	// Destroy query
	as_query_destroy(&query);

	// Resume query using new query instance.
	as_query query_resume;
	as_query_init(&query_resume, g_namespace, set);

	// Use partition filter to set parts_all.
	// Calling as_query_set_partitions(&query_resume, parts_all) works too.
	// as_partition_filter_set_partitions() is just a wrapper for eventually calling
	// as_query_set_partitions().
	as_partition_filter pf;
	as_partition_filter_set_partitions(&pf, parts_all);

	page_counter_reset(&c);

	status = aerospike_query_partitions(p_as, err, NULL, &query_resume, &pf, query_resume_cb, &c);

	LOG("resume records returned: %u", c.count);

	as_partitions_status_release(parts_all);
	as_query_destroy(&query_resume);
	return status;
}

#define cmp_error() \
	printf("Line %d\n", __LINE__);\
	return false;

static bool
val_compare(as_val* v1, as_val* v2)
{
	char* s1 = as_val_tostring(v1);
	char* s2 = as_val_tostring(v2);
	bool rv = strcmp(s1, s2);
	cf_free(s1);
	cf_free(s2);
	return rv == 0;
}

static bool
query_compare(as_query* q1, as_query* q2) {
	if (q1->_free != q2->_free) {
		cmp_error();
	}

	if (strcmp(q1->ns, q2->ns) != 0) {
		cmp_error();
	}

	if (strcmp(q1->set, q2->set) != 0) {
		cmp_error();
	}

	if (q1->select._free != q2->select._free) {
		cmp_error();
	}

	if (q1->select.capacity != q2->select.capacity) {
		cmp_error();
	}

	if (q1->select.size != q2->select.size) {
		cmp_error();
	}

	for (uint16_t i = 0; i < q1->select.size; i++) {
		if (strcmp(q1->select.entries[i], q2->select.entries[i]) != 0) {
			cmp_error();
		}
	}

	if (q1->where._free != q2->where._free) {
		cmp_error();
	}

	if (q1->where.capacity != q2->where.capacity) {
		cmp_error();
	}

	if (q1->select.size != q2->select.size) {
		cmp_error();
	}

	for (uint16_t i = 0; i < q1->where.size; i++) {
		as_predicate* p1 = &q1->where.entries[i];
		as_predicate* p2 = &q2->where.entries[i];

		if (strcmp(p1->bin, p2->bin) != 0) {
			cmp_error();
		}

		if (p1->ctx_free != p2->ctx_free) {
			cmp_error();
		}

		if (p1->ctx_size != p2->ctx_size) {
			cmp_error();
		}

		as_cdt_ctx* c1 = p1->ctx;
		as_cdt_ctx* c2 = p2->ctx;

		if (c1->list.size != c2->list.size) {
			cmp_error();
		}

		for (uint32_t j = 0; j < c1->list.size; j++) {
			as_cdt_ctx_item* ci1 = as_vector_get(&c1->list, j);
			as_cdt_ctx_item* ci2 = as_vector_get(&c2->list, j);

			if (ci1->type != ci2->type) {
				cmp_error();
			}

			if (ci1->type & AS_CDT_CTX_VALUE) {
				if (! val_compare(ci1->val.pval, ci2->val.pval)) {
					cmp_error();
				}
			}
			else {
				if (ci1->val.ival != ci2->val.ival) {
					cmp_error();
				}
			}
		}

		if (p1->type != p2->type) {
			cmp_error();
		}

		if (p1->dtype != p2->dtype) {
			cmp_error();
		}

		if (p1->itype != p2->itype) {
			cmp_error();
		}

		switch(p1->type) {
			case AS_PREDICATE_EQUAL:
				if (p1->dtype == AS_INDEX_STRING) {
					if (strcmp(p1->value.string_val.string, p2->value.string_val.string) != 0) {
						cmp_error();
					}
				}
				else if (p1->dtype == AS_INDEX_NUMERIC) {
					if (p1->value.integer != p2->value.integer) {
						cmp_error();
					}
				}
				break;

			case AS_PREDICATE_RANGE:
				if (p1->dtype == AS_INDEX_NUMERIC) {
					if (p1->value.integer_range.min != p2->value.integer_range.min) {
						cmp_error();
					}

					if (p1->value.integer_range.max != p2->value.integer_range.max) {
						cmp_error();
					}
				}
				else if (p1->dtype == AS_INDEX_GEO2DSPHERE) {
					if (strcmp(p1->value.string_val.string, p2->value.string_val.string) != 0) {
						cmp_error();
					}
				}
				break;
		}
	}

	if (q1->apply._free != q2->apply._free) {
		cmp_error();
	}

	if (strcmp(q1->apply.module, q2->apply.module) != 0) {
		cmp_error();
	}

	if (strcmp(q1->apply.function, q2->apply.function) != 0) {
		cmp_error();
	}

	if (q1->apply.arglist != q2->apply.arglist) {
		if (! val_compare((as_val*)q1->apply.arglist, (as_val*)q2->apply.arglist)) {
			cmp_error();
		}
	}

	if (q1->ops != q2->ops) {
		if (q1->ops->_free != q2->ops->_free) {
			cmp_error();
		}

		if (q1->ops->gen != q2->ops->gen) {
			cmp_error();
		}

		if (q1->ops->ttl != q2->ops->ttl) {
			cmp_error();
		}

		if (q1->ops->binops.size != q2->ops->binops.size) {
			cmp_error();
		}

		for (uint16_t i = 0; i < q1->ops->binops.size; i++) {
			as_binop* op1 = &q1->ops->binops.entries[i];
			as_binop* op2 = &q2->ops->binops.entries[i];

			if (op1->op != op2->op) {
				cmp_error();
			}

			if (strcmp(op1->bin.name, op2->bin.name) != 0) {
				cmp_error();
			}

			if (op1->bin.valuep != op2->bin.valuep) {
				if (! val_compare((as_val*)op1->bin.valuep, (as_val*)op2->bin.valuep)) {
					cmp_error();
				}
			}
		}
	}

	if (q1->parts_all != q2->parts_all) {
		as_partitions_status* p1 = q1->parts_all;
		as_partitions_status* p2 = q2->parts_all;

		if (p1->ref_count != p2->ref_count) {
			cmp_error();
		}

		if (p1->part_begin != p2->part_begin) {
			cmp_error();
		}

		if (p1->part_count != p2->part_count) {
			cmp_error();
		}

		if (p1->done != p2->done) {
			cmp_error();
		}

		for (uint16_t i = 0; i < p1->part_count; i++) {
			as_partition_status* ps1 = &p1->parts[i];
			as_partition_status* ps2 = &p2->parts[i];

			if (ps1->part_id != ps2->part_id) {
				cmp_error();
			}

			if (ps1->retry != ps2->retry) {
				cmp_error();
			}

			if (ps1->bval != ps2->bval) {
				cmp_error();
			}

			if (ps1->replica_index != ps2->replica_index) {
				cmp_error();
			}

			if (ps1->unavailable != ps2->unavailable) {
				cmp_error();
			}

			if (ps1->digest.init != ps2->digest.init) {
				cmp_error();
			}

			if (ps1->digest.init) {
				if (memcmp(ps1->digest.value, ps2->digest.value, AS_DIGEST_VALUE_SIZE) != 0) {
					cmp_error();
				}
			}
		}
	}

	if (q1->max_records != q2->max_records) {
		cmp_error();
	}

	if (q1->records_per_second != q2->records_per_second) {
		cmp_error();
	}

	if (q1->ttl != q2->ttl) {
		cmp_error();
	}

	if (q1->paginate != q2->paginate) {
		cmp_error();
	}

	if (q1->no_bins != q2->no_bins) {
		cmp_error();
	}

	return true;
}

as_status
query_terminate_resume_with_serialization(aerospike* p_as, as_error* err)
{
	// Same as query_terminate_resume(), but the query is saved to bytes that could
	// be resumed in a separate process.
	const char* set = "queryresume";
	uint32_t total_size = 200;

	LOG("write records for query terminate/resume with serialization");
	as_status status = insert_records_for_query_page(p_as, err, set, total_size);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	LOG("records written: %u", total_size);
	LOG("start query terminate");

	struct page_counter c;
	page_counter_init(&c, 50);

	as_query query;
	as_query_init(&query, g_namespace, set);
	as_query_set_paginate(&query, true);

	// Start query. Query will be terminated early in callback.
	status = aerospike_query_foreach(p_as, err, NULL, &query, query_terminate_cb, &c);

	if (status != AEROSPIKE_OK) {
		as_query_destroy(&query);
		return status;
	}

	LOG("terminate records returned: %u", c.count);
	LOG("start query resume");

	// Store completion status of all partitions to bytes.
	size_t bytes_size;
	uint8_t* bytes;

	if (! as_query_to_bytes(&query, &bytes, &bytes_size)) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to serialize query");
	}

	// Destroy query
	//as_query_destroy(&query);

	// Resume query using new query instance.
	as_query query_resume;

	if (! as_query_from_bytes(&query_resume, bytes, bytes_size)) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to deserialize query");
	}

	if (! query_compare(&query, &query_resume)) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "query_compare failed");
	}

	as_query_destroy(&query);

	// Free bytes
	free(bytes);

	page_counter_reset(&c);

	status = aerospike_query_foreach(p_as, err, NULL, &query_resume, query_resume_cb, &c);

	LOG("resume records returned: %u", c.count);

	as_query_destroy(&query_resume);
	return status;
}
