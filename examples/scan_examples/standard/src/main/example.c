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
#include <aerospike/aerospike_scan.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_error.h>
#include <aerospike/as_key.h>
#include <aerospike/as_partition.h>
#include <aerospike/as_record.h>
#include <aerospike/as_scan.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>

#include "example_utils.h"


//==========================================================
// Forward Declarations
//

bool scan_cb(const as_val* p_val, void* udata);
void cleanup(aerospike* p_as);
bool insert_records(aerospike* p_as);
as_status scan_partition(aerospike* p_as, as_error* err);
as_status scan_pages(aerospike* p_as, as_error* err);
as_status scan_terminate_resume(aerospike* p_as, as_error* err);
as_status scan_terminate_resume_with_serialization(aerospike* p_as, as_error* err);

//==========================================================
// STANDARD SCAN Example
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

	// Specify the namespace and set to use during the scan.
	as_scan scan;
	as_scan_init(&scan, g_namespace, g_set);

	LOG("starting scan all ...");

	// Do the scan. This call blocks while the scan is running - callbacks are
	// made in the scope of this call.
	if (aerospike_scan_foreach(&as, &err, NULL, &scan, scan_cb, NULL) !=
			AEROSPIKE_OK) {
		LOG("aerospike_scan_foreach() returned %d - %s", err.code, err.message);
		as_scan_destroy(&scan);
		cleanup(&as);
		exit(-1);
	}

	LOG("... scan all completed");

	// Now specify that only two bins are to be returned by the scan. The first
	// ten records do not have these two bins, so they should not be returned by
	// the scan. The remaining records should be returned without test-bin-1.
	as_scan_select_inita(&scan, 2);
	as_scan_select(&scan, "test-bin-2");
	as_scan_select(&scan, "test-bin-3");

	LOG("starting scan with select ...");

	// Do the scan. This call blocks while the scan is running - callbacks are
	// made in the scope of this call.
	if (aerospike_scan_foreach(&as, &err, NULL, &scan, scan_cb, NULL) !=
			AEROSPIKE_OK) {
		LOG("aerospike_scan_foreach() returned %d - %s", err.code, err.message);
		as_scan_destroy(&scan);
		cleanup(&as);
		exit(-1);
	}

	LOG("... scan with select completed");

	// Destroy the as_scan object.
	as_scan_destroy(&scan);

	// Run scan partition functionality.
	if (scan_partition(&as, &err) != AEROSPIKE_OK) {
		LOG("scan_partition() returned %d - %s", err.code, err.message);
		cleanup(&as);
		exit(-1);
	}

	// Run scan pages.
	if (scan_pages(&as, &err) != AEROSPIKE_OK) {
		LOG("scan_pages() returned %d - %s", err.code, err.message);
		cleanup(&as);
		exit(-1);
	}

	// Run scan terminate/resume.
	if (scan_terminate_resume(&as, &err) != AEROSPIKE_OK) {
		LOG("scan_terminate_resume() returned %d - %s", err.code, err.message);
		cleanup(&as);
		exit(-1);
	}

	// Run scan terminate/resume with serialization.
	if (scan_terminate_resume_with_serialization(&as, &err) != AEROSPIKE_OK) {
		LOG("scan_terminate_resume_with_serialization() returned %d - %s", err.code, err.message);
		cleanup(&as);
		exit(-1);
	}

	// Cleanup and disconnect from the database cluster.
	cleanup(&as);

	LOG("standard scan examples successfully completed");
	return 0;
}

//==========================================================
// Scan Callback
//

bool
scan_cb(const as_val* p_val, void* udata)
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
// Helpers
//

void
cleanup(aerospike* p_as)
{
	example_remove_test_records(p_as);
	example_cleanup(p_as);
}

bool
insert_records(aerospike* p_as)
{
	// Create an as_record object with up to three integer value bins. By using
	// as_record_inita(), we won't need to destroy the record if we only set
	// bins using as_record_set_int64().
	as_record rec;
	as_record_inita(&rec, 3);

	// Re-using rec, write records into the database such that each record's key
	// and (test-bin) value is based on the loop index.
	for (uint32_t i = 0; i < g_n_keys; i++) {
		as_error err;

		// Set up a default as_policy_write object.
		as_policy_write wpol;
		as_policy_write_init(&wpol);

		// No need to destroy a stack as_key object, if we only use
		// as_key_init_int64().
		as_key key;
		as_key_init_int64(&key, g_namespace, g_set, (int64_t)i);

		// In general it's ok to reset a bin value - all as_record_set_... calls
		// destroy any previous value.

		if (i < 10) {
			// Only write one bin in the first ten records.
			as_record_set_int64(&rec, "test-bin-1", (int64_t)i);

			// By default, we don't store the key with the record in the
			// database. For these records, the key will not be returned in the
			// scan callback.
		}
		else {
			// Write three bins in all remaining records.
			as_record_set_int64(&rec, "test-bin-1", (int64_t)i);
			as_record_set_int64(&rec, "test-bin-2", (int64_t)(100 + i));
			as_record_set_int64(&rec, "test-bin-3", (int64_t)(1000 + i));

			// If we want the key to be returned in the scan callback, we must
			// store it with the record in the database. AS_POLICY_KEY_SEND
			// causes the key to be stored.
			wpol.key = AS_POLICY_KEY_SEND;
		}

		// Write a record to the database.
		if (aerospike_key_put(p_as, &err, &wpol, &key, &rec) != AEROSPIKE_OK) {
			LOG("aerospike_key_put() returned %d - %s", err.code, err.message);
			return false;
		}
	}

	LOG("insert succeeded");

	return true;
}

//==========================================================
// Scan Partition
//

static const char* g_pset = "pset";

static as_status
insert_records_in_one_partition(aerospike* p_as, as_error* err, uint32_t part_id, uint32_t* rec_count)
{
	// Write records that belong to the specified partition.
	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, "bin1", 55);

	uint32_t count = 0;
	as_status status;

	for (uint32_t i = 0; i < 80000; i++) {
		as_key key;
		as_key_init_int64(&key, g_namespace, g_pset, (int64_t)i);

		status = as_key_set_digest(err, &key);

		if (status != AEROSPIKE_OK) {
			return status;
		}

		uint32_t id = as_partition_getid(key.digest.value, p_as->cluster->n_partitions);

		if (id != part_id) {
			continue;
		}

		status = aerospike_key_put(p_as, err, NULL, &key, &rec);

		if (status != AEROSPIKE_OK) {
			return status;
		}
		count++;
	}
	*rec_count = count;
	return AEROSPIKE_OK;
}

struct counter {
	uint32_t count;
	uint32_t max;
	as_digest digest;
};

static bool
pscan_cb1(const as_val* val, void* udata)
{
	if (! val) {
		// Scan complete.
		return true;
	}

	struct counter* c = udata;

	if (as_aaf_uint32(&c->count, 1) == c->max) {
		// Save digest cursor and stop scan.
		as_record* rec = as_record_fromval(val);
		c->digest = rec->key.digest;
		return false;
	}
	return true;
}

static bool
pscan_cb2(const as_val* val, void* udata)
{
	if (! val) {
		// Scan complete.
		return true;
	}

	struct counter* c = udata;
	as_incr_uint32(&c->count);
	return true;
}

as_status
scan_partition(aerospike* p_as, as_error* err)
{
	LOG("write records for partition scan");

	// Write records that belong to a single partition.
	uint32_t part_id = 1000;
	uint32_t rec_count;
	as_status status = insert_records_in_one_partition(p_as, err, part_id, &rec_count);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	LOG("records written: %u", rec_count);
	LOG("scan partition");

	// Read first half of records from that partition.
	struct counter c;
	c.count = 0;
	c.max = rec_count / 2;
	c.digest.init = false;

	as_scan scan;
	as_scan_init(&scan, g_namespace, g_pset);

	as_partition_filter pf;
	as_partition_filter_set_id(&pf, part_id);

	status = aerospike_scan_partitions(p_as, err, NULL, &scan, &pf, pscan_cb1, &c);

	if (status != AEROSPIKE_OK) {
		as_scan_destroy(&scan);
		return status;
	}

	LOG("records scanned: %u", c.count);
	LOG("scan partition again from cursor");

	// Read remaining records from that partition using digest cursor.
	as_partition_filter_set_after(&pf, &c.digest);
	c.count = 0;

	status = aerospike_scan_partitions(p_as, err, NULL, &scan, &pf, pscan_cb2, &c);

	if (status != AEROSPIKE_OK) {
		as_scan_destroy(&scan);
		return status;
	}

	LOG("records scanned: %u", c.count);
	as_scan_destroy(&scan);
	return AEROSPIKE_OK;
}

//==========================================================
// Scan Pages
//

static as_status
insert_records_for_scan_page(aerospike* p_as, as_error* err, const char* set, uint32_t size)
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
scan_pages(aerospike* p_as, as_error* err)
{
	const char* set = "scanpage";
	uint32_t total_size = 190;
	uint32_t page_size = 100;

	LOG("write records for scan pagination");
	as_status status = insert_records_for_scan_page(p_as, err, set, total_size);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	LOG("records written: %u", total_size);

	struct counter c;
	c.count = 0;

	as_scan scan;
	as_scan_init(&scan, g_namespace, set);
	as_scan_set_paginate(&scan, true);

	as_policy_scan policy;
	as_policy_scan_init(&policy);
	policy.max_records = page_size;

	// Scan 3 pages of records.
	for (int i = 1; i <= 3 && ! as_scan_is_done(&scan); i++) {
		c.count = 0;

		LOG("scan page: %d", i);
		status = aerospike_scan_foreach(p_as, err, &policy, &scan, pscan_cb2, &c);

		if (status != AEROSPIKE_OK) {
			as_scan_destroy(&scan);
			return status;
		}
		LOG("records returned: %u", c.count);
	}

	as_scan_destroy(&scan);
	return AEROSPIKE_OK;
}

//==========================================================
// Scan Terminate and Resume
//

static bool
scan_terminate_cb(const as_val* val, void* udata)
{
	if (! val) {
		// Scan complete.
		return true;
	}

	struct counter* c = udata;

	// scan.concurrent is false, so atomics are not necessary.
	if (c->count >= c->max) {
		// Since we are terminating the scan here, the scan last digest
		// will not be set and the current record will be returned again
		// if the scan resumes at a later time.
		return false;
	}

	c->count++;
	return true;
}

static bool
scan_resume_cb(const as_val* val, void* udata)
{
	if (! val) {
		// Scan complete.
		return true;
	}

	struct counter* c = udata;
	c->count++;
	return true;
}


as_status
scan_terminate_resume(aerospike* p_as, as_error* err)
{
	const char* set = "scanresume";
	uint32_t total_size = 200;

	LOG("write records for scan terminate/resume");
	as_status status = insert_records_for_scan_page(p_as, err, set, total_size);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	LOG("records written: %u", total_size);
	LOG("start scan terminate");

	struct counter c;
	c.count = 0;
	c.max = 50;

	as_scan scan;
	as_scan_init(&scan, g_namespace, set);
	as_scan_set_paginate(&scan, true);

	// Start scan. Scan will be terminated early in callback.
	status = aerospike_scan_foreach(p_as, err, NULL, &scan, scan_terminate_cb, &c);

	if (status != AEROSPIKE_OK) {
		as_scan_destroy(&scan);
		return status;
	}

	LOG("terminate records returned: %u", c.count);
	LOG("start scan resume");

	// Store completion status of all partitions.
	as_partitions_status* parts_all = as_partitions_status_reserve(scan.parts_all);

	// Destroy scan
	as_scan_destroy(&scan);

	// Resume scan using new scan instance.
	as_scan scan_resume;
	as_scan_init(&scan_resume, g_namespace, set);

	// Use partition filter to set parts_all.
	// Calling as_scan_set_partitions(&scan_resume, parts_all) works too.
	// as_partition_filter_set_partitions() is just a wrapper for eventually calling
	// as_scan_set_partitions().
	as_partition_filter pf;
	as_partition_filter_set_partitions(&pf, parts_all);

	c.count = 0;
	c.max = 0;

	status = aerospike_scan_partitions(p_as, err, NULL, &scan_resume, &pf, scan_resume_cb, &c);

	LOG("resume records returned: %u", c.count);

	as_partitions_status_release(parts_all);
	as_scan_destroy(&scan_resume);
	return status;
}

as_status
scan_terminate_resume_with_serialization(aerospike* p_as, as_error* err)
{
	const char* set = "scanresume";
	uint32_t total_size = 200;

	LOG("write records for scan terminate/resume with serialization");
	as_status status = insert_records_for_scan_page(p_as, err, set, total_size);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	LOG("records written: %u", total_size);
	LOG("start scan terminate");

	struct counter c;
	c.count = 0;
	c.max = 50;

	as_scan scan;
	as_scan_init(&scan, g_namespace, set);
	as_scan_set_paginate(&scan, true);

	// Start scan. Scan will be terminated early in callback.
	status = aerospike_scan_foreach(p_as, err, NULL, &scan, scan_terminate_cb, &c);

	if (status != AEROSPIKE_OK) {
		as_scan_destroy(&scan);
		return status;
	}

	LOG("terminate records returned: %u", c.count);
	LOG("start scan resume");

	// Serialize scan to bytes.
	uint32_t bytes_size;
	uint8_t* bytes;

	if (! as_scan_to_bytes(&scan, &bytes, &bytes_size)) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to serialize scan");
	}

	// Destroy scan
	as_scan_destroy(&scan);

	// Resume scan using new scan instance.
	as_scan scan_resume;

	if (! as_scan_from_bytes(&scan_resume, bytes, bytes_size)) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to deserialize scan");
	}

	// Free bytes
	free(bytes);

	c.count = 0;
	c.max = 0;

	status = aerospike_scan_foreach(p_as, err, NULL, &scan_resume, scan_resume_cb, &c);

	LOG("resume records returned: %u", c.count);

	as_scan_destroy(&scan_resume);
	return status;
}
