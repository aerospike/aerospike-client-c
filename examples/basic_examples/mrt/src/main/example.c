/*******************************************************************************
 * Copyright 2008-2018 by Aerospike.
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
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_scan.h>
#include <aerospike/as_status.h>

#include <aerospike/as_record_iterator.h>

#include "example_utils.h"


//==========================================================
// PUT Example
//

char* bin_name = "test-bin-1";
char* srt_bin_val = "srt";
char* mrt1_bin_val = "mrt1";
char* mrt2_bin_val = "mrt2";

void
srt(aerospike* as, int64_t pk)
{
	as_error err;
	as_key key;
	as_key_init_int64(&key, g_namespace, g_set, pk);

	as_record rec;
	as_record_inita(&rec, 1);

	as_record_set_str(&rec, bin_name, srt_bin_val);

	as_policy_write wpol;
	as_policy_write_init(&wpol);

	wpol.key = AS_POLICY_KEY_SEND;

	// Write the record to the database.
	if (aerospike_key_put_tr(as, NULL, &err, &wpol, &key, &rec, 0) !=
			AEROSPIKE_OK) {
		LOG("aerospike_key_put() returned %d - %s", err.code, err.message);
		example_cleanup(as);
		exit(-1);
	}

	if (! example_read_and_check_test_record_tr(as, NULL, &key, bin_name,
			srt_bin_val)) {
		LOG("FAILED: SRT read");
		exit(-1);
	}

	as_transaction tr = {
			.mrt_trid = 1
	};

	if (! example_read_and_check_test_record_tr(as, &tr, &key, bin_name,
			srt_bin_val)) {
		LOG("FAILED: SRT read");
		exit(-1);
	}
}

void
mrt1(aerospike* as, as_transaction* tr, int64_t pk, char* expected_srt_val)
{
	as_error err;
	as_key key;
	as_key_init_int64(&key, g_namespace, g_set, pk);

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_str(&rec, bin_name, mrt1_bin_val);

	as_policy_write wpol;
	as_policy_write_init(&wpol);

	wpol.key = AS_POLICY_KEY_SEND;

	if (aerospike_key_put_tr(as, tr, &err, &wpol, &key, &rec, 0) !=
			AEROSPIKE_OK) {
		LOG("aerospike_key_put() first MRT write failed - returned %d - %s",
				err.code, err.message);
		exit(1);
	}

	if (expected_srt_val != NULL &&
			! example_read_and_check_test_record_tr(as, NULL, &key, bin_name,
					expected_srt_val)) {
		LOG("FAILED: SRT read");
		exit(-1);
	}

	if (! example_read_and_check_test_record_tr(as, tr, &key, bin_name,
			mrt1_bin_val)) {
		LOG("FAILED: MRT read");
		exit(-1);
	}
}

void
mrt2(aerospike* as, as_transaction* tr, int64_t pk, char* expected_srt_val)
{
	as_error err;
	as_key key;
	as_key_init_int64(&key, g_namespace, g_set, pk);

	as_record rec;
	as_record_inita(&rec, 3);

	as_policy_write wpol;
	as_policy_write_init(&wpol);

	wpol.key = AS_POLICY_KEY_SEND;

	as_record_set_str(&rec, bin_name, mrt2_bin_val);
	as_record_set_int64(&rec, "test-bin-2", 2222);
	// more than 16 bytes to skip write_in_place
	as_record_set_str(&rec, "test-bin-3", "test-bin-3-data-mrt");

	if (aerospike_key_put_tr(as, tr, &err, &wpol, & key, &rec, 0) !=
			AEROSPIKE_OK) {
		LOG("FAILED: aerospike_key_put() second MRT write failed - returned %d - %s",
				err.code, err.message);
		exit(1);
	}

	if (expected_srt_val != NULL &&
			! example_read_and_check_test_record_tr(as, NULL, &key, bin_name,
					expected_srt_val)) {
		LOG("FAILED: SRT read");
		exit(-1);
	}

	if (! example_read_and_check_test_record_tr(as, tr, &key, bin_name,
			mrt2_bin_val)) {
		LOG("FAILED: MRT read");
		exit(-1);
	}
}

void
roll_forward(aerospike* as, as_transaction* tr, int64_t pk, char* expected_bin_val)
{
	as_error err;
	as_record dummy_rec;
	as_key key;
	as_key_init_int64(&key, g_namespace, g_set, (int64_t)pk);

	// commit
	if (aerospike_key_put_tr(as, tr, &err, NULL, &key, &dummy_rec,
			MRT_ROLL_FORWARD) != AEROSPIKE_OK) {
		LOG("aerospike_key_put() MRT roll forward failed - returned %d - %s",
				err.code, err.message);
		exit(1);
	}

	if (! example_read_and_check_test_record_tr(as, tr, &key, bin_name,
			expected_bin_val)) {
		LOG("FAILED: MRT read");
		exit(-1);
	}
}

void
roll_back(aerospike* as, as_transaction* tr, int64_t pk, char* expected_bin_val)
{
	as_error err;
	as_record dummy_rec;
	as_key key;
	as_key_init_int64(&key, g_namespace, g_set, (int64_t)pk);

	// commit
	if (aerospike_key_put_tr(as, tr, &err, NULL, &key, &dummy_rec,
			MRT_ROLL_BACK) != AEROSPIKE_OK) {
		LOG("aerospike_key_put() MRT roll back failed - returned %d - %s",
				err.code, err.message);
		exit(1);
	}

	if (expected_bin_val != NULL &&
			! example_read_and_check_test_record_tr(as, tr, &key, bin_name,
			expected_bin_val)) {
		LOG("FAILED: MRT read");
		exit(-1);
	}
}

#define N_OPS 4

void
test1(aerospike* as)
{
	char* orig_normal;
	char* prov;

	for (int64_t pk = 0; pk < 10000; pk++) {
		orig_normal = NULL;
		prov = NULL;

		if (pk % N_OPS != 0) {
			orig_normal = srt_bin_val;
			srt(as, pk);
		}

		as_transaction tr;
		tr.mrt_trid = 1;

		if (pk % N_OPS != 1) {
			prov = mrt1_bin_val;
			mrt1(as, &tr, pk, orig_normal);
		}

		if (pk % N_OPS != 2) {
			prov = mrt2_bin_val;
			mrt2(as, &tr, pk, orig_normal);
		}

		if (pk % N_OPS != 3) {
			if (pk % 2 == 0) {
				roll_forward(as, &tr, pk, prov == NULL ? orig_normal : prov);
			}
			else {
				roll_back(as, &tr, pk, orig_normal);
			}
			// handle cases where roll_forward and roll_back are both there
		}
	}
}

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

	int64_t pk = (p_rec->key.value.integer).value;

	char* orig_normal = NULL;
	char* prov = NULL;

	if (pk % 4 != 0) {
		orig_normal = srt_bin_val;
	}

	as_transaction tr;
	tr.mrt_trid = 1;

	if (pk % 4 != 1) {
		prov = mrt1_bin_val;
	}

	if (pk % 4 != 2) {
		prov = mrt2_bin_val;
	}

	if (pk % 4 != 3) {
		if (pk % 2 == 0) {
			orig_normal = prov == NULL ? orig_normal : prov;
		}

		prov = NULL;
	}

	if (orig_normal != NULL) {
		as_record_iterator it;
		as_record_iterator_init(&it, p_rec);

		while (as_record_iterator_has_next(&it)) {
			as_bin* bin = as_record_iterator_next(&it);

			if (strcmp(bin->name, bin_name) == 0) {
				char* val_as_str = as_val_tostring(as_bin_get_value(bin));

				if (strncmp(&val_as_str[1], orig_normal, strlen(orig_normal))
						!= 0) {
					LOG("FAILED: read not as expected. pk %ld expected %s got %s",
							pk, orig_normal, val_as_str);
					free(val_as_str);
					as_record_iterator_destroy(&it);
					// Destroy the as_record object.
					as_record_destroy(p_rec);

					return false;
				}

				free(val_as_str);
			}
		}
	}

	return true;
}

void
test2(aerospike* as)
{
	char* orig_normal;
	char* prov;

	for (int64_t pk = 0; pk < 10000; pk++) {
		orig_normal = NULL;
		prov = NULL;

		if (pk % 4 != 0) {
			orig_normal = srt_bin_val;
		}

		as_transaction tr;
		tr.mrt_trid = 1;

		if (pk % 4 != 1) {
			prov = mrt1_bin_val;
		}

		if (pk % 4 != 2) {
			prov = mrt2_bin_val;
		}

		if (pk % 4 != 3) {
			if (pk % 2 == 0) {
				orig_normal = prov == NULL ? orig_normal : prov;
			}

			prov = NULL;
		}

		as_key key;
		as_key_init_int64(&key, g_namespace, g_set, (int64_t)pk);

		if (orig_normal != NULL &&
				! example_read_and_check_test_record_tr(as, NULL, &key,
						bin_name, orig_normal)) {
			LOG("FAILED: SRT read pk %ld bin_val %s", pk, orig_normal);
//			exit(-1);
		}

		if (prov != NULL && ! example_read_and_check_test_record_tr(as, &tr,
				&key, bin_name, prov)) {
			LOG("FAILED: MRT read");
			exit(-1);
		}
	}

	// scan to verify set index
	as_scan scan;
	as_scan_init(&scan, g_namespace, g_set);

	as_error err;

	LOG("starting scan all ...");

	// Do the scan. This call blocks while the scan is running - callbacks are
	// made in the scope of this call.
	if (aerospike_scan_foreach(as, &err, NULL, &scan, scan_cb, NULL) !=
			AEROSPIKE_OK) {
		LOG("aerospike_scan_foreach() returned %d - %s", err.code, err.message);
		as_scan_destroy(&scan);
		exit(-1);
	}

	LOG("... scan all completed");
}

int
main(int argc, char* argv[])
{
	// Parse command line arguments.
	if (! example_get_opts(argc, argv, EXAMPLE_BASIC_OPTS)) {
		exit(-1);
	}

	// Connect to the aerospike database cluster.
	aerospike as;
	example_connect_to_aerospike(&as);

//	test1(&as);
	test2(&as);

	return 0;
}
