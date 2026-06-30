/*******************************************************************************
 * Copyright 2008-2025 by Aerospike.
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
// Error Details Example
//
// A local dev tool: drives ~40 representative server error paths through
// the real C client + wire format, then asserts on the server-side
// subcode (and where stable, a message substring) returned in
// err.message. Run after touching error-detail call sites or subcode
// numbering. Not part of any CI suite.
//
// Each case is a self-contained function that sets up the bin it needs,
// triggers the failing op, and leaves the failure shape in *err. The
// driver loop checks expected status + subcode + (optional) message
// substring per row in CASES[] and prints PASS/FAIL.
//
// Subcode constants mirror AS_SUB_* in aerospike-server/as/include/base/proto.h.
// They are stored as ints in the table and matched against err.message
// as the substring "subcode=N" -- that's what the C client formats from
// the AS_MSG_FIELD_TYPE_ERROR_DETAILS field.
//
// AS_SUB_NONE (0) is never sent on the wire: the server omits map key 1
// entirely when the subcode is NONE. Such cases set subcode_absent = true,
// and the driver asserts no "subcode=" suffix appears in err.message (the
// message text alone carries the context).
//

//==========================================================
// Includes
//

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_batch.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_batch.h>
#include <aerospike/as_bit_operations.h>
#include <aerospike/as_cdt_ctx.h>
#include <aerospike/as_error.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_exp_operations.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_hll_operations.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_list_operations.h>
#include <aerospike/as_map_operations.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>
#include <aerospike/as_vector.h>

#include "example_utils.h"


//==========================================================
// Globals.
//

static const char* BIN = "bin";

static as_policy_write g_write_pol;
static as_policy_operate g_op_pol;
static as_policy_remove g_rm_pol;
static as_policy_read g_read_pol;


//==========================================================
// Case API.
//

typedef as_status (*case_fn)(aerospike* as, as_error* err);

typedef struct {
	const char* name;
	as_status   expected_status;
	bool        subcode_absent;      // true: assert NO subcode on the wire
	uint32_t    expected_subcode;    // checked only when !subcode_absent
	const char* expected_msg_substr; // NULL to skip
	case_fn     run;
} error_case;


//==========================================================
// Helpers.
//

static void
init_policies(void)
{
	as_policy_write_init(&g_write_pol);
	g_write_pol.base.error_detail_verbosity = 2;

	as_policy_operate_init(&g_op_pol);
	g_op_pol.base.error_detail_verbosity = 2;

	as_policy_remove_init(&g_rm_pol);
	g_rm_pol.base.error_detail_verbosity = 2;

	as_policy_read_init(&g_read_pol);
	g_read_pol.base.error_detail_verbosity = 2;
}

// Bail with a clear message if a setup precondition fails. Setup failures
// are programmer errors in this file, not test results.
static void
must(as_status s, const as_error* err, const char* what)
{
	if (s != AEROSPIKE_OK) {
		LOG("setup '%s' failed: %d - %s", what, err->code, err->message);
		exit(1);
	}
}

// Write a record with a single integer bin.
static void
put_int(aerospike* as, as_key* key, int64_t v)
{
	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, BIN, v);

	as_error err;
	must(aerospike_key_put(as, &err, &g_write_pol, key, &rec), &err, "put_int");
}

// Write a record with a single string bin.
static void
put_str(aerospike* as, as_key* key, const char* v)
{
	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_str(&rec, BIN, v);

	as_error err;
	must(aerospike_key_put(as, &err, &g_write_pol, key, &rec), &err, "put_str");
}

// Write a record with a single raw-bytes bin (used to construct deliberately
// corrupt CDT/HLL payloads that the server's parser will reject).
static void
put_raw(aerospike* as, as_key* key, const uint8_t* data, uint32_t size)
{
	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_raw(&rec, BIN, data, size);

	as_error err;
	must(aerospike_key_put(as, &err, &g_write_pol, key, &rec), &err, "put_raw");
}

// Write a record carrying a small seed list with the given integers.
static void
put_int_list(aerospike* as, as_key* key, const int64_t* vals, uint32_t n)
{
	as_arraylist list;
	as_arraylist_init(&list, n, 0);

	for (uint32_t i = 0; i < n; i++) {
		as_arraylist_append_int64(&list, vals[i]);
	}

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_list(&rec, BIN, (as_list*)&list);

	as_error err;
	must(aerospike_key_put(as, &err, &g_write_pol, key, &rec), &err,
			"put_int_list");

	as_arraylist_destroy(&list);
}

// Write a record carrying a small int->string map.
static void
put_int_map(aerospike* as, as_key* key, const int64_t* keys,
		const char* const* vals, uint32_t n)
{
	as_hashmap m;
	as_hashmap_init(&m, n);

	for (uint32_t i = 0; i < n; i++) {
		as_hashmap_set(&m, (as_val*)as_integer_new(keys[i]),
				(as_val*)as_string_new_strdup(vals[i]));
	}

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_map(&rec, BIN, (as_map*)&m);

	as_error err;
	must(aerospike_key_put(as, &err, &g_write_pol, key, &rec), &err,
			"put_int_map");

	as_hashmap_destroy(&m);
}


//==========================================================
// Cases: particle modify type mismatches.
//
// All return AS_ERR_INCOMPATIBLE_TYPE with subcode AS_SUB_NONE -- the
// status is maximally specific, so no subcode is sent on the wire and the
// message substring distinguishes which op was rejected.
//

static as_status
case_append_str_on_int(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-append-on-int");
	put_int(as, &key, 1);

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_append_str(&ops, BIN, "bad");

	as_status s = aerospike_key_operate(as, err, &g_op_pol, &key, &ops, NULL);
	as_operations_destroy(&ops);
	return s;
}

static as_status
case_incr_on_str(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-incr-on-str");
	put_str(as, &key, "hello");

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_incr(&ops, BIN, 1);

	as_status s = aerospike_key_operate(as, err, &g_op_pol, &key, &ops, NULL);
	as_operations_destroy(&ops);
	return s;
}

static as_status
case_prepend_str_on_int(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-prepend-on-int");
	put_int(as, &key, 1);

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_prepend_str(&ops, BIN, "bad");

	as_status s = aerospike_key_operate(as, err, &g_op_pol, &key, &ops, NULL);
	as_operations_destroy(&ops);
	return s;
}

static as_status
case_incr_double_on_int(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-incr-dbl-on-int");
	put_int(as, &key, 1);

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_incr_double(&ops, BIN, 1.5);

	as_status s = aerospike_key_operate(as, err, &g_op_pol, &key, &ops, NULL);
	as_operations_destroy(&ops);
	return s;
}

static as_status
case_hll_add_on_int(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-hll-on-int");
	put_int(as, &key, 1);

	as_arraylist items;
	as_arraylist_init(&items, 1, 0);
	as_arraylist_append_str(&items, "e1");

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_hll_add(&ops, BIN, NULL, NULL, (as_list*)&items, 8);

	as_status s = aerospike_key_operate(as, err, &g_op_pol, &key, &ops, NULL);
	as_operations_destroy(&ops);
	as_arraylist_destroy(&items);
	return s;
}


//==========================================================
// Cases: CDT list ops.
//

static as_status
case_list_get_index_out_of_bounds(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-list-idx-oob");
	int64_t seed[] = { 10, 20, 30 };
	put_int_list(as, &key, seed, 3);

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_list_get(&ops, BIN, NULL, 99);

	as_status s = aerospike_key_operate(as, err, &g_op_pol, &key, &ops, NULL);
	as_operations_destroy(&ops);
	return s;
}

static as_status
case_list_get_by_rank_out_of_bounds(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-list-rank-oob");
	int64_t seed[] = { 10, 20, 30 };
	put_int_list(as, &key, seed, 3);

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_list_get_by_rank(&ops, BIN, NULL, 99,
			AS_LIST_RETURN_VALUE);

	as_status s = aerospike_key_operate(as, err, &g_op_pol, &key, &ops, NULL);
	as_operations_destroy(&ops);
	return s;
}

static as_status
case_list_pop_index_out_of_bounds(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-list-pop-oob");
	int64_t seed[] = { 1, 2 };
	put_int_list(as, &key, seed, 2);

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_list_pop(&ops, BIN, NULL, 99);

	as_status s = aerospike_key_operate(as, err, &g_op_pol, &key, &ops, NULL);
	as_operations_destroy(&ops);
	return s;
}

static as_status
case_list_insert_unique_violation(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-list-unique");
	int64_t seed[] = { 1, 2, 3 };
	put_int_list(as, &key, seed, 3);

	as_list_policy lp;
	as_list_policy_set(&lp, AS_LIST_UNORDERED,
			AS_LIST_WRITE_ADD_UNIQUE);

	as_integer dup;
	as_integer_init(&dup, 2);

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_list_append(&ops, BIN, NULL, &lp, (as_val*)&dup);

	as_status s = aerospike_key_operate(as, err, &g_op_pol, &key, &ops, NULL);
	as_operations_destroy(&ops);
	return s;
}

static as_status
case_list_bounded_overflow(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-list-bounded");
	int64_t seed[] = { 1 };
	put_int_list(as, &key, seed, 1);

	as_list_policy lp;
	as_list_policy_set(&lp, AS_LIST_ORDERED,
			AS_LIST_WRITE_INSERT_BOUNDED);

	as_integer v;
	as_integer_init(&v, 5);

	// Inserting past end of an ordered+bounded list -> overflow.
	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_list_insert(&ops, BIN, NULL, &lp, 10, (as_val*)&v);

	as_status s = aerospike_key_operate(as, err, &g_op_pol, &key, &ops, NULL);
	as_operations_destroy(&ops);
	return s;
}

static as_status
case_list_op_on_corrupt_bin(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-list-corrupt");

	// A bin written as raw bytes is not a list -- list_get against it
	// triggers the bin-type-incompatible path.
	uint8_t junk[] = { 0xDE, 0xAD, 0xBE, 0xEF };
	put_raw(as, &key, junk, sizeof(junk));

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_list_get(&ops, BIN, NULL, 0);

	as_status s = aerospike_key_operate(as, err, &g_op_pol, &key, &ops, NULL);
	as_operations_destroy(&ops);
	return s;
}


//==========================================================
// Cases: CDT map ops.
//

static as_status
case_map_put_create_only_existing(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-map-create-exists");

	int64_t keys[] = { 1 };
	const char* vals[] = { "a" };
	put_int_map(as, &key, keys, vals, 1);

	as_map_policy mp;
	as_map_policy_set_flags(&mp, AS_MAP_UNORDERED, AS_MAP_WRITE_CREATE_ONLY);

	as_integer k;
	as_integer_init(&k, 1);
	as_string v;
	as_string_init(&v, "b", false);

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_map_put(&ops, BIN, NULL, &mp, (as_val*)&k, (as_val*)&v);

	as_status s = aerospike_key_operate(as, err, &g_op_pol, &key, &ops, NULL);
	as_operations_destroy(&ops);
	return s;
}

static as_status
case_map_put_update_only_missing(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-map-update-missing");

	int64_t keys[] = { 1 };
	const char* vals[] = { "a" };
	put_int_map(as, &key, keys, vals, 1);

	as_map_policy mp;
	as_map_policy_set_flags(&mp, AS_MAP_UNORDERED, AS_MAP_WRITE_UPDATE_ONLY);

	as_integer k;
	as_integer_init(&k, 99); // not present
	as_string v;
	as_string_init(&v, "b", false);

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_map_put(&ops, BIN, NULL, &mp, (as_val*)&k, (as_val*)&v);

	as_status s = aerospike_key_operate(as, err, &g_op_pol, &key, &ops, NULL);
	as_operations_destroy(&ops);
	return s;
}

static as_status
case_map_op_on_list_bin(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-map-on-list");
	int64_t seed[] = { 1, 2 };
	put_int_list(as, &key, seed, 2);

	as_integer k;
	as_integer_init(&k, 1);

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_map_get_by_key(&ops, BIN, NULL, (as_val*)&k,
			AS_MAP_RETURN_VALUE);

	as_status s = aerospike_key_operate(as, err, &g_op_pol, &key, &ops, NULL);
	as_operations_destroy(&ops);
	return s;
}

static as_status
case_map_op_on_corrupt_bin(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-map-corrupt");

	uint8_t junk[] = { 0x42, 0x42 };
	put_raw(as, &key, junk, sizeof(junk));

	as_integer k;
	as_integer_init(&k, 1);

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_map_get_by_key(&ops, BIN, NULL, (as_val*)&k,
			AS_MAP_RETURN_VALUE);

	as_status s = aerospike_key_operate(as, err, &g_op_pol, &key, &ops, NULL);
	as_operations_destroy(&ops);
	return s;
}

static as_status
case_map_ctx_into_map_value(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-map-ctx-bad");

	// Map whose value at key 1 is a string -- context can't descend into it.
	int64_t keys[] = { 1 };
	const char* vals[] = { "leaf-string" };
	put_int_map(as, &key, keys, vals, 1);

	as_cdt_ctx ctx;
	as_cdt_ctx_inita(&ctx, 1);
	as_integer ck;
	as_integer_init(&ck, 1);
	as_cdt_ctx_add_map_key(&ctx, (as_val*)&ck);

	// Now try a list op inside that context -- the value is a string, not
	// a list, so the context isn't applicable.
	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_list_get(&ops, BIN, &ctx, 0);

	as_status s = aerospike_key_operate(as, err, &g_op_pol, &key, &ops, NULL);
	as_operations_destroy(&ops);
	as_cdt_ctx_destroy(&ctx);
	return s;
}

// (map remove_by_value_list with all-missing values is a no-op success on
// the server, not an error path -- so it can't drive the
// "values-not-found" path from the client. That branch fires from
// internal index-find failures not reachable through ordinary client
// requests, and the per-status redesign collapsed its old subcode
// (AS_SUB_CDT_VALUES_NOT_FOUND, 3109) to AS_SUB_NONE under
// AS_ERR_ELEMENT_NOT_FOUND.)


//==========================================================
// Cases: bits ops.
//

static as_status
case_bits_get_offset_out_of_range(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-bits-offset-oor");

	uint8_t blob[4] = { 0xAA, 0xBB, 0xCC, 0xDD };
	put_raw(as, &key, blob, sizeof(blob));

	// Server caps offset at PROTO_SIZE_MAX * 8 (~1.07e9 bits). Use a value
	// well above that to trigger the parse-time range check.
	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_bit_get(&ops, BIN, NULL, 2000000000, 8);

	as_status s = aerospike_key_operate(as, err, &g_op_pol, &key, &ops, NULL);
	as_operations_destroy(&ops);
	return s;
}

static as_status
case_bits_get_size_zero(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-bits-size-zero");

	uint8_t blob[4] = { 0xAA, 0xBB, 0xCC, 0xDD };
	put_raw(as, &key, blob, sizeof(blob));

	// bit_get with size 0 is rejected at parse time with SIZE_OUT_OF_RANGE.
	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_bit_get(&ops, BIN, NULL, 0, 0);

	as_status s = aerospike_key_operate(as, err, &g_op_pol, &key, &ops, NULL);
	as_operations_destroy(&ops);
	return s;
}


//==========================================================
// Cases: HLL ops.
//

static as_status
case_hll_init_invalid_index_bits(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-hll-bad-bits");

	// Index bit count out of the legal [4, 16] range.
	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_hll_init(&ops, BIN, NULL, NULL, 30);

	as_status s = aerospike_key_operate(as, err, &g_op_pol, &key, &ops, NULL);
	as_operations_destroy(&ops);
	return s;
}

static as_status
case_hll_fold_target_too_large(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-hll-fold-big");

	// Initialize an HLL with 8 index bits.
	{
		as_operations init_ops;
		as_operations_inita(&init_ops, 1);
		as_operations_hll_init(&init_ops, BIN, NULL, NULL, 8);

		as_error e;
		as_status s = aerospike_key_operate(as, &e, &g_op_pol, &key,
				&init_ops, NULL);
		as_operations_destroy(&init_ops);
		must(s, &e, "hll init for fold-target-too-large");
	}

	// Try folding to MORE bits than we have -- folding can only reduce.
	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_hll_fold(&ops, BIN, NULL, 14);

	as_status s = aerospike_key_operate(as, err, &g_op_pol, &key, &ops, NULL);
	as_operations_destroy(&ops);
	return s;
}

static as_status
case_hll_op_on_corrupt_bin(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-hll-corrupt");

	// Raw bytes != HLL bin -- the bin-type check fires before parsing.
	uint8_t junk[] = { 0x01, 0x02, 0x03 };
	put_raw(as, &key, junk, sizeof(junk));

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_hll_get_count(&ops, BIN, NULL);

	as_status s = aerospike_key_operate(as, err, &g_op_pol, &key, &ops, NULL);
	as_operations_destroy(&ops);
	return s;
}

static as_status
case_hll_refresh_count_no_hll_bin(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-hll-refresh-nf");
	put_int(as, &key, 1); // sibling bin exists, target HLL bin does not

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_hll_refresh_count(&ops, "no-hll-bin", NULL);

	as_status s = aerospike_key_operate(as, err, &g_op_pol, &key, &ops, NULL);
	as_operations_destroy(&ops);
	return s;
}


//==========================================================
// Cases: write/delete/read policy.
//

static as_status
case_write_create_only_existing(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-write-create-exists");
	put_int(as, &key, 1);

	as_policy_write pol = g_write_pol;
	pol.exists = AS_POLICY_EXISTS_CREATE;

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, BIN, 2);

	return aerospike_key_put(as, err, &pol, &key, &rec);
}

static as_status
case_write_replace_only_missing(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-write-replace-missing");
	// No prior put -- key does not exist.

	as_policy_write pol = g_write_pol;
	pol.exists = AS_POLICY_EXISTS_REPLACE;

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, BIN, 1);

	return aerospike_key_put(as, err, &pol, &key, &rec);
}

static as_status
case_write_generation_mismatch(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-write-gen");
	put_int(as, &key, 1);

	as_policy_write pol = g_write_pol;
	pol.gen = AS_POLICY_GEN_EQ;

	// as_policy_write has no generation field -- the expected generation
	// for a write travels on the record itself.
	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, BIN, 2);
	rec.gen = 999;

	return aerospike_key_put(as, err, &pol, &key, &rec);
}

static as_status
case_delete_generation_mismatch(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-del-gen");
	put_int(as, &key, 1);

	as_policy_remove pol = g_rm_pol;
	pol.gen = AS_POLICY_GEN_EQ;
	pol.generation = 777;

	return aerospike_key_remove(as, err, &pol, &key);
}

static as_status
case_read_filtered_out(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-read-filtered");
	put_int(as, &key, 1);

	// Filter: BIN == 99 -- record has BIN=1, so filter rejects.
	as_exp_build(filter,
			as_exp_cmp_eq(as_exp_bin_int(BIN), as_exp_int(99)));

	as_policy_read pol = g_read_pol;
	pol.base.filter_exp = filter;

	as_record* rec = NULL;
	as_status s = aerospike_key_get(as, err, &pol, &key, &rec);

	as_record_destroy(rec);
	as_exp_destroy(filter);
	return s;
}

static as_status
case_operate_filtered_out(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-op-filtered");
	put_int(as, &key, 1);

	as_exp_build(filter,
			as_exp_cmp_eq(as_exp_bin_int(BIN), as_exp_int(99)));

	as_policy_operate pol = g_op_pol;
	pol.base.filter_exp = filter;

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_read(&ops, BIN);

	as_status s = aerospike_key_operate(as, err, &pol, &key, &ops, NULL);
	as_operations_destroy(&ops);
	as_exp_destroy(filter);
	return s;
}

// A filter expression that fails to COMPILE on the server: eq(5, 6.0) has
// int vs float operands, which the server rejects at build time (it does not
// type-check this client-side). Yields AS_ERR_PARAMETER with the build-phase
// detail message. Verbosity 3 so the server also emits the field-45 expression
// trace (key 3); the client skips it as an unknown sub-key, exercising that
// path end-to-end.
static as_status
case_filter_build_fail(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-filter-build-fail");
	put_int(as, &key, 1);

	as_exp_build(filter, as_exp_cmp_eq(as_exp_int(5), as_exp_float(6.0)));

	as_policy_read pol = g_read_pol;
	pol.base.error_detail_verbosity = 3;
	pol.base.filter_exp = filter;

	as_record* rec = NULL;
	as_status s = aerospike_key_get(as, err, &pol, &key, &rec);

	as_record_destroy(rec);
	as_exp_destroy(filter);
	return s;
}

// An expression operation (exp_write) whose expression fails to COMPILE ->
// AS_ERR_PARAMETER. Covers the operate-path build-failure detail authored in
// as_exp_op_parse (distinct message from the filter path).
static as_status
case_exp_op_build_fail(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-expop-build-fail");
	put_int(as, &key, 1);

	as_exp_build(exp, as_exp_cmp_eq(as_exp_int(5), as_exp_float(6.0)));

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_exp_write(&ops, BIN, exp, AS_EXP_WRITE_DEFAULT);

	as_policy_operate pol = g_op_pol;
	pol.base.error_detail_verbosity = 3;

	as_record* rec = NULL;
	as_status s = aerospike_key_operate(as, err, &pol, &key, &ops, &rec);

	as_record_destroy(rec);
	as_operations_destroy(&ops);
	as_exp_destroy(exp);
	return s;
}

// --- SERVER-1138 runtime expression eval FAULTS. ---
//
// These expressions COMPILE cleanly but FAULT at evaluation (integer div/mod
// by zero). The non-breaking contract: which records MATCH is unchanged -- a
// filter fault is still cleanly FILTERED_OUT, an expop fault still
// OP_NOT_APPLICABLE. At verbosity 3 the server ADDS a field-45 key-3 eval-phase
// trace (phase=eval, outcome=fault, op, path, snippet) PLUS the tier-2 detail
// message ("integer division by zero" etc.). The client skips key 3 as an
// unknown sub-key (as_command_parse_error_details), so it surfaces the status +
// message and the trace bytes round-trip harmlessly; the trace's internal
// fields are decoded/asserted by the server-side ErrorDetailsTest unit suite.

// Filter fault: gt(div(5, 0), 1). Literal operands -> faults for ANY record.
static as_status
case_filter_eval_fault_div_zero(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-filter-div0");
	put_int(as, &key, 1);

	as_exp_build(filter,
			as_exp_cmp_gt(as_exp_div(as_exp_int(5), as_exp_int(0)),
					as_exp_int(1)));

	as_policy_read pol = g_read_pol;
	pol.base.error_detail_verbosity = 3;
	pol.base.filter_exp = filter;

	as_record* rec = NULL;
	as_status s = aerospike_key_get(as, err, &pol, &key, &rec);

	as_record_destroy(rec);
	as_exp_destroy(filter);
	return s;
}

// Filter fault: gt(mod(5, 0), 1) -> integer modulo by zero (eval_mod site).
static as_status
case_filter_eval_fault_mod_zero(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-filter-mod0");
	put_int(as, &key, 1);

	as_exp_build(filter,
			as_exp_cmp_gt(as_exp_mod(as_exp_int(5), as_exp_int(0)),
					as_exp_int(1)));

	as_policy_read pol = g_read_pol;
	pol.base.error_detail_verbosity = 3;
	pol.base.filter_exp = filter;

	as_record* rec = NULL;
	as_status s = aerospike_key_get(as, err, &pol, &key, &rec);

	as_record_destroy(rec);
	as_exp_destroy(filter);
	return s;
}

// Tier gating: the SAME div-by-zero filter fault at verbosity 1. Status is
// still FILTERED_OUT, but no field-45 detail rides at all (tier < 2), so the
// eval message must NOT appear in err.message.
static as_status
case_filter_eval_fault_low_verbosity(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-filter-div0-lowv");
	put_int(as, &key, 1);

	as_exp_build(filter,
			as_exp_cmp_gt(as_exp_div(as_exp_int(5), as_exp_int(0)),
					as_exp_int(1)));

	as_policy_read pol = g_read_pol;
	pol.base.error_detail_verbosity = 1;
	pol.base.filter_exp = filter;

	as_record* rec = NULL;
	as_status s = aerospike_key_get(as, err, &pol, &key, &rec);

	as_record_destroy(rec);
	as_exp_destroy(filter);
	return s;
}

// Non-breaking tripwire: a filter over an ABSENT bin still cleanly filters --
// bin_int("missing") resolves to UNKNOWN (not a fault), so gt(<unknown>, 1) ->
// UNKNOWN -> FILTERED_OUT with NO eval message and NO trace.
static as_status
case_filter_absent_bin_no_fault(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-filter-absent");
	put_int(as, &key, 1); // has BIN; filter reads a DIFFERENT, missing bin

	as_exp_build(filter,
			as_exp_cmp_gt(as_exp_bin_int("missing"), as_exp_int(1)));

	as_policy_read pol = g_read_pol;
	pol.base.error_detail_verbosity = 3;
	pol.base.filter_exp = filter;

	as_record* rec = NULL;
	as_status s = aerospike_key_get(as, err, &pol, &key, &rec);

	as_record_destroy(rec);
	as_exp_destroy(filter);
	return s;
}

// Non-breaking tripwire: key() over a keyless record still cleanly filters. The
// record is written without storing its key (default), so key_int() resolves to
// UNKNOWN -> FILTERED_OUT, NOT a fault (eval_rec_key stays absent/UNK in v1).
static as_status
case_filter_keyless_key_no_fault(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-filter-keyless");
	put_int(as, &key, 1); // g_write_pol does not store the key on the server

	as_exp_build(filter,
			as_exp_cmp_eq(as_exp_key_int(), as_exp_int(7)));

	as_policy_read pol = g_read_pol;
	pol.base.error_detail_verbosity = 3;
	pol.base.filter_exp = filter;

	as_record* rec = NULL;
	as_status s = aerospike_key_get(as, err, &pol, &key, &rec);

	as_record_destroy(rec);
	as_exp_destroy(filter);
	return s;
}

// expop fault: a read-expression value op that faults -- exp_read("v",
// div(5,0)). The result fails to materialize -> AEROSPIKE_ERR_OP_NOT_APPLICABLE
// (status unchanged) + eval trace/message at verbosity 3. Distinct boundary
// from the filter path (as_exp_eval_to_result -> eval_op in expop.c).
static as_status
case_expop_eval_fault_div_zero(aerospike* as, as_error* err)
{
	as_key key;
	as_key_init(&key, g_namespace, g_set, "edk-expop-div0");
	put_int(as, &key, 1);

	as_exp_build(exp, as_exp_div(as_exp_int(5), as_exp_int(0)));

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_exp_read(&ops, "v", exp, AS_EXP_READ_DEFAULT);

	as_policy_operate pol = g_op_pol;
	pol.base.error_detail_verbosity = 3;

	as_record* rec = NULL;
	as_status s = aerospike_key_operate(as, err, &pol, &key, &ops, &rec);

	as_record_destroy(rec);
	as_operations_destroy(&ops);
	as_exp_destroy(exp);
	return s;
}


//==========================================================
// Case table.
//
// Subcodes (per-status enums in aerospike-server/as/include/base/proto.h
// as of the 2026-06-02 per-status redesign; integers are members of the
// parent AS_ERR_* status's enum, dense from 1, with AS_SUB_NONE = 0
// universal):
//
//   AS_ERR_PARAMETER (4):
//     2  AS_SUB_PARAM_BITS_OFFSET_OUT_OF_RANGE
//     3  AS_SUB_PARAM_BITS_SIZE_OUT_OF_RANGE
//   AS_ERR_BIN_NOT_FOUND (17):
//     1  AS_SUB_BIN_NOT_FOUND_HLL_CANNOT_CREATE_WITH_OP
//   AS_ERR_OP_NOT_APPLICABLE (26):
//     1  AS_SUB_OPNOT_CDT_INDEX_OUT_OF_BOUNDS
//     2  AS_SUB_OPNOT_CDT_RANK_OUT_OF_BOUNDS
//     3  AS_SUB_OPNOT_CDT_BOUNDED_LIST_OVERFLOW
//     8  AS_SUB_OPNOT_HLL_FOLD_INDEX_BITS_TOO_LARGE
//   AS_SUB_NONE (0): status-is-canonical, or no app-dispatch use case;
//     the message text carries the disambiguating context. Used under
//     INCOMPATIBLE_TYPE, RECORD_EXISTS, RECORD_NOT_FOUND, GENERATION,
//     FILTERED_OUT (filter misses; as_sub_filtered_t removed 2026-06-25),
//     and at COLLAPSE sites under broader statuses (e.g. the old
//     CDT_MAP_KEY_EXISTS / CDT_MAP_KEY_NOT_FOUND / HLL_PARSE_FAILED /
//     CDT_LIST_ELEMENT_EXISTS / CDT_CONTEXT_EVAL_TYPE_MISMATCH paths,
//     which had distinct subcodes pre-redesign).
//

static const error_case CASES[] = {
	// --- Particle modify type mismatches ---
	// All collapse to AS_SUB_NONE: the status is canonical and the message
	// carries the disambiguating context, so no subcode is sent.
	{ "append str on int bin",
	  AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE, true, 0, "append",
	  case_append_str_on_int },
	{ "incr on string bin",
	  AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE, true, 0, "increment",
	  case_incr_on_str },
	{ "prepend str on int bin",
	  AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE, true, 0, "prepend",
	  case_prepend_str_on_int },
	{ "incr double on int bin",
	  AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE, true, 0, NULL,
	  case_incr_double_on_int },
	{ "hll add on int bin",
	  AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE, true, 0, NULL,
	  case_hll_add_on_int },

	// --- CDT list ops ---
	{ "list get index out of bounds",
	  AEROSPIKE_ERR_OP_NOT_APPLICABLE, false, 1, NULL,
	  case_list_get_index_out_of_bounds },
	{ "list get_by_rank out of bounds",
	  AEROSPIKE_ERR_OP_NOT_APPLICABLE, false, 2, NULL,
	  case_list_get_by_rank_out_of_bounds },
	{ "list pop index out of bounds",
	  AEROSPIKE_ERR_OP_NOT_APPLICABLE, false, 1, NULL,
	  case_list_pop_index_out_of_bounds },
	{ "list append add_unique violation",
	  AEROSPIKE_ERR_FAIL_ELEMENT_EXISTS, true, 0, NULL,
	  case_list_insert_unique_violation },
	{ "list insert bounded overflow",
	  AEROSPIKE_ERR_OP_NOT_APPLICABLE, false, 3, NULL,
	  case_list_bounded_overflow },
	{ "list op on raw-bytes bin (wrong type)",
	  AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE, true, 0, NULL,
	  case_list_op_on_corrupt_bin },

	// --- CDT map ops ---
	{ "map put create_only on existing key",
	  AEROSPIKE_ERR_FAIL_ELEMENT_EXISTS, true, 0, NULL,
	  case_map_put_create_only_existing },
	{ "map put update_only on missing key",
	  AEROSPIKE_ERR_FAIL_ELEMENT_NOT_FOUND, true, 0, NULL,
	  case_map_put_update_only_missing },
	{ "map op on list bin (wrong type)",
	  AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE, true, 0, NULL,
	  case_map_op_on_list_bin },
	{ "map op on raw-bytes bin",
	  AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE, true, 0, NULL,
	  case_map_op_on_corrupt_bin },
	{ "list ctx into string map value (type mismatch)",
	  AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE, true, 0, NULL,
	  case_map_ctx_into_map_value },
	// --- Bits ops ---
	{ "bit get offset out of range",
	  AEROSPIKE_ERR_REQUEST_INVALID, false, 2, NULL,
	  case_bits_get_offset_out_of_range },
	{ "bit get size zero",
	  AEROSPIKE_ERR_REQUEST_INVALID, false, 3, NULL,
	  case_bits_get_size_zero },

	// --- HLL ops ---
	{ "hll init parses invalid index bits",
	  AEROSPIKE_ERR_REQUEST_INVALID, true, 0, NULL,
	  case_hll_init_invalid_index_bits },
	{ "hll fold target larger than current",
	  AEROSPIKE_ERR_OP_NOT_APPLICABLE, false, 8, NULL,
	  case_hll_fold_target_too_large },
	{ "hll op on raw-bytes bin",
	  AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE, true, 0, NULL,
	  case_hll_op_on_corrupt_bin },
	{ "hll refresh_count on missing bin",
	  AEROSPIKE_ERR_BIN_NOT_FOUND, false, 1, NULL,
	  case_hll_refresh_count_no_hll_bin },

	// --- Write / delete / read policy ---
	{ "write create_only on existing record",
	  AEROSPIKE_ERR_RECORD_EXISTS, true, 0, NULL,
	  case_write_create_only_existing },
	{ "write replace_only on missing record",
	  AEROSPIKE_ERR_RECORD_NOT_FOUND, true, 0, NULL,
	  case_write_replace_only_missing },
	{ "write generation mismatch",
	  AEROSPIKE_ERR_RECORD_GENERATION, true, 0, NULL,
	  case_write_generation_mismatch },
	{ "delete generation mismatch",
	  AEROSPIKE_ERR_RECORD_GENERATION, true, 0, NULL,
	  case_delete_generation_mismatch },
	{ "read filtered out by filter_exp",
	  AEROSPIKE_FILTERED_OUT, true, 0, "filtered out",
	  case_read_filtered_out },
	{ "operate filtered out by filter_exp",
	  AEROSPIKE_FILTERED_OUT, true, 0, "filtered out",
	  case_operate_filtered_out },
	// --- Expression build (compile) failures: AS_ERR_PARAMETER, AS_SUB_NONE,
	// build-phase detail message (+ field-45 expression trace at verbosity 3). ---
	{ "filter expression fails to build",
	  AEROSPIKE_ERR_REQUEST_INVALID, true, 0,
	  "invalid metadata expression in request", case_filter_build_fail },
	{ "exp_write operation fails to build",
	  AEROSPIKE_ERR_REQUEST_INVALID, true, 0,
	  "invalid expression in operation request", case_exp_op_build_fail },
	// --- SERVER-1138 runtime eval faults (verbosity 3): status unchanged +
	// AS_SUB_NONE + eval-fault message; field-45 key-3 trace rides too (the
	// client skips it as an unknown sub-key). ---
	{ "filter eval fault: div by zero -> FILTERED_OUT + trace",
	  AEROSPIKE_FILTERED_OUT, true, 0,
	  "integer division by zero", case_filter_eval_fault_div_zero },
	{ "filter eval fault: mod by zero -> FILTERED_OUT + trace",
	  AEROSPIKE_FILTERED_OUT, true, 0,
	  "integer modulo by zero", case_filter_eval_fault_mod_zero },
	{ "expop eval fault: div by zero -> OP_NOT_APPLICABLE + trace",
	  AEROSPIKE_ERR_OP_NOT_APPLICABLE, true, 0,
	  "integer division by zero", case_expop_eval_fault_div_zero },
	// --- SERVER-1138 non-breaking tripwires / tier floor: still cleanly
	// filtered, NO fault detail (the must-be-absent message is checked
	// explicitly in run_eval_fault_positive_checks). ---
	{ "tier gating: div-by-zero filter at verbosity 1 -> no eval detail",
	  AEROSPIKE_FILTERED_OUT, true, 0, NULL,
	  case_filter_eval_fault_low_verbosity },
	{ "non-breaking: absent bin filter -> FILTERED_OUT, no fault",
	  AEROSPIKE_FILTERED_OUT, true, 0, NULL,
	  case_filter_absent_bin_no_fault },
	{ "non-breaking: keyless key() filter -> FILTERED_OUT, no fault",
	  AEROSPIKE_FILTERED_OUT, true, 0, NULL,
	  case_filter_keyless_key_no_fault },
};

static const uint32_t N_CASES = (uint32_t)(sizeof(CASES) / sizeof(CASES[0]));


//==========================================================
// Driver.
//

// Check err against expectations from CASES[i]. Returns true on pass.
static bool
check_case(const error_case* c, as_status got, const as_error* err)
{
	bool ok = true;

	if (got != c->expected_status) {
		LOG("    status: got %d want %d (message: %s)",
				got, c->expected_status, err->message);
		ok = false;
	}

	if (c->subcode_absent) {
		// AS_SUB_NONE is omitted on the wire, so the client never formats a
		// "subcode=N" suffix. Assert the absence rather than "subcode=0".
		if (strstr(err->message, "subcode=") != NULL) {
			LOG("    subcode: expected none, but got one in '%s'",
					err->message);
			ok = false;
		}
	}
	else {
		char want_sub[32];
		snprintf(want_sub, sizeof(want_sub), "subcode=%u", c->expected_subcode);

		if (strstr(err->message, want_sub) == NULL) {
			LOG("    subcode: expected '%s' in '%s'", want_sub, err->message);
			ok = false;
		}
	}

	if (c->expected_msg_substr != NULL &&
			strstr(err->message, c->expected_msg_substr) == NULL) {
		LOG("    message: expected substring '%s' in '%s'",
				c->expected_msg_substr, err->message);
		ok = false;
	}

	return ok;
}

//==========================================================
// Batch error-details checks.
//
// Batch surfaces per-row detail differently from single-record: the server
// serializes field 45 into each error row's reply, and the client folds it into
// that record's error_subcode / error_message (NOT the top-level as_error). The
// opt-in is batch-wide via the parent batch policy's error_detail_verbosity.
// These checks run a mixed-row batch and assert: an error row carries the per-
// row detail when opted in, a success row carries none, and opt-in OFF carries
// none. Returns the number of failed checks.
//

static uint32_t
run_batch_cases(aerospike* as)
{
	uint32_t failed = 0;
	as_error err;

	as_key k_err;
	as_key k_ok;
	as_key_init(&k_err, g_namespace, g_set, "edk-batch-err");
	as_key_init(&k_ok, g_namespace, g_set, "edk-batch-ok");
	put_int(as, &k_err, 1);
	put_int(as, &k_ok, 1);

	// Append a string to an int bin -> bin-incompatible error on that row.
	as_operations bad;
	as_operations_inita(&bad, 1);
	as_operations_add_append_str(&bad, BIN, "bad");

	// A valid increment -> success row.
	as_operations good;
	as_operations_inita(&good, 1);
	as_operations_add_incr(&good, BIN, 1);

	// --- opt-in ON: detail rides the error row only. ---
	as_policy_batch bp;
	as_policy_batch_init(&bp);
	bp.base.error_detail_verbosity = 2;
	bp.respond_all_keys = true; // per-record mode: every row (incl. error) sent

	as_batch_records recs;
	as_batch_records_inita(&recs, 2);

	as_batch_write_record* w0 = as_batch_write_reserve(&recs);
	as_key_init(&w0->key, g_namespace, g_set, "edk-batch-err");
	w0->ops = &bad;

	as_batch_write_record* w1 = as_batch_write_reserve(&recs);
	as_key_init(&w1->key, g_namespace, g_set, "edk-batch-ok");
	w1->ops = &good;

	aerospike_batch_write(as, &err, &bp, &recs);

	as_batch_base_record* e = (as_batch_base_record*)as_vector_get(&recs.list, 0);
	as_batch_base_record* o = (as_batch_base_record*)as_vector_get(&recs.list, 1);

	// The bin-incompatible error is authored AS_SUB_NONE + message, so the
	// per-row detail is message-only (subcode legitimately absent). Asserting
	// the message rode proves field 45 round-tripped per record.
	if (e->result == AEROSPIKE_OK) {
		LOG("FAIL batch_optin_error_row: row unexpectedly succeeded");
		failed++;
	}
	else if (e->error_message == NULL || e->error_message[0] == 0) {
		LOG("FAIL batch_optin_error_row: missing per-row message (result=%d)",
				e->result);
		failed++;
	}
	else {
		LOG("PASS batch_optin_error_row (result=%d subcode=%u msg='%s')",
				e->result, e->error_subcode, e->error_message);
	}

	if (o->error_subcode != 0 || o->error_message != NULL) {
		LOG("FAIL batch_success_row: unexpected detail (subcode=%u msg=%s)",
				o->error_subcode,
				o->error_message ? o->error_message : "(null)");
		failed++;
	}
	else {
		LOG("PASS batch_success_row");
	}

	as_batch_records_destroy(&recs);

	// --- opt-in OFF: no detail anywhere (byte-identical legacy reply). ---
	as_policy_batch bp_off;
	as_policy_batch_init(&bp_off);
	bp_off.base.error_detail_verbosity = 0;
	bp_off.respond_all_keys = true;

	as_batch_records recs2;
	as_batch_records_inita(&recs2, 1);
	as_batch_write_record* w2 = as_batch_write_reserve(&recs2);
	as_key_init(&w2->key, g_namespace, g_set, "edk-batch-err");
	w2->ops = &bad;

	aerospike_batch_write(as, &err, &bp_off, &recs2);

	as_batch_base_record* e2 = (as_batch_base_record*)as_vector_get(&recs2.list, 0);

	if (e2->error_subcode != 0 || e2->error_message != NULL) {
		LOG("FAIL batch_optin_off: detail leaked at verbosity 0 (subcode=%u)",
				e2->error_subcode);
		failed++;
	}
	else {
		LOG("PASS batch_optin_off");
	}

	as_batch_records_destroy(&recs2);
	as_operations_destroy(&bad);
	as_operations_destroy(&good);
	as_key_destroy(&k_err);
	as_key_destroy(&k_ok);

	return failed;
}

//==========================================================
// SERVER-1138 positive (must-SUCCEED) eval-fault checks.
//
// The CASES[] loop models only FAILING ops. Two SERVER-1138 behaviors are
// proved by SUCCESS, so they get their own checks (mirrors run_batch_cases):
//   - or(<faulting>, TRUE): the non-breaking invariant -- a Kleene OR with a
//     determining TRUE sibling must still MATCH, not be flipped to filtered.
//   - exp_read(div(5,0)) with EVAL_NO_FAIL: fault swallowed, op SUCCEEDS, no
//     detail.
// Plus a stronger negative check on the three "no fault" filter paths: the
// eval-fault message must be genuinely ABSENT from err.message.
//
// Returns the number of failed checks.
//

static uint32_t
run_eval_fault_positive_checks(aerospike* as)
{
	uint32_t failed = 0;

	// --- or(div(5,0), TRUE) must still MATCH (record is read). ---
	{
		as_key key;
		as_key_init(&key, g_namespace, g_set, "edk-or-fault-true");
		put_int(as, &key, 1);

		as_exp_build(filter,
				as_exp_or(
						as_exp_cmp_gt(as_exp_div(as_exp_int(5), as_exp_int(0)),
								as_exp_int(1)),
						as_exp_bool(true)));

		as_policy_read pol = g_read_pol;
		pol.base.error_detail_verbosity = 3;
		pol.base.filter_exp = filter;

		as_error err;
		as_error_reset(&err);
		as_record* rec = NULL;
		as_status s = aerospike_key_get(as, &err, &pol, &key, &rec);

		if (s != AEROSPIKE_OK) {
			LOG("FAIL eval_or_fault_true_matches: got %d - %s "
					"(must MATCH despite absorbed div-by-zero)", s, err.message);
			failed++;
		}
		else {
			LOG("PASS eval_or_fault_true_matches (record read OK)");
		}

		as_record_destroy(rec);
		as_exp_destroy(filter);
	}

	// --- exp_read(div(5,0)) with EVAL_NO_FAIL must SUCCEED, no detail. ---
	{
		as_key key;
		as_key_init(&key, g_namespace, g_set, "edk-expop-nofail");
		put_int(as, &key, 1);

		as_exp_build(exp, as_exp_div(as_exp_int(5), as_exp_int(0)));

		as_operations ops;
		as_operations_inita(&ops, 1);
		as_operations_exp_read(&ops, "v", exp, AS_EXP_READ_EVAL_NO_FAIL);

		as_policy_operate pol = g_op_pol;
		pol.base.error_detail_verbosity = 3;

		as_error err;
		as_error_reset(&err);
		as_record* rec = NULL;
		as_status s = aerospike_key_operate(as, &err, &pol, &key, &ops, &rec);

		if (s != AEROSPIKE_OK) {
			LOG("FAIL expop_eval_no_fail_succeeds: got %d - %s "
					"(div-by-zero must be swallowed)", s, err.message);
			failed++;
		}
		else {
			LOG("PASS expop_eval_no_fail_succeeds (no detail, op OK)");
		}

		as_record_destroy(rec);
		as_operations_destroy(&ops);
		as_exp_destroy(exp);
	}

	// --- no-fault filter paths: the eval message must be ABSENT. ---
	{
		struct { const char* name; case_fn fn; } checks[] = {
			{ "absent-bin filter carries no eval message",
					case_filter_absent_bin_no_fault },
			{ "keyless key() filter carries no eval message",
					case_filter_keyless_key_no_fault },
			{ "verbosity-1 div-by-zero carries no eval message",
					case_filter_eval_fault_low_verbosity },
		};

		for (uint32_t i = 0; i < sizeof(checks) / sizeof(checks[0]); i++) {
			as_error err;
			as_error_reset(&err);

			as_status s = checks[i].fn(as, &err);

			if (s != AEROSPIKE_FILTERED_OUT) {
				LOG("FAIL %s: status %d (want FILTERED_OUT)", checks[i].name, s);
				failed++;
			}
			else if (strstr(err.message, "division by zero") != NULL) {
				LOG("FAIL %s: eval message leaked in '%s'", checks[i].name,
						err.message);
				failed++;
			}
			else {
				LOG("PASS %s", checks[i].name);
			}
		}
	}

	return failed;
}

int
main(int argc, char* argv[])
{
	if (! example_get_opts(argc, argv, EXAMPLE_BASIC_OPTS)) {
		exit(-1);
	}

	aerospike as;
	example_connect_to_aerospike(&as);

	init_policies();

	uint32_t passed = 0;
	uint32_t failed = 0;

	for (uint32_t i = 0; i < N_CASES; i++) {
		const error_case* c = &CASES[i];

		as_error err;
		as_error_reset(&err);

		as_status got = c->run(&as, &err);

		if (got == AEROSPIKE_OK) {
			LOG("FAIL %s: call unexpectedly succeeded", c->name);
			failed++;
			continue;
		}

		if (check_case(c, got, &err)) {
			LOG("PASS %s", c->name);
			passed++;
		}
		else {
			LOG("FAIL %s", c->name);
			failed++;
		}
	}

	// Batch per-row error details (separate harness - details land per record,
	// not in the top-level as_error).
	LOG("--- batch ---");
	uint32_t batch_failed = run_batch_cases(&as);
	failed += batch_failed;

	// SERVER-1138 positive eval-fault checks (must-succeed + must-be-absent).
	LOG("--- SERVER-1138 eval-fault positive checks ---");
	uint32_t eval_failed = run_eval_fault_positive_checks(&as);
	failed += eval_failed;

	example_cleanup(&as);

	LOG("---");
	LOG("%u passed, %u failed (of %u single-record cases + 3 batch checks "
			"+ 5 eval-fault checks)", passed, failed, N_CASES);

	return failed == 0 ? 0 : 1;
}
