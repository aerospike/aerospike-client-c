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
#include <aerospike/aerospike_key.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_bit_operations.h>
#include <aerospike/as_cdt_ctx.h>
#include <aerospike/as_error.h>
#include <aerospike/as_exp.h>
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
	uint32_t    expected_subcode;
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
// All map to AS_SUB_RW_BIN_TYPE_INCOMPATIBLE (1100) -- the message
// substring is what distinguishes which op was rejected.
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
// the server, not an error path -- so it can't drive AS_SUB_CDT_VALUES_NOT_FOUND
// from the client. That subcode fires from internal index-find failures
// not reachable through ordinary client requests.)


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


//==========================================================
// Case table.
//
// Subcodes (mirror AS_SUB_* in aerospike-server/as/include/base/proto.h):
//   1100  AS_SUB_RW_BIN_TYPE_INCOMPATIBLE
//   1101  AS_SUB_RW_OP_NOT_APPLICABLE
//   1102  AS_SUB_RW_ELEMENT_NOT_FOUND
//   1103  AS_SUB_RW_ELEMENT_EXISTS
//   1025  AS_SUB_RW_RECORD_EXISTS      (write create_only collision)
//   1700  AS_SUB_RW_FILTERED
//   1701  AS_SUB_RW_GENERATION_MISMATCH
//   1714  AS_SUB_RW_RECORD_NOT_FOUND   (write replace_only on missing)
//   3010  AS_SUB_CDT_INDEX_OUT_OF_BOUNDS
//   3011  AS_SUB_CDT_RANK_OUT_OF_BOUNDS
//   3012  AS_SUB_CDT_BOUNDED_LIST_OVERFLOW
//   3102  AS_SUB_CDT_CONTEXT_EVAL_TYPE_MISMATCH
//   3106  AS_SUB_CDT_MAP_KEY_EXISTS
//   3107  AS_SUB_CDT_MAP_KEY_NOT_FOUND
//   3108  AS_SUB_CDT_LIST_ELEMENT_EXISTS
//   3109  AS_SUB_CDT_VALUES_NOT_FOUND
//   4003  AS_SUB_HLL_CANNOT_CREATE_WITH_OP
//   4005  AS_SUB_HLL_FOLD_INDEX_BITS_TOO_LARGE
//   4104  AS_SUB_HLL_PARSE_FAILED
//   5000  AS_SUB_BITS_OFFSET_OUT_OF_RANGE
//   5001  AS_SUB_BITS_SIZE_OUT_OF_RANGE
//

static const error_case CASES[] = {
	// --- Particle modify type mismatches ---
	{ "append str on int bin",
	  AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE, 1100, "append",
	  case_append_str_on_int },
	{ "incr on string bin",
	  AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE, 1100, "increment",
	  case_incr_on_str },
	{ "prepend str on int bin",
	  AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE, 1100, "prepend",
	  case_prepend_str_on_int },
	{ "incr double on int bin",
	  AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE, 1100, NULL,
	  case_incr_double_on_int },
	{ "hll add on int bin",
	  AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE, 1100, NULL,
	  case_hll_add_on_int },

	// --- CDT list ops ---
	{ "list get index out of bounds",
	  AEROSPIKE_ERR_OP_NOT_APPLICABLE, 3010, NULL,
	  case_list_get_index_out_of_bounds },
	{ "list get_by_rank out of bounds",
	  AEROSPIKE_ERR_OP_NOT_APPLICABLE, 3011, NULL,
	  case_list_get_by_rank_out_of_bounds },
	{ "list pop index out of bounds",
	  AEROSPIKE_ERR_OP_NOT_APPLICABLE, 3010, NULL,
	  case_list_pop_index_out_of_bounds },
	{ "list append add_unique violation",
	  AEROSPIKE_ERR_FAIL_ELEMENT_EXISTS, 3108, NULL,
	  case_list_insert_unique_violation },
	{ "list insert bounded overflow",
	  AEROSPIKE_ERR_OP_NOT_APPLICABLE, 3012, NULL,
	  case_list_bounded_overflow },
	{ "list op on raw-bytes bin (wrong type)",
	  AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE, 1100, NULL,
	  case_list_op_on_corrupt_bin },

	// --- CDT map ops ---
	{ "map put create_only on existing key",
	  AEROSPIKE_ERR_FAIL_ELEMENT_EXISTS, 3106, NULL,
	  case_map_put_create_only_existing },
	{ "map put update_only on missing key",
	  AEROSPIKE_ERR_FAIL_ELEMENT_NOT_FOUND, 3107, NULL,
	  case_map_put_update_only_missing },
	{ "map op on list bin (wrong type)",
	  AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE, 1100, NULL,
	  case_map_op_on_list_bin },
	{ "map op on raw-bytes bin",
	  AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE, 1100, NULL,
	  case_map_op_on_corrupt_bin },
	{ "list ctx into string map value (type mismatch)",
	  AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE, 3102, NULL,
	  case_map_ctx_into_map_value },
	// --- Bits ops ---
	{ "bit get offset out of range",
	  AEROSPIKE_ERR_REQUEST_INVALID, 5000, NULL,
	  case_bits_get_offset_out_of_range },
	{ "bit get size zero",
	  AEROSPIKE_ERR_REQUEST_INVALID, 5001, NULL,
	  case_bits_get_size_zero },

	// --- HLL ops ---
	{ "hll init parses invalid index bits",
	  AEROSPIKE_ERR_REQUEST_INVALID, 4104, NULL,
	  case_hll_init_invalid_index_bits },
	{ "hll fold target larger than current",
	  AEROSPIKE_ERR_OP_NOT_APPLICABLE, 4005, NULL,
	  case_hll_fold_target_too_large },
	{ "hll op on raw-bytes bin",
	  AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE, 1100, NULL,
	  case_hll_op_on_corrupt_bin },
	{ "hll refresh_count on missing bin",
	  AEROSPIKE_ERR_BIN_NOT_FOUND, 4003, NULL,
	  case_hll_refresh_count_no_hll_bin },

	// --- Write / delete / read policy ---
	{ "write create_only on existing record",
	  AEROSPIKE_ERR_RECORD_EXISTS, 1025, NULL,
	  case_write_create_only_existing },
	{ "write replace_only on missing record",
	  AEROSPIKE_ERR_RECORD_NOT_FOUND, 1714, NULL,
	  case_write_replace_only_missing },
	{ "write generation mismatch",
	  AEROSPIKE_ERR_RECORD_GENERATION, 1701, NULL,
	  case_write_generation_mismatch },
	{ "delete generation mismatch",
	  AEROSPIKE_ERR_RECORD_GENERATION, 1701, NULL,
	  case_delete_generation_mismatch },
	{ "read filtered out by filter_exp",
	  AEROSPIKE_FILTERED_OUT, 1700, NULL,
	  case_read_filtered_out },
	{ "operate filtered out by filter_exp",
	  AEROSPIKE_FILTERED_OUT, 1700, NULL,
	  case_operate_filtered_out },
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

	char want_sub[32];
	snprintf(want_sub, sizeof(want_sub), "subcode=%u", c->expected_subcode);

	if (strstr(err->message, want_sub) == NULL) {
		LOG("    subcode: expected '%s' in '%s'", want_sub, err->message);
		ok = false;
	}

	if (c->expected_msg_substr != NULL &&
			strstr(err->message, c->expected_msg_substr) == NULL) {
		LOG("    message: expected substring '%s' in '%s'",
				c->expected_msg_substr, err->message);
		ok = false;
	}

	return ok;
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

	example_cleanup(&as);

	LOG("---");
	LOG("%u passed, %u failed (of %u cases)", passed, failed, N_CASES);

	return failed == 0 ? 0 : 1;
}
