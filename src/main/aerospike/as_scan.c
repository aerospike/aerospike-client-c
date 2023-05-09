/*
 * Copyright 2008-2023 Aerospike, Inc.
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
#include <aerospike/as_scan.h>
#include <aerospike/as_atomic.h>
#include <aerospike/as_cdt_internal.h>
#include <aerospike/as_operations.h>
#include <citrusleaf/alloc.h>

//---------------------------------
// Init/Destroy
//---------------------------------

static as_scan*
as_scan_defaults(as_scan* scan, bool free, const char* ns, const char* set)
{
	if (scan == NULL) return scan;

	scan->_free = free;

	if ( strlen(ns) < AS_NAMESPACE_MAX_SIZE ) {
		strcpy(scan->ns, ns);
	}
	else {
		scan->ns[0] = '\0';
	}
	
	//check set==NULL and set name length
	if ( set && strlen(set) < AS_SET_MAX_SIZE ) {
		strcpy(scan->set, set);
	}
	else {
		scan->set[0] = '\0';
	}
	
	scan->select._free = false;
	scan->select.capacity = 0;
	scan->select.size = 0;
	scan->select.entries = NULL;

	scan->ops = NULL;
	scan->no_bins = AS_SCAN_NOBINS_DEFAULT;
	scan->concurrent = AS_SCAN_CONCURRENT_DEFAULT;
	scan->deserialize_list_map = AS_SCAN_DESERIALIZE_DEFAULT;
	
	as_udf_call_init(&scan->apply_each, NULL, NULL, NULL);

	scan->parts_all = NULL;
	scan->ttl = 0;
	scan->paginate = false;

	return scan;
}

as_scan*
as_scan_new(const char* ns, const char* set)
{
	as_scan* scan = (as_scan *) cf_malloc(sizeof(as_scan));
	if ( ! scan ) return NULL;
	return as_scan_defaults(scan, true, ns, set);
}

as_scan*
as_scan_init(as_scan* scan, const char* ns, const char* set)
{
	if ( !scan ) return scan;
	return as_scan_defaults(scan, false, ns, set);
}

void
as_scan_destroy(as_scan* scan)
{
	if ( !scan ) return;

	scan->ns[0] = '\0';
	scan->set[0] = '\0';

	if ( scan->select._free ) {
		cf_free(scan->select.entries);
	}

	as_udf_call_destroy(&scan->apply_each);

	if (scan->ops) {
		as_operations_destroy(scan->ops);
	}

	if (scan->parts_all) {
		as_partitions_status_release(scan->parts_all);
	}

	// If the whole structure should be freed
	if ( scan->_free ) {
		cf_free(scan);
	}
}

//---------------------------------
// Select
//---------------------------------

bool
as_scan_select_init(as_scan* scan, uint16_t n)
{
	if ( !scan ) return false;
	if ( scan->select.entries ) return false;

	scan->select.entries = (as_bin_name *) cf_calloc(n, sizeof(as_bin_name));
	if ( !scan->select.entries ) return false;

	scan->select._free = true;
	scan->select.capacity = n;
	scan->select.size = 0;

	return true;
}

bool
as_scan_select(as_scan* scan, const char * bin)
{
	// test preconditions
	if ( !scan || !bin || strlen(bin) >= AS_BIN_NAME_MAX_SIZE ) {
		return false;
	}

	// insufficient capacity
	if ( scan->select.size >= scan->select.capacity ) return false;

	strcpy(scan->select.entries[scan->select.size], bin);
	scan->select.size++;

	return true;
}

bool
as_scan_set_nobins(as_scan* scan, bool nobins)
{
	if ( !scan ) return false;
	scan->no_bins = nobins;
	return true;
}

bool
as_scan_set_concurrent(as_scan* scan, bool concurrent)
{
	if ( !scan ) return false;
	scan->concurrent = concurrent;
	return true;
}

bool
as_scan_apply_each(as_scan* scan, const char* module, const char* function, as_list* arglist)
{
	if ( !module || !function ) return false;
	as_udf_call_init(&scan->apply_each, module, function, arglist);
	return true;
}

//---------------------------------
// Scan Serialization
//---------------------------------

bool
as_scan_to_bytes(const as_scan* scan, uint8_t** bytes, uint32_t* bytes_size)
{
	as_packer pk = as_cdt_begin();
	as_pack_string(&pk, scan->ns);
	as_pack_string(&pk, scan->set);
	as_pack_uint64(&pk, scan->select.size);

	for (uint16_t i = 0; i < scan->select.size; i++) {
		as_pack_string(&pk, scan->select.entries[i]);
	}

	if (scan->apply_each.function[0]) {
		as_pack_bool(&pk, true);
		as_pack_string(&pk, scan->apply_each.module);
		as_pack_string(&pk, scan->apply_each.function);

		if (scan->apply_each.arglist) {
			as_pack_bool(&pk, true);
			as_pack_val(&pk, (as_val*)scan->apply_each.arglist);
		}
		else {
			as_pack_bool(&pk, false);
		}
	}
	else {
		as_pack_bool(&pk, false);
	}

	if (scan->ops) {
		as_pack_bool(&pk, true);
		as_pack_uint64(&pk, scan->ops->ttl);
		as_pack_uint64(&pk, scan->ops->gen);

		uint16_t max = scan->ops->binops.size;
		as_pack_uint64(&pk, max);

		for (uint16_t i = 0; i < max; i++) {
			as_binop* op = &scan->ops->binops.entries[i];
			as_pack_uint64(&pk, op->op);
			as_pack_string(&pk, op->bin.name);
			as_pack_val(&pk, (as_val*)op->bin.valuep);
		}
	}
	else {
		as_pack_bool(&pk, false);
	}

	if (scan->parts_all) {
		as_pack_bool(&pk, true);

		as_partitions_status* parts_all = scan->parts_all;
		as_pack_uint64(&pk, parts_all->part_count);
		as_pack_uint64(&pk, parts_all->part_begin);
		as_pack_bool(&pk, parts_all->done);

		for (uint16_t i = 0; i < parts_all->part_count; i++) {
			as_partition_status* ps = &parts_all->parts[i];
			as_pack_uint64(&pk, ps->part_id);
			as_pack_bool(&pk, ps->retry);
			as_pack_bool(&pk, ps->digest.init);
			as_pack_byte_string(&pk, ps->digest.value, AS_DIGEST_VALUE_SIZE);
			as_pack_uint64(&pk, ps->bval);
		}
	}
	else {
		as_pack_bool(&pk, false);
	}

	as_pack_uint64(&pk, scan->ttl);
	as_pack_bool(&pk, scan->paginate);
	as_pack_bool(&pk, scan->no_bins);
	as_pack_bool(&pk, scan->concurrent);
	as_pack_bool(&pk, scan->deserialize_list_map);

	as_cdt_end(&pk);

	*bytes = pk.buffer;
	*bytes_size = pk.offset;
	return true;
}

//---------------------------------
// Scan Deserialization
//---------------------------------

bool
as_scan_from_bytes(as_scan* scan, const uint8_t* bytes, uint32_t bytes_size)
{
	// Initialize scan so it can be safely destroyed if a failure occurs.
	// Assume scan passed on the stack.
	as_scan_defaults(scan, false, "", "");

	as_unpacker pk = {
		.buffer = bytes,
		.length = bytes_size,
		.offset = 0,
	};

	if (! as_unpack_str_init(&pk, scan->ns, AS_NAMESPACE_MAX_SIZE)) {
		return false;
	}

	if (! as_unpack_str_init(&pk, scan->set, AS_SET_MAX_SIZE)) {
		return false;
	}

	int64_t ival;
	uint64_t uval;

	// Unpack select
	if (as_unpack_uint64(&pk, &uval) != 0) {
		return false;
	}

	if (uval > 0) {
		scan->select.capacity = scan->select.size = (uint16_t)uval;
		scan->select.entries = cf_malloc(sizeof(as_bin_name) * scan->select.size);
		scan->select._free = true;

		for (uint16_t i = 0; i < scan->select.size; i++) {
			if (! as_unpack_str_init(&pk, scan->select.entries[i], AS_BIN_NAME_MAX_SIZE)) {
				goto HandleError;
			}
		}
	}

	// Query Apply
	bool b;

	if (as_unpack_boolean(&pk, &b) != 0) {
		goto HandleError;
	}

	if (b) {
		if (! as_unpack_str_init(&pk, scan->apply_each.module, AS_UDF_MODULE_MAX_SIZE)) {
			goto HandleError;
		}

		if (! as_unpack_str_init(&pk, scan->apply_each.function, AS_UDF_FUNCTION_MAX_SIZE)) {
			goto HandleError;
		}

		if (as_unpack_boolean(&pk, &b) != 0) {
			goto HandleError;
		}

		if (b) {
			as_val* listval = NULL;
			int rv = as_unpack_val(&pk, &listval);

			if (rv != 0 || !listval) {
				goto HandleError;
			}

			if (listval->type != AS_LIST) {
				as_val_destroy(listval);
				goto HandleError;
			}

			scan->apply_each.arglist = (as_list*)listval;
		}
		else {
			scan->apply_each.arglist = NULL;
		}
	}

	// Query Operations
	if (as_unpack_boolean(&pk, &b) != 0) {
		goto HandleError;
	}

	if (b) {
		scan->ops = cf_malloc(sizeof(as_operations));
		scan->ops->_free = true;

		if (as_unpack_uint64(&pk, &uval) != 0) {
			goto HandleError;
		}

		scan->ops->ttl = (uint32_t)uval;

		if (as_unpack_uint64(&pk, &uval) != 0) {
			goto HandleError;
		}

		scan->ops->gen = (uint16_t)uval;

		if (as_unpack_uint64(&pk, &uval) != 0) {
			goto HandleError;
		}

		uint16_t max = (uint16_t)uval;

		scan->ops->binops.capacity = max;
		scan->ops->binops.entries = cf_malloc(sizeof(as_binop) * max);
		scan->ops->binops._free = true;
		scan->ops->binops.size = 0;

		for (uint16_t i = 0; i < max; i++, scan->ops->binops.size++) {
			as_binop* op = &scan->ops->binops.entries[i];

			if (as_unpack_int64(&pk, &ival) != 0) {
				goto HandleError;
			}
			op->op = (as_operator)ival;

			if (! as_unpack_str_init(&pk, op->bin.name, AS_BIN_NAME_MAX_SIZE)) {
				goto HandleError;
			}

			as_val* val = NULL;
			int rv = as_unpack_val(&pk, &val);

			if (rv != 0 || !val) {
				goto HandleError;
			}
			op->bin.valuep = (as_bin_value*)val;
		}
	}
	else {
		scan->ops = NULL;
	}

	// Partitions Status
	if (as_unpack_boolean(&pk, &b) != 0) {
		goto HandleError;
	}

	if (b) {
		if (as_unpack_uint64(&pk, &uval) != 0) {
			goto HandleError;
		}

		uint16_t max = (uint16_t)uval;
		scan->parts_all = cf_malloc(sizeof(as_partitions_status) +
			(sizeof(as_partition_status) * max));

		scan->parts_all->ref_count = 1;
		scan->parts_all->part_count = max;

		if (as_unpack_uint64(&pk, &uval) != 0) {
			goto HandleError;
		}

		scan->parts_all->part_begin = (uint16_t)uval;

		if (as_unpack_boolean(&pk, &scan->parts_all->done) != 0) {
			goto HandleError;
		}

		scan->parts_all->retry = true;

		for (uint16_t i = 0; i < max; i++) {
			as_partition_status* ps = &scan->parts_all->parts[i];
			ps->replica_index = 0;
			ps->node = NULL;

			if (as_unpack_uint64(&pk, &uval) != 0) {
				goto HandleError;
			}

			ps->part_id = (uint16_t)uval;

			if (as_unpack_boolean(&pk, &ps->retry) != 0) {
				goto HandleError;
			}

			if (as_unpack_boolean(&pk, &ps->digest.init) != 0) {
				goto HandleError;
			}

			if (! as_unpack_bytes_init(&pk, ps->digest.value, AS_DIGEST_VALUE_SIZE)) {
				goto HandleError;
			}

			if (as_unpack_uint64(&pk, &ps->bval) != 0) {
				goto HandleError;
			}
		}
	}
	else {
		scan->parts_all = NULL;
	}

	if (as_unpack_uint64(&pk, &uval) != 0) {
		goto HandleError;
	}

	scan->ttl = (uint32_t)uval;

	if (as_unpack_boolean(&pk, &scan->paginate) != 0) {
		goto HandleError;
	}

	if (as_unpack_boolean(&pk, &scan->no_bins) != 0) {
		goto HandleError;
	}

	if (as_unpack_boolean(&pk, &scan->concurrent) != 0) {
		goto HandleError;
	}

	if (as_unpack_boolean(&pk, &scan->deserialize_list_map) != 0) {
		goto HandleError;
	}

	return true;

HandleError:
	as_scan_destroy(scan);
	return false;
}

as_scan*
as_scan_from_bytes_new(const uint8_t* bytes, uint32_t bytes_size)
{
	as_scan* scan = (as_scan*)cf_malloc(sizeof(as_scan));

	if (! as_scan_from_bytes(scan, bytes, bytes_size)) {
		cf_free(scan);
		return NULL;
	}
	scan->_free = true;
	return scan;
}

//---------------------------------
// Scan Compare
//---------------------------------

bool
as_scan_compare(as_scan* s1, as_scan* s2)
{
	if (s1->_free != s2->_free) {
		as_cmp_error();
	}

	if (strcmp(s1->ns, s2->ns) != 0) {
		as_cmp_error();
	}

	if (strcmp(s1->set, s2->set) != 0) {
		as_cmp_error();
	}

	if (s1->select._free != s2->select._free) {
		as_cmp_error();
	}

	if (s1->select.capacity != s2->select.capacity) {
		as_cmp_error();
	}

	if (s1->select.size != s2->select.size) {
		as_cmp_error();
	}

	for (uint16_t i = 0; i < s1->select.size; i++) {
		if (strcmp(s1->select.entries[i], s2->select.entries[i]) != 0) {
			as_cmp_error();
		}
	}

	if (s1->apply_each._free != s2->apply_each._free) {
		as_cmp_error();
	}

	if (strcmp(s1->apply_each.module, s2->apply_each.module) != 0) {
		as_cmp_error();
	}

	if (strcmp(s1->apply_each.function, s2->apply_each.function) != 0) {
		as_cmp_error();
	}

	if (s1->apply_each.arglist != s2->apply_each.arglist) {
		if (! as_val_compare((as_val*)s1->apply_each.arglist, (as_val*)s2->apply_each.arglist)) {
			as_cmp_error();
		}
	}

	if (s1->ops != s2->ops) {
		/* _free might be different if as_operations_inita() is used.
		if (s1->ops->_free != s2->ops->_free) {
			as_cmp_error();
		}
		*/

		if (s1->ops->gen != s2->ops->gen) {
			as_cmp_error();
		}

		if (s1->ops->ttl != s2->ops->ttl) {
			as_cmp_error();
		}

		if (s1->ops->binops.size != s2->ops->binops.size) {
			as_cmp_error();
		}

		for (uint16_t i = 0; i < s1->ops->binops.size; i++) {
			as_binop* op1 = &s1->ops->binops.entries[i];
			as_binop* op2 = &s2->ops->binops.entries[i];

			if (op1->op != op2->op) {
				as_cmp_error();
			}

			if (strcmp(op1->bin.name, op2->bin.name) != 0) {
				as_cmp_error();
			}

			if (op1->bin.valuep != op2->bin.valuep) {
				if (! as_val_compare((as_val*)op1->bin.valuep, (as_val*)op2->bin.valuep)) {
					as_cmp_error();
				}
			}
		}
	}

	if (s1->parts_all != s2->parts_all) {
		as_partitions_status* p1 = s1->parts_all;
		as_partitions_status* p2 = s2->parts_all;

		if (p1->ref_count != p2->ref_count) {
			as_cmp_error();
		}

		if (p1->part_begin != p2->part_begin) {
			as_cmp_error();
		}

		if (p1->part_count != p2->part_count) {
			as_cmp_error();
		}

		if (p1->done != p2->done) {
			as_cmp_error();
		}

		for (uint16_t i = 0; i < p1->part_count; i++) {
			as_partition_status* ps1 = &p1->parts[i];
			as_partition_status* ps2 = &p2->parts[i];

			if (ps1->part_id != ps2->part_id) {
				as_cmp_error();
			}

			if (ps1->retry != ps2->retry) {
				as_cmp_error();
			}

			if (ps1->bval != ps2->bval) {
				as_cmp_error();
			}

			if (ps1->replica_index != ps2->replica_index) {
				as_cmp_error();
			}

			if (ps1->digest.init != ps2->digest.init) {
				as_cmp_error();
			}

			if (ps1->digest.init) {
				if (memcmp(ps1->digest.value, ps2->digest.value, AS_DIGEST_VALUE_SIZE) != 0) {
					as_cmp_error();
				}
			}
		}
	}

	if (s1->ttl != s2->ttl) {
		as_cmp_error();
	}

	if (s1->paginate != s2->paginate) {
		as_cmp_error();
	}

	if (s1->no_bins != s2->no_bins) {
		as_cmp_error();
	}

	if (s1->concurrent != s2->concurrent) {
		as_cmp_error();
	}

	if (s1->deserialize_list_map != s2->deserialize_list_map) {
		as_cmp_error();
	}

	return true;
}
