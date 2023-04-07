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
#include <aerospike/as_query.h>
#include <aerospike/as_bin.h>
#include <aerospike/as_cdt_internal.h>
#include <aerospike/as_key.h>
#include <aerospike/as_log.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_udf.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_byte_order.h>
#include <stdarg.h>

//---------------------------------
// Init/Destroy
//---------------------------------

static as_query*
as_query_defaults(as_query* query, bool free, const char* ns, const char* set)
{
	query->_free = free;

	as_strncpy(query->ns, ns, AS_NAMESPACE_MAX_SIZE);
	as_strncpy(query->set, set, AS_SET_MAX_SIZE);

	query->select._free = false;
	query->select.capacity = 0;
	query->select.size = 0;
	query->select.entries = NULL;

	query->where._free = false;
	query->where.capacity = 0;
	query->where.size = 0;
	query->where.entries = NULL;

	query->ops = NULL;
	query->ttl = 0;
	query->no_bins = false;

	as_udf_call_init(&query->apply, NULL, NULL, NULL);

	query->parts_all = NULL;
	query->records_per_second = 0;
	query->max_records = 0;
	query->paginate = false;

	return query;
}

as_query*
as_query_init(as_query* query, const char* ns, const char* set)
{
	if ( !query ) return query;
	return as_query_defaults(query, false, ns, set);
}

as_query*
as_query_new(const char* ns, const char* set)
{
	as_query* query = (as_query *) cf_malloc(sizeof(as_query));
	if ( !query ) return query;
	return as_query_defaults(query, true, ns, set);
}

void
as_query_destroy(as_query* query)
{
	if (!query) {
		return;
	}

	query->ns[0] = '\0';
	query->set[0] = '\0';

	if (query->select.entries && query->select._free) {
		cf_free(query->select.entries);
	}

	query->select._free = false;
	query->select.capacity = 0;
	query->select.size = 0;
	query->select.entries = NULL;
	
	if (query->where.entries && query->where._free) {
		for (uint16_t i = 0; i < query->where.size; i++) {
			as_predicate* pred = &query->where.entries[i];

			if (pred->ctx && pred->ctx_free) {
				as_cdt_ctx_destroy(pred->ctx);
			}

			if ((pred->dtype == AS_INDEX_STRING || pred->dtype == AS_INDEX_GEO2DSPHERE) &&
				 pred->value.string_val._free) {
				cf_free(pred->value.string_val.string);
			}
		}
		cf_free(query->where.entries);
	}

	query->where._free = false;
	query->where.capacity = 0;
	query->where.size = 0;
	query->where.entries = NULL;

	as_udf_call_destroy(&query->apply);

	if (query->ops) {
		as_operations_destroy(query->ops);
	}

	if (query->parts_all) {
		as_partitions_status_release(query->parts_all);
	}

	if ( query->_free ) {
		cf_free(query);
	}
}

//---------------------------------
// Select
//---------------------------------

bool
as_query_select_init(as_query* query, uint16_t n)
{
	if ( !query ) return false;
	if ( query->select.entries ) return false;

	query->select.entries = (as_bin_name *) cf_calloc(n, sizeof(as_bin_name));
	if ( !query->select.entries ) return false;

	query->select._free = true;
	query->select.capacity = n;
	query->select.size = 0;

	return true;
}

bool
as_query_select(as_query* query, const char * bin)
{
	// test preconditions
	if ( !query || !bin || strlen(bin) >= AS_BIN_NAME_MAX_SIZE ) {
		return false;
	}

	// insufficient capacity
	if ( query->select.size >= query->select.capacity ) return false;

	strcpy(query->select.entries[query->select.size], bin);
	query->select.size++;

	return true;
}

//---------------------------------
// Where
//---------------------------------

bool
as_query_where_init(as_query* query, uint16_t n)
{
	if ( !query ) return false;
	if ( query->where.entries ) return false;

	query->where.entries = (as_predicate *) cf_calloc(n, sizeof(as_predicate));
	if ( !query->where.entries ) return false;

	query->where._free = true;
	query->where.capacity = n;
	query->where.size = 0;

	return true;
}

static bool
as_query_where_internal(
	as_query* query, const char* bin, as_cdt_ctx* ctx, as_predicate_type type, as_index_type itype,
	as_index_datatype dtype, va_list ap
	)
{
	// test preconditions
	if (! query || !bin || strlen(bin) >= AS_BIN_NAME_MAX_SIZE) {
		return false;
	}

	// insufficient capacity
	if (query->where.size >= query->where.capacity) {
		return false;
	}

	as_predicate* p = &query->where.entries[query->where.size++];
	bool status = true;

	strcpy(p->bin, bin);
	p->type = type;
	p->dtype = dtype;
	p->itype = itype;
	p->ctx = ctx;
	p->ctx_free = false; // Default to false to preserve legacy behavior.

	if (ctx) {
		as_packer pk = {.buffer = NULL, .capacity = UINT32_MAX};
		p->ctx_size = as_cdt_ctx_pack(ctx, &pk);

		if (p->ctx_size == 0) {
			return false;
		}
	}
	else {
		p->ctx_size = 0;
	}

	switch(type) {
	case AS_PREDICATE_EQUAL:
		if (dtype == AS_INDEX_STRING) {
			p->value.string_val.string = va_arg(ap, char*);
			p->value.string_val._free = false;
		}
		else if (dtype == AS_INDEX_NUMERIC) {
			p->value.integer = va_arg(ap, int64_t);
		}
		else {
			status = false;
		}
		break;
	case AS_PREDICATE_RANGE:
		if (dtype == AS_INDEX_NUMERIC) {
			p->value.integer_range.min = va_arg(ap, int64_t);
			p->value.integer_range.max = va_arg(ap, int64_t);
		}
		else if (dtype == AS_INDEX_GEO2DSPHERE) {
			p->value.string_val.string = va_arg(ap, char*);
			p->value.string_val._free = false;
		}
		else {
			status = false;
		}
		break;
	}

	return status;
}

bool
as_query_where(
	as_query* query, const char * bin, as_predicate_type type, as_index_type itype,
	as_index_datatype dtype, ...
	)
{
	va_list ap;
	va_start(ap, dtype);

	bool rv = as_query_where_internal(query, bin, NULL, type, itype, dtype, ap);

	va_end(ap);
	return rv;
}

bool
as_query_where_with_ctx(
	as_query* query, const char* bin, struct as_cdt_ctx* ctx, as_predicate_type type,
	as_index_type itype, as_index_datatype dtype, ...
	)
{
	va_list ap;
	va_start(ap, dtype);

	bool rv = as_query_where_internal(query, bin, ctx, type, itype, dtype, ap);

	va_end(ap);
	return rv;
}

//---------------------------------
// Query UDF
//---------------------------------

bool
as_query_apply(as_query* query, const char* module, const char* function, const as_list* arglist)
{
	if ( !query ) return false;
	as_udf_call_init(&query->apply, module, function, (as_list *) arglist);
	return true;
}

//---------------------------------
// Query Serialization
//---------------------------------

static inline void
pack_str(as_packer* pk, const char* s)
{
	uint32_t len = (uint32_t)strlen(s);
	as_pack_str(pk, (uint8_t*)s, len);
}

static inline void
pack_bytes(as_packer* pk, const uint8_t* b, uint32_t len)
{
	as_pack_str(pk, b, len);
}

bool
as_query_to_bytes(const as_query* query, uint8_t** bytes, size_t* bytes_size)
{
	as_packer pk = as_cdt_begin();
	pack_str(&pk, query->ns);
	pack_str(&pk, query->set);
	as_pack_uint64(&pk, query->select.size);

	for (uint16_t i = 0; i < query->select.size; i++) {
		pack_str(&pk, query->select.entries[i]);
	}

	as_pack_uint64(&pk, query->where.size);

	for (uint16_t i = 0; i < query->where.size; i++) {
		as_predicate* pred = &query->where.entries[i];
		pack_str(&pk, pred->bin);

		if (pred->ctx) {
			as_pack_bool(&pk, true);
			uint32_t size = as_cdt_ctx_pack(pred->ctx, &pk);
			as_pack_uint64(&pk, size);
		}
		else {
			as_pack_bool(&pk, false);
		}

		as_pack_int64(&pk, pred->type);

		switch(pred->type) {
			case AS_PREDICATE_EQUAL:
				if (pred->dtype == AS_INDEX_STRING) {
					pack_str(&pk, pred->value.string_val.string);
				}
				else if (pred->dtype == AS_INDEX_NUMERIC) {
					as_pack_int64(&pk, pred->value.integer);
				}
				break;

			case AS_PREDICATE_RANGE:
				if (pred->dtype == AS_INDEX_NUMERIC) {
					as_pack_int64(&pk, pred->value.integer_range.min);
					as_pack_int64(&pk, pred->value.integer_range.max);
				}
				else if (pred->dtype == AS_INDEX_GEO2DSPHERE) {
					pack_str(&pk, pred->value.string_val.string);
				}
				break;

			default:
				goto HandleError;
		}

		as_pack_int64(&pk, pred->dtype);
		as_pack_int64(&pk, pred->itype);
	}

	if (query->apply.function[0]) {
		as_pack_bool(&pk, true);
		pack_str(&pk, query->apply.module);
		pack_str(&pk, query->apply.function);

		if (query->apply.arglist) {
			as_pack_bool(&pk, true);
			as_pack_val(&pk, (as_val*)query->apply.arglist);
		}
		else {
			as_pack_bool(&pk, false);
		}
	}
	else {
		as_pack_bool(&pk, false);
	}

	if (query->ops) {
		as_pack_bool(&pk, true);

		uint16_t max = query->ops->binops.size;
		as_pack_uint64(&pk, max);

		for (uint16_t i = 0; i < max; i++) {
			as_binop* op = &query->ops->binops.entries[i];
			as_pack_uint64(&pk, op->op);
			pack_str(&pk, op->bin.name);
			as_pack_val(&pk, (as_val*)op->bin.valuep);
		}
	}
	else {
		as_pack_bool(&pk, false);
	}

	if (query->parts_all) {
		as_pack_bool(&pk, true);

		as_partitions_status* parts_all = query->parts_all;
		as_pack_uint64(&pk, parts_all->part_count);
		as_pack_uint64(&pk, parts_all->part_begin);
		as_pack_bool(&pk, parts_all->done);

		for (uint16_t i = 0; i < parts_all->part_count; i++) {
			as_partition_status* ps = &parts_all->parts[i];
			as_pack_uint64(&pk, ps->part_id);
			as_pack_bool(&pk, ps->retry);
			as_pack_bool(&pk, ps->digest.init);
			pack_bytes(&pk, ps->digest.value, AS_DIGEST_VALUE_SIZE);
			as_pack_uint64(&pk, ps->bval);
		}
	}
	else {
		as_pack_bool(&pk, false);
	}

	as_pack_uint64(&pk, query->max_records);
	as_pack_uint64(&pk, query->records_per_second);
	as_pack_uint64(&pk, query->ttl);
	as_pack_bool(&pk, query->paginate);
	as_pack_bool(&pk, query->no_bins);

	as_cdt_end(&pk);

	*bytes = pk.buffer;
	*bytes_size = pk.offset;
	return true;

HandleError:
	if (pk.buffer) {
		cf_free(pk.buffer);
	}
	*bytes = NULL;
	*bytes_size = 0;
	return false;
}

//---------------------------------
// Query Deserialization
//---------------------------------

static bool
unpack_str_init(as_unpacker* pk, char* str, uint32_t max)
{
	uint32_t size;
	const uint8_t* p = as_unpack_str(pk, &size);

	if (!p || size >= max) {
		return false;
	}

	memcpy(str, p, size);
	*(str + size) = 0;
	return true;
}

static bool
unpack_str_new(as_unpacker* pk, char** str, uint32_t max)
{
	uint32_t size;
	const uint8_t* p = as_unpack_str(pk, &size);

	if (!p || size >= max) {
		return false;
	}

	char* s = cf_malloc(size + 1);
	memcpy(s, p, size);
	*(s + size) = 0;
	*str = s;
	return true;
}

static bool
unpack_bytes_init(as_unpacker* pk, uint8_t* b, uint32_t max)
{
	uint32_t size;
	const uint8_t* p = as_unpack_str(pk, &size);

	if (!p || size > max) {
		return false;
	}

	memcpy(b, p, size);
	return true;
}

bool
as_query_from_bytes(as_query* query, const uint8_t* bytes, size_t bytes_size)
{
	// Initialize query so it can be safely destroyed if a failure occurs.
	// Assume query passed on the stack.
	as_query_defaults(query, false, "", "");

	as_unpacker pk = {
		.buffer = bytes,
		.length = bytes_size,
		.offset = 0,
	};

	if (! unpack_str_init(&pk, query->ns, AS_NAMESPACE_MAX_SIZE)) {
		return false;
	}

	if (! unpack_str_init(&pk, query->set, AS_SET_MAX_SIZE)) {
		return false;
	}

	int64_t ival;
	uint64_t uval;

	// Unpack select
	if (as_unpack_uint64(&pk, &uval) != 0) {
		return false;
	}

	query->select.capacity = query->select.size = (uint16_t)uval;
	query->select.entries = cf_malloc(sizeof(as_bin_name) * query->select.size);
	query->select._free = true;

	for (uint16_t i = 0; i < query->select.size; i++) {
		if (! unpack_str_init(&pk, query->select.entries[i], AS_BIN_NAME_MAX_SIZE)) {
			goto HandleError;
		}
	}

	// Unpack where
	if (as_unpack_uint64(&pk, &uval) != 0) {
		goto HandleError;
	}

	uint32_t max = (uint32_t)uval;
	query->where.capacity = (uint16_t)uval;
	query->where.entries = cf_malloc(sizeof(as_predicate) * max);
	query->where._free = true;

	as_predicate* pred = NULL;
	bool free_pred_string = false;
	bool b;

	for (uint16_t i = 0; i < max; i++, query->where.size++) {
		pred = &query->where.entries[i];
		pred->ctx = NULL;
		pred->ctx_size = 0;
		pred->ctx_free = false;
		free_pred_string = false;

		if (! unpack_str_init(&pk, pred->bin, AS_BIN_NAME_MAX_SIZE)) {
			goto HandleError;
		}

		if (as_unpack_boolean(&pk, &b) != 0) {
			goto HandleError;
		}

		if (b) {
			pred->ctx = cf_malloc(sizeof(as_cdt_ctx));
			pred->ctx_free = true;

			if (! as_cdt_ctx_from_unpacker(pred->ctx, &pk)) {
				goto HandlePredError;
			}

			if (as_unpack_uint64(&pk, &uval) != 0) {
				goto HandlePredError;
			}

			pred->ctx_size = (uint32_t)uval;
		}

		if (as_unpack_uint64(&pk, &uval) != 0) {
			goto HandlePredError;
		}

		pred->type = (as_predicate_type)uval;

		switch(pred->type) {
			case AS_PREDICATE_EQUAL:
				if (pred->dtype == AS_INDEX_STRING) {
					if (! unpack_str_new(&pk, &pred->value.string_val.string, 4096)) {
						goto HandlePredError;
					}
					pred->value.string_val._free = true;
					free_pred_string = true;
				}
				else if (pred->dtype == AS_INDEX_NUMERIC) {
					if (as_unpack_int64(&pk, &pred->value.integer) != 0) {
						goto HandlePredError;
					}
				}
				break;

			case AS_PREDICATE_RANGE:
				if (pred->dtype == AS_INDEX_NUMERIC) {
					if (as_unpack_int64(&pk, &pred->value.integer_range.min) != 0) {
						goto HandlePredError;
					}

					if (as_unpack_int64(&pk, &pred->value.integer_range.max) != 0) {
						goto HandlePredError;
					}
				}
				else if (pred->dtype == AS_INDEX_GEO2DSPHERE) {
					if (! unpack_str_new(&pk, &pred->value.string_val.string, 4096)) {
						goto HandlePredError;
					}
					pred->value.string_val._free = true;
					free_pred_string = true;
				}
				break;

			default:
				goto HandlePredError;
		}

		// TODO Destroy what allocated!!
		if (as_unpack_int64(&pk, &ival) != 0) {
			goto HandlePredError;
		}

		if (ival < 0 || ival > AS_INDEX_GEO2DSPHERE) {
			goto HandlePredError;
		}
		pred->dtype = (as_index_datatype)ival;

		if (as_unpack_int64(&pk, &ival) != 0) {
			goto HandlePredError;
		}

		if (ival < 0 || ival > AS_INDEX_TYPE_MAPVALUES) {
			goto HandlePredError;
		}
		pred->itype = (as_index_type)ival;
	}

	// Query Apply
	if (as_unpack_boolean(&pk, &b) != 0) {
		goto HandleError;
	}

	if (b) {
		query->apply._free = true;

		if (! unpack_str_init(&pk, query->apply.module, AS_UDF_MODULE_MAX_SIZE)) {
			goto HandleError;
		}

		if (! unpack_str_init(&pk, query->apply.function, AS_UDF_FUNCTION_MAX_SIZE)) {
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

			query->apply.arglist = (as_list*)listval;
		}
		else {
			query->apply.arglist = NULL;
		}
	}
	else {
		query->apply._free = false;
	}

	// Query Operations
	if (as_unpack_boolean(&pk, &b) != 0) {
		goto HandleError;
	}

	if (b) {
		if (as_unpack_uint64(&pk, &uval) != 0) {
			goto HandleError;
		}

		uint16_t max = (uint16_t)uval;

		query->ops = cf_malloc(sizeof(as_operations));
		query->ops->binops.capacity = max;
		query->ops->binops.entries = cf_malloc(sizeof(as_binop) * max);
		query->ops->binops._free = true;

		for (uint16_t i = 0; i < max; i++, query->ops->binops.size++) {
			as_binop* op = &query->ops->binops.entries[i];

			if (as_unpack_int64(&pk, &ival) != 0) {
				goto HandleError;
			}
			op->op = (as_operator)ival;

			if (! unpack_str_init(&pk, op->bin.name, AS_BIN_NAME_MAX_SIZE)) {
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
		query->ops = NULL;
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
		query->parts_all = cf_malloc(sizeof(as_partitions_status) +
			(sizeof(as_partition_status) * max));

		query->parts_all->ref_count = 1;
		query->parts_all->part_count = max;

		if (as_unpack_uint64(&pk, &uval) != 0) {
			goto HandleError;
		}

		query->parts_all->part_begin = (uint16_t)uval;

		if (as_unpack_boolean(&pk, &query->parts_all->done) != 0) {
			goto HandleError;
		}

		query->parts_all->retry = true;

		for (uint16_t i = 0; i < max; i++) {
			as_partition_status* ps = &query->parts_all->parts[i];

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

			if (! unpack_bytes_init(&pk, ps->digest.value, AS_DIGEST_VALUE_SIZE)) {
				goto HandleError;
			}

			if (as_unpack_uint64(&pk, &uval) != 0) {
				goto HandleError;
			}
		}
	}
	else {
		query->parts_all = NULL;
	}

	if (as_unpack_uint64(&pk, &query->max_records) != 0) {
		goto HandleError;
	}

	if (as_unpack_uint64(&pk, &uval) != 0) {
		goto HandleError;
	}

	query->records_per_second = (uint32_t)uval;

	if (as_unpack_uint64(&pk, &uval) != 0) {
		goto HandleError;
	}

	query->ttl = (uint32_t)uval;

	if (as_unpack_boolean(&pk, &query->paginate) != 0) {
		goto HandleError;
	}

	if (as_unpack_boolean(&pk, &query->no_bins) != 0) {
		goto HandleError;
	}

	return true;

HandlePredError:
	// Destroy existing predicate and let HandleError destroy older predicates.
	if (pred->ctx) {
		cf_free(pred->ctx);
		pred->ctx = NULL;
	}

	if (free_pred_string) {
		cf_free(pred->value.string_val.string);
		pred->value.string_val.string = NULL;
	}

HandleError:
	as_query_destroy(query);
	return false;
}
