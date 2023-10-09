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
				cf_free(pred->ctx);
			}

			switch (pred->dtype) {
				case AS_INDEX_GEO2DSPHERE:
				case AS_INDEX_STRING:
					if (pred->value.string_val._free) {
						cf_free(pred->value.string_val.string);
					}
					break;

				case AS_INDEX_BLOB:
					if (pred->value.blob_val._free) {
						cf_free(pred->value.blob_val.bytes);
					}
					break;

				default:
					break;
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

	switch (type) {
	case AS_PREDICATE_EQUAL:
		switch (dtype) {
			case AS_INDEX_STRING:
				p->value.string_val.string = va_arg(ap, char*);
				p->value.string_val._free = false;
				break;

			case AS_INDEX_NUMERIC:
				p->value.integer = va_arg(ap, int64_t);
				break;

			case AS_INDEX_BLOB:
				p->value.blob_val.bytes = va_arg(ap, uint8_t*);
				p->value.blob_val.bytes_size = va_arg(ap, uint32_t);
				p->value.blob_val._free = (bool)va_arg(ap, uint32_t);
				break;

			default:
				status = false;
				break;
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

bool
as_query_to_bytes(const as_query* query, uint8_t** bytes, uint32_t* bytes_size)
{
	as_packer pk = as_cdt_begin();
	as_pack_string(&pk, query->ns);
	as_pack_string(&pk, query->set);
	as_pack_uint64(&pk, query->select.size);

	for (uint16_t i = 0; i < query->select.size; i++) {
		as_pack_string(&pk, query->select.entries[i]);
	}

	as_pack_uint64(&pk, query->where.size);

	for (uint16_t i = 0; i < query->where.size; i++) {
		as_predicate* pred = &query->where.entries[i];
		as_pack_string(&pk, pred->bin);

		if (pred->ctx) {
			as_pack_bool(&pk, true);
			uint32_t size = as_cdt_ctx_pack(pred->ctx, &pk);
			as_pack_uint64(&pk, size);
		}
		else {
			as_pack_bool(&pk, false);
		}

		as_pack_int64(&pk, pred->type);
		as_pack_int64(&pk, pred->dtype);
		as_pack_int64(&pk, pred->itype);

		switch (pred->type) {
			case AS_PREDICATE_EQUAL:
				switch (pred->dtype) {
					case AS_INDEX_STRING:
						as_pack_string(&pk, pred->value.string_val.string);
						break;

					case AS_INDEX_NUMERIC:
						as_pack_int64(&pk, pred->value.integer);
						break;

					case AS_INDEX_BLOB:
						as_pack_bytes(&pk, pred->value.blob_val.bytes, pred->value.blob_val.bytes_size);
						break;

					default:
						break;
				}
				break;

			case AS_PREDICATE_RANGE:
				if (pred->dtype == AS_INDEX_NUMERIC) {
					as_pack_int64(&pk, pred->value.integer_range.min);
					as_pack_int64(&pk, pred->value.integer_range.max);
				}
				else if (pred->dtype == AS_INDEX_GEO2DSPHERE) {
					as_pack_string(&pk, pred->value.string_val.string);
				}
				break;

			default:
				goto HandleError;
		}
	}

	if (query->apply.function[0]) {
		as_pack_bool(&pk, true);
		as_pack_string(&pk, query->apply.module);
		as_pack_string(&pk, query->apply.function);

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
		as_pack_uint64(&pk, query->ops->ttl);
		as_pack_uint64(&pk, query->ops->gen);

		uint16_t max = query->ops->binops.size;
		as_pack_uint64(&pk, max);

		for (uint16_t i = 0; i < max; i++) {
			as_binop* op = &query->ops->binops.entries[i];
			as_pack_uint64(&pk, op->op);
			as_pack_string(&pk, op->bin.name);
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
			as_pack_byte_string(&pk, ps->digest.value, AS_DIGEST_VALUE_SIZE);
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

bool
as_query_from_bytes(as_query* query, const uint8_t* bytes, uint32_t bytes_size)
{
	// Initialize query so it can be safely destroyed if a failure occurs.
	// Assume query passed on the stack.
	as_query_defaults(query, false, "", "");

	as_unpacker pk = {
		.buffer = bytes,
		.length = bytes_size,
		.offset = 0,
	};

	if (! as_unpack_str_init(&pk, query->ns, AS_NAMESPACE_MAX_SIZE)) {
		return false;
	}

	if (! as_unpack_str_init(&pk, query->set, AS_SET_MAX_SIZE)) {
		return false;
	}

	int64_t ival;
	uint64_t uval;

	// Unpack select
	if (as_unpack_uint64(&pk, &uval) != 0) {
		return false;
	}

	if (uval > 0) {
		query->select.capacity = query->select.size = (uint16_t)uval;
		query->select.entries = cf_malloc(sizeof(as_bin_name) * query->select.size);
		query->select._free = true;

		for (uint16_t i = 0; i < query->select.size; i++) {
			if (! as_unpack_str_init(&pk, query->select.entries[i], AS_BIN_NAME_MAX_SIZE)) {
				goto HandleError;
			}
		}
	}

	// Unpack where
	as_predicate* pred = NULL;
	bool free_pred_val = false;
	bool b = false;

	if (as_unpack_uint64(&pk, &uval) != 0) {
		goto HandleError;
	}

	if (uval > 0) {
		uint32_t max = (uint32_t)uval;
		query->where.capacity = (uint16_t)uval;
		query->where.entries = cf_malloc(sizeof(as_predicate) * max);
		query->where._free = true;
		query->where.size = 0;

		for (uint16_t i = 0; i < max; i++, query->where.size++) {
			pred = &query->where.entries[i];
			pred->ctx = NULL;
			pred->ctx_size = 0;
			pred->ctx_free = false;
			free_pred_val = false;

			if (! as_unpack_str_init(&pk, pred->bin, AS_BIN_NAME_MAX_SIZE)) {
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

			if (as_unpack_int64(&pk, &ival) != 0) {
				goto HandlePredError;
			}

			if (ival < 0 || ival > AS_PREDICATE_RANGE) {
				goto HandlePredError;
			}

			pred->type = (as_predicate_type)ival;

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

			switch (pred->type) {
				case AS_PREDICATE_EQUAL:
					switch (pred->dtype) {
						case AS_INDEX_STRING:
							if (! as_unpack_str_new(&pk, &pred->value.string_val.string, 4096)) {
								goto HandlePredError;
							}
							pred->value.string_val._free = true;
							free_pred_val = true;
							break;

						case AS_INDEX_NUMERIC:
							if (as_unpack_int64(&pk, &pred->value.integer) != 0) {
								goto HandlePredError;
							}
							break;

						case AS_INDEX_BLOB:
							if (! as_unpack_bytes_new(&pk, &pred->value.blob_val.bytes,
														   &pred->value.blob_val.bytes_size, 4096)) {
								goto HandlePredError;
							}
							pred->value.blob_val._free = true;
							free_pred_val = true;
							break;

						default:
							break;
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
						if (! as_unpack_str_new(&pk, &pred->value.string_val.string, 4096)) {
							goto HandlePredError;
						}
						pred->value.string_val._free = true;
						free_pred_val = true;
					}
					break;

				default:
					goto HandlePredError;
			}
		}
	}

	// Query Apply
	if (as_unpack_boolean(&pk, &b) != 0) {
		goto HandleError;
	}

	if (b) {
		if (! as_unpack_str_init(&pk, query->apply.module, AS_UDF_MODULE_MAX_SIZE)) {
			goto HandleError;
		}

		if (! as_unpack_str_init(&pk, query->apply.function, AS_UDF_FUNCTION_MAX_SIZE)) {
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

	// Query Operations
	if (as_unpack_boolean(&pk, &b) != 0) {
		goto HandleError;
	}

	if (b) {
		query->ops = cf_malloc(sizeof(as_operations));
		query->ops->_free = true;

		if (as_unpack_uint64(&pk, &uval) != 0) {
			goto HandleError;
		}

		query->ops->ttl = (uint32_t)uval;

		if (as_unpack_uint64(&pk, &uval) != 0) {
			goto HandleError;
		}

		query->ops->gen = (uint16_t)uval;

		if (as_unpack_uint64(&pk, &uval) != 0) {
			goto HandleError;
		}

		uint16_t max = (uint16_t)uval;

		query->ops->binops.capacity = max;
		query->ops->binops.entries = cf_malloc(sizeof(as_binop) * max);
		query->ops->binops._free = true;
		query->ops->binops.size = 0;

		for (uint16_t i = 0; i < max; i++, query->ops->binops.size++) {
			as_binop* op = &query->ops->binops.entries[i];

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

	if (free_pred_val) {
		switch (pred->dtype) {
			case AS_INDEX_GEO2DSPHERE:
			case AS_INDEX_STRING:
				cf_free(pred->value.string_val.string);
				pred->value.string_val.string = NULL;
				break;

			case AS_INDEX_BLOB:
				cf_free(pred->value.blob_val.bytes);
				pred->value.blob_val.bytes = NULL;
				pred->value.blob_val.bytes_size = 0;
				break;

			default:
				break;
		}
	}

HandleError:
	as_query_destroy(query);
	return false;
}

as_query*
as_query_from_bytes_new(const uint8_t* bytes, uint32_t bytes_size)
{
	as_query* query = (as_query*)cf_malloc(sizeof(as_query));

	if (! as_query_from_bytes(query, bytes, bytes_size)) {
		cf_free(query);
		return NULL;
	}
	query->_free = true;
	return query;
}

//---------------------------------
// Query Compare
//---------------------------------

bool
as_query_compare(as_query* q1, as_query* q2) {
	if (q1->_free != q2->_free) {
		as_cmp_error();
	}

	if (strcmp(q1->ns, q2->ns) != 0) {
		as_cmp_error();
	}

	if (strcmp(q1->set, q2->set) != 0) {
		as_cmp_error();
	}

	if (q1->select._free != q2->select._free) {
		as_cmp_error();
	}

	if (q1->select.capacity != q2->select.capacity) {
		as_cmp_error();
	}

	if (q1->select.size != q2->select.size) {
		as_cmp_error();
	}

	for (uint16_t i = 0; i < q1->select.size; i++) {
		if (strcmp(q1->select.entries[i], q2->select.entries[i]) != 0) {
			as_cmp_error();
		}
	}

	/* _free might be different if as_query_where_inita() is used.
	if (q1->where._free != q2->where._free) {
		as_cmp_error();
	}
	*/

	if (q1->where.capacity != q2->where.capacity) {
		as_cmp_error();
	}

	if (q1->where.size != q2->where.size) {
		as_cmp_error();
	}

	for (uint16_t i = 0; i < q1->where.size; i++) {
		as_predicate* p1 = &q1->where.entries[i];
		as_predicate* p2 = &q2->where.entries[i];

		if (strcmp(p1->bin, p2->bin) != 0) {
			as_cmp_error();
		}

		/* ctx_free may be different.
		if (p1->ctx_free != p2->ctx_free) {
			as_cmp_error();
		}
		*/

		if (p1->ctx_size != p2->ctx_size) {
			as_cmp_error();
		}

		as_cdt_ctx* c1 = p1->ctx;
		as_cdt_ctx* c2 = p2->ctx;

		if (c1 != c2) {
			if (c1->list.size != c2->list.size) {
				as_cmp_error();
			}

			for (uint32_t j = 0; j < c1->list.size; j++) {
				as_cdt_ctx_item* ci1 = as_vector_get(&c1->list, j);
				as_cdt_ctx_item* ci2 = as_vector_get(&c2->list, j);

				if (ci1->type != ci2->type) {
					as_cmp_error();
				}

				if (ci1->type & AS_CDT_CTX_VALUE) {
					if (! as_val_compare(ci1->val.pval, ci2->val.pval)) {
						as_cmp_error();
					}
				}
				else {
					if (ci1->val.ival != ci2->val.ival) {
						as_cmp_error();
					}
				}
			}
		}

		if (p1->type != p2->type) {
			as_cmp_error();
		}

		if (p1->dtype != p2->dtype) {
			as_cmp_error();
		}

		if (p1->itype != p2->itype) {
			as_cmp_error();
		}

		switch (p1->type) {
			case AS_PREDICATE_EQUAL:
				switch (p1->dtype) {
					case AS_INDEX_STRING:
						if (strcmp(p1->value.string_val.string, p2->value.string_val.string) != 0) {
							as_cmp_error();
						}
						break;

					case AS_INDEX_NUMERIC:
						if (p1->value.integer != p2->value.integer) {
							as_cmp_error();
						}
						break;

					case AS_INDEX_BLOB:
						if (p1->value.blob_val.bytes_size != p2->value.blob_val.bytes_size) {
							as_cmp_error();
						}

						if (memcmp(p1->value.blob_val.bytes, p2->value.blob_val.bytes,
								   p1->value.blob_val.bytes_size) != 0) {
							as_cmp_error();
						}
						break;

					default:
						break;
				}
				break;

			case AS_PREDICATE_RANGE:
				if (p1->dtype == AS_INDEX_NUMERIC) {
					if (p1->value.integer_range.min != p2->value.integer_range.min) {
						as_cmp_error();
					}

					if (p1->value.integer_range.max != p2->value.integer_range.max) {
						as_cmp_error();
					}
				}
				else if (p1->dtype == AS_INDEX_GEO2DSPHERE) {
					if (strcmp(p1->value.string_val.string, p2->value.string_val.string) != 0) {
						as_cmp_error();
					}
				}
				break;
		}
	}

	if (q1->apply._free != q2->apply._free) {
		as_cmp_error();
	}

	if (strcmp(q1->apply.module, q2->apply.module) != 0) {
		as_cmp_error();
	}

	if (strcmp(q1->apply.function, q2->apply.function) != 0) {
		as_cmp_error();
	}

	if (q1->apply.arglist != q2->apply.arglist) {
		if (! as_val_compare((as_val*)q1->apply.arglist, (as_val*)q2->apply.arglist)) {
			as_cmp_error();
		}
	}

	if (q1->ops != q2->ops) {
		/* _free might be different if as_operations_inita() is used.
		if (q1->ops->_free != q2->ops->_free) {
			as_cmp_error();
		}
		*/

		if (q1->ops->gen != q2->ops->gen) {
			as_cmp_error();
		}

		if (q1->ops->ttl != q2->ops->ttl) {
			as_cmp_error();
		}

		if (q1->ops->binops.size != q2->ops->binops.size) {
			as_cmp_error();
		}

		for (uint16_t i = 0; i < q1->ops->binops.size; i++) {
			as_binop* op1 = &q1->ops->binops.entries[i];
			as_binop* op2 = &q2->ops->binops.entries[i];

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

	if (q1->parts_all != q2->parts_all) {
		as_partitions_status* p1 = q1->parts_all;
		as_partitions_status* p2 = q2->parts_all;

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

	if (q1->max_records != q2->max_records) {
		as_cmp_error();
	}

	if (q1->records_per_second != q2->records_per_second) {
		as_cmp_error();
	}

	if (q1->ttl != q2->ttl) {
		as_cmp_error();
	}

	if (q1->paginate != q2->paginate) {
		as_cmp_error();
	}

	if (q1->no_bins != q2->no_bins) {
		as_cmp_error();
	}

	return true;
}
