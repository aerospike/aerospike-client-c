/*
 * Copyright 2008-2022 Aerospike, Inc.
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

/******************************************************************************
 * INSTANCE FUNCTIONS
 *****************************************************************************/

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
	if ( !query ) return;

	query->ns[0] = '\0';
	query->set[0] = '\0';

	if ( query->select.entries && query->select._free ) {
		cf_free(query->select.entries);
	}

	query->select._free = false;
	query->select.capacity = 0;
	query->select.size = 0;
	query->select.entries = NULL;
	
	if ( query->where.entries && query->where._free ) {
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

/******************************************************************************
 * SELECT FUNCTIONS
 *****************************************************************************/

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

/******************************************************************************
 * WHERE FUNCTIONS
 *****************************************************************************/

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
			p->value.string = va_arg(ap, char*);
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
			p->value.string = va_arg(ap, char*);
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

/******************************************************************************
 * QUERY MODIFIER FUNCTIONS
 *****************************************************************************/

bool
as_query_apply(as_query* query, const char* module, const char* function, const as_list* arglist)
{
	if ( !query ) return false;
	as_udf_call_init(&query->apply, module, function, (as_list *) arglist);
	return true;
}
