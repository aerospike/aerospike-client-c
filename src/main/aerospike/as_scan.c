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
#include <aerospike/as_scan.h>
#include <aerospike/as_atomic.h>
#include <aerospike/as_operations.h>

#include <citrusleaf/alloc.h>

/******************************************************************************
 * INSTANCE FUNCTIONS
 *****************************************************************************/

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

/******************************************************************************
 * SELECT FUNCTIONS
 *****************************************************************************/

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

/******************************************************************************
 * MODIFIER FUNCTIONS
 *****************************************************************************/

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
