/*
 * Copyright 2008-2018 Aerospike, Inc.
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

#include "log_helper.h"

#include <aerospike/as_bin.h>
#include <aerospike/as_record.h>
#include <aerospike/as_record_iterator.h>
#include "../test.h"


/*****************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static bool dump_cdt(const as_val* v, int level);

static bool
val_is_cdt(as_val_t type)
{
	return type == AS_MAP || type == AS_LIST;
}

static bool
dump_map_callback(const as_val* key, const as_val* value, void* udata)
{
	int level = *(int*)udata;
	char *s = as_val_tostring(key);
	as_val_t type = as_val_type(value);

	if (val_is_cdt(type)) {
		char ch = '[';

		if (type == AS_MAP) {
			ch = '{';
		}

		info("%*s%s: %c", level * 2, "", s, ch);
		dump_cdt(value, level);
	} else {
		char *v = as_val_tostring(value);
		info("%*s%s: %s", level * 2, "", s, v);
		free(v);
	}

	free(s);
	return true;
}

static void
dump_map(const as_map* m, int level)
{
	as_map_foreach(m, dump_map_callback, &level);
}

static void
dump_list(const as_list* l, int level)
{
	for (uint32_t i = 0; i < as_list_size(l); i++) {
		as_val* v = as_list_get(l, i);
		as_val_t type = as_val_type(v);

		if (val_is_cdt(type)) {
			char ch = '[';
			if (type == AS_MAP) {
				ch = '{';
			}
			info("%*s%c", level * 2, "", ch);
			dump_cdt(v, level);
		} else {
			char *s = as_val_tostring(v);
			info("%*s%s,", level * 2, "", s);
			free(s);
		}
	}
}

static bool
dump_cdt(const as_val* v, int level)
{
	as_val_t type = as_val_type(v);
	switch (type) {
	case AS_MAP:
		if (level == 0) {
			info("%*smap(%u) {", level*2, "", as_map_size((as_map*)v));
		}
		dump_map((as_map*)v, level + 1);
		info("%*s}", level * 2, "");
		break;
	case AS_LIST:
		if (level == 0) {
			info("%*slist(%u) [", level*2, "", as_list_size((as_list*)v));
		}
		dump_list((as_list*)v, level + 1);
		info("%*s]", level * 2, "");
		break;
	default:
		return false;
	}

	return true;
}

static void
dump_bin_tabbed(const as_bin* p_bin)
{
	if (! p_bin) {
		info("  null as_bin object");
		return;
	}

	if (dump_cdt((as_val*)as_bin_get_value(p_bin), 0)) {
		return;
	}

	dump_bin(p_bin);
}


/*****************************************************************************
 * PUBLIC FUNCTIONS
 *****************************************************************************/

void
dump_bin(const as_bin* p_bin)
{
	if (! p_bin) {
		info("  null as_bin object");
		return;
	}

	char* val_as_str = as_val_tostring(as_bin_get_value(p_bin));

	info("  %s : %s", as_bin_get_name(p_bin), val_as_str);

	free(val_as_str);
}

void
test_dump_record(const as_record* p_rec, bool tabbed)
{
	if (! p_rec) {
		info("  null as_record object");
		return;
	}

	if (p_rec->key.valuep) {
		char* key_val_as_str = as_val_tostring(p_rec->key.valuep);

		info("  key: %s", key_val_as_str);

		free(key_val_as_str);
	}

	uint16_t num_bins = as_record_numbins(p_rec);

	info("  generation %u, ttl %u, %u bin%s", p_rec->gen, p_rec->ttl, num_bins,
			num_bins == 0 ? "s" : (num_bins == 1 ? ":" : "s:"));

	as_record_iterator it;
	as_record_iterator_init(&it, p_rec);

	while (as_record_iterator_has_next(&it)) {
		if (tabbed) {
			dump_bin_tabbed(as_record_iterator_next(&it));
		} else {
			dump_bin(as_record_iterator_next(&it));
		}
	}

	as_record_iterator_destroy(&it);
}

void
dump_record(const as_record* p_rec)
{
	test_dump_record(p_rec, false);
}
