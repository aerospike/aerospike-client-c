/*
 * Copyright 2008-2016 Aerospike, Inc.
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
#include <aerospike/as_bin.h>
#include <aerospike/as_record.h>
#include <aerospike/as_record_iterator.h>
#include "../test.h"

void dump_bin(const as_bin* p_bin)
{
	if (! p_bin) {
		info("  null as_bin object");
		return;
	}

	char* val_as_str = as_val_tostring(as_bin_get_value(p_bin));

	info("  %s : %s", as_bin_get_name(p_bin), val_as_str);

	free(val_as_str);
}

void dump_record(const as_record* p_rec)
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
		dump_bin(as_record_iterator_next(&it));
	}

	as_record_iterator_destroy(&it);
}

