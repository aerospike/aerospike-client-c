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
#include <aerospike/as_partition_filter.h>

static size_t
as_partitions_bytes_size(uint16_t part_count)
{
	return sizeof(uint16_t) + sizeof(uint16_t) + 1 +
		(part_count * (sizeof(uint16_t) + 2 + AS_DIGEST_VALUE_SIZE + sizeof(uint64_t)));
}

uint8_t*
as_partitions_status_to_bytes(as_partitions_status* parts_all, size_t* bytes_size)
{
	size_t size = as_partitions_bytes_size(parts_all->part_count);
	uint8_t* bytes = cf_malloc(size);
	uint8_t* p = bytes;

	*(uint16_t*)p = parts_all->part_begin;
	p += sizeof(uint16_t);
	*(uint16_t*)p = parts_all->part_count;
	p += sizeof(uint16_t);
	*p++ = (uint8_t)parts_all->done;

	for (uint16_t i = 0; i < parts_all->part_count; i++) {
		as_partition_status* ps = &parts_all->parts[i];

		*(uint16_t*)p = ps->part_id;
		p += sizeof(uint16_t);
		*p++ = (uint8_t)ps->retry;
		*p++ = (uint8_t)ps->digest.init;
		memcpy(p, ps->digest.value, AS_DIGEST_VALUE_SIZE);
		p += AS_DIGEST_VALUE_SIZE;
		*(uint64_t*)p = ps->bval;
		p += sizeof(uint64_t);
	}

	if (p - bytes != size) {
		cf_free(bytes);
		*bytes_size = 0;
		return NULL;
	}

	*bytes_size = size;
	return bytes;
}

as_partitions_status*
as_partitions_status_from_bytes(uint8_t* bytes, size_t bytes_size)
{
	uint8_t* p = bytes;

	uint16_t part_begin = *(uint16_t*)p;
	p += sizeof(uint16_t);

	uint16_t part_count = *(uint16_t*)p;
	p += sizeof(uint16_t);

	size_t size = as_partitions_bytes_size(part_count);

	if (size != bytes_size) {
		return NULL;
	}

	size = sizeof(as_partitions_status) + (sizeof(as_partition_status) * part_count);
	as_partitions_status* parts_all = cf_malloc(size);

	parts_all->ref_count = 1;
	parts_all->part_begin = part_begin;
	parts_all->part_count = part_count;
	parts_all->done = (bool)(*p++);
	parts_all->retry = true;

	for (uint16_t i = 0; i < part_count; i++) {
		as_partition_status* ps = &parts_all->parts[i];

		ps->part_id = *(uint16_t*)p;
		p += sizeof(uint16_t);
		ps->replica_index = 0;
		ps->unavailable = false;
		ps->retry = (bool)(*p++);
		ps->digest.init = (bool)(*p++);
		memcpy(ps->digest.value, p, AS_DIGEST_VALUE_SIZE);
		p += AS_DIGEST_VALUE_SIZE;
		ps->bval = *(uint64_t*)p;
		p += sizeof(uint64_t);
		ps->master_node = NULL;
	}
	return parts_all;
}
