/*
 * Copyright 2008-2026 Aerospike, Inc.
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
#include <aerospike/as_cluster.h>
#include <aerospike/as_latency.h>

#define NS_TO_MS 1000000
#define NS_TO_US 1000

/**
 * Use a non-inline function for the garbage collector function pointer.
 * Forward to inlined as_latency_release().
 */
static void
release_latency(void* latency)
{
	as_latency_release(latency);
}

as_latency*
as_latency_create(uint8_t columns, uint8_t shift, as_metrics_latency_unit unit)
{
	as_latency* latency = cf_calloc(1, sizeof(as_latency) + (sizeof(uint64_t) * columns));
	latency->ref_count = 1;
	latency->shift = shift;
	latency->size = columns;
	latency->unit = unit;
	return latency;
}

void
as_latency_gc_publish(as_cluster* cluster, as_latency** slot, as_latency* latency_new)
{
	as_latency* latency_old = *slot;
	as_store_ptr_rls((void**)slot, latency_new);

	if (!latency_old) {
		return;
	}

	as_gc_item item;
	item.data = latency_old;
	item.release_fn = release_latency;
	as_vector_append(cluster->gc, &item);
}

void
as_latency_gc_init(as_cluster* cluster, as_latency** slot, const as_metrics_policy* policy)
{
	as_latency* latency = *slot;

	if (latency && policy->latency_columns == latency->size && policy->latency_shift == latency->shift &&
			policy->latency_unit == latency->unit) {
		for (uint8_t k = 0; k < latency->size; k++) {
			as_store_uint64(&latency->buckets[k], 0);
		}
		return;
	}

	as_latency* latency_new = as_latency_create(policy->latency_columns, policy->latency_shift,
		policy->latency_unit);
	as_latency_gc_publish(cluster, slot, latency_new);
}

static uint8_t
latency_bucket_index(as_latency* latency, uint64_t elapsed)
{
	uint64_t limit = 1;
	uint8_t last_bucket = latency->size - 1;

	for (uint8_t i = 0; i < last_bucket; i++) {
		if (elapsed <= limit) {
			return i;
		}
		limit <<= latency->shift;
	}
	return last_bucket;
}

void
as_latency_add(as_latency* latency, uint64_t elapsed_nanos)
{
	if (!latency) {
		return;
	}

	latency = as_latency_reserve(latency);
	uint64_t scale = latency->unit == AS_METRICS_LATENCY_MICROSECONDS ? NS_TO_US : NS_TO_MS;

	// Convert nanoseconds to the histogram unit and round up.
	uint64_t elapsed = elapsed_nanos / scale;

	if ((elapsed_nanos - (elapsed * scale)) > 0) {
		elapsed++;
	}

	uint8_t index = latency_bucket_index(latency, elapsed);
	as_incr_uint64(&latency->buckets[index]);

	as_latency_release(latency);
}

char*
as_latency_type_to_string(as_latency_type type)
{
	switch (type) {
	case AS_LATENCY_TYPE_CONN:
		return "conn"; 

	case AS_LATENCY_TYPE_WRITE:
		return "write";

	case AS_LATENCY_TYPE_READ:
		return "read";

	case AS_LATENCY_TYPE_BATCH:
		return "batch";

	case AS_LATENCY_TYPE_QUERY:
		return "query";

	default:
	case AS_LATENCY_TYPE_NONE:
		return "none";
	}
}
