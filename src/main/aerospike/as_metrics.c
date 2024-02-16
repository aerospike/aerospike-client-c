/*
 * Copyright 2008-2024 Aerospike, Inc.
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
#include <aerospike/as_metrics.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_event.h>
#include <aerospike/as_node.h>
#include <aerospike/as_string_builder.h>

//---------------------------------
// Functions
//---------------------------------

as_status
aerospike_enable_metrics(aerospike* as, as_error* err, struct as_policy_metrics_s* policy)
{
	return as_cluster_enable_metrics(err, as->cluster, policy);
}

as_status
aerospike_disable_metrics(aerospike* as, as_error* err)
{
	return as_cluster_disable_metrics(err, as->cluster);
}

void
as_metrics_policy_init(as_metrics_policy* policy)
{
	policy->report_size_limit = 0;
	as_strncpy(policy->report_dir, ".", sizeof(policy->report_dir));
	policy->interval = 30;
	policy->latency_columns = 7;
	policy->latency_shift = 1;
	policy->metrics_listeners.enable_listener = NULL;
	policy->metrics_listeners.snapshot_listener = NULL;
	policy->metrics_listeners.node_close_listener = NULL;
	policy->metrics_listeners.disable_listener = NULL;
	policy->metrics_listeners.udata = NULL;
}

char*
as_latency_type_to_string(as_latency_type type)
{
	switch (type) {
	case AS_LATENCY_TYPE_CONN:
		return "conn"; 
		break;
	case AS_LATENCY_TYPE_WRITE:
		return "write";
		break;
	case AS_LATENCY_TYPE_READ:
		return "read";
		break;
	case AS_LATENCY_TYPE_BATCH:
		return "batch";
		break;
	case AS_LATENCY_TYPE_QUERY:
		return "query";
		break;
	case AS_LATENCY_TYPE_NONE:
		return "none";
		break;
	default:
		return "none";
		break;
	}
}

void
as_metrics_latency_buckets_init(as_latency_buckets* latency_buckets, uint32_t latency_columns, uint32_t latency_shift)
{
	latency_buckets->latency_columns = latency_columns;
	latency_buckets->latency_shift = latency_shift;
	latency_buckets->buckets = cf_malloc(sizeof(uint64_t) * latency_columns);
	for (uint32_t i = 0; i < latency_columns; i++) {
		as_store_uint64(&latency_buckets->buckets[i], 0);
	}
}

uint64_t
as_metrics_get_bucket(as_latency_buckets* buckets, uint32_t i)
{
	return as_load_uint64(&buckets->buckets[i]);
}

void
as_metrics_latency_buckets_add(as_latency_buckets* latency_buckets, uint64_t elapsed)
{
	uint32_t index = as_metrics_get_index(latency_buckets, elapsed);
	as_incr_uint64(&latency_buckets->buckets[index]);
}

uint32_t 
as_metrics_get_index(as_latency_buckets* latency_buckets, uint64_t elapsed_nanos)
{
	// Convert nanoseconds to milliseconds.
	uint64_t elapsed = elapsed_nanos / NS_TO_MS;

	// Round up elapsed to nearest millisecond.
	if ((elapsed_nanos - (elapsed * NS_TO_MS)) > 0) {
		elapsed++;
	}

	uint32_t last_bucket = latency_buckets->latency_columns - 1;
	uint64_t limit = 1;

	for (uint32_t i = 0; i < last_bucket; i++) {
		if (elapsed <= limit) {
			return i;
		}
		limit <<= latency_buckets->latency_shift;
	}
	return last_bucket;
}

as_node_metrics*
as_node_metrics_init(uint32_t latency_columns, uint32_t latency_shift)
{
	as_node_metrics* node_metrics = (as_node_metrics*)cf_malloc(sizeof(as_node_metrics));
	uint32_t max_latency_type = AS_LATENCY_TYPE_NONE;
	node_metrics->latency = (as_latency_buckets*)cf_malloc(sizeof(as_latency_buckets) * max_latency_type);
	for (uint32_t i = 0; i < max_latency_type; i++) {
		as_metrics_latency_buckets_init(&node_metrics->latency[i], latency_columns, latency_shift);
	}

	return node_metrics;
}

void
as_metrics_add_latency(as_node_metrics* node_metrics, as_latency_type latency_type, uint64_t elapsed)
{
	as_metrics_latency_buckets_add(&node_metrics->latency[latency_type], elapsed);
}
