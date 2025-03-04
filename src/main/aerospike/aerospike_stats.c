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
#include <aerospike/aerospike_stats.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_node.h>
#include <aerospike/as_string_builder.h>

/******************************************************************************
 * GLOBALS
 *****************************************************************************/

extern as_event_loop* as_event_loops;
extern uint32_t as_event_loop_capacity;
extern uint32_t as_event_loop_size;

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static inline void
as_conn_stats_tostring(as_string_builder* sb, const char* title, as_conn_stats* cs)
{
	as_string_builder_append_char(sb, ' ');
	as_string_builder_append(sb, title);
	as_string_builder_append_char(sb, '(');
	as_string_builder_append_uint(sb, cs->in_use);
	as_string_builder_append_char(sb, ',');
	as_string_builder_append_uint(sb, cs->in_pool);
	as_string_builder_append_char(sb, ',');
	as_string_builder_append_uint(sb, cs->opened);
	as_string_builder_append_char(sb, ',');
	as_string_builder_append_uint(sb, cs->closed);
	as_string_builder_append_char(sb, ')');
}

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

void
aerospike_cluster_stats(as_cluster* cluster, as_cluster_stats* stats)
{
	// Node stats.
	as_nodes* nodes = as_nodes_reserve(cluster);
	stats->nodes = cf_malloc(sizeof(as_node_stats) * nodes->size);
	stats->nodes_size = nodes->size;

	for (uint32_t i = 0; i < nodes->size; i++) {
		aerospike_node_stats(nodes->array[i], &stats->nodes[i]);
	}
	as_nodes_release(nodes);

	// Event loop stats.
	if (as_event_loop_capacity > 0) {
		stats->event_loops_size = as_event_loop_size;
		stats->event_loops = cf_malloc(sizeof(as_event_loop_stats) * stats->event_loops_size);

		for (uint32_t i = 0; i < stats->event_loops_size; i++) {
			aerospike_event_loop_stats(&as_event_loops[i], &stats->event_loops[i]);
		}
	}
	else {
		stats->event_loops_size = 0;
		stats->event_loops = NULL;
	}

	// cf_queue applies locks, so we are safe here.
	stats->thread_pool_queued_tasks = cf_queue_sz(cluster->thread_pool.dispatch_queue);
	stats->retry_count = cluster->retry_count;
}

void
aerospike_stats_destroy(as_cluster_stats* stats)
{
	uint32_t max = stats->nodes_size;

	// Release individual nodes.
	for (uint32_t i = 0; i < max; i++) {
		aerospike_node_stats_destroy(&stats->nodes[i]);
	}
	cf_free(stats->nodes);

	if (stats->event_loops) {
		cf_free(stats->event_loops);
	}
}

void
aerospike_node_stats(as_node* node, as_node_stats* stats)
{
	as_node_reserve(node); // Released in aerospike_node_stats_destroy()
	stats->node = node;
	stats->error_count = as_node_get_error_count(node);
	stats->timeout_count = as_node_get_timeout_count(node);

	as_conn_stats_init(&stats->sync);
	as_conn_stats_init(&stats->async);
	as_conn_stats_init(&stats->pipeline);

	uint32_t max = node->cluster->conn_pools_per_node;

	// Sync connection summary.
	for (uint32_t i = 0; i < max; i++) {
		as_conn_pool* pool = &node->sync_conn_pools[i];

		pthread_mutex_lock(&pool->lock);
		uint32_t in_pool = as_queue_size(&pool->queue);
		uint32_t total = pool->queue.total;
		pthread_mutex_unlock(&pool->lock);

		stats->sync.in_pool += in_pool;
		stats->sync.in_use += total - in_pool;
	}
	stats->sync.opened = node->sync_conns_opened;
	stats->sync.closed = node->sync_conns_closed;

	// Async connection summary.
	if (as_event_loop_capacity > 0) {
		for (uint32_t i = 0; i < as_event_loop_size; i++) {
			// Regular async.
			as_conn_stats_sum(&stats->async, &node->async_conn_pools[i]);

			// Pipeline async.
			as_conn_stats_sum(&stats->pipeline, &node->pipe_conn_pools[i]);
		}
	}
}

char*
aerospike_stats_to_string(as_cluster_stats* stats)
{
	as_string_builder sb;
	as_string_builder_init(&sb, 4096, true);
	as_string_builder_append(&sb, "nodes(inUse,inPool,opened,closed) error_count,timeout_count");
	as_string_builder_append_newline(&sb);

	for (uint32_t i = 0; i < stats->nodes_size; i++) {
		as_node_stats* node_stats = &stats->nodes[i];
		as_string_builder_append(&sb, as_node_get_address_string(node_stats->node));
		as_conn_stats_tostring(&sb, "sync", &node_stats->sync);
		as_conn_stats_tostring(&sb, "async", &node_stats->async);
		as_conn_stats_tostring(&sb, "pipeline", &node_stats->pipeline);
		as_string_builder_append_char(&sb, ' ');
		as_string_builder_append_uint64(&sb, node_stats->error_count);
		as_string_builder_append_char(&sb, ',');
		as_string_builder_append_uint64(&sb, node_stats->timeout_count);
		as_string_builder_append_newline(&sb);
	}

	if (stats->event_loops) {
		as_string_builder_append(&sb, "event loops(processSize,queueSize): ");

		for (uint32_t i = 0; i < stats->event_loops_size; i++) {
			as_event_loop_stats* ev_stats = &stats->event_loops[i];

			if (i > 0) {
				as_string_builder_append_char(&sb, ',');
			}
			as_string_builder_append_char(&sb, '(');
			as_string_builder_append_int(&sb, ev_stats->process_size);
			as_string_builder_append_char(&sb, ',');
			as_string_builder_append_uint(&sb, ev_stats->queue_size);
			as_string_builder_append_char(&sb, ')');
		}
		as_string_builder_append_newline(&sb);
	}
	
	as_string_builder_append(&sb, "retry_count: ");
	as_string_builder_append_uint64(&sb, stats->retry_count);

	return sb.data;
}

void
as_conn_stats_sum(as_conn_stats* stats, as_async_conn_pool* pool)
{
	// Warning: cross-thread reference without a lock.
	int tmp = as_queue_size(&pool->queue);

	// Timing issues may cause values to go negative. Adjust.
	if (tmp < 0) {
		tmp = 0;
	}
	stats->in_pool += tmp;
	tmp = pool->queue.total - tmp;

	if (tmp < 0) {
		tmp = 0;
	}
	stats->in_use += tmp;
	stats->opened += pool->opened;
	stats->closed += pool->closed;
}
