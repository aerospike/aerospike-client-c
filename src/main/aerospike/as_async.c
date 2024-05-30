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
#include <aerospike/as_async.h>

/******************************************************************************
 * GLOBALS
 *****************************************************************************/

extern uint32_t as_cluster_count;
extern uint32_t as_event_loop_capacity;
extern uint32_t as_event_loop_size;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

void
as_cluster_set_max_socket_idle(as_cluster* cluster, uint32_t max_socket_idle_sec);

uint32_t
as_async_get_cluster_count(void)
{
	return as_load_uint32(&as_cluster_count);
}

uint32_t
as_async_get_pending(as_cluster* cluster)
{
	// Return approximate number of pending commands for given cluster.
	// Results may not be exactly accurate because we accessing pending counts
	// in a non-atomic way.
	int sum = 0;

	for (uint32_t i = 0; i < as_event_loop_size; i++) {
		int pending = cluster->event_state[i].pending;

		if (pending > 0) {
			sum += pending;
		}
	}
	return sum;
}

uint32_t
as_async_get_connections(as_cluster* cluster)
{
	// Return approximate number of open connections for given cluster.
	// Results may not be exactly accurate because we accessing connection counts
	// in a non-atomic way.
	uint32_t sum = 0;

	as_nodes* nodes = as_nodes_reserve(cluster);
	uint32_t size = nodes->size;

	for (uint32_t i = 0; i < size; i++) {
		as_node* node = nodes->array[i];

		for (uint32_t j = 0; j < as_event_loop_size; j++) {
			sum += node->async_conn_pools[j].queue.total + node->pipe_conn_pools[j].queue.total;
		}
	}
	as_nodes_release(nodes);
	return sum;
}

void
as_async_update_max_idle(as_cluster* cluster, uint32_t max_idle)
{
	as_cluster_set_max_socket_idle(cluster, max_idle);
}

void
as_async_update_max_conns(as_cluster* cluster, bool pipe, uint32_t max_conns)
{
	uint32_t max = max_conns / as_event_loop_capacity;
	uint32_t rem = max_conns - max * as_event_loop_capacity;

	as_nodes* nodes = as_nodes_reserve(cluster);
	uint32_t size = nodes->size;

	for (uint32_t i = 0; i < size; i++) {
		as_node* node = nodes->array[i];

		for (uint32_t j = 0; j < as_event_loop_capacity; j++) {
			uint32_t limit = j < rem ? max + 1 : max;

			if (pipe) {
				node->pipe_conn_pools[j].limit = limit;
			}
			else {
				node->async_conn_pools[j].limit = limit;
			}
		}
	}

	as_nodes_release(nodes);

	if (pipe) {
		cluster->pipe_max_conns_per_node = max_conns;
	}
	else {
		cluster->async_max_conns_per_node = max_conns;
	}
}
