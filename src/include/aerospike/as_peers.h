/*
 * Copyright 2008-2025 Aerospike, Inc.
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
#pragma once

#include <aerospike/as_std.h>
#include <aerospike/as_status.h>
#include <aerospike/as_error.h>
#include <aerospike/as_host.h>
#include <aerospike/as_vector.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * TYPES
 *****************************************************************************/

typedef struct as_peers_s {
	as_vector /* as_node* */ nodes;
	as_vector /* as_node */ nodes_to_remove;
	as_vector /* as_host */ invalid_hosts;
	uint32_t refresh_count;
	bool gen_changed;
} as_peers;

struct as_cluster_s;
struct as_node_s;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

struct as_node_s*
as_peers_find_local_node(as_vector* nodes, const char* name);

void
as_peers_append_unique_node(as_vector* nodes, struct as_node_s* node);

as_status
as_peers_parse_peers(
	as_peers* peers, as_error* err, struct as_cluster_s* cluster, struct as_node_s* node, char* buf
	);

bool
as_peers_find_invalid_host(as_peers* peers, as_host* host);

static inline void
as_peers_add_invalid_host(as_peers* peers, as_host* host)
{
	as_host* trg = as_vector_reserve(&peers->invalid_hosts);
	as_host_copy(host, trg);
}

static inline uint32_t
as_peers_invalid_count(as_peers* peers)
{
	return peers->invalid_hosts.size;
}

#ifdef __cplusplus
} // end extern "C"
#endif
