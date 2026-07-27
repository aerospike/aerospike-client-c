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
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/*
 * Shared-memory master / follower attach (same process, same shm_key).
 *
 * Prerequisite: the Aerospike database cluster is already reachable. The test
 * harness (plan_before) connects a normal non-SHM global client to the same
 * seeds to validate the server; that client is unrelated to the SHM pair below.
 * This suite expects a non-TLS cluster (no TLS copied from the harness).
 *
 * Flow exercised:
 *   1) Create SHM client A and connect. First attach wins tend-master in
 *      as_shm_create; on a fresh segment as_cluster_init runs against the live
 *      cluster, as_shm_add_nodes publishes node fields (including version) into
 *      as_node_shm, ready is set, and as_shm_tender starts. No extra "sleep to
 *      tend" is required for this test: that work is part of A's connect.
 *   2) Create SHM client B with the same shm_key and connect. B attaches as a
 *      follower, waits for ready if needed, then as_shm_reset_nodes rebuilds
 *      local as_node objects from SHM (the path that must copy version from
 *      as_node_shm into as_node_info before as_node_create).
 *   3) Assert each node's server version matches between A and B (SHM version
 *      propagation into the follower's local nodes).
 */

#include <aerospike/aerospike.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_config.h>
#include <aerospike/as_error.h>
#include <aerospike/as_shm_cluster.h>
#include <aerospike/as_version.h>

#include "../test.h"
#include "../aerospike_test.h"

extern as_auth_mode g_auth_mode;

TEST(shm_second_client_master_follower,
	 "SHM client A (tend master) then B (follower): version parity")
{
	as_error err;
	int shm_key = (int)(0xA5A50000 | (getpid() & 0xFFFF));

	// --- Client A: first SHM attach -> tend master; populates as_node_shm. ---
	as_config cfg1;
	as_config_init(&cfg1);
	assert_true(as_config_add_hosts(&cfg1, g_host, g_port));
	cfg1.auth_mode = g_auth_mode;
	cfg1.use_shm = true;
	cfg1.shm_key = shm_key;
	cfg1.conn_timeout_ms = 1000;
	aerospike* as1 = aerospike_new(&cfg1);
	assert_not_null(as1);
	as_error_reset(&err);
	assert_int_eq(aerospike_connect(as1, &err), AEROSPIKE_OK);

	assert_not_null(as1->cluster);
	assert_not_null(as1->cluster->shm_info);
	assert_true(as1->cluster->shm_info->is_tend_master);

	// --- Client B: second SHM attach -> follower; as_shm_reset_nodes from SHM. ---
	as_config cfg2;
	as_config_init(&cfg2);
	assert_true(as_config_add_hosts(&cfg2, g_host, g_port));
	cfg2.auth_mode = g_auth_mode;
	cfg2.use_shm = true;
	cfg2.shm_key = shm_key;
	cfg2.conn_timeout_ms = 1000;
	aerospike* as2 = aerospike_new(&cfg2);
	assert_not_null(as2);

	as_error_reset(&err);
	assert_int_eq(aerospike_connect(as2, &err), AEROSPIKE_OK);

	assert_not_null(as2->cluster);
	assert_not_null(as2->cluster->shm_info);
	assert_false(as2->cluster->shm_info->is_tend_master);

	as_cluster* c1 = as1->cluster;
	as_cluster* c2 = as2->cluster;
	as_nodes* nodes1 = as_nodes_reserve(c1);
	as_nodes* nodes2 = as_nodes_reserve(c2);
	uint32_t n1 = nodes1->size;
	uint32_t n2 = nodes2->size;
	assert_int_eq((int64_t)n1, (int64_t)n2);
	assert_true(n1 > 0);

	for (uint32_t i = 0; i < n1; i++) {
		as_node* na = nodes1->array[i];
		as_node* nb = nodes2->array[i];
		assert_string_eq(na->name, nb->name);
		assert_int_eq(as_version_compare(&na->version, &nb->version), 0);
	}
	as_nodes_release(nodes1);
	as_nodes_release(nodes2);

	aerospike_close(as2, &err);
	aerospike_destroy(as2);
	aerospike_close(as1, &err);
	aerospike_destroy(as1);
}

SUITE(shm_second_client, "shared memory second-client (follower) regression")
{
	suite_add(shm_second_client_master_follower);
}
