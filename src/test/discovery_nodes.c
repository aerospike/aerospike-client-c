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
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <aerospike/aerospike.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_config.h>
#include <aerospike/as_event.h>
#include <aerospike/as_lookup.h>
#include <aerospike/as_lookup_internal.h>
#include <aerospike/as_node.h>
#include <aerospike/as_shm_cluster.h>
#include <aerospike/as_sleep.h>
#include <aerospike/as_tls.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_clock.h>

#include "test.h"
#include "aerospike_test.h"

#if !defined(_WIN32)
#include <arpa/inet.h>
#include <errno.h>
#include <ifaddrs.h>
#include <net/if.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/socket.h>
#include <unistd.h>

#include "discovery_fake_node.h"
#endif

// Node lifecycle on the tend path (Testing Strategy U-18). The tests drive each tend from the
// test thread, as the Java tests do with TendDriver, so every step sees exactly one tend.

#ifdef DISCOVERY_FAKE_SERVERS

//---------------------------------
// Static Functions: tend control
//---------------------------------

// The tend thread runs its first tend after connect and then waits for this interval, which no
// test reaches. Each later tend is run by the test.
#define NODES_TEND_INTERVAL 600000

as_status
as_cluster_tend(as_cluster* cluster, as_error* err, bool is_init);

void
aerospike_destroy_internal(aerospike* as);

static uint32_t
tend_count(as_cluster* cluster)
{
	pthread_mutex_lock(&cluster->tend_lock);
	uint32_t count = cluster->tend_count;
	pthread_mutex_unlock(&cluster->tend_lock);
	return count;
}

// The tend thread holds tend_lock for each whole tend, so a tend run under the lock never
// overlaps one of the tend thread.
static as_status
tend(as_cluster* cluster)
{
	as_error err;
	pthread_mutex_lock(&cluster->tend_lock);
	as_status status = as_cluster_tend(cluster, &err, false);
	pthread_mutex_unlock(&cluster->tend_lock);
	return status;
}

static void
nodes_config(as_config* config, const fnode* seed)
{
	as_config_init(config);
	as_config_add_host(config, seed->host, seed->port);
	config->tender_interval = NODES_TEND_INTERVAL;
	config->conn_timeout_ms = 1000;
	config->fail_if_not_connected = true;
}

// Connect and wait for the first tend of the tend thread. Init tends once, so the count is 2.
static as_status
nodes_connect(aerospike** client, as_config* config)
{
	as_error err;
	*client = aerospike_new(config);
	as_status status = aerospike_connect(*client, &err);

	if (status != AEROSPIKE_OK) {
		info("connect failed: %d %s", err.code, err.message);
		return status;
	}

	uint64_t end = cf_getms() + 5000;

	while (tend_count((*client)->cluster) < 2) {
		if (cf_getms() >= end) {
			return AEROSPIKE_ERR_TIMEOUT;
		}
		as_sleep(10);
	}
	return AEROSPIKE_OK;
}

static void
nodes_close(aerospike* client)
{
	as_error err;

	if (client->cluster) {
		aerospike_close(client, &err);

		if (as_event_loop_size == 0 || as_event_single_thread) {
			aerospike_destroy(client);
		}
	}
	else {
		aerospike_destroy_internal(client);
	}
}

//---------------------------------
// Static Functions: nodes
//---------------------------------

static uint32_t
node_count(as_cluster* cluster)
{
	as_nodes* nodes = as_nodes_reserve(cluster);
	uint32_t size = nodes->size;
	as_nodes_release(nodes);
	return size;
}

// Return the last node of the name, reserved, or NULL. as_node_release() it before the client
// is closed.
static as_node*
get_node(as_cluster* cluster, const char* name)
{
	as_nodes* nodes = as_nodes_reserve(cluster);
	as_node* found = NULL;

	for (uint32_t i = nodes->size; i > 0; i--) {
		if (strcmp(nodes->array[i - 1]->name, name) == 0) {
			found = nodes->array[i - 1];
			as_node_reserve(found);
			break;
		}
	}
	as_nodes_release(nodes);
	return found;
}

static bool
node_at(as_node* node, const fnode* n)
{
	char addr[64];
	fnode_addr(n, addr, sizeof(addr));
	return node && strcmp(as_node_get_address_string(node), addr) == 0;
}

// Validate the server n and create a node for it that is not in the cluster.
static as_node*
create_node(as_cluster* cluster, const fnode* n)
{
	struct sockaddr_in addr;
	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(n->port);
	inet_pton(AF_INET, n->host, &addr.sin_addr);

	as_host host = {.name = (char*)n->host, .tls_name = NULL, .port = n->port};
	as_node_info info;
	as_error err;
	as_node* node = NULL;

	pthread_mutex_lock(&cluster->tend_lock);

	if (as_lookup_node(cluster, &err, &host, (struct sockaddr*)&addr, false, &info, NULL) ==
		AEROSPIKE_OK) {
		node = as_node_create(cluster, &info);
	}
	else {
		info("validate %s:%u failed: %s", n->host, n->port, err.message);
	}
	pthread_mutex_unlock(&cluster->tend_lock);
	return node;
}

// Replace the node array with the given nodes, in order, like a node array that already holds
// two nodes with one name.
static void
set_nodes(as_cluster* cluster, as_node** list, uint32_t size)
{
	as_nodes* nodes = cf_malloc(sizeof(as_nodes) + sizeof(as_node*) * size);
	nodes->ref_count = 1;
	nodes->size = size;

	for (uint32_t i = 0; i < size; i++) {
		as_node_reserve(list[i]);
		nodes->array[i] = list[i];
	}

	pthread_mutex_lock(&cluster->tend_lock);
	as_nodes* old = cluster->nodes;
	as_store_ptr_rls((void**)&cluster->nodes, nodes);
	pthread_mutex_unlock(&cluster->tend_lock);
	as_nodes_release(old);
}

#define assert_one_node_per_name(cluster) \
	do { \
		char __dup[AS_NODE_NAME_SIZE]; \
		if (!discovery_one_node_per_name(cluster, __dup, sizeof(__dup))) { \
			assert_log(true, "two nodes named %s", __dup); \
		} \
	} while (0)

// Fail with the message unless cond holds. assert_log() fails when its expression holds.
#define assert_that(cond, fmt, ...) assert_log(!(cond), fmt, ##__VA_ARGS__)

// Return the first up, non-loopback IPv4 address of this machine, or false. The cluster never
// replaces a node on a loopback address.
static bool
local_address(char* buf, size_t size)
{
	struct ifaddrs* list = NULL;

	if (getifaddrs(&list) != 0) {
		return false;
	}

	bool found = false;

	for (struct ifaddrs* ifa = list; ifa && !found; ifa = ifa->ifa_next) {
		if (!ifa->ifa_addr || ifa->ifa_addr->sa_family != AF_INET ||
			(ifa->ifa_flags & IFF_LOOPBACK) || !(ifa->ifa_flags & IFF_UP)) {
			continue;
		}

		struct sockaddr_in* sin = (struct sockaddr_in*)ifa->ifa_addr;
		found = inet_ntop(AF_INET, &sin->sin_addr, buf, (socklen_t)size) != NULL;
	}
	freeifaddrs(list);
	return found;
}

static bool
start_on(fnode* n, const char* name, const char* host)
{
	fnode_init(n, name);
	return fnode_listen_on(n, host, 0);
}

static void
set_peers(fnode* n, const char* fmt, ...)
{
	char list[240];
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(list, sizeof(list), fmt, ap);
	va_end(ap);

	char reply[sizeof(list) + 16];
	snprintf(reply, sizeof(reply), "1,3000,%s", list);
	fnode_set_peers(n, reply);
}

// Make n drop every connection and refuse new ones, and give its serving loop time to close the
// tend connection.
static void
fail_node(fnode* n)
{
	fnode_set_reject(n, true);
	as_sleep(100);
}

//---------------------------------
// Static Functions: log count
//---------------------------------

#define LOG_COUNT_MAX 4

static pthread_mutex_t g_count_lock = PTHREAD_MUTEX_INITIALIZER;
static as_log_callback g_count_saved;
static const char* g_count_needles[LOG_COUNT_MAX];
static uint32_t g_counts[LOG_COUNT_MAX];

static bool
log_count_cb(as_log_level level, const char* func, const char* file, uint32_t line,
	const char* fmt, ...)
{
	char text[512];
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(text, sizeof(text), fmt, ap);
	va_end(ap);

	pthread_mutex_lock(&g_count_lock);

	for (uint32_t i = 0; i < LOG_COUNT_MAX; i++) {
		if (g_count_needles[i] && strstr(text, g_count_needles[i])) {
			g_counts[i]++;
		}
	}
	as_log_callback saved = g_count_saved;
	pthread_mutex_unlock(&g_count_lock);

	if (saved) {
		saved(level, func, file, line, "%s", text);
	}
	return true;
}

// Count the log messages that contain each needle, up to LOG_COUNT_MAX of them, until
// log_count_stop(). The list ends with NULL.
static void
log_count_start_many(const char* needle, ...)
{
	pthread_mutex_lock(&g_count_lock);
	g_count_saved = g_as_log.callback;

	va_list ap;
	va_start(ap, needle);

	for (uint32_t i = 0; i < LOG_COUNT_MAX; i++) {
		g_count_needles[i] = needle;
		g_counts[i] = 0;

		if (needle) {
			needle = va_arg(ap, const char*);
		}
	}
	va_end(ap);
	pthread_mutex_unlock(&g_count_lock);
	as_log_set_callback(log_count_cb);
}

static void
log_count_start(const char* needle)
{
	log_count_start_many(needle, NULL);
}

// Stop counting, copy the count of each needle to counts, when not NULL, and return the count of
// the first.
static uint32_t
log_count_stop_many(uint32_t* counts)
{
	pthread_mutex_lock(&g_count_lock);
	as_log_callback saved = g_count_saved;
	uint32_t count = g_counts[0];

	for (uint32_t i = 0; i < LOG_COUNT_MAX; i++) {
		if (counts) {
			counts[i] = g_counts[i];
		}
		g_count_needles[i] = NULL;
	}
	pthread_mutex_unlock(&g_count_lock);
	as_log_set_callback(saved);
	return count;
}

static uint32_t
log_count_stop(void)
{
	return log_count_stop_many(NULL);
}

//---------------------------------
// Tests: one node per name (U-18)
//---------------------------------

// Seed S owns every partition, so it is kept while nobody lists it. S lists X at X1 on a local
// non-loopback address, which the cluster may replace. Then X1 fails, and S also lists Z. Z joins
// in the tend's first phase, and its peers in the second phase list X at X2, the same node on
// another port, so X is replaced in the second phase of that tend. A non-zero shm_key enables
// shared memory. Returns NULL before that tend, or the reason the setup failed.
static const char*
second_phase_replace(
	fnode* s, fnode* z, fnode* x1, fnode* x2, int shm_key, aerospike** client
	)
{
	char local[32];

	if (!local_address(local, sizeof(local))) {
		return "no non-loopback IPv4 address";
	}

	if (!start_on(s, "S", "127.0.0.1") || !start_on(z, "Z", "127.0.0.1") ||
		!start_on(x1, "X", local) || !start_on(x2, "X", local)) {
		return "cannot start fake nodes";
	}
	fnode_set_partitions(s, true);
	set_peers(s, "[[X,,[%s:%u]]]", local, x1->port);
	set_peers(x1, "[[S,,[127.0.0.1:%u]]]", s->port);
	set_peers(x2, "[[S,,[127.0.0.1:%u]]]", s->port);
	set_peers(z, "[[S,,[127.0.0.1:%u]],[X,,[%s:%u]]]", s->port, local, x2->port);

	as_config config;
	nodes_config(&config, s);

	if (shm_key != 0) {
		config.use_shm = true;
		config.shm_key = shm_key;
	}

	if (nodes_connect(client, &config) != AEROSPIKE_OK) {
		return "connect failed";
	}

	as_cluster* cluster = (*client)->cluster;
	as_node* x = get_node(cluster, "X");
	bool at_x1 = node_at(x, x1);

	if (x) {
		as_node_release(x);
	}

	if (node_count(cluster) != 2 || !at_x1) {
		return "X did not join at X1";
	}

	if (tend(cluster) != AEROSPIKE_OK || node_count(cluster) != 2) {
		return "the cluster is not stable";
	}

	set_peers(s, "[[X,,[%s:%u]],[Z,,[127.0.0.1:%u]]]", local, x1->port, z->port);
	fail_node(x1);
	return NULL;
}

TEST(nodes_one_per_name_after_second_phase_replace,
	"U-18 oneNodePerNameAfterSecondPhaseReplace: tends after a second-phase replace keep one node per name")
{
	DISCOVERY_SCOPED(fnode, s, fnode_stop);
	DISCOVERY_SCOPED(fnode, z, fnode_stop);
	DISCOVERY_SCOPED(fnode, x1, fnode_stop);
	DISCOVERY_SCOPED(fnode, x2, fnode_stop);
	aerospike* client = NULL;
	char local[32];

	if (!local_address(local, sizeof(local))) {
		info("skipped: no non-loopback IPv4 address");
		return;
	}

	const char* failure = second_phase_replace(&s, &z, &x1, &x2, 0, &client);
	assert_log(failure != NULL, "%s", failure ? failure : "");

	for (int i = 0; i < 3; i++) {
		assert_int_eq(tend(client->cluster), AEROSPIKE_OK);
		assert_one_node_per_name(client->cluster);
	}
	assert_int_eq(node_count(client->cluster), 3);
	nodes_close(client);
}

TEST(nodes_second_phase_replace_same_tend,
	"U-18 secondPhaseReplaceRemovesNodeInSameTend: a second-phase replacement removes the old node in that tend")
{
	DISCOVERY_SCOPED(fnode, s, fnode_stop);
	DISCOVERY_SCOPED(fnode, z, fnode_stop);
	DISCOVERY_SCOPED(fnode, x1, fnode_stop);
	DISCOVERY_SCOPED(fnode, x2, fnode_stop);
	aerospike* client = NULL;
	char local[32];

	if (!local_address(local, sizeof(local))) {
		info("skipped: no non-loopback IPv4 address");
		return;
	}

	const char* failure = second_phase_replace(&s, &z, &x1, &x2, 0, &client);
	assert_log(failure != NULL, "%s", failure ? failure : "");

	as_cluster* cluster = client->cluster;
	as_node* old_x = get_node(cluster, "X");
	assert_not_null(old_x);

	log_count_start("Replace node X");
	as_status status = tend(cluster);
	uint32_t replaced = log_count_stop();
	assert_int_eq(status, AEROSPIKE_OK);

	assert_one_node_per_name(cluster);
	assert_int_eq(node_count(cluster), 3);
	assert_false(as_node_is_active(old_x));

	as_node* new_x = get_node(cluster, "X");
	as_node* z_node = get_node(cluster, "Z");
	bool same = new_x == old_x;
	bool active = new_x && as_node_is_active(new_x);
	bool at_x2 = node_at(new_x, &x2);
	bool z_joined = z_node != NULL;

	if (new_x) {
		as_node_release(new_x);
	}

	if (z_node) {
		as_node_release(z_node);
	}
	as_node_release(old_x);

	assert_false(same);
	assert_true(active);
	assert_true(at_x2);
	assert_true(z_joined);
	assert_int_eq(replaced, 1);
	nodes_close(client);
}

TEST(nodes_not_name_holder_removed,
	"U-18 nodeNotInMapIsRemovedWithoutMove: a node that no longer holds its name is removed")
{
	DISCOVERY_SCOPED(fnode, x1, fnode_stop);
	DISCOVERY_SCOPED(fnode, x2, fnode_stop);
	assert_true(start_on(&x1, "X", "127.0.0.1"));
	assert_true(start_on(&x2, "X", "127.0.0.1"));

	// X1 owns every partition, so only the name rule can remove a node of this cluster.
	fnode_set_partitions(&x1, true);

	as_config config;
	nodes_config(&config, &x1);
	aerospike* client = NULL;
	assert_int_eq(nodes_connect(&client, &config), AEROSPIKE_OK);

	as_cluster* cluster = client->cluster;
	as_node* current = get_node(cluster, "X");
	assert_not_null(current);
	assert_int_eq(node_count(cluster), 1);

	// The stale node is first, so the current node holds the name. The stale server keeps
	// answering, and its peers generation is already known, so the stale node neither fails nor
	// makes the tend refresh peers.
	as_node* stale = create_node(cluster, &x2);
	assert_not_null(stale);
	stale->peers_generation = 1;
	set_nodes(cluster, (as_node*[]){stale, current}, 2);
	assert_int_eq(node_count(cluster), 2);

	log_count_start("Replace node");
	as_status status = tend(cluster);
	uint32_t replaced = log_count_stop();
	assert_int_eq(status, AEROSPIKE_OK);

	assert_one_node_per_name(cluster);
	assert_int_eq(node_count(cluster), 1);

	as_node* holder = get_node(cluster, "X");
	bool same = holder == current;
	bool stale_active = as_node_is_active(stale);
	bool current_active = as_node_is_active(current);

	if (holder) {
		as_node_release(holder);
	}
	as_node_release(stale);
	as_node_release(current);

	assert_true(same);
	assert_false(stale_active);
	assert_true(current_active);
	assert_int_eq(replaced, 0);
	nodes_close(client);
}

TEST(nodes_same_name_removals_applied,
	"U-18 sameNameRemovalsBothApplied: two nodes with one name are both removed in one tend")
{
	DISCOVERY_SCOPED(fnode, s, fnode_stop);
	DISCOVERY_SCOPED(fnode, x1, fnode_stop);
	DISCOVERY_SCOPED(fnode, x3, fnode_stop);
	DISCOVERY_SCOPED(fnode, xs, fnode_stop);
	char local[32];

	if (!local_address(local, sizeof(local))) {
		info("skipped: no non-loopback IPv4 address");
		return;
	}
	assert_true(start_on(&s, "S", "127.0.0.1"));
	assert_true(start_on(&x1, "X", local));
	assert_true(start_on(&x3, "X", local));
	assert_true(start_on(&xs, "X", "127.0.0.1"));
	fnode_set_partitions(&s, true);
	set_peers(&s, "[[X,,[%s:%u]]]", local, x1.port);
	set_peers(&x1, "[[S,,[127.0.0.1:%u]]]", s.port);
	set_peers(&x3, "[[S,,[127.0.0.1:%u]]]", s.port);

	as_config config;
	nodes_config(&config, &s);
	aerospike* client = NULL;
	assert_int_eq(nodes_connect(&client, &config), AEROSPIKE_OK);

	as_cluster* cluster = client->cluster;
	as_node* seed = get_node(cluster, "S");
	as_node* current = get_node(cluster, "X");
	assert_not_null(seed);
	assert_not_null(current);
	assert_true(node_at(current, &x1));

	// Current X fails and S lists X at X3, so the tend replaces current X. A stale X before it
	// no longer holds the name, so the same tend removes it too.
	as_node* stale = create_node(cluster, &xs);
	assert_not_null(stale);
	stale->peers_generation = 1;
	set_peers(&s, "[[X,,[%s:%u]]]", local, x3.port);
	fail_node(&x1);
	set_nodes(cluster, (as_node*[]){seed, stale, current}, 3);
	as_node_release(seed);

	log_count_start("Replace node X");
	as_status status = tend(cluster);
	uint32_t replaced = log_count_stop();
	assert_int_eq(status, AEROSPIKE_OK);

	assert_one_node_per_name(cluster);
	assert_int_eq(node_count(cluster), 2);

	as_node* moved = get_node(cluster, "X");
	bool current_active = as_node_is_active(current);
	bool stale_active = as_node_is_active(stale);
	bool moved_active = moved && as_node_is_active(moved);
	bool at_x3 = node_at(moved, &x3);

	if (moved) {
		as_node_release(moved);
	}
	as_node_release(stale);
	as_node_release(current);

	assert_false(current_active);
	assert_false(stale_active);
	assert_true(moved_active);
	assert_true(at_x3);
	assert_int_eq(replaced, 1);
	nodes_close(client);
}

#if !defined(__APPLE__)
// Return a shared memory key of this process that no segment uses, or zero.
static int
shm_test_key(void)
{
	int key = (int)(0xA5D10000 | ((uint32_t)getpid() & 0xFFFF));
	int probe = shmget(key, 4096, IPC_CREAT | IPC_EXCL | 0666);

	if (probe < 0) {
		info("skipped: shared memory unavailable: %s", strerror(errno));
		return 0;
	}
	shmctl(probe, IPC_RMID, 0);
	return key;
}

TEST(nodes_shm_replace_keeps_slot_active,
	"U-18 a node replaced in one tend leaves its shared memory slot active for the replacement")
{
	DISCOVERY_SCOPED(fnode, s, fnode_stop);
	DISCOVERY_SCOPED(fnode, z, fnode_stop);
	DISCOVERY_SCOPED(fnode, x1, fnode_stop);
	DISCOVERY_SCOPED(fnode, x2, fnode_stop);
	char local[32];

	if (!local_address(local, sizeof(local))) {
		info("skipped: no non-loopback IPv4 address");
		return;
	}

	int key = shm_test_key();

	if (key == 0) {
		return;
	}

	// Shared memory partition maps hold node indexes, not node references, so S may leave in
	// the same tend and join again through Z. Only X is checked.
	aerospike* client = NULL;
	const char* failure = second_phase_replace(&s, &z, &x1, &x2, key, &client);
	assert_log(failure != NULL, "%s", failure ? failure : "");

	as_cluster* cluster = client->cluster;
	assert_true(cluster->shm_info->is_tend_master);

	as_node* old_x = get_node(cluster, "X");
	assert_not_null(old_x);
	uint32_t old_index = old_x->index;

	assert_int_eq(tend(cluster), AEROSPIKE_OK);
	assert_one_node_per_name(cluster);

	as_node* new_x = get_node(cluster, "X");
	assert_not_null(new_x);

	as_shm_info* shm_info = cluster->shm_info;
	as_node_shm* node_shm = &shm_info->cluster_shm->nodes[new_x->index];
	as_swlock_read_lock(&node_shm->lock);
	bool active = node_shm->active;
	bool named = strcmp(node_shm->name, "X") == 0;
	as_swlock_read_unlock(&node_shm->lock);

	bool replaced = new_x != old_x;
	bool same_slot = new_x->index == old_index;
	bool local_node = shm_info->local_nodes[new_x->index] == new_x;
	as_node_release(new_x);
	as_node_release(old_x);

	assert_true(replaced);
	assert_true(same_slot);
	assert_true(active);
	assert_true(named);
	assert_true(local_node);
	nodes_close(client);
}

// Samples the node array of a cluster for the nodes named X.
typedef struct {
	as_cluster* cluster;
	pthread_t thread;
	uint8_t stop;
	uint32_t samples;
	uint32_t missing;
	uint32_t duplicates;
} name_watch;

static void*
name_watch_run(void* arg)
{
	name_watch* w = arg;

	while (!as_load_uint8(&w->stop)) {
		as_nodes* nodes = as_nodes_reserve(w->cluster);
		uint32_t count = 0;

		for (uint32_t i = 0; i < nodes->size; i++) {
			if (strcmp(nodes->array[i]->name, "X") == 0) {
				count++;
			}
		}
		as_nodes_release(nodes);

		if (count == 0) {
			w->missing++;
		}
		else if (count > 1) {
			w->duplicates++;
		}
		w->samples++;
		as_sleep(1);
	}
	return NULL;
}

TEST(nodes_shm_follower_replaces_node,
	"U-18 a shared memory process that is not the tend master swaps a replaced node in one step")
{
	DISCOVERY_SCOPED(fnode, s, fnode_stop);
	DISCOVERY_SCOPED(fnode, z, fnode_stop);
	DISCOVERY_SCOPED(fnode, x1, fnode_stop);
	DISCOVERY_SCOPED(fnode, x2, fnode_stop);
	char local[32];

	if (!local_address(local, sizeof(local))) {
		info("skipped: no non-loopback IPv4 address");
		return;
	}

	int key = shm_test_key();

	if (key == 0) {
		return;
	}

	aerospike* client = NULL;
	const char* failure = second_phase_replace(&s, &z, &x1, &x2, key, &client);
	assert_log(failure != NULL, "%s", failure ? failure : "");
	assert_true(client->cluster->shm_info->is_tend_master);

	// The follower attaches to the same segment. Its process, which is the master's too, stays
	// alive, so it never takes over tending.
	as_config config;
	nodes_config(&config, &s);
	config.use_shm = true;
	config.shm_key = key;
	config.tender_interval = 250;

	as_error err;
	aerospike* follower = aerospike_new(&config);

	if (aerospike_connect(follower, &err) != AEROSPIKE_OK) {
		nodes_close(follower);
		nodes_close(client);
		assert_log(true, "follower connect failed: %d %s", err.code, err.message);
	}

	as_cluster* cluster = follower->cluster;
	bool master = cluster->shm_info->is_tend_master;
	as_node* old_x = get_node(cluster, "X");
	bool at_x1 = node_at(old_x, &x1);

	name_watch w = {.cluster = cluster};
	bool watching = !master && at_x1 &&
		pthread_create(&w.thread, NULL, name_watch_run, &w) == 0;

	as_status status = watching ? tend(client->cluster) : AEROSPIKE_ERR_CLIENT;

	// The follower syncs at its next tend interval after the master's swap.
	as_node* new_x = NULL;
	uint64_t end = cf_getms() + 2000;

	while (watching && cf_getms() < end) {
		new_x = get_node(cluster, "X");

		if (new_x && new_x != old_x && node_at(new_x, &x2)) {
			break;
		}

		if (new_x) {
			as_node_release(new_x);
			new_x = NULL;
		}
		as_sleep(10);
	}

	if (watching) {
		as_store_uint8(&w.stop, 1);
		pthread_join(w.thread, NULL);
	}

	char dup[AS_NODE_NAME_SIZE];
	bool unique = discovery_one_node_per_name(cluster, dup, sizeof(dup));
	bool moved = new_x != NULL;
	bool old_active = old_x && as_node_is_active(old_x);

	if (new_x) {
		as_node_release(new_x);
	}

	if (old_x) {
		as_node_release(old_x);
	}

	nodes_close(follower);
	nodes_close(client);

	assert_false(master);
	assert_true(at_x1);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_true(moved);
	assert_false(old_active);
	assert_log(!unique, "two nodes named %s", dup);
	assert_that(w.missing == 0, "X was missing in %u of %u samples", w.missing, w.samples);
	assert_that(w.duplicates == 0, "X was duplicated in %u of %u samples", w.duplicates,
		w.samples);
}
#endif

//---------------------------------
// Resolver
//---------------------------------

#define RESOLVER_MAX_NAMES 8
#define RESOLVER_MAX_ADDRS 4

// Marks the lists the test resolver builds, so its free function tells them from system lists.
#define RESOLVER_MARK 0x10000000

typedef struct {
	char name[64];
	char addrs[RESOLVER_MAX_ADDRS][16];
	uint32_t count;
	uint32_t lookups;

	// Each lookup sleeps delay_ms, and the thread of the last delayed lookup is recorded.
	uint32_t delay_ms;
	pthread_t delay_thread;
	bool delayed;
} resolver_entry;

static pthread_mutex_t g_resolver_lock = PTHREAD_MUTEX_INITIALIZER;
static resolver_entry g_resolver[RESOLVER_MAX_NAMES];
static uint32_t g_resolver_size;
static uint32_t g_resolver_dns;

static resolver_entry*
resolver_find(const char* name)
{
	for (uint32_t i = 0; i < g_resolver_size; i++) {
		if (strcmp(g_resolver[i].name, name) == 0) {
			return &g_resolver[i];
		}
	}
	return NULL;
}

// Registered names resolve to their addresses, other names through the system. Lookups that are
// not of an IP literal are counted.
static int
resolver_getaddrinfo(
	const char* node, const char* service, const struct addrinfo* hints, struct addrinfo** res
	)
{
	pthread_mutex_lock(&g_resolver_lock);

	if (!hints || !(hints->ai_flags & AI_NUMERICHOST)) {
		g_resolver_dns++;
	}

	resolver_entry* e = resolver_find(node);

	if (!e) {
		pthread_mutex_unlock(&g_resolver_lock);
		return getaddrinfo(node, service, hints, res);
	}
	e->lookups++;

	uint32_t delay_ms = e->delay_ms;

	if (delay_ms > 0) {
		e->delay_thread = pthread_self();
		e->delayed = true;
	}

	struct addrinfo* head = NULL;
	struct addrinfo** tail = &head;

	for (uint32_t i = 0; i < e->count; i++) {
		struct addrinfo* ai = calloc(1, sizeof(struct addrinfo) + sizeof(struct sockaddr_in));
		struct sockaddr_in* sin = (struct sockaddr_in*)(ai + 1);
		sin->sin_family = AF_INET;
		inet_pton(AF_INET, e->addrs[i], &sin->sin_addr);
		ai->ai_flags = RESOLVER_MARK;
		ai->ai_family = AF_INET;
		ai->ai_socktype = SOCK_STREAM;
		ai->ai_protocol = IPPROTO_TCP;
		ai->ai_addrlen = sizeof(struct sockaddr_in);
		ai->ai_addr = (struct sockaddr*)sin;
		*tail = ai;
		tail = &ai->ai_next;
	}
	pthread_mutex_unlock(&g_resolver_lock);

	if (delay_ms > 0) {
		as_sleep(delay_ms);
	}
	*res = head;
	return head ? 0 : EAI_NONAME;
}

static void
resolver_freeaddrinfo(struct addrinfo* res)
{
	if (res && !(res->ai_flags & RESOLVER_MARK)) {
		freeaddrinfo(res);
		return;
	}

	while (res) {
		struct addrinfo* next = res->ai_next;
		free(res);
		res = next;
	}
}

void
discovery_nodes_resolver_install(void)
{
	as_lookup_set_resolver(resolver_getaddrinfo, resolver_freeaddrinfo);
}

#if defined(__linux__)

//---------------------------------
// Static Functions: resolver
//---------------------------------

// Linux accepts on all of 127/8 and drops SYNs to a full accept backlog, which the staggered
// connect tests need.

#define STAGGER_NODE "BB9D000000000001"
#define STAGGER_OTHER "BB9D000000000002"
#define STAGGER_DELAY 100
#define STAGGER_MARGIN 400

// Start a test with no registered names and zeroed counters. The resolver itself stays
// installed from before the shared client connected.
static void
resolver_install(void)
{
	pthread_mutex_lock(&g_resolver_lock);
	g_resolver_size = 0;
	g_resolver_dns = 0;
	pthread_mutex_unlock(&g_resolver_lock);
}

static void
resolver_remove(void)
{
	pthread_mutex_lock(&g_resolver_lock);
	g_resolver_size = 0;
	pthread_mutex_unlock(&g_resolver_lock);
}

// Register name, or replace its addresses and reset its lookup count. The address list ends
// with NULL.
static void
resolver_register(const char* name, ...)
{
	pthread_mutex_lock(&g_resolver_lock);
	resolver_entry* e = resolver_find(name);

	if (!e && g_resolver_size < RESOLVER_MAX_NAMES) {
		e = &g_resolver[g_resolver_size++];
		snprintf(e->name, sizeof(e->name), "%s", name);
	}

	if (e) {
		e->count = 0;
		e->lookups = 0;
		e->delay_ms = 0;
		e->delayed = false;

		va_list ap;
		va_start(ap, name);
		const char* addr;

		while ((addr = va_arg(ap, const char*)) != NULL && e->count < RESOLVER_MAX_ADDRS) {
			snprintf(e->addrs[e->count++], sizeof(e->addrs[0]), "%s", addr);
		}
		va_end(ap);
	}
	pthread_mutex_unlock(&g_resolver_lock);
}

static uint32_t
resolver_lookups(const char* name)
{
	pthread_mutex_lock(&g_resolver_lock);
	resolver_entry* e = resolver_find(name);
	uint32_t lookups = e ? e->lookups : 0;
	pthread_mutex_unlock(&g_resolver_lock);
	return lookups;
}

// Make each later lookup of the registered name sleep delay_ms.
static void
resolver_set_delay(const char* name, uint32_t delay_ms)
{
	pthread_mutex_lock(&g_resolver_lock);
	resolver_entry* e = resolver_find(name);

	if (e) {
		e->delay_ms = delay_ms;
	}
	pthread_mutex_unlock(&g_resolver_lock);
}

// Copy the thread of the last delayed lookup of name to thread, or return false.
static bool
resolver_delay_thread(const char* name, pthread_t* thread)
{
	pthread_mutex_lock(&g_resolver_lock);
	resolver_entry* e = resolver_find(name);
	bool delayed = e && e->delayed;

	if (delayed) {
		*thread = e->delay_thread;
	}
	pthread_mutex_unlock(&g_resolver_lock);
	return delayed;
}

static uint32_t
resolver_dns_lookups(void)
{
	pthread_mutex_lock(&g_resolver_lock);
	uint32_t lookups = g_resolver_dns;
	pthread_mutex_unlock(&g_resolver_lock);
	return lookups;
}

//---------------------------------
// Static Functions: staggered connect
//---------------------------------

typedef struct {
	fnode seed;
	aerospike* client;
} stagger_env;

// Client whose cluster has the connect timeout and tend interval of a test, and logs in as user
// when not NULL.
static bool
stagger_cluster(stagger_env* env, uint32_t timeout_ms, uint32_t tend_interval, const char* user)
{
	if (!start_on(&env->seed, "SEED", "127.0.0.1")) {
		return false;
	}

	as_config config;
	as_config_init(&config);
	as_config_add_host(&config, env->seed.host, env->seed.port);
	config.conn_timeout_ms = timeout_ms;
	config.tender_interval = tend_interval;
	config.fail_if_not_connected = true;

	if (user) {
		as_config_set_user(&config, user, "password");
	}
	resolver_install();

	as_error err;
	env->client = aerospike_new(&config);

	if (aerospike_connect(env->client, &err) != AEROSPIKE_OK) {
		info("connect failed: %d %s", err.code, err.message);
		return false;
	}
	return true;
}

static void
stagger_close(stagger_env* env)
{
	if (env->client) {
		nodes_close(env->client);
		env->client = NULL;
	}
	resolver_remove();
	fnode_stop(&env->seed);
}

// A port free on 127.0.0.1, 127.0.0.2 and 127.0.0.3, or zero.
static uint16_t
free_port(void)
{
	const char* hosts[] = {"127.0.0.2", "127.0.0.1", "127.0.0.3"};

	for (int attempt = 0; attempt < 100; attempt++) {
		uint16_t port = 0;
		bool bound = true;

		for (int i = 0; i < 3 && bound; i++) {
			int fd = socket(AF_INET, SOCK_STREAM, 0);
			struct sockaddr_in addr;
			memset(&addr, 0, sizeof(addr));
			addr.sin_family = AF_INET;
			addr.sin_port = htons(port);
			inet_pton(AF_INET, hosts[i], &addr.sin_addr);
			socklen_t len = sizeof(addr);

			bound = fd >= 0 && bind(fd, (struct sockaddr*)&addr, sizeof(addr)) == 0 &&
				getsockname(fd, (struct sockaddr*)&addr, &len) == 0;
			port = ntohs(addr.sin_port);

			if (fd >= 0) {
				close(fd);
			}
		}

		if (bound) {
			return port;
		}
	}
	return 0;
}

static bool
serve(fnode* n, const char* ip, uint16_t port)
{
	fnode_init(n, STAGGER_NODE);
	return fnode_listen_on(n, ip, port);
}

// Listener at ip and port whose connects hang.
static bool
hung(fnode* n, const char* ip, uint16_t port)
{
	return serve(n, ip, port) && fnode_set_silent(n, true);
}

// Unique host name, registered with the given addresses, ending with NULL.
#define stagger_hostname(buf, prefix, ...) \
	do { \
		snprintf(buf, sizeof(buf), "%s-%llu.connect.test", prefix, \
			(unsigned long long)cf_getns()); \
		resolver_register(buf, __VA_ARGS__, NULL); \
	} while (0)

typedef struct {
	as_status status;
	as_error err;
	as_node_info info;
	bool alias;
	bool alternate_missing;
	uint64_t begin_us;
	uint64_t elapsed_ms;
} stagger_result;

// Settings of one validation. The cluster's TLS context and use_services_alternate are replaced
// for its duration.
typedef struct {
	as_lookup_accept_fn accept;
	void* udata;
	as_tls_context* tls;
	bool alternate;
} validate_opts;

// Validate host name and port as a new peer is validated, on the tend thread's lock. On
// success the node's socket is closed.
static void
validate_with(
	as_cluster* cluster, const char* name, uint16_t port, const validate_opts* opts,
	stagger_result* r
	)
{
	static const validate_opts none = {0};

	if (!opts) {
		opts = &none;
	}

	as_host host = {.name = (char*)name, .tls_name = opts->tls ? (char*)"fake" : NULL,
		.port = port};
	as_address_iterator iter;
	as_error_init(&r->err);
	r->alternate_missing = false;

	pthread_mutex_lock(&cluster->tend_lock);
	as_tls_context* tls = cluster->tls_ctx;
	bool alternate = cluster->use_services_alternate;
	cluster->tls_ctx = opts->tls;
	cluster->use_services_alternate = opts->alternate;
	r->begin_us = cf_getus();
	r->status = as_lookup_host(&iter, &r->err, name, port);

	if (r->status == AEROSPIKE_OK) {
		r->alias = iter.hostname_is_alias;
		r->status = as_lookup_validate_any(cluster, &r->err, &host, &iter, opts->accept,
			opts->udata, &r->info, &r->alternate_missing);
		as_lookup_end(&iter);
	}
	r->elapsed_ms = (cf_getus() - r->begin_us) / 1000;
	cluster->tls_ctx = tls;
	cluster->use_services_alternate = alternate;
	pthread_mutex_unlock(&cluster->tend_lock);

	if (r->status == AEROSPIKE_OK) {
		as_node_info_destroy(&r->info);
	}
}

static void
validate(as_cluster* cluster, const char* name, uint16_t port, stagger_result* r)
{
	validate_with(cluster, name, port, NULL, r);
}

// Accept only the node named udata.
static as_lookup_verdict
accept_name(as_node_info* info, void* udata)
{
	return strcmp(info->name, (const char*)udata) == 0 ? AS_LOOKUP_ACCEPT : AS_LOOKUP_NEXT;
}

static as_lookup_verdict
reject_all(as_node_info* info, void* udata)
{
	(void)info;
	(void)udata;
	return AS_LOOKUP_NEXT;
}

// Number of last accepted addresses of the cluster.
static uint32_t
valid_count(as_cluster* cluster)
{
	pthread_mutex_lock(&cluster->tend_lock);
	uint32_t count = cluster->valid_addresses ? cluster->valid_addresses->size : 0;
	pthread_mutex_unlock(&cluster->tend_lock);
	return count;
}

// Copy the node name of the last accepted address of name to node, or return false.
static bool
valid_node(as_cluster* cluster, const char* name, char* node, size_t size)
{
	bool found = false;
	pthread_mutex_lock(&cluster->tend_lock);
	as_vector* list = cluster->valid_addresses;

	for (uint32_t i = 0; list && i < list->size && !found; i++) {
		as_lookup_valid_address* va = as_vector_get(list, i);

		if (strcmp(va->name, name) == 0) {
			snprintf(node, size, "%s", va->node_name);
			found = true;
		}
	}
	pthread_mutex_unlock(&cluster->tend_lock);
	return found;
}

static bool
validated_at(const stagger_result* r, const char* ip)
{
	char name[INET_ADDRSTRLEN];
	const struct sockaddr_in* sin = (const struct sockaddr_in*)&r->info.addr;
	return r->status == AEROSPIKE_OK && sin->sin_family == AF_INET &&
		inet_ntop(AF_INET, &sin->sin_addr, name, sizeof(name)) && strcmp(name, ip) == 0;
}

// Milliseconds from the start of the validation to the first accept of n.
static int64_t
started_ms(fnode* n, const stagger_result* r)
{
	uint64_t accepted = fnode_first_accept_us(n);
	return accepted ? ((int64_t)accepted - (int64_t)r->begin_us) / 1000 : -1;
}

#define FIRST "127.0.0.1"
#define SECOND "127.0.0.2"
#define THIRD "127.0.0.3"

#define stagger_setup_user(env, timeout_ms, tend_interval, user, port) \
	DISCOVERY_SCOPED(stagger_env, env, stagger_close); \
	assert_true(stagger_cluster(&env, timeout_ms, tend_interval, user)); \
	uint16_t port = free_port(); \
	assert_int_ne(port, 0)

#define stagger_setup(env, timeout_ms, tend_interval, port) \
	stagger_setup_user(env, timeout_ms, tend_interval, NULL, port)

#define assert_hung(n, ip, port) \
	do { \
		if (!hung(n, ip, port)) { \
			info("skipped: a connect to %s:%u does not hang", ip, port); \
			return; \
		} \
	} while (0)

//---------------------------------
// Tests: staggered connect (U-17)
//---------------------------------

TEST(nodes_stagger_first_address_one_connect,
	"U-17 firstAddressAnswersWithOneConnect: an answering first address is validated, the next never starts")
{
	stagger_setup(env, 1000, 250, port);
	DISCOVERY_SCOPED(fnode, live, fnode_stop);
	DISCOVERY_SCOPED(fnode, other, fnode_stop);
	assert_true(serve(&live, FIRST, port));
	assert_true(serve(&other, SECOND, port));
	char hostname[64];
	stagger_hostname(hostname, "first", FIRST, SECOND);

	stagger_result r;
	validate(env.client->cluster, hostname, port, &r);
	assert_int_eq(r.status, AEROSPIKE_OK);
	assert_true(validated_at(&r, FIRST));

	// The validation runs on the connect of the staggered attempt.
	assert_int_eq(fnode_get(&live, &live.accepts), 1);
	assert_int_eq(fnode_get(&other, &other.accepts), 0);
}

TEST(nodes_stagger_hung_address_delays_next,
	"U-17 hungAddressDelaysNextByStagger: a hung first address starts the next one after the stagger delay")
{
	stagger_setup(env, 3000, 250, port);
	DISCOVERY_SCOPED(fnode, silent, fnode_stop);
	DISCOVERY_SCOPED(fnode, live, fnode_stop);
	assert_hung(&silent, SECOND, port);
	assert_true(serve(&live, FIRST, port));
	char hostname[64];
	stagger_hostname(hostname, "hung", SECOND, FIRST);

	stagger_result r;
	validate(env.client->cluster, hostname, port, &r);
	int64_t started = started_ms(&live, &r);

	assert_int_eq(r.status, AEROSPIKE_OK);
	assert_that(started >= STAGGER_DELAY && started < STAGGER_DELAY + STAGGER_MARGIN,
		"second address started at %lld ms", (long long)started);
	assert_that(r.elapsed_ms < 250 + STAGGER_MARGIN, "validation took %llu ms",
		(unsigned long long)r.elapsed_ms);
	assert_string_eq(r.info.name, STAGGER_NODE);
	assert_true(validated_at(&r, FIRST));
}

TEST(nodes_stagger_every_address_starts_within_bound,
	"U-17 everyAddressStartsWithinBound: the delay shrinks so the last address starts within the bound")
{
	stagger_setup(env, 200, 1000, port);
	DISCOVERY_SCOPED(fnode, hung1, fnode_stop);
	DISCOVERY_SCOPED(fnode, hung2, fnode_stop);
	DISCOVERY_SCOPED(fnode, live, fnode_stop);
	assert_hung(&hung1, SECOND, port);
	assert_hung(&hung2, THIRD, port);
	assert_true(serve(&live, FIRST, port));
	char hostname[64];
	stagger_hostname(hostname, "three", SECOND, THIRD, FIRST);

	stagger_result r;
	validate(env.client->cluster, hostname, port, &r);
	int64_t started = started_ms(&live, &r);

	// The delay is 200 / 3 ms, so the third address starts after two delays and before the bound.
	assert_int_eq(r.status, AEROSPIKE_OK);
	assert_that(started >= 2 * (200 / 3) && started < 200, "third address started at %lld ms",
		(long long)started);
	assert_true(validated_at(&r, FIRST));
}

TEST(nodes_stagger_refused_address_starts_next,
	"U-17 refusedAddressStartsNextAtOnce: a refused connect starts the next address at once")
{
	stagger_setup(env, 1000, 250, port);
	DISCOVERY_SCOPED(fnode, live, fnode_stop);
	assert_true(serve(&live, SECOND, port));
	char hostname[64];
	stagger_hostname(hostname, "refused", FIRST, SECOND);

	stagger_result r;
	validate(env.client->cluster, hostname, port, &r);
	int64_t started = started_ms(&live, &r);

	assert_int_eq(r.status, AEROSPIKE_OK);
	assert_that(started >= 0 && started < STAGGER_DELAY / 2, "second address started at %lld ms",
		(long long)started);
	assert_true(validated_at(&r, SECOND));
}

TEST(nodes_stagger_every_address_hung_fails,
	"U-17 everyAddressHungFailsWithinBound: hung addresses fail within the bound, naming the host, count and bound")
{
	stagger_setup(env, 3000, 250, port);
	DISCOVERY_SCOPED(fnode, hung1, fnode_stop);
	DISCOVERY_SCOPED(fnode, hung2, fnode_stop);
	assert_hung(&hung1, FIRST, port);
	assert_hung(&hung2, SECOND, port);
	char hostname[64];
	stagger_hostname(hostname, "dead", FIRST, SECOND);

	stagger_result r;
	validate(env.client->cluster, hostname, port, &r);

	char host_port[80];
	snprintf(host_port, sizeof(host_port), "%s %u", hostname, port);
	assert_int_eq(r.status, AEROSPIKE_ERR_CONNECTION);
	assert_that(r.elapsed_ms < 250 + STAGGER_MARGIN, "validation took %llu ms",
		(unsigned long long)r.elapsed_ms);
	assert_that(strstr(r.err.message, host_port), "%s", r.err.message);
	assert_that(strstr(r.err.message, "2 addresses"), "%s", r.err.message);
	assert_that(strstr(r.err.message, "250ms"), "%s", r.err.message);
}

TEST(nodes_stagger_last_valid_preferred,
	"U-17 lastValidAddressIsPreferred: the address last validated for the host is tried first")
{
	stagger_setup(env, 1000, 250, port);
	DISCOVERY_SCOPED(fnode, a, fnode_stop);
	DISCOVERY_SCOPED(fnode, b, fnode_stop);
	assert_true(serve(&a, FIRST, port));
	assert_true(serve(&b, SECOND, port));
	char hostname[64];
	stagger_hostname(hostname, "prefer", FIRST, SECOND);

	stagger_result r;
	validate(env.client->cluster, hostname, port, &r);
	assert_int_eq(r.status, AEROSPIKE_OK);

	const char* chosen = validated_at(&r, FIRST) ? FIRST : SECOND;
	const char* other = strcmp(chosen, FIRST) == 0 ? SECOND : FIRST;
	assert_true(validated_at(&r, chosen));
	resolver_register(hostname, other, chosen, NULL);

	for (int i = 0; i < 10; i++) {
		validate(env.client->cluster, hostname, port, &r);
		assert_that(validated_at(&r, chosen), "call %d validated another address", i);
	}
	assert_int_eq(resolver_lookups(hostname), 10);
}

TEST(nodes_stagger_single_address_as_before,
	"U-17 singleAddressConnectsAsBefore: a host with one address waits the full connect timeout")
{
	stagger_setup(env, 600, 250, port);
	DISCOVERY_SCOPED(fnode, live, fnode_stop);
	assert_true(serve(&live, FIRST, port));
	char hostname[64];
	stagger_hostname(hostname, "single", FIRST);

	stagger_result r;
	validate(env.client->cluster, hostname, port, &r);
	assert_int_eq(r.status, AEROSPIKE_OK);
	assert_true(validated_at(&r, FIRST));
	assert_true(r.alias);
	assert_int_eq(resolver_lookups(hostname), 1);

	uint16_t hung_port = free_port();
	assert_int_ne(hung_port, 0);
	DISCOVERY_SCOPED(fnode, silent, fnode_stop);
	assert_hung(&silent, FIRST, hung_port);
	char hung_name[64];
	stagger_hostname(hung_name, "single-hung", FIRST);

	validate(env.client->cluster, hung_name, hung_port, &r);
	assert_int_ne(r.status, AEROSPIKE_OK);
	assert_that(r.elapsed_ms >= 600 - 50, "validation took %llu ms, not the connect timeout",
		(unsigned long long)r.elapsed_ms);
	assert_int_eq(resolver_lookups(hung_name), 1);
}

TEST(nodes_stagger_ip_literal_as_before,
	"U-17 ipLiteralConnectsAsBefore: an IP literal is not resolved and waits the full connect timeout")
{
	stagger_setup(env, 600, 250, port);
	DISCOVERY_SCOPED(fnode, live, fnode_stop);
	assert_true(serve(&live, FIRST, port));
	uint32_t lookups = resolver_dns_lookups();

	stagger_result r;
	validate(env.client->cluster, FIRST, port, &r);
	assert_int_eq(r.status, AEROSPIKE_OK);
	assert_true(validated_at(&r, FIRST));
	assert_false(r.alias);

	uint16_t hung_port = free_port();
	assert_int_ne(hung_port, 0);
	DISCOVERY_SCOPED(fnode, silent, fnode_stop);
	assert_hung(&silent, FIRST, hung_port);

	validate(env.client->cluster, FIRST, hung_port, &r);
	assert_int_ne(r.status, AEROSPIKE_OK);
	assert_that(r.elapsed_ms >= 600 - 50, "validation took %llu ms, not the connect timeout",
		(unsigned long long)r.elapsed_ms);
	assert_int_eq(resolver_dns_lookups(), lookups);
}

TEST(nodes_stagger_timeout_below_tend_interval,
	"U-17 timeoutBelowTendIntervalBoundsWait: a connect timeout below the tend interval bounds the wait")
{
	stagger_setup(env, 400, 1000, port);
	DISCOVERY_SCOPED(fnode, hung1, fnode_stop);
	DISCOVERY_SCOPED(fnode, hung2, fnode_stop);
	assert_hung(&hung1, FIRST, port);
	assert_hung(&hung2, SECOND, port);
	char hostname[64];
	stagger_hostname(hostname, "timeout", FIRST, SECOND);

	stagger_result r;
	validate(env.client->cluster, hostname, port, &r);

	assert_int_eq(r.status, AEROSPIKE_ERR_CONNECTION);
	assert_that(r.elapsed_ms >= 400 - 50 && r.elapsed_ms < 400 + STAGGER_MARGIN,
		"validation took %llu ms", (unsigned long long)r.elapsed_ms);
	assert_that(strstr(r.err.message, "400ms"), "%s", r.err.message);
}

TEST(nodes_stagger_wrong_node_tries_next,
	"U-17 wrongNodeTriesNextAddress: an address that is another node goes on to the next, which is then preferred")
{
	stagger_setup(env, 1000, 250, port);
	DISCOVERY_SCOPED(fnode, wrong, fnode_stop);
	DISCOVERY_SCOPED(fnode, right, fnode_stop);
	fnode_init(&wrong, STAGGER_OTHER);
	assert_true(fnode_listen_on(&wrong, FIRST, port));
	assert_true(serve(&right, SECOND, port));
	char hostname[64];
	stagger_hostname(hostname, "wrong", FIRST, SECOND);

	as_cluster* cluster = env.client->cluster;
	validate_opts opts = {.accept = accept_name, .udata = (void*)STAGGER_NODE};
	stagger_result r;
	validate_with(cluster, hostname, port, &opts, &r);
	assert_int_eq(r.status, AEROSPIKE_OK);
	assert_true(validated_at(&r, SECOND));
	assert_int_eq(fnode_get(&wrong, &wrong.accepts), 1);

	char node[AS_NODE_NAME_SIZE];
	assert_true(valid_node(cluster, hostname, node, sizeof(node)));
	assert_string_eq(node, STAGGER_NODE);

	for (int i = 0; i < 5; i++) {
		validate_with(cluster, hostname, port, &opts, &r);
		assert_that(validated_at(&r, SECOND), "call %d validated another address", i);
	}
	assert_int_eq(fnode_get(&wrong, &wrong.accepts), 1);

	// When no address is accepted, the error says so.
	opts.accept = reject_all;
	validate_with(cluster, hostname, port, &opts, &r);

	char expected[128];
	snprintf(expected, sizeof(expected), "No address of %s %u was accepted", hostname, port);
	assert_int_eq(r.status, AEROSPIKE_ERR_INVALID_NODE);
	assert_that(strstr(r.err.message, expected), "%s", r.err.message);
}

TEST(nodes_stagger_login_skips_hung_address,
	"U-17 loginSkipsHungAddress: with a user, a hung address neither delays nor fails the login at the next")
{
	stagger_setup_user(env, 3000, 250, "user", port);
	DISCOVERY_SCOPED(fnode, silent, fnode_stop);
	DISCOVERY_SCOPED(fnode, live, fnode_stop);
	assert_hung(&silent, SECOND, port);
	assert_true(serve(&live, FIRST, port));
	char hostname[64];
	stagger_hostname(hostname, "login", SECOND, FIRST);

	stagger_result r;
	validate(env.client->cluster, hostname, port, &r);
	assert_int_eq(r.status, AEROSPIKE_OK);
	assert_true(validated_at(&r, FIRST));
	assert_that(r.elapsed_ms < STAGGER_DELAY + STAGGER_MARGIN, "validation took %llu ms",
		(unsigned long long)r.elapsed_ms);
	assert_that(fnode_get(&live, &live.logins) >= 1, "no login reached the live address");

	// Hung addresses fail within the bound, not the login timeout.
	uint16_t dead_port = free_port();
	assert_int_ne(dead_port, 0);
	DISCOVERY_SCOPED(fnode, hung1, fnode_stop);
	DISCOVERY_SCOPED(fnode, hung2, fnode_stop);
	assert_hung(&hung1, FIRST, dead_port);
	assert_hung(&hung2, SECOND, dead_port);
	char dead[64];
	stagger_hostname(dead, "login-dead", FIRST, SECOND);

	validate(env.client->cluster, dead, dead_port, &r);
	assert_int_eq(r.status, AEROSPIKE_ERR_CONNECTION);
	assert_that(r.elapsed_ms < 250 + STAGGER_MARGIN, "validation took %llu ms",
		(unsigned long long)r.elapsed_ms);
}

// Wait up to two seconds for the descriptor count to return to expected, and return it.
static int
settle_fds(int expected)
{
	int value = discovery_fd_count();

	for (int i = 0; i < 200 && value != expected; i++) {
		as_sleep(10);
		value = discovery_fd_count();
	}
	return value;
}

TEST(nodes_stagger_no_descriptor_leak,
	"U-17 staggered validations that succeed, fail or reject every node leave no descriptor open")
{
	stagger_setup(env, 300, 250, port);
	DISCOVERY_SCOPED(fnode, silent, fnode_stop);
	DISCOVERY_SCOPED(fnode, live, fnode_stop);
	assert_hung(&silent, SECOND, port);
	assert_true(serve(&live, FIRST, port));
	char found[64];
	char dead[64];
	stagger_hostname(found, "fd-found", SECOND, FIRST);
	stagger_hostname(dead, "fd-dead", SECOND, THIRD);

	as_sleep(100);
	int before = discovery_fd_count();

	if (before < 0) {
		info("skipped: the descriptor count is unknown");
		return;
	}

	as_cluster* cluster = env.client->cluster;
	validate_opts reject = {.accept = reject_all};
	stagger_result r;

	for (int i = 0; i < 3; i++) {
		validate(cluster, found, port, &r);
		assert_int_eq(r.status, AEROSPIKE_OK);
		validate(cluster, dead, port, &r);
		assert_int_eq(r.status, AEROSPIKE_ERR_CONNECTION);
		validate_with(cluster, found, port, &reject, &r);
		assert_int_eq(r.status, AEROSPIKE_ERR_INVALID_NODE);
	}
	assert_int_eq(settle_fds(before), before);
}

// TLS context that verifies peers against the default locations. The fake nodes do not speak
// TLS, so every handshake fails.
static bool
tls_context(as_tls_context* ctx)
{
	as_config_tls config;
	memset(&config, 0, sizeof(config));
	config.enable = true;

	as_error err;

	if (as_tls_context_setup(&config, ctx, &err) != AEROSPIKE_OK) {
		info("skipped: TLS context setup failed: %s", err.message);
		as_tls_context_destroy(ctx);
		return false;
	}
	return true;
}

TEST(nodes_stagger_tls_failure_bounded,
	"U-17 tlsHandshakeWithinBound: a failed or silent TLS handshake goes on to the next address within the bound")
{
	stagger_setup(env, 400, 1000, port);
	as_tls_context tls;

	if (!tls_context(&tls)) {
		return;
	}

	DISCOVERY_SCOPED(fnode, plain, fnode_stop);
	DISCOVERY_SCOPED(fnode, silent, fnode_stop);
	assert_true(serve(&plain, FIRST, port));
	assert_true(serve(&silent, SECOND, port));
	fnode_set_mute(&silent, true);

	// The plain server closes on the client hello, then the next address never answers it.
	char first[64];
	stagger_hostname(first, "tls-plain", FIRST, SECOND);
	validate_opts opts = {.tls = &tls};
	stagger_result r1;
	validate_with(env.client->cluster, first, port, &opts, &r1);

	// The silent address holds the handshake until the bound, so the next is never started.
	char second[64];
	stagger_hostname(second, "tls-silent", SECOND, FIRST);
	uint32_t plain_accepts = fnode_get(&plain, &plain.accepts);
	stagger_result r2;
	validate_with(env.client->cluster, second, port, &opts, &r2);
	uint32_t plain_after = fnode_get(&plain, &plain.accepts);
	as_tls_context_destroy(&tls);

	char detail[96];
	snprintf(detail, sizeof(detail), "; first failure: %s %u TLS handshake failed", FIRST, port);
	assert_int_eq(r1.status, AEROSPIKE_ERR_CONNECTION);
	assert_that(strstr(r1.err.message, detail), "%s", r1.err.message);
	assert_that(r1.elapsed_ms >= 400 - 50 && r1.elapsed_ms < 400 + STAGGER_MARGIN,
		"validation took %llu ms", (unsigned long long)r1.elapsed_ms);

	snprintf(detail, sizeof(detail), "; first failure: %s %u TLS handshake failed", SECOND, port);
	assert_int_eq(r2.status, AEROSPIKE_ERR_CONNECTION);
	assert_that(strstr(r2.err.message, detail), "%s", r2.err.message);
	assert_that(r2.elapsed_ms >= 400 - 50 && r2.elapsed_ms < 400 + STAGGER_MARGIN,
		"validation took %llu ms", (unsigned long long)r2.elapsed_ms);
	assert_int_eq(plain_after, plain_accepts);
}

TEST(nodes_stagger_refused_names_first_failure,
	"U-17 every refused address fails at once, naming the first failed address")
{
	stagger_setup(env, 1000, 250, port);
	char hostname[64];
	stagger_hostname(hostname, "refused-all", FIRST, SECOND);

	stagger_result r;
	validate(env.client->cluster, hostname, port, &r);

	char detail[96];
	snprintf(detail, sizeof(detail), "; first failure: %s %u connect error", FIRST, port);
	assert_int_eq(r.status, AEROSPIKE_ERR_CONNECTION);
	assert_that(strstr(r.err.message, detail), "%s", r.err.message);
	assert_that(r.elapsed_ms < STAGGER_DELAY, "validation took %llu ms",
		(unsigned long long)r.elapsed_ms);
}

TEST(nodes_stagger_alternate_missing,
	"U-17 a node without alternate addresses reports alternate_missing through the staggered path")
{
	stagger_setup(env, 1000, 250, port);
	DISCOVERY_SCOPED(fnode, a, fnode_stop);
	DISCOVERY_SCOPED(fnode, b, fnode_stop);
	assert_true(serve(&a, FIRST, port));
	assert_true(serve(&b, SECOND, port));
	fnode_set_empty_alt(&a, true);
	fnode_set_empty_alt(&b, true);
	char hostname[64];
	stagger_hostname(hostname, "no-alt", FIRST, SECOND);

	validate_opts opts = {.alternate = true};
	stagger_result r;
	validate_with(env.client->cluster, hostname, port, &opts, &r);
	assert_int_eq(r.status, AEROSPIKE_ERR_INVALID_NODE);
	assert_true(r.alternate_missing);
	assert_that(strstr(r.err.message, "no alternate-access-address"), "%s", r.err.message);
}

//---------------------------------
// Tests: peers with several addresses (U-17)
//---------------------------------

// Seed S on 127.0.0.1, which owns every partition, with the resolver table reset.
static bool
peer_seed(fnode* s)
{
	resolver_install();

	if (!start_on(s, "S", "127.0.0.1")) {
		return false;
	}
	fnode_set_partitions(s, true);
	return true;
}

static bool
start_named(fnode* n, const char* name, const char* ip, uint16_t port)
{
	fnode_init(n, name);
	return fnode_listen_on(n, ip, port);
}

// Connect a client and close it, returning the status and error of the connect.
static as_status
connect_error(as_config* config, as_error* err)
{
	aerospike* client = aerospike_new(config);
	as_status status = aerospike_connect(client, err);
	nodes_close(client);
	return status;
}

#define PEER_MISMATCH "is different than actual node"
#define PEER_FAILED "Failed to connect to peer"

TEST(nodes_peer_wrong_node_tries_next,
	"U-17 a peer address that is another node goes on to the peer's next address")
{
	DISCOVERY_SCOPED(fnode, s, fnode_stop);
	DISCOVERY_SCOPED(fnode, y, fnode_stop);
	DISCOVERY_SCOPED(fnode, x, fnode_stop);
	assert_true(peer_seed(&s));
	uint16_t port = free_port();
	assert_int_ne(port, 0);
	assert_true(start_named(&y, "Y", SECOND, port));
	assert_true(start_named(&x, "X", THIRD, port));
	char hostname[64];
	stagger_hostname(hostname, "peer-next", SECOND, THIRD);
	set_peers(&s, "[[X,,[%s:%u]]]", hostname, port);

	as_config config;
	nodes_config(&config, &s);
	aerospike* client = NULL;
	log_count_start_many(PEER_MISMATCH, PEER_FAILED, NULL);
	as_status status = nodes_connect(&client, &config);
	uint32_t counts[LOG_COUNT_MAX];
	log_count_stop_many(counts);

	as_cluster* cluster = client->cluster;
	uint32_t size = status == AEROSPIKE_OK ? node_count(cluster) : 0;
	as_node* node = status == AEROSPIKE_OK ? get_node(cluster, "X") : NULL;
	bool at_x = node_at(node, &x);
	char valid[AS_NODE_NAME_SIZE] = {0};
	bool recorded = status == AEROSPIKE_OK && valid_node(cluster, hostname, valid, sizeof(valid));

	if (node) {
		as_node_release(node);
	}
	nodes_close(client);

	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(size, 2);
	assert_true(at_x);
	assert_true(recorded);
	assert_string_eq(valid, "X");
	assert_int_eq(counts[0], 1);
	assert_int_eq(counts[1], 0);
}

TEST(nodes_peer_every_address_wrong_node,
	"U-17 a peer whose every address is another node is not reachable, with one warning per address")
{
	DISCOVERY_SCOPED(fnode, s, fnode_stop);
	DISCOVERY_SCOPED(fnode, y, fnode_stop);
	DISCOVERY_SCOPED(fnode, w, fnode_stop);
	assert_true(peer_seed(&s));
	uint16_t port = free_port();
	assert_int_ne(port, 0);
	assert_true(start_named(&y, "Y", SECOND, port));
	assert_true(start_named(&w, "W", THIRD, port));
	char hostname[64];
	stagger_hostname(hostname, "peer-wrong", SECOND, THIRD);
	set_peers(&s, "[[X,,[%s:%u]]]", hostname, port);

	as_config config;
	nodes_config(&config, &s);
	as_error err;
	log_count_start_many(PEER_MISMATCH, PEER_FAILED, NULL);
	as_status status = connect_error(&config, &err);
	uint32_t counts[LOG_COUNT_MAX];
	log_count_stop_many(counts);

	assert_int_ne(status, AEROSPIKE_OK);
	assert_that(strstr(err.message, "Peers not reachable"), "%s", err.message);
	assert_that(strstr(err.message, hostname), "%s", err.message);
	assert_int_eq(counts[0], 2);
	assert_int_eq(counts[1], 0);
}

TEST(nodes_peer_wrong_node_then_alternate_missing,
	"U-17 a missing alternate address at a later address still fails init after a wrong node")
{
	DISCOVERY_SCOPED(fnode, s, fnode_stop);
	DISCOVERY_SCOPED(fnode, y, fnode_stop);
	DISCOVERY_SCOPED(fnode, x, fnode_stop);
	assert_true(peer_seed(&s));
	uint16_t port = free_port();
	assert_int_ne(port, 0);
	assert_true(start_named(&y, "Y", SECOND, port));
	assert_true(start_named(&x, "X", THIRD, port));
	fnode_set_empty_alt(&x, true);
	char hostname[64];
	stagger_hostname(hostname, "peer-alt", SECOND, THIRD);
	set_peers(&s, "[[X,,[%s:%u]]]", hostname, port);

	as_config config;
	nodes_config(&config, &s);
	config.use_services_alternate = true;
	as_error err;
	log_count_start_many(PEER_MISMATCH, PEER_FAILED, NULL);
	as_status status = connect_error(&config, &err);
	uint32_t counts[LOG_COUNT_MAX];
	log_count_stop_many(counts);

	assert_int_eq(status, AEROSPIKE_ERR_INVALID_NODE);
	assert_that(strstr(err.message, "Node X ") && strstr(err.message, "no alternate-access-address"),
		"%s", err.message);
	assert_int_eq(counts[0], 1);
	assert_int_eq(counts[1], 0);
}

TEST(nodes_peer_known_node_skipped,
	"U-17 FR-47 a peer address that answers as a cluster node is skipped for the peer's next address")
{
	DISCOVERY_SCOPED(fnode, s, fnode_stop);
	DISCOVERY_SCOPED(fnode, impostor, fnode_stop);
	DISCOVERY_SCOPED(fnode, x, fnode_stop);
	assert_true(peer_seed(&s));
	uint16_t port = free_port();
	assert_int_ne(port, 0);
	assert_true(start_named(&impostor, "S", FIRST, port));
	assert_true(start_named(&x, "X", SECOND, port));
	char hostname[64];
	stagger_hostname(hostname, "peer-known", FIRST, SECOND);
	set_peers(&s, "[[X,,[%s:%u]]]", hostname, port);

	char skip[160];
	snprintf(skip, sizeof(skip), "Peer X skips address %s:%u because it answers as node S",
		hostname, port);
	as_config config;
	nodes_config(&config, &s);
	aerospike* client = NULL;
	log_count_start_many(PEER_MISMATCH, skip, "has no address of its own", PEER_FAILED, NULL);
	as_status status = nodes_connect(&client, &config);
	uint32_t counts[LOG_COUNT_MAX];
	log_count_stop_many(counts);

	as_cluster* cluster = client->cluster;
	uint32_t size = status == AEROSPIKE_OK ? node_count(cluster) : 0;
	as_node* node = status == AEROSPIKE_OK ? get_node(cluster, "X") : NULL;
	bool at_x = node_at(node, &x);
	char valid[AS_NODE_NAME_SIZE] = {0};
	bool recorded = status == AEROSPIKE_OK && valid_node(cluster, hostname, valid, sizeof(valid));

	if (node) {
		as_node_release(node);
	}
	nodes_close(client);

	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(size, 2);
	assert_true(at_x);
	assert_true(recorded);
	assert_string_eq(valid, "X");
	assert_int_eq(counts[0], 0);
	assert_that(counts[1] >= 1, "the skip was not logged");
	assert_int_eq(counts[2], 0);
	assert_int_eq(counts[3], 0);
}

typedef struct known_host_result_s {
	as_status connect;
	as_status tend;
	bool x_created;
	uint32_t counts[LOG_COUNT_MAX];
} known_host_result;

// Peer X is advertised at one hostname that resolves to FIRST, where a server answers as the seed
// S, and then SECOND, where a server answers as second_name. One tend refreshes the peers of S
// and counts mismatches, skips of the hostname, -26 reports and connect failures.
static void
run_known_host(const char* label, const char* second_name, known_host_result* r)
{
	memset(r, 0, sizeof(*r));
	DISCOVERY_SCOPED(fnode, s, fnode_stop);
	DISCOVERY_SCOPED(fnode, first, fnode_stop);
	DISCOVERY_SCOPED(fnode, second, fnode_stop);
	r->connect = AEROSPIKE_ERR_CLIENT;

	if (!peer_seed(&s)) {
		return;
	}
	uint16_t port = free_port();

	if (port == 0 || !start_named(&first, "S", FIRST, port) ||
		!start_named(&second, second_name, SECOND, port)) {
		return;
	}
	char hostname[64];
	stagger_hostname(hostname, label, FIRST, SECOND);

	as_config config;
	nodes_config(&config, &s);
	aerospike* client = NULL;
	r->connect = nodes_connect(&client, &config);

	if (r->connect != AEROSPIKE_OK) {
		nodes_close(client);
		return;
	}
	as_cluster* cluster = client->cluster;

	char skip[160];
	snprintf(skip, sizeof(skip), "Peer X skips address %s:%u because it answers as node S",
		hostname, port);
	set_peers(&s, "[[X,,[%s:%u]]]", hostname, port);
	fnode_bump_peers_generation(&s);
	log_count_start_many(PEER_MISMATCH, skip, "has no address of its own", PEER_FAILED, NULL);
	r->tend = tend(cluster);
	log_count_stop_many(r->counts);

	as_node* node = get_node(cluster, "X");
	r->x_created = node != NULL;

	if (node) {
		as_node_release(node);
	}
	nodes_close(client);
}

TEST(nodes_peer_known_host_logged_once,
	"U-17 FR-47 a hostname whose addresses all answer as a known node logs one skip and one -26")
{
	known_host_result r;
	run_known_host("peer-known-twice", "S", &r);

	assert_int_eq(r.connect, AEROSPIKE_OK);
	assert_int_eq(r.tend, AEROSPIKE_OK);
	assert_false(r.x_created);
	assert_int_eq(r.counts[0], 0);
	assert_int_eq(r.counts[1], 1);
	assert_int_eq(r.counts[2], 1);
	assert_int_eq(r.counts[3], 0);
}

TEST(nodes_peer_known_and_unknown_fails,
	"U-17 FR-47 a hostname with an address that answers as a known node and one that answers as an unknown node fails without -26")
{
	known_host_result r;
	run_known_host("peer-known-unknown", "Y", &r);

	assert_int_eq(r.connect, AEROSPIKE_OK);
	assert_int_eq(r.tend, AEROSPIKE_OK);
	assert_false(r.x_created);
	assert_int_eq(r.counts[0], 1);
	assert_int_eq(r.counts[1], 1);
	assert_int_eq(r.counts[2], 0);
	assert_int_eq(r.counts[3], 0);
}

TEST(nodes_peer_socket_collision_stops,
	"U-17 a peer address where a cluster node answers under another name stops the peer's other addresses")
{
	DISCOVERY_SCOPED(fnode, s, fnode_stop);
	DISCOVERY_SCOPED(fnode, holder, fnode_stop);
	DISCOVERY_SCOPED(fnode, x, fnode_stop);
	assert_true(peer_seed(&s));
	uint16_t port = free_port();
	assert_int_ne(port, 0);
	assert_true(start_named(&holder, "B", SECOND, port));
	assert_true(start_named(&x, "X", THIRD, port));
	char hostname[64];
	stagger_hostname(hostname, "peer-socket", SECOND, THIRD);
	set_peers(&s, "[[B,,[%s:%u]]]", SECOND, port);

	as_config config;
	nodes_config(&config, &s);
	aerospike* client = NULL;
	assert_int_eq(nodes_connect(&client, &config), AEROSPIKE_OK);
	as_cluster* cluster = client->cluster;
	uint32_t before = node_count(cluster);

	// B keeps its tend connection, so the client still sees B there, while a new connection to
	// the same socket answers X.
	fnode_set_new_name(&holder, "X");
	log_count_start_many(PEER_MISMATCH, "answer at the same address", "has no address of its own",
		PEER_FAILED, NULL);
	set_peers(&s, "[[B,,[%s:%u]],[X,,[%s:%u]]]", SECOND, port, hostname, port);
	fnode_bump_peers_generation(&s);
	as_status status = tend(cluster);
	uint32_t counts[LOG_COUNT_MAX];
	log_count_stop_many(counts);

	uint32_t size = node_count(cluster);
	as_node* b = get_node(cluster, "B");
	as_node* node = get_node(cluster, "X");

	if (b) {
		as_node_release(b);
	}

	if (node) {
		as_node_release(node);
	}
	nodes_close(client);

	assert_int_eq(before, 2);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(size, 2);
	assert_not_null(b);
	assert_null(node);
	assert_int_eq(fnode_get(&x, &x.accepts), 0);
	assert_int_eq(counts[0], 0);
	assert_that(counts[1] >= 1, "no collision was reported");
	assert_int_eq(counts[2], 0);
	assert_int_eq(counts[3], 0);
}

TEST(nodes_peer_hung_address_joins_in_one_tend,
	"U-17 W-3 shape: a peer whose first address hangs joins at the next address within one tend")
{
	DISCOVERY_SCOPED(fnode, s, fnode_stop);
	DISCOVERY_SCOPED(fnode, silent, fnode_stop);
	DISCOVERY_SCOPED(fnode, x, fnode_stop);
	assert_true(peer_seed(&s));
	uint16_t port = free_port();
	assert_int_ne(port, 0);
	assert_hung(&silent, SECOND, port);
	assert_true(start_named(&x, "X", THIRD, port));
	char hostname[64];
	stagger_hostname(hostname, "peer-hung", SECOND, THIRD);

	as_config config;
	nodes_config(&config, &s);
	aerospike* client = NULL;
	assert_int_eq(nodes_connect(&client, &config), AEROSPIKE_OK);
	as_cluster* cluster = client->cluster;
	uint32_t before = node_count(cluster);

	set_peers(&s, "[[X,,[%s:%u]]]", hostname, port);
	fnode_bump_peers_generation(&s);
	uint64_t begin = cf_getms();
	as_status status = tend(cluster);
	uint64_t elapsed = cf_getms() - begin;

	as_node* node = get_node(cluster, "X");
	bool at_x = node_at(node, &x);
	uint32_t size = node_count(cluster);

	if (node) {
		as_node_release(node);
	}
	nodes_close(client);

	assert_int_eq(before, 1);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(size, 2);
	assert_true(at_x);
	assert_that(elapsed < STAGGER_DELAY + STAGGER_MARGIN, "the tend took %llu ms",
		(unsigned long long)elapsed);
}

TEST(nodes_valid_addresses_pruned,
	"U-17 the address accepted for a host is dropped with its node and kept when the node is replaced")
{
	DISCOVERY_SCOPED(fnode, s, fnode_stop);
	DISCOVERY_SCOPED(fnode, x1, fnode_stop);
	DISCOVERY_SCOPED(fnode, x2, fnode_stop);
	DISCOVERY_SCOPED(fnode, c, fnode_stop);
	DISCOVERY_SCOPED(fnode, b, fnode_stop);
	char local[32];

	if (!local_address(local, sizeof(local))) {
		info("skipped: no non-loopback IPv4 address");
		return;
	}

	// X is on a non-loopback address, so the cluster may replace it.
	assert_true(peer_seed(&s));
	assert_true(start_on(&x1, "X", local));
	assert_true(start_on(&x2, "X", local));
	assert_true(start_on(&c, "C", FIRST));
	assert_true(start_on(&b, "B", FIRST));
	char hx[64];
	char hc[64];
	char hb[64];
	stagger_hostname(hx, "x", local, SECOND);
	stagger_hostname(hc, "c", FIRST, SECOND);
	stagger_hostname(hb, "b", FIRST);
	set_peers(&s, "[[X,,[%s:%u]],[C,,[%s:%u]],[B,,[%s:%u]]]", hx, x1.port, hc, c.port, hb,
		b.port);

	as_config config;
	nodes_config(&config, &s);
	aerospike* client = NULL;
	assert_int_eq(nodes_connect(&client, &config), AEROSPIKE_OK);
	as_cluster* cluster = client->cluster;
	uint32_t joined = node_count(cluster);
	uint32_t initial = valid_count(cluster);

	// C leaves the peers list and has no partitions, so the tend removes it.
	set_peers(&s, "[[X,,[%s:%u]],[B,,[%s:%u]]]", hx, x1.port, hb, b.port);
	fnode_bump_peers_generation(&s);
	as_status removed_status = tend(cluster);
	uint32_t after_remove = node_count(cluster);
	char node[AS_NODE_NAME_SIZE] = {0};
	bool c_kept = valid_node(cluster, hc, node, sizeof(node));
	uint32_t remaining = valid_count(cluster);

	// X fails and S lists X by IP at X2, so the tend replaces X under the same name.
	set_peers(&s, "[[X,,[%s:%u]],[B,,[%s:%u]]]", local, x2.port, hb, b.port);
	fnode_bump_peers_generation(&s);
	fail_node(&x1);
	as_status replaced_status = tend(cluster);
	as_node* x = get_node(cluster, "X");
	bool at_x2 = node_at(x, &x2);

	if (x) {
		as_node_release(x);
	}
	bool x_kept = valid_node(cluster, hx, node, sizeof(node));
	uint32_t after_replace = valid_count(cluster);
	nodes_close(client);

	assert_int_eq(joined, 4);
	assert_int_eq(initial, 2);
	assert_int_eq(removed_status, AEROSPIKE_OK);
	assert_int_eq(after_remove, 3);
	assert_false(c_kept);
	assert_int_eq(remaining, 1);
	assert_int_eq(replaced_status, AEROSPIKE_OK);
	assert_true(at_x2);
	assert_true(x_kept);
	assert_string_eq(node, "X");
	assert_int_eq(after_replace, 1);
}

//---------------------------------
// Static Functions: hostname address move
//---------------------------------

// name_watch counts the nodes named X.
#define MOVE_NODE "X"
#define MOVE_OTHER "B"

typedef struct {
	aerospike* client;
} move_env;

static void
move_close(move_env* env)
{
	if (env->client) {
		nodes_close(env->client);
		env->client = NULL;
	}
	resolver_remove();
}

// Connect to host and port, logging in as user when not NULL. Every tend after the first is run
// by the test, and the tend interval is never reached, so a move is bounded by timeout_ms.
static bool
move_connect_user(move_env* env, const char* host, uint16_t port, uint32_t timeout_ms,
	const char* user, uint32_t login_timeout_ms)
{
	as_config config;
	as_config_init(&config);
	as_config_add_host(&config, host, port);
	config.tender_interval = NODES_TEND_INTERVAL;
	config.conn_timeout_ms = timeout_ms;
	config.fail_if_not_connected = true;

	if (user) {
		as_config_set_user(&config, user, "password");
		config.login_timeout_ms = login_timeout_ms;
	}
	return nodes_connect(&env->client, &config) == AEROSPIKE_OK;
}

static bool
move_connect(move_env* env, const char* host, uint16_t port, uint32_t timeout_ms)
{
	return move_connect_user(env, host, port, timeout_ms, NULL, 0);
}

#define move_setup_env(env) \
	DISCOVERY_SCOPED(move_env, env, move_close); \
	resolver_install()

#define move_setup(env, port) \
	move_setup_env(env); \
	uint16_t port = free_port(); \
	assert_int_ne(port, 0)

// Servers named first_name on FIRST and second_name on SECOND, both on port.
static bool
move_pair(fnode* a, fnode* b, const char* first_name, const char* second_name, uint16_t port)
{
	return start_named(a, first_name, FIRST, port) && start_named(b, second_name, SECOND, port);
}

// Servers named name on ip1 and ip2, on one port that is free on both.
static bool
move_pair_on(fnode* a, fnode* b, const char* name, const char* ip1, const char* ip2)
{
	for (int i = 0; i < 20; i++) {
		if (start_on(a, name, ip1) && start_named(b, name, ip2, a->port)) {
			return true;
		}
		fnode_stop(a);
		fnode_stop(b);
	}
	return false;
}

static void
fnode_stop_three(fnode (*list)[3])
{
	for (int i = 0; i < 3; i++) {
		fnode_stop(&(*list)[i]);
	}
}

// Is node the node of its name in the cluster.
static bool
holds_name(as_cluster* cluster, as_node* node)
{
	as_node* current = get_node(cluster, node->name);

	if (current) {
		as_node_release(current);
	}
	return current == node;
}

// Is node the master of every partition of namespace test.
static bool
owns_partitions(as_cluster* cluster, as_node* node)
{
	pthread_mutex_lock(&cluster->tend_lock);
	as_partition_table* table = as_partition_tables_get(&cluster->partition_tables, "test");
	bool owns = table && table->size > 0;

	for (uint32_t i = 0; owns && i < table->size; i++) {
		owns = as_node_load(&table->partitions[i].nodes[0]) == node;
	}
	pthread_mutex_unlock(&cluster->tend_lock);
	return owns;
}

// Copy the IPv4 address last accepted for host name and port to ip, or return false.
static bool
valid_ip(as_cluster* cluster, const char* name, uint16_t port, char* ip, size_t size)
{
	bool found = false;
	pthread_mutex_lock(&cluster->tend_lock);
	as_vector* list = cluster->valid_addresses;

	for (uint32_t i = 0; list && i < list->size && !found; i++) {
		as_lookup_valid_address* va = as_vector_get(list, i);

		if (va->port == port && strcmp(va->name, name) == 0) {
			struct sockaddr_in* sin = (struct sockaddr_in*)&va->addr;
			found = inet_ntop(AF_INET, &sin->sin_addr, ip, (socklen_t)size) != NULL;
		}
	}
	pthread_mutex_unlock(&cluster->tend_lock);
	return found;
}

static uint64_t
timed_tend(as_cluster* cluster, as_status* status)
{
	uint64_t begin = cf_getms();
	*status = tend(cluster);
	return cf_getms() - begin;
}

// Run count tends. Return false when a tend fails or replaces node.
static bool
tends_keep(as_cluster* cluster, as_node* node, int count)
{
	bool kept = true;

	for (int i = 0; i < count; i++) {
		kept = tend(cluster) == AEROSPIKE_OK && holds_name(cluster, node) && kept;
	}
	return kept;
}

// The log message of a move of node MOVE_NODE of hostname from server from to server to.
static void
move_message(char* buf, size_t size, const char* hostname, const fnode* from, const fnode* to)
{
	char a[64];
	char b[64];
	snprintf(buf, size, "Move node %s %s from %s to %s", MOVE_NODE, hostname,
		fnode_addr(from, a, sizeof(a)), fnode_addr(to, b, sizeof(b)));
}

//---------------------------------
// Tests: hostname address move (U-16)
//---------------------------------

TEST(nodes_move_not_before_third_failed_tend,
	"U-16 noMoveBeforeThirdFailedTend: a failing node keeps its address, with no lookup, until its third failed tend")
{
	move_setup(env, port);
	DISCOVERY_SCOPED(fnode, a1, fnode_stop);
	DISCOVERY_SCOPED(fnode, a2, fnode_stop);
	assert_true(move_pair(&a1, &a2, MOVE_NODE, MOVE_NODE, port));
	char hostname[64];
	stagger_hostname(hostname, "third", FIRST, SECOND);
	assert_true(move_connect(&env, hostname, port, 1000));

	as_cluster* cluster = env.client->cluster;
	as_node* node = get_node(cluster, MOVE_NODE);
	assert_true(node_at(node, &a1));
	uint32_t lookups = resolver_lookups(hostname);

	fail_node(&a1);
	log_count_start("Move node");
	bool kept = tends_keep(cluster, node, AS_NODE_MOVE_FAILED_TENDS - 1);
	uint32_t moves_before = log_count_stop();
	uint32_t lookups_before = resolver_lookups(hostname);

	log_count_start("Move node");
	as_status status = tend(cluster);
	uint32_t moves = log_count_stop();

	as_node* moved = get_node(cluster, MOVE_NODE);
	bool at_a2 = node_at(moved, &a2);
	bool old_active = as_node_is_active(node);

	if (moved) {
		as_node_release(moved);
	}
	as_node_release(node);

	assert_true(kept);
	assert_int_eq(moves_before, 0);
	assert_int_eq(lookups_before, lookups);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_true(at_a2);
	assert_false(old_active);
	assert_int_eq(moves, 1);
}

TEST(nodes_move_failing_node_moves,
	"U-16 failingNodeMovesToOtherAddress: a failing node moves to the other address of its hostname in one swap")
{
	move_setup(env, port);
	DISCOVERY_SCOPED(fnode, a1, fnode_stop);
	DISCOVERY_SCOPED(fnode, a2, fnode_stop);
	assert_true(move_pair(&a1, &a2, MOVE_NODE, MOVE_NODE, port));
	fnode_set_partitions(&a1, true);
	fnode_set_partitions(&a2, true);
	char hostname[64];
	stagger_hostname(hostname, "move", FIRST, SECOND);
	assert_true(move_connect(&env, hostname, port, 1000));

	as_cluster* cluster = env.client->cluster;
	as_node* node = get_node(cluster, MOVE_NODE);
	assert_true(node_at(node, &a1));
	assert_true(owns_partitions(cluster, node));

	fail_node(&a1);
	assert_true(tends_keep(cluster, node, AS_NODE_MOVE_FAILED_TENDS - 1));

	// The node array never misses the name or holds it twice while the node moves.
	name_watch w = {.cluster = cluster};
	assert_int_eq(pthread_create(&w.thread, NULL, name_watch_run, &w), 0);

	char expected[256];
	move_message(expected, sizeof(expected), hostname, &a1, &a2);
	log_count_start(expected);
	as_status status = tend(cluster);
	uint32_t moves = log_count_stop();

	as_store_uint8(&w.stop, 1);
	pthread_join(w.thread, NULL);

	as_node* moved = get_node(cluster, MOVE_NODE);
	bool at_a2 = node_at(moved, &a2);
	bool same_hostname = moved && moved->hostname && strcmp(moved->hostname, hostname) == 0;
	bool old_active = as_node_is_active(node);
	uint32_t size = node_count(cluster);
	char ip[INET_ADDRSTRLEN] = {0};
	bool preferred = valid_ip(cluster, hostname, port, ip, sizeof(ip));
	bool owns_now = moved && owns_partitions(cluster, moved);
	as_status next_status = tend(cluster);
	bool owns = moved && owns_partitions(cluster, moved);

	if (moved) {
		as_node_release(moved);
	}
	as_node_release(node);

	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(moves, 1);
	assert_true(at_a2);
	assert_true(same_hostname);
	assert_false(old_active);
	assert_int_eq(size, 1);
	assert_one_node_per_name(cluster);
	assert_that(w.missing == 0, "X was missing in %u of %u samples", w.missing, w.samples);
	assert_that(w.duplicates == 0, "X was duplicated in %u of %u samples", w.duplicates,
		w.samples);
	assert_true(preferred);
	assert_string_eq(ip, SECOND);
	assert_that(owns_now, "the moved node did not own the partitions in the tend that moved it");
	assert_int_eq(next_status, AEROSPIKE_OK);
	assert_true(owns);
}

TEST(nodes_move_recovered_node_counts_again,
	"U-16 nodeRecoveringOnSecondTendDoesNotMove: a node that recovers starts counting its failed tends again")
{
	move_setup(env, port);
	DISCOVERY_SCOPED(fnode, a1, fnode_stop);
	DISCOVERY_SCOPED(fnode, a2, fnode_stop);
	assert_true(move_pair(&a1, &a2, MOVE_NODE, MOVE_NODE, port));
	char hostname[64];
	stagger_hostname(hostname, "recover", FIRST, SECOND);
	assert_true(move_connect(&env, hostname, port, 1000));

	as_cluster* cluster = env.client->cluster;
	as_node* node = get_node(cluster, MOVE_NODE);
	assert_true(node_at(node, &a1));

	log_count_start("Move node");
	fail_node(&a1);
	bool failed_kept = tends_keep(cluster, node, 1);
	fnode_set_reject(&a1, false);
	bool recovered_kept = tends_keep(cluster, node, 1);
	uint32_t failures = node->failures;
	fail_node(&a1);
	bool kept = tends_keep(cluster, node, AS_NODE_MOVE_FAILED_TENDS - 1);
	uint32_t moves_before = log_count_stop();

	log_count_start("Move node");
	as_status status = tend(cluster);
	uint32_t moves = log_count_stop();

	as_node* moved = get_node(cluster, MOVE_NODE);
	bool at_a2 = node_at(moved, &a2);

	if (moved) {
		as_node_release(moved);
	}
	as_node_release(node);

	assert_true(failed_kept);
	assert_true(recovered_kept);
	assert_int_eq(failures, 0);
	assert_true(kept);
	assert_int_eq(moves_before, 0);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_true(at_a2);
	assert_int_eq(moves, 1);
}

TEST(nodes_move_every_node_on_dead_address,
	"U-16 everyNodeOnDeadAddressMoves: every node of a hostname whose first address dies moves")
{
	move_setup_env(env);
	const char* names[3] = {MOVE_NODE, MOVE_OTHER, "C"};
	fnode dead[3] __attribute__((cleanup(fnode_stop_three))) = {{0}};
	fnode live[3] __attribute__((cleanup(fnode_stop_three))) = {{0}};
	uint16_t ports[3];
	char hostname[64];
	stagger_hostname(hostname, "zone", FIRST, SECOND);

	for (int i = 0; i < 3; i++) {
		ports[i] = free_port();
		assert_int_ne(ports[i], 0);
		assert_true(move_pair(&dead[i], &live[i], names[i], names[i], ports[i]));
	}

	// Every node lists the others at the hostname, each on its own port.
	for (int i = 0; i < 3; i++) {
		int j = (i + 1) % 3;
		int k = (i + 2) % 3;
		char peers[240];
		snprintf(peers, sizeof(peers), "1,3000,[[%s,,[%s:%u]],[%s,,[%s:%u]]]", names[j],
			hostname, ports[j], names[k], hostname, ports[k]);
		fnode_set_peers(&dead[i], peers);
		fnode_set_peers(&live[i], peers);
	}
	assert_true(move_connect(&env, hostname, ports[0], 1000));

	as_cluster* cluster = env.client->cluster;
	assert_int_eq(node_count(cluster), 3);

	for (int i = 0; i < 3; i++) {
		as_node* node = get_node(cluster, names[i]);
		bool at_first = node_at(node, &dead[i]);

		if (node) {
			as_node_release(node);
		}
		assert_that(at_first, "node %s is not on its first address", names[i]);
	}

	for (int i = 0; i < 3; i++) {
		fnode_set_reject(&dead[i], true);
	}
	as_sleep(100);

	for (int i = 0; i < AS_NODE_MOVE_FAILED_TENDS - 1; i++) {
		assert_int_eq(tend(cluster), AEROSPIKE_OK);
	}

	log_count_start("Move node");
	as_status status = tend(cluster);
	uint32_t moves = log_count_stop();

	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(moves, 3);
	assert_int_eq(node_count(cluster), 3);
	assert_one_node_per_name(cluster);

	for (int i = 0; i < 3; i++) {
		as_node* node = get_node(cluster, names[i]);
		bool at_second = node_at(node, &live[i]);

		if (node) {
			as_node_release(node);
		}
		assert_that(at_second, "node %s did not move to its second address", names[i]);
	}
}

TEST(nodes_move_one_candidate_per_tend,
	"U-16 oneCandidatePerTend: a hung candidate costs one tend within the bound, and the next tend moves")
{
	move_setup(env, port);
	DISCOVERY_SCOPED(fnode, dead, fnode_stop);
	DISCOVERY_SCOPED(fnode, live, fnode_stop);
	DISCOVERY_SCOPED(fnode, silent, fnode_stop);
	assert_true(move_pair(&dead, &live, MOVE_NODE, MOVE_NODE, port));
	fnode_set_partitions(&dead, true);
	fnode_set_partitions(&live, true);
	assert_hung(&silent, THIRD, port);
	char hostname[64];
	stagger_hostname(hostname, "one", FIRST, THIRD, SECOND);
	assert_true(move_connect(&env, hostname, port, 250));

	as_cluster* cluster = env.client->cluster;
	as_node* node = get_node(cluster, MOVE_NODE);
	assert_true(node_at(node, &dead));
	assert_true(owns_partitions(cluster, node));

	fail_node(&dead);
	assert_true(tends_keep(cluster, node, AS_NODE_MOVE_FAILED_TENDS - 1));

	char expected[256];
	move_message(expected, sizeof(expected), hostname, &dead, &live);
	log_count_start(expected);
	uint32_t connections = fnode_get(&live, &live.accepts);
	as_status hung_status;
	uint64_t elapsed = timed_tend(cluster, &hung_status);
	bool kept = holds_name(cluster, node);
	uint32_t live_after = fnode_get(&live, &live.accepts);

	as_status status = tend(cluster);
	uint32_t moves = log_count_stop();

	as_node* moved = get_node(cluster, MOVE_NODE);
	bool at_live = node_at(moved, &live);
	bool old_active = as_node_is_active(node);
	bool owns_now = moved && owns_partitions(cluster, moved);
	as_status next_status = tend(cluster);
	bool owns = moved && owns_partitions(cluster, moved);

	if (moved) {
		as_node_release(moved);
	}
	as_node_release(node);

	assert_int_eq(hung_status, AEROSPIKE_OK);
	assert_true(kept);
	assert_that(live_after == connections,
		"the live address was tried in the tend that tried the hung address");
	assert_that(elapsed >= 250 - 10 && elapsed < 250 + STAGGER_MARGIN, "the tend took %llu ms",
		(unsigned long long)elapsed);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_true(at_live);
	assert_false(old_active);
	assert_that(owns_now, "the moved node did not own the partitions in the tend that moved it");
	assert_int_eq(next_status, AEROSPIKE_OK);
	assert_true(owns);
	assert_int_eq(moves, 1);
}

// The harness never reaches the tend interval, so only the conn_timeout_ms side of the bound,
// min(conn_timeout_ms, tend_interval / 2), is tested.
TEST(nodes_move_silent_candidate_within_bound,
	"U-16 silentCandidateEndsWithinBound: a candidate that accepts and never answers ends within the bound")
{
	move_setup(env, port);
	DISCOVERY_SCOPED(fnode, dead, fnode_stop);
	DISCOVERY_SCOPED(fnode, silent, fnode_stop);
	assert_true(start_named(&dead, MOVE_NODE, FIRST, port));
	assert_true(serve(&silent, SECOND, port));
	fnode_set_mute(&silent, true);
	char hostname[64];
	stagger_hostname(hostname, "silent", FIRST, SECOND);
	assert_true(move_connect(&env, hostname, port, 200));

	as_cluster* cluster = env.client->cluster;
	as_node* node = get_node(cluster, MOVE_NODE);
	assert_true(node_at(node, &dead));

	fail_node(&dead);
	assert_true(tends_keep(cluster, node, AS_NODE_MOVE_FAILED_TENDS - 1));

	log_count_start("Move node");
	as_status status;
	uint64_t elapsed = timed_tend(cluster, &status);
	uint32_t moves = log_count_stop();
	bool kept = holds_name(cluster, node);
	as_node_release(node);

	assert_int_eq(status, AEROSPIKE_OK);
	assert_that(elapsed >= 200 - 10 && elapsed < 200 + STAGGER_MARGIN, "the tend took %llu ms",
		(unsigned long long)elapsed);
	assert_true(kept);
	assert_int_eq(moves, 0);
}

TEST(nodes_move_other_name_skipped,
	"U-16 candidateWithOtherNameIsSkipped: a candidate that is another node is skipped and never joins")
{
	move_setup(env, port);
	DISCOVERY_SCOPED(fnode, dead, fnode_stop);
	DISCOVERY_SCOPED(fnode, other, fnode_stop);
	assert_true(move_pair(&dead, &other, MOVE_NODE, MOVE_OTHER, port));
	char hostname[64];
	stagger_hostname(hostname, "other", FIRST, SECOND);
	assert_true(move_connect(&env, hostname, port, 1000));

	as_cluster* cluster = env.client->cluster;
	as_node* node = get_node(cluster, MOVE_NODE);
	assert_true(node_at(node, &dead));

	fail_node(&dead);
	log_count_start("Move node");
	bool kept[4];
	bool tried[4];

	for (int i = 0; i < 4; i++) {
		uint32_t connections = fnode_get(&other, &other.accepts);
		kept[i] = tends_keep(cluster, node, 1);
		tried[i] = fnode_get(&other, &other.accepts) > connections;
	}
	uint32_t moves = log_count_stop();
	as_node* b = get_node(cluster, MOVE_OTHER);

	if (b) {
		as_node_release(b);
	}
	as_node_release(node);

	for (int i = 0; i < 4; i++) {
		assert_that(kept[i], "tend %d replaced the node", i + 1);
		assert_that(tried[i] == (i + 1 >= AS_NODE_MOVE_FAILED_TENDS),
			"tend %d %s the other address", i + 1, tried[i] ? "tried" : "did not try");
	}
	assert_null(b);
	assert_int_eq(moves, 0);
}

// C refreshes every node before the move, so the holder of the candidate address must stay
// healthy on its tend socket while a new connection to that address answers the moving node's
// name. Java's test injects a holder that is never refreshed instead.
TEST(nodes_move_collision_skipped,
	"U-16 candidateRejectedByCollisionCheckIsSkipped: a candidate held by another node is skipped with a warning")
{
	move_setup(env, port);
	DISCOVERY_SCOPED(fnode, dead, fnode_stop);
	DISCOVERY_SCOPED(fnode, holder, fnode_stop);
	assert_true(move_pair(&dead, &holder, MOVE_NODE, MOVE_OTHER, port));
	char hostname[64];
	stagger_hostname(hostname, "collide", FIRST, SECOND);

	// B joins from X's peers as an IP literal. B owns every partition, so it is kept while the
	// failing X does not list it.
	set_peers(&dead, "[[%s,,[%s:%u]]]", MOVE_OTHER, SECOND, port);
	set_peers(&holder, "[[%s,,[%s:%u]]]", MOVE_NODE, hostname, port);
	fnode_set_partitions(&holder, true);
	assert_true(move_connect(&env, hostname, port, 1000));

	as_cluster* cluster = env.client->cluster;
	as_node* node = get_node(cluster, MOVE_NODE);
	as_node* b = get_node(cluster, MOVE_OTHER);
	assert_true(node_at(node, &dead));
	assert_true(node_at(b, &holder));

	fnode_set_new_name(&holder, MOVE_NODE);
	fail_node(&dead);

	char warning[256];
	snprintf(warning, sizeof(warning),
		"Address %s:%u of %s is also node %s, so node %s does not move to it", SECOND, port,
		hostname, MOVE_OTHER, MOVE_NODE);
	log_count_start_many("Move node", warning, "(address translator ", NULL);
	bool kept[4];
	bool b_kept[4];

	for (int i = 0; i < 4; i++) {
		kept[i] = tends_keep(cluster, node, 1);
		b_kept[i] = holds_name(cluster, b) && as_node_is_active(b);
	}
	uint32_t counts[LOG_COUNT_MAX];
	log_count_stop_many(counts);
	as_node_release(b);
	as_node_release(node);

	for (int i = 0; i < 4; i++) {
		assert_that(kept[i], "tend %d replaced node X", i + 1);
		assert_that(b_kept[i], "tend %d replaced node B", i + 1);
	}
	assert_int_eq(counts[0], 0);
	assert_that(counts[1] >= 1, "no collision warning names X, B and %s:%u", SECOND, port);
	assert_that(counts[2] == 0, "a move collision was reported as a translation collision");
}

TEST(nodes_move_ip_literal_no_move,
	"U-16 ipLiteralNodeDoesNotMove: a node seeded by an IP literal never moves and is never looked up")
{
	move_setup(env, port);
	DISCOVERY_SCOPED(fnode, dead, fnode_stop);
	DISCOVERY_SCOPED(fnode, other, fnode_stop);
	assert_true(move_pair(&dead, &other, MOVE_NODE, MOVE_NODE, port));
	assert_true(move_connect(&env, FIRST, port, 1000));

	as_cluster* cluster = env.client->cluster;
	as_node* node = get_node(cluster, MOVE_NODE);
	assert_true(node_at(node, &dead));
	uint32_t lookups = resolver_dns_lookups();

	fail_node(&dead);
	log_count_start("Move node");
	bool kept = tends_keep(cluster, node, 4);
	uint32_t moves = log_count_stop();
	as_node_release(node);

	assert_true(kept);
	assert_int_eq(resolver_dns_lookups(), lookups);
	assert_int_eq(fnode_get(&other, &other.accepts), 0);
	assert_int_eq(moves, 0);
}

TEST(nodes_move_single_address_no_move,
	"U-16 singleAddressNodeDoesNotMove: a node of a hostname with one address never moves")
{
	move_setup(env, port);
	DISCOVERY_SCOPED(fnode, dead, fnode_stop);
	DISCOVERY_SCOPED(fnode, other, fnode_stop);
	assert_true(move_pair(&dead, &other, MOVE_NODE, MOVE_NODE, port));
	char hostname[64];
	stagger_hostname(hostname, "single", FIRST);
	assert_true(move_connect(&env, hostname, port, 1000));

	as_cluster* cluster = env.client->cluster;
	as_node* node = get_node(cluster, MOVE_NODE);
	assert_true(node_at(node, &dead));

	fail_node(&dead);
	log_count_start("Move node");
	bool kept = tends_keep(cluster, node, 4);
	uint32_t moves = log_count_stop();
	as_node_release(node);

	assert_true(kept);
	assert_int_eq(fnode_get(&other, &other.accepts), 0);
	assert_int_eq(moves, 0);
}

TEST(nodes_move_no_lookup_without_failures,
	"U-16 noLookupForNodeWithoutFailures: a healthy node of a hostname is not looked up while another node moves")
{
	move_setup(env, port_x);
	DISCOVERY_SCOPED(fnode, x1, fnode_stop);
	DISCOVERY_SCOPED(fnode, x2, fnode_stop);
	DISCOVERY_SCOPED(fnode, b1, fnode_stop);
	assert_true(move_pair(&x1, &x2, MOVE_NODE, MOVE_NODE, port_x));
	uint16_t port_b = free_port();
	assert_int_ne(port_b, 0);
	assert_true(start_named(&b1, MOVE_OTHER, FIRST, port_b));
	char hx[64];
	char hb[64];
	stagger_hostname(hx, "x", FIRST, SECOND);
	stagger_hostname(hb, "b", FIRST, SECOND);

	// B owns every partition, so it is kept while the failing X does not list it.
	set_peers(&x1, "[[%s,,[%s:%u]]]", MOVE_OTHER, hb, port_b);
	set_peers(&x2, "[[%s,,[%s:%u]]]", MOVE_OTHER, hb, port_b);
	set_peers(&b1, "[[%s,,[%s:%u]]]", MOVE_NODE, hx, port_x);
	fnode_set_partitions(&b1, true);
	assert_true(move_connect(&env, hx, port_x, 1000));

	as_cluster* cluster = env.client->cluster;
	assert_int_eq(node_count(cluster), 2);
	as_node* b = get_node(cluster, MOVE_OTHER);
	assert_true(node_at(b, &b1));
	uint32_t lookups = resolver_lookups(hb);

	fail_node(&x1);
	log_count_start("Move node");
	bool ok = true;

	for (int i = 0; i < AS_NODE_MOVE_FAILED_TENDS + 2; i++) {
		ok = tend(cluster) == AEROSPIKE_OK && ok;
	}
	uint32_t moves = log_count_stop();

	as_node* x = get_node(cluster, MOVE_NODE);
	bool at_x2 = node_at(x, &x2);
	bool b_kept = holds_name(cluster, b) && as_node_is_active(b);

	if (x) {
		as_node_release(x);
	}
	as_node_release(b);

	assert_true(ok);
	assert_true(at_x2);
	assert_true(b_kept);
	assert_int_eq(resolver_lookups(hb), lookups);
	assert_int_eq(moves, 1);
}

// On loopback the cluster never replaces a node through peers, so this test would pass without
// the hostname rule. The node is on a non-loopback address instead.
TEST(nodes_move_peer_path_not_sooner,
	"U-16 peerPathDoesNotMoveSooner: a peer that lists a failing node by its hostname neither moves nor replaces it")
{
	char local[32];

	if (!local_address(local, sizeof(local))) {
		info("skipped: no non-loopback IPv4 address");
		return;
	}

	move_setup_env(env);
	DISCOVERY_SCOPED(fnode, a1, fnode_stop);
	DISCOVERY_SCOPED(fnode, a2, fnode_stop);
	DISCOVERY_SCOPED(fnode, b, fnode_stop);
	assert_true(move_pair_on(&a1, &a2, MOVE_NODE, local, SECOND));
	assert_true(start_on(&b, MOVE_OTHER, FIRST));
	char hostname[64];
	stagger_hostname(hostname, "peer", local, SECOND);

	// B owns every partition, so it is kept while the failing X does not list it.
	set_peers(&a1, "[[%s,,[%s:%u]]]", MOVE_OTHER, FIRST, b.port);
	set_peers(&a2, "[[%s,,[%s:%u]]]", MOVE_OTHER, FIRST, b.port);
	set_peers(&b, "[[%s,,[%s:%u]]]", MOVE_NODE, hostname, a1.port);
	fnode_set_partitions(&b, true);
	assert_true(move_connect(&env, hostname, a1.port, 1000));

	as_cluster* cluster = env.client->cluster;
	assert_int_eq(node_count(cluster), 2);
	as_node* node = get_node(cluster, MOVE_NODE);
	assert_true(node_at(node, &a1));

	fail_node(&a1);
	log_count_start_many("Move node", "Replace node", NULL);
	bool kept[AS_NODE_MOVE_FAILED_TENDS - 1];
	bool untried[AS_NODE_MOVE_FAILED_TENDS - 1];

	for (int i = 0; i < AS_NODE_MOVE_FAILED_TENDS - 1; i++) {
		fnode_bump_peers_generation(&b);
		uint32_t connections = fnode_get(&a2, &a2.accepts);
		kept[i] = tends_keep(cluster, node, 1);
		untried[i] = fnode_get(&a2, &a2.accepts) == connections;
	}

	fnode_bump_peers_generation(&b);
	as_status status = tend(cluster);
	uint32_t counts[LOG_COUNT_MAX];
	log_count_stop_many(counts);

	as_node* moved = get_node(cluster, MOVE_NODE);
	bool at_a2 = node_at(moved, &a2);

	if (moved) {
		as_node_release(moved);
	}
	as_node_release(node);

	for (int i = 0; i < AS_NODE_MOVE_FAILED_TENDS - 1; i++) {
		assert_that(kept[i], "tend %d replaced the node", i + 1);
		assert_that(untried[i], "tend %d connected to the other address", i + 1);
	}
	assert_int_eq(status, AEROSPIKE_OK);
	assert_true(at_a2);
	assert_int_eq(counts[0], 1);
	assert_int_eq(counts[1], 0);
}

TEST(nodes_move_slow_lookup_on_tend_thread,
	"U-16 slowLookupDelaysMoveOnTendThread: the hostname lookup of a move runs on the tend thread and delays the tend")
{
	move_setup(env, port);
	DISCOVERY_SCOPED(fnode, a1, fnode_stop);
	DISCOVERY_SCOPED(fnode, a2, fnode_stop);
	assert_true(move_pair(&a1, &a2, MOVE_NODE, MOVE_NODE, port));
	char hostname[64];
	stagger_hostname(hostname, "slow", FIRST, SECOND);
	assert_true(move_connect(&env, hostname, port, 250));

	as_cluster* cluster = env.client->cluster;
	as_node* node = get_node(cluster, MOVE_NODE);
	assert_true(node_at(node, &a1));

	fail_node(&a1);
	assert_true(tends_keep(cluster, node, AS_NODE_MOVE_FAILED_TENDS - 1));

	// Twice the move bound.
	uint32_t delay = 2 * 250;
	resolver_set_delay(hostname, delay);
	as_status status;
	uint64_t elapsed = timed_tend(cluster, &status);
	resolver_set_delay(hostname, 0);

	as_node* moved = get_node(cluster, MOVE_NODE);
	bool at_a2 = node_at(moved, &a2);
	bool old_active = as_node_is_active(node);
	pthread_t thread;
	bool delayed = resolver_delay_thread(hostname, &thread);

	if (moved) {
		as_node_release(moved);
	}
	as_node_release(node);

	assert_int_eq(status, AEROSPIKE_OK);
	assert_that(elapsed >= delay, "the tend took %llu ms", (unsigned long long)elapsed);
	assert_true(at_a2);
	assert_false(old_active);
	assert_true(delayed);
	assert_true(pthread_equal(thread, pthread_self()));
}

TEST(nodes_move_silent_login_within_bound,
	"U-16 silentLoginEndsWithinBound: with a user, a candidate that never answers its login ends within the bound, not login_timeout_ms")
{
	move_setup(env, port);
	DISCOVERY_SCOPED(fnode, dead, fnode_stop);
	DISCOVERY_SCOPED(fnode, silent, fnode_stop);
	assert_true(start_named(&dead, MOVE_NODE, FIRST, port));
	assert_true(serve(&silent, SECOND, port));
	fnode_set_mute(&silent, true);
	char hostname[64];
	stagger_hostname(hostname, "silent-login", FIRST, SECOND);
	assert_true(move_connect_user(&env, hostname, port, 200, "user", 3000));

	as_cluster* cluster = env.client->cluster;
	as_node* node = get_node(cluster, MOVE_NODE);
	assert_true(node_at(node, &dead));
	assert_that(fnode_get(&dead, &dead.logins) >= 1, "no login reached the node");

	fail_node(&dead);
	assert_true(tends_keep(cluster, node, AS_NODE_MOVE_FAILED_TENDS - 1));

	log_count_start("Move node");
	uint32_t accepts = fnode_get(&silent, &silent.accepts);
	as_status status;
	uint64_t elapsed = timed_tend(cluster, &status);
	uint32_t moves = log_count_stop();
	bool tried = fnode_get(&silent, &silent.accepts) > accepts;
	bool kept = holds_name(cluster, node);
	as_node_release(node);

	assert_int_eq(status, AEROSPIKE_OK);
	assert_that(tried, "the move did not connect to the silent candidate");
	assert_that(elapsed >= 200 - 10 && elapsed < 200 + STAGGER_MARGIN, "the tend took %llu ms",
		(unsigned long long)elapsed);
	assert_true(kept);
	assert_int_eq(moves, 0);
}

// The peer path matches only a failing node off loopback, so the node is on a non-loopback
// address and the hostname's addresses are loopback ones.
static bool
move_peer_listed(move_env* env, fnode* a1, fnode* a2, fnode* b, const char* local,
	const char* hostname)
{
	if (!start_on(b, MOVE_OTHER, FIRST)) {
		return false;
	}

	// X joins by its IP literal, and B lists it by hostname. B owns every partition, so it is
	// kept while the failing X does not list it.
	set_peers(a1, "[[%s,,[%s:%u]]]", MOVE_OTHER, FIRST, b->port);
	set_peers(a2, "[[%s,,[%s:%u]]]", MOVE_OTHER, FIRST, b->port);
	set_peers(b, "[[%s,,[%s:%u]]]", MOVE_NODE, hostname, a1->port);
	fnode_set_partitions(b, true);
	return move_connect(env, local, a1->port, 1000);
}

TEST(nodes_move_loopback_peer_match_no_move,
	"U-16 loopbackPeerMatchDoesNotMove: a node matched to a hostname only through loopback addresses never moves")
{
	char local[32];

	if (!local_address(local, sizeof(local))) {
		info("skipped: no non-loopback IPv4 address");
		return;
	}

	move_setup_env(env);
	DISCOVERY_SCOPED(fnode, a1, fnode_stop);
	DISCOVERY_SCOPED(fnode, a2, fnode_stop);
	DISCOVERY_SCOPED(fnode, b, fnode_stop);
	assert_true(move_pair_on(&a1, &a2, MOVE_NODE, local, SECOND));
	char hostname[64];
	stagger_hostname(hostname, "loopback", SECOND, THIRD);
	assert_true(move_peer_listed(&env, &a1, &a2, &b, local, hostname));

	as_cluster* cluster = env.client->cluster;
	assert_int_eq(node_count(cluster), 2);
	as_node* node = get_node(cluster, MOVE_NODE);
	assert_true(node_at(node, &a1));
	assert_null(node->hostname);

	fail_node(&a1);
	log_count_start("Move node");
	bool kept = true;

	for (int i = 0; i < AS_NODE_MOVE_FAILED_TENDS + 1; i++) {
		fnode_bump_peers_generation(&b);
		kept = tends_keep(cluster, node, 1) && kept;
	}
	uint32_t moves = log_count_stop();
	bool named = node->hostname && strcmp(node->hostname, hostname) == 0;
	bool resolved = node->hostname_resolved;
	as_node_release(node);

	assert_true(kept);
	assert_that(named, "the peer path did not set the hostname");
	assert_false(resolved);
	assert_int_eq(fnode_get(&a2, &a2.accepts), 0);
	assert_int_eq(moves, 0);
}

TEST(nodes_move_loopback_before_address_moves,
	"U-16 loopbackBeforeAddressMoves: a hostname that resolves to a loopback address before the node's address still moves the node")
{
	char local[32];

	if (!local_address(local, sizeof(local))) {
		info("skipped: no non-loopback IPv4 address");
		return;
	}

	move_setup_env(env);
	DISCOVERY_SCOPED(fnode, a1, fnode_stop);
	DISCOVERY_SCOPED(fnode, a2, fnode_stop);
	DISCOVERY_SCOPED(fnode, b, fnode_stop);
	assert_true(move_pair_on(&a1, &a2, MOVE_NODE, local, SECOND));
	char hostname[64];
	stagger_hostname(hostname, "loopback-first", SECOND, local);
	assert_true(move_peer_listed(&env, &a1, &a2, &b, local, hostname));

	as_cluster* cluster = env.client->cluster;
	assert_int_eq(node_count(cluster), 2);
	as_node* node = get_node(cluster, MOVE_NODE);
	assert_true(node_at(node, &a1));

	fail_node(&a1);
	char expected[256];
	move_message(expected, sizeof(expected), hostname, &a1, &a2);
	log_count_start(expected);
	bool ok = true;

	for (int i = 0; i < AS_NODE_MOVE_FAILED_TENDS; i++) {
		fnode_bump_peers_generation(&b);
		ok = tend(cluster) == AEROSPIKE_OK && ok;
	}
	uint32_t moves = log_count_stop();

	as_node* moved = get_node(cluster, MOVE_NODE);
	bool at_a2 = node_at(moved, &a2);

	if (moved) {
		as_node_release(moved);
	}
	as_node_release(node);

	assert_true(ok);
	assert_true(at_a2);
	assert_int_eq(moves, 1);
}

// Load balancer detection skips a seed on loopback, so the seed is on a non-loopback address.
TEST(nodes_move_load_balancer_seed_no_move,
	"U-16 loadBalancerSeedDoesNotMove: a seed node whose address load balancer detection replaced never moves")
{
	char local[32];

	if (!local_address(local, sizeof(local))) {
		info("skipped: no non-loopback IPv4 address");
		return;
	}

	move_setup_env(env);
	DISCOVERY_SCOPED(fnode, lb, fnode_stop);
	DISCOVERY_SCOPED(fnode, real, fnode_stop);
	assert_true(move_pair_on(&lb, &real, MOVE_NODE, local, THIRD));
	char service[64];
	fnode_set_service_std(&lb, fnode_addr(&real, service, sizeof(service)));
	char hostname[64];
	stagger_hostname(hostname, "lb", local, SECOND);
	assert_true(move_connect(&env, hostname, lb.port, 1000));

	as_cluster* cluster = env.client->cluster;
	as_node* node = get_node(cluster, MOVE_NODE);
	assert_true(node_at(node, &real));
	assert_true(node->hostname && strcmp(node->hostname, hostname) == 0);
	assert_false(node->hostname_resolved);

	uint32_t lb_accepts = fnode_get(&lb, &lb.accepts);
	fail_node(&real);
	log_count_start("Move node");
	bool kept = tends_keep(cluster, node, AS_NODE_MOVE_FAILED_TENDS + 1);
	uint32_t moves = log_count_stop();
	as_node_release(node);

	assert_true(kept);
	assert_int_eq(fnode_get(&lb, &lb.accepts), lb_accepts);
	assert_int_eq(moves, 0);
}

//---------------------------------
// Tests: init error when no seed connects
//---------------------------------

#define SEEDS_MANY 40

static void
seeds_config(as_config* config)
{
	as_config_init(config);
	config->tender_interval = NODES_TEND_INTERVAL;
	config->conn_timeout_ms = 1000;
	config->fail_if_not_connected = true;
	resolver_install();
}

// Does s end with suffix?
static bool
ends_with(const char* s, const char* suffix)
{
	size_t len = strlen(s);
	size_t size = strlen(suffix);
	return len >= size && strcmp(s + len - size, suffix) == 0;
}

static uint32_t
occurrences(const char* s, const char* needle)
{
	uint32_t count = 0;

	for (const char* p = strstr(s, needle); p; p = strstr(p + 1, needle)) {
		count++;
	}
	return count;
}

TEST(nodes_seeds_failed_lists_each,
	"init with no reachable seed fails with each seed and its error, and a skipped seed with none")
{
	uint16_t port = free_port();
	assert_int_ne(port, 0);
	as_config config;
	seeds_config(&config);
	as_config_add_host(&config, FIRST, port);
	as_config_add_host(&config, SECOND, port);

	// ip_map points the third seed at the first, which is a known invalid host by then, so it is
	// skipped.
	as_config_add_host(&config, "10.255.0.1", port);
	as_addr_map map = {.orig = "10.255.0.1", .alt = FIRST};
	config.ip_map = &map;
	config.ip_map_size = 1;
	as_error err;
	as_status status = connect_error(&config, &err);
	resolver_remove();
	info("%d %s", status, err.message);

	char first[96];
	snprintf(first, sizeof(first), "Failed to connect to [3] host(s): %s %u ", FIRST, port);
	char second[64];
	snprintf(second, sizeof(second), "; %s %u ", SECOND, port);
	char skipped[64];
	snprintf(skipped, sizeof(skipped), "; %s %u", FIRST, port);
	const char* second_at = strstr(err.message, second);

	assert_int_ne(status, AEROSPIKE_OK);
	assert_that(strncmp(err.message, first, strlen(first)) == 0, "%s", err.message);
	assert_that(err.message[strlen(first)] != ';', "no error for the first seed: %s", err.message);
	assert_that(second_at && second_at[strlen(second)] != ';', "%s", err.message);
	assert_that(ends_with(err.message, skipped), "%s", err.message);
}

TEST(nodes_seeds_failed_cut_to_fit,
	"init with more failed seeds than the error holds lists those that fit and counts the rest")
{
	uint16_t port = free_port();
	assert_int_ne(port, 0);
	as_config config;
	seeds_config(&config);

	for (uint32_t i = 0; i < SEEDS_MANY; i++) {
		char ip[INET_ADDRSTRLEN];
		snprintf(ip, sizeof(ip), "127.0.1.%u", i + 1);
		as_config_add_host(&config, ip, port);
	}
	as_error err;
	as_status status = connect_error(&config, &err);
	resolver_remove();
	info("%d %s", status, err.message);

	char first[96];
	snprintf(first, sizeof(first), "Failed to connect to [%u] host(s): 127.0.1.1 %u ", SEEDS_MANY,
		port);
	const char* tail = NULL;

	for (const char* p = strstr(err.message, "; and "); p; p = strstr(p + 1, "; and ")) {
		tail = p;
	}

	unsigned int more = 0;
	uint32_t listed = 1 + occurrences(err.message, "; 127.0.1.");

	assert_int_ne(status, AEROSPIKE_OK);
	assert_that(strncmp(err.message, first, strlen(first)) == 0, "%s", err.message);
	assert_that(strlen(err.message) < AS_ERROR_MESSAGE_MAX_SIZE, "%s", err.message);
	assert_that(tail && sscanf(tail, "; and %u more", &more) == 1 && ends_with(err.message, " more"),
		"%s", err.message);
	assert_that(more > 0 && listed + more == SEEDS_MANY, "%u listed and %u more: %s", listed, more,
		err.message);
}

TEST(nodes_seeds_failed_alternate_missing,
	"init returns the alternate-missing error of a seed, not the list of failed seeds")
{
	uint16_t port = free_port();
	assert_int_ne(port, 0);
	DISCOVERY_SCOPED(fnode, x, fnode_stop);
	assert_true(start_named(&x, "X", SECOND, port));
	fnode_set_empty_alt(&x, true);
	as_config config;
	seeds_config(&config);
	config.use_services_alternate = true;
	as_config_add_host(&config, FIRST, port);
	as_config_add_host(&config, SECOND, port);
	as_error err;
	as_status status = connect_error(&config, &err);
	resolver_remove();
	info("%d %s", status, err.message);

	assert_int_eq(status, AEROSPIKE_ERR_INVALID_NODE);
	assert_that(strstr(err.message, "Node X ") && strstr(err.message, "no alternate-access-address") &&
		strstr(err.message, "use_services_alternate (useServicesAlternate) is true"), "%s", err.message);
	assert_that(! strstr(err.message, "Failed to connect to ["), "%s", err.message);
}

TEST(nodes_seeds_failed_lookup_listed,
	"init lists a seed whose name does not resolve with its lookup error")
{
	uint16_t port = free_port();
	assert_int_ne(port, 0);
	as_config config;
	seeds_config(&config);
	char hostname[64];
	stagger_hostname(hostname, "no-address", NULL);
	as_config_add_host(&config, hostname, port);
	as_config_add_host(&config, FIRST, port);
	as_error err;
	as_status status = connect_error(&config, &err);
	resolver_remove();
	info("%d %s", status, err.message);

	char first[256];
	snprintf(first, sizeof(first), "Failed to connect to [2] host(s): %s %u Invalid hostname %s: ",
		hostname, port, hostname);
	char second[64];
	snprintf(second, sizeof(second), "; %s %u ", FIRST, port);

	assert_int_ne(status, AEROSPIKE_OK);
	assert_that(strncmp(err.message, first, strlen(first)) == 0, "%s", err.message);
	assert_that(strstr(err.message, second), "%s", err.message);
}

#endif // __linux__

#else

TEST(nodes_fake_servers_skipped, "node lifecycle tests need POSIX loopback servers")
{
	info("skipped: the node lifecycle tests need POSIX sockets");
}

#endif // DISCOVERY_FAKE_SERVERS

//---------------------------------
// Test Suite
//---------------------------------

SUITE(discovery_nodes, "pluggable discovery node lifecycle tests")
{
#ifdef DISCOVERY_FAKE_SERVERS
	suite_add(nodes_one_per_name_after_second_phase_replace);
	suite_add(nodes_second_phase_replace_same_tend);
	suite_add(nodes_not_name_holder_removed);
	suite_add(nodes_same_name_removals_applied);
#if !defined(__APPLE__)
	suite_add(nodes_shm_replace_keeps_slot_active);
	suite_add(nodes_shm_follower_replaces_node);
#endif
#if defined(__linux__)
	suite_add(nodes_stagger_first_address_one_connect);
	suite_add(nodes_stagger_hung_address_delays_next);
	suite_add(nodes_stagger_every_address_starts_within_bound);
	suite_add(nodes_stagger_refused_address_starts_next);
	suite_add(nodes_stagger_every_address_hung_fails);
	suite_add(nodes_stagger_last_valid_preferred);
	suite_add(nodes_stagger_single_address_as_before);
	suite_add(nodes_stagger_ip_literal_as_before);
	suite_add(nodes_stagger_timeout_below_tend_interval);
	suite_add(nodes_stagger_wrong_node_tries_next);
	suite_add(nodes_stagger_login_skips_hung_address);
	suite_add(nodes_stagger_no_descriptor_leak);
	suite_add(nodes_stagger_tls_failure_bounded);
	suite_add(nodes_stagger_refused_names_first_failure);
	suite_add(nodes_stagger_alternate_missing);
	suite_add(nodes_peer_wrong_node_tries_next);
	suite_add(nodes_peer_every_address_wrong_node);
	suite_add(nodes_peer_wrong_node_then_alternate_missing);
	suite_add(nodes_peer_known_node_skipped);
	suite_add(nodes_peer_known_host_logged_once);
	suite_add(nodes_peer_known_and_unknown_fails);
	suite_add(nodes_peer_socket_collision_stops);
	suite_add(nodes_peer_hung_address_joins_in_one_tend);
	suite_add(nodes_valid_addresses_pruned);
	suite_add(nodes_move_not_before_third_failed_tend);
	suite_add(nodes_move_failing_node_moves);
	suite_add(nodes_move_recovered_node_counts_again);
	suite_add(nodes_move_every_node_on_dead_address);
	suite_add(nodes_move_one_candidate_per_tend);
	suite_add(nodes_move_silent_candidate_within_bound);
	suite_add(nodes_move_other_name_skipped);
	suite_add(nodes_move_collision_skipped);
	suite_add(nodes_move_ip_literal_no_move);
	suite_add(nodes_move_single_address_no_move);
	suite_add(nodes_move_no_lookup_without_failures);
	suite_add(nodes_move_peer_path_not_sooner);
	suite_add(nodes_move_slow_lookup_on_tend_thread);
	suite_add(nodes_move_silent_login_within_bound);
	suite_add(nodes_move_loopback_peer_match_no_move);
	suite_add(nodes_move_loopback_before_address_moves);
	suite_add(nodes_move_load_balancer_seed_no_move);
	suite_add(nodes_seeds_failed_lists_each);
	suite_add(nodes_seeds_failed_cut_to_fit);
	suite_add(nodes_seeds_failed_alternate_missing);
	suite_add(nodes_seeds_failed_lookup_listed);
#endif
#else
	suite_add(nodes_fake_servers_skipped);
#endif
}
