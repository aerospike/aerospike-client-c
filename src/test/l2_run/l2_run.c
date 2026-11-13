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

/*
 * L2_RUN of the l2-proxy harness in aerospike-client-conformance (l2-proxy/README.md there, "A
 * client's own tests"): the L2 tests that need client internals. run.sh runs it once per profile
 * in the client image on the client network, with the profile's values file as its environment
 * and L2_PROFILE naming the profile.
 *
 *   idle-reset  F-1: idle pooled connections reset by the proxy give no failure to the caller,
 *               synchronously and on each event loop.
 *   default     N-18: two peers port-mapped to one listener; neither becomes a node.
 *
 * Every other profile has no test here and exits 0. The exit status is non-zero when a test
 * fails, a value is missing or cleanup did not complete. Built against installed headers only by
 * "make l2-run"; POSIX only.
 */
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <netdb.h>
#include <poll.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_stats.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_config.h>
#include <aerospike/as_event.h>
#include <aerospike/as_key.h>
#include <aerospike/as_node.h>
#include <aerospike/as_record.h>
#include <aerospike/as_sleep.h>
#include <aerospike/version.h>

#include "l2_run/live.h"
#include "test.h"

//---------------------------------
// Macros
//---------------------------------

#define L2_NAMESPACE "test"
#define L2_LISTENERS_MAX 16
#define L2_NODES 3
#define L2_CONNECT_MS 5000
#define L2_PROBE_MS 5000
#define L2_HTTP_MS 10000
#define L2_HTTP_MAX (64 * 1024)
#define L2_JOIN_MS 30000
#define L2_KEYS 100
#define L2_WARM_THREADS 4
#define L2_WARM_ITERATIONS 50
#define L2_EVENT_LOOPS 2
#define L2_WARM_CONCURRENCY 8
#define L2_WARM_ROUNDS 25
#define L2_ASYNC_WAIT_MS 30000
#define L2_FAILED_MAX 20
#define L2_COLLISION_MS 15000
#define L2_COLLISION_WARNS 3
#define L2_SAMPLE_MS 100
#define L2_N18_KEYS 300
#define L2_LINE_SIZE 4096

//---------------------------------
// Types
//---------------------------------

typedef struct l2_listener_s {
	char proxy[128];
	uint16_t port;
	char node[AS_NODE_NAME_SIZE];
} l2_listener;

typedef struct l2_toxic_ref_s {
	char proxy[128];
	char toxic[64];
} l2_toxic_ref;

typedef struct l2_node_counts_s {
	char name[AS_NODE_NAME_SIZE];
	as_conn_stats sync;
	as_conn_stats async;
	uint64_t errors;
	uint64_t timeouts;
} l2_node_counts;

typedef struct l2_counts_s {
	l2_node_counts nodes[L2_LISTENERS_MAX];
	uint32_t size;
	uint64_t retries;
} l2_counts;

typedef struct l2_warm_s {
	aerospike* as;
	uint32_t thread;
	uint32_t errors;
	char first[512];
} l2_warm;

//---------------------------------
// Globals
//---------------------------------

int g_argc = 0;
char** g_argv = NULL;

static const char* l2_profile;
static const char* l2_endpoint;
static const char* l2_toxi_host;
static uint16_t l2_toxi_port;
static l2_listener l2_listeners[L2_LISTENERS_MAX];
static uint32_t l2_listeners_size;

//---------------------------------
// Sockets
//---------------------------------

// Connect to host:port within timeout_ms and leave the socket blocking, with send and receive
// timeouts of timeout_ms. Returns the descriptor, or -1 with the reason in msg.
static int
l2_connect(const char* host, uint16_t port, uint32_t timeout_ms, char* msg, size_t size)
{
	char service[8];
	snprintf(service, sizeof(service), "%u", port);

	struct addrinfo hints;
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;

	struct addrinfo* res = NULL;
	int rv = getaddrinfo(host, service, &hints, &res);

	if (rv != 0) {
		snprintf(msg, size, "cannot resolve %s: %s", host, gai_strerror(rv));
		return -1;
	}

	int fd = -1;
	int last = 0;

	for (struct addrinfo* ai = res; ai && fd < 0; ai = ai->ai_next) {
		fd = socket(ai->ai_family, SOCK_STREAM, 0);

		if (fd < 0) {
			last = errno;
			continue;
		}

		int flags = fcntl(fd, F_GETFL, 0);

		if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
			last = errno;
			close(fd);
			fd = -1;
			continue;
		}

		if (connect(fd, ai->ai_addr, ai->ai_addrlen) != 0) {
			if (errno != EINPROGRESS) {
				last = errno;
				close(fd);
				fd = -1;
				continue;
			}

			struct pollfd p = {.fd = fd, .events = POLLOUT, .revents = 0};
			int so_error = 0;
			socklen_t len = sizeof(so_error);
			rv = poll(&p, 1, (int)timeout_ms);

			if (rv <= 0 || getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_error, &len) != 0 || so_error != 0) {
				last = rv == 0 ? ETIMEDOUT : so_error ? so_error : errno;
				close(fd);
				fd = -1;
				continue;
			}
		}

		struct timeval tv = {.tv_sec = timeout_ms / 1000, .tv_usec = (timeout_ms % 1000) * 1000};

		if (fcntl(fd, F_SETFL, flags) < 0 ||
			setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) != 0 ||
			setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) != 0) {
			last = errno;
			close(fd);
			fd = -1;
		}
	}
	freeaddrinfo(res);

	if (fd < 0) {
		snprintf(msg, size, "cannot connect to %s:%u: %s", host, port, strerror(last));
	}
	return fd;
}

static bool
l2_send_all(int fd, const void* buf, size_t len)
{
	const uint8_t* p = buf;

	while (len > 0) {
		ssize_t n = send(fd, p, len, MSG_NOSIGNAL);

		if (n <= 0) {
			if (n < 0 && errno == EINTR) {
				continue;
			}
			return false;
		}
		p += n;
		len -= (size_t)n;
	}
	return true;
}

static bool
l2_recv_all(int fd, void* buf, size_t len)
{
	uint8_t* p = buf;

	while (len > 0) {
		ssize_t n = recv(fd, p, len, 0);

		if (n <= 0) {
			if (n < 0 && errno == EINTR) {
				continue;
			}
			return false;
		}
		p += n;
		len -= (size_t)n;
	}
	return true;
}

//---------------------------------
// Info
//---------------------------------

// One raw info request "node" on fd (proto version 2, type 1); the node name goes to name.
static bool
l2_info_node_fd(int fd, char* name, size_t size, char* msg, size_t msg_size)
{
	static const char command[] = "node\n";
	uint64_t len = sizeof(command) - 1;
	uint64_t header = (2ULL << 56) | (1ULL << 48) | len;
	uint8_t request[8 + sizeof(command) - 1];

	for (int i = 0; i < 8; i++) {
		request[i] = (uint8_t)(header >> (56 - 8 * i));
	}
	memcpy(request + 8, command, len);

	if (! l2_send_all(fd, request, sizeof(request))) {
		snprintf(msg, msg_size, "info send: %s", strerror(errno));
		return false;
	}

	uint8_t reply[8];

	if (! l2_recv_all(fd, reply, sizeof(reply))) {
		snprintf(msg, msg_size, "info reply header: %s", strerror(errno));
		return false;
	}

	uint64_t value = 0;

	for (int i = 0; i < 8; i++) {
		value = (value << 8) | reply[i];
	}

	uint64_t body = value & 0xFFFFFFFFFFFFULL;
	char text[1024];

	if (body == 0 || body >= sizeof(text)) {
		snprintf(msg, msg_size, "info reply of %" PRIu64 " bytes", body);
		return false;
	}

	if (! l2_recv_all(fd, text, (size_t)body)) {
		snprintf(msg, msg_size, "info reply body: %s", strerror(errno));
		return false;
	}
	text[body] = '\0';

	// "node\t<name>\n"
	char* tab = strchr(text, '\t');
	char* begin = tab ? tab + 1 : text;
	begin[strcspn(begin, "\n")] = '\0';

	if (! begin[0] || strlen(begin) >= size) {
		snprintf(msg, msg_size, "info reply without a node name: %s", text);
		return false;
	}
	as_strncpy(name, begin, size);
	return true;
}

// Read every listener's node name through the listener.
static bool
l2_read_nodes(char* msg, size_t size)
{
	for (uint32_t i = 0; i < l2_listeners_size; i++) {
		l2_listener* l = &l2_listeners[i];
		char reason[512];
		int fd = l2_connect(l2_endpoint, l->port, L2_CONNECT_MS, reason, sizeof(reason));

		if (fd < 0) {
			snprintf(msg, size, "listener %s: %s", l->proxy, reason);
			return false;
		}

		bool ok = l2_info_node_fd(fd, l->node, sizeof(l->node), reason, sizeof(reason));
		close(fd);

		if (! ok) {
			snprintf(msg, size, "listener %s: %s", l->proxy, reason);
			return false;
		}
		printf("L2-NODE listener=%s port=%u node=%s\n", l->proxy, l->port, l->node);
	}
	return true;
}

//---------------------------------
// Probes
//---------------------------------

static bool
l2_probe_undo(void* ptr)
{
	close(*(int*)ptr);
	return true;
}

// A connection through listener i held open after one info round trip, as Java's probe, so
// Toxiproxy has linked it to the node. Registered for cleanup under "probe <i>".
static int
l2_probe_open(uint32_t i, char* msg, size_t size)
{
	l2_listener* l = &l2_listeners[i];
	int fd = l2_connect(l2_endpoint, l->port, L2_PROBE_MS, msg, size);

	if (fd < 0) {
		return -1;
	}

	int* ptr = malloc(sizeof(int));

	if (! ptr) {
		close(fd);
		snprintf(msg, size, "out of memory");
		return -1;
	}
	*ptr = fd;

	char key[32];
	snprintf(key, sizeof(key), "probe %u", i);
	live_cleanup_push(key, l2_probe_undo, ptr);

	char name[AS_NODE_NAME_SIZE];
	return l2_info_node_fd(fd, name, sizeof(name), msg, size) ? fd : -1;
}

static void
l2_probe_close(uint32_t i, int fd)
{
	char key[32];
	snprintf(key, sizeof(key), "probe %u", i);
	close(fd);
	live_cleanup_drop(key);
}

// Read the next byte of a probe: RST when the peer reset the connection, FIN when it closed it,
// DATA with the byte, otherwise ERROR with errno (a timeout after L2_PROBE_MS included).
static void
l2_probe_result(int fd, char* result, size_t size)
{
	uint8_t b;
	ssize_t n;

	do {
		n = recv(fd, &b, 1, 0);
	} while (n < 0 && errno == EINTR);

	if (n == 1) {
		snprintf(result, size, "DATA %u", b);
	}
	else if (n == 0) {
		snprintf(result, size, "FIN");
	}
	else if (errno == ECONNRESET) {
		snprintf(result, size, "RST");
	}
	else {
		snprintf(result, size, "ERROR %d %s", errno, strerror(errno));
	}
}

//---------------------------------
// Toxiproxy
//---------------------------------

// One request to Toxiproxy's API with Connection: close. Returns the HTTP status, or 0 when no
// response was read, with the body (or the reason) in body.
static int
l2_toxiproxy(const char* method, const char* path, const char* json, char* body, size_t size)
{
	char msg[512];
	int fd = l2_connect(l2_toxi_host, l2_toxi_port, L2_HTTP_MS, msg, sizeof(msg));

	if (fd < 0) {
		as_strncpy(body, msg, size);
		return 0;
	}

	size_t json_len = json ? strlen(json) : 0;
	char head[1024];
	int head_len = snprintf(head, sizeof(head),
		"%s %s HTTP/1.1\r\nHost: %s:%u\r\nContent-Type: application/json\r\nContent-Length: %zu\r\n"
		"Connection: close\r\n\r\n", method, path, l2_toxi_host, l2_toxi_port, json_len);

	if (head_len <= 0 || (size_t)head_len >= sizeof(head) || ! l2_send_all(fd, head, (size_t)head_len) ||
		(json_len > 0 && ! l2_send_all(fd, json, json_len))) {
		snprintf(body, size, "cannot send %s %s: %s", method, path, strerror(errno));
		close(fd);
		return 0;
	}

	char* response = malloc(L2_HTTP_MAX + 1);

	if (! response) {
		snprintf(body, size, "out of memory");
		close(fd);
		return 0;
	}

	size_t len = 0;

	while (len < L2_HTTP_MAX) {
		ssize_t n = recv(fd, response + len, L2_HTTP_MAX - len, 0);

		if (n < 0 && errno == EINTR) {
			continue;
		}

		if (n <= 0) {
			break;
		}
		len += (size_t)n;
	}
	close(fd);
	response[len] = '\0';

	int status = 0;
	char* end = strstr(response, "\r\n\r\n");

	if (sscanf(response, "HTTP/%*u.%*u %d", &status) != 1 || ! end) {
		snprintf(body, size, "no HTTP response to %s %s (%zu bytes)", method, path, len);
		free(response);
		return 0;
	}
	as_strncpy(body, end + 4, size);
	free(response);
	return status;
}

static bool
l2_listener_set(const char* proxy, bool enabled)
{
	char path[256];
	char body[L2_LINE_SIZE];
	snprintf(path, sizeof(path), "/proxies/%.127s", proxy);
	int status = l2_toxiproxy("POST", path, enabled ? "{\"enabled\":true}" : "{\"enabled\":false}",
		body, sizeof(body));
	printf("L2-TOXIPROXY %s enabled=%s status=%d %s\n", proxy, enabled ? "true" : "false", status, body);
	return status == 200;
}

static bool
l2_listener_undo(void* ptr)
{
	return l2_listener_set(ptr, true);
}

// Disable or enable a listener. A disabled listener is registered for cleanup until enabled.
static bool
l2_toxi_listener(const char* proxy, bool enabled)
{
	char key[192];
	snprintf(key, sizeof(key), "listener %.127s", proxy);

	if (! enabled) {
		char* ptr = strdup(proxy);

		if (! ptr) {
			return false;
		}
		live_cleanup_push(key, l2_listener_undo, ptr);
		return l2_listener_set(proxy, false);
	}

	if (! l2_listener_set(proxy, true)) {
		return false;
	}
	live_cleanup_drop(key);
	return true;
}

static bool
l2_toxic_delete(const char* proxy, const char* toxic)
{
	char path[256];
	char body[L2_LINE_SIZE];
	snprintf(path, sizeof(path), "/proxies/%.127s/toxics/%.63s", proxy, toxic);
	int status = l2_toxiproxy("DELETE", path, NULL, body, sizeof(body));
	printf("L2-TOXIPROXY toxic remove %s %s status=%d\n", proxy, toxic, status);
	return status == 204;
}

static bool
l2_toxic_undo(void* ptr)
{
	l2_toxic_ref* ref = ptr;
	return l2_toxic_delete(ref->proxy, ref->toxic);
}

// Add a toxic, registered for cleanup before the request so a toxic is removed even when the
// reply is lost.
static bool
l2_toxi_add(const char* proxy, const char* toxic, const char* type, const char* stream,
	double toxicity, const char* attributes)
{
	l2_toxic_ref* ref = calloc(1, sizeof(l2_toxic_ref));

	if (! ref) {
		return false;
	}
	as_strncpy(ref->proxy, proxy, sizeof(ref->proxy));
	as_strncpy(ref->toxic, toxic, sizeof(ref->toxic));

	char key[192];
	snprintf(key, sizeof(key), "toxic %.100s %.63s", proxy, toxic);
	live_cleanup_push(key, l2_toxic_undo, ref);

	char path[256];
	char json[512];
	char body[L2_LINE_SIZE];
	snprintf(path, sizeof(path), "/proxies/%.127s/toxics", proxy);
	snprintf(json, sizeof(json),
		"{\"name\":\"%s\",\"type\":\"%s\",\"stream\":\"%s\",\"toxicity\":%.1f,\"attributes\":%s}",
		toxic, type, stream, toxicity, attributes);
	int status = l2_toxiproxy("POST", path, json, body, sizeof(body));
	printf("L2-TOXIPROXY toxic add %s %s status=%d %s\n", proxy, toxic, status, body);
	return status == 200;
}

//---------------------------------
// Client
//---------------------------------

// as_config_init() with the seed at the first listener and a port-map from each node name to
// the listener at node_listener[i], DISCOVERY_ENDPOINT_HOSTNAME:<port>.
static as_status
l2_config(as_config* config, const uint32_t* node_listener, as_error* err)
{
	as_config_init(config);
	as_config_add_host(config, l2_endpoint, l2_listeners[0].port);

	as_addr_map map[L2_LISTENERS_MAX];
	char alt[L2_LISTENERS_MAX][300];

	for (uint32_t i = 0; i < l2_listeners_size; i++) {
		snprintf(alt[i], sizeof(alt[i]), "%s:%u", l2_endpoint, l2_listeners[node_listener[i]].port);
		map[i].orig = l2_listeners[i].node;
		map[i].alt = alt[i];
		printf("L2-PORT-MAP %s=%s\n", map[i].orig, map[i].alt);
	}
	return as_config_set_port_map_translator(config, map, l2_listeners_size, err);
}

static void
l2_all_names(char* buf, size_t size)
{
	char* items[L2_LISTENERS_MAX];

	for (uint32_t i = 0; i < l2_listeners_size; i++) {
		items[i] = l2_listeners[i].node;
	}
	live_sort_join(items, l2_listeners_size, buf, size);
}

static int
l2_compare_counts(const void* a, const void* b)
{
	return strcmp(((const l2_node_counts*)a)->name, ((const l2_node_counts*)b)->name);
}

static void
l2_counts_read(aerospike* as, l2_counts* c)
{
	as_cluster_stats stats;
	aerospike_stats(as, &stats);
	c->size = 0;

	for (uint32_t i = 0; i < stats.nodes_size && c->size < L2_LISTENERS_MAX; i++) {
		as_node_stats* s = &stats.nodes[i];
		l2_node_counts* n = &c->nodes[c->size++];
		as_strncpy(n->name, s->node->name, sizeof(n->name));
		n->sync = s->sync;
		n->async = s->async;
		n->errors = s->error_count;
		n->timeouts = s->timeout_count;
	}
	c->retries = stats.retry_count;
	aerospike_stats_destroy(&stats);
	qsort(c->nodes, c->size, sizeof(l2_node_counts), l2_compare_counts);
}

static const l2_node_counts*
l2_counts_find(const l2_counts* c, const char* name)
{
	for (uint32_t i = 0; i < c->size; i++) {
		if (strcmp(c->nodes[i].name, name) == 0) {
			return &c->nodes[i];
		}
	}
	return NULL;
}

static void
l2_counts_format(const l2_counts* c, bool async, char* buf, size_t size)
{
	size_t len = 0;
	buf[0] = '\0';

	for (uint32_t i = 0; i < c->size && len < size; i++) {
		const l2_node_counts* n = &c->nodes[i];
		const as_conn_stats* s = async ? &n->async : &n->sync;
		len += (size_t)snprintf(buf + len, size - len,
			"%s%s{inUse=%u,inPool=%u,opened=%u,closed=%u,errors=%" PRIu64 ",timeouts=%" PRIu64 "}",
			i ? " " : "", n->name, s->in_use, s->in_pool, s->opened, s->closed, n->errors, n->timeouts);
	}
}

// Disable and re-enable every listener, so each drops the connections it holds.
static bool
l2_cycle_listeners(char* msg, size_t size)
{
	for (uint32_t i = 0; i < l2_listeners_size; i++) {
		if (! l2_toxi_listener(l2_listeners[i].proxy, false) || ! l2_toxi_listener(l2_listeners[i].proxy, true)) {
			snprintf(msg, size, "cannot cycle listener %s", l2_listeners[i].proxy);
			return false;
		}
	}
	return true;
}

// Open one probe per listener; read them with l2_probes_read() after the listeners cycled.
static bool
l2_probes_open(int* probes, char* msg, size_t size)
{
	for (uint32_t i = 0; i < l2_listeners_size; i++) {
		probes[i] = l2_probe_open(i, msg, size);

		if (probes[i] < 0) {
			return false;
		}
	}
	return true;
}

// Read and close every probe. Returns the number that did not read RST; the first goes to msg.
static uint32_t
l2_probes_read(const char* label, int* probes, char* msg, size_t size)
{
	uint32_t bad = 0;
	msg[0] = '\0';

	for (uint32_t i = 0; i < l2_listeners_size; i++) {
		char result[256];
		l2_probe_result(probes[i], result, sizeof(result));
		l2_probe_close(i, probes[i]);
		printf("%s listener=%u result=%s\n", label, l2_listeners[i].port, result);

		if (strncmp(result, "RST", 3) != 0) {
			if (bad++ == 0) {
				snprintf(msg, size, "listener %u did not reset an idle flow with RST: %s",
					l2_listeners[i].port, result);
			}
		}
	}
	return bad;
}

// Add the reset_peer toxic at toxicity 0 on every listener, as Java's F-1 recipe does: it adds
// no fault of its own, but makes a disabled listener close its flows with RST rather than FIN.
static bool
l2_add_reset_toxics(const char* toxic, char* msg, size_t size)
{
	for (uint32_t i = 0; i < l2_listeners_size; i++) {
		if (! l2_toxi_add(l2_listeners[i].proxy, toxic, "reset_peer", "upstream", 0.0, "{\"timeout\":0}")) {
			snprintf(msg, size, "cannot add toxic %s on %s", toxic, l2_listeners[i].proxy);
			return false;
		}
	}
	return true;
}

//---------------------------------
// F-1 sync
//---------------------------------

static void*
l2_warm_run(void* udata)
{
	l2_warm* w = udata;

	for (uint32_t i = 0; i < L2_WARM_ITERATIONS; i++) {
		char name[32];
		snprintf(name, sizeof(name), "f1-%u", (w->thread * L2_WARM_ITERATIONS + i) % L2_KEYS);
		as_key key;
		as_key_init_str(&key, L2_NAMESPACE, "l2f", name);

		as_error err;
		as_record rec;
		as_record_init(&rec, 1);
		as_record_set_int64(&rec, "v", i);
		as_status status = aerospike_key_put(w->as, &err, NULL, &key, &rec);
		as_record_destroy(&rec);

		if (status == AEROSPIKE_OK) {
			as_record* got = NULL;
			status = aerospike_key_get(w->as, &err, NULL, &key, &got);

			if (got) {
				as_record_destroy(got);
			}
		}

		if (status != AEROSPIKE_OK && w->errors++ == 0) {
			snprintf(w->first, sizeof(w->first), "%s: %d %.400s", name, err.code, err.message);
		}
		as_key_destroy(&key);
	}
	return NULL;
}

TEST(f1_idle_reset_sync, "F-1: idle pooled sync connections reset with RST, no failure to the caller")
{
	live_cleanup();
	char msg[L2_LINE_SIZE];
	check(l2_read_nodes(msg, sizeof(msg)), "%s", msg);

	uint32_t node_listener[L2_LISTENERS_MAX];

	for (uint32_t i = 0; i < l2_listeners_size; i++) {
		node_listener[i] = i;
	}

	as_config config;
	as_error err;

	if (l2_config(&config, node_listener, &err) != AEROSPIKE_OK) {
		as_config_destroy(&config);
		fail("port-map: %s", err.message);
	}

	uint32_t max_socket_idle = config.max_socket_idle;

	if (max_socket_idle != 0) {
		as_config_destroy(&config);
		fail("max_socket_idle is %u, not the as_config_init() default 0", max_socket_idle);
	}

	if (! l2_add_reset_toxics("f1-rst", msg, sizeof(msg))) {
		as_config_destroy(&config);
		fail("%s", msg);
	}

	aerospike* as = live_connect(&config, &err);
	check(as, "client not created: %d %s", err.code, err.message);

	char names[LIVE_LIST_SIZE];
	l2_all_names(names, sizeof(names));
	check(live_wait_nodes(as, L2_NAMESPACE, names, L2_JOIN_MS, msg, sizeof(msg)), "expected %s: %s", names, msg);

	pthread_t threads[L2_WARM_THREADS];
	l2_warm warm[L2_WARM_THREADS];
	uint32_t started = 0;

	for (uint32_t t = 0; t < L2_WARM_THREADS; t++) {
		memset(&warm[t], 0, sizeof(warm[t]));
		warm[t].as = as;
		warm[t].thread = t;

		if (pthread_create(&threads[t], NULL, l2_warm_run, &warm[t]) != 0) {
			break;
		}
		started++;
	}

	uint32_t warm_errors = 0;
	const char* warm_first = "";

	for (uint32_t t = 0; t < started; t++) {
		pthread_join(threads[t], NULL);
		warm_errors += warm[t].errors;

		if (warm[t].errors && ! warm_first[0]) {
			warm_first = warm[t].first;
		}
	}
	check(started == L2_WARM_THREADS, "started %u of %u warm-up threads", started, L2_WARM_THREADS);
	check(warm_errors == 0, "%u warm-up errors, first %s", warm_errors, warm_first);

	int probes[L2_LISTENERS_MAX];
	check(l2_probes_open(probes, msg, sizeof(msg)), "probe: %s", msg);

	l2_counts idle;
	l2_counts_read(as, &idle);
	char text[L2_LINE_SIZE];
	l2_counts_format(&idle, false, text, sizeof(text));
	uint64_t pooled = 0;
	const char* empty = NULL;

	for (uint32_t i = 0; i < l2_listeners_size; i++) {
		const l2_node_counts* n = l2_counts_find(&idle, l2_listeners[i].node);
		uint32_t in_pool = n ? n->sync.in_pool : 0;
		pooled += in_pool;

		if (in_pool == 0 && ! empty) {
			empty = l2_listeners[i].node;
		}
	}
	printf("L2-F1-IDLE maxSocketIdle=%u pooled=%" PRIu64 " %s\n", max_socket_idle, pooled, text);
	check(! empty, "node %s has no idle pooled connection to reset", empty);

	check(l2_cycle_listeners(msg, sizeof(msg)), "%s", msg);
	uint32_t not_reset = l2_probes_read("L2-F1-PROBE", probes, msg, sizeof(msg));
	check(not_reset == 0, "F-1: %s", msg);

	l2_counts start;
	l2_counts_read(as, &start);

	char failed[L2_FAILED_MAX][512];
	uint32_t failures = 0;
	uint32_t commands = 0;

	for (uint32_t i = 0; i < L2_KEYS; i++) {
		char name[32];
		snprintf(name, sizeof(name), "f1-%u", i);
		as_key key;
		as_key_init_str(&key, L2_NAMESPACE, "l2f", name);

		as_error e;
		as_record rec;
		as_record_init(&rec, 1);
		as_record_set_int64(&rec, "v", i);
		commands++;

		if (aerospike_key_put(as, &e, NULL, &key, &rec) != AEROSPIKE_OK && failures++ < L2_FAILED_MAX) {
			snprintf(failed[failures - 1], sizeof(failed[0]), "put %u: %d %.400s", i, e.code, e.message);
		}
		as_record_destroy(&rec);

		as_record* got = NULL;
		commands++;

		if (aerospike_key_get(as, &e, NULL, &key, &got) != AEROSPIKE_OK && failures++ < L2_FAILED_MAX) {
			snprintf(failed[failures - 1], sizeof(failed[0]), "get %u: %d %.400s", i, e.code, e.message);
		}

		if (got) {
			as_record_destroy(got);
		}
		as_key_destroy(&key);
	}

	l2_counts end;
	l2_counts_read(as, &end);
	uint64_t closed = 0;
	int64_t errors = 0;
	const char* undetected = NULL;

	for (uint32_t i = 0; i < l2_listeners_size; i++) {
		const l2_node_counts* s = l2_counts_find(&start, l2_listeners[i].node);
		const l2_node_counts* e = l2_counts_find(&end, l2_listeners[i].node);
		uint32_t node_closed = s && e ? e->sync.closed - s->sync.closed : 0;

		if (node_closed == 0 && ! undetected) {
			undetected = l2_listeners[i].node;
		}
		closed += node_closed;
		errors += s && e ? (int64_t)(e->errors - s->errors) : 0;
	}

	l2_counts_format(&end, false, text, sizeof(text));
	printf("L2-F1-RESULT commands=%u failedToCaller=%u pooledReset=%" PRIu64 " closedDelta=%" PRIu64
		" errorDelta=%" PRId64 " retryDelta=%" PRIu64 " writeMaxRetries=%u readMaxRetries=%u\n",
		commands, failures, pooled, closed, errors, end.retries - start.retries,
		as->config.policies.write.base.max_retries, as->config.policies.read.base.max_retries);
	printf("L2-F1-STATS %s\n", text);

	for (uint32_t i = 0; i < failures && i < L2_FAILED_MAX; i++) {
		printf("L2-F1-FAILED %s\n", failed[i]);
	}
	check(failures == 0, "F-1: %u commands failed to the caller after idle RST, first %s", failures,
		failed[0]);
	check(! undetected, "no reset pooled connection was closed on node %s", undetected);
	check(closed >= pooled, "closed %" PRIu64 " pooled connections, fewer than the %" PRIu64 " reset",
		closed, pooled);
	live_cleanup();
}

//---------------------------------
// F-1 async
//---------------------------------

#if AS_EVENT_LIB_DEFINED

typedef struct l2_batch_s {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	uint32_t pending;
	uint32_t failures;
	char failed[L2_FAILED_MAX][512];
} l2_batch;

typedef struct l2_command_s {
	l2_batch* batch;
	bool write;
	uint32_t loop;
	char key[32];
} l2_command;

// The callbacks record their outcome and signal; they use no assert macro. A get of a record
// not written yet succeeds, as Java's get returns null.
static void
l2_batch_done(l2_command* c, as_error* err)
{
	l2_batch* b = c->batch;
	pthread_mutex_lock(&b->lock);

	if (err && ! (! c->write && err->code == AEROSPIKE_ERR_RECORD_NOT_FOUND)) {
		if (b->failures < L2_FAILED_MAX) {
			snprintf(b->failed[b->failures], sizeof(b->failed[0]), "%s %s loop %u: %d %.400s",
				c->write ? "put" : "get", c->key, c->loop, err->code, err->message);
		}
		b->failures++;
	}
	b->pending--;
	pthread_cond_broadcast(&b->cond);
	pthread_mutex_unlock(&b->lock);
	free(c);
}

static void
l2_write_listener(as_error* err, void* udata, as_event_loop* event_loop)
{
	(void)event_loop;
	l2_batch_done(udata, err);
}

static void
l2_record_listener(as_error* err, as_record* record, void* udata, as_event_loop* event_loop)
{
	(void)record;
	(void)event_loop;
	l2_batch_done(udata, err);
}

static l2_batch*
l2_batch_new(void)
{
	l2_batch* b = calloc(1, sizeof(l2_batch));

	if (! b) {
		fprintf(stderr, "l2_run: out of memory\n");
		exit(1);
	}
	pthread_mutex_init(&b->lock, NULL);
	pthread_cond_init(&b->cond, NULL);
	return b;
}

// Issue one put or get of key on event loop loop. A command the client rejects at once is
// recorded as a failure; its listener is never called.
static void
l2_async(aerospike* as, l2_batch* b, bool write, uint32_t loop, const char* name, int64_t value)
{
	l2_command* c = calloc(1, sizeof(l2_command));

	if (! c) {
		fprintf(stderr, "l2_run: out of memory\n");
		exit(1);
	}
	c->batch = b;
	c->write = write;
	c->loop = loop;
	as_strncpy(c->key, name, sizeof(c->key));

	pthread_mutex_lock(&b->lock);
	b->pending++;
	pthread_mutex_unlock(&b->lock);

	as_key key;
	as_key_init_str(&key, L2_NAMESPACE, "l2fa", name);
	as_event_loop* event_loop = as_event_loop_get_by_index(loop);
	as_error err;
	as_status status;

	if (write) {
		as_record rec;
		as_record_init(&rec, 1);
		as_record_set_int64(&rec, "v", value);
		status = aerospike_key_put_async(as, &err, NULL, &key, &rec, l2_write_listener, c, event_loop, NULL);
		as_record_destroy(&rec);
	}
	else {
		status = aerospike_key_get_async(as, &err, NULL, &key, l2_record_listener, c, event_loop, NULL);
	}
	as_key_destroy(&key);

	if (status != AEROSPIKE_OK) {
		l2_batch_done(c, &err);
	}
}

// Wait until every command of b completed, at most bound_ms. On a timeout b is left to the
// callbacks still to come, never freed.
static bool
l2_batch_wait(l2_batch* b, uint32_t bound_ms)
{
	uint64_t deadline = live_now() + bound_ms;
	pthread_mutex_lock(&b->lock);

	while (b->pending > 0 && live_now() < deadline) {
		struct timeval tv;
		gettimeofday(&tv, NULL);
		uint64_t ns = (uint64_t)tv.tv_usec * 1000 + 100 * 1000000ULL;
		struct timespec ts = {.tv_sec = tv.tv_sec + (time_t)(ns / 1000000000), .tv_nsec = (long)(ns % 1000000000)};
		pthread_cond_timedwait(&b->cond, &b->lock, &ts);
	}

	bool done = b->pending == 0;
	pthread_mutex_unlock(&b->lock);
	return done;
}

static void
l2_batch_destroy(l2_batch* b)
{
	pthread_mutex_destroy(&b->lock);
	pthread_cond_destroy(&b->cond);
	free(b);
}

static bool
l2_loops_undo(void* ptr)
{
	(void)ptr;
	return as_event_close_loops();
}

// The async pool of every node held and loop, read from the test thread once the loops are idle.
typedef struct l2_pools_s {
	char name[L2_LISTENERS_MAX][AS_NODE_NAME_SIZE];
	uint32_t in_pool[L2_LISTENERS_MAX][L2_EVENT_LOOPS];
	uint32_t closed[L2_LISTENERS_MAX][L2_EVENT_LOOPS];
	uint32_t size;
} l2_pools;

static void
l2_pools_read(aerospike* as, l2_pools* p)
{
	as_nodes* nodes = as_nodes_reserve(as->cluster);
	p->size = 0;

	for (uint32_t i = 0; i < nodes->size && p->size < L2_LISTENERS_MAX; i++) {
		as_node* node = nodes->array[i];
		uint32_t n = p->size++;
		as_strncpy(p->name[n], node->name, sizeof(p->name[n]));

		for (uint32_t loop = 0; loop < L2_EVENT_LOOPS; loop++) {
			as_async_conn_pool* pool = &node->async_conn_pools[loop];
			p->in_pool[n][loop] = as_queue_size(&pool->queue);
			p->closed[n][loop] = pool->closed;
		}
	}
	as_nodes_release(nodes);
}

static int
l2_pools_find(const l2_pools* p, const char* name)
{
	for (uint32_t i = 0; i < p->size; i++) {
		if (strcmp(p->name[i], name) == 0) {
			return (int)i;
		}
	}
	return -1;
}

static void
l2_async_phase(aerospike* as, l2_batch* b, bool write, uint32_t round)
{
	for (uint32_t loop = 0; loop < L2_EVENT_LOOPS; loop++) {
		for (uint32_t i = 0; i < L2_KEYS; i++) {
			char name[32];
			snprintf(name, sizeof(name), "fa-%u", i);
			l2_async(as, b, write, loop, name, round);
		}
	}
}

#endif

TEST(f1_idle_reset_async, "F-1: idle pooled async connections reset with RST, on each event loop")
{
#if AS_EVENT_LIB_DEFINED
	live_cleanup();
	char msg[L2_LINE_SIZE];
	check(l2_read_nodes(msg, sizeof(msg)), "%s", msg);

	uint32_t node_listener[L2_LISTENERS_MAX];

	for (uint32_t i = 0; i < l2_listeners_size; i++) {
		node_listener[i] = i;
	}

	check(as_event_create_loops(L2_EVENT_LOOPS), "cannot create %u event loops", L2_EVENT_LOOPS);
	live_cleanup_push("event loops", l2_loops_undo, NULL);

	as_config config;
	as_error err;

	if (l2_config(&config, node_listener, &err) != AEROSPIKE_OK) {
		as_config_destroy(&config);
		fail("port-map: %s", err.message);
	}

	uint32_t max_socket_idle = config.max_socket_idle;

	if (max_socket_idle != 0) {
		as_config_destroy(&config);
		fail("max_socket_idle is %u, not the as_config_init() default 0", max_socket_idle);
	}

	if (! l2_add_reset_toxics("fa-rst", msg, sizeof(msg))) {
		as_config_destroy(&config);
		fail("%s", msg);
	}

	aerospike* as = live_connect(&config, &err);
	check(as, "client not created: %d %s", err.code, err.message);

	char names[LIVE_LIST_SIZE];
	l2_all_names(names, sizeof(names));
	check(live_wait_nodes(as, L2_NAMESPACE, names, L2_JOIN_MS, msg, sizeof(msg)), "expected %s: %s", names, msg);

	l2_batch* warm = l2_batch_new();

	for (uint32_t round = 0; round < L2_WARM_ROUNDS && warm->failures == 0; round++) {
		for (uint32_t loop = 0; loop < L2_EVENT_LOOPS; loop++) {
			for (uint32_t i = 0; i < L2_WARM_CONCURRENCY; i++) {
				char name[32];
				snprintf(name, sizeof(name), "fa-%u", (round * L2_WARM_CONCURRENCY + i) % L2_KEYS);
				l2_async(as, warm, true, loop, name, round);
				l2_async(as, warm, false, loop, name, round);
			}
		}
		check(l2_batch_wait(warm, L2_ASYNC_WAIT_MS), "warm-up round %u did not complete within %u ms", round,
			L2_ASYNC_WAIT_MS);
	}
	check(warm->failures == 0, "%u warm-up errors, first %s", warm->failures, warm->failed[0]);
	l2_batch_destroy(warm);
	// Each connection is back in its pool once the loop has finished the callback's command.
	as_sleep(200);

	int probes[L2_LISTENERS_MAX];
	check(l2_probes_open(probes, msg, sizeof(msg)), "probe: %s", msg);

	l2_pools idle;
	l2_pools_read(as, &idle);
	l2_counts idle_counts;
	l2_counts_read(as, &idle_counts);
	char text[L2_LINE_SIZE];
	l2_counts_format(&idle_counts, true, text, sizeof(text));
	uint64_t pooled = 0;
	char empty[128] = "";

	for (uint32_t i = 0; i < l2_listeners_size; i++) {
		int n = l2_pools_find(&idle, l2_listeners[i].node);

		for (uint32_t loop = 0; loop < L2_EVENT_LOOPS; loop++) {
			uint32_t in_pool = n >= 0 ? idle.in_pool[n][loop] : 0;
			pooled += in_pool;

			if (in_pool == 0 && ! empty[0]) {
				snprintf(empty, sizeof(empty), "node %s loop %u", l2_listeners[i].node, loop);
			}
		}
	}
	printf("L2-ASYNC-IDLE eventLib=libev loops=%u maxSocketIdle=%u pooled=%" PRIu64 " %s\n", L2_EVENT_LOOPS,
		max_socket_idle, pooled, text);
	check(! empty[0], "%s has no idle pooled async connection to reset", empty);

	check(l2_cycle_listeners(msg, sizeof(msg)), "%s", msg);
	uint32_t not_reset = l2_probes_read("L2-ASYNC-PROBE", probes, msg, sizeof(msg));
	check(not_reset == 0, "%s", msg);

	l2_pools start;
	l2_pools_read(as, &start);
	l2_counts start_counts;
	l2_counts_read(as, &start_counts);

	// The async pools are LIFO: each phase is issued at once on every loop, so every idle pooled
	// connection is checked out.
	l2_batch* run = l2_batch_new();
	l2_async_phase(as, run, true, 1);
	check(l2_batch_wait(run, L2_ASYNC_WAIT_MS), "puts did not complete within %u ms", L2_ASYNC_WAIT_MS);
	l2_async_phase(as, run, false, 1);
	check(l2_batch_wait(run, L2_ASYNC_WAIT_MS), "gets did not complete within %u ms", L2_ASYNC_WAIT_MS);
	as_sleep(200);

	l2_pools end;
	l2_pools_read(as, &end);
	l2_counts end_counts;
	l2_counts_read(as, &end_counts);
	uint64_t closed = 0;
	char undetected[128] = "";

	for (uint32_t i = 0; i < l2_listeners_size; i++) {
		int s = l2_pools_find(&start, l2_listeners[i].node);
		int e = l2_pools_find(&end, l2_listeners[i].node);

		for (uint32_t loop = 0; loop < L2_EVENT_LOOPS; loop++) {
			int n = l2_pools_find(&idle, l2_listeners[i].node);
			uint32_t delta = s >= 0 && e >= 0 ? end.closed[e][loop] - start.closed[s][loop] : 0;
			closed += delta;
			printf("L2-ASYNC-LOOP node=%s loop=%u pooled=%u closedDelta=%u\n", l2_listeners[i].node, loop,
				n >= 0 ? idle.in_pool[n][loop] : 0, delta);

			if (delta == 0 && ! undetected[0]) {
				snprintf(undetected, sizeof(undetected), "node %s loop %u", l2_listeners[i].node, loop);
			}
		}
	}

	int64_t errors = 0;

	for (uint32_t i = 0; i < l2_listeners_size; i++) {
		const l2_node_counts* s = l2_counts_find(&start_counts, l2_listeners[i].node);
		const l2_node_counts* e = l2_counts_find(&end_counts, l2_listeners[i].node);
		errors += s && e ? (int64_t)(e->errors - s->errors) : 0;
	}

	uint32_t commands = 2 * L2_EVENT_LOOPS * L2_KEYS;
	uint32_t failures = run->failures;
	l2_counts_format(&end_counts, true, text, sizeof(text));
	printf("L2-ASYNC-RESULT eventLib=libev commands=%u failedToCaller=%u pooledReset=%" PRIu64
		" closedDelta=%" PRIu64 " errorDelta=%" PRId64 " retryDelta=%" PRIu64
		" writeMaxRetries=%u readMaxRetries=%u\n", commands, failures, pooled, closed, errors,
		end_counts.retries - start_counts.retries, as->config.policies.write.base.max_retries,
		as->config.policies.read.base.max_retries);
	printf("L2-ASYNC-STATS %s\n", text);

	for (uint32_t i = 0; i < failures && i < L2_FAILED_MAX; i++) {
		printf("L2-ASYNC-FAILED %s\n", run->failed[i]);
	}

	char first[512];
	as_strncpy(first, run->failed[0], sizeof(first));
	l2_batch_destroy(run);
	check(failures == 0, "%u async commands failed to the caller after idle RST, first %s", failures, first);
	check(! undetected[0], "no reset pooled async connection was closed on %s", undetected);
	check(closed >= pooled, "closed %" PRIu64 " pooled async connections, fewer than the %" PRIu64 " reset",
		closed, pooled);
	live_cleanup();
#else
	fail("built without an event library: build with EVENT_LIB=libev");
#endif
}

//---------------------------------
// N-18
//---------------------------------

TEST(n18_two_peers_port_map, "N-18: two peers port-mapped to one listener, neither becomes a node")
{
	live_cleanup();
	char msg[L2_LINE_SIZE];
	check(l2_listeners_size == L2_NODES, "%u listeners in DISCOVERY_PROXY_MAP, not %u", l2_listeners_size,
		L2_NODES);
	check(l2_read_nodes(msg, sizeof(msg)), "%s", msg);

	const char* seed = l2_listeners[0].node;
	const char* a = l2_listeners[1].node;
	const char* b = l2_listeners[2].node;
	uint32_t node_listener[L2_NODES] = {0, 1, 1};
	char address[300];
	snprintf(address, sizeof(address), "%s:%u", l2_endpoint, l2_listeners[1].port);

	as_config config;
	as_error err;

	if (l2_config(&config, node_listener, &err) != AEROSPIKE_OK) {
		as_config_destroy(&config);
		fail("port-map: %s", err.message);
	}

	uint32_t mark = live_log_mark();
	uint64_t begin = live_now();
	aerospike* as = live_connect(&config, &err);
	check(as, "client not created: %d %s", err.code, err.message);

	// Each tend reports each of the two colliding peers once, as the second node of its WARN, so
	// each name order counts the WARNs of one peer.
	uint32_t forward = 0;
	uint32_t reverse = 0;
	uint32_t warns = 0;
	uint32_t samples = 0;
	uint32_t bad = 0;
	char bad_sample[LIVE_LIST_SIZE] = "";

	while (true) {
		char held[LIVE_LIST_SIZE];
		live_nodes(as, held, sizeof(held));
		samples++;

		if (strcmp(held, seed) != 0 && bad++ == 0) {
			as_strncpy(bad_sample, held, sizeof(bad_sample));
		}
		warns = live_collisions(mark, "port-map", a, b, address, &forward, &reverse);

		if ((forward >= L2_COLLISION_WARNS && reverse >= L2_COLLISION_WARNS) ||
			live_now() - begin >= L2_COLLISION_MS) {
			break;
		}
		as_sleep(L2_SAMPLE_MS);
	}

	uint64_t elapsed = live_now() - begin;
	uint64_t deadline = live_now() + L2_JOIN_MS;

	while (live_owned(as, L2_NAMESPACE, seed) == 0 && live_now() < deadline) {
		as_sleep(L2_SAMPLE_MS);
	}

	uint32_t owned = live_owned(as, L2_NAMESPACE, seed);
	char first[1024];
	uint32_t failures = live_round_trip(as, L2_NAMESPACE, "l2n18", "n18", seed, L2_N18_KEYS, first, sizeof(first));

	printf("L2-N18-RESULT translator=port-map address=%s colliding=%s,%s warns=%u forward=%u reverse=%u "
		"elapsedMs=%" PRIu64 " samples=%u badSamples=%u seedOwned=%u keys=%u failures=%u\n", address, a, b,
		warns, forward, reverse, elapsed, samples, bad, owned, L2_N18_KEYS, failures);
	check(bad == 0, "%u of %u samples held other nodes than %s, first %s", bad, samples, seed, bad_sample);
	check(forward >= L2_COLLISION_WARNS && reverse >= L2_COLLISION_WARNS,
		"collision warnings within %u ms: %u for %s, %u for %s, expected %u each", L2_COLLISION_MS, reverse, a,
		forward, b, L2_COLLISION_WARNS);
	check(failures == 0, "%u of %u keys mastered by %s failed, first %s", failures, L2_N18_KEYS, seed, first);
	live_cleanup();
}

//---------------------------------
// Suites
//---------------------------------

static bool
l2_suite_after(atf_suite* suite)
{
	(void)suite;
	live_cleanup();
	return true;
}

static bool
l2_plan_after(atf_plan* plan)
{
	(void)plan;
	live_cleanup();
	live_shutdown();

	if (! live_cleanup_ok()) {
		printf("L2-RUN %s: cleanup did not complete; the topology may have a toxic or a disabled listener left\n",
			l2_profile);
		return false;
	}
	return true;
}

SUITE(l2_f1_suite, "F-1 idle pooled connections reset by the proxy (idle-reset profile)")
{
	suite_after(l2_suite_after);
	suite_add(f1_idle_reset_sync);
	suite_add(f1_idle_reset_async);
}

SUITE(l2_n18_suite, "N-18 two peers on one port-map listener (default profile)")
{
	suite_after(l2_suite_after);
	suite_add(n18_two_peers_port_map);
}

// DISCOVERY_PROXY_MAP: <listener>:<port>, comma separated, in node order.
static void
l2_parse_proxy_map(const char* value)
{
	const char* entry = value;

	while (*entry) {
		size_t len = strcspn(entry, ",");
		const char* colon = NULL;

		for (const char* p = entry; p < entry + len; p++) {
			if (*p == ':') {
				colon = p;
			}
		}

		char* end = NULL;
		long port = colon ? strtol(colon + 1, &end, 10) : 0;

		if (! colon || colon == entry || end != entry + len || port < 1 || port > 65535 ||
			(size_t)(colon - entry) >= sizeof(l2_listeners[0].proxy) || l2_listeners_size == L2_LISTENERS_MAX) {
			printf("L2-RUN: DISCOVERY_PROXY_MAP entry is not <listener>:<port>: %.*s\n", (int)len, entry);
			exit(1);
		}

		l2_listener* l = &l2_listeners[l2_listeners_size++];
		memcpy(l->proxy, entry, (size_t)(colon - entry));
		l->proxy[colon - entry] = '\0';
		l->port = (uint16_t)port;
		entry += len;

		if (*entry == ',') {
			entry++;
		}
	}

	if (l2_listeners_size == 0) {
		printf("L2-RUN: DISCOVERY_PROXY_MAP has no listener\n");
		exit(1);
	}
}

PLAN(l2_run)
{
	live_init("L2");
	l2_profile = live_value("L2_PROFILE");
	bool f1 = strcmp(l2_profile, "idle-reset") == 0;
	bool n18 = strcmp(l2_profile, "default") == 0;

	if (! f1 && ! n18) {
		printf("L2-RUN %s: no client test for this profile\n", l2_profile);
		exit(0);
	}

	l2_endpoint = live_value("DISCOVERY_ENDPOINT_HOSTNAME");
	l2_parse_proxy_map(live_value("DISCOVERY_PROXY_MAP"));
	l2_toxi_host = live_value("TOXIPROXY_HOST");
	l2_toxi_port = live_port_value("TOXIPROXY_CONTROL_PORT");

	if (f1) {
		const char* idle_reset = live_value("DISCOVERY_IDLE_RESET");

		if (strcmp(idle_reset, "1") != 0) {
			printf("L2-RUN: DISCOVERY_IDLE_RESET is %s, not 1\n", idle_reset);
			exit(1);
		}
	}
	printf("L2-RUN %s: client %s, endpoint %s, %u listeners, Toxiproxy %s:%u\n", l2_profile,
		aerospike_client_version, l2_endpoint, l2_listeners_size, l2_toxi_host, l2_toxi_port);
	plan_after(l2_plan_after);

	if (f1) {
		plan_add(l2_f1_suite);
	}
	else {
		plan_add(l2_n18_suite);
	}
}
