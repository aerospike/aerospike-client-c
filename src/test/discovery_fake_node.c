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
#include "discovery_fake_node.h"

#ifdef DISCOVERY_FAKE_SERVERS

#include <aerospike/aerospike.h>
#include <aerospike/as_atomic.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_node.h>
#include <aerospike/as_sleep.h>
#include <citrusleaf/cf_clock.h>

#include <arpa/inet.h>
#include <dirent.h>
#include <netinet/in.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#define FNODE_MAX_CLIENTS 32

extern aerospike* as;

//---------------------------------
// Static Functions
//---------------------------------

static bool
same_endpoint(const struct sockaddr_storage* a, const struct sockaddr_storage* b)
{
	if (a->ss_family != b->ss_family) {
		return false;
	}

	if (a->ss_family == AF_INET) {
		const struct sockaddr_in* x = (const struct sockaddr_in*)a;
		const struct sockaddr_in* y = (const struct sockaddr_in*)b;
		return x->sin_port == y->sin_port && x->sin_addr.s_addr == y->sin_addr.s_addr;
	}

	if (a->ss_family == AF_INET6) {
		const struct sockaddr_in6* x = (const struct sockaddr_in6*)a;
		const struct sockaddr_in6* y = (const struct sockaddr_in6*)b;
		return x->sin6_port == y->sin6_port &&
			memcmp(&x->sin6_addr, &y->sin6_addr, sizeof(x->sin6_addr)) == 0;
	}
	return false;
}

// Is fd connected to a node of the shared test client.
static bool
is_client_socket(int fd, as_nodes* nodes)
{
	if (!nodes) {
		return false;
	}

	struct sockaddr_storage peer;
	socklen_t len = sizeof(peer);

	if (getpeername(fd, (struct sockaddr*)&peer, &len) != 0) {
		return false;
	}

	for (uint32_t i = 0; i < nodes->size; i++) {
		if (same_endpoint(&as_node_get_address(nodes->array[i])->addr, &peer)) {
			return true;
		}
	}
	return false;
}

static size_t
fnode_reply(fnode* n, char* body, char* out, size_t cap)
{
	size_t len = 0;
	char* save = NULL;

	for (char* line = strtok_r(body, "\n", &save); line; line = strtok_r(NULL, "\n", &save)) {
		const char* value = "";

		if (strcmp(line, "node") == 0) {
			value = n->name;
		}
		else if (strcmp(line, "partition-generation") == 0) {
			value = "1";
		}
		else if (strcmp(line, "build") == 0) {
			value = "8.1.0.0";
		}
		else if (strcmp(line, "peers-generation") == 0) {
			value = "1";
			n->tends++;
		}
		else if (strcmp(line, "peers-clear-std") == 0) {
			value = n->peers[0] ? n->peers : "1,3000,[]";
		}
		else if (strcmp(line, "rebalance-generation") == 0) {
			value = "0";
		}
		else if (strncmp(line, "user-agent-set", 14) == 0) {
			value = "ok";
		}

		int w = snprintf(out + len, cap - len, "%s\t%s\n", line, value);

		if (w < 0 || (size_t)w >= cap - len) {
			break;
		}
		len += (size_t)w;
	}
	return len;
}

static bool
fnode_serve(fnode* n, int fd)
{
	uint8_t header[8];

	if (recv(fd, header, sizeof(header), MSG_WAITALL) != (ssize_t)sizeof(header) ||
		header[0] != 2 || header[1] != 1) {
		return false;
	}

	size_t len = 0;

	for (int i = 2; i < 8; i++) {
		len = (len << 8) | header[i];
	}

	char body[4096];

	if (len >= sizeof(body) || (len > 0 && recv(fd, body, len, MSG_WAITALL) != (ssize_t)len)) {
		return false;
	}
	body[len] = 0;

	pthread_mutex_lock(&n->lock);
	bool first = !n->seen;
	n->seen = true;
	pthread_mutex_unlock(&n->lock);

	if (first && n->on_first) {
		n->on_first(n);
	}

	uint8_t reply[8192];
	pthread_mutex_lock(&n->lock);
	n->requests++;
	size_t reply_len = fnode_reply(n, body, (char*)reply + 8, sizeof(reply) - 8);
	pthread_mutex_unlock(&n->lock);

	reply[0] = 2;
	reply[1] = 1;

	for (int i = 0; i < 6; i++) {
		reply[2 + i] = (uint8_t)(reply_len >> (8 * (5 - i)));
	}
	return send(fd, reply, reply_len + 8, MSG_NOSIGNAL) == (ssize_t)(reply_len + 8);
}

static void*
fnode_run(void* arg)
{
	fnode* n = arg;
	int clients[FNODE_MAX_CLIENTS];
	int n_clients = 0;

	while (!as_load_uint8(&n->stop)) {
		pthread_mutex_lock(&n->lock);
		bool rejecting = n->reject;
		pthread_mutex_unlock(&n->lock);

		// A rejecting node also drops established connections, including the tend connection.
		if (rejecting) {
			for (int i = 0; i < n_clients; i++) {
				close(clients[i]);
			}
			n_clients = 0;
		}

		struct pollfd pfds[FNODE_MAX_CLIENTS + 1];
		pfds[0].fd = n->listen_fd;
		pfds[0].events = POLLIN;
		pfds[0].revents = 0;

		for (int i = 0; i < n_clients; i++) {
			pfds[i + 1].fd = clients[i];
			pfds[i + 1].events = POLLIN;
			pfds[i + 1].revents = 0;
		}

		if (poll(pfds, (nfds_t)(n_clients + 1), 20) <= 0) {
			continue;
		}

		for (int i = n_clients - 1; i >= 0; i--) {
			if (pfds[i + 1].revents && !fnode_serve(n, clients[i])) {
				close(clients[i]);
				clients[i] = clients[--n_clients];
			}
		}

		if ((pfds[0].revents & POLLIN) && n_clients < FNODE_MAX_CLIENTS) {
			int fd = accept(n->listen_fd, NULL, NULL);

			if (fd < 0) {
				continue;
			}

			pthread_mutex_lock(&n->lock);
			bool reject = n->reject;

			if (reject) {
				n->rejects++;
			}
			pthread_mutex_unlock(&n->lock);

			if (reject) {
				close(fd);
				continue;
			}

			struct timeval tv = {2, 0};
			setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
			clients[n_clients++] = fd;
		}
	}

	for (int i = 0; i < n_clients; i++) {
		close(clients[i]);
	}
	return NULL;
}

//---------------------------------
// Functions
//---------------------------------

void
fnode_init(fnode* n, const char* name)
{
	memset(n, 0, sizeof(fnode));
	n->listen_fd = -1;
	pthread_mutex_init(&n->lock, NULL);
	snprintf(n->name, sizeof(n->name), "%s", name);
}

bool
fnode_listen(fnode* n, uint16_t port)
{
	n->listen_fd = socket(AF_INET, SOCK_STREAM, 0);

	if (n->listen_fd < 0) {
		return false;
	}

	int one = 1;
	setsockopt(n->listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

	struct sockaddr_in addr;
	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	addr.sin_addr.s_addr = inet_addr("127.0.0.1");
	socklen_t addr_len = sizeof(addr);

	if (bind(n->listen_fd, (struct sockaddr*)&addr, sizeof(addr)) != 0 ||
		listen(n->listen_fd, 16) != 0 ||
		getsockname(n->listen_fd, (struct sockaddr*)&addr, &addr_len) != 0) {
		close(n->listen_fd);
		n->listen_fd = -1;
		return false;
	}

	n->port = ntohs(addr.sin_port);
	as_store_uint8(&n->stop, 0);
	n->running = pthread_create(&n->thread, NULL, fnode_run, n) == 0;
	return n->running;
}

bool
fnode_start(fnode* n, const char* name)
{
	fnode_init(n, name);
	return fnode_listen(n, 0);
}

void
fnode_halt(fnode* n)
{
	if (n->running) {
		as_store_uint8(&n->stop, 1);
		pthread_join(n->thread, NULL);
		n->running = false;
	}

	if (n->listen_fd >= 0) {
		close(n->listen_fd);
		n->listen_fd = -1;
	}
}

void
fnode_stop(fnode* n)
{
	fnode_halt(n);
	pthread_mutex_destroy(&n->lock);
}

uint32_t
fnode_get(fnode* n, const uint32_t* field)
{
	pthread_mutex_lock(&n->lock);
	uint32_t value = *field;
	pthread_mutex_unlock(&n->lock);
	return value;
}

void
fnode_set_reject(fnode* n, bool reject)
{
	pthread_mutex_lock(&n->lock);
	n->reject = reject;
	pthread_mutex_unlock(&n->lock);
}

void
fnode_set_peers(fnode* n, const char* peers)
{
	pthread_mutex_lock(&n->lock);
	snprintf(n->peers, sizeof(n->peers), "%s", peers);
	pthread_mutex_unlock(&n->lock);
}

bool
fnode_wait(fnode* n, const uint32_t* field, uint32_t target, uint32_t timeout_ms)
{
	uint64_t end = cf_getms() + timeout_ms;

	while (fnode_get(n, field) < target) {
		if (cf_getms() >= end) {
			return false;
		}
		as_sleep(10);
	}
	return true;
}

const char*
fnode_addr(const fnode* n, char* buf, size_t size)
{
	snprintf(buf, size, "127.0.0.1:%u", n->port);
	return buf;
}

int
discovery_fd_count(void)
{
#if defined(__linux__)
	DIR* dir = opendir("/proc/self/fd");
#else
	DIR* dir = opendir("/dev/fd");
#endif

	if (!dir) {
		return -1;
	}

	as_nodes* nodes = (as && as->cluster) ? as_nodes_reserve(as->cluster) : NULL;
	int count = 0;
	struct dirent* entry;

	while ((entry = readdir(dir)) != NULL) {
		if (entry->d_name[0] != '.' && !is_client_socket(atoi(entry->d_name), nodes)) {
			count++;
		}
	}

	if (nodes) {
		as_nodes_release(nodes);
	}
	closedir(dir);
	return count;
}

#endif // DISCOVERY_FAKE_SERVERS
