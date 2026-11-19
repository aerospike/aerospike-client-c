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
#pragma once

// Restartable loopback fake info server shared by the discovery tests.

#if !defined(_WIN32)
#define DISCOVERY_FAKE_SERVERS 1

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

// Declare a fake server, zeroed, that stop() releases when the scope ends. A failed assertion
// returns from the test early, which would otherwise leave the server thread running on the freed
// stack frame. stop() must do nothing for a server that is zeroed or already stopped.
#define DISCOVERY_SCOPED(type, name, stop) type name __attribute__((cleanup(stop))) = {0}

typedef struct fnode_s fnode;

struct as_cluster_s;

#define FNODE_MAX_FILLERS 8

struct fnode_s {
	int listen_fd;
	uint16_t port;
	pthread_t thread;
	pthread_mutex_t lock;
	uint8_t stop;
	bool started;
	bool running;
	char name[32];

	// IPv4 address the node listens on.
	char host[32];

	// Reply to peers-clear-std and peers-clear-alt. Empty for no peers. service-clear-alt
	// answers this node's own address.
	char peers[256];

	// Accept and immediately close every connection.
	bool reject;

	// Claim every partition of namespace test, so the client keeps the node even when no peer
	// lists it.
	bool partitions;

	// Listen with a full accept backlog and never accept, so a further connect hangs.
	bool silent;
	int fillers[FNODE_MAX_FILLERS];
	int n_fillers;

	// Accept every connection and never read from it or reply. The TCP connect completes.
	bool mute;

	// Answer service-clear-alt with an empty reply.
	bool empty_alt;

	// Reply to peers-generation.
	uint32_t peers_gen;

	uint32_t requests;
	uint32_t tends;
	uint32_t rejects;
	uint32_t accepts;

	// Admin logins, answered with AEROSPIKE_SECURITY_NOT_ENABLED, which the client takes as a
	// successful login.
	uint32_t logins;

	// cf_getus() at the first accept.
	uint64_t first_accept_us;

	// Called by the server thread before the first request it answers.
	void (*on_first)(fnode* n);
	void* udata;
	bool seen;
};

void
fnode_init(fnode* n, const char* name);

// Listen on 127.0.0.1:port, or an ephemeral port when port is zero.
bool
fnode_listen(fnode* n, uint16_t port);

// Listen on the IPv4 address host and port, or an ephemeral port when port is zero.
bool
fnode_listen_on(fnode* n, const char* host, uint16_t port);

bool
fnode_start(fnode* n, const char* name);

// Stop serving and close every connection. The port can be listened on again.
void
fnode_halt(fnode* n);

// Does nothing for a node that is zeroed or already stopped.
void
fnode_stop(fnode* n);

uint32_t
fnode_get(fnode* n, const uint32_t* field);

void
fnode_set_reject(fnode* n, bool reject);

// Set the peers-clear-std reply, for example "1,3000,[[B,,[127.0.0.1:3001]]]".
void
fnode_set_peers(fnode* n, const char* peers);

void
fnode_set_partitions(fnode* n, bool partitions);

// Stop or resume accepting. A silent node fills its accept backlog, so the kernel drops further
// SYNs and a connect to it hangs until its deadline (Linux). Return false when silent and no
// connect hangs.
bool
fnode_set_silent(fnode* n, bool silent);

void
fnode_set_mute(fnode* n, bool mute);

void
fnode_set_empty_alt(fnode* n, bool empty_alt);

// Increment the peers-generation reply, so a client refreshes the peers of n.
void
fnode_bump_peers_generation(fnode* n);

uint64_t
fnode_first_accept_us(fnode* n);

bool
fnode_wait(fnode* n, const uint32_t* field, uint32_t target, uint32_t timeout_ms);

// Format "host:port".
const char*
fnode_addr(const fnode* n, char* buf, size_t size);

// Return false and copy the name to dup, when not NULL, if two nodes of the cluster have one
// name.
bool
discovery_one_node_per_name(struct as_cluster_s* cluster, char* dup, size_t size);

// Install the resolver of the discovery_nodes tests, where unregistered names resolve through
// the system. as_lookup_set_resolver() must not run while a lookup runs, so this is called once,
// before the shared test client connects, and never removed.
void
discovery_nodes_resolver_install(void);

// Number of open descriptors of this process, or -1 when unknown. Sockets connected to a node of
// the shared test client are not counted: its tend thread trims idle ones at any time.
int
discovery_fd_count(void);

#endif // !_WIN32
