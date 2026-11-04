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

typedef struct fnode_s fnode;

struct fnode_s {
	int listen_fd;
	uint16_t port;
	pthread_t thread;
	pthread_mutex_t lock;
	uint8_t stop;
	bool running;
	char name[32];

	// Reply to peers-clear-std. Empty for no peers.
	char peers[256];

	// Accept and immediately close every connection.
	bool reject;

	uint32_t requests;
	uint32_t tends;
	uint32_t rejects;

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

bool
fnode_start(fnode* n, const char* name);

// Stop serving and close every connection. The port can be listened on again.
void
fnode_halt(fnode* n);

void
fnode_stop(fnode* n);

uint32_t
fnode_get(fnode* n, const uint32_t* field);

void
fnode_set_reject(fnode* n, bool reject);

// Set the peers-clear-std reply, for example "1,3000,[[B,,[127.0.0.1:3001]]]".
void
fnode_set_peers(fnode* n, const char* peers);

bool
fnode_wait(fnode* n, const uint32_t* field, uint32_t target, uint32_t timeout_ms);

// Format "127.0.0.1:port".
const char*
fnode_addr(const fnode* n, char* buf, size_t size);

#endif // !_WIN32
