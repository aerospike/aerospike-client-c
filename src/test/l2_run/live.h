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
 * Helpers shared by the tests that run against a live topology of aerospike-client-conformance:
 * l2_run (L2_RUN of l2-proxy) and discovery_env (envs/). Values, the client's WARN and ERROR log,
 * a cleanup registry, and node, partition and key checks. POSIX only.
 */
#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include <aerospike/aerospike.h>
#include <aerospike/as_config.h>
#include <aerospike/as_error.h>
#include <aerospike/as_vector.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Macros
//---------------------------------

#define LIVE_PARTITIONS 4096
#define LIVE_LIST_SIZE 1024

// ATF assertions with a formatted message, for files that include test.h. Like ATF's assert
// macros, they return from the test at once.
#define fail(fmt, ...) \
	return atf_assert_log(__result__, "", __FILE__, __LINE__, fmt, ##__VA_ARGS__)

#define check(EXP, fmt, ...) \
	if (! (EXP)) return atf_assert_log(__result__, #EXP, __FILE__, __LINE__, fmt, ##__VA_ARGS__)

//---------------------------------
// Types
//---------------------------------

// Undo one change. Returns false when the change could not be undone.
typedef bool (*live_undo_fn)(void* ptr);

//---------------------------------
// Functions
//---------------------------------

// Set the prefix of the lines this file prints ("<prefix>-LOG", "<prefix>-CLEANUP") and start
// capturing the client's WARN and ERROR log. Call before any client is created.
void
live_init(const char* prefix);

// Free the captured log. No client may exist any more.
void
live_shutdown(void);

// Milliseconds on a monotonic clock.
uint64_t
live_now(void);

// The value of an environment variable; prints its name and exits 1 when it is unset or empty.
const char*
live_value(const char* name);

// A port from the environment; prints its name and exits 1 unless it is 1 to 65535.
uint16_t
live_port_value(const char* name);

// Index of the next captured log line, to count lines from.
uint32_t
live_log_mark(void);

// Captured lines from index from on that contain every needle.
uint32_t
live_log_count(uint32_t from, const char* const* needles, uint32_t size);

// Collision WARNs from index from on, in the exact form the client logs them, naming translator,
// nodes a and b in either order, and address ("host:port"). forward counts "nodes a and b",
// reverse "nodes b and a". Returns their sum.
uint32_t
live_collisions(uint32_t from, const char* translator, const char* a, const char* b,
	const char* address, uint32_t* forward, uint32_t* reverse);

// Register an undo under key; live_cleanup() runs it. ptr is freed with free() after the undo
// runs or the entry is dropped.
void
live_cleanup_push(const char* key, live_undo_fn undo, void* ptr);

// Forget the entry under key without running it, once its change has been undone.
void
live_cleanup_drop(const char* key);

// Run every registered undo, the most recent first, and forget them. Idempotent. A failed undo
// is printed and fails the run (live_cleanup_ok()).
void
live_cleanup(void);

// In a forked child: forget the parent's entries and failures without running or freeing anything,
// since they belong to the parent.
void
live_cleanup_forget(void);

// False once an undo failed.
bool
live_cleanup_ok(void);

// aerospike_init() and aerospike_connect(), which take over config. On success the client is
// registered for cleanup, which closes and destroys it; otherwise NULL with the reason in err.
aerospike*
live_connect(as_config* config, as_error* err);

// Sort items and join them with ',' into buf.
void
live_sort_join(char** items, uint32_t size, char* buf, size_t buf_size);

// Hosts (as_host elements) as "name:port", sorted, joined with ','.
void
live_hosts_join(const as_vector* hosts, char* buf, size_t size);

// Names of the nodes the client holds, sorted, joined with ','. Returns their number.
uint32_t
live_nodes(aerospike* as, char* buf, size_t size);

// Partitions of ns whose master is an active node the client holds and, when name is not NULL,
// is named name.
uint32_t
live_owned(aerospike* as, const char* ns, const char* name);

// Wait at most bound_ms until the client holds exactly the nodes names (sorted, joined with ',')
// and they own every partition of ns. last receives the last nodes and owned count seen.
bool
live_wait_nodes(aerospike* as, const char* ns, const char* names, uint32_t bound_ms, char* last,
	size_t size);

// Put and get count keys "<prefix>-<i>" of ns and set, each with value i, taking only keys whose
// partition master is the node named master when master is not NULL. Returns the failures and
// the first of them in first.
uint32_t
live_round_trip(aerospike* as, const char* ns, const char* set, const char* prefix,
	const char* master, uint32_t count, char* first, size_t size);

#ifdef __cplusplus
} // end extern "C"
#endif
