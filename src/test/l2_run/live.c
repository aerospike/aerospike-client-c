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
#include "l2_run/live.h"

#include <inttypes.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <aerospike/aerospike_key.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_host.h>
#include <aerospike/as_key.h>
#include <aerospike/as_log.h>
#include <aerospike/as_node.h>
#include <aerospike/as_partition.h>
#include <aerospike/as_record.h>
#include <aerospike/as_sleep.h>

//---------------------------------
// Macros
//---------------------------------

#define LIVE_LOG_SIZE 4096
#define LIVE_CLEANUP_MAX 64
#define LIVE_POLL_MS 100
#define LIVE_KEY_SEARCH_LIMIT 1000000

//---------------------------------
// Types
//---------------------------------

typedef struct live_entry_s {
	char key[192];
	live_undo_fn undo;
	void* ptr;
} live_entry;

void
aerospike_destroy_internal(aerospike* as);

//---------------------------------
// Globals
//---------------------------------

static const char* live_prefix = "LIVE";

static pthread_mutex_t live_log_lock = PTHREAD_MUTEX_INITIALIZER;
static char** live_log_lines;
static uint32_t live_log_size;
static uint32_t live_log_capacity;
static bool live_log_stopped;

static live_entry live_entries[LIVE_CLEANUP_MAX];
static uint32_t live_entries_size;
static bool live_cleanup_failed;

//---------------------------------
// Log
//---------------------------------

static bool
live_log_callback(as_log_level level, const char* func, const char* file, uint32_t line,
	const char* fmt, ...)
{
	(void)func;
	(void)file;
	(void)line;

	if (level > AS_LOG_LEVEL_WARN) {
		return true;
	}

	char msg[LIVE_LOG_SIZE];
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(msg, sizeof(msg), fmt, ap);
	va_end(ap);

	pthread_mutex_lock(&live_log_lock);

	if (! live_log_stopped) {
		if (live_log_size == live_log_capacity) {
			uint32_t capacity = live_log_capacity ? live_log_capacity * 2 : 256;
			char** lines = realloc(live_log_lines, sizeof(char*) * capacity);

			if (! lines) {
				fprintf(stderr, "%s: out of memory\n", live_prefix);
				exit(1);
			}
			live_log_lines = lines;
			live_log_capacity = capacity;
		}

		char* copy = strdup(msg);

		if (! copy) {
			fprintf(stderr, "%s: out of memory\n", live_prefix);
			exit(1);
		}
		live_log_lines[live_log_size++] = copy;
		printf("%s-LOG %s %s\n", live_prefix, level == AS_LOG_LEVEL_ERROR ? "ERROR" : "WARN", msg);
	}
	pthread_mutex_unlock(&live_log_lock);
	return true;
}

void
live_init(const char* prefix)
{
	live_prefix = prefix;
	as_log_set_level(AS_LOG_LEVEL_WARN);
	as_log_set_callback(live_log_callback);
}

void
live_shutdown(void)
{
	pthread_mutex_lock(&live_log_lock);
	live_log_stopped = true;

	for (uint32_t i = 0; i < live_log_size; i++) {
		free(live_log_lines[i]);
	}
	free(live_log_lines);
	live_log_lines = NULL;
	live_log_size = 0;
	live_log_capacity = 0;
	pthread_mutex_unlock(&live_log_lock);
}

uint32_t
live_log_mark(void)
{
	pthread_mutex_lock(&live_log_lock);
	uint32_t mark = live_log_size;
	pthread_mutex_unlock(&live_log_lock);
	return mark;
}

uint32_t
live_log_count(uint32_t from, const char* const* needles, uint32_t size)
{
	uint32_t count = 0;
	pthread_mutex_lock(&live_log_lock);

	for (uint32_t i = from; i < live_log_size; i++) {
		uint32_t n = 0;

		while (n < size && strstr(live_log_lines[i], needles[n])) {
			n++;
		}

		if (n == size) {
			count++;
		}
	}
	pthread_mutex_unlock(&live_log_lock);
	return count;
}

uint32_t
live_collisions(uint32_t from, const char* translator, const char* a, const char* b,
	const char* address, uint32_t* forward, uint32_t* reverse)
{
	// The address ends at the period, so a port cannot match a longer one.
	char f[512];
	char r[512];
	snprintf(f, sizeof(f), "Address translator %s translated nodes %s and %s to the same address %s.",
		translator, a, b, address);
	snprintf(r, sizeof(r), "Address translator %s translated nodes %s and %s to the same address %s.",
		translator, b, a, address);

	const char* fn[] = {f};
	const char* rn[] = {r};
	*forward = live_log_count(from, fn, 1);
	*reverse = live_log_count(from, rn, 1);
	return *forward + *reverse;
}

//---------------------------------
// Values
//---------------------------------

uint64_t
live_now(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

const char*
live_value(const char* name)
{
	const char* value = getenv(name);

	if (! value || ! value[0]) {
		printf("%s: %s is not set; load the environment's values file\n", live_prefix, name);
		exit(1);
	}
	return value;
}

uint16_t
live_port_value(const char* name)
{
	const char* value = live_value(name);
	char* end = NULL;
	long port = strtol(value, &end, 10);

	if (*end != '\0' || port < 1 || port > 65535) {
		printf("%s: %s is not a port: %s\n", live_prefix, name, value);
		exit(1);
	}
	return (uint16_t)port;
}

//---------------------------------
// Cleanup
//---------------------------------

void
live_cleanup_push(const char* key, live_undo_fn undo, void* ptr)
{
	if (live_entries_size == LIVE_CLEANUP_MAX) {
		printf("%s-CLEANUP-FAILED registry full at %s\n", live_prefix, key);
		live_cleanup_failed = true;
		undo(ptr);
		free(ptr);
		return;
	}

	live_entry* e = &live_entries[live_entries_size++];
	as_strncpy(e->key, key, sizeof(e->key));
	e->undo = undo;
	e->ptr = ptr;
}

void
live_cleanup_drop(const char* key)
{
	for (uint32_t i = live_entries_size; i > 0; i--) {
		live_entry* e = &live_entries[i - 1];

		if (strcmp(e->key, key) == 0) {
			free(e->ptr);
			memmove(e, e + 1, sizeof(live_entry) * (live_entries_size - i));
			live_entries_size--;
			return;
		}
	}
}

void
live_cleanup(void)
{
	while (live_entries_size > 0) {
		live_entry e = live_entries[--live_entries_size];
		printf("%s-CLEANUP %s\n", live_prefix, e.key);

		if (! e.undo(e.ptr)) {
			printf("%s-CLEANUP-FAILED %s\n", live_prefix, e.key);
			live_cleanup_failed = true;
		}
		free(e.ptr);
	}
}

void
live_cleanup_forget(void)
{
	live_entries_size = 0;
	live_cleanup_failed = false;
}

bool
live_cleanup_ok(void)
{
	return ! live_cleanup_failed;
}

static bool
live_client_undo(void* ptr)
{
	aerospike* as = ptr;
	as_error err;
	bool ok = aerospike_close(as, &err) == AEROSPIKE_OK;

	if (! ok) {
		printf("%s-CLEANUP client close: %d %s\n", live_prefix, err.code, err.message);
	}
	aerospike_destroy(as);
	return ok;
}

aerospike*
live_connect(as_config* config, as_error* err)
{
	aerospike* as = malloc(sizeof(aerospike));

	if (! as) {
		as_config_destroy(config);
		as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "out of memory");
		return NULL;
	}
	aerospike_init(as, config);

	if (aerospike_connect(as, err) != AEROSPIKE_OK) {
		// Not aerospike_destroy(): with event loops it leaves the destroy to the cluster's close,
		// and a client that did not connect has no cluster.
		aerospike_destroy_internal(as);
		free(as);
		return NULL;
	}
	live_cleanup_push("client", live_client_undo, as);
	return as;
}

//---------------------------------
// Nodes and partitions
//---------------------------------

static int
live_compare_strings(const void* a, const void* b)
{
	return strcmp(*(char* const*)a, *(char* const*)b);
}

void
live_sort_join(char** items, uint32_t size, char* buf, size_t buf_size)
{
	qsort(items, size, sizeof(char*), live_compare_strings);
	size_t len = 0;
	buf[0] = '\0';

	for (uint32_t i = 0; i < size && len < buf_size; i++) {
		len += (size_t)snprintf(buf + len, buf_size - len, "%s%s", i ? "," : "", items[i]);
	}
}

void
live_hosts_join(const as_vector* hosts, char* buf, size_t size)
{
	uint32_t n = hosts->size < LIVE_LIST_SIZE ? hosts->size : LIVE_LIST_SIZE;
	char (*text)[300] = malloc(sizeof(*text) * (n ? n : 1));
	char** items = malloc(sizeof(char*) * (n ? n : 1));

	if (! text || ! items) {
		fprintf(stderr, "%s: out of memory\n", live_prefix);
		exit(1);
	}

	for (uint32_t i = 0; i < n; i++) {
		as_host* host = as_vector_get((as_vector*)hosts, i);
		snprintf(text[i], sizeof(text[i]), "%s:%u", host->name, host->port);
		items[i] = text[i];
	}
	live_sort_join(items, n, buf, size);
	free(items);
	free(text);
}

uint32_t
live_nodes(aerospike* as, char* buf, size_t size)
{
	as_nodes* nodes = as_nodes_reserve(as->cluster);
	uint32_t n = nodes->size < LIVE_LIST_SIZE ? nodes->size : LIVE_LIST_SIZE;
	char (*names)[AS_NODE_NAME_SIZE] = malloc(sizeof(*names) * (n ? n : 1));
	char** items = malloc(sizeof(char*) * (n ? n : 1));

	if (! names || ! items) {
		fprintf(stderr, "%s: out of memory\n", live_prefix);
		exit(1);
	}

	for (uint32_t i = 0; i < n; i++) {
		as_strncpy(names[i], nodes->array[i]->name, sizeof(names[i]));
		items[i] = names[i];
	}
	as_nodes_release(nodes);
	live_sort_join(items, n, buf, size);
	free(items);
	free(names);
	return n;
}

// The master of partition p when it is an active node of nodes, otherwise NULL. The node is only
// dereferenced once it is found among the reserved nodes.
static as_node*
live_master(as_partition_table* table, uint32_t p, as_nodes* nodes)
{
	as_node* master = as_node_load(&table->partitions[p].nodes[0]);

	for (uint32_t i = 0; i < nodes->size; i++) {
		if (nodes->array[i] == master) {
			return as_node_is_active(master) ? master : NULL;
		}
	}
	return NULL;
}

uint32_t
live_owned(aerospike* as, const char* ns, const char* name)
{
	as_cluster* cluster = as->cluster;
	as_partition_table* table = as_partition_tables_get(&cluster->partition_tables, ns);

	if (! table) {
		return 0;
	}

	as_nodes* nodes = as_nodes_reserve(cluster);
	uint32_t owned = 0;

	for (uint32_t p = 0; p < table->size; p++) {
		as_node* master = live_master(table, p, nodes);

		if (master && (! name || strcmp(master->name, name) == 0)) {
			owned++;
		}
	}
	as_nodes_release(nodes);
	return owned;
}

bool
live_wait_nodes(aerospike* as, const char* ns, const char* names, uint32_t bound_ms, char* last,
	size_t size)
{
	uint64_t deadline = live_now() + bound_ms;

	while (true) {
		char held[LIVE_LIST_SIZE];
		live_nodes(as, held, sizeof(held));
		uint32_t owned = live_owned(as, ns, NULL);
		snprintf(last, size, "nodes=%s owned=%u", held, owned);

		if (strcmp(held, names) == 0 && owned == LIVE_PARTITIONS) {
			return true;
		}

		if (live_now() >= deadline) {
			return false;
		}
		as_sleep(LIVE_POLL_MS);
	}
}

// Whether the partition of key has the node named name as its active master.
static bool
live_key_mastered_by(aerospike* as, as_key* key, const char* ns, const char* name)
{
	as_cluster* cluster = as->cluster;
	as_partition_table* table = as_partition_tables_get(&cluster->partition_tables, ns);

	if (! table || cluster->n_partitions == 0) {
		return false;
	}

	uint32_t p = as_partition_getid(as_key_digest(key)->value, cluster->n_partitions);
	as_nodes* nodes = as_nodes_reserve(cluster);
	as_node* master = p < table->size ? live_master(table, p, nodes) : NULL;
	bool mastered = master && strcmp(master->name, name) == 0;
	as_nodes_release(nodes);
	return mastered;
}

static void
live_first(char* first, size_t size, const char* fmt, ...)
{
	if (first[0]) {
		return;
	}

	va_list ap;
	va_start(ap, fmt);
	vsnprintf(first, size, fmt, ap);
	va_end(ap);
}

uint32_t
live_round_trip(aerospike* as, const char* ns, const char* set, const char* prefix,
	const char* master, uint32_t count, char* first, size_t size)
{
	uint32_t failures = 0;
	uint32_t done = 0;
	first[0] = '\0';

	for (uint32_t i = 0; i < LIVE_KEY_SEARCH_LIMIT && done < count; i++) {
		char name[128];
		snprintf(name, sizeof(name), "%s-%u", prefix, i);
		as_key key;
		as_key_init_str(&key, ns, set, name);

		if (master && ! live_key_mastered_by(as, &key, ns, master)) {
			as_key_destroy(&key);
			continue;
		}
		done++;

		as_error err;
		as_record rec;
		as_record_init(&rec, 1);
		as_record_set_int64(&rec, "v", (int64_t)i);
		as_status status = aerospike_key_put(as, &err, NULL, &key, &rec);
		as_record_destroy(&rec);

		if (status != AEROSPIKE_OK) {
			failures++;
			live_first(first, size, "put %s: %d %s", name, err.code, err.message);
			as_key_destroy(&key);
			continue;
		}

		as_record* got = NULL;
		status = aerospike_key_get(as, &err, NULL, &key, &got);

		if (status != AEROSPIKE_OK) {
			failures++;
			live_first(first, size, "get %s: %d %s", name, err.code, err.message);
		}
		else if (as_record_get_int64(got, "v", -1) != (int64_t)i) {
			failures++;
			live_first(first, size, "get %s: v is %" PRId64 ", not %u", name,
				as_record_get_int64(got, "v", -1), i);
		}

		if (got) {
			as_record_destroy(got);
		}
		as_key_destroy(&key);
	}

	if (done < count) {
		failures += count - done;
		live_first(first, size, "only %u of %u keys found with master %s", done, count,
			master ? master : "any");
	}
	return failures;
}
