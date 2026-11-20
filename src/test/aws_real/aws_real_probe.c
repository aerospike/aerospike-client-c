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
 * The C probe for the aws-tags real AWS harness in aerospike-client-conformance (aws-real/).
 * Runs the built-in AWS EC2 seed provider against real AWS in one of three modes and prints one
 * JSON object on one line of stdout, as that harness's README describes:
 * - latency: a cold first refresh, credentials included, then AWS_REAL_WARM warm refreshes;
 * - denied: one refresh with the credentials in the environment;
 * - join: a client seeded only by the provider joins the tagged nodes, then AWS_REAL_REFRESHES
 *   refreshes are counted.
 * Environment: AWS_REAL_REGION, AWS_REAL_TAG (key=value), and optionally AWS_REAL_WARM (default
 * 20), AWS_REAL_EXPECTED_NODES (required by join), AWS_REAL_NAMESPACE (default test) and
 * AWS_REAL_REFRESHES (default 30). An empty variable counts as unset.
 *
 * Exits with 2, printing nothing on stdout, on bad usage or a missing or invalid variable;
 * otherwise exits with 0 once the JSON object is printed, also when it carries an error.
 * Built against installed headers only by "make aws-real-probe"; Linux only.
 */
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <aerospike/aerospike.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_config.h>
#include <aerospike/as_discovery.h>
#include <aerospike/as_error.h>
#include <aerospike/as_host.h>
#include <aerospike/as_log.h>
#include <aerospike/as_node.h>
#include <aerospike/as_partition.h>
#include <aerospike/as_sleep.h>
#include <aerospike/as_vector.h>
#include <citrusleaf/cf_clock.h>

//---------------------------------
// Macros
//---------------------------------

#define AWS_PREFIX "AWS EC2 seed provider "
#define CREDENTIALS_PREFIX AWS_PREFIX "credentials from "
#define REFRESH_FAILED "Seed provider aws-tags refresh failed"
#define OVERRUN "exceeding refresh_timeout_ms"
#define TRIPPED "exceeded tend_refresh_deadline_ms"
#define JOIN_WAIT_MS 60000
#define POLL_MS 100

//---------------------------------
// Types
//---------------------------------

typedef struct probe_env_s {
	const char* region;
	char* filter_name;
	const char* filter_value;
} probe_env;

typedef struct json_s {
	char* data;
	size_t len;
	size_t capacity;
	bool first_field;
	bool first_element;
} json;

typedef struct cluster_counts_s {
	uint32_t nodes;
	int64_t owned_partitions;
	uint32_t partitions;
} cluster_counts;

//---------------------------------
// Globals
//---------------------------------

static pthread_mutex_t log_lock = PTHREAD_MUTEX_INITIALIZER;
static char** log_messages;
static uint32_t log_size;
static uint32_t log_capacity;
static bool log_closed;
static bool log_join_filter;

//---------------------------------
// Static Functions: usage and environment
//---------------------------------

static void
fail_usage(const char* fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	vfprintf(stderr, fmt, ap);
	va_end(ap);
	fputc('\n', stderr);
	exit(2);
}

static void
fail_memory(void)
{
	fprintf(stderr, "aws_real_probe: out of memory\n");
	exit(1);
}

static const char*
env_value(const char* name)
{
	const char* value = getenv(name);
	return (value && value[0]) ? value : NULL;
}

static const char*
env_required(const char* name)
{
	const char* value = env_value(name);

	if (!value) {
		fail_usage("%s is not set", name);
	}
	return value;
}

// Plain decimal, greater than zero, fits a uint32_t.
static uint32_t
env_count(const char* name, const char* default_value)
{
	const char* value = default_value ? env_value(name) : env_required(name);

	if (!value) {
		value = default_value;
	}

	for (const char* p = value; *p; p++) {
		if (*p < '0' || *p > '9') {
			fail_usage("%s must be a positive integer: %s", name, value);
		}
	}

	char* end = NULL;
	errno = 0;
	unsigned long n = strtoul(value, &end, 10);

	if (errno != 0 || end == value || *end != '\0' || n == 0 || n > UINT32_MAX) {
		fail_usage("%s must be a positive integer: %s", name, value);
	}
	return (uint32_t)n;
}

static void
env_load(probe_env* env)
{
	const char* tag = env_required("AWS_REAL_TAG");
	const char* eq = strchr(tag, '=');

	if (!eq || eq == tag || eq[1] == '\0') {
		fail_usage("AWS_REAL_TAG must be key=value");
	}

	size_t key_len = (size_t)(eq - tag);
	env->filter_name = malloc(sizeof("tag:") + key_len);

	if (!env->filter_name) {
		fail_memory();
	}
	memcpy(env->filter_name, "tag:", 4);
	memcpy(env->filter_name + 4, tag, key_len);
	env->filter_name[4 + key_len] = '\0';
	env->filter_value = eq + 1;
	env->region = env_required("AWS_REAL_REGION");
}

//---------------------------------
// Static Functions: log capture
//---------------------------------

static bool
starts_with(const char* s, const char* prefix)
{
	return strncmp(s, prefix, strlen(prefix)) == 0;
}

// Takes ownership of msg.
static void
log_append(char* msg)
{
	pthread_mutex_lock(&log_lock);

	if (log_closed) {
		pthread_mutex_unlock(&log_lock);
		free(msg);
		return;
	}

	if (log_size == log_capacity) {
		uint32_t capacity = log_capacity ? log_capacity * 2 : 64;
		char** messages = realloc(log_messages, sizeof(char*) * capacity);

		if (!messages) {
			pthread_mutex_unlock(&log_lock);
			free(msg);
			return;
		}
		log_messages = messages;
		log_capacity = capacity;
	}
	log_messages[log_size++] = msg;
	pthread_mutex_unlock(&log_lock);
}

static bool
log_callback(as_log_level level, const char* func, const char* file, uint32_t line,
	const char* fmt, ...)
{
	(void)func;
	(void)file;
	(void)line;

	char buf[1024];
	char* msg = buf;
	va_list ap;
	va_list copy;

	va_start(ap, fmt);
	va_copy(copy, ap);
	int n = vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);

	if (n >= 0 && (size_t)n >= sizeof(buf)) {
		msg = malloc((size_t)n + 1);

		if (msg) {
			vsnprintf(msg, (size_t)n + 1, fmt, copy);
		}
	}
	va_end(copy);

	if (n < 0 || !msg) {
		return true;
	}

	if (log_join_filter && level > AS_LOG_LEVEL_WARN && !starts_with(msg, AWS_PREFIX)) {
		if (msg != buf) {
			free(msg);
		}
		return true;
	}

	fprintf(stderr, "%s %s\n", as_log_level_tostring(level), msg);

	if (msg == buf) {
		msg = strdup(buf);

		if (!msg) {
			return true;
		}
	}
	log_append(msg);
	return true;
}

static void
log_start(as_log_level level, bool join_filter)
{
	log_join_filter = join_filter;
	as_log_set_level(level);
	as_log_set_callback(log_callback);
}

// The callback stays installed, because a refresh that outlived aerospike_close() can still log
// from the discovery thread; later messages are dropped.
static void
log_finish(void)
{
	pthread_mutex_lock(&log_lock);
	log_closed = true;

	for (uint32_t i = 0; i < log_size; i++) {
		free(log_messages[i]);
	}
	free(log_messages);
	log_messages = NULL;
	log_size = 0;
	log_capacity = 0;
	pthread_mutex_unlock(&log_lock);
}

static void
log_clear(void)
{
	pthread_mutex_lock(&log_lock);

	for (uint32_t i = 0; i < log_size; i++) {
		free(log_messages[i]);
	}
	log_size = 0;
	pthread_mutex_unlock(&log_lock);
}

static uint32_t
log_count(const char* text)
{
	uint32_t n = 0;
	pthread_mutex_lock(&log_lock);

	for (uint32_t i = 0; i < log_size; i++) {
		if (strstr(log_messages[i], text)) {
			n++;
		}
	}
	pthread_mutex_unlock(&log_lock);
	return n;
}

static uint32_t
log_refreshes(void)
{
	uint32_t n = 0;
	pthread_mutex_lock(&log_lock);

	for (uint32_t i = 0; i < log_size; i++) {
		if (starts_with(log_messages[i], AWS_PREFIX) && strstr(log_messages[i], " returned ")) {
			n++;
		}
	}
	pthread_mutex_unlock(&log_lock);
	return n;
}

// The label of the first "AWS EC2 seed provider credentials from <label>" line, trimmed, or
// NULL. The caller frees it.
static char*
log_credentials_source(void)
{
	char* source = NULL;
	pthread_mutex_lock(&log_lock);

	for (uint32_t i = 0; i < log_size; i++) {
		if (starts_with(log_messages[i], CREDENTIALS_PREFIX)) {
			const char* begin = log_messages[i] + strlen(CREDENTIALS_PREFIX);
			const char* end = begin + strlen(begin);

			while (begin < end && (unsigned char)*begin <= ' ') {
				begin++;
			}

			while (end > begin && (unsigned char)end[-1] <= ' ') {
				end--;
			}
			source = strndup(begin, (size_t)(end - begin));

			if (!source) {
				pthread_mutex_unlock(&log_lock);
				fail_memory();
			}
			break;
		}
	}
	pthread_mutex_unlock(&log_lock);
	return source;
}

//---------------------------------
// Static Functions: JSON
//---------------------------------

static void
json_raw(json* j, const char* s, size_t n)
{
	if (j->len + n + 1 > j->capacity) {
		size_t capacity = j->capacity ? j->capacity : 1024;

		while (j->len + n + 1 > capacity) {
			capacity *= 2;
		}

		char* data = realloc(j->data, capacity);

		if (!data) {
			fail_memory();
		}
		j->data = data;
		j->capacity = capacity;
	}
	memcpy(j->data + j->len, s, n);
	j->len += n;
	j->data[j->len] = '\0';
}

static void
json_text(json* j, const char* s)
{
	json_raw(j, s, strlen(s));
}

// Quote as Java's Json.quote() does, and also escape bytes 0x7f and above so that the output
// is always ASCII.
static void
json_quote(json* j, const char* s)
{
	json_raw(j, "\"", 1);

	for (const unsigned char* p = (const unsigned char*)s; *p; p++) {
		switch (*p) {
		case '"':
			json_text(j, "\\\"");
			break;
		case '\\':
			json_text(j, "\\\\");
			break;
		case '\n':
			json_text(j, "\\n");
			break;
		case '\r':
			json_text(j, "\\r");
			break;
		case '\t':
			json_text(j, "\\t");
			break;
		default:
			if (*p < 0x20 || *p >= 0x7f) {
				char esc[8];
				snprintf(esc, sizeof(esc), "\\u%04x", (unsigned int)*p);
				json_text(j, esc);
			}
			else {
				json_raw(j, (const char*)p, 1);
			}
		}
	}
	json_raw(j, "\"", 1);
}

static void
json_key(json* j, const char* name)
{
	if (!j->first_field) {
		json_raw(j, ",", 1);
	}
	j->first_field = false;
	json_quote(j, name);
	json_raw(j, ":", 1);
}

static void
json_init(json* j, const char* mode)
{
	j->data = NULL;
	j->len = 0;
	j->capacity = 0;
	j->first_field = true;
	j->first_element = true;
	json_raw(j, "{", 1);
	json_key(j, "mode");
	json_quote(j, mode);
}

static void
json_string(json* j, const char* name, const char* value)
{
	json_key(j, name);

	if (value) {
		json_quote(j, value);
	}
	else {
		json_text(j, "null");
	}
}

static void
json_uint(json* j, const char* name, uint64_t value)
{
	char buf[32];
	snprintf(buf, sizeof(buf), "%" PRIu64, value);
	json_key(j, name);
	json_text(j, buf);
}

static void
json_int(json* j, const char* name, int64_t value)
{
	char buf[32];
	snprintf(buf, sizeof(buf), "%" PRId64, value);
	json_key(j, name);
	json_text(j, buf);
}

static void
json_bool(json* j, const char* name, bool value)
{
	json_key(j, name);
	json_text(j, value ? "true" : "false");
}

static void
json_array_begin(json* j, const char* name)
{
	json_key(j, name);
	json_raw(j, "[", 1);
	j->first_element = true;
}

static void
json_array_separator(json* j)
{
	if (!j->first_element) {
		json_raw(j, ",", 1);
	}
	j->first_element = false;
}

static void
json_array_string(json* j, const char* value)
{
	json_array_separator(j);
	json_quote(j, value);
}

static void
json_array_uint(json* j, uint64_t value)
{
	char buf[32];
	snprintf(buf, sizeof(buf), "%" PRIu64, value);
	json_array_separator(j);
	json_text(j, buf);
}

static void
json_array_end(json* j)
{
	json_raw(j, "]", 1);
}

// Print the object on one line of stdout and release it.
static void
json_print(json* j)
{
	json_raw(j, "}", 1);
	fputs(j->data, stdout);
	fputc('\n', stdout);
	fflush(stdout);
	free(j->data);
	j->data = NULL;
}

// The error message with newlines replaced by spaces, as Java's probe reports it.
static void
error_text(const as_error* err, char out[AS_ERROR_MESSAGE_MAX_SIZE])
{
	memcpy(out, err->message, AS_ERROR_MESSAGE_MAX_SIZE);
	out[AS_ERROR_MESSAGE_MAX_SIZE - 1] = '\0';

	for (char* p = out; *p; p++) {
		if (*p == '\n') {
			*p = ' ';
		}
	}
}

static void
json_error(json* j, const as_error* err)
{
	char msg[AS_ERROR_MESSAGE_MAX_SIZE];
	error_text(err, msg);
	json_string(j, "error", msg);
}

static void
json_credentials_source(json* j)
{
	char* source = log_credentials_source();
	json_string(j, "credentials_source", source);
	free(source);
}

static void
json_hosts(json* j, const as_seed_candidates* candidates)
{
	json_array_begin(j, "hosts");

	for (uint32_t i = 0; i < candidates->hosts->size; i++) {
		const as_host* host = as_vector_get(candidates->hosts, i);
		size_t size = strlen(host->name) + 8;
		char* text = malloc(size);

		if (!text) {
			fail_memory();
		}
		snprintf(text, size, "%s:%u", host->name, (unsigned int)host->port);
		json_array_string(j, text);
		free(text);
	}
	json_array_end(j);
}

// Every captured message containing text, as a JSON array.
static void
json_messages(json* j, const char* name, const char* text)
{
	json_array_begin(j, name);
	pthread_mutex_lock(&log_lock);

	for (uint32_t i = 0; i < log_size; i++) {
		if (strstr(log_messages[i], text)) {
			json_array_string(j, log_messages[i]);
		}
	}
	pthread_mutex_unlock(&log_lock);
	json_array_end(j);
}

//---------------------------------
// Static Functions: provider
//---------------------------------

static void
provider_config(as_aws_seed_provider_config* config, as_aws_filter* filter,
	const probe_env* env)
{
	as_aws_seed_provider_config_init(config);
	filter->name = env->filter_name;
	filter->values = &env->filter_value;
	filter->values_size = 1;
	config->region = env->region;
	config->filters = filter;
	config->filters_size = 1;
}

static as_status
provider_create(as_error* err, const probe_env* env, as_seed_provider* provider)
{
	as_aws_seed_provider_config config;
	as_aws_filter filter;
	provider_config(&config, &filter, env);
	return as_discovery_aws_seed_provider(err, &config, provider);
}

// One refresh into a fresh candidates container, timed in microseconds.
static as_status
provider_refresh(as_error* err, as_seed_provider* provider, as_seed_candidates* candidates,
	uint64_t* micros)
{
	as_seed_candidate_context context;
	memset(&context, 0, sizeof(context));

	if (!as_seed_candidates_init(candidates, 4)) {
		fail_memory();
	}
	as_error_init(err);

	uint64_t begin = cf_getns();
	as_status status = provider->refresh(err, &context, candidates, provider->udata);
	*micros = (cf_getns() - begin) / 1000;
	return status;
}

//---------------------------------
// Static Functions: modes
//---------------------------------

static void
run_latency(const probe_env* env, uint32_t warm)
{
	json j;
	json_init(&j, "latency");

	as_error err;
	as_seed_provider provider;

	if (provider_create(&err, env, &provider) != AEROSPIKE_OK) {
		json_credentials_source(&j);
		json_error(&j, &err);
		json_print(&j);
		return;
	}

	as_seed_candidates hosts;
	uint64_t cold;

	if (provider_refresh(&err, &provider, &hosts, &cold) != AEROSPIKE_OK) {
		as_seed_candidates_destroy(&hosts);
		provider.destroy(provider.udata);
		json_credentials_source(&j);
		json_error(&j, &err);
		json_print(&j);
		return;
	}

	uint64_t* warm_us = malloc(sizeof(uint64_t) * warm);
	char** failures = malloc(sizeof(char*) * warm);
	uint32_t n_failures = 0;

	if (!warm_us || !failures) {
		fail_memory();
	}

	for (uint32_t i = 0; i < warm; i++) {
		as_seed_candidates candidates;

		if (provider_refresh(&err, &provider, &candidates, &warm_us[i]) != AEROSPIKE_OK) {
			char msg[AS_ERROR_MESSAGE_MAX_SIZE];
			error_text(&err, msg);
			failures[n_failures] = strdup(msg);

			if (!failures[n_failures]) {
				fail_memory();
			}
			n_failures++;
		}
		as_seed_candidates_destroy(&candidates);
	}
	provider.destroy(provider.udata);

	json_credentials_source(&j);
	json_hosts(&j, &hosts);
	json_uint(&j, "cold_us", cold);
	json_array_begin(&j, "warm_us");

	for (uint32_t i = 0; i < warm; i++) {
		json_array_uint(&j, warm_us[i]);
	}
	json_array_end(&j);
	json_array_begin(&j, "warm_failures");

	for (uint32_t i = 0; i < n_failures; i++) {
		json_array_string(&j, failures[i]);
	}
	json_array_end(&j);
	json_print(&j);

	for (uint32_t i = 0; i < n_failures; i++) {
		free(failures[i]);
	}
	free(failures);
	free(warm_us);
	as_seed_candidates_destroy(&hosts);
}

static void
run_denied(const probe_env* env)
{
	json j;
	json_init(&j, "denied");

	as_error err;
	as_seed_provider provider;

	if (provider_create(&err, env, &provider) != AEROSPIKE_OK) {
		json_credentials_source(&j);
		json_error(&j, &err);
		json_print(&j);
		return;
	}

	as_seed_candidates hosts;
	uint64_t micros;
	as_status status = provider_refresh(&err, &provider, &hosts, &micros);
	provider.destroy(provider.udata);
	json_credentials_source(&j);

	if (status == AEROSPIKE_OK) {
		json_hosts(&j, &hosts);
	}
	else {
		json_error(&j, &err);
	}
	as_seed_candidates_destroy(&hosts);
	json_print(&j);
}

// Node count, and partitions of ns whose master is one of those nodes. Partition nodes are
// compared by pointer only and never dereferenced.
static void
cluster_count(as_cluster* cluster, const char* ns, cluster_counts* counts)
{
	as_nodes* nodes = as_nodes_reserve(cluster);
	as_partition_table* table = as_partition_tables_get(&cluster->partition_tables, ns);

	counts->nodes = nodes->size;
	counts->owned_partitions = 0;
	counts->partitions = cluster->n_partitions;

	if (table) {
		for (uint32_t i = 0; i < table->size; i++) {
			as_node* master = as_node_load(&table->partitions[i].nodes[0]);

			if (!master) {
				continue;
			}

			for (uint32_t k = 0; k < nodes->size; k++) {
				if (nodes->array[k] == master) {
					counts->owned_partitions++;
					break;
				}
			}
		}
	}
	as_nodes_release(nodes);
}

static void
json_join_counts(json* j, as_cluster* cluster, const char* ns, cluster_counts* counts)
{
	cluster_count(cluster, ns, counts);
	json_uint(j, "nodes", counts->nodes);
	json_int(j, "owned_partitions", counts->owned_partitions);
	json_uint(j, "partitions", counts->partitions);
	json_uint(j, "refreshes", log_refreshes());
	json_uint(j, "overruns", log_count(OVERRUN));
	json_messages(j, "failures", REFRESH_FAILED);
	json_bool(j, "tripped", log_count(TRIPPED) > 0);
}

static void
run_join(const probe_env* env, uint32_t expected_nodes, const char* ns, uint32_t refreshes)
{
	json j;
	json_init(&j, "join");

	as_config config;
	as_config_init(&config);
	config.discovery.execution_mode = AS_DISCOVERY_EXECUTION_THREAD;
	config.discovery.refresh_interval_ms = config.tender_interval;

	as_aws_seed_provider_config aws;
	as_aws_filter filter;
	as_error err;
	provider_config(&aws, &filter, env);

	if (as_config_set_aws_seed_provider(&config, &aws, &err) != AEROSPIKE_OK) {
		as_config_destroy(&config);
		json_error(&j, &err);
		json_print(&j);
		return;
	}

	aerospike as;
	aerospike_init(&as, &config);

	if (aerospike_connect(&as, &err) != AEROSPIKE_OK) {
		aerospike_destroy(&as);
		json_error(&j, &err);
		json_print(&j);
		return;
	}

	cluster_counts counts;
	uint64_t deadline = cf_getms() + JOIN_WAIT_MS;
	char error[256];
	bool failed = false;

	while (true) {
		cluster_count(as.cluster, ns, &counts);

		if (counts.nodes >= expected_nodes && counts.partitions > 0 &&
			counts.owned_partitions >= counts.partitions) {
			break;
		}

		if (cf_getms() > deadline) {
			snprintf(error, sizeof(error), "nodes=%u owned=%" PRId64 " after 60s", counts.nodes,
				counts.owned_partitions);
			failed = true;
			break;
		}
		as_sleep(POLL_MS);
	}

	if (!failed) {
		log_clear();

		uint64_t wait = (uint64_t)refreshes * as.config.discovery.refresh_interval_ms +
			JOIN_WAIT_MS;
		deadline = cf_getms() + wait;

		while (log_refreshes() < refreshes && log_count(TRIPPED) == 0) {
			if (cf_getms() > deadline) {
				snprintf(error, sizeof(error), "only %u refreshes in %" PRIu64 "ms",
					log_refreshes(), wait);
				failed = true;
				break;
			}
			as_sleep(POLL_MS);
		}
	}

	json_join_counts(&j, as.cluster, ns, &counts);

	if (failed) {
		json_string(&j, "error", error);
	}
	json_print(&j);

	aerospike_close(&as, &err);
	aerospike_destroy(&as);
}

//---------------------------------
// Main
//---------------------------------

int
main(int argc, char* argv[])
{
	if (argc != 2) {
		fail_usage("usage: aws_real_probe latency|denied|join");
	}

	const char* mode = argv[1];

	if (strcmp(mode, "latency") != 0 && strcmp(mode, "denied") != 0 &&
		strcmp(mode, "join") != 0) {
		fail_usage("unknown mode %s", mode);
	}

	probe_env env;
	env_load(&env);

	if (strcmp(mode, "latency") == 0) {
		uint32_t warm = env_count("AWS_REAL_WARM", "20");
		log_start(AS_LOG_LEVEL_INFO, false);
		run_latency(&env, warm);
	}
	else if (strcmp(mode, "denied") == 0) {
		log_start(AS_LOG_LEVEL_INFO, false);
		run_denied(&env);
	}
	else {
		uint32_t expected_nodes = env_count("AWS_REAL_EXPECTED_NODES", NULL);
		const char* ns = env_value("AWS_REAL_NAMESPACE");
		uint32_t refreshes = env_count("AWS_REAL_REFRESHES", "30");
		log_start(AS_LOG_LEVEL_DEBUG, true);
		run_join(&env, expected_nodes, ns ? ns : "test", refreshes);
	}

	log_finish();
	free(env.filter_name);
	return 0;
}
