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
#include <inttypes.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <aerospike/aerospike.h>
#include <aerospike/as_atomic.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_config.h>
#include <aerospike/as_discovery.h>
#include <aerospike/as_event.h>
#include <aerospike/as_host.h>
#include <aerospike/as_log.h>
#include <aerospike/as_sleep.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_clock.h>
#include <pthread.h>

#include "test.h"
#include "aerospike_test.h"
#include "discovery_aws_stub.h"

#if defined(_MSC_VER)
#include <windows.h>
#else
#include <unistd.h>
#endif

#if defined(DISCOVERY_FAKE_SERVERS)
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <time.h>
#include <utime.h>
#endif

// Coverage: the static.client discovery, translation, ip_map and use_services_alternate keys of
// the YAML config file. Every key is applied and checked through the registered plugins; an
// invalid file is rejected as a whole and keeps the code registrations; unknown keys are logged
// and skipped; a reload skips the static section; FR-41 and M-10 fail at connect.

//---------------------------------
// Static Functions: shared helpers
//---------------------------------

#define fail(fmt, ...) \
	return atf_assert_log(__result__, "", __FILE__, __LINE__, fmt, ##__VA_ARGS__)

#define CF_HEAD "version: 1.2.0\nstatic:\n  client:\n"
#define CF_BODY_MAX 2048

static char cf_dir[256];
static char cf_path[320];

static bool
cf_dir_create(void)
{
#if defined(_MSC_VER)
	char tmp[MAX_PATH];
	char file[MAX_PATH];

	if (GetTempPathA(MAX_PATH, tmp) == 0 || GetTempFileNameA(tmp, "acf", 0, file) == 0) {
		return false;
	}

	DeleteFileA(file);

	if (!CreateDirectoryA(file, NULL)) {
		return false;
	}

	snprintf(cf_dir, sizeof(cf_dir), "%s", file);
	return true;
#else
	snprintf(cf_dir, sizeof(cf_dir), "/tmp/as-config-file-XXXXXX");
	return mkdtemp(cf_dir) != NULL;
#endif
}

static void
cf_dir_remove(void)
{
	remove(cf_path);
#if defined(_MSC_VER)
	RemoveDirectoryA(cf_dir);
#else
	rmdir(cf_dir);
#endif
}

static bool
cf_write(const char* body)
{
	FILE* fp = fopen(cf_path, "w");

	if (!fp) {
		return false;
	}

	bool ok = fputs(body, fp) >= 0;
	return fclose(fp) == 0 && ok;
}

static void
cf_setenv(const char* name, const char* value)
{
#if defined(_MSC_VER)
	_putenv_s(name, value ? value : "");
#else
	if (value) {
		setenv(name, value, 1);
	}
	else {
		unsetenv(name);
	}
#endif
}

typedef struct cf_env_s {
	const char* name;
	char value[1024];
	bool present;
} cf_env;

static void
cf_env_save(cf_env* e, const char* name)
{
	const char* value = getenv(name);
	e->name = name;
	e->present = value != NULL;
	snprintf(e->value, sizeof(e->value), "%s", value ? value : "");
}

static void
cf_env_restore(cf_env* e)
{
	cf_setenv(e->name, e->present ? e->value : NULL);
}

//---------------------------------
// Static Functions: log capture
//---------------------------------

#define CF_LOG_MAX 256
#define CF_LOG_SIZE 1100

static pthread_mutex_t cf_log_lock = PTHREAD_MUTEX_INITIALIZER;
static char cf_log_lines[CF_LOG_MAX][CF_LOG_SIZE];
static as_log_level cf_log_levels[CF_LOG_MAX];
static uint32_t cf_log_count;
static as_log_callback cf_log_prev;

static bool
cf_log_capture(as_log_level level, const char* func, const char* file, uint32_t line,
	const char* fmt, ...)
{
	(void)func;
	(void)file;
	(void)line;

	char msg[CF_LOG_SIZE];
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(msg, sizeof(msg), fmt, ap);
	va_end(ap);

	pthread_mutex_lock(&cf_log_lock);

	if (cf_log_count < CF_LOG_MAX) {
		cf_log_levels[cf_log_count] = level;
		snprintf(cf_log_lines[cf_log_count++], sizeof(cf_log_lines[0]), "%s", msg);
	}
	pthread_mutex_unlock(&cf_log_lock);
	return true;
}

static void
cf_log_start(void)
{
	pthread_mutex_lock(&cf_log_lock);
	cf_log_count = 0;
	pthread_mutex_unlock(&cf_log_lock);
	cf_log_prev = g_as_log.callback;
	as_log_set_callback(cf_log_capture);
}

static void
cf_log_stop(void)
{
	as_log_set_callback(cf_log_prev);
}

// Number of captured lines at level that contain needle.
static uint32_t
cf_log_find(as_log_level level, const char* needle)
{
	uint32_t n = 0;
	pthread_mutex_lock(&cf_log_lock);

	for (uint32_t i = 0; i < cf_log_count; i++) {
		if (cf_log_levels[i] == level && strstr(cf_log_lines[i], needle)) {
			n++;
		}
	}
	pthread_mutex_unlock(&cf_log_lock);
	return n;
}

// The first captured line at level, or "" when there is none.
static const char*
cf_log_first(as_log_level level)
{
	const char* line = "";
	pthread_mutex_lock(&cf_log_lock);

	for (uint32_t i = 0; i < cf_log_count; i++) {
		if (cf_log_levels[i] == level) {
			line = cf_log_lines[i];
			break;
		}
	}
	pthread_mutex_unlock(&cf_log_lock);
	return line;
}

//---------------------------------
// Static Functions: clients
//---------------------------------

// Write body, then create a client from config and the file while capturing the log. The
// client owns config afterwards.
static aerospike*
cf_init(as_config* config, const char* body)
{
	if (!cf_write(body)) {
		as_config_destroy(config);
		return NULL;
	}

	as_config_provider_set_path(config, cf_path);
	cf_log_start();
	aerospike* client = aerospike_new(config);
	cf_log_stop();
	return client;
}

static aerospike*
cf_init_default(const char* body)
{
	as_config config;
	as_config_init(&config);
	return cf_init(&config, body);
}

// Close and destroy the client in every event-loop mode. With async event loops
// aerospike_destroy() is a no-op: close destroys a connected client, and an unconnected one is
// released here as aerospike_destroy() releases it without event loops.
static void
cf_close(aerospike* client)
{
	bool sync = as_event_loop_size == 0 || as_event_single_thread;

	if (client->cluster) {
		as_error err;
		aerospike_close(client, &err);

		if (sync) {
			aerospike_destroy(client);
		}
		return;
	}

	if (sync) {
		aerospike_destroy(client);
		return;
	}

	as_config_destroy(&client->config);
	cf_free(client->config_orig);
	cf_free(client->config_bitmap);
	cf_free(client);
}

//---------------------------------
// Static Functions: plugins
//---------------------------------

static uint32_t cf_provider_destroys;
static uint32_t cf_translator_destroys;

static as_status
cf_fake_refresh(as_error* err, const as_seed_candidate_context* context,
	as_seed_candidates* candidates, void* udata)
{
	(void)err;
	(void)context;
	(void)candidates;
	(void)udata;
	return AEROSPIKE_OK;
}

static bool
cf_fake_periodic(void* udata)
{
	(void)udata;
	return false;
}

static bool
cf_fake_translate(const as_endpoint* endpoint, as_host* out, void* udata)
{
	(void)endpoint;
	(void)out;
	(void)udata;
	return false;
}

static void
cf_fake_destroy(void* udata)
{
	(*(uint32_t*)udata)++;
}

// A config with a code provider and a code translator whose destroys are counted.
static bool
cf_code_config(as_config* config)
{
	as_config_init(config);
	cf_provider_destroys = 0;
	cf_translator_destroys = 0;

	as_seed_provider provider = {
		.size = sizeof(as_seed_provider),
		.api_version = AS_DISCOVERY_API_VERSION,
		.name = "code-provider",
		.refresh = cf_fake_refresh,
		.needs_periodic_refresh = cf_fake_periodic,
		.destroy = cf_fake_destroy,
		.udata = &cf_provider_destroys
	};

	as_address_translator translator = {
		.size = sizeof(as_address_translator),
		.api_version = AS_DISCOVERY_API_VERSION,
		.name = "code-translator",
		.translate = cf_fake_translate,
		.destroy = cf_fake_destroy,
		.udata = &cf_translator_destroys
	};

	as_error err;
	return as_config_set_seed_provider(config, &provider, &err) == AEROSPIKE_OK &&
		as_config_set_address_translator(config, &translator, &err) == AEROSPIKE_OK;
}

static bool
cf_code_registered(aerospike* client)
{
	as_config* c = &client->config;
	return c->discovery.provider_registered &&
		strcmp(c->discovery.provider.name, "code-provider") == 0 &&
		c->translation.provider_registered &&
		strcmp(c->translation.provider.name, "code-translator") == 0;
}

// Translate an endpoint through the registered translator into "host:port", or "miss".
static const char*
cf_translate(aerospike* client, const char* node, const char* host, uint16_t port, char* buf,
	size_t size)
{
	as_address_translator* t = &client->config.translation.provider;
	as_endpoint endpoint = {
		.node_name = node,
		.tls_name = NULL,
		.host = host,
		.port = port,
		.source = AS_ENDPOINT_SOURCE_STANDARD
	};
	as_host out;
	memset(&out, 0, sizeof(out));

	if (!t->translate(&endpoint, &out, t->udata)) {
		snprintf(buf, size, "miss");
		return buf;
	}

	snprintf(buf, size, "%s:%u", out.name, out.port);
	as_host_destroy(&out);
	return buf;
}

//---------------------------------
// Tests: keys applied
//---------------------------------

TEST(cf_every_discovery_key, "every discovery field, ip_map and use_services_alternate are applied and logged")
{
	aerospike* client = cf_init_default(CF_HEAD
		"    use_services_alternate: true\n"
		"    ip_map:\n"
		"      10.0.0.1: 192.168.0.1\n"
		"      10.0.0.2: 192.168.0.2\n"
		"    discovery:\n"
		"      provider: dns\n"
		"      merge_policy: discovery_only\n"
		"      execution_mode: tend\n"
		"      refresh_interval_ms: 2000\n"
		"      refresh_timeout_ms: 700\n"
		"      tend_refresh_deadline_ms: 40\n"
		"      dns:\n"
		"        names:\n"
		"          - seeds.example.com\n"
		"          - more.example.com\n"
		"        record_type: srv\n"
		"        port: 3100\n"
		"        tls_name: tls-a\n"
		"        timeout_ms: 30\n"
		"        servers:\n"
		"          - 127.0.0.1:5353\n");
	assert_not_null(client);

	as_config* c = &client->config;
	assert_true(c->discovery.provider_registered);
	assert_string_eq(c->discovery.provider.name, "dns");
	assert_int_eq(c->discovery.merge_policy, AS_SEED_DISCOVERY_ONLY);
	assert_int_eq(c->discovery.execution_mode, AS_DISCOVERY_EXECUTION_TEND);
	assert_int_eq(c->discovery.refresh_interval_ms, 2000);
	assert_int_eq(c->discovery.refresh_timeout_ms, 700);
	assert_int_eq(c->discovery.tend_refresh_deadline_ms, 40);
	assert_false(c->translation.provider_registered);
	assert_true(c->use_services_alternate);
	assert_true(client->config_orig->use_services_alternate);
	assert_int_eq(c->ip_map_size, 2);
	assert_string_eq(c->ip_map[0].orig, "10.0.0.1");
	assert_string_eq(c->ip_map[0].alt, "192.168.0.1");
	assert_string_eq(c->ip_map[1].orig, "10.0.0.2");
	assert_string_eq(c->ip_map[1].alt, "192.168.0.2");
	assert_true(c->translation.file_ip_map == c->ip_map);
	assert_int_eq(c->translation.file_ip_map_size, 2);

	static const char* const lines[] = {
		"Set static.client.use_services_alternate = true",
		"Set static.client.ip_map[10.0.0.1] = 192.168.0.1",
		"Set static.client.ip_map[10.0.0.2] = 192.168.0.2",
		"Set static.client.discovery.provider = dns",
		"Set static.client.discovery.merge_policy = discovery_only",
		"Set static.client.discovery.execution_mode = tend",
		"Set static.client.discovery.refresh_interval_ms = 2000",
		"Set static.client.discovery.refresh_timeout_ms = 700",
		"Set static.client.discovery.tend_refresh_deadline_ms = 40",
		"Set static.client.discovery.dns.names = [seeds.example.com,more.example.com]",
		"Set static.client.discovery.dns.record_type = srv",
		"Set static.client.discovery.dns.port = 3100",
		"Set static.client.discovery.dns.tls_name = tls-a",
		"Set static.client.discovery.dns.timeout_ms = 30",
		"Set static.client.discovery.dns.servers = [127.0.0.1:5353]"
	};

	for (uint32_t i = 0; i < sizeof(lines) / sizeof(lines[0]); i++) {
		if (cf_log_find(AS_LOG_LEVEL_INFO, lines[i]) != 1) {
			cf_close(client);
			fail("missing info line: %s", lines[i]);
		}
	}
	assert_int_eq(cf_log_find(AS_LOG_LEVEL_ERROR, ""), 0);
	assert_int_eq(cf_log_find(AS_LOG_LEVEL_WARN, "registered in code is replaced"), 0);
	cf_close(client);
}

TEST(cf_port_map, "the port-map translator maps a host:port key and a node-name key, and on_miss is strict")
{
	aerospike* client = cf_init_default(CF_HEAD
		"    translation:\n"
		"      provider: port-map\n"
		"      on_miss: strict\n"
		"      port_map:\n"
		"        \"10.0.0.1:3000\": \"lb.example.com:30001\"\n"
		"        BB9: \"lb.example.com:30002\"\n");
	assert_not_null(client);

	as_config* c = &client->config;
	assert_true(c->translation.provider_registered);
	assert_string_eq(c->translation.provider.name, "port-map");
	assert_int_eq(c->translation.on_miss, AS_TRANSLATION_STRICT);
	assert_false(c->discovery.provider_registered);
	assert_null(c->ip_map);

	char buf[128];
	assert_string_eq(cf_translate(client, "AA1", "10.0.0.1", 3000, buf, sizeof(buf)),
		"lb.example.com:30001");
	assert_string_eq(cf_translate(client, "BB9", "10.9.9.9", 3000, buf, sizeof(buf)),
		"lb.example.com:30002");
	assert_string_eq(cf_translate(client, "CC1", "10.0.0.3", 3000, buf, sizeof(buf)), "miss");
	assert_int_eq(cf_log_find(AS_LOG_LEVEL_INFO,
		"Set static.client.translation.port_map[10.0.0.1:3000] = lb.example.com:30001"), 1);
	assert_int_eq(cf_log_find(AS_LOG_LEVEL_INFO, "Set static.client.translation.on_miss = strict"),
		1);
	cf_close(client);
}

TEST(cf_pass_through, "the pass-through translator returns the advertised endpoint")
{
	aerospike* client = cf_init_default(CF_HEAD
		"    translation:\n"
		"      provider: pass-through\n");
	assert_not_null(client);

	as_config* c = &client->config;
	assert_true(c->translation.provider_registered);
	assert_string_eq(c->translation.provider.name, "pass-through");
	assert_int_eq(c->translation.on_miss, AS_TRANSLATION_PASS_THROUGH);

	char buf[128];
	assert_string_eq(cf_translate(client, "A1", "10.0.0.5", 3001, buf, sizeof(buf)),
		"10.0.0.5:3001");
	cf_close(client);
}

TEST(cf_static_map, "static_map entries land in ip_map, keep the port and reset a code translator")
{
	as_config config;
	assert_true(cf_code_config(&config));
	aerospike* client = cf_init(&config, CF_HEAD
		"    translation:\n"
		"      static_map:\n"
		"        10.0.0.1: 192.168.0.1\n");
	assert_not_null(client);

	as_config* c = &client->config;
	assert_int_eq(cf_translator_destroys, 1);
	assert_int_eq(cf_provider_destroys, 0);
	assert_false(c->translation.provider_registered);
	assert_true(c->discovery.provider_registered);
	assert_int_eq(c->ip_map_size, 1);
	assert_string_eq(c->ip_map[0].orig, "10.0.0.1");
	assert_string_eq(c->ip_map[0].alt, "192.168.0.1");

	as_endpoint endpoint = {
		.node_name = "A1",
		.tls_name = NULL,
		.host = "10.0.0.1",
		.port = 3100,
		.source = AS_ENDPOINT_SOURCE_STANDARD
	};
	as_host out;
	assert_true(as_discovery_static_map_translate(c->ip_map, c->ip_map_size, &endpoint, &out));
	bool ok = strcmp(out.name, "192.168.0.1") == 0 && out.port == 3100;
	as_host_destroy(&out);
	assert_true(ok);
	assert_int_eq(cf_log_find(AS_LOG_LEVEL_INFO,
		"Set static.client.translation.static_map[10.0.0.1] = 192.168.0.1"), 1);
	assert_int_eq(cf_log_find(AS_LOG_LEVEL_WARN, "Address translator code-translator registered "
		"in code is replaced by static.client.translation.provider static-map in "), 1);
	assert_int_eq(cf_log_find(AS_LOG_LEVEL_WARN, "Seed provider code-provider"), 0);

	cf_close(client);
	assert_int_eq(cf_provider_destroys, 1);
	assert_int_eq(cf_translator_destroys, 1);
}

//---------------------------------
// Tests: code registrations
//---------------------------------

TEST(cf_overrides_code, "a file provider and translator replace the code ones, each destroyed once")
{
	as_config config;
	assert_true(cf_code_config(&config));
	aerospike* client = cf_init(&config, CF_HEAD
		"    discovery:\n"
		"      provider: dns\n"
		"      dns:\n"
		"        names: [seeds.example.com]\n"
		"        servers: ['127.0.0.1:5353']\n"
		"    translation:\n"
		"      provider: pass-through\n");
	assert_not_null(client);

	as_config* c = &client->config;
	assert_int_eq(cf_provider_destroys, 1);
	assert_int_eq(cf_translator_destroys, 1);
	assert_true(c->discovery.provider_registered);
	assert_string_eq(c->discovery.provider.name, "dns");
	assert_true(c->translation.provider_registered);
	assert_string_eq(c->translation.provider.name, "pass-through");
	assert_int_eq(cf_log_find(AS_LOG_LEVEL_WARN, "Seed provider code-provider registered in "
		"code is replaced by static.client.discovery.provider dns in "), 1);
	assert_int_eq(cf_log_find(AS_LOG_LEVEL_WARN, "Address translator code-translator registered "
		"in code is replaced by static.client.translation.provider pass-through in "), 1);

	cf_close(client);
	assert_int_eq(cf_provider_destroys, 1);
	assert_int_eq(cf_translator_destroys, 1);
}

TEST(cf_absent_keeps_code, "a file with only merge_policy keeps the code plugins and the other discovery fields")
{
	as_config config;
	assert_true(cf_code_config(&config));
	config.discovery.execution_mode = AS_DISCOVERY_EXECUTION_TEND;
	config.discovery.refresh_interval_ms = 4000;
	config.discovery.refresh_timeout_ms = 900;
	config.discovery.tend_refresh_deadline_ms = 60;
	config.translation.on_miss = AS_TRANSLATION_STRICT;

	aerospike* client = cf_init(&config, CF_HEAD
		"    discovery:\n"
		"      merge_policy: replace\n");
	assert_not_null(client);

	as_config* c = &client->config;
	assert_int_eq(cf_provider_destroys, 0);
	assert_int_eq(cf_translator_destroys, 0);
	assert_true(cf_code_registered(client));
	assert_int_eq(c->discovery.merge_policy, AS_SEED_REPLACE);
	assert_int_eq(c->discovery.execution_mode, AS_DISCOVERY_EXECUTION_TEND);
	assert_int_eq(c->discovery.refresh_interval_ms, 4000);
	assert_int_eq(c->discovery.refresh_timeout_ms, 900);
	assert_int_eq(c->discovery.tend_refresh_deadline_ms, 60);
	assert_int_eq(c->translation.on_miss, AS_TRANSLATION_STRICT);
	assert_int_eq(cf_log_find(AS_LOG_LEVEL_WARN, "registered in code is replaced"), 0);

	cf_close(client);
	assert_int_eq(cf_provider_destroys, 1);
	assert_int_eq(cf_translator_destroys, 1);
}

TEST(cf_static_resets, "provider static and translation provider static-map reset the code plugins")
{
	as_config config;
	assert_true(cf_code_config(&config));
	aerospike* client = cf_init(&config, CF_HEAD
		"    discovery:\n"
		"      provider: static\n"
		"    translation:\n"
		"      provider: static-map\n");
	assert_not_null(client);

	assert_int_eq(cf_provider_destroys, 1);
	assert_int_eq(cf_translator_destroys, 1);
	assert_false(client->config.discovery.provider_registered);
	assert_false(client->config.translation.provider_registered);
	assert_null(client->config.ip_map);
	assert_int_eq(cf_log_find(AS_LOG_LEVEL_WARN, "Seed provider code-provider registered in "
		"code is replaced by static.client.discovery.provider static in "), 1);
	assert_int_eq(cf_log_find(AS_LOG_LEVEL_WARN, "Address translator code-translator registered "
		"in code is replaced by static.client.translation.provider static-map in "), 1);
	cf_close(client);
	assert_int_eq(cf_provider_destroys, 1);
	assert_int_eq(cf_translator_destroys, 1);

	// An explicit static-map with entries.
	assert_true(cf_code_config(&config));
	client = cf_init(&config, CF_HEAD
		"    translation:\n"
		"      provider: static-map\n"
		"      static_map:\n"
		"        10.0.0.1: 192.168.0.1\n");
	assert_not_null(client);

	assert_int_eq(cf_provider_destroys, 0);
	assert_int_eq(cf_translator_destroys, 1);
	assert_false(client->config.translation.provider_registered);
	assert_int_eq(client->config.ip_map_size, 1);
	cf_close(client);
	assert_int_eq(cf_provider_destroys, 1);
}

//---------------------------------
// Tests: invalid files
//---------------------------------

typedef struct cf_invalid_s {
	const char* body;
	const char* expected;
} cf_invalid;

static const cf_invalid cf_invalid_rows[] = {
	// YAML syntax errors after valid keys, also inside skipped unknown values.
	{"   bad: : x\n", "yaml_parser_parse failure at line 5, column 4: did not find expected key"},
	{"    translation:\n"
	 "      provider: port-map\n"
	 "      port_map:\n"
	 "        \"10.0.0.1:3000\": \"lb.example.com:30001\"\n"
	 "        \"10.0.0.2:3000\": \"lb.example.com:30002\n",
	 "yaml_parser_parse failure at line 10, column 1: found unexpected end of stream"},
	{"    baz: [1, 2\n",
	 "yaml_parser_parse failure at line 6, column 1: did not find expected ',' or ']'"},
	{"    bar: {a: 1\n",
	 "yaml_parser_parse failure at line 6, column 1: did not find expected ',' or '}'"},
	{"    discovery:\n      extra: [1, 2\n",
	 "yaml_parser_parse failure at line 7, column 1: did not find expected ',' or ']'"},

	// Known keys of the wrong kind.
	{"    discovery: static\n", "Invalid static.client.discovery: expected a map, not a scalar"},
	{"    translation: [a]\n", "Invalid static.client.translation: expected a map, not a sequence"},
	{"    ip_map: 10.0.0.1\n", "Invalid static.client.ip_map: expected a map, not a scalar"},
	{"    ip_map:\n      10.0.0.1:\n        a: b\n",
	 "Invalid static.client.ip_map[10.0.0.1]: expected a scalar, not a map"},
	{"    min_connections_per_node:\n      a: b\n",
	 "Invalid static.client.min_connections_per_node: expected a scalar, not a map"},
	{"    discovery:\n      merge_policy: [replace]\n",
	 "Invalid static.client.discovery.merge_policy: expected a scalar, not a sequence"},
	{"    discovery:\n      dns:\n        - a\n",
	 "Invalid static.client.discovery.dns: expected a map, not a sequence"},
	{"    discovery:\n      ? [a]\n      : b\n",
	 "Invalid static.client.discovery: expected scalar keys, not a sequence"},
	{"    discovery:\n      provider: dns\n      dns:\n        names: seeds.example.com\n",
	 "Invalid static.client.discovery.dns.names: expected a sequence, not a scalar"},
	{"    discovery:\n      provider: dns\n      dns:\n        names:\n          - a: b\n",
	 "Invalid static.client.discovery.dns.names[0]: expected a scalar, not a map"},
	{"    discovery:\n      provider: dns\n      dns:\n        port: [1]\n",
	 "Invalid static.client.discovery.dns.port: expected a scalar, not a sequence"},
	{"    discovery:\n      provider: aws-tags\n      aws:\n        filters:\n          - tag:cluster\n",
	 "Invalid static.client.discovery.aws.filters[0]: expected a map, not a scalar"},
	{"    discovery:\n      provider: aws-tags\n      aws:\n        filters:\n"
	 "          - name: tag:cluster\n",
	 "Invalid static.client.discovery.aws.filters[0]: values is required"},
	{"    discovery:\n      provider: aws-tags\n      aws:\n        filters:\n"
	 "          - values: [a]\n",
	 "Invalid static.client.discovery.aws.filters[0]: name is required"},

	// Integers and booleans.
	{"    discovery:\n      refresh_interval_ms: 5s\n",
	 "Invalid uint static.client.discovery.refresh_interval_ms: 5s"},
	{"    discovery:\n      tend_refresh_deadline_ms: '1.5'\n",
	 "Invalid uint static.client.discovery.tend_refresh_deadline_ms: 1.5"},
	{"    discovery:\n      refresh_interval_ms: -1\n",
	 "Invalid uint static.client.discovery.refresh_interval_ms: -1"},
	{"    discovery:\n      refresh_timeout_ms: 4294967296\n",
	 "Invalid uint static.client.discovery.refresh_timeout_ms: 4294967296"},
	{"    discovery:\n      provider: dns\n      dns:\n        timeout_ms: 50abc\n",
	 "Invalid uint static.client.discovery.dns.timeout_ms: 50abc"},
	{"    discovery:\n      provider: dns\n      dns:\n        port: 70000\n",
	 "Invalid uint static.client.discovery.dns.port: 70000. valid range: 1 - 65535"},
	{"    discovery:\n      provider: dns\n      dns:\n        port: 0\n",
	 "Invalid uint static.client.discovery.dns.port: 0. valid range: 1 - 65535"},
	{"    discovery:\n      provider: aws-tags\n      aws:\n        init_timeout_ms: 0\n",
	 "Invalid uint static.client.discovery.aws.init_timeout_ms: 0. valid range: 1 - 4294967295"},
	{"    use_services_alternate: yes\n",
	 "Invalid bool static.client.use_services_alternate: yes"},

	// Enumerations.
	{"    discovery:\n      provider: consul\n",
	 "Invalid static.client.discovery.provider: consul. valid values: static, dns, aws-tags"},
	{"    discovery:\n      merge_policy: union\n",
	 "Invalid static.client.discovery.merge_policy: union. valid values: merge, replace, "
	 "discovery_only"},
	{"    discovery:\n      execution_mode: background\n",
	 "Invalid static.client.discovery.execution_mode: background. valid values: thread, tend"},
	{"    discovery:\n      provider: dns\n      dns:\n        record_type: aaaa\n",
	 "Invalid static.client.discovery.dns.record_type: aaaa. valid values: a, srv"},
	{"    discovery:\n      provider: aws-tags\n      aws:\n        address_field: ipv6\n",
	 "Invalid static.client.discovery.aws.address_field: ipv6. valid values: private_ip, "
	 "public_ip, private_dns, public_dns"},
	{"    translation:\n      provider: nat\n",
	 "Invalid static.client.translation.provider: nat. valid values: static-map, port-map, "
	 "pass-through"},
	{"    translation:\n      on_miss: drop\n",
	 "Invalid static.client.translation.on_miss: drop. valid values: pass_through, strict"},

	// Cross-key rules.
	{"    discovery:\n      provider: dns\n",
	 "static.client.discovery.dns is required with provider dns"},
	{"    discovery:\n      dns:\n        names: [seeds.example.com]\n",
	 "static.client.discovery.dns is only valid with provider dns"},
	{"    discovery:\n      provider: aws-tags\n",
	 "static.client.discovery.aws is required with provider aws-tags"},
	{"    discovery:\n      provider: dns\n      dns:\n        names: [seeds.example.com]\n"
	 "      aws:\n        region: us-east-1\n",
	 "static.client.discovery.aws is only valid with provider aws-tags"},
	{"    translation:\n      provider: port-map\n      port_map:\n        A1: \"h:3000\"\n"
	 "      static_map:\n        10.0.0.1: 192.168.0.1\n",
	 "static.client.translation.static_map is only valid with provider static-map, not 'port-map'"},
	{"    translation:\n      provider: pass-through\n      port_map:\n        A1: \"h:3000\"\n",
	 "static.client.translation.port_map is only valid with provider port-map, not 'pass-through'"},
	{"    translation:\n      port_map:\n        A1: \"h:3000\"\n",
	 "static.client.translation.port_map is only valid with provider port-map"},
	{"    translation:\n      provider: port-map\n",
	 "static.client.translation.port_map is required with provider port-map"},
	{"    ip_map:\n      10.0.0.2: 192.168.0.2\n"
	 "    translation:\n      static_map:\n        10.0.0.1: 192.168.0.1\n",
	 "static.client.translation.static_map and static.client.ip_map are both set"},

	// Factory rejections.
	{"    discovery:\n      provider: dns\n      dns:\n"
	 "        names: [seeds.example.com, SEEDS.example.com]\n        servers: ['127.0.0.1:53']\n",
	 "Invalid static.client.discovery.dns: Invalid DNS seed provider names: duplicate name"},
	{"    discovery:\n      provider: dns\n      dns:\n        names: []\n",
	 "Invalid static.client.discovery.dns: Invalid DNS seed provider names: at least one name"},
	{"    discovery:\n      provider: dns\n      dns:\n        names: [seeds.example.com]\n"
	 "        servers: [not-an-ip]\n",
	 "Invalid static.client.discovery.dns: Invalid DNS seed provider server"},
	{"    discovery:\n      provider: aws-tags\n      aws:\n        region: bad region\n"
	 "        filters:\n          - name: tag:cluster\n            values: [a]\n",
	 "Invalid static.client.discovery.aws: Invalid AWS EC2 seed provider region"},
	{"    translation:\n      provider: port-map\n      port_map:\n        A1: nohostport\n",
	 "Invalid static.client.translation.port_map: "}
};

TEST(cf_invalid_keeps_code, "an invalid file is rejected as a whole: code plugins kept, no key applied, one error naming the key")
{
	uint32_t rows = sizeof(cf_invalid_rows) / sizeof(cf_invalid_rows[0]);

	for (uint32_t i = 0; i < rows; i++) {
		const cf_invalid* row = &cf_invalid_rows[i];
		char body[CF_BODY_MAX];
		snprintf(body, sizeof(body), CF_HEAD "    max_connections_per_node: 60\n%s", row->body);

		as_config config;
		assert_true(cf_code_config(&config));
		config.max_conns_per_node = 123;
		config.discovery.merge_policy = AS_SEED_REPLACE;
		config.discovery.refresh_interval_ms = 4321;

		aerospike* client = cf_init(&config, body);
		assert_not_null(client);

		as_config* c = &client->config;
		uint32_t errors = cf_log_find(AS_LOG_LEVEL_ERROR, "Failed to parse");
		uint32_t matches = cf_log_find(AS_LOG_LEVEL_ERROR, row->expected);
		bool kept = cf_provider_destroys == 0 && cf_translator_destroys == 0 &&
			cf_code_registered(client) && c->max_conns_per_node == 123 &&
			c->discovery.merge_policy == AS_SEED_REPLACE &&
			c->discovery.refresh_interval_ms == 4321 &&
			c->discovery.execution_mode == AS_DISCOVERY_EXECUTION_THREAD &&
			c->translation.on_miss == AS_TRANSLATION_PASS_THROUGH &&
			!c->use_services_alternate && !c->ip_map && !c->translation.file_ip_map;
		char first[CF_LOG_SIZE];
		snprintf(first, sizeof(first), "%s", cf_log_first(AS_LOG_LEVEL_ERROR));

		cf_close(client);

		if (errors != 1 || matches != 1) {
			fail("row %u: expected one error with '%s', got %u: %s", i, row->expected, errors,
				first);
		}

		if (!kept) {
			fail("row %u: the code configuration was not kept", i);
		}

		if (cf_provider_destroys != 1 || cf_translator_destroys != 1) {
			fail("row %u: destroys %u %u", i, cf_provider_destroys, cf_translator_destroys);
		}
	}
}

#define CF_BAD_KEY "yaml_parser_parse failure at line 5, column 4: did not find expected key"

TEST(cf_parser_error_existing_keys, "a YAML syntax error after valid existing keys rejects them too")
{
	as_config init;
	as_config_init(&init);

	aerospike* client = cf_init_default(CF_HEAD
		"    max_connections_per_node: 60\n"
		"   bad: : x\n");
	assert_not_null(client);
	uint32_t max_conns = client->config.max_conns_per_node;
	uint32_t errors = cf_log_find(AS_LOG_LEVEL_ERROR, CF_BAD_KEY);
	cf_close(client);
	assert_int_eq(max_conns, init.max_conns_per_node);
	assert_int_eq(errors, 1);

	client = cf_init_default("version: 1.2.0\n"
		"dynamic:\n"
		"  client:\n"
		"    max_error_rate: 50\n"
		"   bad: : x\n");
	assert_not_null(client);
	uint32_t max_error_rate = client->config.max_error_rate;
	errors = cf_log_find(AS_LOG_LEVEL_ERROR, CF_BAD_KEY);
	cf_close(client);
	assert_int_eq(max_error_rate, init.max_error_rate);
	assert_int_eq(errors, 1);
	as_config_destroy(&init);
}

//---------------------------------
// Tests: repeated, unknown and absent keys
//---------------------------------

TEST(cf_repeated_key, "a repeated dns.names, servers, tls_name or port_map keeps only the last value")
{
	// Accumulated names would be duplicates and fail the DNS factory.
	aerospike* client = cf_init_default(CF_HEAD
		"    discovery:\n"
		"      provider: dns\n"
		"      dns:\n"
		"        names: [seeds.example.com]\n"
		"        tls_name: first\n"
		"        servers: ['127.0.0.1:5353']\n"
		"        names: [seeds.example.com]\n"
		"        tls_name: second\n"
		"        servers: ['127.0.0.1:5354']\n");
	assert_not_null(client);
	bool registered = client->config.discovery.provider_registered;
	uint32_t errors = cf_log_find(AS_LOG_LEVEL_ERROR, "");
	uint32_t names = cf_log_find(AS_LOG_LEVEL_INFO,
		"Set static.client.discovery.dns.names = [seeds.example.com]");
	cf_close(client);
	assert_true(registered);
	assert_int_eq(errors, 0);
	assert_int_eq(names, 2);

	client = cf_init_default(CF_HEAD
		"    translation:\n"
		"      provider: port-map\n"
		"      port_map:\n"
		"        \"10.0.0.1:3000\": \"lb.example.com:30001\"\n"
		"      port_map:\n"
		"        \"10.0.0.2:3000\": \"lb.example.com:30002\"\n");
	assert_not_null(client);
	assert_true(client->config.translation.provider_registered);

	char buf[128];
	assert_string_eq(cf_translate(client, "A1", "10.0.0.1", 3000, buf, sizeof(buf)), "miss");
	assert_string_eq(cf_translate(client, "A2", "10.0.0.2", 3000, buf, sizeof(buf)),
		"lb.example.com:30002");
	cf_close(client);
}

TEST(cf_unknown_keys, "unknown keys at every level, and unknown nested maps and sequences, log at info while the rest applies")
{
	aerospike* client = cf_init_default(CF_HEAD
		"    foo: 1\n"
		"    bar:\n"
		"      a: 1\n"
		"    baz: [1, 2]\n"
		"    discovery:\n"
		"      provider: dns\n"
		"      provider_class: com.example.Provider\n"
		"      extra:\n"
		"        a: b\n"
		"      dns:\n"
		"        names: [seeds.example.com]\n"
		"        servers: ['127.0.0.1:5353']\n"
		"        search: [example.com]\n"
		"      merge_policy: replace\n"
		"    translation:\n"
		"      provider: pass-through\n"
		"      table:\n"
		"        a: b\n"
		"      on_miss: strict\n");
	assert_not_null(client);

	as_config* c = &client->config;
	assert_int_eq(cf_log_find(AS_LOG_LEVEL_ERROR, ""), 0);
	assert_true(c->discovery.provider_registered);
	assert_string_eq(c->discovery.provider.name, "dns");
	assert_int_eq(c->discovery.merge_policy, AS_SEED_REPLACE);
	assert_string_eq(c->translation.provider.name, "pass-through");
	assert_int_eq(c->translation.on_miss, AS_TRANSLATION_STRICT);

	static const char* const lines[] = {
		"Unexpected field: client.foo",
		"Unexpected section: client.bar",
		"Unexpected sequence: client.baz",
		"Unexpected field: static.client.discovery.provider_class",
		"Unexpected field: static.client.discovery.extra",
		"Unexpected field: static.client.discovery.dns.search",
		"Unexpected field: static.client.translation.table"
	};

	for (uint32_t i = 0; i < sizeof(lines) / sizeof(lines[0]); i++) {
		if (cf_log_find(AS_LOG_LEVEL_INFO, lines[i]) != 1) {
			cf_close(client);
			fail("missing info line: %s", lines[i]);
		}
	}
	cf_close(client);

	client = cf_init_default(CF_HEAD
		"    discovery:\n"
		"      provider: aws-tags\n"
		"      aws:\n"
		"        region: us-east-1\n"
		"        credentials:\n"
		"          access_key_id: AKIDEXAMPLE\n"
		"        profile: dev\n"
		"        filters:\n"
		"          - name: tag:cluster\n"
		"            values: [prod-a]\n"
		"            match: exact\n"
		"        endpoint: http://127.0.0.1:1\n");
	assert_not_null(client);
	c = &client->config;
	assert_int_eq(cf_log_find(AS_LOG_LEVEL_ERROR, ""), 0);
	assert_true(c->discovery.provider_registered);
	assert_string_eq(c->discovery.provider.name, "aws-tags");

	static const char* const aws_lines[] = {
		"Unexpected field: static.client.discovery.aws.credentials",
		"Unexpected field: static.client.discovery.aws.profile",
		"Unexpected field: static.client.discovery.aws.filters[0].match"
	};

	for (uint32_t i = 0; i < sizeof(aws_lines) / sizeof(aws_lines[0]); i++) {
		if (cf_log_find(AS_LOG_LEVEL_INFO, aws_lines[i]) != 1) {
			cf_close(client);
			fail("missing info line: %s", aws_lines[i]);
		}
	}
	cf_close(client);
}

static bool
cf_is_default(aerospike* client, const as_config* init)
{
	const as_config* c = &client->config;
	return !c->discovery.provider_registered &&
		c->discovery.merge_policy == init->discovery.merge_policy &&
		c->discovery.execution_mode == init->discovery.execution_mode &&
		c->discovery.refresh_interval_ms == init->discovery.refresh_interval_ms &&
		c->discovery.refresh_timeout_ms == init->discovery.refresh_timeout_ms &&
		c->discovery.tend_refresh_deadline_ms == init->discovery.tend_refresh_deadline_ms &&
		!c->translation.provider_registered &&
		c->translation.on_miss == init->translation.on_miss &&
		!c->translation.file_ip_map && c->translation.file_ip_map_size == 0 &&
		!c->ip_map && c->ip_map_size == 0 &&
		c->use_services_alternate == init->use_services_alternate &&
		client->config_orig->use_services_alternate == init->use_services_alternate;
}

TEST(cf_defaults, "a version-only file and a dynamic-only file leave discovery and translation at their defaults")
{
	as_config init;
	as_config_init(&init);

	aerospike* client = cf_init_default("version: 1.2.0\n");
	assert_not_null(client);
	bool is_default = cf_is_default(client, &init);
	cf_close(client);
	assert_true(is_default);

	client = cf_init_default("version: 1.2.0\n"
		"dynamic:\n"
		"  client:\n"
		"    max_error_rate: 50\n");
	assert_not_null(client);
	is_default = cf_is_default(client, &init);
	uint32_t max_error_rate = client->config.max_error_rate;
	cf_close(client);
	assert_true(is_default);
	assert_int_eq(max_error_rate, 50);
	as_config_destroy(&init);
}

TEST(cf_use_services_alternate, "static use_services_alternate applies unless dynamic use_service_alternative is set, in either order")
{
	static const struct {
		const char* body;
		bool config;
		bool orig;
	} rows[] = {
		{CF_HEAD "    use_services_alternate: true\n", true, true},
		{CF_HEAD "    use_services_alternate: true\n"
		 "dynamic:\n  client:\n    use_service_alternative: false\n", false, true},
		{"version: 1.2.0\ndynamic:\n  client:\n    use_service_alternative: false\n"
		 "static:\n  client:\n    use_services_alternate: true\n", false, true},
		{CF_HEAD "    use_services_alternate: false\n"
		 "dynamic:\n  client:\n    use_service_alternative: true\n", true, false},
		{"version: 1.2.0\ndynamic:\n  client:\n    use_service_alternative: true\n", true, false}
	};

	for (uint32_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
		aerospike* client = cf_init_default(rows[i].body);
		assert_not_null(client);
		bool config = client->config.use_services_alternate;
		bool orig = client->config_orig->use_services_alternate;
		uint32_t errors = cf_log_find(AS_LOG_LEVEL_ERROR, "");
		cf_close(client);

		if (config != rows[i].config || orig != rows[i].orig || errors != 0) {
			fail("row %u: config %d orig %d errors %u", i, config, orig, errors);
		}
	}
}

//---------------------------------
// Tests: providers, reload and connect (fake servers)
//---------------------------------

#if defined(DISCOVERY_FAKE_SERVERS)

#define AWS_KEY "AKIDEXAMPLE"
#define AWS_SECRET "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
#define AWS_TOKEN "FwoGZXIvYXdzEXAMPLE/token+="

// Call the registered provider once. out is left initialized; the caller destroys it.
static as_status
cf_refresh(aerospike* client, as_error* err, as_seed_candidates* out)
{
	as_seed_provider* provider = &client->config.discovery.provider;
	as_vector configured;
	as_vector_init(&configured, sizeof(as_host), 1);
	as_seed_candidate_context context = {
		.cluster_name = NULL,
		.configured_seeds = &configured,
		.tend_iteration = 0
	};
	as_seed_candidates_init(out, 4);
	as_error_reset(err);
	as_status status = provider->refresh(err, &context, out, provider->udata);
	as_vector_destroy(&configured);
	return status;
}

// A loopback UDP socket that is bound but never answers, so queries time out instead of failing
// at once through ICMP.
static int
cf_silent_udp(uint16_t* port)
{
	int fd = socket(AF_INET, SOCK_DGRAM, 0);

	if (fd < 0) {
		return -1;
	}

	struct sockaddr_in sa;
	memset(&sa, 0, sizeof(sa));
	sa.sin_family = AF_INET;
	sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	socklen_t len = sizeof(sa);

	if (bind(fd, (struct sockaddr*)&sa, sizeof(sa)) != 0 ||
		getsockname(fd, (struct sockaddr*)&sa, &len) != 0) {
		close(fd);
		return -1;
	}

	*port = ntohs(sa.sin_port);
	return fd;
}

// Refresh time in milliseconds of a dns provider from body, or UINT64_MAX on failure.
static uint64_t
cf_dns_refresh_ms(const char* body, as_status* status)
{
	aerospike* client = cf_init_default(body);

	if (!client || !client->config.discovery.provider_registered ||
		strcmp(client->config.discovery.provider.name, "dns") != 0) {
		if (client) {
			cf_close(client);
		}
		return UINT64_MAX;
	}

	as_error err;
	as_seed_candidates out;
	uint64_t start = cf_getms();
	*status = cf_refresh(client, &err, &out);
	uint64_t elapsed = cf_getms() - start;
	as_seed_candidates_destroy(&out);
	cf_close(client);
	return elapsed;
}

TEST(cf_dns_provider, "the dns provider from the file uses its servers and timeout_ms")
{
	uint16_t port = 0;
	int fd = cf_silent_udp(&port);
	assert_true(fd >= 0);

	// One silent resolver gets up to 3/4 of timeout_ms, while the 80 ms default ends far sooner.
	char body[CF_BODY_MAX];
	snprintf(body, sizeof(body), CF_HEAD
		"    discovery:\n"
		"      provider: dns\n"
		"      dns:\n"
		"        names: [seeds.example.com]\n"
		"        timeout_ms: 600\n"
		"        servers: ['127.0.0.1:%u']\n", port);
	as_status status = AEROSPIKE_OK;
	uint64_t yaml_ms = cf_dns_refresh_ms(body, &status);
	as_status default_status = AEROSPIKE_OK;

	snprintf(body, sizeof(body), CF_HEAD
		"    discovery:\n"
		"      provider: dns\n"
		"      dns:\n"
		"        names: [seeds.example.com]\n"
		"        servers: ['127.0.0.1:%u']\n", port);
	uint64_t default_ms = cf_dns_refresh_ms(body, &default_status);
	close(fd);

	assert_true(yaml_ms != UINT64_MAX && default_ms != UINT64_MAX);
	assert_int_eq(status, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_int_eq(default_status, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);

	if (yaml_ms < 250 || yaml_ms > 5000 || default_ms >= 250) {
		fail("refresh took %" PRIu64 " ms with timeout_ms 600 and %" PRIu64 " ms by default",
			yaml_ms, default_ms);
	}
}

TEST(cf_aws_provider, "the aws-tags provider from the file sends its region and filters and returns its port and tls_name")
{
	DISCOVERY_SCOPED(aws_stub, s, aws_stub_stop);
	assert_true(aws_stub_start(&s, false));
	char* instance = aws_xml_instance("i-1", "10.0.0.7", "203.0.113.7", NULL, NULL);
	const char* instances[] = {instance};
	aws_stub_describe(&s, NULL, instances, 1);
	free(instance);

	static const char* const names[] = {
		"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"
	};
	static const char* const values[] = {AWS_KEY, AWS_SECRET, AWS_TOKEN};
	cf_env saved[3];

	for (uint32_t i = 0; i < 3; i++) {
		cf_env_save(&saved[i], names[i]);
		cf_setenv(names[i], values[i]);
	}

	char body[CF_BODY_MAX];
	snprintf(body, sizeof(body), CF_HEAD
		"    discovery:\n"
		"      provider: aws-tags\n"
		"      aws:\n"
		"        region: eu-west-1\n"
		"        filters:\n"
		"          - name: tag:cluster\n"
		"            values:\n"
		"              - prod-a\n"
		"              - prod-b\n"
		"        address_field: public_ip\n"
		"        port: 3456\n"
		"        tls_name: tls-aws\n"
		"        timeout_ms: 2000\n"
		"        init_timeout_ms: 5000\n"
		"        endpoint: http://127.0.0.1:%u\n", s.port);
	aerospike* client = cf_init_default(body);

	bool registered = client && client->config.discovery.provider_registered &&
		strcmp(client->config.discovery.provider.name, "aws-tags") == 0;
	uint32_t ignored = cf_log_find(AS_LOG_LEVEL_INFO,
		"Ignore static.client.discovery.aws.init_timeout_ms = 5000: the first refresh is bounded "
		"by static.client.discovery.refresh_timeout_ms");
	as_status status = AEROSPIKE_ERR_CLIENT;
	as_error err;
	as_error_init(&err);
	char hosts[256] = "";

	if (registered) {
		as_seed_candidates out;
		status = cf_refresh(client, &err, &out);

		for (uint32_t i = 0; i < out.hosts->size; i++) {
			as_host* host = as_vector_get(out.hosts, i);
			size_t len = strlen(hosts);
			snprintf(hosts + len, sizeof(hosts) - len, "%s%s:%u/%s", i ? "," : "", host->name,
				host->port, host->tls_name ? host->tls_name : "");
		}
		as_seed_candidates_destroy(&out);
	}

	if (client) {
		cf_close(client);
	}

	for (uint32_t i = 0; i < 3; i++) {
		cf_env_restore(&saved[i]);
	}

	aws_request* r = malloc(sizeof(aws_request));
	bool requested = aws_stub_request(&s, 0, r);
	aws_stub_stop(&s);

	if (!registered || status != AEROSPIKE_OK || !requested) {
		free(r);
		fail("registered %d status %d requested %d: %s", registered, status, requested,
			err.message);
	}

	char auth[512] = "";
	char token[256] = "";
	aws_request_header(r, "authorization", auth, sizeof(auth));
	aws_request_header(r, "x-amz-security-token", token, sizeof(token));
	bool body_ok = strcmp(r->body, "Action=DescribeInstances&Version=2016-11-15"
		"&Filter.1.Name=tag%3Acluster&Filter.1.Value.1=prod-a&Filter.1.Value.2=prod-b"
		"&Filter.2.Name=instance-state-name&Filter.2.Value.1=running") == 0;
	free(r);

	assert_true(body_ok);
	assert_not_null(strstr(auth, "Credential=" AWS_KEY "/"));
	assert_not_null(strstr(auth, "/eu-west-1/ec2/aws4_request"));
	assert_string_eq(token, AWS_TOKEN);
	assert_string_eq(hosts, "203.0.113.7:3456/tls-aws");
	assert_int_eq(ignored, 1);
}

// Replace the file atomically with body and a later modification time. The tender reads the file
// whenever its modification time changes, so an in-place rewrite could be read truncated.
static bool
cf_replace_later(const char* body)
{
	char tmp[sizeof(cf_path) + 8];
	snprintf(tmp, sizeof(tmp), "%s.tmp", cf_path);

	FILE* fp = fopen(tmp, "w");

	if (!fp) {
		return false;
	}

	bool ok = fputs(body, fp) >= 0;

	if (fclose(fp) != 0 || !ok) {
		remove(tmp);
		return false;
	}

	time_t later = time(NULL) + 5;
	struct utimbuf t = {.actime = later, .modtime = later};

	if (utime(tmp, &t) != 0 || rename(tmp, cf_path) != 0) {
		remove(tmp);
		return false;
	}
	return true;
}

// A client connected to fnode a from body, reading the file every tend.
static aerospike*
cf_connect(fnode* a, const char* body, as_error* err)
{
	as_config config;
	as_config_init(&config);
	config.tender_interval = AS_TEND_INTERVAL_MIN;
	config.config_provider.interval = AS_TEND_INTERVAL_MIN;
	config.conn_timeout_ms = 1000;
	config.fail_if_not_connected = true;
	as_config_add_host(&config, "127.0.0.1", a->port);

	aerospike* client = cf_init(&config, body);

	if (!client) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to write %s", cf_path);
		return NULL;
	}

	if (aerospike_connect(client, err) != AEROSPIKE_OK) {
		cf_close(client);
		return NULL;
	}
	return client;
}

// Replace the file with a later modification time and wait until the reload set
// max_error_rate. The log is captured meanwhile.
static bool
cf_reload(aerospike* client, const char* body, uint32_t max_error_rate)
{
	cf_log_start();

	if (!cf_replace_later(body)) {
		cf_log_stop();
		return false;
	}

	uint64_t end = cf_getms() + 10000;
	bool done = false;

	while (cf_getms() < end) {
		if (as_load_uint32(&client->config.max_error_rate) == max_error_rate &&
			cf_log_find(AS_LOG_LEVEL_INFO, "Update dynamic config") > 0) {
			done = true;
			break;
		}
		as_sleep(20);
	}
	cf_log_stop();
	return done;
}

TEST(cf_reload_keeps_static_alternate, "a reload without the dynamic key keeps the static use_services_alternate")
{
	DISCOVERY_SCOPED(fnode, a, fnode_stop);
	assert_true(fnode_start(&a, "A"));

	as_error err;
	aerospike* client = cf_connect(&a, CF_HEAD
		"    use_services_alternate: true\n"
		"dynamic:\n"
		"  client:\n"
		"    use_service_alternative: true\n"
		"    max_error_rate: 100\n", &err);

	if (!client) {
		fnode_stop(&a);
		fail("connect: %d %s", err.code, err.message);
	}

	bool before = client->cluster->use_services_alternate;
	bool reloaded = cf_reload(client, CF_HEAD
		"    use_services_alternate: true\n"
		"dynamic:\n"
		"  client:\n"
		"    max_error_rate: 77\n", 77);
	bool after = client->cluster->use_services_alternate;
	bool config = client->config.use_services_alternate;
	cf_close(client);
	fnode_stop(&a);

	assert_true(before);
	assert_true(reloaded);
	assert_true(after);
	assert_true(config);
}

TEST(cf_reload_skips_static, "a reload skips changed and broken static discovery keys")
{
	DISCOVERY_SCOPED(fnode, a, fnode_stop);
	assert_true(fnode_start(&a, "A"));

	as_error err;
	aerospike* client = cf_connect(&a, CF_HEAD
		"    discovery:\n"
		"      merge_policy: replace\n"
		"      refresh_interval_ms: 2000\n"
		"dynamic:\n"
		"  client:\n"
		"    max_error_rate: 100\n", &err);

	if (!client) {
		fnode_stop(&a);
		fail("connect: %d %s", err.code, err.message);
	}

	uint32_t max_conns = client->config.max_conns_per_node;
	bool reloaded = cf_reload(client, CF_HEAD
		"    max_connections_per_node: 7\n"
		"    discovery:\n"
		"      provider: consul\n"
		"      merge_policy: discovery_only\n"
		"      refresh_interval_ms: 5s\n"
		"    translation: broken\n"
		"dynamic:\n"
		"  client:\n"
		"    max_error_rate: 77\n", 77);
	uint32_t errors = cf_log_find(AS_LOG_LEVEL_ERROR, "");
	uint32_t warnings = cf_log_find(AS_LOG_LEVEL_WARN, "Dynamic configuration error");
	as_config* c = &client->config;
	bool unchanged = c->discovery.merge_policy == AS_SEED_REPLACE &&
		c->discovery.refresh_interval_ms == 2000 && !c->discovery.provider_registered &&
		!c->translation.provider_registered && c->max_conns_per_node == max_conns;
	cf_close(client);
	fnode_stop(&a);

	assert_true(reloaded);
	assert_int_eq(errors, 0);
	assert_int_eq(warnings, 0);
	assert_true(unchanged);
}

TEST(cf_fr41_at_connect, "FR-41 from the file: a refresh_interval_ms below the tend interval fails aerospike_connect")
{
	aerospike* client = cf_init_default(CF_HEAD
		"    discovery:\n"
		"      provider: dns\n"
		"      refresh_interval_ms: 500\n"
		"      dns:\n"
		"        names: [seeds.example.com]\n"
		"        servers: ['127.0.0.1:1']\n");
	assert_not_null(client);
	assert_true(client->config.discovery.provider_registered);

	as_error err;
	as_status status = aerospike_connect(client, &err);
	bool message = strstr(err.message,
		"Discovery refresh_interval_ms 500 must be greater or equal to the tend interval 1000") != NULL;
	cf_close(client);
	assert_int_eq(status, AEROSPIKE_ERR_PARAM);
	assert_true(message);
}

#define CF_M10 "Seed provider aws-tags does not support a deadline and cannot use discovery " \
	"execution_mode tend. Use execution_mode thread"

static const char* const cf_prod_a[] = {"prod-a"};
static const as_aws_filter cf_tag_filter[] = {{"tag:cluster", cf_prod_a, 1}};

TEST(cf_m10_at_connect, "M-10 from the file: aws-tags with execution_mode tend is accepted at init and fails aerospike_connect")
{
	aerospike* client = cf_init_default(CF_HEAD
		"    discovery:\n"
		"      provider: aws-tags\n"
		"      execution_mode: tend\n"
		"      aws:\n"
		"        region: us-east-1\n"
		"        filters:\n"
		"          - name: tag:cluster\n"
		"            values: [prod-a]\n"
		"        endpoint: http://127.0.0.1:1\n");
	assert_not_null(client);
	bool accepted = client->config.discovery.provider_registered &&
		strcmp(client->config.discovery.provider.name, "aws-tags") == 0 &&
		client->config.discovery.execution_mode == AS_DISCOVERY_EXECUTION_TEND;

	as_error err;
	as_status status = aerospike_connect(client, &err);
	bool message = strstr(err.message, CF_M10) != NULL;
	cf_close(client);
	assert_true(accepted);
	assert_int_eq(status, AEROSPIKE_ERR_PARAM);
	assert_true(message);

	// A code aws provider, registered in thread mode, with execution_mode tend from the file.
	as_config config;
	as_config_init(&config);
	as_aws_seed_provider_config aws;
	as_aws_seed_provider_config_init(&aws);
	aws.region = "us-east-1";
	aws.filters = cf_tag_filter;
	aws.filters_size = 1;
	aws.endpoint = "http://127.0.0.1:1";
	aws.access_key_id = AWS_KEY;
	aws.secret_access_key = AWS_SECRET;
	assert_int_eq(as_config_set_aws_seed_provider(&config, &aws, &err), AEROSPIKE_OK);

	client = cf_init(&config, CF_HEAD
		"    discovery:\n"
		"      execution_mode: tend\n");
	assert_not_null(client);
	accepted = client->config.discovery.provider_registered &&
		client->config.discovery.execution_mode == AS_DISCOVERY_EXECUTION_TEND;
	status = aerospike_connect(client, &err);
	message = strstr(err.message, CF_M10) != NULL;
	cf_close(client);
	assert_true(accepted);
	assert_int_eq(status, AEROSPIKE_ERR_PARAM);
	assert_true(message);
}

#endif // DISCOVERY_FAKE_SERVERS

//---------------------------------
// Test Suite
//---------------------------------

static cf_env cf_config_url;

static bool
before(atf_suite* suite)
{
	(void)suite;
	cf_env_save(&cf_config_url, "AEROSPIKE_CLIENT_CONFIG_URL");
	cf_setenv("AEROSPIKE_CLIENT_CONFIG_URL", NULL);

	if (!cf_dir_create()) {
		return false;
	}

	snprintf(cf_path, sizeof(cf_path), "%s/client.yaml", cf_dir);
	return true;
}

static bool
after(atf_suite* suite)
{
	(void)suite;
	cf_dir_remove();
	cf_env_restore(&cf_config_url);
	return true;
}

SUITE(discovery_config_file, "YAML discovery and translation keys")
{
	suite_before(before);
	suite_after(after);

	suite_add(cf_every_discovery_key);
	suite_add(cf_port_map);
	suite_add(cf_pass_through);
	suite_add(cf_static_map);
	suite_add(cf_overrides_code);
	suite_add(cf_absent_keeps_code);
	suite_add(cf_static_resets);
	suite_add(cf_invalid_keeps_code);
	suite_add(cf_parser_error_existing_keys);
	suite_add(cf_repeated_key);
	suite_add(cf_unknown_keys);
	suite_add(cf_defaults);
	suite_add(cf_use_services_alternate);
#if defined(DISCOVERY_FAKE_SERVERS)
	suite_add(cf_dns_provider);
	suite_add(cf_aws_provider);
	suite_add(cf_reload_keeps_static_alternate);
	suite_add(cf_reload_skips_static);
	suite_add(cf_fr41_at_connect);
	suite_add(cf_m10_at_connect);
#endif
}
