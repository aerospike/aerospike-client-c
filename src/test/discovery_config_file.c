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
#include <aerospike/as_config_internal.h>
#include <aerospike/as_discovery.h>
#include <aerospike/as_double.h>
#include <aerospike/as_event.h>
#include <aerospike/as_host.h>
#include <aerospike/as_list.h>
#include <aerospike/as_log.h>
#include <aerospike/as_map.h>
#include <aerospike/as_sleep.h>
#include <aerospike/as_string.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_clock.h>
#include <pthread.h>

#include "test.h"
#include "aerospike_test.h"
#include "discovery_aws_stub.h"

#if defined(_MSC_VER)
#include <windows.h>
#else
#include <locale.h>
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
// and skipped; a reload skips the static section; FR-41 and M-10 fail at connect. Names registered
// in code are built from the file with the other keys as typed options (ProviderRegistrationTest).

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
// Static Functions: factories
//---------------------------------

// A registered factory. The second of each kind fails and the third returns an invalid table.
// Each instance gets its own heap udata that points back here.
typedef struct cf_plugin_s {
	const char* name;
	bool fail;
	bool invalid;
	uint32_t calls;
	uint32_t created;
	uint32_t destroyed;
	uint32_t released;
	uint32_t refreshes;
	char seeds[128];
	as_val* options;
} cf_plugin;

#define CF_PLUGINS 3

static cf_plugin cf_providers[CF_PLUGINS];
static cf_plugin cf_translators[CF_PLUGINS];

static const char* const cf_provider_names[CF_PLUGINS] = {
	"consul", "consul-fail", "consul-invalid"
};

static const char* const cf_translator_names[CF_PLUGINS] = {
	"nat-table", "nat-fail", "nat-invalid"
};

static void
cf_plugin_instance_destroy(void* udata)
{
	cf_plugin* p = *(cf_plugin**)udata;
	p->destroyed++;
	cf_free(udata);
}

static void
cf_plugin_release(void* udata)
{
	((cf_plugin*)udata)->released++;
}

// Record the call and keep the options. Returns the instance udata, or NULL on a failure.
static void*
cf_plugin_call(cf_plugin* p, as_error* err, const as_map* options, as_status* status)
{
	p->calls++;
	as_val_destroy(p->options);
	p->options = as_val_reserve((as_val*)options);

	if (p->fail) {
		*status = as_error_update(err, AEROSPIKE_ERR_CLIENT, "%s unavailable", p->name);
		return NULL;
	}

	cf_plugin** udata = cf_malloc(sizeof(cf_plugin*));
	*udata = p;
	p->created++;
	*status = AEROSPIKE_OK;
	return udata;
}

// Record the configured seeds of the first call.
static as_status
cf_plugin_refresh(as_error* err, const as_seed_candidate_context* context,
	as_seed_candidates* candidates, void* udata)
{
	(void)err;
	(void)candidates;
	cf_plugin* p = *(cf_plugin**)udata;

	if (as_aaf_uint32(&p->refreshes, 1) == 1) {
		size_t len = 0;

		for (uint32_t i = 0; i < context->configured_seeds->size; i++) {
			as_host* host = as_vector_get((as_vector*)context->configured_seeds, i);
			len += snprintf(p->seeds + len, sizeof(p->seeds) - len, "%s%s:%u", i ? "," : "",
				host->name, host->port);

			if (len >= sizeof(p->seeds)) {
				break;
			}
		}
	}
	return AEROSPIKE_OK;
}

static as_status
cf_provider_factory(as_error* err, const as_map* options, as_seed_provider* provider,
	void* udata)
{
	cf_plugin* p = udata;
	as_status status;
	void* instance = cf_plugin_call(p, err, options, &status);

	if (instance) {
		provider->size = sizeof(as_seed_provider);
		provider->api_version = AS_DISCOVERY_API_VERSION;
		provider->name = p->name;
		provider->refresh = p->invalid ? NULL : cf_plugin_refresh;
		provider->needs_periodic_refresh = cf_fake_periodic;
		provider->destroy = cf_plugin_instance_destroy;
		provider->udata = instance;
	}
	return status;
}

static as_status
cf_translator_factory(as_error* err, const as_map* options, as_address_translator* translator,
	void* udata)
{
	cf_plugin* p = udata;
	as_status status;
	void* instance = cf_plugin_call(p, err, options, &status);

	if (instance) {
		translator->size = sizeof(as_address_translator);
		translator->api_version = AS_DISCOVERY_API_VERSION;
		translator->name = p->name;
		translator->translate = p->invalid ? NULL : cf_fake_translate;
		translator->destroy = cf_plugin_instance_destroy;
		translator->udata = instance;
	}
	return status;
}

// Register every factory on config.
static bool
cf_register(as_config* config)
{
	as_error err;

	for (uint32_t i = 0; i < CF_PLUGINS; i++) {
		cf_plugin* p = &cf_providers[i];
		cf_plugin* t = &cf_translators[i];
		as_val_destroy(p->options);
		as_val_destroy(t->options);
		memset(p, 0, sizeof(cf_plugin));
		memset(t, 0, sizeof(cf_plugin));
		p->name = cf_provider_names[i];
		t->name = cf_translator_names[i];
		p->fail = t->fail = i == 1;
		p->invalid = t->invalid = i == 2;

		if (as_config_register_seed_provider(config, p->name, cf_provider_factory,
				cf_plugin_release, p, &err) != AEROSPIKE_OK ||
			as_config_register_address_translator(config, t->name, cf_translator_factory,
				cf_plugin_release, t, &err) != AEROSPIKE_OK) {
			return false;
		}
	}
	return true;
}

// Instances built and not yet destroyed.
static uint32_t
cf_plugins_alive(void)
{
	uint32_t n = 0;

	for (uint32_t i = 0; i < CF_PLUGINS; i++) {
		n += cf_providers[i].created - cf_providers[i].destroyed;
		n += cf_translators[i].created - cf_translators[i].destroyed;
	}
	return n;
}

// Every factory released exactly once.
static bool
cf_plugins_released(void)
{
	for (uint32_t i = 0; i < CF_PLUGINS; i++) {
		if (cf_providers[i].released != 1 || cf_translators[i].released != 1) {
			return false;
		}
	}
	return true;
}

static void
cf_plugins_done(void)
{
	for (uint32_t i = 0; i < CF_PLUGINS; i++) {
		as_val_destroy(cf_providers[i].options);
		as_val_destroy(cf_translators[i].options);
		cf_providers[i].options = NULL;
		cf_translators[i].options = NULL;
	}
}

// as_val_tostring() of val compared with expected.
static bool
cf_val_str_eq(const as_val* val, const char* expected)
{
	char* s = val ? as_val_tostring(val) : NULL;
	bool rv = s && strcmp(s, expected) == 0;

	if (!rv) {
		info("got %s, expected %s", s ? s : "(null)", expected);
	}
	cf_free(s);
	return rv;
}

// Option name of the options a factory received, or NULL.
static as_val*
cf_option(const cf_plugin* p, const char* name)
{
	if (!p->options) {
		return NULL;
	}

	as_string key;
	as_string_init(&key, (char*)name, false);
	return as_map_get((as_map*)p->options, (as_val*)&key);
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

TEST(cf_record_type_any_case, "the dns record_type is accepted in any case: A, a, SRV, srv and Srv")
{
	static const char* const values[] = {"A", "a", "SRV", "srv", "Srv"};

	for (uint32_t i = 0; i < sizeof(values) / sizeof(values[0]); i++) {
		const char* value = values[i];
		char body[CF_BODY_MAX];
		snprintf(body, sizeof(body), CF_HEAD
			"    discovery:\n"
			"      provider: dns\n"
			"      dns:\n"
			"        names: [seeds.example.com]\n"
			"        record_type: %s\n"
			"        servers: ['127.0.0.1:53']\n", value);
		aerospike* client = cf_init_default(body);

		if (!client) {
			fail("record_type %s: no client: %s", value, cf_log_first(AS_LOG_LEVEL_ERROR));
		}

		char line[128];
		snprintf(line, sizeof(line), "Set static.client.discovery.dns.record_type = %s", value);

		if (!client->config.discovery.provider_registered ||
			strcmp(client->config.discovery.provider.name, "dns") != 0 ||
			cf_log_find(AS_LOG_LEVEL_ERROR, "") != 0 ||
			cf_log_find(AS_LOG_LEVEL_INFO, line) != 1) {
			cf_close(client);
			fail("record_type %s: not applied as a dns provider with one info line: %s", value,
				cf_log_first(AS_LOG_LEVEL_ERROR));
		}
		cf_close(client);
	}
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

	// Enumerations. Provider names include the registered ones.
	{"    discovery:\n      provider: eureka\n",
	 "Invalid static.client.discovery.provider: eureka. valid values: static, dns, aws-tags, "
	 "consul, consul-fail, consul-invalid"},
	{"    discovery:\n      merge_policy: union\n",
	 "Invalid static.client.discovery.merge_policy: union. valid values: merge, replace, "
	 "discovery_only"},
	{"    discovery:\n      execution_mode: background\n",
	 "Invalid static.client.discovery.execution_mode: background. valid values: thread, tend"},
	{"    discovery:\n      provider: dns\n      dns:\n        record_type: aaaa\n",
	 "Invalid static.client.discovery.dns.record_type: aaaa. valid values: a, srv"},
	{"    discovery:\n      provider: dns\n      dns:\n        record_type: SRVX\n",
	 "Invalid static.client.discovery.dns.record_type: SRVX. valid values: a, srv"},
	{"    discovery:\n      provider: aws-tags\n      aws:\n        address_field: ipv6\n",
	 "Invalid static.client.discovery.aws.address_field: ipv6. valid values: private_ip, "
	 "public_ip, private_dns, public_dns"},
	{"    translation:\n      provider: nat\n",
	 "Invalid static.client.translation.provider: nat. valid values: static-map, port-map, "
	 "pass-through, nat-table, nat-fail, nat-invalid"},
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
	 "Invalid static.client.translation.port_map: "},

	// Registered factories that fail or return an invalid table.
	{"    discovery:\n      provider: consul-fail\n",
	 "Invalid static.client.discovery.provider: consul-fail. factory failed: consul-fail "
	 "unavailable"},
	{"    discovery:\n      provider: consul-invalid\n",
	 "Invalid static.client.discovery.provider: consul-invalid. factory returned an invalid "
	 "provider"},
	{"    translation:\n      provider: nat-fail\n",
	 "Invalid static.client.translation.provider: nat-fail. factory failed: nat-fail unavailable"},
	{"    translation:\n      provider: nat-invalid\n",
	 "Invalid static.client.translation.provider: nat-invalid. factory returned an invalid "
	 "translator"},
	{"    discovery:\n      provider: consul\n    translation:\n      provider: nat-fail\n",
	 "Invalid static.client.translation.provider: nat-fail. factory failed: nat-fail unavailable"},

	// Built-in sections with a registered name.
	{"    discovery:\n      provider: consul\n      dns:\n        names: [seeds.example.com]\n",
	 "static.client.discovery.dns is only valid with provider dns"},
	{"    discovery:\n      provider: consul\n      aws:\n        region: us-east-1\n",
	 "static.client.discovery.aws is only valid with provider aws-tags"},
	{"    translation:\n      provider: nat-table\n      static_map:\n        10.0.0.1: 192.168.0.1\n",
	 "static.client.translation.static_map is only valid with provider static-map, not "
	 "'nat-table'"},
	{"    translation:\n      provider: nat-table\n      port_map:\n        A1: \"h:3000\"\n",
	 "static.client.translation.port_map is only valid with provider port-map, not 'nat-table'"},

	// Option values SnakeYAML's constructors reject.
	{"    discovery:\n      provider: consul\n      x: !!int abc\n",
	 "Invalid static.client.discovery.x: 'abc' is not a valid !!int"},
	{"    discovery:\n      provider: consul\n      x: !!int 0x\n",
	 "Invalid static.client.discovery.x: '0x' is not a valid !!int"},
	{"    discovery:\n      provider: consul\n      x: !!int 08\n",
	 "Invalid static.client.discovery.x: '08' is not a valid !!int"},
	{"    discovery:\n      provider: consul\n      x: !!int --5\n",
	 "Invalid static.client.discovery.x: '--5' is not a valid !!int"},
	{"    discovery:\n      provider: consul\n      x: !!int 1::30\n",
	 "Invalid static.client.discovery.x: '1::30' is not a valid !!int"},
	{"    discovery:\n      provider: consul\n      x: !!int ''\n",
	 "Invalid static.client.discovery.x: '' is not a valid !!int"},
	{"    discovery:\n      provider: consul\n      x: -10000:00:00:00\n",
	 "Invalid static.client.discovery.x: '-10000:00:00:00' is not a valid !!int"},
	{"    discovery:\n      provider: consul\n      x: !!float 0x1.8\n",
	 "Invalid static.client.discovery.x: '0x1.8' is not a valid !!float"},
	{"    discovery:\n      provider: consul\n      x: !!float infinity\n",
	 "Invalid static.client.discovery.x: 'infinity' is not a valid !!float"},
	{"    discovery:\n      provider: consul\n      x: !!float 1::30\n",
	 "Invalid static.client.discovery.x: '1::30' is not a valid !!float"},
	{"    discovery:\n      provider: consul\n      x: !!float 1.5x\n",
	 "Invalid static.client.discovery.x: '1.5x' is not a valid !!float"},
	{"    discovery:\n      provider: consul\n      x: !!timestamp foo\n",
	 "Invalid static.client.discovery.x: 'foo' is not a valid !!timestamp"},
	{"    discovery:\n      provider: consul\n      x: !!timestamp 2001-12-14 21:59\n",
	 "Invalid static.client.discovery.x: '2001-12-14 21:59' is not a valid !!timestamp"},
	{"    discovery:\n      provider: consul\n      x: <<\n",
	 "Invalid static.client.discovery.x: a YAML merge key (<<) is not a value"},
	{"    discovery:\n      provider: consul\n      x: [!!merge m]\n",
	 "Invalid static.client.discovery.x[0]: a YAML merge key (<<) is not a value"},
	{"    discovery:\n      provider: consul\n      x: {a: [1, {b: !!int z}]}\n",
	 "Invalid static.client.discovery.x.a[1].b: 'z' is not a valid !!int"},
	{"    discovery:\n      provider: consul\n      x: !foo x\n",
	 "Invalid static.client.discovery.x: unsupported tag !foo"},
	{"    discovery:\n      provider: consul\n      x: !!binary aGk=\n",
	 "Invalid static.client.discovery.x: unsupported tag !!binary"},
	{"    translation:\n      provider: nat-table\n      x:\n        - !!omap [a]\n",
	 "Invalid static.client.translation.x[0]: unsupported tag !!omap"},
	{"    discovery:\n      merge_policy: !!int merge\n      provider: consul\n",
	 "Invalid static.client.discovery.merge_policy: 'merge' is not a valid !!int"},

	// Option keys, depth, aliases and merge keys.
	{"    discovery:\n      provider: consul\n      1: a\n",
	 "Invalid key '1' in static.client.discovery: expected a string"},
	{"    discovery:\n      provider: consul\n      x:\n        true: a\n",
	 "Invalid static.client.discovery.x: map keys must be strings or integers"},
	{"    discovery:\n      provider: consul\n      x: {a: [{~: 1}]}\n",
	 "Invalid static.client.discovery.x.a[0]: map keys must be strings or integers"},
	{"    discovery:\n      provider: consul\n      x: {[a]: 1, b: !!int z}\n",
	 "Invalid static.client.discovery.x: map keys must be strings or integers"},

	// Aliases. A scalar alias is its scalar anywhere; an alias to a collection is reused only as
	// a factory option.
	{"    discovery:\n      provider: consul\n      x: &a 1\n      *a : 2\n",
	 "Invalid key '1' in static.client.discovery: expected a string"},
	{"    discovery:\n      provider: consul\n      x: *zz\n",
	 "Undefined YAML alias *zz: line 7, column 10"},
	{"*zz : 1\n", "Undefined YAML alias *zz: line 5, column 1"},
	{"    discovery:\n      provider: static\n      x: [&a 1, *b]\n",
	 "Undefined YAML alias *b: line 7, column 17"},
	{"    extra: &m {a: 1}\n    discovery:\n      provider: consul\n      x: *m\n",
	 "Invalid static.client.discovery.x: alias *m is to a collection outside the discovery and "
	 "translation blocks"},
	{"    discovery:\n      provider: dns\n      dns:\n        names: [seeds.example.com]\n"
	 "        extra: &m {a: 1}\n    translation:\n      provider: nat-table\n      x: *m\n",
	 "Invalid static.client.translation.x: alias *m is to a collection in a built-in section"},
	{"    discovery: &d\n      provider: static\n    translation:\n      provider: nat-table\n"
	 "      x: *d\n",
	 "Invalid static.client.translation.x: alias *d is to a whole discovery or translation block"},
	{"    discovery:\n      provider: consul\n      x: &r [1, *r]\n",
	 "Invalid static.client.discovery.x[1]: alias *r is recursive"},
	{"    discovery:\n      provider: consul\n      m: &m [a]\n      merge_policy: *m\n",
	 "Invalid static.client.discovery.merge_policy: YAML aliases to collections are not supported "
	 "for client settings"},
	{"    discovery:\n      provider: consul\n      m: &m [a]\n      *m : 1\n",
	 "Invalid static.client.discovery: expected scalar keys, not an alias"},
	{"    discovery:\n      provider: dns\n      m: &m [seeds.example.com]\n      dns:\n"
	 "        names: *m\n",
	 "Invalid static.client.discovery.dns.names: YAML aliases to collections are not supported "
	 "for client settings"},
	{"    discovery:\n      provider: dns\n      m: &m [x]\n      dns:\n"
	 "        names: [seeds.example.com]\n        servers: [*m]\n",
	 "Invalid static.client.discovery.dns.servers[0]: YAML aliases to collections are not "
	 "supported for client settings"},
	{"    extra: &m {10.0.0.1: 192.168.0.1}\n    ip_map: *m\n",
	 "Invalid static.client.ip_map: YAML aliases to collections are not supported for client "
	 "settings"},
	{"    extra: &m {a: 1}\n    min_connections_per_node: *m\n",
	 "Invalid static.client.min_connections_per_node: YAML aliases to collections are not "
	 "supported for client settings"},

	// Merge keys.
	{"    discovery:\n      provider: consul\n      x: {<<: 5}\n",
	 "Invalid static.client.discovery.x: expected a mapping or list of mappings for merging, but "
	 "found scalar"},
	{"    discovery:\n      provider: consul\n      x: {<<: [{a: 1}, [b]]}\n",
	 "Invalid static.client.discovery.x: expected a mapping for merging, but found sequence"},
	{"    discovery:\n      provider: consul\n      x: {<<: {a: !!int z}}\n",
	 "Invalid static.client.discovery.x.a: 'z' is not a valid !!int"},
	{"    discovery:\n      provider: static\n      <<: 5\n",
	 "Invalid static.client.discovery: expected a mapping or list of mappings for merging, but "
	 "found scalar"},
	{"    extra: &m {a: 1}\n    discovery:\n      provider: static\n      <<: *m\n",
	 "Invalid static.client.discovery: alias *m is to a collection outside the discovery and "
	 "translation blocks"},
	{"    discovery:\n      provider: static\n      <<: {merge_policy: replace}\n",
	 "Invalid static.client.discovery.merge_policy: client settings from YAML merge keys (<<) "
	 "are not supported"},
	{"    discovery:\n      provider: consul\n      <<: {merge_policy: replace}\n",
	 "Invalid static.client.discovery.merge_policy: client settings from YAML merge keys (<<) "
	 "are not supported"},
	{"    discovery:\n      <<: {provider: consul}\n",
	 "Invalid static.client.discovery.provider: client settings from YAML merge keys (<<) are "
	 "not supported"},
	{"    discovery:\n      provider: dns\n      <<: [{a: 1}, {dns: {names: [x]}}]\n",
	 "Invalid static.client.discovery.dns: client settings from YAML merge keys (<<) are not "
	 "supported"},
	{"    translation:\n      provider: nat-table\n      <<: {on_miss: strict}\n",
	 "Invalid static.client.translation.on_miss: client settings from YAML merge keys (<<) are "
	 "not supported"},
	{"    <<: {max_connections_per_node: 7}\n",
	 "Invalid static.client.<<: YAML merge keys (<<) are only supported in factory options"},
	{"<<: {static: {}}\n", "Invalid <<: YAML merge keys (<<) are only supported in factory options"},
	{"dynamic:\n  read:\n    <<: [{total_timeout: 5}]\n",
	 "Invalid dynamic.read.<<: YAML merge keys (<<) are only supported in factory options"},
	{"dynamic:\n  client:\n    ? !!merge m\n    : {timeout: 5}\n",
	 "Invalid dynamic.client.m: YAML merge keys (<<) are only supported in factory options"},
	{"dynamic:\n  metrics:\n    labels:\n      <<: {a: b}\n",
	 "Invalid dynamic.metrics.labels.<<: YAML merge keys (<<) are only supported in factory "
	 "options"},
	{"dynamic:\n  metrics:\n    extended:\n      <<: {usage: {enabled: true}}\n",
	 "Invalid dynamic.metrics.extended.<<: YAML merge keys (<<) are only supported in factory "
	 "options"},
	{"    discovery:\n      provider: dns\n      dns:\n        <<: {port: 53}\n        names: [x]\n",
	 "Invalid static.client.discovery.dns.<<: YAML merge keys (<<) are only supported in factory "
	 "options"},
	{"    ip_map:\n      <<: {10.0.0.1: 10.0.0.2}\n",
	 "Invalid static.client.ip_map.<<: YAML merge keys (<<) are only supported in factory options"},
	{"    discovery:\n      provider: consul\n      <<: {1: a}\n",
	 "Invalid key '1' in static.client.discovery: expected a string"},

	// Keys that are not scalars end the file at any level.
	{"[a]: b\n", "Invalid top-level key: expected a scalar, not a sequence"},
	{"  {a: 1}: b\n", "Invalid static: expected scalar keys, not a map"},
	{"    [a]: b\n", "Invalid static.client: expected scalar keys, not a sequence"},
	{"dynamic:\n  [a]: b\n", "Invalid dynamic: expected scalar keys, not a sequence"},
	{"dynamic:\n  client:\n    [a]: b\n",
	 "Invalid dynamic.client: expected scalar keys, not a sequence"},
	{"dynamic:\n  read:\n    [a]: b\n",
	 "Invalid dynamic.read: expected scalar keys, not a sequence"},
	{"dynamic:\n  metrics:\n    labels:\n      [a]: b\n",
	 "Invalid dynamic.metrics.labels: expected scalar keys, not a sequence"},
	{"dynamic:\n  metrics:\n    labels: x\n    enable: true\n",
	 "Invalid dynamic.metrics.labels: expected a map, not a scalar"},
	{"dynamic:\n  metrics:\n    extended: [a]\n",
	 "Invalid dynamic.metrics.extended: expected a map, not a sequence"},
	{"dynamic:\n  metrics:\n    labels:\n      a: [1]\n",
	 "Invalid dynamic.metrics.labels.a: expected a scalar, not a sequence"},
	{"dynamic:\n  client:\n    rack_ids: [1, [2]]\n",
	 "Invalid dynamic.client.rack_ids[1]: expected a scalar, not a sequence"},
	{"m: &m [2]\ndynamic:\n  client:\n    rack_ids: [1, *m]\n",
	 "Invalid dynamic.client.rack_ids[1]: YAML aliases to collections are not supported for "
	 "client settings"}
};

// Flow sequences nested depth deep around inner.
static void
cf_nested(char* buf, size_t size, uint32_t depth, const char* inner)
{
	size_t len = 0;

	for (uint32_t i = 0; i < depth && len < size; i++) {
		buf[len++] = '[';
	}

	len += snprintf(buf + len, size - len, "%s", inner);

	for (uint32_t i = 0; i < depth && len < size - 1; i++) {
		buf[len++] = ']';
	}
	buf[len] = '\0';
}

// A consul block whose option x lists count aliases to the map m.
static void
cf_aliases(char* buf, size_t size, uint32_t count)
{
	size_t len = (size_t)snprintf(buf, size,
		"    discovery:\n      provider: consul\n      m: &m {a: 1}\n      x: [");

	for (uint32_t i = 0; i < count && len < size; i++) {
		len += (size_t)snprintf(buf + len, size - len, i == 0 ? "*m" : ", *m");
	}

	if (len < size) {
		snprintf(buf + len, size - len, "]\n");
	}
}

TEST(cf_invalid_keeps_code, "an invalid file is rejected as a whole: code plugins kept, no key applied, one error naming the key")
{
	uint32_t rows = sizeof(cf_invalid_rows) / sizeof(cf_invalid_rows[0]);

	// SnakeYAML's limit counts every open collection of the file: an option value opens inside
	// four, so 48 empty lists are too deep; a skipped dns value inside five, so a scalar in 46
	// lists is; an unknown top-level section inside one.
	char deep[3][160];
	char deep_body[3][256];
	cf_nested(deep[0], sizeof(deep[0]), 48, "");
	cf_nested(deep[1], sizeof(deep[1]), 46, "1");
	cf_nested(deep[2], sizeof(deep[2]), 51, "");
	snprintf(deep_body[0], sizeof(deep_body[0]),
		"    discovery:\n      provider: consul\n      x: %s\n", deep[0]);
	snprintf(deep_body[1], sizeof(deep_body[1]),
		"    discovery:\n      provider: dns\n      dns:\n        extra: %s\n", deep[1]);
	snprintf(deep_body[2], sizeof(deep_body[2]), "zz: %s\n", deep[2]);

	// SnakeYAML's limit of 50 aliases to collections; the 51st starts at column 211.
	char aliases[512];
	cf_aliases(aliases, sizeof(aliases), 51);
	const cf_invalid deep_rows[] = {
		{deep_body[0], "YAML nesting deeper than 50 levels: line 7, column 57"},
		{deep_body[1], "YAML nesting deeper than 50 levels: line 8, column 62"},
		{deep_body[2], "YAML nesting deeper than 50 levels: line 5, column 55"},
		{aliases, "More than 50 YAML aliases to collections: line 8, column 211"}
	};
	uint32_t total = rows + sizeof(deep_rows) / sizeof(deep_rows[0]);

	for (uint32_t i = 0; i < total; i++) {
		const cf_invalid* row = i < rows ? &cf_invalid_rows[i] : &deep_rows[i - rows];
		char body[CF_BODY_MAX];
		snprintf(body, sizeof(body), CF_HEAD "    max_connections_per_node: 60\n%s", row->body);

		as_config config;
		assert_true(cf_code_config(&config));
		assert_true(cf_register(&config));
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
		uint32_t alive = cf_plugins_alive();

		cf_close(client);
		cf_plugins_done();

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

		if (alive != 0 || cf_plugins_alive() != 0 || !cf_plugins_released()) {
			fail("row %u: factory instances alive %u after the file, %u after close", i, alive,
				cf_plugins_alive());
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
// Tests: registered factories
//---------------------------------

#define CF_REGISTERED CF_HEAD \
	"    max_connections_per_node: 99\n" \
	"    discovery:\n" \
	"      provider: consul\n" \
	"      merge_policy: merge\n" \
	"      service: aerospike\n" \
	"      endpoints:\n" \
	"        - 127.0.0.1:1\n" \
	"    translation:\n" \
	"      provider: nat-table\n" \
	"      on_miss: strict\n" \
	"      table:\n" \
	"        \"10.0.0.1:3000\": \"127.0.0.1:1\"\n"

#define CF_REGISTERED_NAME_ONLY CF_HEAD \
	"    discovery:\n" \
	"      provider: consul\n" \
	"    translation:\n" \
	"      provider: nat-table\n"

TEST(cf_registered_options, "a registered provider and translator are built from the file with every other key as options")
{
	as_config config;
	as_config_init(&config);
	assert_true(cf_register(&config));
	aerospike* client = cf_init(&config, CF_HEAD
		"    max_connections_per_node: 99\n"
		"    discovery:\n"
		"      provider: consul\n"
		"      merge_policy: merge\n"
		"      service: aerospike\n"
		"      endpoints:\n"
		"        - 127.0.0.1:1\n"
		"      options:\n"
		"        a: 1\n"
		"    translation:\n"
		"      provider: nat-table\n"
		"      on_miss: strict\n"
		"      table:\n"
		"        \"10.0.0.1:3000\": \"127.0.0.1:1\"\n");
	assert_not_null(client);

	as_config* c = &client->config;
	cf_plugin* p = &cf_providers[0];
	cf_plugin* t = &cf_translators[0];
	bool built = c->discovery.provider_registered &&
		strcmp(c->discovery.provider.name, "consul") == 0 &&
		c->translation.provider_registered &&
		strcmp(c->translation.provider.name, "nat-table") == 0 && p->calls == 1 && t->calls == 1;
	bool applied = c->discovery.merge_policy == AS_SEED_MERGE &&
		c->translation.on_miss == AS_TRANSLATION_STRICT && c->max_conns_per_node == 99;
	uint32_t errors = cf_log_find(AS_LOG_LEVEL_ERROR, "");
	uint32_t unexpected = cf_log_find(AS_LOG_LEVEL_INFO, "Unexpected field");
	uint32_t provider_set = cf_log_find(AS_LOG_LEVEL_INFO,
		"Set static.client.discovery.provider = consul");
	uint32_t translator_set = cf_log_find(AS_LOG_LEVEL_INFO,
		"Set static.client.translation.provider = nat-table");
	cf_close(client);

	assert_true(built);
	assert_true(applied);
	assert_int_eq(errors, 0);
	assert_int_eq(unexpected, 0);
	assert_int_eq(provider_set, 1);
	assert_int_eq(translator_set, 1);
	assert_true(cf_val_str_eq(p->options, "{\"endpoints\":[\"127.0.0.1:1\"], "
		"\"merge_policy\":\"merge\", \"options\":{\"a\":1}, \"service\":\"aerospike\"}"));
	assert_true(cf_val_str_eq(t->options, "{\"on_miss\":\"strict\", "
		"\"table\":{\"10.0.0.1:3000\":\"127.0.0.1:1\"}}"));
	assert_int_eq(cf_plugins_alive(), 0);
	assert_true(cf_plugins_released());
	assert_int_eq(cf_providers[1].calls + cf_providers[2].calls, 0);
	cf_plugins_done();
}

TEST(cf_registered_name_only, "a registered name alone gives empty options and replaces the code plugins")
{
	as_config config;
	assert_true(cf_code_config(&config));
	assert_true(cf_register(&config));
	aerospike* client = cf_init(&config, CF_REGISTERED_NAME_ONLY);
	assert_not_null(client);

	bool built = client->config.discovery.provider_registered &&
		strcmp(client->config.discovery.provider.name, "consul") == 0 &&
		strcmp(client->config.translation.provider.name, "nat-table") == 0;
	uint32_t provider_warn = cf_log_find(AS_LOG_LEVEL_WARN, "Seed provider code-provider "
		"registered in code is replaced by static.client.discovery.provider consul in ");
	uint32_t translator_warn = cf_log_find(AS_LOG_LEVEL_WARN, "Address translator "
		"code-translator registered in code is replaced by static.client.translation.provider "
		"nat-table in ");
	cf_close(client);

	assert_true(built);
	assert_int_eq(provider_warn, 1);
	assert_int_eq(translator_warn, 1);
	assert_int_eq(cf_provider_destroys, 1);
	assert_int_eq(cf_translator_destroys, 1);
	assert_true(cf_val_str_eq(cf_providers[0].options, "{}"));
	assert_true(cf_val_str_eq(cf_translators[0].options, "{}"));
	assert_int_eq(cf_plugins_alive(), 0);
	assert_true(cf_plugins_released());
	cf_plugins_done();
}

TEST(cf_unregistered_name, "an unregistered provider name lists only the built-ins when nothing is registered")
{
	aerospike* client = cf_init_default(CF_REGISTERED_NAME_ONLY);
	assert_not_null(client);
	char first[CF_LOG_SIZE];
	snprintf(first, sizeof(first), "%s", cf_log_first(AS_LOG_LEVEL_ERROR));
	bool registered = client->config.discovery.provider_registered ||
		client->config.translation.provider_registered;
	cf_close(client);

	assert_false(registered);
	const char* expected =
		"Invalid static.client.discovery.provider: consul. valid values: static, dns, aws-tags";
	const char* found = strstr(first, expected);

	if (!found || strcmp(found, expected) != 0) {
		fail("unexpected error: %s", first);
	}
}

TEST(cf_registered_staging, "a translator factory failure after the provider factory succeeded destroys the provider once and keeps the code plugins")
{
	as_config config;
	assert_true(cf_code_config(&config));
	assert_true(cf_register(&config));
	aerospike* client = cf_init(&config, CF_HEAD
		"    discovery:\n"
		"      provider: consul\n"
		"    translation:\n"
		"      provider: nat-fail\n");
	assert_not_null(client);

	cf_plugin* p = &cf_providers[0];
	uint32_t calls = p->calls;
	uint32_t destroyed = p->destroyed;
	bool kept = cf_code_registered(client) && cf_provider_destroys == 0 &&
		cf_translator_destroys == 0;
	uint32_t errors = cf_log_find(AS_LOG_LEVEL_ERROR, "Invalid static.client.translation."
		"provider: nat-fail. factory failed: nat-fail unavailable");
	cf_close(client);

	assert_int_eq(calls, 1);
	assert_int_eq(destroyed, 1);
	assert_true(kept);
	assert_int_eq(errors, 1);
	assert_int_eq(cf_translators[1].calls, 1);
	assert_int_eq(cf_plugins_alive(), 0);
	assert_true(cf_plugins_released());
	assert_int_eq(cf_provider_destroys, 1);
	assert_int_eq(cf_translator_destroys, 1);
	cf_plugins_done();
}

TEST(cf_registrations_carry, "registrations made before aerospike_init carry into the client, where the same name is a duplicate")
{
	as_config config;
	as_config_init(&config);
	assert_true(cf_register(&config));
	aerospike* client = cf_init(&config, CF_REGISTERED_NAME_ONLY);
	assert_not_null(client);

	bool built = client->config.discovery.provider_registered &&
		strcmp(client->config.discovery.provider.name, "consul") == 0;
	as_error err;
	as_status duplicate = as_config_register_seed_provider(&client->config, "consul",
		cf_provider_factory, cf_plugin_release, &cf_providers[0], &err);
	bool message = strstr(err.message, "'consul': already registered") != NULL;

	// A later registration on the client is released with the others.
	uint32_t etcd_released = 0;
	as_status added = as_config_register_seed_provider(&client->config, "etcd",
		cf_provider_factory, cf_fake_destroy, &etcd_released, &err);
	cf_close(client);

	assert_true(built);
	assert_int_eq(duplicate, AEROSPIKE_ERR_PARAM);
	assert_true(message);
	assert_int_eq(added, AEROSPIKE_OK);
	assert_int_eq(etcd_released, 1);
	assert_true(cf_plugins_released());
	cf_plugins_done();
}

static cf_plugin cf_binding;

// Register cf_binding, reset, as a binding factory on config.
static bool
cf_register_binding(as_config* config)
{
	as_error err;
	as_val_destroy(cf_binding.options);
	memset(&cf_binding, 0, sizeof(cf_plugin));
	cf_binding.name = "py-consul";
	return as_config_register_binding_seed_provider(config, cf_binding.name, cf_provider_factory,
		cf_plugin_release, &cf_binding, &err) == AEROSPIKE_OK;
}

static void
cf_binding_done(void)
{
	as_val_destroy(cf_binding.options);
	cf_binding.options = NULL;
}

#define CF_BINDING_MODE "Binding seed provider requires thread execution mode"

TEST(cf_registered_binding, "a binding name from the file gives a binding provider; with execution_mode tend the file applies, the factory is not called and aerospike_connect fails")
{
	as_config config;
	as_config_init(&config);
	assert_true(cf_register_binding(&config));
	aerospike* client = cf_init(&config, CF_HEAD
		"    discovery:\n"
		"      provider: py-consul\n"
		"      service: aerospike\n");
	assert_not_null(client);

	bool binding = client->config.discovery.provider_registered &&
		client->config.discovery.binding_provider &&
		strcmp(client->config.discovery.provider.name, "py-consul") == 0;
	uint32_t errors = cf_log_find(AS_LOG_LEVEL_ERROR, "");
	cf_close(client);

	assert_true(binding);
	assert_int_eq(errors, 0);
	assert_int_eq(cf_binding.calls, 1);
	assert_true(cf_val_str_eq(cf_binding.options, "{\"service\":\"aerospike\"}"));
	assert_int_eq(cf_binding.destroyed, 1);
	assert_int_eq(cf_binding.released, 1);

	// With execution_mode tend the file is accepted, every key applies and the code provider
	// is replaced, but only the recipe is kept: neither init nor connect calls the factory.
	assert_true(cf_code_config(&config));
	assert_true(cf_register_binding(&config));
	client = cf_init(&config, CF_HEAD
		"    max_connections_per_node: 77\n"
		"    discovery:\n"
		"      provider: py-consul\n"
		"      execution_mode: tend\n"
		"      merge_policy: discovery_only\n");
	assert_not_null(client);

	as_config* c = &client->config;
	bool applied = c->discovery.execution_mode == AS_DISCOVERY_EXECUTION_TEND &&
		c->discovery.merge_policy == AS_SEED_DISCOVERY_ONLY && c->max_conns_per_node == 77;
	bool deferred = !c->discovery.provider_registered && !c->discovery.binding_provider &&
		!c->discovery.provider_released && c->discovery.recipe && c->discovery.recipe->binding;
	uint32_t replaced = cf_log_find(AS_LOG_LEVEL_WARN,
		"Seed provider code-provider registered in code is replaced by "
		"static.client.discovery.provider py-consul");
	uint32_t code_destroys = cf_provider_destroys;
	errors = cf_log_find(AS_LOG_LEVEL_ERROR, "");
	uint32_t init_calls = cf_binding.calls;
	as_error err;
	as_status status = aerospike_connect(client, &err);
	bool message = strstr(err.message, CF_BINDING_MODE) != NULL;
	bool connected = client->cluster != NULL;
	uint32_t calls = cf_binding.calls;
	cf_close(client);

	assert_true(applied);
	assert_true(deferred);
	assert_int_eq(replaced, 1);
	assert_int_eq(code_destroys, 1);
	assert_int_eq(errors, 0);
	assert_int_eq(init_calls, 0);
	assert_int_eq(status, AEROSPIKE_ERR_PARAM);
	assert_true(message);
	assert_false(connected);
	assert_int_eq(calls, 0);
	assert_int_eq(cf_binding.created, 0);
	assert_int_eq(cf_binding.released, 1);
	cf_binding_done();
}

typedef struct cf_typed_s {
	const char* yaml;
	as_val_t type;
	const char* expected;
} cf_typed;

static const cf_typed cf_typed_rows[] = {
	// Null.
	{"~", AS_NIL, "NIL"},
	{"null", AS_NIL, "NIL"},
	{"Null", AS_NIL, "NIL"},
	{"NULL", AS_NIL, "NIL"},
	{"", AS_NIL, "NIL"},
	{"nULL", AS_STRING, "\"nULL\""},

	// Bool.
	{"yes", AS_BOOLEAN, "true"},
	{"No", AS_BOOLEAN, "false"},
	{"TRUE", AS_BOOLEAN, "true"},
	{"false", AS_BOOLEAN, "false"},
	{"On", AS_BOOLEAN, "true"},
	{"OFF", AS_BOOLEAN, "false"},
	{"y", AS_STRING, "\"y\""},
	{"tRuE", AS_STRING, "\"tRuE\""},

	// Int.
	{"0", AS_INTEGER, "0"},
	{"42", AS_INTEGER, "42"},
	{"-17", AS_INTEGER, "-17"},
	{"+5", AS_INTEGER, "5"},
	{"1_000", AS_INTEGER, "1000"},
	{"017", AS_INTEGER, "15"},
	{"0x1F", AS_INTEGER, "31"},
	{"-0x_1f", AS_INTEGER, "-31"},
	{"0b101", AS_INTEGER, "5"},
	{"1:30", AS_INTEGER, "90"},
	{"-1:30", AS_INTEGER, "-90"},
	{"9223372036854775807", AS_INTEGER, "9223372036854775807"},
	{"-9223372036854775808", AS_INTEGER, "-9223372036854775808"},
	{"9223372036854775808", AS_STRING, "\"9223372036854775808\""},
	{"09", AS_STRING, "\"09\""},
	{"0x", AS_STRING, "\"0x\""},

	// Float.
	{"1.5", AS_DOUBLE, "1.5"},
	{"1.", AS_DOUBLE, "1"},
	{"-.5", AS_DOUBLE, "-0.5"},
	{"1e5", AS_DOUBLE, "100000"},
	{"1_0.2_5", AS_DOUBLE, "10.25"},
	{"1:30.5", AS_DOUBLE, "90.5"},
	{".inf", AS_DOUBLE, "inf"},
	{"-.Inf", AS_DOUBLE, "-inf"},
	{".NaN", AS_DOUBLE, "nan"},
	{"+.nan", AS_STRING, "\"+.nan\""},
	{"1e", AS_STRING, "\"1e\""},

	// Strings: quoted, timestamps and the rest.
	{"'yes'", AS_STRING, "\"yes\""},
	{"\"5\"", AS_STRING, "\"5\""},
	{"2001-12-14", AS_STRING, "\"2001-12-14\""},
	{"2001-12-14t21:59:43.10-05:00", AS_STRING, "\"2001-12-14t21:59:43.10-05:00\""},
	{"hello world", AS_STRING, "\"hello world\""},

	// Base 60 wraps as Java's 32-bit int does.
	{"100000:00:00:00", AS_INTEGER, "125163520"},
	{"10000:00:00:00", AS_INTEGER, "-2134967296"},
	{"-0x8000000000000000", AS_INTEGER, "-9223372036854775808"},

	// Tags: the non-specific ! resolves any style; a core tag runs SnakeYAML's constructor.
	{"!!str 5", AS_STRING, "\"5\""},
	{"! 5", AS_INTEGER, "5"},
	{"! '5'", AS_INTEGER, "5"},
	{"! \"yes\"", AS_BOOLEAN, "true"},
	{"!!null ~", AS_NIL, "NIL"},
	{"!!null x", AS_NIL, "NIL"},
	{"!!bool yes", AS_BOOLEAN, "true"},
	{"!!bool tRuE", AS_BOOLEAN, "true"},
	{"!!bool YES", AS_BOOLEAN, "true"},
	{"!!bool maybe", AS_NIL, "NIL"},
	{"!!bool 1", AS_NIL, "NIL"},
	{"!!int \"5\"", AS_INTEGER, "5"},
	{"!!int _1", AS_INTEGER, "1"},
	{"!!int ++5", AS_INTEGER, "5"},
	{"!!int +-5", AS_INTEGER, "-5"},
	{"!!int 0x-5", AS_INTEGER, "-5"},
	{"!!int 1:-5", AS_INTEGER, "55"},
	{"!!int 1:+5", AS_INTEGER, "65"},
	{"!!int '1:'", AS_INTEGER, "1"},
	{"!!int ':'", AS_INTEGER, "0"},
	{"!!int 4294967296:0", AS_INTEGER, "0"},
	{"!!int 9223372036854775808", AS_STRING, "\"9223372036854775808\""},
	{"!!float 1.5", AS_DOUBLE, "1.5"},
	{"!!float 1", AS_DOUBLE, "1"},
	{"!!float 1_000", AS_DOUBLE, "1000"},
	{"!!float 1e3", AS_DOUBLE, "1000"},
	{"!!float .5f", AS_DOUBLE, "0.5"},
	{"!!float 1.e5", AS_DOUBLE, "100000"},
	{"!!float ' 1.5 '", AS_DOUBLE, "1.5"},
	{"!!float Infinity", AS_DOUBLE, "inf"},
	{"!!float --1", AS_DOUBLE, "1"},
	{"!!float 0x1.8p1", AS_DOUBLE, "3"},
	{"!!float 0x.8p1", AS_DOUBLE, "1"},
	{"!!float 1:30.5e1", AS_DOUBLE, "365"},
	{"!!float 1:-30", AS_DOUBLE, "30"},
	{"!!float 1.5e3:0", AS_DOUBLE, "90000"},
	{"!!float ':'", AS_DOUBLE, "0"},
	{"!!float 1000000000000000000000000000000.0e-30", AS_DOUBLE, "1"},
	{"!!float 0.0000000000000000000000000000001e31", AS_DOUBLE, "1"},
	{"!!float 1e400", AS_DOUBLE, "inf"},
	{"!!float -1e99999999999999999999", AS_DOUBLE, "-inf"},
	{"!!float 1e-99999999999999999999", AS_DOUBLE, "0"},
	{"!!timestamp 2001-12-14", AS_STRING, "\"2001-12-14\""},
	{"!!timestamp 2001-12-14 21:59:43.10 -5", AS_STRING, "\"2001-12-14 21:59:43.10 -5\""},
	{"!!map {a: 1}", AS_MAP, "{\"a\":1}"},
	{"!!seq [1]", AS_LIST, "[1]"},

	// Collections: integer keys, duplicate keys (the last wins) and nesting.
	{"{b: [x, {c: ~}], 1: a}", AS_MAP, "{1:\"a\", \"b\":[\"x\", {\"c\":NIL}]}"},
	{"{a: 1, a: 2}", AS_MAP, "{\"a\":2}"},
	{"[]", AS_LIST, "[]"},
	{"{\"<<\": 1}", AS_MAP, "{\"<<\":1}"},

	// Aliases: a scalar keeps its tag and style, an anchor redefined replaces the earlier one,
	// and a collection is the same value again.
	{"[&x '5', *x, &y !!float 1, *y]", AS_LIST, "[\"5\", \"5\", 1, 1]"},
	{"[&r a, &r b, *r]", AS_LIST, "[\"a\", \"b\", \"b\"]"},
	{"[&k a, {*k : 1}]", AS_LIST, "[\"a\", {\"a\":1}]"},
	{"[{&k <<: {a: 1}}, {*k : {b: 2}}]", AS_LIST, "[{\"a\":1}, {\"b\":2}]"},
	{"[&m {a: [1]}, *m, {b: *m}]", AS_LIST, "[{\"a\":[1]}, {\"a\":[1]}, {\"b\":{\"a\":[1]}}]"},

	// Merge keys: explicit keys win, the last duplicate explicit key wins, the earlier source
	// wins, and an anchored source keeps its own merges.
	{"{<<: {a: 1, b: 2}, b: 3}", AS_MAP, "{\"a\":1, \"b\":3}"},
	{"{<<: {a: 1}, a: 2, a: 3}", AS_MAP, "{\"a\":3}"},
	{"{<<: [{a: 1}, {a: 2, b: 2}]}", AS_MAP, "{\"a\":1, \"b\":2}"},
	{"{<<: {a: 1}, <<: {a: 2, b: 2}}", AS_MAP, "{\"a\":1, \"b\":2}"},
	{"{!!merge m: {a: 1}}", AS_MAP, "{\"a\":1}"},
	{"{<<: [], b: 1}", AS_MAP, "{\"b\":1}"},
	{"{<<: {}}", AS_MAP, "{}"},
	{"[&m {<<: {a: 1}, a: 2}, {<<: *m}]", AS_LIST, "[{\"a\":2}, {\"a\":2}]"},
	{"[&m {a: 1}, &n {b: 2}, {<<: [*m, *n], b: 3}]", AS_LIST,
	 "[{\"a\":1}, {\"b\":2}, {\"a\":1, \"b\":3}]"},

	// Where SnakeYAML keeps the first: an inline source's duplicate key and nested merge.
	{"{<<: {a: 1, a: 2}}", AS_MAP, "{\"a\":2}"},
	{"{<<: {<<: {a: 1}, a: 2}}", AS_MAP, "{\"a\":2}"}
};

TEST(cf_registered_typing, "option values follow SnakeYAML's resolver and tag constructors, and collections keep their types")
{
	uint32_t rows = sizeof(cf_typed_rows) / sizeof(cf_typed_rows[0]);
	// The deepest accepted nesting under the block prints as written.
	char deep[2][160];
	cf_nested(deep[0], sizeof(deep[0]), 47, "");
	cf_nested(deep[1], sizeof(deep[1]), 46, "1");
	const cf_typed deep_rows[] = {{deep[0], AS_LIST, deep[0]}, {deep[1], AS_LIST, deep[1]}};
	uint32_t total = rows + sizeof(deep_rows) / sizeof(deep_rows[0]);

	for (uint32_t i = 0; i < total; i++) {
		const cf_typed* row = i < rows ? &cf_typed_rows[i] : &deep_rows[i - rows];
		char body[CF_BODY_MAX];
		snprintf(body, sizeof(body), CF_HEAD
			"    discovery:\n"
			"      provider: consul\n"
			"      v: %s\n", row->yaml);

		as_config config;
		as_config_init(&config);
		assert_true(cf_register(&config));
		aerospike* client = cf_init(&config, body);
		assert_not_null(client);

		char first[CF_LOG_SIZE];
		snprintf(first, sizeof(first), "%s", cf_log_first(AS_LOG_LEVEL_ERROR));
		cf_close(client);

		as_val* v = cf_option(&cf_providers[0], "v");
		as_val_t type = v ? as_val_type(v) : AS_UNDEF;
		bool ok = type == row->type && cf_val_str_eq(v, row->expected);
		cf_plugins_done();

		if (!ok) {
			fail("row %u '%s': type %d, expected %d. %s", i, row->yaml, type, row->type, first);
		}
	}
}

TEST(cf_registered_keys, "option keys: a long key arrives whole, a quoted << is a plain key, and non-string keys are skipped for a built-in provider")
{
	char key[301];
	memset(key, 'k', sizeof(key) - 1);
	key[sizeof(key) - 1] = '\0';

	char body[CF_BODY_MAX];
	snprintf(body, sizeof(body), CF_HEAD
		"    discovery:\n"
		"      provider: consul\n"
		"      %s: long\n"
		"      \"<<\": quoted\n"
		"      service: first\n"
		"      service: second\n", key);

	as_config config;
	as_config_init(&config);
	assert_true(cf_register(&config));
	aerospike* client = cf_init(&config, body);
	assert_not_null(client);
	uint32_t errors = cf_log_find(AS_LOG_LEVEL_ERROR, "");
	cf_close(client);

	cf_plugin* p = &cf_providers[0];
	bool ok = cf_val_str_eq(cf_option(p, key), "\"long\"") &&
		cf_val_str_eq(cf_option(p, "<<"), "\"quoted\"") &&
		cf_val_str_eq(cf_option(p, "service"), "\"second\"") &&
		as_map_size((as_map*)p->options) == 3;
	cf_plugins_done();
	assert_int_eq(errors, 0);
	assert_true(ok);

	// With a built-in provider these keys are unexpected fields.
	client = cf_init_default(CF_HEAD
		"    discovery:\n"
		"      provider: static\n"
		"      1: a\n"
		"      <<: {a: 1}\n"
		"      x: !foo y\n"
		"      y:\n"
		"        true: a\n");
	assert_not_null(client);
	errors = cf_log_find(AS_LOG_LEVEL_ERROR, "");
	static const char* const lines[] = {
		"Unexpected field: static.client.discovery.1",
		"Unexpected field: static.client.discovery.a",
		"Unexpected field: static.client.discovery.x",
		"Unexpected field: static.client.discovery.y"
	};
	uint32_t found = 0;

	for (uint32_t i = 0; i < sizeof(lines) / sizeof(lines[0]); i++) {
		found += cf_log_find(AS_LOG_LEVEL_INFO, lines[i]) == 1;
	}
	cf_close(client);
	assert_int_eq(errors, 0);
	assert_int_eq(found, 4);
}

TEST(cf_registered_aliases, "aliases and merge keys: scalar aliases anywhere, collection aliases across blocks and in skipped content, block merges under explicit keys")
{
	// Scalar anchors at the top of the file reach client settings and options; collections are
	// shared between the blocks, and skipped content may alias anything.
	as_config config;
	as_config_init(&config);
	assert_true(cf_register(&config));
	aerospike* client = cf_init(&config,
		"version: 1.2.0\n"
		"anchors:\n"
		"  policy: &p replace\n"
		"  service: &s aerospike\n"
		"  skipped: &k {x: 1}\n"
		"static:\n"
		"  client:\n"
		"    extra: [*k, *p]\n"
		"    max_connections_per_node: &c 77\n"
		"    discovery:\n"
		"      provider: consul\n"
		"      merge_policy: *p\n"
		"      service: *s\n"
		"      conns: *c\n"
		"      shared: &t {a: [1]}\n"
		"      <<: [{port: 1, tags: [x]}, {port: 2, zone: z}]\n"
		"      port: 3\n"
		"    translation:\n"
		"      provider: nat-table\n"
		"      table: *t\n"
		"      on_miss: strict\n"
		"other:\n"
		"  unknown: {y: *k}\n");
	assert_not_null(client);

	as_config* c = &client->config;
	bool applied = c->discovery.merge_policy == AS_SEED_REPLACE && c->max_conns_per_node == 77 &&
		c->translation.on_miss == AS_TRANSLATION_STRICT;
	uint32_t errors = cf_log_find(AS_LOG_LEVEL_ERROR, "");
	cf_close(client);

	bool options = cf_val_str_eq(cf_providers[0].options, "{\"conns\":77, "
		"\"merge_policy\":\"replace\", \"port\":3, \"service\":\"aerospike\", "
		"\"shared\":{\"a\":[1]}, \"tags\":[\"x\"], \"zone\":\"z\"}") &&
		cf_val_str_eq(cf_translators[0].options, "{\"on_miss\":\"strict\", "
		"\"table\":{\"a\":[1]}}");
	cf_plugins_done();
	assert_int_eq(errors, 0);
	assert_true(applied);
	assert_true(options);

	// An explicit client setting wins over a merged one; with a built-in provider merged keys
	// are unexpected fields, and a scalar alias reaches a built-in section.
	client = cf_init_default(CF_HEAD
		"    discovery:\n"
		"      provider: dns\n"
		"      merge_policy: merge\n"
		"      <<: {merge_policy: replace, foo: 1, 2: x}\n"
		"      name: &n seeds.example.com\n"
		"      dns:\n"
		"        names: [*n]\n");
	assert_not_null(client);
	bool merged = client->config.discovery.merge_policy == AS_SEED_MERGE;
	errors = cf_log_find(AS_LOG_LEVEL_ERROR, "");
	uint32_t foo = cf_log_find(AS_LOG_LEVEL_INFO, "Unexpected field: static.client.discovery.foo");
	uint32_t two = cf_log_find(AS_LOG_LEVEL_INFO, "Unexpected field: static.client.discovery.2");
	uint32_t names = cf_log_find(AS_LOG_LEVEL_INFO,
		"Set static.client.discovery.dns.names = [seeds.example.com]");
	cf_close(client);
	assert_int_eq(errors, 0);
	assert_true(merged);
	assert_int_eq(foo, 1);
	assert_int_eq(two, 1);
	assert_int_eq(names, 1);

	// SnakeYAML's limit allows 50 aliases to collections.
	char body[CF_BODY_MAX];
	char aliases[512];
	cf_aliases(aliases, sizeof(aliases), 50);
	snprintf(body, sizeof(body), CF_HEAD "%s", aliases);
	as_config_init(&config);
	assert_true(cf_register(&config));
	client = cf_init(&config, body);
	assert_not_null(client);
	errors = cf_log_find(AS_LOG_LEVEL_ERROR, "");
	cf_close(client);
	as_val* x = cf_option(&cf_providers[0], "x");
	uint32_t size = as_val_type(x) == AS_LIST ? as_list_size((as_list*)x) : 0;
	cf_plugins_done();
	assert_int_eq(errors, 0);
	assert_int_eq(size, 50);
}

#if !defined(_MSC_VER)
// Locales whose decimal point is a comma; machines have some of them.
static const char* const cf_comma_locales[] = {
	"de_DE.UTF-8", "de_DE.utf8", "en_DK.UTF-8", "en_DK.utf8", "fr_FR.UTF-8", "fr_FR.utf8",
	"nl_NL.UTF-8", "nl_NL.utf8"
};
#endif

TEST(cf_registered_locale, "option floats do not depend on the locale's decimal point")
{
#if defined(_MSC_VER)
	info("skipped: no per-thread locale");
#else
	locale_t loc = (locale_t)0;

	for (uint32_t i = 0; !loc && i < sizeof(cf_comma_locales) / sizeof(cf_comma_locales[0]); i++) {
		loc = newlocale(LC_NUMERIC_MASK, cf_comma_locales[i], (locale_t)0);
	}

	if (!loc) {
		info("skipped: no locale with a comma decimal point");
		return;
	}

	// The file is read on the calling thread, so only this thread's locale changes.
	locale_t prev = uselocale(loc);
	bool comma = strcmp(localeconv()->decimal_point, ",") == 0;

	as_config config;
	as_config_init(&config);
	assert_true(cf_register(&config));
	aerospike* client = cf_init(&config, CF_HEAD
		"    discovery:\n"
		"      provider: consul\n"
		"      a: 1.5\n"
		"      b: !!float 12.5e-1\n"
		"      c: 1:30.5\n"
		"      d: !!float 0x1.8p1\n"
		"      e: -.25\n");
	uselocale(prev);
	freelocale(loc);
	assert_not_null(client);
	uint32_t errors = cf_log_find(AS_LOG_LEVEL_ERROR, "");
	cf_close(client);

	static const char* const names[] = {"a", "b", "c", "d", "e"};
	static const double expected[] = {1.5, 1.25, 90.5, 3.0, -0.25};
	uint32_t matched = 0;

	for (uint32_t i = 0; i < sizeof(names) / sizeof(names[0]); i++) {
		as_double* d = as_double_fromval(cf_option(&cf_providers[0], names[i]));

		if (d && as_double_get(d) == expected[i]) {
			matched++;
		}
		else {
			info("option %s: %s", names[i], d ? "wrong value" : "not a double");
		}
	}
	cf_plugins_done();
	assert_true(comma);
	assert_int_eq(errors, 0);
	assert_int_eq(matched, 5);
#endif
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

	// One silent resolver gets up to 3/4 of timeout_ms. Without timeout_ms the provider, called
	// directly outside a seed manager, uses the 2000 ms default, so it ends later.
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
	assert_int_eq(status, AEROSPIKE_ERR_CLIENT);
	assert_int_eq(default_status, AEROSPIKE_ERR_CLIENT);

	if (yaml_ms < 250 || yaml_ms > 5000 || default_ms < 1000 || default_ms > 5000 ||
		default_ms <= yaml_ms) {
		fail("refresh took %" PRIu64 " ms with timeout_ms 600 and %" PRIu64 " ms by default",
			yaml_ms, default_ms);
	}
}

// QTYPE of the first query waiting on fd, or -1 when there is none or it is malformed.
static int
cf_dns_first_qtype(int fd)
{
	uint8_t buf[512];
	ssize_t n = recv(fd, buf, sizeof(buf), MSG_DONTWAIT);

	if (n < 12) {
		return -1;
	}

	size_t pos = 12;

	while (pos < (size_t)n && buf[pos] != 0) {
		pos += 1 + buf[pos];
	}

	if (pos + 3 > (size_t)n) {
		return -1;
	}
	return (buf[pos + 1] << 8) | buf[pos + 2];
}

// The srv forms show that the value is applied. For a and A the QTYPE is also the default's, so
// cf_record_type_any_case, which checks that they are accepted and logged, is their proof.
TEST(cf_dns_record_type_query, "the dns record_type in any case sets the query type: A or AAAA for a forms, SRV for srv forms")
{
	static const char* const values[] = {"A", "a", "SRV", "srv", "Srv"};

	for (uint32_t i = 0; i < sizeof(values) / sizeof(values[0]); i++) {
		const char* value = values[i];
		bool srv = value[0] == 's' || value[0] == 'S';
		uint16_t port = 0;
		int fd = cf_silent_udp(&port);
		assert_true(fd >= 0);

		char body[CF_BODY_MAX];
		snprintf(body, sizeof(body), CF_HEAD
			"    discovery:\n"
			"      provider: dns\n"
			"      dns:\n"
			"        names: [seeds.example.com]\n"
			"        record_type: %s\n"
			"        timeout_ms: 200\n"
			"        servers: ['127.0.0.1:%u']\n", value, port);
		aerospike* client = cf_init_default(body);

		if (!client) {
			close(fd);
			fail("record_type %s: no client: %s", value, cf_log_first(AS_LOG_LEVEL_ERROR));
		}

		if (!client->config.discovery.provider_registered ||
			strcmp(client->config.discovery.provider.name, "dns") != 0) {
			cf_close(client);
			close(fd);
			fail("record_type %s: no dns provider: %s", value, cf_log_first(AS_LOG_LEVEL_ERROR));
		}

		as_error err;
		as_seed_candidates out;
		cf_refresh(client, &err, &out);
		as_seed_candidates_destroy(&out);
		cf_close(client);

		int qtype = cf_dns_first_qtype(fd);
		close(fd);

		if (srv ? qtype != 33 : (qtype != 1 && qtype != 28)) {
			fail("record_type %s: first query has QTYPE %d", value, qtype);
		}
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

	// Each replacement gets a later time than the previous one, even within a second.
	static time_t offset = 5;
	time_t later = time(NULL) + offset++;
	struct utimbuf t = {.actime = later, .modtime = later};

	if (utime(tmp, &t) != 0 || rename(tmp, cf_path) != 0) {
		remove(tmp);
		return false;
	}
	return true;
}

// Settings of a client from config that connects to fnode a and reads the file every tend.
static void
cf_fnode_config(as_config* config, fnode* a)
{
	config->tender_interval = AS_TEND_INTERVAL_MIN;
	config->config_provider.interval = AS_TEND_INTERVAL_MIN;
	config->conn_timeout_ms = 1000;
	config->fail_if_not_connected = true;
	as_config_add_host(config, "127.0.0.1", a->port);
}

// A client from config connected to fnode a from body, reading the file every tend.
static aerospike*
cf_connect_config(as_config* config, fnode* a, const char* body, as_error* err)
{
	cf_fnode_config(config, a);

	aerospike* client = cf_init(config, body);

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

static aerospike*
cf_connect(fnode* a, const char* body, as_error* err)
{
	as_config config;
	as_config_init(&config);
	return cf_connect_config(&config, a, body, err);
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

TEST(cf_registered_connect, "a client uses the registered provider and translator from the file, and rejects registrations while connected")
{
	DISCOVERY_SCOPED(fnode, a, fnode_stop);
	assert_true(fnode_start(&a, "A"));

	as_config config;
	as_config_init(&config);
	assert_true(cf_register(&config));

	as_error err;
	aerospike* client = cf_connect_config(&config, &a, CF_REGISTERED_NAME_ONLY, &err);

	if (!client) {
		fnode_stop(&a);
		cf_plugins_done();
		fail("connect: %d %s", err.code, err.message);
	}

	cf_plugin* p = &cf_providers[0];
	uint64_t end = cf_getms() + 5000;

	while (as_load_uint32(&p->refreshes) == 0 && cf_getms() < end) {
		as_sleep(10);
	}

	char seeds[128];
	snprintf(seeds, sizeof(seeds), "%s", as_load_uint32(&p->refreshes) ? p->seeds : "");
	char expected[64];
	snprintf(expected, sizeof(expected), "127.0.0.1:%u", a.port);
	bool used = strcmp(client->config.discovery.provider.name, "consul") == 0 &&
		strcmp(client->config.translation.provider.name, "nat-table") == 0;

	as_status registered = as_config_register_seed_provider(&client->config, "etcd",
		cf_provider_factory, cf_plugin_release, p, &err);
	bool register_message = strstr(err.message, "cannot register while the client is "
		"connected") != NULL;
	as_status translator = as_config_register_address_translator(&client->config, "etcd",
		cf_translator_factory, cf_plugin_release, p, &err);
	as_status selected = as_config_select_seed_provider(&client->config, "consul", NULL, &err);
	bool select_message = strstr(err.message, "Seed provider cannot be replaced while the "
		"client is connected") != NULL;
	uint32_t calls = p->calls;
	uint32_t t_calls = cf_translators[0].calls;

	cf_close(client);
	fnode_stop(&a);

	assert_true(used);
	assert_string_eq(seeds, expected);
	assert_int_eq(calls, 1);
	assert_int_eq(t_calls, 1);
	assert_int_eq(registered, AEROSPIKE_ERR_PARAM);
	assert_true(register_message);
	assert_int_eq(translator, AEROSPIKE_ERR_PARAM);
	assert_int_eq(selected, AEROSPIKE_ERR_PARAM);
	assert_true(select_message);
	assert_int_eq(cf_plugins_alive(), 0);
	assert_true(cf_plugins_released());
	cf_plugins_done();
}

TEST(cf_registered_reload, "a reload reuses the registered instances: the factories run once")
{
	DISCOVERY_SCOPED(fnode, a, fnode_stop);
	assert_true(fnode_start(&a, "A"));

	as_config config;
	as_config_init(&config);
	assert_true(cf_register(&config));

	as_error err;
	aerospike* client = cf_connect_config(&config, &a, CF_REGISTERED
		"dynamic:\n"
		"  client:\n"
		"    max_error_rate: 100\n", &err);

	if (!client) {
		fnode_stop(&a);
		cf_plugins_done();
		fail("connect: %d %s", err.code, err.message);
	}

	void* provider = client->config.discovery.provider.udata;
	void* translator = client->config.translation.provider.udata;
	bool first = cf_reload(client, CF_REGISTERED
		"      extra: 1\n"
		"dynamic:\n"
		"  client:\n"
		"    max_error_rate: 77\n", 77);
	bool second = cf_reload(client, CF_REGISTERED_NAME_ONLY
		"dynamic:\n"
		"  client:\n"
		"    max_error_rate: 55\n", 55);
	bool same = client->config.discovery.provider.udata == provider &&
		client->config.translation.provider.udata == translator;
	uint32_t errors = cf_log_find(AS_LOG_LEVEL_ERROR, "");
	cf_close(client);
	fnode_stop(&a);

	assert_true(first);
	assert_true(second);
	assert_true(same);
	assert_int_eq(errors, 0);
	assert_int_eq(cf_providers[0].calls, 1);
	assert_int_eq(cf_translators[0].calls, 1);
	assert_int_eq(cf_plugins_alive(), 0);
	assert_true(cf_plugins_released());
	cf_plugins_done();
}

// What a sync-only aerospike_close() does, also when async event loops exist.
static void
cf_sync_close(aerospike* client)
{
	as_cluster_destroy(client->cluster);
	client->cluster = NULL;
	client->config.discovery.registration_locked = false;
	as_config_release_plugins(&client->config, true);
}

TEST(cf_reconnect_rebuilds, "plugins from the file are destroyed at close and rebuilt by reconnect: registered ones with the same options, and a port-map")
{
	DISCOVERY_SCOPED(fnode, a, fnode_stop);
	assert_true(fnode_start(&a, "A"));

	as_config config;
	as_config_init(&config);
	assert_true(cf_register(&config));

	as_error err;
	aerospike* client = cf_connect_config(&config, &a, CF_REGISTERED, &err);

	if (!client) {
		fnode_stop(&a);
		cf_plugins_done();
		fail("connect: %d %s", err.code, err.message);
	}

	char* first = as_val_tostring(cf_providers[0].options);
	cf_sync_close(client);
	uint32_t alive = cf_plugins_alive();
	as_status status = aerospike_connect(client, &err);
	char* second = as_val_tostring(cf_providers[0].options);
	bool same = first && second && strcmp(first, second) == 0;
	bool used = client->config.discovery.provider_registered &&
		strcmp(client->config.discovery.provider.name, "consul") == 0 &&
		strcmp(client->config.translation.provider.name, "nat-table") == 0;
	cf_free(first);
	cf_free(second);
	cf_close(client);

	assert_int_eq(alive, 0);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_true(same);
	assert_true(used);
	assert_int_eq(cf_providers[0].calls, 2);
	assert_int_eq(cf_translators[0].calls, 2);
	assert_int_eq(cf_plugins_alive(), 0);
	assert_true(cf_plugins_released());
	cf_plugins_done();

	client = cf_connect(&a, CF_HEAD
		"    translation:\n"
		"      provider: port-map\n"
		"      port_map:\n"
		"        \"10.0.0.1:3000\": \"lb.example.com:30001\"\n", &err);

	if (!client) {
		fnode_stop(&a);
		fail("connect: %d %s", err.code, err.message);
	}

	cf_sync_close(client);
	bool released = !client->config.translation.provider_registered &&
		client->config.translation.recipe && !client->config.translation.provider_released;
	status = aerospike_connect(client, &err);
	char buf[128] = "";

	if (status == AEROSPIKE_OK) {
		cf_translate(client, "AA1", "10.0.0.1", 3000, buf, sizeof(buf));
	}
	cf_close(client);
	fnode_stop(&a);

	assert_true(released);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_string_eq(buf, "lb.example.com:30001");
}

TEST(cf_registered_binding_reconnect, "a binding name from the file stays a binding provider after close and reconnect; selected in tend mode, connect builds it once the mode is thread")
{
	DISCOVERY_SCOPED(fnode, a, fnode_stop);
	assert_true(fnode_start(&a, "A"));

	as_config config;
	as_config_init(&config);
	assert_true(cf_register_binding(&config));

	as_error err;
	aerospike* client = cf_connect_config(&config, &a, CF_HEAD
		"    discovery:\n"
		"      provider: py-consul\n", &err);

	if (!client) {
		fnode_stop(&a);
		cf_binding_done();
		fail("connect: %d %s", err.code, err.message);
	}

	as_discovery_config* d = &client->config.discovery;
	bool first = d->provider_registered && d->binding_provider;
	cf_sync_close(client);
	bool released = !d->provider_registered && !d->binding_provider && d->recipe &&
		d->recipe->binding;
	as_status status = aerospike_connect(client, &err);
	bool rebuilt = d->provider_registered && d->binding_provider &&
		strcmp(d->provider.name, "py-consul") == 0;
	uint32_t calls = cf_binding.calls;
	cf_close(client);

	assert_true(first);
	assert_true(released);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_true(rebuilt);
	assert_int_eq(calls, 2);
	assert_int_eq(cf_binding.destroyed, 2);
	assert_int_eq(cf_binding.released, 1);

	// Selected in tend mode: the factory runs only at the first connect in thread mode.
	as_config_init(&config);
	assert_true(cf_register_binding(&config));
	cf_fnode_config(&config, &a);
	client = cf_init(&config, CF_HEAD
		"    discovery:\n"
		"      provider: py-consul\n"
		"      execution_mode: tend\n");

	if (!client) {
		fnode_stop(&a);
		cf_binding_done();
		fail("Failed to write %s", cf_path);
	}

	d = &client->config.discovery;
	uint32_t init_calls = cf_binding.calls;
	as_status tend_status = aerospike_connect(client, &err);
	bool message = strstr(err.message, CF_BINDING_MODE) != NULL;
	uint32_t tend_calls = cf_binding.calls;
	d->execution_mode = AS_DISCOVERY_EXECUTION_THREAD;
	status = aerospike_connect(client, &err);
	bool built = client->cluster != NULL && d->provider_registered && d->binding_provider;
	calls = cf_binding.calls;
	cf_close(client);
	fnode_stop(&a);

	assert_int_eq(init_calls, 0);
	assert_int_eq(tend_status, AEROSPIKE_ERR_PARAM);
	assert_true(message);
	assert_int_eq(tend_calls, 0);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_true(built);
	assert_int_eq(calls, 1);
	assert_int_eq(cf_binding.destroyed, 1);
	assert_int_eq(cf_binding.released, 1);
	cf_binding_done();
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
	suite_add(cf_record_type_any_case);
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
	suite_add(cf_registered_options);
	suite_add(cf_registered_name_only);
	suite_add(cf_unregistered_name);
	suite_add(cf_registered_staging);
	suite_add(cf_registrations_carry);
	suite_add(cf_registered_binding);
	suite_add(cf_registered_typing);
	suite_add(cf_registered_keys);
	suite_add(cf_registered_aliases);
	suite_add(cf_registered_locale);
#if defined(DISCOVERY_FAKE_SERVERS)
	suite_add(cf_dns_provider);
	suite_add(cf_dns_record_type_query);
	suite_add(cf_aws_provider);
	suite_add(cf_reload_keeps_static_alternate);
	suite_add(cf_reload_skips_static);
	suite_add(cf_registered_connect);
	suite_add(cf_registered_reload);
	suite_add(cf_reconnect_rebuilds);
	suite_add(cf_registered_binding_reconnect);
	suite_add(cf_fr41_at_connect);
	suite_add(cf_m10_at_connect);
#endif
}
