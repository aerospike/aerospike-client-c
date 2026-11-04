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
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <aerospike/aerospike.h>
#include <aerospike/as_atomic.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_config.h>
#include <aerospike/as_discovery.h>
#include <aerospike/as_event.h>
#include <aerospike/as_log.h>
#include <aerospike/as_sleep.h>
#include <citrusleaf/cf_clock.h>
#include <pthread.h>

#include "test.h"
#include "aerospike_test.h"
#include "discovery_fake_node.h"

#if !defined(_WIN32)
#include <arpa/inet.h>
#include <dirent.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
#endif

// Coverage: U-4 (A / AAAA / SRV against a stub resolver; NXDOMAIN keeps the previous snapshot)
// and a local N-4 precursor (seed discovery through a DNS record served on loopback) of the
// bounded DNS provider commit, plus configuration, ownership, packet hardening, total deadline,
// resolver failover, resolver budget and rotation, the 256-record and CNAME caps, tend equality
// with silent resolvers, execution-mode validation and lifecycle tests. Every wire test uses a
// loopback stub resolver that serves UDP and TCP; none depends on public DNS. The shared
// in-network N-4 scenario is owned by the final integration commit.

//---------------------------------
// Static Functions: shared helpers
//---------------------------------

#define DNS_MARGIN_MS 50

#define fail(fmt, ...) \
	return atf_assert_log(__result__, "", __FILE__, __LINE__, fmt, ##__VA_ARGS__)

static const char*
hosts_str(const as_vector* hosts, char* buf, size_t size)
{
	buf[0] = 0;
	size_t len = 0;

	for (uint32_t i = 0; i < hosts->size && len < size; i++) {
		as_host* host = as_vector_get((as_vector*)hosts, i);
		len += snprintf(buf + len, size - len, "%s%s:%u", i ? "," : "", host->name, host->port);
	}
	return buf;
}

// Call refresh once. out is left initialized; the caller destroys it.
static as_status
run_refresh(as_seed_provider* provider, as_error* err, as_seed_candidates* out)
{
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

static as_status
refresh_str(as_seed_provider* provider, as_error* err, char* buf, size_t size)
{
	as_seed_candidates out;
	as_status status = run_refresh(provider, err, &out);
	hosts_str(out.hosts, buf, size);
	as_seed_candidates_destroy(&out);
	return status;
}

//---------------------------------
// Static Functions: warning capture
//---------------------------------

#define LOG_MAX 32

static pthread_mutex_t log_lock = PTHREAD_MUTEX_INITIALIZER;
static char log_lines[LOG_MAX][512];
static uint32_t log_count;
static as_log_callback log_prev;

static bool
log_capture(as_log_level level, const char* func, const char* file, uint32_t line,
	const char* fmt, ...)
{
	(void)func;
	(void)file;
	(void)line;

	if (level != AS_LOG_LEVEL_WARN) {
		return true;
	}

	char msg[512];
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(msg, sizeof(msg), fmt, ap);
	va_end(ap);

	// Other components may warn concurrently; only DNS and seed provider lines are relevant.
	if (strncmp(msg, "DNS seed", 8) != 0 && strncmp(msg, "Seed provider", 13) != 0) {
		return true;
	}

	pthread_mutex_lock(&log_lock);

	if (log_count < LOG_MAX) {
		snprintf(log_lines[log_count++], sizeof(log_lines[0]), "%s", msg);
	}
	pthread_mutex_unlock(&log_lock);
	return true;
}

static void
log_start(void)
{
	pthread_mutex_lock(&log_lock);
	log_count = 0;
	pthread_mutex_unlock(&log_lock);
	log_prev = g_as_log.callback;
	as_log_set_callback(log_capture);
}

static void
log_stop(void)
{
	as_log_set_callback(log_prev);
}

static uint32_t
log_size(void)
{
	pthread_mutex_lock(&log_lock);
	uint32_t count = log_count;
	pthread_mutex_unlock(&log_lock);
	return count;
}

static const char*
log_line(uint32_t i)
{
	return log_lines[i];
}

//---------------------------------
// Tests: configuration and ownership
//---------------------------------

TEST(dns_config_defaults, "defaults are A, port 3000, no TLS name, 80 ms and system resolvers")
{
	as_dns_seed_provider_config cfg;
	memset(&cfg, 0xa5, sizeof(cfg));
	as_dns_seed_provider_config_init(&cfg);
	assert_null(cfg.names);
	assert_int_eq(cfg.names_size, 0);
	assert_int_eq(cfg.record_type, AS_DNS_RECORD_A);
	assert_int_eq(cfg.port, 3000);
	assert_int_eq(cfg.port, AS_DNS_DEFAULT_PORT);
	assert_null(cfg.tls_name);
	assert_int_eq(cfg.timeout_ms, 80);
	assert_int_eq(cfg.timeout_ms, AS_DNS_DEFAULT_TIMEOUT_MS);
	assert_null(cfg.servers);
	assert_int_eq(cfg.servers_size, 0);
	assert_int_eq(AS_DNS_DEFAULT_SERVER_PORT, 53);

	const char* names[] = {"seeds.example.com"};
	const char* servers[] = {"127.0.0.1"};
	cfg.names = names;
	cfg.names_size = 1;
	cfg.servers = servers;
	cfg.servers_size = 1;

	as_error err;
	as_seed_provider provider;
	assert_int_eq(as_discovery_dns_seed_provider(&err, &cfg, &provider), AEROSPIKE_OK);
	assert_int_eq(provider.size, sizeof(as_seed_provider));
	assert_int_eq(provider.api_version, AS_DISCOVERY_API_VERSION);
	assert_string_eq(provider.name, "dns");
	assert_not_null(provider.refresh);
	assert_not_null(provider.destroy);
	assert_not_null(provider.udata);
	assert_true(provider.needs_periodic_refresh(provider.udata));
	assert_not_null(provider.supports_deadline);
	assert_true(provider.supports_deadline(provider.udata));
	provider.destroy(provider.udata);
}

typedef struct invalid_case_s {
	const char* names[3];
	uint32_t names_size;
	int record_type;
	uint32_t port;
	const char* tls_name;
	uint32_t timeout_ms;
	const char* server;
	bool null_servers;
	const char* expected;
} invalid_case;

static as_status
invalid_run(const invalid_case* c, as_error* err, as_seed_provider* provider)
{
	as_dns_seed_provider_config cfg;
	as_dns_seed_provider_config_init(&cfg);
	cfg.names = c->names_size ? c->names : NULL;
	cfg.names_size = c->names_size;

	if (c->record_type >= 0) {
		cfg.record_type = (as_dns_record_type)c->record_type;
	}

	if (c->port != 1) {
		cfg.port = (uint16_t)c->port;
	}

	if (c->tls_name) {
		cfg.tls_name = c->tls_name;
	}

	if (c->timeout_ms != 1) {
		cfg.timeout_ms = c->timeout_ms;
	}

	const char* servers[] = {c->server};
	cfg.servers = c->null_servers ? NULL : servers;
	cfg.servers_size = 1;
	return as_discovery_dns_seed_provider(err, &cfg, provider);
}

TEST(dns_config_invalid, "invalid names, values and resolvers are rejected at creation with a message naming them")
{
	char long_label[80];
	memset(long_label, 'a', 64);
	strcpy(long_label + 64, ".com");

	// Four 63-byte labels: 255 characters, beyond the 253-character limit.
	char long_name[300];
	memset(long_name, 'a', 255);
	long_name[63] = long_name[127] = long_name[191] = '.';
	long_name[255] = 0;

	// Defaults are kept for record_type -1, port 1 and timeout_ms 1.
	const invalid_case cases[] = {
		{{0}, 0, -1, 1, NULL, 1, "127.0.0.1", false,
			"Invalid DNS seed provider names: at least one name is required"},
		{{"a.example.com", ""}, 2, -1, 1, NULL, 1, "127.0.0.1", false,
			"Invalid DNS seed provider name: ''"},
		{{"a.example.com", NULL}, 2, -1, 1, NULL, 1, "127.0.0.1", false,
			"Invalid DNS seed provider name: '(null)'"},
		{{"."}, 1, -1, 1, NULL, 1, "127.0.0.1", false, "Invalid DNS seed provider name: '.'"},
		{{"a..example.com"}, 1, -1, 1, NULL, 1, "127.0.0.1", false,
			"Invalid DNS seed provider name: 'a..example.com'"},
		{{".example.com"}, 1, -1, 1, NULL, 1, "127.0.0.1", false,
			"Invalid DNS seed provider name: '.example.com'"},
		{{"a b.example.com"}, 1, -1, 1, NULL, 1, "127.0.0.1", false,
			"Invalid DNS seed provider name: 'a b.example.com'"},
		{{long_label}, 1, -1, 1, NULL, 1, "127.0.0.1", false, "Invalid DNS seed provider name: 'aaaa"},
		{{long_name}, 1, -1, 1, NULL, 1, "127.0.0.1", false, "Invalid DNS seed provider name: 'aaaa"},
		{{"a.example.com", "A.Example.COM"}, 2, -1, 1, NULL, 1, "127.0.0.1", false,
			"Invalid DNS seed provider names: duplicate name 'A.Example.COM'"},
		{{"a.example.com", "a.example.com."}, 2, -1, 1, NULL, 1, "127.0.0.1", false,
			"Invalid DNS seed provider names: duplicate name 'a.example.com.'"},
		{{"a.example.com"}, 1, 7, 1, NULL, 1, "127.0.0.1", false,
			"Invalid DNS seed provider record_type: 7"},
		{{"a.example.com"}, 1, -1, 0, NULL, 1, "127.0.0.1", false,
			"Invalid DNS seed provider port: 0"},
		{{"a.example.com"}, 1, -1, 1, "", 1, "127.0.0.1", false,
			"Invalid DNS seed provider tls_name: empty"},
		{{"a.example.com"}, 1, -1, 1, NULL, 0, "127.0.0.1", false,
			"Invalid DNS seed provider timeout_ms: 0"},
		{{"a.example.com"}, 1, -1, 1, NULL, 1, NULL, true,
			"Invalid DNS seed provider servers: null"},
		{{"a.example.com"}, 1, -1, 1, NULL, 1, NULL, false,
			"Invalid DNS seed provider server: null"},
		{{"a.example.com"}, 1, -1, 1, NULL, 1, "", false,
			"Invalid DNS seed provider server: '': expected an IP address"},
		{{"a.example.com"}, 1, -1, 1, NULL, 1, "dns.example.com", false,
			"Invalid DNS seed provider server: 'dns.example.com': expected an IP address"},
		{{"a.example.com"}, 1, -1, 1, NULL, 1, "10.0.0.256", false,
			"Invalid DNS seed provider server: '10.0.0.256': expected an IP address"},
		{{"a.example.com"}, 1, -1, 1, NULL, 1, "10.0.0.1:x", false,
			"Invalid DNS seed provider server: '10.0.0.1:x': invalid port"},
		{{"a.example.com"}, 1, -1, 1, NULL, 1, "10.0.0.1:", false,
			"Invalid DNS seed provider server: '10.0.0.1:': invalid port"},
		{{"a.example.com"}, 1, -1, 1, NULL, 1, "10.0.0.1:0", false,
			"Invalid DNS seed provider server: '10.0.0.1:0': invalid port"},
		{{"a.example.com"}, 1, -1, 1, NULL, 1, "10.0.0.1:70000", false,
			"Invalid DNS seed provider server: '10.0.0.1:70000': invalid port"},
		{{"a.example.com"}, 1, -1, 1, NULL, 1, "[2001:db8::1", false,
			"Invalid DNS seed provider server: '[2001:db8::1'"},
		{{"a.example.com"}, 1, -1, 1, NULL, 1, "[::1]x", false,
			"Invalid DNS seed provider server: '[::1]x'"},
		{{"a.example.com"}, 1, -1, 1, NULL, 1, "[10.0.0.1]:53", false,
			"Invalid DNS seed provider server: '[10.0.0.1]:53': expected an IPv6 address in brackets"},
		{{"a.example.com"}, 1, -1, 1, NULL, 1, "2001:db8::zz", false,
			"Invalid DNS seed provider server: '2001:db8::zz': expected an IP address"},
		{{"a.example.com"}, 1, -1, 1, NULL, 1, "[2001:db8::1]:99999", false,
			"Invalid DNS seed provider server: '[2001:db8::1]:99999': invalid port"},
		{{"a.example.com"}, 1, -1, 1, NULL, 1, "fe80::1%lo", false,
			"Invalid DNS seed provider server: 'fe80::1%lo': expected an IP address"},
	};

	for (uint32_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
		as_error err;
		as_seed_provider provider;
		memset(&provider, 0x5a, sizeof(provider));
		as_seed_provider pattern = provider;

		as_status status = invalid_run(&cases[i], &err, &provider);

		if (status != AEROSPIKE_ERR_PARAM || !strstr(err.message, cases[i].expected)) {
			fail("case %u: status %d message '%s', expected '%s'", i, status, err.message,
				cases[i].expected);
		}
		assert_int_eq(memcmp(&provider, &pattern, sizeof(provider)), 0);
	}

	as_error err;
	as_seed_provider provider;
	as_dns_seed_provider_config cfg;
	as_dns_seed_provider_config_init(&cfg);
	assert_int_eq(as_discovery_dns_seed_provider(&err, NULL, &provider), AEROSPIKE_ERR_PARAM);
	assert_int_eq(as_discovery_dns_seed_provider(&err, &cfg, NULL), AEROSPIKE_ERR_PARAM);
	assert_string_eq(err.message, "Invalid DNS seed provider");
}

TEST(dns_config_servers, "explicit resolvers accept ip, ip:port, bare IPv6, [IPv6] and [IPv6]:port; names accept a trailing dot")
{
	const char* names[] = {"a.example.com", "_aerospike._tcp.Example.COM.", "b"};
	const char* servers[] = {"10.0.0.1", "10.0.0.2:5353", "2001:db8::1", "[2001:db8::2]:5353",
		"[::1]", "10.0.0.1"};
	as_dns_seed_provider_config cfg;
	as_dns_seed_provider_config_init(&cfg);
	cfg.names = names;
	cfg.names_size = 3;
	cfg.servers = servers;
	cfg.servers_size = 6;
	cfg.record_type = AS_DNS_RECORD_SRV;
	cfg.port = 65535;
	cfg.tls_name = "tls";
	cfg.timeout_ms = 1;

	as_error err;
	as_seed_provider provider;
	assert_int_eq(as_discovery_dns_seed_provider(&err, &cfg, &provider), AEROSPIKE_OK);
	provider.destroy(provider.udata);
}

#if !defined(_WIN32)
// True when /etc/resolv.conf names at least one numeric resolver.
static bool
resolv_conf_has_server(void)
{
	FILE* f = fopen("/etc/resolv.conf", "r");

	if (!f) {
		return false;
	}

	char line[512];
	bool found = false;

	while (!found && fgets(line, sizeof(line), f)) {
		char word[64];
		char value[256];

		if (sscanf(line, " %63s %255s", word, value) != 2 || strcmp(word, "nameserver") != 0) {
			continue;
		}

		char* zone = strchr(value, '%');

		if (zone) {
			*zone = 0;
		}

		uint8_t addr[16];
		found = inet_pton(AF_INET, value, addr) == 1 || inet_pton(AF_INET6, value, addr) == 1;
	}
	fclose(f);
	return found;
}
#endif

TEST(dns_config_system_resolvers, "without servers the system resolvers are read once at creation, or creation asks for servers")
{
	const char* names[] = {"seeds.example.com"};
	as_dns_seed_provider_config cfg;
	as_dns_seed_provider_config_init(&cfg);
	cfg.names = names;
	cfg.names_size = 1;

	as_error err;
	as_seed_provider provider;
	as_status status = as_discovery_dns_seed_provider(&err, &cfg, &provider);

#if !defined(_WIN32)
	if (resolv_conf_has_server()) {
		assert_int_eq(status, AEROSPIKE_OK);
	}
	else {
		assert_int_eq(status, AEROSPIKE_ERR_PARAM);
	}
#endif

	if (status == AEROSPIKE_OK) {
		provider.destroy(provider.udata);
	}
	else {
		assert_int_eq(status, AEROSPIKE_ERR_PARAM);
		assert_not_null(strstr(err.message, "Set servers"));
	}
}

TEST(dns_config_registration, "the config helper copies its input, replaces a previous provider and leaves it on failure")
{
	as_config config;
	as_error err;
	as_config_init(&config);

	char name[64] = "seeds.example.com";
	char server[64] = "127.0.0.1";
	const char* names[] = {name};
	const char* servers[] = {server};
	as_dns_seed_provider_config cfg;
	as_dns_seed_provider_config_init(&cfg);
	cfg.servers = servers;
	cfg.servers_size = 1;

	// Invalid: nothing is registered.
	assert_int_eq(as_config_set_dns_seed_provider(&config, &cfg, &err), AEROSPIKE_ERR_PARAM);
	assert_false(config.discovery.provider_registered);
	assert_int_eq(as_config_set_dns_seed_provider(NULL, &cfg, &err), AEROSPIKE_ERR_PARAM);
	assert_int_eq(as_config_set_dns_seed_provider(&config, NULL, &err), AEROSPIKE_ERR_PARAM);
	assert_false(config.discovery.provider_registered);

	cfg.names = names;
	cfg.names_size = 1;
	assert_int_eq(as_config_set_dns_seed_provider(&config, &cfg, &err), AEROSPIKE_OK);
	assert_true(config.discovery.provider_registered);
	assert_false(config.discovery.binding_provider);
	assert_string_eq(config.discovery.provider.name, "dns");
	assert_true(config.discovery.provider.name != cfg.names[0]);
	void* first = config.discovery.provider.udata;

	// Replacement destroys the previous state.
	assert_int_eq(as_config_set_dns_seed_provider(&config, &cfg, &err), AEROSPIKE_OK);
	void* second = config.discovery.provider.udata;
	assert_not_null(second);
	(void)first;

	// A failed replacement leaves the registration unchanged.
	cfg.timeout_ms = 0;
	assert_int_eq(as_config_set_dns_seed_provider(&config, &cfg, &err), AEROSPIKE_ERR_PARAM);
	assert_true(config.discovery.provider_registered);
	assert_true(config.discovery.provider.udata == second);
	cfg.timeout_ms = 80;

	config.discovery.registration_locked = true;
	assert_int_eq(as_config_set_dns_seed_provider(&config, &cfg, &err), AEROSPIKE_ERR_PARAM);
	assert_not_null(strstr(err.message, "cannot be replaced while the client is connected"));
	assert_true(config.discovery.provider.udata == second);
	config.discovery.registration_locked = false;

	// Caller strings are not referenced after registration.
	strcpy(name, "changed");
	strcpy(server, "x");
	as_config_destroy(&config);
	assert_false(config.discovery.provider_registered);
}

#ifdef DISCOVERY_FAKE_SERVERS

//---------------------------------
// Static Functions: thread counts
//---------------------------------

static int
thread_count(void)
{
#if defined(__linux__)
	FILE* f = fopen("/proc/self/status", "r");

	if (!f) {
		return -1;
	}

	char line[256];
	int count = -1;

	while (fgets(line, sizeof(line), f)) {
		if (strncmp(line, "Threads:", 8) == 0) {
			count = atoi(line + 8);
			break;
		}
	}
	fclose(f);
	return count;
#else
	return -1;
#endif
}

// Number of threads of this process named "discovery", or -1 when unknown.
static int
discovery_thread_count(void)
{
#if defined(__linux__)
	DIR* dir = opendir("/proc/self/task");

	if (!dir) {
		return -1;
	}

	int count = 0;
	struct dirent* entry;

	while ((entry = readdir(dir)) != NULL) {
		if (entry->d_name[0] == '.') {
			continue;
		}

		char path[300];
		snprintf(path, sizeof(path), "/proc/self/task/%s/comm", entry->d_name);
		FILE* f = fopen(path, "r");

		if (!f) {
			continue;
		}

		char name[64] = {0};

		if (fgets(name, sizeof(name), f) && strncmp(name, "discovery", 9) == 0 &&
			(name[9] == '\n' || name[9] == 0)) {
			count++;
		}
		fclose(f);
	}
	closedir(dir);
	return count;
#else
	return -1;
#endif
}

static int
settle(int (*count)(void), int expected)
{
	int value = count();

	for (int i = 0; i < 200 && value != expected; i++) {
		as_sleep(10);
		value = count();
	}
	return value;
}

// Number of open descriptors of this process, or -1 when unknown.
static int
fd_count(void)
{
#if defined(__linux__)
	DIR* dir = opendir("/proc/self/fd");
#else
	DIR* dir = opendir("/dev/fd");
#endif

	if (!dir) {
		return -1;
	}

	int count = 0;
	struct dirent* entry;

	while ((entry = readdir(dir)) != NULL) {
		if (entry->d_name[0] != '.') {
			count++;
		}
	}
	closedir(dir);
	return count;
}

//---------------------------------
// Static Functions: loopback stub resolver
//---------------------------------

#define T_A 1
#define T_CNAME 5
#define T_AAAA 28
#define T_SRV 33

#define STUB_SILENT -1
#define STUB_NONE -2
#define STUB_MAX_RECORDS 64
#define STUB_MAX_RCODES 32
#define STUB_MAX_QUERIES 128
#define STUB_MAX_HUNG 8
#define STUB_BUFFER 4096
#define STUB_TCP_BUFFER 65535

typedef struct dns_stub_s dns_stub;

// Build a complete answer to query in out (STUB_BUFFER bytes over UDP, STUB_TCP_BUFFER over TCP),
// or return 0 to stay silent.
typedef size_t (*stub_custom)(const uint8_t* query, size_t len, uint8_t* out, bool tcp);

typedef struct stub_record_s {
	char qname[128];
	uint16_t qtype;
	bool additional;
	char owner[128];
	uint16_t type;
	char data[160];
} stub_record;

typedef struct stub_rcode_s {
	char qname[128];
	uint16_t qtype;
	int rcode;
} stub_rcode_entry;

struct dns_stub_s {
	int udp_fd;
	int tcp_fd;
	uint16_t port;
	pthread_t thread;
	bool running;
	uint8_t stop;
	pthread_mutex_t lock;

	// Never answer over UDP.
	bool silent;

	// Answer every UDP query with an empty truncated (TC) answer.
	bool truncate;

	// Accept TCP connections and never answer.
	bool hang_tcp;

	// Send a datagram with a wrong ID, one with a wrong question and a non-response first.
	bool spoof_first;

	// Delay every UDP answer by this many milliseconds.
	uint32_t delay_ms;

	stub_custom custom;

	stub_record records[STUB_MAX_RECORDS];
	uint32_t records_size;
	stub_rcode_entry rcodes[STUB_MAX_RCODES];
	uint32_t rcodes_size;

	char queries[STUB_MAX_QUERIES][280];
	uint32_t queries_size;
	uint32_t udp_queries;
	uint32_t tcp_queries;
	uint32_t tcp_accepts;

	int hung[STUB_MAX_HUNG];
	uint32_t hung_size;
};

static void
put16(uint8_t* p, uint16_t v)
{
	p[0] = (uint8_t)(v >> 8);
	p[1] = (uint8_t)v;
}

static uint16_t
get16(const uint8_t* p)
{
	return (uint16_t)((p[0] << 8) | p[1]);
}

static const char*
type_name(uint16_t type)
{
	switch (type) {
	case T_A:
		return "A";
	case T_AAAA:
		return "AAAA";
	case T_SRV:
		return "SRV";
	case T_CNAME:
		return "CNAME";
	default:
		return "OTHER";
	}
}

// Read the uncompressed question of a query. Returns false when it is not a valid query.
static bool
stub_question(const uint8_t* q, size_t len, char* name, uint16_t* type, size_t* end)
{
	if (len < 12 || get16(q + 4) != 1) {
		return false;
	}

	size_t pos = 12;
	size_t n = 0;

	while (pos < len && q[pos]) {
		uint8_t l = q[pos];

		if (l > 63 || pos + 1 + l >= len || n + l + 2 > 255) {
			return false;
		}

		if (n) {
			name[n++] = '.';
		}

		for (uint8_t i = 0; i < l; i++) {
			char c = (char)q[pos + 1 + i];
			name[n++] = (c >= 'A' && c <= 'Z') ? (char)(c + 32) : c;
		}
		pos += 1 + l;
	}
	name[n] = 0;

	if (pos + 5 > len) {
		return false;
	}
	*type = get16(q + pos + 1);
	*end = pos + 5;
	return true;
}

// Write name, compressing its longest label-aligned suffix shared with the question name, which
// starts at offset 12.
static size_t
stub_write_name(uint8_t* out, size_t pos, const char* qname, const char* name)
{
	if (strcmp(name, ".") == 0 || !name[0]) {
		out[pos++] = 0;
		return pos;
	}

	size_t qlen = strlen(qname);
	const char* p = name;

	while (*p) {
		size_t slen = strlen(p);

		if (slen <= qlen && strcmp(qname + qlen - slen, p) == 0 &&
			(slen == qlen || qname[qlen - slen - 1] == '.')) {
			put16(out + pos, (uint16_t)(0xc000 | (12 + qlen - slen)));
			return pos + 2;
		}

		const char* dot = strchr(p, '.');
		size_t l = dot ? (size_t)(dot - p) : strlen(p);
		out[pos++] = (uint8_t)l;
		memcpy(out + pos, p, l);
		pos += l;
		p += l;

		if (*p == '.') {
			p++;
		}
	}
	out[pos++] = 0;
	return pos;
}

static size_t
stub_write_rr(uint8_t* out, size_t pos, const char* qname, const stub_record* r)
{
	pos = stub_write_name(out, pos, qname, r->owner);
	put16(out + pos, r->type);
	put16(out + pos + 2, 1);
	memset(out + pos + 4, 0, 4);
	out[pos + 7] = 60;
	size_t rdlen_pos = pos + 8;
	pos += 10;

	switch (r->type) {
	case T_A:
		inet_pton(AF_INET, r->data, out + pos);
		pos += 4;
		break;

	case T_AAAA:
		inet_pton(AF_INET6, r->data, out + pos);
		pos += 16;
		break;

	case T_CNAME:
		pos = stub_write_name(out, pos, qname, r->data);
		break;

	case T_SRV: {
		unsigned priority, weight, port;
		char target[128];

		if (sscanf(r->data, "%u %u %u %127s", &priority, &weight, &port, target) == 4) {
			put16(out + pos, (uint16_t)priority);
			put16(out + pos + 2, (uint16_t)weight);
			put16(out + pos + 4, (uint16_t)port);
			pos = stub_write_name(out, pos + 6, qname, target);
		}
		break;
	}
	}
	put16(out + rdlen_pos, (uint16_t)(pos - rdlen_pos - 2));
	return pos;
}

static size_t
stub_respond(dns_stub* s, const uint8_t* q, size_t len, uint8_t* out, bool tcp)
{
	char name[256];
	uint16_t type;
	size_t qend;

	if (!stub_question(q, len, name, &type, &qend)) {
		return 0;
	}

	pthread_mutex_lock(&s->lock);
	stub_custom custom = s->custom;
	pthread_mutex_unlock(&s->lock);

	if (custom) {
		return custom(q, len, out, tcp);
	}

	pthread_mutex_lock(&s->lock);
	int rcode = STUB_NONE;

	for (uint32_t i = 0; i < s->rcodes_size; i++) {
		if (strcmp(s->rcodes[i].qname, name) == 0 && s->rcodes[i].qtype == type) {
			rcode = s->rcodes[i].rcode;
		}
	}

	if (rcode == STUB_SILENT) {
		pthread_mutex_unlock(&s->lock);
		return 0;
	}

	const stub_record* answers[STUB_MAX_RECORDS];
	const stub_record* extras[STUB_MAX_RECORDS];
	uint32_t an = 0;
	uint32_t ar = 0;
	bool exists = false;

	for (uint32_t i = 0; i < s->records_size; i++) {
		const stub_record* r = &s->records[i];

		if (strcmp(r->qname, name) != 0) {
			continue;
		}
		exists = true;

		if (r->qtype == type) {
			if (r->additional) {
				extras[ar++] = r;
			}
			else {
				answers[an++] = r;
			}
		}
	}

	if (rcode == STUB_NONE) {
		rcode = exists ? 0 : 3;
	}

	bool truncated = s->truncate && !tcp;

	if (rcode != 0 || truncated) {
		an = 0;
		ar = 0;
	}

	out[0] = q[0];
	out[1] = q[1];
	put16(out + 2, (uint16_t)(0x8000 | ((q[2] & 1) << 8) | 0x0080 | (truncated ? 0x0200 : 0) |
		rcode));
	put16(out + 4, 1);
	put16(out + 6, (uint16_t)an);
	put16(out + 8, 0);
	put16(out + 10, (uint16_t)ar);
	memcpy(out + 12, q + 12, qend - 12);
	size_t pos = qend;

	for (uint32_t i = 0; i < an; i++) {
		pos = stub_write_rr(out, pos, name, answers[i]);
	}

	for (uint32_t i = 0; i < ar; i++) {
		pos = stub_write_rr(out, pos, name, extras[i]);
	}
	pthread_mutex_unlock(&s->lock);
	return pos;
}

static void
stub_note(dns_stub* s, const uint8_t* q, size_t len, bool tcp)
{
	char name[256];
	uint16_t type;
	size_t end;

	if (!stub_question(q, len, name, &type, &end)) {
		snprintf(name, sizeof(name), "(invalid)");
		type = 0;
	}

	pthread_mutex_lock(&s->lock);

	if (tcp) {
		s->tcp_queries++;
	}
	else {
		s->udp_queries++;
	}

	if (s->queries_size < STUB_MAX_QUERIES) {
		snprintf(s->queries[s->queries_size++], sizeof(s->queries[0]), "%s/%s/%s", name,
			type_name(type), tcp ? "tcp" : "udp");
	}
	pthread_mutex_unlock(&s->lock);
}

static void
stub_udp(dns_stub* s)
{
	uint8_t q[512];
	struct sockaddr_storage from;
	socklen_t from_len = sizeof(from);
	ssize_t n = recvfrom(s->udp_fd, q, sizeof(q), 0, (struct sockaddr*)&from, &from_len);

	if (n <= 0) {
		return;
	}
	stub_note(s, q, (size_t)n, false);

	pthread_mutex_lock(&s->lock);
	bool silent = s->silent;
	bool spoof = s->spoof_first;
	uint32_t delay = s->delay_ms;
	pthread_mutex_unlock(&s->lock);

	if (silent) {
		return;
	}

	uint8_t out[STUB_BUFFER];
	size_t len = stub_respond(s, q, (size_t)n, out, false);

	if (len == 0) {
		return;
	}

	if (delay) {
		as_sleep(delay);
	}

	if (spoof) {
		uint8_t bad[STUB_BUFFER];

		memcpy(bad, out, len);
		bad[0] ^= 0x5a;
		sendto(s->udp_fd, bad, len, 0, (struct sockaddr*)&from, from_len);

		memcpy(bad, out, len);
		bad[13] ^= 0x01;
		sendto(s->udp_fd, bad, len, 0, (struct sockaddr*)&from, from_len);

		memcpy(bad, out, len);
		bad[2] &= 0x7f;
		sendto(s->udp_fd, bad, len, 0, (struct sockaddr*)&from, from_len);

		sendto(s->udp_fd, bad, 5, 0, (struct sockaddr*)&from, from_len);
	}
	sendto(s->udp_fd, out, len, 0, (struct sockaddr*)&from, from_len);
}

static void
stub_accept(dns_stub* s)
{
	int fd = accept(s->tcp_fd, NULL, NULL);

	if (fd < 0) {
		return;
	}

	pthread_mutex_lock(&s->lock);
	s->tcp_accepts++;
	bool hang = s->hang_tcp;

	if (hang && s->hung_size < STUB_MAX_HUNG) {
		s->hung[s->hung_size++] = fd;
		fd = -1;
	}
	pthread_mutex_unlock(&s->lock);

	if (hang) {
		if (fd >= 0) {
			close(fd);
		}
		return;
	}

	struct timeval tv = {1, 0};
	setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

	uint8_t prefix[2];
	uint8_t q[512];

	if (recv(fd, prefix, 2, MSG_WAITALL) == 2) {
		size_t len = get16(prefix);

		if (len <= sizeof(q) && recv(fd, q, len, MSG_WAITALL) == (ssize_t)len) {
			stub_note(s, q, len, true);

			uint8_t out[2 + STUB_TCP_BUFFER];
			size_t n = stub_respond(s, q, len, out + 2, true);

			if (n > 0) {
				put16(out, (uint16_t)n);
				send(fd, out, n + 2, MSG_NOSIGNAL);
			}
		}
	}
	close(fd);
}

static void*
stub_run(void* arg)
{
	dns_stub* s = arg;

	while (!as_load_uint8(&s->stop)) {
		struct pollfd pfds[2] = {{s->udp_fd, POLLIN, 0}, {s->tcp_fd, POLLIN, 0}};

		if (poll(pfds, 2, 20) <= 0) {
			continue;
		}

		if (pfds[0].revents & POLLIN) {
			stub_udp(s);
		}

		if (pfds[1].revents & POLLIN) {
			stub_accept(s);
		}
	}
	return NULL;
}

// Serve UDP and TCP on the same ephemeral 127.0.0.1 port.
static bool
stub_start(dns_stub* s)
{
	memset(s, 0, sizeof(dns_stub));
	pthread_mutex_init(&s->lock, NULL);
	s->udp_fd = -1;
	s->tcp_fd = -1;

	for (int attempt = 0; attempt < 20 && s->tcp_fd < 0; attempt++) {
		struct sockaddr_in addr;
		memset(&addr, 0, sizeof(addr));
		addr.sin_family = AF_INET;
		addr.sin_addr.s_addr = inet_addr("127.0.0.1");
		socklen_t addr_len = sizeof(addr);

		int u = socket(AF_INET, SOCK_DGRAM, 0);

		if (u < 0 || bind(u, (struct sockaddr*)&addr, sizeof(addr)) != 0 ||
			getsockname(u, (struct sockaddr*)&addr, &addr_len) != 0) {
			if (u >= 0) {
				close(u);
			}
			continue;
		}

		int t = socket(AF_INET, SOCK_STREAM, 0);
		int one = 1;
		setsockopt(t, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

		if (t < 0 || bind(t, (struct sockaddr*)&addr, sizeof(addr)) != 0 || listen(t, 8) != 0) {
			if (t >= 0) {
				close(t);
			}
			close(u);
			continue;
		}
		s->udp_fd = u;
		s->tcp_fd = t;
		s->port = ntohs(addr.sin_port);
	}

	if (s->tcp_fd < 0) {
		return false;
	}
	s->running = pthread_create(&s->thread, NULL, stub_run, s) == 0;
	return s->running;
}

static void
stub_stop(dns_stub* s)
{
	if (s->running) {
		as_store_uint8(&s->stop, 1);
		pthread_join(s->thread, NULL);
		s->running = false;
	}

	for (uint32_t i = 0; i < s->hung_size; i++) {
		close(s->hung[i]);
	}

	if (s->udp_fd >= 0) {
		close(s->udp_fd);
	}

	if (s->tcp_fd >= 0) {
		close(s->tcp_fd);
	}
	pthread_mutex_destroy(&s->lock);
}

static const char*
stub_server(const dns_stub* s, char* buf, size_t size)
{
	snprintf(buf, size, "127.0.0.1:%u", s->port);
	return buf;
}

static void
stub_rr(dns_stub* s, const char* qname, uint16_t qtype, bool additional, const char* owner,
	uint16_t type, const char* data)
{
	pthread_mutex_lock(&s->lock);

	if (s->records_size < STUB_MAX_RECORDS) {
		stub_record* r = &s->records[s->records_size++];
		snprintf(r->qname, sizeof(r->qname), "%s", qname);
		r->qtype = qtype;
		r->additional = additional;
		snprintf(r->owner, sizeof(r->owner), "%s", owner);
		r->type = type;
		snprintf(r->data, sizeof(r->data), "%s", data);
	}
	pthread_mutex_unlock(&s->lock);
}

static void
stub_a(dns_stub* s, const char* name, const char* addr)
{
	stub_rr(s, name, T_A, false, name, T_A, addr);
}

static void
stub_aaaa(dns_stub* s, const char* name, const char* addr)
{
	stub_rr(s, name, T_AAAA, false, name, T_AAAA, addr);
}

static void
stub_srv(dns_stub* s, const char* name, unsigned priority, unsigned weight, unsigned port,
	const char* target)
{
	char data[160];
	snprintf(data, sizeof(data), "%u %u %u %s", priority, weight, port, target);
	stub_rr(s, name, T_SRV, false, name, T_SRV, data);
}

// Address record of target in the additional section of the SRV answer for name.
static void
stub_extra(dns_stub* s, const char* name, const char* target, uint16_t type, const char* addr)
{
	stub_rr(s, name, T_SRV, true, target, type, addr);
}

static void
stub_rcode(dns_stub* s, const char* name, uint16_t qtype, int rcode)
{
	pthread_mutex_lock(&s->lock);

	if (s->rcodes_size < STUB_MAX_RCODES) {
		stub_rcode_entry* r = &s->rcodes[s->rcodes_size++];
		snprintf(r->qname, sizeof(r->qname), "%s", name);
		r->qtype = qtype;
		r->rcode = rcode;
	}
	pthread_mutex_unlock(&s->lock);
}

static void
stub_clear(dns_stub* s)
{
	pthread_mutex_lock(&s->lock);
	s->records_size = 0;
	s->rcodes_size = 0;
	pthread_mutex_unlock(&s->lock);
}

static void
stub_set(dns_stub* s, bool* field, bool value)
{
	pthread_mutex_lock(&s->lock);
	*field = value;
	pthread_mutex_unlock(&s->lock);
}

static void
stub_set_delay(dns_stub* s, uint32_t delay_ms)
{
	pthread_mutex_lock(&s->lock);
	s->delay_ms = delay_ms;
	pthread_mutex_unlock(&s->lock);
}

static void
stub_set_custom(dns_stub* s, stub_custom custom)
{
	pthread_mutex_lock(&s->lock);
	s->custom = custom;
	pthread_mutex_unlock(&s->lock);
}

static uint32_t
stub_get(dns_stub* s, const uint32_t* field)
{
	pthread_mutex_lock(&s->lock);
	uint32_t value = *field;
	pthread_mutex_unlock(&s->lock);
	return value;
}

// Number of queries "name/TYPE" received over UDP or TCP.
static uint32_t
stub_count(dns_stub* s, const char* query)
{
	size_t len = strlen(query);
	uint32_t count = 0;
	pthread_mutex_lock(&s->lock);

	for (uint32_t i = 0; i < s->queries_size; i++) {
		if (strncmp(s->queries[i], query, len) == 0 && s->queries[i][len] == '/') {
			count++;
		}
	}
	pthread_mutex_unlock(&s->lock);
	return count;
}

static const char*
stub_queries(dns_stub* s, char* buf, size_t size)
{
	buf[0] = 0;
	size_t len = 0;
	pthread_mutex_lock(&s->lock);

	for (uint32_t i = 0; i < s->queries_size && len < size; i++) {
		len += snprintf(buf + len, size - len, "%s%s", i ? "," : "", s->queries[i]);
	}
	pthread_mutex_unlock(&s->lock);
	return buf;
}

static void
stub_reset_queries(dns_stub* s)
{
	pthread_mutex_lock(&s->lock);
	s->queries_size = 0;
	s->udp_queries = 0;
	s->tcp_queries = 0;
	s->tcp_accepts = 0;
	pthread_mutex_unlock(&s->lock);
}

// A port with nothing listening on it.
static uint16_t
closed_port(void)
{
	int fd = socket(AF_INET, SOCK_DGRAM, 0);
	struct sockaddr_in addr;
	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = inet_addr("127.0.0.1");
	socklen_t addr_len = sizeof(addr);
	bind(fd, (struct sockaddr*)&addr, sizeof(addr));
	getsockname(fd, (struct sockaddr*)&addr, &addr_len);
	close(fd);
	return ntohs(addr.sin_port);
}

//---------------------------------
// Static Functions: provider helpers
//---------------------------------

typedef struct dns_setup_s {
	char servers_buf[4][64];
	const char* servers[4];
	as_dns_seed_provider_config cfg;
} dns_setup;

// Configure names against the given stubs, in order.
static as_dns_seed_provider_config*
setup_init(dns_setup* d, const char* const* names, uint32_t names_size, dns_stub* s1, dns_stub* s2)
{
	memset(d, 0, sizeof(dns_setup));
	as_dns_seed_provider_config_init(&d->cfg);
	// Answered queries take microseconds, but slow runners (valgrind, TSan) need headroom. The
	// largest tend_refresh_deadline_ms keeps this valid in tend mode too.
	d->cfg.timeout_ms = 500;
	d->cfg.names = names;
	d->cfg.names_size = names_size;
	dns_stub* stubs[] = {s1, s2};

	for (uint32_t i = 0; i < 2 && stubs[i]; i++) {
		stub_server(stubs[i], d->servers_buf[i], sizeof(d->servers_buf[i]));
		d->servers[i] = d->servers_buf[i];
		d->cfg.servers_size++;
	}
	d->cfg.servers = d->servers;
	return &d->cfg;
}

static bool
is_numeric(const char* host)
{
	uint8_t addr[16];
	return inet_pton(AF_INET, host, addr) == 1 || inet_pton(AF_INET6, host, addr) == 1;
}

static bool
all_numeric(const as_vector* hosts)
{
	for (uint32_t i = 0; i < hosts->size; i++) {
		if (!is_numeric(((as_host*)as_vector_get((as_vector*)hosts, i))->name)) {
			return false;
		}
	}
	return true;
}

//---------------------------------
// Tests: A and AAAA (U-4)
//---------------------------------

TEST(dns_a_aaaa, "U-4 A then AAAA: each family sorted, A first, canonical numeric text, configured port")
{
	dns_stub s;
	assert_true(stub_start(&s));
	stub_a(&s, "seeds.example.com", "10.0.0.9");
	stub_a(&s, "seeds.example.com", "10.0.0.2");
	stub_aaaa(&s, "seeds.example.com", "2001:0db8:0:0::0001");
	stub_aaaa(&s, "seeds.example.com", "2001:db8::0");

	const char* names[] = {"seeds.example.com"};
	dns_setup d;
	as_dns_seed_provider_config* cfg = setup_init(&d, names, 1, &s, NULL);
	cfg->port = 3100;
	as_seed_provider provider;
	as_error err;
	assert_int_eq(as_discovery_dns_seed_provider(&err, cfg, &provider), AEROSPIKE_OK);

	log_start();
	as_seed_candidates out;
	assert_int_eq(run_refresh(&provider, &err, &out), AEROSPIKE_OK);
	char buf[512];
	assert_string_eq(hosts_str(out.hosts, buf, sizeof(buf)),
		"10.0.0.2:3100,10.0.0.9:3100,2001:db8:::3100,2001:db8::1:3100");
	assert_true(all_numeric(out.hosts));

	for (uint32_t i = 0; i < out.hosts->size; i++) {
		assert_null(((as_host*)as_vector_get(out.hosts, i))->tls_name);
	}
	as_seed_candidates_destroy(&out);
	assert_string_eq(stub_queries(&s, buf, sizeof(buf)),
		"seeds.example.com/A/udp,seeds.example.com/AAAA/udp");

	// The same answers give the same order.
	char again[512];
	assert_int_eq(refresh_str(&provider, &err, again, sizeof(again)), AEROSPIKE_OK);
	assert_string_eq(again, "10.0.0.2:3100,10.0.0.9:3100,2001:db8:::3100,2001:db8::1:3100");
	log_stop();
	assert_int_eq(log_size(), 0);

	provider.destroy(provider.udata);
	stub_stop(&s);
}

TEST(dns_a_names_in_order, "U-4 names are resolved in configured order and hosts deduplicated by address and port")
{
	dns_stub s;
	assert_true(stub_start(&s));
	stub_a(&s, "b.example.com", "10.0.0.2");
	stub_a(&s, "b.example.com", "10.0.0.3");
	stub_a(&s, "a.example.com", "10.0.0.1");
	stub_a(&s, "a.example.com", "10.0.0.2");
	stub_a(&s, "a.example.com", "10.0.0.2");

	// Configured case and the trailing dot do not matter: queries use the lowercase name.
	const char* names[] = {"B.Example.COM", "a.example.com."};
	dns_setup d;
	as_dns_seed_provider_config* cfg = setup_init(&d, names, 2, &s, NULL);
	as_seed_provider provider;
	as_error err;
	assert_int_eq(as_discovery_dns_seed_provider(&err, cfg, &provider), AEROSPIKE_OK);

	char buf[512];
	assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)), AEROSPIKE_OK);
	assert_string_eq(buf, "10.0.0.2:3000,10.0.0.3:3000,10.0.0.1:3000");
	assert_int_eq(stub_count(&s, "b.example.com/A"), 1);
	assert_int_eq(stub_count(&s, "a.example.com/A"), 1);

	provider.destroy(provider.udata);
	stub_stop(&s);
}

TEST(dns_tls_name, "the configured TLS name is copied to every host in A and SRV mode")
{
	dns_stub s;
	assert_true(stub_start(&s));
	stub_a(&s, "seeds.example.com", "10.0.0.1");
	stub_a(&s, "seeds.example.com", "10.0.0.2");
	stub_srv(&s, "_aerospike._tcp.example.com", 10, 1, 4333, "seeds.example.com");

	for (int mode = 0; mode < 2; mode++) {
		const char* names[] = {mode ? "_aerospike._tcp.example.com" : "seeds.example.com"};
		char tls[32] = "aerospike-tls";
		dns_setup d;
		as_dns_seed_provider_config* cfg = setup_init(&d, names, 1, &s, NULL);
		cfg->record_type = mode ? AS_DNS_RECORD_SRV : AS_DNS_RECORD_A;
		cfg->tls_name = tls;
		as_seed_provider provider;
		as_error err;
		assert_int_eq(as_discovery_dns_seed_provider(&err, cfg, &provider), AEROSPIKE_OK);
		strcpy(tls, "changed");

		as_seed_candidates out;
		assert_int_eq(run_refresh(&provider, &err, &out), AEROSPIKE_OK);
		assert_true(out.hosts->size > 0);

		for (uint32_t i = 0; i < out.hosts->size; i++) {
			as_host* host = as_vector_get(out.hosts, i);
			assert_string_eq(host->tls_name, "aerospike-tls");
		}
		as_seed_candidates_destroy(&out);
		provider.destroy(provider.udata);
	}
	stub_stop(&s);
}

TEST(dns_cname, "U-4 answers through a CNAME chain are used; records of other owners are ignored")
{
	dns_stub s;
	assert_true(stub_start(&s));
	stub_rr(&s, "alias.example.com", T_A, false, "alias.example.com", T_CNAME, "mid.example.com");
	stub_rr(&s, "alias.example.com", T_A, false, "mid.example.com", T_CNAME, "real.example.org");
	stub_rr(&s, "alias.example.com", T_A, false, "real.example.org", T_A, "10.0.0.5");
	stub_rr(&s, "alias.example.com", T_A, false, "unrelated.example.com", T_A, "10.0.0.66");

	const char* names[] = {"alias.example.com"};
	dns_setup d;
	as_seed_provider provider;
	as_error err;
	assert_int_eq(as_discovery_dns_seed_provider(&err, setup_init(&d, names, 1, &s, NULL),
		&provider), AEROSPIKE_OK);

	char buf[256];
	assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)), AEROSPIKE_OK);
	assert_string_eq(buf, "10.0.0.5:3000");

	provider.destroy(provider.udata);
	stub_stop(&s);
}

//---------------------------------
// Tests: SRV (U-4)
//---------------------------------

TEST(dns_srv_additional, "U-4 SRV: priority then weight descending, targets from the additional section, record ports")
{
	dns_stub s;
	assert_true(stub_start(&s));
	const char* n = "_aerospike._tcp.example.com";
	stub_srv(&s, n, 20, 5, 3002, "node2.example.com");
	stub_srv(&s, n, 10, 1, 3001, "node1.example.com");
	stub_srv(&s, n, 10, 9, 3003, "node3.example.com");
	stub_extra(&s, n, "node1.example.com", T_A, "10.0.1.1");
	stub_extra(&s, n, "node2.example.com", T_A, "10.0.1.2");
	stub_extra(&s, n, "node3.example.com", T_A, "10.0.1.3");
	stub_extra(&s, n, "node3.example.com", T_AAAA, "2001:db8::3");

	const char* names[] = {n};
	dns_setup d;
	as_dns_seed_provider_config* cfg = setup_init(&d, names, 1, &s, NULL);
	cfg->record_type = AS_DNS_RECORD_SRV;
	cfg->port = 4000;
	as_seed_provider provider;
	as_error err;
	assert_int_eq(as_discovery_dns_seed_provider(&err, cfg, &provider), AEROSPIKE_OK);

	as_seed_candidates out;
	assert_int_eq(run_refresh(&provider, &err, &out), AEROSPIKE_OK);
	char buf[512];
	assert_string_eq(hosts_str(out.hosts, buf, sizeof(buf)),
		"10.0.1.3:3003,10.0.1.1:3001,10.0.1.2:3002");
	assert_true(all_numeric(out.hosts));
	as_seed_candidates_destroy(&out);

	// No follow-up query was needed.
	assert_string_eq(stub_queries(&s, buf, sizeof(buf)), "_aerospike._tcp.example.com/SRV/udp");

	char again[512];
	assert_int_eq(refresh_str(&provider, &err, again, sizeof(again)), AEROSPIKE_OK);
	assert_string_eq(again, "10.0.1.3:3003,10.0.1.1:3001,10.0.1.2:3002");

	provider.destroy(provider.udata);
	stub_stop(&s);
}

TEST(dns_srv_follow_up, "U-4 SRV targets without additional records are resolved by A, then AAAA only without A")
{
	dns_stub s;
	assert_true(stub_start(&s));
	const char* n = "_aerospike._tcp.example.com";
	stub_srv(&s, n, 10, 1, 3001, "node1.example.com");
	stub_srv(&s, n, 10, 1, 3002, "node2.example.com");
	stub_a(&s, "node1.example.com", "10.0.1.9");
	stub_a(&s, "node1.example.com", "10.0.1.1");
	stub_aaaa(&s, "node1.example.com", "2001:db8::1");
	stub_aaaa(&s, "node2.example.com", "2001:db8::2");

	const char* names[] = {n};
	dns_setup d;
	as_dns_seed_provider_config* cfg = setup_init(&d, names, 1, &s, NULL);
	cfg->record_type = AS_DNS_RECORD_SRV;
	as_seed_provider provider;
	as_error err;
	assert_int_eq(as_discovery_dns_seed_provider(&err, cfg, &provider), AEROSPIKE_OK);

	char buf[512];
	assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)), AEROSPIKE_OK);

	// One address per record: the first sorted A address of node1.
	assert_string_eq(buf, "10.0.1.1:3001,2001:db8::2:3002");
	assert_int_eq(stub_count(&s, "node1.example.com/A"), 1);
	assert_int_eq(stub_count(&s, "node1.example.com/AAAA"), 0);
	assert_int_eq(stub_count(&s, "node2.example.com/A"), 1);
	assert_int_eq(stub_count(&s, "node2.example.com/AAAA"), 1);

	provider.destroy(provider.udata);
	stub_stop(&s);
}

TEST(dns_srv_targets, "U-4 SRV: a target is resolved once, '.' and port 0 records are dropped, unresolved targets warn; invalid records are logged once; additional AAAA records are not used")
{
	dns_stub s;
	assert_true(stub_start(&s));
	const char* n = "_aerospike._tcp.example.com";
	stub_srv(&s, n, 10, 1, 3001, "node1.example.com");
	stub_srv(&s, n, 10, 1, 3002, "gone.example.com");
	stub_srv(&s, n, 10, 1, 3003, ".");
	stub_srv(&s, n, 10, 1, 0, "node1.example.com");
	stub_srv(&s, n, 10, 1, 0, "node2.example.com");
	stub_srv(&s, n, 30, 1, 3009, "node1.example.com");
	stub_srv(&s, n, 20, 1, 3005, "node2.example.com");
	stub_a(&s, "node1.example.com", "10.0.1.1");
	stub_a(&s, "node2.example.com", "10.0.1.1");

	// An AAAA record of node3 in the additional section is not used: node3 is resolved by A, then
	// AAAA.
	const char* m = "_other._tcp.example.com";
	stub_srv(&s, m, 1, 1, 4000, "node3.example.com");
	stub_extra(&s, m, "node3.example.com", T_AAAA, "2001:db8::3");
	stub_extra(&s, m, "unrelated.example.com", T_A, "10.9.9.9");
	stub_aaaa(&s, "node3.example.com", "2001:db8::3");

	const char* names[] = {n, m};
	dns_setup d;
	as_dns_seed_provider_config* cfg = setup_init(&d, names, 2, &s, NULL);
	cfg->record_type = AS_DNS_RECORD_SRV;
	as_seed_provider provider;
	as_error err;
	assert_int_eq(as_discovery_dns_seed_provider(&err, cfg, &provider), AEROSPIKE_OK);

	log_start();
	char buf[512];
	assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)), AEROSPIKE_OK);
	log_stop();

	// Same address with distinct ports is kept; node2 shares node1's address on another port.
	assert_string_eq(buf, "10.0.1.1:3001,10.0.1.1:3005,10.0.1.1:3009,2001:db8::3:4000");
	assert_int_eq(stub_count(&s, "node1.example.com/A"), 1);
	assert_int_eq(stub_count(&s, "node3.example.com/A"), 1);
	assert_int_eq(stub_count(&s, "node3.example.com/AAAA"), 1);
	assert_int_eq(stub_count(&s, "gone.example.com/A"), 1);
	assert_int_eq(stub_count(&s, "gone.example.com/AAAA"), 0);

	assert_int_eq(log_size(), 3);
	assert_string_eq(log_line(0), "DNS seed name _aerospike._tcp.example.com returned an invalid "
		"SRV record: 10 1 0 node1.example.com.");
	assert_string_eq(log_line(1), "DNS seed name _aerospike._tcp.example.com returned 2 invalid "
		"SRV records; only the first was logged");
	assert_string_eq(log_line(2), "DNS seed name _aerospike._tcp.example.com SRV target "
		"gone.example.com. dropped: NXDOMAIN");

	provider.destroy(provider.udata);
	stub_stop(&s);
}

//---------------------------------
// Tests: partial and failed refreshes (U-4)
//---------------------------------

TEST(dns_partial, "U-4 partial success returns the resolved names and warns once per failed name")
{
	dns_stub s;
	assert_true(stub_start(&s));
	stub_a(&s, "good.example.com", "10.0.0.1");
	stub_rcode(&s, "bad.example.com", T_A, 2);
	stub_rcode(&s, "bad.example.com", T_AAAA, 2);

	const char* names[] = {"missing.example.com", "good.example.com", "bad.example.com"};
	dns_setup d;
	as_seed_provider provider;
	as_error err;
	assert_int_eq(as_discovery_dns_seed_provider(&err, setup_init(&d, names, 3, &s, NULL),
		&provider), AEROSPIKE_OK);

	log_start();
	char buf[256];
	assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)), AEROSPIKE_OK);
	log_stop();
	assert_string_eq(buf, "10.0.0.1:3000");
	assert_int_eq(log_size(), 2);
	assert_string_eq(log_line(0), "DNS seed name missing.example.com failed: NXDOMAIN");
	assert_string_eq(log_line(1), "DNS seed name bad.example.com failed: SERVFAIL");

	// NXDOMAIN for A ends that name: no AAAA query.
	assert_int_eq(stub_count(&s, "missing.example.com/AAAA"), 0);

	provider.destroy(provider.udata);
	stub_stop(&s);
}

TEST(dns_all_failed, "U-4 when every name fails the refresh returns -27 naming each name and its failure")
{
	dns_stub s;
	assert_true(stub_start(&s));
	stub_rcode(&s, "bad.example.com", T_A, 2);
	stub_rcode(&s, "bad.example.com", T_AAAA, 2);
	stub_srv(&s, "other.example.com", 1, 1, 1, "x.example.com");
	stub_rcode(&s, "refused.example.com", T_A, 5);
	stub_rcode(&s, "refused.example.com", T_AAAA, 5);
	stub_rcode(&s, "notimp.example.com", T_A, 4);
	stub_rcode(&s, "notimp.example.com", T_AAAA, 4);

	const char* names[] = {"missing.example.com", "bad.example.com", "other.example.com",
		"refused.example.com", "notimp.example.com"};
	dns_setup d;
	as_seed_provider provider;
	as_error err;
	assert_int_eq(as_discovery_dns_seed_provider(&err, setup_init(&d, names, 5, &s, NULL),
		&provider), AEROSPIKE_OK);

	char buf[256];
	assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_int_eq(err.code, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(err.message, "DNS seed discovery failed for every name: "
		"missing.example.com: NXDOMAIN; bad.example.com: SERVFAIL; other.example.com: empty answer; "
		"refused.example.com: REFUSED; notimp.example.com: rcode 4");
	provider.destroy(provider.udata);

	// SRV: an empty answer, only "." targets and unresolvable targets.
	stub_srv(&s, "_dot._tcp.example.com", 1, 1, 3000, ".");
	stub_srv(&s, "_gone._tcp.example.com", 1, 1, 3000, "gone.example.com");
	const char* srv_names[] = {"other.example.com", "_dot._tcp.example.com",
		"_gone._tcp.example.com", "_missing._tcp.example.com"};
	as_dns_seed_provider_config* cfg = setup_init(&d, srv_names, 4, &s, NULL);
	cfg->record_type = AS_DNS_RECORD_SRV;
	assert_int_eq(as_discovery_dns_seed_provider(&err, cfg, &provider), AEROSPIKE_OK);

	log_start();
	assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	log_stop();
	assert_string_eq(err.message, "DNS seed discovery failed for every name: "
		"other.example.com: no SRV target resolved; _dot._tcp.example.com: empty answer; "
		"_gone._tcp.example.com: no SRV target resolved; _missing._tcp.example.com: NXDOMAIN");

	// Dropped targets warn even though the refresh fails; failed names are in the error only.
	assert_int_eq(log_size(), 2);
	assert_string_eq(log_line(0), "DNS seed name other.example.com SRV target x.example.com. "
		"dropped: NXDOMAIN");
	assert_string_eq(log_line(1), "DNS seed name _gone._tcp.example.com SRV target "
		"gone.example.com. dropped: NXDOMAIN");

	provider.destroy(provider.udata);
	stub_stop(&s);
}

//---------------------------------
// Tests: resolver order and failover
//---------------------------------

TEST(dns_failover, "resolvers are tried in order once per query, starting with the one that last answered; SERVFAIL, REFUSED and ICMP errors fail over, NXDOMAIN does not")
{
	dns_stub bad, good;
	assert_true(stub_start(&bad));
	assert_true(stub_start(&good));
	stub_rcode(&bad, "seeds.example.com", T_A, 2);
	stub_rcode(&bad, "seeds.example.com", T_AAAA, 5);
	stub_a(&good, "seeds.example.com", "10.0.0.1");

	const char* names[] = {"seeds.example.com"};
	dns_setup d;
	as_seed_provider provider;
	as_error err;
	assert_int_eq(as_discovery_dns_seed_provider(&err, setup_init(&d, names, 1, &bad, &good),
		&provider), AEROSPIKE_OK);

	char buf[256];
	assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)), AEROSPIKE_OK);
	assert_string_eq(buf, "10.0.0.1:3000");

	// AAAA goes first to the resolver that answered A.
	assert_int_eq(stub_get(&bad, &bad.udp_queries), 1);
	assert_int_eq(stub_get(&good, &good.udp_queries), 2);

	// Each refresh starts again at the first resolver.
	assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)), AEROSPIKE_OK);
	assert_string_eq(buf, "10.0.0.1:3000");
	assert_int_eq(stub_get(&bad, &bad.udp_queries), 2);
	assert_int_eq(stub_get(&good, &good.udp_queries), 4);
	provider.destroy(provider.udata);

	// NXDOMAIN from the first resolver is final.
	stub_reset_queries(&good);
	const char* missing[] = {"missing.example.com"};
	assert_int_eq(as_discovery_dns_seed_provider(&err, setup_init(&d, missing, 1, &good, &bad),
		&provider), AEROSPIKE_OK);
	assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(err.message, "DNS seed discovery failed for every name: missing.example.com: NXDOMAIN");
	assert_int_eq(stub_get(&good, &good.udp_queries), 1);
	provider.destroy(provider.udata);

	// A closed resolver port fails over at once instead of waiting for the deadline.
	char closed[64];
	snprintf(closed, sizeof(closed), "127.0.0.1:%u", closed_port());
	setup_init(&d, names, 1, &good, NULL);
	d.servers[1] = d.servers[0];
	d.servers[0] = closed;
	d.cfg.servers_size = 2;
	d.cfg.timeout_ms = 2000;
	assert_int_eq(as_discovery_dns_seed_provider(&err, &d.cfg, &provider), AEROSPIKE_OK);

	uint64_t begin = cf_getms();
	assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)), AEROSPIKE_OK);
	assert_true(cf_getms() - begin < 500);
	assert_string_eq(buf, "10.0.0.1:3000");
	provider.destroy(provider.udata);

	// Every resolver failing reports the last non-timeout reason.
	assert_int_eq(as_discovery_dns_seed_provider(&err, setup_init(&d, names, 1, &bad, &bad),
		&provider), AEROSPIKE_OK);
	assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(err.message, "DNS seed discovery failed for every name: seeds.example.com: REFUSED");
	provider.destroy(provider.udata);

	stub_stop(&good);
	stub_stop(&bad);
}

TEST(dns_tcp_fallback, "a truncated UDP answer is retried over TCP to the same resolver")
{
	dns_stub s;
	assert_true(stub_start(&s));
	stub_set(&s, &s.truncate, true);
	stub_a(&s, "seeds.example.com", "10.0.0.1");
	stub_aaaa(&s, "seeds.example.com", "2001:db8::1");

	const char* names[] = {"seeds.example.com"};
	dns_setup d;
	as_seed_provider provider;
	as_error err;
	assert_int_eq(as_discovery_dns_seed_provider(&err, setup_init(&d, names, 1, &s, NULL),
		&provider), AEROSPIKE_OK);

	char buf[512];
	assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)), AEROSPIKE_OK);
	assert_string_eq(buf, "10.0.0.1:3000,2001:db8::1:3000");
	assert_string_eq(stub_queries(&s, buf, sizeof(buf)),
		"seeds.example.com/A/udp,seeds.example.com/A/tcp,seeds.example.com/AAAA/udp,"
		"seeds.example.com/AAAA/tcp");

	provider.destroy(provider.udata);
	stub_stop(&s);
}

TEST(dns_spoofed_datagrams, "datagrams with a wrong ID or question, non-responses and runts are ignored while waiting")
{
	dns_stub s;
	assert_true(stub_start(&s));
	stub_set(&s, &s.spoof_first, true);
	stub_a(&s, "seeds.example.com", "10.0.0.1");

	const char* names[] = {"seeds.example.com"};
	dns_setup d;
	as_seed_provider provider;
	as_error err;
	assert_int_eq(as_discovery_dns_seed_provider(&err, setup_init(&d, names, 1, &s, NULL),
		&provider), AEROSPIKE_OK);

	for (int i = 0; i < 5; i++) {
		char buf[256];
		assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)), AEROSPIKE_OK);
		assert_string_eq(buf, "10.0.0.1:3000");
	}
	provider.destroy(provider.udata);
	stub_stop(&s);
}

//---------------------------------
// Tests: packet hardening
//---------------------------------

// Copy the query ID and question into a response header. Returns the offset after the question.
static size_t
raw_begin(const uint8_t* q, size_t len, uint8_t* out, uint16_t an, uint16_t ar, uint16_t flags)
{
	char name[256];
	uint16_t type;
	size_t end = 12;

	stub_question(q, len, name, &type, &end);
	out[0] = q[0];
	out[1] = q[1];
	put16(out + 2, (uint16_t)(0x8180 | flags));
	put16(out + 4, 1);
	put16(out + 6, an);
	put16(out + 8, 0);
	put16(out + 10, ar);
	memcpy(out + 12, q + 12, end - 12);
	return end;
}

static size_t
raw_rr(uint8_t* out, size_t pos, uint16_t type, uint16_t rdlen)
{
	put16(out + pos, type);
	put16(out + pos + 2, 1);
	memset(out + pos + 4, 0, 4);
	put16(out + pos + 8, rdlen);
	return pos + 10;
}

static size_t
raw_self_pointer(const uint8_t* q, size_t len, uint8_t* out, bool tcp)
{
	(void)tcp;
	size_t pos = raw_begin(q, len, out, 1, 0, 0);
	put16(out + pos, (uint16_t)(0xc000 | pos));
	pos = raw_rr(out, pos + 2, T_A, 4);
	memset(out + pos, 1, 4);
	return pos + 4;
}

static size_t
raw_label_loop(const uint8_t* q, size_t len, uint8_t* out, bool tcp)
{
	(void)tcp;
	size_t pos = raw_begin(q, len, out, 1, 0, 0);
	size_t start = pos;
	out[pos++] = 1;
	out[pos++] = 'a';
	put16(out + pos, (uint16_t)(0xc000 | start));
	pos = raw_rr(out, pos + 2, T_A, 4);
	memset(out + pos, 1, 4);
	return pos + 4;
}

static size_t
raw_forward_pointer(const uint8_t* q, size_t len, uint8_t* out, bool tcp)
{
	(void)tcp;
	size_t pos = raw_begin(q, len, out, 1, 0, 0);
	put16(out + pos, (uint16_t)(0xc000 | (pos + 16)));
	pos = raw_rr(out, pos + 2, T_A, 4);
	memset(out + pos, 1, 4);
	pos += 4;
	out[pos++] = 1;
	out[pos++] = 'a';
	out[pos++] = 0;
	return pos;
}

static size_t
raw_bad_rdlen(const uint8_t* q, size_t len, uint8_t* out, bool tcp)
{
	(void)tcp;
	char name[256];
	uint16_t type;
	size_t end;
	stub_question(q, len, name, &type, &end);
	size_t pos = raw_begin(q, len, out, 1, 0, 0);
	put16(out + pos, 0xc00c);

	// A record of 5 bytes, or an AAAA record of 4 bytes.
	uint16_t rdlen = type == T_AAAA ? 4 : 5;
	pos = raw_rr(out, pos + 2, type, rdlen);
	memset(out + pos, 1, rdlen);
	return pos + rdlen;
}

static size_t
raw_rdlen_overflow(const uint8_t* q, size_t len, uint8_t* out, bool tcp)
{
	(void)tcp;
	size_t pos = raw_begin(q, len, out, 1, 0, 0);
	put16(out + pos, 0xc00c);
	pos = raw_rr(out, pos + 2, T_A, 200);
	memset(out + pos, 1, 4);
	return pos + 4;
}

static size_t
raw_count_overflow(const uint8_t* q, size_t len, uint8_t* out, bool tcp)
{
	(void)tcp;
	size_t pos = raw_begin(q, len, out, 3, 0, 0);
	put16(out + pos, 0xc00c);
	pos = raw_rr(out, pos + 2, T_A, 4);
	memset(out + pos, 1, 4);
	return pos + 4;
}

static size_t
raw_short_record(const uint8_t* q, size_t len, uint8_t* out, bool tcp)
{
	(void)tcp;
	size_t pos = raw_begin(q, len, out, 1, 0, 0);
	put16(out + pos, 0xc00c);
	put16(out + pos + 2, T_A);
	return pos + 4;
}

static size_t
raw_bad_label_type(const uint8_t* q, size_t len, uint8_t* out, bool tcp)
{
	(void)tcp;
	size_t pos = raw_begin(q, len, out, 1, 0, 0);
	out[pos++] = 0x41;
	out[pos++] = 'a';
	out[pos++] = 0;
	pos = raw_rr(out, pos, T_A, 4);
	memset(out + pos, 1, 4);
	return pos + 4;
}

static size_t
raw_bad_label_char(const uint8_t* q, size_t len, uint8_t* out, bool tcp)
{
	(void)tcp;
	size_t pos = raw_begin(q, len, out, 1, 0, 0);
	out[pos++] = 3;
	out[pos++] = 'a';
	out[pos++] = '.';
	out[pos++] = 'b';
	put16(out + pos, 0xc00c);
	pos = raw_rr(out, pos + 2, T_A, 4);
	memset(out + pos, 1, 4);
	return pos + 4;
}

static size_t
raw_long_name(const uint8_t* q, size_t len, uint8_t* out, bool tcp)
{
	(void)tcp;
	size_t pos = raw_begin(q, len, out, 1, 0, 0);

	for (int i = 0; i < 5; i++) {
		out[pos++] = 63;
		memset(out + pos, 'a', 63);
		pos += 63;
	}
	out[pos++] = 0;
	pos = raw_rr(out, pos, T_A, 4);
	memset(out + pos, 1, 4);
	return pos + 4;
}

static size_t
raw_always_truncated(const uint8_t* q, size_t len, uint8_t* out, bool tcp)
{
	(void)tcp;
	return raw_begin(q, len, out, 0, 0, 0x0200);
}

static size_t
raw_cname_overrun(const uint8_t* q, size_t len, uint8_t* out, bool tcp)
{
	(void)tcp;
	size_t pos = raw_begin(q, len, out, 1, 0, 0);
	put16(out + pos, 0xc00c);

	// The target is 3 bytes but rdlength says 2.
	pos = raw_rr(out, pos + 2, T_CNAME, 2);
	out[pos++] = 1;
	out[pos++] = 'x';
	out[pos++] = 0;
	return pos;
}

static size_t
raw_many_cnames(const uint8_t* q, size_t len, uint8_t* out, bool tcp)
{
	(void)tcp;
	size_t pos = raw_begin(q, len, out, 33, 0, 0);

	for (int i = 0; i < 33; i++) {
		put16(out + pos, 0xc00c);
		pos = raw_rr(out, pos + 2, T_CNAME, 3);
		out[pos++] = 1;
		out[pos++] = 'x';
		out[pos++] = 0;
	}
	return pos;
}

static size_t
raw_srv_overrun(const uint8_t* q, size_t len, uint8_t* out, bool tcp)
{
	(void)tcp;
	size_t pos = raw_begin(q, len, out, 1, 0, 0);
	put16(out + pos, 0xc00c);

	// Two stray bytes follow the target inside the record data.
	pos = raw_rr(out, pos + 2, T_SRV, 6 + 3 + 2);
	put16(out + pos, 1);
	put16(out + pos + 2, 1);
	put16(out + pos + 4, 3000);
	pos += 6;
	out[pos++] = 1;
	out[pos++] = 'x';
	out[pos++] = 0;
	out[pos++] = 0;
	out[pos++] = 0;
	return pos;
}

static size_t
raw_extra_bad_rdlen(const uint8_t* q, size_t len, uint8_t* out, bool tcp)
{
	(void)tcp;
	size_t pos = raw_begin(q, len, out, 1, 1, 0);
	put16(out + pos, 0xc00c);
	pos = raw_rr(out, pos + 2, T_SRV, 6 + 3);
	put16(out + pos, 1);
	put16(out + pos + 2, 1);
	put16(out + pos + 4, 3000);
	pos += 6;
	out[pos++] = 1;
	out[pos++] = 'x';
	out[pos++] = 0;

	// Additional AAAA record with 4 bytes of data.
	out[pos++] = 1;
	out[pos++] = 'x';
	out[pos++] = 0;
	pos = raw_rr(out, pos, T_AAAA, 4);
	memset(out + pos, 1, 4);
	return pos + 4;
}

static size_t
raw_wrong_question(const uint8_t* q, size_t len, uint8_t* out, bool tcp)
{
	(void)tcp;
	size_t pos = raw_begin(q, len, out, 0, 0, 0);
	put16(out + pos - 4, 15);
	return pos;
}

// _big._tcp.example.com/SRV: 2000 records over TCP. big.example.com/A: 300 records over TCP.
// tNNNN.example.com/A: one address. Anything else: an empty answer.
static size_t
raw_large(const uint8_t* q, size_t len, uint8_t* out, bool tcp)
{
	char name[256];
	uint16_t type;
	size_t end;

	if (!stub_question(q, len, name, &type, &end)) {
		return 0;
	}

	unsigned i;

	if (type == T_SRV && strcmp(name, "_big._tcp.example.com") == 0) {
		if (!tcp) {
			return raw_begin(q, len, out, 0, 0, 0x0200);
		}

		// 26 bytes per record, about 52 KB in total.
		size_t pos = raw_begin(q, len, out, 2000, 0, 0);

		for (i = 0; i < 2000; i++) {
			char label[8];
			snprintf(label, sizeof(label), "t%04u", i);
			put16(out + pos, 0xc00c);
			pos = raw_rr(out, pos + 2, T_SRV, 6 + 1 + 5 + 2);
			put16(out + pos, (uint16_t)i);
			put16(out + pos + 2, 0);
			put16(out + pos + 4, 3000);
			pos += 6;
			out[pos++] = 5;
			memcpy(out + pos, label, 5);
			pos += 5;

			// "example.com" in the question.
			put16(out + pos, 0xc016);
			pos += 2;
		}
		return pos;
	}

	if (type == T_A && strcmp(name, "big.example.com") == 0) {
		if (!tcp) {
			return raw_begin(q, len, out, 0, 0, 0x0200);
		}

		size_t pos = raw_begin(q, len, out, 300, 0, 0);

		for (i = 0; i < 300; i++) {
			put16(out + pos, 0xc00c);
			pos = raw_rr(out, pos + 2, T_A, 4);
			out[pos++] = 10;
			out[pos++] = 1;
			out[pos++] = (uint8_t)(i / 256);
			out[pos++] = (uint8_t)(i % 256);
		}
		return pos;
	}

	if (type == T_A && sscanf(name, "t%4u.example.com", &i) == 1) {
		size_t pos = raw_begin(q, len, out, 1, 0, 0);
		put16(out + pos, 0xc00c);
		pos = raw_rr(out, pos + 2, T_A, 4);
		out[pos++] = 10;
		out[pos++] = 2;
		out[pos++] = (uint8_t)(i / 256);
		out[pos++] = (uint8_t)(i % 256);
		return pos;
	}
	return raw_begin(q, len, out, 0, 0, 0);
}

typedef struct malformed_case_s {
	const char* label;
	stub_custom custom;
	bool srv;
} malformed_case;

TEST(dns_malformed, "malformed answers (pointer loops, bad lengths, counts, labels, TC over TCP) fail the query without overreads")
{
	dns_stub s;
	assert_true(stub_start(&s));

	const malformed_case cases[] = {
		{"self pointer", raw_self_pointer, false},
		{"label loop", raw_label_loop, false},
		{"forward pointer", raw_forward_pointer, false},
		{"bad rdlength", raw_bad_rdlen, false},
		{"rdlength overflow", raw_rdlen_overflow, false},
		{"count overflow", raw_count_overflow, false},
		{"short record", raw_short_record, false},
		{"bad label type", raw_bad_label_type, false},
		{"bad label char", raw_bad_label_char, false},
		{"long name", raw_long_name, false},
		{"truncated over TCP", raw_always_truncated, false},
		{"cname overrun", raw_cname_overrun, false},
		{"srv overrun", raw_srv_overrun, true},
		{"additional AAAA length", raw_extra_bad_rdlen, true},
		{"more than 32 CNAME records", raw_many_cnames, false},
	};

	for (uint32_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
		stub_set_custom(&s, cases[i].custom);
		const char* names[] = {"seeds.example.com"};
		dns_setup d;
		as_dns_seed_provider_config* cfg = setup_init(&d, names, 1, &s, NULL);
		cfg->timeout_ms = 1000;
		cfg->record_type = cases[i].srv ? AS_DNS_RECORD_SRV : AS_DNS_RECORD_A;
		as_seed_provider provider;
		as_error err;
		assert_int_eq(as_discovery_dns_seed_provider(&err, cfg, &provider), AEROSPIKE_OK);

		char buf[256];
		uint64_t begin = cf_getms();
		as_status status = refresh_str(&provider, &err, buf, sizeof(buf));
		uint64_t elapsed = cf_getms() - begin;
		provider.destroy(provider.udata);

		if (status != AEROSPIKE_ERR_SEED_DISCOVERY_FAILED || strcmp(err.message,
			"DNS seed discovery failed for every name: seeds.example.com: malformed answer") != 0 ||
			elapsed >= 500) {
			fail("case %s: status %d elapsed %llu message '%s'", cases[i].label, status,
				(unsigned long long)elapsed, err.message);
		}
	}

	// An answer to another question is ignored, so the query times out.
	stub_set_custom(&s, raw_wrong_question);
	const char* names[] = {"seeds.example.com"};
	dns_setup d;
	as_dns_seed_provider_config* cfg = setup_init(&d, names, 1, &s, NULL);
	cfg->timeout_ms = 100;
	as_seed_provider provider;
	as_error err;
	assert_int_eq(as_discovery_dns_seed_provider(&err, cfg, &provider), AEROSPIKE_OK);

	char buf[256];
	uint64_t begin = cf_getms();
	assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_true(cf_getms() - begin <= 100 + DNS_MARGIN_MS);
	assert_string_eq(err.message, "DNS seed discovery failed for every name: seeds.example.com: timeout");
	provider.destroy(provider.udata);
	stub_stop(&s);
}

TEST(dns_large_answers, "a large SRV or A answer is capped at 256 records, warned once, and resolved within timeout_ms")
{
	dns_stub s;
	assert_true(stub_start(&s));
	stub_set_custom(&s, raw_large);

	// 257 round trips need headroom under valgrind.
	const char* names[] = {"_big._tcp.example.com"};
	dns_setup d;
	as_dns_seed_provider_config* cfg = setup_init(&d, names, 1, &s, NULL);
	cfg->record_type = AS_DNS_RECORD_SRV;
	cfg->timeout_ms = 3000;
	as_seed_provider provider;
	as_error err;
	assert_int_eq(as_discovery_dns_seed_provider(&err, cfg, &provider), AEROSPIKE_OK);

	log_start();
	as_seed_candidates out;
	uint64_t begin = cf_getms();
	assert_int_eq(run_refresh(&provider, &err, &out), AEROSPIKE_OK);
	uint64_t elapsed = cf_getms() - begin;
	log_stop();
	info("2000 SRV records elapsed=%llu ms timeout=3000 ms", (unsigned long long)elapsed);
	assert_int_eq(out.hosts->size, 256);

	as_host* first = as_vector_get(out.hosts, 0);
	as_host* last = as_vector_get(out.hosts, 255);
	assert_string_eq(first->name, "10.2.0.0");
	assert_int_eq(first->port, 3000);
	assert_string_eq(last->name, "10.2.0.255");
	assert_int_eq(last->port, 3000);
	as_seed_candidates_destroy(&out);
	assert_int_eq(stub_get(&s, &s.udp_queries), 257);
	assert_int_eq(stub_get(&s, &s.tcp_queries), 1);
	assert_int_eq(log_size(), 1);
	assert_string_eq(log_line(0), "DNS seed name _big._tcp.example.com returned 2000 SRV records. "
		"Using the first 256");
	assert_true(elapsed <= 3000 + DNS_MARGIN_MS);
	provider.destroy(provider.udata);

	// The SRV part filled the query log.
	stub_reset_queries(&s);
	const char* a_names[] = {"big.example.com"};
	cfg = setup_init(&d, a_names, 1, &s, NULL);
	cfg->timeout_ms = 3000;
	assert_int_eq(as_discovery_dns_seed_provider(&err, cfg, &provider), AEROSPIKE_OK);

	log_start();
	assert_int_eq(run_refresh(&provider, &err, &out), AEROSPIKE_OK);
	log_stop();
	assert_int_eq(out.hosts->size, 256);
	first = as_vector_get(out.hosts, 0);
	assert_string_eq(first->name, "10.1.0.0");
	assert_int_eq(first->port, 3000);
	assert_true(all_numeric(out.hosts));
	as_seed_candidates_destroy(&out);
	assert_int_eq(log_size(), 1);
	assert_string_eq(log_line(0), "DNS seed name big.example.com returned 300 A records. "
		"Using the first 256");
	assert_int_eq(stub_count(&s, "big.example.com/AAAA"), 1);
	provider.destroy(provider.udata);
	stub_stop(&s);
}

//---------------------------------
// Tests: total deadline
//---------------------------------

TEST(dns_deadline_unanswered, "an unanswered refresh returns within timeout_ms for every name, type and resolver")
{
	dns_stub silent, silent2;
	assert_true(stub_start(&silent));
	assert_true(stub_start(&silent2));
	stub_set(&silent, &silent.silent, true);
	stub_set(&silent2, &silent2.silent, true);

	const char* names[] = {"a.example.com", "b.example.com", "c.example.com"};

	for (int servers = 1; servers <= 2; servers++) {
		dns_setup d;
		as_dns_seed_provider_config* cfg = setup_init(&d, names, 3, &silent,
			servers == 2 ? &silent2 : NULL);
		cfg->timeout_ms = 80;
		as_seed_provider provider;
		as_error err;
		assert_int_eq(as_discovery_dns_seed_provider(&err, cfg, &provider), AEROSPIKE_OK);

		for (int i = 0; i < 3; i++) {
			char buf[256];
			uint64_t begin = cf_getns();
			assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)),
				AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
			uint64_t elapsed = (cf_getns() - begin) / 1000000;
			info("unanswered servers=%d elapsed=%llu ms timeout=80 ms", servers,
				(unsigned long long)elapsed);
			assert_true(elapsed <= 80 + DNS_MARGIN_MS);

			// The first name alone takes 3/4 of the budget.
			assert_true(elapsed >= 60);
			assert_string_eq(err.message, "DNS seed discovery failed for every name: "
				"a.example.com: timeout; b.example.com: timeout; c.example.com: timeout");
		}
		provider.destroy(provider.udata);
	}

	// Each query takes half of the time left, so every name still reaches a resolver.
	assert_true(stub_count(&silent, "a.example.com/A") >= 3);
	assert_true(stub_count(&silent, "b.example.com/A") >= 3);
	assert_true(stub_count(&silent, "c.example.com/A") >= 3);
	assert_true(stub_count(&silent2, "c.example.com/A") >= 3);
	stub_stop(&silent2);
	stub_stop(&silent);
}

TEST(dns_deadline_shares, "a silent resolver costs one share and is skipped once another answers; a silent name leaves a quarter of the time left to the next")
{
	dns_stub silent, good;
	assert_true(stub_start(&silent));
	assert_true(stub_start(&good));
	stub_set(&silent, &silent.silent, true);
	stub_a(&good, "seeds.example.com", "10.0.0.1");
	stub_aaaa(&good, "seeds.example.com", "2001:db8::1");

	const char* names[] = {"seeds.example.com"};
	dns_setup d;
	as_dns_seed_provider_config* cfg = setup_init(&d, names, 1, &silent, &good);
	cfg->timeout_ms = 200;
	as_seed_provider provider;
	as_error err;
	assert_int_eq(as_discovery_dns_seed_provider(&err, cfg, &provider), AEROSPIKE_OK);

	char buf[256];
	uint64_t begin = cf_getms();
	assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)), AEROSPIKE_OK);
	uint64_t elapsed = cf_getms() - begin;
	info("silent first resolver elapsed=%llu ms", (unsigned long long)elapsed);
	assert_string_eq(buf, "10.0.0.1:3000,2001:db8::1:3000");
	assert_true(elapsed <= 200 + DNS_MARGIN_MS);

	// The A query spent the silent resolver's share of 50 ms; AAAA went to the resolver that
	// answered.
	assert_int_eq(stub_count(&silent, "seeds.example.com/A"), 1);
	assert_int_eq(stub_count(&silent, "seeds.example.com/AAAA"), 0);
	assert_true(elapsed >= 40);
	provider.destroy(provider.udata);

	// A name whose queries are dropped leaves a quarter of the time left to the name after it.
	stub_rcode(&good, "dropped.example.com", T_A, STUB_SILENT);
	stub_rcode(&good, "dropped.example.com", T_AAAA, STUB_SILENT);
	const char* two[] = {"dropped.example.com", "seeds.example.com"};
	cfg = setup_init(&d, two, 2, &good, NULL);
	cfg->timeout_ms = 200;
	assert_int_eq(as_discovery_dns_seed_provider(&err, cfg, &provider), AEROSPIKE_OK);

	log_start();
	begin = cf_getms();
	assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)), AEROSPIKE_OK);
	elapsed = cf_getms() - begin;
	log_stop();
	assert_string_eq(buf, "10.0.0.1:3000,2001:db8::1:3000");
	assert_true(elapsed <= 200 + DNS_MARGIN_MS);
	assert_int_eq(log_size(), 1);
	assert_string_eq(log_line(0), "DNS seed name dropped.example.com failed: timeout");
	provider.destroy(provider.udata);

	stub_stop(&good);
	stub_stop(&silent);
}

TEST(dns_resolver_rotation, "a dead first resolver costs one share per refresh: later queries start at the resolver that answered")
{
	dns_stub silent, good;
	assert_true(stub_start(&silent));
	assert_true(stub_start(&good));
	stub_set(&silent, &silent.silent, true);
	stub_set_delay(&good, 10);
	const char* n = "_aerospike._tcp.example.com";

	for (unsigned i = 1; i <= 3; i++) {
		char target[64];
		char addr[32];
		snprintf(target, sizeof(target), "node%u.example.com", i);
		snprintf(addr, sizeof(addr), "10.0.1.%u", i);
		stub_srv(&good, n, 10, 1, 3000 + i, target);
		stub_a(&good, target, addr);
	}

	const char* names[] = {n};
	dns_setup d;
	as_dns_seed_provider_config* cfg = setup_init(&d, names, 1, &silent, &good);
	cfg->record_type = AS_DNS_RECORD_SRV;
	cfg->timeout_ms = 160;
	as_seed_provider provider;
	as_error err;
	assert_int_eq(as_discovery_dns_seed_provider(&err, cfg, &provider), AEROSPIKE_OK);

	// 40 ms for the silent SRV share, 10 ms for the answer, then 3 x 10 ms for the targets. Each
	// target's share (27.5, 25 and 22.5 ms) is well above the 10 ms delay.
	char buf[256];
	uint64_t begin = cf_getms();
	assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)), AEROSPIKE_OK);
	uint64_t elapsed = cf_getms() - begin;
	info("rotation elapsed=%llu ms timeout=160 ms", (unsigned long long)elapsed);
	assert_string_eq(buf, "10.0.1.1:3001,10.0.1.2:3002,10.0.1.3:3003");
	assert_true(elapsed <= 160 + DNS_MARGIN_MS);

	// Without rotation the silent resolver would get every target's A query.
	assert_int_eq(stub_get(&silent, &silent.udp_queries), 1);
	assert_int_eq(stub_count(&silent, "node1.example.com/A"), 0);

	// Rotation does not persist across refreshes.
	assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)), AEROSPIKE_OK);
	assert_string_eq(buf, "10.0.1.1:3001,10.0.1.2:3002,10.0.1.3:3003");
	assert_int_eq(stub_get(&silent, &silent.udp_queries), 2);

	provider.destroy(provider.udata);
	stub_stop(&good);
	stub_stop(&silent);
}

TEST(dns_deadline_srv_targets, "unanswered SRV targets share the refresh budget and are each dropped with a warning; targets left without time are counted in one line")
{
	dns_stub s;
	assert_true(stub_start(&s));
	const char* n = "_aerospike._tcp.example.com";
	const char* targets[] = {"node1.example.com", "node2.example.com", "node3.example.com"};

	for (int i = 0; i < 3; i++) {
		stub_srv(&s, n, 10, 1, 3001 + i, targets[i]);
		stub_rcode(&s, targets[i], T_A, STUB_SILENT);
		stub_rcode(&s, targets[i], T_AAAA, STUB_SILENT);
	}

	const char* names[] = {n};
	dns_setup d;
	as_dns_seed_provider_config* cfg = setup_init(&d, names, 1, &s, NULL);
	cfg->record_type = AS_DNS_RECORD_SRV;
	cfg->timeout_ms = 60;
	as_seed_provider provider;
	as_error err;
	assert_int_eq(as_discovery_dns_seed_provider(&err, cfg, &provider), AEROSPIKE_OK);

	log_start();
	char buf[256];
	uint64_t begin = cf_getms();
	assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	uint64_t elapsed = cf_getms() - begin;
	log_stop();
	info("unanswered SRV targets elapsed=%llu ms timeout=60 ms", (unsigned long long)elapsed);
	assert_true(elapsed <= 60 + DNS_MARGIN_MS);
	assert_string_eq(err.message, "DNS seed discovery failed for every name: "
		"_aerospike._tcp.example.com: no SRV target resolved");
	assert_int_eq(log_size(), 3);

	for (uint32_t i = 0; i < 3; i++) {
		const char* line = log_line(i);
		size_t len = strlen(line);
		assert_true(len > 16 && strcmp(line + len - 16, "dropped: timeout") == 0);
	}

	// Every target reached the resolver within the one budget.
	for (int i = 0; i < 3; i++) {
		char query[64];
		snprintf(query, sizeof(query), "%s/A", targets[i]);
		assert_int_eq(stub_count(&s, query), 1);
	}
	provider.destroy(provider.udata);
	stub_stop(&s);

	// Each silent target uses 3/4 of the time left, so targets 1-3 take about 59 ms and the rest
	// get no query.
	dns_stub late;
	assert_true(stub_start(&late));
	const char* ln = "_late._tcp.example.com";

	for (unsigned i = 1; i <= 12; i++) {
		char target[64];
		snprintf(target, sizeof(target), "late%u.example.com", i);
		stub_srv(&late, ln, 10, 1, 3000 + i, target);
		stub_rcode(&late, target, T_A, STUB_SILENT);
		stub_rcode(&late, target, T_AAAA, STUB_SILENT);
	}

	const char* late_names[] = {ln};
	cfg = setup_init(&d, late_names, 1, &late, NULL);
	cfg->record_type = AS_DNS_RECORD_SRV;
	cfg->timeout_ms = 60;
	assert_int_eq(as_discovery_dns_seed_provider(&err, cfg, &provider), AEROSPIKE_OK);

	log_start();
	begin = cf_getms();
	assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	elapsed = cf_getms() - begin;
	log_stop();
	info("12 unanswered SRV targets elapsed=%llu ms timeout=60 ms", (unsigned long long)elapsed);
	assert_true(elapsed <= 60 + DNS_MARGIN_MS);
	assert_string_eq(err.message, "DNS seed discovery failed for every name: "
		"_late._tcp.example.com: no SRV target resolved");

	uint32_t k = 0;

	for (uint32_t i = 0; i < log_size(); i++) {
		const char* line = log_line(i);
		size_t len = strlen(line);

		if (len > 16 && strcmp(line + len - 16, "dropped: timeout") == 0) {
			k++;
		}
	}
	info("queried targets=%u", k);
	assert_true(k >= 2 && k <= 5);
	assert_int_eq(log_size(), k + 1);

	char expected[128];
	snprintf(expected, sizeof(expected),
		"DNS seed name _late._tcp.example.com SRV targets not queried before the deadline: %u",
		12 - k);
	assert_string_eq(log_line(k), expected);
	provider.destroy(provider.udata);
	stub_stop(&late);
}

TEST(dns_deadline_tcp, "a truncated answer whose TCP retry hangs, after a silent resolver, still returns within timeout_ms")
{
	dns_stub silent, truncated;
	assert_true(stub_start(&silent));
	assert_true(stub_start(&truncated));
	stub_set(&silent, &silent.silent, true);
	stub_set(&silent, &silent.hang_tcp, true);
	stub_set(&truncated, &truncated.truncate, true);
	stub_set(&truncated, &truncated.hang_tcp, true);
	stub_a(&truncated, "a.example.com", "10.0.0.1");

	const char* names[] = {"a.example.com"};
	dns_setup d;
	as_dns_seed_provider_config* cfg = setup_init(&d, names, 1, &silent, &truncated);
	cfg->timeout_ms = 200;
	as_seed_provider provider;
	as_error err;
	assert_int_eq(as_discovery_dns_seed_provider(&err, cfg, &provider), AEROSPIKE_OK);

	char buf[256];
	uint64_t begin = cf_getms();
	assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	uint64_t elapsed = cf_getms() - begin;
	info("truncated with hanging TCP elapsed=%llu ms timeout=200 ms", (unsigned long long)elapsed);
	assert_true(elapsed <= 200 + DNS_MARGIN_MS);
	assert_string_eq(err.message, "DNS seed discovery failed for every name: a.example.com: timeout");
	assert_true(stub_get(&truncated, &truncated.udp_queries) >= 1);
	assert_true(stub_get(&truncated, &truncated.tcp_accepts) >= 1);
	assert_int_eq(stub_get(&silent, &silent.tcp_accepts), 0);

	provider.destroy(provider.udata);
	stub_stop(&truncated);
	stub_stop(&silent);
}

TEST(dns_no_thread, "the provider creates no thread across repeated refreshes")
{
	dns_stub s;
	assert_true(stub_start(&s));
	stub_a(&s, "seeds.example.com", "10.0.0.1");

	const char* names[] = {"seeds.example.com"};
	dns_setup d;
	as_seed_provider provider;
	as_error err;
	assert_int_eq(as_discovery_dns_seed_provider(&err, setup_init(&d, names, 1, &s, NULL),
		&provider), AEROSPIKE_OK);

	char buf[256];
	assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)), AEROSPIKE_OK);
	int before = thread_count();

	for (int i = 0; i < 10; i++) {
		assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)), AEROSPIKE_OK);
		assert_string_eq(buf, "10.0.0.1:3000");
	}

	if (before >= 0) {
		assert_int_eq(thread_count(), before);
	}
	provider.destroy(provider.udata);
	stub_stop(&s);
}

TEST(dns_no_leaked_descriptors, "every transport path, error and failed construction releases its sockets")
{
	int before = fd_count();
	assert_true(before > 0);

	dns_stub good;
	dns_stub silent;
	assert_true(stub_start(&good));
	assert_true(stub_start(&silent));
	stub_a(&good, "seeds.example.com", "10.0.0.1");
	stub_rcode(&good, "bad.example.com", T_A, 2);
	stub_rcode(&good, "bad.example.com", T_AAAA, 2);
	stub_set(&silent, &silent.silent, true);

	dns_stub tc;
	assert_true(stub_start(&tc));
	stub_a(&tc, "seeds.example.com", "10.0.0.2");
	stub_set(&tc, &tc.truncate, true);

	char closed[64];
	snprintf(closed, sizeof(closed), "127.0.0.1:%u", closed_port());

	// Answered, NXDOMAIN, SERVFAIL, silent UDP, truncated UDP with TCP success, truncated UDP
	// with a hanging TCP retry, and an ICMP-refused resolver.
	const char* names[] = {"seeds.example.com", "missing.example.com", "bad.example.com"};

	for (int round = 0; round < 3; round++) {
		dns_setup d;
		as_seed_provider provider;
		as_error err;
		char buf[256];

		as_dns_seed_provider_config* cfg = (round == 0) ? setup_init(&d, names, 3, &good, NULL) :
			(round == 1) ? setup_init(&d, names, 1, &silent, &tc) :
			setup_init(&d, names, 1, &good, NULL);

		if (round == 2) {
			d.servers[1] = d.servers[0];
			d.servers[0] = closed;
			d.cfg.servers_size = 2;
		}
		cfg->timeout_ms = 200;
		assert_int_eq(as_discovery_dns_seed_provider(&err, cfg, &provider), AEROSPIKE_OK);

		for (int i = 0; i < 3; i++) {
			refresh_str(&provider, &err, buf, sizeof(buf));
		}
		provider.destroy(provider.udata);

		if (round == 1) {
			stub_set(&tc, &tc.hang_tcp, true);
			assert_int_eq(as_discovery_dns_seed_provider(&err, cfg, &provider), AEROSPIKE_OK);
			assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)),
				AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
			provider.destroy(provider.udata);
		}
	}
	assert_true(stub_get(&tc, &tc.tcp_accepts) >= 2);

	// Construction failures after names were copied.
	const char* bad_servers[] = {"127.0.0.1", "not-an-ip"};
	as_dns_seed_provider_config cfg;
	as_dns_seed_provider_config_init(&cfg);
	cfg.names = names;
	cfg.names_size = 1;
	cfg.servers = bad_servers;
	cfg.servers_size = 2;
	as_seed_provider provider;
	as_error err;
	assert_int_eq(as_discovery_dns_seed_provider(&err, &cfg, &provider), AEROSPIKE_ERR_PARAM);

	stub_stop(&tc);
	stub_stop(&silent);
	stub_stop(&good);
	assert_int_eq(settle(fd_count, before), before);
}

//---------------------------------
// Static Functions: manager harness
//---------------------------------

static const char*
snap_str(as_seed_manager* m, char* buf, size_t size)
{
	as_seed_snapshot* snapshot = as_seed_manager_reserve(m);
	hosts_str(as_seed_snapshot_hosts(snapshot), buf, size);
	as_seed_snapshot_release(snapshot);
	return buf;
}

static as_seed_manager_stats
stats_of(as_seed_manager* m)
{
	as_seed_manager_stats stats;
	as_seed_manager_get_stats(m, &stats);
	return stats;
}

static void
mgr_config(as_config* config, as_discovery_execution_mode mode, uint32_t tend_interval)
{
	as_config_init(config);
	config->tender_interval = tend_interval;
	config->discovery.execution_mode = mode;
	config->discovery.refresh_interval_ms = tend_interval;
	config->discovery.tend_refresh_deadline_ms = tend_interval / 2;
}

static as_status
mgr_register(as_config* config, dns_stub* s, const char* name, uint32_t timeout_ms, as_error* err)
{
	const char* names[] = {name};
	dns_setup d;
	as_dns_seed_provider_config* cfg = setup_init(&d, names, 1, s, NULL);
	cfg->timeout_ms = timeout_ms;
	return as_config_set_dns_seed_provider(config, cfg, err);
}

static bool
fake_periodic(void* udata)
{
	(void)udata;
	return true;
}

static as_status
fake_refresh(as_error* err, const as_seed_candidate_context* context, as_seed_candidates* out,
	void* udata)
{
	(void)err;
	(void)context;
	(void)udata;
	as_seed_candidates_append(out, "10.0.0.7", NULL, 3000);
	return AEROSPIKE_OK;
}

//---------------------------------
// Tests: seed manager integration
//---------------------------------

TEST(dns_tend_validation, "execution-mode validation rejects a DNS timeout above tend_refresh_deadline_ms or refresh_timeout_ms before any query and accepts equality")
{
	dns_stub s;
	assert_true(stub_start(&s));
	stub_a(&s, "seeds.example.com", "10.0.0.1");

	as_config config;
	as_error err;
	as_seed_manager* m = NULL;

	mgr_config(&config, AS_DISCOVERY_EXECUTION_TEND, 1000);
	config.discovery.tend_refresh_deadline_ms = 79;
	assert_int_eq(mgr_register(&config, &s, "seeds.example.com", 80, &err), AEROSPIKE_OK);
	assert_int_eq(as_seed_manager_create(&err, &config, &m), AEROSPIKE_ERR_PARAM);
	assert_string_eq(err.message, "DNS seed provider timeout_ms 80 exceeds discovery "
		"tend_refresh_deadline_ms 79. Lower timeout_ms or use execution_mode thread");
	assert_int_eq(stub_get(&s, &s.udp_queries), 0);

	// The generic rules still apply first.
	config.discovery.tend_refresh_deadline_ms = 501;
	assert_int_eq(as_seed_manager_create(&err, &config, &m), AEROSPIKE_ERR_PARAM);
	assert_not_null(strstr(err.message, "tend_refresh_deadline_ms 501"));

	// Equality is accepted and refreshes on the tend cadence.
	config.discovery.tend_refresh_deadline_ms = 80;
	assert_int_eq(as_seed_manager_create(&err, &config, &m), AEROSPIKE_OK);
	assert_int_eq(as_seed_manager_start(m, &err), AEROSPIKE_OK);
	char buf[256];
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "10.0.0.1:3000");
	assert_int_eq(stub_get(&s, &s.udp_queries), 2);

	for (int i = 0; i < 3; i++) {
		as_seed_manager_tend(m);
	}
	assert_int_eq(stub_get(&s, &s.udp_queries), 8);
	as_seed_manager_stats stats = stats_of(m);
	assert_int_eq(stats.refreshes, 4);
	assert_int_eq(stats.overruns, 0);
	assert_int_eq(stats.failures, 0);
	assert_false(stats.thread_running);
	as_seed_manager_destroy(m);
	as_config_destroy(&config);

	// Thread mode does not impose the tend deadline, but the DNS timeout must not exceed
	// refresh_timeout_ms.
	mgr_config(&config, AS_DISCOVERY_EXECUTION_THREAD, 1000);
	config.discovery.tend_refresh_deadline_ms = 10;
	config.discovery.refresh_interval_ms = 600000;
	assert_int_eq(mgr_register(&config, &s, "seeds.example.com", 2000, &err), AEROSPIKE_OK);
	uint32_t queries = stub_get(&s, &s.udp_queries);
	config.discovery.refresh_timeout_ms = 1999;
	assert_int_eq(as_seed_manager_create(&err, &config, &m), AEROSPIKE_ERR_PARAM);
	assert_string_eq(err.message, "DNS seed provider timeout_ms 2000 exceeds discovery "
		"refresh_timeout_ms 1999. Lower timeout_ms or raise refresh_timeout_ms");
	config.discovery.refresh_timeout_ms = 2000;
	assert_int_eq(as_seed_manager_create(&err, &config, &m), AEROSPIKE_OK);
	assert_int_eq(stub_get(&s, &s.udp_queries), queries);
	as_seed_manager_destroy(m);
	as_config_destroy(&config);

	// The check identifies the built-in, not a provider that is only named "dns".
	mgr_config(&config, AS_DISCOVERY_EXECUTION_TEND, 1000);
	config.discovery.tend_refresh_deadline_ms = 10;
	as_seed_provider fake = {
		.size = sizeof(as_seed_provider),
		.api_version = AS_DISCOVERY_API_VERSION,
		.name = "dns",
		.refresh = fake_refresh,
		.needs_periodic_refresh = fake_periodic,
		.supports_deadline = fake_periodic
	};
	assert_int_eq(as_config_set_seed_provider(&config, &fake, &err), AEROSPIKE_OK);
	assert_int_eq(as_seed_manager_create(&err, &config, &m), AEROSPIKE_OK);
	as_seed_manager_destroy(m);
	as_config_destroy(&config);
	stub_stop(&s);
}

TEST(dns_tend_silent_resolver, "timeout_ms equal to tend_refresh_deadline_ms with silent resolvers ends before the deadline and never trips")
{
	// This checks the budget shape: 1 name and 1 silent resolver end at about 60 ms, 2 names and
	// 2 silent resolvers at about 75 ms. 4 names and 1 silent resolver use up the budget, so that
	// variant only guards against tripping. Whole-millisecond rounding of the waits and of the
	// overrun check cannot be told apart from nanoseconds here.
	dns_stub s1, s2;
	assert_true(stub_start(&s1));
	assert_true(stub_start(&s2));
	stub_set(&s1, &s1.silent, true);
	stub_set(&s2, &s2.silent, true);

	const char* names[] = {"a.example.com", "b.example.com", "c.example.com", "d.example.com"};
	const uint32_t names_sizes[] = {1, 2, 4};
	dns_stub* seconds[] = {NULL, &s2, NULL};

	for (uint32_t v = 0; v < 3; v++) {
		as_config config;
		as_error err;
		as_seed_manager* m = NULL;
		mgr_config(&config, AS_DISCOVERY_EXECUTION_TEND, 1000);
		config.discovery.tend_refresh_deadline_ms = 80;
		as_config_add_host(&config, "10.9.9.9", 3000);

		dns_setup d;
		as_dns_seed_provider_config* cfg = setup_init(&d, names, names_sizes[v], &s1, seconds[v]);
		cfg->timeout_ms = 80;
		assert_int_eq(as_config_set_dns_seed_provider(&config, cfg, &err), AEROSPIKE_OK);
		assert_int_eq(as_seed_manager_create(&err, &config, &m), AEROSPIKE_OK);
		assert_int_eq(as_seed_manager_start(m, &err), AEROSPIKE_OK);

		uint32_t failures = stats_of(m).failures;

		for (int i = 0; i < 5; i++) {
			uint64_t begin = cf_getns();
			as_seed_manager_tend(m);
			uint64_t elapsed = (cf_getns() - begin) / 1000000;
			info("silent variant=%u names=%u tend=%d elapsed=%llu ms deadline=80 ms", v,
				names_sizes[v], i, (unsigned long long)elapsed);
			assert_true(elapsed <= 80 + DNS_MARGIN_MS);
		}

		as_seed_manager_stats stats = stats_of(m);
		info("silent variant=%u overruns=%u", v, stats.overruns);
		assert_false(stats.tripped);

		if (v < 2) {
			assert_int_eq(stats.overruns, 0);

			// The overrun path does not count a failure.
			assert_int_eq(stats.failures, failures + 5);
		}

		char buf[256];
		assert_string_eq(snap_str(m, buf, sizeof(buf)), "10.9.9.9:3000");
		as_seed_manager_destroy(m);
		as_config_destroy(&config);
	}
	stub_stop(&s2);
	stub_stop(&s1);
}

TEST(dns_snapshot_retention, "U-4 NXDOMAIN after a publication keeps the exact previous snapshot; recovery publishes again")
{
	dns_stub s;
	assert_true(stub_start(&s));
	stub_a(&s, "seeds.example.com", "10.0.0.1");

	for (int mode = 0; mode < 2; mode++) {
		as_config config;
		as_error err;
		as_seed_manager* m = NULL;
		mgr_config(&config, AS_DISCOVERY_EXECUTION_TEND, 1000);
		config.discovery.merge_policy = mode ? AS_SEED_REPLACE : AS_SEED_MERGE;
		as_config_add_host(&config, "10.9.9.9", 3000);
		assert_int_eq(mgr_register(&config, &s, "seeds.example.com", 80, &err), AEROSPIKE_OK);
		assert_int_eq(as_seed_manager_create(&err, &config, &m), AEROSPIKE_OK);
		assert_int_eq(as_seed_manager_start(m, &err), AEROSPIKE_OK);

		char buf[256];
		assert_string_eq(snap_str(m, buf, sizeof(buf)), "10.9.9.9:3000,10.0.0.1:3000");
		as_seed_snapshot* before = as_seed_manager_reserve(m);
		uint32_t publications = stats_of(m).publications;

		stub_clear(&s);
		log_start();
		as_seed_manager_tend(m);
		log_stop();

		as_seed_snapshot* after = as_seed_manager_reserve(m);
		assert_true(after == before);
		assert_int_eq(stats_of(m).publications, publications);
		assert_int_eq(stats_of(m).failures, 1);
		assert_int_eq(log_size(), 1);
		assert_string_eq(log_line(0), "Seed provider dns refresh failed: AEROSPIKE_ERR_SEED_DISCOVERY_FAILED "
			"DNS seed discovery failed for every name: seeds.example.com: NXDOMAIN. Keeping the "
			"previous seeds");
		as_seed_snapshot_release(after);
		as_seed_snapshot_release(before);

		stub_a(&s, "seeds.example.com", "10.0.0.2");
		as_seed_manager_tend(m);
		assert_string_eq(snap_str(m, buf, sizeof(buf)),
			mode ? "10.0.0.2:3000" : "10.9.9.9:3000,10.0.0.2:3000");
		assert_int_eq(stats_of(m).publications, publications + 1);

		as_seed_manager_destroy(m);
		as_config_destroy(&config);
		stub_clear(&s);
		stub_a(&s, "seeds.example.com", "10.0.0.1");
	}
	stub_stop(&s);
}

TEST(dns_thread_mode, "thread mode runs DNS on the one client-owned discovery thread and allows a longer timeout")
{
	dns_stub s;
	assert_true(stub_start(&s));
	stub_a(&s, "seeds.example.com", "10.0.0.1");
	int before = discovery_thread_count();

	as_config config;
	as_error err;
	as_seed_manager* m = NULL;
	mgr_config(&config, AS_DISCOVERY_EXECUTION_THREAD, 1000);
	config.discovery.refresh_interval_ms = 600000;
	assert_int_eq(mgr_register(&config, &s, "seeds.example.com", 2000, &err), AEROSPIKE_OK);
	assert_int_eq(as_seed_manager_create(&err, &config, &m), AEROSPIKE_OK);
	assert_int_eq(as_seed_manager_start(m, &err), AEROSPIKE_OK);

	char buf[256];
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "10.0.0.1:3000");
	assert_true(stats_of(m).thread_running);

	if (before >= 0) {
		assert_int_eq(settle(discovery_thread_count, before + 1), before + 1);
	}

	// Tend never refreshes in thread mode.
	uint32_t queries = stub_get(&s, &s.udp_queries);

	for (int i = 0; i < 3; i++) {
		as_seed_manager_tend(m);
	}
	assert_int_eq(stub_get(&s, &s.udp_queries), queries);

	as_seed_manager_destroy(m);
	as_config_destroy(&config);

	if (before >= 0) {
		assert_int_eq(settle(discovery_thread_count, before), before);
	}
	stub_stop(&s);
}

TEST(dns_init_failure, "an all-failed initial refresh falls back to configured seeds, or returns -27 naming dns and every name")
{
	dns_stub s;
	assert_true(stub_start(&s));
	stub_rcode(&s, "bad.example.com", T_A, 2);
	stub_rcode(&s, "bad.example.com", T_AAAA, 2);

	for (int mode = 0; mode < 2; mode++) {
		as_config config;
		as_error err;
		as_seed_manager* m = NULL;
		mgr_config(&config, (as_discovery_execution_mode)mode, 1000);
		as_config_add_host(&config, "10.9.9.9", 3000);

		const char* names[] = {"seeds.example.com", "bad.example.com"};
		dns_setup d;
		assert_int_eq(as_config_set_dns_seed_provider(&config, setup_init(&d, names, 2, &s, NULL),
			&err), AEROSPIKE_OK);
		assert_int_eq(as_seed_manager_create(&err, &config, &m), AEROSPIKE_OK);
		assert_int_eq(as_seed_manager_start(m, &err), AEROSPIKE_OK);
		char buf[256];
		assert_string_eq(snap_str(m, buf, sizeof(buf)), "10.9.9.9:3000");
		as_seed_manager_destroy(m);
		as_config_destroy(&config);

		mgr_config(&config, (as_discovery_execution_mode)mode, 1000);
		assert_int_eq(as_config_set_dns_seed_provider(&config, setup_init(&d, names, 2, &s, NULL),
			&err), AEROSPIKE_OK);
		assert_int_eq(as_seed_manager_create(&err, &config, &m), AEROSPIKE_OK);
		assert_int_eq(as_seed_manager_start(m, &err), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
		assert_string_eq(err.message, "Seed provider dns failed and no seed host is left: "
			"DNS seed discovery failed for every name: seeds.example.com: NXDOMAIN; "
			"bad.example.com: SERVFAIL");
		as_seed_manager_destroy(m);
		as_config_destroy(&config);
	}
	stub_stop(&s);
}

//---------------------------------
// Static Functions: client helpers
//---------------------------------

void
aerospike_destroy_internal(aerospike* as);

static void
client_config(as_config* config, as_discovery_execution_mode mode)
{
	as_config_init(config);
	config->tender_interval = AS_TEND_INTERVAL_MIN;
	config->conn_timeout_ms = 1000;
	config->fail_if_not_connected = true;
	config->discovery.execution_mode = mode;
	config->discovery.refresh_interval_ms = AS_TEND_INTERVAL_MIN;
	config->discovery.tend_refresh_deadline_ms = AS_TEND_INTERVAL_MIN / 2;
}

// Close and destroy the client. Async close also destroys it; a client that never connected
// is destroyed directly so its registrations are released in every mode.
static void
client_close(aerospike* as)
{
	as_error err;

	if (as->cluster) {
		aerospike_close(as, &err);

		if (as_event_loop_size == 0 || as_event_single_thread) {
			aerospike_destroy(as);
		}
	}
	else {
		aerospike_destroy_internal(as);
	}
}

// What a sync-only aerospike_close() does, also when async event loops exist.
static void
client_sync_close(aerospike* as)
{
	as_cluster_destroy(as->cluster);
	as->cluster = NULL;
	as->config.discovery.registration_locked = false;
}

static bool
has_node(aerospike* client, const char* name)
{
	as_nodes* nodes = as_nodes_reserve(client->cluster);
	bool found = false;

	for (uint32_t i = 0; i < nodes->size; i++) {
		if (strcmp(nodes->array[i]->name, name) == 0) {
			found = true;
			break;
		}
	}
	as_nodes_release(nodes);
	return found;
}

static bool
wait_for_nodes(aerospike* client, uint32_t size, uint32_t timeout_ms)
{
	uint64_t end = cf_getms() + timeout_ms;

	while (true) {
		as_nodes* nodes = as_nodes_reserve(client->cluster);
		uint32_t count = nodes->size;
		as_nodes_release(nodes);

		if (count == size) {
			return true;
		}

		if (cf_getms() >= end) {
			return false;
		}
		as_sleep(5);
	}
}

//---------------------------------
// Tests: local N-4 precursor
//---------------------------------

typedef struct n4_env_s {
	fnode a;
	fnode b;
	dns_stub s;
} n4_env;

// Node A is published in DNS; node B is only advertised by A in peers-clear-std.
static bool
n4_start(n4_env* env)
{
	if (!fnode_start(&env->a, "A") || !fnode_start(&env->b, "B") || !stub_start(&env->s)) {
		return false;
	}

	char peers[128];
	snprintf(peers, sizeof(peers), "1,3000,[[B,,[127.0.0.1:%u]]]", env->b.port);
	fnode_set_peers(&env->a, peers);
	snprintf(peers, sizeof(peers), "1,3000,[[A,,[127.0.0.1:%u]]]", env->a.port);
	fnode_set_peers(&env->b, peers);

	stub_a(&env->s, "seeds.n4.test", "127.0.0.1");

	char target[64];
	snprintf(target, sizeof(target), "%u 1 %u a.n4.test", 10, env->a.port);
	stub_rr(&env->s, "_aerospike._tcp.n4.test", T_SRV, false, "_aerospike._tcp.n4.test", T_SRV,
		target);
	stub_extra(&env->s, "_aerospike._tcp.n4.test", "a.n4.test", T_A, "127.0.0.1");
	return true;
}

static void
n4_stop(n4_env* env)
{
	stub_stop(&env->s);
	fnode_stop(&env->b);
	fnode_stop(&env->a);
}

static as_status
n4_register(n4_env* env, as_config* config, bool srv, as_error* err)
{
	const char* names[] = {srv ? "_aerospike._tcp.n4.test" : "seeds.n4.test"};
	dns_setup d;
	as_dns_seed_provider_config* cfg = setup_init(&d, names, 1, &env->s, NULL);
	cfg->record_type = srv ? AS_DNS_RECORD_SRV : AS_DNS_RECORD_A;
	cfg->port = env->a.port;
	cfg->timeout_ms = AS_TEND_INTERVAL_MIN / 2;
	return as_config_set_dns_seed_provider(config, cfg, err);
}

// Bootstrap one client from DNS and check the resulting cluster. Returns NULL on success or a
// description of the first failed check.
static const char*
n4_bootstrap(n4_env* env, bool srv, as_discovery_execution_mode mode, char* msg, size_t size)
{
	as_config config;
	as_error err;
	client_config(&config, mode);

	if (n4_register(env, &config, srv, &err) != AEROSPIKE_OK) {
		snprintf(msg, size, "register failed: %.400s", err.message);
		as_config_destroy(&config);
		return msg;
	}

	stub_reset_queries(&env->s);
	uint32_t tends = fnode_get(&env->a, &env->a.tends);
	aerospike* as = aerospike_new(&config);
	as_status status = aerospike_connect(as, &err);
	const char* failure = NULL;

	if (status != AEROSPIKE_OK) {
		snprintf(msg, size, "connect failed: %d %.400s", status, err.message);
		failure = msg;
	}
	// B was never returned by DNS: the server-sourced peers list added it.
	else if (!wait_for_nodes(as, 2, 10000) || !has_node(as, "A") || !has_node(as, "B")) {
		failure = "A and B did not both join";
	}
	else if (!fnode_wait(&env->a, &env->a.tends, tends + 1, 5000)) {
		failure = "A was not tended";
	}
	else if (stub_count(&env->s, srv ? "_aerospike._tcp.n4.test/SRV" : "seeds.n4.test/A") < 1) {
		failure = "DNS was not queried";
	}
	else if (!as->config.discovery.registration_locked) {
		failure = "registration is not locked while connected";
	}
	else {
		as_seed_snapshot* snapshot = as_seed_manager_reserve(as->cluster->seed_manager);
		const as_vector* hosts = as_seed_snapshot_hosts(snapshot);
		as_host* first = hosts->size ? as_vector_get((as_vector*)hosts, 0) : NULL;

		if (!first || strcmp(first->name, "127.0.0.1") != 0 || first->port != env->a.port ||
			!all_numeric(hosts)) {
			failure = "snapshot does not start with the DNS-published seed";
		}
		as_seed_snapshot_release(snapshot);
	}

	client_close(as);
	return failure;
}

TEST(dns_n4_bootstrap, "N-4 precursor: a DNS-published seed bootstraps the client in both modes; membership comes from peers")
{
	int fds = fd_count();
	n4_env env;
	assert_true(n4_start(&env));

	for (int srv = 0; srv < 2; srv++) {
		for (int mode = 0; mode < 2; mode++) {
			char msg[512];
			const char* failure = n4_bootstrap(&env, srv != 0, (as_discovery_execution_mode)mode,
				msg, sizeof(msg));

			if (failure) {
				uint32_t a_tends = fnode_get(&env.a, &env.a.tends);
				uint32_t a_requests = fnode_get(&env.a, &env.a.requests);
				uint32_t b_requests = fnode_get(&env.b, &env.b.requests);
				n4_stop(&env);
				fail("srv %d mode %d: %s (A tends %u requests %u, B requests %u)", srv, mode,
					failure, a_tends, a_requests, b_requests);
			}
		}
	}
	n4_stop(&env);
	assert_int_eq(settle(fd_count, fds), fds);
}

static const char*
n4_close_reconnect(n4_env* env)
{
	int before = discovery_thread_count();

	as_config config;
	as_error err;
	client_config(&config, AS_DISCOVERY_EXECUTION_THREAD);
	config.discovery.refresh_interval_ms = 600000;

	if (n4_register(env, &config, false, &err) != AEROSPIKE_OK) {
		as_config_destroy(&config);
		return "register failed";
	}

	aerospike* as = aerospike_new(&config);
	const char* failure = NULL;

	if (aerospike_connect(as, &err) != AEROSPIKE_OK || !has_node(as, "A")) {
		failure = "first connect failed";
	}
	else if (before >= 0 && settle(discovery_thread_count, before + 1) != before + 1) {
		failure = "connect did not start one discovery thread";
	}

	if (failure) {
		client_close(as);
		return failure;
	}

	uint32_t queries = stub_count(&env->s, "seeds.n4.test/A");
	client_sync_close(as);

	if (!as->config.discovery.provider_registered) {
		failure = "sync close released the DNS registration";
	}
	else if (before >= 0 && settle(discovery_thread_count, before) != before) {
		failure = "sync close left the discovery thread running";
	}
	else if (aerospike_connect(as, &err) != AEROSPIKE_OK || !has_node(as, "A")) {
		failure = "reconnect failed";
	}
	else if (stub_count(&env->s, "seeds.n4.test/A") <= queries) {
		failure = "reconnect did not resolve DNS again";
	}
	client_close(as);

	if (failure) {
		return failure;
	}

	if (before >= 0 && settle(discovery_thread_count, before) != before) {
		return "destroy left the discovery thread running";
	}

	// A tend-mode DNS timeout above the deadline fails connect before any query or cluster work.
	client_config(&config, AS_DISCOVERY_EXECUTION_TEND);
	const char* names[] = {"seeds.n4.test"};
	dns_setup d;
	as_dns_seed_provider_config* cfg = setup_init(&d, names, 1, &env->s, NULL);
	cfg->timeout_ms = AS_TEND_INTERVAL_MIN / 2 + 1;

	if (as_config_set_dns_seed_provider(&config, cfg, &err) != AEROSPIKE_OK) {
		as_config_destroy(&config);
		return "tend-mode register failed";
	}
	stub_reset_queries(&env->s);

	as = aerospike_new(&config);

	if (aerospike_connect(as, &err) != AEROSPIKE_ERR_PARAM ||
		!strstr(err.message, "DNS seed provider timeout_ms 126 exceeds")) {
		failure = "tend-mode timeout above the deadline was not rejected";
	}
	else if (as->cluster || as->config.discovery.registration_locked) {
		failure = "rejected connect left a cluster or a locked registration";
	}
	else if (stub_get(&env->s, &env->s.udp_queries) != 0) {
		failure = "rejected connect sent a DNS query";
	}
	client_close(as);
	return failure;
}

TEST(dns_n4_close_reconnect, "N-4 precursor: sync close keeps the DNS registration and reconnect resolves again; destroy releases it")
{
	int fds = fd_count();
	n4_env env;
	assert_true(n4_start(&env));
	const char* failure = n4_close_reconnect(&env);
	n4_stop(&env);

	if (failure) {
		fail("%s", failure);
	}
	assert_int_eq(settle(fd_count, fds), fds);
}

#else

TEST(dns_fake_servers_skipped, "wire, deadline and client DNS tests need POSIX loopback servers")
{
	info("skipped: the stub resolver and fake node tests need POSIX sockets");
}

#endif // DISCOVERY_FAKE_SERVERS

//---------------------------------
// Test Suite
//---------------------------------

SUITE(discovery_dns, "pluggable discovery bounded DNS seed provider tests")
{
	suite_add(dns_config_defaults);
	suite_add(dns_config_invalid);
	suite_add(dns_config_servers);
	suite_add(dns_config_system_resolvers);
	suite_add(dns_config_registration);
#ifdef DISCOVERY_FAKE_SERVERS
	suite_add(dns_a_aaaa);
	suite_add(dns_a_names_in_order);
	suite_add(dns_tls_name);
	suite_add(dns_cname);
	suite_add(dns_srv_additional);
	suite_add(dns_srv_follow_up);
	suite_add(dns_srv_targets);
	suite_add(dns_partial);
	suite_add(dns_all_failed);
	suite_add(dns_failover);
	suite_add(dns_tcp_fallback);
	suite_add(dns_spoofed_datagrams);
	suite_add(dns_malformed);
	suite_add(dns_large_answers);
	suite_add(dns_deadline_unanswered);
	suite_add(dns_deadline_shares);
	suite_add(dns_resolver_rotation);
	suite_add(dns_deadline_srv_targets);
	suite_add(dns_deadline_tcp);
	suite_add(dns_no_thread);
	suite_add(dns_no_leaked_descriptors);
	suite_add(dns_tend_validation);
	suite_add(dns_tend_silent_resolver);
	suite_add(dns_snapshot_retention);
	suite_add(dns_thread_mode);
	suite_add(dns_init_failure);
	suite_add(dns_n4_bootstrap);
	suite_add(dns_n4_close_reconnect);
#else
	suite_add(dns_fake_servers_skipped);
#endif
}
