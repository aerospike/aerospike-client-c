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
#include <aerospike/as_aws_internal.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_config.h>
#include <aerospike/as_discovery.h>
#include <aerospike/as_event.h>
#include <aerospike/as_log.h>
#include <aerospike/as_sleep.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_clock.h>
#include <pthread.h>

#include "test.h"
#include "aerospike_test.h"
#include "discovery_aws_stub.h"

#if !defined(_WIN32)
#include <dirent.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

// Coverage: the aws-tags seed provider of D-14 (SigV4 against the AWS test vectors, no new
// thread after construction and refreshes, a refresh bounded against an unanswered endpoint),
// M-10 (tend rejected before any request), and configuration, ownership, HTTP framing, HTTPS
// trust and host checks, XML hardening, request shape, pagination, error text and redaction,
// snapshot rules and client lifecycle. Every wire test
// uses a loopback HTTP or HTTPS stub with certificates generated at run time; none reaches AWS.

//---------------------------------
// Static Functions: shared helpers
//---------------------------------

#define AWS_MARGIN_MS 250
#define AWS_MAX_RESPONSE (16 * 1024 * 1024)
#define AWS_KEY "AKIDEXAMPLE"
#define AWS_SECRET "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
#define AWS_TOKEN "FwoGZXIvYXdzEXAMPLE/token+="
#define AWS_FORM "application/x-www-form-urlencoded; charset=utf-8"
#define AWS_FAILED "AWS EC2 seed provider: DescribeInstances failed: "
#define AWS_EMPTY_HASH "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
#define AWS_403_CODE "UnauthorizedOperation"
#define AWS_403_MESSAGE "You are not authorized to perform this operation."
#define AWS_403_ID "7a62c49f-347e-4fc4-9331-6e8eEXAMPLE"
#define AWS_403_TEXT AWS_FAILED "HTTP 403 " AWS_403_CODE ": " AWS_403_MESSAGE " (request id " \
	AWS_403_ID ")"

#define fail(fmt, ...) \
	return atf_assert_log(__result__, "", __FILE__, __LINE__, fmt, ##__VA_ARGS__)

static const char* const prod_a[] = {"prod-a"};
static const as_aws_filter tag_filter[] = {{"tag:aerospike-cluster", prod_a, 1}};

static void
aws_cfg(as_aws_seed_provider_config* cfg, const char* endpoint)
{
	as_aws_seed_provider_config_init(cfg);
	cfg->region = "us-east-1";
	cfg->filters = tag_filter;
	cfg->filters_size = 1;
	cfg->endpoint = endpoint;
	cfg->access_key_id = AWS_KEY;
	cfg->secret_access_key = AWS_SECRET;
}

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

static char*
sha256(const char* text, char* out)
{
	as_aws_sha256_hex(text, strlen(text), out);
	return out;
}

//---------------------------------
// Static Functions: fake environment and hook
//---------------------------------

#define FAKE_VARS 16

// A fake credential-chain environment. Every base URL points at a stub or at a loopback port with
// no listener, so no test reaches a real AWS or metadata address.
typedef struct fake_env_s {
	const char* names[FAKE_VARS];
	const char* values[FAKE_VARS];
	uint32_t size;
	uint32_t calls;

	// Fixed wall clock in ms; 0 reads the real clock.
	uint64_t now_ms;

	char sts[64];
	char ecs[64];
	char imds[64];
	as_aws_env env;
} fake_env;

static const char*
fake_getenv(const char* name, void* udata)
{
	fake_env* f = udata;
	f->calls++;

	for (uint32_t i = 0; i < f->size; i++) {
		if (strcmp(f->names[i], name) == 0) {
			return f->values[i];
		}
	}
	return NULL;
}

static uint64_t
fake_now(void* udata)
{
	fake_env* f = udata;
	return f->now_ms ? f->now_ms : cf_clock_getabsolute();
}

static void
fake_set(fake_env* f, const char* name, const char* value)
{
	for (uint32_t i = 0; i < f->size; i++) {
		if (strcmp(f->names[i], name) == 0) {
			f->values[i] = value;
			return;
		}
	}

	if (f->size < FAKE_VARS) {
		f->names[f->size] = name;
		f->values[f->size++] = value;
	}
}

// Ports 0 mean no listener. IMDS is disabled unless imds_port is set.
static void
fake_init(fake_env* f, uint16_t sts_port, uint16_t ecs_port, uint16_t imds_port)
{
	memset(f, 0, sizeof(fake_env));
	snprintf(f->sts, sizeof(f->sts), "http://127.0.0.1:%u/sts", sts_port ? sts_port : 1);
	snprintf(f->ecs, sizeof(f->ecs), "http://127.0.0.1:%u/ecs", ecs_port ? ecs_port : 1);
	snprintf(f->imds, sizeof(f->imds), "http://127.0.0.1:%u", imds_port ? imds_port : 1);

	if (!imds_port) {
		fake_set(f, "AWS_EC2_METADATA_DISABLED", "true");
	}
	f->env.getenv = fake_getenv;
	f->env.sts_base = f->sts;
	f->env.ecs_base = f->ecs;
	f->env.imds_base = f->imds;
	f->env.now_ms = fake_now;
	f->env.udata = f;
}

static as_status
hook_never(as_error* err, uint32_t timeout_ms, as_aws_credentials* credentials, void* udata)
{
	(void)timeout_ms;
	(void)credentials;
	(void)udata;
	return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "never called");
}

static void
hook_noop(void* udata)
{
	(void)udata;
}

// Behavior and record of one test hook. Counters are atomic because get and destroy may run on
// the discovery thread.
typedef struct hook_state_s {
	const char* key;
	const char* secret;
	const char* token;
	uint64_t expiration_ms;

	// Expiration from the second call on, when not 0.
	uint64_t later_expiration_ms;

	as_status status;
	const char* message;
	uint32_t sleep_ms;

	// Sleep only from this call on (1-based), when not 0.
	uint32_t sleep_from;

	uint32_t gets;
	uint32_t destroys;
	uint32_t last_timeout_ms;
	uint8_t get_returned;
	uint8_t destroyed_after_get;
	pthread_t get_thread;
	pthread_t destroy_thread;
} hook_state;

static as_status
hook_get(as_error* err, uint32_t timeout_ms, as_aws_credentials* credentials, void* udata)
{
	hook_state* h = udata;
	uint32_t call = as_aaf_uint32(&h->gets, 1);

	h->last_timeout_ms = timeout_ms;
	h->get_thread = pthread_self();
	as_store_uint8(&h->get_returned, 0);

	if (h->key) {
		uint64_t expiration = call > 1 && h->later_expiration_ms ? h->later_expiration_ms :
			h->expiration_ms;
		as_aws_credentials_set(credentials, h->key, h->secret, h->token, expiration);
	}

	if (h->sleep_ms && (h->sleep_from == 0 || call >= h->sleep_from)) {
		as_sleep(h->sleep_ms);
	}

	as_status status = h->status;

	if (status != AEROSPIKE_OK && h->message) {
		as_error_set_message(err, status, h->message);
	}
	as_store_uint8_rls(&h->get_returned, 1);
	return status;
}

static void
hook_destroy(void* udata)
{
	hook_state* h = udata;
	h->destroy_thread = pthread_self();
	h->destroyed_after_get = as_load_uint8_acq(&h->get_returned);
	as_aaf_uint32(&h->destroys, 1);
}

static void
hook_init(hook_state* h, as_aws_seed_provider_config* cfg)
{
	memset(h, 0, sizeof(hook_state));
	h->key = "AKIDHOOK";
	h->secret = "hook/secret+key";
	h->token = "hook-session-token";
	cfg->access_key_id = NULL;
	cfg->secret_access_key = NULL;
	cfg->session_token = NULL;
	cfg->credentials_hook.get = hook_get;
	cfg->credentials_hook.destroy = hook_destroy;
	cfg->credentials_hook.udata = h;
}

static bool
wait_count(uint32_t* counter, uint32_t target, uint32_t timeout_ms)
{
	uint64_t end = cf_getms() + timeout_ms;

	while (as_load_uint32_acq(counter) < target) {
		if (cf_getms() >= end) {
			return false;
		}
		as_sleep(5);
	}
	return true;
}

//---------------------------------
// Static Functions: log capture
//---------------------------------

#define LOG_MAX 32

static pthread_mutex_t log_lock = PTHREAD_MUTEX_INITIALIZER;
static char log_lines[LOG_MAX][1100];
static as_log_level log_levels[LOG_MAX];
static uint32_t log_count;
static as_log_callback log_prev;
static as_log_level log_prev_level;

static bool
log_capture(as_log_level level, const char* func, const char* file, uint32_t line,
	const char* fmt, ...)
{
	(void)func;
	(void)file;
	(void)line;

	char msg[1100];
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(msg, sizeof(msg), fmt, ap);
	va_end(ap);

	if (strncmp(msg, "AWS EC2 seed", 12) != 0 && strncmp(msg, "Seed provider", 13) != 0) {
		return true;
	}

	pthread_mutex_lock(&log_lock);

	if (log_count < LOG_MAX) {
		log_levels[log_count] = level;
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
	log_prev_level = g_as_log.level;
	as_log_set_level(AS_LOG_LEVEL_DEBUG);
	as_log_set_callback(log_capture);
}

static void
log_stop(void)
{
	as_log_set_callback(log_prev);
	as_log_set_level(log_prev_level);
}

static uint32_t
log_size(void)
{
	pthread_mutex_lock(&log_lock);
	uint32_t count = log_count;
	pthread_mutex_unlock(&log_lock);
	return count;
}

// Index of the first captured line equal to text at level, or -1.
static int
log_find(as_log_level level, const char* text)
{
	int found = -1;
	pthread_mutex_lock(&log_lock);

	for (uint32_t i = 0; i < log_count && found < 0; i++) {
		if (log_levels[i] == level && strcmp(log_lines[i], text) == 0) {
			found = (int)i;
		}
	}
	pthread_mutex_unlock(&log_lock);
	return found;
}

static bool
log_contains(const char* text)
{
	bool found = false;
	pthread_mutex_lock(&log_lock);

	for (uint32_t i = 0; i < log_count && !found; i++) {
		found = strstr(log_lines[i], text) != NULL;
	}
	pthread_mutex_unlock(&log_lock);
	return found;
}

//---------------------------------
// Tests: configuration and ownership
//---------------------------------

TEST(aws_config_defaults, "defaults are private IP, port 3000, no TLS name, 5000 ms, the regional endpoint and the OpenSSL default trust")
{
	as_aws_seed_provider_config cfg;
	memset(&cfg, 0xa5, sizeof(cfg));
	as_aws_seed_provider_config_init(&cfg);
	assert_null(cfg.region);
	assert_null(cfg.filters);
	assert_int_eq(cfg.filters_size, 0);
	assert_int_eq(cfg.address_field, AS_AWS_ADDRESS_PRIVATE_IP);
	assert_int_eq(cfg.port, 3000);
	assert_int_eq(cfg.port, AS_AWS_DEFAULT_PORT);
	assert_null(cfg.tls_name);
	assert_int_eq(cfg.timeout_ms, 5000);
	assert_int_eq(cfg.timeout_ms, AS_AWS_DEFAULT_TIMEOUT_MS);
	assert_null(cfg.endpoint);
	assert_null(cfg.ca_file);
	assert_null(cfg.ca_path);
	assert_null(cfg.access_key_id);
	assert_null(cfg.secret_access_key);
	assert_null(cfg.session_token);
	assert_null(cfg.credentials_hook.get);
	assert_null(cfg.credentials_hook.destroy);
	assert_null(cfg.credentials_hook.udata);

	char* endpoint = as_aws_ec2_endpoint("us-east-1");
	assert_string_eq(endpoint, "https://ec2.us-east-1.amazonaws.com");
	cf_free(endpoint);
	endpoint = as_aws_ec2_endpoint("cn-north-1");
	assert_string_eq(endpoint, "https://ec2.cn-north-1.amazonaws.com.cn");
	cf_free(endpoint);

	aws_cfg(&cfg, NULL);
	as_error err;
	as_seed_provider provider;
	assert_int_eq(as_discovery_aws_seed_provider(&err, &cfg, &provider), AEROSPIKE_OK);
	assert_int_eq(provider.size, sizeof(as_seed_provider));
	assert_int_eq(provider.api_version, AS_DISCOVERY_API_VERSION);
	assert_string_eq(provider.name, "aws-tags");
	assert_not_null(provider.refresh);
	assert_not_null(provider.destroy);
	assert_not_null(provider.udata);
	assert_true(provider.needs_periodic_refresh(provider.udata));
	assert_not_null(provider.supports_deadline);
	assert_false(provider.supports_deadline(provider.udata));

	// The seam with a NULL environment builds the same provider as the public factory.
	as_seed_provider seam;
	assert_int_eq(as_discovery_aws_seed_provider_env(&err, &cfg, NULL, &seam), AEROSPIKE_OK);
	assert_int_eq(seam.size, provider.size);
	assert_int_eq(seam.api_version, provider.api_version);
	assert_string_eq(seam.name, provider.name);
	assert_true(seam.refresh == provider.refresh);
	assert_true(seam.destroy == provider.destroy);
	assert_true(seam.needs_periodic_refresh == provider.needs_periodic_refresh);
	assert_true(seam.supports_deadline == provider.supports_deadline);
	seam.destroy(seam.udata);
	provider.destroy(provider.udata);

	cfg.region = "cn-north-1";
	assert_int_eq(as_discovery_aws_seed_provider(&err, &cfg, &provider), AEROSPIKE_OK);
	provider.destroy(provider.udata);

	assert_int_eq(as_discovery_aws_seed_provider(&err, NULL, &provider), AEROSPIKE_ERR_PARAM);
	assert_string_eq(err.message, "Invalid AWS EC2 seed provider");
	assert_int_eq(as_discovery_aws_seed_provider(&err, &cfg, NULL), AEROSPIKE_ERR_PARAM);
}

static const char* const no_values[] = {NULL};
static const char* const empty_values[] = {""};
static const char* const two_values[] = {"a", "b"};

static const char* const bad_endpoints[] = {
	"ftp://host", "http://", "http://:80", "http://user@host", "http://user:pw@host/",
	"http://host?x=1", "http://host?", "http://host#f", "http://host/#f", "http://::1",
	"http://host:0", "http://host:65536", "http://host:", "http://host:8a", "https://[::1",
	"https://[zz]", "https://[::1]x", "host", "http//host", "http://ho st", "http://host/a b",
	"http://ho_st"
};

// Apply invalid case i to cfg and return its message, or NULL past the last case.
static const char*
invalid_case(uint32_t i, as_aws_seed_provider_config* cfg, as_aws_filter* filters, char* msg,
	size_t size)
{
	static const char* const regions[] = {
		NULL, "", "useast1", "us-east-", "US-EAST-1", "-us-east", "us--east-1", "us_east-1"
	};
	uint32_t n = sizeof(regions) / sizeof(regions[0]);

	if (i < n) {
		cfg->region = regions[i];
		snprintf(msg, size, "Invalid AWS EC2 seed provider region: '%s'",
			regions[i] ? regions[i] : "(null)");
		return msg;
	}
	i -= n;

	filters[0] = (as_aws_filter){"tag:a", two_values, 2};
	filters[1] = (as_aws_filter){"tag:b", two_values, 2};
	cfg->filters = filters;
	cfg->filters_size = 2;

	switch (i) {
	case 0:
		cfg->filters = NULL;
		return "Invalid AWS EC2 seed provider filters: at least one filter is required";
	case 1:
		cfg->filters_size = 0;
		return "Invalid AWS EC2 seed provider filters: at least one filter is required";
	case 2:
		filters[1].name = NULL;
		return "Invalid AWS EC2 seed provider filter name: '(null)'";
	case 3:
		filters[1].name = "";
		return "Invalid AWS EC2 seed provider filter name: ''";
	case 4:
		filters[1].name = "instance-state-name";
		return "Invalid AWS EC2 seed provider filter name: 'instance-state-name': the provider "
			"adds instance-state-name=running";
	case 5:
		filters[1].name = "tag:a";
		return "Invalid AWS EC2 seed provider filter name: 'tag:a': duplicate";
	case 6:
		filters[1].values = NULL;
		return "Invalid AWS EC2 seed provider filter 'tag:b' values: empty";
	case 7:
		filters[1].values_size = 0;
		return "Invalid AWS EC2 seed provider filter 'tag:b' values: empty";
	case 8:
		filters[1].values = no_values;
		filters[1].values_size = 1;
		return "Invalid AWS EC2 seed provider filter 'tag:b' value: '(null)'";
	case 9:
		filters[1].values = empty_values;
		filters[1].values_size = 1;
		return "Invalid AWS EC2 seed provider filter 'tag:b' value: ''";
	case 10:
		cfg->address_field = (as_aws_address_field)4;
		return "Invalid AWS EC2 seed provider address_field: 4";
	case 11:
		cfg->address_field = (as_aws_address_field)-1;
		return "Invalid AWS EC2 seed provider address_field: -1";
	case 12:
		cfg->port = 0;
		return "Invalid AWS EC2 seed provider port: 0";
	case 13:
		cfg->tls_name = "";
		return "Invalid AWS EC2 seed provider tls_name: empty";
	case 14:
		cfg->timeout_ms = 0;
		return "Invalid AWS EC2 seed provider timeout_ms: 0";
	case 15:
		cfg->ca_file = "";
		return "Invalid AWS EC2 seed provider ca_file: empty";
	case 16:
		cfg->ca_path = "";
		return "Invalid AWS EC2 seed provider ca_path: empty";
	case 17:
		cfg->access_key_id = NULL;
		return "Invalid AWS EC2 seed provider credentials: access_key_id is empty";
	case 18:
		cfg->access_key_id = "";
		return "Invalid AWS EC2 seed provider credentials: access_key_id is empty";
	case 19:
		cfg->secret_access_key = NULL;
		return "Invalid AWS EC2 seed provider credentials: secret_access_key is empty";
	case 20:
		cfg->secret_access_key = "";
		return "Invalid AWS EC2 seed provider credentials: secret_access_key is empty";
	case 21:
		cfg->access_key_id = "AKID-1";
		return "Invalid AWS EC2 seed provider access_key_id: expected letters and digits";
	case 22:
		cfg->access_key_id = "AKID/1";
		return "Invalid AWS EC2 seed provider access_key_id: expected letters and digits";
	case 23:
		cfg->secret_access_key = "a b";
		return "Invalid AWS EC2 seed provider secret_access_key: invalid character";
	case 24:
		cfg->secret_access_key = "a\x80";
		return "Invalid AWS EC2 seed provider secret_access_key: invalid character";
	case 25:
		cfg->session_token = "tok\r\nx-injected: 1";
		return "Invalid AWS EC2 seed provider session_token: invalid character";
	case 26:
		cfg->session_token = "tok\x7f";
		return "Invalid AWS EC2 seed provider session_token: invalid character";
	case 27:
		cfg->access_key_id = NULL;
		cfg->secret_access_key = NULL;
		cfg->session_token = "tok";
		return "Invalid AWS EC2 seed provider credentials: session_token requires access_key_id "
			"and secret_access_key";
	case 28:
		cfg->credentials_hook.get = hook_never;
		return "Invalid AWS EC2 seed provider credentials: static keys and credentials_hook "
			"cannot both be set";
	case 29:
		cfg->access_key_id = NULL;
		cfg->secret_access_key = NULL;
		cfg->session_token = "tok";
		cfg->credentials_hook.get = hook_never;
		return "Invalid AWS EC2 seed provider credentials: session_token requires access_key_id "
			"and secret_access_key";
	case 30:
		cfg->access_key_id = NULL;
		cfg->secret_access_key = NULL;
		cfg->credentials_hook.destroy = hook_noop;
		return "Invalid AWS EC2 seed provider credentials_hook: get is required";
	case 31:
		cfg->access_key_id = NULL;
		cfg->secret_access_key = NULL;
		cfg->credentials_hook.udata = cfg;
		return "Invalid AWS EC2 seed provider credentials_hook: get is required";
	case 32:
		// The table rows come before the character checks.
		cfg->access_key_id = "AKID-1";
		cfg->credentials_hook.get = hook_never;
		return "Invalid AWS EC2 seed provider credentials: static keys and credentials_hook "
			"cannot both be set";
	case 33:
		cfg->access_key_id = NULL;
		cfg->secret_access_key = "a b";
		return "Invalid AWS EC2 seed provider credentials: access_key_id is empty";
	case 34:
		cfg->session_token = "tok";
		cfg->credentials_hook.destroy = hook_noop;
		return "Invalid AWS EC2 seed provider credentials: static keys and credentials_hook "
			"cannot both be set";
	default:
		break;
	}
	i -= 35;

	if (i < sizeof(bad_endpoints) / sizeof(bad_endpoints[0])) {
		cfg->endpoint = bad_endpoints[i];
		snprintf(msg, size, "Invalid AWS EC2 seed provider endpoint: '%s': expected http or https "
			"URL with a host and no query", bad_endpoints[i]);
		return msg;
	}
	return NULL;
}

TEST(aws_config_invalid, "invalid values are rejected at creation with a message naming them and leave the provider untouched")
{
	uint32_t cases = 0;

	for (uint32_t i = 0; ; i++) {
		as_aws_seed_provider_config cfg;
		as_aws_filter filters[2];
		char msg[300];
		aws_cfg(&cfg, NULL);
		const char* expected = invalid_case(i, &cfg, filters, msg, sizeof(msg));

		if (!expected) {
			break;
		}

		as_seed_provider provider;
		memset(&provider, 0xa5, sizeof(provider));
		as_seed_provider untouched;
		memset(&untouched, 0xa5, sizeof(untouched));
		as_error err;

		if (as_discovery_aws_seed_provider(&err, &cfg, &provider) != AEROSPIKE_ERR_PARAM) {
			fail("case %u (%s) was accepted", i, expected);
		}

		if (strcmp(err.message, expected) != 0) {
			fail("case %u: got \"%s\", expected \"%s\"", i, err.message, expected);
		}

		if (memcmp(&provider, &untouched, sizeof(provider)) != 0) {
			fail("case %u changed the provider", i);
		}
		cases++;
	}
	assert_true(cases > 50);

	// Valid endpoint forms.
	const char* good[] = {
		"http://127.0.0.1:8080", "https://[::1]:8443/x", "HTTPS://ec2.example.com",
		"http://host/", "https://ec2.us-east-1.amazonaws.com:443/", "http://localhost:1/a/b",
		"Http://vpce-0123.ec2.us-east-1.vpce.amazonaws.com", "http://[2001:db8::1]"
	};

	for (uint32_t i = 0; i < sizeof(good) / sizeof(good[0]); i++) {
		as_aws_seed_provider_config cfg;
		aws_cfg(&cfg, good[i]);
		as_seed_provider provider;
		as_error err;

		if (as_discovery_aws_seed_provider(&err, &cfg, &provider) != AEROSPIKE_OK) {
			fail("endpoint %s rejected: %s", good[i], err.message);
		}
		provider.destroy(provider.udata);
	}

	// Every address field and an empty session token are accepted.
	for (int field = 0; field < 4; field++) {
		as_aws_seed_provider_config cfg;
		aws_cfg(&cfg, NULL);
		cfg.address_field = (as_aws_address_field)field;
		cfg.session_token = "";
		as_seed_provider provider;
		as_error err;
		assert_int_eq(as_discovery_aws_seed_provider(&err, &cfg, &provider), AEROSPIKE_OK);
		provider.destroy(provider.udata);
	}

	// Static keys are optional: no keys (the chain), empty keys, or a hook alone.
	as_aws_seed_provider_config cfg;
	as_seed_provider provider;
	as_error err;
	fake_env f;
	fake_init(&f, 0, 0, 0);
	aws_cfg(&cfg, NULL);
	cfg.access_key_id = NULL;
	cfg.secret_access_key = NULL;
	assert_int_eq(as_discovery_aws_seed_provider_env(&err, &cfg, &f.env, &provider), AEROSPIKE_OK);
	provider.destroy(provider.udata);
	assert_true(f.calls > 0);

	cfg.access_key_id = "";
	cfg.secret_access_key = "";
	cfg.session_token = "";
	assert_int_eq(as_discovery_aws_seed_provider_env(&err, &cfg, &f.env, &provider), AEROSPIKE_OK);
	provider.destroy(provider.udata);

	f.calls = 0;
	cfg.credentials_hook.get = hook_never;
	assert_int_eq(as_discovery_aws_seed_provider_env(&err, &cfg, &f.env, &provider), AEROSPIKE_OK);
	provider.destroy(provider.udata);
	assert_int_eq(f.calls, 0);
}

TEST(aws_config_trust_files, "an unreadable ca_file or ca_path fails creation with AEROSPIKE_ERR_TLS_ERROR")
{
	as_aws_seed_provider_config cfg;
	as_seed_provider provider;
	as_error err;
	aws_cfg(&cfg, NULL);

	cfg.ca_file = "/nonexistent/as-aws-ca.pem";
	assert_int_eq(as_discovery_aws_seed_provider(&err, &cfg, &provider), AEROSPIKE_ERR_TLS_ERROR);
	assert_int_eq(strncmp(err.message, "AWS EC2 seed provider: failed to load ca_file "
		"/nonexistent/as-aws-ca.pem: ", 72), 0);

	cfg.ca_file = NULL;
	cfg.ca_path = "/nonexistent/as-aws-ca";
	assert_int_eq(as_discovery_aws_seed_provider(&err, &cfg, &provider), AEROSPIKE_ERR_TLS_ERROR);
	assert_string_eq(err.message, "AWS EC2 seed provider: failed to load ca_path "
		"/nonexistent/as-aws-ca: No such file or directory");

#if !defined(_WIN32)
	cfg.ca_path = "/dev/null";
	assert_int_eq(as_discovery_aws_seed_provider(&err, &cfg, &provider), AEROSPIKE_ERR_TLS_ERROR);
	assert_string_eq(err.message, "AWS EC2 seed provider: failed to load ca_path /dev/null: "
		"not a directory");
#endif
}

TEST(aws_config_registration, "the config helper copies its input, replaces a previous provider and leaves it on every failure")
{
	as_config config;
	as_error err;
	as_aws_seed_provider_config cfg;
	as_config_init(&config);
	aws_cfg(&cfg, NULL);

	assert_int_eq(as_config_set_aws_seed_provider(NULL, &cfg, &err), AEROSPIKE_ERR_PARAM);
	assert_string_eq(err.message, "Invalid seed provider");
	assert_int_eq(as_config_set_aws_seed_provider(&config, NULL, &err), AEROSPIKE_ERR_PARAM);
	assert_string_eq(err.message, "Invalid AWS EC2 seed provider");
	assert_false(config.discovery.provider_registered);

	assert_int_eq(as_config_set_aws_seed_provider(&config, &cfg, &err), AEROSPIKE_OK);
	assert_true(config.discovery.provider_registered);
	assert_false(config.discovery.binding_provider);
	assert_string_eq(config.discovery.provider.name, "aws-tags");

	// Replacement destroys the previous state (Valgrind checks it is released exactly once).
	assert_int_eq(as_config_set_aws_seed_provider(&config, &cfg, &err), AEROSPIKE_OK);
	void* second = config.discovery.provider.udata;
	assert_not_null(second);

	// Failed replacements leave the registration unchanged.
	cfg.timeout_ms = 0;
	assert_int_eq(as_config_set_aws_seed_provider(&config, &cfg, &err), AEROSPIKE_ERR_PARAM);
	assert_true(config.discovery.provider.udata == second);
	cfg.timeout_ms = 100;

	cfg.ca_file = "/nonexistent/as-aws-ca.pem";
	assert_int_eq(as_config_set_aws_seed_provider(&config, &cfg, &err), AEROSPIKE_ERR_TLS_ERROR);
	assert_true(config.discovery.provider.udata == second);
	cfg.ca_file = NULL;

	config.discovery.registration_locked = true;
	assert_int_eq(as_config_set_aws_seed_provider(&config, &cfg, &err), AEROSPIKE_ERR_PARAM);
	assert_string_eq(err.message, "Seed provider cannot be replaced while the client is connected");
	assert_true(config.discovery.provider.udata == second);
	config.discovery.registration_locked = false;

	config.discovery.execution_mode = AS_DISCOVERY_EXECUTION_TEND;
	assert_int_eq(as_config_set_aws_seed_provider(&config, &cfg, &err), AEROSPIKE_ERR_PARAM);
	assert_string_eq(err.message, "AWS EC2 seed provider requires thread execution mode");
	assert_true(config.discovery.provider.udata == second);
	assert_string_eq(config.discovery.provider.name, "aws-tags");
	as_config_destroy(&config);
}

TEST(aws_hook_ownership, "the hook destroy runs once with the provider, on replacement and with the config, never after a failed construction or setter rejection; construction reads no environment")
{
	as_aws_seed_provider_config cfg;
	as_seed_provider provider;
	as_error err;
	hook_state a;
	hook_state b;
	hook_state c;

	aws_cfg(&cfg, NULL);
	hook_init(&a, &cfg);
	assert_int_eq(as_discovery_aws_seed_provider(&err, &cfg, &provider), AEROSPIKE_OK);
	assert_int_eq(a.destroys, 0);
	provider.destroy(provider.udata);
	assert_int_eq(a.destroys, 1);
	assert_int_eq(a.gets, 0);

	// Failed constructions, including an unreadable ca_file, the last failure path.
	hook_init(&a, &cfg);
	cfg.timeout_ms = 0;
	assert_int_eq(as_discovery_aws_seed_provider(&err, &cfg, &provider), AEROSPIKE_ERR_PARAM);
	cfg.timeout_ms = 1000;
	cfg.ca_file = "/nonexistent/as-aws-ca.pem";
	assert_int_eq(as_discovery_aws_seed_provider(&err, &cfg, &provider), AEROSPIKE_ERR_TLS_ERROR);
	cfg.ca_file = NULL;
	assert_int_eq(a.destroys, 0);

	// With a hook or static keys the environment is never read.
	fake_env f;
	fake_init(&f, 0, 0, 0);
	assert_int_eq(as_discovery_aws_seed_provider_env(&err, &cfg, &f.env, &provider), AEROSPIKE_OK);
	provider.destroy(provider.udata);
	assert_int_eq(a.destroys, 1);
	aws_cfg(&cfg, NULL);
	assert_int_eq(as_discovery_aws_seed_provider_env(&err, &cfg, &f.env, &provider), AEROSPIKE_OK);
	provider.destroy(provider.udata);
	assert_int_eq(f.calls, 0);

	// Each registration owns its own udata; replacement destroys the previous one.
	as_config config;
	as_config_init(&config);
	hook_init(&a, &cfg);
	assert_int_eq(as_config_set_aws_seed_provider(&config, &cfg, &err), AEROSPIKE_OK);
	hook_init(&b, &cfg);
	assert_int_eq(as_config_set_aws_seed_provider(&config, &cfg, &err), AEROSPIKE_OK);
	assert_int_eq(a.destroys, 1);
	assert_int_eq(b.destroys, 0);

	// Setter rejections leave the udata with the caller.
	hook_init(&c, &cfg);
	cfg.ca_file = "/nonexistent/as-aws-ca.pem";
	assert_int_eq(as_config_set_aws_seed_provider(&config, &cfg, &err), AEROSPIKE_ERR_TLS_ERROR);
	cfg.ca_file = NULL;
	cfg.timeout_ms = 0;
	assert_int_eq(as_config_set_aws_seed_provider(&config, &cfg, &err), AEROSPIKE_ERR_PARAM);
	cfg.timeout_ms = 1000;
	config.discovery.registration_locked = true;
	assert_int_eq(as_config_set_aws_seed_provider(&config, &cfg, &err), AEROSPIKE_ERR_PARAM);
	config.discovery.registration_locked = false;
	config.discovery.execution_mode = AS_DISCOVERY_EXECUTION_TEND;
	assert_int_eq(as_config_set_aws_seed_provider(&config, &cfg, &err), AEROSPIKE_ERR_PARAM);
	config.discovery.execution_mode = AS_DISCOVERY_EXECUTION_THREAD;
	assert_int_eq(c.destroys, 0);
	assert_int_eq(b.destroys, 0);

	as_config_destroy(&config);
	assert_int_eq(a.destroys, 1);
	assert_int_eq(b.destroys, 1);
	assert_int_eq(c.destroys, 0);
	assert_int_eq(a.gets + b.gets + c.gets, 0);
}

TEST(aws_credentials_set, "as_aws_credentials_set() validates and copies; invalid input keeps the earlier value; a second call replaces it")
{
	as_aws_credentials c;
	memset(&c, 0, sizeof(c));
	char key[16] = "AKID1";
	char secret[16] = "secret/1+x";
	char token[16] = "token-1";
	assert_true(as_aws_credentials_set(&c, key, secret, token, 1000));
	strcpy(key, "changed");
	strcpy(secret, "changed");
	strcpy(token, "changed");
	assert_string_eq(c.access_key_id, "AKID1");
	assert_string_eq(c.secret_access_key, "secret/1+x");
	assert_string_eq(c.session_token, "token-1");
	assert_int_eq(c.expiration_ms, 1000);

	static const struct {
		const char* key;
		const char* secret;
		const char* token;
	} invalid[] = {
		{NULL, "s", NULL}, {"", "s", NULL}, {"AKID-1", "s", NULL}, {"AKID\x80", "s", NULL},
		{"AKID", NULL, NULL}, {"AKID", "", NULL}, {"AKID", "a b", NULL}, {"AKID", "s\n", NULL},
		{"AKID", "s", "tok\r\nx: 1"}, {"AKID", "s", "tok\x7f"}, {"AKID", "s", "tok en"}
	};

	for (uint32_t i = 0; i < sizeof(invalid) / sizeof(invalid[0]); i++) {
		if (as_aws_credentials_set(&c, invalid[i].key, invalid[i].secret, invalid[i].token, 5)) {
			fail("case %u was accepted", i);
		}
		assert_string_eq(c.access_key_id, "AKID1");
		assert_string_eq(c.session_token, "token-1");
		assert_int_eq(c.expiration_ms, 1000);
	}
	assert_false(as_aws_credentials_set(NULL, "AKID", "s", NULL, 0));

	// An empty token is no token.
	assert_true(as_aws_credentials_set(&c, "AKID2", "secret2", "", 0));
	assert_string_eq(c.access_key_id, "AKID2");
	assert_string_eq(c.secret_access_key, "secret2");
	assert_null(c.session_token);
	assert_int_eq(c.expiration_ms, 0);
	cf_free(c.access_key_id);
	cf_free(c.secret_access_key);
	cf_free(c.session_token);
}

// The map as Java's AbstractMap.toString() prints it.
static const char*
json_str(const char* text, char* buf, size_t size)
{
	as_aws_json json;

	if (!as_aws_json_parse((const uint8_t*)text, strlen(text), &json)) {
		snprintf(buf, size, "rejected");
		return buf;
	}

	size_t len = (size_t)snprintf(buf, size, "{");

	for (uint32_t i = 0; i < json.size && len < size; i++) {
		len += (size_t)snprintf(buf + len, size - len, "%s%s=%s", i ? ", " : "",
			json.entries[i].key, json.entries[i].value);
	}

	if (len < size) {
		snprintf(buf + len, size - len, "}");
	}
	as_aws_json_destroy(&json);
	return buf;
}

TEST(aws_json_reader, "the JSON reader keeps flat strings, numbers and booleans as Java does, skips nesting to depth 64 and rejects bad escapes, surrogates, NUL and trailing data")
{
	char buf[256];
	assert_string_eq(json_str("{\"a\":\"x\\\"y\", \"n\": 12, \"o\": {\"k\": [1, {\"z\": \"}\"}]}, "
		"\"b\":true, \"nil\": null, \"s\": \"\\u00e9\\n\"}", buf, sizeof(buf)),
		"{a=x\"y, n=12, b=true, s=\xc3\xa9\n}");
	assert_string_eq(json_str(" { } ", buf, sizeof(buf)), "{}");
	assert_string_eq(json_str("{\"a\" 1}", buf, sizeof(buf)), "rejected");

	// A repeated key keeps the last value in the first position; null, objects and arrays are
	// dropped and leave the earlier value, as in Java.
	assert_string_eq(json_str("{\"a\":\"1\",\"b\":\"2\",\"a\":\"3\"}", buf, sizeof(buf)),
		"{a=3, b=2}");
	assert_string_eq(json_str("{\"a\":\"1\",\"a\":null,\"a\":{},\"a\":[]}", buf, sizeof(buf)),
		"{a=1}");
	assert_string_eq(json_str("{\"a\":\"1\"} \r\n\t", buf, sizeof(buf)), "{a=1}");
	assert_string_eq(json_str("{\"e\":\"\\\\\\/\\b\\f\\r\\t\",\"f\":false,\"n\":-1.5e+3}", buf,
		sizeof(buf)), "{e=\\/\b\f\r\t, f=false, n=-1.5e+3}");
	assert_string_eq(json_str("{\"p\":\"\\ud83d\\ude00\"}", buf, sizeof(buf)),
		"{p=\xf0\x9f\x98\x80}");

	static const char* const rejected[] = {
		"", "[1]", "\"a\"", "{\"a\":\"1\"}x", "{\"a\":\"1\"}{}", "{\"a\":\"\\u0000\"}",
		"{\"a\":\"\\ud800\"}", "{\"a\":\"\\udc00\"}", "{\"a\":\"\\ud800\\u0041\"}",
		"{\"a\":\"\\ud800x\"}", "{\"a\":\"\\x\"}", "{\"a\":\"\\u12\"}", "{\"a\":\"\\u12g4\"}",
		"{\"a\":\"1\"", "{\"a\":\"1", "{\"a\":", "{\"a\"", "{\"a", "{", "{\"a\":\"1\",}",
		"{,}", "{\"a\":01}", "{\"a\":1.}", "{\"a\":.5}", "{\"a\":1e}", "{\"a\":-}", "{\"a\":tru}",
		"{\"a\":nul}", "{\"a\":True}", "{\"a\":[1,2}", "{\"a\":{\"b\":\"}}", "{1:2}",
		"{\"a\":1 2}"
	};

	for (uint32_t i = 0; i < sizeof(rejected) / sizeof(rejected[0]); i++) {
		if (strcmp(json_str(rejected[i], buf, sizeof(buf)), "rejected") != 0) {
			fail("%s parsed as %s", rejected[i], buf);
		}
	}

	// A NUL byte anywhere, and no data.
	as_aws_json json;
	assert_false(as_aws_json_parse((const uint8_t*)"{\"a\":\"1\0\"}", 10, &json));
	assert_false(as_aws_json_parse((const uint8_t*)"{}\0", 3, &json));
	assert_false(as_aws_json_parse(NULL, 0, &json));
	assert_int_eq(json.size, 0);

	// Nesting up to depth 64 is skipped; 65 is rejected.
	for (uint32_t depth = 64; depth <= 65; depth++) {
		char open[66];
		char close[66];
		memset(open, '[', depth);
		open[depth] = 0;
		memset(close, ']', depth);
		close[depth] = 0;
		char* text = aws_strf("{\"a\":%s%s,\"b\":\"2\"}", open, close);
		assert_string_eq(json_str(text, buf, sizeof(buf)), depth == 64 ? "{b=2}" : "rejected");
		free(text);
	}
}

static const char*
ini_str(const char* text, const char* profile, char* buf, size_t size)
{
	as_aws_profile p;

	if (!as_aws_ini_profile(text, strlen(text), profile, &p)) {
		snprintf(buf, size, "failed");
		return buf;
	}
	snprintf(buf, size, "%s|%s|%s", p.access_key_id ? p.access_key_id : "-",
		p.secret_access_key ? p.secret_access_key : "-", p.session_token ? p.session_token : "-");
	as_aws_profile_destroy(&p);
	return buf;
}

#define JAVA_PROFILE "# shared credentials\n[default]\naws_access_key_id = AKIADEFAULT\n" \
	"aws_secret_access_key = SECRET\n\n[seed]\naws_access_key_id=AKIASEEDPROFILE\n" \
	"aws_secret_access_key=SECRET\naws_session_token=TOKEN\n\n[sso-only]\nsso_session = corp\n"

TEST(aws_ini_reader, "the INI reader selects a trimmed section, allows a comment after a header, ends the section at a malformed one, skips a BOM and comments, splits at the first '=', merges repeated sections and keeps the last repeated key")
{
	char buf[256];
	assert_string_eq(ini_str(JAVA_PROFILE, "default", buf, sizeof(buf)), "AKIADEFAULT|SECRET|-");
	assert_string_eq(ini_str(JAVA_PROFILE, "seed", buf, sizeof(buf)),
		"AKIASEEDPROFILE|SECRET|TOKEN");
	assert_string_eq(ini_str(JAVA_PROFILE, "sso-only", buf, sizeof(buf)), "-|-|-");
	assert_string_eq(ini_str(JAVA_PROFILE, "missing", buf, sizeof(buf)), "-|-|-");
	assert_string_eq(ini_str("", "default", buf, sizeof(buf)), "-|-|-");

	const char* text = "aws_access_key_id = OUTSIDE\n[a]\n; aws_access_key_id = SEMI\n"
		"  # aws_access_key_id = HASH\naws_access_key_id = K1\n[b]\naws_access_key_id = B\n"
		"[ a ]\r\n\taws_access_key_id = K2 # inline\r\naws_secret_access_key=S=1\n=novalue\n"
		"noequals\nregion = us-east-1\n";
	assert_string_eq(ini_str(text, "a", buf, sizeof(buf)), "K2 # inline|S=1|-");
	assert_string_eq(ini_str(text, "b", buf, sizeof(buf)), "B|-|-");

	// A lone CR ends a line, as Java's readAllLines() does.
	assert_string_eq(ini_str("[c]\raws_access_key_id=C\raws_session_token = TO\rKEN\r", "c", buf,
		sizeof(buf)), "C|-|TO");
	assert_string_eq(ini_str("[d]\naws_access_key_id=\n", "d", buf, sizeof(buf)), "|-|-");

	// A header may carry a trailing comment; any other '[' line ends the section and starts none,
	// so its keys never reach the previous profile.
	text = "[other]\naws_access_key_id=OTHER\n[default] # main account\naws_access_key_id=DEFAULT\n"
		"[semi] ; note\naws_access_key_id=SEMI\n[junk] text\naws_access_key_id=JUNK\n"
		"[unclosed\naws_access_key_id=UNCLOSED\n[a]]\naws_access_key_id=BRACKET\n";
	assert_string_eq(ini_str(text, "other", buf, sizeof(buf)), "OTHER|-|-");
	assert_string_eq(ini_str(text, "default", buf, sizeof(buf)), "DEFAULT|-|-");
	assert_string_eq(ini_str(text, "semi", buf, sizeof(buf)), "SEMI|-|-");
	assert_string_eq(ini_str(text, "junk", buf, sizeof(buf)), "-|-|-");
	assert_string_eq(ini_str(text, "a", buf, sizeof(buf)), "-|-|-");
	assert_string_eq(ini_str(text, "a]", buf, sizeof(buf)), "-|-|-");

	// A leading UTF-8 byte order mark is skipped.
	assert_string_eq(ini_str("\xEF\xBB\xBF[default]\naws_access_key_id=BOM\n", "default", buf,
		sizeof(buf)), "BOM|-|-");
}

TEST(aws_iso8601, "ISO-8601 accepts AWS forms with fractions and offsets and rejects other forms, out-of-range fields and times at or before the epoch")
{
	static const struct {
		const char* text;
		uint64_t ms;
	} good[] = {
		{"2015-08-30T12:36:00Z", 1440938160000ULL},
		{"2015-08-30T12:36:00.123Z", 1440938160123ULL},
		{"2015-08-30T12:36:00.5Z", 1440938160500ULL},
		{"2015-08-30T12:36:00.123456789Z", 1440938160123ULL},
		{"2015-08-30T12:36:00+00:00", 1440938160000ULL},
		{"2015-08-30T14:36:00+02:00", 1440938160000ULL},
		{"2015-08-30T10:06:00-02:30", 1440938160000ULL},
		{"2016-02-29T00:00:00Z", 1456704000000ULL},
		{"2000-02-29T23:59:59Z", 951868799000ULL},
		{"1970-01-01T00:00:00.001Z", 1},
		{"2100-12-31T23:59:59.999Z", 4133980799999ULL}
	};

	for (uint32_t i = 0; i < sizeof(good) / sizeof(good[0]); i++) {
		uint64_t ms = 0;

		if (!as_aws_iso8601(good[i].text, &ms) || ms != good[i].ms) {
			fail("%s: got %" PRIu64 ", expected %" PRIu64, good[i].text, ms, good[i].ms);
		}
	}

	static const char* const bad[] = {
		"", "2015-08-30T12:36:00", "2015-08-30T12:36:00.1234567890Z", "2015-08-30t12:36:00Z",
		"2015-08-30T12:36:00z", "2015-08-30T12:36:60Z", "2015-08-30T24:00:00Z",
		"2015-08-30T12:60:00Z", "2015-13-01T00:00:00Z", "2015-00-01T00:00:00Z",
		"2015-01-32T00:00:00Z", "2015-01-00T00:00:00Z", "2015-02-29T00:00:00Z",
		"1900-02-29T00:00:00Z", "1970-01-01T00:00:00Z", "1969-12-31T23:59:59Z",
		"2015-08-30T12:36:00.Z", "2015-08-30T12:36:00+02:00:00", "2015-08-30T12:36:00+19:00",
		"2015-08-30T12:36:00+02:60", "2015-08-30T12:36:00+0200", "2015-08-30T12:36:00Zx",
		"2015-8-30T12:36:00Z", "2015-08-30 12:36:00Z", "2015-08-30T12:36Z", "+2015-08-30T12:36:00Z",
		"2015-08-30T12:36:00 Z", "1970-01-01T01:00:00+01:00"
	};

	for (uint32_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
		uint64_t ms = 0;

		if (as_aws_iso8601(bad[i], &ms)) {
			fail("%s accepted as %" PRIu64, bad[i], ms);
		}
	}
}

//---------------------------------
// Tests: SigV4
//---------------------------------

TEST(aws_sigv4_vectors, "D-14 SigV4 matches the AWS signing key, IAM ListUsers and get-vanilla vectors; encoding and amz-date match Java")
{
	char hex[AS_AWS_SHA256_HEX_SIZE];
	uint8_t key[32];

	assert_string_eq(sha256("", hex), AWS_EMPTY_HASH);

	assert_true(as_aws_sigv4_signing_key(AWS_SECRET, "20120215", "us-east-1", "iam", key));
	as_aws_hex(key, sizeof(key), hex);
	assert_string_eq(hex, "f4780e2d9f65fa895f9c67b32ce1baf0b0d8a43505a000a1a9e090d414db404d");

	// IAM ListUsers, AWS General Reference "Signature Version 4 signing process".
	const char* secret = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
	as_aws_header iam[] = {
		{"content-type", "application/x-www-form-urlencoded; charset=utf-8"},
		{"host", "iam.amazonaws.com"},
		{"x-amz-date", "20150830T123600Z"}
	};
	char* canonical = as_aws_sigv4_canonical_request("GET", "/",
		"Action=ListUsers&Version=2010-05-08", iam, 3, AWS_EMPTY_HASH);
	assert_string_eq(canonical, "GET\n/\nAction=ListUsers&Version=2010-05-08\n"
		"content-type:application/x-www-form-urlencoded; charset=utf-8\nhost:iam.amazonaws.com\n"
		"x-amz-date:20150830T123600Z\n\ncontent-type;host;x-amz-date\n" AWS_EMPTY_HASH);
	assert_string_eq(sha256(canonical, hex),
		"f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59");

	char* sts = as_aws_sigv4_string_to_sign("20150830T123600Z", "20150830/us-east-1/iam/aws4_request",
		canonical);
	assert_string_eq(sts, "AWS4-HMAC-SHA256\n20150830T123600Z\n20150830/us-east-1/iam/aws4_request\n"
		"f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59");
	cf_free(sts);
	cf_free(canonical);

	assert_true(as_aws_sigv4_signing_key(secret, "20150830", "us-east-1", "iam", key));
	as_aws_hex(key, sizeof(key), hex);
	assert_string_eq(hex, "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9");

	char* auth = as_aws_sigv4_authorization("AKIDEXAMPLE", secret, "GET", "/",
		"Action=ListUsers&Version=2010-05-08", iam, 3, NULL, 0, "us-east-1", "iam",
		"20150830T123600Z");
	assert_string_eq(auth, "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/"
		"aws4_request, SignedHeaders=content-type;host;x-amz-date, "
		"Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7");
	cf_free(auth);

	// get-vanilla from the SigV4 test suite, headers given out of order.
	as_aws_header vanilla[] = {
		{"x-amz-date", "20150830T123600Z"},
		{"host", "example.amazonaws.com"}
	};
	canonical = as_aws_sigv4_canonical_request("GET", "/", "", vanilla, 2, AWS_EMPTY_HASH);
	assert_string_eq(sha256(canonical, hex),
		"bb579772317eb040ac9ed261061d46c1f17a8133879d6129b6e1c25292927e63");
	cf_free(canonical);

	auth = as_aws_sigv4_authorization("AKIDEXAMPLE", secret, "GET", "/", "", vanilla, 2,
		(const uint8_t*)"", 0, "us-east-1", "service", "20150830T123600Z");
	assert_string_eq(auth, "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/"
		"aws4_request, SignedHeaders=host;x-amz-date, "
		"Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31");
	cf_free(auth);

	// Header values are trimmed without collapsing inner spaces.
	as_aws_header spaced[] = {{"x-test", "  a  b \t"}, {"host", "h"}};
	canonical = as_aws_sigv4_canonical_request("POST", "/p", "", spaced, 2, AWS_EMPTY_HASH);
	assert_string_eq(canonical, "POST\n/p\n\nhost:h\nx-test:a  b\n\nhost;x-test\n" AWS_EMPTY_HASH);
	cf_free(canonical);

	// The EC2 signed-header sets.
	as_aws_header ec2[] = {
		{"content-type", AWS_FORM},
		{"host", "ec2.us-east-1.amazonaws.com"},
		{"x-amz-date", "20150830T123600Z"},
		{"x-amz-security-token", AWS_TOKEN}
	};
	auth = as_aws_sigv4_authorization(AWS_KEY, secret, "POST", "/", "", ec2, 3,
		(const uint8_t*)"Action=DescribeInstances", 24, "us-east-1", "ec2", "20150830T123600Z");
	assert_not_null(strstr(auth, "Credential=AKIDEXAMPLE/20150830/us-east-1/ec2/aws4_request, "
		"SignedHeaders=content-type;host;x-amz-date, Signature="));
	assert_int_eq(strlen(strstr(auth, "Signature=") + 10), 64);
	cf_free(auth);
	auth = as_aws_sigv4_authorization(AWS_KEY, secret, "POST", "/", "", ec2, 4,
		(const uint8_t*)"Action=DescribeInstances", 24, "us-east-1", "ec2", "20150830T123600Z");
	assert_not_null(strstr(auth, "SignedHeaders=content-type;host;x-amz-date;x-amz-security-token, "
		"Signature="));
	cf_free(auth);

	// Encoding and dates.
	char* e = as_aws_encode("tag:aerospike-cluster");
	assert_string_eq(e, "tag%3Aaerospike-cluster");
	cf_free(e);
	e = as_aws_encode("a b~c_d.e-f/+=");
	assert_string_eq(e, "a%20b~c_d.e-f%2F%2B%3D");
	cf_free(e);
	e = as_aws_encode("\xC3\xA9");
	assert_string_eq(e, "%C3%A9");
	cf_free(e);
	e = as_aws_encode("");
	assert_string_eq(e, "");
	cf_free(e);

	char date[AS_AWS_AMZ_DATE_SIZE];
	assert_true(as_aws_amz_date(1440938160000ULL, date));
	assert_string_eq(date, "20150830T123600Z");
	assert_true(as_aws_amz_date(0, date));
	assert_string_eq(date, "19700101T000000Z");

	// Redaction replaces every occurrence of every non-empty secret.
	const char* secrets[] = {"S3CR3T", NULL, "", "tok"};
	char* r = as_aws_redact("a S3CR3T b S3CR3T tok", secrets, 4);
	assert_string_eq(r, "a " AS_AWS_REDACTED " b " AS_AWS_REDACTED " " AS_AWS_REDACTED);
	cf_free(r);
}

//---------------------------------
// Tests: XML
//---------------------------------

static as_aws_xml*
xml_parse(const char* text, char* reason)
{
	return as_aws_xml_parse((const uint8_t*)text, strlen(text), reason, 128);
}

TEST(aws_xml_reader, "the XML reader handles nesting, entities, character references, CDATA, comments, PIs and attributes with Java's child, text and find semantics")
{
	char reason[128];
	as_aws_xml* doc = xml_parse("<?xml version=\"1.0\"?>\n<!-- c --><a x=\"1>2\" y='3'>"
		"<b> x </b><b>y</b><c><b>z</b><d/></c><b z=\"/>\"/></a><!-- end -->\n", reason);
	assert_not_null(doc);

	const as_aws_xml_node* root = as_aws_xml_root(doc);
	assert_string_eq(as_aws_xml_name(root), "a");

	const as_aws_xml_node* b = as_aws_xml_child(root, "b");
	assert_string_eq(as_aws_xml_content(b), "x");
	b = as_aws_xml_next(b);
	assert_string_eq(as_aws_xml_content(b), "y");
	b = as_aws_xml_next(b);
	assert_not_null(b);
	assert_string_eq(as_aws_xml_content(b), "");
	assert_null(as_aws_xml_next(b));

	assert_string_eq(as_aws_xml_text(root, "c"), "z");
	assert_string_eq(as_aws_xml_find(root, "b"), "x");
	assert_string_eq(as_aws_xml_find(as_aws_xml_child(root, "c"), "b"), "z");
	assert_string_eq(as_aws_xml_find(root, "d"), "");
	assert_null(as_aws_xml_find(as_aws_xml_child(root, "c"), "c"));
	assert_null(as_aws_xml_text(root, "d"));
	assert_null(as_aws_xml_text(root, "missing"));
	assert_null(as_aws_xml_child(NULL, "b"));
	assert_string_eq(as_aws_xml_content(root), "x yz");
	as_aws_xml_destroy(doc);

	doc = xml_parse("\xEF\xBB\xBF<a> p <b>q</b> r </a>", reason);
	assert_string_eq(as_aws_xml_content(as_aws_xml_root(doc)), "p q r");
	as_aws_xml_destroy(doc);

	doc = xml_parse("<a>&lt;&gt;&amp;&apos;&quot;&#65;&#x42;&#xe9;&#x1F600;</a>", reason);
	assert_string_eq(as_aws_xml_content(as_aws_xml_root(doc)),
		"<>&'\"AB\xC3\xA9\xF0\x9F\x98\x80");
	as_aws_xml_destroy(doc);

	doc = xml_parse("<a><?pi <b>?>t<![CDATA[<x>&amp;]]><!-- <b> --></a>", reason);
	assert_string_eq(as_aws_xml_content(as_aws_xml_root(doc)), "t<x>&amp;");
	assert_null(as_aws_xml_child(as_aws_xml_root(doc), "b"));
	as_aws_xml_destroy(doc);

	doc = xml_parse("<a>x\r\ny\rz</a>", reason);
	assert_string_eq(as_aws_xml_content(as_aws_xml_root(doc)), "x\ny\nz");
	as_aws_xml_destroy(doc);

	// Not namespace-aware: prefixed names are plain names.
	doc = xml_parse("<p:a xmlns:p=\"urn:x\"><p:b>v</p:b></p:a>", reason);
	assert_string_eq(as_aws_xml_text(as_aws_xml_root(doc), "p:b"), "v");
	as_aws_xml_destroy(doc);
}

static const char*
xml_reject(const char* text, size_t size, char* reason)
{
	as_aws_xml* doc = as_aws_xml_parse((const uint8_t*)text, size, reason, 128);

	if (doc) {
		as_aws_xml_destroy(doc);
		return NULL;
	}
	return reason;
}

static char*
repeat(const char* head, const char* unit, uint32_t n, const char* tail)
{
	size_t h = strlen(head);
	size_t u = strlen(unit);
	size_t t = strlen(tail);
	char* s = malloc(h + u * n + t + 1);
	char* p = s;
	memcpy(p, head, h);
	p += h;

	for (uint32_t i = 0; i < n; i++) {
		memcpy(p, unit, u);
		p += u;
	}
	memcpy(p, tail, t + 1);
	return s;
}

TEST(aws_xml_hardening, "DOCTYPE, ENTITY, NUL, invalid references, depth and node bombs and truncation are rejected; a 16 MiB-scale body parses in linear time")
{
	char reason[128];
	static const struct {
		const char* text;
		const char* reason;
	} cases[] = {
		{"<!DOCTYPE a [<!ENTITY x SYSTEM \"file:///etc/passwd\">]><a>&x;</a>",
			"DOCTYPE is not allowed at offset 0"},
		{"<?xml version=\"1.0\"?><!DOCTYPE a><a/>", "DOCTYPE is not allowed at offset 21"},
		{"<a><!ENTITY x 'y'></a>", "ENTITY is not allowed at offset 3"},
		{"<a><!foo></a>", "invalid markup at offset 3"},
		{"<a>&x;</a>", "undefined entity reference at offset 3"},
		{"<a>&#0;</a>", "invalid character reference at offset 3"},
		{"<a>&#x0;</a>", "invalid character reference at offset 3"},
		{"<a>&#xD800;</a>", "invalid character reference at offset 3"},
		{"<a>&#xDFFF;</a>", "invalid character reference at offset 3"},
		{"<a>&#x110000;</a>", "invalid character reference at offset 3"},
		{"<a>&#99999999999;</a>", "invalid reference at offset 3"},
		{"<a>&#;</a>", "invalid character reference at offset 3"},
		{"<a>&#x;</a>", "invalid character reference at offset 3"},
		{"<a>&#12a;</a>", "invalid character reference at offset 3"},
		{"<a>&#x1;</a>", "invalid character reference at offset 3"},
		{"<a>&amp</a>", "unexpected end of document at offset 3"},
		{"<a>&amp</a>xxxxxxxxxxxx", "invalid reference at offset 3"},
		{"<a></b>", "mismatched end tag at offset 3"},
		{"<a><b></b>", "unexpected end of document at offset 10"},
		{"<a/><b/>", "content outside the root element at offset 4"},
		{"text<a/>", "content outside the root element at offset 0"},
		{"<a/>text", "content outside the root element at offset 4"},
		{"<a/><![CDATA[x]]>", "content outside the root element at offset 4"},
		{"", "no root element at offset 0"},
		{"<!-- only -->", "no root element at offset 13"},
		{"</a>", "unexpected end tag at offset 0"},
		{"<1a/>", "invalid element or attribute name at offset 1"},
		{"<a x=1/>", "invalid attribute at offset 5"},
		{"<a x=\"1\"y=\"2\"/>", "invalid attribute at offset 8"},
		{"<a x=\"<\"/>", "invalid attribute at offset 6"},
		{"<a/ >", "invalid empty element tag at offset 3"},
		{"<a><!-- x", "unexpected end of document at offset 9"}
	};

	for (uint32_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
		const char* got = xml_reject(cases[i].text, strlen(cases[i].text), reason);

		if (!got) {
			fail("case %u (%s) was accepted", i, cases[i].text);
		}

		if (strcmp(got, cases[i].reason) != 0) {
			fail("case %u (%s): got \"%s\", expected \"%s\"", i, cases[i].text, got,
				cases[i].reason);
		}
	}

	assert_string_eq(xml_reject("<a>\0</a>", 8, reason), "NUL character at offset 3");

	// Depth: 64 nested elements are accepted, 65 are not.
	char* deep = repeat("", "<d>", 64, "");
	char* text = malloc(64 * 7 + 1);
	strcpy(text, deep);

	for (int i = 0; i < 64; i++) {
		strcat(text, "</d>");
	}
	as_aws_xml* doc = xml_parse(text, reason);
	assert_not_null(doc);
	as_aws_xml_destroy(doc);
	free(text);
	free(deep);

	deep = repeat("", "<d>", 65, "");
	assert_string_eq(xml_reject(deep, strlen(deep), reason),
		"nesting deeper than 64 elements at offset 192");
	free(deep);

	// Node cap: 1,000,000 elements are accepted, 1,000,001 are not.
	char* many = repeat("<r>", "<a/>", 999999, "</r>");
	doc = xml_parse(many, reason);
	assert_not_null(doc);
	as_aws_xml_destroy(doc);
	free(many);

	many = repeat("<r>", "<a/>", 1000000, "</r>");
	assert_string_eq(xml_reject(many, strlen(many), reason),
		"more than 1000000 elements at offset 3999999");
	free(many);

	// Every proper prefix of a DescribeInstances document is rejected.
	char* instance = aws_xml_instance("i-1", "10.0.0.1", "54.0.0.1", "ip-10-0-0-1", "ec2-54");
	const char* instances[] = {instance};
	char* body = aws_xml_describe("t", instances, 1);
	size_t end = (size_t)(strrchr(body, '>') - body);

	for (size_t len = 0; len <= end; len++) {
		if (!xml_reject(body, len, reason)) {
			fail("prefix of %zu bytes was accepted", len);
		}
	}
	doc = as_aws_xml_parse((const uint8_t*)body, end + 1, reason, sizeof(reason));
	assert_not_null(doc);
	as_aws_xml_destroy(doc);
	free(body);
	free(instance);

	// About 15 MB of items parse in linear time.
	const char* item = "<item><privateIpAddress>10.0.0.1</privateIpAddress></item>";
	uint32_t items = (uint32_t)((15 * 1024 * 1024) / strlen(item));
	char* big = repeat("<r>", item, items, "</r>");
	uint64_t start = cf_getms();
	doc = xml_parse(big, reason);
	assert_not_null(doc);

	uint32_t count = 0;

	for (const as_aws_xml_node* n = as_aws_xml_child(as_aws_xml_root(doc), "item"); n;
		n = as_aws_xml_next(n)) {
		count++;
	}
	assert_null(as_aws_xml_find(as_aws_xml_root(doc), "Code"));
	uint64_t elapsed = cf_getms() - start;
	assert_int_eq(count, items);
	as_aws_xml_destroy(doc);
	free(big);
	info("parsed %u items (%zu bytes) in %" PRIu64 " ms", items, items * strlen(item), elapsed);
	assert_true(elapsed < 60000);
}

TEST(aws_xml_error_text, "API errors carry code, message and request id from EC2 and STS bodies or the header; secrets are redacted before truncation")
{
	as_error err;
	char* body = aws_xml_error(AWS_403_CODE, AWS_403_MESSAGE, AWS_403_ID);
	as_aws_xml_error(&err, "DescribeInstances", 403, (const uint8_t*)body, strlen(body), "hdr-1",
		NULL, 0);
	assert_int_eq(err.code, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(err.message, AWS_403_TEXT);
	free(body);

	const char* sts = "<ErrorResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\"><Error>"
		"<Type>Sender</Type><Code>InvalidIdentityToken</Code><Message>bad token</Message></Error>"
		"<RequestId>c6104cbe-af31-11e0-8154-cbc7ccf896c7</RequestId></ErrorResponse>";
	as_aws_xml_error(&err, "AssumeRoleWithWebIdentity", 400, (const uint8_t*)sts, strlen(sts),
		NULL, NULL, 0);
	assert_string_eq(err.message, "AWS EC2 seed provider: AssumeRoleWithWebIdentity failed: HTTP "
		"400 InvalidIdentityToken: bad token (request id c6104cbe-af31-11e0-8154-cbc7ccf896c7)");

	as_aws_xml_error(&err, "DescribeInstances", 500, (const uint8_t*)"oops", 4, "hdr-2", NULL, 0);
	assert_string_eq(err.message, AWS_FAILED "HTTP 500 (request id hdr-2)");

	const char* no_id = "<Response><Errors><Error><Code>X</Code></Error></Errors></Response>";
	as_aws_xml_error(&err, "DescribeInstances", 400, (const uint8_t*)no_id, strlen(no_id), "hdr-3",
		NULL, 0);
	assert_string_eq(err.message, AWS_FAILED "HTTP 400 X (request id hdr-3)");

	as_aws_xml_error(&err, "DescribeInstances", 503, NULL, 0, "", NULL, 0);
	assert_string_eq(err.message, AWS_FAILED "HTTP 503");

	// A secret echoed in Message is redacted.
	const char* secrets[] = {AWS_SECRET, AWS_TOKEN, "0123abcd"};
	body = aws_xml_error("AuthFailure", "key " AWS_SECRET " token " AWS_TOKEN " sig 0123abcd", "r1");
	as_aws_xml_error(&err, "DescribeInstances", 401, (const uint8_t*)body, strlen(body), NULL,
		secrets, 3);
	assert_string_eq(err.message, AWS_FAILED "HTTP 401 AuthFailure: key <redacted> token "
		"<redacted> sig <redacted> (request id r1)");
	free(body);

	// A secret across the 1023-byte truncation point leaves no prefix of it in the message.
	const char* prefix = AWS_FAILED "HTTP 403 C: ";
	size_t filler = AS_ERROR_MESSAGE_MAX_LEN - 20 - strlen(prefix);
	char* message = malloc(filler + strlen(AWS_SECRET) + 1);
	memset(message, 'x', filler);
	strcpy(message + filler, AWS_SECRET);
	body = aws_xml_error("C", message, "r2");
	as_aws_xml_error(&err, "DescribeInstances", 403, (const uint8_t*)body, strlen(body), NULL,
		secrets, 3);
	// as_error_update() keeps at most AS_ERROR_MESSAGE_MAX_LEN - 1 characters.
	assert_int_eq(strlen(err.message), AS_ERROR_MESSAGE_MAX_LEN - 1);

	for (size_t k = 2; k <= strlen(AWS_SECRET); k++) {
		char part[64];
		snprintf(part, sizeof(part), "%.*s", (int)k, AWS_SECRET);

		if (strstr(err.message, part)) {
			fail("message holds a %zu byte prefix of the secret", k);
		}
	}
	free(body);
	free(message);
}

#ifdef DISCOVERY_FAKE_SERVERS

//---------------------------------
// Static Functions: process counters
//---------------------------------

static int
thread_count(void)
{
#if defined(__linux__)
	DIR* dir = opendir("/proc/self/task");

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
// Static Functions: stub helpers
//---------------------------------

static const char*
stub_url(const aws_stub* s, bool tls, const char* host, char* buf, size_t size)
{
	snprintf(buf, size, "%s://%s:%u", tls ? "https" : "http", host, s->port);
	return buf;
}

static void
stub_403(aws_stub* s)
{
	char* body = aws_xml_error(AWS_403_CODE, AWS_403_MESSAGE, AWS_403_ID);
	aws_stub_http(s, 403, NULL, body);
	free(body);
}

static void
stub_one(aws_stub* s, const char* private_ip)
{
	char* instance = aws_xml_instance("i-1", private_ip, NULL, NULL, NULL);
	const char* instances[] = {instance};
	aws_stub_describe(s, NULL, instances, 1);
	free(instance);
}

static void
stub_raw(aws_stub* s, const char* data, size_t size)
{
	aws_reply reply = {.mode = AWS_REPLY_SEND, .data = (char*)data, .size = size};
	aws_stub_reply(s, &reply);
}

static as_status
provider_for(aws_stub* s, bool tls, const char* host, as_aws_seed_provider_config* cfg,
	as_seed_provider* provider, as_error* err)
{
	char url[128];

	if (!cfg->endpoint) {
		cfg->endpoint = stub_url(s, tls, host, url, sizeof(url));
	}

	as_status status = as_discovery_aws_seed_provider(err, cfg, provider);
	cfg->endpoint = NULL;
	return status;
}

// Refresh a provider for the stub with default settings once; returns the status and the hosts
// or the error message in buf.
static as_status
stub_refresh(aws_stub* s, bool tls, const char* host, const char* ca_file, uint32_t timeout_ms,
	char* buf, size_t size)
{
	as_aws_seed_provider_config cfg;
	aws_cfg(&cfg, NULL);
	cfg.ca_file = ca_file;
	cfg.timeout_ms = timeout_ms;

	as_seed_provider provider;
	as_error err;
	as_status status = provider_for(s, tls, host, &cfg, &provider, &err);

	if (status != AEROSPIKE_OK) {
		snprintf(buf, size, "create: %s", err.message);
		return status;
	}

	status = refresh_str(&provider, &err, buf, size);

	if (status != AEROSPIKE_OK) {
		snprintf(buf, size, "%s", err.message);
	}
	provider.destroy(provider.udata);
	return status;
}

//---------------------------------
// Static Functions: credential chain helpers
//---------------------------------

// 2015-08-30T12:36:00Z, the date of the AWS SigV4 test suite.
#define CRED_NOW_MS 1440938160000ULL
#define CRED_DATE "20150830T123600Z"
#define CRED_EXPIRATION "2015-08-30T13:36:00Z"
#define CRED_EXPIRATION_MS (CRED_NOW_MS + 3600000)
#define CRED_KEY "AKIDTEMPORARY"
#define CRED_SECRET "temporary/secret+key"
#define CRED_TOKEN "temporary-session-token"
#define CRED_NO_SOURCE "AWS EC2 seed provider: no AWS credentials found in config, environment, " \
	"shared credentials file, web identity or ECS, and "
#define CRED_DISABLED CRED_NO_SOURCE "IMDS is disabled"
#define WEB_TOKEN "eyJhbGciOiJSUzI1NiIsImtpZCI6IkVYQU1QTEUifQ.web-identity-token"
#define ROLE_ARN "arn:aws:iam::123456789012:role/seed-role"
#define ECS_AUTHORIZATION "ecs-authorization-token"
#define IMDS_ROLES_PATH "/latest/meta-data/iam/security-credentials/"

static uint32_t
log_matches(const char* text)
{
	uint32_t count = 0;
	pthread_mutex_lock(&log_lock);

	for (uint32_t i = 0; i < log_count; i++) {
		if (strstr(log_lines[i], text)) {
			count++;
		}
	}
	pthread_mutex_unlock(&log_lock);
	return count;
}

// Temporary directory and the entries a test creates in it, removed in reverse order.
typedef struct tmp_dir_s {
	char path[64];
	char entries[16][192];
	uint32_t size;
} tmp_dir;

static bool
tmp_create(tmp_dir* d)
{
	d->size = 0;
	snprintf(d->path, sizeof(d->path), "/tmp/as-aws-cred-XXXXXX");
	return mkdtemp(d->path) != NULL;
}

static const char*
tmp_path(tmp_dir* d, const char* name)
{
	char full[sizeof(d->entries[0])];
	snprintf(full, sizeof(full), "%s/%s", d->path, name);

	char* path = d->entries[d->size++];
	memcpy(path, full, sizeof(full));
	return path;
}

static const char*
tmp_file(tmp_dir* d, const char* name, const char* data, size_t size)
{
	const char* path = tmp_path(d, name);
	FILE* f = fopen(path, "wb");

	if (f) {
		fwrite(data, 1, size, f);
		fclose(f);
	}
	return path;
}

static const char*
tmp_text(tmp_dir* d, const char* name, const char* text)
{
	return tmp_file(d, name, text, strlen(text));
}

static const char*
tmp_mkdir(tmp_dir* d, const char* name)
{
	const char* path = tmp_path(d, name);
	mkdir(path, 0700);
	return path;
}

static const char*
tmp_fifo(tmp_dir* d, const char* name)
{
	const char* path = tmp_path(d, name);
	mkfifo(path, 0600);
	return path;
}

// A file one byte over the 1 MiB cap.
static const char*
tmp_big(tmp_dir* d, const char* name)
{
	size_t size = 1024 * 1024 + 1;
	char* data = malloc(size);
	memset(data, 'a', size);
	const char* path = tmp_file(d, name, data, size);
	free(data);
	return path;
}

static void
tmp_destroy(tmp_dir* d)
{
	while (d->size > 0) {
		remove(d->entries[--d->size]);
	}
	rmdir(d->path);
}

// Chain configuration for the EC2 stub: no static keys.
static void
cred_cfg(as_aws_seed_provider_config* cfg, aws_stub* ec2, char* url, size_t size)
{
	aws_cfg(cfg, stub_url(ec2, false, "127.0.0.1", url, size));
	cfg->access_key_id = NULL;
	cfg->secret_access_key = NULL;
}

// Refresh once; the hosts or the error message go to buf.
static as_status
cred_refresh(as_seed_provider* provider, char* buf, size_t size)
{
	as_error err;
	as_status status = refresh_str(provider, &err, buf, size);

	if (status != AEROSPIKE_OK) {
		snprintf(buf, size, "%s", err.message);
	}
	return status;
}

static as_status
cred_provider(aws_stub* ec2, fake_env* f, as_seed_provider* provider, char* buf, size_t size)
{
	char url[64];
	as_aws_seed_provider_config cfg;
	cred_cfg(&cfg, ec2, url, sizeof(url));
	as_error err;
	as_status status = as_discovery_aws_seed_provider_env(&err, &cfg, &f->env, provider);

	if (status != AEROSPIKE_OK) {
		snprintf(buf, size, "create: %.400s", err.message);
	}
	return status;
}

// Create a chain provider, refresh it once and destroy it.
static as_status
cred_once(aws_stub* ec2, fake_env* f, char* buf, size_t size)
{
	as_seed_provider provider;
	as_status status = cred_provider(ec2, f, &provider, buf, size);

	if (status != AEROSPIKE_OK) {
		return status;
	}
	status = cred_refresh(&provider, buf, size);
	provider.destroy(provider.udata);
	return status;
}

// Request i of s on the heap, or NULL. The caller frees it.
static aws_request*
cred_request(aws_stub* s, uint32_t i)
{
	aws_request* r = malloc(sizeof(aws_request));

	if (!aws_stub_request(s, i, r)) {
		free(r);
		return NULL;
	}
	return r;
}

// True when DescribeInstances request i was signed with these credentials: the authorization
// equals a SigV4 signature recomputed from the recorded x-amz-date.
static bool
signed_with(aws_stub* ec2, uint32_t i, const char* key, const char* secret, const char* token)
{
	aws_request* r = cred_request(ec2, i);

	if (!r) {
		return false;
	}

	char host[64];
	char date[64];
	char auth[512];
	char value[512];
	bool ok = aws_request_header(r, "host", host, sizeof(host)) == 1 &&
		aws_request_header(r, "x-amz-date", date, sizeof(date)) == 1 &&
		aws_request_header(r, "authorization", auth, sizeof(auth)) == 1;
	uint32_t tokens = aws_request_header(r, "x-amz-security-token", value, sizeof(value));

	ok = ok && (token ? tokens == 1 && strcmp(value, token) == 0 : tokens == 0);

	if (ok) {
		as_aws_header headers[] = {
			{"content-type", AWS_FORM},
			{"host", host},
			{"x-amz-date", date},
			{"x-amz-security-token", token}
		};
		char* expected = as_aws_sigv4_authorization(key, secret, "POST", "/", "", headers,
			token ? 4 : 3, (const uint8_t*)r->body, r->body_size, "us-east-1", "ec2", date);
		ok = expected && strcmp(expected, auth) == 0;
		cf_free(expected);
	}
	free(r);
	return ok;
}

static bool
request_is(aws_stub* s, uint32_t i, const char* method, const char* target)
{
	aws_request* r = cred_request(s, i);
	bool ok = r && strcmp(r->method, method) == 0 && strcmp(r->target, target) == 0;
	free(r);
	return ok;
}

// Number of headers named name in request i, with the first value in out.
static uint32_t
request_header(aws_stub* s, uint32_t i, const char* name, char* out, size_t size)
{
	aws_request* r = cred_request(s, i);
	uint32_t count = r ? aws_request_header(r, name, out, size) : 0;
	free(r);
	return count;
}

//---------------------------------
// Tests: DescribeInstances
//---------------------------------

TEST(aws_refresh_addresses, "each address field is selected, missing and invalid values are skipped and counted, hosts are sorted, deduplicated and carry port and TLS name")
{
	aws_stub s;
	assert_true(aws_stub_start(&s, false));

	char* long_ip = repeat("", "1", 300, "");
	char* items[] = {
		aws_xml_instance("i-1", "10.0.0.2", "54.0.0.2", "ip-10-0-0-2.ec2.internal",
			"ec2-54-0-0-2.compute-1.amazonaws.com"),
		aws_xml_instance("i-2", "10.0.0.1", NULL, NULL, NULL),
		aws_xml_instance("i-3", "10.0.0.2", NULL, NULL, NULL),
		aws_xml_instance("i-4", "  10.0.0.3\n ", NULL, NULL, NULL),
		aws_xml_instance("i-5", "10.0.0.4 10.0.0.5", NULL, NULL, NULL),
		aws_xml_instance("i-6", long_ip, NULL, NULL, NULL),
		aws_xml_instance("i-7", "", NULL, NULL, NULL)
	};
	aws_stub_describe(&s, NULL, (const char* const*)items, 7);

	static const struct {
		as_aws_address_field field;
		const char* hosts;
		const char* skipped;
	} expect[] = {
		{AS_AWS_ADDRESS_PRIVATE_IP, "10.0.0.1:3100,10.0.0.2:3100,10.0.0.3:3100",
			"AWS EC2 seed provider skipped 3 instances without privateIpAddress"},
		{AS_AWS_ADDRESS_PUBLIC_IP, "54.0.0.2:3100",
			"AWS EC2 seed provider skipped 6 instances without ipAddress"},
		{AS_AWS_ADDRESS_PRIVATE_DNS, "ip-10-0-0-2.ec2.internal:3100",
			"AWS EC2 seed provider skipped 6 instances without privateDnsName"},
		{AS_AWS_ADDRESS_PUBLIC_DNS, "ec2-54-0-0-2.compute-1.amazonaws.com:3100",
			"AWS EC2 seed provider skipped 6 instances without dnsName"}
	};

	for (uint32_t i = 0; i < 4; i++) {
		as_aws_seed_provider_config cfg;
		aws_cfg(&cfg, NULL);
		cfg.address_field = expect[i].field;
		cfg.port = 3100;
		cfg.tls_name = "aerospike-tls";

		as_seed_provider provider;
		as_error err;
		assert_int_eq(provider_for(&s, false, "127.0.0.1", &cfg, &provider, &err), AEROSPIKE_OK);

		log_start();
		as_seed_candidates out;
		as_status status = run_refresh(&provider, &err, &out);
		as_seed_candidates out2;
		as_status status2 = run_refresh(&provider, &err, &out2);
		log_stop();

		assert_int_eq(status, AEROSPIKE_OK);
		assert_int_eq(status2, AEROSPIKE_OK);

		char buf[512];
		assert_string_eq(hosts_str(out.hosts, buf, sizeof(buf)), expect[i].hosts);

		for (uint32_t j = 0; j < out.hosts->size; j++) {
			as_host* host = as_vector_get(out.hosts, j);
			assert_string_eq(host->tls_name, "aerospike-tls");
			assert_int_eq(host->port, 3100);
		}
		assert_string_eq(hosts_str(out2.hosts, buf, sizeof(buf)), expect[i].hosts);
		as_seed_candidates_destroy(&out);
		as_seed_candidates_destroy(&out2);
		provider.destroy(provider.udata);

		// The source line is logged once per provider, the summary on every refresh.
		char summary[200];
		snprintf(summary, sizeof(summary), "AWS EC2 seed provider us-east-1 returned %u hosts from "
			"7 instances in 1 pages", i == 0 ? 3 : 1);
		int source = log_find(AS_LOG_LEVEL_INFO, "AWS EC2 seed provider credentials from config");
		assert_true(source >= 0);
		assert_int_eq(log_find(AS_LOG_LEVEL_DEBUG, expect[i].skipped) >= 0, true);
		assert_int_eq(log_find(AS_LOG_LEVEL_DEBUG, summary) >= 0, true);
		assert_int_eq(log_size(), 5);
		assert_false(log_contains(AWS_SECRET));
		assert_false(log_contains(AWS_KEY));
	}

	for (uint32_t i = 0; i < 7; i++) {
		free(items[i]);
	}
	free(long_ip);
	assert_int_eq(aws_stub_requests(&s), 8);
	aws_stub_stop(&s);
}

TEST(aws_request_shape, "the request is one POST form in configured filter order with running appended, one signed host header and an authorization equal to a recomputed SigV4 signature")
{
	aws_stub s;
	assert_true(aws_stub_start(&s, false));
	stub_one(&s, "10.0.0.1");

	// Mutable caller buffers, changed after creation.
	char region[32] = "us-east-1";
	char name1[32] = "tag:aerospike-cluster";
	char value1[32] = "prod a";
	char value2[32] = "b/c";
	char name2[32] = "instance-type";
	char value3[32] = "c5.large";
	char key[32] = AWS_KEY;
	char secret[64] = AWS_SECRET;
	char token[64] = AWS_TOKEN;
	char endpoint[128];
	snprintf(endpoint, sizeof(endpoint), "http://127.0.0.1:%u/ec2/", s.port);

	const char* values1[] = {value1, value2};
	const char* values2[] = {value3};
	as_aws_filter filters[] = {{name1, values1, 2}, {name2, values2, 1}};

	as_aws_seed_provider_config cfg;
	aws_cfg(&cfg, endpoint);
	cfg.region = region;
	cfg.filters = filters;
	cfg.filters_size = 2;
	cfg.access_key_id = key;
	cfg.secret_access_key = secret;
	cfg.session_token = token;

	as_seed_provider provider;
	as_error err;
	assert_int_eq(as_discovery_aws_seed_provider(&err, &cfg, &provider), AEROSPIKE_OK);

	strcpy(region, "eu-west-9");
	strcpy(name1, "tag:changed");
	strcpy(value1, "changed");
	values1[1] = "changed";
	filters[1].name = "changed";
	filters[1].values_size = 0;
	strcpy(key, "CHANGED");
	strcpy(secret, "changed");
	strcpy(token, "changed");
	strcpy(endpoint, "http://192.0.2.1:1/");

	char buf[256];
	assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)), AEROSPIKE_OK);
	assert_string_eq(buf, "10.0.0.1:3000");
	provider.destroy(provider.udata);

	aws_request* r = malloc(sizeof(aws_request));
	assert_true(aws_stub_request(&s, 0, r));
	assert_string_eq(r->method, "POST");
	assert_string_eq(r->target, "/ec2/");

	const char* expected_body = "Action=DescribeInstances&Version=2016-11-15"
		"&Filter.1.Name=tag%3Aaerospike-cluster&Filter.1.Value.1=prod%20a&Filter.1.Value.2=b%2Fc"
		"&Filter.2.Name=instance-type&Filter.2.Value.1=c5.large"
		"&Filter.3.Name=instance-state-name&Filter.3.Value.1=running";
	assert_string_eq(r->body, expected_body);

	char host[64];
	char date[64];
	char auth[512];
	char value[512];
	char expected_host[64];
	snprintf(expected_host, sizeof(expected_host), "127.0.0.1:%u", s.port);
	assert_int_eq(aws_request_header(r, "host", host, sizeof(host)), 1);
	assert_string_eq(host, expected_host);
	assert_int_eq(aws_request_header(r, "content-type", value, sizeof(value)), 1);
	assert_string_eq(value, AWS_FORM);
	assert_int_eq(aws_request_header(r, "content-length", value, sizeof(value)), 1);
	assert_int_eq(atoi(value), (int)strlen(expected_body));
	assert_int_eq(aws_request_header(r, "connection", value, sizeof(value)), 1);
	assert_string_eq(value, "close");
	assert_int_eq(aws_request_header(r, "x-amz-security-token", value, sizeof(value)), 1);
	assert_string_eq(value, AWS_TOKEN);
	assert_int_eq(aws_request_header(r, "x-amz-date", date, sizeof(date)), 1);
	assert_int_eq(strlen(date), 16);
	assert_int_eq(date[15], 'Z');
	assert_int_eq(aws_request_header(r, "authorization", auth, sizeof(auth)), 1);

	// Headers go out sorted by name, as Java's TreeMap sends them.
	assert_int_eq(strncmp(r->head, "authorization: ", 15), 0);
	const char* order[] = {
		"\r\ncontent-type: ", "\r\nhost: ", "\r\nx-amz-date: ", "\r\nx-amz-security-token: ",
		"\r\ncontent-length: ", "\r\nconnection: "
	};
	const char* at = r->head;

	for (uint32_t i = 0; i < 6; i++) {
		const char* next = strstr(at, order[i]);
		assert_not_null(next);
		at = next;
	}

	as_aws_header signed_headers[] = {
		{"content-type", AWS_FORM},
		{"host", host},
		{"x-amz-date", date},
		{"x-amz-security-token", AWS_TOKEN}
	};
	char* recomputed = as_aws_sigv4_authorization(AWS_KEY, AWS_SECRET, "POST", "/ec2/", "",
		signed_headers, 4, (const uint8_t*)r->body, r->body_size, "us-east-1", "ec2", date);
	assert_string_eq(auth, recomputed);
	assert_not_null(strstr(auth, "SignedHeaders=content-type;host;x-amz-date;"
		"x-amz-security-token, "));
	cf_free(recomputed);

	// Without a session token and without a path.
	aws_stub_reset(&s);
	stub_one(&s, "10.0.0.1");
	aws_cfg(&cfg, NULL);
	assert_int_eq(provider_for(&s, false, "127.0.0.1", &cfg, &provider, &err), AEROSPIKE_OK);
	assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)), AEROSPIKE_OK);
	provider.destroy(provider.udata);

	assert_true(aws_stub_request(&s, 0, r));
	assert_string_eq(r->target, "/");
	assert_string_eq(r->body, "Action=DescribeInstances&Version=2016-11-15"
		"&Filter.1.Name=tag%3Aaerospike-cluster&Filter.1.Value.1=prod-a"
		"&Filter.2.Name=instance-state-name&Filter.2.Value.1=running");
	assert_int_eq(aws_request_header(r, "x-amz-security-token", NULL, 0), 0);
	assert_int_eq(aws_request_header(r, "x-amz-date", date, sizeof(date)), 1);
	assert_int_eq(aws_request_header(r, "authorization", auth, sizeof(auth)), 1);
	recomputed = as_aws_sigv4_authorization(AWS_KEY, AWS_SECRET, "POST", "/", "", signed_headers,
		3, (const uint8_t*)r->body, r->body_size, "us-east-1", "ec2", date);
	assert_string_eq(auth, recomputed);
	assert_not_null(strstr(auth, "SignedHeaders=content-type;host;x-amz-date, "));
	cf_free(recomputed);
	free(r);
	aws_stub_stop(&s);
}

TEST(aws_pagination, "NextToken pages are followed and encoded, collected across pages, a failed page fails the refresh and a repeated token is rejected")
{
	aws_stub s;
	assert_true(aws_stub_start(&s, false));

	char* i1 = aws_xml_instance("i-1", "10.0.0.3", NULL, NULL, NULL);
	char* i2 = aws_xml_instance("i-2", "10.0.0.1", NULL, NULL, NULL);
	char* i3 = aws_xml_instance("i-3", "10.0.0.2", NULL, NULL, NULL);
	const char* p1[] = {i1};
	const char* p2[] = {i2};
	const char* p3[] = {i3, i1};
	aws_stub_describe(&s, "page-2", p1, 1);
	aws_stub_describe(&s, "a b/c+=", p2, 1);
	aws_stub_describe(&s, NULL, p3, 2);

	as_aws_seed_provider_config cfg;
	as_seed_provider provider;
	as_error err;
	char buf[512];
	aws_cfg(&cfg, NULL);
	assert_int_eq(provider_for(&s, false, "127.0.0.1", &cfg, &provider, &err), AEROSPIKE_OK);

	log_start();
	as_status status = refresh_str(&provider, &err, buf, sizeof(buf));
	log_stop();
	assert_int_eq(status, AEROSPIKE_OK);
	assert_string_eq(buf, "10.0.0.1:3000,10.0.0.2:3000,10.0.0.3:3000");
	assert_true(log_find(AS_LOG_LEVEL_DEBUG, "AWS EC2 seed provider us-east-1 returned 3 hosts "
		"from 4 instances in 3 pages") >= 0);
	assert_int_eq(aws_stub_requests(&s), 3);

	aws_request* r = malloc(sizeof(aws_request));
	assert_true(aws_stub_request(&s, 0, r));
	assert_null(strstr(r->body, "NextToken"));
	assert_true(aws_stub_request(&s, 1, r));
	assert_not_null(strstr(r->body, "&Filter.2.Value.1=running&NextToken=page-2"));
	assert_int_eq(strlen(strstr(r->body, "&NextToken=")), strlen("&NextToken=page-2"));
	assert_true(aws_stub_request(&s, 2, r));
	assert_not_null(strstr(r->body, "&NextToken=a%20b%2Fc%2B%3D"));

	// A failed second page fails the whole refresh without hosts.
	aws_stub_reset(&s);
	aws_stub_describe(&s, "page-2", p1, 1);
	stub_403(&s);
	as_seed_candidates out;
	assert_int_eq(run_refresh(&provider, &err, &out), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(err.message, AWS_403_TEXT);
	assert_int_eq(out.hosts->size, 0);
	as_seed_candidates_destroy(&out);

	// A repeated token.
	aws_stub_reset(&s);
	aws_stub_describe(&s, "same", p1, 1);
	aws_stub_describe(&s, "same", p2, 1);
	assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)),
		AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(err.message, "AWS EC2 seed provider: invalid DescribeInstances response: "
		"pagination did not advance");
	assert_int_eq(aws_stub_requests(&s), 2);

	// An empty nextToken ends the pages.
	aws_stub_reset(&s);
	aws_stub_describe(&s, "", p1, 1);
	assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)), AEROSPIKE_OK);
	assert_string_eq(buf, "10.0.0.3:3000");
	assert_int_eq(aws_stub_requests(&s), 1);

	provider.destroy(provider.udata);
	free(r);
	free(i1);
	free(i2);
	free(i3);
	aws_stub_stop(&s);
}

TEST(aws_empty_and_errors, "an empty result is success; API errors carry code, message and request id from the body or header, also EOF-framed; DOCTYPE and empty bodies are invalid")
{
	aws_stub s;
	assert_true(aws_stub_start(&s, false));
	char buf[1100];

	aws_stub_describe(&s, NULL, NULL, 0);
	assert_int_eq(stub_refresh(&s, false, "127.0.0.1", NULL, 2000, buf, sizeof(buf)),
		AEROSPIKE_OK);
	assert_string_eq(buf, "");

	aws_stub_reset(&s);
	aws_stub_http(&s, 200, NULL, "<DescribeInstancesResponse><reservationSet><item>"
		"<reservationId>r-1</reservationId></item></reservationSet></DescribeInstancesResponse>");
	assert_int_eq(stub_refresh(&s, false, "127.0.0.1", NULL, 2000, buf, sizeof(buf)),
		AEROSPIKE_OK);
	assert_string_eq(buf, "");

	aws_stub_reset(&s);
	stub_403(&s);
	assert_int_eq(stub_refresh(&s, false, "127.0.0.1", NULL, 2000, buf, sizeof(buf)),
		AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, AWS_403_TEXT);

	// EOF-framed error body.
	aws_stub_reset(&s);
	char* body = aws_xml_error(AWS_403_CODE, AWS_403_MESSAGE, AWS_403_ID);
	char* raw = aws_strf("HTTP/1.1 403 Forbidden\r\nconnection: close\r\n\r\n%s", body);
	stub_raw(&s, raw, strlen(raw));
	assert_int_eq(stub_refresh(&s, false, "127.0.0.1", NULL, 2000, buf, sizeof(buf)),
		AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, AWS_403_TEXT);
	free(raw);
	free(body);

	aws_stub_reset(&s);
	aws_stub_http(&s, 500, "x-amzn-RequestId: hdr-123\r\n", "");
	assert_int_eq(stub_refresh(&s, false, "127.0.0.1", NULL, 2000, buf, sizeof(buf)),
		AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, AWS_FAILED "HTTP 500 (request id hdr-123)");

	aws_stub_reset(&s);
	aws_stub_http(&s, 503, NULL, "Service Unavailable");
	assert_int_eq(stub_refresh(&s, false, "127.0.0.1", NULL, 2000, buf, sizeof(buf)),
		AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, AWS_FAILED "HTTP 503");

	// Secrets echoed by the server are redacted.
	aws_stub_reset(&s);
	body = aws_xml_error("AuthFailure", "secret " AWS_SECRET " token " AWS_TOKEN, "r-9");
	aws_stub_http(&s, 401, NULL, body);
	free(body);
	as_aws_seed_provider_config cfg;
	as_seed_provider provider;
	as_error err;
	aws_cfg(&cfg, NULL);
	cfg.session_token = AWS_TOKEN;
	assert_int_eq(provider_for(&s, false, "127.0.0.1", &cfg, &provider, &err), AEROSPIKE_OK);
	assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)),
		AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(err.message, AWS_FAILED "HTTP 401 AuthFailure: secret <redacted> token "
		"<redacted> (request id r-9)");
	provider.destroy(provider.udata);

	aws_stub_reset(&s);
	aws_stub_http(&s, 200, NULL,
		"<!DOCTYPE r [<!ENTITY x SYSTEM \"file:///etc/passwd\">]><r>&x;</r>");
	assert_int_eq(stub_refresh(&s, false, "127.0.0.1", NULL, 2000, buf, sizeof(buf)),
		AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, "AWS EC2 seed provider: invalid DescribeInstances response: DOCTYPE "
		"is not allowed at offset 0");

	aws_stub_reset(&s);
	aws_stub_http(&s, 200, NULL, "");
	assert_int_eq(stub_refresh(&s, false, "127.0.0.1", NULL, 2000, buf, sizeof(buf)),
		AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, "AWS EC2 seed provider: invalid DescribeInstances response: no root "
		"element at offset 0");

	// A 200 body of another action, or a 200 error document, is not a result.
	const char* roots[] = {
		"<DescribeTagsResponse><reservationSet/></DescribeTagsResponse>",
		"<Response><Errors><Error><Code>X</Code></Error></Errors></Response>",
		"<ec2:DescribeInstancesResponse/>"
	};

	for (uint32_t i = 0; i < sizeof(roots) / sizeof(roots[0]); i++) {
		aws_stub_reset(&s);
		aws_stub_http(&s, 200, NULL, roots[i]);
		assert_int_eq(stub_refresh(&s, false, "127.0.0.1", NULL, 2000, buf, sizeof(buf)),
			AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
		assert_string_eq(buf, "AWS EC2 seed provider: invalid DescribeInstances response: the "
			"root element is not DescribeInstancesResponse");
	}
	aws_stub_stop(&s);
}

typedef struct framing_case_s {
	const char* name;
	char* raw;
	size_t size;
	const char* expected;
} framing_case;

TEST(aws_http_framing, "Content-Length, chunked and EOF framing are read as Java reads them; oversize, malformed and truncated responses fail with a reason")
{
	aws_stub s;
	assert_true(aws_stub_start(&s, false));

	char* instance = aws_xml_instance("i-1", "10.0.0.1", NULL, NULL, NULL);
	const char* instances[] = {instance};
	char* doc = aws_xml_describe(NULL, instances, 1);
	size_t half = strlen(doc) / 2;
	char* big_header = repeat("x-big: ", "a", 9000, "\r\n");
	char* many_headers = repeat("", "x-h: 1\r\n", 101, "");
	char* hundred_headers = repeat("", "x-h: 1\r\n", 98, "");

	size_t over = AWS_MAX_RESPONSE + 100;
	char* oversize = malloc(over);
	int head = snprintf(oversize, over, "HTTP/1.1 200 OK\r\nconnection: close\r\n\r\n");
	memset(oversize + head, ' ', over - (size_t)head);

	framing_case cases[] = {
		{"chunked with extensions and trailers", aws_strf("HTTP/1.1 200 OK\r\n"
			"Transfer-Encoding: chunked\r\n\r\n%zx;ext=1\r\n%.*s\r\n%zX \r\n%s\r\n0\r\n"
			"x-trailer: 1\r\n\r\n", half, (int)half, doc, strlen(doc) - half, doc + half), 0,
			"10.0.0.1:3000"},
		{"EOF-framed 200", aws_strf("HTTP/1.1 200 OK\r\nconnection: close\r\n\r\n%s", doc), 0,
			"10.0.0.1:3000"},
		{"duplicate equal content-length", aws_strf("HTTP/1.1 200 OK\r\ncontent-length: %zu\r\n"
			"Content-Length: %zu\r\n\r\n%s", strlen(doc), strlen(doc), doc), 0, "10.0.0.1:3000"},
		{"100 headers", aws_strf("HTTP/1.1 200 OK\r\ncontent-length: %zu\r\n%sx-amzn-requestid: "
			"r\r\n\r\n%s", strlen(doc), hundred_headers, doc), 0, "10.0.0.1:3000"},
		{"LF line ends", aws_strf("HTTP/1.1 200 OK\ncontent-length: %zu\n\n%s", strlen(doc),
			doc), 0, "10.0.0.1:3000"},
		{"204", aws_strf("HTTP/1.1 204 No Content\r\n\r\n"), 0, AWS_FAILED "HTTP 204"},
		{"304", aws_strf("HTTP/1.1 304 Not Modified\r\n\r\n"), 0, AWS_FAILED "HTTP 304"},
		{"declared oversize", aws_strf("HTTP/1.1 200 OK\r\ncontent-length: 16777217\r\n\r\n"), 0,
			AWS_FAILED "HTTP response exceeds 16777216 bytes"},
		{"oversize header line", aws_strf("HTTP/1.1 200 OK\r\n%s\r\n", big_header), 0,
			AWS_FAILED "HTTP header line exceeds 8192 bytes"},
		{"101 headers", aws_strf("HTTP/1.1 200 OK\r\n%s\r\n", many_headers), 0,
			AWS_FAILED "HTTP response has more than 100 headers"},
		{"bad status line", aws_strf("HTTX/1.1 200 OK\r\n\r\n"), 0,
			AWS_FAILED "invalid HTTP status line"},
		{"status not numeric", aws_strf("HTTP/1.1 2x0 OK\r\n\r\n"), 0,
			AWS_FAILED "invalid HTTP status line"},
		{"status missing", aws_strf("HTTP/1.1\r\n\r\n"), 0, AWS_FAILED "invalid HTTP status line"},
		{"bad chunk size", aws_strf("HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\nzz\r\n"),
			0, AWS_FAILED "invalid HTTP chunk size"},
		{"chunk size overflow", aws_strf("HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n"
			"1fffffffffffffff\r\n"), 0, AWS_FAILED "invalid HTTP chunk size"},
		{"chunk over the cap", aws_strf("HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n"
			"1000001\r\n"), 0, AWS_FAILED "HTTP response exceeds 16777216 bytes"},
		{"truncated chunk", aws_strf("HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n"
			"100\r\nabc"), 0, AWS_FAILED "truncated response"},
		{"chunk data over its size", aws_strf("HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n"
			"\r\n3\r\nabcd\r\n0\r\n\r\n"), 0, AWS_FAILED "HTTP chunk data exceeds its size"},
		{"chunk data without its line end", aws_strf("HTTP/1.1 200 OK\r\ntransfer-encoding: "
			"chunked\r\n\r\n3\r\nabc0\r\n\r\n"), 0, AWS_FAILED "HTTP chunk data exceeds its size"},
		{"truncated content-length body", aws_strf("HTTP/1.1 200 OK\r\ncontent-length: 100\r\n\r\n"
			"abc"), 0, AWS_FAILED "truncated response"},
		{"truncated headers", aws_strf("HTTP/1.1 200 OK\r\ncontent-length: 100\r\n"), 0,
			AWS_FAILED "truncated response"},
		{"conflicting content-length", aws_strf("HTTP/1.1 200 OK\r\ncontent-length: 5\r\n"
			"content-length: 6\r\n\r\n"), 0, AWS_FAILED "conflicting HTTP content-length headers"},
		{"invalid content-length", aws_strf("HTTP/1.1 200 OK\r\ncontent-length: 1e3\r\n\r\n"), 0,
			AWS_FAILED "invalid HTTP content-length"},
		{"EOF-framed oversize", oversize, over, AWS_FAILED "HTTP response exceeds 16777216 bytes"}
	};
	uint32_t cases_size = sizeof(cases) / sizeof(cases[0]);

	for (uint32_t i = 0; i < cases_size; i++) {
		aws_stub_reset(&s);
		stub_raw(&s, cases[i].raw, cases[i].size ? cases[i].size : strlen(cases[i].raw));

		char buf[1100];
		stub_refresh(&s, false, "127.0.0.1", NULL, 3000, buf, sizeof(buf));

		if (strcmp(buf, cases[i].expected) != 0) {
			fail("%s: got \"%s\", expected \"%s\"", cases[i].name, buf, cases[i].expected);
		}
	}

	// No response at all.
	aws_stub_reset(&s);
	aws_reply close_reply = {.mode = AWS_REPLY_CLOSE};
	aws_stub_reply(&s, &close_reply);
	char buf[1100];
	stub_refresh(&s, false, "127.0.0.1", NULL, 3000, buf, sizeof(buf));
	assert_string_eq(buf, AWS_FAILED "connection closed without a response");

	// A refused connection.
	uint16_t port = s.port;
	aws_stub_stop(&s);
	char url[64];
	snprintf(url, sizeof(url), "http://127.0.0.1:%u", port);
	as_aws_seed_provider_config cfg;
	as_seed_provider provider;
	as_error err;
	aws_cfg(&cfg, url);
	assert_int_eq(as_discovery_aws_seed_provider(&err, &cfg, &provider), AEROSPIKE_OK);
	assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)),
		AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	char prefix[128];
	snprintf(prefix, sizeof(prefix), AWS_FAILED "connect to 127.0.0.1:%u failed: ", port);
	assert_int_eq(strncmp(err.message, prefix, strlen(prefix)), 0);
	provider.destroy(provider.udata);

	// An unresolvable endpoint host.
	aws_cfg(&cfg, "http://as-aws-test.invalid:1");
	assert_int_eq(as_discovery_aws_seed_provider(&err, &cfg, &provider), AEROSPIKE_OK);
	assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)),
		AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_int_eq(strncmp(err.message, AWS_FAILED "Invalid hostname as-aws-test.invalid: ",
		strlen(AWS_FAILED "Invalid hostname as-aws-test.invalid: ")), 0);
	provider.destroy(provider.udata);

	for (uint32_t i = 0; i < cases_size; i++) {
		free(cases[i].raw);
	}
	free(hundred_headers);
	free(many_headers);
	free(big_header);
	free(doc);
	free(instance);
}

TEST(aws_header_injection, "a CR or LF in a credential is rejected at creation, before any connection")
{
	aws_stub s;
	assert_true(aws_stub_start(&s, false));
	stub_one(&s, "10.0.0.1");

	as_aws_seed_provider_config cfg;
	as_seed_provider provider;
	as_error err;
	aws_cfg(&cfg, NULL);
	cfg.session_token = "tok\r\nhost: evil";
	assert_int_eq(provider_for(&s, false, "127.0.0.1", &cfg, &provider, &err), AEROSPIKE_ERR_PARAM);
	assert_string_eq(err.message, "Invalid AWS EC2 seed provider session_token: invalid character");

	cfg.session_token = NULL;
	cfg.secret_access_key = "secret\n";
	assert_int_eq(provider_for(&s, false, "127.0.0.1", &cfg, &provider, &err), AEROSPIKE_ERR_PARAM);

	cfg.secret_access_key = AWS_SECRET;
	cfg.endpoint = "http://127.0.0.1/\r\nx: 1";
	assert_int_eq(as_discovery_aws_seed_provider(&err, &cfg, &provider), AEROSPIKE_ERR_PARAM);
	as_sleep(50);
	assert_int_eq(aws_stub_accepts(&s), 0);
	aws_stub_stop(&s);
}

TEST(aws_endpoint_forms, "a bracketed [::1] endpoint sends a bracketed host header; a localhost name tries each resolved address in order")
{
	aws_stub s;

	if (aws_stub_start(&s, true)) {
		stub_one(&s, "10.0.0.1");
		char buf[512];
		assert_int_eq(stub_refresh(&s, false, "[::1]", NULL, 2000, buf, sizeof(buf)),
			AEROSPIKE_OK);
		assert_string_eq(buf, "10.0.0.1:3000");

		aws_request* r = malloc(sizeof(aws_request));
		char host[64];
		char expected[64];
		assert_true(aws_stub_request(&s, 0, r));
		assert_int_eq(aws_request_header(r, "host", host, sizeof(host)), 1);
		snprintf(expected, sizeof(expected), "[::1]:%u", s.port);
		assert_string_eq(host, expected);
		free(r);
		aws_stub_stop(&s);
	}
	else {
		aws_stub_stop(&s);
		info("skipped [::1]: IPv6 loopback is unavailable");
	}

	struct addrinfo hints;
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	struct addrinfo* list = NULL;
	bool v6_first = false;

	if (getaddrinfo("localhost", "80", &hints, &list) == 0 && list) {
		bool v4_later = false;

		for (struct addrinfo* a = list->ai_next; a; a = a->ai_next) {
			v4_later = v4_later || a->ai_family == AF_INET;
		}
		v6_first = list->ai_family == AF_INET6 && v4_later;
		freeaddrinfo(list);
	}

	if (!v6_first) {
		info("skipped localhost fallback: getaddrinfo(\"localhost\") does not return ::1 before "
			"127.0.0.1");
		return;
	}

	assert_true(aws_stub_start(&s, false));
	stub_one(&s, "10.0.0.2");
	char buf[512];
	assert_int_eq(stub_refresh(&s, false, "localhost", NULL, 2000, buf, sizeof(buf)), AEROSPIKE_OK);
	assert_string_eq(buf, "10.0.0.2:3000");
	aws_stub_stop(&s);
}

//---------------------------------
// Tests: HTTPS
//---------------------------------

TEST(aws_tls, "HTTPS verifies trust through ca_file or ca_path, the host name or IP against the SAN, sends SNI for names, and requires close_notify for EOF framing")
{
	aws_certs certs;
	assert_true(aws_certs_create(&certs));
	aws_stub s;
	assert_true(aws_stub_start(&s, false));
	aws_stub_set_tls(&s, certs.good);
	stub_one(&s, "10.0.0.1");

	char buf[1100];
	aws_request* r = malloc(sizeof(aws_request));

	assert_int_eq(stub_refresh(&s, true, "localhost", certs.ca_file, 2000, buf, sizeof(buf)),
		AEROSPIKE_OK);
	assert_string_eq(buf, "10.0.0.1:3000");
	assert_true(aws_stub_request(&s, 0, r));
	assert_string_eq(r->sni, "localhost");

	assert_int_eq(stub_refresh(&s, true, "127.0.0.1", certs.ca_file, 2000, buf, sizeof(buf)),
		AEROSPIKE_OK);
	assert_true(aws_stub_request(&s, 1, r));
	assert_string_eq(r->sni, "");

	// ca_path alone.
	as_aws_seed_provider_config cfg;
	as_seed_provider provider;
	as_error err;
	aws_cfg(&cfg, NULL);
	cfg.ca_path = certs.ca_path;
	assert_int_eq(provider_for(&s, true, "localhost", &cfg, &provider, &err), AEROSPIKE_OK);
	assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)), AEROSPIKE_OK);
	assert_string_eq(buf, "10.0.0.1:3000");
	provider.destroy(provider.udata);

	// The default verify paths do not trust the generated CA.
	assert_int_eq(stub_refresh(&s, true, "localhost", NULL, 2000, buf, sizeof(buf)),
		AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_int_eq(strncmp(buf, AWS_FAILED "certificate verification failed: ",
		strlen(AWS_FAILED "certificate verification failed: ")), 0);
	const char* hint = "the OpenSSL default verify paths; set ca_file or ca_path)";
	assert_string_eq(buf + strlen(buf) - strlen(hint), hint);

	aws_stub_set_tls(&s, certs.untrusted);
	assert_int_eq(stub_refresh(&s, true, "localhost", certs.ca_file, 2000, buf, sizeof(buf)),
		AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, AWS_FAILED "certificate verification failed: unable to get local "
		"issuer certificate");

	aws_stub_set_tls(&s, certs.wrong_dns);
	assert_int_eq(stub_refresh(&s, true, "localhost", certs.ca_file, 2000, buf, sizeof(buf)),
		AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, AWS_FAILED "certificate verification failed: certificate does not "
		"match host localhost");

	aws_stub_set_tls(&s, certs.no_ip);
	assert_int_eq(stub_refresh(&s, true, "127.0.0.1", certs.ca_file, 2000, buf, sizeof(buf)),
		AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, AWS_FAILED "certificate verification failed: certificate does not "
		"match host 127.0.0.1");
	assert_int_eq(stub_refresh(&s, true, "localhost", certs.ca_file, 2000, buf, sizeof(buf)),
		AEROSPIKE_OK);

	// EOF framing over TLS needs close_notify; Content-Length framing does not.
	aws_stub_set_tls(&s, certs.good);
	char* instance = aws_xml_instance("i-1", "10.0.0.5", NULL, NULL, NULL);
	const char* instances[] = {instance};
	char* doc = aws_xml_describe(NULL, instances, 1);
	char* eof = aws_strf("HTTP/1.1 200 OK\r\nconnection: close\r\n\r\n%s", doc);
	char* length = aws_strf("HTTP/1.1 200 OK\r\ncontent-length: %zu\r\n\r\n%s", strlen(doc), doc);

	aws_stub_reset(&s);
	aws_reply reply = {.mode = AWS_REPLY_SEND, .data = eof, .size = strlen(eof),
		.no_close_notify = true};
	aws_stub_reply(&s, &reply);
	assert_int_eq(stub_refresh(&s, true, "localhost", certs.ca_file, 2000, buf, sizeof(buf)),
		AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, AWS_FAILED "truncated response");

	aws_stub_reset(&s);
	reply.no_close_notify = false;
	aws_stub_reply(&s, &reply);
	assert_int_eq(stub_refresh(&s, true, "localhost", certs.ca_file, 2000, buf, sizeof(buf)),
		AEROSPIKE_OK);
	assert_string_eq(buf, "10.0.0.5:3000");

	aws_stub_reset(&s);
	reply = (aws_reply){.mode = AWS_REPLY_SEND, .data = length, .size = strlen(length),
		.no_close_notify = true};
	aws_stub_reply(&s, &reply);
	assert_int_eq(stub_refresh(&s, true, "localhost", certs.ca_file, 2000, buf, sizeof(buf)),
		AEROSPIKE_OK);
	assert_string_eq(buf, "10.0.0.5:3000");

	// A server that closes after the request without a reply.
	aws_stub_reset(&s);
	reply = (aws_reply){.mode = AWS_REPLY_CLOSE};
	aws_stub_reply(&s, &reply);
	assert_int_eq(stub_refresh(&s, true, "localhost", certs.ca_file, 2000, buf, sizeof(buf)),
		AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, AWS_FAILED "connection closed without a response");

	free(length);
	free(eof);
	free(doc);
	free(instance);
	free(r);
	aws_stub_stop(&s);
	aws_certs_destroy(&certs);
}

TEST(aws_trust_bundle, "without ca_file or ca_path a system CA bundle is chosen only when SSL_CERT_FILE, SSL_CERT_DIR and the OpenSSL default certificate file are all absent")
{
	aws_certs certs;
	assert_true(aws_certs_create(&certs));
	const char* missing = "/nonexistent/as-aws-cert.pem";
	const char* candidates[] = {missing, certs.dir, certs.ca_file};
	uint32_t candidates_size = sizeof(candidates) / sizeof(candidates[0]);

	// The first candidate that is a regular file: a missing file and a directory are skipped.
	assert_true(as_aws_trust_bundle(NULL, NULL, missing, candidates, candidates_size) ==
		certs.ca_file);
	assert_true(as_aws_trust_bundle("", "", missing, candidates, candidates_size) ==
		certs.ca_file);
	assert_true(as_aws_trust_bundle(NULL, NULL, certs.dir, candidates, candidates_size) ==
		certs.ca_file);

	// SSL_CERT_FILE, SSL_CERT_DIR or an existing default certificate file keeps the defaults.
	assert_null(as_aws_trust_bundle(missing, NULL, missing, candidates, candidates_size));
	assert_null(as_aws_trust_bundle(NULL, certs.dir, missing, candidates, candidates_size));
	assert_null(as_aws_trust_bundle(NULL, NULL, certs.ca_file, candidates, candidates_size));

	// No candidate exists.
	assert_null(as_aws_trust_bundle(NULL, NULL, missing, candidates, 2));
	assert_null(as_aws_trust_bundle(NULL, NULL, NULL, NULL, 0));
	aws_certs_destroy(&certs);
}

//---------------------------------
// Tests: credential chain
//---------------------------------

TEST(aws_cred_config, "D-14 config keys sign DescribeInstances with the injected clock, read no environment and log the config source once")
{
	aws_stub ec2;
	assert_true(aws_stub_start(&ec2, false));
	stub_one(&ec2, "10.0.0.1");
	fake_env f;
	fake_init(&f, 0, 0, 0);
	f.now_ms = CRED_NOW_MS;

	char url[64];
	as_aws_seed_provider_config cfg;
	aws_cfg(&cfg, stub_url(&ec2, false, "127.0.0.1", url, sizeof(url)));
	cfg.session_token = AWS_TOKEN;
	as_seed_provider provider;
	as_error err;
	assert_int_eq(as_discovery_aws_seed_provider_env(&err, &cfg, &f.env, &provider), AEROSPIKE_OK);
	assert_int_eq(f.calls, 0);

	char buf[256];
	log_start();
	as_status first = cred_refresh(&provider, buf, sizeof(buf));
	as_status second = cred_refresh(&provider, buf, sizeof(buf));
	log_stop();
	provider.destroy(provider.udata);
	assert_int_eq(first, AEROSPIKE_OK);
	assert_int_eq(second, AEROSPIKE_OK);
	assert_string_eq(buf, "10.0.0.1:3000");
	assert_true(log_find(AS_LOG_LEVEL_INFO, "AWS EC2 seed provider credentials from config") >= 0);
	assert_int_eq(log_matches("credentials from"), 1);

	char date[64];
	assert_int_eq(request_header(&ec2, 0, "x-amz-date", date, sizeof(date)), 1);
	assert_string_eq(date, CRED_DATE);
	assert_true(signed_with(&ec2, 0, AWS_KEY, AWS_SECRET, AWS_TOKEN));
	assert_true(signed_with(&ec2, 1, AWS_KEY, AWS_SECRET, AWS_TOKEN));
	aws_stub_stop(&ec2);
}

TEST(aws_cred_environment, "D-14 environment keys are snapshotted at construction and sign; a key without a secret is skipped; invalid values fail before any request")
{
	aws_stub ec2;
	assert_true(aws_stub_start(&ec2, false));
	stub_one(&ec2, "10.0.0.1");
	fake_env f;
	fake_init(&f, 0, 0, 0);

	char key[32] = "AKIDENVIRONMENT";
	char secret[32] = "environment/secret";
	fake_set(&f, "AWS_ACCESS_KEY_ID", key);
	fake_set(&f, "AWS_SECRET_ACCESS_KEY", secret);
	fake_set(&f, "AWS_SESSION_TOKEN", "environment-token");

	char buf[256];
	as_seed_provider provider;
	assert_int_eq(cred_provider(&ec2, &f, &provider, buf, sizeof(buf)), AEROSPIKE_OK);

	// Changes after construction are ignored.
	strcpy(key, "CHANGED");
	strcpy(secret, "changed");
	fake_set(&f, "AWS_SESSION_TOKEN", NULL);
	f.calls = 0;

	log_start();
	as_status status = cred_refresh(&provider, buf, sizeof(buf));
	log_stop();
	provider.destroy(provider.udata);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(f.calls, 0);
	assert_true(signed_with(&ec2, 0, "AKIDENVIRONMENT", "environment/secret",
		"environment-token"));
	assert_true(log_find(AS_LOG_LEVEL_INFO, "AWS EC2 seed provider credentials from environment")
		>= 0);

	// A key without a secret is not a source.
	fake_init(&f, 0, 0, 0);
	fake_set(&f, "AWS_ACCESS_KEY_ID", "AKIDENVIRONMENT");
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, CRED_DISABLED);
	fake_set(&f, "AWS_SECRET_ACCESS_KEY", "");
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, CRED_DISABLED);

	fake_set(&f, "AWS_ACCESS_KEY_ID", "AKID-1");
	fake_set(&f, "AWS_SECRET_ACCESS_KEY", "s");
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, "AWS EC2 seed provider: environment returned an invalid access key id");

	fake_set(&f, "AWS_ACCESS_KEY_ID", "AKIDENVIRONMENT");
	fake_set(&f, "AWS_SECRET_ACCESS_KEY", "env secret");
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf,
		"AWS EC2 seed provider: environment returned an invalid secret access key");

	fake_set(&f, "AWS_SECRET_ACCESS_KEY", "s");
	fake_set(&f, "AWS_SESSION_TOKEN", "tok\r\nhost: evil");
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, "AWS EC2 seed provider: environment returned an invalid session token");
	assert_int_eq(aws_stub_requests(&ec2), 1);
	aws_stub_stop(&ec2);
}

TEST(aws_cred_profile, "D-14 the shared credentials file: default and AWS_PROFILE sections, explicit and ~/ paths; missing, FIFO and directory skipped; over 1 MiB and invalid values fail")
{
	aws_stub ec2;
	assert_true(aws_stub_start(&ec2, false));
	stub_one(&ec2, "10.0.0.1");
	tmp_dir d;
	assert_true(tmp_create(&d));
	const char* aws_dir = tmp_mkdir(&d, ".aws");
	tmp_text(&d, ".aws/credentials", JAVA_PROFILE);

	fake_env f;
	fake_init(&f, 0, 0, 0);
	f.env.home = d.path;
	char buf[512];

	log_start();
	as_status status = cred_once(&ec2, &f, buf, sizeof(buf));
	log_stop();
	assert_int_eq(status, AEROSPIKE_OK);
	assert_true(signed_with(&ec2, 0, "AKIADEFAULT", "SECRET", NULL));
	assert_true(log_find(AS_LOG_LEVEL_INFO, "AWS EC2 seed provider credentials from profile") >= 0);

	fake_set(&f, "AWS_PROFILE", "seed");
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_OK);
	assert_true(signed_with(&ec2, 1, "AKIASEEDPROFILE", "SECRET", "TOKEN"));

	// A profile without static keys is not a source.
	fake_set(&f, "AWS_PROFILE", "sso-only");
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, CRED_DISABLED);
	fake_set(&f, "AWS_PROFILE", NULL);

	const char* other = tmp_text(&d, "other", "[default]\naws_access_key_id=AKIAOTHER\n"
		"aws_secret_access_key=OTHER\n");
	fake_set(&f, "AWS_SHARED_CREDENTIALS_FILE", other);
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_OK);
	assert_true(signed_with(&ec2, 2, "AKIAOTHER", "OTHER", NULL));
	fake_set(&f, "AWS_SHARED_CREDENTIALS_FILE", "~/other");
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_OK);
	assert_true(signed_with(&ec2, 3, "AKIAOTHER", "OTHER", NULL));

	// A missing file, a FIFO and a directory are skipped at once.
	const char* fifo = tmp_fifo(&d, "fifo");
	const char* skipped[] = {"~/missing", fifo, aws_dir};

	for (uint32_t i = 0; i < 3; i++) {
		fake_set(&f, "AWS_SHARED_CREDENTIALS_FILE", skipped[i]);
		uint64_t start = cf_getms();
		status = cred_once(&ec2, &f, buf, sizeof(buf));
		uint64_t elapsed = cf_getms() - start;

		if (status != AEROSPIKE_ERR_SEED_DISCOVERY_FAILED || strcmp(buf, CRED_DISABLED) != 0 ||
			elapsed > 1000) {
			fail("%s: %d \"%s\" after %" PRIu64 " ms", skipped[i], status, buf, elapsed);
		}
	}

	// A file over 1 MiB fails the resolution.
	const char* big = tmp_big(&d, "big");
	fake_set(&f, "AWS_SHARED_CREDENTIALS_FILE", big);
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	char expected[300];
	snprintf(expected, sizeof(expected), "AWS EC2 seed provider: failed to read %s: larger than "
		"1 MiB", big);
	assert_string_eq(buf, expected);

	// Without a home there is no default path and no ~/ expansion.
	f.env.home = NULL;
	fake_set(&f, "AWS_SHARED_CREDENTIALS_FILE", NULL);
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, CRED_DISABLED);
	fake_set(&f, "AWS_SHARED_CREDENTIALS_FILE", "~/other");
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, CRED_DISABLED);

	// Invalid values. A CR ends the line, as in Java, so it cannot reach the token.
	const char* bad = tmp_text(&d, "bad", "[default]\naws_access_key_id=AKIABAD\n"
		"aws_secret_access_key=S\naws_session_token=TO\tKEN\n");
	fake_set(&f, "AWS_SHARED_CREDENTIALS_FILE", bad);
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, "AWS EC2 seed provider: shared credentials file returned an invalid "
		"session token");

	const char* bad_key = tmp_text(&d, "bad-key", "[default]\naws_access_key_id=AKIA/BAD\n"
		"aws_secret_access_key=S\n");
	fake_set(&f, "AWS_SHARED_CREDENTIALS_FILE", bad_key);
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, "AWS EC2 seed provider: shared credentials file returned an invalid "
		"access key id");

	const char* cr = tmp_text(&d, "cr", "[default]\r\naws_access_key_id=AKIACR\r"
		"aws_secret_access_key=S\naws_session_token=TO\rKEN\n");
	fake_set(&f, "AWS_SHARED_CREDENTIALS_FILE", cr);
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_OK);
	assert_true(signed_with(&ec2, 4, "AKIACR", "S", "TO"));
	assert_int_eq(aws_stub_requests(&ec2), 5);

	tmp_destroy(&d);
	aws_stub_stop(&ec2);
}

TEST(aws_cred_web_identity, "D-14 web identity: unsigned STS form POST with the trimmed token; temporary credentials sign; STS errors redact the token; token file failures never fall through")
{
	aws_stub ec2;
	aws_stub sts;
	aws_stub ecs;
	aws_stub imds;
	assert_true(aws_stub_start(&ec2, false));
	assert_true(aws_stub_start(&sts, false));
	assert_true(aws_stub_start(&ecs, false));
	assert_true(aws_stub_start(&imds, false));
	stub_one(&ec2, "10.0.0.1");
	tmp_dir d;
	assert_true(tmp_create(&d));
	const char* token_file = tmp_text(&d, "token", WEB_TOKEN "\n");

	char* xml = aws_xml_sts(CRED_KEY, CRED_SECRET, CRED_TOKEN, CRED_EXPIRATION);
	aws_stub_http(&sts, 200, NULL, xml);

	fake_env f;
	fake_init(&f, sts.port, 0, 0);
	f.now_ms = CRED_NOW_MS;
	fake_set(&f, "AWS_WEB_IDENTITY_TOKEN_FILE", token_file);
	fake_set(&f, "AWS_ROLE_ARN", ROLE_ARN);
	fake_set(&f, "AWS_ROLE_SESSION_NAME", "seed session");

	char buf[512];
	log_start();
	as_status status = cred_once(&ec2, &f, buf, sizeof(buf));
	log_stop();
	assert_int_eq(status, AEROSPIKE_OK);
	assert_true(signed_with(&ec2, 0, CRED_KEY, CRED_SECRET, CRED_TOKEN));
	assert_true(log_find(AS_LOG_LEVEL_INFO, "AWS EC2 seed provider credentials from web-identity")
		>= 0);

	aws_request* r = cred_request(&sts, 0);
	assert_not_null(r);
	assert_string_eq(r->method, "POST");
	assert_string_eq(r->target, "/sts");
	assert_string_eq(r->body, "Action=AssumeRoleWithWebIdentity&Version=2011-06-15"
		"&RoleArn=arn%3Aaws%3Aiam%3A%3A123456789012%3Arole%2Fseed-role"
		"&RoleSessionName=seed%20session&WebIdentityToken=" WEB_TOKEN);
	char value[256];
	assert_int_eq(aws_request_header(r, "authorization", NULL, 0), 0);
	assert_int_eq(aws_request_header(r, "content-type", value, sizeof(value)), 1);
	assert_string_eq(value, AWS_FORM);
	free(r);

	// The default session name comes from the injected clock.
	fake_set(&f, "AWS_ROLE_SESSION_NAME", NULL);
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_OK);
	r = cred_request(&sts, 1);
	assert_not_null(r);
	assert_not_null(strstr(r->body, "&RoleSessionName=aerospike-client-1440938160000&"));
	free(r);

	// Java's STS error test: code, message and request id, with the token redacted.
	char* error = aws_strf("<ErrorResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">"
		"<Error><Type>Sender</Type><Code>InvalidIdentityToken</Code><Message>Token %s is invalid"
		"</Message></Error><RequestId>c6104cbe-af31-11e0-8154-cbc7ccf896c7</RequestId>"
		"</ErrorResponse>", WEB_TOKEN);
	aws_stub_set_http(&sts, 400, NULL, error);
	free(error);
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, "AWS EC2 seed provider: STS AssumeRoleWithWebIdentity failed: HTTP 400 "
		"InvalidIdentityToken: Token <redacted> is invalid (request id "
		"c6104cbe-af31-11e0-8154-cbc7ccf896c7)");

	static const struct {
		const char* body;
		const char* message;
	} bad_bodies[] = {
		{"<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult/>"
			"</AssumeRoleWithWebIdentityResponse>",
			"AWS EC2 seed provider: STS AssumeRoleWithWebIdentity response has no credentials"},
		{"not xml", "AWS EC2 seed provider: invalid STS AssumeRoleWithWebIdentity response"},
		{"", "AWS EC2 seed provider: invalid STS AssumeRoleWithWebIdentity response"}
	};

	for (uint32_t i = 0; i < 3; i++) {
		aws_stub_set_http(&sts, 200, NULL, bad_bodies[i].body);
		assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
		assert_string_eq(buf, bad_bodies[i].message);
	}

	// A CR in the STS session token fails before DescribeInstances; so does a bad expiration.
	char* bad = aws_xml_sts(CRED_KEY, CRED_SECRET, "tok&#13;en", CRED_EXPIRATION);
	aws_stub_set_http(&sts, 200, NULL, bad);
	free(bad);
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, "AWS EC2 seed provider: STS AssumeRoleWithWebIdentity returned an "
		"invalid session token");
	bad = aws_xml_sts(CRED_KEY, CRED_SECRET, CRED_TOKEN, "2015-08-30T13:36:00");
	aws_stub_set_http(&sts, 200, NULL, bad);
	free(bad);
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, "AWS EC2 seed provider: STS AssumeRoleWithWebIdentity returned an "
		"invalid expiration");
	assert_int_eq(aws_stub_requests(&ec2), 2);

	// Token file failures fail the load: no request to STS, and no fall-through to ECS or IMDS.
	aws_stub_reset(&sts);
	aws_stub_http(&sts, 200, NULL, xml);
	fake_init(&f, sts.port, ecs.port, imds.port);
	fake_set(&f, "AWS_ROLE_ARN", ROLE_ARN);
	fake_set(&f, "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/creds");

	char missing[128];
	snprintf(missing, sizeof(missing), "%s/missing", d.path);
	const char* files[] = {
		missing, tmp_fifo(&d, "fifo"), tmp_big(&d, "big"), tmp_text(&d, "space", "eyJ bad\n"),
		tmp_text(&d, "empty", " \n"), tmp_file(&d, "nul", "eyJ\0x", 5)
	};
	const char* messages[] = {
		"AWS EC2 seed provider: failed to read AWS_WEB_IDENTITY_TOKEN_FILE: No such file or "
			"directory",
		"AWS EC2 seed provider: failed to read AWS_WEB_IDENTITY_TOKEN_FILE: not a regular file",
		"AWS EC2 seed provider: failed to read AWS_WEB_IDENTITY_TOKEN_FILE: larger than 1 MiB",
		"AWS EC2 seed provider: STS AssumeRoleWithWebIdentity failed: invalid web identity token",
		"AWS EC2 seed provider: STS AssumeRoleWithWebIdentity failed: invalid web identity token",
		"AWS EC2 seed provider: STS AssumeRoleWithWebIdentity failed: invalid web identity token"
	};

	for (uint32_t i = 0; i < 6; i++) {
		fake_set(&f, "AWS_WEB_IDENTITY_TOKEN_FILE", files[i]);
		uint64_t start = cf_getms();
		status = cred_once(&ec2, &f, buf, sizeof(buf));
		uint64_t elapsed = cf_getms() - start;

		if (status != AEROSPIKE_ERR_SEED_DISCOVERY_FAILED || strcmp(buf, messages[i]) != 0 ||
			elapsed > 1000) {
			fail("token file %u: %d \"%s\" after %" PRIu64 " ms", i, status, buf, elapsed);
		}
	}
	assert_int_eq(aws_stub_requests(&sts), 0);
	assert_int_eq(aws_stub_accepts(&ecs), 0);
	assert_int_eq(aws_stub_accepts(&imds), 0);
	assert_int_eq(aws_stub_requests(&ec2), 2);

	free(xml);
	tmp_destroy(&d);
	aws_stub_stop(&imds);
	aws_stub_stop(&ecs);
	aws_stub_stop(&sts);
	aws_stub_stop(&ec2);
}

TEST(aws_cred_ecs, "D-14 ECS: relative and full URIs with query and [::1], token file preferred over the variable, invalid URLs and untrusted values fail at load")
{
	aws_stub ec2;
	aws_stub ecs;
	assert_true(aws_stub_start(&ec2, false));
	assert_true(aws_stub_start(&ecs, false));
	stub_one(&ec2, "10.0.0.1");
	tmp_dir d;
	assert_true(tmp_create(&d));
	char* json = aws_json_credentials(CRED_KEY, CRED_SECRET, CRED_TOKEN, CRED_EXPIRATION);
	aws_stub_http(&ecs, 200, NULL, json);

	fake_env f;
	fake_init(&f, 0, ecs.port, 0);
	fake_set(&f, "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/credentials/abc?id=1");
	fake_set(&f, "AWS_CONTAINER_AUTHORIZATION_TOKEN", ECS_AUTHORIZATION);

	char buf[512];
	char value[256];
	log_start();
	as_status status = cred_once(&ec2, &f, buf, sizeof(buf));
	log_stop();
	assert_int_eq(status, AEROSPIKE_OK);
	assert_true(signed_with(&ec2, 0, CRED_KEY, CRED_SECRET, CRED_TOKEN));
	assert_true(log_find(AS_LOG_LEVEL_INFO, "AWS EC2 seed provider credentials from ecs") >= 0);
	assert_true(request_is(&ecs, 0, "GET", "/ecs/v2/credentials/abc?id=1"));
	assert_int_eq(request_header(&ecs, 0, "authorization", value, sizeof(value)), 1);
	assert_string_eq(value, ECS_AUTHORIZATION);
	assert_int_eq(request_header(&ecs, 0, "content-length", NULL, 0), 0);

	// A full URI with a query and a token file, which wins over the variable.
	char full[96];
	snprintf(full, sizeof(full), "http://127.0.0.1:%u/full?x=1", ecs.port);
	fake_init(&f, 0, 0, 0);
	fake_set(&f, "AWS_CONTAINER_CREDENTIALS_FULL_URI", full);
	fake_set(&f, "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
		tmp_text(&d, "ecs-token", ECS_AUTHORIZATION "-file\n"));
	fake_set(&f, "AWS_CONTAINER_AUTHORIZATION_TOKEN", "ignored");
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_OK);
	assert_true(request_is(&ecs, 1, "GET", "/full?x=1"));
	assert_int_eq(request_header(&ecs, 1, "authorization", value, sizeof(value)), 1);
	assert_string_eq(value, ECS_AUTHORIZATION "-file");

	// A token with a scheme keeps its inner space, as in Java and the AWS SDKs.
	fake_set(&f, "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", NULL);
	fake_set(&f, "AWS_CONTAINER_AUTHORIZATION_TOKEN", " Bearer " ECS_AUTHORIZATION " ");
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_OK);
	assert_int_eq(request_header(&ecs, 2, "authorization", value, sizeof(value)), 1);
	assert_string_eq(value, "Bearer " ECS_AUTHORIZATION);

	// A token variable of only spaces sends no authorization.
	fake_set(&f, "AWS_CONTAINER_AUTHORIZATION_TOKEN", "   ");
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_OK);
	assert_int_eq(request_header(&ecs, 3, "authorization", NULL, 0), 0);

	aws_stub ecs6;

	if (aws_stub_start(&ecs6, true)) {
		aws_stub_http(&ecs6, 200, NULL, json);
		snprintf(full, sizeof(full), "http://[::1]:%u/creds", ecs6.port);
		fake_set(&f, "AWS_CONTAINER_CREDENTIALS_FULL_URI", full);
		assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_OK);
		assert_int_eq(aws_stub_requests(&ecs6), 1);
		aws_stub_stop(&ecs6);
	}
	else {
		info("skipped the [::1] full URI: no IPv6 loopback");
	}
	snprintf(full, sizeof(full), "http://127.0.0.1:%u/creds", ecs.port);
	fake_set(&f, "AWS_CONTAINER_CREDENTIALS_FULL_URI", full);
	fake_set(&f, "AWS_CONTAINER_AUTHORIZATION_TOKEN", NULL);

	aws_stub_set_http(&ecs, 401, NULL, "unauthorized");
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, "AWS EC2 seed provider: ECS credentials failed: HTTP 401");

	// Invalid URLs do not fail construction; they fail the ECS load.
	uint32_t ecs_requests = aws_stub_requests(&ecs);
	fake_set(&f, "AWS_CONTAINER_CREDENTIALS_FULL_URI", "not a url");
	fake_set(&f, "AWS_ACCESS_KEY_ID", "AKIDENVIRONMENT");
	fake_set(&f, "AWS_SECRET_ACCESS_KEY", "environment/secret");
	uint32_t ec2_requests = aws_stub_requests(&ec2);
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_OK);
	assert_true(signed_with(&ec2, ec2_requests, "AKIDENVIRONMENT", "environment/secret", NULL));
	fake_set(&f, "AWS_ACCESS_KEY_ID", NULL);
	fake_set(&f, "AWS_SECRET_ACCESS_KEY", NULL);
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, "AWS EC2 seed provider: ECS credentials failed: invalid URL");

	fake_init(&f, 0, ecs.port, 0);
	fake_set(&f, "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", ".example.com/creds");
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, "AWS EC2 seed provider: ECS credentials failed: invalid URL");
	assert_int_eq(aws_stub_requests(&ecs), ecs_requests);

	// Untrusted tokens fail before the request.
	aws_stub_set_http(&ecs, 200, NULL, json);
	fake_set(&f, "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/creds");
	fake_set(&f, "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", tmp_text(&d, "cr-token", "abc\rdef\n"));
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, "AWS EC2 seed provider: ECS credentials failed: invalid authorization "
		"token");
	fake_set(&f, "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", NULL);
	fake_set(&f, "AWS_CONTAINER_AUTHORIZATION_TOKEN", "abc\r\nx-injected: 1");
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, "AWS EC2 seed provider: ECS credentials failed: invalid authorization "
		"token");
	fake_set(&f, "AWS_CONTAINER_AUTHORIZATION_TOKEN", "abc\tdef");
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, "AWS EC2 seed provider: ECS credentials failed: invalid authorization "
		"token");
	fake_set(&f, "AWS_CONTAINER_AUTHORIZATION_TOKEN", "abc\xc3\xa9");
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, "AWS EC2 seed provider: ECS credentials failed: invalid authorization "
		"token");
	char missing[128];
	snprintf(missing, sizeof(missing), "%s/missing", d.path);
	fake_set(&f, "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", missing);
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, "AWS EC2 seed provider: failed to read "
		"AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE: No such file or directory");
	assert_int_eq(aws_stub_requests(&ecs), ecs_requests);
	fake_set(&f, "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", NULL);
	fake_set(&f, "AWS_CONTAINER_AUTHORIZATION_TOKEN", NULL);

	// Untrusted or incomplete responses fail before DescribeInstances.
	char* bad_key = aws_json_credentials("AKID-BAD", CRED_SECRET, CRED_TOKEN, CRED_EXPIRATION);
	char* no_expiration = aws_json_credentials(CRED_KEY, CRED_SECRET, CRED_TOKEN, NULL);
	char* no_secret = aws_json_credentials(CRED_KEY, NULL, CRED_TOKEN, CRED_EXPIRATION);
	char* epoch = aws_json_credentials(CRED_KEY, CRED_SECRET, CRED_TOKEN, "1970-01-01T00:00:00Z");
	char* cr_token = aws_json_credentials(CRED_KEY, CRED_SECRET, "tok\\r\\nen", CRED_EXPIRATION);
	char* bad_secret = aws_json_credentials(CRED_KEY, "sec ret", CRED_TOKEN, CRED_EXPIRATION);
	const char* bodies[] = {bad_key, no_expiration, no_secret, epoch, cr_token, "not json", "",
		bad_secret};
	const char* messages[] = {
		"AWS EC2 seed provider: ECS credentials returned an invalid access key id",
		"AWS EC2 seed provider: ECS credentials response has no credentials",
		"AWS EC2 seed provider: ECS credentials response has no credentials",
		"AWS EC2 seed provider: ECS credentials returned an invalid expiration",
		"AWS EC2 seed provider: ECS credentials returned an invalid session token",
		"AWS EC2 seed provider: invalid ECS credentials response",
		"AWS EC2 seed provider: invalid ECS credentials response",
		"AWS EC2 seed provider: ECS credentials returned an invalid secret access key"
	};
	ec2_requests = aws_stub_requests(&ec2);

	for (uint32_t i = 0; i < 8; i++) {
		aws_stub_set_http(&ecs, 200, NULL, bodies[i]);
		status = cred_once(&ec2, &f, buf, sizeof(buf));

		if (status != AEROSPIKE_ERR_SEED_DISCOVERY_FAILED || strcmp(buf, messages[i]) != 0) {
			fail("body %u: %d \"%s\"", i, status, buf);
		}
	}
	assert_int_eq(aws_stub_requests(&ec2), ec2_requests);

	free(bad_secret);
	free(cr_token);
	free(epoch);
	free(no_secret);
	free(no_expiration);
	free(bad_key);
	free(json);
	tmp_destroy(&d);
	aws_stub_stop(&ecs);
	aws_stub_stop(&ec2);
}

// Refresh the provider and return the message, or the hosts on success.
static const char*
cred_step(as_seed_provider* provider, char* buf, size_t size)
{
	cred_refresh(provider, buf, size);
	return buf;
}

TEST(aws_cred_imds, "D-14 IMDSv2: token PUT, cached role, credentials GET; a 404 drops the role; empty, invalid and failed responses fail cleanly; disabled case-insensitively")
{
	aws_stub ec2;
	aws_stub imds;
	assert_true(aws_stub_start(&ec2, false));
	assert_true(aws_stub_start(&imds, false));
	stub_one(&ec2, "10.0.0.1");
	char* json = aws_json_credentials(CRED_KEY, CRED_SECRET, CRED_TOKEN, CRED_EXPIRATION);
	const char* role_path = IMDS_ROLES_PATH AWS_STUB_IMDS_ROLE;

	fake_env f;
	fake_init(&f, 0, 0, imds.port);
	f.now_ms = CRED_NOW_MS;
	aws_stub_imds(&imds, AWS_STUB_IMDS_ROLE "\nsecond-role\n", json);
	aws_stub_imds(&imds, NULL, json);

	char buf[512];
	char value[256];
	as_seed_provider provider;
	assert_int_eq(cred_provider(&ec2, &f, &provider, buf, sizeof(buf)), AEROSPIKE_OK);
	log_start();
	as_status status = cred_refresh(&provider, buf, sizeof(buf));
	log_stop();
	assert_int_eq(status, AEROSPIKE_OK);
	assert_true(signed_with(&ec2, 0, CRED_KEY, CRED_SECRET, CRED_TOKEN));
	assert_true(log_find(AS_LOG_LEVEL_INFO, "AWS EC2 seed provider credentials from imds") >= 0);

	assert_true(request_is(&imds, 0, "PUT", "/latest/api/token"));
	assert_int_eq(request_header(&imds, 0, "x-aws-ec2-metadata-token-ttl-seconds", value,
		sizeof(value)), 1);
	assert_string_eq(value, "21600");
	assert_int_eq(request_header(&imds, 0, "content-length", value, sizeof(value)), 1);
	assert_string_eq(value, "0");
	assert_int_eq(request_header(&imds, 0, "x-aws-ec2-metadata-token", NULL, 0), 0);
	assert_true(request_is(&imds, 1, "GET", IMDS_ROLES_PATH));
	assert_true(request_is(&imds, 2, "GET", role_path));

	for (uint32_t i = 1; i <= 2; i++) {
		assert_int_eq(request_header(&imds, i, "x-aws-ec2-metadata-token", value, sizeof(value)),
			1);
		assert_string_eq(value, AWS_STUB_IMDS_TOKEN);
		assert_int_eq(request_header(&imds, i, "content-length", NULL, 0), 0);
	}

	// The role is cached: the next load sends the token PUT and the credentials GET only.
	f.now_ms = CRED_EXPIRATION_MS - 300000;
	assert_int_eq(cred_refresh(&provider, buf, sizeof(buf)), AEROSPIKE_OK);
	assert_int_eq(aws_stub_requests(&imds), 5);
	assert_true(request_is(&imds, 3, "PUT", "/latest/api/token"));
	assert_true(request_is(&imds, 4, "GET", role_path));
	provider.destroy(provider.udata);

	// A 404 on the first resolution: the combined message; the next load lists the role again.
	aws_stub_reset(&imds);
	f.now_ms = CRED_NOW_MS;
	aws_stub_http(&imds, 200, NULL, AWS_STUB_IMDS_TOKEN);
	aws_stub_http(&imds, 200, NULL, AWS_STUB_IMDS_ROLE);
	aws_stub_http(&imds, 404, NULL, "");
	aws_stub_imds(&imds, AWS_STUB_IMDS_ROLE, json);
	assert_int_eq(cred_provider(&ec2, &f, &provider, buf, sizeof(buf)), AEROSPIKE_OK);
	assert_string_eq(cred_step(&provider, buf, sizeof(buf)),
		CRED_NO_SOURCE "IMDS credentials failed: HTTP 404");
	assert_string_eq(cred_step(&provider, buf, sizeof(buf)), "10.0.0.1:3000");
	assert_int_eq(aws_stub_requests(&imds), 6);
	assert_true(request_is(&imds, 4, "GET", IMDS_ROLES_PATH));
	provider.destroy(provider.udata);

	// A 404 on a reload after expiry: the plain message; the next load lists the role again.
	aws_stub_reset(&imds);
	aws_stub_imds(&imds, AWS_STUB_IMDS_ROLE, json);
	aws_stub_http(&imds, 200, NULL, AWS_STUB_IMDS_TOKEN);
	aws_stub_http(&imds, 404, NULL, "");
	aws_stub_imds(&imds, AWS_STUB_IMDS_ROLE, json);
	assert_int_eq(cred_provider(&ec2, &f, &provider, buf, sizeof(buf)), AEROSPIKE_OK);
	assert_string_eq(cred_step(&provider, buf, sizeof(buf)), "10.0.0.1:3000");
	f.now_ms = CRED_EXPIRATION_MS;
	assert_string_eq(cred_step(&provider, buf, sizeof(buf)),
		"AWS EC2 seed provider: IMDS credentials failed: HTTP 404");
	assert_string_eq(cred_step(&provider, buf, sizeof(buf)), "10.0.0.1:3000");
	assert_int_eq(aws_stub_requests(&imds), 8);
	assert_true(request_is(&imds, 4, "GET", role_path));
	assert_true(request_is(&imds, 6, "GET", IMDS_ROLES_PATH));
	provider.destroy(provider.udata);
	f.now_ms = CRED_NOW_MS;

	// Failures of one load, each with the number of IMDS requests it makes.
	char* failure = aws_strf("{\"Code\":\"AssumeRoleUnauthorizedAccess\",\"AccessKeyId\":\"%s\","
		"\"SecretAccessKey\":\"%s\",\"Expiration\":\"%s\"}", CRED_KEY, CRED_SECRET,
		CRED_EXPIRATION);
	char* cr_token = aws_json_credentials(CRED_KEY, CRED_SECRET, "tok\\ren", CRED_EXPIRATION);
	static const struct {
		int token_status;
		const char* token;
		int role_status;
		const char* role;
		uint32_t requests;
		const char* message;
	} cases[] = {
		{403, "", 0, NULL, 1, "IMDS token failed: HTTP 403"},
		{200, "", 0, NULL, 1, "IMDS token returned an invalid token"},
		{200, " \n", 0, NULL, 1, "IMDS token returned an invalid token"},
		{200, "tok en", 0, NULL, 1, "IMDS token returned an invalid token"},
		{200, AWS_STUB_IMDS_TOKEN, 404, "", 2, "IMDS role failed: HTTP 404 (no instance profile "
			"attached?)"},
		{200, AWS_STUB_IMDS_TOKEN, 200, "", 2, "IMDS returned no instance profile role"},
		{200, AWS_STUB_IMDS_TOKEN, 200, " \r\n", 2, "IMDS returned no instance profile role"},
		{200, AWS_STUB_IMDS_TOKEN, 200, "a/b", 2, "IMDS role returned an invalid role name"},
		{200, AWS_STUB_IMDS_TOKEN, 200, ".", 2, "IMDS role returned an invalid role name"},
		{200, AWS_STUB_IMDS_TOKEN, 200, "..", 2, "IMDS role returned an invalid role name"},
		{200, AWS_STUB_IMDS_TOKEN, 200, "a?b", 2, "IMDS role returned an invalid role name"},
		{200, AWS_STUB_IMDS_TOKEN, 200, AWS_STUB_IMDS_ROLE, 3, NULL},
		{200, AWS_STUB_IMDS_TOKEN, 200, AWS_STUB_IMDS_ROLE, 3, NULL}
	};
	const char* last_bodies[] = {failure, cr_token};
	const char* last_messages[] = {
		"IMDS credentials failed: AssumeRoleUnauthorizedAccess",
		"IMDS credentials returned an invalid session token"
	};
	uint32_t ec2_requests = aws_stub_requests(&ec2);

	for (uint32_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
		aws_stub_reset(&imds);
		aws_stub_http(&imds, cases[i].token_status, NULL, cases[i].token);

		if (cases[i].role) {
			aws_stub_http(&imds, cases[i].role_status, NULL, cases[i].role);
		}

		const char* message = cases[i].message;

		if (!message) {
			uint32_t last = i - (sizeof(cases) / sizeof(cases[0]) - 2);
			aws_stub_http(&imds, 200, NULL, last_bodies[last]);
			message = last_messages[last];
		}

		char expected[300];
		snprintf(expected, sizeof(expected), CRED_NO_SOURCE "%s", message);
		status = cred_once(&ec2, &f, buf, sizeof(buf));

		if (status != AEROSPIKE_ERR_SEED_DISCOVERY_FAILED || strcmp(buf, expected) != 0 ||
			aws_stub_requests(&imds) != cases[i].requests) {
			fail("case %u: %d \"%s\" after %u requests", i, status, buf,
				aws_stub_requests(&imds));
		}
	}
	assert_int_eq(aws_stub_requests(&ec2), ec2_requests);

	// A missing Code is accepted.
	aws_stub_reset(&imds);
	char* no_code = aws_strf("{\"AccessKeyId\":\"AKIDNOCODE\",\"SecretAccessKey\":\"%s\","
		"\"Expiration\":\"%s\"}", CRED_SECRET, CRED_EXPIRATION);
	aws_stub_imds(&imds, AWS_STUB_IMDS_ROLE, no_code);
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_OK);
	assert_true(signed_with(&ec2, ec2_requests, "AKIDNOCODE", CRED_SECRET, NULL));

	// Disabled, case-insensitively, before any request; any other value leaves it enabled.
	aws_stub_reset(&imds);
	fake_set(&f, "AWS_EC2_METADATA_DISABLED", "TRUE");
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, CRED_DISABLED);
	assert_int_eq(aws_stub_requests(&imds), 0);
	aws_stub_imds(&imds, AWS_STUB_IMDS_ROLE, json);
	fake_set(&f, "AWS_EC2_METADATA_DISABLED", "false");
	assert_int_eq(cred_once(&ec2, &f, buf, sizeof(buf)), AEROSPIKE_OK);

	free(no_code);
	free(cr_token);
	free(failure);
	free(json);
	aws_stub_stop(&imds);
	aws_stub_stop(&ec2);
}

TEST(aws_cred_order, "the chain resolves in Java's order: environment, profile, web identity, ECS, IMDS; a higher source wins over running lower ones")
{
	aws_stub ec2;
	aws_stub sts;
	aws_stub ecs;
	aws_stub imds;
	assert_true(aws_stub_start(&ec2, false));
	assert_true(aws_stub_start(&sts, false));
	assert_true(aws_stub_start(&ecs, false));
	assert_true(aws_stub_start(&imds, false));
	stub_one(&ec2, "10.0.0.1");

	char* xml = aws_xml_sts("AKIDSTS", CRED_SECRET, CRED_TOKEN, CRED_EXPIRATION);
	aws_stub_http(&sts, 200, NULL, xml);
	char* ecs_json = aws_json_credentials("AKIDECS", CRED_SECRET, CRED_TOKEN, CRED_EXPIRATION);
	aws_stub_http(&ecs, 200, NULL, ecs_json);
	char* imds_json = aws_json_credentials("AKIDIMDS", CRED_SECRET, CRED_TOKEN, CRED_EXPIRATION);
	aws_stub_imds(&imds, AWS_STUB_IMDS_ROLE, imds_json);

	tmp_dir d;
	assert_true(tmp_create(&d));
	tmp_mkdir(&d, ".aws");
	tmp_text(&d, ".aws/credentials", "[default]\naws_access_key_id=AKIAPROFILE\n"
		"aws_secret_access_key=profile-secret\n");
	const char* token_file = tmp_text(&d, "token", WEB_TOKEN);

	static const struct {
		const char* key;
		const char* secret;
		const char* token;
		const char* label;
	} expect[] = {
		{"AKIDENV", "env-secret", NULL, "environment"},
		{"AKIAPROFILE", "profile-secret", NULL, "profile"},
		{"AKIDSTS", CRED_SECRET, CRED_TOKEN, "web-identity"},
		{"AKIDECS", CRED_SECRET, CRED_TOKEN, "ecs"},
		{"AKIDIMDS", CRED_SECRET, CRED_TOKEN, "imds"}
	};

	for (uint32_t step = 0; step < 5; step++) {
		fake_env f;
		fake_init(&f, sts.port, ecs.port, imds.port);

		if (step == 0) {
			fake_set(&f, "AWS_ACCESS_KEY_ID", "AKIDENV");
			fake_set(&f, "AWS_SECRET_ACCESS_KEY", "env-secret");
		}

		if (step <= 1) {
			f.env.home = d.path;
		}

		if (step <= 2) {
			fake_set(&f, "AWS_WEB_IDENTITY_TOKEN_FILE", token_file);
			fake_set(&f, "AWS_ROLE_ARN", ROLE_ARN);
		}

		if (step <= 3) {
			fake_set(&f, "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/creds");
		}

		char buf[512];
		char label[128];
		snprintf(label, sizeof(label), "AWS EC2 seed provider credentials from %s",
			expect[step].label);
		log_start();
		as_status status = cred_once(&ec2, &f, buf, sizeof(buf));
		log_stop();

		if (status != AEROSPIKE_OK || log_find(AS_LOG_LEVEL_INFO, label) < 0 ||
			!signed_with(&ec2, step, expect[step].key, expect[step].secret, expect[step].token)) {
			fail("step %u (%s): %d \"%s\"", step, expect[step].label, status, buf);
		}
	}
	assert_int_eq(aws_stub_requests(&sts), 1);
	assert_int_eq(aws_stub_requests(&ecs), 1);
	assert_int_eq(aws_stub_requests(&imds), 3);

	tmp_destroy(&d);
	free(imds_json);
	free(ecs_json);
	free(xml);
	aws_stub_stop(&imds);
	aws_stub_stop(&ecs);
	aws_stub_stop(&sts);
	aws_stub_stop(&ec2);
}

TEST(aws_cred_cache, "credentials refresh five minutes before expiry, a failed reload keeps unexpired ones with a warning, an expired cache propagates the error; no underflow")
{
	aws_stub ec2;
	aws_stub ecs;
	assert_true(aws_stub_start(&ec2, false));
	assert_true(aws_stub_start(&ecs, false));
	stub_one(&ec2, "10.0.0.1");
	char* json = aws_json_credentials(CRED_KEY, CRED_SECRET, CRED_TOKEN, CRED_EXPIRATION);
	aws_stub_http(&ecs, 200, NULL, json);

	fake_env f;
	fake_init(&f, 0, ecs.port, 0);
	fake_set(&f, "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/creds");
	f.now_ms = CRED_NOW_MS;

	char buf[512];
	as_seed_provider provider;
	assert_int_eq(cred_provider(&ec2, &f, &provider, buf, sizeof(buf)), AEROSPIKE_OK);
	assert_int_eq(cred_refresh(&provider, buf, sizeof(buf)), AEROSPIKE_OK);
	assert_int_eq(aws_stub_requests(&ecs), 1);

	f.now_ms = CRED_EXPIRATION_MS - 300000 - 1;
	assert_int_eq(cred_refresh(&provider, buf, sizeof(buf)), AEROSPIKE_OK);
	assert_int_eq(aws_stub_requests(&ecs), 1);

	f.now_ms = CRED_EXPIRATION_MS - 300000;
	assert_int_eq(cred_refresh(&provider, buf, sizeof(buf)), AEROSPIKE_OK);
	assert_int_eq(aws_stub_requests(&ecs), 2);

	// A failed reload while the cached credentials are unexpired: warning and reuse.
	aws_stub_set_http(&ecs, 500, NULL, "boom");
	f.now_ms = CRED_EXPIRATION_MS - 1000;
	log_start();
	as_status status = cred_refresh(&provider, buf, sizeof(buf));
	log_stop();
	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(aws_stub_requests(&ecs), 3);
	assert_true(log_find(AS_LOG_LEVEL_WARN, "AWS EC2 seed provider: credentials refresh from ecs "
		"failed, using cached credentials until they expire: AWS EC2 seed provider: ECS "
		"credentials failed: HTTP 500") >= 0);
	assert_true(signed_with(&ec2, aws_stub_requests(&ec2) - 1, CRED_KEY, CRED_SECRET,
		CRED_TOKEN));

	// Once expired, the error propagates.
	f.now_ms = CRED_EXPIRATION_MS;
	assert_int_eq(cred_refresh(&provider, buf, sizeof(buf)), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, "AWS EC2 seed provider: ECS credentials failed: HTTP 500");
	provider.destroy(provider.udata);

	// A hook expiry 1 ms after the epoch is reloaded at the next refresh.
	char url[64];
	as_aws_seed_provider_config cfg;
	hook_state h;
	cred_cfg(&cfg, &ec2, url, sizeof(url));
	hook_init(&h, &cfg);
	h.expiration_ms = 1;
	as_error err;
	assert_int_eq(as_discovery_aws_seed_provider_env(&err, &cfg, &f.env, &provider), AEROSPIKE_OK);
	assert_int_eq(cred_refresh(&provider, buf, sizeof(buf)), AEROSPIKE_OK);
	assert_int_eq(cred_refresh(&provider, buf, sizeof(buf)), AEROSPIKE_OK);
	assert_int_eq(h.gets, 2);
	provider.destroy(provider.udata);
	assert_int_eq(h.destroys, 1);

	free(json);
	aws_stub_stop(&ecs);
	aws_stub_stop(&ec2);
}

static as_status
hook_provider(aws_stub* ec2, fake_env* f, hook_state* h, uint32_t timeout_ms,
	as_seed_provider* provider)
{
	char url[64];
	as_aws_seed_provider_config cfg;
	cred_cfg(&cfg, ec2, url, sizeof(url));
	cfg.timeout_ms = timeout_ms;
	hook_init(h, &cfg);
	as_error err;
	return as_discovery_aws_seed_provider_env(&err, &cfg, &f->env, provider);
}

TEST(aws_cred_hook, "D-14 the hook replaces the chain, gets the remaining budget, caches by expiry, reports redacted errors and fails a late return while caching its result")
{
	aws_stub ec2;
	aws_stub imds;
	assert_true(aws_stub_start(&ec2, false));
	assert_true(aws_stub_start(&imds, false));
	stub_one(&ec2, "10.0.0.1");

	fake_env f;
	fake_init(&f, 0, 0, imds.port);
	f.now_ms = CRED_NOW_MS;

	// A first expiry within the margin, then 60 minutes: two calls in three refreshes.
	hook_state h;
	as_seed_provider provider;
	char buf[512];
	assert_int_eq(hook_provider(&ec2, &f, &h, 1000, &provider), AEROSPIKE_OK);
	assert_int_eq(f.calls, 0);
	h.expiration_ms = CRED_NOW_MS + 60000;
	h.later_expiration_ms = CRED_NOW_MS + 3600000;
	log_start();

	for (uint32_t i = 0; i < 3; i++) {
		if (cred_refresh(&provider, buf, sizeof(buf)) != AEROSPIKE_OK) {
			log_stop();
			fail("refresh %u: %s", i, buf);
		}
	}
	log_stop();
	assert_int_eq(h.gets, 2);
	assert_int_eq(log_matches("credentials from"), 1);
	assert_true(log_find(AS_LOG_LEVEL_INFO, "AWS EC2 seed provider credentials from hook") >= 0);
	assert_true(h.last_timeout_ms <= 1000 && h.last_timeout_ms + 50 >= 1000);
	assert_int_eq(aws_stub_accepts(&imds), 0);

	for (uint32_t i = 0; i < 3; i++) {
		assert_true(signed_with(&ec2, i, h.key, h.secret, h.token));
	}
	provider.destroy(provider.udata);
	assert_int_eq(h.destroys, 1);

	// Errors: the hook's message, its status when the message is empty, and no credentials.
	assert_int_eq(hook_provider(&ec2, &f, &h, 1000, &provider), AEROSPIKE_OK);
	h.status = AEROSPIKE_ERR_CLIENT;
	h.message = "vault unavailable";
	assert_string_eq(cred_step(&provider, buf, sizeof(buf)),
		"AWS EC2 seed provider: credentials hook failed: vault unavailable");
	h.message = NULL;
	assert_string_eq(cred_step(&provider, buf, sizeof(buf)),
		"AWS EC2 seed provider: credentials hook failed: status -1");
	h.status = AEROSPIKE_OK;
	h.key = NULL;
	assert_string_eq(cred_step(&provider, buf, sizeof(buf)),
		"AWS EC2 seed provider: credentials hook returned no credentials");
	provider.destroy(provider.udata);

	// The secret the hook set before failing and the cached session token are redacted.
	assert_int_eq(hook_provider(&ec2, &f, &h, 1000, &provider), AEROSPIKE_OK);
	h.token = "cached-session-token";
	h.expiration_ms = CRED_NOW_MS + 60000;
	assert_string_eq(cred_step(&provider, buf, sizeof(buf)), "10.0.0.1:3000");
	f.now_ms = CRED_NOW_MS + 60000;
	h.secret = "new/secret";
	h.token = "new-session-token";
	h.status = AEROSPIKE_ERR_CLIENT;
	h.message = "denied for new/secret after cached-session-token and new-session-token";
	assert_string_eq(cred_step(&provider, buf, sizeof(buf)), "AWS EC2 seed provider: credentials "
		"hook failed: denied for <redacted> after <redacted> and <redacted>");
	provider.destroy(provider.udata);
	f.now_ms = CRED_NOW_MS;

	// A late success fails this refresh, resolves the source and is used by the next refresh.
	const uint32_t timeout = 200;
	assert_int_eq(hook_provider(&ec2, &f, &h, timeout, &provider), AEROSPIKE_OK);
	h.sleep_ms = timeout + 100;
	uint32_t ec2_requests = aws_stub_requests(&ec2);
	log_start();
	uint64_t start = cf_getms();
	as_status status = cred_refresh(&provider, buf, sizeof(buf));
	uint64_t elapsed = cf_getms() - start;
	log_stop();
	info("late hook: %" PRIu64 " ms (timeout_ms %u, hook sleep %u)", elapsed, timeout,
		h.sleep_ms);
	char expected[128];
	snprintf(expected, sizeof(expected), "AWS EC2 seed provider: credentials hook timed out after "
		"%ums", h.last_timeout_ms);
	assert_int_eq(status, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_string_eq(buf, expected);
	assert_true(h.last_timeout_ms <= timeout && h.last_timeout_ms + 50 >= timeout);
	assert_true(elapsed <= h.sleep_ms + AWS_MARGIN_MS);
	assert_int_eq(log_matches("credentials from hook"), 1);
	assert_int_eq(aws_stub_requests(&ec2), ec2_requests);
	h.sleep_ms = 0;
	assert_int_eq(cred_refresh(&provider, buf, sizeof(buf)), AEROSPIKE_OK);
	assert_int_eq(h.gets, 1);
	assert_true(signed_with(&ec2, ec2_requests, h.key, h.secret, h.token));
	provider.destroy(provider.udata);

	// A late error caches nothing and resolves nothing.
	assert_int_eq(hook_provider(&ec2, &f, &h, timeout, &provider), AEROSPIKE_OK);
	h.sleep_ms = timeout + 50;
	h.status = AEROSPIKE_ERR_CLIENT;
	h.message = "late failure";
	log_start();
	status = cred_refresh(&provider, buf, sizeof(buf));
	log_stop();
	assert_int_eq(status, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_not_null(strstr(buf, "credentials hook timed out after "));
	assert_int_eq(log_matches("credentials from"), 0);
	h.sleep_ms = 0;
	h.status = AEROSPIKE_OK;
	assert_int_eq(cred_refresh(&provider, buf, sizeof(buf)), AEROSPIKE_OK);
	assert_int_eq(h.gets, 2);
	provider.destroy(provider.udata);

	// Credentials without an expiry: get once across ten refreshes.
	assert_int_eq(hook_provider(&ec2, &f, &h, 1000, &provider), AEROSPIKE_OK);

	for (uint32_t i = 0; i < 10; i++) {
		assert_int_eq(cred_refresh(&provider, buf, sizeof(buf)), AEROSPIKE_OK);
	}
	assert_int_eq(h.gets, 1);
	provider.destroy(provider.udata);
	assert_int_eq(h.destroys, 1);
	assert_int_eq(aws_stub_accepts(&imds), 0);

	aws_stub_stop(&imds);
	aws_stub_stop(&ec2);
}

//---------------------------------
// Tests: deadlines, threads and descriptors
//---------------------------------

static const char*
timed_refresh(aws_stub* s, bool tls, const char* ca_file, uint32_t timeout_ms, uint64_t* elapsed,
	char* buf, size_t size)
{
	uint64_t start = cf_getms();
	stub_refresh(s, tls, "localhost", ca_file, timeout_ms, buf, size);
	*elapsed = cf_getms() - start;
	return buf;
}

TEST(aws_deadline_unanswered, "D-14 a silent endpoint, a stalled handshake, a trickled body, slow pages and silent STS, ECS and IMDS endpoints each end within timeout_ms")
{
	aws_certs certs;
	assert_true(aws_certs_create(&certs));
	aws_stub s;
	assert_true(aws_stub_start(&s, false));

	const uint32_t timeout = 300;
	char expected[128];
	snprintf(expected, sizeof(expected), "AWS EC2 seed provider: DescribeInstances timed out "
		"after %ums", timeout);

	char* instance = aws_xml_instance("i-1", "10.0.0.1", NULL, NULL, NULL);
	const char* instances[] = {instance};
	char* doc = aws_xml_describe("next", instances, 1);
	char* page = aws_strf("HTTP/1.1 200 OK\r\ncontent-length: %zu\r\n\r\n%s", strlen(doc), doc);

	for (int variant = 0; variant < 4; variant++) {
		static const char* const names[] = {
			"silent endpoint", "stalled handshake", "trickled body", "slow pages"
		};
		aws_stub_reset(&s);
		aws_stub_set_tls(&s, variant == 1 ? certs.good : NULL);

		aws_reply reply = {.mode = AWS_REPLY_SILENT};

		if (variant == 2) {
			reply = (aws_reply){.mode = AWS_REPLY_TRICKLE, .data = page, .size = strlen(page),
				.trickle_ms = 20};
		}
		else if (variant == 3) {
			reply = (aws_reply){.mode = AWS_REPLY_SEND, .data = page, .size = strlen(page),
				.delay_ms = timeout * 2 / 3};
		}
		aws_stub_reply(&s, &reply);

		char buf[1100];
		uint64_t elapsed;
		timed_refresh(&s, variant == 1, certs.ca_file, timeout, &elapsed, buf, sizeof(buf));
		info("%s: %" PRIu64 " ms (timeout_ms %u)", names[variant], elapsed, timeout);

		if (strcmp(buf, expected) != 0) {
			fail("%s: got \"%s\"", names[variant], buf);
		}

		if (elapsed + 5 < timeout || elapsed > timeout + AWS_MARGIN_MS) {
			fail("%s: took %" PRIu64 " ms", names[variant], elapsed);
		}

		if (variant == 3 && aws_stub_requests(&s) != 2) {
			fail("slow pages: %u requests", aws_stub_requests(&s));
		}
	}

	// Silent credential endpoints: STS, ECS, and IMDS as the only source.
	aws_stub ec2;
	assert_true(aws_stub_start(&ec2, false));
	stub_one(&ec2, "10.0.0.1");
	tmp_dir d;
	assert_true(tmp_create(&d));
	const char* token_file = tmp_text(&d, "token", WEB_TOKEN);

	for (int variant = 0; variant < 3; variant++) {
		static const char* const names[] = {"silent STS", "silent ECS", "silent IMDS"};
		static const char* const messages[] = {
			"AWS EC2 seed provider: STS AssumeRoleWithWebIdentity timed out",
			"AWS EC2 seed provider: ECS credentials timed out",
			CRED_NO_SOURCE "IMDS token timed out"
		};
		aws_stub_reset(&s);
		aws_stub_set_tls(&s, NULL);
		aws_reply reply = {.mode = AWS_REPLY_SILENT};
		aws_stub_reply(&s, &reply);

		fake_env f;
		fake_init(&f, variant == 0 ? s.port : 0, variant == 1 ? s.port : 0,
			variant == 2 ? s.port : 0);

		if (variant == 0) {
			fake_set(&f, "AWS_WEB_IDENTITY_TOKEN_FILE", token_file);
			fake_set(&f, "AWS_ROLE_ARN", ROLE_ARN);
		}
		else if (variant == 1) {
			fake_set(&f, "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/creds");
		}

		char url[64];
		as_aws_seed_provider_config cfg;
		cred_cfg(&cfg, &ec2, url, sizeof(url));
		cfg.timeout_ms = timeout;
		as_seed_provider provider;
		as_error err;
		assert_int_eq(as_discovery_aws_seed_provider_env(&err, &cfg, &f.env, &provider),
			AEROSPIKE_OK);

		char buf[1100];
		uint64_t start = cf_getms();
		cred_refresh(&provider, buf, sizeof(buf));
		uint64_t elapsed = cf_getms() - start;
		provider.destroy(provider.udata);
		info("%s: %" PRIu64 " ms (timeout_ms %u)", names[variant], elapsed, timeout);

		if (strcmp(buf, messages[variant]) != 0) {
			fail("%s: got \"%s\"", names[variant], buf);
		}

		if (elapsed + 5 < timeout || elapsed > timeout + AWS_MARGIN_MS) {
			fail("%s: took %" PRIu64 " ms", names[variant], elapsed);
		}
	}
	assert_int_eq(aws_stub_requests(&ec2), 0);
	tmp_destroy(&d);
	aws_stub_stop(&ec2);

	free(page);
	free(doc);
	free(instance);
	aws_stub_stop(&s);
	aws_certs_destroy(&certs);
}

TEST(aws_no_thread, "D-14 construction and ten refreshes create no thread")
{
#if defined(__linux__)
	aws_stub s;
	assert_true(aws_stub_start(&s, false));
	stub_one(&s, "10.0.0.1");
	int before = thread_count();
	assert_true(before > 0);

	as_aws_seed_provider_config cfg;
	as_seed_provider provider;
	as_error err;
	aws_cfg(&cfg, NULL);
	assert_int_eq(provider_for(&s, false, "127.0.0.1", &cfg, &provider, &err), AEROSPIKE_OK);
	assert_int_eq(thread_count(), before);

	char buf[256];

	for (int i = 0; i < 10; i++) {
		assert_int_eq(refresh_str(&provider, &err, buf, sizeof(buf)), AEROSPIKE_OK);
		assert_string_eq(buf, "10.0.0.1:3000");
	}
	assert_int_eq(thread_count(), before);
	provider.destroy(provider.udata);
	assert_int_eq(thread_count(), before);
	aws_stub_stop(&s);
#else
	info("skipped: thread counts need /proc/self/task");
#endif
}

TEST(aws_no_leaked_descriptors, "every transport path, error and failed construction releases its sockets")
{
	aws_certs certs;
	assert_true(aws_certs_create(&certs));
	aws_stub plain;
	aws_stub tls;
	assert_true(aws_stub_start(&plain, false));
	assert_true(aws_stub_start(&tls, false));
	aws_stub_set_tls(&tls, certs.good);

	// Warm up OpenSSL so lazily opened files are not counted.
	char buf[1100];
	stub_one(&tls, "10.0.0.1");
	assert_int_eq(stub_refresh(&tls, true, "localhost", certs.ca_file, 2000, buf, sizeof(buf)),
		AEROSPIKE_OK);

	int before = fd_count();
	assert_true(before > 0);

	for (int round = 0; round < 2; round++) {
		aws_stub_reset(&plain);
		stub_one(&plain, "10.0.0.1");
		stub_403(&plain);
		aws_reply silent = {.mode = AWS_REPLY_SILENT};
		aws_stub_reply(&plain, &silent);
		const char* truncated = "HTTP/1.1 200 OK\r\ncontent-length: 10\r\n\r\nab";
		stub_raw(&plain, truncated, strlen(truncated));

		for (int i = 0; i < 4; i++) {
			stub_refresh(&plain, false, "127.0.0.1", NULL, 200, buf, sizeof(buf));
		}

		aws_stub_reset(&tls);
		stub_one(&tls, "10.0.0.1");
		stub_refresh(&tls, true, "localhost", certs.ca_file, 200, buf, sizeof(buf));
		stub_refresh(&tls, true, "localhost", NULL, 200, buf, sizeof(buf));
		aws_stub_set_tls(&tls, certs.wrong_dns);
		stub_refresh(&tls, true, "localhost", certs.ca_file, 200, buf, sizeof(buf));
		aws_stub_set_tls(&tls, certs.good);
		aws_stub_reset(&tls);
		aws_stub_reply(&tls, &silent);
		stub_refresh(&tls, true, "localhost", certs.ca_file, 200, buf, sizeof(buf));

		// Construction failures.
		as_aws_seed_provider_config cfg;
		as_seed_provider provider;
		as_error err;
		aws_cfg(&cfg, NULL);
		cfg.ca_file = "/nonexistent/as-aws-ca.pem";
		assert_int_eq(as_discovery_aws_seed_provider(&err, &cfg, &provider),
			AEROSPIKE_ERR_TLS_ERROR);
		cfg.ca_file = NULL;
		cfg.ca_path = "/nonexistent";
		assert_int_eq(as_discovery_aws_seed_provider(&err, &cfg, &provider),
			AEROSPIKE_ERR_TLS_ERROR);
	}

	aws_stub_reset(&plain);
	aws_stub_reset(&tls);
	assert_int_eq(settle(fd_count, before), before);
	aws_stub_stop(&tls);
	aws_stub_stop(&plain);
	aws_certs_destroy(&certs);
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
mgr_register(as_config* config, aws_stub* s, uint32_t timeout_ms, as_error* err)
{
	char url[64];
	as_aws_seed_provider_config cfg;
	aws_cfg(&cfg, stub_url(s, false, "127.0.0.1", url, sizeof(url)));
	cfg.timeout_ms = timeout_ms;
	return as_config_set_aws_seed_provider(config, &cfg, err);
}

static bool
wait_stat(as_seed_manager* m, size_t offset, uint32_t target, uint32_t timeout_ms)
{
	uint64_t end = cf_getms() + timeout_ms;

	while (true) {
		as_seed_manager_stats stats = stats_of(m);

		if (*(uint32_t*)((char*)&stats + offset) >= target) {
			return true;
		}

		if (cf_getms() >= end) {
			return false;
		}
		as_sleep(5);
	}
}

static bool
fake_true(void* udata)
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

TEST(aws_thread_validation, "thread mode rejects timeout_ms above refresh_timeout_ms before any request and accepts equality")
{
	aws_stub s;
	assert_true(aws_stub_start(&s, false));
	stub_one(&s, "10.0.0.1");

	as_config config;
	as_error err;
	as_seed_manager* m = NULL;
	mgr_config(&config, AS_DISCOVERY_EXECUTION_THREAD, 1000);
	config.discovery.refresh_interval_ms = 600000;
	assert_int_eq(mgr_register(&config, &s, 2000, &err), AEROSPIKE_OK);

	config.discovery.refresh_timeout_ms = 1999;
	assert_int_eq(as_seed_manager_create(&err, &config, &m), AEROSPIKE_ERR_PARAM);
	assert_string_eq(err.message, "AWS EC2 seed provider timeout_ms 2000 exceeds discovery "
		"refresh_timeout_ms 1999. Lower timeout_ms or raise refresh_timeout_ms");

	config.discovery.refresh_timeout_ms = 2000;
	assert_int_eq(as_seed_manager_create(&err, &config, &m), AEROSPIKE_OK);
	as_seed_manager_destroy(m);
	as_config_destroy(&config);

	// The check identifies the built-in, not a provider that is only named "aws-tags".
	mgr_config(&config, AS_DISCOVERY_EXECUTION_THREAD, 1000);
	config.discovery.refresh_timeout_ms = 1;
	as_seed_provider fake = {
		.size = sizeof(as_seed_provider),
		.api_version = AS_DISCOVERY_API_VERSION,
		.name = "aws-tags",
		.refresh = fake_refresh,
		.needs_periodic_refresh = fake_true,
		.supports_deadline = NULL
	};
	assert_int_eq(as_config_set_seed_provider(&config, &fake, &err), AEROSPIKE_OK);
	assert_int_eq(as_seed_manager_create(&err, &config, &m), AEROSPIKE_OK);
	as_seed_manager_destroy(m);
	as_config_destroy(&config);

	// aerospike_connect() applies the same rule.
	mgr_config(&config, AS_DISCOVERY_EXECUTION_THREAD, 1000);
	config.discovery.refresh_timeout_ms = 1999;
	assert_int_eq(mgr_register(&config, &s, 2000, &err), AEROSPIKE_OK);
	aerospike as;
	aerospike_init(&as, &config);
	assert_int_eq(aerospike_connect(&as, &err), AEROSPIKE_ERR_PARAM);
	assert_not_null(strstr(err.message, "AWS EC2 seed provider timeout_ms 2000 exceeds"));
	assert_null(as.cluster);
	aerospike_destroy(&as);

	assert_int_eq(aws_stub_accepts(&s), 0);
	aws_stub_stop(&s);
}

TEST(aws_tend_rejected, "M-10 tend mode is rejected at registration and at connect, with zero requests")
{
	aws_stub s;
	assert_true(aws_stub_start(&s, false));
	stub_one(&s, "10.0.0.1");

	as_config config;
	as_error err;
	mgr_config(&config, AS_DISCOVERY_EXECUTION_TEND, 1000);
	assert_int_eq(mgr_register(&config, &s, 100, &err), AEROSPIKE_ERR_PARAM);
	assert_string_eq(err.message, "AWS EC2 seed provider requires thread execution mode");
	assert_false(config.discovery.provider_registered);

	// Registered through the generic setter, the provider still fails connect.
	char url[64];
	as_aws_seed_provider_config cfg;
	as_seed_provider provider;
	aws_cfg(&cfg, stub_url(&s, false, "127.0.0.1", url, sizeof(url)));
	cfg.timeout_ms = 100;
	assert_int_eq(as_discovery_aws_seed_provider(&err, &cfg, &provider), AEROSPIKE_OK);
	assert_int_eq(as_config_set_seed_provider(&config, &provider, &err), AEROSPIKE_OK);

	as_seed_manager* m = NULL;
	assert_int_eq(as_seed_manager_create(&err, &config, &m), AEROSPIKE_ERR_PARAM);
	assert_string_eq(err.message, "Seed provider aws-tags does not support a deadline and cannot "
		"use discovery execution_mode tend. Use execution_mode thread");

	config.tender_interval = AS_TEND_INTERVAL_MIN;
	config.discovery.refresh_interval_ms = AS_TEND_INTERVAL_MIN;
	config.discovery.tend_refresh_deadline_ms = AS_TEND_INTERVAL_MIN / 2;
	aerospike as;
	aerospike_init(&as, &config);
	assert_int_eq(aerospike_connect(&as, &err), AEROSPIKE_ERR_PARAM);
	assert_string_eq(err.message, "Seed provider aws-tags does not support a deadline and cannot "
		"use discovery execution_mode tend. Use execution_mode thread");
	assert_null(as.cluster);
	assert_false(as.config.discovery.registration_locked);
	aerospike_destroy(&as);

	as_sleep(50);
	assert_int_eq(aws_stub_accepts(&s), 0);
	aws_stub_stop(&s);
}

TEST(aws_thread_mode, "thread mode runs aws-tags on the one client-owned discovery thread; tend never refreshes it")
{
	aws_stub s;
	assert_true(aws_stub_start(&s, false));
	stub_one(&s, "10.0.0.1");
	int before = discovery_thread_count();

	as_config config;
	as_error err;
	as_seed_manager* m = NULL;
	mgr_config(&config, AS_DISCOVERY_EXECUTION_THREAD, 1000);
	config.discovery.refresh_interval_ms = 600000;
	assert_int_eq(mgr_register(&config, &s, 2000, &err), AEROSPIKE_OK);
	assert_int_eq(as_seed_manager_create(&err, &config, &m), AEROSPIKE_OK);
	assert_int_eq(as_seed_manager_start(m, &err), AEROSPIKE_OK);

	char buf[256];
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "10.0.0.1:3000");
	assert_true(stats_of(m).thread_running);

	if (before >= 0) {
		assert_int_eq(settle(discovery_thread_count, before + 1), before + 1);
	}

	uint32_t requests = aws_stub_requests(&s);
	assert_int_eq(requests, 1);

	for (int i = 0; i < 3; i++) {
		as_seed_manager_tend(m);
	}
	assert_int_eq(aws_stub_requests(&s), requests);

	as_seed_manager_destroy(m);
	as_config_destroy(&config);

	if (before >= 0) {
		assert_int_eq(settle(discovery_thread_count, before), before);
	}
	aws_stub_stop(&s);
}

TEST(aws_snapshot_retention, "an API error after a publication keeps the exact previous snapshot; recovery publishes again")
{
	aws_stub s;
	assert_true(aws_stub_start(&s, false));
	stub_one(&s, "10.0.0.1");

	as_config config;
	as_error err;
	as_seed_manager* m = NULL;
	mgr_config(&config, AS_DISCOVERY_EXECUTION_THREAD, AS_TEND_INTERVAL_MIN);
	as_config_add_host(&config, "10.9.9.9", 3000);
	assert_int_eq(mgr_register(&config, &s, 1000, &err), AEROSPIKE_OK);
	assert_int_eq(as_seed_manager_create(&err, &config, &m), AEROSPIKE_OK);
	assert_int_eq(as_seed_manager_start(m, &err), AEROSPIKE_OK);

	char buf[256];
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "10.9.9.9:3000,10.0.0.1:3000");
	as_seed_snapshot* before = as_seed_manager_reserve(m);
	uint32_t publications = stats_of(m).publications;

	log_start();
	char* body = aws_xml_error(AWS_403_CODE, AWS_403_MESSAGE, AWS_403_ID);
	aws_stub_set_http(&s, 403, NULL, body);
	free(body);
	bool failed = wait_stat(m, offsetof(as_seed_manager_stats, failures), 1, 5000);
	log_stop();
	assert_true(failed);

	as_seed_snapshot* after = as_seed_manager_reserve(m);
	assert_true(after == before);
	assert_int_eq(stats_of(m).publications, publications);
	assert_true(log_find(AS_LOG_LEVEL_WARN, "Seed provider aws-tags refresh failed: "
		"AEROSPIKE_ERR_SEED_DISCOVERY_FAILED " AWS_403_TEXT ". Keeping the previous seeds") >= 0);
	as_seed_snapshot_release(after);
	as_seed_snapshot_release(before);

	char* instance = aws_xml_instance("i-2", "10.0.0.2", NULL, NULL, NULL);
	const char* instances[] = {instance};
	body = aws_xml_describe(NULL, instances, 1);
	aws_stub_set_http(&s, 200, NULL, body);
	free(body);
	free(instance);
	assert_true(wait_stat(m, offsetof(as_seed_manager_stats, publications), publications + 1,
		5000));
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "10.9.9.9:3000,10.0.0.2:3000");

	as_seed_manager_destroy(m);
	as_config_destroy(&config);
	aws_stub_stop(&s);
}

TEST(aws_init_failure, "an initial failure falls back to configured seeds, or returns -27 naming aws-tags and the AWS error")
{
	aws_stub s;
	assert_true(aws_stub_start(&s, false));
	stub_403(&s);

	as_config config;
	as_error err;
	as_seed_manager* m = NULL;
	mgr_config(&config, AS_DISCOVERY_EXECUTION_THREAD, 1000);
	as_config_add_host(&config, "10.9.9.9", 3000);
	assert_int_eq(mgr_register(&config, &s, 1000, &err), AEROSPIKE_OK);
	assert_int_eq(as_seed_manager_create(&err, &config, &m), AEROSPIKE_OK);
	assert_int_eq(as_seed_manager_start(m, &err), AEROSPIKE_OK);
	char buf[256];
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "10.9.9.9:3000");
	as_seed_manager_destroy(m);
	as_config_destroy(&config);

	mgr_config(&config, AS_DISCOVERY_EXECUTION_THREAD, 1000);
	assert_int_eq(mgr_register(&config, &s, 1000, &err), AEROSPIKE_OK);
	assert_int_eq(as_seed_manager_create(&err, &config, &m), AEROSPIKE_OK);
	assert_int_eq(as_seed_manager_start(m, &err), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_int_eq(AEROSPIKE_ERR_SEED_DISCOVERY_FAILED, -27);
	assert_string_eq(err.message, "Seed provider aws-tags failed and no seed host is left: "
		AWS_403_TEXT);
	as_seed_manager_destroy(m);
	as_config_destroy(&config);
	aws_stub_stop(&s);
}

//---------------------------------
// Static Functions: client helpers
//---------------------------------

void
aerospike_destroy_internal(aerospike* as);

static void
client_config(as_config* config)
{
	as_config_init(config);
	config->tender_interval = AS_TEND_INTERVAL_MIN;
	config->conn_timeout_ms = 1000;
	config->fail_if_not_connected = true;
	config->discovery.execution_mode = AS_DISCOVERY_EXECUTION_THREAD;
	config->discovery.refresh_interval_ms = AS_TEND_INTERVAL_MIN;
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
// Tests: client bootstrap
//---------------------------------

typedef struct boot_env_s {
	fnode a;
	fnode b;
	aws_stub s;
} boot_env;

// Node A is returned by DescribeInstances; node B is only advertised by A in peers-clear-std.
static bool
boot_start(boot_env* env)
{
	if (!fnode_start(&env->a, "A") || !fnode_start(&env->b, "B") ||
		!aws_stub_start(&env->s, false)) {
		return false;
	}

	char peers[128];
	snprintf(peers, sizeof(peers), "1,3000,[[B,,[127.0.0.1:%u]]]", env->b.port);
	fnode_set_peers(&env->a, peers);
	snprintf(peers, sizeof(peers), "1,3000,[[A,,[127.0.0.1:%u]]]", env->a.port);
	fnode_set_peers(&env->b, peers);
	stub_one(&env->s, "127.0.0.1");
	return true;
}

static void
boot_stop(boot_env* env)
{
	aws_stub_stop(&env->s);
	fnode_stop(&env->b);
	fnode_stop(&env->a);
}

// Register aws-tags with static keys, or with a hook when hook is not NULL.
static as_status
boot_register(boot_env* env, as_config* config, hook_state* hook, as_error* err)
{
	char url[64];
	as_aws_seed_provider_config cfg;
	aws_cfg(&cfg, stub_url(&env->s, false, "127.0.0.1", url, sizeof(url)));
	cfg.port = env->a.port;
	cfg.timeout_ms = 2000;

	if (hook) {
		hook_init(hook, &cfg);
	}
	return as_config_set_aws_seed_provider(config, &cfg, err);
}

static const char*
boot_once(boot_env* env, char* msg, size_t size)
{
	as_config config;
	as_error err;
	client_config(&config);

	if (boot_register(env, &config, NULL, &err) != AEROSPIKE_OK) {
		snprintf(msg, size, "register failed: %.400s", err.message);
		as_config_destroy(&config);
		return msg;
	}

	uint32_t requests = aws_stub_requests(&env->s);
	aerospike* as = aerospike_new(&config);
	as_status status = aerospike_connect(as, &err);
	const char* failure = NULL;

	if (status != AEROSPIKE_OK) {
		snprintf(msg, size, "connect failed: %d %.400s", status, err.message);
		failure = msg;
	}
	// B was never returned by EC2: the server-sourced peers list added it.
	else if (!wait_for_nodes(as, 2, 10000) || !has_node(as, "A") || !has_node(as, "B")) {
		failure = "A and B did not both join";
	}
	else if (aws_stub_requests(&env->s) <= requests) {
		failure = "DescribeInstances was not called";
	}
	else if (!as->config.discovery.registration_locked) {
		failure = "registration is not locked while connected";
	}
	else {
		as_seed_snapshot* snapshot = as_seed_manager_reserve(as->cluster->seed_manager);
		const as_vector* hosts = as_seed_snapshot_hosts(snapshot);
		as_host* first = hosts->size ? as_vector_get((as_vector*)hosts, 0) : NULL;

		if (!first || strcmp(first->name, "127.0.0.1") != 0 || first->port != env->a.port) {
			char list[256];
			snprintf(msg, size, "snapshot %s does not start with the EC2-published seed",
				hosts_str(hosts, list, sizeof(list)));
			failure = msg;
		}
		as_seed_snapshot_release(snapshot);
	}

	client_close(as);
	return failure;
}

TEST(aws_bootstrap, "an EC2-published seed bootstraps the client in thread mode with no static seeds; membership comes from peers")
{
	int fds = fd_count();
	boot_env env;
	assert_true(boot_start(&env));

	char msg[512];
	const char* failure = boot_once(&env, msg, sizeof(msg));

	if (failure) {
		uint32_t a_requests = fnode_get(&env.a, &env.a.requests);
		uint32_t b_requests = fnode_get(&env.b, &env.b.requests);
		boot_stop(&env);
		fail("%s (A requests %u, B requests %u)", failure, a_requests, b_requests);
	}
	boot_stop(&env);
	assert_int_eq(settle(fd_count, fds), fds);
}

static const char*
boot_close_reconnect(boot_env* env, hook_state* hook)
{
	int before = discovery_thread_count();

	as_config config;
	as_error err;
	client_config(&config);
	config.discovery.refresh_interval_ms = 600000;

	if (boot_register(env, &config, hook, &err) != AEROSPIKE_OK) {
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

	uint32_t requests = aws_stub_requests(&env->s);
	client_sync_close(as);

	if (!as->config.discovery.provider_registered) {
		failure = "sync close released the aws-tags registration";
	}
	else if (as_load_uint32_acq(&hook->destroys) != 0) {
		failure = "sync close ran the hook destroy";
	}
	else if (before >= 0 && settle(discovery_thread_count, before) != before) {
		failure = "sync close left the discovery thread running";
	}
	else if (aerospike_connect(as, &err) != AEROSPIKE_OK || !has_node(as, "A")) {
		failure = "reconnect failed";
	}
	else if (aws_stub_requests(&env->s) <= requests) {
		failure = "reconnect did not call DescribeInstances again";
	}
	client_close(as);

	if (!failure && before >= 0 && settle(discovery_thread_count, before) != before) {
		failure = "destroy left the discovery thread running";
	}

	if (!failure && !wait_count(&hook->destroys, 1, 5000)) {
		failure = "destroy did not run the hook destroy";
	}
	return failure;
}

TEST(aws_close_reconnect, "sync close keeps the aws-tags registration and reconnect calls EC2 again; destroy releases it and runs the hook destroy once")
{
	int fds = fd_count();
	boot_env env;
	assert_true(boot_start(&env));
	hook_state hook;
	const char* failure = boot_close_reconnect(&env, &hook);
	boot_stop(&env);

	if (failure) {
		fail("%s", failure);
	}
	as_sleep(50);
	assert_int_eq(as_load_uint32_acq(&hook.destroys), 1);

	// The credentials have no expiry, so the reconnect reused them without calling get.
	assert_int_eq(as_load_uint32_acq(&hook.gets), 1);
	assert_int_eq(settle(fd_count, fds), fds);
}

TEST(aws_hook_deferred_destroy, "a hook blocked in get past refresh_timeout_ms blocks neither close nor destroy; its destroy runs once, on the discovery thread, after get returns")
{
	boot_env env;
	assert_true(boot_start(&env));

	const uint32_t refresh_timeout = 300;
	as_config config;
	as_error err;
	client_config(&config);
	config.discovery.refresh_timeout_ms = refresh_timeout;

	char url[64];
	as_aws_seed_provider_config cfg;
	aws_cfg(&cfg, stub_url(&env.s, false, "127.0.0.1", url, sizeof(url)));
	cfg.port = env.a.port;
	cfg.timeout_ms = refresh_timeout;
	hook_state h;
	hook_init(&h, &cfg);

	// Inside the expiry margin, so every refresh calls get; from the second call get blocks.
	h.expiration_ms = cf_clock_getabsolute() + 60000;
	h.sleep_ms = 1500;
	h.sleep_from = 2;
	assert_int_eq(as_config_set_aws_seed_provider(&config, &cfg, &err), AEROSPIKE_OK);

	aerospike* as = aerospike_new(&config);

	if (aerospike_connect(as, &err) != AEROSPIKE_OK) {
		client_close(as);
		boot_stop(&env);
		fail("connect failed: %s", err.message);
	}

	if (!wait_count(&h.gets, 2, 5000)) {
		client_close(as);
		boot_stop(&env);
		fail("the second refresh did not call get");
	}
	as_sleep(20);

	uint64_t start = cf_getms();
	client_close(as);
	uint64_t elapsed = cf_getms() - start;
	uint32_t destroys = as_load_uint32_acq(&h.destroys);
	info("close and destroy with get blocked: %" PRIu64 " ms (refresh_timeout_ms %u)", elapsed,
		refresh_timeout);

	bool destroyed = wait_count(&h.destroys, 1, 5000);
	as_sleep(50);
	boot_stop(&env);

	assert_true(elapsed <= refresh_timeout + AWS_MARGIN_MS);
	assert_int_eq(destroys, 0);
	assert_true(destroyed);
	assert_int_eq(as_load_uint32_acq(&h.destroys), 1);
	assert_true(h.destroyed_after_get);
	assert_true(pthread_equal(h.destroy_thread, h.get_thread));
	assert_false(pthread_equal(h.destroy_thread, pthread_self()));
}

#else

TEST(aws_fake_servers_skipped, "wire, TLS, deadline and client aws-tags tests need POSIX loopback servers")
{
	info("skipped: the EC2 stub and fake node tests need POSIX sockets");
}

#endif // DISCOVERY_FAKE_SERVERS

//---------------------------------
// Test Suite
//---------------------------------

SUITE(discovery_aws, "pluggable discovery aws-tags seed provider tests")
{
	suite_add(aws_config_defaults);
	suite_add(aws_config_invalid);
	suite_add(aws_config_trust_files);
	suite_add(aws_config_registration);
	suite_add(aws_hook_ownership);
	suite_add(aws_credentials_set);
	suite_add(aws_json_reader);
	suite_add(aws_ini_reader);
	suite_add(aws_iso8601);
	suite_add(aws_sigv4_vectors);
	suite_add(aws_xml_reader);
	suite_add(aws_xml_hardening);
	suite_add(aws_xml_error_text);
#ifdef DISCOVERY_FAKE_SERVERS
	suite_add(aws_refresh_addresses);
	suite_add(aws_request_shape);
	suite_add(aws_pagination);
	suite_add(aws_empty_and_errors);
	suite_add(aws_http_framing);
	suite_add(aws_header_injection);
	suite_add(aws_endpoint_forms);
	suite_add(aws_tls);
	suite_add(aws_trust_bundle);
	suite_add(aws_cred_config);
	suite_add(aws_cred_environment);
	suite_add(aws_cred_profile);
	suite_add(aws_cred_web_identity);
	suite_add(aws_cred_ecs);
	suite_add(aws_cred_imds);
	suite_add(aws_cred_order);
	suite_add(aws_cred_cache);
	suite_add(aws_cred_hook);
	suite_add(aws_deadline_unanswered);
	suite_add(aws_no_thread);
	suite_add(aws_no_leaked_descriptors);
	suite_add(aws_thread_validation);
	suite_add(aws_tend_rejected);
	suite_add(aws_thread_mode);
	suite_add(aws_snapshot_retention);
	suite_add(aws_init_failure);
	suite_add(aws_bootstrap);
	suite_add(aws_close_reconnect);
	suite_add(aws_hook_deferred_destroy);
#else
	suite_add(aws_fake_servers_skipped);
#endif
}
