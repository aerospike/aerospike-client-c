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
		return "Invalid AWS EC2 seed provider credentials: access_key_id and secret_access_key "
			"are required";
	case 18:
		cfg->access_key_id = "";
		return "Invalid AWS EC2 seed provider credentials: access_key_id and secret_access_key "
			"are required";
	case 19:
		cfg->secret_access_key = NULL;
		return "Invalid AWS EC2 seed provider credentials: access_key_id and secret_access_key "
			"are required";
	case 20:
		cfg->secret_access_key = "";
		return "Invalid AWS EC2 seed provider credentials: access_key_id and secret_access_key "
			"are required";
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
	default:
		break;
	}
	i -= 27;

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

TEST(aws_deadline_unanswered, "D-14 a silent endpoint, a stalled handshake, a trickled body and slow pages each end within timeout_ms")
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

static as_status
boot_register(boot_env* env, as_config* config, as_error* err)
{
	char url[64];
	as_aws_seed_provider_config cfg;
	aws_cfg(&cfg, stub_url(&env->s, false, "127.0.0.1", url, sizeof(url)));
	cfg.port = env->a.port;
	cfg.timeout_ms = 2000;
	return as_config_set_aws_seed_provider(config, &cfg, err);
}

static const char*
boot_once(boot_env* env, char* msg, size_t size)
{
	as_config config;
	as_error err;
	client_config(&config);

	if (boot_register(env, &config, &err) != AEROSPIKE_OK) {
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
boot_close_reconnect(boot_env* env)
{
	int before = discovery_thread_count();

	as_config config;
	as_error err;
	client_config(&config);
	config.discovery.refresh_interval_ms = 600000;

	if (boot_register(env, &config, &err) != AEROSPIKE_OK) {
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
	return failure;
}

TEST(aws_close_reconnect, "sync close keeps the aws-tags registration and reconnect calls EC2 again; destroy releases it")
{
	int fds = fd_count();
	boot_env env;
	assert_true(boot_start(&env));
	const char* failure = boot_close_reconnect(&env);
	boot_stop(&env);

	if (failure) {
		fail("%s", failure);
	}
	assert_int_eq(settle(fd_count, fds), fds);
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
#else
	suite_add(aws_fake_servers_skipped);
#endif
}
