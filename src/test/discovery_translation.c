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
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include <aerospike/aerospike.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_config.h>
#include <aerospike/as_discovery.h>
#include <aerospike/as_event.h>
#include <aerospike/as_node.h>
#include <aerospike/as_sleep.h>
#include <aerospike/as_tls.h>
#include <citrusleaf/cf_clock.h>

#include "test.h"
#include "aerospike_test.h"

#if !defined(_WIN32)
#include <arpa/inet.h>
#include <errno.h>
#include <ifaddrs.h>
#include <net/if.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <sys/socket.h>
#include <unistd.h>
#define DISCOVERY_FAKE_SERVERS 1
#endif

// Coverage: U-7, U-8, U-9, U-10, U-13, U-15 and the FR-40/FR-45 TLS rows of the
// translation, topology and TLS commit. Component tests use loopback fake info
// servers so that no live server topology is needed; they are the local
// precursors of N-2/N-6/N-8/N-10/N-14/N-15/N-16/N-18/N-19. N-17 (CN-only
// certificates rejected) intentionally stays red until the next major release
// and the shared N-* workflows are owned by the final integration commit.
// Component rows added with the collision checks after validation:
// - U-15 aliases: translation_collision_alias (both orders, default and
//   pass-through, no min connection reaches a dropped newcomer) and the alias
//   phase of translation_collision_live_node.
// - U-15 shared addresses with a registered translator:
//   translation_shared_address_registered.
// - L1 collision fixture rows: discovery_conformance_collision in discovery_contract.c and
//   src/test/conformance/conformance_runner.c.
// - Init is not failed by collisions or strict misses: translation_collision_*,
//   translation_wrong_list_miss and translation_service_strict_miss.
// - FR-40 runtime TLS name of a translated peer: translation_peer_tls_name_runtime.
// - FR-45 warn once per full name: translation_tls_legacy_warn_once.
// - Peers parser inputs (bracketed IPv6, truncated list):
//   translation_peers_parser_inputs.

//---------------------------------
// Static Functions: shared helpers
//---------------------------------

static const char*
format(char* buf, size_t size, const char* fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(buf, size, fmt, ap);
	va_end(ap);
	return buf;
}

static void
endpoint_init(as_endpoint* endpoint, const char* node, const char* tls, const char* host,
	uint16_t port, as_endpoint_source source)
{
	endpoint->node_name = node;
	endpoint->tls_name = tls;
	endpoint->host = host;
	endpoint->port = port;
	endpoint->source = source;
}

static int
cmp_u64(const void* a, const void* b)
{
	uint64_t x = *(const uint64_t*)a;
	uint64_t y = *(const uint64_t*)b;
	return x < y ? -1 : x > y;
}

//---------------------------------
// Static Functions: custom translators
//---------------------------------

typedef struct probe_s {
	as_endpoint_source last_source;
	uint32_t standard_calls;
	uint32_t alternate_calls;
	uint32_t destroyed;
	const char* tls_override;
	const char* miss_prefix;
} probe;

static bool
probe_translate(const as_endpoint* endpoint, as_host* out, void* udata)
{
	probe* p = udata;

	p->last_source = endpoint->source;

	if (endpoint->source == AS_ENDPOINT_SOURCE_ALTERNATE) {
		p->alternate_calls++;
	}
	else {
		p->standard_calls++;
	}

	if (p->miss_prefix && strncmp(endpoint->host, p->miss_prefix, strlen(p->miss_prefix)) == 0) {
		return false;
	}

	as_host_copy_fields(out, endpoint->host, p->tls_override, endpoint->port);
	return true;
}

static void
probe_destroy(void* udata)
{
	((probe*)udata)->destroyed++;
}

static void
probe_init(as_address_translator* translator, probe* p)
{
	memset(p, 0, sizeof(probe));
	memset(translator, 0, sizeof(as_address_translator));
	translator->size = sizeof(as_address_translator);
	translator->api_version = AS_DISCOVERY_API_VERSION;
	translator->name = "probe";
	translator->translate = probe_translate;
	translator->destroy = probe_destroy;
	translator->udata = p;
}

//---------------------------------
// Tests: port-map and pass-through translators (no I/O)
//---------------------------------

TEST(translation_port_map_grammar, "U-7 port-map grammar: IPv4, IPv6, node names and rejected entries")
{
	as_error err;
	as_address_translator translator;
	as_addr_map ok[] = {
		{"10.0.0.1:3000", "198.51.100.1:4000"},
		{"[2001:db8::1]:3000", "[2001:db8::2]:4000"},
		{"A1", "node-a.example:5000"}
	};

	assert_int_eq(as_discovery_port_map_translator(&err, ok, 3, &translator), AEROSPIKE_OK);
	assert_string_eq(translator.name, "port-map");
	assert_not_null(translator.destroy);
	translator.destroy(translator.udata);

	// A zero-length table is valid and misses everything.
	assert_int_eq(as_discovery_port_map_translator(&err, NULL, 0, &translator), AEROSPIKE_OK);
	translator.destroy(translator.udata);

	struct {
		as_addr_map entries[2];
		uint32_t size;
		const char* reason;
	} bad[] = {
		{{{"", "h:1"}}, 1, "empty key"},
		{{{"h:1", ""}}, 1, "empty value"},
		{{{NULL, "h:1"}}, 1, "null key"},
		{{{"h:1", NULL}}, 1, "null value"},
		{{{"h:3000", "h2"}}, 1, "value without port"},
		{{{"h:3000", "h2:0"}}, 1, "port 0"},
		{{{"h:3000", "h2:65536"}}, 1, "port above range"},
		{{{"h:3000", "h2:80x"}}, 1, "non numeric port"},
		{{{"h:3000", ":4000"}}, 1, "empty host"},
		{{{"::1:3000", "h2:4000"}}, 1, "unbracketed IPv6 key"},
		{{{"h:3000", "2001:db8::2:4000"}}, 1, "unbracketed IPv6 value"},
		{{{"h:0", "h2:4000"}}, 1, "key port 0"},
		{{{"h:3000", "h2:4000"}, {"h:3000", "h3:4000"}}, 2, "duplicate key"},
		{{{"A1", "h2:4000"}, {"A1", "h3:4000"}}, 2, "duplicate node name"}
	};

	for (uint32_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
		memset(&translator, 0x5a, sizeof(translator));
		as_status status = as_discovery_port_map_translator(&err, bad[i].entries, bad[i].size,
			&translator);

		if (status != AEROSPIKE_ERR_PARAM) {
			assert_log(true, "%s was accepted", bad[i].reason);
		}
		assert_int_eq(translator.size, 0x5a5a5a5a);
		assert_true(strlen(err.message) > 0);
	}
}

TEST(translation_port_map_lookup, "U-7 port-map precedence, TLS preservation and copied immutable state")
{
	char orig[2][32] = {"10.0.0.1:3000", "A1"};
	char alt[2][32] = {"198.51.100.1:4000", "node-a.example:5000"};
	as_addr_map entries[] = {{orig[0], alt[0]}, {orig[1], alt[1]}};
	as_address_translator translator;
	as_error err;
	as_endpoint endpoint;
	as_host out;

	assert_int_eq(as_discovery_port_map_translator(&err, entries, 2, &translator), AEROSPIKE_OK);

	// Later changes to the caller's strings have no effect.
	memset(orig, 'x', sizeof(orig));
	memset(alt, 'y', sizeof(alt));

	// Endpoint key wins over node name key.
	endpoint_init(&endpoint, "A1", "tls-a", "10.0.0.1", 3000, AS_ENDPOINT_SOURCE_STANDARD);
	assert_true(translator.translate(&endpoint, &out, translator.udata));
	assert_string_eq(out.name, "198.51.100.1");
	assert_int_eq(out.port, 4000);
	as_host_destroy(&out);

	// Different advertised port, same host: node name key applies.
	endpoint_init(&endpoint, "A1", "tls-a", "10.0.0.1", 3001, AS_ENDPOINT_SOURCE_STANDARD);
	assert_true(translator.translate(&endpoint, &out, translator.udata));
	assert_string_eq(out.name, "node-a.example");
	assert_int_eq(out.port, 5000);
	as_host_destroy(&out);

	// Host match is exact and case-sensitive; unknown node name is a miss.
	endpoint_init(&endpoint, "B2", "tls-b", "10.0.0.1", 3001, AS_ENDPOINT_SOURCE_STANDARD);
	assert_false(translator.translate(&endpoint, &out, translator.udata));
	endpoint_init(&endpoint, "B2", NULL, "10.0.0.1", 3000, AS_ENDPOINT_SOURCE_ALTERNATE);
	assert_true(translator.translate(&endpoint, &out, translator.udata));
	as_host_destroy(&out);

	translator.destroy(translator.udata);
}

TEST(translation_port_map_ipv6, "U-7 port-map maps bracketed IPv6 endpoints")
{
	as_addr_map entries[] = {{"[2001:db8::1]:3000", "[2001:db8::2]:4000"}};
	as_address_translator translator;
	as_error err;
	as_endpoint endpoint;
	as_host out;

	assert_int_eq(as_discovery_port_map_translator(&err, entries, 1, &translator), AEROSPIKE_OK);

	endpoint_init(&endpoint, "A1", "tls-a", "2001:db8::1", 3000, AS_ENDPOINT_SOURCE_ALTERNATE);
	assert_true(translator.translate(&endpoint, &out, translator.udata));
	assert_string_eq(out.name, "2001:db8::2");
	assert_int_eq(out.port, 4000);
	as_host_destroy(&out);

	endpoint_init(&endpoint, "A1", "tls-a", "2001:db8::3", 3000, AS_ENDPOINT_SOURCE_ALTERNATE);
	assert_false(translator.translate(&endpoint, &out, translator.udata));
	translator.destroy(translator.udata);
}

TEST(translation_translator_latency, "U-8 port-map and pass-through translate p99 is below 1 ms")
{
	enum { ENTRIES = 64, SAMPLES = 10000 };
	as_addr_map entries[ENTRIES];
	char orig[ENTRIES][40];
	char alt[ENTRIES][40];

	for (uint32_t i = 0; i < ENTRIES; i++) {
		snprintf(orig[i], sizeof(orig[i]), "10.0.0.%u:3000", i);
		snprintf(alt[i], sizeof(alt[i]), "198.51.100.%u:4000", i);
		entries[i].orig = orig[i];
		entries[i].alt = alt[i];
	}

	as_error err;
	as_address_translator translators[2];
	assert_int_eq(as_discovery_port_map_translator(&err, entries, ENTRIES, &translators[0]),
		AEROSPIKE_OK);
	as_discovery_pass_through_translator(&translators[1]);

	as_endpoint endpoint;
	endpoint_init(&endpoint, "A1", "tls", "10.0.0.63", 3000, AS_ENDPOINT_SOURCE_STANDARD);

	uint64_t* samples = malloc(sizeof(uint64_t) * SAMPLES);
	assert_not_null(samples);

	for (uint32_t t = 0; t < 2; t++) {
		for (uint32_t i = 0; i < SAMPLES; i++) {
			as_host out;
			uint64_t begin = cf_getns();
			bool mapped = translators[t].translate(&endpoint, &out, translators[t].udata);
			samples[i] = cf_getns() - begin;

			if (mapped) {
				as_host_destroy(&out);
			}
		}
		qsort(samples, SAMPLES, sizeof(uint64_t), cmp_u64);

		if (samples[SAMPLES * 99 / 100] >= 1000000) {
			free(samples);
			assert_log(true, "%s p99 is not below 1 ms", translators[t].name);
		}
	}
	free(samples);
	translators[0].destroy(translators[0].udata);
}

//---------------------------------
// Tests: central translation helper
//---------------------------------

TEST(translation_strict_error_content, "U-9 strict miss names translator, node, advertised endpoint and list")
{
	probe p;
	as_cluster cluster;
	as_error err;
	as_host out = {0};

	memset(&cluster, 0, sizeof(cluster));
	probe_init(&cluster.translator, &p);
	p.miss_prefix = "10.";
	cluster.translator_registered = true;
	cluster.translation_on_miss = AS_TRANSLATION_STRICT;

	assert_int_eq(as_cluster_translate_address(&cluster, &err, "B7", "tls-b", "10.1.2.3", 3010,
		AS_ENDPOINT_SOURCE_ALTERNATE, &out), AEROSPIKE_ERR_TRANSLATION_MISS);
	assert_not_null(strstr(err.message, "probe"));
	assert_not_null(strstr(err.message, "B7"));
	assert_not_null(strstr(err.message, "10.1.2.3:3010"));
	assert_not_null(strstr(err.message, "alternate"));
	assert_not_null(strstr(err.message, "pass_through"));
	assert_null(out.name);

	assert_int_eq(as_cluster_translate_address(&cluster, &err, "B7", "tls-b", "10.1.2.3", 3010,
		AS_ENDPOINT_SOURCE_STANDARD, &out), AEROSPIKE_ERR_TRANSLATION_MISS);
	assert_not_null(strstr(err.message, "standard"));

	// The default policy falls back to the advertised endpoint.
	cluster.translation_on_miss = AS_TRANSLATION_PASS_THROUGH;
	assert_int_eq(as_cluster_translate_address(&cluster, &err, "B7", "tls-b", "10.1.2.3", 3010,
		AS_ENDPOINT_SOURCE_STANDARD, &out), AEROSPIKE_OK);
	assert_string_eq(out.name, "10.1.2.3");
	assert_int_eq(out.port, 3010);
	assert_string_eq(out.tls_name, "tls-b");
	as_host_destroy(&out);
}

TEST(translation_tls_name_preserved, "U-10 translation never changes the node TLS identity")
{
	probe p;
	as_cluster cluster;
	as_error err;
	as_host out;

	memset(&cluster, 0, sizeof(cluster));
	probe_init(&cluster.translator, &p);
	p.tls_override = "evil.example";
	cluster.translator_registered = true;
	cluster.translation_on_miss = AS_TRANSLATION_STRICT;

	assert_int_eq(as_cluster_translate_address(&cluster, &err, "B7", "node-b.example", "10.1.2.3",
		3010, AS_ENDPOINT_SOURCE_STANDARD, &out), AEROSPIKE_OK);
	assert_string_eq(out.name, "10.1.2.3");
	assert_string_eq(out.tls_name, "node-b.example");
	as_host_destroy(&out);

	// Absent and empty TLS names stay absent and empty.
	assert_int_eq(as_cluster_translate_address(&cluster, &err, "B7", NULL, "10.1.2.3", 3010,
		AS_ENDPOINT_SOURCE_STANDARD, &out), AEROSPIKE_OK);
	assert_null(out.tls_name);
	as_host_destroy(&out);

	assert_int_eq(as_cluster_translate_address(&cluster, &err, "B7", "", "10.1.2.3", 3010,
		AS_ENDPOINT_SOURCE_STANDARD, &out), AEROSPIKE_OK);
	assert_string_eq(out.tls_name, "");
	as_host_destroy(&out);

	// Static map over ip_map also keeps the endpoint TLS identity.
	as_addr_map entry = {"10.1.2.3", "198.51.100.3"};
	as_vector ip_map;
	as_vector_init(&ip_map, sizeof(as_addr_map), 1);
	as_vector_append(&ip_map, &entry);
	memset(&cluster, 0, sizeof(cluster));
	cluster.ip_map = &ip_map;
	assert_int_eq(as_cluster_translate_address(&cluster, &err, "B7", "node-b.example", "10.1.2.3",
		3010, AS_ENDPOINT_SOURCE_STANDARD, &out), AEROSPIKE_OK);
	assert_string_eq(out.name, "198.51.100.3");
	assert_int_eq(out.port, 3010);
	assert_string_eq(out.tls_name, "node-b.example");
	as_host_destroy(&out);
	as_vector_destroy(&ip_map);
}

TEST(translation_invalid_output, "U-9 translator output must be a usable host and port")
{
	as_cluster cluster;
	as_error err;
	as_host out;
	probe p;

	memset(&cluster, 0, sizeof(cluster));
	probe_init(&cluster.translator, &p);
	cluster.translator_registered = true;
	cluster.translation_on_miss = AS_TRANSLATION_PASS_THROUGH;

	// Port 0 cannot be connected to.
	assert_int_eq(as_cluster_translate_address(&cluster, &err, "B7", "tls", "10.1.2.3", 0,
		AS_ENDPOINT_SOURCE_STANDARD, &out), AEROSPIKE_ERR_CLIENT);
	assert_not_null(strstr(err.message, "probe"));
}

TEST(translation_source_unit, "U-13 translators receive the list the endpoint was advertised from")
{
	probe p;
	as_cluster cluster;
	as_error err;
	as_host out;

	memset(&cluster, 0, sizeof(cluster));
	probe_init(&cluster.translator, &p);
	cluster.translator_registered = true;

	assert_int_eq(as_cluster_endpoint_source(&cluster), AS_ENDPOINT_SOURCE_STANDARD);
	cluster.use_services_alternate = true;
	assert_int_eq(as_cluster_endpoint_source(&cluster), AS_ENDPOINT_SOURCE_ALTERNATE);

	assert_int_eq(as_cluster_translate_address(&cluster, &err, "B7", "tls", "h", 3000,
		as_cluster_endpoint_source(&cluster), &out), AEROSPIKE_OK);
	as_host_destroy(&out);
	assert_int_eq(p.last_source, AS_ENDPOINT_SOURCE_ALTERNATE);
	assert_int_eq(p.alternate_calls, 1);
	assert_int_eq(p.standard_calls, 0);

	cluster.use_services_alternate = false;
	assert_int_eq(as_cluster_translate_address(&cluster, &err, "B7", "tls", "h", 3000,
		as_cluster_endpoint_source(&cluster), &out), AEROSPIKE_OK);
	as_host_destroy(&out);
	assert_int_eq(p.last_source, AS_ENDPOINT_SOURCE_STANDARD);
	assert_int_eq(p.standard_calls, 1);
}

TEST(translation_config_helpers, "U-7 native translator helpers validate, copy and hand over ownership")
{
	as_config config;
	as_error err;
	as_addr_map bad[] = {{"h:3000", "h2"}};
	as_addr_map good[] = {{"A1", "h2:4000"}};
	as_endpoint endpoint;
	as_host out;

	as_config_init(&config);

	// An invalid table leaves the configuration untouched.
	assert_int_eq(as_config_set_port_map_translator(&config, bad, 1, &err), AEROSPIKE_ERR_PARAM);
	assert_false(config.translation.provider_registered);

	assert_int_eq(as_config_set_port_map_translator(&config, good, 1, &err), AEROSPIKE_OK);
	assert_true(config.translation.provider_registered);
	assert_string_eq(config.translation.provider.name, "port-map");

	endpoint_init(&endpoint, "A1", "tls", "h", 3000, AS_ENDPOINT_SOURCE_STANDARD);
	assert_true(config.translation.provider.translate(&endpoint, &out,
		config.translation.provider.udata));
	assert_string_eq(out.name, "h2");
	as_host_destroy(&out);

	// Replacing releases the previous table; pass-through has no state.
	assert_int_eq(as_config_set_pass_through_translator(&config, &err), AEROSPIKE_OK);
	assert_string_eq(config.translation.provider.name, "pass-through");
	assert_null(config.translation.provider.destroy);
	assert_true(config.translation.provider.translate(&endpoint, &out,
		config.translation.provider.udata));
	assert_string_eq(out.name, "h");
	as_host_destroy(&out);

	as_config_destroy(&config);
}

//---------------------------------
// Tests: FR-45 certificate identity matching
//---------------------------------

typedef struct san_s {
	int type;
	const char* data;
	int len;
} san;

#define DNS(s) {GEN_DNS, (s), (int)strlen(s)}
#define IP4(a, b, c, d) {GEN_IPADD, (const char[]){(char)(a), (char)(b), (char)(c), (char)(d)}, 4}
#define IP6_DB8_1 {GEN_IPADD, "\x20\x01\x0d\xb8\0\0\0\0\0\0\0\0\0\0\0\x01", 16}

static X509*
cert_create(const char* cn, const san* sans, int n_sans)
{
	X509* cert = X509_new();

	if (!cert) {
		return NULL;
	}

	if (cn) {
		X509_NAME_add_entry_by_NID(X509_get_subject_name(cert), NID_commonName, MBSTRING_ASC,
			(const unsigned char*)cn, -1, -1, 0);
	}

	if (n_sans > 0) {
		GENERAL_NAMES* names = sk_GENERAL_NAME_new_null();

		for (int i = 0; i < n_sans; i++) {
			GENERAL_NAME* name = GENERAL_NAME_new();

			if (sans[i].type == GEN_DNS) {
				ASN1_IA5STRING* s = ASN1_IA5STRING_new();
				ASN1_STRING_set(s, sans[i].data, sans[i].len);
				GENERAL_NAME_set0_value(name, GEN_DNS, s);
			}
			else {
				ASN1_OCTET_STRING* s = ASN1_OCTET_STRING_new();
				ASN1_OCTET_STRING_set(s, (const unsigned char*)sans[i].data, sans[i].len);
				GENERAL_NAME_set0_value(name, GEN_IPADD, s);
			}
			sk_GENERAL_NAME_push(names, name);
		}
		X509_add1_ext_i2d(cert, NID_subject_alt_name, names, 0, X509V3_ADD_DEFAULT);
		sk_GENERAL_NAME_pop_free(names, GENERAL_NAME_free);
	}
	return cert;
}

TEST(translation_tls_dns, "FR-45 DNS identifiers match only exact or ASCII case-insensitive dNSName")
{
	san sans[] = {DNS("Node-A.Example"), DNS("other.example")};
	X509* cert = cert_create(NULL, sans, 2);
	assert_not_null(cert);

	assert_true(as_tls_match_identity_strict(cert, "node-a.example"));
	assert_true(as_tls_match_identity_strict(cert, "NODE-A.EXAMPLE"));
	assert_true(as_tls_match_identity_strict(cert, "other.example"));
	assert_false(as_tls_match_identity_strict(cert, "node-b.example"));
	assert_false(as_tls_match_identity_strict(cert, "node-a.example.evil"));
	assert_false(as_tls_match_identity_strict(cert, "xnode-a.example"));
	assert_false(as_tls_match_identity_strict(cert, ""));
	assert_false(as_tls_match_identity_strict(cert, NULL));
	assert_int_eq(as_tls_match_identity(cert, "node-a.example"), AS_TLS_MATCH_STRICT);
	X509_free(cert);
}

TEST(translation_tls_wildcard, "FR-45 only a sole left-most label wildcard matches one non-empty label")
{
	san sans[] = {DNS("*.example.com")};
	X509* cert = cert_create(NULL, sans, 1);
	assert_not_null(cert);

	assert_true(as_tls_match_identity_strict(cert, "a.example.com"));
	assert_true(as_tls_match_identity_strict(cert, "A-1.Example.COM"));
	assert_false(as_tls_match_identity_strict(cert, "example.com"));
	assert_false(as_tls_match_identity_strict(cert, "a.b.example.com"));
	assert_false(as_tls_match_identity_strict(cert, ".example.com"));
	assert_false(as_tls_match_identity_strict(cert, "a.example.org"));
	X509_free(cert);

	const char* invalid[] = {"a*.example.com", "*a.example.com", "a.*.example.com",
		"*.*.example.com", "**.example.com", "w*w.example.com"};

	for (uint32_t i = 0; i < sizeof(invalid) / sizeof(invalid[0]); i++) {
		san bad[] = {DNS(invalid[i])};
		cert = cert_create(NULL, bad, 1);
		assert_not_null(cert);

		if (as_tls_match_identity_strict(cert, "ab.example.com") ||
			as_tls_match_identity_strict(cert, "a.b.example.com") ||
			as_tls_match_identity_strict(cert, "a.a.example.com")) {
			X509_free(cert);
			assert_log(true, "wildcard shape %s matched", invalid[i]);
		}
		X509_free(cert);
	}
}

TEST(translation_tls_ip, "FR-45 IP identifiers match only iPAddress bytes, never dNSName")
{
	san sans[] = {IP4(10, 1, 2, 3), IP6_DB8_1, DNS("10.9.9.9")};
	X509* cert = cert_create(NULL, sans, 3);
	assert_not_null(cert);

	assert_true(as_tls_match_identity_strict(cert, "10.1.2.3"));
	assert_false(as_tls_match_identity_strict(cert, "10.1.2.4"));
	assert_false(as_tls_match_identity_strict(cert, "010.1.2.3"));
	assert_false(as_tls_match_identity_strict(cert, "10.1.2"));
	assert_true(as_tls_match_identity_strict(cert, "2001:db8::1"));
	assert_true(as_tls_match_identity_strict(cert, "2001:DB8:0:0:0:0:0:1"));
	assert_false(as_tls_match_identity_strict(cert, "2001:db8::2"));

	// An IP literal in a dNSName is not an IP identity.
	assert_false(as_tls_match_identity_strict(cert, "10.9.9.9"));

	// A DNS identifier never matches an iPAddress entry.
	assert_false(as_tls_match_identity_strict(cert, "example.com"));
	X509_free(cert);
}

TEST(translation_tls_malformed, "FR-45 embedded NUL, bad IP lengths and missing SAN never match strictly")
{
	// The dNSName holds "good.example\0.evil.com".
	san nul[] = {{GEN_DNS, "good.example\0.evil.com", 21}};
	X509* cert = cert_create(NULL, nul, 1);
	assert_not_null(cert);
	assert_false(as_tls_match_identity_strict(cert, "good.example"));
	assert_false(as_tls_match_identity_strict(cert, "good.example.evil.com"));
	X509_free(cert);

	// iPAddress entries must be 4 or 16 bytes.
	san ips[] = {{GEN_IPADD, "\x0a\x01\x02\x03\x04", 5}, {GEN_IPADD, "\x0a\x01\x02", 3}};
	cert = cert_create(NULL, ips, 2);
	assert_not_null(cert);
	assert_false(as_tls_match_identity_strict(cert, "10.1.2.3"));
	assert_false(as_tls_match_identity_strict(cert, "10.1.2.3.4"));
	X509_free(cert);

	// Dotted-quad or colon-shaped invalid literals are IP identifiers that cannot match.
	san dns[] = {DNS("1.2.3.256"), DNS("1:2:3")};
	cert = cert_create(NULL, dns, 2);
	assert_not_null(cert);
	assert_false(as_tls_match_identity_strict(cert, "1.2.3.256"));
	assert_false(as_tls_match_identity_strict(cert, "1:2:3"));
	X509_free(cert);

	cert = cert_create("node-a.example", NULL, 0);
	assert_not_null(cert);
	assert_false(as_tls_match_identity_strict(cert, "node-a.example"));
	// A CN-only certificate is still accepted by the deprecated fallback (N-17 stays red).
	assert_int_eq(as_tls_match_identity(cert, "node-a.example"), AS_TLS_MATCH_LEGACY);
	assert_int_eq(as_tls_match_identity(cert, "other.example"), AS_TLS_MATCH_NONE);
	X509_free(cert);
}

TEST(translation_tls_legacy_fallback, "FR-45 legacy CN fallback is flagged, and a full mismatch is rejected")
{
	// A certificate with an unrelated SAN and a matching CN fails strict matching but is still
	// accepted by the deprecated fallback, which is reported as LEGACY so a warning is logged.
	san sans[] = {DNS("other.example")};
	X509* cert = cert_create("legacy-cn.example", sans, 1);
	assert_not_null(cert);

	assert_false(as_tls_match_identity_strict(cert, "legacy-cn.example"));
	assert_int_eq(as_tls_match_identity(cert, "legacy-cn.example"), AS_TLS_MATCH_LEGACY);
	assert_int_eq(as_tls_match_identity(cert, "other.example"), AS_TLS_MATCH_STRICT);
	assert_int_eq(as_tls_match_identity(cert, "mismatch.example"), AS_TLS_MATCH_NONE);
	X509_free(cert);

	// The verifier compares the node TLS name, so a translated transport address cannot match.
	san ip[] = {DNS("node-a.example")};
	cert = cert_create(NULL, ip, 1);
	assert_not_null(cert);
	assert_int_eq(as_tls_match_identity(cert, "node-a.example"), AS_TLS_MATCH_STRICT);
	assert_int_eq(as_tls_match_identity(cert, "198.51.100.1"), AS_TLS_MATCH_NONE);
	X509_free(cert);
}

#ifdef DISCOVERY_FAKE_SERVERS

//---------------------------------
// Static Functions: loopback fake info server
//---------------------------------

#define FAKE_MAX_CLIENTS 32

typedef struct fake_node_s {
	int listen_fd;
	uint16_t port;
	pthread_t thread;
	pthread_mutex_t lock;
	volatile bool stop;
	char name[32];
	char peers_gen[16];
	char peers_std[1024];
	char peers_alt[1024];
	char service_std[256];
	char service_alt[256];
	uint32_t peers_std_requests;
	uint32_t peers_alt_requests;
	uint32_t accepts;
} fake_node;

static size_t
fake_node_reply(fake_node* n, char* body, char* out, size_t cap)
{
	size_t len = 0;
	char* save = NULL;

	pthread_mutex_lock(&n->lock);

	for (char* line = strtok_r(body, "\n", &save); line; line = strtok_r(NULL, "\n", &save)) {
		const char* value = "";

		if (strcmp(line, "node") == 0) {
			value = n->name;
		}
		else if (strcmp(line, "partition-generation") == 0) {
			value = "1";
		}
		else if (strcmp(line, "build") == 0) {
			value = "8.1.0.0";
		}
		else if (strcmp(line, "peers-generation") == 0) {
			value = n->peers_gen;
		}
		else if (strcmp(line, "peers-clear-std") == 0) {
			value = n->peers_std;
			n->peers_std_requests++;
		}
		else if (strcmp(line, "peers-clear-alt") == 0) {
			value = n->peers_alt;
			n->peers_alt_requests++;
		}
		else if (strcmp(line, "service-clear-std") == 0) {
			value = n->service_std;
		}
		else if (strcmp(line, "service-clear-alt") == 0) {
			value = n->service_alt;
		}
		else if (strcmp(line, "rebalance-generation") == 0) {
			value = "0";
		}
		else if (strncmp(line, "user-agent-set", 14) == 0) {
			value = "ok";
		}

		int w = snprintf(out + len, cap - len, "%s\t%s\n", line, value);

		if (w < 0 || (size_t)w >= cap - len) {
			break;
		}
		len += (size_t)w;
	}
	pthread_mutex_unlock(&n->lock);
	return len;
}

static bool
fake_node_serve(fake_node* n, int fd)
{
	uint8_t header[8];

	if (recv(fd, header, sizeof(header), MSG_WAITALL) != (ssize_t)sizeof(header) ||
		header[0] != 2 || header[1] != 1) {
		return false;
	}

	size_t len = 0;

	for (int i = 2; i < 8; i++) {
		len = (len << 8) | header[i];
	}

	char body[4096];

	if (len >= sizeof(body) ||
		(len > 0 && recv(fd, body, len, MSG_WAITALL) != (ssize_t)len)) {
		return false;
	}
	body[len] = 0;

	uint8_t reply[8192];
	size_t reply_len = fake_node_reply(n, body, (char*)reply + 8, sizeof(reply) - 8);

	reply[0] = 2;
	reply[1] = 1;

	for (int i = 0; i < 6; i++) {
		reply[2 + i] = (uint8_t)(reply_len >> (8 * (5 - i)));
	}
	return send(fd, reply, reply_len + 8, MSG_NOSIGNAL) == (ssize_t)(reply_len + 8);
}

static void*
fake_node_run(void* arg)
{
	fake_node* n = arg;
	int clients[FAKE_MAX_CLIENTS];
	int n_clients = 0;

	while (!n->stop) {
		struct pollfd pfds[FAKE_MAX_CLIENTS + 1];
		pfds[0].fd = n->listen_fd;
		pfds[0].events = POLLIN;
		pfds[0].revents = 0;

		for (int i = 0; i < n_clients; i++) {
			pfds[i + 1].fd = clients[i];
			pfds[i + 1].events = POLLIN;
			pfds[i + 1].revents = 0;
		}

		if (poll(pfds, (nfds_t)(n_clients + 1), 20) <= 0) {
			continue;
		}

		for (int i = n_clients - 1; i >= 0; i--) {
			if (pfds[i + 1].revents && !fake_node_serve(n, clients[i])) {
				close(clients[i]);
				clients[i] = clients[--n_clients];
			}
		}

		if ((pfds[0].revents & POLLIN) && n_clients < FAKE_MAX_CLIENTS) {
			int fd = accept(n->listen_fd, NULL, NULL);

			if (fd >= 0) {
				pthread_mutex_lock(&n->lock);
				n->accepts++;
				pthread_mutex_unlock(&n->lock);

				struct timeval tv = {2, 0};
				setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
				clients[n_clients++] = fd;
			}
		}
	}

	for (int i = 0; i < n_clients; i++) {
		close(clients[i]);
	}
	return NULL;
}

static void
fake_node_set_peers_port(fake_node* n, uint32_t gen, uint16_t default_port, const char* std,
	const char* alt)
{
	pthread_mutex_lock(&n->lock);
	snprintf(n->peers_gen, sizeof(n->peers_gen), "%u", gen);
	snprintf(n->peers_std, sizeof(n->peers_std), "%u,%u,%s", gen, default_port, std ? std : "[]");
	snprintf(n->peers_alt, sizeof(n->peers_alt), "%u,%u,%s", gen, default_port, alt ? alt : "[]");
	pthread_mutex_unlock(&n->lock);
}

static void
fake_node_set_peers(fake_node* n, uint32_t gen, const char* std, const char* alt)
{
	fake_node_set_peers_port(n, gen, 3000, std, alt);
}

static void
fake_node_set_service(fake_node* n, const char* std, const char* alt)
{
	pthread_mutex_lock(&n->lock);
	snprintf(n->service_std, sizeof(n->service_std), "%s", std ? std : "");
	snprintf(n->service_alt, sizeof(n->service_alt), "%s", alt ? alt : "");
	pthread_mutex_unlock(&n->lock);
}

static uint32_t
fake_node_requests(fake_node* n, const uint32_t* counter)
{
	pthread_mutex_lock(&n->lock);
	uint32_t value = *counter;
	pthread_mutex_unlock(&n->lock);
	return value;
}

// Start a fake node bound to bind_addr (loopback when NULL) on an ephemeral port.
static bool
fake_node_start_on(fake_node* n, const char* name, const char* bind_addr)
{
	memset(n, 0, sizeof(fake_node));
	pthread_mutex_init(&n->lock, NULL);
	snprintf(n->name, sizeof(n->name), "%s", name);
	fake_node_set_peers(n, 1, NULL, NULL);
	// Alternate addresses of the node itself are only needed in alternate mode.
	fake_node_set_service(n, NULL, "0.0.0.0:1");

	n->listen_fd = socket(AF_INET, SOCK_STREAM, 0);

	if (n->listen_fd < 0) {
		return false;
	}

	int one = 1;
	setsockopt(n->listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

	struct sockaddr_in addr;
	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = 0;
	addr.sin_addr.s_addr = inet_addr(bind_addr ? bind_addr : "127.0.0.1");
	socklen_t addr_len = sizeof(addr);

	if (bind(n->listen_fd, (struct sockaddr*)&addr, sizeof(addr)) != 0 ||
		listen(n->listen_fd, 16) != 0 ||
		getsockname(n->listen_fd, (struct sockaddr*)&addr, &addr_len) != 0) {
		close(n->listen_fd);
		return false;
	}

	n->port = ntohs(addr.sin_port);
	return pthread_create(&n->thread, NULL, fake_node_run, n) == 0;
}

static bool
fake_node_start(fake_node* n, const char* name)
{
	return fake_node_start_on(n, name, NULL);
}

static void
fake_node_stop(fake_node* n)
{
	n->stop = true;
	pthread_join(n->thread, NULL);
	close(n->listen_fd);
	pthread_mutex_destroy(&n->lock);
}

// Return the first up, non-loopback IPv4 address of this machine, or false.
static bool
local_non_loopback_address(char* buf, size_t size)
{
	struct ifaddrs* list = NULL;

	if (getifaddrs(&list) != 0) {
		return false;
	}

	bool found = false;

	for (struct ifaddrs* ifa = list; ifa && !found; ifa = ifa->ifa_next) {
		if (!ifa->ifa_addr || ifa->ifa_addr->sa_family != AF_INET ||
			(ifa->ifa_flags & IFF_LOOPBACK) || !(ifa->ifa_flags & IFF_UP)) {
			continue;
		}

		struct sockaddr_in* sin = (struct sockaddr_in*)ifa->ifa_addr;
		found = inet_ntop(AF_INET, &sin->sin_addr, buf, (socklen_t)size) != NULL;
	}
	freeifaddrs(list);
	return found;
}

//---------------------------------
// Static Functions: client helpers
//---------------------------------

typedef struct fake_client_s {
	aerospike* as;
	as_error err;
	as_status status;
} fake_client;

static void
fake_config(as_config* config, const fake_node* seed, bool alternate)
{
	as_config_init(config);
	as_config_add_host(config, "127.0.0.1", seed->port);
	config->use_services_alternate = alternate;
	config->tender_interval = AS_TEND_INTERVAL_MIN;
	config->conn_timeout_ms = 1000;
	config->fail_if_not_connected = true;
}

static as_status
fake_client_connect(fake_client* c, as_config* config)
{
	c->as = aerospike_new(config);
	c->status = aerospike_connect(c->as, &c->err);
	return c->status;
}

void
aerospike_destroy_internal(aerospike* as);

// Close and destroy the client. Async close also destroys it; a client that never connected
// is destroyed directly so its registrations are released in every mode.
static void
fake_client_close(fake_client* c)
{
	as_error err;

	if (c->as->cluster) {
		aerospike_close(c->as, &err);

		if (as_event_loop_size == 0 || as_event_single_thread) {
			aerospike_destroy(c->as);
		}
	}
	else {
		aerospike_destroy_internal(c->as);
	}
}

static uint32_t
node_count(aerospike* client)
{
	as_nodes* nodes = as_nodes_reserve(client->cluster);
	uint32_t size = nodes->size;
	as_nodes_release(nodes);
	return size;
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

	while (node_count(client) != size) {
		if (cf_getms() >= end) {
			return false;
		}
		as_sleep(20);
	}
	return true;
}

// Wait until the fake node has seen at least count more peers requests than before.
static bool
wait_for_peers_requests(fake_node* n, uint32_t count, uint32_t timeout_ms)
{
	uint32_t target = fake_node_requests(n, &n->peers_std_requests) + count;
	uint64_t end = cf_getms() + timeout_ms;

	while (fake_node_requests(n, &n->peers_std_requests) < target) {
		if (cf_getms() >= end) {
			return false;
		}
		as_sleep(20);
	}
	return true;
}

//---------------------------------
// Static Functions: log capture
//---------------------------------

// The tend thread logs concurrently with the test thread. The first messages are kept, later
// ones are only counted, so early warnings are never overwritten.
#define LOG_CAPTURE_MAX 256
#define LOG_CAPTURE_TEXT 512
#define LOG_CAPTURE_NEEDLES 8

typedef struct log_entry_s {
	as_log_level level;
	char text[LOG_CAPTURE_TEXT];
} log_entry;

static pthread_mutex_t g_capture_lock = PTHREAD_MUTEX_INITIALIZER;
static log_entry g_capture[LOG_CAPTURE_MAX];
static uint32_t g_capture_size;
static uint32_t g_capture_overflow;
static as_log_callback g_capture_saved;

static bool
log_capture_cb(as_log_level level, const char* func, const char* file, uint32_t line,
	const char* fmt, ...)
{
	char text[LOG_CAPTURE_TEXT];
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(text, sizeof(text), fmt, ap);
	va_end(ap);

	pthread_mutex_lock(&g_capture_lock);

	if (g_capture_size < LOG_CAPTURE_MAX) {
		log_entry* entry = &g_capture[g_capture_size++];
		entry->level = level;
		memcpy(entry->text, text, sizeof(text));
	}
	else {
		g_capture_overflow++;
	}

	as_log_callback saved = g_capture_saved;
	pthread_mutex_unlock(&g_capture_lock);

	// Keep the suite output complete.
	if (saved) {
		saved(level, func, file, line, "%s", text);
	}
	return true;
}

static void
log_capture_start(void)
{
	pthread_mutex_lock(&g_capture_lock);
	g_capture_saved = g_as_log.callback;
	g_capture_size = 0;
	g_capture_overflow = 0;
	pthread_mutex_unlock(&g_capture_lock);
	as_log_set_callback(log_capture_cb);
}

static void
log_capture_stop(void)
{
	pthread_mutex_lock(&g_capture_lock);
	as_log_callback saved = g_capture_saved;
	pthread_mutex_unlock(&g_capture_lock);
	as_log_set_callback(saved);
}

// Count the captured messages of a level that contain every substring of the NULL-terminated
// list.
static uint32_t
log_capture_count(as_log_level level, ...)
{
	const char* needles[LOG_CAPTURE_NEEDLES];
	uint32_t n_needles = 0;
	const char* needle;
	va_list ap;
	va_start(ap, level);

	while ((needle = va_arg(ap, const char*)) && n_needles < LOG_CAPTURE_NEEDLES) {
		needles[n_needles++] = needle;
	}
	va_end(ap);

	uint32_t count = 0;
	pthread_mutex_lock(&g_capture_lock);

	for (uint32_t i = 0; i < g_capture_size; i++) {
		if (g_capture[i].level != level) {
			continue;
		}

		bool all = true;

		for (uint32_t k = 0; k < n_needles && all; k++) {
			all = strstr(g_capture[i].text, needles[k]) != NULL;
		}

		if (all) {
			count++;
		}
	}
	pthread_mutex_unlock(&g_capture_lock);
	return count;
}

//---------------------------------
// Tests: alternate completeness and service translation
//---------------------------------

TEST(translation_alt_peer_only_in_standard, "N-14 peer present only in the standard list fails alternate init")
{
	fake_node s, b;
	assert_true(fake_node_start(&s, "S"));
	assert_true(fake_node_start(&b, "B"));

	char std[128];
	format(std, sizeof(std), "[[B,,[127.0.0.1:%u]]]", b.port);
	fake_node_set_peers(&s, 1, std, "[]");

	as_config config;
	fake_client c;
	fake_config(&config, &s, true);

	assert_int_eq(fake_client_connect(&c, &config), AEROSPIKE_ERR_INVALID_NODE);
	assert_not_null(strstr(c.err.message, "Node B"));
	assert_not_null(strstr(c.err.message, "alternate-access-address"));
	assert_not_null(strstr(c.err.message, "use_services_alternate"));
	fake_client_close(&c);

	// Both lists were requested in one command.
	assert_true(fake_node_requests(&s, &s.peers_alt_requests) > 0);

	fake_node_stop(&b);
	fake_node_stop(&s);
}

TEST(translation_alt_empty_host_list, "N-14 empty alternate host list fails alternate init")
{
	fake_node s, b;
	assert_true(fake_node_start(&s, "S"));
	assert_true(fake_node_start(&b, "B"));

	char std[128];
	format(std, sizeof(std), "[[B,,[127.0.0.1:%u]]]", b.port);
	fake_node_set_peers(&s, 1, std, "[[B,,[]]]");

	as_config config;
	fake_client c;
	fake_config(&config, &s, true);

	assert_int_eq(fake_client_connect(&c, &config), AEROSPIKE_ERR_INVALID_NODE);
	assert_not_null(strstr(c.err.message, "Node B"));
	assert_not_null(strstr(c.err.message, "use_services_alternate"));
	fake_client_close(&c);

	fake_node_stop(&b);
	fake_node_stop(&s);
}

TEST(translation_alt_empty_service, "N-14 empty service-clear-alt fails alternate init and names the setting")
{
	fake_node s;
	assert_true(fake_node_start(&s, "S"));
	fake_node_set_service(&s, "127.0.0.1:1", "");

	as_config config;
	fake_client c;
	fake_config(&config, &s, true);

	assert_int_eq(fake_client_connect(&c, &config), AEROSPIKE_ERR_INVALID_NODE);
	assert_not_null(strstr(c.err.message, "alternate-access-address"));
	assert_not_null(strstr(c.err.message, "use_services_alternate"));
	assert_not_null(strstr(c.err.message, "S"));
	fake_client_close(&c);

	fake_node_stop(&s);
}

TEST(translation_alt_empty_service_no_fail, "N-14 without fail_if_not_connected an empty service-clear-alt connects with no nodes and warns")
{
	fake_node s;
	assert_true(fake_node_start(&s, "S"));
	fake_node_set_service(&s, "127.0.0.1:1", "");

	as_config config;
	fake_client c;
	fake_config(&config, &s, true);
	config.fail_if_not_connected = false;

	log_capture_start();
	as_status status = fake_client_connect(&c, &config);
	log_capture_stop();

	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(node_count(c.as), 0);
	assert_true(log_capture_count(AS_LOG_LEVEL_WARN, "use_services_alternate", NULL) > 0);
	fake_client_close(&c);

	fake_node_stop(&s);
}

TEST(translation_alt_missing_after_init, "N-14 without fail_if_not_connected a peer missing from the alternate list is logged on every tend")
{
	fake_node s, b;
	assert_true(fake_node_start(&s, "S"));
	assert_true(fake_node_start(&b, "B"));

	char std[128];
	format(std, sizeof(std), "[[B,,[127.0.0.1:%u]]]", b.port);
	fake_node_set_peers(&s, 1, std, "[]");

	as_config config;
	fake_client c;
	fake_config(&config, &s, true);
	config.fail_if_not_connected = false;

	log_capture_start();
	assert_int_eq(fake_client_connect(&c, &config), AEROSPIKE_OK);
	assert_int_eq(node_count(c.as), 1);
	assert_true(has_node(c.as, "S"));

	// The peers generation is not advanced, so every tend reports B again.
	uint64_t end = cf_getms() + 3000;
	uint32_t errors = 0;

	while ((errors = log_capture_count(AS_LOG_LEVEL_ERROR, "Node B", "use_services_alternate",
		NULL)) < 2 && cf_getms() < end) {
		as_sleep(20);
	}
	log_capture_stop();

	assert_true(errors >= 2);
	assert_false(has_node(c.as, "B"));
	fake_client_close(&c);

	fake_node_stop(&b);
	fake_node_stop(&s);
}

TEST(translation_standard_mode_compat, "standard mode keeps the single-command peers path")
{
	fake_node s, b;
	assert_true(fake_node_start(&s, "S"));
	assert_true(fake_node_start(&b, "B"));

	char std[128];
	format(std, sizeof(std), "[[B,,[127.0.0.1:%u]]]", b.port);
	fake_node_set_peers(&s, 1, std, "[]");
	// An empty alternate service list must not matter in standard mode.
	fake_node_set_service(&s, "", "");

	as_config config;
	fake_client c;
	fake_config(&config, &s, false);

	assert_int_eq(fake_client_connect(&c, &config), AEROSPIKE_OK);
	assert_int_eq(node_count(c.as), 2);
	assert_true(has_node(c.as, "S"));
	assert_true(has_node(c.as, "B"));
	assert_int_eq(fake_node_requests(&s, &s.peers_alt_requests), 0);
	fake_client_close(&c);

	fake_node_stop(&b);
	fake_node_stop(&s);
}

TEST(translation_peers_default_port, "peer hosts without a port use the default port of the reply")
{
	for (int alternate = 0; alternate <= 1; alternate++) {
		fake_node s, b;
		assert_true(fake_node_start(&s, "S"));
		assert_true(fake_node_start(&b, "B"));

		// Servers omit the port of a peer host that listens on the default port.
		fake_node_set_peers_port(&s, 1, b.port, "[[B,,[127.0.0.1]]]", "[[B,,[127.0.0.2,127.0.0.1]]]");

		as_config config;
		fake_client c;
		fake_config(&config, &s, alternate);

		assert_int_eq(fake_client_connect(&c, &config), AEROSPIKE_OK);
		assert_int_eq(node_count(c.as), 2);
		assert_true(has_node(c.as, "B"));
		fake_client_close(&c);

		fake_node_stop(&b);
		fake_node_stop(&s);
	}
}

TEST(translation_alt_peer_empty_service, "N-14 a peer without alternate service addresses fails init and names the setting")
{
	fake_node s, b;
	assert_true(fake_node_start(&s, "S"));
	assert_true(fake_node_start(&b, "B"));

	char list[128];
	format(list, sizeof(list), "[[B,,[127.0.0.1:%u]]]", b.port);
	fake_node_set_peers(&s, 1, list, list);
	fake_node_set_service(&b, "127.0.0.1:1", "");

	as_config config;
	fake_client c;
	fake_config(&config, &s, true);

	assert_int_eq(fake_client_connect(&c, &config), AEROSPIKE_ERR_INVALID_NODE);
	assert_not_null(strstr(c.err.message, "Node B"));
	assert_not_null(strstr(c.err.message, "use_services_alternate"));
	fake_client_close(&c);

	fake_node_stop(&b);
	fake_node_stop(&s);
}

TEST(translation_alt_translated_peer, "N-2 alternate peers are translated and complete against the standard list")
{
	fake_node s, b;
	assert_true(fake_node_start(&s, "S"));
	assert_true(fake_node_start(&b, "B"));

	char std[128], alt[128], key[64], value[64];
	format(std, sizeof(std), "[[B,,[127.0.0.1:%u]]]", b.port);
	format(alt, sizeof(alt), "[[B,,[10.255.0.1:3000]]]");
	fake_node_set_peers(&s, 1, std, alt);

	format(key, sizeof(key), "10.255.0.1:3000");
	format(value, sizeof(value), "127.0.0.1:%u", b.port);
	as_addr_map entries[] = {{key, value}};

	as_config config;
	fake_client c;
	fake_config(&config, &s, true);
	config.translation.on_miss = AS_TRANSLATION_STRICT;
	as_error err;
	assert_int_eq(as_config_set_port_map_translator(&config, entries, 1, &err), AEROSPIKE_OK);

	assert_int_eq(fake_client_connect(&c, &config), AEROSPIKE_OK);
	assert_int_eq(node_count(c.as), 2);
	assert_true(has_node(c.as, "B"));
	fake_client_close(&c);

	fake_node_stop(&b);
	fake_node_stop(&s);
}

TEST(translation_source_runtime, "U-13 peer endpoints carry the source of the selected list")
{
	for (int alternate = 0; alternate <= 1; alternate++) {
		fake_node s, b;
		assert_true(fake_node_start(&s, "S"));
		assert_true(fake_node_start(&b, "B"));

		char list[128];
		format(list, sizeof(list), "[[B,,[127.0.0.1:%u]]]", b.port);
		fake_node_set_peers(&s, 1, list, list);

		as_config config;
		fake_client c;
		as_address_translator translator;
		probe p;
		as_error err;
		fake_config(&config, &s, alternate);
		probe_init(&translator, &p);
		assert_int_eq(as_config_set_address_translator(&config, &translator, &err), AEROSPIKE_OK);

		assert_int_eq(fake_client_connect(&c, &config), AEROSPIKE_OK);
		assert_int_eq(node_count(c.as), 2);

		// The translator is called on the tend thread; the init tend finished before connect.
		if (alternate) {
			assert_true(p.alternate_calls > 0);
			assert_int_eq(p.standard_calls, 0);
			assert_int_eq(p.last_source, AS_ENDPOINT_SOURCE_ALTERNATE);
		}
		else {
			assert_true(p.standard_calls > 0);
			assert_int_eq(p.alternate_calls, 0);
			assert_int_eq(p.last_source, AS_ENDPOINT_SOURCE_STANDARD);
		}
		fake_client_close(&c);
		assert_int_eq(p.destroyed, 1);

		fake_node_stop(&b);
		fake_node_stop(&s);
	}
}

TEST(translation_wrong_list_miss, "U-13 a mapping for the standard address misses an alternate endpoint")
{
	fake_node s, b;
	assert_true(fake_node_start(&s, "S"));
	assert_true(fake_node_start(&b, "B"));

	char std[128], alt[128], key[64], value[64];
	format(std, sizeof(std), "[[B,,[10.0.0.1:3000]]]");
	format(alt, sizeof(alt), "[[B,,[10.255.0.1:3000]]]");
	fake_node_set_peers(&s, 1, std, alt);

	// Only the standard address is mapped, but the client reads the alternate list.
	format(key, sizeof(key), "10.0.0.1:3000");
	format(value, sizeof(value), "127.0.0.1:%u", b.port);
	as_addr_map entries[] = {{key, value}};

	as_config config;
	fake_client c;
	as_error err;
	fake_config(&config, &s, true);
	config.translation.on_miss = AS_TRANSLATION_STRICT;
	assert_int_eq(as_config_set_port_map_translator(&config, entries, 1, &err), AEROSPIKE_OK);

	// A strict miss alone does not fail init. The miss is logged and retried on later tends.
	log_capture_start();
	as_status status = fake_client_connect(&c, &config);
	log_capture_stop();
	uint32_t misses = log_capture_count(AS_LOG_LEVEL_WARN, "port-map", "B", "10.255.0.1:3000",
		"alternate", NULL);

	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(node_count(c.as), 1);
	assert_true(has_node(c.as, "S"));
	assert_true(misses > 0);
	fake_client_close(&c);

	fake_node_stop(&b);
	fake_node_stop(&s);
}

TEST(translation_miss_then_mapped, "N-6 a strict miss on one address is skipped when another maps")
{
	fake_node s, b;
	assert_true(fake_node_start(&s, "S"));
	assert_true(fake_node_start(&b, "B"));

	char std[128], alt[160], key[64], value[64];
	format(std, sizeof(std), "[[B,,[127.0.0.1:%u]]]", b.port);
	format(alt, sizeof(alt), "[[B,,[10.255.0.9:3000,10.255.0.1:3000]]]");
	fake_node_set_peers(&s, 1, std, alt);

	format(key, sizeof(key), "10.255.0.1:3000");
	format(value, sizeof(value), "127.0.0.1:%u", b.port);
	as_addr_map entries[] = {{key, value}};

	as_config config;
	fake_client c;
	as_error err;
	fake_config(&config, &s, true);
	config.translation.on_miss = AS_TRANSLATION_STRICT;
	assert_int_eq(as_config_set_port_map_translator(&config, entries, 1, &err), AEROSPIKE_OK);

	assert_int_eq(fake_client_connect(&c, &config), AEROSPIKE_OK);
	assert_int_eq(node_count(c.as), 2);
	assert_true(has_node(c.as, "B"));
	fake_client_close(&c);

	fake_node_stop(&b);
	fake_node_stop(&s);
}

TEST(translation_miss_retry, "N-6 a strict miss does not advance the peers generation and is retried")
{
	fake_node s, c_node, b;
	assert_true(fake_node_start(&s, "S"));
	assert_true(fake_node_start(&c_node, "C"));
	assert_true(fake_node_start(&b, "B"));

	char list[256], key_c[64], key_b[64];
	char missing[] = "10.255.0.1:3000";
	format(key_c, sizeof(key_c), "127.0.0.1:%u", c_node.port);
	format(key_b, sizeof(key_b), "127.0.0.1:%u", b.port);
	as_addr_map entries[] = {{key_c, key_c}, {key_b, key_b}};

	// C is healthy so initialization succeeds. B is advertised at an unmapped address.
	format(list, sizeof(list), "[[C,,[%s]],[B,,[%s]]]", key_c, missing);
	fake_node_set_peers(&s, 5, list, NULL);

	// C references the seed so that the seed is not dropped as an unreferenced node.
	char seed_list[128];
	format(seed_list, sizeof(seed_list), "[[S,,[127.0.0.1:%u]]]", s.port);
	fake_node_set_peers(&c_node, 1, seed_list, NULL);

	as_config config;
	fake_client c;
	as_error err;
	fake_config(&config, &s, false);
	config.translation.on_miss = AS_TRANSLATION_STRICT;
	assert_int_eq(as_config_set_port_map_translator(&config, entries, 2, &err), AEROSPIKE_OK);

	assert_int_eq(fake_client_connect(&c, &config), AEROSPIKE_OK);
	assert_int_eq(node_count(c.as), 2);
	assert_false(has_node(c.as, "B"));

	// The peers generation is unchanged, so S keeps asking for its peers on every tend.
	assert_true(wait_for_peers_requests(&s, 3, 5000));
	assert_int_eq(node_count(c.as), 2);

	// B becomes reachable through a mapped address and joins on a later tend.
	format(list, sizeof(list), "[[C,,[%s]],[B,,[%s]]]", key_c, key_b);
	fake_node_set_peers(&s, 5, list, NULL);
	assert_true(wait_for_nodes(c.as, 3, 5000));
	assert_true(has_node(c.as, "B"));

	// With the peers generation advanced S goes quiet.
	uint32_t before = fake_node_requests(&s, &s.peers_std_requests);
	as_sleep(1200);
	assert_int_eq(fake_node_requests(&s, &s.peers_std_requests), before);
	fake_client_close(&c);

	fake_node_stop(&b);
	fake_node_stop(&c_node);
	fake_node_stop(&s);
}

//---------------------------------
// Tests: collisions
//---------------------------------

typedef enum collision_kind_e {
	COLLISION_STATIC_MAP,
	COLLISION_PORT_MAP,
	COLLISION_PASS_THROUGH
} collision_kind;

// Two peers of S, B and C, translate to the same host and port. Both orders of the reply are
// tried, with and without a healthy third peer.
static const char*
run_collision(collision_kind kind, bool reversed, bool healthy_peer, as_status expected,
	uint32_t expected_nodes)
{
	fake_node s, b, d;

	if (!fake_node_start(&s, "S") || !fake_node_start(&b, "B") || !fake_node_start(&d, "D")) {
		return "cannot start fake nodes";
	}

	char b_adv[64], c_adv[64], d_adv[64], list[512], shared[64];
	format(shared, sizeof(shared), "127.0.0.1:%u", b.port);
	format(d_adv, sizeof(d_adv), "127.0.0.1:%u", d.port);

	switch (kind) {
	case COLLISION_STATIC_MAP:
		// The legacy ip_map keeps the advertised port and rewrites only the host.
		format(b_adv, sizeof(b_adv), "10.0.0.1:%u", b.port);
		format(c_adv, sizeof(c_adv), "10.0.0.2:%u", b.port);
		break;
	case COLLISION_PORT_MAP:
		format(b_adv, sizeof(b_adv), "10.0.0.1:3000");
		format(c_adv, sizeof(c_adv), "10.0.0.2:3000");
		break;
	default:
		snprintf(b_adv, sizeof(b_adv), "%s", shared);
		snprintf(c_adv, sizeof(c_adv), "%s", shared);
		break;
	}

	const char* first = reversed ? "C" : "B";
	const char* second = reversed ? "B" : "C";
	const char* first_adv = reversed ? c_adv : b_adv;
	const char* second_adv = reversed ? b_adv : c_adv;

	if (healthy_peer) {
		format(list, sizeof(list), "[[%s,,[%s]],[%s,,[%s]],[D,,[%s]]]", first, first_adv, second,
			second_adv, d_adv);
	}
	else {
		format(list, sizeof(list), "[[%s,,[%s]],[%s,,[%s]]]", first, first_adv, second, second_adv);
	}
	fake_node_set_peers(&s, 1, list, NULL);

	as_config config;
	fake_client c;
	as_error err;
	fake_config(&config, &s, false);

	as_addr_map ip_entries[] = {{"10.0.0.1", "127.0.0.1"}, {"10.0.0.2", "127.0.0.1"}};
	as_addr_map port_entries[] = {{"B", shared}, {"C", shared}, {"D", d_adv}};

	if (kind == COLLISION_STATIC_MAP) {
		config.ip_map = ip_entries;
		config.ip_map_size = 2;
	}
	else if (kind == COLLISION_PORT_MAP) {
		as_config_set_port_map_translator(&config, port_entries, 3, &err);
	}
	else {
		as_config_set_pass_through_translator(&config, &err);
	}

	const char* translator = kind == COLLISION_STATIC_MAP ? "static-map" :
		kind == COLLISION_PORT_MAP ? "port-map" : "pass-through";

	// D advertises a distinct host for static-map so that its address is also mapped.
	const char* result = NULL;
	log_capture_start();
	as_status status = fake_client_connect(&c, &config);
	log_capture_stop();
	uint32_t warnings = log_capture_count(AS_LOG_LEVEL_WARN, "translated nodes", "B", "C", shared,
		translator, NULL);
	// One message per colliding peer, each naming the other peer as the first owner.
	uint32_t b_first = log_capture_count(AS_LOG_LEVEL_WARN, "nodes B and C", shared, translator,
		NULL);
	uint32_t c_first = log_capture_count(AS_LOG_LEVEL_WARN, "nodes C and B", shared, translator,
		NULL);

	if (status != expected) {
		result = "unexpected connect status";
	}
	else if (node_count(c.as) != expected_nodes || has_node(c.as, "B") || has_node(c.as, "C") ||
		!has_node(c.as, "S") || (healthy_peer && !has_node(c.as, "D"))) {
		result = "colliding nodes were created";
	}
	else if (warnings == 0) {
		result = "no collision warning names both nodes, the shared address and the translator";
	}
	else if (b_first == 0 || c_first == 0) {
		result = "the collision is not reported once for each colliding peer";
	}

	fake_client_close(&c);
	fake_node_stop(&d);
	fake_node_stop(&b);
	fake_node_stop(&s);
	return result;
}

#define COLLISION_TEST(NAME, KIND, DESC) \
TEST(NAME, DESC) \
{ \
	for (int reversed = 0; reversed <= 1; reversed++) { \
		const char* failure = run_collision(KIND, reversed, false, AEROSPIKE_OK, 1); \
		if (failure) { assert_log(true, "reversed=%d no healthy peer: %s", reversed, failure); } \
		failure = run_collision(KIND, reversed, true, AEROSPIKE_OK, 2); \
		if (failure) { assert_log(true, "reversed=%d healthy peer: %s", reversed, failure); } \
	} \
}

COLLISION_TEST(translation_collision_static_map, COLLISION_STATIC_MAP,
	"U-15 static-map collision drops both newcomers in either reply order")
COLLISION_TEST(translation_collision_port_map, COLLISION_PORT_MAP,
	"U-15 port-map collision drops both newcomers in either reply order")
COLLISION_TEST(translation_collision_pass_through, COLLISION_PASS_THROUGH,
	"U-15 pass-through collision drops both newcomers in either reply order")

TEST(translation_collision_live_node, "U-15 a newcomer colliding with a live node is dropped, the live node stays")
{
	fake_node s, d;
	assert_true(fake_node_start(&s, "S"));
	assert_true(fake_node_start(&d, "D"));

	char d_adv[64], list[256];
	format(d_adv, sizeof(d_adv), "127.0.0.1:%u", d.port);
	format(list, sizeof(list), "[[D,,[%s]]]", d_adv);
	fake_node_set_peers(&s, 1, list, NULL);

	// D references the seed so that the seed is not dropped as an unreferenced node.
	char seed_list[128];
	format(seed_list, sizeof(seed_list), "[[S,,[127.0.0.1:%u]]]", s.port);
	fake_node_set_peers(&d, 1, seed_list, NULL);

	as_config config;
	fake_client c;
	as_error err;
	fake_config(&config, &s, false);
	assert_int_eq(as_config_set_pass_through_translator(&config, &err), AEROSPIKE_OK);
	assert_int_eq(fake_client_connect(&c, &config), AEROSPIKE_OK);
	assert_int_eq(node_count(c.as), 2);

	// E is advertised at the exact address of the live node D, so the check before validation
	// rejects it.
	log_capture_start();
	format(list, sizeof(list), "[[D,,[%s]],[E,,[%s]]]", d_adv, d_adv);
	fake_node_set_peers(&s, 2, list, NULL);
	bool waited = wait_for_peers_requests(&s, 3, 5000);
	as_sleep(300);
	log_capture_stop();
	uint32_t warnings = log_capture_count(AS_LOG_LEVEL_WARN, "translated nodes", "E", "D",
		"pass-through", NULL);

	assert_true(waited);
	assert_true(warnings > 0);
	assert_int_eq(node_count(c.as), 2);
	assert_false(has_node(c.as, "E"));

	// E is advertised at an alias of D's address. The strings differ, so only the check after
	// validation finds that E is the live node D.
	char e_alias[64];
	format(e_alias, sizeof(e_alias), "localhost:%u", d.port);
	log_capture_start();
	format(list, sizeof(list), "[[D,,[%s]],[E,,[%s]]]", d_adv, e_alias);
	fake_node_set_peers(&s, 3, list, NULL);
	waited = wait_for_peers_requests(&s, 3, 5000);
	as_sleep(300);
	log_capture_stop();
	warnings = log_capture_count(AS_LOG_LEVEL_WARN, "translated nodes", "E", "D", "pass-through",
		NULL);
	uint32_t mismatches = log_capture_count(AS_LOG_LEVEL_WARN, "is different than actual node",
		NULL);

	assert_true(waited);
	assert_true(warnings > 0);
	assert_int_eq(mismatches, 0);
	assert_int_eq(node_count(c.as), 2);
	assert_true(has_node(c.as, "S"));
	assert_true(has_node(c.as, "D"));
	assert_false(has_node(c.as, "E"));
	fake_client_close(&c);

	fake_node_stop(&d);
	fake_node_stop(&s);
}

TEST(translation_collision_seed, "U-15 a peer advertised at the seed address is rejected before it joins")
{
	fake_node s;
	assert_true(fake_node_start(&s, "S"));

	char list[128];
	format(list, sizeof(list), "[[B,,[127.0.0.1:%u]]]", s.port);
	fake_node_set_peers(&s, 1, list, NULL);

	as_config config;
	fake_client c;
	as_error err;
	fake_config(&config, &s, false);
	assert_int_eq(as_config_set_pass_through_translator(&config, &err), AEROSPIKE_OK);

	// The collision alone does not fail init: the seed connects and B is retried.
	log_capture_start();
	as_status status = fake_client_connect(&c, &config);
	log_capture_stop();
	uint32_t warnings = log_capture_count(AS_LOG_LEVEL_WARN, "translated nodes", "S", "B",
		"pass-through", NULL);

	assert_int_eq(status, AEROSPIKE_OK);
	assert_true(warnings > 0);
	assert_int_eq(node_count(c.as), 1);
	assert_true(has_node(c.as, "S"));

	assert_true(wait_for_peers_requests(&s, 3, 5000));
	assert_int_eq(node_count(c.as), 1);
	assert_false(has_node(c.as, "B"));
	fake_client_close(&c);

	fake_node_stop(&s);
}

TEST(translation_shared_address_skipped, "U-15 an address shared by peers is never used, peers keep their own addresses")
{
	fake_node s, b, d;
	assert_true(fake_node_start(&s, "S"));
	assert_true(fake_node_start(&b, "B"));
	assert_true(fake_node_start(&d, "D"));

	// Every node also advertises one common address, as with a container bridge interface and
	// no access-address. Port 1 refuses connections, so it can never validate either node.
	const char* shared = "127.0.0.1:1";
	char list[256];
	format(list, sizeof(list), "[[B,,[%s,127.0.0.1:%u]],[D,,[127.0.0.1:%u,%s]]]", shared, b.port,
		d.port, shared);
	fake_node_set_peers(&s, 1, list, NULL);

	// B and D reference the seed so that the seed is not dropped as an unreferenced node.
	char seed_list[128];
	format(seed_list, sizeof(seed_list), "[[S,,[127.0.0.1:%u]]]", s.port);
	fake_node_set_peers(&b, 1, seed_list, NULL);
	fake_node_set_peers(&d, 1, seed_list, NULL);

	as_config config;
	fake_client c;
	fake_config(&config, &s, false);

	// The default static-map path only drops the shared address, so legacy configurations keep
	// working. translation_shared_address_registered covers registered translators.
	assert_int_eq(fake_client_connect(&c, &config), AEROSPIKE_OK);
	assert_int_eq(node_count(c.as), 3);
	assert_true(has_node(c.as, "B"));
	assert_true(has_node(c.as, "D"));

	// Nothing collided, so the peers generation advanced and S goes quiet.
	uint32_t before = fake_node_requests(&s, &s.peers_std_requests);
	as_sleep(1200);
	assert_int_eq(fake_node_requests(&s, &s.peers_std_requests), before);
	assert_int_eq(node_count(c.as), 3);
	fake_client_close(&c);

	// D has no address of its own left, so it collides and is not created. B still joins.
	format(list, sizeof(list), "[[B,,[%s,127.0.0.1:%u]],[D,,[%s]]]", shared, b.port, shared);
	fake_node_set_peers(&s, 1, list, NULL);
	fake_config(&config, &s, false);

	assert_int_eq(fake_client_connect(&c, &config), AEROSPIKE_OK);
	assert_int_eq(node_count(c.as), 2);
	assert_true(has_node(c.as, "B"));
	assert_false(has_node(c.as, "D"));

	// The collision keeps the peers generation, so it is retried on later tends.
	assert_true(wait_for_peers_requests(&s, 3, 5000));
	assert_int_eq(node_count(c.as), 2);
	assert_false(has_node(c.as, "D"));
	fake_client_close(&c);

	fake_node_stop(&d);
	fake_node_stop(&b);
	fake_node_stop(&s);
}

TEST(translation_shared_address_registered, "U-15 with a registered translator a peer with any shared address is not created")
{
	fake_node s, b, d;
	assert_true(fake_node_start(&s, "S"));
	assert_true(fake_node_start(&b, "B"));
	assert_true(fake_node_start(&d, "D"));

	// The same reply as translation_shared_address_skipped: each peer has an address of its own.
	const char* shared = "127.0.0.1:1";
	char list[256];
	format(list, sizeof(list), "[[B,,[%s,127.0.0.1:%u]],[D,,[127.0.0.1:%u,%s]]]", shared, b.port,
		d.port, shared);
	fake_node_set_peers(&s, 1, list, NULL);

	char seed_list[128];
	format(seed_list, sizeof(seed_list), "[[S,,[127.0.0.1:%u]]]", s.port);
	fake_node_set_peers(&b, 1, seed_list, NULL);
	fake_node_set_peers(&d, 1, seed_list, NULL);

	as_config config;
	fake_client c;
	as_error err;
	fake_config(&config, &s, false);
	assert_int_eq(as_config_set_pass_through_translator(&config, &err), AEROSPIKE_OK);

	log_capture_start();
	as_status status = fake_client_connect(&c, &config);
	log_capture_stop();
	uint32_t warnings = log_capture_count(AS_LOG_LEVEL_WARN, "translated nodes", "B", "D", shared,
		"pass-through", NULL);

	assert_int_eq(status, AEROSPIKE_OK);
	assert_true(warnings > 0);
	assert_int_eq(node_count(c.as), 1);
	assert_true(has_node(c.as, "S"));

	assert_true(wait_for_peers_requests(&s, 3, 5000));
	assert_int_eq(node_count(c.as), 1);
	assert_false(has_node(c.as, "B"));
	assert_false(has_node(c.as, "D"));
	fake_client_close(&c);

	fake_node_stop(&d);
	fake_node_stop(&b);
	fake_node_stop(&s);
}

// Two peers, B and C, are advertised at different strings that reach the one server B. The
// strings differ, so only the check after validation finds the collision.
static const char*
run_collision_alias(bool reversed, bool pass_through)
{
	fake_node s, b;

	if (!fake_node_start(&s, "S") || !fake_node_start(&b, "B")) {
		return "cannot start fake nodes";
	}

	char b_adv[64], c_adv[64], list[256];
	format(b_adv, sizeof(b_adv), "localhost:%u", b.port);
	format(c_adv, sizeof(c_adv), "127.0.0.1:%u", b.port);

	if (reversed) {
		format(list, sizeof(list), "[[C,,[%s]],[B,,[%s]]]", c_adv, b_adv);
	}
	else {
		format(list, sizeof(list), "[[B,,[%s]],[C,,[%s]]]", b_adv, c_adv);
	}
	fake_node_set_peers(&s, 1, list, NULL);

	// Min connections would reach B if a dropped newcomer had any.
	as_config config;
	fake_client c;
	as_error err;
	fake_config(&config, &s, false);
	config.min_conns_per_node = 2;
#if AS_EVENT_LIB_DEFINED
	config.async_min_conns_per_node = 2;
#endif

	if (pass_through) {
		as_config_set_pass_through_translator(&config, &err);
	}

	const char* result = NULL;
	log_capture_start();
	as_status status = fake_client_connect(&c, &config);
	log_capture_stop();
	uint32_t warnings = log_capture_count(AS_LOG_LEVEL_WARN, "translated nodes", "B", "C", NULL);

	if (status != AEROSPIKE_OK) {
		result = "unexpected connect status";
	}
	else if (node_count(c.as) != 1 || !has_node(c.as, "S") || has_node(c.as, "B") ||
		has_node(c.as, "C")) {
		result = "an alias peer was created";
	}
	else if (warnings == 0) {
		result = "no collision warning names both peers";
	}
	else if (!wait_for_peers_requests(&s, 3, 5000)) {
		result = "the seed was not tended";
	}
	else if (has_node(c.as, "B") || has_node(c.as, "C")) {
		result = "an alias peer joined on a later tend";
	}
	else {
		// Each peers reply of S makes one validation connection per alias at B, and node creation
		// reuses the validation socket. Any min connection to B breaks the bound. The tender keeps
		// running, so accepts is read first.
		uint32_t accepts = fake_node_requests(&b, &b.accepts);
		uint32_t requests = fake_node_requests(&s, &s.peers_std_requests);

		if (accepts > 2 * requests) {
			result = "a dropped newcomer had min connections";
		}
	}

	fake_client_close(&c);
	fake_node_stop(&b);
	fake_node_stop(&s);
	return result;
}

TEST(translation_collision_alias, "U-15 two peers that reach one server through aliases create neither node")
{
	for (int pass_through = 0; pass_through <= 1; pass_through++) {
		for (int reversed = 0; reversed <= 1; reversed++) {
			const char* failure = run_collision_alias(reversed, pass_through);

			if (failure) {
				assert_log(true, "pass_through=%d reversed=%d: %s", pass_through, reversed, failure);
			}
		}
	}
}

TEST(translation_distinct_ports_one_host, "U-15 peers on one host with distinct ports do not collide")
{
	fake_node s, b, d;
	assert_true(fake_node_start(&s, "S"));
	assert_true(fake_node_start(&b, "B"));
	assert_true(fake_node_start(&d, "D"));

	char list[256];
	format(list, sizeof(list), "[[B,,[127.0.0.1:%u]],[D,,[127.0.0.1:%u]]]", b.port, d.port);
	fake_node_set_peers(&s, 1, list, NULL);

	as_config config;
	fake_client c;
	as_error err;
	fake_config(&config, &s, false);
	assert_int_eq(as_config_set_pass_through_translator(&config, &err), AEROSPIKE_OK);

	assert_int_eq(fake_client_connect(&c, &config), AEROSPIKE_OK);
	assert_int_eq(node_count(c.as), 3);
	fake_client_close(&c);

	fake_node_stop(&d);
	fake_node_stop(&b);
	fake_node_stop(&s);
}

//---------------------------------
// Tests: TLS identity at runtime and peers parsing
//---------------------------------

#define WARN_ONCE_NAMES 20

TEST(translation_tls_legacy_warn_once, "FR-45 the legacy name check warns once per full name, without a limit")
{
	// The names are distinct from the names that earlier tests already warned about.
	char names[WARN_ONCE_NAMES][64];
	as_tls_match_kind kinds[WARN_ONCE_NAMES][2];
	san sans[] = {DNS("other.example")};

	log_capture_start();

	for (uint32_t i = 0; i < WARN_ONCE_NAMES; i++) {
		format(names[i], sizeof(names[i]), "warn-once-%u.example", i + 1);
		X509* cert = cert_create(names[i], sans, 1);
		kinds[i][0] = cert ? as_tls_match_identity(cert, names[i]) : AS_TLS_MATCH_NONE;
		kinds[i][1] = cert ? as_tls_match_identity(cert, names[i]) : AS_TLS_MATCH_NONE;
		X509_free(cert);
	}
	log_capture_stop();

	for (uint32_t i = 0; i < WARN_ONCE_NAMES; i++) {
		char quoted[80];
		format(quoted, sizeof(quoted), "'%s'", names[i]);

		assert_int_eq(kinds[i][0], AS_TLS_MATCH_LEGACY);
		assert_int_eq(kinds[i][1], AS_TLS_MATCH_LEGACY);
		assert_int_eq(log_capture_count(AS_LOG_LEVEL_WARN, "legacy name check", quoted, NULL), 1);
	}
}

TEST(translation_peer_tls_name_runtime, "FR-40 a translated peer keeps the TLS name advertised in peers")
{
	fake_node s, b;
	assert_true(fake_node_start(&s, "S"));
	assert_true(fake_node_start(&b, "B"));

	fake_node_set_peers(&s, 1, "[[B,b.tls,[10.0.0.1:3000]]]", NULL);

	char seed_list[128], value[64];
	format(seed_list, sizeof(seed_list), "[[S,,[127.0.0.1:%u]]]", s.port);
	fake_node_set_peers(&b, 1, seed_list, NULL);

	format(value, sizeof(value), "127.0.0.1:%u", b.port);
	as_addr_map entries[] = {{"10.0.0.1:3000", value}};

	as_config config;
	fake_client c;
	as_error err;
	fake_config(&config, &s, false);
	assert_int_eq(as_config_set_port_map_translator(&config, entries, 1, &err), AEROSPIKE_OK);

	assert_int_eq(fake_client_connect(&c, &config), AEROSPIKE_OK);
	assert_int_eq(node_count(c.as), 2);

	// The node keeps its TLS name even though the socket is clear.
	as_nodes* nodes = as_nodes_reserve(c.as->cluster);
	as_node* node = NULL;

	for (uint32_t i = 0; i < nodes->size; i++) {
		if (strcmp(nodes->array[i]->name, "B") == 0) {
			node = nodes->array[i];
		}
	}

	bool tls_name = node && node->tls_name && strcmp(node->tls_name, "b.tls") == 0;
	uint16_t port = node ?
		as_address_port((struct sockaddr*)&as_node_get_address(node)->addr) : 0;
	as_nodes_release(nodes);

	assert_true(tls_name);
	assert_int_eq(port, b.port);
	fake_client_close(&c);

	fake_node_stop(&b);
	fake_node_stop(&s);
}

TEST(translation_peers_parser_inputs, "bracketed IPv6 peer hosts are translated, truncated peers fail the seed")
{
	fake_node s, b;
	assert_true(fake_node_start(&s, "S"));
	assert_true(fake_node_start(&b, "B"));

	// The peers parser and the port-map key parser both remove the brackets.
	fake_node_set_peers(&s, 1, "[[B,,[[2001:db8::1]:3000]]]", NULL);

	char seed_list[128], value[64];
	format(seed_list, sizeof(seed_list), "[[S,,[127.0.0.1:%u]]]", s.port);
	fake_node_set_peers(&b, 1, seed_list, NULL);

	format(value, sizeof(value), "127.0.0.1:%u", b.port);
	as_addr_map entries[] = {{"[2001:db8::1]:3000", value}};

	as_config config;
	fake_client c;
	as_error err;
	fake_config(&config, &s, false);
	assert_int_eq(as_config_set_port_map_translator(&config, entries, 1, &err), AEROSPIKE_OK);

	assert_int_eq(fake_client_connect(&c, &config), AEROSPIKE_OK);
	assert_int_eq(node_count(c.as), 2);
	assert_true(has_node(c.as, "B"));
	fake_client_close(&c);

	// The peers list is missing its closing bracket, so the only seed is dropped.
	fake_node_set_peers(&s, 1, "[[B,,[127.0.0.1:1]]", NULL);
	fake_config(&config, &s, false);

	assert_int_eq(fake_client_connect(&c, &config), AEROSPIKE_ERR_CLIENT);
	fake_client_close(&c);

	fake_node_stop(&b);
	fake_node_stop(&s);
}

//---------------------------------
// Tests: service translation for load balancer seeds
//---------------------------------

// Loopback seeds are never treated as load balancers, so these tests need a routable local
// address. They are skipped, loudly, where the machine has none.
static bool
service_seed_start(fake_node* seed, fake_node* real, char* seed_host, size_t size)
{
	if (!local_non_loopback_address(seed_host, size)) {
		info("skipped: no non-loopback IPv4 address for a load balancer seed");
		return false;
	}
	return fake_node_start_on(seed, "N", seed_host) && fake_node_start(real, "N");
}

TEST(translation_service_translated, "N-8 translated service address replaces a load balancer seed")
{
	fake_node seed, real;
	char seed_host[64];
	memset(&seed, 0, sizeof(seed));

	if (!service_seed_start(&seed, &real, seed_host, sizeof(seed_host))) {
		return;
	}

	char adv[64], key[64], value[64];
	format(adv, sizeof(adv), "10.1.1.1:%u", real.port);
	fake_node_set_service(&seed, adv, NULL);
	fake_node_set_service(&real, adv, NULL);

	format(key, sizeof(key), "%s", adv);
	format(value, sizeof(value), "127.0.0.1:%u", real.port);
	as_addr_map entries[] = {{key, value}};

	as_config config;
	fake_client c;
	as_error err;
	as_config_init(&config);
	as_config_add_host(&config, seed_host, seed.port);
	config.tender_interval = AS_TEND_INTERVAL_MIN;
	config.conn_timeout_ms = 1000;
	config.translation.on_miss = AS_TRANSLATION_STRICT;
	assert_int_eq(as_config_set_port_map_translator(&config, entries, 1, &err), AEROSPIKE_OK);

	assert_int_eq(fake_client_connect(&c, &config), AEROSPIKE_OK);
	assert_int_eq(node_count(c.as), 1);

	as_nodes* nodes = as_nodes_reserve(c.as->cluster);
	as_address* address = as_node_get_address(nodes->array[0]);
	assert_int_eq(as_address_port((struct sockaddr*)&address->addr), real.port);
	as_nodes_release(nodes);
	fake_client_close(&c);

	fake_node_stop(&real);
	fake_node_stop(&seed);
}

TEST(translation_service_strict_miss, "N-8 strict service misses are skipped, and the seed is kept when all miss")
{
	fake_node seed, real;
	char seed_host[64];
	memset(&seed, 0, sizeof(seed));

	if (!service_seed_start(&seed, &real, seed_host, sizeof(seed_host))) {
		return;
	}

	char adv[160], key[64], value[64];
	format(key, sizeof(key), "10.1.1.1:%u", real.port);
	format(value, sizeof(value), "127.0.0.1:%u", real.port);
	as_addr_map entries[] = {{key, value}};

	// The first advertised address misses, the second maps.
	format(adv, sizeof(adv), "10.9.9.9:1,%s", key);
	fake_node_set_service(&seed, adv, NULL);
	fake_node_set_service(&real, adv, NULL);

	as_config config;
	fake_client c;
	as_error err;
	as_config_init(&config);
	as_config_add_host(&config, seed_host, seed.port);
	config.tender_interval = AS_TEND_INTERVAL_MIN;
	config.conn_timeout_ms = 1000;
	config.translation.on_miss = AS_TRANSLATION_STRICT;
	assert_int_eq(as_config_set_port_map_translator(&config, entries, 1, &err), AEROSPIKE_OK);

	assert_int_eq(fake_client_connect(&c, &config), AEROSPIKE_OK);
	as_nodes* nodes = as_nodes_reserve(c.as->cluster);
	as_address* address = as_node_get_address(nodes->array[0]);
	assert_int_eq(as_address_port((struct sockaddr*)&address->addr), real.port);
	as_nodes_release(nodes);
	fake_client_close(&c);

	// Every advertised address misses: the misses are skipped, as if access-address were not
	// configured, and the seed keeps its configured address.
	fake_node_set_service(&seed, "10.9.9.9:1,10.9.9.8:1", NULL);
	as_config_init(&config);
	as_config_add_host(&config, seed_host, seed.port);
	config.tender_interval = AS_TEND_INTERVAL_MIN;
	config.conn_timeout_ms = 1000;
	config.translation.on_miss = AS_TRANSLATION_STRICT;
	assert_int_eq(as_config_set_port_map_translator(&config, entries, 1, &err), AEROSPIKE_OK);

	log_capture_start();
	as_status status = fake_client_connect(&c, &config);
	log_capture_stop();
	uint32_t infos = log_capture_count(AS_LOG_LEVEL_INFO, "access-address is probably not configured",
		NULL);

	assert_int_eq(status, AEROSPIKE_OK);
	assert_true(infos > 0);
	assert_int_eq(node_count(c.as), 1);
	nodes = as_nodes_reserve(c.as->cluster);
	address = as_node_get_address(nodes->array[0]);
	assert_int_eq(as_address_port((struct sockaddr*)&address->addr), seed.port);
	as_nodes_release(nodes);
	fake_client_close(&c);

	fake_node_stop(&real);
	fake_node_stop(&seed);
}

//---------------------------------
// Tests: view change notification
//---------------------------------

typedef struct view_s {
	volatile uint32_t calls;
	volatile uint32_t last_generation;
	volatile uint32_t out_of_sequence;
	volatile bool thread_set;
	volatile bool thread_changed;
	pthread_t thread;
	uint32_t destroyed;
} view;

static bool
view_translate(const as_endpoint* endpoint, as_host* out, void* udata)
{
	(void)udata;
	as_host_copy_fields(out, endpoint->host, NULL, endpoint->port);
	return true;
}

static void
view_changed(uint32_t generation, void* udata)
{
	view* v = udata;
	pthread_t self = pthread_self();

	if (generation != v->last_generation + 1) {
		v->out_of_sequence++;
	}
	v->last_generation = generation;

	if (!v->thread_set) {
		v->thread = self;
		v->thread_set = true;
	}
	else if (!pthread_equal(v->thread, self)) {
		v->thread_changed = true;
	}
	v->calls++;
}

static void
view_destroy(void* udata)
{
	((view*)udata)->destroyed++;
}

static void
view_init(as_address_translator* translator, view* v, bool with_callback)
{
	memset(v, 0, sizeof(view));
	memset(translator, 0, sizeof(as_address_translator));
	translator->size = sizeof(as_address_translator);
	translator->api_version = AS_DISCOVERY_API_VERSION;
	translator->name = "view";
	translator->translate = view_translate;
	translator->on_cluster_view_changed = with_callback ? view_changed : NULL;
	translator->destroy = view_destroy;
	translator->udata = v;
}

// Wait until the view callback has been called count times, or give up.
static bool
wait_for_view_calls(view* v, uint32_t count, uint32_t timeout_ms)
{
	uint64_t end = cf_getms() + timeout_ms;

	while (v->calls < count) {
		if (cf_getms() >= end) {
			return false;
		}
		as_sleep(20);
	}
	return true;
}

TEST(translation_view_notifications, "view changes notify once per tend, never at init or on a no-op")
{
	fake_node s, d, e, f;
	assert_true(fake_node_start(&s, "S"));
	assert_true(fake_node_start(&d, "D"));
	assert_true(fake_node_start(&e, "E"));
	assert_true(fake_node_start(&f, "F"));

	char d_adv[64], e_adv[64], f_adv[64], list[512];
	format(d_adv, sizeof(d_adv), "127.0.0.1:%u", d.port);
	format(e_adv, sizeof(e_adv), "127.0.0.1:%u", e.port);
	format(f_adv, sizeof(f_adv), "127.0.0.1:%u", f.port);
	format(list, sizeof(list), "[[D,,[%s]]]", d_adv);
	fake_node_set_peers(&s, 1, list, NULL);

	// Every peer references the seed so that it is not dropped as an unreferenced node.
	char seed_list[128];
	format(seed_list, sizeof(seed_list), "[[S,,[127.0.0.1:%u]]]", s.port);
	fake_node_set_peers(&d, 1, seed_list, NULL);
	fake_node_set_peers(&e, 1, seed_list, NULL);
	fake_node_set_peers(&f, 1, seed_list, NULL);

	as_config config;
	fake_client c;
	as_address_translator translator;
	view v;
	as_error err;
	fake_config(&config, &s, false);
	view_init(&translator, &v, true);
	assert_int_eq(as_config_set_address_translator(&config, &translator, &err), AEROSPIKE_OK);

	assert_int_eq(fake_client_connect(&c, &config), AEROSPIKE_OK);
	assert_true(c.as->cluster->translation_view_callback);
	assert_int_eq(node_count(c.as), 2);

	// Initialization never notifies, and neither does a quiet tend.
	assert_int_eq(v.calls, 0);
	as_sleep(1000);
	assert_int_eq(v.calls, 0);

	// A new peers generation with the same view is not a view change.
	fake_node_set_peers(&s, 2, list, NULL);
	as_sleep(1000);
	assert_int_eq(v.calls, 0);

	// Node added.
	format(list, sizeof(list), "[[D,,[%s]],[E,,[%s]]]", d_adv, e_adv);
	fake_node_set_peers(&s, 3, list, NULL);
	assert_true(wait_for_view_calls(&v, 1, 5000));
	as_sleep(1000);
	assert_int_eq(v.calls, 1);
	assert_int_eq(v.last_generation, 1);
	assert_int_eq(node_count(c.as), 3);
	assert_int_eq(c.as->cluster->translation_view_generation, 1);

	// Advertised address change only: the node set is the same.
	char d_two[128];
	format(d_two, sizeof(d_two), "%s,10.9.9.9:3000", d_adv);
	format(list, sizeof(list), "[[D,,[%s]],[E,,[%s]]]", d_two, e_adv);
	fake_node_set_peers(&s, 4, list, NULL);
	assert_true(wait_for_view_calls(&v, 2, 5000));
	as_sleep(1000);
	assert_int_eq(v.calls, 2);
	assert_int_eq(node_count(c.as), 3);

	// Several changes in one reply produce one notification: F joins and E's address changes.
	char e_two[128];
	format(e_two, sizeof(e_two), "%s,10.9.9.8:3000", e_adv);
	format(list, sizeof(list), "[[D,,[%s]],[E,,[%s]],[F,,[%s]]]", d_two, e_two, f_adv);
	fake_node_set_peers(&s, 5, list, NULL);
	assert_true(wait_for_view_calls(&v, 3, 5000));
	as_sleep(1000);
	assert_int_eq(v.calls, 3);
	assert_int_eq(node_count(c.as), 4);

	// Node removed.
	format(list, sizeof(list), "[[D,,[%s]],[E,,[%s]]]", d_two, e_two);
	fake_node_set_peers(&s, 6, list, NULL);
	assert_true(wait_for_view_calls(&v, 4, 5000));
	as_sleep(1000);
	assert_int_eq(v.calls, 4);
	assert_int_eq(node_count(c.as), 3);

	// Generations are consecutive and always delivered on the tend thread.
	assert_int_eq(v.out_of_sequence, 0);
	assert_int_eq(v.last_generation, 4);
	assert_true(v.thread_set);
	assert_false(v.thread_changed);
	assert_false(pthread_equal(v.thread, pthread_self()));

	fake_client_close(&c);
	assert_int_eq(v.destroyed, 1);

	fake_node_stop(&f);
	fake_node_stop(&e);
	fake_node_stop(&d);
	fake_node_stop(&s);
}

TEST(translation_view_not_tracked, "no snapshots or notifications without a callback")
{
	for (int custom = 0; custom <= 1; custom++) {
		fake_node s, d, e;
		assert_true(fake_node_start(&s, "S"));
		assert_true(fake_node_start(&d, "D"));
		assert_true(fake_node_start(&e, "E"));

		char d_adv[64], e_adv[64], list[256];
		format(d_adv, sizeof(d_adv), "127.0.0.1:%u", d.port);
		format(e_adv, sizeof(e_adv), "127.0.0.1:%u", e.port);
		format(list, sizeof(list), "[[D,,[%s]]]", d_adv);
		fake_node_set_peers(&s, 1, list, NULL);

		char seed_list[128];
		format(seed_list, sizeof(seed_list), "[[S,,[127.0.0.1:%u]]]", s.port);
		fake_node_set_peers(&d, 1, seed_list, NULL);
		fake_node_set_peers(&e, 1, seed_list, NULL);

		as_config config;
		fake_client c;
		as_address_translator translator;
		view v;
		as_error err;
		fake_config(&config, &s, false);

		if (custom) {
			view_init(&translator, &v, false);
			assert_int_eq(as_config_set_address_translator(&config, &translator, &err),
				AEROSPIKE_OK);
		}

		assert_int_eq(fake_client_connect(&c, &config), AEROSPIKE_OK);
		assert_false(c.as->cluster->translation_view_callback);

		format(list, sizeof(list), "[[D,,[%s]],[E,,[%s]]]", d_adv, e_adv);
		fake_node_set_peers(&s, 2, list, NULL);
		assert_true(wait_for_nodes(c.as, 3, 5000));
		assert_int_eq(c.as->cluster->translation_view_generation, 0);

		as_nodes* nodes = as_nodes_reserve(c.as->cluster);

		for (uint32_t i = 0; i < nodes->size; i++) {
			assert_null(nodes->array[i]->advertised_hosts);
		}
		as_nodes_release(nodes);
		fake_client_close(&c);

		fake_node_stop(&e);
		fake_node_stop(&d);
		fake_node_stop(&s);
	}
}

TEST(translation_ip_map_ignored, "ip_map is ignored and a warning logged while a translator is registered")
{
	fake_node s;
	assert_true(fake_node_start(&s, "S"));
	as_error err;

	// The seed is not rewritten: nothing listens on 127.0.0.2.
	{
		as_addr_map map[] = {{"127.0.0.1", "127.0.0.2"}};
		as_config config;
		fake_client c;
		fake_config(&config, &s, false);
		assert_int_eq(as_config_set_pass_through_translator(&config, &err), AEROSPIKE_OK);
		config.ip_map = map;
		config.ip_map_size = 1;

		log_capture_start();
		as_status status = fake_client_connect(&c, &config);
		log_capture_stop();

		assert_int_eq(status, AEROSPIKE_OK);
		assert_true(has_node(c.as, "S"));
		assert_null(c.as->cluster->ip_map);
		assert_int_eq(log_capture_count(AS_LOG_LEVEL_WARN, "ip_map is ignored", "pass-through",
			NULL), 1);
		fake_client_close(&c);
	}

	// Without a translator ip_map rewrites the seed, which is never resolved, and nothing warns.
	{
		as_addr_map map[] = {{"seed.invalid", "127.0.0.1"}};
		as_config config;
		fake_client c;
		as_config_init(&config);
		as_config_add_host(&config, "seed.invalid", s.port);
		config.tender_interval = AS_TEND_INTERVAL_MIN;
		config.conn_timeout_ms = 1000;
		config.fail_if_not_connected = true;
		config.ip_map = map;
		config.ip_map_size = 1;

		log_capture_start();
		as_status status = fake_client_connect(&c, &config);
		log_capture_stop();

		assert_int_eq(status, AEROSPIKE_OK);
		assert_true(has_node(c.as, "S"));
		assert_not_null(c.as->cluster->ip_map);
		assert_int_eq(log_capture_count(AS_LOG_LEVEL_WARN, "ip_map", NULL), 0);
		fake_client_close(&c);
	}

	// An empty ip_map is not reported.
	{
		as_addr_map map[] = {{"127.0.0.1", "127.0.0.2"}};
		as_config config;
		fake_client c;
		fake_config(&config, &s, false);
		assert_int_eq(as_config_set_pass_through_translator(&config, &err), AEROSPIKE_OK);
		config.ip_map = map;
		config.ip_map_size = 0;

		log_capture_start();
		as_status status = fake_client_connect(&c, &config);
		log_capture_stop();

		assert_int_eq(status, AEROSPIKE_OK);
		assert_null(c.as->cluster->ip_map);
		assert_int_eq(log_capture_count(AS_LOG_LEVEL_WARN, "ip_map", NULL), 0);
		fake_client_close(&c);
	}

	fake_node_stop(&s);
}

#endif // DISCOVERY_FAKE_SERVERS

//---------------------------------
// Test Suite
//---------------------------------

SUITE(discovery_translation, "pluggable discovery translation, topology and TLS tests")
{
	suite_add(translation_port_map_grammar);
	suite_add(translation_port_map_lookup);
	suite_add(translation_port_map_ipv6);
	suite_add(translation_translator_latency);
	suite_add(translation_strict_error_content);
	suite_add(translation_tls_name_preserved);
	suite_add(translation_invalid_output);
	suite_add(translation_source_unit);
	suite_add(translation_config_helpers);
	suite_add(translation_tls_dns);
	suite_add(translation_tls_wildcard);
	suite_add(translation_tls_ip);
	suite_add(translation_tls_malformed);
	suite_add(translation_tls_legacy_fallback);

#ifdef DISCOVERY_FAKE_SERVERS
	suite_add(translation_alt_peer_only_in_standard);
	suite_add(translation_alt_empty_host_list);
	suite_add(translation_alt_empty_service);
	suite_add(translation_alt_empty_service_no_fail);
	suite_add(translation_alt_missing_after_init);
	suite_add(translation_standard_mode_compat);
	suite_add(translation_peers_default_port);
	suite_add(translation_alt_peer_empty_service);
	suite_add(translation_alt_translated_peer);
	suite_add(translation_source_runtime);
	suite_add(translation_wrong_list_miss);
	suite_add(translation_miss_then_mapped);
	suite_add(translation_miss_retry);
	suite_add(translation_collision_static_map);
	suite_add(translation_collision_port_map);
	suite_add(translation_collision_pass_through);
	suite_add(translation_collision_live_node);
	suite_add(translation_collision_seed);
	suite_add(translation_shared_address_skipped);
	suite_add(translation_shared_address_registered);
	suite_add(translation_collision_alias);
	suite_add(translation_distinct_ports_one_host);
	suite_add(translation_tls_legacy_warn_once);
	suite_add(translation_peer_tls_name_runtime);
	suite_add(translation_peers_parser_inputs);
	suite_add(translation_service_translated);
	suite_add(translation_service_strict_miss);
	suite_add(translation_view_notifications);
	suite_add(translation_view_not_tracked);
	suite_add(translation_ip_map_ignored);
#endif
}
