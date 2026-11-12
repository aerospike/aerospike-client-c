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
 * L1 conformance runner: runs the aerospike-client-conformance fixture set through the C
 * built-ins and writes the shared report (schema/report.schema.json of that repository).
 *
 *   conformance_runner --fixtures <dir> --declarations <file> --report <path>
 *
 * Every *.yaml under <dir>/discovery and <dir>/tls is a fixture file; each row is run through a
 * public C API (translate, collision and merge through the conformance entry points, dns and
 * aws-tags through the built-in providers against fixture-server.py, tls through
 * as_tls_match_identity()) or reported blocked. Nothing is skipped. The actual result of every
 * row is compared with its declaration in <file> (c-declared.yaml); a row not declared there is
 * declared pass, or blocked when the fixture row is blocked.
 *
 * Exit codes: 0 when every actual result equals its declaration and the report was written; 1 on
 * any mismatch (the report is still written); 2 on a usage, I/O, YAML, fixture, declaration or
 * fixture-server error, which leaves no report at <path>.
 *
 * Built against installed headers only by "make conformance"; uses the @private
 * as_tls_match_identity() for the tls rows. Linux only (posix_spawnp, sockets).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <getopt.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <spawn.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <aerospike/as_config.h>
#include <aerospike/as_discovery.h>
#include <aerospike/as_error.h>
#include <aerospike/as_host.h>
#include <aerospike/as_log.h>
#include <aerospike/as_tls.h>
#include <aerospike/as_vector.h>
#include <aerospike/version.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <yaml.h>

extern char** environ;

//---------------------------------
// Macros
//---------------------------------

#define FIXTURE_SET_VERSION "1.1.0"
#define RUNTIME "c"

enum { EXIT_MATCH = 0, EXIT_MISMATCH = 1, EXIT_ERROR = 2 };

#define FIXTURE_SERVER "discovery/providers/fixture-server.py"
#define FIXTURE_SERVER_LOG "conformance-fixture-server.log"
#define FIXTURE_SERVER_START_MS 10000
#define SELECT_TIMEOUT_MS 5000
#define PROVIDER_TIMEOUT_MS 2000

#define ACCESS_KEY "AKIDCONFORMANCE00000"
#define SECRET_KEY "conformance-secret-key-not-real"
#define SESSION_TOKEN "conformance-session-token-not-real"

#define FACT_SIZE 512

//---------------------------------
// Types
//---------------------------------

typedef struct sb_s {
	char* data;
	size_t len;
	size_t cap;
} sb;

typedef enum y_type_e {
	Y_NULL,
	Y_BOOL,
	Y_INT,
	Y_STRING,
	Y_SEQ,
	Y_MAP
} y_type;

typedef struct fixture_file_s {
	char* name;            // Path from the fixture root, the file's name in reports.
	char* dir;             // Directory of the file, for tls certificates.
	yaml_document_t* doc;
} fixture_file;

typedef struct row_s {
	fixture_file* file;
	const char* kind;
	const char* name;
	const char* blocked_by;
	yaml_node_t* node;
	const char* declared;
	const char* declared_reason;
	bool has_declaration;
	const char* result;
	char* reason;
	char* key;             // file:case, the report order.
} row;

typedef struct outcome_s {
	const char* result;
	char* reason;
} outcome;

typedef struct cert_s {
	char* path;
	X509* x509;
} cert;

typedef struct host_keys_s {
	char** keys;
	uint32_t size;
} host_keys;

//---------------------------------
// Globals
//---------------------------------

// Results are compared by pointer, so each is one object.
static const char RESULT_PASS[] = "pass";
static const char RESULT_FAIL[] = "fail";
static const char RESULT_UNSUPPORTED[] = "unsupported";
static const char RESULT_BLOCKED[] = "blocked";
static const char* const RESULTS[] = {RESULT_PASS, RESULT_FAIL, RESULT_UNSUPPORTED, RESULT_BLOCKED};

static const char* const KINDS[] = {"translate", "collision", "merge", "tls", "dns", "aws-tags"};

static const char* g_fixtures;
static const char* g_declarations;
static const char* g_report;
static char* g_report_dir;

static fixture_file* g_files;
static uint32_t g_files_size;
static row* g_rows;
static uint32_t g_rows_size;
static yaml_document_t* g_decl_doc;

// Document and row of the lookups in progress, for node access and error messages.
static yaml_document_t* g_doc;
static const char* g_where;

static cert* g_certs;
static uint32_t g_certs_size;

static pid_t g_server_pid;
static uint16_t g_server_http;
static uint16_t g_server_dns;

static char** g_walk;
static uint32_t g_walk_size;

//---------------------------------
// Errors
//---------------------------------

static void
stop_fixture_server(void)
{
	if (g_server_pid <= 0) {
		return;
	}

	kill(g_server_pid, SIGTERM);

	while (waitpid(g_server_pid, NULL, 0) < 0 && errno == EINTR) {
	}
	g_server_pid = 0;
}

// Exit 2. No report exists yet, and the fixture server is stopped by the atexit handler.
static void
die(const char* fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	fprintf(stderr, "conformance_runner: error: ");

	if (g_where) {
		fprintf(stderr, "%s: ", g_where);
	}
	vfprintf(stderr, fmt, ap);
	fputc('\n', stderr);
	va_end(ap);
	exit(EXIT_ERROR);
}

static void*
xmalloc(size_t size)
{
	void* p = malloc(size ? size : 1);

	if (!p) {
		die("out of memory");
	}
	return p;
}

static void*
xcalloc(size_t n, size_t size)
{
	void* p = calloc(n ? n : 1, size ? size : 1);

	if (!p) {
		die("out of memory");
	}
	return p;
}

static void*
xrealloc(void* p, size_t size)
{
	p = realloc(p, size ? size : 1);

	if (!p) {
		die("out of memory");
	}
	return p;
}

static char*
xstrdup(const char* s)
{
	char* p = strdup(s);

	if (!p) {
		die("out of memory");
	}
	return p;
}

static char*
xasprintf(const char* fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	char* s;
	int rv = vasprintf(&s, fmt, ap);
	va_end(ap);

	if (rv < 0) {
		die("out of memory");
	}
	return s;
}

static int
compare_strings(const void* a, const void* b)
{
	return strcmp(*(char* const*)a, *(char* const*)b);
}

static uint64_t
now_ms(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

//---------------------------------
// String builder
//---------------------------------

static void
sb_raw(sb* b, const char* s, size_t n)
{
	if (b->len + n + 1 > b->cap) {
		size_t cap = b->cap ? b->cap : 256;

		while (b->len + n + 1 > cap) {
			cap *= 2;
		}
		b->data = xrealloc(b->data, cap);
		b->cap = cap;
	}
	memcpy(b->data + b->len, s, n);
	b->len += n;
	b->data[b->len] = '\0';
}

static void
sb_text(sb* b, const char* s)
{
	sb_raw(b, s, strlen(s));
}

static void
sb_printf(sb* b, const char* fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	char* s;
	int rv = vasprintf(&s, fmt, ap);
	va_end(ap);

	if (rv < 0) {
		die("out of memory");
	}
	sb_raw(b, s, (size_t)rv);
	free(s);
}

// JSON string. Only '"', '\' and control bytes are escaped; every other byte, UTF-8 included, is
// written as is.
static void
sb_quote(sb* b, const char* s)
{
	sb_raw(b, "\"", 1);

	for (const unsigned char* p = (const unsigned char*)s; *p; p++) {
		switch (*p) {
		case '"':
			sb_text(b, "\\\"");
			break;
		case '\\':
			sb_text(b, "\\\\");
			break;
		case '\n':
			sb_text(b, "\\n");
			break;
		case '\r':
			sb_text(b, "\\r");
			break;
		case '\t':
			sb_text(b, "\\t");
			break;
		default:
			if (*p < 0x20) {
				sb_printf(b, "\\u%04x", (unsigned)*p);
			}
			else {
				sb_raw(b, (const char*)p, 1);
			}
		}
	}
	sb_raw(b, "\"", 1);
}

//---------------------------------
// YAML
//---------------------------------

static yaml_document_t*
yaml_load_file(const char* path, bool required)
{
	FILE* fp = fopen(path, "rb");

	if (!fp) {
		if (!required && errno == ENOENT) {
			return NULL;
		}
		die("cannot open %s: %s", path, strerror(errno));
	}

	yaml_parser_t parser;

	if (!yaml_parser_initialize(&parser)) {
		die("cannot initialize the YAML parser");
	}
	yaml_parser_set_input_file(&parser, fp);

	yaml_document_t* doc = xmalloc(sizeof(yaml_document_t));

	if (!yaml_parser_load(&parser, doc)) {
		die("%s:%zu:%zu: invalid YAML: %s", path, parser.problem_mark.line + 1,
			parser.problem_mark.column + 1, parser.problem ? parser.problem : "unknown error");
	}
	yaml_parser_delete(&parser);
	fclose(fp);
	return doc;
}

static void
yaml_free_doc(yaml_document_t* doc)
{
	if (doc) {
		yaml_document_delete(doc);
		free(doc);
	}
}

static yaml_node_t*
y_node(yaml_node_item_t index)
{
	return yaml_document_get_node(g_doc, index);
}

static const char*
y_text(const yaml_node_t* n)
{
	return (const char*)n->data.scalar.value;
}

static bool
y_is_int_text(const char* s)
{
	if (*s == '-' || *s == '+') {
		s++;
	}

	if (!*s) {
		return false;
	}

	for (; *s; s++) {
		if (*s < '0' || *s > '9') {
			return false;
		}
	}
	return true;
}

// The YAML core schema for plain scalars; a quoted scalar is always a string.
static y_type
y_typeof(const yaml_node_t* n)
{
	if (!n) {
		return Y_NULL;
	}

	if (n->type == YAML_SEQUENCE_NODE) {
		return Y_SEQ;
	}

	if (n->type == YAML_MAPPING_NODE) {
		return Y_MAP;
	}

	if (n->type != YAML_SCALAR_NODE || n->data.scalar.style != YAML_PLAIN_SCALAR_STYLE) {
		return Y_STRING;
	}

	const char* s = y_text(n);

	if (!*s || !strcmp(s, "~") || !strcmp(s, "null") || !strcmp(s, "Null") ||
		!strcmp(s, "NULL")) {
		return Y_NULL;
	}

	if (!strcmp(s, "true") || !strcmp(s, "True") || !strcmp(s, "TRUE") ||
		!strcmp(s, "false") || !strcmp(s, "False") || !strcmp(s, "FALSE")) {
		return Y_BOOL;
	}

	if (y_is_int_text(s)) {
		return Y_INT;
	}
	return Y_STRING;
}

static uint32_t
y_len(const yaml_node_t* n)
{
	if (n->type == YAML_SEQUENCE_NODE) {
		return (uint32_t)(n->data.sequence.items.top - n->data.sequence.items.start);
	}

	if (n->type == YAML_MAPPING_NODE) {
		return (uint32_t)(n->data.mapping.pairs.top - n->data.mapping.pairs.start);
	}
	return 0;
}

static yaml_node_t*
y_at(const yaml_node_t* seq, uint32_t i)
{
	return y_node(seq->data.sequence.items.start[i]);
}

static yaml_node_t*
y_key_at(const yaml_node_t* map, uint32_t i)
{
	return y_node(map->data.mapping.pairs.start[i].key);
}

static yaml_node_t*
y_value_at(const yaml_node_t* map, uint32_t i)
{
	return y_node(map->data.mapping.pairs.start[i].value);
}

static yaml_node_t*
y_get(const yaml_node_t* map, const char* key)
{
	if (!map || map->type != YAML_MAPPING_NODE) {
		return NULL;
	}

	uint32_t size = y_len(map);

	for (uint32_t i = 0; i < size; i++) {
		yaml_node_t* k = y_key_at(map, i);

		if (k->type == YAML_SCALAR_NODE && !strcmp(y_text(k), key)) {
			return y_value_at(map, i);
		}
	}
	return NULL;
}

static long long
y_int_value(const yaml_node_t* n, const char* what)
{
	if (y_typeof(n) != Y_INT) {
		die("%s must be an integer", what);
	}

	errno = 0;
	char* end;
	long long v = strtoll(y_text(n), &end, 10);

	if (errno || *end) {
		die("%s is out of range", what);
	}
	return v;
}

static const char*
y_str_value(const yaml_node_t* n, const char* what)
{
	if (y_typeof(n) != Y_STRING) {
		die("%s must be a string", what);
	}
	return y_text(n);
}

static yaml_node_t*
need_map(const yaml_node_t* map, const char* key)
{
	yaml_node_t* n = y_get(map, key);

	if (y_typeof(n) != Y_MAP) {
		die("%s must be a map", key);
	}
	return n;
}

static yaml_node_t*
opt_map(const yaml_node_t* map, const char* key)
{
	yaml_node_t* n = y_get(map, key);
	y_type t = y_typeof(n);

	if (t == Y_NULL) {
		return NULL;
	}

	if (t != Y_MAP) {
		die("%s must be a map", key);
	}
	return n;
}

static yaml_node_t*
need_seq(const yaml_node_t* map, const char* key)
{
	yaml_node_t* n = y_get(map, key);

	if (y_typeof(n) != Y_SEQ) {
		die("%s must be a list", key);
	}
	return n;
}

static const char*
need_str(const yaml_node_t* map, const char* key)
{
	return y_str_value(y_get(map, key), key);
}

// NULL when the key is absent or null.
static const char*
opt_str(const yaml_node_t* map, const char* key)
{
	yaml_node_t* n = y_get(map, key);
	return y_typeof(n) == Y_NULL ? NULL : y_str_value(n, key);
}

static bool
opt_int(const yaml_node_t* map, const char* key, long long* out)
{
	yaml_node_t* n = y_get(map, key);

	if (y_typeof(n) == Y_NULL) {
		return false;
	}
	*out = y_int_value(n, key);
	return true;
}

static uint16_t
port_value(const yaml_node_t* n, const char* what)
{
	long long v = y_int_value(n, what);

	if (v < 0 || v > 65535) {
		die("%s %lld is not a port", what, v);
	}
	return (uint16_t)v;
}

static uint16_t
need_port(const yaml_node_t* map, const char* key)
{
	return port_value(y_get(map, key), key);
}

static uint32_t
opt_uint32(const yaml_node_t* map, const char* key, uint32_t otherwise)
{
	long long v;

	if (!opt_int(map, key, &v)) {
		return otherwise;
	}

	if (v < 0 || v > UINT32_MAX) {
		die("%s %lld is out of range", key, v);
	}
	return (uint32_t)v;
}

// Text of a string or integer fact; integers as their decimal text.
static void
fact_text(const yaml_node_t* n, char* buf, size_t size)
{
	switch (y_typeof(n)) {
	case Y_STRING:
		snprintf(buf, size, "%s", y_text(n));
		break;
	case Y_INT:
		snprintf(buf, size, "%lld", y_int_value(n, "fact"));
		break;
	default:
		die("a messageContains value must be a string or an integer");
	}
}

// JSON of a YAML node, typed by y_typeof().
static void
y_json(sb* b, const yaml_node_t* n)
{
	switch (y_typeof(n)) {
	case Y_NULL:
		sb_text(b, "null");
		break;
	case Y_BOOL:
		sb_text(b, (y_text(n)[0] == 't' || y_text(n)[0] == 'T') ? "true" : "false");
		break;
	case Y_INT:
		sb_printf(b, "%lld", y_int_value(n, "integer"));
		break;
	case Y_STRING:
		sb_quote(b, y_text(n));
		break;
	case Y_SEQ: {
		uint32_t size = y_len(n);
		sb_raw(b, "[", 1);

		for (uint32_t i = 0; i < size; i++) {
			if (i) {
				sb_raw(b, ",", 1);
			}
			y_json(b, y_at(n, i));
		}
		sb_raw(b, "]", 1);
		break;
	}
	case Y_MAP: {
		uint32_t size = y_len(n);
		sb_raw(b, "{", 1);

		for (uint32_t i = 0; i < size; i++) {
			yaml_node_t* k = y_key_at(n, i);

			if (k->type != YAML_SCALAR_NODE) {
				die("a map key must be a scalar");
			}

			if (i) {
				sb_raw(b, ",", 1);
			}
			sb_quote(b, y_text(k));
			sb_raw(b, ":", 1);
			y_json(b, y_value_at(n, i));
		}
		sb_raw(b, "}", 1);
		break;
	}
	}
}

//---------------------------------
// Outcomes
//---------------------------------

static outcome
outcome_pass(void)
{
	return (outcome){RESULT_PASS, NULL};
}

static outcome
outcome_vmake(const char* result, const char* fmt, va_list ap)
{
	char* s;

	if (vasprintf(&s, fmt, ap) < 0) {
		die("out of memory");
	}

	// The schema requires a non-empty reason.
	if (!*s) {
		free(s);
		s = xstrdup("no reason given");
	}
	return (outcome){result, s};
}

static outcome
outcome_fail(const char* fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	outcome o = outcome_vmake(RESULT_FAIL, fmt, ap);
	va_end(ap);
	return o;
}

static outcome
outcome_unsupported(const char* fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	outcome o = outcome_vmake(RESULT_UNSUPPORTED, fmt, ap);
	va_end(ap);
	return o;
}

// The message of a failed call, or the status when it has none.
static const char*
error_text(const as_error* err)
{
	return err->message[0] ? err->message : as_error_string(err->code);
}

//---------------------------------
// Facts
//---------------------------------

static bool
is_host_char(char c)
{
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
		c == '.' || c == '-' || c == ':';
}

// endpoint ("<host>:<port>") as a whole token of message, so that port 3000 does not match 30001
// and host 0.0.1 does not match 10.0.0.1.
static bool
contains_endpoint(const char* message, const char* endpoint)
{
	size_t len = strlen(endpoint);

	for (const char* p = strstr(message, endpoint); p; p = strstr(p + 1, endpoint)) {
		char after = p[len];

		if ((p == message || !is_host_char(p[-1])) && !(after >= '0' && after <= '9')) {
			return true;
		}
	}
	return false;
}

// Return the first fact of messageContains missing from message, or NULL. A map's host and port
// are matched together as a <host>:<port> token; every other value, and each element of a list
// value, is matched as a substring.
static const char*
missing_fact(const char* message, const yaml_node_t* facts, char* buf)
{
	if (!facts || y_typeof(facts) == Y_NULL) {
		return NULL;
	}

	if (y_typeof(facts) == Y_SEQ) {
		uint32_t size = y_len(facts);

		for (uint32_t i = 0; i < size; i++) {
			fact_text(y_at(facts, i), buf, FACT_SIZE);

			if (!strstr(message, buf)) {
				return buf;
			}
		}
		return NULL;
	}

	if (y_typeof(facts) != Y_MAP) {
		die("messageContains must be a map or a list");
	}

	yaml_node_t* host = y_get(facts, "host");
	yaml_node_t* port = y_get(facts, "port");
	bool endpoint = host && port;

	if (endpoint) {
		char h[FACT_SIZE - 64];
		char p[32];
		fact_text(host, h, sizeof(h));
		fact_text(port, p, sizeof(p));
		snprintf(buf, FACT_SIZE, "%s:%s", h, p);

		if (!contains_endpoint(message, buf)) {
			return buf;
		}
	}

	uint32_t size = y_len(facts);

	for (uint32_t i = 0; i < size; i++) {
		const char* key = y_text(y_key_at(facts, i));
		yaml_node_t* value = y_value_at(facts, i);

		if (endpoint && (!strcmp(key, "host") || !strcmp(key, "port"))) {
			continue;
		}

		if (y_typeof(value) == Y_SEQ) {
			uint32_t n = y_len(value);

			for (uint32_t k = 0; k < n; k++) {
				fact_text(y_at(value, k), buf, FACT_SIZE);

				if (!strstr(message, buf)) {
					return buf;
				}
			}
			continue;
		}

		fact_text(value, buf, FACT_SIZE);

		if (!strstr(message, buf)) {
			return buf;
		}
	}
	return NULL;
}

//---------------------------------
// Translation config
//---------------------------------

typedef struct translation_s {
	as_conformance_translation conf;
	as_addr_map* entries;
} translation;

static as_addr_map*
addr_map_entries(const yaml_node_t* map, uint32_t* size)
{
	*size = map ? y_len(map) : 0;
	as_addr_map* entries = xcalloc(*size, sizeof(as_addr_map));

	for (uint32_t i = 0; i < *size; i++) {
		yaml_node_t* k = y_key_at(map, i);
		yaml_node_t* v = y_value_at(map, i);

		if (k->type != YAML_SCALAR_NODE || v->type != YAML_SCALAR_NODE ||
			y_typeof(v) == Y_NULL) {
			die("a map entry must map a scalar to a scalar");
		}
		entries[i].orig = (char*)k->data.scalar.value;
		entries[i].alt = (char*)v->data.scalar.value;
	}
	return entries;
}

// Return NULL, or the name of a translator C does not implement.
static const char*
translation_init(translation* t, const yaml_node_t* config)
{
	memset(t, 0, sizeof(translation));
	t->conf.on_miss = AS_TRANSLATION_PASS_THROUGH;

	yaml_node_t* tr = opt_map(config, "translation");

	if (!tr) {
		// The implicit static map over ip_map, also with no map at all.
		t->conf.kind = AS_TRANSLATOR_STATIC_MAP;
		t->entries = addr_map_entries(opt_map(config, "ip_map"), &t->conf.map_size);
		t->conf.map = t->entries;
		return NULL;
	}

	const char* provider = need_str(tr, "provider");
	const char* on_miss = opt_str(tr, "on_miss");

	if (on_miss) {
		if (!strcmp(on_miss, "strict")) {
			t->conf.on_miss = AS_TRANSLATION_STRICT;
		}
		else if (strcmp(on_miss, "pass_through")) {
			die("unknown on_miss %s", on_miss);
		}
	}

	if (!strcmp(provider, "static-map")) {
		t->conf.kind = AS_TRANSLATOR_STATIC_MAP;
		t->entries = addr_map_entries(opt_map(tr, "static_map"), &t->conf.map_size);
	}
	else if (!strcmp(provider, "port-map")) {
		t->conf.kind = AS_TRANSLATOR_PORT_MAP;
		t->entries = addr_map_entries(opt_map(tr, "port_map"), &t->conf.map_size);
	}
	else if (!strcmp(provider, "pass-through")) {
		t->conf.kind = AS_TRANSLATOR_PASS_THROUGH;
	}
	else {
		return provider;
	}
	t->conf.map = t->entries;
	return NULL;
}

static void
endpoint_init(as_endpoint* e, const yaml_node_t* input)
{
	if (y_typeof(input) != Y_MAP) {
		die("an advertised endpoint must be a map");
	}

	const char* source = need_str(input, "sourceList");

	e->node_name = need_str(input, "nodeName");
	e->tls_name = opt_str(input, "tlsName");
	e->host = need_str(input, "host");
	e->port = need_port(input, "port");

	if (!strcmp(source, "standard")) {
		e->source = AS_ENDPOINT_SOURCE_STANDARD;
	}
	else if (!strcmp(source, "alternate")) {
		e->source = AS_ENDPOINT_SOURCE_ALTERNATE;
	}
	else {
		die("unknown sourceList %s", source);
	}
}

static const char*
null_text(const char* s)
{
	return s ? s : "null";
}

//---------------------------------
// Rows: translate
//---------------------------------

static outcome
run_translate(const row* r)
{
	yaml_node_t* config = need_map(r->node, "config");
	yaml_node_t* input = need_map(r->node, "input");
	yaml_node_t* expect = need_map(r->node, "expect");

	translation t;
	const char* unsupported = translation_init(&t, config);

	if (unsupported) {
		free(t.entries);
		return outcome_unsupported("translator %s is not implemented", unsupported);
	}

	as_endpoint endpoint;
	endpoint_init(&endpoint, input);

	yaml_node_t* error = opt_map(expect, "error");
	as_error err;
	as_host out;
	as_status status = as_discovery_conformance_translate(&err, &t.conf, &endpoint, &out);
	free(t.entries);

	if (status == AEROSPIKE_ERR_TRANSLATION_MISS) {
		if (!error || strcmp(need_str(error, "category"), "translation-miss")) {
			return outcome_fail("translation miss: %s", error_text(&err));
		}

		char buf[FACT_SIZE];
		const char* missing = missing_fact(err.message, y_get(error, "messageContains"), buf);

		if (missing) {
			return outcome_fail("message does not contain %s: %s", missing, err.message);
		}
		return outcome_pass();
	}

	if (status != AEROSPIKE_OK) {
		return outcome_fail("translate failed with status %d: %s", (int)status, error_text(&err));
	}

	char got[1024];
	snprintf(got, sizeof(got), "%s %u %s", out.name, (unsigned)out.port, null_text(out.tls_name));
	as_host_destroy(&out);

	if (error) {
		return outcome_fail("expected %s error, got %s", need_str(error, "category"), got);
	}

	char expected[1024];
	snprintf(expected, sizeof(expected), "%s %u %s", need_str(expect, "host"),
		(unsigned)need_port(expect, "port"), null_text(opt_str(expect, "tlsName")));

	if (strcmp(expected, got)) {
		return outcome_fail("expected %s, got %s", expected, got);
	}
	return outcome_pass();
}

//---------------------------------
// Rows: collision
//---------------------------------

static bool
collision_reports(const char* message, const yaml_node_t* error)
{
	char buf[FACT_SIZE];

	if (y_typeof(error) != Y_MAP) {
		die("an expected error must be a map");
	}

	if (strcmp(need_str(error, "category"), "translation-collision")) {
		return false;
	}
	return !missing_fact(message, y_get(error, "messageContains"), buf);
}

static void
join_strings(sb* b, char** items, uint32_t size)
{
	sb_raw(b, "[", 1);

	for (uint32_t i = 0; i < size; i++) {
		if (i) {
			sb_text(b, ", ");
		}
		sb_text(b, items[i]);
	}
	sb_raw(b, "]", 1);
}

static outcome
run_collision(const row* r)
{
	yaml_node_t* config = need_map(r->node, "config");
	yaml_node_t* input = need_seq(r->node, "input");
	yaml_node_t* expect = need_map(r->node, "expect");
	yaml_node_t* errors = need_seq(expect, "errors");
	yaml_node_t* nodes = need_seq(expect, "nodes");

	translation t;
	const char* unsupported = translation_init(&t, config);

	if (unsupported) {
		free(t.entries);
		return outcome_unsupported("translator %s is not implemented", unsupported);
	}

	uint32_t size = y_len(input);
	as_endpoint* peers = xcalloc(size, sizeof(as_endpoint));

	for (uint32_t i = 0; i < size; i++) {
		endpoint_init(&peers[i], y_at(input, i));
	}

	for (uint32_t i = 1; i < size; i++) {
		if (peers[i].source != peers[0].source) {
			free(peers);
			free(t.entries);
			return outcome_fail("one peers reply cannot mix standard and alternate peers");
		}
	}

	as_error err;
	as_conformance_collisions out;
	as_status status = as_discovery_conformance_collision(&err, &t.conf, peers, size, &out);
	free(peers);
	free(t.entries);

	if (status != AEROSPIKE_OK) {
		return outcome_fail("collision check failed with status %d: %s", (int)status,
			error_text(&err));
	}

	outcome o = outcome_pass();
	uint32_t errors_size = y_len(errors);

	for (uint32_t i = 0; i < errors_size && o.result == RESULT_PASS; i++) {
		yaml_node_t* error = y_at(errors, i);
		bool found = false;

		for (uint32_t k = 0; k < out.errors_size && !found; k++) {
			found = collision_reports(out.errors[k], error);
		}

		if (!found) {
			sb b = {0};
			y_json(&b, y_get(error, "messageContains"));
			sb_text(&b, ", got ");
			join_strings(&b, out.errors, out.errors_size);
			o = outcome_fail("no %s reported naming %s", need_str(error, "category"), b.data);
			free(b.data);
		}
	}

	for (uint32_t k = 0; k < out.errors_size && o.result == RESULT_PASS; k++) {
		bool found = false;

		for (uint32_t i = 0; i < errors_size && !found; i++) {
			found = collision_reports(out.errors[k], y_at(errors, i));
		}

		if (!found) {
			o = outcome_fail("unexpected collision: %s", out.errors[k]);
		}
	}

	if (o.result == RESULT_PASS) {
		uint32_t expected_size = y_len(nodes);
		char** expected = xcalloc(expected_size, sizeof(char*));

		for (uint32_t i = 0; i < expected_size; i++) {
			expected[i] = (char*)y_str_value(y_at(nodes, i), "nodes");
		}
		qsort(expected, expected_size, sizeof(char*), compare_strings);

		// out.nodes is NULL when no peer becomes a node, and qsort() requires a non-NULL base.
		if (out.nodes_size > 1) {
			qsort(out.nodes, out.nodes_size, sizeof(char*), compare_strings);
		}

		bool equal = expected_size == out.nodes_size;

		for (uint32_t i = 0; i < expected_size && equal; i++) {
			equal = !strcmp(expected[i], out.nodes[i]);
		}

		if (!equal) {
			sb b = {0};
			join_strings(&b, expected, expected_size);
			sb_text(&b, ", got ");
			join_strings(&b, out.nodes, out.nodes_size);
			o = outcome_fail("expected nodes %s", b.data);
			free(b.data);
		}
		free(expected);
	}
	as_conformance_collisions_destroy(&out);
	return o;
}

//---------------------------------
// Rows: merge
//---------------------------------

// Hosts of a YAML list of {host, port, tlsName}; the strings are borrowed from the document.
static void
hosts_init(as_vector* v, const yaml_node_t* list)
{
	if (y_typeof(list) != Y_SEQ) {
		die("a host list must be a list");
	}

	uint32_t size = y_len(list);
	as_vector_init(v, sizeof(as_host), size ? size : 1);

	for (uint32_t i = 0; i < size; i++) {
		yaml_node_t* h = y_at(list, i);

		if (y_typeof(h) != Y_MAP) {
			die("a host must be a map");
		}

		as_host host = {
			.name = (char*)need_str(h, "host"),
			.tls_name = (char*)opt_str(h, "tlsName"),
			.port = need_port(h, "port")
		};
		as_vector_append(v, &host);
	}
}

static void
hosts_text(sb* b, const as_vector* hosts)
{
	sb_raw(b, "[", 1);

	for (uint32_t i = 0; i < hosts->size; i++) {
		const as_host* h = as_vector_get((as_vector*)hosts, i);
		sb_printf(b, "%s%s:%u", i ? ", " : "", h->name, (unsigned)h->port);
	}
	sb_raw(b, "]", 1);
}

static as_seed_merge_policy
merge_policy(const yaml_node_t* config)
{
	const char* policy = opt_str(opt_map(config, "discovery"), "merge_policy");

	if (!policy || !strcmp(policy, "merge")) {
		return AS_SEED_MERGE;
	}

	if (!strcmp(policy, "replace")) {
		return AS_SEED_REPLACE;
	}

	if (!strcmp(policy, "discovery_only")) {
		return AS_SEED_DISCOVERY_ONLY;
	}
	die("unknown merge_policy %s", policy);
	return AS_SEED_MERGE;
}

static outcome
run_merge(const row* r)
{
	yaml_node_t* config = need_map(r->node, "config");
	yaml_node_t* results = need_seq(need_map(r->node, "input"), "discoveryResults");
	yaml_node_t* expected = need_seq(need_map(r->node, "expect"), "snapshots");
	uint32_t size = y_len(results);

	if (size == 0 || y_len(expected) != size) {
		die("a merge row needs one snapshot per discovery result");
	}

	as_seed_merge_policy policy = merge_policy(config);
	as_vector statics;
	yaml_node_t* seeds = y_get(config, "seeds");

	if (y_typeof(seeds) == Y_NULL) {
		as_vector_init(&statics, sizeof(as_host), 1);
	}
	else {
		hosts_init(&statics, seeds);
	}

	as_vector* vectors = xcalloc(size, sizeof(as_vector));
	const as_vector** pointers = xcalloc(size, sizeof(as_vector*));
	as_seed_candidates* snapshots = xcalloc(size, sizeof(as_seed_candidates));

	for (uint32_t i = 0; i < size; i++) {
		hosts_init(&vectors[i], y_at(results, i));
		pointers[i] = &vectors[i];
	}

	as_error err;
	as_status status = as_discovery_conformance_merge(&err, policy, &statics, pointers, size,
		snapshots);
	outcome o = outcome_pass();

	if (status != AEROSPIKE_OK) {
		o = outcome_fail("merge failed with status %d: %s", (int)status, error_text(&err));
	}

	for (uint32_t i = 0; i < size && status == AEROSPIKE_OK; i++) {
		as_vector exp;
		hosts_init(&exp, y_at(expected, i));

		sb e = {0};
		sb g = {0};
		hosts_text(&e, &exp);
		hosts_text(&g, snapshots[i].hosts);

		if (o.result == RESULT_PASS && strcmp(e.data, g.data)) {
			o = outcome_fail("snapshot %u: expected %s, got %s", i, e.data, g.data);
		}
		free(e.data);
		free(g.data);
		as_vector_destroy(&exp);
		as_seed_candidates_destroy(&snapshots[i]);
	}

	for (uint32_t i = 0; i < size; i++) {
		as_vector_destroy(&vectors[i]);
	}
	as_vector_destroy(&statics);
	free(snapshots);
	free(pointers);
	free(vectors);
	return o;
}

//---------------------------------
// Rows: tls
//---------------------------------

static X509*
load_cert(const char* dir, const char* shape)
{
	char* path = xasprintf("%s/certs/%s.pem", dir, shape);

	for (uint32_t i = 0; i < g_certs_size; i++) {
		if (!strcmp(g_certs[i].path, path)) {
			free(path);
			return g_certs[i].x509;
		}
	}

	FILE* fp = fopen(path, "r");

	if (!fp) {
		die("cannot open certificate %s: %s", path, strerror(errno));
	}

	X509* x509 = PEM_read_X509(fp, NULL, NULL, NULL);
	fclose(fp);

	if (!x509) {
		die("%s is not a PEM certificate", path);
	}

	g_certs = xrealloc(g_certs, (g_certs_size + 1) * sizeof(cert));
	g_certs[g_certs_size].path = path;
	g_certs[g_certs_size].x509 = x509;
	g_certs_size++;
	return x509;
}

static outcome
run_tls(const row* r)
{
	yaml_node_t* input = need_map(r->node, "input");
	const char* tls_name = need_str(input, "tlsName");
	const char* expected = need_str(need_map(r->node, "expect"), "outcome");

	if (strcmp(expected, "accept") && strcmp(expected, "reject")) {
		die("unknown outcome %s", expected);
	}

	X509* x509 = load_cert(r->file->dir, need_str(input, "shape"));

	// Production accepts both the strict match and the deprecated legacy name check.
	as_tls_match_kind kind = as_tls_match_identity(x509, tls_name);
	const char* got = kind == AS_TLS_MATCH_NONE ? "reject" : "accept";

	if (strcmp(expected, got)) {
		return outcome_fail("expected %s, got %s%s", expected, got,
			kind == AS_TLS_MATCH_LEGACY ? " through the legacy name check" : "");
	}
	return outcome_pass();
}

//---------------------------------
// Fixture server
//---------------------------------

static void
start_fixture_server(void)
{
	char* script = xasprintf("%s/%s", g_fixtures, FIXTURE_SERVER);
	struct stat st;

	if (stat(script, &st) || !S_ISREG(st.st_mode)) {
		die("fixture server not found: %s", script);
	}

	char* log_path = xasprintf("%s/%s", g_report_dir, FIXTURE_SERVER_LOG);
	int log_fd = open(log_path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);

	if (log_fd < 0) {
		die("cannot open %s: %s", log_path, strerror(errno));
	}

	int out[2];

	if (pipe2(out, O_CLOEXEC)) {
		die("pipe2: %s", strerror(errno));
	}

	posix_spawn_file_actions_t actions;
	posix_spawn_file_actions_init(&actions);
	posix_spawn_file_actions_addopen(&actions, STDIN_FILENO, "/dev/null", O_RDONLY, 0);
	posix_spawn_file_actions_adddup2(&actions, out[1], STDOUT_FILENO);
	posix_spawn_file_actions_adddup2(&actions, log_fd, STDERR_FILENO);

	char* argv[] = {"python3", script, NULL};
	int rv = posix_spawnp(&g_server_pid, "python3", &actions, NULL, argv, environ);
	posix_spawn_file_actions_destroy(&actions);
	close(out[1]);
	close(log_fd);

	if (rv) {
		g_server_pid = 0;
		die("python3 is required to run the dns and aws-tags fixtures: %s", strerror(rv));
	}

	char line[256];
	size_t len = 0;
	uint64_t deadline = now_ms() + FIXTURE_SERVER_START_MS;

	while (!memchr(line, '\n', len)) {
		uint64_t now = now_ms();

		if (now >= deadline || len >= sizeof(line) - 1) {
			die("the fixture server printed no ports line within %u ms, see %s",
				FIXTURE_SERVER_START_MS, log_path);
		}

		struct pollfd pfd = {.fd = out[0], .events = POLLIN};
		int n = poll(&pfd, 1, (int)(deadline - now));

		if (n < 0 && errno != EINTR) {
			die("poll: %s", strerror(errno));
		}

		if (n <= 0) {
			continue;
		}

		ssize_t got = read(out[0], line + len, sizeof(line) - 1 - len);

		if (got == 0) {
			die("python3 is required to run the dns and aws-tags fixtures: the fixture server "
				"exited during startup, see %s", log_path);
		}

		if (got < 0) {
			if (errno == EINTR) {
				continue;
			}
			die("read: %s", strerror(errno));
		}
		len += (size_t)got;
	}
	close(out[0]);
	line[len] = '\0';

	const char* http = strstr(line, "\"http\":");
	const char* dns = strstr(line, "\"dns\":");
	long http_port = http ? strtol(http + 7, NULL, 10) : 0;
	long dns_port = dns ? strtol(dns + 6, NULL, 10) : 0;

	if (http_port <= 0 || http_port > 65535 || dns_port <= 0 || dns_port > 65535) {
		die("unexpected fixture server ports line: %s", line);
	}
	g_server_http = (uint16_t)http_port;
	g_server_dns = (uint16_t)dns_port;
	free(log_path);
	free(script);
}

// POST /fixture/select with Connection: close, since the server keeps HTTP/1.1 connections open.
static void
select_refresh(const yaml_node_t* refresh, const char* region)
{
	sb body = {0};
	sb_text(&body, "{\"refresh\":");
	y_json(&body, refresh);
	sb_text(&body, ",\"region\":");

	if (region) {
		sb_quote(&body, region);
	}
	else {
		sb_text(&body, "null");
	}
	sb_raw(&body, "}", 1);

	sb req = {0};
	sb_printf(&req, "POST /fixture/select HTTP/1.1\r\nHost: 127.0.0.1:%u\r\n"
		"Content-Type: application/json\r\nContent-Length: %zu\r\nConnection: close\r\n\r\n",
		(unsigned)g_server_http, body.len);
	sb_raw(&req, body.data, body.len);
	free(body.data);

	int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);

	if (fd < 0) {
		die("socket: %s", strerror(errno));
	}

	struct timeval tv = {.tv_sec = SELECT_TIMEOUT_MS / 1000, .tv_usec = 0};
	setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
	setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

	struct sockaddr_in addr = {.sin_family = AF_INET, .sin_port = htons(g_server_http)};
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

	if (connect(fd, (struct sockaddr*)&addr, sizeof(addr))) {
		die("fixture server unreachable: %s", strerror(errno));
	}

	for (size_t off = 0; off < req.len;) {
		ssize_t n = send(fd, req.data + off, req.len - off, MSG_NOSIGNAL);

		if (n < 0) {
			if (errno == EINTR) {
				continue;
			}
			die("fixture server select: %s", strerror(errno));
		}
		off += (size_t)n;
	}
	free(req.data);

	sb resp = {0};
	char buf[4096];

	while (true) {
		ssize_t n = recv(fd, buf, sizeof(buf), 0);

		if (n == 0) {
			break;
		}

		if (n < 0) {
			if (errno == EINTR) {
				continue;
			}
			die("fixture server select: %s", strerror(errno));
		}
		sb_raw(&resp, buf, (size_t)n);
	}
	close(fd);

	if (!resp.data || strncmp(resp.data, "HTTP/1.1 200", 12)) {
		die("fixture server rejected the refresh: %s", resp.data ? resp.data : "no response");
	}
	free(resp.data);
}

//---------------------------------
// Rows: dns and aws-tags
//---------------------------------

static const char* const DNS_KEYS[] = {
	"names", "record_type", "port", "tls_name", "timeout_ms", NULL
};

static const char* const AWS_KEYS[] = {
	"region", "filters", "address_field", "port", "tls_name", "timeout_ms", NULL
};

// Return NULL, or the reason a provider option is not implemented.
static char*
provider_unsupported(const yaml_node_t* block, const char* block_name, const char* const* keys)
{
	uint32_t size = y_len(block);

	for (uint32_t i = 0; i < size; i++) {
		const char* key = y_text(y_key_at(block, i));
		bool known = false;

		for (const char* const* k = keys; *k && !known; k++) {
			known = !strcmp(*k, key);
		}

		if (known) {
			continue;
		}

		if (!strcmp(key, "init_timeout_ms")) {
			return xstrdup("C has no init_timeout_ms; 6c decides the YAML mapping");
		}
		return xasprintf("option discovery.%s.%s is not implemented", block_name, key);
	}
	return NULL;
}

typedef struct provider_args_s {
	const char** names;
	const char** values;
	as_aws_filter* filters;
	char* server;
} provider_args;

static void
provider_args_destroy(provider_args* a)
{
	free(a->names);
	free(a->values);
	free(a->filters);
	free(a->server);
}

static as_status
dns_provider(as_error* err, const yaml_node_t* dns, provider_args* a, as_seed_provider* provider)
{
	as_dns_seed_provider_config config;
	as_dns_seed_provider_config_init(&config);

	yaml_node_t* names = need_seq(dns, "names");
	uint32_t size = y_len(names);
	a->names = xcalloc(size, sizeof(char*));

	for (uint32_t i = 0; i < size; i++) {
		a->names[i] = y_str_value(y_at(names, i), "names");
	}
	config.names = a->names;
	config.names_size = size;

	const char* type = opt_str(dns, "record_type");

	if (type && !strcmp(type, "srv")) {
		config.record_type = AS_DNS_RECORD_SRV;
	}
	else if (type && strcmp(type, "a")) {
		die("unknown record_type %s", type);
	}

	if (y_get(dns, "port")) {
		config.port = need_port(dns, "port");
	}
	config.tls_name = opt_str(dns, "tls_name");
	config.timeout_ms = opt_uint32(dns, "timeout_ms", PROVIDER_TIMEOUT_MS);

	a->server = xasprintf("127.0.0.1:%u", (unsigned)g_server_dns);
	config.servers = (const char* const*)&a->server;
	config.servers_size = 1;
	return as_discovery_dns_seed_provider(err, &config, provider);
}

static as_status
aws_provider(as_error* err, const yaml_node_t* aws, provider_args* a, as_seed_provider* provider)
{
	as_aws_seed_provider_config config;
	as_aws_seed_provider_config_init(&config);
	config.region = need_str(aws, "region");

	yaml_node_t* filters = need_seq(aws, "filters");
	uint32_t size = y_len(filters);
	uint32_t total = 0;

	for (uint32_t i = 0; i < size; i++) {
		total += y_len(need_seq(y_at(filters, i), "values"));
	}

	a->filters = xcalloc(size, sizeof(as_aws_filter));
	a->values = xcalloc(total, sizeof(char*));
	total = 0;

	for (uint32_t i = 0; i < size; i++) {
		yaml_node_t* f = y_at(filters, i);
		yaml_node_t* values = need_seq(f, "values");
		uint32_t n = y_len(values);

		a->filters[i].name = need_str(f, "name");
		a->filters[i].values = &a->values[total];
		a->filters[i].values_size = n;

		for (uint32_t k = 0; k < n; k++) {
			a->values[total++] = y_str_value(y_at(values, k), "values");
		}
	}
	config.filters = a->filters;
	config.filters_size = size;

	const char* field = opt_str(aws, "address_field");

	if (!field || !strcmp(field, "private_ip")) {
		config.address_field = AS_AWS_ADDRESS_PRIVATE_IP;
	}
	else if (!strcmp(field, "public_ip")) {
		config.address_field = AS_AWS_ADDRESS_PUBLIC_IP;
	}
	else if (!strcmp(field, "private_dns")) {
		config.address_field = AS_AWS_ADDRESS_PRIVATE_DNS;
	}
	else if (!strcmp(field, "public_dns")) {
		config.address_field = AS_AWS_ADDRESS_PUBLIC_DNS;
	}
	else {
		die("unknown address_field %s", field);
	}

	if (y_get(aws, "port")) {
		config.port = need_port(aws, "port");
	}
	config.tls_name = opt_str(aws, "tls_name");
	config.timeout_ms = opt_uint32(aws, "timeout_ms", PROVIDER_TIMEOUT_MS);

	a->server = xasprintf("http://127.0.0.1:%u", (unsigned)g_server_http);
	config.endpoint = a->server;
	config.access_key_id = ACCESS_KEY;
	config.secret_access_key = SECRET_KEY;
	config.session_token = SESSION_TOKEN;
	return as_discovery_aws_seed_provider(err, &config, provider);
}

static void
canonical_host(const char* host, char* out, size_t size)
{
	unsigned char addr[sizeof(struct in6_addr)];

	if (inet_pton(AF_INET, host, addr) == 1 && inet_ntop(AF_INET, addr, out, (socklen_t)size)) {
		return;
	}

	if (inet_pton(AF_INET6, host, addr) == 1 && inet_ntop(AF_INET6, addr, out, (socklen_t)size)) {
		return;
	}
	snprintf(out, size, "%s", host);
}

// Sorted host keys, so that hosts compare as a multiset: a duplicate host is not collapsed.
static void
host_keys_init(host_keys* k, const as_vector* hosts)
{
	k->size = hosts ? hosts->size : 0;
	k->keys = xcalloc(k->size, sizeof(char*));

	for (uint32_t i = 0; i < k->size; i++) {
		const as_host* h = as_vector_get((as_vector*)hosts, i);
		char canon[INET6_ADDRSTRLEN + 256];
		canonical_host(h->name, canon, sizeof(canon));
		k->keys[i] = h->tls_name ?
			xasprintf("%s %u %s", canon, (unsigned)h->port, h->tls_name) :
			xasprintf("%s %u", canon, (unsigned)h->port);
	}
	qsort(k->keys, k->size, sizeof(char*), compare_strings);
}

static void
host_keys_destroy(host_keys* k)
{
	for (uint32_t i = 0; i < k->size; i++) {
		free(k->keys[i]);
	}
	free(k->keys);
}

// NULL when equal, otherwise "expected [..], got [..]".
static char*
host_keys_diff(const as_vector* expected, const as_vector* got)
{
	host_keys e;
	host_keys g;
	host_keys_init(&e, expected);
	host_keys_init(&g, got);

	bool equal = e.size == g.size;

	for (uint32_t i = 0; i < e.size && equal; i++) {
		equal = !strcmp(e.keys[i], g.keys[i]);
	}

	char* diff = NULL;

	if (!equal) {
		sb b = {0};
		sb_text(&b, "expected ");
		join_strings(&b, e.keys, e.size);
		sb_text(&b, ", got ");
		join_strings(&b, g.keys, g.size);
		diff = b.data;
	}
	host_keys_destroy(&e);
	host_keys_destroy(&g);
	return diff;
}

typedef struct step_s {
	as_status status;
	char* message;
	as_seed_candidates candidates;
} step;

static const char*
category_marker(const char* category)
{
	if (!strcmp(category, "discovery-failed")) {
		return "DNS seed discovery failed for every name";
	}

	if (!strcmp(category, "aws-error")) {
		return "DescribeInstances failed";
	}

	if (!strcmp(category, "timeout")) {
		return "timed out";
	}
	die("unknown error category %s", category);
	return NULL;
}

static outcome
check_step(uint32_t i, const step* s, const yaml_node_t* exp, const as_seed_candidates* seeds)
{
	yaml_node_t* error = opt_map(exp, "error");

	if (error) {
		const char* category = need_str(error, "category");
		const char* marker = category_marker(category);

		if (s->status == AEROSPIKE_OK) {
			sb b = {0};
			hosts_text(&b, s->candidates.hosts);
			outcome o = outcome_fail("refresh %u: expected %s, got hosts %s", i, category, b.data);
			free(b.data);
			return o;
		}

		if (s->status != AEROSPIKE_ERR_SEED_DISCOVERY_FAILED) {
			return outcome_fail("refresh %u: failed with status %d instead of %d: %s", i,
				(int)s->status, (int)AEROSPIKE_ERR_SEED_DISCOVERY_FAILED, s->message);
		}

		if (!strstr(s->message, marker)) {
			return outcome_fail("refresh %u: message lacks '%s': %s", i, marker, s->message);
		}

		char buf[FACT_SIZE];
		const char* missing = missing_fact(s->message, y_get(error, "messageContains"), buf);

		if (missing) {
			return outcome_fail("refresh %u: message lacks '%s': %s", i, missing, s->message);
		}

		if (strstr(s->message, SECRET_KEY) || strstr(s->message, SESSION_TOKEN)) {
			return outcome_fail("refresh %u: message contains a credential secret", i);
		}
	}
	else {
		if (s->status != AEROSPIKE_OK) {
			return outcome_fail("refresh %u: failed with status %d: %s", i, (int)s->status,
				s->message);
		}

		as_vector hosts;
		hosts_init(&hosts, need_seq(exp, "hosts"));
		char* diff = host_keys_diff(&hosts, s->candidates.hosts);
		as_vector_destroy(&hosts);

		if (diff) {
			outcome o = outcome_fail("refresh %u: hosts %s", i, diff);
			free(diff);
			return o;
		}
	}

	as_vector expected_seeds;
	hosts_init(&expected_seeds, need_seq(exp, "seeds"));
	char* diff = host_keys_diff(&expected_seeds, seeds->hosts);
	as_vector_destroy(&expected_seeds);

	if (diff) {
		outcome o = outcome_fail("refresh %u: seeds %s", i, diff);
		free(diff);
		return o;
	}
	return outcome_pass();
}

static outcome
run_provider(const row* r, bool aws)
{
	yaml_node_t* discovery = need_map(need_map(r->node, "config"), "discovery");
	yaml_node_t* block = need_map(discovery, aws ? "aws" : "dns");
	yaml_node_t* refreshes = need_seq(need_map(r->node, "input"), "refreshes");
	yaml_node_t* expected = need_seq(need_map(r->node, "expect"), "refreshes");
	uint32_t size = y_len(refreshes);

	if (size == 0 || y_len(expected) != size) {
		die("a provider row needs one expected refresh per input refresh");
	}

	char* unsupported = provider_unsupported(block, aws ? "aws" : "dns", aws ? AWS_KEYS : DNS_KEYS);

	if (unsupported) {
		outcome o = outcome_unsupported("%s", unsupported);
		free(unsupported);
		return o;
	}

	provider_args args = {0};
	as_seed_provider provider;
	as_error err;
	as_error_init(&err);
	as_status status = aws ? aws_provider(&err, block, &args, &provider) :
		dns_provider(&err, block, &args, &provider);

	if (status != AEROSPIKE_OK) {
		provider_args_destroy(&args);
		return outcome_fail("provider construction failed with status %d: %s", (int)status,
			error_text(&err));
	}

	const char* region = aws ? need_str(block, "region") : NULL;
	step* steps = xcalloc(size, sizeof(step));

	for (uint32_t i = 0; i < size; i++) {
		select_refresh(y_at(refreshes, i), region);

		as_seed_candidate_context context;
		memset(&context, 0, sizeof(context));

		if (!as_seed_candidates_init(&steps[i].candidates, 4)) {
			die("out of memory");
		}
		as_error_init(&err);
		steps[i].status = provider.refresh(&err, &context, &steps[i].candidates, provider.udata);
		steps[i].message = xstrdup(error_text(&err));
	}

	if (provider.destroy) {
		provider.destroy(provider.udata);
	}
	provider_args_destroy(&args);

	// The client's merge with discovery_only and no static seeds. A failed refresh keeps the
	// previous seeds; a failed first refresh (no current row) counts as an empty result.
	as_vector statics;
	as_vector empty;
	as_vector_init(&statics, sizeof(as_host), 1);
	as_vector_init(&empty, sizeof(as_host), 1);
	const as_vector** results = xcalloc(size, sizeof(as_vector*));
	as_seed_candidates* seeds = xcalloc(size, sizeof(as_seed_candidates));

	for (uint32_t i = 0; i < size; i++) {
		results[i] = steps[i].status == AEROSPIKE_OK ? steps[i].candidates.hosts :
			(i == 0 ? &empty : NULL);
	}

	status = as_discovery_conformance_merge(&err, AS_SEED_DISCOVERY_ONLY, &statics, results, size,
		seeds);
	outcome o = outcome_pass();

	if (status != AEROSPIKE_OK) {
		o = outcome_fail("merge failed with status %d: %s", (int)status, error_text(&err));
	}

	for (uint32_t i = 0; i < size; i++) {
		if (status == AEROSPIKE_OK) {
			if (o.result == RESULT_PASS) {
				o = check_step(i, &steps[i], y_at(expected, i), &seeds[i]);
			}
			as_seed_candidates_destroy(&seeds[i]);
		}
		as_seed_candidates_destroy(&steps[i].candidates);
		free(steps[i].message);
	}
	as_vector_destroy(&statics);
	as_vector_destroy(&empty);
	free(seeds);
	free(results);
	free(steps);
	return o;
}

//---------------------------------
// Loading
//---------------------------------

static int
walk_entry(const char* path, const struct stat* st, int type, struct FTW* ftw)
{
	(void)st;
	(void)ftw;
	size_t len = strlen(path);

	if (type == FTW_F && len > 5 && !strcmp(path + len - 5, ".yaml")) {
		g_walk = xrealloc(g_walk, (g_walk_size + 1) * sizeof(char*));
		g_walk[g_walk_size++] = xstrdup(path);
	}
	return 0;
}

static bool
known_kind(const char* kind, const char** out)
{
	for (size_t i = 0; i < sizeof(KINDS) / sizeof(KINDS[0]); i++) {
		if (!strcmp(kind, KINDS[i])) {
			*out = KINDS[i];
			return true;
		}
	}
	return false;
}

static void
load_version(void)
{
	char* path = xasprintf("%s/VERSION", g_fixtures);
	FILE* fp = fopen(path, "r");

	if (!fp) {
		die("not a conformance fixture set, cannot open %s: %s", path, strerror(errno));
	}

	char version[64] = {0};

	if (!fgets(version, sizeof(version), fp)) {
		version[0] = '\0';
	}
	fclose(fp);

	size_t len = strlen(version);

	while (len && (version[len - 1] == '\n' || version[len - 1] == '\r' ||
		version[len - 1] == ' ' || version[len - 1] == '\t')) {
		version[--len] = '\0';
	}

	if (strcmp(version, FIXTURE_SET_VERSION)) {
		die("conformance fixture set %s found, %s expected: check out that release or update "
			"the runner and declarations", version, FIXTURE_SET_VERSION);
	}
	free(path);
}

static void
load_file(fixture_file* f)
{
	g_where = f->name;
	g_doc = f->doc;
	yaml_node_t* root = yaml_document_get_root_node(f->doc);

	if (y_typeof(root) != Y_MAP) {
		die("a fixture file must be a map");
	}

	const char* kind_text = need_str(root, "kind");
	const char* kind;

	if (!known_kind(kind_text, &kind)) {
		die("unknown kind %s", kind_text);
	}

	yaml_node_t* rows = need_seq(root, "rows");
	uint32_t size = y_len(rows);
	uint32_t first = g_rows_size;

	if (size == 0) {
		die("a fixture file must have rows");
	}

	g_rows = xrealloc(g_rows, (g_rows_size + size) * sizeof(row));

	for (uint32_t i = 0; i < size; i++) {
		yaml_node_t* node = y_at(rows, i);

		if (y_typeof(node) != Y_MAP) {
			die("row %u must be a map", i);
		}

		row* r = &g_rows[g_rows_size];
		memset(r, 0, sizeof(row));
		r->file = f;
		r->kind = kind;
		r->node = node;
		r->name = need_str(node, "case");

		if (!*r->name) {
			die("row %u has an empty case", i);
		}

		for (uint32_t k = first; k < g_rows_size; k++) {
			if (!strcmp(g_rows[k].name, r->name)) {
				die("duplicate case %s", r->name);
			}
		}

		const char* state = opt_str(node, "state");

		if (state) {
			if (strcmp(state, "blocked")) {
				die("%s: unknown state %s", r->name, state);
			}
			r->blocked_by = need_str(node, "blockedBy");

			if (!*r->blocked_by) {
				die("%s: blockedBy is empty", r->name);
			}
		}
		r->key = xasprintf("%s:%s", f->name, r->name);
		r->declared = r->blocked_by ? RESULT_BLOCKED : RESULT_PASS;
		g_rows_size++;
	}
	g_where = NULL;
}

static void
load_fixtures(void)
{
	load_version();

	const char* roots[] = {"discovery", "tls"};

	for (size_t i = 0; i < 2; i++) {
		char* dir = xasprintf("%s/%s", g_fixtures, roots[i]);
		struct stat st;

		if (stat(dir, &st) || !S_ISDIR(st.st_mode)) {
			die("fixture directory not found: %s", dir);
		}

		if (nftw(dir, walk_entry, 16, FTW_PHYS)) {
			die("cannot read %s: %s", dir, strerror(errno));
		}
		free(dir);
	}

	size_t prefix = strlen(g_fixtures) + 1;

	for (uint32_t i = 0; i < g_walk_size; i++) {
		char* rel = xstrdup(g_walk[i] + prefix);
		free(g_walk[i]);
		g_walk[i] = rel;
	}

	if (g_walk_size > 1) {
		qsort(g_walk, g_walk_size, sizeof(char*), compare_strings);
	}

	g_files = xcalloc(g_walk_size, sizeof(fixture_file));
	g_files_size = g_walk_size;

	for (uint32_t i = 0; i < g_walk_size; i++) {
		fixture_file* f = &g_files[i];
		f->name = g_walk[i];

		char* path = xasprintf("%s/%s", g_fixtures, f->name);
		char* slash = strrchr(path, '/');
		f->dir = xstrdup(path);
		f->dir[slash - path] = '\0';
		f->doc = yaml_load_file(path, true);
		free(path);
		load_file(f);
	}
	free(g_walk);
	g_walk = NULL;
	g_walk_size = 0;

	if (g_rows_size == 0) {
		die("no fixture rows under %s", g_fixtures);
	}
}

static const char*
result_name(const char* result)
{
	for (size_t i = 0; i < 4; i++) {
		if (!strcmp(result, RESULTS[i])) {
			return RESULTS[i];
		}
	}
	return NULL;
}

static void
load_declarations(void)
{
	g_decl_doc = yaml_load_file(g_declarations, true);
	g_doc = g_decl_doc;
	g_where = g_declarations;
	yaml_node_t* root = yaml_document_get_root_node(g_decl_doc);

	if (y_typeof(root) == Y_NULL) {
		g_where = NULL;
		return;
	}

	if (y_typeof(root) != Y_MAP) {
		die("declarations must map fixture files to cases to {result, reason}");
	}

	uint32_t files = y_len(root);

	for (uint32_t i = 0; i < files; i++) {
		const char* file = y_str_value(y_key_at(root, i), "a fixture file name");
		yaml_node_t* cases = y_value_at(root, i);
		bool known = false;

		for (uint32_t k = 0; k < g_files_size && !known; k++) {
			known = !strcmp(g_files[k].name, file);
		}

		if (!known) {
			die("%s is not a fixture file", file);
		}

		if (y_typeof(cases) != Y_MAP) {
			die("%s must map cases to {result, reason}", file);
		}

		uint32_t size = y_len(cases);

		for (uint32_t c = 0; c < size; c++) {
			const char* name = y_str_value(y_key_at(cases, c), "a case name");
			yaml_node_t* body = y_value_at(cases, c);
			row* r = NULL;

			for (uint32_t k = 0; k < g_rows_size && !r; k++) {
				if (!strcmp(g_rows[k].file->name, file) && !strcmp(g_rows[k].name, name)) {
					r = &g_rows[k];
				}
			}

			if (!r) {
				die("%s:%s is not a fixture case", file, name);
			}

			if (r->has_declaration) {
				die("%s:%s is declared twice", file, name);
			}

			if (y_typeof(body) != Y_MAP) {
				die("%s:%s must be {result, reason}", file, name);
			}

			uint32_t keys = y_len(body);

			for (uint32_t k = 0; k < keys; k++) {
				const char* key = y_text(y_key_at(body, k));

				if (strcmp(key, "result") && strcmp(key, "reason")) {
					die("%s:%s: unknown key %s", file, name, key);
				}
			}

			const char* result = result_name(need_str(body, "result"));

			if (!result) {
				die("%s:%s: result must be pass, fail, unsupported or blocked", file, name);
			}

			const char* reason = opt_str(body, "reason");

			if (reason && !*reason) {
				die("%s:%s: reason is empty", file, name);
			}

			if (result != RESULT_PASS && !reason) {
				die("%s:%s: a %s declaration needs a reason", file, name, result);
			}
			r->declared = result;
			r->declared_reason = reason;
			r->has_declaration = true;
		}
	}
	g_where = NULL;
}

//---------------------------------
// Report
//---------------------------------

static int
compare_rows(const void* a, const void* b)
{
	return strcmp((*(row* const*)a)->key, (*(row* const*)b)->key);
}

static const char*
report_reason(const row* r)
{
	if (r->declared_reason && r->declared == r->result) {
		return r->declared_reason;
	}
	return r->reason;
}

static void
write_report(row** sorted)
{
	uint32_t counts[4] = {0};

	for (uint32_t i = 0; i < g_rows_size; i++) {
		for (int k = 0; k < 4; k++) {
			if (g_rows[i].result == RESULTS[k]) {
				counts[k]++;
			}
		}
	}

	sb b = {0};
	sb_text(&b, "{\n  \"runtime\": ");
	sb_quote(&b, RUNTIME);
	sb_text(&b, ",\n  \"clientVersion\": ");
	sb_quote(&b, aerospike_client_version);
	sb_text(&b, ",\n  \"fixtureSetVersion\": ");
	sb_quote(&b, FIXTURE_SET_VERSION);
	sb_printf(&b, ",\n  \"counts\": {\"pass\": %u, \"fail\": %u, \"unsupported\": %u, "
		"\"blocked\": %u},\n  \"rows\": [", counts[0], counts[1], counts[2], counts[3]);

	for (uint32_t i = 0; i < g_rows_size; i++) {
		const row* r = sorted[i];
		const char* reason = report_reason(r);

		sb_text(&b, i ? ",\n    {\"file\": " : "\n    {\"file\": ");
		sb_quote(&b, r->file->name);
		sb_text(&b, ", \"case\": ");
		sb_quote(&b, r->name);
		sb_text(&b, ", \"result\": ");
		sb_quote(&b, r->result);

		if (reason) {
			sb_text(&b, ", \"reason\": ");
			sb_quote(&b, reason);
		}

		if (r->reason && (!reason || strcmp(r->reason, reason))) {
			sb_text(&b, ", \"detail\": ");
			sb_quote(&b, r->reason);
		}
		sb_raw(&b, "}", 1);
	}
	sb_text(&b, "\n  ]\n}\n");

	// Written next to the report under another name, so no partial report is ever read.
	char* tmp = xasprintf("%s/conformance-report.tmp.%ld", g_report_dir, (long)getpid());
	FILE* fp = fopen(tmp, "w");

	if (!fp) {
		die("cannot create %s: %s", tmp, strerror(errno));
	}

	bool ok = fwrite(b.data, 1, b.len, fp) == b.len;
	ok = (fclose(fp) == 0) && ok;

	if (!ok || rename(tmp, g_report)) {
		int e = errno;
		unlink(tmp);
		die("cannot write %s: %s", g_report, strerror(e));
	}
	free(tmp);
	free(b.data);

	fprintf(stderr, "conformance: %u rows: %u pass, %u fail, %u unsupported, %u blocked\n",
		g_rows_size, counts[0], counts[1], counts[2], counts[3]);
}

//---------------------------------
// Main
//---------------------------------

static bool
log_callback(as_log_level level, const char* func, const char* file, uint32_t line,
	const char* fmt, ...)
{
	(void)func;
	(void)file;
	(void)line;
	va_list ap;
	va_start(ap, fmt);
	fprintf(stderr, "client %s: ", as_log_level_tostring(level));
	vfprintf(stderr, fmt, ap);
	fputc('\n', stderr);
	va_end(ap);
	return true;
}

static void
usage(void)
{
	fprintf(stderr, "usage: conformance_runner --fixtures <dir> --declarations <file> "
		"--report <path>\n");
	exit(EXIT_ERROR);
}

static void
parse_options(int argc, char* argv[])
{
	static const struct option options[] = {
		{"fixtures", required_argument, NULL, 'f'},
		{"declarations", required_argument, NULL, 'd'},
		{"report", required_argument, NULL, 'r'},
		{NULL, 0, NULL, 0}
	};
	bool bad = false;
	int c;

	// Parse every option before failing, so that a usage error after --report still removes
	// the earlier report.
	while ((c = getopt_long(argc, argv, "", options, NULL)) != -1) {
		switch (c) {
		case 'f':
			g_fixtures = optarg;
			break;
		case 'd':
			g_declarations = optarg;
			break;
		case 'r':
			g_report = optarg;
			break;
		default:
			bad = true;
		}
	}

	if (bad || optind != argc || !g_fixtures || !*g_fixtures || !g_declarations ||
		!*g_declarations || !g_report || !*g_report) {
		if (g_report && *g_report) {
			unlink(g_report);
		}
		usage();
	}
}

static outcome
run_row(const row* r)
{
	if (r->blocked_by) {
		return (outcome){RESULT_BLOCKED, xstrdup(r->blocked_by)};
	}

	if (!strcmp(r->kind, "translate")) {
		return run_translate(r);
	}

	if (!strcmp(r->kind, "collision")) {
		return run_collision(r);
	}

	if (!strcmp(r->kind, "merge")) {
		return run_merge(r);
	}

	if (!strcmp(r->kind, "tls")) {
		return run_tls(r);
	}

	if (!strcmp(r->kind, "dns")) {
		return run_provider(r, false);
	}
	return run_provider(r, true);
}

static void
cleanup(void)
{
	for (uint32_t i = 0; i < g_rows_size; i++) {
		free(g_rows[i].reason);
		free(g_rows[i].key);
	}
	free(g_rows);

	for (uint32_t i = 0; i < g_files_size; i++) {
		free(g_files[i].name);
		free(g_files[i].dir);
		yaml_free_doc(g_files[i].doc);
	}
	free(g_files);
	yaml_free_doc(g_decl_doc);

	for (uint32_t i = 0; i < g_certs_size; i++) {
		free(g_certs[i].path);
		X509_free(g_certs[i].x509);
	}
	free(g_certs);
	free(g_report_dir);
}

int
main(int argc, char* argv[])
{
	parse_options(argc, argv);

	// An earlier report must not survive a run that fails before writing one.
	if (unlink(g_report) && errno != ENOENT) {
		die("cannot remove %s: %s", g_report, strerror(errno));
	}

	char* slash = strrchr(g_report, '/');
	g_report_dir = slash ? strndup(g_report, (size_t)(slash - g_report)) : xstrdup(".");

	if (!g_report_dir) {
		die("out of memory");
	}

	if (!*g_report_dir) {
		free(g_report_dir);
		g_report_dir = xstrdup("/");
	}

	atexit(stop_fixture_server);
	signal(SIGPIPE, SIG_IGN);
	as_log_set_level(AS_LOG_LEVEL_WARN);
	as_log_set_callback(log_callback);

	load_fixtures();
	load_declarations();

	bool need_server = false;

	for (uint32_t i = 0; i < g_rows_size && !need_server; i++) {
		const row* r = &g_rows[i];
		need_server = !r->blocked_by && (!strcmp(r->kind, "dns") || !strcmp(r->kind, "aws-tags"));
	}

	if (need_server) {
		start_fixture_server();
	}

	for (uint32_t i = 0; i < g_rows_size; i++) {
		row* r = &g_rows[i];
		g_where = r->key;
		g_doc = r->file->doc;
		outcome o = run_row(r);
		r->result = o.result;
		r->reason = o.reason;
	}
	g_where = NULL;
	stop_fixture_server();

	row** sorted = xcalloc(g_rows_size, sizeof(row*));

	for (uint32_t i = 0; i < g_rows_size; i++) {
		sorted[i] = &g_rows[i];
	}
	qsort(sorted, g_rows_size, sizeof(row*), compare_rows);

	uint32_t mismatches = 0;

	for (uint32_t i = 0; i < g_rows_size; i++) {
		const row* r = sorted[i];
		bool mismatch = r->result != r->declared;

		if (mismatch) {
			mismatches++;
		}

		if (mismatch || r->result != RESULT_PASS) {
			fprintf(stderr, "%s %s: %s (declared %s)%s%s\n", mismatch ? "MISMATCH" : "declared",
				r->key, r->result, r->declared, r->reason ? ": " : "", r->reason ? r->reason : "");
		}
	}

	write_report(sorted);
	free(sorted);
	cleanup();

	if (mismatches) {
		fprintf(stderr, "conformance: %u rows differ from their declarations\n", mismatches);
		return EXIT_MISMATCH;
	}
	return EXIT_MATCH;
}
