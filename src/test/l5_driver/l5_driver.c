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
 * The C driver for the l5-aws and l2-proxy harnesses in aerospike-client-conformance
 * (l5-aws/DRIVER.md there, contract version 5). It builds a client from the flags, runs a
 * retrying load, writes what the client sees to stdout as JSON lines and answers the commands it
 * reads from stdin. It measures and reports; the harness decides every row.
 *
 * Exit status: 0 after quit or end of input; 2 on bad flags, the reason on stderr; 3 when the
 * client could not be created, an error line on stdout and then the reason on stderr; 1 when the
 * driver's own threads cannot be started.
 * Built against installed headers only by "make l5-driver"; POSIX only. With L5_DRIVER_NO_MAIN
 * defined, main() is left out so that l5_driver_test.c can include this file.
 */
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_info.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_address.h>
#include <aerospike/as_atomic.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_config.h>
#include <aerospike/as_discovery.h>
#include <aerospike/as_error.h>
#include <aerospike/as_info.h>
#include <aerospike/as_key.h>
#include <aerospike/as_log.h>
#include <aerospike/as_node.h>
#include <aerospike/as_partition.h>
#include <aerospike/as_record.h>
#include <aerospike/as_sleep.h>
#include <aerospike/as_string_builder.h>
#include <aerospike/version.h>

//---------------------------------
// Macros
//---------------------------------

#define L5_CONTRACT 5
#define L5_POLL_MS 10
#define L5_HEARTBEAT_MS 100
#define L5_REACH_INTERVAL_MS 500
#define L5_REACH_TIMEOUT_MS 1000
#define L5_REACH_DRAIN_MS 5000
#define L5_PROBE_INTERVAL_MS 10
#define L5_PROBE_CONNECT_MS 50
#define L5_PROBE_MAX 16
#define L5_SERVING_ROUNDS 3
#define L5_WATCH_BOUND_MS 60000
#define L5_QUIT_MS 30000
#define L5_KEY_SEARCH_LIMIT 1000000
#define L5_PARTITIONS 4096
#define L5_LOG_SIZE 4096
#define L5_WORKERS_MAX 1024
#define L5_LARGE_BYTES_MAX (64 * 1024 * 1024)
#define L5_LARGE_KEYS_MAX 100000
#define L5_NONE (-1)

//---------------------------------
// Types
//---------------------------------

typedef struct l5_options_s {
	char seed_host[256];
	uint16_t seed_port;
	bool pass_through;
	as_addr_map* port_map;
	uint32_t port_map_size;
	as_translation_miss_policy on_miss;
	char* user;
	char* password;
	uint32_t tend_interval;
	uint32_t timeout;
	uint32_t workers;
	uint32_t keys;
	char* ns;
	char* set;
} l5_options;

// Stdout carries JSON lines only, each flushed, ready first. Lines written before ready go to
// stderr, except the exit 3 error line; failure lines before ready are dropped.
typedef struct l5_output_s {
	FILE* out;
	FILE* err;
	bool open;
	pthread_mutex_t lock;
} l5_output;

typedef enum l5_line_kind_e {
	L5_LINE_REPLY,
	L5_LINE_FAILURE,
	L5_LINE_NOT_CREATED
} l5_line_kind;

typedef enum l5_watch_start_e {
	L5_WATCH_STARTED,
	L5_WATCH_RUNNING,
	L5_WATCH_FAILED
} l5_watch_start_result;

typedef struct l5_probe_address_s {
	char address[INET6_ADDRSTRLEN];
	int64_t first_connected_at;
} l5_probe_address;

// The watch probe: each round resolves the host and connects to every address it resolves to.
typedef struct l5_probe_s {
	char host[256];
	uint16_t port;
	l5_probe_address addresses[L5_PROBE_MAX];
	uint32_t size;
	uint32_t serving_rounds;
	uint64_t serving_start;
	int64_t serving_at;
	int64_t last_closed_at;
} l5_probe;

// A node as one routing read saw it. node is for identity only and is never dereferenced once
// the read has returned.
typedef struct l5_node_view_s {
	as_node* node;
	char name[AS_NODE_NAME_SIZE];
	char host[INET6_ADDRSTRLEN];
	uint16_t port;
	bool active;
	int8_t reachable;
	uint32_t owned;
} l5_node_view;

typedef struct l5_view_s {
	l5_node_view* nodes;
	uint32_t size;
	uint32_t capacity;
	uint32_t unowned;
} l5_view;

typedef struct l5_reach_entry_s {
	as_node* node;
	int8_t reachable;
	bool checking;
} l5_reach_entry;

typedef struct l5_driver_s {
	l5_options* opt;
	l5_output* out;
	aerospike* as;
	uint8_t stop;
	uint8_t started;
	// The t by which a running watch must have replied once stop is set.
	uint64_t quit_deadline;

	pthread_mutex_t load_lock;
	pthread_cond_t load_cond;
	uint32_t workers_running;
	uint64_t succeeded;
	uint64_t failed;
	bool has_last_error;
	char last_error[AS_ERROR_MESSAGE_MAX_SIZE];
	uint64_t reset_ns;
	uint64_t max_latency_ns;

	// Only the routing thread uses signature.
	char* signature;
	// Routing state, under the output lock.
	uint64_t next_heartbeat;
	int64_t unowned_since;
	uint64_t longest_closed;

	pthread_mutex_t thread_lock;
	bool routing_started;
	pthread_t routing_thread;
	bool reach_started;
	pthread_t reach_thread;

	pthread_mutex_t reach_lock;
	pthread_cond_t reach_cond;
	l5_reach_entry* reach;
	uint32_t reach_size;
	uint32_t reach_capacity;
	uint32_t reach_in_flight;

	pthread_mutex_t watch_lock;
	bool watch_active;
	bool watch_joinable;
	pthread_t watch_thread;
} l5_driver;

typedef struct l5_watch_s {
	l5_driver* d;
	char address[300];
	char node[AS_NODE_NAME_SIZE];
	l5_probe probe;
	l5_view view;
} l5_watch;

typedef struct l5_reach_arg_s {
	l5_driver* d;
	as_node* node;
} l5_reach_arg;

//---------------------------------
// Globals
//---------------------------------

static const char L5_USAGE[] =
	"usage: l5_driver --seed HOST:PORT --shape pass-through|port-map [--port-map NODE=HOST:PORT,...]\n"
	"                 [--on-miss strict|pass-through] [--user USER] --tend-interval MS --timeout MS\n"
	"                 --namespace NS --set SET [--workers N] [--keys N]";

static uint64_t l5_start_ns;
static l5_output* l5_log_output;

//---------------------------------
// Clock
//---------------------------------

static uint64_t
l5_mono_ns(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static void
l5_clock_start(void)
{
	l5_start_ns = l5_mono_ns();
}

// Milliseconds since the driver started, on the one monotonic clock every t is read from.
uint64_t
l5_now(void)
{
	return (l5_mono_ns() - l5_start_ns) / 1000000;
}

static uint64_t
l5_epoch_ms(void)
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return (uint64_t)tv.tv_sec * 1000 + (uint64_t)tv.tv_usec / 1000;
}

static void
l5_sleep_until(uint64_t begin, uint64_t interval)
{
	uint64_t elapsed = l5_now() - begin;

	if (elapsed < interval) {
		as_sleep((uint32_t)(interval - elapsed));
	}
}

//---------------------------------
// JSON
//---------------------------------

// Length of the valid UTF-8 sequence at p, or 0 when it is not one.
static uint32_t
l5_utf8_length(const unsigned char* p)
{
	uint32_t n;
	uint32_t cp;

	if (p[0] < 0x80) {
		return 1;
	}

	if (p[0] >= 0xc2 && p[0] <= 0xdf) {
		n = 2;
		cp = p[0] & 0x1f;
	}
	else if (p[0] >= 0xe0 && p[0] <= 0xef) {
		n = 3;
		cp = p[0] & 0x0f;
	}
	else if (p[0] >= 0xf0 && p[0] <= 0xf4) {
		n = 4;
		cp = p[0] & 0x07;
	}
	else {
		return 0;
	}

	for (uint32_t i = 1; i < n; i++) {
		if ((p[i] & 0xc0) != 0x80) {
			return 0;
		}
		cp = (cp << 6) | (p[i] & 0x3f);
	}

	if ((n == 3 && (cp < 0x800 || (cp >= 0xd800 && cp <= 0xdfff))) ||
		(n == 4 && (cp < 0x10000 || cp > 0x10ffff))) {
		return 0;
	}
	return n;
}

// Quote s as a JSON string: quote, backslash and control characters escaped, U+2028 and U+2029
// escaped as JavaScript requires, and a byte that is not valid UTF-8 replaced with U+FFFD, so
// the harness can always decode the line.
void
l5_json_quote(as_string_builder* sb, const char* s)
{
	as_string_builder_append_char(sb, '"');

	for (const unsigned char* p = (const unsigned char*)s; *p;) {
		switch (*p) {
		case '"':
			as_string_builder_append(sb, "\\\"");
			p++;
			continue;
		case '\\':
			as_string_builder_append(sb, "\\\\");
			p++;
			continue;
		case '\n':
			as_string_builder_append(sb, "\\n");
			p++;
			continue;
		case '\r':
			as_string_builder_append(sb, "\\r");
			p++;
			continue;
		case '\t':
			as_string_builder_append(sb, "\\t");
			p++;
			continue;
		default:
			break;
		}

		if (*p < 0x20) {
			char esc[8];
			snprintf(esc, sizeof(esc), "\\u%04x", (unsigned int)*p);
			as_string_builder_append(sb, esc);
			p++;
			continue;
		}

		uint32_t n = l5_utf8_length(p);

		if (n == 0) {
			as_string_builder_append(sb, "\\ufffd");
			p++;
		}
		else if (n == 3 && p[0] == 0xe2 && p[1] == 0x80 && (p[2] == 0xa8 || p[2] == 0xa9)) {
			as_string_builder_append(sb, p[2] == 0xa8 ? "\\u2028" : "\\u2029");
			p += 3;
		}
		else {
			// Not as_string_builder_append_chars(): when it grows the buffer it appends up to the
			// end of src, not len bytes.
			for (uint32_t i = 0; i < n; i++) {
				as_string_builder_append_char(sb, (char)*p++);
			}
		}
	}
	as_string_builder_append_char(sb, '"');
}

// Each field helper appends ,"name":value, for the body of an object whose first field is out.
static void
l5_json_key(as_string_builder* sb, const char* name)
{
	as_string_builder_append(sb, ",\"");
	as_string_builder_append(sb, name);
	as_string_builder_append(sb, "\":");
}

static void
l5_json_string(as_string_builder* sb, const char* name, const char* value)
{
	l5_json_key(sb, name);

	if (value) {
		l5_json_quote(sb, value);
	}
	else {
		as_string_builder_append(sb, "null");
	}
}

static void
l5_json_int(as_string_builder* sb, const char* name, int64_t value)
{
	l5_json_key(sb, name);
	as_string_builder_append_int64(sb, value);
}

// A t or interval that may be absent: L5_NONE is null.
static void
l5_json_optional(as_string_builder* sb, const char* name, int64_t value)
{
	l5_json_key(sb, name);

	if (value == L5_NONE) {
		as_string_builder_append(sb, "null");
	}
	else {
		as_string_builder_append_int64(sb, value);
	}
}

static void
l5_json_bool(as_string_builder* sb, const char* name, bool value)
{
	l5_json_key(sb, name);
	as_string_builder_append(sb, value ? "true" : "false");
}

//---------------------------------
// Output
//---------------------------------

void
l5_output_init(l5_output* o, FILE* out, FILE* err)
{
	o->out = out;
	o->err = err;
	o->open = false;
	pthread_mutex_init(&o->lock, NULL);
}

// Under the output lock.
static void
l5_output_print(l5_output* o, l5_line_kind kind, const char* type, uint64_t t, const char* body)
{
	FILE* f;

	if (o->open || kind == L5_LINE_NOT_CREATED) {
		f = o->out;
	}
	else if (kind == L5_LINE_FAILURE) {
		return;
	}
	else {
		f = o->err;
	}
	fprintf(f, "{\"type\":\"%s\",\"t\":%" PRIu64 "%s}\n", type, t, body ? body : "");
	fflush(f);
}

// Stamp t and write one line. body is the object's fields after type and t, each with its
// leading comma. Never call a client function while holding the lock: the client's log callback
// takes it.
uint64_t
l5_output_write(l5_output* o, l5_line_kind kind, const char* type, const char* body)
{
	pthread_mutex_lock(&o->lock);
	uint64_t t = l5_now();
	l5_output_print(o, kind, type, t, body);
	pthread_mutex_unlock(&o->lock);
	return t;
}

// Write ready to stdout and open it.
void
l5_output_open(l5_output* o, const char* client_version, const char* shape)
{
	as_string_builder sb;
	as_string_builder_init(&sb, 256, true);
	pthread_mutex_lock(&o->lock);
	uint64_t t = l5_now();
	l5_json_int(&sb, "contract", L5_CONTRACT);
	l5_json_int(&sb, "epochMs", (int64_t)l5_epoch_ms());
	l5_json_string(&sb, "client", "c");
	l5_json_string(&sb, "clientVersion", client_version);
	l5_json_string(&sb, "shape", shape);
	o->open = true;
	l5_output_print(o, L5_LINE_REPLY, "ready", t, sb.data);
	pthread_mutex_unlock(&o->lock);
	as_string_builder_destroy(&sb);
}

static void
l5_output_error(l5_output* o, const char* message)
{
	as_string_builder sb;
	as_string_builder_init(&sb, 256, true);
	l5_json_string(&sb, "message", message);
	l5_output_write(o, L5_LINE_REPLY, "error", sb.data);
	as_string_builder_destroy(&sb);
}

bool
l5_log_callback(as_log_level level, const char* func, const char* file, uint32_t line,
	const char* fmt, ...)
{
	(void)func;
	(void)file;
	(void)line;

	l5_output* o = l5_log_output;

	if (level > AS_LOG_LEVEL_WARN || !o) {
		return true;
	}

	char msg[L5_LOG_SIZE];
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(msg, sizeof(msg), fmt, ap);
	va_end(ap);

	as_string_builder sb;
	as_string_builder_init(&sb, 256, true);
	l5_json_string(&sb, "level", level == AS_LOG_LEVEL_ERROR ? "ERROR" : "WARN");
	l5_json_string(&sb, "message", msg);
	l5_output_write(o, L5_LINE_REPLY, "log", sb.data);
	as_string_builder_destroy(&sb);
	return true;
}

//---------------------------------
// Options
//---------------------------------

static int
l5_fail(char* msg, size_t size, const char* fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(msg, size, fmt, ap);
	va_end(ap);
	return 2;
}

// Parse HOST:PORT, an IPv6 host in brackets, into host without brackets and port.
bool
l5_parse_host_port(const char* value, char* host, size_t size, uint16_t* port)
{
	const char* colon = strrchr(value, ':');

	if (!colon || colon == value) {
		return false;
	}

	const char* begin = value;
	const char* end = colon;

	if (*begin == '[') {
		if (end - begin < 2 || end[-1] != ']') {
			return false;
		}
		begin++;
		end--;
	}
	else if (memchr(begin, ':', (size_t)(end - begin))) {
		return false;
	}

	size_t len = (size_t)(end - begin);

	if (len == 0 || len >= size) {
		return false;
	}

	const char* digits = colon + 1;

	if (*digits == '\0' || strlen(digits) > 5) {
		return false;
	}

	uint32_t n = 0;

	for (const char* p = digits; *p; p++) {
		if (*p < '0' || *p > '9') {
			return false;
		}
		n = n * 10 + (uint32_t)(*p - '0');
	}

	if (n == 0 || n > 65535) {
		return false;
	}
	memcpy(host, begin, len);
	host[len] = '\0';
	*port = (uint16_t)n;
	return true;
}

// Plain decimal, greater than zero, at most max.
static bool
l5_parse_positive(const char* value, uint32_t max, uint32_t* out)
{
	if (*value == '\0' || strlen(value) > 10) {
		return false;
	}

	uint64_t n = 0;

	for (const char* p = value; *p; p++) {
		if (*p < '0' || *p > '9') {
			return false;
		}
		n = n * 10 + (uint64_t)(*p - '0');
	}

	if (n == 0 || n > max) {
		return false;
	}
	*out = (uint32_t)n;
	return true;
}

static char*
l5_strdup(const char* s)
{
	char* copy = strdup(s);

	if (!copy) {
		fprintf(stderr, "l5_driver: out of memory\n");
		exit(1);
	}
	return copy;
}

// Each entry splits at its first '='. The translator validates the host:port and rejects a
// duplicate key when the port-map is registered.
static int
l5_parse_port_map(const char* value, l5_options* o, char* msg, size_t size)
{
	uint32_t capacity = 1;

	for (const char* p = value; *p; p++) {
		if (*p == ',') {
			capacity++;
		}
	}
	o->port_map = calloc(capacity, sizeof(as_addr_map));

	if (!o->port_map) {
		return l5_fail(msg, size, "out of memory");
	}

	const char* entry = value;

	while (true) {
		const char* comma = strchr(entry, ',');
		size_t len = comma ? (size_t)(comma - entry) : strlen(entry);
		const char* eq = memchr(entry, '=', len);

		if (!eq || eq == entry || eq + 1 == entry + len) {
			return l5_fail(msg, size, "--port-map entry must be NODE=HOST:PORT: %.*s", (int)len, entry);
		}

		as_addr_map* m = &o->port_map[o->port_map_size++];
		m->orig = l5_strdup(entry);
		m->orig[eq - entry] = '\0';
		m->alt = l5_strdup(eq + 1);
		m->alt[len - (size_t)(eq - entry) - 1] = '\0';

		if (!comma) {
			return 0;
		}
		entry = comma + 1;
	}
}

void
l5_options_destroy(l5_options* o)
{
	for (uint32_t i = 0; i < o->port_map_size; i++) {
		free(o->port_map[i].orig);
		free(o->port_map[i].alt);
	}
	free(o->port_map);
	free(o->user);
	free(o->password);
	free(o->ns);
	free(o->set);
	memset(o, 0, sizeof(*o));
}

static const char* const L5_FLAGS[] = {
	"--seed", "--shape", "--port-map", "--on-miss", "--user", "--tend-interval", "--timeout",
	"--namespace", "--set", "--workers", "--keys"
};

#define L5_FLAG_COUNT (sizeof(L5_FLAGS) / sizeof(L5_FLAGS[0]))

// Parse the flags of argv[1..argc-1] into o. Returns 0, or 2 with the reason in msg. o is
// always left for l5_options_destroy().
int
l5_parse_options(int argc, char** argv, const char* password, l5_options* o, char* msg, size_t size)
{
	memset(o, 0, sizeof(*o));
	o->on_miss = AS_TRANSLATION_STRICT;
	o->workers = 4;
	o->keys = 1000;

	bool seen[L5_FLAG_COUNT] = {false};
	const char* shape = NULL;

	for (int i = 1; i < argc; i += 2) {
		const char* flag = argv[i];

		if (strncmp(flag, "--", 2) != 0) {
			return l5_fail(msg, size, "unexpected argument: %s", flag);
		}

		uint32_t f = 0;

		while (f < L5_FLAG_COUNT && strcmp(flag, L5_FLAGS[f]) != 0) {
			f++;
		}

		if (f == L5_FLAG_COUNT) {
			return l5_fail(msg, size, "unknown flag: %s", flag);
		}

		if (i + 1 >= argc) {
			return l5_fail(msg, size, "%s needs a value", flag);
		}

		if (seen[f]) {
			return l5_fail(msg, size, "%s given twice", flag);
		}
		seen[f] = true;

		const char* value = argv[i + 1];
		bool ok = true;

		switch (f) {
		case 0:
			ok = l5_parse_host_port(value, o->seed_host, sizeof(o->seed_host), &o->seed_port);
			break;
		case 1:
			ok = strcmp(value, "pass-through") == 0 || strcmp(value, "port-map") == 0;
			shape = value;
			break;
		case 2: {
			int rv = l5_parse_port_map(value, o, msg, size);

			if (rv != 0) {
				return rv;
			}
			break;
		}
		case 3:
			if (strcmp(value, "strict") == 0) {
				o->on_miss = AS_TRANSLATION_STRICT;
			}
			else if (strcmp(value, "pass-through") == 0) {
				o->on_miss = AS_TRANSLATION_PASS_THROUGH;
			}
			else {
				ok = false;
			}
			break;
		case 4:
			if (!password) {
				return l5_fail(msg, size, "--user needs AEROSPIKE_ADMIN_PASSWORD in the environment");
			}
			o->user = l5_strdup(value);
			o->password = l5_strdup(password);
			break;
		case 5:
			ok = l5_parse_positive(value, INT32_MAX, &o->tend_interval);
			break;
		case 6:
			ok = l5_parse_positive(value, INT32_MAX, &o->timeout);
			break;
		case 7:
			ok = value[0] && strlen(value) < AS_NAMESPACE_MAX_SIZE;
			o->ns = l5_strdup(value);
			break;
		case 8:
			ok = value[0] && strlen(value) < AS_SET_MAX_SIZE;
			o->set = l5_strdup(value);
			break;
		case 9:
			ok = l5_parse_positive(value, L5_WORKERS_MAX, &o->workers);
			break;
		case 10:
			ok = l5_parse_positive(value, INT32_MAX, &o->keys);
			break;
		}

		if (!ok) {
			return l5_fail(msg, size, "invalid %s: %s", flag, value);
		}
	}

	for (uint32_t f = 0; f < L5_FLAG_COUNT; f++) {
		bool required = f == 0 || f == 1 || (f >= 5 && f <= 8);

		if (required && !seen[f]) {
			return l5_fail(msg, size, "missing %s", L5_FLAGS[f]);
		}
	}
	o->pass_through = strcmp(shape, "pass-through") == 0;

	if (o->pass_through && o->port_map) {
		return l5_fail(msg, size, "--port-map only with --shape port-map");
	}

	if (!o->pass_through && !o->port_map) {
		return l5_fail(msg, size, "missing --port-map with --shape port-map");
	}
	return 0;
}

// Build the client configuration from the flags. A translator setup error is a bad flag.
as_status
l5_configure(const l5_options* o, as_config* c, as_error* err)
{
	as_config_init(c);
	as_config_add_host(c, o->seed_host, o->seed_port);

	if (o->user && !as_config_set_user(c, o->user, o->password)) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "--user or its password is too long");
	}
	c->tender_interval = o->tend_interval;
	c->conn_timeout_ms = o->timeout;
	c->policies.read.base.socket_timeout = o->timeout;
	c->policies.read.base.total_timeout = o->timeout;
	c->policies.read.base.max_retries = 0;
	c->policies.write.base.socket_timeout = o->timeout;
	c->policies.write.base.total_timeout = o->timeout;
	c->policies.write.base.max_retries = 0;

	as_status status;

	if (o->pass_through) {
		c->use_services_alternate = true;
		status = as_config_set_pass_through_translator(c, err);
	}
	else {
		c->use_services_alternate = false;
		status = as_config_set_port_map_translator(c, o->port_map, o->port_map_size, err);
	}
	// The library's default is AS_TRANSLATION_PASS_THROUGH; the contract's is strict.
	c->translation.on_miss = o->on_miss;
	return status;
}

//---------------------------------
// Routing reads
//---------------------------------

static bool
l5_stopping(l5_driver* d)
{
	return as_load_uint8_acq(&d->stop) != 0;
}

void
l5_address_format(struct sockaddr* addr, char* host, socklen_t size, uint16_t* port)
{
	as_address_short_name(addr, host, size);
	*port = as_address_port(addr);
}

static int
l5_compare_nodes(const void* a, const void* b)
{
	return strcmp((*(as_node* const*)a)->name, (*(as_node* const*)b)->name);
}

static int8_t
l5_reach_get(l5_driver* d, as_node* node)
{
	int8_t reachable = L5_NONE;
	pthread_mutex_lock(&d->reach_lock);

	for (uint32_t i = 0; i < d->reach_size; i++) {
		if (d->reach[i].node == node) {
			reachable = d->reach[i].reachable;
			break;
		}
	}
	pthread_mutex_unlock(&d->reach_lock);
	return reachable;
}

static void
l5_view_reserve(l5_view* v, uint32_t size)
{
	if (size <= v->capacity) {
		return;
	}

	l5_node_view* nodes = realloc(v->nodes, sizeof(l5_node_view) * size);

	if (!nodes) {
		fprintf(stderr, "l5_driver: out of memory\n");
		exit(1);
	}
	v->nodes = nodes;
	v->capacity = size;
}

// Read the nodes the client holds, sorted by name, and the masters of --namespace. A partition
// is unowned when its master is missing, not active, or not a node the client holds. While the
// namespace has no partition table, every partition is unowned.
static void
l5_view_read(l5_driver* d, l5_view* v)
{
	as_cluster* cluster = d->as->cluster;
	as_nodes* nodes = as_nodes_reserve(cluster);
	uint32_t size = nodes->size;
	as_node* stack[16];
	as_node** sorted = size <= 16 ? stack : malloc(sizeof(as_node*) * size);

	if (!sorted) {
		fprintf(stderr, "l5_driver: out of memory\n");
		exit(1);
	}
	memcpy(sorted, nodes->array, sizeof(as_node*) * size);
	qsort(sorted, size, sizeof(as_node*), l5_compare_nodes);
	l5_view_reserve(v, size);
	v->size = size;

	for (uint32_t i = 0; i < size; i++) {
		as_node* node = sorted[i];
		l5_node_view* n = &v->nodes[i];
		n->node = node;
		as_strncpy(n->name, node->name, sizeof(n->name));
		as_address* address = as_node_get_address(node);
		l5_address_format((struct sockaddr*)&address->addr, n->host, sizeof(n->host), &n->port);
		n->active = as_node_is_active(node);
		n->reachable = l5_reach_get(d, node);
		n->owned = 0;
	}

	as_partition_table* table = as_partition_tables_get(&cluster->partition_tables, d->opt->ns);

	if (!table) {
		v->unowned = cluster->n_partitions ? cluster->n_partitions : L5_PARTITIONS;
	}
	else {
		v->unowned = 0;

		for (uint32_t p = 0; p < table->size; p++) {
			as_node* master = as_node_load(&table->partitions[p].nodes[0]);
			uint32_t i = 0;

			while (i < size && v->nodes[i].node != master) {
				i++;
			}

			if (!master || i == size || !v->nodes[i].active) {
				v->unowned++;
			}
			else {
				v->nodes[i].owned++;
			}
		}
	}

	if (sorted != stack) {
		free(sorted);
	}
	as_nodes_release(nodes);
}

//---------------------------------
// Load
//---------------------------------

static const char*
l5_error_message(const as_error* err)
{
	return err->message[0] ? err->message : as_error_string(err->code);
}

static void
l5_ready(l5_driver* d);

static void
l5_load_failed(l5_driver* d, uint64_t started_at, const as_error* err)
{
	const char* message = l5_error_message(err);

	pthread_mutex_lock(&d->load_lock);
	d->failed++;
	d->has_last_error = true;
	as_strncpy(d->last_error, message, sizeof(d->last_error));
	pthread_mutex_unlock(&d->load_lock);

	as_string_builder sb;
	as_string_builder_init(&sb, 256, true);
	l5_json_int(&sb, "startedAt", (int64_t)started_at);
	l5_json_string(&sb, "message", message);
	l5_output_write(d->out, L5_LINE_FAILURE, "failure", sb.data);
	as_string_builder_destroy(&sb);
}

static void
l5_load_succeeded(l5_driver* d, uint64_t first_ns, uint64_t end_ns)
{
	pthread_mutex_lock(&d->load_lock);
	d->succeeded++;

	if (first_ns >= d->reset_ns && end_ns - first_ns > d->max_latency_ns) {
		d->max_latency_ns = end_ns - first_ns;
	}
	pthread_mutex_unlock(&d->load_lock);

	if (as_cas_uint8(&d->started, 0, 1)) {
		l5_ready(d);
	}
}

// One attempt of the load's command for iteration i. A get of a record not written yet
// succeeds, as it does in Java, where get returns null.
static as_status
l5_load_attempt(l5_driver* d, as_error* err, as_key* key, uint64_t i)
{
	if (i % 2 == 0) {
		as_record rec;
		as_record_init(&rec, 1);
		as_record_set_int64(&rec, "v", (int64_t)i);
		as_status status = aerospike_key_put(d->as, err, NULL, key, &rec);
		as_record_destroy(&rec);
		return status;
	}

	as_record* rec = NULL;
	as_status status = aerospike_key_get(d->as, err, NULL, key, &rec);

	if (rec) {
		as_record_destroy(rec);
	}
	return status == AEROSPIKE_ERR_RECORD_NOT_FOUND ? AEROSPIKE_OK : status;
}

// Worker w: iteration i uses key load-<(w + i * workers) % keys>. A failed attempt sleeps one
// tend interval and issues the same command again.
static void*
l5_worker_run(void* udata)
{
	l5_driver* d = udata;
	pthread_mutex_lock(&d->load_lock);
	uint32_t w = d->workers_running++;
	pthread_cond_broadcast(&d->load_cond);
	pthread_mutex_unlock(&d->load_lock);

	const l5_options* o = d->opt;
	char name[32];

	for (uint64_t i = 0; !l5_stopping(d); i++) {
		snprintf(name, sizeof(name), "load-%" PRIu64, (w + i * o->workers) % o->keys);

		as_key key;
		as_key_init_str(&key, o->ns, o->set, name);
		uint64_t first_ns = l5_mono_ns();
		bool done = false;

		while (true) {
			uint64_t started_at = l5_now();
			as_error err;

			if (l5_load_attempt(d, &err, &key, i) == AEROSPIKE_OK) {
				done = true;
				break;
			}
			l5_load_failed(d, started_at, &err);

			if (l5_stopping(d)) {
				break;
			}
			as_sleep(o->tend_interval);

			if (l5_stopping(d)) {
				break;
			}
		}
		as_key_destroy(&key);

		if (!done) {
			break;
		}
		l5_load_succeeded(d, first_ns, l5_mono_ns());
	}

	pthread_mutex_lock(&d->load_lock);
	d->workers_running--;
	pthread_cond_broadcast(&d->load_cond);
	pthread_mutex_unlock(&d->load_lock);
	return NULL;
}

// Start the workers detached; each counts itself in workers_running before its first command,
// so the start waits until all are counted.
bool
l5_load_start(l5_driver* d)
{
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
	uint32_t started = 0;

	for (uint32_t w = 0; w < d->opt->workers; w++) {
		pthread_t thread;

		if (pthread_create(&thread, &attr, l5_worker_run, d) != 0) {
			break;
		}
		started++;
	}
	pthread_attr_destroy(&attr);

	pthread_mutex_lock(&d->load_lock);

	while (d->workers_running < started) {
		pthread_cond_wait(&d->load_cond, &d->load_lock);
	}
	pthread_mutex_unlock(&d->load_lock);
	return started == d->opt->workers;
}

static void
l5_timespec_after(struct timespec* ts, uint32_t ms)
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	uint64_t ns = (uint64_t)tv.tv_usec * 1000 + (uint64_t)ms * 1000000;
	ts->tv_sec = tv.tv_sec + (time_t)(ns / 1000000000);
	ts->tv_nsec = (long)(ns % 1000000000);
}

// Wait until *count is zero or bound_ms passed; returns whether it reached zero.
static bool
l5_wait_zero(pthread_mutex_t* lock, pthread_cond_t* cond, uint32_t* count, uint32_t bound_ms)
{
	uint64_t deadline = l5_now() + bound_ms;
	pthread_mutex_lock(lock);

	while (*count > 0 && l5_now() < deadline) {
		struct timespec ts;
		l5_timespec_after(&ts, 100);
		pthread_cond_timedwait(cond, lock, &ts);
	}

	bool zero = *count == 0;
	pthread_mutex_unlock(lock);
	return zero;
}

//---------------------------------
// Snapshots
//---------------------------------

// Under the output lock.
static uint64_t
l5_unowned_longest(l5_driver* d, uint64_t t, uint32_t unowned)
{
	if (unowned > 0 && d->unowned_since < 0) {
		d->unowned_since = (int64_t)t;
	}
	else if (unowned == 0 && d->unowned_since >= 0) {
		uint64_t stretch = t - (uint64_t)d->unowned_since;

		if (stretch > d->longest_closed) {
			d->longest_closed = stretch;
		}
		d->unowned_since = L5_NONE;
	}

	if (d->unowned_since >= 0) {
		uint64_t open = t - (uint64_t)d->unowned_since;
		return open > d->longest_closed ? open : d->longest_closed;
	}
	return d->longest_closed;
}

// The routing as the change check compares it: unowned, and name, host, port and active per node.
static void
l5_view_signature(const l5_view* v, as_string_builder* sb)
{
	as_string_builder_reset(sb);
	as_string_builder_append_uint(sb, v->unowned);

	for (uint32_t i = 0; i < v->size; i++) {
		const l5_node_view* n = &v->nodes[i];
		as_string_builder_append_char(sb, '|');
		as_string_builder_append(sb, n->name);
		as_string_builder_append_char(sb, ',');
		as_string_builder_append(sb, n->host);
		as_string_builder_append_char(sb, ',');
		as_string_builder_append_uint(sb, n->port);
		as_string_builder_append(sb, n->active ? ",1" : ",0");
	}
}

// The nodes, unowned and owners fields of a snapshot.
static void
l5_view_json(const l5_view* v, as_string_builder* sb)
{
	as_string_builder_reset(sb);
	l5_json_key(sb, "nodes");
	as_string_builder_append_char(sb, '[');

	for (uint32_t i = 0; i < v->size; i++) {
		const l5_node_view* n = &v->nodes[i];

		if (i > 0) {
			as_string_builder_append_char(sb, ',');
		}
		as_string_builder_append(sb, "{\"name\":");
		l5_json_quote(sb, n->name);
		l5_json_string(sb, "host", n->host);
		l5_json_int(sb, "port", n->port);
		l5_json_string(sb, "address", n->host);
		l5_json_bool(sb, "active", n->active);
		l5_json_key(sb, "reachable");
		as_string_builder_append(sb, n->reachable == L5_NONE ? "null" : n->reachable ? "true" : "false");
		as_string_builder_append_char(sb, '}');
	}
	as_string_builder_append_char(sb, ']');
	l5_json_int(sb, "unowned", v->unowned);
	l5_json_key(sb, "owners");
	as_string_builder_append_char(sb, '{');
	bool first = true;

	for (uint32_t i = 0; i < v->size; i++) {
		if (v->nodes[i].owned == 0) {
			continue;
		}

		if (!first) {
			as_string_builder_append_char(sb, ',');
		}
		first = false;
		l5_json_quote(sb, v->nodes[i].name);
		as_string_builder_append_char(sb, ':');
		as_string_builder_append_uint(sb, v->nodes[i].owned);
	}
	as_string_builder_append_char(sb, '}');
}

// Under the output lock: longestUnownedMs and the load's fields.
static void
l5_snapshot_tail(l5_driver* d, uint64_t longest, as_string_builder* sb)
{
	as_string_builder_reset(sb);
	l5_json_int(sb, "longestUnownedMs", (int64_t)longest);
	pthread_mutex_lock(&d->load_lock);
	l5_json_int(sb, "succeeded", (int64_t)d->succeeded);
	l5_json_int(sb, "failed", (int64_t)d->failed);
	l5_json_int(sb, "maxLatencyMs", (int64_t)(d->max_latency_ns / 1000000));
	l5_json_string(sb, "lastError", d->has_last_error ? d->last_error : NULL);
	pthread_mutex_unlock(&d->load_lock);
}

// The heartbeat due after the one due at heartbeat, for a poll at t: one interval later, or the
// next poll when the poll fell more than one interval behind.
uint64_t
l5_next_heartbeat(uint64_t heartbeat, uint64_t t)
{
	uint64_t next = heartbeat + L5_HEARTBEAT_MS;
	return next > t ? next : t + 1;
}

// Under the output lock.
static void
l5_snapshot_print(l5_driver* d, const char* reason, uint64_t t, const char* nodes, const char* tail)
{
	FILE* f = d->out->out;
	fprintf(f, "{\"type\":\"snapshot\",\"t\":%" PRIu64 ",\"reason\":\"%s\"%s%s}\n", t, reason, nodes, tail);
	fflush(f);
}

// Every L5_POLL_MS: a change snapshot when the routing differs from the last poll, and a
// heartbeat snapshot every L5_HEARTBEAT_MS.
static void*
l5_routing_run(void* udata)
{
	l5_driver* d = udata;
	l5_view view = {0};
	as_string_builder signature;
	as_string_builder nodes;
	as_string_builder tail;
	as_string_builder_init(&signature, 256, true);
	as_string_builder_init(&nodes, 1024, true);
	as_string_builder_init(&tail, 512, true);
	bool first = true;

	while (!l5_stopping(d)) {
		uint64_t begin = l5_now();
		l5_view_read(d, &view);
		l5_view_signature(&view, &signature);
		l5_view_json(&view, &nodes);

		bool changed = !first && strcmp(signature.data, d->signature) != 0;
		free(d->signature);
		d->signature = l5_strdup(signature.data);

		pthread_mutex_lock(&d->out->lock);
		uint64_t t = l5_now();
		l5_snapshot_tail(d, l5_unowned_longest(d, t, view.unowned), &tail);

		if (first) {
			d->next_heartbeat = t;
		}
		else if (changed) {
			l5_snapshot_print(d, "change", t, nodes.data, tail.data);
		}

		if (t >= d->next_heartbeat) {
			l5_snapshot_print(d, "heartbeat", t, nodes.data, tail.data);
			d->next_heartbeat = l5_next_heartbeat(d->next_heartbeat, t);
		}
		pthread_mutex_unlock(&d->out->lock);
		first = false;
		l5_sleep_until(begin, L5_POLL_MS);
	}

	as_string_builder_destroy(&signature);
	as_string_builder_destroy(&nodes);
	as_string_builder_destroy(&tail);
	free(view.nodes);
	return NULL;
}

static void
l5_ready(l5_driver* d)
{
	l5_output_open(d->out, aerospike_client_version, d->opt->pass_through ? "pass-through" : "port-map");

	pthread_mutex_lock(&d->thread_lock);

	if (!l5_stopping(d) && pthread_create(&d->routing_thread, NULL, l5_routing_run, d) == 0) {
		d->routing_started = true;
	}
	pthread_mutex_unlock(&d->thread_lock);
}

//---------------------------------
// Reachability
//---------------------------------

static l5_reach_entry*
l5_reach_find(l5_driver* d, as_node* node)
{
	for (uint32_t i = 0; i < d->reach_size; i++) {
		if (d->reach[i].node == node) {
			return &d->reach[i];
		}
	}
	return NULL;
}

// Reachable: an info request for "node" answers with the node's name within
// L5_REACH_TIMEOUT_MS. The checker holds its own reference to the node.
static void*
l5_reach_check(void* udata)
{
	l5_reach_arg* arg = udata;
	l5_driver* d = arg->d;
	as_node* node = arg->node;
	free(arg);

	as_policy_info policy;
	as_policy_info_init(&policy);
	policy.timeout = L5_REACH_TIMEOUT_MS;

	as_error err;
	char* response = NULL;
	bool reachable = false;

	if (aerospike_info_node(d->as, &err, &policy, node, "node", &response) == AEROSPIKE_OK && response) {
		char* value = NULL;
		reachable = as_info_parse_single_response(response, &value) == AEROSPIKE_OK &&
			strcmp(value, node->name) == 0;
	}
	free(response);

	pthread_mutex_lock(&d->reach_lock);
	l5_reach_entry* entry = l5_reach_find(d, node);

	if (entry) {
		entry->reachable = reachable ? 1 : 0;
		entry->checking = false;
	}
	d->reach_in_flight--;
	pthread_cond_broadcast(&d->reach_cond);
	pthread_mutex_unlock(&d->reach_lock);
	as_node_release(node);
	return NULL;
}

// Start one detached checker for each node not already being checked, and drop the entries of
// nodes the client no longer holds.
static void
l5_reach_round(l5_driver* d)
{
	as_nodes* nodes = as_nodes_reserve(d->as->cluster);
	pthread_mutex_lock(&d->reach_lock);

	as_node** gone = malloc(sizeof(as_node*) * (d->reach_size + nodes->size + 1));
	uint32_t gone_size = 0;

	if (!gone) {
		fprintf(stderr, "l5_driver: out of memory\n");
		exit(1);
	}

	for (uint32_t i = 0; i < d->reach_size;) {
		bool held = false;

		for (uint32_t j = 0; j < nodes->size && !held; j++) {
			held = nodes->array[j] == d->reach[i].node;
		}

		if (held) {
			i++;
			continue;
		}
		gone[gone_size++] = d->reach[i].node;
		d->reach[i] = d->reach[--d->reach_size];
	}

	pthread_attr_t attr;
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

	for (uint32_t j = 0; j < nodes->size; j++) {
		as_node* node = nodes->array[j];
		l5_reach_entry* entry = l5_reach_find(d, node);

		if (!entry) {
			if (d->reach_size == d->reach_capacity) {
				uint32_t capacity = d->reach_capacity ? d->reach_capacity * 2 : 8;
				l5_reach_entry* reach = realloc(d->reach, sizeof(l5_reach_entry) * capacity);

				if (!reach) {
					fprintf(stderr, "l5_driver: out of memory\n");
					exit(1);
				}
				d->reach = reach;
				d->reach_capacity = capacity;
			}
			as_node_reserve(node);
			entry = &d->reach[d->reach_size++];
			entry->node = node;
			entry->reachable = L5_NONE;
			entry->checking = false;
		}

		if (entry->checking) {
			continue;
		}

		l5_reach_arg* arg = malloc(sizeof(l5_reach_arg));

		if (!arg) {
			continue;
		}
		arg->d = d;
		arg->node = node;
		as_node_reserve(node);
		entry->checking = true;
		d->reach_in_flight++;
		pthread_t thread;

		if (pthread_create(&thread, &attr, l5_reach_check, arg) != 0) {
			entry->checking = false;
			d->reach_in_flight--;
			gone[gone_size++] = node;
			free(arg);
		}
	}
	pthread_attr_destroy(&attr);
	pthread_mutex_unlock(&d->reach_lock);

	for (uint32_t i = 0; i < gone_size; i++) {
		as_node_release(gone[i]);
	}
	free(gone);
	as_nodes_release(nodes);
}

void*
l5_reach_run(void* udata)
{
	l5_driver* d = udata;

	while (!l5_stopping(d)) {
		uint64_t begin = l5_now();
		l5_reach_round(d);

		while (!l5_stopping(d) && l5_now() - begin < L5_REACH_INTERVAL_MS) {
			as_sleep(L5_POLL_MS);
		}
	}
	return NULL;
}

//---------------------------------
// Watch
//---------------------------------

void
l5_probe_init(l5_probe* p, const char* host, uint16_t port)
{
	memset(p, 0, sizeof(*p));
	as_strncpy(p->host, host, sizeof(p->host));
	p->port = port;
	p->serving_at = L5_NONE;
	p->last_closed_at = L5_NONE;
}

static uint32_t
l5_probe_address_index(l5_probe* p, const char* address)
{
	for (uint32_t i = 0; i < p->size; i++) {
		if (strcmp(p->addresses[i].address, address) == 0) {
			return i;
		}
	}

	if (p->size == L5_PROBE_MAX) {
		return L5_PROBE_MAX;
	}

	l5_probe_address* a = &p->addresses[p->size];
	as_strncpy(a->address, address, sizeof(a->address));
	a->first_connected_at = L5_NONE;
	return p->size++;
}

static void
l5_probe_connected(l5_probe* p, uint32_t index, uint64_t t)
{
	if (p->addresses[index].first_connected_at == L5_NONE) {
		p->addresses[index].first_connected_at = (int64_t)t;
	}
}

// Resolve the host and connect to every address it resolves to at once, each within
// L5_PROBE_CONNECT_MS, then close them. Returns whether every address connected.
static bool
l5_probe_connect_all(l5_probe* p, uint64_t t)
{
	char port[8];
	snprintf(port, sizeof(port), "%u", p->port);

	struct addrinfo hints;
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_protocol = IPPROTO_TCP;

	struct addrinfo* res = NULL;

	if (getaddrinfo(p->host, port, &hints, &res) != 0) {
		return false;
	}

	struct pollfd fds[L5_PROBE_MAX];
	uint32_t index[L5_PROBE_MAX];
	bool seen[L5_PROBE_MAX] = {false};
	uint32_t n = 0;
	uint32_t total = 0;
	uint32_t connected = 0;

	for (struct addrinfo* ai = res; ai; ai = ai->ai_next) {
		char address[INET6_ADDRSTRLEN];
		uint16_t ignored;
		l5_address_format(ai->ai_addr, address, sizeof(address), &ignored);
		uint32_t a = l5_probe_address_index(p, address);

		if (a == L5_PROBE_MAX || seen[a]) {
			continue;
		}
		seen[a] = true;
		total++;

		int fd = socket(ai->ai_family, SOCK_STREAM, 0);

		if (fd < 0) {
			continue;
		}

		int flags = fcntl(fd, F_GETFL, 0);

		if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
			close(fd);
			continue;
		}

		if (connect(fd, ai->ai_addr, ai->ai_addrlen) == 0) {
			l5_probe_connected(p, a, t);
			connected++;
			close(fd);
		}
		else if (errno == EINPROGRESS) {
			fds[n].fd = fd;
			fds[n].events = POLLOUT;
			fds[n].revents = 0;
			index[n] = a;
			n++;
		}
		else {
			close(fd);
		}
	}
	freeaddrinfo(res);

	uint64_t deadline = l5_mono_ns() + (uint64_t)L5_PROBE_CONNECT_MS * 1000000;
	uint32_t pending = n;

	while (pending > 0) {
		uint64_t now = l5_mono_ns();

		if (now >= deadline) {
			break;
		}

		int rv = poll(fds, n, (int)((deadline - now + 999999) / 1000000));

		if (rv < 0 && errno != EINTR) {
			break;
		}

		for (uint32_t i = 0; rv > 0 && i < n; i++) {
			if (fds[i].fd < 0 || fds[i].revents == 0) {
				continue;
			}

			int so_error = 0;
			socklen_t len = sizeof(so_error);

			if (getsockopt(fds[i].fd, SOL_SOCKET, SO_ERROR, &so_error, &len) == 0 && so_error == 0) {
				l5_probe_connected(p, index[i], t);
				connected++;
			}
			close(fds[i].fd);
			fds[i].fd = -1;
			pending--;
		}
	}

	for (uint32_t i = 0; i < n; i++) {
		if (fds[i].fd >= 0) {
			close(fds[i].fd);
		}
	}
	return total > 0 && connected == total;
}

// Run the round that starts at t. servingAt is the start of the first of L5_SERVING_ROUNDS
// consecutive rounds in which every address connected.
bool
l5_probe_round(l5_probe* p, uint64_t t)
{
	if (l5_probe_connect_all(p, t)) {
		if (p->serving_rounds++ == 0) {
			p->serving_start = t;
		}

		if (p->serving_rounds == L5_SERVING_ROUNDS) {
			p->serving_at = (int64_t)p->serving_start;
		}
		return true;
	}
	p->serving_rounds = 0;
	p->last_closed_at = (int64_t)t;
	return false;
}

// Usable: the client holds the node, it is active, and no partition is unowned.
static bool
l5_usable(l5_driver* d, l5_view* v, const char* name)
{
	l5_view_read(d, v);

	for (uint32_t i = 0; i < v->size; i++) {
		if (strcmp(v->nodes[i].name, name) == 0) {
			return v->nodes[i].active && v->unowned == 0;
		}
	}
	return false;
}

static void
l5_watch_reply(l5_watch* w, int64_t usable_at)
{
	l5_probe* p = &w->probe;
	as_string_builder sb;
	as_string_builder_init(&sb, 512, true);
	l5_json_string(&sb, "address", w->address);
	l5_json_string(&sb, "node", w->node);
	l5_json_optional(&sb, "servingAt", p->serving_at);
	l5_json_optional(&sb, "lastClosedAt", p->last_closed_at);
	l5_json_key(&sb, "addresses");
	as_string_builder_append_char(&sb, '[');

	for (uint32_t i = 0; i < p->size; i++) {
		if (i > 0) {
			as_string_builder_append_char(&sb, ',');
		}
		as_string_builder_append(&sb, "{\"address\":");
		l5_json_quote(&sb, p->addresses[i].address);
		l5_json_optional(&sb, "firstConnectedAt", p->addresses[i].first_connected_at);
		as_string_builder_append_char(&sb, '}');
	}
	as_string_builder_append_char(&sb, ']');
	l5_json_optional(&sb, "usableAt", usable_at);
	l5_json_optional(&sb, "usableMs", usable_at == L5_NONE ? L5_NONE : usable_at - p->serving_at);
	l5_json_bool(&sb, "ownershipChecked", true);
	l5_output_write(w->d->out, L5_LINE_REPLY, "watch", sb.data);
	as_string_builder_destroy(&sb);
}

// Probe every L5_PROBE_INTERVAL_MS until the listener serves, then check every
// L5_PROBE_INTERVAL_MS whether the node is usable, and reply. A round that takes longer delays
// the next one. After quit a watch may still reply until the quit deadline, then ends without
// a reply, since a listener that never serves has no 60 s bound.
static void*
l5_watch_run(void* udata)
{
	l5_watch* w = udata;
	l5_driver* d = w->d;

	while (!l5_stopping(d) || l5_now() < d->quit_deadline) {
		uint64_t begin = l5_now();

		if (w->probe.serving_at == L5_NONE) {
			l5_probe_round(&w->probe, begin);
		}

		if (w->probe.serving_at != L5_NONE) {
			uint64_t t = l5_now();

			if (l5_usable(d, &w->view, w->node)) {
				l5_watch_reply(w, (int64_t)t);
				break;
			}

			if ((int64_t)t - w->probe.serving_at >= L5_WATCH_BOUND_MS) {
				l5_watch_reply(w, L5_NONE);
				break;
			}
		}
		l5_sleep_until(begin, L5_PROBE_INTERVAL_MS);
	}

	pthread_mutex_lock(&d->watch_lock);
	d->watch_active = false;
	pthread_mutex_unlock(&d->watch_lock);
	free(w->view.nodes);
	free(w);
	return NULL;
}

// Start a watch unless one is running.
static l5_watch_start_result
l5_watch_start(l5_driver* d, const char* address, const char* host, uint16_t port, const char* node)
{
	pthread_mutex_lock(&d->watch_lock);

	if (d->watch_active) {
		pthread_mutex_unlock(&d->watch_lock);
		return L5_WATCH_RUNNING;
	}

	if (d->watch_joinable) {
		pthread_join(d->watch_thread, NULL);
		d->watch_joinable = false;
	}

	l5_watch* w = calloc(1, sizeof(l5_watch));

	if (!w) {
		fprintf(stderr, "l5_driver: out of memory\n");
		exit(1);
	}
	w->d = d;
	as_strncpy(w->address, address, sizeof(w->address));
	as_strncpy(w->node, node, sizeof(w->node));
	l5_probe_init(&w->probe, host, port);
	d->watch_active = true;
	l5_watch_start_result result = L5_WATCH_STARTED;

	if (pthread_create(&d->watch_thread, NULL, l5_watch_run, w) == 0) {
		d->watch_joinable = true;
	}
	else {
		d->watch_active = false;
		free(w);
		result = L5_WATCH_FAILED;
	}
	pthread_mutex_unlock(&d->watch_lock);
	return result;
}

static void
l5_watch_join(l5_driver* d)
{
	pthread_mutex_lock(&d->watch_lock);
	bool joinable = d->watch_joinable;
	d->watch_joinable = false;
	pthread_mutex_unlock(&d->watch_lock);

	if (joinable) {
		pthread_join(d->watch_thread, NULL);
	}
}

//---------------------------------
// Commands
//---------------------------------

// Take keys STEP-0, STEP-1, ... keeping the first that falls in each partition, until every
// partition has one. index[p] is the i of partition p's key, or UINT32_MAX. Returns the number
// of partitions with a key.
uint32_t
l5_serve_keys(const char* ns, const char* set, const char* step, uint32_t n_partitions, uint32_t* index)
{
	size_t size = strlen(step) + 16;
	char* name = malloc(size);

	if (!name) {
		fprintf(stderr, "l5_driver: out of memory\n");
		exit(1);
	}

	for (uint32_t p = 0; p < n_partitions; p++) {
		index[p] = UINT32_MAX;
	}

	uint32_t found = 0;

	for (uint32_t i = 0; i < L5_KEY_SEARCH_LIMIT && found < n_partitions; i++) {
		snprintf(name, size, "%s-%u", step, i);
		as_key key;
		as_key_init_str(&key, ns, set, name);
		uint32_t p = as_partition_getid(as_key_digest(&key)->value, n_partitions);
		as_key_destroy(&key);

		if (index[p] == UINT32_MAX) {
			index[p] = i;
			found++;
		}
	}
	free(name);
	return found;
}

static uint64_t
l5_elapsed_ms(uint64_t begin_ns)
{
	return (l5_mono_ns() - begin_ns) / 1000000;
}

static void
l5_first_failure(char* first, size_t size, const char* what, const as_error* err)
{
	if (!first[0]) {
		snprintf(first, size, "%s: %s", what, l5_error_message(err));
	}
}

typedef enum l5_put_get_e {
	L5_PUT_FAILED,
	L5_GET_FAILED,
	L5_READ,
	L5_MISMATCHED
} l5_put_get_result;

// Put value under key, then get it back, one attempt each. A get that returns another value or
// no record is a mismatch. max_ns, when set, keeps the longest single put or get.
static l5_put_get_result
l5_put_get(l5_driver* d, as_key* key, const char* value, const char* what, char* first, size_t size,
	uint64_t* max_ns)
{
	as_error err;
	as_record rec;
	as_record_init(&rec, 1);
	as_record_set_str(&rec, "v", value);
	uint64_t begin = l5_mono_ns();
	as_status status = aerospike_key_put(d->as, &err, NULL, key, &rec);
	uint64_t elapsed = l5_mono_ns() - begin;
	as_record_destroy(&rec);

	if (max_ns && elapsed > *max_ns) {
		*max_ns = elapsed;
	}

	char label[300];

	if (status != AEROSPIKE_OK) {
		snprintf(label, sizeof(label), "%s put", what);
		l5_first_failure(first, size, label, &err);
		return L5_PUT_FAILED;
	}

	as_record* got = NULL;
	begin = l5_mono_ns();
	status = aerospike_key_get(d->as, &err, NULL, key, &got);
	elapsed = l5_mono_ns() - begin;

	if (max_ns && elapsed > *max_ns) {
		*max_ns = elapsed;
	}

	if (status != AEROSPIKE_OK && status != AEROSPIKE_ERR_RECORD_NOT_FOUND) {
		snprintf(label, sizeof(label), "%s get", what);
		l5_first_failure(first, size, label, &err);
		return L5_GET_FAILED;
	}

	char* v = got ? as_record_get_str(got, "v") : NULL;
	l5_put_get_result result = v && strcmp(v, value) == 0 ? L5_READ : L5_MISMATCHED;

	if (got) {
		as_record_destroy(got);
	}
	return result;
}

typedef struct l5_tally_s {
	uint32_t written;
	uint32_t read;
	uint32_t mismatched;
	uint32_t failures;
} l5_tally;

static void
l5_tally_add(l5_tally* tally, l5_put_get_result result)
{
	switch (result) {
	case L5_PUT_FAILED:
		tally->failures++;
		break;
	case L5_GET_FAILED:
		tally->written++;
		tally->failures++;
		break;
	case L5_READ:
		tally->written++;
		tally->read++;
		break;
	case L5_MISMATCHED:
		tally->written++;
		tally->mismatched++;
		break;
	}
}

static void
l5_tally_json(const l5_tally* tally, const char* first, as_string_builder* sb)
{
	l5_json_int(sb, "written", tally->written);
	l5_json_int(sb, "read", tally->read);
	l5_json_int(sb, "mismatched", tally->mismatched);
	l5_json_int(sb, "failures", tally->failures);
	l5_json_string(sb, "firstFailure", first[0] ? first : NULL);
}

static void
l5_serve(l5_driver* d, const char* step)
{
	uint32_t n = d->as->cluster->n_partitions ? d->as->cluster->n_partitions : L5_PARTITIONS;
	uint32_t* index = malloc(sizeof(uint32_t) * n);

	if (!index) {
		fprintf(stderr, "l5_driver: out of memory\n");
		exit(1);
	}
	l5_serve_keys(d->opt->ns, d->opt->set, step, n, index);

	uint64_t begin = l5_mono_ns();
	l5_tally tally = {0};
	char first[AS_ERROR_MESSAGE_MAX_SIZE + 400] = "";
	size_t size = strlen(step) + 16;
	char* name = malloc(size);
	char* value = malloc(size);

	if (!name || !value) {
		fprintf(stderr, "l5_driver: out of memory\n");
		exit(1);
	}

	for (uint32_t p = 0; p < n; p++) {
		char what[32];
		snprintf(what, sizeof(what), "partition %u", p);

		if (index[p] == UINT32_MAX) {
			tally.failures++;

			if (!first[0]) {
				snprintf(first, sizeof(first), "%s: no key in %u candidates", what, L5_KEY_SEARCH_LIMIT);
			}
			continue;
		}
		snprintf(name, size, "%s-%u", step, index[p]);
		snprintf(value, size, "%s-%u", step, p);

		as_key key;
		as_key_init_str(&key, d->opt->ns, d->opt->set, name);
		l5_tally_add(&tally, l5_put_get(d, &key, value, what, first, sizeof(first), NULL));
		as_key_destroy(&key);
	}
	free(name);
	free(value);
	free(index);

	as_string_builder sb;
	as_string_builder_init(&sb, 512, true);
	l5_json_string(&sb, "step", step);
	l5_json_int(&sb, "partitions", n);
	l5_tally_json(&tally, first, &sb);
	l5_json_int(&sb, "elapsedMs", (int64_t)l5_elapsed_ms(begin));
	l5_output_write(d->out, L5_LINE_REPLY, "served", sb.data);
	as_string_builder_destroy(&sb);
}

// Write and read KEYS keys STEP-0 .. STEP-<KEYS-1>, each with a string of exactly BYTES
// characters that starts with the key's name.
static void
l5_large(l5_driver* d, const char* step, uint32_t bytes, uint32_t keys)
{
	size_t size = strlen(step) + 16;
	char* name = malloc(size);
	char* value = malloc((size_t)bytes + 1);

	if (!name || !value) {
		fprintf(stderr, "l5_driver: out of memory\n");
		exit(1);
	}

	uint64_t begin = l5_mono_ns();
	uint64_t max_ns = 0;
	l5_tally tally = {0};
	char first[AS_ERROR_MESSAGE_MAX_SIZE + 400] = "";

	for (uint32_t i = 0; i < keys; i++) {
		snprintf(name, size, "%s-%u", step, i);
		size_t len = strlen(name);
		memcpy(value, name, len);
		memset(value + len, 'x', bytes - len);
		value[bytes] = '\0';

		char what[300];
		snprintf(what, sizeof(what), "key %s", name);

		as_key key;
		as_key_init_str(&key, d->opt->ns, d->opt->set, name);
		l5_tally_add(&tally, l5_put_get(d, &key, value, what, first, sizeof(first), &max_ns));
		as_key_destroy(&key);
	}
	free(name);
	free(value);

	as_string_builder sb;
	as_string_builder_init(&sb, 512, true);
	l5_json_string(&sb, "step", step);
	l5_json_int(&sb, "bytes", bytes);
	l5_json_int(&sb, "keys", keys);
	l5_tally_json(&tally, first, &sb);
	l5_json_int(&sb, "maxMs", (int64_t)(max_ns / 1000000));
	l5_json_int(&sb, "elapsedMs", (int64_t)l5_elapsed_ms(begin));
	l5_output_write(d->out, L5_LINE_REPLY, "large", sb.data);
	as_string_builder_destroy(&sb);
}

// Restart maxLatencyMs and longestUnownedMs from now.
static void
l5_reset(l5_driver* d)
{
	pthread_mutex_lock(&d->out->lock);
	uint64_t t = l5_now();
	pthread_mutex_lock(&d->load_lock);
	d->reset_ns = l5_mono_ns();
	d->max_latency_ns = 0;
	pthread_mutex_unlock(&d->load_lock);
	d->longest_closed = 0;

	if (d->unowned_since >= 0) {
		d->unowned_since = (int64_t)t;
	}
	l5_output_print(d->out, L5_LINE_REPLY, "reset", t, NULL);
	pthread_mutex_unlock(&d->out->lock);
}

static uint32_t
l5_digits(uint32_t n)
{
	uint32_t digits = 1;

	while (n >= 10) {
		n /= 10;
		digits++;
	}
	return digits;
}

static void
l5_command_error(l5_driver* d, const char* what, const char* line)
{
	size_t size = strlen(what) + strlen(line) + 3;
	char* message = malloc(size);

	if (!message) {
		fprintf(stderr, "l5_driver: out of memory\n");
		exit(1);
	}
	snprintf(message, size, "%s: %s", what, line);
	l5_output_error(d->out, message);
	free(message);
}

// Run one command line. Returns false on quit.
bool
l5_command(l5_driver* d, const char* line)
{
	char* copy = l5_strdup(line);
	char* words[5];
	uint32_t n = 0;
	char* save = NULL;

	for (char* word = strtok_r(copy, " \t\r\n\f\v", &save); word; word = strtok_r(NULL, " \t\r\n\f\v", &save)) {
		if (n == 5) {
			n++;
			break;
		}
		words[n++] = word;
	}

	bool more = true;
	const char* command = n > 0 ? words[0] : "";

	if (strcmp(command, "serve") == 0) {
		if (n != 2) {
			l5_command_error(d, "malformed command", line);
		}
		else {
			l5_serve(d, words[1]);
		}
	}
	else if (strcmp(command, "reset") == 0) {
		if (n != 1) {
			l5_command_error(d, "malformed command", line);
		}
		else {
			l5_reset(d);
		}
	}
	else if (strcmp(command, "watch") == 0) {
		char host[256];
		uint16_t port;

		if (n != 3 || !l5_parse_host_port(words[1], host, sizeof(host), &port) ||
			strlen(words[2]) >= AS_NODE_NAME_SIZE) {
			l5_command_error(d, "malformed command", line);
		}
		else {
			l5_watch_start_result started = l5_watch_start(d, words[1], host, port, words[2]);

			if (started == L5_WATCH_RUNNING) {
				l5_command_error(d, "a watch is already running", line);
			}
			else if (started == L5_WATCH_FAILED) {
				l5_command_error(d, "cannot start the watch thread", line);
			}
		}
	}
	else if (strcmp(command, "large") == 0) {
		uint32_t bytes;
		uint32_t keys;

		if (n != 4 || !l5_parse_positive(words[2], L5_LARGE_BYTES_MAX, &bytes) ||
			!l5_parse_positive(words[3], L5_LARGE_KEYS_MAX, &keys) ||
			bytes < strlen(words[1]) + 1 + l5_digits(keys - 1)) {
			l5_command_error(d, "malformed command", line);
		}
		else {
			l5_large(d, words[1], bytes, keys);
		}
	}
	else if (strcmp(command, "quit") == 0) {
		if (n != 1) {
			l5_command_error(d, "malformed command", line);
		}
		else {
			more = false;
		}
	}
	else {
		l5_command_error(d, "unknown command", line);
	}
	free(copy);
	return more;
}

//---------------------------------
// Driver
//---------------------------------

void
l5_driver_init(l5_driver* d, l5_options* opt, l5_output* out, aerospike* as)
{
	memset(d, 0, sizeof(*d));
	d->opt = opt;
	d->out = out;
	d->as = as;
	d->unowned_since = L5_NONE;
	d->reset_ns = l5_mono_ns();
	pthread_mutex_init(&d->load_lock, NULL);
	pthread_cond_init(&d->load_cond, NULL);
	pthread_mutex_init(&d->thread_lock, NULL);
	pthread_mutex_init(&d->reach_lock, NULL);
	pthread_cond_init(&d->reach_cond, NULL);
	pthread_mutex_init(&d->watch_lock, NULL);
}

// Stop the load, letting each worker finish its current attempt, and let a running watch reply,
// both within quit_ms; then stop the routing and reachability threads and drain the running
// checkers. Returns whether every worker and checker exited, so that the client may be closed.
bool
l5_driver_stop(l5_driver* d, uint32_t quit_ms)
{
	d->quit_deadline = l5_now() + quit_ms;
	as_store_uint8_rls(&d->stop, 1);
	bool workers = l5_wait_zero(&d->load_lock, &d->load_cond, &d->workers_running, quit_ms);
	l5_watch_join(d);

	pthread_mutex_lock(&d->thread_lock);
	bool routing = d->routing_started;
	bool reach = d->reach_started;
	d->routing_started = false;
	d->reach_started = false;
	pthread_mutex_unlock(&d->thread_lock);

	if (routing) {
		pthread_join(d->routing_thread, NULL);
	}

	if (reach) {
		pthread_join(d->reach_thread, NULL);
	}

	bool checkers = l5_wait_zero(&d->reach_lock, &d->reach_cond, &d->reach_in_flight, L5_REACH_DRAIN_MS);

	if (!workers) {
		fprintf(stderr, "l5_driver: %u workers still running after %u ms\n", d->workers_running, quit_ms);
	}

	if (!checkers) {
		fprintf(stderr, "l5_driver: %u reachability checks still running after %d ms\n", d->reach_in_flight,
			L5_REACH_DRAIN_MS);
	}

	if (!workers || !checkers) {
		return false;
	}

	pthread_mutex_lock(&d->reach_lock);

	for (uint32_t i = 0; i < d->reach_size; i++) {
		as_node_release(d->reach[i].node);
	}
	free(d->reach);
	d->reach = NULL;
	d->reach_size = 0;
	d->reach_capacity = 0;
	pthread_mutex_unlock(&d->reach_lock);
	free(d->signature);
	d->signature = NULL;
	return true;
}

#if !defined(L5_DRIVER_NO_MAIN)

static l5_driver l5_main_driver;

int
main(int argc, char** argv)
{
	l5_clock_start();
	signal(SIGPIPE, SIG_IGN);

	l5_options opt;
	char msg[512];

	if (l5_parse_options(argc, argv, getenv("AEROSPIKE_ADMIN_PASSWORD"), &opt, msg, sizeof(msg)) != 0) {
		fprintf(stderr, "ERROR: %s\n%s\n", msg, L5_USAGE);
		l5_options_destroy(&opt);
		return 2;
	}

	static l5_output out;
	l5_output_init(&out, stdout, stderr);
	l5_log_output = &out;
	as_log_set_level(AS_LOG_LEVEL_WARN);
	as_log_set_callback(l5_log_callback);

	as_config config;
	as_error err;

	if (l5_configure(&opt, &config, &err) != AEROSPIKE_OK) {
		fprintf(stderr, "ERROR: %s\n%s\n", err.message, L5_USAGE);
		as_config_destroy(&config);
		l5_options_destroy(&opt);
		return 2;
	}

	aerospike as;
	aerospike_init(&as, &config);

	if (aerospike_connect(&as, &err) != AEROSPIKE_OK) {
		char message[AS_ERROR_MESSAGE_MAX_SIZE + 32];
		snprintf(message, sizeof(message), "client not created: %s", l5_error_message(&err));

		as_string_builder sb;
		as_string_builder_init(&sb, 256, true);
		l5_json_string(&sb, "message", message);
		l5_output_write(&out, L5_LINE_NOT_CREATED, "error", sb.data);
		as_string_builder_destroy(&sb);
		fprintf(stderr, "ERROR: %s\n", message);
		aerospike_destroy(&as);
		l5_options_destroy(&opt);
		return 3;
	}

	l5_driver* d = &l5_main_driver;
	l5_driver_init(d, &opt, &out, &as);

	pthread_mutex_lock(&d->thread_lock);
	d->reach_started = pthread_create(&d->reach_thread, NULL, l5_reach_run, d) == 0;
	pthread_mutex_unlock(&d->thread_lock);

	int status = 0;

	if (!d->reach_started || !l5_load_start(d)) {
		fprintf(stderr, "l5_driver: cannot start the driver's threads\n");
		status = 1;
	}
	else {
		char* line = NULL;
		size_t capacity = 0;
		ssize_t len;

		while ((len = getline(&line, &capacity, stdin)) >= 0) {
			while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r')) {
				line[--len] = '\0';
			}

			if (!l5_command(d, line)) {
				break;
			}
		}
		free(line);
	}

	if (!l5_driver_stop(d, L5_QUIT_MS)) {
		// Threads are still inside client calls: skip the exit handlers that would run under them.
		fflush(stdout);
		fflush(stderr);
		_exit(status);
	}
	aerospike_close(&as, &err);
	aerospike_destroy(&as);
	l5_log_output = NULL;
	l5_options_destroy(&opt);
	return status;
}

#endif
