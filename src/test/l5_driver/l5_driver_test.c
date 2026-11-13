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
 * Unit tests of the L5 driver's own logic, with no server: JSON quoting, flag parsing, node
 * address formatting, the serve key search, the watch probe, the order of output lines and
 * quit during a watch. Built with L5_DRIVER_NO_MAIN by "make l5-driver-test".
 */
#include "l5_driver/l5_driver.c"

#include "test.h"

//---------------------------------
// Globals
//---------------------------------

int g_argc = 0;
char** g_argv = NULL;

//---------------------------------
// Static Functions
//---------------------------------

static char*
quoted(const char* s)
{
	static char buf[256];
	as_string_builder sb;
	as_string_builder_init(&sb, 64, true);
	l5_json_quote(&sb, s);
	as_strncpy(buf, sb.data, sizeof(buf));
	as_string_builder_destroy(&sb);
	return buf;
}

#define ARGC(a) ((int)(sizeof(a) / sizeof(a[0])))

static int
parse(int argc, char** argv, const char* password, l5_options* o)
{
	char msg[256];
	return l5_parse_options(argc, argv, password, o, msg, sizeof(msg));
}

// argv for a valid port-map run with one flag replaced or appended.
static int
parse_with(const char* flag, const char* value, const char* password)
{
	char* argv[] = {
		"l5_driver", "--seed", "as-endpoint.test:4901", "--shape", "port-map",
		"--port-map", "A=as-endpoint.test:4901", "--tend-interval", "250", "--timeout", "1000",
		"--namespace", "test", "--set", "s", NULL, NULL
	};
	int argc = ARGC(argv) - 2;
	bool replaced = false;

	for (int i = 1; i < argc; i += 2) {
		if (strcmp(argv[i], flag) == 0) {
			argv[i + 1] = (char*)value;
			replaced = true;
		}
	}

	if (!replaced) {
		argv[argc++] = (char*)flag;

		if (value) {
			argv[argc++] = (char*)value;
		}
	}

	l5_options o;
	int rv = parse(argc, argv, password, &o);
	l5_options_destroy(&o);
	return rv;
}

static uint16_t
listen_port(int* fd)
{
	struct sockaddr_in addr;
	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port = 0;

	*fd = socket(AF_INET, SOCK_STREAM, 0);

	if (*fd < 0 || bind(*fd, (struct sockaddr*)&addr, sizeof(addr)) != 0 || listen(*fd, 16) != 0) {
		return 0;
	}

	socklen_t len = sizeof(addr);
	getsockname(*fd, (struct sockaddr*)&addr, &len);
	return ntohs(addr.sin_port);
}

// A port that refuses connections: bound once, then closed.
static uint16_t
closed_port(void)
{
	int fd;
	uint16_t port = listen_port(&fd);
	close(fd);
	return port;
}

static char*
contents(FILE* f)
{
	fflush(f);
	long size = ftell(f);
	char* buf = calloc(1, (size_t)size + 1);
	rewind(f);

	if (fread(buf, 1, (size_t)size, f) != (size_t)size) {
		buf[0] = '\0';
	}
	return buf;
}

static uint32_t
count(const char* text, const char* what)
{
	uint32_t n = 0;

	for (const char* p = strstr(text, what); p; p = strstr(p + 1, what)) {
		n++;
	}
	return n;
}

static void
view_node(l5_node_view* n, const char* name, const char* host, uint16_t port, bool active,
	int8_t reachable, uint32_t owned)
{
	memset(n, 0, sizeof(*n));
	as_strncpy(n->name, name, sizeof(n->name));
	as_strncpy(n->host, host, sizeof(n->host));
	n->port = port;
	n->active = active;
	n->reachable = reachable;
	n->owned = owned;
}

static char*
signature(const l5_view* v)
{
	static char buf[512];
	as_string_builder sb;
	as_string_builder_init(&sb, 64, true);
	l5_view_signature(v, &sb);
	as_strncpy(buf, sb.data, sizeof(buf));
	as_string_builder_destroy(&sb);
	return buf;
}

//---------------------------------
// Test Cases
//---------------------------------

TEST(test_l5_json, "JSON quoting escapes quotes, backslashes, control characters and U+2028, also across a resize")
{
	assert_string_eq(quoted("plain"), "\"plain\"");
	assert_string_eq(quoted("a\"b\\c"), "\"a\\\"b\\\\c\"");
	assert_string_eq(quoted("n\nt\tr\r"), "\"n\\nt\\tr\\r\"");
	assert_string_eq(quoted("x\x01y\x1fz"), "\"x\\u0001y\\u001fz\"");
	assert_string_eq(quoted("ls\xe2\x80\xa8ps\xe2\x80\xa9"), "\"ls\\u2028ps\\u2029\"");
	assert_string_eq(quoted("caf\xc3\xa9"), "\"caf\xc3\xa9\"");
	assert_string_eq(quoted("bad\xff\xc3"), "\"bad\\ufffd\\ufffd\"");

	// Long enough that the builder grows in the middle of the string.
	char long_in[160];
	char long_out[200];
	memset(long_in, 'a', 150);
	long_in[150] = '"';
	long_in[151] = '\0';
	snprintf(long_out, sizeof(long_out), "\"%.150s\\\"\"", long_in);
	assert_string_eq(quoted(long_in), long_out);
}

TEST(test_l5_options_valid, "a valid port-map run keeps name and HOST:PORT keys and defaults to strict")
{
	char* argv[] = {
		"l5_driver", "--seed", "as-endpoint.test:4901", "--shape", "port-map",
		"--port-map", "BB9=as-endpoint.test:4901,10.0.0.2:3000=as-endpoint.test:4902",
		"--tend-interval", "250", "--timeout", "1000", "--namespace", "test", "--set", "l2"
	};
	l5_options o;
	assert_int_eq(parse(ARGC(argv), argv, NULL, &o), 0);
	assert_string_eq(o.seed_host, "as-endpoint.test");
	assert_int_eq(o.seed_port, 4901);
	assert_false(o.pass_through);
	assert_int_eq(o.port_map_size, 2);
	assert_string_eq(o.port_map[0].orig, "BB9");
	assert_string_eq(o.port_map[0].alt, "as-endpoint.test:4901");
	assert_string_eq(o.port_map[1].orig, "10.0.0.2:3000");
	assert_string_eq(o.port_map[1].alt, "as-endpoint.test:4902");
	assert_int_eq(o.on_miss, AS_TRANSLATION_STRICT);
	assert_int_eq(o.tend_interval, 250);
	assert_int_eq(o.timeout, 1000);
	assert_int_eq(o.workers, 4);
	assert_int_eq(o.keys, 1000);
	assert_string_eq(o.ns, "test");
	assert_string_eq(o.set, "l2");
	assert_null(o.user);

	as_config config;
	as_error err;
	as_status status = l5_configure(&o, &config, &err);
	bool alternate = config.use_services_alternate;
	as_translation_miss_policy on_miss = config.translation.on_miss;
	uint32_t retries = config.policies.read.base.max_retries + config.policies.write.base.max_retries;
	uint32_t total = config.policies.write.base.total_timeout;
	as_config_destroy(&config);
	l5_options_destroy(&o);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_false(alternate);
	assert_int_eq(on_miss, AS_TRANSLATION_STRICT);
	assert_int_eq(retries, 0);
	assert_int_eq(total, 1000);
}

TEST(test_l5_options_shapes, "pass-through, explicit on-miss, bracketed IPv6 seed, workers and user")
{
	char* argv[] = {
		"l5_driver", "--seed", "[::1]:3000", "--shape", "pass-through", "--on-miss", "pass-through",
		"--user", "admin", "--tend-interval", "250", "--timeout", "1000", "--namespace", "test",
		"--set", "s", "--workers", "64", "--keys", "10"
	};
	l5_options o;
	assert_int_eq(parse(ARGC(argv), argv, "secret", &o), 0);
	assert_string_eq(o.seed_host, "::1");
	assert_int_eq(o.seed_port, 3000);
	assert_true(o.pass_through);
	assert_int_eq(o.on_miss, AS_TRANSLATION_PASS_THROUGH);
	assert_string_eq(o.user, "admin");
	assert_string_eq(o.password, "secret");
	assert_int_eq(o.workers, 64);
	assert_int_eq(o.keys, 10);

	as_config config;
	as_error err;
	as_status status = l5_configure(&o, &config, &err);
	bool alternate = config.use_services_alternate;
	as_config_destroy(&config);
	l5_options_destroy(&o);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_true(alternate);
}

TEST(test_l5_options_bad, "each bad flag is exit 2")
{
	assert_int_eq(parse_with("--timeout", "1000", NULL), 0);
	assert_int_eq(parse_with("--bogus", "x", NULL), 2);
	assert_int_eq(parse_with("--seed", "as-endpoint.test", NULL), 2);
	assert_int_eq(parse_with("--seed", "host:0", NULL), 2);
	assert_int_eq(parse_with("--seed", "host:70000", NULL), 2);
	assert_int_eq(parse_with("--seed", "::1:3000", NULL), 2);
	assert_int_eq(parse_with("--shape", "nat", NULL), 2);
	assert_int_eq(parse_with("--on-miss", "loose", NULL), 2);
	assert_int_eq(parse_with("--timeout", "0", NULL), 2);
	assert_int_eq(parse_with("--timeout", "-5", NULL), 2);
	assert_int_eq(parse_with("--tend-interval", "abc", NULL), 2);
	assert_int_eq(parse_with("--workers", "0", NULL), 2);
	assert_int_eq(parse_with("--keys", "", NULL), 2);
	assert_int_eq(parse_with("--port-map", "A", NULL), 2);
	assert_int_eq(parse_with("--port-map", "=h:1", NULL), 2);
	assert_int_eq(parse_with("--port-map", "A=h:1,", NULL), 2);
	assert_int_eq(parse_with("--user", "admin", NULL), 2);
	assert_int_eq(parse_with("--user", "admin", "secret"), 0);
	assert_int_eq(parse_with("--keys", NULL, NULL), 2);
	assert_int_eq(parse_with("stray", NULL, NULL), 2);

	char* twice[] = {
		"l5_driver", "--seed", "h:1", "--seed", "h:2", "--shape", "pass-through", "--tend-interval", "250",
		"--timeout", "1000", "--namespace", "test", "--set", "s"
	};
	l5_options o;
	assert_int_eq(parse(ARGC(twice), twice, NULL, &o), 2);
	l5_options_destroy(&o);

	char* missing[] = {
		"l5_driver", "--shape", "pass-through", "--tend-interval", "250", "--timeout", "1000",
		"--namespace", "test", "--set", "s"
	};
	assert_int_eq(parse(ARGC(missing), missing, NULL, &o), 2);
	l5_options_destroy(&o);

	char* map_with_pass_through[] = {
		"l5_driver", "--seed", "h:1", "--shape", "pass-through", "--port-map", "A=h:1",
		"--tend-interval", "250", "--timeout", "1000", "--namespace", "test", "--set", "s"
	};
	assert_int_eq(parse(ARGC(map_with_pass_through), map_with_pass_through, NULL, &o), 2);
	l5_options_destroy(&o);

	char* port_map_without_map[] = {
		"l5_driver", "--seed", "h:1", "--shape", "port-map", "--tend-interval", "250", "--timeout", "1000",
		"--namespace", "test", "--set", "s"
	};
	assert_int_eq(parse(ARGC(port_map_without_map), port_map_without_map, NULL, &o), 2);
	l5_options_destroy(&o);

	// The translator rejects what the flag split accepts; main() exits 2 on it.
	char* bad_target[] = {
		"l5_driver", "--seed", "h:1", "--shape", "port-map", "--port-map", "A=nohostport,A=h:2",
		"--tend-interval", "250", "--timeout", "1000", "--namespace", "test", "--set", "s"
	};
	assert_int_eq(parse(ARGC(bad_target), bad_target, NULL, &o), 0);
	as_config config;
	as_error err;
	as_status status = l5_configure(&o, &config, &err);
	as_config_destroy(&config);
	l5_options_destroy(&o);
	assert_int_eq(status, AEROSPIKE_ERR_PARAM);
}

TEST(test_l5_address, "node host is the IP without the port, and the port is a number")
{
	struct sockaddr_in in4;
	memset(&in4, 0, sizeof(in4));
	in4.sin_family = AF_INET;
	in4.sin_port = htons(4901);
	inet_pton(AF_INET, "10.213.49.11", &in4.sin_addr);

	char host[INET6_ADDRSTRLEN];
	uint16_t port = 0;
	l5_address_format((struct sockaddr*)&in4, host, sizeof(host), &port);
	assert_string_eq(host, "10.213.49.11");
	assert_int_eq(port, 4901);

	struct sockaddr_in6 in6;
	memset(&in6, 0, sizeof(in6));
	in6.sin6_family = AF_INET6;
	in6.sin6_port = htons(3000);
	inet_pton(AF_INET6, "2001:db8::1", &in6.sin6_addr);
	l5_address_format((struct sockaddr*)&in6, host, sizeof(host), &port);
	assert_string_eq(host, "2001:db8::1");
	assert_int_eq(port, 3000);
}

TEST(test_l5_serve_keys, "the serve key search finds a key in each of the 4096 partitions")
{
	uint32_t* index = malloc(sizeof(uint32_t) * L5_PARTITIONS);
	uint32_t found = l5_serve_keys("test", "l2-serve", "after", L5_PARTITIONS, index);
	uint32_t wrong = 0;

	for (uint32_t p = 0; p < L5_PARTITIONS; p++) {
		char name[32];
		snprintf(name, sizeof(name), "after-%u", index[p]);
		as_key key;
		as_key_init_str(&key, "test", "l2-serve", name);

		if (index[p] == UINT32_MAX || as_partition_getid(as_key_digest(&key)->value, L5_PARTITIONS) != p) {
			wrong++;
		}
		as_key_destroy(&key);
	}
	free(index);
	assert_int_eq(found, L5_PARTITIONS);
	assert_int_eq(wrong, 0);
}

TEST(test_l5_probe, "servingAt is the start of 3 connected rounds; a closed port sets lastClosedAt only")
{
	int fd;
	uint16_t port = listen_port(&fd);
	assert_int_ne(port, 0);

	l5_probe p;
	l5_probe_init(&p, "127.0.0.1", port);
	bool r1 = l5_probe_round(&p, 100);
	int64_t after1 = p.serving_at;
	bool r2 = l5_probe_round(&p, 110);
	int64_t after2 = p.serving_at;
	bool r3 = l5_probe_round(&p, 120);
	close(fd);

	assert_true(r1 && r2 && r3);
	assert_int_eq(after1, L5_NONE);
	assert_int_eq(after2, L5_NONE);
	assert_int_eq(p.serving_at, 100);
	assert_int_eq(p.last_closed_at, L5_NONE);
	assert_int_eq(p.size, 1);
	assert_string_eq(p.addresses[0].address, "127.0.0.1");
	assert_int_eq(p.addresses[0].first_connected_at, 100);

	l5_probe_init(&p, "127.0.0.1", closed_port());
	assert_false(l5_probe_round(&p, 200));
	assert_false(l5_probe_round(&p, 210));
	assert_int_eq(p.serving_at, L5_NONE);
	assert_int_eq(p.last_closed_at, 210);
	assert_int_eq(p.size, 1);
	assert_int_eq(p.addresses[0].first_connected_at, L5_NONE);
}

TEST(test_l5_output_order, "lines before ready go to stderr, failures before ready are dropped, t never goes back")
{
	FILE* out = tmpfile();
	FILE* err = tmpfile();
	l5_output o;
	l5_output_init(&o, out, err);

	l5_output_write(&o, L5_LINE_REPLY, "log", ",\"level\":\"WARN\",\"message\":\"before\"");
	l5_output_write(&o, L5_LINE_FAILURE, "failure", ",\"startedAt\":0,\"message\":\"dropped\"");
	l5_output_write(&o, L5_LINE_NOT_CREATED, "error", ",\"message\":\"client not created: x\"");
	l5_output_open(&o, "7.6.2", "port-map");
	as_sleep(5);
	l5_output_write(&o, L5_LINE_FAILURE, "failure", ",\"startedAt\":0,\"message\":\"after\"");
	l5_output_write(&o, L5_LINE_REPLY, "log", ",\"level\":\"ERROR\",\"message\":\"after\"");

	char* stdout_text = contents(out);
	char* stderr_text = contents(err);
	fclose(out);
	fclose(err);

	const char* error_at = strstr(stdout_text, "\"type\":\"error\"");
	const char* ready_at = strstr(stdout_text, "\"type\":\"ready\"");
	const char* failure_at = strstr(stdout_text, "\"type\":\"failure\"");
	const char* log_at = strstr(stdout_text, "\"type\":\"log\"");
	bool ordered = error_at && ready_at && failure_at && log_at && error_at < ready_at &&
		ready_at < failure_at && failure_at < log_at;
	bool ready_fields = ready_at && strstr(ready_at, "\"contract\":5,\"epochMs\":") &&
		strstr(ready_at, "\"client\":\"c\",\"clientVersion\":\"7.6.2\",\"shape\":\"port-map\"}");
	uint32_t lines = count(stdout_text, "\n");
	uint32_t dropped = count(stdout_text, "dropped") + count(stderr_text, "dropped");
	bool before_on_stderr = strstr(stderr_text, "\"message\":\"before\"") != NULL &&
		count(stderr_text, "\n") == 1;

	int64_t last = -1;
	bool monotonic = true;

	for (const char* p = strstr(stdout_text, "\"t\":"); p; p = strstr(p + 1, "\"t\":")) {
		int64_t t = strtoll(p + 4, NULL, 10);
		monotonic = monotonic && t >= last;
		last = t;
	}
	free(stdout_text);
	free(stderr_text);

	assert_true(ordered);
	assert_true(ready_fields);
	assert_int_eq(lines, 4);
	assert_int_eq(dropped, 0);
	assert_true(before_on_stderr);
	assert_true(monotonic);
}

TEST(test_l5_snapshot_json, "snapshot nodes keep their order, reachable may be null, owners skip nodes without partitions")
{
	l5_node_view nodes[3];
	view_node(&nodes[0], "A1", "10.0.0.1", 4901, true, 1, 4000);
	view_node(&nodes[1], "B2", "10.0.0.2", 4902, false, L5_NONE, 0);
	view_node(&nodes[2], "C3", "10.0.0.3", 4903, true, 0, 0);
	l5_view v = {nodes, 3, 3, 96};

	as_string_builder sb;
	as_string_builder_init(&sb, 64, true);
	l5_view_json(&v, &sb);
	char* json = l5_strdup(sb.data);
	as_string_builder_destroy(&sb);

	const char* expected =
		",\"nodes\":["
		"{\"name\":\"A1\",\"host\":\"10.0.0.1\",\"port\":4901,\"address\":\"10.0.0.1\",\"active\":true,\"reachable\":true},"
		"{\"name\":\"B2\",\"host\":\"10.0.0.2\",\"port\":4902,\"address\":\"10.0.0.2\",\"active\":false,\"reachable\":null},"
		"{\"name\":\"C3\",\"host\":\"10.0.0.3\",\"port\":4903,\"address\":\"10.0.0.3\",\"active\":true,\"reachable\":false}"
		"],\"unowned\":96,\"owners\":{\"A1\":4000}";
	bool equal = strcmp(json, expected) == 0;

	if (!equal) {
		fprintf(stderr, "got %s\n", json);
	}
	free(json);
	assert_true(equal);
}

TEST(test_l5_signature, "a change is a node added or removed, a host, port or active change, or unowned")
{
	l5_node_view nodes[2];
	view_node(&nodes[0], "A1", "10.0.0.1", 4901, true, L5_NONE, 2048);
	view_node(&nodes[1], "B2", "10.0.0.2", 4902, true, L5_NONE, 2048);
	l5_view v = {nodes, 2, 2, 0};
	char base[512];
	as_strncpy(base, signature(&v), sizeof(base));

	nodes[0].reachable = 1;
	nodes[1].owned = 1;
	nodes[0].owned = 4095;
	bool same_without_routing = strcmp(signature(&v), base) == 0;

	nodes[1].active = false;
	bool active = strcmp(signature(&v), base) != 0;
	nodes[1].active = true;

	nodes[1].port = 4903;
	bool port = strcmp(signature(&v), base) != 0;
	nodes[1].port = 4902;

	as_strncpy(nodes[1].host, "10.0.0.9", sizeof(nodes[1].host));
	bool host = strcmp(signature(&v), base) != 0;
	as_strncpy(nodes[1].host, "10.0.0.2", sizeof(nodes[1].host));

	v.unowned = 1;
	bool unowned = strcmp(signature(&v), base) != 0;
	v.unowned = 0;

	v.size = 1;
	bool removed = strcmp(signature(&v), base) != 0;
	v.size = 2;
	bool restored = strcmp(signature(&v), base) == 0;

	assert_true(same_without_routing);
	assert_true(active);
	assert_true(port);
	assert_true(host);
	assert_true(unowned);
	assert_true(removed);
	assert_true(restored);
}

TEST(test_l5_heartbeat, "heartbeats are one interval apart, or the next poll after a stall")
{
	assert_int_eq(l5_next_heartbeat(100, 100), 200);
	assert_int_eq(l5_next_heartbeat(100, 199), 200);
	assert_int_eq(l5_next_heartbeat(100, 200), 201);
	assert_int_eq(l5_next_heartbeat(100, 350), 351);
}

TEST(test_l5_unowned_longest, "longestUnownedMs keeps the longest stretch, counts an open one, and restarts on reset")
{
	FILE* out = tmpfile();
	FILE* err = tmpfile();
	l5_output o;
	l5_output_init(&o, out, err);
	o.open = true;

	l5_options opt;
	memset(&opt, 0, sizeof(opt));
	l5_driver d;
	l5_driver_init(&d, &opt, &o, NULL);

	uint64_t none = l5_unowned_longest(&d, 100, 0);
	uint64_t opened = l5_unowned_longest(&d, 200, 5);
	uint64_t open = l5_unowned_longest(&d, 350, 5);
	uint64_t closed = l5_unowned_longest(&d, 400, 0);
	uint64_t shorter = l5_unowned_longest(&d, 500, 3);
	uint64_t longer = l5_unowned_longest(&d, 800, 2);

	d.max_latency_ns = 42000000;
	l5_reset(&d);
	uint64_t t = l5_now();
	uint64_t after_reset = l5_unowned_longest(&d, t + 50, 2);
	uint64_t after_close = l5_unowned_longest(&d, t + 60, 0);
	uint64_t max_latency = d.max_latency_ns;

	char* stdout_text = contents(out);
	fclose(out);
	fclose(err);
	uint32_t resets = count(stdout_text, "\"type\":\"reset\"");
	free(stdout_text);

	assert_int_eq(none, 0);
	assert_int_eq(opened, 0);
	assert_int_eq(open, 150);
	assert_int_eq(closed, 200);
	assert_int_eq(shorter, 200);
	assert_int_eq(longer, 300);
	assert_true(after_reset >= 50 && after_reset < 1000);
	assert_true(after_close >= 60 && after_close < 1000);
	assert_int_eq(max_latency, 0);
	assert_int_eq(resets, 1);
}

TEST(test_l5_commands, "unknown and malformed commands get an error and quit ends the input")
{
	FILE* out = tmpfile();
	FILE* err = tmpfile();
	l5_output o;
	l5_output_init(&o, out, err);
	o.open = true;

	l5_options opt;
	memset(&opt, 0, sizeof(opt));
	l5_driver d;
	l5_driver_init(&d, &opt, &o, NULL);

	const char* malformed[] = {
		"serve", "serve a b", "serve a b c d e f", "reset now", "quit now",
		"watch 127.0.0.1:3000", "watch nohost N1", "watch 127.0.0.1:0 N1",
		"watch 127.0.0.1:3000 0123456789012345678901234567890",
		"large s 10", "large s 0 1", "large s 10 0", "large s 3 100", "large s 70000000 1"
	};
	const char* unknown[] = {"", "serv x", "QUIT"};
	bool more = true;

	for (uint32_t i = 0; i < ARGC(malformed); i++) {
		more = l5_command(&d, malformed[i]) && more;
	}

	for (uint32_t i = 0; i < ARGC(unknown); i++) {
		more = l5_command(&d, unknown[i]) && more;
	}

	char watch[64];
	snprintf(watch, sizeof(watch), "watch 127.0.0.1:%u N1", closed_port());
	more = l5_command(&d, watch) && more;
	more = l5_command(&d, watch) && more;
	bool quit = l5_command(&d, "quit");
	bool stopped = l5_driver_stop(&d, 50);

	char* stdout_text = contents(out);
	fclose(out);
	fclose(err);
	uint32_t lines = count(stdout_text, "\n");
	uint32_t errors = count(stdout_text, "{\"type\":\"error\"");
	uint32_t malformed_errors = count(stdout_text, "\"message\":\"malformed command: ");
	uint32_t unknown_errors = count(stdout_text, "\"message\":\"unknown command: ");
	uint32_t running = count(stdout_text, "\"message\":\"a watch is already running: ");
	bool named = strstr(stdout_text, "\"message\":\"malformed command: large s 3 100\"") != NULL;
	free(stdout_text);

	assert_true(more);
	assert_false(quit);
	assert_true(stopped);
	assert_int_eq(malformed_errors, ARGC(malformed));
	assert_int_eq(unknown_errors, ARGC(unknown));
	assert_int_eq(running, 1);
	assert_int_eq(errors, ARGC(malformed) + ARGC(unknown) + 1);
	assert_int_eq(lines, errors);
	assert_true(named);
}

TEST(test_l5_quit_watch, "quit ends a watch whose listener never serves at the quit deadline, without a reply")
{
	FILE* out = tmpfile();
	FILE* err = tmpfile();
	l5_output o;
	l5_output_init(&o, out, err);
	o.open = true;

	l5_options opt;
	memset(&opt, 0, sizeof(opt));
	l5_driver d;
	l5_driver_init(&d, &opt, &o, NULL);

	uint16_t port = closed_port();
	char address[32];
	snprintf(address, sizeof(address), "127.0.0.1:%u", port);
	l5_watch_start_result started = l5_watch_start(&d, address, "127.0.0.1", port, "N1");
	l5_watch_start_result second = l5_watch_start(&d, address, "127.0.0.1", port, "N1");
	as_sleep(50);

	uint64_t begin = l5_now();
	bool stopped = l5_driver_stop(&d, 200);
	uint64_t elapsed = l5_now() - begin;
	bool active = d.watch_active;

	char* stdout_text = contents(out);
	fclose(out);
	fclose(err);
	uint32_t replies = count(stdout_text, "\"type\":\"watch\"");
	free(stdout_text);

	assert_int_eq(started, L5_WATCH_STARTED);
	assert_int_eq(second, L5_WATCH_RUNNING);
	assert_true(stopped);
	assert_false(active);
	assert_int_eq(replies, 0);
	assert_true(elapsed >= 200 && elapsed < 2000);
}

//---------------------------------
// Test Suite
//---------------------------------

SUITE(l5_driver_suite, "L5 driver logic without a server")
{
	l5_clock_start();
	suite_add(test_l5_json);
	suite_add(test_l5_options_valid);
	suite_add(test_l5_options_shapes);
	suite_add(test_l5_options_bad);
	suite_add(test_l5_address);
	suite_add(test_l5_serve_keys);
	suite_add(test_l5_probe);
	suite_add(test_l5_output_order);
	suite_add(test_l5_snapshot_json);
	suite_add(test_l5_signature);
	suite_add(test_l5_heartbeat);
	suite_add(test_l5_unowned_longest);
	suite_add(test_l5_commands);
	suite_add(test_l5_quit_watch);
}

PLAN(l5_driver_test)
{
	plan_add(l5_driver_suite);
}
