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
 * U-8: translate() of the built-in translators (static map, port-map and pass-through) makes no
 * system call other than memory management and time, and its p99 is below 1 ms. Needs no
 * server. Built against the installed headers only by "make discovery-translate-test".
 *
 * On Linux (x86_64 and aarch64) each guarded run forks a child that installs a seccomp filter
 * allowing only brk, mmap, munmap, mremap, mprotect, madvise, futex, exit, exit_group,
 * rt_sigreturn, clock_gettime and gettimeofday. Any other system call raises SIGSYS, whose
 * handler records the call and exits the child with GUARD_TRAPPED. The positive controls check
 * that a socket, a name lookup and a file open are caught.
 */
#include <aerospike/as_config.h>
#include <aerospike/as_discovery.h>
#include <aerospike/as_error.h>
#include <aerospike/as_host.h>
#include <aerospike/as_log.h>
#include <citrusleaf/cf_clock.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "test.h"

#if defined(__linux__) && (defined(__x86_64__) || defined(__aarch64__))
#define GUARD_SUPPORTED 1
#include <errno.h>
#include <linux/audit.h>
#include <linux/filter.h>
#include <linux/seccomp.h>
#include <netdb.h>
#include <signal.h>
#include <stddef.h>
#include <sys/mman.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>

#if defined(__x86_64__)
#define GUARD_ARCH AUDIT_ARCH_X86_64
#else
#define GUARD_ARCH AUDIT_ARCH_AARCH64
#endif

#if !defined(SECCOMP_RET_KILL_PROCESS)
#define SECCOMP_RET_KILL_PROCESS SECCOMP_RET_KILL
#endif
#endif

//---------------------------------
// Globals
//---------------------------------

int g_argc = 0;
char** g_argv = NULL;

//---------------------------------
// Translators
//---------------------------------

#define ENTRIES 64
#define CALLS 10000
#define SAMPLES 10000

// Child exit codes.
#define GUARD_OK 0
#define GUARD_TRAPPED 3
#define GUARD_WRONG 4

static as_addr_map g_static_entries[ENTRIES];
static char g_static_orig[ENTRIES][32];
static char g_static_alt[ENTRIES][32];

// Port-map entries: ENTRIES endpoint keys, a node-name key and a bracketed IPv6 key.
static as_addr_map g_port_entries[ENTRIES + 2];
static char g_port_orig[ENTRIES][40];
static char g_port_alt[ENTRIES][40];

static as_address_translator g_port_map;
static as_address_translator g_pass_through;
static bool g_translators;

static void
endpoint_set(as_endpoint* e, const char* node_name, const char* host, uint16_t port)
{
	e->node_name = node_name;
	e->tls_name = "tls";
	e->host = host;
	e->port = port;
	e->source = AS_ENDPOINT_SOURCE_STANDARD;
}

// Built once, before any guard is armed: construction is not translate.
static bool
translators_init(void)
{
	if (g_translators) {
		return true;
	}

	for (uint32_t i = 0; i < ENTRIES; i++) {
		snprintf(g_static_orig[i], sizeof(g_static_orig[i]), "10.0.0.%u", i);
		snprintf(g_static_alt[i], sizeof(g_static_alt[i]), "198.51.100.%u", i);
		g_static_entries[i].orig = g_static_orig[i];
		g_static_entries[i].alt = g_static_alt[i];

		snprintf(g_port_orig[i], sizeof(g_port_orig[i]), "10.0.0.%u:3000", i);
		snprintf(g_port_alt[i], sizeof(g_port_alt[i]), "198.51.100.%u:4000", i);
		g_port_entries[i].orig = g_port_orig[i];
		g_port_entries[i].alt = g_port_alt[i];
	}
	g_port_entries[ENTRIES].orig = "B7";
	g_port_entries[ENTRIES].alt = "203.0.113.7:4007";
	g_port_entries[ENTRIES + 1].orig = "[2001:db8::1]:3000";
	g_port_entries[ENTRIES + 1].alt = "[2001:db8::2]:4000";

	as_error err;

	if (as_discovery_port_map_translator(&err, g_port_entries, ENTRIES + 2, &g_port_map) !=
		AEROSPIKE_OK) {
		error("port-map translator: %s", err.message);
		return false;
	}
	as_discovery_pass_through_translator(&g_pass_through);
	g_translators = true;
	return true;
}

static bool
mapped_to(bool mapped, as_host* out, const char* host, uint16_t port)
{
	bool ok = mapped && out->name && strcmp(out->name, host) == 0 && out->port == port;

	if (mapped) {
		as_host_destroy(out);
	}
	return ok;
}

// Each workload runs CALLS translations of each of its endpoints and returns false on a wrong
// result. They run in the guarded child, so they make no log or stdio call.

static bool
run_static_map(void)
{
	as_endpoint hit;
	as_endpoint miss;
	endpoint_set(&hit, "A1", "10.0.0.63", 3000);
	endpoint_set(&miss, "A1", "192.0.2.1", 3000);

	for (uint32_t i = 0; i < CALLS; i++) {
		as_host out;

		if (! mapped_to(as_discovery_static_map_translate(g_static_entries, ENTRIES, &hit, &out),
			&out, "198.51.100.63", 3000)) {
			return false;
		}

		if (as_discovery_static_map_translate(g_static_entries, ENTRIES, &miss, &out)) {
			as_host_destroy(&out);
			return false;
		}
	}
	return true;
}

static bool
run_port_map(void)
{
	as_endpoint endpoint_key;
	as_endpoint node_key;
	as_endpoint miss;
	as_endpoint ipv6;
	endpoint_set(&endpoint_key, "A1", "10.0.0.63", 3000);
	endpoint_set(&node_key, "B7", "192.0.2.7", 3000);
	endpoint_set(&miss, "ZZ", "192.0.2.1", 3000);
	endpoint_set(&ipv6, "A1", "2001:db8::1", 3000);
	void* udata = g_port_map.udata;

	for (uint32_t i = 0; i < CALLS; i++) {
		as_host out;

		if (! mapped_to(g_port_map.translate(&endpoint_key, &out, udata), &out, "198.51.100.63",
			4000)) {
			return false;
		}

		if (! mapped_to(g_port_map.translate(&node_key, &out, udata), &out, "203.0.113.7", 4007)) {
			return false;
		}

		if (g_port_map.translate(&miss, &out, udata)) {
			as_host_destroy(&out);
			return false;
		}

		if (! mapped_to(g_port_map.translate(&ipv6, &out, udata), &out, "2001:db8::2", 4000)) {
			return false;
		}
	}
	return true;
}

static bool
run_pass_through(void)
{
	as_endpoint endpoint;
	endpoint_set(&endpoint, "A1", "10.0.0.63", 3000);

	for (uint32_t i = 0; i < CALLS; i++) {
		as_host out;

		if (! mapped_to(g_pass_through.translate(&endpoint, &out, g_pass_through.udata), &out,
			"10.0.0.63", 3000)) {
			return false;
		}
	}
	return true;
}

//---------------------------------
// Guard
//---------------------------------

#if defined(GUARD_SUPPORTED)

// Shared with the guarded child, which stores the trapped system call number here.
static volatile long* g_trapped;

static const char*
syscall_name(long nr)
{
	switch (nr) {
#if defined(__NR_open)
		case __NR_open: return "open";
#endif
		case __NR_openat: return "openat";
		case __NR_read: return "read";
		case __NR_write: return "write";
		case __NR_close: return "close";
		case __NR_socket: return "socket";
		case __NR_connect: return "connect";
		case __NR_sendto: return "sendto";
		case __NR_recvfrom: return "recvfrom";
		case __NR_ioctl: return "ioctl";
		case __NR_fstat: return "fstat";
#if defined(__NR_newfstatat)
		case __NR_newfstatat: return "newfstatat";
#endif
#if defined(__NR_stat)
		case __NR_stat: return "stat";
#endif
		case __NR_getpid: return "getpid";
		case __NR_uname: return "uname";
		default: return "?";
	}
}

static void
guard_trap(int sig, siginfo_t* si, void* context)
{
	(void)sig;
	(void)context;
	*g_trapped = si->si_syscall;
	_exit(GUARD_TRAPPED);
}

#define GUARD_ALLOW(nr) \
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, (nr), 0, 1), \
	BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ALLOW)

// Return 0, or the errno of the step that failed. On success every system call outside the allow
// list traps.
static int
guard_arm(void)
{
	struct sock_filter filter[] = {
		// x32 calls carry the x86_64 arch, but their numbers match no allowed number.
		BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(struct seccomp_data, arch)),
		BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, GUARD_ARCH, 1, 0),
		BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_KILL_PROCESS),
		BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(struct seccomp_data, nr)),
		GUARD_ALLOW(__NR_brk),
		GUARD_ALLOW(__NR_mmap),
		GUARD_ALLOW(__NR_munmap),
		GUARD_ALLOW(__NR_mremap),
		GUARD_ALLOW(__NR_mprotect),
		GUARD_ALLOW(__NR_madvise),
		GUARD_ALLOW(__NR_futex),
		GUARD_ALLOW(__NR_exit),
		GUARD_ALLOW(__NR_exit_group),
		GUARD_ALLOW(__NR_rt_sigreturn),
		GUARD_ALLOW(__NR_clock_gettime),
		GUARD_ALLOW(__NR_gettimeofday),
		BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_TRAP),
	};
	struct sock_fprog prog = {
		.len = (unsigned short)(sizeof(filter) / sizeof(filter[0])),
		.filter = filter,
	};

	struct sigaction sa;
	memset(&sa, 0, sizeof(sa));
	sa.sa_sigaction = guard_trap;
	sa.sa_flags = SA_SIGINFO;
	sigemptyset(&sa.sa_mask);

	if (sigaction(SIGSYS, &sa, NULL) != 0 || prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 ||
		prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, &prog) != 0) {
		return errno;
	}
	return 0;
}

typedef bool (*guard_fn)(void);

// Run fn in a child under the guard. Return true when it ends without a trapped system call and
// with right results, and describe the outcome in msg.
static bool
guarded(guard_fn fn, char* msg, size_t size)
{
	*g_trapped = -1;
	fflush(stdout);
	fflush(stderr);
	pid_t pid = fork();

	if (pid < 0) {
		snprintf(msg, size, "fork failed: %d", errno);
		return false;
	}

	if (pid == 0) {
		// A child that hangs is killed instead of hanging the run.
		alarm(60);

		int rv = guard_arm();

		if (rv != 0) {
			*g_trapped = -2 - rv;
			_exit(5);
		}
		_exit(fn() ? GUARD_OK : GUARD_WRONG);
	}

	int status = 0;

	while (waitpid(pid, &status, 0) < 0) {
		if (errno != EINTR) {
			snprintf(msg, size, "waitpid failed: %d", errno);
			return false;
		}
	}

	if (WIFEXITED(status)) {
		switch (WEXITSTATUS(status)) {
			case GUARD_OK:
				snprintf(msg, size, "no system call outside the allow list");
				return true;
			case GUARD_TRAPPED:
				snprintf(msg, size, "system call %ld (%s)", *g_trapped, syscall_name(*g_trapped));
				return false;
			case GUARD_WRONG:
				snprintf(msg, size, "wrong translation result");
				return false;
			case 5:
				snprintf(msg, size, "the guard could not be armed: errno %ld", -2 - *g_trapped);
				return false;
			default:
				snprintf(msg, size, "child exited with %d", WEXITSTATUS(status));
				return false;
		}
	}

	if (WIFSIGNALED(status)) {
		snprintf(msg, size, "child killed by signal %d", WTERMSIG(status));
		return false;
	}
	snprintf(msg, size, "child status %d", status);
	return false;
}

static bool
guard_init(void)
{
	if (g_trapped) {
		return true;
	}

	void* page = mmap(NULL, sizeof(long), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);

	if (page == MAP_FAILED) {
		return false;
	}
	g_trapped = page;
	return true;
}

// Positive controls: each must be trapped.

static bool
control_socket(void)
{
	int fd = socket(AF_INET, SOCK_STREAM, 0);
	return fd < 0;
}

static bool
control_lookup(void)
{
	struct addrinfo* res = NULL;
	int rv = getaddrinfo("translator-guard.invalid", NULL, NULL, &res);

	if (rv == 0) {
		freeaddrinfo(res);
	}
	return false;
}

static bool
control_open(void)
{
	FILE* f = fopen("/dev/null", "r");
	return f == NULL;
}

#endif // GUARD_SUPPORTED

#if defined(GUARD_SUPPORTED)
#define guard_or_skip(fn) \
	do { \
		assert_true(translators_init()); \
		assert_true(guard_init()); \
		char __msg[128]; \
		bool __ok = guarded(fn, __msg, sizeof(__msg)); \
		info("%s: %s", #fn, __msg); \
		assert_log(! __ok, "%s: %s", #fn, __msg); \
	} while (0)

#define control_or_skip(fn, expected) \
	do { \
		assert_true(guard_init()); \
		char __msg[128]; \
		bool __ok = guarded(fn, __msg, sizeof(__msg)); \
		long __nr = *g_trapped; \
		info("%s: %s", #fn, __msg); \
		assert_log(__ok || __nr < 0, "%s was not trapped: %s", #fn, __msg); \
		assert_log((expected) >= 0 && __nr != (expected), "%s trapped %s, expected %ld", #fn, \
			__msg, (long)(expected)); \
	} while (0)
#else
#define guard_or_skip(fn) \
	do { \
		assert_true(translators_init()); \
		info("skip: seccomp guard is Linux only"); \
	} while (0)

#define control_or_skip(fn, expected) info("skip: seccomp guard is Linux only")
#endif

//---------------------------------
// Latency
//---------------------------------

static int
cmp_u64(const void* a, const void* b)
{
	uint64_t x = *(const uint64_t*)a;
	uint64_t y = *(const uint64_t*)b;
	return x < y ? -1 : (x > y ? 1 : 0);
}

typedef bool (*translate_fn)(const as_endpoint* endpoint, as_host* out, void* udata);

static bool
static_map_fn(const as_endpoint* endpoint, as_host* out, void* udata)
{
	(void)udata;
	return as_discovery_static_map_translate(g_static_entries, ENTRIES, endpoint, out);
}

// p99 of SAMPLES translations of endpoint in nanoseconds, measured outside any guard.
static uint64_t
p99_ns(translate_fn fn, void* udata, const as_endpoint* endpoint)
{
	uint64_t* samples = malloc(sizeof(uint64_t) * SAMPLES);

	if (! samples) {
		return UINT64_MAX;
	}

	for (uint32_t i = 0; i < SAMPLES; i++) {
		as_host out;
		uint64_t begin = cf_getns();
		bool mapped = fn(endpoint, &out, udata);
		samples[i] = cf_getns() - begin;

		if (mapped) {
			as_host_destroy(&out);
		}
	}
	qsort(samples, SAMPLES, sizeof(uint64_t), cmp_u64);
	uint64_t p99 = samples[SAMPLES * 99 / 100];
	free(samples);
	return p99;
}

//---------------------------------
// Tests
//---------------------------------

TEST(translate_static_map_no_io, "U-8 static map translate makes no I/O system call")
{
	guard_or_skip(run_static_map);
}

TEST(translate_port_map_no_io, "U-8 port-map translate makes no I/O system call")
{
	guard_or_skip(run_port_map);
}

TEST(translate_pass_through_no_io, "U-8 pass-through translate makes no I/O system call")
{
	guard_or_skip(run_pass_through);
}

TEST(translate_guard_detects_socket, "U-8 the guard catches a socket")
{
	control_or_skip(control_socket, (long)__NR_socket);
}

TEST(translate_guard_detects_lookup, "U-8 the guard catches a name lookup")
{
	control_or_skip(control_lookup, -1L);
}

TEST(translate_guard_detects_open, "U-8 the guard catches a file open")
{
	control_or_skip(control_open, -1L);
}

TEST(translate_p99_below_1ms, "U-8 static map, port-map and pass-through translate p99 is below 1 ms")
{
	assert_true(translators_init());

	as_endpoint endpoint;
	endpoint_set(&endpoint, "A1", "10.0.0.63", 3000);
	uint64_t static_map = p99_ns(static_map_fn, NULL, &endpoint);
	uint64_t port_map = p99_ns(g_port_map.translate, g_port_map.udata, &endpoint);
	uint64_t pass_through = p99_ns(g_pass_through.translate, g_pass_through.udata, &endpoint);
	info("p99 ns: static-map %llu, port-map %llu, pass-through %llu",
		(unsigned long long)static_map, (unsigned long long)port_map,
		(unsigned long long)pass_through);

	assert_log(static_map >= 1000000, "static map p99 %llu ns", (unsigned long long)static_map);
	assert_log(port_map >= 1000000, "port-map p99 %llu ns", (unsigned long long)port_map);
	assert_log(pass_through >= 1000000, "pass-through p99 %llu ns",
		(unsigned long long)pass_through);
}

//---------------------------------
// Test Suite
//---------------------------------

SUITE(discovery_translate_suite, "U-8 translate() makes no I/O and is fast, without a server")
{
	suite_add(translate_static_map_no_io);
	suite_add(translate_port_map_no_io);
	suite_add(translate_pass_through_no_io);
	suite_add(translate_guard_detects_socket);
	suite_add(translate_guard_detects_lookup);
	suite_add(translate_guard_detects_open);
	suite_add(translate_p99_below_1ms);
}

PLAN(discovery_translate_test)
{
	// Translators log nothing; keep any later log call quiet in the guarded child.
	as_log_set_level(AS_LOG_LEVEL_ERROR);
	plan_add(discovery_translate_suite);
}
