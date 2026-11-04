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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

#include <aerospike/aerospike.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_config.h>
#include <aerospike/as_discovery.h>
#include <aerospike/as_event.h>
#include <aerospike/as_sleep.h>
#include <citrusleaf/cf_clock.h>
#include <pthread.h>

#include "test.h"
#include "aerospike_test.h"
#include "discovery_fake_node.h"

#if !defined(_WIN32)
#include <dirent.h>
#include <unistd.h>
#endif

// Coverage: U-1 (runtime degradation), U-3, U-5 (through real publication), M-1 to M-12 and a
// local N-12 precursor of the seed manager commit. Manager tests drive as_seed_manager directly
// with scripted providers and condition gates, so cadence and ordering need no wall-clock
// sleeps. Component tests use restartable loopback fake info servers for provider-only
// bootstrap, zero-node reseed, close, shared memory and force_single_node. The shared N-12
// scenario is owned by the final integration commit.
// They also cover the bounded close and reconnect, deferred provider destroy at every site that
// ends a registration, the binding provider's discovery thread, late initial publication after
// the connect fallback, the urgent throttle, thread-mode overrun counting and start-to-start
// cadence, and the ip_map scope of provider-discovered seeds.

//---------------------------------
// Static Functions: shared helpers
//---------------------------------

#if !defined(_WIN32)
// Private: called by aerospike_close() on an unconnected async client.
void
as_config_release_plugins(as_config* config);
#endif

static void
hosts_add(as_vector* hosts, const char* name, const char* tls_name, uint16_t port)
{
	as_host_copy_fields(as_vector_reserve(hosts), name, tls_name, port);
}

static void
hosts_free(as_vector* hosts)
{
	for (uint32_t i = 0; i < hosts->size; i++) {
		as_host_destroy(as_vector_get(hosts, i));
	}
	as_vector_destroy(hosts);
}

// Format hosts as "name:port,..." for order-sensitive comparison.
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

static const char*
snap_str(as_seed_manager* m, char* buf, size_t size)
{
	as_seed_snapshot* s = as_seed_manager_reserve(m);
	hosts_str(as_seed_snapshot_hosts(s), buf, size);
	as_seed_snapshot_release(s);
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
abstime_ms(uint32_t ms, struct timespec* abstime)
{
	struct timespec delta;
	cf_clock_set_timespec_ms((int)ms, &delta);
	cf_clock_current_add(&delta, abstime);
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

// A joined thread can stay listed in /proc for a moment, so poll a thread count until it
// reaches the expected value or two seconds pass.
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

//---------------------------------
// Static Functions: log capture
//---------------------------------

// The discovery and tend threads log concurrently with the test thread. The first messages are
// kept, later ones are only counted, so early warnings are never overwritten.
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

// One-shot hook: run once by the logging thread, outside g_capture_lock, after the first message
// that contains g_hook_needle is recorded.
static const char* g_hook_needle;
static void (*g_hook_fn)(void);

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

	void (*hook)(void) = NULL;

	if (g_hook_fn && strstr(text, g_hook_needle)) {
		hook = g_hook_fn;
		g_hook_fn = NULL;
		g_hook_needle = NULL;
	}

	as_log_callback saved = g_capture_saved;
	pthread_mutex_unlock(&g_capture_lock);

	if (hook) {
		hook();
	}

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
	g_hook_fn = NULL;
	g_hook_needle = NULL;
	pthread_mutex_unlock(&g_capture_lock);
	as_log_set_callback(saved);
}

// Run fn once, on the thread that logs the first message containing needle.
static void
log_capture_hook(const char* needle, void (*fn)(void))
{
	pthread_mutex_lock(&g_capture_lock);
	g_hook_needle = needle;
	g_hook_fn = fn;
	pthread_mutex_unlock(&g_capture_lock);
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
// Static Functions: scripted seed provider
//---------------------------------

#define FP_MAX_CALLS 256

typedef struct fake_provider_s fake_provider;

// Produce the result of one call (1-based). Runs without the provider lock.
typedef as_status (*fake_step)(fake_provider* fp, uint32_t call, as_error* err,
	as_seed_candidates* out);

struct fake_provider_s {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	fake_step step;
	void* arg;
	bool periodic;
	bool deadline;
	bool no_deadline_fn;
	char hosts[512];

	// Calls numbered gate_call and later wait until gate_open. Zero disables the gate.
	uint32_t gate_call;
	bool gate_open;
	bool blocked;

	uint32_t calls;
	uint32_t finished;
	uint32_t active;
	uint32_t max_active;
	uint32_t destroyed;
	uint32_t active_at_destroy;
	uint32_t finished_at_destroy;
	pthread_t destroy_thread;
	pthread_t thread[FP_MAX_CALLS];
	uint64_t iteration[FP_MAX_CALLS];
	uint64_t start_ms[FP_MAX_CALLS];
	uint32_t configured[FP_MAX_CALLS];
	char cluster_name[64];
};

// Append "name:port,name:port" to out.
static as_status
fp_append(as_error* err, as_seed_candidates* out, const char* list)
{
	char copy[512];
	snprintf(copy, sizeof(copy), "%s", list);
	char* save = NULL;

	for (char* item = strtok_r(copy, ",", &save); item; item = strtok_r(NULL, ",", &save)) {
		char* colon = strrchr(item, ':');

		if (!colon) {
			return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "bad fake host");
		}
		*colon = 0;

		if (!as_seed_candidates_append(out, item, NULL, (uint16_t)atoi(colon + 1))) {
			return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "append failed");
		}
	}
	return AEROSPIKE_OK;
}

static as_status
fp_refresh(as_error* err, const as_seed_candidate_context* context, as_seed_candidates* out,
	void* udata)
{
	fake_provider* fp = udata;

	pthread_mutex_lock(&fp->lock);
	uint32_t call = ++fp->calls;

	if (call <= FP_MAX_CALLS) {
		fp->thread[call - 1] = pthread_self();
		fp->iteration[call - 1] = context->tend_iteration;
		fp->start_ms[call - 1] = cf_getms();
		fp->configured[call - 1] = context->configured_seeds->size;
	}
	snprintf(fp->cluster_name, sizeof(fp->cluster_name), "%s",
		context->cluster_name ? context->cluster_name : "");

	if (++fp->active > fp->max_active) {
		fp->max_active = fp->active;
	}
	pthread_cond_broadcast(&fp->cond);

	while (fp->gate_call && call >= fp->gate_call && !fp->gate_open) {
		fp->blocked = true;
		pthread_cond_broadcast(&fp->cond);
		pthread_cond_wait(&fp->cond, &fp->lock);
	}
	fp->blocked = false;

	char hosts[512];
	snprintf(hosts, sizeof(hosts), "%s", fp->hosts);
	fake_step step = fp->step;
	pthread_mutex_unlock(&fp->lock);

	as_status status = step ? step(fp, call, err, out) : fp_append(err, out, hosts);

	pthread_mutex_lock(&fp->lock);
	fp->active--;
	fp->finished++;
	pthread_cond_broadcast(&fp->cond);
	pthread_mutex_unlock(&fp->lock);
	return status;
}

static bool
fp_periodic(void* udata)
{
	return ((fake_provider*)udata)->periodic;
}

static bool
fp_deadline(void* udata)
{
	return ((fake_provider*)udata)->deadline;
}

static void
fp_destroy(void* udata)
{
	fake_provider* fp = udata;
	pthread_mutex_lock(&fp->lock);
	fp->destroyed++;
	fp->active_at_destroy = fp->active;
	fp->finished_at_destroy = fp->finished;
	fp->destroy_thread = pthread_self();
	pthread_cond_broadcast(&fp->cond);
	pthread_mutex_unlock(&fp->lock);
}

static void
fp_init(fake_provider* fp, bool periodic, bool deadline, const char* hosts)
{
	memset(fp, 0, sizeof(fake_provider));
	pthread_mutex_init(&fp->lock, NULL);
	pthread_cond_init(&fp->cond, NULL);
	fp->periodic = periodic;
	fp->deadline = deadline;
	snprintf(fp->hosts, sizeof(fp->hosts), "%s", hosts ? hosts : "");
}

// Release the provider. Call after the config that owns its registration is destroyed.
static void
fp_free(fake_provider* fp)
{
	pthread_cond_destroy(&fp->cond);
	pthread_mutex_destroy(&fp->lock);
}

static void
fp_table(fake_provider* fp, as_seed_provider* provider)
{
	memset(provider, 0, sizeof(as_seed_provider));
	provider->size = sizeof(as_seed_provider);
	provider->api_version = AS_DISCOVERY_API_VERSION;
	provider->name = "fake";
	provider->refresh = fp_refresh;
	provider->needs_periodic_refresh = fp_periodic;
	provider->supports_deadline = fp->no_deadline_fn ? NULL : fp_deadline;
	provider->destroy = fp_destroy;
	provider->udata = fp;
}

static as_status
fp_register(as_config* config, fake_provider* fp, as_error* err)
{
	as_seed_provider provider;
	fp_table(fp, &provider);
	return as_config_set_seed_provider(config, &provider, err);
}

static void
fp_set_hosts(fake_provider* fp, const char* hosts)
{
	pthread_mutex_lock(&fp->lock);
	snprintf(fp->hosts, sizeof(fp->hosts), "%s", hosts);
	pthread_mutex_unlock(&fp->lock);
}

static uint32_t
fp_get(fake_provider* fp, const uint32_t* field)
{
	pthread_mutex_lock(&fp->lock);
	uint32_t value = *field;
	pthread_mutex_unlock(&fp->lock);
	return value;
}

// Wait until *field >= target or the timeout expires.
static bool
fp_wait(fake_provider* fp, const uint32_t* field, uint32_t target, uint32_t timeout_ms)
{
	struct timespec abstime;
	abstime_ms(timeout_ms, &abstime);

	pthread_mutex_lock(&fp->lock);

	while (*field < target) {
		if (pthread_cond_timedwait(&fp->cond, &fp->lock, &abstime) == ETIMEDOUT) {
			break;
		}
	}
	bool ok = *field >= target;
	pthread_mutex_unlock(&fp->lock);
	return ok;
}

static bool
fp_wait_blocked(fake_provider* fp, uint32_t timeout_ms)
{
	struct timespec abstime;
	abstime_ms(timeout_ms, &abstime);

	pthread_mutex_lock(&fp->lock);

	while (!fp->blocked) {
		if (pthread_cond_timedwait(&fp->cond, &fp->lock, &abstime) == ETIMEDOUT) {
			break;
		}
	}
	bool ok = fp->blocked;
	pthread_mutex_unlock(&fp->lock);
	return ok;
}

static bool
fp_is_blocked(fake_provider* fp)
{
	pthread_mutex_lock(&fp->lock);
	bool blocked = fp->blocked;
	pthread_mutex_unlock(&fp->lock);
	return blocked;
}

static void
fp_open(fake_provider* fp)
{
	pthread_mutex_lock(&fp->lock);
	fp->gate_open = true;
	pthread_cond_broadcast(&fp->cond);
	pthread_mutex_unlock(&fp->lock);
}

// Wait for destroy. A bounded close can leave the discovery thread inside a call; it destroys
// the provider once the call returns and no longer touches fp after that. Call after the close
// and before fp_free(), the end of the scope or reuse through fp_init().
static void
fp_settle(fake_provider* fp)
{
	fp_wait(fp, &fp->destroyed, 1, 5000);
}

static bool
fp_destroyed_off_thread(fake_provider* fp)
{
	pthread_mutex_lock(&fp->lock);
	bool off = fp->destroyed > 0 && !pthread_equal(fp->destroy_thread, pthread_self());
	pthread_mutex_unlock(&fp->lock);
	return off;
}

//---------------------------------
// Static Functions: manager harness
//---------------------------------

static void
mgr_config(as_config* config, as_discovery_execution_mode mode, uint32_t tend_interval)
{
	as_config_init(config);
	config->tender_interval = tend_interval;
	config->discovery.execution_mode = mode;
	config->discovery.refresh_interval_ms = tend_interval;
	config->discovery.tend_refresh_deadline_ms = tend_interval / 2;
}

// Register fp (when not NULL), then create and start a manager. *m is left set when creation
// succeeded, even if start failed, so the caller can release the gate before mgr_close().
static as_status
mgr_open(as_config* config, fake_provider* fp, as_seed_manager** m, as_error* err)
{
	*m = NULL;

	if (fp) {
		as_status status = fp_register(config, fp, err);

		if (status != AEROSPIKE_OK) {
			return status;
		}
	}

	as_status status = as_seed_manager_create(err, config, m);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = as_seed_manager_start(*m, err);

	if (status == AEROSPIKE_OK) {
		as_seed_manager_connected(*m);
	}
	return status;
}

static void
mgr_close(as_seed_manager* m, as_config* config)
{
	if (m) {
		as_seed_manager_destroy(m);
	}
	as_config_destroy(config);
}

static void
tend_n(as_seed_manager* m, uint32_t count)
{
	for (uint32_t i = 0; i < count; i++) {
		as_seed_manager_tend(m);
	}
}

// Wait until no provider call runs on the discovery thread, for at most five seconds.
static bool
wait_idle(as_seed_manager* m)
{
	uint64_t end = cf_getms() + 5000;

	while (stats_of(m).in_refresh) {
		if (cf_getms() >= end) {
			return false;
		}
		as_sleep(1);
	}
	return true;
}

//---------------------------------
// Tests: snapshots
//---------------------------------

TEST(seed_snapshot_copy_on_write, "M-6 add and remove publish new snapshots; a reserved snapshot never changes")
{
	as_config config;
	as_error err;
	as_seed_manager* m;
	char buf[256];

	mgr_config(&config, AS_DISCOVERY_EXECUTION_THREAD, 1000);
	as_config_add_host(&config, "a", 3000);
	as_config_add_host(&config, "b", 3000);
	assert_int_eq(mgr_open(&config, NULL, &m, &err), AEROSPIKE_OK);
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "a:3000,b:3000");

	as_seed_snapshot* old = as_seed_manager_reserve(m);
	const as_vector* old_hosts = as_seed_snapshot_hosts(old);
	uint32_t publications = stats_of(m).publications;

	as_vector* add = as_vector_create(sizeof(as_host), 4);
	hosts_add(add, "c", "tls-c", 3000);
	hosts_add(add, "a", NULL, 3000);
	hosts_add(add, "c", NULL, 3000);
	as_seed_manager_add_hosts(m, add);
	hosts_free(add);

	assert_string_eq(snap_str(m, buf, sizeof(buf)), "a:3000,b:3000,c:3000");
	assert_int_eq(stats_of(m).publications, publications + 1);
	assert_string_eq(hosts_str(old_hosts, buf, sizeof(buf)), "a:3000,b:3000");

	// The TLS name of the first occurrence is kept.
	as_seed_snapshot* cur = as_seed_manager_reserve(m);
	as_host* c = as_vector_get((as_vector*)as_seed_snapshot_hosts(cur), 2);
	assert_string_eq(c->tls_name, "tls-c");
	as_seed_snapshot_release(cur);

	// Adding only known hosts and removing an unknown host publish nothing.
	add = as_vector_create(sizeof(as_host), 1);
	hosts_add(add, "b", NULL, 3000);
	as_seed_manager_add_hosts(m, add);
	hosts_free(add);
	as_seed_manager_remove_host(m, "zz", 3000);
	as_seed_manager_remove_host(m, "a", 3001);
	assert_int_eq(stats_of(m).publications, publications + 1);

	as_seed_manager_remove_host(m, "a", 3000);
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "b:3000,c:3000");
	assert_string_eq(hosts_str(old_hosts, buf, sizeof(buf)), "a:3000,b:3000");
	as_seed_snapshot_release(old);

	// Public cluster wrappers use the same copy-on-write path.
	as_cluster cluster;
	memset(&cluster, 0, sizeof(cluster));
	cluster.seed_manager = m;
	as_cluster_add_seed(&cluster, "d", NULL, 4000);
	as_cluster_remove_seed(&cluster, "b", 3000);
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "c:3000,d:4000");

	// Repeated swaps release every replaced snapshot (checked under valgrind).
	for (uint32_t i = 0; i < 200; i++) {
		as_cluster_add_seed(&cluster, "e", NULL, 5000);
		as_cluster_remove_seed(&cluster, "e", 5000);
	}
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "c:3000,d:4000");
	mgr_close(m, &config);
}

#define STRESS_HOSTS 8
#define STRESS_READERS 4
#define STRESS_TENDS 3000
#define STRESS_MAX_TENDS 2000000
#define STRESS_GENERATIONS (STRESS_READERS * 16)

static as_status
stress_step(fake_provider* fp, uint32_t call, as_error* err, as_seed_candidates* out)
{
	// Each call publishes one generation: STRESS_HOSTS hosts named after the call.
	char name[32];
	snprintf(name, sizeof(name), "g%u", call);

	for (uint16_t i = 1; i <= STRESS_HOSTS; i++) {
		if (!as_seed_candidates_append(out, name, NULL, i)) {
			return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "append failed");
		}
	}
	return AEROSPIKE_OK;
}

typedef struct stress_state_s {
	as_seed_manager* m;
	uint8_t done;
	uint32_t bad;
	uint32_t generations;
} stress_state;

static void*
stress_reader(void* udata)
{
	stress_state* st = udata;
	char last[32] = "";
	uint32_t bad = 0;

	while (!as_load_uint8(&st->done)) {
		as_seed_snapshot* s = as_seed_manager_reserve(st->m);
		const as_vector* hosts = as_seed_snapshot_hosts(s);

		if (hosts->size != STRESS_HOSTS) {
			bad++;
		}
		else {
			const as_host* first = as_vector_get((as_vector*)hosts, 0);

			for (uint32_t i = 0; i < hosts->size; i++) {
				const as_host* h = as_vector_get((as_vector*)hosts, i);

				if (strcmp(h->name, first->name) != 0 || h->port != i + 1) {
					bad++;
					break;
				}
			}

			if (strcmp(first->name, last) != 0) {
				snprintf(last, sizeof(last), "%s", first->name);
				as_incr_uint32(&st->generations);
			}
		}
		as_seed_snapshot_release(s);
	}

	as_aaf_uint32(&st->bad, bad);
	return NULL;
}

TEST(seed_snapshot_atomic, "M-6 concurrent reads and refresh publications never observe a partial seed list")
{
	as_config config;
	as_error err;
	as_seed_manager* m;
	fake_provider fp;

	fp_init(&fp, true, true, NULL);
	fp.step = stress_step;
	mgr_config(&config, AS_DISCOVERY_EXECUTION_TEND, 10000);
	config.discovery.merge_policy = AS_SEED_REPLACE;
	assert_int_eq(mgr_open(&config, &fp, &m, &err), AEROSPIKE_OK);

	stress_state st = {.m = m, .done = 0, .bad = 0, .generations = 0};
	pthread_t readers[STRESS_READERS];

	for (uint32_t i = 0; i < STRESS_READERS; i++) {
		assert_int_eq(pthread_create(&readers[i], NULL, stress_reader, &st), 0);
	}

	// One publication per tend. Keep publishing until the readers have seen many swaps, also
	// when the scheduler runs the threads one at a time.
	// A call stalled past the deadline by the scheduler is discarded, which is counted.
	uint32_t tends = 0;
	uint32_t discarded = 0;
	uint32_t overruns = 0;

	while (tends < STRESS_MAX_TENDS &&
		(tends < STRESS_TENDS || as_load_uint32(&st.generations) < STRESS_GENERATIONS)) {
		as_seed_manager_tend(m);
		tends++;

		uint32_t current = stats_of(m).overruns;

		if (current == overruns + 1) {
			discarded++;
		}
		overruns = current;
	}
	as_store_uint8(&st.done, 1);

	for (uint32_t i = 0; i < STRESS_READERS; i++) {
		pthread_join(readers[i], NULL);
	}

	as_seed_manager_stats stats = stats_of(m);
	mgr_close(m, &config);
	fp_settle(&fp);
	fp_free(&fp);

	assert_int_eq(st.bad, 0);
	assert_true(st.generations >= STRESS_GENERATIONS);
	assert_false(stats.tripped);
	assert_int_eq(stats.publications + discarded, tends + 1);
}

//---------------------------------
// Tests: defaults and initialization
//---------------------------------

TEST(seed_default_no_thread, "U-3/M-7 static and non-periodic providers add no thread or periodic work in either mode")
{
	as_config config;
	as_error err;
	as_seed_manager* m;
	char buf[256];
	int before = discovery_thread_count();

	// Static default. Refresh settings are not validated because they are unused.
	mgr_config(&config, AS_DISCOVERY_EXECUTION_THREAD, 1000);
	config.discovery.refresh_interval_ms = 0;
	config.discovery.refresh_timeout_ms = 0;
	as_config_add_host(&config, "s", 3000);
	as_config_add_host(&config, "s", 3000);
	assert_int_eq(mgr_open(&config, NULL, &m, &err), AEROSPIKE_OK);
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "s:3000");
	assert_false(stats_of(m).thread_running);

	if (before >= 0) {
		assert_int_eq(settle(discovery_thread_count, before), before);
	}
	tend_n(m, 100);
	assert_int_eq(stats_of(m).refreshes, 1);
	assert_int_eq(stats_of(m).tend_iteration, 100);
	mgr_close(m, &config);

	for (int mode = 0; mode < 2; mode++) {
		fake_provider fp;
		fp_init(&fp, false, false, "p:1");
		mgr_config(&config, (as_discovery_execution_mode)mode, 1000);
		config.discovery.refresh_interval_ms = 1;
		config.discovery.tend_refresh_deadline_ms = 0;
		assert_int_eq(mgr_open(&config, &fp, &m, &err), AEROSPIKE_OK);

		// The only call ran on this thread during start, with tend iteration zero.
		assert_int_eq(fp.calls, 1);
		assert_true(pthread_equal(fp.thread[0], pthread_self()));
		assert_int_eq(fp.iteration[0], 0);
		assert_false(stats_of(m).thread_running);

		if (before >= 0) {
			assert_int_eq(settle(discovery_thread_count, before), before);
		}

		tend_n(m, 100);
		as_seed_manager_request_urgent(m);
		assert_int_eq(fp_get(&fp, &fp.calls), 1);
		assert_int_eq(stats_of(m).urgent_wakes, 0);
		mgr_close(m, &config);
		fp_settle(&fp);
		assert_int_eq(fp.destroyed, 1);
		fp_free(&fp);
	}
}

static const char*
merge_run(as_seed_merge_policy policy, char* out, size_t size)
{
	// Same rows as the frozen merge contract: init, a refresh, an empty refresh, a failure.
	static const char* results[] = {"d1:1,s1:1,d2:2", "d3:3", "", NULL};
	as_config config;
	as_error err;
	as_seed_manager* m;
	fake_provider fp;
	char buf[256];

	fp_init(&fp, true, true, results[0]);
	mgr_config(&config, AS_DISCOVERY_EXECUTION_TEND, 1000);
	config.discovery.merge_policy = policy;
	as_config_add_host(&config, "s1", 1);
	as_config_add_host(&config, "s2", 2);

	if (mgr_open(&config, &fp, &m, &err) != AEROSPIKE_OK) {
		mgr_close(m, &config);
		fp_settle(&fp);
		fp_free(&fp);
		return "open failed";
	}

	size_t len = snprintf(out, size, "%s", snap_str(m, buf, sizeof(buf)));

	for (uint32_t i = 1; i < 4; i++) {
		if (results[i]) {
			fp_set_hosts(&fp, results[i]);
		}
		else {
			fp_set_hosts(&fp, "bad");
		}
		as_seed_manager_tend(m);
		len += snprintf(out + len, size - len, "|%s", snap_str(m, buf, sizeof(buf)));
	}
	mgr_close(m, &config);
	fp_settle(&fp);
	fp_free(&fp);
	return out;
}

static const char*
merge_contract(as_seed_merge_policy policy, char* out, size_t size)
{
	as_vector* statics = as_vector_create(sizeof(as_host), 2);
	hosts_add(statics, "s1", NULL, 1);
	hosts_add(statics, "s2", NULL, 2);
	as_vector* d0 = as_vector_create(sizeof(as_host), 3);
	hosts_add(d0, "d1", NULL, 1);
	hosts_add(d0, "s1", NULL, 1);
	hosts_add(d0, "d2", NULL, 2);
	as_vector* d1 = as_vector_create(sizeof(as_host), 1);
	hosts_add(d1, "d3", NULL, 3);
	as_vector* d2 = as_vector_create(sizeof(as_host), 1);
	const as_vector* results[] = {d0, d1, d2, NULL};
	as_seed_candidates snapshots[4];
	as_error err;
	size_t len = 0;
	out[0] = 0;

	if (as_discovery_conformance_merge(&err, policy, statics, results, 4, snapshots) ==
		AEROSPIKE_OK) {
		char buf[256];

		for (uint32_t i = 0; i < 4; i++) {
			len += snprintf(out + len, size - len, "%s%s", i ? "|" : "",
				hosts_str(snapshots[i].hosts, buf, sizeof(buf)));
			as_seed_candidates_destroy(&snapshots[i]);
		}
	}
	hosts_free(statics);
	hosts_free(d0);
	hosts_free(d1);
	hosts_free(d2);
	return out;
}

TEST(seed_merge_publication, "U-5 merge, replace and discovery_only publish ordered, deduplicated snapshots")
{
	char run[1024];
	char contract[1024];

	assert_string_eq(merge_run(AS_SEED_MERGE, run, sizeof(run)),
		"s1:1,s2:2,d1:1,d2:2|s1:1,s2:2,d3:3|s1:1,s2:2|s1:1,s2:2");
	assert_string_eq(merge_run(AS_SEED_REPLACE, run, sizeof(run)),
		"s1:1,s2:2,d1:1,d2:2|d3:3|d3:3|d3:3");
	assert_string_eq(merge_run(AS_SEED_DISCOVERY_ONLY, run, sizeof(run)),
		"d1:1,s1:1,d2:2|d3:3|s1:1,s2:2|s1:1,s2:2");

	// Real publication matches the frozen merge contract row for row.
	for (int policy = AS_SEED_MERGE; policy <= AS_SEED_DISCOVERY_ONLY; policy++) {
		merge_run((as_seed_merge_policy)policy, run, sizeof(run));
		merge_contract((as_seed_merge_policy)policy, contract, sizeof(contract));
		assert_string_eq(run, contract);
	}
}

static as_status
tls_step(fake_provider* fp, uint32_t call, as_error* err, as_seed_candidates* out)
{
	(void)fp;
	(void)call;
	as_seed_candidates_append(out, "x", NULL, 1);
	as_seed_candidates_append(out, "y", "own", 2);
	return AEROSPIKE_OK;
}

static as_status
tls_case(bool enable, const char* cluster_name, char* out, size_t size)
{
	as_config config;
	as_error err;
	as_seed_manager* m;
	fake_provider fp;

	fp_init(&fp, true, true, NULL);
	fp.step = tls_step;
	mgr_config(&config, AS_DISCOVERY_EXECUTION_TEND, 1000);
	config.tls.enable = enable;

	if (cluster_name) {
		as_config_set_cluster_name(&config, cluster_name);
	}
	as_config_add_host(&config, "s", 3);

	as_status status = mgr_open(&config, &fp, &m, &err);

	if (status == AEROSPIKE_OK) {
		// Refresh publications receive the same defaults as the initialization snapshot.
		as_seed_manager_tend(m);

		// So does an added seed, whose empty TLS name is the same as none.
		as_cluster cluster;
		memset(&cluster, 0, sizeof(cluster));
		cluster.seed_manager = m;
		as_cluster_add_seed(&cluster, "late", "", 8);

		as_seed_snapshot* s = as_seed_manager_reserve(m);
		const as_vector* hosts = as_seed_snapshot_hosts(s);
		size_t len = 0;
		out[0] = 0;

		for (uint32_t i = 0; i < hosts->size; i++) {
			const as_host* h = as_vector_get((as_vector*)hosts, i);
			len += snprintf(out + len, size - len, "%s%s/%s", i ? "," : "", h->name,
				h->tls_name ? h->tls_name : "-");
		}
		as_seed_snapshot_release(s);
	}
	mgr_close(m, &config);
	fp_settle(&fp);
	fp_free(&fp);
	return status;
}

TEST(seed_tls_defaults, "discovered and added seeds get the configured-seed TLS name default before publication")
{
	char out[256];

	assert_int_eq(tls_case(true, "c1", out, sizeof(out)), AEROSPIKE_OK);
	assert_string_eq(out, "s/c1,x/c1,y/own,late/c1");
	assert_int_eq(tls_case(true, NULL, out, sizeof(out)), AEROSPIKE_OK);
	assert_string_eq(out, "s/s,x/x,y/own,late/late");
	assert_int_eq(tls_case(false, "c1", out, sizeof(out)), AEROSPIKE_OK);
	assert_string_eq(out, "s/-,x/-,y/own,late/-");
}

static as_status
fail_step(fake_provider* fp, uint32_t call, as_error* err, as_seed_candidates* out)
{
	(void)fp;
	(void)call;
	(void)out;
	return as_error_set_message(err, AEROSPIKE_ERR_TIMEOUT, "cloud API unreachable");
}

static as_status
silent_fail_step(fake_provider* fp, uint32_t call, as_error* err, as_seed_candidates* out)
{
	(void)fp;
	(void)call;
	(void)err;
	(void)out;
	return AEROSPIKE_ERR_CLIENT;
}

static as_status
replace_container_step(fake_provider* fp, uint32_t call, as_error* err, as_seed_candidates* out)
{
	(void)fp;
	(void)call;
	(void)err;
	// Never dereferenced: the client must detect the swap and restore its own container.
	static as_vector foreign;
	out->hosts = &foreign;
	return AEROSPIKE_OK;
}

static as_status
empty_name_step(fake_provider* fp, uint32_t call, as_error* err, as_seed_candidates* out)
{
	(void)fp;
	(void)call;
	(void)err;
	as_seed_candidates_append(out, "ok", NULL, 1);
	as_host* host = as_vector_reserve(out->hosts);
	host->name = cf_strdup("");
	host->tls_name = NULL;
	host->port = 2;
	return AEROSPIKE_OK;
}

TEST(seed_init_failure, "U-1 an init failure falls back to configured seeds, or returns -27 naming provider and message")
{
	as_config config;
	as_error err;
	as_seed_manager* m;
	fake_provider fp;
	char buf[256];

	fake_step steps[] = {fail_step, silent_fail_step, replace_container_step, empty_name_step};

	for (uint32_t i = 0; i < sizeof(steps) / sizeof(steps[0]); i++) {
		for (int mode = 0; mode < 2; mode++) {
			// With configured seeds the client starts on them.
			fp_init(&fp, true, true, NULL);
			fp.step = steps[i];
			mgr_config(&config, (as_discovery_execution_mode)mode, 1000);
			as_config_add_host(&config, "s", 3000);
			assert_int_eq(mgr_open(&config, &fp, &m, &err), AEROSPIKE_OK);
			assert_string_eq(snap_str(m, buf, sizeof(buf)), "s:3000");
			mgr_close(m, &config);
			fp_settle(&fp);
			fp_free(&fp);

			// Without them initialization fails with the discovery code.
			fp_init(&fp, true, true, NULL);
			fp.step = steps[i];
			mgr_config(&config, (as_discovery_execution_mode)mode, 1000);
			assert_int_eq(mgr_open(&config, &fp, &m, &err), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
			assert_int_eq(err.code, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
			assert_not_null(strstr(err.message, "Seed provider fake"));

			if (i == 0) {
				assert_not_null(strstr(err.message, "cloud API unreachable"));
			}
			mgr_close(m, &config);
			fp_settle(&fp);
			fp_free(&fp);
		}
	}

	// A successful empty answer is not a provider failure.
	fp_init(&fp, false, false, "");
	mgr_config(&config, AS_DISCOVERY_EXECUTION_THREAD, 1000);
	assert_int_eq(mgr_open(&config, &fp, &m, &err), AEROSPIKE_OK);
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "");
	mgr_close(m, &config);
	fp_settle(&fp);
	fp_free(&fp);
}

TEST(seed_validation, "M-1/M-9/M-10 construction rejects ineligible tend providers and invalid settings")
{
	as_config config;
	as_error err;
	as_seed_manager* m;
	fake_provider fp;

	// M-10: a periodic tend provider must support a deadline.
	for (int variant = 0; variant < 2; variant++) {
		fp_init(&fp, true, false, "p:1");
		fp.no_deadline_fn = variant == 0;
		mgr_config(&config, AS_DISCOVERY_EXECUTION_TEND, 1000);
		assert_int_eq(mgr_open(&config, &fp, &m, &err), AEROSPIKE_ERR_PARAM);
		assert_null(m);
		assert_not_null(strstr(err.message, "Seed provider fake does not support a deadline"));
		assert_not_null(strstr(err.message, "execution_mode thread"));
		assert_int_eq(fp.calls, 0);
		mgr_close(m, &config);
		fp_settle(&fp);
		fp_free(&fp);
	}

	// The same provider is valid in thread mode, and non-periodic in tend mode.
	fp_init(&fp, true, false, "p:1");
	mgr_config(&config, AS_DISCOVERY_EXECUTION_THREAD, 1000);
	assert_int_eq(mgr_open(&config, &fp, &m, &err), AEROSPIKE_OK);
	mgr_close(m, &config);
	fp_settle(&fp);
	fp_free(&fp);
	fp_init(&fp, false, false, "p:1");
	mgr_config(&config, AS_DISCOVERY_EXECUTION_TEND, 1000);
	assert_int_eq(mgr_open(&config, &fp, &m, &err), AEROSPIKE_OK);
	mgr_close(m, &config);
	fp_settle(&fp);
	fp_free(&fp);

	// M-1: refresh_interval_ms below the tend interval, in either mode.
	for (int mode = 0; mode < 2; mode++) {
		fp_init(&fp, true, true, "p:1");
		mgr_config(&config, (as_discovery_execution_mode)mode, 1000);
		config.discovery.refresh_interval_ms = 999;
		assert_int_eq(mgr_open(&config, &fp, &m, &err), AEROSPIKE_ERR_PARAM);
		assert_not_null(strstr(err.message, "refresh_interval_ms 999"));
		assert_not_null(strstr(err.message, "tend interval 1000"));
		assert_int_eq(fp.calls, 0);
		mgr_close(m, &config);
		fp_settle(&fp);
		fp_free(&fp);
	}

	// M-9: tend_refresh_deadline_ms must be > 0 and <= tend interval / 2. Default 100.
	uint32_t deadlines[] = {0, 501, 500, 1};
	as_status expected[] = {AEROSPIKE_ERR_PARAM, AEROSPIKE_ERR_PARAM, AEROSPIKE_OK, AEROSPIKE_OK};

	for (uint32_t i = 0; i < 4; i++) {
		fp_init(&fp, true, true, "p:1");
		mgr_config(&config, AS_DISCOVERY_EXECUTION_TEND, 1000);
		config.discovery.tend_refresh_deadline_ms = deadlines[i];
		assert_int_eq(mgr_open(&config, &fp, &m, &err), expected[i]);

		if (expected[i] != AEROSPIKE_OK) {
			assert_not_null(strstr(err.message, "tend_refresh_deadline_ms"));
			assert_not_null(strstr(err.message, "(500)"));
		}
		mgr_close(m, &config);
		fp_settle(&fp);
		fp_free(&fp);
	}

	fp_init(&fp, true, true, "p:1");
	as_config_init(&config);
	assert_int_eq(config.discovery.tend_refresh_deadline_ms, 100);
	config.discovery.execution_mode = AS_DISCOVERY_EXECUTION_TEND;
	assert_int_eq(mgr_open(&config, &fp, &m, &err), AEROSPIKE_OK);
	mgr_close(m, &config);
	fp_settle(&fp);
	fp_free(&fp);

	// Thread mode needs a refresh timeout.
	fp_init(&fp, true, true, "p:1");
	mgr_config(&config, AS_DISCOVERY_EXECUTION_THREAD, 1000);
	config.discovery.refresh_timeout_ms = 0;
	assert_int_eq(mgr_open(&config, &fp, &m, &err), AEROSPIKE_ERR_PARAM);
	assert_not_null(strstr(err.message, "refresh_timeout_ms"));
	mgr_close(m, &config);
	fp_settle(&fp);
	fp_free(&fp);

	// Unknown enumerations.
	mgr_config(&config, AS_DISCOVERY_EXECUTION_THREAD, 1000);
	config.discovery.merge_policy = (as_seed_merge_policy)99;
	assert_int_eq(mgr_open(&config, NULL, &m, &err), AEROSPIKE_ERR_PARAM);
	assert_not_null(strstr(err.message, "merge_policy"));
	mgr_close(m, &config);
	mgr_config(&config, (as_discovery_execution_mode)99, 1000);
	assert_int_eq(mgr_open(&config, NULL, &m, &err), AEROSPIKE_ERR_PARAM);
	assert_not_null(strstr(err.message, "execution_mode"));
	mgr_close(m, &config);
}

//---------------------------------
// Tests: tend execution mode
//---------------------------------

TEST(seed_tend_cadence, "M-2 tend mode refreshes on exactly one tend in refresh_interval / tend_interval")
{
	as_config config;
	as_error err;
	as_seed_manager* m;
	fake_provider fp;

	fp_init(&fp, true, true, "p:1");
	mgr_config(&config, AS_DISCOVERY_EXECUTION_TEND, 1000);
	config.discovery.refresh_interval_ms = 30000;
	assert_int_eq(mgr_open(&config, &fp, &m, &err), AEROSPIKE_OK);
	assert_int_eq(fp.calls, 1);
	assert_int_eq(fp.iteration[0], 0);

	for (uint32_t tend = 1; tend <= 90; tend++) {
		as_seed_manager_tend(m);
		assert_int_eq(fp.calls, 1 + tend / 30);
	}

	assert_int_eq(fp.iteration[1], 30);
	assert_int_eq(fp.iteration[2], 60);
	assert_int_eq(fp.iteration[3], 90);
	assert_int_eq(stats_of(m).tend_iteration, 90);

	// Calls run on the tending thread.
	assert_true(pthread_equal(fp.thread[3], pthread_self()));
	mgr_close(m, &config);
	fp_settle(&fp);
	fp_free(&fp);
}

static as_status
errors_step(fake_provider* fp, uint32_t call, as_error* err, as_seed_candidates* out)
{
	if (call == 1) {
		return fp_append(err, out, "a:1");
	}

	if (call == 2) {
		return replace_container_step(fp, call, err, out);
	}

	if (call == 3) {
		return empty_name_step(fp, call, err, out);
	}

	if (call < 7) {
		return fail_step(fp, call, err, out);
	}
	return fp_append(err, out, "b:2");
}

TEST(seed_tend_errors, "M-3/U-1 tend-mode provider errors keep the last seeds and never stop later refreshes")
{
	as_config config;
	as_error err;
	as_seed_manager* m;
	fake_provider fp;
	char buf[256];

	fp_init(&fp, true, true, NULL);
	fp.step = errors_step;
	mgr_config(&config, AS_DISCOVERY_EXECUTION_TEND, 1000);
	config.discovery.merge_policy = AS_SEED_REPLACE;
	assert_int_eq(mgr_open(&config, &fp, &m, &err), AEROSPIKE_OK);

	for (uint32_t i = 0; i < 5; i++) {
		as_seed_manager_tend(m);
		assert_string_eq(snap_str(m, buf, sizeof(buf)), "a:1");
	}

	as_seed_manager_stats stats = stats_of(m);
	assert_int_eq(stats.failures, 5);
	assert_int_eq(stats.overruns, 0);
	assert_false(stats.tripped);

	// Tend keeps calling the provider and recovers when it does.
	as_seed_manager_tend(m);
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "b:2");
	assert_int_eq(fp.calls, 7);
	mgr_close(m, &config);
	fp_settle(&fp);
	fp_free(&fp);
}

typedef struct overrun_script_s {
	uint32_t delay_ms;
	const char* plan; // One char per call: 'o' overrun, 'e' on-time error, 'k' on-time ok.
} overrun_script;

static as_status
overrun_step(fake_provider* fp, uint32_t call, as_error* err, as_seed_candidates* out)
{
	overrun_script* script = fp->arg;
	char kind = (call - 1 < strlen(script->plan)) ? script->plan[call - 1] : 'k';
	char host[32];
	snprintf(host, sizeof(host), "c%u:%u", call, call);

	if (kind == 'o') {
		as_sleep(script->delay_ms);
		return fp_append(err, out, host);
	}

	if (kind == 'e') {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "scripted error");
	}
	return fp_append(err, out, host);
}

TEST(seed_tend_overrun, "M-4 an overrun blocks tend for its full duration, is discarded and counted")
{
	as_config config;
	as_error err;
	as_seed_manager* m;
	fake_provider fp;
	char buf[256];
	overrun_script script = {.delay_ms = 60, .plan = "kok"};

	fp_init(&fp, true, true, NULL);
	fp.step = overrun_step;
	fp.arg = &script;
	mgr_config(&config, AS_DISCOVERY_EXECUTION_TEND, 1000);
	config.discovery.merge_policy = AS_SEED_REPLACE;
	config.discovery.tend_refresh_deadline_ms = 20;
	assert_int_eq(mgr_open(&config, &fp, &m, &err), AEROSPIKE_OK);
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "c1:1");

	log_capture_start();
	uint64_t begin = cf_getms();
	as_seed_manager_tend(m);
	uint64_t elapsed = cf_getms() - begin;
	log_capture_stop();
	assert_int_eq(log_capture_count(AS_LOG_LEVEL_WARN, "Seed provider fake",
		"exceeding tend_refresh_deadline_ms 20", "Result discarded", NULL), 1);

	// Measured, not preempted: tend waited for the whole call, then dropped its result.
	assert_true(elapsed >= script.delay_ms);
	assert_int_eq(fp.finished, 2);
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "c1:1");
	assert_int_eq(stats_of(m).overruns, 1);
	assert_int_eq(stats_of(m).failures, 0);

	// An on-time call publishes and resets the count.
	as_seed_manager_tend(m);
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "c3:3");
	assert_int_eq(stats_of(m).overruns, 0);
	mgr_close(m, &config);
	fp_settle(&fp);
	fp_free(&fp);
}

TEST(seed_tend_breaker, "M-8 three consecutive overruns stop tend refreshes and restore the init snapshot")
{
	as_config config;
	as_error err;
	as_seed_manager* m;
	fake_provider fp;
	char buf[256];

	// Calls 2-3 overrun, 4 is an on-time error (resets), 5-6 overrun, 7 is on time, 8-10 overrun.
	overrun_script script = {.delay_ms = 40, .plan = "kooeooko" "oo"};

	fp_init(&fp, true, true, NULL);
	fp.step = overrun_step;
	fp.arg = &script;
	mgr_config(&config, AS_DISCOVERY_EXECUTION_TEND, 1000);
	config.discovery.merge_policy = AS_SEED_REPLACE;
	config.discovery.tend_refresh_deadline_ms = 20;
	as_config_add_host(&config, "s", 9);
	assert_int_eq(mgr_open(&config, &fp, &m, &err), AEROSPIKE_OK);
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "s:9,c1:1");

	// Node addresses appended after initialization are not part of the init snapshot.
	as_vector* add = as_vector_create(sizeof(as_host), 1);
	hosts_add(add, "node", NULL, 7);
	as_seed_manager_add_hosts(m, add);
	hosts_free(add);

	uint32_t expect_overruns[] = {1, 2, 0, 1, 2, 0, 1, 2};

	for (uint32_t i = 0; i < 8; i++) {
		as_seed_manager_tend(m);
		assert_int_eq(stats_of(m).overruns, expect_overruns[i]);
		assert_false(stats_of(m).tripped);
	}
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "c7:7");
	assert_int_eq(stats_of(m).failures, 1);

	// Third consecutive overrun trips the breaker.
	log_capture_start();
	as_seed_manager_tend(m);
	log_capture_stop();
	assert_int_eq(log_capture_count(AS_LOG_LEVEL_ERROR, "Seed provider fake",
		"no longer refreshed", NULL), 1);
	assert_true(stats_of(m).tripped);
	assert_int_eq(stats_of(m).overruns, 3);
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "s:9,c1:1");
	assert_int_eq(fp.calls, 10);

	tend_n(m, 20);
	assert_int_eq(fp.calls, 10);
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "s:9,c1:1");

	// The restored snapshot is still copy-on-write.
	as_cluster cluster;
	memset(&cluster, 0, sizeof(cluster));
	cluster.seed_manager = m;
	as_cluster_add_seed(&cluster, "late", NULL, 8);
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "s:9,c1:1,late:8");
	mgr_close(m, &config);
	fp_settle(&fp);
	fp_free(&fp);
}

TEST(seed_tend_breaker_thread_mode, "M-8 the overrun breaker is tend mode only; thread-mode overruns are counted")
{
	as_config config;
	as_error err;
	as_seed_manager* m;
	fake_provider fp;
	overrun_script script = {.delay_ms = 60, .plan = "koooook"};

	fp_init(&fp, true, true, NULL);
	fp.step = overrun_step;
	fp.arg = &script;
	mgr_config(&config, AS_DISCOVERY_EXECUTION_THREAD, AS_TEND_INTERVAL_MIN);
	config.discovery.refresh_timeout_ms = 30;
	config.discovery.tend_refresh_deadline_ms = 1;

	// A slow scheduler at initialization falls back to the configured seed instead of failing.
	as_config_add_host(&config, "s", 1);
	log_capture_start();
	as_status status = mgr_open(&config, &fp, &m, &err);

	// Slow thread-mode refreshes are counted against refresh_timeout_ms but keep running.
	bool finished = fp_wait(&fp, &fp.finished, 7, 10000);
	as_seed_manager_stats stats = stats_of(m);
	log_capture_stop();

	assert_int_eq(status, AEROSPIKE_OK);
	assert_true(finished);
	assert_false(stats.tripped);
	assert_true(stats.overruns >= 5);
	assert_int_eq(log_capture_count(AS_LOG_LEVEL_WARN, "Seed provider fake",
		"exceeding refresh_timeout_ms 30", "Overruns: 5", NULL), 1);
	mgr_close(m, &config);
	fp_settle(&fp);
	fp_free(&fp);
}

//---------------------------------
// Tests: thread execution mode
//---------------------------------

TEST(seed_thread_context, "thread mode runs every refresh on one refresh thread, never on the caller")
{
	as_config config;
	as_error err;
	as_seed_manager* m;
	fake_provider fp;
	int before = discovery_thread_count();

	fp_init(&fp, true, false, "p:1");
	mgr_config(&config, AS_DISCOVERY_EXECUTION_THREAD, AS_TEND_INTERVAL_MIN);
	as_config_set_cluster_name(&config, "cn");
	as_config_add_host(&config, "s", 1);
	assert_int_eq(mgr_open(&config, &fp, &m, &err), AEROSPIKE_OK);

	// U-3/M-7 counterpart: exactly one refresh thread for a periodic provider.
	assert_true(stats_of(m).thread_running);

	if (before >= 0) {
		assert_int_eq(settle(discovery_thread_count, before + 1), before + 1);
	}

	assert_true(fp_wait(&fp, &fp.finished, 3, 5000));

	pthread_mutex_lock(&fp.lock);
	pthread_t first = fp.thread[0];
	bool same = pthread_equal(fp.thread[1], first) && pthread_equal(fp.thread[2], first);
	uint64_t iteration = fp.iteration[0];
	uint32_t configured = fp.configured[0];
	pthread_mutex_unlock(&fp.lock);

	assert_false(pthread_equal(first, pthread_self()));
	assert_true(same);
	assert_int_eq(iteration, 0);
	assert_int_eq(configured, 1);
	assert_string_eq(fp.cluster_name, "cn");
	assert_int_eq(fp_get(&fp, &fp.max_active), 1);

	// Thread-mode callbacks read the latest tend iteration.
	tend_n(m, 5);
	uint32_t calls = fp_get(&fp, &fp.calls);
	assert_true(fp_wait(&fp, &fp.calls, calls + 1, 5000));
	pthread_mutex_lock(&fp.lock);
	uint64_t later = fp.iteration[fp.calls - 1];
	pthread_mutex_unlock(&fp.lock);
	assert_int_eq(later, 5);

	as_seed_manager_stop(m);
	assert_false(stats_of(m).thread_running);

	if (before >= 0) {
		assert_int_eq(settle(discovery_thread_count, before), before);
	}

	calls = fp_get(&fp, &fp.calls);
	as_seed_manager_request_urgent(m);
	as_sleep(300);
	assert_int_eq(fp_get(&fp, &fp.calls), calls);
	mgr_close(m, &config);
	fp_settle(&fp);
	assert_int_eq(fp.destroyed, 1);
	fp_free(&fp);
}

TEST(seed_thread_initial_wait, "the initial thread-mode wait is bounded by refresh_timeout_ms")
{
	as_config config;
	as_error err;
	as_seed_manager* m;
	fake_provider fp;
	char buf[256];

	// With configured seeds the client starts on them and adopts the late result later.
	fp_init(&fp, true, false, "d:2");
	fp.gate_call = 1;
	mgr_config(&config, AS_DISCOVERY_EXECUTION_THREAD, AS_TEND_INTERVAL_MIN);
	config.discovery.refresh_interval_ms = 60000;
	config.discovery.refresh_timeout_ms = 200;
	as_config_add_host(&config, "s", 1);

	uint64_t begin = cf_getms();
	assert_int_eq(mgr_open(&config, &fp, &m, &err), AEROSPIKE_OK);
	uint64_t elapsed = cf_getms() - begin;
	assert_true(elapsed >= 190);
	assert_true(elapsed < 5000);
	assert_true(fp_is_blocked(&fp));
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "s:1");

	uint32_t publications = stats_of(m).publications;
	fp_open(&fp);
	assert_true(fp_wait(&fp, &fp.finished, 1, 5000));

	for (int i = 0; i < 100 && stats_of(m).publications == publications; i++) {
		as_sleep(10);
	}
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "s:1,d:2");
	mgr_close(m, &config);
	fp_settle(&fp);
	fp_free(&fp);

	// Without configured seeds the timeout is a discovery failure. The failed connect spent the
	// whole budget, so close does not wait and the call finishes on the discovery thread.
	fp_init(&fp, true, false, "d:2");
	fp.gate_call = 1;
	mgr_config(&config, AS_DISCOVERY_EXECUTION_THREAD, AS_TEND_INTERVAL_MIN);
	config.discovery.refresh_timeout_ms = 200;
	assert_int_eq(mgr_open(&config, &fp, &m, &err), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_not_null(strstr(err.message, "Seed provider fake"));
	assert_not_null(strstr(err.message, "refresh_timeout_ms 200"));

	begin = cf_getms();
	mgr_close(m, &config);
	assert_true(cf_getms() - begin < 100);
	assert_int_eq(fp_get(&fp, &fp.destroyed), 0);

	fp_open(&fp);
	assert_true(fp_wait(&fp, &fp.destroyed, 1, 5000));
	assert_true(fp_destroyed_off_thread(&fp));
	assert_int_eq(fp.active_at_destroy, 0);
	fp_settle(&fp);
	fp_free(&fp);

	// A prompt initial refresh is not delayed by the timeout.
	fp_init(&fp, true, false, "d:2");
	mgr_config(&config, AS_DISCOVERY_EXECUTION_THREAD, AS_TEND_INTERVAL_MIN);
	config.discovery.refresh_timeout_ms = 30000;
	begin = cf_getms();
	assert_int_eq(mgr_open(&config, &fp, &m, &err), AEROSPIKE_OK);
	assert_true(cf_getms() - begin < 5000);
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "d:2");
	mgr_close(m, &config);
	fp_settle(&fp);
	fp_free(&fp);
}

TEST(seed_thread_interval, "thread mode sleeps refresh_interval_ms between refreshes")
{
	as_config config;
	as_error err;
	as_seed_manager* m;
	fake_provider fp;

	fp_init(&fp, true, false, "p:1");
	mgr_config(&config, AS_DISCOVERY_EXECUTION_THREAD, AS_TEND_INTERVAL_MIN);
	config.discovery.refresh_interval_ms = 300;
	assert_int_eq(mgr_open(&config, &fp, &m, &err), AEROSPIKE_OK);
	assert_true(fp_wait(&fp, &fp.calls, 4, 10000));
	as_seed_manager_stop(m);

	for (uint32_t i = 1; i < 4; i++) {
		uint64_t gap = fp.start_ms[i] - fp.start_ms[i - 1];
		assert_true(gap >= 280);
	}
	assert_int_eq(stats_of(m).urgent_wakes, 0);
	mgr_close(m, &config);
	fp_settle(&fp);
	fp_free(&fp);

	// The interval runs from start to start: a 200 ms call does not push the next start to 500 ms.
	overrun_script script = {.delay_ms = 200, .plan = "oooo"};
	fp_init(&fp, true, false, NULL);
	fp.step = overrun_step;
	fp.arg = &script;
	mgr_config(&config, AS_DISCOVERY_EXECUTION_THREAD, AS_TEND_INTERVAL_MIN);
	config.discovery.refresh_interval_ms = 300;
	assert_int_eq(mgr_open(&config, &fp, &m, &err), AEROSPIKE_OK);
	assert_true(fp_wait(&fp, &fp.calls, 4, 10000));
	as_seed_manager_stop(m);

	for (uint32_t i = 1; i < 4; i++) {
		uint64_t gap = fp.start_ms[i] - fp.start_ms[i - 1];
		assert_true(gap >= 280);
		assert_true(gap < 480);
	}
	mgr_close(m, &config);
	fp_settle(&fp);
	fp_free(&fp);
}

TEST(seed_thread_urgent, "an urgent request wakes a sleeping refresh at most once per refresh_interval_ms; while one runs it is dropped")
{
	as_config config;
	as_error err;
	as_seed_manager* m;
	fake_provider fp;

	// in_refresh is false both before a woken call starts and after it ends, so every honored
	// request first waits for its call to finish, then for the thread to go idle.

	// Within one refresh interval only the first request is honored.
	fp_init(&fp, true, false, "p:1");
	mgr_config(&config, AS_DISCOVERY_EXECUTION_THREAD, AS_TEND_INTERVAL_MIN);
	config.discovery.refresh_interval_ms = 600000;
	assert_int_eq(mgr_open(&config, &fp, &m, &err), AEROSPIKE_OK);
	assert_true(fp_wait(&fp, &fp.finished, 1, 5000));
	assert_true(wait_idle(m));

	as_seed_manager_request_urgent(m);
	assert_int_eq(stats_of(m).urgent_wakes, 1);
	assert_true(fp_wait(&fp, &fp.finished, 2, 5000));
	assert_true(wait_idle(m));

	for (uint32_t i = 0; i < 10; i++) {
		as_seed_manager_request_urgent(m);
	}
	assert_int_eq(stats_of(m).urgent_wakes, 1);
	assert_int_eq(fp_get(&fp, &fp.calls), 2);
	mgr_close(m, &config);
	fp_settle(&fp);
	fp_free(&fp);

	// The window reopens refresh_interval_ms after the last honored request.
	fp_init(&fp, true, false, "p:1");
	mgr_config(&config, AS_DISCOVERY_EXECUTION_THREAD, AS_TEND_INTERVAL_MIN);
	config.discovery.refresh_interval_ms = 1000;
	assert_int_eq(mgr_open(&config, &fp, &m, &err), AEROSPIKE_OK);
	assert_true(fp_wait(&fp, &fp.finished, 1, 5000));
	assert_true(wait_idle(m));

	// Request B is honored, the next one is dropped.
	as_seed_manager_request_urgent(m);
	assert_int_eq(stats_of(m).urgent_wakes, 1);
	assert_true(fp_wait(&fp, &fp.finished, 2, 5000));
	assert_true(wait_idle(m));
	as_seed_manager_request_urgent(m);
	assert_int_eq(stats_of(m).urgent_wakes, 1);

	// Call 3 is periodic, at least refresh_interval_ms after B's call began, so B's window is over.
	assert_true(fp_wait(&fp, &fp.finished, 3, 5000));
	assert_true(wait_idle(m));
	as_seed_manager_request_urgent(m);
	assert_int_eq(stats_of(m).urgent_wakes, 2);
	assert_true(fp_wait(&fp, &fp.finished, 4, 5000));
	assert_true(wait_idle(m));

	// Periodic call 5 blocks. Requests made meanwhile return at once and are dropped.
	pthread_mutex_lock(&fp.lock);
	fp.gate_call = 5;
	pthread_mutex_unlock(&fp.lock);
	assert_true(fp_wait_blocked(&fp, 5000));

	uint64_t begin = cf_getns();

	for (uint32_t i = 0; i < 100; i++) {
		as_seed_manager_request_urgent(m);
	}
	uint64_t elapsed_ms = (cf_getns() - begin) / (1000 * 1000);
	assert_true(elapsed_ms < 100);
	assert_int_eq(stats_of(m).urgent_wakes, 2);
	assert_int_eq(fp_get(&fp, &fp.calls), 5);

	// A request dropped during a refresh does not use up the window.
	fp_open(&fp);
	assert_true(fp_wait(&fp, &fp.finished, 5, 5000));
	assert_true(wait_idle(m));
	as_seed_manager_request_urgent(m);
	assert_int_eq(stats_of(m).urgent_wakes, 3);
	assert_int_eq(fp_get(&fp, &fp.max_active), 1);
	mgr_close(m, &config);
	fp_settle(&fp);
	fp_free(&fp);
}

TEST(seed_thread_never_blocks_tend, "M-5 a blocked thread-mode refresh adds no delay to tend, seed reads or urgent requests")
{
	as_config config;
	as_error err;
	as_seed_manager* m;
	fake_provider fp;
	char buf[256];

	fp_init(&fp, true, false, "p:1");
	fp.gate_call = 2;
	mgr_config(&config, AS_DISCOVERY_EXECUTION_THREAD, AS_TEND_INTERVAL_MIN);
	assert_int_eq(mgr_open(&config, &fp, &m, &err), AEROSPIKE_OK);
	assert_true(fp_wait_blocked(&fp, 5000));

	uint64_t begin = cf_getns();

	for (uint32_t i = 0; i < 1000; i++) {
		as_seed_snapshot* s = as_seed_manager_reserve(m);
		as_seed_snapshot_release(s);
		as_seed_manager_tend(m);
		as_seed_manager_request_urgent(m);
	}
	uint64_t elapsed_ms = (cf_getns() - begin) / (1000 * 1000);

	assert_true(elapsed_ms < 200);
	assert_true(fp_is_blocked(&fp));
	assert_int_eq(fp_get(&fp, &fp.calls), 2);
	assert_int_eq(stats_of(m).tend_iteration, 1000);
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "p:1");

	fp_open(&fp);
	mgr_close(m, &config);
	fp_settle(&fp);
	fp_free(&fp);
}

static as_status
thread_errors_step(fake_provider* fp, uint32_t call, as_error* err, as_seed_candidates* out)
{
	if (call == 1) {
		return fp_append(err, out, "a:1");
	}

	if (call < 5) {
		return fail_step(fp, call, err, out);
	}
	return fp_append(err, out, "b:2");
}

TEST(seed_thread_failures, "U-1 thread-mode failures keep the last-good seeds and the loop continues")
{
	as_config config;
	as_error err;
	as_seed_manager* m;
	fake_provider fp;
	char buf[256];

	fp_init(&fp, true, false, NULL);
	fp.step = thread_errors_step;
	fp.gate_call = 5;
	mgr_config(&config, AS_DISCOVERY_EXECUTION_THREAD, AS_TEND_INTERVAL_MIN);
	config.discovery.merge_policy = AS_SEED_REPLACE;
	assert_int_eq(mgr_open(&config, &fp, &m, &err), AEROSPIKE_OK);

	assert_true(fp_wait_blocked(&fp, 10000));
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "a:1");
	assert_int_eq(stats_of(m).failures, 3);

	uint32_t publications = stats_of(m).publications;
	fp_open(&fp);
	assert_true(fp_wait(&fp, &fp.finished, 5, 5000));

	for (int i = 0; i < 100 && stats_of(m).publications == publications; i++) {
		as_sleep(10);
	}
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "b:2");
	mgr_close(m, &config);
	fp_settle(&fp);
	fp_free(&fp);
}

static as_seed_manager** g_hook_mgr;
static fake_provider* g_hook_fp;
static bool g_hook_published;

// Runs on the connecting thread inside the connect fallback, before the fallback is published.
static void
late_init_hook(void)
{
	as_seed_manager* m = *g_hook_mgr;
	uint32_t publications = stats_of(m).publications;
	fp_open(g_hook_fp);

	// The late result returns now. It must wait for the fallback instead of racing it.
	uint64_t end = cf_getms() + 500;

	while (cf_getms() < end) {
		if (stats_of(m).publications != publications) {
			g_hook_published = true;
			break;
		}
		as_sleep(5);
	}
}

TEST(seed_late_init_publication, "a late initial result is published after the connect fallback, never overwritten by it")
{
	as_config config;
	as_error err;
	as_seed_manager* m = NULL;
	fake_provider fp;
	char buf[256];

	fp_init(&fp, true, false, "d:2");
	fp.gate_call = 1;
	mgr_config(&config, AS_DISCOVERY_EXECUTION_THREAD, AS_TEND_INTERVAL_MIN);
	config.discovery.refresh_interval_ms = 600000;
	config.discovery.refresh_timeout_ms = 200;
	as_config_add_host(&config, "s", 1);

	g_hook_mgr = &m;
	g_hook_fp = &fp;
	g_hook_published = false;
	log_capture_start();
	log_capture_hook("Initial refresh did not complete", late_init_hook);
	as_status status = mgr_open(&config, &fp, &m, &err);
	uint64_t end = cf_getms() + 5000;

	while (status == AEROSPIKE_OK && stats_of(m).publications < 2 && cf_getms() < end) {
		as_sleep(5);
	}
	log_capture_stop();

	assert_int_eq(status, AEROSPIKE_OK);
	assert_false(g_hook_published);
	assert_int_eq(stats_of(m).publications, 2);
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "s:1,d:2");
	mgr_close(m, &config);
	fp_settle(&fp);
	fp_free(&fp);
}

TEST(seed_thread_stop_bounded, "stop waits at most refresh_timeout_ms for a blocked refresh; destroy runs on the discovery thread once it returns")
{
	as_config config;
	as_error err;
	as_seed_manager* m;
	fake_provider fp;
	int before = discovery_thread_count();

	fp_init(&fp, true, false, "p:1");
	fp.gate_call = 2;
	mgr_config(&config, AS_DISCOVERY_EXECUTION_THREAD, AS_TEND_INTERVAL_MIN);
	config.discovery.refresh_timeout_ms = 300;
	assert_int_eq(mgr_open(&config, &fp, &m, &err), AEROSPIKE_OK);
	assert_true(fp_wait_blocked(&fp, 5000));

	uint64_t begin = cf_getms();
	as_seed_manager_stop(m);
	uint64_t elapsed = cf_getms() - begin;
	assert_true(elapsed >= 290);
	assert_true(elapsed < 2000);

	// The detached thread still exists and finishes the call.
	assert_true(stats_of(m).thread_running);
	mgr_close(m, &config);
	assert_int_eq(fp_get(&fp, &fp.destroyed), 0);

	fp_open(&fp);
	assert_true(fp_wait(&fp, &fp.destroyed, 1, 5000));
	assert_true(fp_destroyed_off_thread(&fp));
	assert_int_eq(fp_get(&fp, &fp.active_at_destroy), 0);

	if (before >= 0) {
		assert_int_eq(settle(discovery_thread_count, before), before);
	}
	assert_int_eq(fp_get(&fp, &fp.calls), 2);
	assert_int_eq(fp_get(&fp, &fp.destroyed), 1);
	fp_settle(&fp);
	fp_free(&fp);
}

// Block call 2 of fp on a discovery thread, then destroy its manager, which gives up on the call
// after refresh_timeout_ms. The config keeps the registration.
static bool
strand_call(as_config* config, fake_provider* fp)
{
	as_error err;
	as_seed_manager* m;

	fp_init(fp, true, false, "p:1");
	fp->gate_call = 2;
	mgr_config(config, AS_DISCOVERY_EXECUTION_THREAD, AS_TEND_INTERVAL_MIN);
	config->discovery.refresh_timeout_ms = 200;

	if (mgr_open(config, fp, &m, &err) != AEROSPIKE_OK || !fp_wait_blocked(fp, 5000)) {
		fp_open(fp);
		mgr_close(m, config);
		fp_settle(fp);
		return false;
	}
	as_seed_manager_destroy(m);
	return true;
}

static void*
open_later(void* udata)
{
	as_sleep(100);
	fp_open(udata);
	return NULL;
}

TEST(seed_reconnect_wait, "a new manager waits at most refresh_timeout_ms for a refresh left running by the previous one")
{
	as_config config;
	as_error err;
	as_seed_manager* m;
	fake_provider fp;
	int before = discovery_thread_count();

	assert_true(strand_call(&config, &fp));

	// Still running: creation fails with the discovery code, names the provider and calls nothing.
	uint64_t begin = cf_getms();
	assert_int_eq(as_seed_manager_create(&err, &config, &m), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	uint64_t elapsed = cf_getms() - begin;
	assert_true(elapsed >= 190);
	assert_true(elapsed < 2000);
	assert_not_null(strstr(err.message, "Seed provider fake"));
	assert_not_null(strstr(err.message, "previous connection"));
	assert_int_eq(fp_get(&fp, &fp.calls), 2);

	// The call returns within the wait: the next manager starts and its initial call is call 3.
	config.discovery.refresh_timeout_ms = 3000;
	config.discovery.refresh_interval_ms = 600000;
	pthread_t opener;
	assert_int_eq(pthread_create(&opener, NULL, open_later, &fp), 0);
	as_status status = as_seed_manager_create(&err, &config, &m);
	as_status started = status == AEROSPIKE_OK ? as_seed_manager_start(m, &err) : status;
	pthread_join(opener, NULL);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(started, AEROSPIKE_OK);
	as_seed_manager_connected(m);
	assert_int_eq(fp_get(&fp, &fp.calls), 3);
	assert_int_eq(fp_get(&fp, &fp.max_active), 1);
	mgr_close(m, &config);
	fp_settle(&fp);

	if (before >= 0) {
		assert_int_eq(settle(discovery_thread_count, before), before);
	}
	fp_free(&fp);
}

TEST(seed_deferred_destroy_sites, "a registration that ends during its call is destroyed once, on the discovery thread, after the call")
{
	as_config config;
	as_error err;
	fake_provider fp;
	fake_provider fp2;
	int before = discovery_thread_count();

	// Replaced by another provider: the old destroy is handed over, the new provider is untouched.
	assert_true(strand_call(&config, &fp));
	fp_init(&fp2, false, false, "q:1");
	assert_int_eq(fp_register(&config, &fp2, &err), AEROSPIKE_OK);
	assert_int_eq(fp_get(&fp, &fp.destroyed), 0);
	assert_int_eq(fp_get(&fp2, &fp2.destroyed), 0);
	fp_open(&fp);
	assert_true(fp_wait(&fp, &fp.destroyed, 1, 5000));
	assert_true(fp_destroyed_off_thread(&fp));

	// No call of the new provider is running, so the config destroys it synchronously.
	as_config_destroy(&config);
	assert_int_eq(fp2.destroyed, 1);
	assert_true(pthread_equal(fp2.destroy_thread, pthread_self()));
	fp_settle(&fp2);
	fp_free(&fp2);

	if (before >= 0) {
		assert_int_eq(settle(discovery_thread_count, before), before);
	}
	assert_int_eq(fp_get(&fp, &fp.destroyed), 1);
	fp_settle(&fp);
	fp_free(&fp);

	// Re-registered with the same udata, which destroys nothing, then the config is destroyed.
	assert_true(strand_call(&config, &fp));
	assert_int_eq(fp_register(&config, &fp, &err), AEROSPIKE_OK);
	assert_int_eq(fp_get(&fp, &fp.destroyed), 0);
	as_config_destroy(&config);
	assert_int_eq(fp_get(&fp, &fp.destroyed), 0);
	fp_open(&fp);
	assert_true(fp_wait(&fp, &fp.destroyed, 1, 5000));
	assert_true(fp_destroyed_off_thread(&fp));

	if (before >= 0) {
		assert_int_eq(settle(discovery_thread_count, before), before);
	}
	assert_int_eq(fp_get(&fp, &fp.destroyed), 1);
	fp_settle(&fp);
	fp_free(&fp);

#if !defined(_WIN32)
	// Released by the close of an unconnected async client, then destroyed with the config.
	assert_true(strand_call(&config, &fp));
	as_config_release_plugins(&config);
	assert_int_eq(fp_get(&fp, &fp.destroyed), 0);
	fp_open(&fp);
	assert_true(fp_wait(&fp, &fp.destroyed, 1, 5000));
	assert_true(fp_destroyed_off_thread(&fp));
	as_config_destroy(&config);

	if (before >= 0) {
		assert_int_eq(settle(discovery_thread_count, before), before);
	}
	assert_int_eq(fp_get(&fp, &fp.destroyed), 1);
	fp_settle(&fp);
	fp_free(&fp);
#endif
}

TEST(seed_binding_thread, "a non-periodic binding provider runs its single call on the discovery thread, which then exits")
{
	as_config config;
	as_error err;
	as_seed_manager* m;
	fake_provider fp;
	char buf[256];
	int before = discovery_thread_count();

	fp_init(&fp, false, false, "b:1");
	mgr_config(&config, AS_DISCOVERY_EXECUTION_THREAD, AS_TEND_INTERVAL_MIN);
	assert_int_eq(as_config_set_binding_seed_provider(&config, "bind", fp_refresh, false,
		fp_destroy, &fp, &err), AEROSPIKE_OK);
	assert_int_eq(mgr_open(&config, NULL, &m, &err), AEROSPIKE_OK);

	assert_int_eq(fp_get(&fp, &fp.calls), 1);
	assert_false(pthread_equal(fp.thread[0], pthread_self()));
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "b:1");

	uint64_t end = cf_getms() + 5000;

	while (stats_of(m).thread_running && cf_getms() < end) {
		as_sleep(5);
	}
	assert_false(stats_of(m).thread_running);

	if (before >= 0) {
		assert_int_eq(settle(discovery_thread_count, before), before);
	}

	// Nothing calls it again.
	tend_n(m, 10);

	for (uint32_t i = 0; i < 10; i++) {
		as_seed_manager_request_urgent(m);
	}
	assert_int_eq(fp_get(&fp, &fp.calls), 1);
	assert_int_eq(stats_of(m).urgent_wakes, 0);
	mgr_close(m, &config);
	fp_settle(&fp);
	assert_int_eq(fp.destroyed, 1);

	// Its discovery thread needs a refresh timeout.
	mgr_config(&config, AS_DISCOVERY_EXECUTION_THREAD, AS_TEND_INTERVAL_MIN);
	config.discovery.refresh_timeout_ms = 0;
	assert_int_eq(as_config_set_binding_seed_provider(&config, "bind", fp_refresh, false,
		fp_destroy, &fp, &err), AEROSPIKE_OK);
	assert_int_eq(as_seed_manager_create(&err, &config, &m), AEROSPIKE_ERR_PARAM);
	assert_not_null(strstr(err.message, "refresh_timeout_ms"));
	assert_int_eq(fp_get(&fp, &fp.calls), 1);
	as_config_destroy(&config);
	assert_int_eq(fp.destroyed, 2);
	fp_free(&fp);
}

// Format the ip_map flags of the current snapshot as "1,0,...".
static const char*
ip_map_str(as_seed_manager* m, char* buf, size_t size)
{
	as_seed_snapshot* s = as_seed_manager_reserve(m);
	const as_vector* hosts = as_seed_snapshot_hosts(s);
	size_t len = 0;
	buf[0] = 0;

	for (uint32_t i = 0; i < hosts->size && len < size; i++) {
		len += snprintf(buf + len, size - len, "%s%d", i ? "," : "",
			as_seed_snapshot_uses_ip_map(s, i) ? 1 : 0);
	}
	as_seed_snapshot_release(s);
	return buf;
}

TEST(seed_snapshot_ip_map_flags, "ip_map applies to configured seeds and added hosts, not to provider-only hosts")
{
	as_config config;
	as_error err;
	as_seed_manager* m;
	fake_provider fp;
	char buf[256];

	fp_init(&fp, true, true, "d:2,s:1");
	mgr_config(&config, AS_DISCOVERY_EXECUTION_TEND, 1000);
	as_config_add_host(&config, "s", 1);
	assert_int_eq(mgr_open(&config, &fp, &m, &err), AEROSPIKE_OK);
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "s:1,d:2");
	assert_string_eq(ip_map_str(m, buf, sizeof(buf)), "1,0");

	as_seed_snapshot* s = as_seed_manager_reserve(m);
	assert_false(as_seed_snapshot_uses_ip_map(s, 2));
	as_seed_snapshot_release(s);

	// Added hosts, such as node addresses, are rewritten as in earlier releases.
	as_vector* add = as_vector_create(sizeof(as_host), 1);
	hosts_add(add, "n", NULL, 3);
	as_seed_manager_add_hosts(m, add);
	hosts_free(add);
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "s:1,d:2,n:3");
	assert_string_eq(ip_map_str(m, buf, sizeof(buf)), "1,0,1");

	// Removal keeps the flags of the remaining hosts.
	as_seed_manager_remove_host(m, "s", 1);
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "d:2,n:3");
	assert_string_eq(ip_map_str(m, buf, sizeof(buf)), "0,1");
	mgr_close(m, &config);
	fp_settle(&fp);
	fp_free(&fp);

	// A discovered host equal to a configured seed is rewritten, also after a replace refresh.
	fp_init(&fp, true, true, "d:2,s:1");
	mgr_config(&config, AS_DISCOVERY_EXECUTION_TEND, 1000);
	config.discovery.merge_policy = AS_SEED_REPLACE;
	as_config_add_host(&config, "s", 1);
	assert_int_eq(mgr_open(&config, &fp, &m, &err), AEROSPIKE_OK);
	fp_set_hosts(&fp, "s:1,x:4");
	as_seed_manager_tend(m);
	assert_string_eq(snap_str(m, buf, sizeof(buf)), "s:1,x:4");
	assert_string_eq(ip_map_str(m, buf, sizeof(buf)), "1,0");
	mgr_close(m, &config);
	fp_settle(&fp);
	fp_free(&fp);
}

typedef struct stopper_s {
	as_seed_manager* m;
	uint8_t done;
} stopper;

static void*
stop_run(void* udata)
{
	stopper* s = udata;
	as_seed_manager_stop(s->m);
	as_store_uint8(&s->done, 1);
	return NULL;
}

TEST(seed_thread_stop_in_flight, "stop waits for an in-flight refresh and no callback runs after it")
{
	as_config config;
	as_error err;
	as_seed_manager* m;
	fake_provider fp;

	fp_init(&fp, true, false, "p:1");
	fp.gate_call = 2;
	mgr_config(&config, AS_DISCOVERY_EXECUTION_THREAD, AS_TEND_INTERVAL_MIN);
	assert_int_eq(mgr_open(&config, &fp, &m, &err), AEROSPIKE_OK);
	assert_true(fp_wait_blocked(&fp, 5000));

	stopper s = {.m = m, .done = 0};
	pthread_t thread;
	assert_int_eq(pthread_create(&thread, NULL, stop_run, &s), 0);
	as_sleep(300);
	assert_int_eq(as_load_uint8(&s.done), 0);

	fp_open(&fp);
	pthread_join(thread, NULL);
	assert_int_eq(s.done, 1);
	assert_int_eq(fp_get(&fp, &fp.active), 0);
	assert_int_eq(fp_get(&fp, &fp.calls), 2);

	as_sleep(400);
	assert_int_eq(fp_get(&fp, &fp.calls), 2);
	mgr_close(m, &config);
	fp_settle(&fp);
	assert_int_eq(fp.destroyed, 1);
	assert_int_eq(fp.finished_at_destroy, 2);
	fp_free(&fp);
}

#ifdef DISCOVERY_FAKE_SERVERS

//---------------------------------
// Static Functions: client helpers
//---------------------------------

void
aerospike_destroy_internal(aerospike* as);

typedef struct fclient_s {
	aerospike* as;
	as_error err;
	as_status status;
} fclient;

static void
client_config(as_config* config)
{
	as_config_init(config);
	config->tender_interval = AS_TEND_INTERVAL_MIN;
	config->conn_timeout_ms = 1000;
	config->fail_if_not_connected = true;
	config->discovery.refresh_interval_ms = AS_TEND_INTERVAL_MIN;
	config->discovery.tend_refresh_deadline_ms = AS_TEND_INTERVAL_MIN / 2;
}

static as_status
client_connect(fclient* c, as_config* config)
{
	c->as = aerospike_new(config);
	c->status = aerospike_connect(c->as, &c->err);
	return c->status;
}

// Close and destroy the client. Async close also destroys it; a client that never connected
// is destroyed directly so its registrations are released in every mode.
static void
client_close(fclient* c)
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
	c->as = NULL;
}

// What a sync-only aerospike_close() does, also when async event loops exist, so that the
// reconnect path can be tested in every mode.
static void
client_sync_close(fclient* c)
{
	as_cluster_destroy(c->as->cluster);
	c->as->cluster = NULL;
	c->as->config.discovery.registration_locked = false;
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
		as_sleep(5);
	}
	return true;
}

static as_seed_manager_stats
client_stats(aerospike* client)
{
	return stats_of(client->cluster->seed_manager);
}

//---------------------------------
// Tests: initialization through aerospike_connect()
//---------------------------------

TEST(seed_provider_only_bootstrap, "a registered provider may supply every seed; the default keeps the no-host error")
{
	fnode a;
	assert_true(fnode_start(&a, "A"));
	char addr[64];
	as_config config;
	as_error err;
	fclient c;

	// Static default without hosts: unchanged parameter error, before any cluster work.
	client_config(&config);
	assert_int_eq(client_connect(&c, &config), AEROSPIKE_ERR_PARAM);
	assert_string_eq(c.err.message, "No hosts provided");
	assert_null(c.as->cluster);
	client_close(&c);

	fake_provider fp;
	fp_init(&fp, false, false, fnode_addr(&a, addr, sizeof(addr)));
	client_config(&config);
	as_config_set_cluster_name(&config, NULL);
	assert_int_eq(fp_register(&config, &fp, &err), AEROSPIKE_OK);

	assert_int_eq(client_connect(&c, &config), AEROSPIKE_OK);
	assert_true(has_node(c.as, "A"));
	assert_true(c.as->config.discovery.registration_locked);

	// The initialization call ran on the connecting thread with an empty configured list.
	assert_int_eq(fp.calls, 1);
	assert_true(pthread_equal(fp.thread[0], pthread_self()));
	assert_int_eq(fp.iteration[0], 0);
	assert_int_eq(fp.configured[0], 0);

	char buf[256];
	assert_string_eq(snap_str(c.as->cluster->seed_manager, buf, sizeof(buf)), addr);
	assert_false(client_stats(c.as).thread_running);
	client_close(&c);
	fnode_stop(&a);
	fp_settle(&fp);
	assert_int_eq(fp.destroyed, 1);
	fp_free(&fp);
}

TEST(seed_connect_init_failure, "connect falls back to configured seeds on a provider error, and returns -27 without them")
{
	fnode a;
	assert_true(fnode_start(&a, "A"));
	as_config config;
	as_error err;
	fclient c;
	fake_provider fp;

	for (int mode = 0; mode < 2; mode++) {
		fp_init(&fp, true, true, NULL);
		fp.step = fail_step;
		client_config(&config);
		config.discovery.execution_mode = (as_discovery_execution_mode)mode;
		as_config_add_host(&config, "127.0.0.1", a.port);
		assert_int_eq(fp_register(&config, &fp, &err), AEROSPIKE_OK);
		assert_int_eq(client_connect(&c, &config), AEROSPIKE_OK);
		assert_true(has_node(c.as, "A"));
		client_close(&c);
		fp_settle(&fp);
		fp_free(&fp);

		fp_init(&fp, true, true, NULL);
		fp.step = fail_step;
		client_config(&config);
		config.discovery.execution_mode = (as_discovery_execution_mode)mode;
		assert_int_eq(fp_register(&config, &fp, &err), AEROSPIKE_OK);
		assert_int_eq(client_connect(&c, &config), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
		assert_not_null(strstr(c.err.message, "Seed provider fake"));
		assert_not_null(strstr(c.err.message, "cloud API unreachable"));
		assert_null(c.as->cluster);
		assert_false(c.as->config.discovery.registration_locked);
		client_close(&c);
		fp_settle(&fp);
		assert_int_eq(fp.destroyed, 1);
		assert_int_eq(fp.active_at_destroy, 0);
		fp_free(&fp);
	}

	// An empty successful answer is not a discovery failure: bootstrap reports the seed failure.
	fp_init(&fp, false, false, "");
	client_config(&config);
	assert_int_eq(fp_register(&config, &fp, &err), AEROSPIKE_OK);
	assert_int_eq(client_connect(&c, &config), AEROSPIKE_ERR_CLIENT);
	client_close(&c);
	fp_settle(&fp);
	fp_free(&fp);
	fnode_stop(&a);
}

TEST(seed_connect_validation, "M-1/M-10 connect rejects ineligible settings before any refresh and unlocks registration")
{
	fnode a;
	assert_true(fnode_start(&a, "A"));
	as_config config;
	as_error err;
	fclient c;
	fake_provider fp;
	char addr[64];

	fp_init(&fp, true, false, fnode_addr(&a, addr, sizeof(addr)));
	client_config(&config);
	config.discovery.execution_mode = AS_DISCOVERY_EXECUTION_TEND;
	assert_int_eq(fp_register(&config, &fp, &err), AEROSPIKE_OK);
	assert_int_eq(client_connect(&c, &config), AEROSPIKE_ERR_PARAM);
	assert_not_null(strstr(c.err.message, "does not support a deadline"));
	assert_int_eq(fp.calls, 0);
	assert_null(c.as->cluster);
	assert_false(c.as->config.discovery.registration_locked);

	// The registration can be corrected and the same client connects.
	c.as->config.discovery.execution_mode = AS_DISCOVERY_EXECUTION_THREAD;
	c.as->config.discovery.refresh_interval_ms = AS_TEND_INTERVAL_MIN - 1;
	assert_int_eq(aerospike_connect(c.as, &err), AEROSPIKE_ERR_PARAM);
	assert_not_null(strstr(err.message, "refresh_interval_ms"));
	c.as->config.discovery.refresh_interval_ms = 60000;
	assert_int_eq(aerospike_connect(c.as, &err), AEROSPIKE_OK);
	assert_true(has_node(c.as, "A"));
	client_close(&c);
	fp_settle(&fp);
	assert_int_eq(fp.destroyed, 1);
	fp_free(&fp);
	fnode_stop(&a);
}

TEST(seed_default_compat, "U-3/M-7 default static seeds keep the ip_map seed rewrite and add no discovery thread")
{
	fnode a;
	assert_true(fnode_start(&a, "A"));
	as_config config;
	as_error err;
	fclient c;
	int before = discovery_thread_count();

	client_config(&config);
	as_config_add_host(&config, "seed-alias.invalid", a.port);
	as_addr_map map = {.orig = "seed-alias.invalid", .alt = "127.0.0.1"};
	config.ip_map = &map;
	config.ip_map_size = 1;
	assert_int_eq(client_connect(&c, &config), AEROSPIKE_OK);
	assert_true(has_node(c.as, "A"));

	if (before >= 0) {
		assert_int_eq(settle(discovery_thread_count, before), before);
	}

	as_sleep(AS_TEND_INTERVAL_MIN * 3);
	as_seed_manager_stats stats = client_stats(c.as);
	assert_false(stats.thread_running);
	assert_int_eq(stats.refreshes, 1);
	assert_true(stats.tend_iteration >= 2);

	// Configured seeds stay first; the node address is appended after initialization.
	char buf[256];
	char expected[128];
	snprintf(expected, sizeof(expected), "seed-alias.invalid:%u,127.0.0.1:%u", a.port, a.port);
	assert_string_eq(snap_str(c.as->cluster->seed_manager, buf, sizeof(buf)), expected);

	// ip_map is owned by the caller here.
	c.as->config.ip_map = NULL;
	c.as->config.ip_map_size = 0;
	client_close(&c);

	// A periodic thread-mode provider adds exactly one discovery thread.
	fake_provider fp;
	fp_init(&fp, true, false, fnode_addr(&a, buf, sizeof(buf)));
	client_config(&config);
	config.discovery.refresh_interval_ms = 60000;
	assert_int_eq(fp_register(&config, &fp, &err), AEROSPIKE_OK);
	assert_int_eq(client_connect(&c, &config), AEROSPIKE_OK);

	if (before >= 0) {
		assert_int_eq(settle(discovery_thread_count, before + 1), before + 1);
	}
	client_close(&c);

	if (before >= 0) {
		assert_int_eq(settle(discovery_thread_count, before), before);
	}
	fp_settle(&fp);
	assert_int_eq(fp.destroyed, 1);
	fp_free(&fp);
	fnode_stop(&a);
}

//---------------------------------
// Tests: runtime through the tend thread
//---------------------------------

TEST(seed_thread_mode_tend_continues, "M-5 tend keeps its cadence while a thread-mode refresh is blocked")
{
	fnode a;
	assert_true(fnode_start(&a, "A"));
	as_config config;
	as_error err;
	fclient c;
	fake_provider fp;
	char addr[64];

	fp_init(&fp, true, false, fnode_addr(&a, addr, sizeof(addr)));
	fp.gate_call = 2;
	client_config(&config);
	assert_int_eq(fp_register(&config, &fp, &err), AEROSPIKE_OK);
	assert_int_eq(client_connect(&c, &config), AEROSPIKE_OK);
	assert_true(fp_wait_blocked(&fp, 5000));

	// Eight tends at 250 ms complete well within five seconds.
	uint32_t tends = fnode_get(&a, &a.tends);
	uint64_t begin = cf_getms();
	assert_true(fnode_wait(&a, &a.tends, tends + 8, 5000));
	assert_true(cf_getms() - begin < 4000);
	assert_true(fp_is_blocked(&fp));
	assert_true(has_node(c.as, "A"));
	assert_true(client_stats(c.as).tend_iteration >= 8);

	fp_open(&fp);
	client_close(&c);
	fp_settle(&fp);
	assert_int_eq(fp.destroyed, 1);
	fp_free(&fp);
	fnode_stop(&a);
}

TEST(seed_reseed_in_flight, "M-11 zero-node reseed uses the held snapshot while a thread refresh is in flight")
{
	fnode a;
	assert_true(fnode_start(&a, "A"));
	as_config config;
	as_error err;
	fclient c;
	fake_provider fp;
	char addr[64];

	fp_init(&fp, true, false, fnode_addr(&a, addr, sizeof(addr)));
	fp.gate_call = 2;
	client_config(&config);
	config.discovery.refresh_interval_ms = 600000;
	assert_int_eq(fp_register(&config, &fp, &err), AEROSPIKE_OK);
	assert_int_eq(client_connect(&c, &config), AEROSPIKE_OK);
	assert_int_eq(fp_get(&fp, &fp.calls), 1);

	// Lose every node. The first zero-node tend wakes the refresh, which then blocks.
	uint16_t port = a.port;
	fnode_halt(&a);
	assert_true(wait_for_nodes(c.as, 0, 10000));
	assert_true(fp_wait_blocked(&fp, 5000));
	assert_int_eq(client_stats(c.as).urgent_wakes, 1);

	// Later zero-node tends keep reseeding without joining or restarting the refresh.
	uint64_t iteration = client_stats(c.as).tend_iteration;

	while (client_stats(c.as).tend_iteration < iteration + 3) {
		as_sleep(20);
	}
	assert_int_eq(client_stats(c.as).urgent_wakes, 1);
	assert_int_eq(fp_get(&fp, &fp.calls), 2);

	// The node comes back at an address in the snapshot and is reseeded while the refresh is
	// still blocked.
	assert_true(fnode_listen(&a, port));
	assert_true(wait_for_nodes(c.as, 1, 5000));
	assert_true(has_node(c.as, "A"));
	assert_true(fp_is_blocked(&fp));
	assert_int_eq(fp_get(&fp, &fp.calls), 2);
	assert_int_eq(fp_get(&fp, &fp.max_active), 1);

	fp_open(&fp);
	assert_true(fp_wait(&fp, &fp.finished, 2, 5000));
	client_close(&c);
	fp_settle(&fp);
	assert_int_eq(fp.destroyed, 1);
	fp_free(&fp);
	fnode_stop(&a);
}

typedef struct switch_state_s {
	pthread_mutex_t lock;
	fnode* a;
	char b_addr[64];
	bool switched;
	bool recorded;
	uint64_t switch_ms;
	uint64_t b_first_ms;
	bool b_first_seen;
} switch_state;

static as_status
switch_step(fake_provider* fp, uint32_t call, as_error* err, as_seed_candidates* out)
{
	switch_state* st = fp->arg;
	char addr[64];

	pthread_mutex_lock(&st->lock);

	if (st->switched) {
		if (!st->recorded) {
			st->recorded = true;
			st->switch_ms = cf_getms();
		}
		snprintf(addr, sizeof(addr), "%s", st->b_addr);
	}
	else {
		fnode_addr(st->a, addr, sizeof(addr));
	}
	pthread_mutex_unlock(&st->lock);
	return fp_append(err, out, addr);
}

static void
switch_first_b(fnode* n)
{
	switch_state* st = n->udata;
	pthread_mutex_lock(&st->lock);
	st->b_first_ms = cf_getms();
	st->b_first_seen = true;
	pthread_mutex_unlock(&st->lock);
}

TEST(seed_reseed_tend_mode, "M-12 a same-tend tend-mode refresh applies only to the next reseed attempt")
{
	fnode a, b;
	assert_true(fnode_start(&a, "A"));
	fnode_init(&b, "B");
	switch_state st;
	memset(&st, 0, sizeof(st));
	pthread_mutex_init(&st.lock, NULL);
	st.a = &a;
	b.on_first = switch_first_b;
	b.udata = &st;
	assert_true(fnode_listen(&b, 0));
	fnode_addr(&b, st.b_addr, sizeof(st.b_addr));

	as_config config;
	as_error err;
	fclient c;
	fake_provider fp;

	// One deadline-bound refresh per tend. Replace publishes the discovered seeds only.
	fp_init(&fp, true, true, NULL);
	fp.step = switch_step;
	fp.arg = &st;
	client_config(&config);
	config.discovery.execution_mode = AS_DISCOVERY_EXECUTION_TEND;
	config.discovery.merge_policy = AS_SEED_REPLACE;
	assert_int_eq(fp_register(&config, &fp, &err), AEROSPIKE_OK);
	assert_int_eq(client_connect(&c, &config), AEROSPIKE_OK);
	assert_true(has_node(c.as, "A"));

	// A refuses every connection until the cluster is empty and reseeding against A fails.
	fnode_set_reject(&a, true);
	assert_true(wait_for_nodes(c.as, 0, 10000));
	uint32_t rejects = fnode_get(&a, &a.rejects);
	assert_true(fnode_wait(&a, &a.rejects, rejects + 1, 5000));

	// The next refresh returns only B.
	pthread_mutex_lock(&st.lock);
	st.switched = true;
	pthread_mutex_unlock(&st.lock);
	assert_true(wait_for_nodes(c.as, 1, 5000));
	assert_true(has_node(c.as, "B"));

	pthread_mutex_lock(&st.lock);
	bool recorded = st.recorded;
	bool b_first_seen = st.b_first_seen;
	uint64_t gap_ms = st.b_first_ms - st.switch_ms;
	pthread_mutex_unlock(&st.lock);
	assert_true(recorded);
	assert_true(b_first_seen);

	// The tend whose refresh returned B had already reseeded against A, so B was first
	// contacted by the next tend, a tend sleep later, not right after the refresh.
	assert_true(gap_ms >= AS_TEND_INTERVAL_MIN / 2);
	assert_int_eq(stats_of(c.as->cluster->seed_manager).overruns, 0);

	client_close(&c);
	fp_settle(&fp);
	assert_int_eq(fp.destroyed, 1);
	fp_free(&fp);
	fnode_stop(&b);
	fnode_stop(&a);
	pthread_mutex_destroy(&st.lock);
}

static bool
reseed_next_tend(as_discovery_execution_mode mode, bool provider, uint64_t* elapsed)
{
	fnode a;

	if (!fnode_start(&a, "A")) {
		return false;
	}

	as_config config;
	as_error err;
	fclient c;
	fake_provider fp;
	char addr[64];
	bool ok = false;

	fp_init(&fp, true, true, fnode_addr(&a, addr, sizeof(addr)));
	client_config(&config);
	config.discovery.execution_mode = mode;
	config.discovery.refresh_interval_ms = 600000;

	if (provider) {
		fp_register(&config, &fp, &err);
	}
	else {
		as_config_add_host(&config, "127.0.0.1", a.port);
	}

	if (client_connect(&c, &config) == AEROSPIKE_OK) {
		uint16_t port = a.port;
		fnode_halt(&a);

		if (wait_for_nodes(c.as, 0, 10000)) {
			// Let at least one zero-node tend fail before the node returns.
			uint64_t iteration = client_stats(c.as).tend_iteration;

			while (client_stats(c.as).tend_iteration < iteration + 2) {
				as_sleep(10);
			}

			uint64_t begin = cf_getms();

			if (fnode_listen(&a, port) && wait_for_nodes(c.as, 1, 10000)) {
				*elapsed = cf_getms() - begin;
				ok = has_node(c.as, "A");
			}
		}
	}
	client_close(&c);

	if (provider) {
		fp_settle(&fp);
	}
	fp_free(&fp);
	fnode_stop(&a);
	return ok;
}

TEST(seed_reseed_next_tend, "N-12 precursor: a restored node is reseeded on the next tend, not after refresh_interval_ms")
{
	uint64_t elapsed = 0;

	// Two tend intervals plus scheduling slack, against a refresh interval of ten minutes.
	assert_true(reseed_next_tend(AS_DISCOVERY_EXECUTION_THREAD, true, &elapsed));
	assert_true(elapsed < AS_TEND_INTERVAL_MIN * 2 + 500);
	assert_true(reseed_next_tend(AS_DISCOVERY_EXECUTION_TEND, true, &elapsed));
	assert_true(elapsed < AS_TEND_INTERVAL_MIN * 2 + 500);
	assert_true(reseed_next_tend(AS_DISCOVERY_EXECUTION_THREAD, false, &elapsed));
	assert_true(elapsed < AS_TEND_INTERVAL_MIN * 2 + 500);
}

//---------------------------------
// Tests: lifecycle
//---------------------------------

typedef struct closer_s {
	fclient* c;
	uint8_t done;
} closer;

static void*
close_run(void* udata)
{
	closer* cl = udata;
	client_close(cl->c);
	as_store_uint8(&cl->done, 1);
	return NULL;
}

TEST(seed_close_in_flight, "close waits for an in-flight refresh; destroy runs once, after it returns")
{
	fnode a;
	assert_true(fnode_start(&a, "A"));
	as_config config;
	as_error err;
	fclient c;
	fake_provider fp;
	char addr[64];

	fp_init(&fp, true, false, fnode_addr(&a, addr, sizeof(addr)));
	fp.gate_call = 2;
	client_config(&config);
	assert_int_eq(fp_register(&config, &fp, &err), AEROSPIKE_OK);
	assert_int_eq(client_connect(&c, &config), AEROSPIKE_OK);
	assert_true(fp_wait_blocked(&fp, 5000));

	closer cl = {.c = &c, .done = 0};
	pthread_t thread;
	assert_int_eq(pthread_create(&thread, NULL, close_run, &cl), 0);
	as_sleep(400);
	assert_int_eq(as_load_uint8(&cl.done), 0);
	assert_int_eq(fp_get(&fp, &fp.destroyed), 0);

	fp_open(&fp);
	pthread_join(thread, NULL);
	assert_int_eq(cl.done, 1);

	// Async close destroys the client; sync close plus destroy does the same.
	assert_int_eq(fp.destroyed, 1);
	assert_int_eq(fp.active_at_destroy, 0);
	assert_int_eq(fp.finished_at_destroy, 2);
	as_sleep(300);
	assert_int_eq(fp_get(&fp, &fp.calls), 2);
	fp_settle(&fp);
	fp_free(&fp);
	fnode_stop(&a);
}

TEST(seed_close_reconnect, "sync close stops discovery and keeps the registration; reconnect starts it again")
{
	fnode a;
	assert_true(fnode_start(&a, "A"));
	as_config config;
	as_error err;
	fclient c;
	fake_provider fp;
	char addr[64];
	int before = discovery_thread_count();

	fp_init(&fp, true, false, fnode_addr(&a, addr, sizeof(addr)));
	client_config(&config);
	config.discovery.refresh_interval_ms = 600000;
	assert_int_eq(fp_register(&config, &fp, &err), AEROSPIKE_OK);
	assert_int_eq(client_connect(&c, &config), AEROSPIKE_OK);

	if (before >= 0) {
		assert_int_eq(settle(discovery_thread_count, before + 1), before + 1);
	}

	client_sync_close(&c);
	assert_false(c.as->config.discovery.registration_locked);
	assert_int_eq(fp.destroyed, 0);

	if (before >= 0) {
		assert_int_eq(settle(discovery_thread_count, before), before);
	}

	assert_int_eq(aerospike_connect(c.as, &err), AEROSPIKE_OK);
	assert_true(has_node(c.as, "A"));
	assert_int_eq(fp_get(&fp, &fp.calls), 2);
	assert_int_eq(fp.iteration[1], 0);

	if (before >= 0) {
		assert_int_eq(settle(discovery_thread_count, before + 1), before + 1);
	}

	client_close(&c);
	fp_settle(&fp);
	assert_int_eq(fp.destroyed, 1);

	if (before >= 0) {
		assert_int_eq(settle(discovery_thread_count, before), before);
	}
	fp_free(&fp);
	fnode_stop(&a);
}

TEST(seed_force_single, "force_single_node bootstraps from the provider snapshot and keeps no refresh thread")
{
	fnode a;
	assert_true(fnode_start(&a, "A"));
	as_config config;
	as_error err;
	fclient c;
	fake_provider fp;
	char addr[64];
	int before = discovery_thread_count();

	fp_init(&fp, true, false, fnode_addr(&a, addr, sizeof(addr)));
	client_config(&config);
	config.force_single_node = true;
	config.discovery.refresh_interval_ms = 600000;
	assert_int_eq(fp_register(&config, &fp, &err), AEROSPIKE_OK);
	assert_int_eq(client_connect(&c, &config), AEROSPIKE_OK);
	assert_true(has_node(c.as, "A"));

	// The initial refresh ran on the refresh thread, which is gone once the node is forced.
	assert_int_eq(fp.calls, 1);
	assert_false(pthread_equal(fp.thread[0], pthread_self()));
	assert_false(client_stats(c.as).thread_running);

	if (before >= 0) {
		assert_int_eq(settle(discovery_thread_count, before), before);
	}

	client_close(&c);
	fp_settle(&fp);
	assert_int_eq(fp.destroyed, 1);
	fp_free(&fp);

	// An empty provider result cannot force a node.
	fp_init(&fp, false, false, "");
	client_config(&config);
	config.force_single_node = true;
	assert_int_eq(fp_register(&config, &fp, &err), AEROSPIKE_OK);
	assert_int_eq(client_connect(&c, &config), AEROSPIKE_ERR_CLIENT);
	client_close(&c);
	fp_settle(&fp);
	fp_free(&fp);
	fnode_stop(&a);
}

TEST(seed_close_bounded, "close waits at most refresh_timeout_ms for a blocked refresh; destroy runs after it returns")
{
	fnode a;
	assert_true(fnode_start(&a, "A"));
	as_config config;
	as_error err;
	fclient c;
	fake_provider fp;
	char addr[64];
	int before = discovery_thread_count();

	fp_init(&fp, true, false, fnode_addr(&a, addr, sizeof(addr)));
	fp.gate_call = 2;
	client_config(&config);
	config.discovery.refresh_timeout_ms = 300;
	assert_int_eq(fp_register(&config, &fp, &err), AEROSPIKE_OK);
	assert_int_eq(client_connect(&c, &config), AEROSPIKE_OK);
	assert_true(fp_wait_blocked(&fp, 5000));

	uint64_t begin = cf_getms();
	client_close(&c);
	assert_true(cf_getms() - begin < 2000);
	assert_int_eq(fp_get(&fp, &fp.destroyed), 0);

	fp_open(&fp);
	assert_true(fp_wait(&fp, &fp.destroyed, 1, 5000));
	assert_true(fp_destroyed_off_thread(&fp));
	assert_int_eq(fp_get(&fp, &fp.active_at_destroy), 0);

	if (before >= 0) {
		assert_int_eq(settle(discovery_thread_count, before), before);
	}
	assert_int_eq(fp_get(&fp, &fp.destroyed), 1);
	fp_settle(&fp);
	fp_free(&fp);
	fnode_stop(&a);
}

TEST(seed_reconnect_in_flight, "reconnect fails naming the provider while the previous connection's refresh runs, then succeeds")
{
	fnode a;
	assert_true(fnode_start(&a, "A"));
	as_config config;
	as_error err;
	fclient c;
	fake_provider fp;
	char addr[64];
	int before = discovery_thread_count();

	fp_init(&fp, true, false, fnode_addr(&a, addr, sizeof(addr)));
	fp.gate_call = 2;
	client_config(&config);
	config.discovery.refresh_timeout_ms = 300;
	assert_int_eq(fp_register(&config, &fp, &err), AEROSPIKE_OK);
	assert_int_eq(client_connect(&c, &config), AEROSPIKE_OK);
	assert_true(fp_wait_blocked(&fp, 5000));

	// The sync close gives up on the blocked call; the registration stays.
	client_sync_close(&c);
	uint32_t calls = fp_get(&fp, &fp.calls);
	assert_int_eq(fp_get(&fp, &fp.destroyed), 0);

	assert_int_eq(aerospike_connect(c.as, &err), AEROSPIKE_ERR_SEED_DISCOVERY_FAILED);
	assert_not_null(strstr(err.message, "Seed provider fake"));
	assert_not_null(strstr(err.message, "previous connection"));
	assert_null(c.as->cluster);
	assert_false(c.as->config.discovery.registration_locked);
	assert_int_eq(fp_get(&fp, &fp.calls), calls);

	fp_open(&fp);
	assert_true(fp_wait(&fp, &fp.finished, calls, 5000));
	assert_int_eq(aerospike_connect(c.as, &err), AEROSPIKE_OK);
	assert_true(has_node(c.as, "A"));
	assert_int_eq(fp_get(&fp, &fp.max_active), 1);
	client_close(&c);
	fp_settle(&fp);
	assert_int_eq(fp.destroyed, 1);

	if (before >= 0) {
		assert_int_eq(settle(discovery_thread_count, before), before);
	}
	fp_free(&fp);
	fnode_stop(&a);
}

TEST(seed_discovered_no_ip_map, "ip_map rewrites configured seeds as before, but not provider-discovered seeds")
{
	fnode a;
	assert_true(fnode_start(&a, "A"));
	as_config config;
	as_error err;
	fclient c;
	char addr[64];

	// The fake node listens on 127.0.0.1 only; nothing answers on 127.0.0.2.
	as_addr_map map = {.orig = "127.0.0.2", .alt = "127.0.0.1"};

	client_config(&config);
	as_config_add_host(&config, "127.0.0.2", a.port);
	config.ip_map = &map;
	config.ip_map_size = 1;
	assert_int_eq(client_connect(&c, &config), AEROSPIKE_OK);
	assert_true(has_node(c.as, "A"));

	// ip_map is owned by the caller here.
	c.as->config.ip_map = NULL;
	c.as->config.ip_map_size = 0;
	client_close(&c);

	// The same address returned by a provider is used as returned.
	fake_provider fp;
	snprintf(addr, sizeof(addr), "127.0.0.2:%u", a.port);
	fp_init(&fp, false, false, addr);
	client_config(&config);
	config.ip_map = &map;
	config.ip_map_size = 1;
	assert_int_eq(fp_register(&config, &fp, &err), AEROSPIKE_OK);
	as_status status = client_connect(&c, &config);
	c.as->config.ip_map = NULL;
	c.as->config.ip_map_size = 0;
	assert_true(status != AEROSPIKE_OK);
	assert_int_eq(fp.calls, 1);
	client_close(&c);
	fp_settle(&fp);
	fp_free(&fp);
	fnode_stop(&a);
}

#if !defined(__APPLE__)
#include <aerospike/as_shm_cluster.h>
#include <sys/ipc.h>
#include <sys/shm.h>

TEST(seed_shm_takeover, "SHM: only the tend master refreshes in tend mode; a prole takes over with its own seeds")
{
	fnode a;
	assert_true(fnode_start(&a, "A"));
	as_error err;
	char addr[64];
	fnode_addr(&a, addr, sizeof(addr));

	int key = (int)(0xA5D00000 | ((uint32_t)getpid() & 0xFFFF));
	int probe = shmget(key, 4096, IPC_CREAT | IPC_EXCL | 0666);

	if (probe < 0) {
		info("skipped: shared memory unavailable: %s", strerror(errno));
		fnode_stop(&a);
		return;
	}
	shmctl(probe, IPC_RMID, 0);

	fake_provider fp1, fp2;
	as_config config1, config2;
	fclient c1, c2;

	fp_init(&fp1, true, true, addr);
	client_config(&config1);
	config1.use_shm = true;
	config1.shm_key = key;
	config1.discovery.execution_mode = AS_DISCOVERY_EXECUTION_TEND;
	assert_int_eq(fp_register(&config1, &fp1, &err), AEROSPIKE_OK);
	assert_int_eq(client_connect(&c1, &config1), AEROSPIKE_OK);
	assert_true(c1.as->cluster->shm_info->is_tend_master);

	fp_init(&fp2, true, true, addr);
	client_config(&config2);
	config2.use_shm = true;
	config2.shm_key = key;
	config2.discovery.execution_mode = AS_DISCOVERY_EXECUTION_TEND;
	assert_int_eq(fp_register(&config2, &fp2, &err), AEROSPIKE_OK);
	assert_int_eq(client_connect(&c2, &config2), AEROSPIKE_OK);
	assert_false(c2.as->cluster->shm_info->is_tend_master);

	// Each process initialized its own seeds; only the master refreshes on tend.
	assert_int_eq(fp_get(&fp2, &fp2.calls), 1);
	assert_true(fp_wait(&fp1, &fp1.calls, 5, 5000));
	assert_int_eq(fp_get(&fp2, &fp2.calls), 1);

	char buf[256];
	assert_string_eq(snap_str(c2.as->cluster->seed_manager, buf, sizeof(buf)), addr);

	// The prole takes over and starts refreshing with its local manager.
	client_close(&c1);
	assert_true(fp_wait(&fp2, &fp2.calls, 4, 15000));
	assert_true(c2.as->cluster->shm_info->is_tend_master);
	assert_true(client_stats(c2.as).tend_iteration >= 3);
	client_close(&c2);
	fp_settle(&fp1);
	fp_settle(&fp2);

	assert_int_eq(fp1.destroyed, 1);
	assert_int_eq(fp2.destroyed, 1);
	fp_free(&fp1);
	fp_free(&fp2);
	fnode_stop(&a);
}
#endif

#else

TEST(seed_fake_servers_skipped, "client tests with fake nodes need POSIX loopback servers")
{
	info("skipped: the fake node client tests need POSIX sockets");
}

#endif // DISCOVERY_FAKE_SERVERS

//---------------------------------
// Test Suite
//---------------------------------

SUITE(discovery_seed_manager, "pluggable discovery seed manager and execution mode tests")
{
	suite_add(seed_snapshot_copy_on_write);
	suite_add(seed_snapshot_atomic);
	suite_add(seed_default_no_thread);
	suite_add(seed_merge_publication);
	suite_add(seed_tls_defaults);
	suite_add(seed_init_failure);
	suite_add(seed_validation);
	suite_add(seed_tend_cadence);
	suite_add(seed_tend_errors);
	suite_add(seed_tend_overrun);
	suite_add(seed_tend_breaker);
	suite_add(seed_tend_breaker_thread_mode);
	suite_add(seed_thread_context);
	suite_add(seed_thread_initial_wait);
	suite_add(seed_thread_interval);
	suite_add(seed_thread_urgent);
	suite_add(seed_thread_never_blocks_tend);
	suite_add(seed_thread_failures);
	suite_add(seed_late_init_publication);
	suite_add(seed_thread_stop_bounded);
	suite_add(seed_reconnect_wait);
	suite_add(seed_deferred_destroy_sites);
	suite_add(seed_binding_thread);
	suite_add(seed_snapshot_ip_map_flags);
	suite_add(seed_thread_stop_in_flight);
#ifdef DISCOVERY_FAKE_SERVERS
	suite_add(seed_provider_only_bootstrap);
	suite_add(seed_connect_init_failure);
	suite_add(seed_connect_validation);
	suite_add(seed_default_compat);
	suite_add(seed_thread_mode_tend_continues);
	suite_add(seed_reseed_in_flight);
	suite_add(seed_reseed_tend_mode);
	suite_add(seed_reseed_next_tend);
	suite_add(seed_close_in_flight);
	suite_add(seed_close_reconnect);
	suite_add(seed_force_single);
	suite_add(seed_close_bounded);
	suite_add(seed_reconnect_in_flight);
	suite_add(seed_discovered_no_ip_map);
#if !defined(__APPLE__)
	suite_add(seed_shm_takeover);
#endif
#else
	suite_add(seed_fake_servers_skipped);
#endif
}
