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
#include <aerospike/as_cluster.h>
#include <aerospike/as_atomic.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_thread.h>
#include <aerospike/as_tls.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_clock.h>

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <string.h>

//---------------------------------
// Macros
//---------------------------------

#define AS_SEED_MAX_OVERRUNS 3

//---------------------------------
// Types
//---------------------------------

struct as_seed_snapshot_s {
	uint32_t ref_count;

	// Never modified after publication.
	as_vector* /* <as_host> */ hosts;

	// Parallel to hosts: true for configured seeds, node addresses and as_cluster_add_seed()
	// hosts; provider-only hosts are used as returned.
	bool* ip_map;
};

typedef enum {
	AS_SEED_INIT_PENDING,
	AS_SEED_INIT_CLAIMED,
	AS_SEED_INIT_DONE,
	AS_SEED_INIT_TIMED_OUT,
	AS_SEED_INIT_FALLBACK,
	AS_SEED_INIT_FAILED
} as_seed_init_state;

typedef struct as_seed_deferred_s {
	void (*destroy)(void* udata);
	void* udata;
} as_seed_deferred;

struct as_seed_inflight_s {
	pthread_mutex_t lock;

	// Broadcast when the running call returns.
	pthread_cond_t cond;

	uint32_t ref_count;

	// A provider call runs on a discovery thread. name is owned by that call's seed manager,
	// which outlives the call.
	bool running;
	void* udata;
	const char* name;

	// Destroys of registrations that ended while their udata was in use.
	as_vector /* <as_seed_deferred> */ deferred;
};

struct as_seed_manager_s {
	// Guards the current snapshot pointer, refresh thread state and counters. Never held across
	// provider, DNS or socket work.
	pthread_mutex_t lock;

	// Serializes publishers so that a copy-on-write update cannot lose another publication.
	// Acquired before lock.
	pthread_mutex_t publish_lock;

	// Wakes the refresh thread for an urgent refresh or stop.
	pthread_cond_t wake_cond;

	// Signals the connecting thread that the initial refresh completed.
	pthread_cond_t init_cond;

	// Signals stop that the discovery thread exited.
	pthread_cond_t exit_cond;

	// One reference for the owner and one for a live discovery thread.
	uint32_t ref_count;

	as_seed_snapshot* current;

	// Merged initialization seeds, before node addresses are appended. Restored by the tend-mode
	// overrun breaker.
	as_seed_snapshot* init;

	as_vector* /* <as_host> */ configured;
	char* cluster_name;

	// Copy. The name is owned by the manager; udata stays owned by the registration, whose
	// destroy is deferred while a call runs.
	as_seed_provider provider;

	// Shared with the config. Set when the manager runs a discovery thread.
	as_seed_inflight* inflight;

	as_seed_merge_policy merge_policy;
	as_discovery_execution_mode execution_mode;
	uint32_t refresh_interval_ms;
	uint32_t refresh_timeout_ms;
	uint32_t tend_refresh_deadline_ms;
	uint32_t refresh_tends;
	bool tls_enabled;
	bool periodic;
	bool threaded;

	uint64_t tend_iteration;
	uint32_t refreshes;
	uint32_t failures;
	uint32_t overruns;
	uint32_t urgent_wakes;
	uint32_t publications;
	bool tripped;

	pthread_t thread;
	as_seed_init_state init_state;
	as_status init_status;
	as_error init_error;

	// Start of the initial call plus refresh_timeout_ms. Bounds stop while connecting.
	uint64_t connect_deadline;
	uint64_t last_urgent_ms;
	bool connecting;
	bool thread_started;
	bool thread_exited;
	bool thread_released;
	bool in_refresh;
	bool urgent;
	bool urgent_honored;
	bool stop;
};

//---------------------------------
// Static Functions: hosts
//---------------------------------

static void
hosts_destroy(as_vector* hosts)
{
	if (!hosts) {
		return;
	}

	for (uint32_t i = 0; i < hosts->size; i++) {
		as_host_destroy(as_vector_get(hosts, i));
	}
	as_vector_destroy(hosts);
}

static as_vector*
hosts_copy(const as_vector* src, uint32_t extra)
{
	uint32_t size = src ? src->size : 0;
	as_vector* trg = as_vector_create(sizeof(as_host), size + extra + 1);

	for (uint32_t i = 0; i < size; i++) {
		as_host_copy(as_vector_get((as_vector*)src, i), as_vector_reserve(trg));
	}
	return trg;
}

static bool
hosts_contain(const as_vector* hosts, const char* name, uint16_t port)
{
	for (uint32_t i = 0; i < hosts->size; i++) {
		const as_host* host = as_vector_get((as_vector*)hosts, i);

		if (host->port == port && strcmp(host->name, name) == 0) {
			return true;
		}
	}
	return false;
}

// Same default as configured seeds: the cluster name when set, otherwise the host name. An empty
// TLS name is the same as none.
static void
default_tls_name(as_seed_manager* m, as_host* host)
{
	if (host->tls_name && !host->tls_name[0]) {
		cf_free(host->tls_name);
		host->tls_name = NULL;
	}

	if (m->tls_enabled && !host->tls_name) {
		host->tls_name = cf_strdup(m->cluster_name ? m->cluster_name : host->name);
	}
}

//---------------------------------
// Static Functions: snapshots
//---------------------------------

// One ip_map flag per host. NULL when there is no host or the allocation failed.
static bool*
ip_map_create(uint32_t size)
{
	return size ? cf_malloc(size * sizeof(bool)) : NULL;
}

// Takes ownership of hosts and ip_map.
static as_seed_snapshot*
snapshot_create(as_vector* hosts, bool* ip_map)
{
	as_seed_snapshot* snapshot = NULL;

	if (ip_map || hosts->size == 0) {
		snapshot = cf_malloc(sizeof(as_seed_snapshot));
	}

	if (!snapshot) {
		hosts_destroy(hosts);
		cf_free(ip_map);
		return NULL;
	}

	snapshot->ref_count = 1;
	snapshot->hosts = hosts;
	snapshot->ip_map = ip_map;
	return snapshot;
}

static inline void
snapshot_reserve(as_seed_snapshot* snapshot)
{
	as_incr_uint32(&snapshot->ref_count);
}

// Swap in a complete snapshot, taking over the caller's reference. Caller holds publish_lock.
static void
publish(as_seed_manager* m, as_seed_snapshot* snapshot)
{
	pthread_mutex_lock(&m->lock);
	as_seed_snapshot* old = m->current;
	m->current = snapshot;
	m->publications++;
	pthread_mutex_unlock(&m->lock);

	as_seed_snapshot_release(old);
}

static void
publish_locked(as_seed_manager* m, as_seed_snapshot* snapshot)
{
	pthread_mutex_lock(&m->publish_lock);
	publish(m, snapshot);
	pthread_mutex_unlock(&m->publish_lock);
}

//---------------------------------
// Static Functions: provider
//---------------------------------

static const char*
error_message(const as_error* err)
{
	return err->message[0] ? err->message : "no message";
}

// Call the provider once. candidates is always left destroyable. On AEROSPIKE_OK it holds the
// copied hosts.
static as_status
call_provider(as_seed_manager* m, as_error* err, as_seed_candidates* candidates)
{
	as_error_reset(err);

	if (!as_seed_candidates_init(candidates, 8)) {
		candidates->hosts = NULL;
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to allocate seed candidates");
	}

	as_seed_candidate_context context = {
		.cluster_name = m->cluster_name,
		.configured_seeds = m->configured,
		.tend_iteration = as_load_uint64(&m->tend_iteration)
	};

	as_vector* hosts = candidates->hosts;
	as_status status = m->provider.refresh(err, &context, candidates, m->provider.udata);

	if (candidates->hosts != hosts) {
		// The container is client-owned. Restore it so that it is released.
		candidates->hosts = hosts;
		as_seed_candidates_destroy(candidates);
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT,
			"Seed provider replaced the client-owned candidate container");
	}

	if (status != AEROSPIKE_OK) {
		as_seed_candidates_destroy(candidates);

		if (err->code == AEROSPIKE_OK) {
			err->code = status;
		}
		return status;
	}

	for (uint32_t i = 0; i < hosts->size; i++) {
		const as_host* host = as_vector_get(hosts, i);

		if (!host->name || !host->name[0]) {
			as_seed_candidates_destroy(candidates);
			return as_error_update(err, AEROSPIKE_ERR_CLIENT,
				"Seed provider returned a candidate without a host name at index %u", i);
		}
	}
	return AEROSPIKE_OK;
}

// Merge discovered hosts with the configured seeds and apply TLS name defaults.
static as_status
build_snapshot(
	as_seed_manager* m, as_error* err, const as_vector* discovered, bool init,
	as_seed_snapshot** out
	)
{
	uint32_t capacity = m->configured->size + (discovered ? discovered->size : 0);
	as_seed_candidates merged;

	if (!as_seed_candidates_init(&merged, capacity) ||
		!as_discovery_merge_hosts(m->merge_policy, init, m->configured, discovered, &merged)) {
		as_seed_candidates_destroy(&merged);
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to merge seeds of provider %s",
			m->provider.name);
	}

	uint32_t size = merged.hosts->size;
	bool* ip_map = ip_map_create(size);

	for (uint32_t i = 0; i < size; i++) {
		as_host* host = as_vector_get(merged.hosts, i);

		default_tls_name(m, host);

		if (ip_map) {
			ip_map[i] = hosts_contain(m->configured, host->name, host->port);
		}
	}

	as_seed_snapshot* snapshot = snapshot_create(merged.hosts, ip_map);

	if (!snapshot) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to allocate seed snapshot");
	}

	*out = snapshot;
	return AEROSPIKE_OK;
}

// Publish the initialization seeds. A provider error falls back to the configured seeds.
static as_status
complete_init(
	as_seed_manager* m, as_error* err, as_status status, const as_error* provider_err,
	const as_seed_candidates* candidates
	)
{
	const as_vector* discovered = NULL;

	if (status == AEROSPIKE_OK) {
		discovered = candidates->hosts;
	}
	else {
		as_log_warn("Seed provider %s failed: %s %s", m->provider.name, as_error_string(status),
			error_message(provider_err));
	}

	as_seed_snapshot* snapshot;
	as_status rv = build_snapshot(m, err, discovered, true, &snapshot);

	if (rv != AEROSPIKE_OK) {
		return rv;
	}

	if (status != AEROSPIKE_OK && snapshot->hosts->size == 0) {
		as_seed_snapshot_release(snapshot);
		return as_error_update(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
			"Seed provider %s failed and no seed host is left: %s", m->provider.name,
			error_message(provider_err));
	}

	pthread_mutex_lock(&m->publish_lock);
	snapshot_reserve(snapshot);
	m->init = snapshot;
	publish(m, snapshot);
	pthread_mutex_unlock(&m->publish_lock);
	return AEROSPIKE_OK;
}

static void
count_failure(as_seed_manager* m)
{
	pthread_mutex_lock(&m->lock);
	m->failures++;
	pthread_mutex_unlock(&m->lock);
}

// Publish a refresh result, or keep the previous seeds. init merges as at initialization.
static void
complete_refresh(
	as_seed_manager* m, as_status status, const as_error* provider_err,
	const as_seed_candidates* candidates, bool init
	)
{
	if (status != AEROSPIKE_OK) {
		as_log_warn("Seed provider %s refresh failed: %s %s. Keeping the previous seeds",
			m->provider.name, as_error_string(status), error_message(provider_err));
		count_failure(m);
		return;
	}

	as_error err;
	as_seed_snapshot* snapshot;

	if (build_snapshot(m, &err, candidates->hosts, init, &snapshot) != AEROSPIKE_OK) {
		as_log_warn("%s. Keeping the previous seeds", err.message);
		count_failure(m);
		return;
	}

	if (snapshot->hosts->size == 0) {
		as_log_warn("Seed provider %s refresh left no seed host. Keeping the previous seeds",
			m->provider.name);
		as_seed_snapshot_release(snapshot);
		return;
	}
	publish_locked(m, snapshot);
}

static void
count_refresh(as_seed_manager* m)
{
	pthread_mutex_lock(&m->lock);
	m->refreshes++;
	pthread_mutex_unlock(&m->lock);
}

//---------------------------------
// Static Functions: waits
//---------------------------------

static void
abstime_after(uint64_t ms, struct timespec* abstime)
{
	struct timespec delta;
	cf_clock_set_timespec_ms(ms > INT_MAX ? INT_MAX : (int)ms, &delta);
	cf_clock_current_add(&delta, abstime);

	// cf_clock_current_add() can leave tv_nsec at one second, which pthread_cond_timedwait()
	// rejects with EINVAL.
	if (abstime->tv_nsec >= 1000000000) {
		abstime->tv_nsec -= 1000000000;
		abstime->tv_sec++;
	}
}

// Wait once, at most until deadline_ms on the cf_getms() clock. Callers loop on their own
// condition and the deadline.
static void
wait_until(pthread_cond_t* cond, pthread_mutex_t* lock, uint64_t deadline_ms)
{
	uint64_t now = cf_getms();

	if (now >= deadline_ms) {
		return;
	}

	struct timespec abstime;
	abstime_after(deadline_ms - now, &abstime);
	pthread_cond_timedwait(cond, lock, &abstime);
}

//---------------------------------
// Static Functions: in-flight call
//---------------------------------

// Create the config's tracker when needed and add a reference for the caller.
static as_seed_inflight*
inflight_acquire(as_discovery_config* d)
{
	as_seed_inflight* inflight = d->inflight;

	if (!inflight) {
		inflight = cf_calloc(1, sizeof(as_seed_inflight));

		if (!inflight) {
			return NULL;
		}

		pthread_mutex_init(&inflight->lock, NULL);
		pthread_cond_init(&inflight->cond, NULL);
		as_vector_init(&inflight->deferred, sizeof(as_seed_deferred), 2);

		// The config's reference.
		inflight->ref_count = 1;
		d->inflight = inflight;
	}
	as_incr_uint32(&inflight->ref_count);
	return inflight;
}

// Caller holds the seed manager lock, which is acquired before the tracker lock.
static void
inflight_begin(as_seed_inflight* inflight, void* udata, const char* name)
{
	pthread_mutex_lock(&inflight->lock);
	inflight->running = true;
	inflight->udata = udata;
	inflight->name = name;
	pthread_mutex_unlock(&inflight->lock);
}

// Run the destroys handed over during the call, then mark it returned. Returns true when a
// destroy ran: udata is gone and the provider must not be called again.
static bool
inflight_end(as_seed_inflight* inflight)
{
	bool released = false;

	pthread_mutex_lock(&inflight->lock);

	// Still running while destroys run, so a destroy handed over meanwhile is not lost.
	while (inflight->deferred.size > 0) {
		as_seed_deferred deferred = *(as_seed_deferred*)as_vector_get(&inflight->deferred, 0);
		as_vector_remove(&inflight->deferred, 0);
		pthread_mutex_unlock(&inflight->lock);

		deferred.destroy(deferred.udata);
		released = true;

		pthread_mutex_lock(&inflight->lock);
	}

	inflight->running = false;
	inflight->udata = NULL;
	inflight->name = NULL;
	pthread_cond_broadcast(&inflight->cond);
	pthread_mutex_unlock(&inflight->lock);
	return released;
}

//---------------------------------
// Static Functions: lifetime
//---------------------------------

static void
manager_free(as_seed_manager* m)
{
	as_seed_snapshot_release(m->current);

	if (m->init) {
		as_seed_snapshot_release(m->init);
	}

	hosts_destroy(m->configured);
	cf_free(m->cluster_name);
	cf_free((void*)m->provider.name);
	as_seed_inflight_release(m->inflight);

	pthread_cond_destroy(&m->exit_cond);
	pthread_cond_destroy(&m->init_cond);
	pthread_cond_destroy(&m->wake_cond);
	pthread_mutex_destroy(&m->publish_lock);
	pthread_mutex_destroy(&m->lock);
	cf_free(m);
}

static void
manager_release(as_seed_manager* m)
{
	if (as_aaf_uint32_rls(&m->ref_count, -1) == 0) {
		as_fence_acq();
		manager_free(m);
	}
}

//---------------------------------
// Static Functions: thread execution mode
//---------------------------------

static void*
refresh_thread(void* udata)
{
	as_seed_manager* m = udata;
	as_thread_set_name("discovery");

	bool first = true;

	pthread_mutex_lock(&m->lock);

	while (!m->stop) {
		m->in_refresh = true;
		m->urgent = false;
		inflight_begin(m->inflight, m->provider.udata, m->provider.name);
		pthread_mutex_unlock(&m->lock);

		as_error provider_err;
		as_seed_candidates candidates;
		uint64_t begin = cf_getms();
		as_status status = call_provider(m, &provider_err, &candidates);
		uint64_t elapsed = cf_getms() - begin;
		bool released = inflight_end(m->inflight);

		if (elapsed > m->refresh_timeout_ms) {
			pthread_mutex_lock(&m->lock);
			uint32_t overruns = ++m->overruns;
			pthread_mutex_unlock(&m->lock);

			as_log_warn("Seed provider %s refresh took %" PRIu64 " ms, exceeding "
				"refresh_timeout_ms %u. Overruns: %u", m->provider.name, elapsed,
				m->refresh_timeout_ms, overruns);
		}

		pthread_mutex_lock(&m->lock);
		m->refreshes++;
		bool init = m->init_state == AS_SEED_INIT_PENDING;
		bool late = false;

		if (init) {
			m->init_state = AS_SEED_INIT_CLAIMED;
		}
		else if (first) {
			// Connect stopped waiting and is publishing the fallback, which does no I/O.
			while (m->init_state == AS_SEED_INIT_TIMED_OUT && !m->stop) {
				pthread_cond_wait(&m->init_cond, &m->lock);
			}
			late = m->init_state == AS_SEED_INIT_FALLBACK && !m->stop;
		}
		pthread_mutex_unlock(&m->lock);

		if (init) {
			as_error err;
			as_error_init(&err);
			as_status rv = complete_init(m, &err, status, &provider_err, &candidates);

			pthread_mutex_lock(&m->lock);
			m->init_status = rv;
			m->init_error = err;
			m->init_state = AS_SEED_INIT_DONE;
			pthread_cond_broadcast(&m->init_cond);
			pthread_mutex_unlock(&m->lock);
		}
		else if (late) {
			complete_refresh(m, status, &provider_err, &candidates, true);
		}
		else if (!first) {
			complete_refresh(m, status, &provider_err, &candidates, false);
		}
		// Otherwise the late first result of a failed or stopping connect is dropped.
		as_seed_candidates_destroy(&candidates);

		pthread_mutex_lock(&m->lock);
		m->in_refresh = false;

		if (released || !m->periodic) {
			break;
		}

		// Start to start, as in the Java client.
		uint64_t next = begin + m->refresh_interval_ms;

		while (!m->stop && !m->urgent && cf_getms() < next) {
			wait_until(&m->wake_cond, &m->lock, next);
		}
		first = false;
	}

	m->thread_exited = true;
	pthread_cond_broadcast(&m->exit_cond);
	pthread_mutex_unlock(&m->lock);

	as_tls_thread_cleanup();
	manager_release(m);
	return NULL;
}

static as_status
start_thread(as_seed_manager* m, as_error* err)
{
	m->init_state = AS_SEED_INIT_PENDING;
	m->connecting = true;
	m->connect_deadline = cf_getms() + m->refresh_timeout_ms;

	// The thread's reference.
	m->ref_count++;
	m->thread_started = true;

	int rc = pthread_create(&m->thread, NULL, refresh_thread, m);

	if (rc != 0) {
		m->ref_count--;
		m->thread_started = false;
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
			"Failed to create discovery refresh thread: %s", strerror(rc));
	}

	pthread_mutex_lock(&m->lock);

	while (m->init_state == AS_SEED_INIT_PENDING) {
		if (cf_getms() >= m->connect_deadline) {
			m->init_state = AS_SEED_INIT_TIMED_OUT;
			break;
		}
		wait_until(&m->init_cond, &m->lock, m->connect_deadline);
	}

	// A claimed result is being published, which does no I/O.
	while (m->init_state == AS_SEED_INIT_CLAIMED) {
		pthread_cond_wait(&m->init_cond, &m->lock);
	}

	as_seed_init_state state = m->init_state;
	as_status status = m->init_status;

	if (status != AEROSPIKE_OK) {
		*err = m->init_error;
	}
	pthread_mutex_unlock(&m->lock);

	if (state == AS_SEED_INIT_TIMED_OUT) {
		// The thread publishes the late result once the fallback is published.
		as_error provider_err;
		as_error_update(&provider_err, AEROSPIKE_ERR_TIMEOUT,
			"Initial refresh did not complete within refresh_timeout_ms %u", m->refresh_timeout_ms);
		as_status rv = complete_init(m, err, AEROSPIKE_ERR_TIMEOUT, &provider_err, NULL);

		pthread_mutex_lock(&m->lock);
		m->init_state = rv == AEROSPIKE_OK ? AS_SEED_INIT_FALLBACK : AS_SEED_INIT_FAILED;
		pthread_cond_broadcast(&m->init_cond);
		pthread_mutex_unlock(&m->lock);
		return rv;
	}
	return status;
}

//---------------------------------
// Static Functions: validation
//---------------------------------

static as_status
validate(
	as_error* err, const as_config* config, const as_seed_provider* provider, bool* periodic,
	bool* threaded
	)
{
	const as_discovery_config* d = &config->discovery;

	if (d->merge_policy != AS_SEED_MERGE && d->merge_policy != AS_SEED_REPLACE &&
		d->merge_policy != AS_SEED_DISCOVERY_ONLY) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid discovery merge_policy: %d",
			(int)d->merge_policy);
	}

	if (d->execution_mode != AS_DISCOVERY_EXECUTION_THREAD &&
		d->execution_mode != AS_DISCOVERY_EXECUTION_TEND) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid discovery execution_mode: %d",
			(int)d->execution_mode);
	}

	if (config->tender_interval == 0) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "Invalid tend interval: 0");
	}

	*periodic = provider->needs_periodic_refresh(provider->udata);
	*threaded = (d->provider_registered && d->binding_provider) ||
		(*periodic && d->execution_mode == AS_DISCOVERY_EXECUTION_THREAD);

	if (!*periodic) {
		if (*threaded && d->refresh_timeout_ms == 0) {
			// A binding provider's single call runs on the discovery thread.
			return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
				"Invalid discovery refresh_timeout_ms 0: must be > 0");
		}
		// Refresh settings are unused: the provider is called once at initialization.
		return AEROSPIKE_OK;
	}

	if (d->execution_mode == AS_DISCOVERY_EXECUTION_TEND) {
		bool deadline = provider->supports_deadline && provider->supports_deadline(provider->udata);

		if (!deadline) {
			return as_error_update(err, AEROSPIKE_ERR_PARAM,
				"Seed provider %s does not support a deadline and cannot use discovery "
				"execution_mode tend. Use execution_mode thread", provider->name);
		}
	}

	if (d->refresh_interval_ms < config->tender_interval) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
			"Discovery refresh_interval_ms %u must be greater or equal to the tend interval %u",
			d->refresh_interval_ms, config->tender_interval);
	}

	if (d->execution_mode == AS_DISCOVERY_EXECUTION_TEND) {
		uint32_t max = config->tender_interval / 2;

		if (d->tend_refresh_deadline_ms == 0 || d->tend_refresh_deadline_ms > max) {
			return as_error_update(err, AEROSPIKE_ERR_PARAM,
				"Invalid discovery tend_refresh_deadline_ms %u: must be > 0 and <= tend interval / 2 (%u)",
				d->tend_refresh_deadline_ms, max);
		}
	}
	else if (d->refresh_timeout_ms == 0) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"Invalid discovery refresh_timeout_ms 0: must be > 0");
	}
	return AEROSPIKE_OK;
}

//---------------------------------
// Functions
//---------------------------------

void
as_seed_inflight_release(as_seed_inflight* inflight)
{
	if (inflight && as_aaf_uint32_rls(&inflight->ref_count, -1) == 0) {
		as_fence_acq();
		as_vector_destroy(&inflight->deferred);
		pthread_cond_destroy(&inflight->cond);
		pthread_mutex_destroy(&inflight->lock);
		cf_free(inflight);
	}
}

bool
as_seed_inflight_defer_destroy(as_seed_inflight* inflight, void (*destroy)(void*), void* udata)
{
	if (!inflight) {
		return false;
	}

	pthread_mutex_lock(&inflight->lock);
	bool defer = inflight->running && inflight->udata == udata;

	if (defer) {
		as_seed_deferred deferred = {.destroy = destroy, .udata = udata};
		as_vector_append(&inflight->deferred, &deferred);
	}
	pthread_mutex_unlock(&inflight->lock);
	return defer;
}

as_status
as_seed_inflight_wait(as_seed_inflight* inflight, uint32_t timeout_ms, as_error* err)
{
	if (!inflight) {
		return AEROSPIKE_OK;
	}

	uint64_t deadline = cf_getms() + timeout_ms;

	pthread_mutex_lock(&inflight->lock);

	while (inflight->running && cf_getms() < deadline) {
		wait_until(&inflight->cond, &inflight->lock, deadline);
	}

	if (inflight->running) {
		// name belongs to the old seed manager and is valid only while running.
		as_error_update(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
			"Seed provider %s refresh started by a previous connection did not return within "
			"refresh_timeout_ms %u", inflight->name, timeout_ms);
		pthread_mutex_unlock(&inflight->lock);
		return err->code;
	}
	pthread_mutex_unlock(&inflight->lock);
	return AEROSPIKE_OK;
}

as_status
as_seed_manager_create(as_error* err, as_config* config, as_seed_manager** manager)
{
	as_status status = as_seed_inflight_wait(config->discovery.inflight,
		config->discovery.refresh_timeout_ms, err);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_seed_provider provider;

	if (config->discovery.provider_registered) {
		provider = config->discovery.provider;
	}
	else {
		as_discovery_static_seed_provider(&provider);
	}

	bool periodic = false;
	bool threaded = false;
	status = validate(err, config, &provider, &periodic, &threaded);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_seed_manager* m = cf_calloc(1, sizeof(as_seed_manager));

	if (!m) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to allocate seed manager");
	}

	const as_discovery_config* d = &config->discovery;

	m->ref_count = 1;
	m->provider = provider;
	m->provider.name = cf_strdup(provider.name);
	m->merge_policy = d->merge_policy;
	m->execution_mode = d->execution_mode;
	m->refresh_interval_ms = d->refresh_interval_ms;
	m->refresh_timeout_ms = d->refresh_timeout_ms;
	m->tend_refresh_deadline_ms = d->tend_refresh_deadline_ms;
	m->refresh_tends = periodic ? d->refresh_interval_ms / config->tender_interval : 0;
	m->tls_enabled = config->tls.enable;
	m->periodic = periodic;
	m->threaded = threaded;
	m->init_status = AEROSPIKE_OK;
	as_error_init(&m->init_error);
	m->cluster_name = config->cluster_name ? cf_strdup(config->cluster_name) : NULL;
	m->configured = hosts_copy(config->hosts, 0);
	m->current = snapshot_create(as_vector_create(sizeof(as_host), 1), NULL);

	if (!m->current) {
		hosts_destroy(m->configured);
		cf_free(m->cluster_name);
		cf_free((void*)m->provider.name);
		cf_free(m);
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to allocate seed snapshot");
	}

	pthread_mutex_init(&m->lock, NULL);
	pthread_mutex_init(&m->publish_lock, NULL);
	pthread_cond_init(&m->wake_cond, NULL);
	pthread_cond_init(&m->init_cond, NULL);
	pthread_cond_init(&m->exit_cond, NULL);

	if (threaded) {
		m->inflight = inflight_acquire(&config->discovery);

		if (!m->inflight) {
			manager_free(m);
			return as_error_set_message(err, AEROSPIKE_ERR_CLIENT,
				"Failed to allocate seed refresh tracker");
		}
	}

	*manager = m;
	return AEROSPIKE_OK;
}

as_status
as_seed_manager_start(as_seed_manager* m, as_error* err)
{
	if (m->threaded) {
		return start_thread(m, err);
	}

	as_error provider_err;
	as_seed_candidates candidates;
	as_status status = call_provider(m, &provider_err, &candidates);
	count_refresh(m);

	as_status rv = complete_init(m, err, status, &provider_err, &candidates);
	as_seed_candidates_destroy(&candidates);
	return rv;
}

void
as_seed_manager_connected(as_seed_manager* m)
{
	pthread_mutex_lock(&m->lock);
	m->connecting = false;
	pthread_mutex_unlock(&m->lock);
}

as_seed_snapshot*
as_seed_manager_reserve(as_seed_manager* m)
{
	pthread_mutex_lock(&m->lock);
	as_seed_snapshot* snapshot = m->current;
	snapshot_reserve(snapshot);
	pthread_mutex_unlock(&m->lock);
	return snapshot;
}

const as_vector*
as_seed_snapshot_hosts(const as_seed_snapshot* snapshot)
{
	return snapshot->hosts;
}

bool
as_seed_snapshot_uses_ip_map(const as_seed_snapshot* snapshot, uint32_t index)
{
	return index < snapshot->hosts->size && snapshot->ip_map[index];
}

void
as_seed_snapshot_release(as_seed_snapshot* snapshot)
{
	if (as_aaf_uint32_rls(&snapshot->ref_count, -1) == 0) {
		as_fence_acq();
		hosts_destroy(snapshot->hosts);
		cf_free(snapshot->ip_map);
		cf_free(snapshot);
	}
}

void
as_seed_manager_request_urgent(as_seed_manager* m)
{
	if (!m->threaded || !m->periodic) {
		return;
	}

	pthread_mutex_lock(&m->lock);

	if (m->thread_started && !m->thread_exited && !m->in_refresh && !m->urgent && !m->stop) {
		uint64_t now = cf_getms();

		// Honored at most once per refresh_interval_ms, as in the Java client.
		if (!m->urgent_honored || now - m->last_urgent_ms >= m->refresh_interval_ms) {
			m->urgent_honored = true;
			m->last_urgent_ms = now;
			m->urgent = true;
			m->urgent_wakes++;
			pthread_cond_signal(&m->wake_cond);
		}
	}
	pthread_mutex_unlock(&m->lock);
}

void
as_seed_manager_tend(as_seed_manager* m)
{
	uint64_t iteration = as_aaf_uint64(&m->tend_iteration, 1);

	if (m->execution_mode != AS_DISCOVERY_EXECUTION_TEND || !m->periodic || m->tripped ||
		!m->init || iteration % m->refresh_tends != 0) {
		return;
	}

	as_error provider_err;
	as_seed_candidates candidates;
	uint64_t begin = cf_getns();
	as_status status = call_provider(m, &provider_err, &candidates);
	uint64_t elapsed_ns = cf_getns() - begin;
	count_refresh(m);

	if (elapsed_ns > (uint64_t)m->tend_refresh_deadline_ms * 1000 * 1000) {
		as_seed_candidates_destroy(&candidates);

		pthread_mutex_lock(&m->lock);
		uint32_t overruns = ++m->overruns;
		pthread_mutex_unlock(&m->lock);

		as_log_warn("Seed provider %s refresh took %" PRIu64 " ms, exceeding "
			"tend_refresh_deadline_ms %u. Result discarded. Consecutive overruns: %u",
			m->provider.name, elapsed_ns / (1000 * 1000), m->tend_refresh_deadline_ms, overruns);

		if (overruns >= AS_SEED_MAX_OVERRUNS) {
			pthread_mutex_lock(&m->lock);
			m->tripped = true;
			pthread_mutex_unlock(&m->lock);

			snapshot_reserve(m->init);
			publish_locked(m, m->init);

			as_log_error("Seed provider %s exceeded tend_refresh_deadline_ms %u %u consecutive "
				"times. It is no longer refreshed and the initial seeds are restored until the "
				"client is restarted", m->provider.name, m->tend_refresh_deadline_ms, overruns);
		}
		return;
	}

	pthread_mutex_lock(&m->lock);
	m->overruns = 0;
	pthread_mutex_unlock(&m->lock);

	complete_refresh(m, status, &provider_err, &candidates, false);
	as_seed_candidates_destroy(&candidates);
}

void
as_seed_manager_add_hosts(as_seed_manager* m, const as_vector* hosts)
{
	pthread_mutex_lock(&m->publish_lock);
	as_seed_snapshot* current = as_seed_manager_reserve(m);
	as_vector* trg = NULL;

	for (uint32_t i = 0; i < hosts->size; i++) {
		const as_host* host = as_vector_get((as_vector*)hosts, i);

		if (hosts_contain(trg ? trg : current->hosts, host->name, host->port)) {
			continue;
		}

		if (!trg) {
			trg = hosts_copy(current->hosts, hosts->size - i);
		}

		as_host* added = as_vector_reserve(trg);
		as_host_copy(host, added);
		default_tls_name(m, added);
		as_log_debug("Add seed %s %d", host->name, host->port);
	}

	if (trg) {
		uint32_t size = current->hosts->size;
		bool* ip_map = ip_map_create(trg->size);

		if (ip_map) {
			if (size > 0) {
				memcpy(ip_map, current->ip_map, size * sizeof(bool));
			}

			for (uint32_t i = size; i < trg->size; i++) {
				ip_map[i] = true;
			}
		}

		as_seed_snapshot* snapshot = snapshot_create(trg, ip_map);

		if (snapshot) {
			publish(m, snapshot);
		}
	}
	as_seed_snapshot_release(current);
	pthread_mutex_unlock(&m->publish_lock);
}

void
as_seed_manager_remove_host(as_seed_manager* m, const char* name, uint16_t port)
{
	pthread_mutex_lock(&m->publish_lock);
	as_seed_snapshot* current = as_seed_manager_reserve(m);

	if (hosts_contain(current->hosts, name, port)) {
		as_vector* trg = as_vector_create(sizeof(as_host), current->hosts->size);
		bool* ip_map = ip_map_create(current->hosts->size);

		// Remove all seeds even if there are duplicates.
		for (uint32_t i = 0; i < current->hosts->size; i++) {
			const as_host* host = as_vector_get(current->hosts, i);

			if (host->port == port && strcmp(host->name, name) == 0) {
				as_log_debug("Remove seed %s %d", name, port);
				continue;
			}

			if (ip_map) {
				ip_map[trg->size] = current->ip_map[i];
			}
			as_host_copy(host, as_vector_reserve(trg));
		}

		as_seed_snapshot* snapshot = snapshot_create(trg, ip_map);

		if (snapshot) {
			publish(m, snapshot);
		}
	}
	as_seed_snapshot_release(current);
	pthread_mutex_unlock(&m->publish_lock);
}

void
as_seed_manager_stop(as_seed_manager* m)
{
	pthread_mutex_lock(&m->lock);
	m->stop = true;
	pthread_cond_broadcast(&m->wake_cond);
	pthread_cond_broadcast(&m->init_cond);

	if (!m->thread_started || m->thread_released) {
		pthread_mutex_unlock(&m->lock);
		return;
	}
	m->thread_released = true;

	// An idle thread exits at once. Only a provider call in progress is bounded.
	uint64_t deadline = m->connecting ? m->connect_deadline : cf_getms() + m->refresh_timeout_ms;
	bool logged = false;

	while (!m->thread_exited) {
		if (!m->in_refresh) {
			pthread_cond_wait(&m->exit_cond, &m->lock);
			continue;
		}

		uint64_t now = cf_getms();

		if (now >= deadline) {
			break;
		}

		if (!logged) {
			logged = true;
			pthread_mutex_unlock(&m->lock);
			as_log_info("Waiting at most %" PRIu64 " ms for seed provider %s refresh to return",
				deadline - now, m->provider.name);
			pthread_mutex_lock(&m->lock);
			continue;
		}
		wait_until(&m->exit_cond, &m->lock, deadline);
	}

	bool exited = m->thread_exited;
	pthread_mutex_unlock(&m->lock);

	if (exited) {
		pthread_join(m->thread, NULL);
	}
	else {
		pthread_detach(m->thread);
		as_log_warn("Seed provider %s refresh did not return in time. It finishes on the "
			"discovery thread, which then releases the provider", m->provider.name);
	}
}

void
as_seed_manager_destroy(as_seed_manager* m)
{
	as_seed_manager_stop(m);
	manager_release(m);
}

void
as_seed_manager_get_stats(as_seed_manager* m, as_seed_manager_stats* stats)
{
	pthread_mutex_lock(&m->lock);
	stats->tend_iteration = as_load_uint64(&m->tend_iteration);
	stats->refreshes = m->refreshes;
	stats->failures = m->failures;
	stats->overruns = m->overruns;
	stats->urgent_wakes = m->urgent_wakes;
	stats->publications = m->publications;
	stats->thread_running = m->thread_started && !m->thread_exited;
	stats->in_refresh = m->in_refresh;
	stats->tripped = m->tripped;
	pthread_mutex_unlock(&m->lock);
}
