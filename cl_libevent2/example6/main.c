/*
 * cl_libevent2/example6/main.c
 *
 * Simple multi-event-base usage of the Citrusleaf libevent2 client.
 */


//==========================================================
// Includes
//

#include <errno.h>
#include <execinfo.h>	// for debugging
#include <getopt.h>
#include <pthread.h>
#include <signal.h>		// for debugging
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <event2/dns.h>
#include <event2/event.h>

#include "citrusleaf/cf_clock.h"
#include "citrusleaf_event2/ev2citrusleaf.h"


//==========================================================
// Constants
//

const char DEFAULT_HOST[] = "127.0.0.1";
const int DEFAULT_PORT = 3000;
const char DEFAULT_NAMESPACE[] = "rwtest";
const char DEFAULT_SET[] = "set";
const int DEFAULT_TIMEOUT_MS = 10;
const int DEFAULT_NUM_BASES = 16;
const int DEFAULT_KEYS_PER_BASE = 1000;
const int DEFAULT_EXTRA_LAPS = 10;
const int DEFAULT_WRITE_PCT = 20;

const char BIN_NAME[] = "test-bin-name";
const char BIN_DATA[] = "test-object";

#define TRANS_FMT "base %2d, lap %d, op-count %d, key %d"
#define TRANS_PARAMS(_b, _k) _b, g_bases[_b].lap, g_bases[_b].op_count, _k


//==========================================================
// Typedefs
//

typedef struct _config {
	const char* p_host;
	int port;
	const char* p_namespace;
	const char* p_set;
	int timeout_ms;
	int num_bases;
	int keys_per_base;
	int extra_laps;
	int write_pct;
} config;

typedef struct _cluster_mgr {
	ev2citrusleaf_cluster* p_cluster;
	struct event_base* p_event_base;
	pthread_t thread;
} cluster_mgr;

typedef struct _base {
	struct event_base* p_event_base;
	pthread_t thread;
	int lap;
	int op_count;
	int num_timeouts;
	int num_not_found;
} base;

typedef struct _key {
	ev2citrusleaf_object obj;
	char str[64];
} key;


//==========================================================
// Globals
//

static config g_config;
static cluster_mgr g_cluster_mgr;
static base* g_bases = NULL;
static key* g_keys = NULL;
static int g_total_keys;
static ev2citrusleaf_bin g_bin;
static ev2citrusleaf_write_parameters g_write_parameters;
static uint64_t g_start_ms;


//==========================================================
// Forward Declarations
//

static void* run_cluster_mgr(void* pv_unused);
static void* run_transactions(void* pv_b);
static void transaction_cb(int return_value, ev2citrusleaf_bin* bins,
	int n_bins, uint32_t generation, void* pv_udata);
static bool get(int b, int k);
static bool put(int b, int k);
static void init_lock_cbs(ev2citrusleaf_lock_callbacks* p_lock_cbs);
static void* mutex_alloc();
static void mutex_free(void* pv_lock);
static int mutex_lock(void* pv_lock);
static int mutex_unlock(void* pv_lock);
static bool begin_cluster_mgr();
static bool begin_transactions();
static void block_until_transactions_done();
static void cleanup();
static void destroy_config();
static void init_keys();
static void init_value();
static void* pack_user_data(int b, int k);
static void parse_user_data(void* pv_udata, int* p_b, int* p_k);
static bool set_config();
static void usage();
static void as_sig_handle_segv(int sig_num);
static void as_sig_handle_term(int sig_num);


//==========================================================
// Main
//

int main(int argc, char* argv[]) {
	signal(SIGSEGV, as_sig_handle_segv);
	signal(SIGTERM , as_sig_handle_term);

	if (! set_config(argc, argv)) {
		exit(-1);
	}

	srand(time(NULL));

	key keys[g_total_keys];

	g_keys = keys;

	init_keys();
	init_value();
	ev2citrusleaf_write_parameters_init(&g_write_parameters);

	ev2citrusleaf_lock_callbacks lock_cbs;

	init_lock_cbs(&lock_cbs);
	ev2citrusleaf_init(&lock_cbs);

	if (! begin_cluster_mgr()) {
		cleanup();
		exit(-1);
	}

	base bases[g_config.num_bases];

	g_bases = bases;

	if (! begin_transactions()) {
		cleanup();
		exit(-1);
	}

	block_until_transactions_done();
	cleanup();

	return 0;
}


//==========================================================
// Thread "Run" Functions
//

static void* run_cluster_mgr(void* pv_unused) {
	event_base_dispatch(g_cluster_mgr.p_event_base);

	return NULL;
}

static void* run_transactions(void* pv_b) {
	int b = (int)(uint64_t)pv_b;

	// There's always an insertion lap - start it here.
	if (put(b, b * g_config.keys_per_base)) {
		event_base_dispatch(g_bases[b].p_event_base);
	}

	return NULL;
}


//==========================================================
// Citrusleaf Client Callback Functions
//

static void transaction_cb(int return_value, ev2citrusleaf_bin* bins,
		int n_bins, uint32_t generation, void* pv_udata) {
	int b;
	int k;

	parse_user_data(pv_udata, &b, &k);

	if (bins) {
		ev2citrusleaf_bins_free(bins, n_bins);
	}

	switch (return_value) {
	case EV2CITRUSLEAF_OK:
		break;

	case EV2CITRUSLEAF_FAIL_TIMEOUT:
//		fprintf(stdout, "TIMEOUT: " TRANS_FMT "\n", TRANS_PARAMS(b, k));
//		fflush(stdout);
		g_bases[b].num_timeouts++;
		// Otherwise ok...
		break;

	case EV2CITRUSLEAF_FAIL_NOTFOUND:
//		fprintf(stdout, "NOT FOUND: " TRANS_FMT "\n", TRANS_PARAMS(b, k));
//		fflush(stdout);
		g_bases[b].num_not_found++;
		// Otherwise ok...
		break;

	default:
		fprintf(stdout, "ERROR: return-value %d, " TRANS_FMT "\n", return_value,
			TRANS_PARAMS(b, k));
		// Will exit dispatch loop.
		return;
	}

	if (++g_bases[b].op_count >= g_config.keys_per_base) {
		if (++g_bases[b].lap > g_config.extra_laps) {
			fprintf(stdout, "base %2d - done [timeouts %d, not-found %d]\n", b,
				g_bases[b].num_timeouts, g_bases[b].num_not_found);
			fflush(stdout);
			// Will exit dispatch loop.
			return;
		}

		g_bases[b].op_count = 0;

		fprintf(stdout, "base %2d - lap %d\n", b, g_bases[b].lap);
		fflush(stdout);
	}

	bool is_put = true;

	if (g_bases[b].lap == 0) {
		// Lap 0 sequentially inserts every key in this base's key range.
		k = (b * g_config.keys_per_base) + g_bases[b].op_count;
	}
	else {
		// Extra laps are reads and writes, in the ratio specified by config.
		// The key is randomly selected from among this base's key range.

		int r = rand();

		k = (b * g_config.keys_per_base) + (r % g_config.keys_per_base);
		is_put = (r >> 16) % 100 < g_config.write_pct;
	}

	is_put ? put(b, k) : get(b, k);
	// Will exit dispatch loop if put/get call failed.
}


//==========================================================
// Transaction Operations
//

static bool get(int b, int k) {
	if (ev2citrusleaf_get_all(g_cluster_mgr.p_cluster,
			(char*)g_config.p_namespace, (char*)g_config.p_set, &g_keys[k].obj,
			g_config.timeout_ms, transaction_cb, pack_user_data(b, k),
			g_bases[b].p_event_base)) {
		fprintf(stdout, "ERROR: get(), " TRANS_FMT "\n", TRANS_PARAMS(b, k));
		return false;
	}

	return true;
}

static bool put(int b, int k) {
	if (ev2citrusleaf_put(g_cluster_mgr.p_cluster, (char*)g_config.p_namespace,
			(char*)g_config.p_set, &g_keys[k].obj, &g_bin, 1,
			&g_write_parameters, g_config.timeout_ms, transaction_cb,
			pack_user_data(b, k), g_bases[b].p_event_base)) {
		fprintf(stdout, "ERROR: put(), " TRANS_FMT "\n", TRANS_PARAMS(b, k));
		return false;
	}

	return true;
}


//==========================================================
// Mutex Callbacks
//

static void init_lock_cbs(ev2citrusleaf_lock_callbacks* p_lock_cbs) {
	p_lock_cbs->alloc = mutex_alloc;
	p_lock_cbs->free = mutex_free;
	p_lock_cbs->lock = mutex_lock;
	p_lock_cbs->unlock = mutex_unlock;
}

static void* mutex_alloc() {
	pthread_mutex_t* p_lock = malloc(sizeof(pthread_mutex_t));

	return p_lock && pthread_mutex_init(p_lock, NULL) == 0 ?
		(void*)p_lock : NULL;
}

static void mutex_free(void* pv_lock) {
	pthread_mutex_destroy((pthread_mutex_t*)pv_lock);
	free(pv_lock);
}

static int mutex_lock(void* pv_lock) {
	return pthread_mutex_lock((pthread_mutex_t*)pv_lock);
}

static int mutex_unlock(void* pv_lock) {
	return pthread_mutex_unlock((pthread_mutex_t*)pv_lock);
}


//==========================================================
// Helpers
//

static bool begin_cluster_mgr() {
	memset(&g_cluster_mgr, 0, sizeof(cluster_mgr));
	g_cluster_mgr.p_event_base = event_base_new();

	if (! g_cluster_mgr.p_event_base) {
		fprintf(stdout, "ERROR: creating cluster event base\n");
		return false;
	}

	g_cluster_mgr.p_cluster =
		ev2citrusleaf_cluster_create(g_cluster_mgr.p_event_base);

	if (! g_cluster_mgr.p_cluster) {
		fprintf(stdout, "ERROR: creating cluster\n");
		return false;
	}

	ev2citrusleaf_cluster_add_host(g_cluster_mgr.p_cluster,
		(char*)g_config.p_host, g_config.port);

	if (pthread_create(&g_cluster_mgr.thread, NULL, run_cluster_mgr, NULL)) {
		fprintf(stdout, "ERROR: creating cluster manager thread\n");
		return false;
	}

	return true;
}

static bool begin_transactions() {
	memset(g_bases, 0, sizeof(base) * g_config.num_bases);

	for (int b = 0; b < g_config.num_bases; b++) {
		if ((g_bases[b].p_event_base = event_base_new()) == NULL) {
			fprintf(stdout, "ERROR: creating event base %d\n", b);
			return false;
		}
	}

	g_start_ms = cf_getms();

	for (int b = 0; b < g_config.num_bases; b++) {
		if (pthread_create(&g_bases[b].thread, NULL, run_transactions,
				(void*)(uint64_t)b)) {
			fprintf(stdout, "ERROR: creating thread %d\n", b);
			return false;
		}
	}

	return true;
}

static void block_until_transactions_done() {
	void* pv_value;
	int total_timeouts = 0;

	for (int b = 0; b < g_config.num_bases; b++) {
		pthread_join(g_bases[b].thread, &pv_value);
		event_base_free(g_bases[b].p_event_base);
		g_bases[b].p_event_base = NULL;

		total_timeouts += g_bases[b].num_timeouts;
	}

	int total_transactions = g_total_keys * (g_config.extra_laps + 1);
	uint64_t elapsed_ms = cf_getms() - g_start_ms;

	fprintf(stdout, "elapsed-ms %" PRIu64 ", tps %" PRIu64 "\n", elapsed_ms,
		((uint64_t)total_transactions * 1000) / elapsed_ms);
	fprintf(stdout, "timeouts %d, timeout-percent %.2lf\n", total_timeouts,
		(double)(total_timeouts * 100) / (double)total_transactions);
}

static void cleanup() {
	void* pv_value;

	if (g_bases) {
		for (int b = 0; b < g_config.num_bases; b++) {
			if (g_bases[b].p_event_base) {
				// Is it ok to assume 0 is not a valid pthread ID?
				if (g_bases[b].thread) {
					// This is not rigorous - it will leave transactions in
					// progress, causing ev2citrusleaf_cluster_destroy() to
					// leak transaction resources. TODO - deal with this!
					event_base_loopbreak(g_bases[b].p_event_base);
					pthread_join(g_bases[b].thread, &pv_value);
				}

				event_base_free(g_bases[b].p_event_base);
			}
		}
	}

	if (g_cluster_mgr.p_event_base) {
		// Is it ok to assume 0 is not a valid pthread ID?
		if (g_cluster_mgr.thread) {
			event_base_loopbreak(g_cluster_mgr.p_event_base);
			pthread_join(g_cluster_mgr.thread, &pv_value);
		}

		if (g_cluster_mgr.p_cluster) {
			ev2citrusleaf_cluster_destroy(g_cluster_mgr.p_cluster);
		}

		event_base_free(g_cluster_mgr.p_event_base);
	}

	ev2citrusleaf_shutdown(true);
	destroy_config();
}

static void destroy_config() {
	if (g_config.p_host != DEFAULT_HOST) {
		free((char*)g_config.p_host);
	}

	if (g_config.p_namespace != DEFAULT_NAMESPACE) {
		free((char*)g_config.p_namespace);
	}

	if (g_config.p_set != DEFAULT_SET) {
		free((char*)g_config.p_set);
	}
}

static void init_keys() {
	for (int i = 0; i < g_total_keys; i++) {
		sprintf(g_keys[i].str, "%063d", i);
		// Do keys have to be string types? Why this length?
//		fprintf(stdout, "key %6d: %s\n", i, g_keys[i].str);
		ev2citrusleaf_object_init_str(&g_keys[i].obj, g_keys[i].str);
	}
}

static void init_value() {
	strcpy(g_bin.bin_name, BIN_NAME);
	ev2citrusleaf_object_init_str(&g_bin.object, (char*)BIN_DATA);
}

static void* pack_user_data(int b, int k) {
	// Yes, we're assuming a void* is 64 bits...
	return (void*)(((uint64_t)b << 48) + (uint64_t)k);
}

static void parse_user_data(void* pv_udata, int* p_b, int* p_k) {
	*p_b = (int)((uint64_t)pv_udata >> 48);
	*p_k = (int)((uint64_t)pv_udata & 0xffffFFFFffff);
}

static bool set_config(int argc, char* argv[]) {
	g_config.p_host = DEFAULT_HOST;
	g_config.port = DEFAULT_PORT;
	g_config.p_namespace = DEFAULT_NAMESPACE;
	g_config.p_set = DEFAULT_SET;
	g_config.timeout_ms = DEFAULT_TIMEOUT_MS;
	g_config.num_bases = DEFAULT_NUM_BASES;
	g_config.keys_per_base = DEFAULT_KEYS_PER_BASE;
	g_config.extra_laps = DEFAULT_EXTRA_LAPS;
	g_config.write_pct = DEFAULT_WRITE_PCT;

	int c;

	while ((c = getopt(argc, argv, "h:p:n:s:m:b:k:x:w:")) != -1) {
		switch (c) {
		case 'h':
			g_config.p_host = strdup(optarg);
			break;

		case 'p':
			g_config.port = atoi(optarg);
			break;

		case 'n':
			g_config.p_namespace = strdup(optarg);
			break;

		case 's':
			g_config.p_set = strdup(optarg);
			break;

		case 'm':
			g_config.timeout_ms = atoi(optarg);
			break;

		case 'b':
			g_config.num_bases = atoi(optarg);
			break;

		case 'k':
			g_config.keys_per_base = atoi(optarg);
			break;

		case 'x':
			g_config.extra_laps = atoi(optarg);
			break;

		case 'w':
			g_config.write_pct = atoi(optarg);
			break;

		default:
			usage();
			return false;
		}
	}

	g_total_keys = g_config.num_bases * g_config.keys_per_base;

	fprintf(stderr, "example6:\n");
	fprintf(stderr, "host %s, port %d\n", g_config.p_host, g_config.port);
	fprintf(stderr, "namespace %s, set %s, timeout-ms %d\n",
		g_config.p_namespace, g_config.p_set, g_config.timeout_ms);
	fprintf(stderr, "num-bases %d, keys-per-base %d, total-keys %d\n",
		g_config.num_bases, g_config.keys_per_base, g_total_keys);
	fprintf(stderr, "extra-laps %d, write-pct %d\n", g_config.extra_laps,
		g_config.write_pct);

	return true;
}

static void usage() {
	fprintf(stderr, "Usage:\n");
	fprintf(stderr, "-h host [default: %s]\n", DEFAULT_HOST);
	fprintf(stderr, "-p port [default: %d]\n", DEFAULT_PORT);
	fprintf(stderr, "-n namespace [default: %s]\n", DEFAULT_NAMESPACE);
	fprintf(stderr, "-s set [default: %s]\n", DEFAULT_SET);
	fprintf(stderr, "-m timeout ms [default: %d]\n", DEFAULT_TIMEOUT_MS);
	fprintf(stderr, "-b number of bases [default: %d]\n", DEFAULT_NUM_BASES);
	fprintf(stderr, "-k keys per base [default: %d]\n", DEFAULT_KEYS_PER_BASE);
	fprintf(stderr, "-x extra laps [default: %d]\n", DEFAULT_EXTRA_LAPS);
	fprintf(stderr, "-w write percent [default: %d]\n", DEFAULT_WRITE_PCT);

	destroy_config();
}


//==========================================================
// Debugging Helpers
//

static void as_sig_handle_segv(int sig_num) {
	fprintf(stdout, "Signal SEGV received: stack trace\n");

	void* bt[50];
	uint sz = backtrace(bt, 50);
	
	char** strings = backtrace_symbols(bt, sz);

	for (int i = 0; i < sz; ++i) {
		fprintf(stdout, "stacktrace: frame %d: %s\n", i, strings[i]);
	}

	free(strings);
	
	fflush(stdout);
	_exit(-1);
}

static void as_sig_handle_term(int sig_num) {
	fprintf(stdout, "Signal TERM received, aborting\n");

  	void* bt[50];
	uint sz = backtrace(bt, 50);

	char** strings = backtrace_symbols(bt, sz);

	for (int i = 0; i < sz; ++i) {
		fprintf(stdout, "stacktrace: frame %d: %s\n", i, strings[i]);
	}

	free(strings);

	fflush(stdout);
	_exit(0);
}

