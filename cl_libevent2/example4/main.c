/*
 * cl_libevent2/example4/main.c
 *
 * Multi-event-base usage of the Citrusleaf libevent2 client.
 *
 * This example uses the same transaction event base and thread model as
 * example3, but it runs indefinitely and may be used for load testing.
 *
 * The main steps are:
 *	- Initialize database cluster management.
 *	- Create some event bases and run their event loops in dedicated threads.
 *	- Write lots of simple records to the database using all these event bases.
 *	- Continuously read or overwrite random records, using all the bases.
 *	- On Ctrl-C, stop initiating transactions and shut down cleanly.
 */


//==========================================================
// Includes
//

#include <execinfo.h>
#include <getopt.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <bits/time.h>
#include <bits/types.h>
#include <event2/event.h>

#include "citrusleaf_event2/ev2citrusleaf.h"


//==========================================================
// Local Logging Macros
//

#define LOG(_fmt, _args...) { printf(_fmt "\n", ## _args); fflush(stdout); }

#ifdef SHOW_DETAIL
#define DETAIL(_fmt, _args...) { printf(_fmt "\n", ## _args); fflush(stdout); }
#else
#define DETAIL(_fmt, _args...)
#endif


//==========================================================
// Constants
//

const char DEFAULT_HOST[] = "127.0.0.1";
const int DEFAULT_PORT = 3000;
const char DEFAULT_NAMESPACE[] = "test";
const char DEFAULT_SET[] = "test-set";
const int DEFAULT_TRIGGER_USEC = 500;
const int DEFAULT_TIMEOUT_MSEC = 10;
const int DEFAULT_NUM_BASES = 16;
const int DEFAULT_NUM_KEYS = 1000 * 16;
const int DEFAULT_READ_PERCENT = 80;
const int DEFAULT_VALUE_SIZE = 1300;

const char BIN_NAME[] = "test-bin-name";

const int CLUSTER_VERIFY_TRIES = 5;
const __useconds_t CLUSTER_VERIFY_INTERVAL = 1000 * 1000; // 1 second

const int REPORT_INTERVAL_SEC = 3;


//==========================================================
// Typedefs
//

typedef struct config_s {
	const char* p_host;
	int port;
	const char* p_namespace;
	const char* p_set;
	int trigger_usec;
	int timeout_msec;
	int num_bases;
	int num_keys;
	int read_percent;
	int value_size;
} config;

typedef enum {
	INSERT,
	MIXED
} base_phase;

typedef struct base_s {
	pthread_t thread;
	struct event_base* p_event_base;
	struct event* p_trigger_event;
	base_phase trigger_phase;
	int trigger_k;
	uint64_t num_puts;
	uint64_t num_put_timeouts;
	uint64_t num_gets;
	uint64_t num_get_timeouts;
	uint64_t num_not_found;
} base;


//==========================================================
// Globals
//

static config g_config;
static ev2citrusleaf_cluster* g_p_cluster = NULL;
static base* g_bases = NULL;
static ev2citrusleaf_object* g_keys = NULL;
static ev2citrusleaf_write_parameters g_write_parameters;
static uint32_t g_running;


//==========================================================
// Forward Declarations
//

static bool set_config();
static void destroy_config();
static void usage();
static bool start_cluster_management();
static void stop_cluster_management();
static void start_transactions();
static void block_until_transactions_done();
static void* run_event_loop(void* pv_b);
static void log_statistics();
static void trigger_cb(int fd, short event, void* pv_udata);
static bool put(int b, int k);
static void put_cb(int return_value, ev2citrusleaf_bin* bins, int n_bins,
		uint32_t generation, uint32_t expiration, void* pv_udata);
static bool get(int b, int k);
static void get_cb(int return_value, ev2citrusleaf_bin* bins, int n_bins,
		uint32_t generation, uint32_t expiration, void* pv_udata);
static void validate_data(int b, int k, ev2citrusleaf_bin* bins, int n_bins);
static void sig_handle_segv(int sig_num);
static void sig_handle_user_stop(int sig_num);


//==========================================================
// Main
//

int
main(int argc, char* argv[])
{
	g_running = true;

	// Register signal handlers.
	signal(SIGSEGV, sig_handle_segv);
	signal(SIGTERM, sig_handle_user_stop);
	signal(SIGINT, sig_handle_user_stop);

	// Parse command line arguments.
	if (! set_config(argc, argv)) {
		exit(-1);
	}

	// Use default Citrusleaf client logging, but set a filter.
	cf_set_log_level(CF_WARN);

	// Connect to the database server cluster.
	if (! start_cluster_management()) {
		stop_cluster_management();
		destroy_config();
		exit(-1);
	}

	// Set up an array of event bases and thread IDs on the stack.
	base bases[g_config.num_bases];

	// Make these globally available.
	g_bases = bases;

	// Set up an array of record keys on the stack.
	ev2citrusleaf_object keys[g_config.num_keys];

	// Make these globally available.
	g_keys = keys;

	// Start transactions on all the bases.
	start_transactions();

	// Wait for everything to finish, then exit cleanly.
	block_until_transactions_done();
	stop_cluster_management();
	destroy_config();

	LOG("example4 is done");

	return 0;
}


//==========================================================
// Command Line Options
//

//------------------------------------------------
// Parse command line options.
//
static bool
set_config(int argc, char* argv[])
{
	g_config.p_host = DEFAULT_HOST;
	g_config.port = DEFAULT_PORT;
	g_config.p_namespace = DEFAULT_NAMESPACE;
	g_config.p_set = DEFAULT_SET;
	g_config.trigger_usec = DEFAULT_TRIGGER_USEC;
	g_config.timeout_msec = DEFAULT_TIMEOUT_MSEC;
	g_config.num_bases = DEFAULT_NUM_BASES;
	g_config.num_keys = DEFAULT_NUM_KEYS;
	g_config.read_percent = DEFAULT_READ_PERCENT;
	g_config.value_size = DEFAULT_VALUE_SIZE;

	int c;

	while ((c = getopt(argc, argv, "h:p:n:s:u:m:b:k:r:x:")) != -1) {
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

		case 'u':
			g_config.trigger_usec = atoi(optarg);
			break;

		case 'm':
			g_config.timeout_msec = atoi(optarg);
			break;

		case 'b':
			g_config.num_bases = atoi(optarg);
			break;

		case 'k':
			g_config.num_keys = atoi(optarg);
			break;

		case 'r':
			g_config.read_percent = atoi(optarg);
			break;

		case 'x':
			g_config.value_size = atoi(optarg);
			break;

		default:
			destroy_config();
			usage();
			return false;
		}
	}

	LOG("host:                %s", g_config.p_host);
	LOG("port:                %d", g_config.port);
	LOG("namespace:           %s", g_config.p_namespace);
	LOG("set name:            %s", g_config.p_set);
	LOG("transaction trigger: every %d usec", g_config.trigger_usec);
	LOG("transaction timeout: %d msec", g_config.timeout_msec);
	LOG("number of bases:     %d", g_config.num_bases);
	LOG("number of keys:      %d", g_config.num_keys);
	LOG("read percent:        %d", g_config.read_percent);
	LOG("value size:          %d bytes", g_config.value_size);

	return true;
}

//------------------------------------------------
// Free any resources allocated for configuration.
//
static void
destroy_config()
{
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

//------------------------------------------------
// Display supported command line options.
//
static void
usage()
{
	LOG("Usage:");
	LOG("-h host [default: %s]", DEFAULT_HOST);
	LOG("-p port [default: %d]", DEFAULT_PORT);
	LOG("-n namespace [default: %s]", DEFAULT_NAMESPACE);
	LOG("-s set name [default: %s]", DEFAULT_SET);
	LOG("-u transaction trigger usec [default: %d]", DEFAULT_TRIGGER_USEC);
	LOG("-m transaction timeout msec [default: %d]", DEFAULT_TIMEOUT_MSEC);
	LOG("-b number of bases [default: %d]", DEFAULT_NUM_BASES);
	LOG("-k number of keys [default: %d]", DEFAULT_NUM_KEYS);
	LOG("-r read percent [default: %d]", DEFAULT_READ_PERCENT);
	LOG("-x value size in bytes [default: %d]", DEFAULT_VALUE_SIZE);
}


//==========================================================
// Cluster Management
//

//------------------------------------------------
// Initialize client and connect to database.
//
static bool
start_cluster_management()
{
	// Initialize Citrusleaf client.
	int result = ev2citrusleaf_init(NULL);

	if (result != 0) {
		LOG("ERROR: initializing cluster [%d]", result);
		return false;
	}

	// Create cluster object needed for all database operations.
	g_p_cluster = ev2citrusleaf_cluster_create(NULL, NULL);

	if (! g_p_cluster) {
		LOG("ERROR: creating cluster");
		return false;
	}

	// Connect to Citrusleaf database server cluster.
	result = ev2citrusleaf_cluster_add_host(g_p_cluster, (char*)g_config.p_host,
			g_config.port);

	if (result != 0) {
		LOG("ERROR: adding host [%d]", result);
		return false;
	}

	// Verify database server cluster is ready.
	int tries = 0;
	int n_prev = 0;

	while (tries < CLUSTER_VERIFY_TRIES) {
		int n = ev2citrusleaf_cluster_get_active_node_count(g_p_cluster);

		if (n > 0 && n == n_prev) {
			LOG("found %d cluster node%s", n, n > 1 ? "s" : "");
			return true;
		}

		usleep(CLUSTER_VERIFY_INTERVAL);
		tries++;
		n_prev = n;
	}

	LOG("ERROR: connecting to cluster");
	return false;
}

//------------------------------------------------
// Disconnect from database and clean up client.
//
static void
stop_cluster_management()
{
	if (g_p_cluster) {
		ev2citrusleaf_cluster_destroy(g_p_cluster);
	}

	ev2citrusleaf_shutdown(true);
}


//==========================================================
// Transaction Management
//

//------------------------------------------------
// Start transaction threads and event loops.
//
static void
start_transactions()
{
	// Seed the random number generator (used in the mixed phase).
	srand(time(NULL));

	// Initialize the record keys.
	for (int k = 0; k < g_config.num_keys; k++) {
		ev2citrusleaf_object_init_int(&g_keys[k], (int64_t)k);
	}

	// Initialize (default) write parameters - used by every put transaction.
	ev2citrusleaf_write_parameters_init(&g_write_parameters);

	// Start all the transaction threads. If any thread or event loop fails to
	// get rolling, just continue with the others.

	memset(g_bases, 0, sizeof(base) * g_config.num_bases);

	for (int b = 0; b < g_config.num_bases; b++) {
		if (pthread_create(&g_bases[b].thread, NULL, run_event_loop,
				(void*)(uint64_t)b) != 0) {
			LOG("ERROR: creating event base thread %d", b);
		}
	}
}

//------------------------------------------------
// Wait for all transaction event loops to exit.
//
static void
block_until_transactions_done()
{
	// Use the main thread for reporting.
	while (g_running) {
		if (0 == sleep(REPORT_INTERVAL_SEC)) {
			log_statistics();
		}
	}

	// Make sure all transaction threads are done.
	for (int b = 0; b < g_config.num_bases; b++) {
		pthread_join(g_bases[b].thread, NULL);
	}

	LOG("example4 transactions done");
	log_statistics();
}

//------------------------------------------------
// One event loop runs in each transaction thread.
//
static void*
run_event_loop(void* pv_b)
{
	int b = (int)(uint64_t)pv_b;

	// Create the event base.
	if ((g_bases[b].p_event_base = event_base_new()) == NULL) {
		LOG("ERROR: creating event base %d", b);
		return NULL;
	}

	// Create the trigger timer event, which is used to initiate transactions.
	if ((g_bases[b].p_trigger_event =
			evtimer_new(g_bases[b].p_event_base, trigger_cb, pv_b)) == NULL) {
		LOG("ERROR: creating event trigger event for base %d", b);
		event_base_free(g_bases[b].p_event_base);
		return NULL;
	}

	// During the insertion phase, every base uses k = b for its first
	// transaction, then keeps advancing k by adding the number of bases N. That
	// way, each base will cover:
	//		k = b + (N * i), for i = 0, 1, 2, 3...
	// and so together all the bases will cover all the keys.

	g_bases[b].trigger_phase = INSERT;
	g_bases[b].trigger_k = b;

	// Start the event loop. There must be an event added on the base before
	// calling event_base_dispatch(), or the event loop will just exit. Here we
	// add the trigger timer event.

	struct timeval trigger_timeval = { 0, 1 };

	if (evtimer_add(g_bases[b].p_trigger_event, &trigger_timeval) == 0) {

		// event_base_dispatch() will block and run the event loop until no more
		// events are added, or until something calls event_base_loopbreak() or
		// event_base_loopexit().

		// To keep an event loop running, an application must therefore ensure
		// at least one event is always added.

		// In this example's "non-serialized" transaction model, we'll exit the
		// loop when a trigger event callback is made in which we don't re-add
		// the trigger event, and all in-progress transactions are completed.

		if (event_base_dispatch(g_bases[b].p_event_base) < 0) {
			LOG("ERROR: event base %d dispatch", b);
		}
	}
	else {
		LOG("ERROR: adding timer on event base %d", b);
	}

	// Free the trigger timer event and event base.
	event_free(g_bases[b].p_trigger_event);
	event_base_free(g_bases[b].p_event_base);

	return NULL;
}

//------------------------------------------------
// Print the interesting totals.
//
static void
log_statistics()
{
	uint64_t total_puts = 0;
	uint64_t total_put_timeouts = 0;
	uint64_t total_gets = 0;
	uint64_t total_get_timeouts = 0;
	uint64_t total_not_found = 0;
	bool started_mixed_phase = false;

	// AKG - a base object's statistics members are always modified on the same
	// thread (that running the base's event loop). Although we read them here
	// on the reporting (main) thread, I didn't bother making them atomic.

	for (int b = 0; b < g_config.num_bases; b++) {
		total_puts += g_bases[b].num_puts;
		total_put_timeouts += g_bases[b].num_put_timeouts;

		if (g_bases[b].trigger_phase == MIXED) {
			total_gets += g_bases[b].num_gets;
			total_get_timeouts += g_bases[b].num_get_timeouts;
			total_not_found += g_bases[b].num_not_found;

			// As long as any base is doing gets, count and log them.
			started_mixed_phase = true;
		}
	}

	if (started_mixed_phase) {
		LOG("total puts: %lu [%lu timeouts]; "
			"total gets: %lu [%lu timeouts, %lu not found]",
			total_puts, total_put_timeouts,
			total_gets, total_get_timeouts, total_not_found);
	}
	else {
		LOG("total puts: %lu [%lu timeouts]", total_puts, total_put_timeouts);
	}
}


//==========================================================
// Transaction Triggering
//

//------------------------------------------------
// Trigger event callback starts all transactions.
//
static void
trigger_cb(int fd, short event, void* pv_udata)
{
	if (! g_running) {
		// We were stopped - don't initiate any more transactions.
		return;
	}

	int b = (int)(uint64_t)pv_udata;

	// Continue insertion (write-only) phase.
	if (g_bases[b].trigger_phase == INSERT) {
		if (! put(b, g_bases[b].trigger_k)) {
			// Will exit event loop if put call failed.
			return;
		}

		// Advance the key index.
		g_bases[b].trigger_k += g_config.num_bases;

		// Check whether insertion phase is done.
		if (g_bases[b].trigger_k >= g_config.num_keys) {
			LOG("base %2d - done insertions [%lu timeouts]", b,
					g_bases[b].num_put_timeouts);

			// Done with insertion phase on this base, start mixed phase.
			g_bases[b].trigger_phase = MIXED;
			g_bases[b].num_puts = 0;
			g_bases[b].num_put_timeouts = 0;
		}
	}
	// Continue mixed (read or write) phase until user stops run.
	else {
		int r = rand();

		// Randomly choose a key, and determine whether to read or write.
		int k = r % g_config.num_keys;
		bool is_get = (r >> 16) % 100 < g_config.read_percent;

		if (! (is_get ? get(b, k) : put(b, k))) {
			// Will exit event loop if get/put call failed.
			return;
		}
	}

	// Re-add the trigger timer event. Its callback (this function) will start
	// the next transaction, independent of when the current one is completed.

	struct timeval trigger_timeval = { 0, g_config.trigger_usec };

	if (evtimer_add(g_bases[b].p_trigger_event, &trigger_timeval) != 0) {
		LOG("ERROR: adding timer on event base %d, to trigger key %d", b,
				g_bases[b].trigger_k);
		// Will exit event loop if timer add failed.
	}
}


//==========================================================
// Transaction Operations
//

//------------------------------------------------
// Start a database write operation.
//
static bool
put(int b, int k)
{
	// Store base index and key index in user-data field.
	uint64_t udata = ((uint64_t)b << 32) | (uint64_t)k;

	// Write just one bin per record.
	ev2citrusleaf_bin bin;

	// Always the same bin name.
	strcpy(bin.bin_name, BIN_NAME);

	// Always the same size blob-type value, with key index embedded.
	char value[g_config.value_size];

	*(uint64_t*)value = (uint64_t)k;
	ev2citrusleaf_object_init_blob(&bin.object, (void*)value,
			(size_t)g_config.value_size);

	if (0 != ev2citrusleaf_put(
			g_p_cluster,					// cluster
			(char*)g_config.p_namespace,	// namespace
			(char*)g_config.p_set,			// set name
			&g_keys[k],						// key of record to write
			&bin,							// bin (array) to write
			1,								// just one bin for us
			&g_write_parameters,			// write parameters
			g_config.timeout_msec,			// transaction timeout
			put_cb,							// callback for this transaction
			(void*)udata,					// "user data" - here, b and k
			g_bases[b].p_event_base)) {		// event base for this transaction
		LOG("ERROR: put(), base %2d, key %d", b, k);
		return false;
	}

	return true;
}

//------------------------------------------------
// Complete a database write operation.
//
static void
put_cb(int return_value, ev2citrusleaf_bin* bins, int n_bins,
		uint32_t generation, uint32_t expiration, void* pv_udata)
{
	int k = (int)((uint64_t)pv_udata & 0xFFFF);
	int b = (int)((uint64_t)pv_udata >> 32);

	g_bases[b].num_puts++;

	switch (return_value) {
	case EV2CITRUSLEAF_OK:
		break;

	case EV2CITRUSLEAF_FAIL_TIMEOUT:
		DETAIL("PUT TIMEOUT: base %2d, key %d", b, k);
		g_bases[b].num_put_timeouts++;
		// Otherwise ok... Likely leads to EV2CITRUSLEAF_FAIL_NOTFOUND on get.
		break;

	default:
		LOG("ERROR: return-value %d, base %2d, key %d", return_value, b, k);
		// Won't exit event loop...
		break;
	}
}

//------------------------------------------------
// Start a database read operation.
//
static bool
get(int b, int k)
{
	// Store base index and key index in user-data field.
	uint64_t udata = ((uint64_t)b << 32) | (uint64_t)k;

	if (0 != ev2citrusleaf_get_all(
			g_p_cluster,					// cluster
			(char*)g_config.p_namespace,	// namespace
			(char*)g_config.p_set,			// set name
			&g_keys[k],						// key of record to get
			g_config.timeout_msec,			// transaction timeout
			get_cb,							// callback for this transaction
			(void*)udata,					// "user data" - here, b and k
			g_bases[b].p_event_base)) {		// event base for this transaction
		LOG("ERROR: get(), base %2d, key %d", b, k);
		return false;
	}

	return true;
}

//------------------------------------------------
// Complete a database read operation.
//
static void
get_cb(int return_value, ev2citrusleaf_bin* bins, int n_bins,
		uint32_t generation, uint32_t expiration, void* pv_udata)
{
	int k = (int)((uint64_t)pv_udata & 0xFFFF);
	int b = (int)((uint64_t)pv_udata >> 32);

	g_bases[b].num_gets++;

	switch (return_value) {
	case EV2CITRUSLEAF_OK:
		// Not 100% sure we only get bins if return value is OK. TODO - check.
		validate_data(b, k, bins, n_bins);
		// Invalid data will log complaints, but won't exit event loop.
		break;

	case EV2CITRUSLEAF_FAIL_TIMEOUT:
		DETAIL("GET TIMEOUT: base %2d, key %d", b, k);
		g_bases[b].num_get_timeouts++;
		// Otherwise ok...
		break;

	case EV2CITRUSLEAF_FAIL_NOTFOUND:
		DETAIL("NOT FOUND: base %2d, key %d", b, k);
		g_bases[b].num_not_found++;
		// Otherwise ok...
		break;

	default:
		LOG("ERROR: return-value %d, base %2d, key %d", return_value, b, k);
		// Won't exit event loop.
		break;
	}
}

//------------------------------------------------
// Validate bin data read from database.
//
static void
validate_data(int b, int k, ev2citrusleaf_bin* bins, int n_bins)
{
	if (! bins) {
		LOG("ERROR: base %2d, key %d, no bin data with return value OK", b, k);
		return;
	}

	if (n_bins != 1) {
		LOG("ERROR: base %2d, key %d, got unexpected n_bins %d", b, k, n_bins);
	}
	else if (strcmp(bins[0].bin_name, BIN_NAME) != 0) {
		LOG("ERROR: base %2d, key %d, got unexpected bin name %s", b, k,
				bins[0].bin_name);
	}
	else if (bins[0].object.type != CL_BLOB) {
		LOG("ERROR: base %2d, key %d, got unexpected data type %d", b, k,
				bins[0].object.type);
	}
	else if (bins[0].object.size != (size_t)g_config.value_size) {
		LOG("ERROR: base %2d, key %d, got unexpected data size %lu", b, k,
				bins[0].object.size);
	}
	else if (*(uint64_t*)bins[0].object.u.blob != (int64_t)k) {
		LOG("ERROR: base %2d, key %d, got unexpected data value %lu", b, k,
				*(uint64_t*)bins[0].object.u.blob);
	}

	// Bins with integer data type don't need this, but it's good form.
	ev2citrusleaf_bins_free(bins, n_bins);
}


//==========================================================
// Signal Handlers
//

//------------------------------------------------
// Show the call stack if we get a segv.
//
static void
sig_handle_segv(int sig_num)
{
	LOG("signal SEGV received");

	void* bt[50];
	uint sz = backtrace(bt, 50);

	char** strings = backtrace_symbols(bt, sz);

	for (int i = 0; i < sz; ++i) {
		LOG("stacktrace: frame %d: %s", i, strings[i]);
	}

	free(strings);

	_exit(-1);
}

//------------------------------------------------
// Stop cleanly.
//
static void
sig_handle_user_stop(int sig_num)
{
	// Either INT or TERM will work.
	g_running = false;
}
