/*
 * cl_libevent2/example2/main.c
 *
 * Optional API features for the Citrusleaf libevent2 client.
 *
 * This example demonstrates some optional client API features. It shows:
 *	- Application control of client logging.
 *	- Application implementation of client mutex locks.
 *	- Application control of client's cluster management event base and thread.
 *
 * Otherwise, it is similar to the first example, doing a (shorter) series of
 * database operations on a single transaction thread and event base. For
 * simplicity there is no info query in this example.
 *
 * The main steps are:
 *	- Initialize database cluster management, using optional external controls.
 *	- Do a short series of database operations.
 *	- Clean up.
 */


//==========================================================
// Includes
//

#include <getopt.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
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
const int DEFAULT_TIMEOUT_MSEC = 200;

// Must correspond to cf_log_level values:
const char* LOG_PREFIXES[] = {
	"CL-CLIENT ERROR: ",
	"CL-CLIENT WARNING: ",
	"CL-CLIENT INFO: ",
	"CL-CLIENT DEBUG: ",
};

const int CLUSTER_VERIFY_TRIES = 5;
const __useconds_t CLUSTER_VERIFY_INTERVAL = 1000 * 1000; // 1 second

const char KEY_STRING[] = "test-key";


//==========================================================
// Typedefs
//

typedef struct config_s {
	const char* p_host;
	int port;
	const char* p_namespace;
	const char* p_set;
	int timeout_msec;
} config;

typedef bool (*phase_start_fn)();
typedef bool (*phase_complete_fn)(int return_value, ev2citrusleaf_bin* bins,
		int n_bins, void* pv_udata);


//==========================================================
// Globals
//

static config g_config;
static ev2citrusleaf_lock_callbacks g_lock_cbs;
static ev2citrusleaf_cluster* g_p_cluster = NULL;
static pthread_t g_cluster_mgr_thread = 0;
static struct event_base* g_p_cluster_mgr_event_base = NULL;
static struct event_base* g_p_event_base = NULL;
static ev2citrusleaf_object g_key;
static ev2citrusleaf_write_parameters g_write_parameters;
static int g_phase_index = 0;


//==========================================================
// Forward Declarations
//

static bool set_config();
static void destroy_config();
static void usage();
static void client_log_cb(cf_log_level level, const char* fmt_no_newline, ...);
static void* client_mutex_alloc();
static void client_mutex_free(void* pv_lock);
static int client_mutex_lock(void* pv_lock);
static int client_mutex_unlock(void* pv_lock);
static bool start_cluster_management();
static void stop_cluster_management();
static void* run_cluster_mgr_event_loop(void* pv_unused);
static void do_transactions();
static void client_cb(int return_value, ev2citrusleaf_bin* bins, int n_bins,
		uint32_t generation, uint32_t expiration, void* pv_udata);
static bool start_phase_1();
static bool start_phase_2();
static bool complete_phase_2(int return_value, ev2citrusleaf_bin* bins,
		int n_bins, void* pv_udata);
static bool start_phase_3();
static bool verify_return_value(int return_value, ev2citrusleaf_bin* bins,
		int n_bins, void* pv_udata);


//==========================================================
// Demonstration Phases
//

const phase_start_fn PHASE_START_FUNCTIONS[] = {
	start_phase_1,			// write a 2-bin record
	start_phase_2,			// read all bins of record
	start_phase_3,			// delete the record
	NULL
};

const phase_complete_fn PHASE_COMPLETE_FUNCTIONS[] = {
	verify_return_value,	// verify write success
	complete_phase_2,		// verify everything that was read
	verify_return_value		// verify delete success
};


//==========================================================
// Main
//

int
main(int argc, char* argv[])
{
	// Parse command line arguments.
	if (! set_config(argc, argv)) {
		exit(-1);
	}

	// Handle Citrusleaf client logging.
	cf_set_log_callback(client_log_cb);
	cf_set_log_level(CF_INFO);

	// Connect to the database server cluster.
	if (! start_cluster_management()) {
		stop_cluster_management();
		destroy_config();
		exit(-1);
	}

	// Do the series of database operations.
	do_transactions();

	// Exit cleanly.
	stop_cluster_management();
	destroy_config();

	LOG("example2 is done");

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
	g_config.timeout_msec = DEFAULT_TIMEOUT_MSEC;

	int c;

	while ((c = getopt(argc, argv, "h:p:n:s:m:")) != -1) {
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
			g_config.timeout_msec = atoi(optarg);
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
	LOG("transaction timeout: %d msec", g_config.timeout_msec);

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
	LOG("-m transaction timeout msec [default: %d]", DEFAULT_TIMEOUT_MSEC);
}


//==========================================================
// Client Logging - Optional
//

//------------------------------------------------
// Handle log statements from the client.
//
static void
client_log_cb(cf_log_level level, const char* fmt_no_newline, ...)
{
	if (level < 0 || level > CF_DEBUG) {
		LOG("ERROR: unrecognized client log level %d", level);
		level = CF_ERROR;
	}

	// Add a prefix to show it's a client log statement, and to show the level.
	const char* prefix = LOG_PREFIXES[level];

	// Final size is: prefix length + format parameter length + 1 (for newline)
	// + 1 (for null terminator).
	size_t prefix_len = strlen(prefix);
	size_t fmt_len = prefix_len + strlen(fmt_no_newline) + 1;
	char fmt[fmt_len + 1];

	strcpy(fmt, prefix);
	strcpy(fmt + prefix_len, fmt_no_newline);
	fmt[fmt_len - 1] = '\n';
	fmt[fmt_len] = 0;

	va_list ap;

	va_start(ap, fmt_no_newline);
	vfprintf(stdout, fmt, ap);
	va_end(ap);

	fflush(stdout);
}


//==========================================================
// Client Mutex Callbacks - Optional
//

//------------------------------------------------
// Allocate a mutex for the client.
//
static void*
client_mutex_alloc()
{
	pthread_mutex_t* p_lock = malloc(sizeof(pthread_mutex_t));

	return p_lock && pthread_mutex_init(p_lock, NULL) == 0 ?
		(void*)p_lock : NULL;
}

//------------------------------------------------
// Free a mutex for the client.
//
static void
client_mutex_free(void* pv_lock)
{
	pthread_mutex_destroy((pthread_mutex_t*)pv_lock);
	free(pv_lock);
}

//------------------------------------------------
// Lock a mutex for the client.
//
static int
client_mutex_lock(void* pv_lock)
{
	return pthread_mutex_lock((pthread_mutex_t*)pv_lock);
}

//------------------------------------------------
// Unlock a mutex for the client.
//
static int
client_mutex_unlock(void* pv_lock)
{
	return pthread_mutex_unlock((pthread_mutex_t*)pv_lock);
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
	// Set the mutex functions that the client will use.
	g_lock_cbs.alloc = client_mutex_alloc;
	g_lock_cbs.free = client_mutex_free;
	g_lock_cbs.lock = client_mutex_lock;
	g_lock_cbs.unlock = client_mutex_unlock;

	// Initialize Citrusleaf client.
	int result = ev2citrusleaf_init(&g_lock_cbs);

	if (result != 0) {
		LOG("ERROR: initializing cluster [%d]", result);
		return false;
	}

	// Create an event base for cluster management - optional.
	if ((g_p_cluster_mgr_event_base = event_base_new()) == NULL) {
		LOG("ERROR: creating cluster manager event base");
		return false;
	}

	// Create cluster object needed for all database operations.
	g_p_cluster = ev2citrusleaf_cluster_create(g_p_cluster_mgr_event_base,
			NULL);

	if (! g_p_cluster) {
		LOG("ERROR: creating cluster");
		return false;
	}

	// Start the cluster manager thread. Must be done after calling
	// ev2citrusleaf_cluster_create(), which adds events to the cluster manager
	// event base, ensuring the event loop will not exit when we run it.
	if (pthread_create(&g_cluster_mgr_thread, NULL, run_cluster_mgr_event_loop,
			NULL) != 0) {
		LOG("ERROR: creating cluster manager thread");
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
	if (g_p_cluster_mgr_event_base) {
		// Break from cluster manager event loop (ok if it's not running).
		event_base_loopbreak(g_p_cluster_mgr_event_base);

		// Make sure the cluster manager thread is done.
		pthread_join(g_cluster_mgr_thread, NULL);

		// AKG - I find that pthread_join() seg-faults if an invalid pthread_t
		// is passed, though the Linux docs say it should just return ESRCH.
		// However to keep all these examples simple, I won't bother flagging
		// and checking that threads start successfully.

		// Must be done after breaking from the event loop.
		if (g_p_cluster) {
			ev2citrusleaf_cluster_destroy(g_p_cluster);
		}

		// Must be done after calling ev2citrusleaf_cluster_destroy(), which
		// single-steps the base's event loop to clean up in-progress cluster
		// management events.
		event_base_free(g_p_cluster_mgr_event_base);
	}

	ev2citrusleaf_shutdown(true);
}

//------------------------------------------------
// Run the client cluster manager event loop.
//
static void*
run_cluster_mgr_event_loop(void* pv_unused)
{
	// event_base_dispatch() will block and run the event loop until no more
	// events are added, or until something calls event_base_loopbreak() or
	// event_base_loopexit().

	// A timer event is added in ev2citrusleaf_cluster_create() to manage the
	// cluster. This event always re-adds itself when it fires, so to exit this
	// event loop, we ultimately call event_base_loopbreak() when all our
	// transactions (on the other event base) are completed.

	if (event_base_dispatch(g_p_cluster_mgr_event_base) < 0) {
		LOG("ERROR: cluster manager event base dispatch");
	}

	return NULL;
}


//==========================================================
// Transaction Management
//

//------------------------------------------------
// Do the series of database operations.
//
static void
do_transactions()
{
	// Create the event base for transactions.
	if ((g_p_event_base = event_base_new()) == NULL) {
		LOG("ERROR: creating event base");
		return;
	}

	// Initialize a key to be used in multiple phases.
	ev2citrusleaf_object_init_str(&g_key, (char*)KEY_STRING);

	// Initialize (default) write parameters - used in many phases.
	ev2citrusleaf_write_parameters_init(&g_write_parameters);

	// Start the event loop. There must be an event added on the base before
	// calling event_base_dispatch(), or the event loop will just exit. Here we
	// start our first transaction to ensure an event is added.

	g_phase_index = 0;

	if (PHASE_START_FUNCTIONS[g_phase_index](0)) {

		// event_base_dispatch() will block and run the event loop until no more
		// events are added, or until something calls event_base_loopbreak() or
		// event_base_loopexit().

		// To keep an event loop running, an application must therefore ensure
		// at least one event is always added.

		// In this example, we'll exit the event loop when a transaction
		// callback is made in which we don't start another transaction.

		if (event_base_dispatch(g_p_event_base) < 0) {
			LOG("ERROR: event base dispatch");
		}
	}
	else {
		LOG("ERROR: starting phase 1");
	}

	// Free the event base.
	event_base_free(g_p_event_base);
}

//------------------------------------------------
// Complete a database operation, start the next.
//
static void
client_cb(int return_value, ev2citrusleaf_bin* bins, int n_bins,
		uint32_t generation, uint32_t expiration, void* pv_udata)
{
	// Complete the current phase.
	if (PHASE_COMPLETE_FUNCTIONS[g_phase_index](
			return_value, bins, n_bins, pv_udata)) {
		LOG("completed phase %d", g_phase_index + 1);
	}
	else {
		LOG("ERROR: completing phase %d", g_phase_index + 1);
		// Will exit event loop.
		return;
	}

	// Start the next phase, if there is one. If not, exit the event loop.

	if (! PHASE_START_FUNCTIONS[++g_phase_index]) {
		LOG("example2 completed all %d database transactions", g_phase_index);
		// Will exit event loop.
		return;
	}

	if (! PHASE_START_FUNCTIONS[g_phase_index]()) {
		LOG("ERROR: starting phase %d", g_phase_index + 1);
		// Will exit event loop.
	}
}


//==========================================================
// Transaction Operations
//

//------------------------------------------------
// Write a record with two bins.
//
static bool
start_phase_1()
{
	ev2citrusleaf_bin bins[2];

	// First bin has a string value.
	strcpy(bins[0].bin_name, "test-bin-A");
	ev2citrusleaf_object_init_str(&bins[0].object, "test-value-A");

	// Second bin has an integer value.
	strcpy(bins[1].bin_name, "test-bin-B");
	ev2citrusleaf_object_init_int(&bins[1].object, 0xBBBBbbbb);

	if (0 != ev2citrusleaf_put(
			g_p_cluster,					// cluster
			(char*)g_config.p_namespace,	// namespace
			(char*)g_config.p_set,			// set name
			&g_key,							// key of record to write
			bins,							// bins (array) to write
			2,								// two bins for this transaction
			&g_write_parameters,			// write parameters
			g_config.timeout_msec,			// transaction timeout
			client_cb,						// callback for this transaction
			NULL,							// "user data" - nothing for us
			g_p_event_base)) {				// event base for this transaction
		LOG("ERROR: fail put() for 2-bin record");
		return false;
	}

	return true;
}

//------------------------------------------------
// Read all bins of the record we just wrote.
//
static bool
start_phase_2()
{
	if (0 != ev2citrusleaf_get_all(
			g_p_cluster,					// cluster
			(char*)g_config.p_namespace,	// namespace
			(char*)g_config.p_set,			// set name
			&g_key,							// key of record to get
			g_config.timeout_msec,			// transaction timeout
			client_cb,						// callback for this transaction
			NULL,							// "user data" - nothing for us
			g_p_event_base)) {				// event base for this transaction
		LOG("ERROR: fail get_all() for 2-bin record");
		return false;
	}

	return true;
}

//------------------------------------------------
// Verify the record is as it was written.
//
static bool
complete_phase_2(int return_value, ev2citrusleaf_bin* bins, int n_bins,
		void* pv_udata)
{
	if (return_value != EV2CITRUSLEAF_OK) {
		LOG("ERROR: client callback return_value %d", return_value);
		return false;
	}

	if (! bins) {
		LOG("ERROR: no bin data");
		return false;
	}

	if (n_bins != 2) {
		LOG("ERROR: unexpected n_bins %d - already existing record?", n_bins);

		// Free any allocated bin resources.
		ev2citrusleaf_bins_free(bins, n_bins);

		return false;
	}

	// Order of bins in array is not guaranteed - find which is which by name.

	ev2citrusleaf_bin* p_bin_A = NULL;
	ev2citrusleaf_bin* p_bin_B = NULL;
	bool valid = true;

	// Find out what bins[0] is.
	if (strcmp(bins[0].bin_name, "test-bin-A") == 0) {
		p_bin_A = &bins[0];
	}
	else if (strcmp(bins[0].bin_name, "test-bin-B") == 0) {
		p_bin_B = &bins[0];
	}
	else {
		LOG("ERROR: unexpected bins[0] name %s", bins[0].bin_name);
		valid = false;
	}

	// Find out what bins[1] is.
	if (strcmp(bins[1].bin_name, "test-bin-A") == 0 && ! p_bin_A) {
		p_bin_A = &bins[1];
	}
	else if (strcmp(bins[1].bin_name, "test-bin-B") == 0 && ! p_bin_B) {
		p_bin_B = &bins[1];
	}
	else {
		LOG("ERROR: unexpected bins[1] name %s", bins[1].bin_name);
		valid = false;
	}

	// Validate bin-A.
	if (p_bin_A) {
		if (p_bin_A->object.type != CL_STR) {
			LOG("ERROR: unexpected bin-A type %d", p_bin_A->object.type);
			valid = false;
		}
		else if (strcmp(p_bin_A->object.u.str, "test-value-A") != 0) {
			LOG("ERROR: unexpected bin-A value %s", p_bin_A->object.u.str);
			valid = false;
		}
	}

	// Validate bin-B.
	if (p_bin_B) {
		if (p_bin_B->object.type != CL_INT) {
			LOG("ERROR: unexpected bin-B type %d", p_bin_B->object.type);
			valid = false;
		}
		else if (p_bin_B->object.u.i64 != (int64_t)0xBBBBbbbb) {
			LOG("ERROR: unexpected bin-B value 0x%lx", p_bin_B->object.u.i64);
			valid = false;
		}
	}

	// Free any allocated bin resources.
	ev2citrusleaf_bins_free(bins, n_bins);

	return valid;
}

//------------------------------------------------
// Delete the record.
//
static bool
start_phase_3()
{
	if (0 != ev2citrusleaf_delete(
			g_p_cluster,					// cluster
			(char*)g_config.p_namespace,	// namespace
			(char*)g_config.p_set,			// set name
			&g_key,							// key of record to delete
			&g_write_parameters,			// write parameters
			g_config.timeout_msec,			// transaction timeout
			client_cb,						// callback for this transaction
			NULL,							// "user data" - nothing for us
			g_p_event_base)) {				// event base for this transaction
		LOG("ERROR: fail delete()");
		return false;
	}

	return true;
}

//------------------------------------------------
// Verify a write or delete operation succeeded.
//
static bool
verify_return_value(int return_value, ev2citrusleaf_bin* bins, int n_bins,
		void* pv_udata)
{
	if (return_value != EV2CITRUSLEAF_OK) {
		LOG("ERROR: client callback return_value %d", return_value);
		return false;
	}

	return true;
}
