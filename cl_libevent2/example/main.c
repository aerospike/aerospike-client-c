/*
 * cl_libevent2/example/main.c
 *
 * Simple API demonstration for the Citrusleaf libevent2 client.
 *
 * This example demonstrates some basic database operations. The example uses
 * a single transaction thread (the programs's main thread) and event base. The
 * callback that completes an operation initiates the next one. This is not
 * intended to mimic a realistic application transaction model.
 *
 * The main steps are:
 *	- Initialize database cluster management.
 *	- Do a database info query.
 *	- Do a series of demonstration database operations.
 *	- Clean up.
 */


//==========================================================
// Includes
//

#include <getopt.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <bits/types.h>
#include <event2/dns.h>
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

const int CLUSTER_VERIFY_TRIES = 3;
const __useconds_t CLUSTER_VERIFY_INTERVAL = 1000 * 1000; // 1 second

const char KEY_STRING[] = "test-key";
const uint8_t BLOB[] = {
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF
};


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

typedef bool (*phase_start_fn)(uint32_t generation);
typedef bool (*phase_complete_fn)(int return_value, ev2citrusleaf_bin* bins,
		int n_bins, void* pv_udata);


//==========================================================
// Globals
//

static config g_config;
static ev2citrusleaf_cluster* g_p_cluster = NULL;
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
static bool start_cluster_management();
static void stop_cluster_management();
static void do_info_query();
static void client_info_cb(int return_value, char* response,
		size_t response_len, void* pv_udata);
static void do_transactions();
static void client_cb(int return_value, ev2citrusleaf_bin* bins, int n_bins,
		uint32_t generation, uint32_t expiration, void* pv_udata);
static bool start_phase_1(uint32_t generation);
static bool start_phase_2(uint32_t generation);
static bool complete_phase_2(int return_value, ev2citrusleaf_bin* bins,
		int n_bins, void* pv_udata);
static bool start_phase_3(uint32_t generation);
static bool start_phase_4(uint32_t generation);
static bool complete_phase_4(int return_value, ev2citrusleaf_bin* bins,
		int n_bins, void* pv_udata);
static bool start_phase_5(uint32_t generation);
static bool start_phase_6(uint32_t generation);
static bool complete_phase_6(int return_value, ev2citrusleaf_bin* bins,
		int n_bins, void* pv_udata);
static bool start_phase_7(uint32_t generation);
static bool verify_return_value(int return_value, ev2citrusleaf_bin* bins,
		int n_bins, void* pv_udata);


//==========================================================
// Demonstration Phases
//

const phase_start_fn PHASE_START_FUNCTIONS[] = {
	start_phase_1,			// write a 2-bin record
	start_phase_2,			// read all bins of record
	start_phase_3,			// overwrite one existing bin and add 3rd bin
	start_phase_4,			// read 2 of 3 bins (overwritten and added bin)
	start_phase_5,			// overwrite a bin, using correct generation
	start_phase_6,			// overwrite a bin, using incorrect generation
	start_phase_7,			// delete the record
	NULL
};

const phase_complete_fn PHASE_COMPLETE_FUNCTIONS[] = {
	verify_return_value,	// verify write success
	complete_phase_2,		// verify everything that was read
	verify_return_value,	// verify write success
	complete_phase_4,		// verify everything that was read
	verify_return_value,	// verify write success
	complete_phase_6,		// verify write failure (generation)
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

	// Use default Citrusleaf client logging, but set a filter.
	cf_set_log_level(CF_WARN);

	// Connect to the database server cluster.
	if (! start_cluster_management()) {
		stop_cluster_management();
		destroy_config();
		exit(-1);
	}

	// Do a database info query.
	do_info_query();

	// Do the series of database operations.
	do_transactions();

	// Exit cleanly.
	stop_cluster_management();
	destroy_config();

	LOG("example is done");

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

	while (tries < CLUSTER_VERIFY_TRIES) {
		int n = ev2citrusleaf_cluster_get_active_node_count(g_p_cluster);

		if (n > 0) {
			LOG("found %d cluster node%s", n, n > 1 ? "s" : "");
			return true;
		}

		usleep(CLUSTER_VERIFY_INTERVAL);
		tries++;
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
// Info API Demo
//

//------------------------------------------------
// Do a database info query.
//
static void
do_info_query()
{
	// We could use the same event base for the info query and the transaction
	// series, but we'll do it this way just to keep the info query separate.
	struct event_base* p_event_base = event_base_new();

	if (! p_event_base) {
		LOG("ERROR: creating event base for info query");
		return;
	}

	// Info calls need a DNS event base.
	struct evdns_base* p_dns_base = evdns_base_new(p_event_base, 1);

	if (! p_dns_base) {
		LOG("ERROR: creating dns base for info query");
		event_base_free(p_event_base);
		return;
	}

	char info_names[256];

	// We'll do an info query to get only the namespace information. (Consult
	// the handbook to see what's possible with info queries.)
	strcpy(info_names, "namespace/");
	strcat(info_names, g_config.p_namespace);

	// Start our info query.
	if (0 == ev2citrusleaf_info(
			p_event_base,					// event base for this transaction
			p_dns_base,						// DNS base for this transaction
			(char*)g_config.p_host,			// database server host
			g_config.port,					// database server port
			info_names,						// what info to get
			g_config.timeout_msec,			// transaction timeout
			client_info_cb,					// callback for this transaction
			(void*)p_event_base)) {			// "user data" - the event base

		// Normally, we would exit the event loop when no more events are added.
		// However because we created a DNS base, the event loop will continue
		// to run (i.e. event_base_dispatch() will block) even with no events
		// added. Therefore we must exit the event loop explicitly via the info
		// callback.

		if (event_base_dispatch(p_event_base) < 0) {
			LOG("ERROR: event base dispatch");
		}
	}
	else {
		LOG("ERROR: starting info query");
	}

	evdns_base_free(p_dns_base, 0);
	event_base_free(p_event_base);
}

//------------------------------------------------
// Complete the database info query.
//
void
client_info_cb(int return_value, char* response, size_t response_len,
		void* pv_udata)
{
	if (return_value == EV2CITRUSLEAF_OK) {
		DETAIL("info callback response_len: %lu", response_len);
		DETAIL("info callback response:");
		DETAIL("%s", response);

		// Do something useful with the info.
		if (strstr(response, "single-bin=true")) {
			LOG("VERY BAD: server is single-bin - example is muti-bin!");
		}
	}
	else {
		LOG("ERROR: info callback return_value %d", return_value);
	}

	// Application is responsible for freeing response.
	if (response) {
		free(response);
	}

	LOG("completed info query");

	// Because of the DNS base, we have to exit the event loop explicitly.
	// (Calling evdns_base_free() here would also work, but we'll do it this way
	// so the evdns_base_free() in do_info_query() can cover failure cases too.)

	event_base_loopbreak((struct event_base*)pv_udata);
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
		LOG("example completed all %d database transactions", g_phase_index);
		// Will exit event loop.
		return;
	}

	if (! PHASE_START_FUNCTIONS[g_phase_index](generation)) {
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
start_phase_1(uint32_t generation)
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
start_phase_2(uint32_t generation)
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
// Overwrite an existing bin and add a third bin.
//
static bool
start_phase_3(uint32_t generation)
{
	ev2citrusleaf_bin bins[2];

	// Overwrite the second bin, now with a string value.
	strcpy(bins[0].bin_name, "test-bin-B");
	ev2citrusleaf_object_init_str(&bins[0].object, "test-value-B");

	// Third bin has a blob value.
	strcpy(bins[1].bin_name, "test-bin-C");
	ev2citrusleaf_object_init_blob(&bins[1].object, (void*)BLOB, sizeof(BLOB));

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
		LOG("ERROR: fail put() to overwrite 2nd bin and add 3rd bin");
		return false;
	}

	return true;
}

//------------------------------------------------
// Read the two bins we just wrote.
//
static bool
start_phase_4(uint32_t generation)
{
	const char* bin_names[] = { "test-bin-B", "test-bin-C" };

	if (0 != ev2citrusleaf_get(
			g_p_cluster,					// cluster
			(char*)g_config.p_namespace,	// namespace
			(char*)g_config.p_set,			// set name
			&g_key,							// key of record to get
			bin_names,						// names of bins to get
			2,								// get two (of the three) bins
			g_config.timeout_msec,			// transaction timeout
			client_cb,						// callback for this transaction
			NULL,							// "user data" - nothing for us
			g_p_event_base)) {				// event base for this transaction
		LOG("ERROR: fail get() for 2 bins of 3-bin record");
		return false;
	}

	return true;
}

//------------------------------------------------
// Verify the two bins are as expected.
//
static bool
complete_phase_4(int return_value, ev2citrusleaf_bin* bins, int n_bins,
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
		LOG("ERROR: unexpected n_bins %d", n_bins);

		// Free any allocated bin resources.
		ev2citrusleaf_bins_free(bins, n_bins);

		return false;
	}

	// Order of bins in array is not guaranteed - find which is which by name.

	ev2citrusleaf_bin* p_bin_B = NULL;
	ev2citrusleaf_bin* p_bin_C = NULL;
	bool valid = true;

	// Find out what bins[0] is.
	if (strcmp(bins[0].bin_name, "test-bin-B") == 0) {
		p_bin_B = &bins[0];
	}
	else if (strcmp(bins[0].bin_name, "test-bin-C") == 0) {
		p_bin_C = &bins[0];
	}
	else {
		LOG("ERROR: unexpected bins[0] name %s", bins[0].bin_name);
		valid = false;
	}

	// Find out what bins[1] is.
	if (strcmp(bins[1].bin_name, "test-bin-B") == 0 && ! p_bin_B) {
		p_bin_B = &bins[1];
	}
	else if (strcmp(bins[1].bin_name, "test-bin-C") == 0 && ! p_bin_C) {
		p_bin_C = &bins[1];
	}
	else {
		LOG("ERROR: unexpected bins[1] name %s", bins[1].bin_name);
		valid = false;
	}

	// Validate bin-B.
	if (p_bin_B) {
		if (p_bin_B->object.type != CL_STR) {
			LOG("ERROR: unexpected bin-B type %d", p_bin_B->object.type);
			valid = false;
		}
		else if (strcmp(p_bin_B->object.u.str, "test-value-B") != 0) {
			LOG("ERROR: unexpected bin-B value %s", p_bin_B->object.u.str);
			valid = false;
		}
	}

	// Validate bin-C.
	if (p_bin_C) {
		if (p_bin_C->object.type != CL_BLOB) {
			LOG("ERROR: unexpected bin-C type %d", p_bin_C->object.type);
			valid = false;
		}
		else if (p_bin_C->object.size != sizeof(BLOB)) {
			LOG("ERROR: unexpected bin-C blob size %lu", p_bin_C->object.size);
			valid = false;
		}
		else if (memcmp(p_bin_C->object.u.blob, BLOB, sizeof(BLOB)) != 0) {
			LOG("ERROR: unexpected bin-C blob value");
			valid = false;
		}
	}

	// Free any allocated bin resources.
	ev2citrusleaf_bins_free(bins, n_bins);

	return valid;
}

//------------------------------------------------
// Overwrite a bin, using correct generation.
//
static bool
start_phase_5(uint32_t generation)
{
	ev2citrusleaf_bin bin;

	// Overwrite the first bin.
	strcpy(bin.bin_name, "test-bin-A");
	ev2citrusleaf_object_init_str(&bin.object, "overwritten-value-A");

	ev2citrusleaf_write_parameters write_parameters;

	// Specify correct generation in write parameters.
	ev2citrusleaf_write_parameters_init(&write_parameters);
	write_parameters.use_generation = true;
	write_parameters.generation = generation;

	if (0 != ev2citrusleaf_put(
			g_p_cluster,					// cluster
			(char*)g_config.p_namespace,	// namespace
			(char*)g_config.p_set,			// set name
			&g_key,							// key of record to write
			&bin,							// bin (as array) to write
			1,								// one bin for this transaction
			&write_parameters,				// write parameters using generation
			g_config.timeout_msec,			// transaction timeout
			client_cb,						// callback for this transaction
			NULL,							// "user data" - nothing for us
			g_p_event_base)) {				// event base for this transaction
		LOG("ERROR: fail put() to overwrite bin with correct generation");
		return false;
	}

	return true;
}

//------------------------------------------------
// Overwrite a bin, using incorrect generation.
//
static bool
start_phase_6(uint32_t generation)
{
	ev2citrusleaf_bin bin;

	// Overwrite the second bin.
	strcpy(bin.bin_name, "test-bin-B");
	ev2citrusleaf_object_init_str(&bin.object, "overwritten-value-B");

	ev2citrusleaf_write_parameters write_parameters;

	// Specify incorrect generation in write parameters.
	ev2citrusleaf_write_parameters_init(&write_parameters);
	write_parameters.use_generation = true;
	write_parameters.generation = generation - 1;

	if (0 != ev2citrusleaf_put(
			g_p_cluster,					// cluster
			(char*)g_config.p_namespace,	// namespace
			(char*)g_config.p_set,			// set name
			&g_key,							// key of record to write
			&bin,							// bin (as array) to write
			1,								// one bin for this transaction
			&write_parameters,				// write parameters using generation
			g_config.timeout_msec,			// transaction timeout
			client_cb,						// callback for this transaction
			NULL,							// "user data" - nothing for us
			g_p_event_base)) {				// event base for this transaction
		LOG("ERROR: fail put() to overwrite bin with incorrect generation");
		return false;
	}

	return true;
}

//------------------------------------------------
// Verify the write failed as expected.
//
static bool
complete_phase_6(int return_value, ev2citrusleaf_bin* bins, int n_bins,
		void* pv_udata)
{
	if (return_value != EV2CITRUSLEAF_FAIL_GENERATION) {
		LOG("ERROR: client callback return_value %d", return_value);
		return false;
	}

	return true;
}

//------------------------------------------------
// Delete the record.
//
static bool
start_phase_7(uint32_t generation)
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
