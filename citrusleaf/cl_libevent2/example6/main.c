/*
 * cl_libevent2/example6/main.c
 *
 * Simple batch API demonstration for the Citrusleaf libevent2 client.
 *
 * This example demonstrates batch database operations. The example uses a
 * single transaction thread (the programs's main thread) and event base. The
 * callback that completes an operation initiates the next one. This is not
 * intended to mimic a realistic application transaction model.
 *
 * The main steps are:
 *	- Initialize database cluster management.
 *	- Write several simple records to the database.
 *	- Using the batch API, check for the existence of all the records.
 *	- Delete 10% of the records.
 *	- Using the batch API, read all the records.
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
const int DEFAULT_NUM_KEYS = 100;

const char BIN_NAME[] = "test-bin-name";

const int CLUSTER_VERIFY_TRIES = 5;
const __useconds_t CLUSTER_VERIFY_INTERVAL = 1000 * 1000; // 1 second


//==========================================================
// Typedefs
//

typedef struct config_s {
	const char* p_host;
	int port;
	const char* p_namespace;
	const char* p_set;
	int timeout_msec;
	int num_keys;
} config;


//==========================================================
// Globals
//

static config g_config;
static ev2citrusleaf_cluster* g_p_cluster = NULL;
static struct event_base* g_p_event_base = NULL;
static ev2citrusleaf_object* g_keys = NULL;
static cf_digest* g_digests = NULL;
static ev2citrusleaf_write_parameters g_write_parameters;
static uint32_t g_num_puts_ok = 0;
static uint32_t g_num_keys_to_delete = 0;
static uint32_t g_num_deletes_ok = 0;


//==========================================================
// Forward Declarations
//

static bool set_config();
static void destroy_config();
static void usage();
static bool start_cluster_management();
static void stop_cluster_management();
static void do_transactions();
static bool put(int k);
static void put_cb(int return_value, ev2citrusleaf_bin* bins, int n_bins,
		uint32_t generation, uint32_t expiration, void* pv_udata);
static void batch_exists();
static void batch_exists_cb(int result, ev2citrusleaf_rec* recs, int n_recs,
		void* pv_udata);
static void delete(int k);
static void delete_cb(int return_value, ev2citrusleaf_bin* bins, int n_bins,
		uint32_t generation, uint32_t expiration, void* pv_udata);
static void batch_get();
static void batch_get_cb(int result, ev2citrusleaf_rec* recs, int n_recs,
		void* pv_udata);
static void validate_data(ev2citrusleaf_rec* p_rec);


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

	// Set up arrays of record keys and digests on the stack.
	ev2citrusleaf_object keys[g_config.num_keys];
	cf_digest digests[g_config.num_keys];

	// Make these globally available.
	g_keys = keys;
	g_digests = digests;

	// Do the series of database operations.
	do_transactions();

	// Exit cleanly.
	stop_cluster_management();
	destroy_config();

	LOG("example6 is done");

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
	g_config.num_keys = DEFAULT_NUM_KEYS;

	int c;

	while ((c = getopt(argc, argv, "h:p:n:s:m:k:")) != -1) {
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

		case 'k':
			g_config.num_keys = atoi(optarg);
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
	LOG("number of keys:      %d", g_config.num_keys);

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
	LOG("-k number of keys [default: %d]", DEFAULT_NUM_KEYS);
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
// Do the series of database operations.
//
static void
do_transactions()
{
	// Initialize the record keys and digests.
	for (int k = 0; k < g_config.num_keys; k++) {
		ev2citrusleaf_object_init_int(&g_keys[k], (int64_t)k);

		if (0 != ev2citrusleaf_calculate_digest(g_config.p_set, &g_keys[k],
				&g_digests[k])) {
			LOG("ERROR: calculating digest");
			return;
		}
	}

	// Initialize (default) write parameters - used in insert phase.
	ev2citrusleaf_write_parameters_init(&g_write_parameters);

	// Create the event base for transactions.
	if ((g_p_event_base = event_base_new()) == NULL) {
		LOG("ERROR: creating event base");
		return;
	}

	// Start the event loop. There must be an event added on the base before
	// calling event_base_dispatch(), or the event loop will just exit. Here we
	// start our first transaction to ensure an event is added.

	if (put(0)) {

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


//==========================================================
// Transaction Operations
//

//------------------------------------------------
// Start a database write operation.
//
static bool
put(int k)
{
	// Write just one bin per record.
	ev2citrusleaf_bin bin;

	// Always the same bin name, use truncated digest as (integer type) value.
	strcpy(bin.bin_name, BIN_NAME);
	ev2citrusleaf_object_init_int(&bin.object, *(int64_t*)&g_digests[k]);

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
			(void*)(uint64_t)k,				// "user data" - key index for us
			g_p_event_base)) {				// event base for this transaction
		LOG("ERROR: put(), key %d", k);
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
	int k = (int)(uint64_t)pv_udata;

	switch (return_value) {
	case EV2CITRUSLEAF_OK:
		g_num_puts_ok++;
		break;

	case EV2CITRUSLEAF_FAIL_TIMEOUT:
		DETAIL("PUT TIMEOUT: digest %lx", *(uint64_t*)&g_digests[k]);
		// Causes EV2CITRUSLEAF_FAIL_NOTFOUND on existence check, delete, get.
		break;

	default:
		LOG("ERROR: put return-value %d, digest %lx", return_value,
				*(uint64_t*)&g_digests[k]);
		break;
	}

	// Insert the next record if this wasn't the last one. Obviously inserting
	// all the records in series like this is sub-optimal, but it's simple, and
	// the purpose here is to demonstrate the batch-exists and batch-get APIs.
	// Perhaps in the future we'll implement a batch put API.

	if (++k < g_config.num_keys) {
		put(k);
	}
	else {
		// Done inserting records.
		LOG("inserted %d records ok, %d failed", g_num_puts_ok,
				g_config.num_keys - g_num_puts_ok);

		// Do batch existence check.
		batch_exists();
	}

	// Will exit event loop if put() or batch_exists() fails...
}

//------------------------------------------------
// Start the batch existence check operation.
//
static void
batch_exists()
{
	if (0 != ev2citrusleaf_exists_many_digest(
			g_p_cluster,					// cluster
			(char*)g_config.p_namespace,	// namespace
			g_digests,						// batch of digests to check
			g_config.num_keys,				// number of digests in batch
			g_config.timeout_msec,			// transaction timeout
			batch_exists_cb,				// callback for this transaction
			NULL,							// "user data" - nothing for us
			g_p_event_base)) {				// event base for this transaction
		LOG("ERROR: batch_exists()");
	}
}

//------------------------------------------------
// Complete the batch existence check operation.
//
static void
batch_exists_cb(int result, ev2citrusleaf_rec* recs, int n_recs, void* pv_udata)
{
	switch (result) {
	case EV2CITRUSLEAF_OK:
		LOG("batch exists result ok, full record results");
		break;

	case EV2CITRUSLEAF_FAIL_TIMEOUT:
		LOG("BATCH EXISTS TIMEOUT: partial record results");
		break;

	default:
		LOG("ERROR: batch exists result %d, partial record results", result);
		break;
	}

	// Examine the result.

	int n_found = 0;

	for (int i = 0; i < n_recs; i++) {
		switch (recs[i].result) {
		case EV2CITRUSLEAF_OK:
			n_found++;
			break;

		case EV2CITRUSLEAF_FAIL_NOTFOUND:
			DETAIL("batch exists record %lx not found",
					*(uint64_t*)&recs[i].digest);
			break;

		default:
			// Individual records' result should be either OK or NOTFOUND.
			LOG("ERROR: batch exists record %lx result %d",
					*(uint64_t*)&recs[i].digest, recs[i].result);
			break;
		}

		// Sanity check - no bin data should ever be returned.
		if (recs[i].bins || recs[i].n_bins) {
			LOG("ERROR: batch exists unexpectedly returned bins");
		}
	}

	LOG("batch exists queried %d records, returned %d, found %d",
			g_config.num_keys, n_recs, n_found);

	// Delete 10% of the records, somewhere in the middle of the key range.

	g_num_keys_to_delete = g_config.num_keys / 10;

	if (g_num_keys_to_delete == 0) {
		g_num_keys_to_delete = 1;
	}

	int k_start = g_config.num_keys / 2;

	if (k_start + g_num_keys_to_delete > g_config.num_keys) {
		k_start = 0;
	}

	delete(k_start);
	// Will exit event loop if delete() fails...
}

//------------------------------------------------
// Delete a record.
//
static void
delete(int k)
{
	if (0 != ev2citrusleaf_delete(
			g_p_cluster,					// cluster
			(char*)g_config.p_namespace,	// namespace
			(char*)g_config.p_set,			// set name
			&g_keys[k],						// key of record to delete
			&g_write_parameters,			// write parameters
			g_config.timeout_msec,			// transaction timeout
			delete_cb,						// callback for this transaction
			(void*)(uint64_t)k,				// "user data" - key index for us
			g_p_event_base)) {				// event base for this transaction
		LOG("ERROR: fail delete()");
	}
}

//------------------------------------------------
// Complete delete operation.
//
static void
delete_cb(int return_value, ev2citrusleaf_bin* bins, int n_bins,
		uint32_t generation, uint32_t expiration, void* pv_udata)
{
	int k = (int)(uint64_t)pv_udata;

	switch (return_value) {
	case EV2CITRUSLEAF_OK:
		g_num_deletes_ok++;
		// Causes EV2CITRUSLEAF_FAIL_NOTFOUND on get.
		break;

	case EV2CITRUSLEAF_FAIL_TIMEOUT:
		DETAIL("DELETE TIMEOUT: digest %lx", *(uint64_t*)&g_digests[k]);
		break;

	default:
		LOG("ERROR: delete return-value %d, digest %lx", return_value,
				*(uint64_t*)&g_digests[k]);
		break;
	}

	// Delete the next record if this wasn't the last one.

	if (--g_num_keys_to_delete > 0) {
		delete(k + 1);
	}
	else {
		// Done deleting records.
		LOG("deleted %d records", g_num_deletes_ok);

		// Do batch get.
		batch_get();
	}

	// Will exit event loop if delete() or batch_get() fails...
}

//------------------------------------------------
// Start the batch read operation.
//
static void
batch_get()
{
	if (0 != ev2citrusleaf_get_many_digest(
			g_p_cluster,					// cluster
			(char*)g_config.p_namespace,	// namespace
			g_digests,						// batch of digests to check
			g_config.num_keys,				// number of digests in batch
			NULL,							// bin name filter, NULL means all
			0,								// number of bin names
			g_config.timeout_msec,			// transaction timeout
			batch_get_cb,					// callback for this transaction
			NULL,							// "user data" - nothing for us
			g_p_event_base)) {				// event base for this transaction
		LOG("ERROR: batch_get()");
	}
}

//------------------------------------------------
// Complete the batch read operation.
//
static void
batch_get_cb(int result, ev2citrusleaf_rec* recs, int n_recs, void* pv_udata)
{
	switch (result) {
	case EV2CITRUSLEAF_OK:
		LOG("batch get result ok, full record results");
		break;

	case EV2CITRUSLEAF_FAIL_TIMEOUT:
		LOG("BATCH GET TIMEOUT: partial record results");
		break;

	default:
		LOG("ERROR: batch get result %d, partial record results", result);
		break;
	}

	// Examine the result.

	int n_found = 0;

	for (int i = 0; i < n_recs; i++) {
		switch (recs[i].result) {
		case EV2CITRUSLEAF_OK:
			n_found++;
			validate_data(&recs[i]);
			break;

		case EV2CITRUSLEAF_FAIL_NOTFOUND:
			DETAIL("batch get record %lx not found",
					*(uint64_t*)&recs[i].digest);
			// Sanity check - no bin data should ever be returned.
			if (recs[i].bins || recs[i].n_bins) {
				LOG("ERROR: batch get returned bins on record not-found");
			}
			break;

		default:
			// Individual records' result should be either OK or NOTFOUND.
			LOG("ERROR: batch get record %lx result %d",
					*(uint64_t*)&recs[i].digest, recs[i].result);
			// Sanity check - no bin data should ever be returned.
			if (recs[i].bins || recs[i].n_bins) {
				LOG("ERROR: batch get returned bins on record error");
			}
			break;
		}
	}

	LOG("batch get queried %d records, returned %d, found %d",
			g_config.num_keys, n_recs, n_found);

	// Done with last test, will exit event loop...
}

//------------------------------------------------
// Validate bin data read from database.
//
static void
validate_data(ev2citrusleaf_rec* p_rec)
{
	uint64_t d = *(uint64_t*)&p_rec->digest;

	if (! p_rec->bins) {
		LOG("ERROR: no bin data with returned record %lx", d);
		return;
	}

	if (p_rec->n_bins != 1) {
		LOG("ERROR: record %lx had unexpected n_bins %d", d, p_rec->n_bins);
	}
	else if (strcmp(p_rec->bins[0].bin_name, BIN_NAME) != 0) {
		LOG("ERROR: record %lx had unexpected bin name %s", d,
				p_rec->bins[0].bin_name);
	}
	else if (p_rec->bins[0].object.type != CL_INT) {
		LOG("ERROR: record %lx had unexpected data type %d", d,
				p_rec->bins[0].object.type);
	}
	else if (p_rec->bins[0].object.u.i64 != (int64_t)d) {
		LOG("ERROR: record %lx had unexpected data value %ld", d,
				p_rec->bins[0].object.u.i64);
	}

	// Bins with integer data type don't need this, but it's good form.
	ev2citrusleaf_bins_free(p_rec->bins, p_rec->n_bins);
}

