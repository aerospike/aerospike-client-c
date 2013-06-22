/*******************************************************************************
 * Copyright 2008-2013 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/


//==========================================================
// Includes
//

#include <getopt.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_bin.h>
#include <aerospike/as_error.h>
#include <aerospike/as_config.h>
#include <aerospike/as_key.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>

#include "example_utils.h"


//==========================================================
// Constants
//

#define MAX_HOST_SIZE 1024
#define MAX_NAMESPACE_SIZE 32	// based on current server limit
#define MAX_SET_SIZE 64			// based on current server limit
#define MAX_KEY_STR_SIZE 1024

const char DEFAULT_HOST[] = "127.0.0.1";
const int DEFAULT_PORT = 3000;
const char DEFAULT_NAMESPACE[] = "test";
const char DEFAULT_SET[] = "test-set";
const char DEFAULT_KEY_STR[] = "test-key";


//==========================================================
// Globals
//

//------------------------------------------------
// The test key used by all basic examples.
// Created using command line options:
// -n <namespace>
// -s <set name>
// -k <key string>
//
as_key g_key;

//------------------------------------------------
// The host info used by all basic examples.
// Obtained using command line options:
// -h <host name>
// -p <port>
//
static char g_host[MAX_HOST_SIZE];
static int g_port;


//==========================================================
// Forward Declarations
//

static void usage(const char* which_opts);


//==========================================================
// Command Line Options
//

//------------------------------------------------
// Parse command line options.
//
bool
example_get_opts(int argc, char* argv[], const char* which_opts)
{
	strcpy(g_host, DEFAULT_HOST);
	g_port = DEFAULT_PORT;

	char namespace[MAX_NAMESPACE_SIZE];
	char set[MAX_SET_SIZE];
	char key_str[MAX_KEY_STR_SIZE];

	strcpy(namespace, DEFAULT_NAMESPACE);
	strcpy(set, DEFAULT_SET);
	strcpy(key_str, DEFAULT_KEY_STR);

	int c;

	while ((c = getopt(argc, argv, which_opts)) != -1) {
		switch (c) {
		case 'h':
			if (strlen(optarg) >= sizeof(g_host)) {
				LOG("ERROR: host exceeds max length");
				return false;
			}
			strcpy(g_host, optarg);
			break;

		case 'p':
			g_port = atoi(optarg);
			break;

		case 'n':
			if (strlen(optarg) >= sizeof(namespace)) {
				LOG("ERROR: namespace exceeds max length");
				return false;
			}
			strcpy(namespace, optarg);
			break;

		case 's':
			if (strlen(optarg) >= sizeof(set)) {
				LOG("ERROR: set name exceeds max length");
				return false;
			}
			strcpy(set, optarg);
			break;

		case 'k':
			if (strlen(optarg) >= sizeof(key_str)) {
				LOG("ERROR: key string exceeds max length");
				return false;
			}
			strcpy(key_str, optarg);
			break;

		default:
			usage(which_opts);
			return false;
		}
	}

	if (strchr(which_opts, 'h')) {
		LOG("host:          %s", g_host);
	}

	if (strchr(which_opts, 'p')) {
		LOG("port:          %d", g_port);
	}

	if (strchr(which_opts, 'n')) {
		LOG("namespace:     %s", namespace);
	}

	if (strchr(which_opts, 's')) {
		LOG("set name:      %s", set);
	}

	if (strchr(which_opts, 'k')) {
		LOG("key (string):  %s", key_str);
	}

	// Initialize the test as_key object.
	as_key_init_str(&g_key, namespace, set, key_str);

	return true;
}

//------------------------------------------------
// Display supported command line options.
//
static void
usage(const char* which_opts)
{
	LOG("Usage:");

	if (strchr(which_opts, 'h')) {
		LOG("-h host [default: %s]", DEFAULT_HOST);
	}

	if (strchr(which_opts, 'p')) {
		LOG("-p port [default: %d]", DEFAULT_PORT);
	}

	if (strchr(which_opts, 'n')) {
		LOG("-n namespace [default: %s]", DEFAULT_NAMESPACE);
	}

	if (strchr(which_opts, 's')) {
		LOG("-s set name [default: %s]", DEFAULT_SET);
	}

	if (strchr(which_opts, 'k')) {
		LOG("-k key string [default: %s]", DEFAULT_KEY_STR);
	}
}


//==========================================================
// Connect/Disconnect
//

//------------------------------------------------
// Connect to database cluster.
//
void
example_connect_to_aerospike(aerospike* p_as)
{
	// Start with default configuration.
	as_config cfg;
	as_config_init(&cfg);

	// Must provide host and port. Example must have called example_get_opts()!
	cfg.hosts[0].addr = g_host;
	cfg.hosts[0].port = g_port;

	as_error err;

	// Connect to the Aerospike database cluster. Assume this is the first thing
	// done after calling example_get_opts(), so it's ok to exit on failure.
	if (aerospike_connect(aerospike_init(p_as, &cfg), &err) != AEROSPIKE_OK) {
		LOG("aerospike_connect() returned %d - %s", err.code, err.message);
		aerospike_destroy(p_as);
		exit(-1);
	}
}

//------------------------------------------------
// Cleanup local record object, remove test record
// from database, and disconnect from cluster.
//
void
example_cleanup(aerospike* p_as, as_record* p_rec)
{
	as_record_destroy(p_rec); // internally handles null p_rec

	// Destroy the test as_key object.
	as_key_destroy(&g_key);

	// Clean up the database. Note that with database "storage-engine device"
	// configurations, this record may come back to life if the server is re-
	// started. That's why examples that want to start clean remove the test
	// record at the beginning.
	example_remove_test_record(p_as);

	as_error err;

	// Disconnect from the database cluster and clean up the aerospike object.
	aerospike_close(p_as, &err);
	aerospike_destroy(p_as);
}


//==========================================================
// Database Operation Helpers
//

//------------------------------------------------
// Remove the test record from the database.
//
void
example_remove_test_record(aerospike* p_as)
{
	as_error err;

	// Try to remove the test record from the database. If the example has not
	// inserted the record, or it has already been removed, this call will
	// return as_status NOT_FOUND - which we'll just ignore.
	aerospike_key_remove(p_as, &err, NULL, &g_key);
}


//==========================================================
// Logging Helpers
// TODO - put (something like) these in aerospike library?
//

static void
example_dump_bin(as_bin* p_bin)
{
	if (! p_bin) {
		LOG("  null as_bin object");
		return;
	}

	char* val_as_str = as_val_tostring(p_bin->valuep);

	LOG("  %s : %s", p_bin->name, val_as_str);

	free(val_as_str);
}

void
example_dump_record(as_record* p_rec)
{
	if (! p_rec) {
		LOG("  null as_record object");
		return;
	}

	uint16_t num_bins = as_record_numbins(p_rec);

	LOG("  generation %u, ttl %u, %u bin%s:", p_rec->gen, p_rec->ttl,
			num_bins, num_bins == 1 ? "" : "s");

	for (uint16_t b = 0; b < num_bins; b++) {
		example_dump_bin(&p_rec->bins.entries[b]);
	}
}
