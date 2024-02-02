/*******************************************************************************
 * Copyright 2008-2023 by Aerospike.
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

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_index.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_udf.h>
#include <aerospike/as_bin.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_dir.h>
#include <aerospike/as_error.h>
#include <aerospike/as_config.h>
#include <aerospike/as_key.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_password.h>
#include <aerospike/as_record.h>
#include <aerospike/as_record_iterator.h>
#include <aerospike/as_sleep.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>
#include <aerospike/as_val.h>

#include "example_utils.h"

#if defined(_MSC_VER)
#undef _UNICODE  // Use ASCII version on windows.
#endif
#include <getopt.h>

//==========================================================
// Constants
//

#define MAX_HOST_SIZE 1024
#define MAX_KEY_STR_SIZE 1024

const char DEFAULT_HOST[] = "127.0.0.1";
const int DEFAULT_PORT = 3000;
const char DEFAULT_NAMESPACE[] = "test";
const char DEFAULT_SET[] = "eg-set";
const char DEFAULT_KEY_STR[] = "eg-key";
const uint32_t DEFAULT_NUM_KEYS = 20;

const char SHORT_OPTS_BASIC[] = "h:p:U:P::n:s:k:";
const struct option LONG_OPTS_BASIC[] = {
	{"hosts",                required_argument, 0, 'h'},
	{"port",                 required_argument, 0, 'p'},
	{"user",                 required_argument, 0, 'U'},
	{"password",             optional_argument, 0, 'P'},
	{"namespace",            required_argument, 0, 'n'},
	{"set",                  required_argument, 0, 's'},
	{"key",                  required_argument, 0, 'k'},
	{"tlsEnable",            no_argument,       0, 'A'},
	{"tlsCaFile",            required_argument, 0, 'E'},
	{"tlsCaPath",            required_argument, 0, 'F'},
	{"tlsProtocols",         required_argument, 0, 'G'},
	{"tlsCipherSuite",       required_argument, 0, 'H'},
	{"tlsCrlCheck",          no_argument,       0, 'I'},
	{"tlsCrlCheckAll",       no_argument,       0, 'J'},
	{"tlsCertBlackList",     required_argument, 0, 'O'},
	{"tlsLogSessionInfo",    no_argument,       0, 'Q'},
	{"tlsKeyFile",           required_argument, 0, 'Z'},
	{"tlsCertFile",          required_argument, 0, 'y'},
	{"tlsLoginOnly",         no_argument,       0, 'f'},
	{"auth",                 required_argument, 0, 'e'},
	{0, 0, 0, 0}
};

const char SHORT_OPTS_MULTI_KEY[] = "h:p:U:P::n:s:K:";
const struct option LONG_OPTS_MULTI_KEY[] = {
	{"hosts",                required_argument, 0, 'h'},
	{"port",                 required_argument, 0, 'p'},
	{"user",                 required_argument, 0, 'U'},
	{"password",             optional_argument, 0, 'P'},
	{"namespace",            required_argument, 0, 'n'},
	{"set",                  required_argument, 0, 's'},
	{"multikey",             required_argument, 0, 'K'},
	{"tlsEnable",            no_argument,       0, 'A'},
	{"tlsCaFile",            required_argument, 0, 'E'},
	{"tlsCaPath",            required_argument, 0, 'F'},
	{"tlsProtocols",         required_argument, 0, 'G'},
	{"tlsCipherSuite",       required_argument, 0, 'H'},
	{"tlsCrlCheck",          no_argument,       0, 'I'},
	{"tlsCrlCheckAll",       no_argument,       0, 'J'},
	{"tlsCertBlackList",     required_argument, 0, 'O'},
	{"tlsLogSessionInfo",    no_argument,       0, 'Q'},
	{"tlsKeyFile",           required_argument, 0, 'Z'},
	{"tlsCertFile",          required_argument, 0, 'y'},
	{"tlsLoginOnly",         no_argument,       0, 'f'},
	{"auth",                 required_argument, 0, 'e'},
	{0, 0, 0, 0}
};

//==========================================================
// Globals
//

//------------------------------------------------
// The namespace and set used by all examples.
// Created using command line options:
// -n <namespace>
// -s <set name>
//
char g_namespace[MAX_NAMESPACE_SIZE];
char g_set[MAX_SET_SIZE];

//------------------------------------------------
// The test key used by all basic examples.
// Created using command line options:
// -n <namespace>
// -s <set name>
// -k <key string>
//
as_key g_key;

//------------------------------------------------
// For examples that use many keys.
// Obtained using command line option:
// -K <number of keys>
//
uint32_t g_n_keys;

//------------------------------------------------
// The host info used by all basic examples.
// Obtained using command line options:
// -h <host name>
// -p <port>
//
static char g_host[MAX_HOST_SIZE];
static int g_port;

//------------------------------------------------
// The optional user/password.
// Obtained using command line options:
// -U <user name>
// -P [<password>]
//
static char g_user[AS_USER_SIZE];
static char g_password[AS_PASSWORD_SIZE];

//------------------------------------------------
// The (string) value of the test key used by all
// basic examples. From command line option:
// -k <key string>
//
static char g_key_str[MAX_KEY_STR_SIZE];

//------------------------------------------------
// TLS configuration variables.
as_config_tls g_tls = {0};

as_auth_mode g_auth_mode = AS_AUTH_INTERNAL;

//==========================================================
// Forward Declarations
//

static void usage(const char* short_opts);


//==========================================================
// Command Line Options
//

//------------------------------------------------
// Parse command line options.
//
bool
example_get_opts(int argc, char* argv[], int which_opts)
{
	strcpy(g_host, DEFAULT_HOST);
	g_port = DEFAULT_PORT;
	strcpy(g_namespace, DEFAULT_NAMESPACE);
	strcpy(g_set, DEFAULT_SET);
	strcpy(g_key_str, DEFAULT_KEY_STR);
	g_n_keys = DEFAULT_NUM_KEYS;

	const char* short_opts;
	const struct option* long_opts;

	switch (which_opts) {
	case EXAMPLE_BASIC_OPTS:
		short_opts = SHORT_OPTS_BASIC;
		long_opts = LONG_OPTS_BASIC;
		break;
	case EXAMPLE_MULTI_KEY_OPTS:
		short_opts = SHORT_OPTS_MULTI_KEY;
		long_opts = LONG_OPTS_MULTI_KEY;
		break;
	default:
		LOG("ERROR: unrecognized which_opts parameter");
		return false;
	}

	int c;
	int i;

	while ((c = getopt_long(argc, argv, short_opts, long_opts, &i)) != -1) {
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

		case 'U':
			strcpy(g_user, optarg);
			break;

		case 'P':
			as_password_acquire(g_password, optarg, AS_PASSWORD_SIZE);
			break;

		case 'n':
			if (strlen(optarg) >= sizeof(g_namespace)) {
				LOG("ERROR: namespace exceeds max length");
				return false;
			}
			strcpy(g_namespace, optarg);
			break;

		case 's':
			if (strlen(optarg) >= sizeof(g_set)) {
				LOG("ERROR: set name exceeds max length");
				return false;
			}
			strcpy(g_set, optarg);
			break;

		case 'k':
			if (strlen(optarg) >= sizeof(g_key_str)) {
				LOG("ERROR: key string exceeds max length");
				return false;
			}
			strcpy(g_key_str, optarg);
			break;

		case 'K':
			g_n_keys = atoi(optarg);
			break;

		case 'A':
			g_tls.enable = true;
			break;

		case 'E':
			g_tls.cafile = strdup(optarg);
			break;

		case 'F':
			g_tls.capath = strdup(optarg);
			break;

		case 'G':
			g_tls.protocols = strdup(optarg);
			break;

		case 'H':
			g_tls.cipher_suite = strdup(optarg);
			break;

		case 'I':
			g_tls.crl_check = true;
			break;

		case 'J':
			g_tls.crl_check_all = true;
			break;

		case 'O':
			g_tls.cert_blacklist = strdup(optarg);
			break;

		case 'Q':
			g_tls.log_session_info = true;
			break;

		case 'Z':
			g_tls.keyfile = strdup(optarg);
			break;
			
		case 'y':
			g_tls.certfile = strdup(optarg);
			break;

		case 'f':
			g_tls.for_login_only = true;
			break;

		case 'e':
			if (! as_auth_mode_from_string(&g_auth_mode, optarg)) {
				LOG("ERROR: invalid authentication mode: %s", optarg);
				return false;
			}
			break;

		default:
			usage(short_opts);
			return false;
		}
	}

	if (strchr(short_opts, 'h')) {
		LOG("host:           %s", g_host);
	}

	if (strchr(short_opts, 'p')) {
		LOG("port:           %d", g_port);
	}

	if (strchr(short_opts, 'U')) {
		LOG("user:           %s", g_user);
	}

	if (strchr(short_opts, 'n')) {
		LOG("namespace:      %s", g_namespace);
	}

	if (strchr(short_opts, 's')) {
		LOG("set name:       %s", g_set);
	}

	if (strchr(short_opts, 'k')) {
		LOG("key (string):   %s", g_key_str);
	}

	if (strchr(short_opts, 'K')) {
		LOG("number of keys: %u", g_n_keys);
	}

	// Initialize the test as_key object. We won't need to destroy it since it
	// isn't being created on the heap or with an external as_key_value.
	as_key_init_str(&g_key, g_namespace, g_set, g_key_str);

	return true;
}

//------------------------------------------------
// Display supported command line options.
//
static void
usage(const char* short_opts)
{
	LOG("Usage:");
	
	if (strchr(short_opts, 'h')) {
		LOG("-h host1[:tlsname1][:port1],... [default: %s]", DEFAULT_HOST);
		LOG("   Seed hostnames or IP addresses.");
		LOG("   The tlsname is only used when connecting with a secure TLS enabled server.");
		LOG("   If the port is not specified, the default port is used.");
		LOG("   Examples:");
		LOG("   host1");
		LOG("   host1:3000,host2:3000");
		LOG("   192.168.1.10:cert1:3000,192.168.1.20:cert2:3000");
	}

	if (strchr(short_opts, 'p')) {
		LOG("-p <port> [default: %d]", DEFAULT_PORT);
		LOG("   Default port.");
	}

	if (strchr(short_opts, 'U')) {
		LOG("-U <username> [default: none]");
	}

	if (strchr(short_opts, 'P')) {
		LOG("-P [<password>] [default: none]");
	}

	if (strchr(short_opts, 'n')) {
		LOG("-n <namespace> [default: %s]", DEFAULT_NAMESPACE);
	}

	if (strchr(short_opts, 's')) {
		LOG("-s <set name> [default: %s]", DEFAULT_SET);
	}

	if (strchr(short_opts, 'k')) {
		LOG("-k <key string> [default: %s]", DEFAULT_KEY_STR);
	}

	if (strchr(short_opts, 'K')) {
		LOG("-K <number of keys> [default: %u]", DEFAULT_NUM_KEYS);
	}

	LOG("--tlsEnable  [default: TLS disabled]");
	LOG("  Enable TLS.");

	LOG("--tlsCaFile <path>");
	LOG("  Set the TLS certificate authority file.");

	LOG("--tlsCaPath <path>");
	LOG("  Set the TLS certificate authority directory.");

	LOG("--tlsProtocols <protocols>");
	LOG("  Set the TLS protocol selection criteria.");

	LOG("--tlsCipherSuite <suite>");
	LOG("  Set the TLS cipher selection criteria.");

	LOG("--tlsCrlCheck");
	LOG("  Enable CRL checking for leaf certs.");

	LOG("--tlsCrlCheckAll");
	LOG("  Enable CRL checking for all certs.");

	LOG("--tlsCertBlackList <path>");
	LOG("  Path to a certificate blacklist file.");

	LOG("--tlsLogSessionInfo");
	LOG("  Log TLS connected session info.");

	LOG("--tlsKeyFile <path>");
	LOG("  Set the TLS client key file for mutual authentication.");

	LOG("--tlsCertFile <path>");
	LOG("  Set the TLS client certificate chain file for mutual authentication.");

	LOG("--tlsLoginOnly");
	LOG("  Use TLS for node login only.");

	LOG("--auth {INTERNAL,EXTERNAL,EXTERNAL_SECURE,PKI}");
	LOG("  Set authentication mode when user/password is defined.");
}

//==========================================================
// Logging
//
static bool
example_log_callback(as_log_level level, const char * func, const char * file, uint32_t line, const char * fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	vprintf(fmt, ap);
	printf("\n");
	va_end(ap);
	return true;
}

//==========================================================
// Initialize asynchronous event loop
//
bool
example_create_event_loop()
{
#if AS_EVENT_LIB_DEFINED
	// Initialize logging.
	as_log_set_callback(example_log_callback);

	if (as_event_create_loops(1)) {
		return true;
	}
#endif
	LOG("Event library not defined. Skip async example.");
	return false;
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
	example_connect_to_aerospike_with_udf_config(p_as, NULL);
}

//------------------------------------------------
// Connect to database cluster, setting UDF
// configuration.
//
void
example_connect_to_aerospike_with_udf_config(aerospike* p_as,
		const char* lua_user_path)
{
	// Initialize logging.
	as_log_set_callback(example_log_callback);

	// Initialize default lua configuration.
	as_config_lua lua;
	as_config_lua_init(&lua);
	
	if (lua_user_path) {
		strcpy(lua.user_path, lua_user_path);
	}
	
	// Initialize global lua configuration.
	aerospike_init_lua(&lua);
		
	// Initialize cluster configuration.
	as_config config;
	as_config_init(&config);
	
	if (! as_config_add_hosts(&config, g_host, g_port)) {
		printf("Invalid host(s) %s\n", g_host);
		as_event_close_loops();
		exit(-1);
	}
	
	as_config_set_user(&config, g_user, g_password);

	// Transfer ownership of all heap allocated TLS fields via shallow copy.
	memcpy(&config.tls, &g_tls, sizeof(as_config_tls));
	config.auth_mode = g_auth_mode;

	aerospike_init(p_as, &config);

	as_error err;

	// Connect to the Aerospike database cluster. Assume this is the first thing
	// done after calling example_get_opts(), so it's ok to exit on failure.
	if (aerospike_connect(p_as, &err) != AEROSPIKE_OK) {
		LOG("aerospike_connect() returned %d - %s", err.code, err.message);
		as_event_close_loops();
		aerospike_destroy(p_as);
		exit(-1);
	}
}

//------------------------------------------------
// Remove the test record from database, and
// disconnect from cluster.
//
void
example_cleanup(aerospike* p_as)
{
	// Clean up the database. Note that with database "storage-engine device"
	// configurations, this record may come back to life if the server is re-
	// started. That's why examples that want to start clean remove the test
	// record at the beginning.
	example_remove_test_record(p_as);

	// Note also example_remove_test_records() is not called here - examples
	// using multiple records call that from their own cleanup utilities.

	as_error err;

	// Disconnect from the database cluster and clean up the aerospike object.
	aerospike_close(p_as, &err);
	aerospike_destroy(p_as);
}


//==========================================================
// Database Operation Helpers
//

//------------------------------------------------
// Read the whole test record from the database.
//
bool
example_read_test_record(aerospike* p_as)
{
	as_error err;
	as_record* p_rec = NULL;

	// Read the test record from the database.
	if (aerospike_key_get(p_as, &err, NULL, &g_key, &p_rec) != AEROSPIKE_OK) {
		LOG("aerospike_key_get() returned %d - %s", err.code, err.message);
		return false;
	}

	// If we didn't get an as_record object back, something's wrong.
	if (! p_rec) {
		LOG("aerospike_key_get() retrieved null as_record object");
		return false;
	}

	// Log the result.
	LOG("record was successfully read from database:");
	example_dump_record(p_rec);

	// Destroy the as_record object.
	as_record_destroy(p_rec);

	return true;
}

//------------------------------------------------
// Remove the test record from the database.
//
void
example_remove_test_record(aerospike* p_as)
{
	as_error err;

	// Try to remove the test record from the database. If the example has not
	// inserted the record, or it has already been removed, this call will
	// return as_status AEROSPIKE_ERR_RECORD_NOT_FOUND - which we just ignore.
	aerospike_key_remove(p_as, &err, NULL, &g_key);
}

//------------------------------------------------
// Read multiple-record examples' test records
// from the database.
//
bool
example_read_test_records(aerospike* p_as)
{
	// Multiple-record examples insert g_n_keys records, using integer keys from
	// 0 to (g_n_keys - 1).
	for (uint32_t i = 0; i < g_n_keys; i++) {
		as_error err;

		// No need to destroy a stack as_key object, if we only use
		// as_key_init_int64().
		as_key key;
		as_key_init_int64(&key, g_namespace, g_set, (int64_t)i);

		as_record* p_rec = NULL;

		// Read a test record from the database.
		if (aerospike_key_get(p_as, &err, NULL, &key, &p_rec) != AEROSPIKE_OK) {
			LOG("aerospike_key_get() returned %d - %s", err.code, err.message);
			return false;
		}

		// If we didn't get an as_record object back, something's wrong.
		if (! p_rec) {
			LOG("aerospike_key_get() retrieved null as_record object");
			return false;
		}

		// Log the result.
		LOG("read record with key %u from database:", i);
		example_dump_record(p_rec);

		// Destroy the as_record object.
		as_record_destroy(p_rec);
	}

	return true;
}

//------------------------------------------------
// Remove multiple-record examples' test records
// from the database.
//
void
example_remove_test_records(aerospike* p_as)
{
	// Multiple-record examples insert g_n_keys records, using integer keys from
	// 0 to (g_n_keys - 1).
	for (uint32_t i = 0; i < g_n_keys; i++) {
		as_error err;

		// No need to destroy a stack as_key object, if we only use
		// as_key_init_int64().
		as_key key;
		as_key_init_int64(&key, g_namespace, g_set, (int64_t)i);

		// Ignore errors - just trying to leave the database as we found it.
		aerospike_key_remove(p_as, &err, NULL, &key);
	}
}


//==========================================================
// UDF Function (Script) Registration
//

//------------------------------------------------
// Register a UDF function in the database.
//
bool
example_register_udf(aerospike* p_as, const char* udf_file_path)
{
	FILE* file = fopen(udf_file_path, "r");

	if (! file) {
		// If we get here it's likely that we're not running the example from
		// the right directory - the specific example directory.
		LOG("cannot open script file %s : %s", udf_file_path, strerror(errno));
		return false;
	}

	// Read the file's content into a local buffer.

	uint8_t* content = (uint8_t*)malloc(1024 * 1024);

	if (! content) {
		LOG("script content allocation failed");
		fclose(file);
		return false;
	}

	uint8_t* p_write = content;
	int read = (int)fread(p_write, 1, 512, file);
	int size = 0;

	while (read) {
		size += read;
		p_write += read;
		read = (int)fread(p_write, 1, 512, file);
	}

	fclose(file);

	// Wrap the local buffer as an as_bytes object.
	as_bytes udf_content;
	as_bytes_init_wrap(&udf_content, content, size, true);

	as_error err;
	as_string base_string;
	const char* base = as_basename(&base_string, udf_file_path);

	// Register the UDF file in the database cluster.
	if (aerospike_udf_put(p_as, &err, NULL, base, AS_UDF_TYPE_LUA,
			&udf_content) == AEROSPIKE_OK) {
		// Wait for the system metadata to spread to all nodes.
		aerospike_udf_put_wait(p_as, &err, NULL, base, 1000);
	}
	else {
		LOG("aerospike_udf_put() returned %d - %s", err.code, err.message);
	}

	as_string_destroy(&base_string);

	// This frees the local buffer.
	as_bytes_destroy(&udf_content);

	return err.code == AEROSPIKE_OK;
}

//------------------------------------------------
// Remove a UDF function from the database.
//
bool
example_remove_udf(aerospike* p_as, const char* udf_file_path)
{
	as_error err;
	as_string base_string;
	const char* base = as_basename(&base_string, udf_file_path);

	if (aerospike_udf_remove(p_as, &err, NULL, base) != AEROSPIKE_OK) {
		LOG("aerospike_udf_remove() returned %d - %s", err.code, err.message);
		return false;
	}

	as_string_destroy(&base_string);

	// Wait for the system metadata to spread to all nodes.
	as_sleep(100);

	return true;
}


//==========================================================
// Secondary Index Registration
//

//------------------------------------------------
// Create a numeric secondary index for a
// specified bin in the database.
//
bool
example_create_integer_index(aerospike* p_as, const char* set, const char* bin, const char* index)
{
	as_error err;
	as_index_task task;
	as_status status = aerospike_index_create(p_as, &err, &task, NULL, g_namespace, set, bin, index, AS_INDEX_NUMERIC);

	switch (status) {
		case AEROSPIKE_OK:
			// Wait for the system metadata to spread to all nodes.
			status = aerospike_index_create_wait(&err, &task, 0);

			if (status != AEROSPIKE_OK) {
				LOG("aerospike_index_create_wait() returned %d - %s", err.code, err.message);
				return false;
			}
			break;

		case AEROSPIKE_ERR_INDEX_FOUND:
			LOG("index already exists");
			break;

		default:
			LOG("aerospike_index_create() returned %d - %s", err.code, err.message);
			return false;
	}
	return true;
}

//------------------------------------------------
// Create a geospatial secondary index for a
// specified bin in the database.
//
bool
example_create_2dsphere_index(aerospike* p_as, const char* bin, const char* index)
{
	as_error err;
	as_index_task task;
	as_status status = aerospike_index_create(p_as, &err, &task, NULL, g_namespace, g_set, bin, index, AS_INDEX_GEO2DSPHERE);

	switch (status) {
		case AEROSPIKE_OK:
			// Wait for the system metadata to spread to all nodes.
			aerospike_index_create_wait(&err, &task, 0);
			break;

		case AEROSPIKE_ERR_INDEX_FOUND:
			LOG("index already exists");
			break;

		default:
			LOG("aerospike_index_create() returned %d - %s", err.code, err.message);
			return false;
	}
	return true;
}

//------------------------------------------------
// Remove a secondary index from the database.
//
void
example_remove_index(aerospike* p_as, const char* index)
{
	as_error err;

	// Ignore errors - just trying to leave the database as we found it.
	aerospike_index_remove(p_as, &err, NULL, g_namespace, index);

	// Wait for the system metadata to spread to all nodes.
	as_sleep(100);
}


//==========================================================
// Logging Helpers
// TODO - put (something like) these in aerospike library?
//

static void
example_dump_bin(const as_bin* p_bin)
{
	if (! p_bin) {
		LOG("  null as_bin object");
		return;
	}

	char* val_as_str = as_val_tostring(as_bin_get_value(p_bin));

	LOG("  %s : %s", as_bin_get_name(p_bin), val_as_str);

	free(val_as_str);
}

void
example_dump_record(const as_record* p_rec)
{
	if (! p_rec) {
		LOG("  null as_record object");
		return;
	}

	if (p_rec->key.valuep) {
		char* key_val_as_str = as_val_tostring(p_rec->key.valuep);

		LOG("  key: %s", key_val_as_str);

		free(key_val_as_str);
	}

	uint16_t num_bins = as_record_numbins(p_rec);

	LOG("  generation %u, ttl %u, %u bin%s", p_rec->gen, p_rec->ttl, num_bins,
			num_bins == 0 ? "s" : (num_bins == 1 ? ":" : "s:"));

	as_record_iterator it;
	as_record_iterator_init(&it, p_rec);

	while (as_record_iterator_has_next(&it)) {
		example_dump_bin(as_record_iterator_next(&it));
	}

	as_record_iterator_destroy(&it);
}

#define OP_CASE_ASSIGN(__enum) \
	case __enum : \
		return #__enum; \

static const char*
operator_to_string(as_operator op)
{
	switch (op) {
		OP_CASE_ASSIGN(AS_OPERATOR_READ);
		OP_CASE_ASSIGN(AS_OPERATOR_WRITE);
		OP_CASE_ASSIGN(AS_OPERATOR_INCR);
		OP_CASE_ASSIGN(AS_OPERATOR_EXP_READ);
		OP_CASE_ASSIGN(AS_OPERATOR_EXP_MODIFY);
		OP_CASE_ASSIGN(AS_OPERATOR_APPEND);
		OP_CASE_ASSIGN(AS_OPERATOR_PREPEND);
		OP_CASE_ASSIGN(AS_OPERATOR_TOUCH);
		OP_CASE_ASSIGN(AS_OPERATOR_CDT_MODIFY);
		OP_CASE_ASSIGN(AS_OPERATOR_CDT_READ);
		OP_CASE_ASSIGN(AS_OPERATOR_MAP_MODIFY);
		OP_CASE_ASSIGN(AS_OPERATOR_MAP_READ);
		OP_CASE_ASSIGN(AS_OPERATOR_BIT_MODIFY);
		OP_CASE_ASSIGN(AS_OPERATOR_BIT_READ);
		OP_CASE_ASSIGN(AS_OPERATOR_DELETE);
		OP_CASE_ASSIGN(AS_OPERATOR_HLL_MODIFY);
		OP_CASE_ASSIGN(AS_OPERATOR_HLL_READ);
	}

	return "NOT DEFINED";
}

static void
example_dump_op(const as_binop* p_binop)
{
	if (! p_binop) {
		LOG("  null as_binop object");
		return;
	}

	const char* op_string = operator_to_string(p_binop->op);

	if (p_binop->op == AS_OPERATOR_TOUCH || p_binop->op == AS_OPERATOR_DELETE) {
		LOG("  %s", op_string);
		return;
	}

	if (p_binop->op == AS_OPERATOR_READ) {
		LOG("  %s : %s", op_string, p_binop->bin.name);
		return;
	}

	char* val_as_str = as_val_tostring(p_binop->bin.valuep);

	LOG("  %s : %s : %s", op_string, p_binop->bin.name,
			val_as_str);

	free(val_as_str);
}

void
example_dump_operations(const as_operations* p_ops)
{
	if (! p_ops) {
		LOG("  null as_operations object");
		return;
	}

	uint16_t num_ops = p_ops->binops.size;

	LOG("  generation %u, ttl %u, %u op%s:", p_ops->gen, p_ops->ttl, num_ops,
			num_ops == 1 ? "" : "s");

	for (uint16_t n = 0; n < num_ops; n++) {
		example_dump_op(&p_ops->binops.entries[n]);
	}
}

int
example_handle_udf_error(as_error* err, const char* prefix)
{
	if (strstr(err->message, " 1500:")) {
		LOG("LDT not enabled on server. Skipping example.");
		return 0;
	}
	else {
		LOG("%s returned %d - %s", prefix, err->code, err->message);
		return -1;
	}
}
