/*
 * Aerospike Demo
 * ad_udf.cpp - Demonstrate using Aerospike UDFs for ad campaigns.
 *
 *  Copyright 2013 by Aerospike.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 *
 *  SYNOPSIS
 *    Load and execute the example ad campaign UDFs.
 */

extern "C" {
#include <errno.h>
#include <getopt.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_udf.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_bin.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_error.h>
#include <aerospike/as_config.h>
#include <aerospike/as_key.h>
#include <aerospike/as_list.h>
#include <aerospike/as_record.h>
#include <aerospike/as_record_iterator.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>
}

#include <iostream>
using namespace std;

// Class representing the Ad UDF application.
class Ad_Udf {
  public:
	Ad_Udf(int argc, char *argv[]);
	~Ad_Udf(void);

	int init(void);
	int register_module(void);

	int run(void);
	int dump_user_record(int user_id);
	int do_udf_user_write(int user_id);
	int do_udf_user_read(int user_id);

  protected:
	const char *host;
	int port;
	const char *ns;
	const char *set;

	uint32_t timeout_ms;
	uint32_t record_ttl;

	bool verbose;

	const char *module_file;
	const char *module_name;

	int n_behaviors;
	int n_users;

	aerospike as;
	as_config config;
};

// Default values.
const char *default_host = "127.0.0.1";
const int default_port = 3000;
const int default_n_behaviors = 1000;
const int default_n_users = 100;

// UDF module values.
#define LUA_MODULE_PATH   "src/lua"
#define UDF_MODULE        "ad_udf"
#define UDF_FILE          UDF_MODULE".lua"

// Parameters for random test data generation.
#define CLICK_RATE 100
#define N_CAMPAIGNS 10

static int usage(int argc, char *argv[])
{
	cout << "Usage: " << argv[0] << " <Options>\n";
	cout << "  where <Options> are:\n";
	cout << "    -h host [default " << default_host << "]\n";
	cout << "    -p port [default " << default_port << "]\n";
	cout << "    -n namespace [default test]\n";
	cout << "    -s set [default *all*]\n";
	cout << "    -v verbose [default false]\n";
	cout << "    -f udf_file [default \"" << LUA_MODULE_PATH "/" UDF_FILE << "\"]\n";
	cout << "    -P udf_module [default \"" << UDF_MODULE << "\"]\n";
	cout << "    -b n_behaviors [default " << default_n_behaviors << "]\n";
	cout << "    -u n_users [default " << default_n_users << "]\n";

	exit(-1);
}

Ad_Udf::Ad_Udf(int argc, char *argv[])
{
	host         = default_host;
	port         = default_port;
	ns           = "test";
	set          = "demo";
	timeout_ms   = 1000;
	record_ttl   = 864000;
	verbose      = false;
	module_file  = LUA_MODULE_PATH "/" UDF_FILE;
	module_name  = UDF_MODULE;
	n_behaviors  = default_n_behaviors;
	n_users      = default_n_users;

	int optcase;
	while ((optcase = getopt(argc, argv, "b:h:p:n:s:P:f:v:u:")) != -1) {
		switch (optcase) {
		  case 'h':
			  host = strdup(optarg);
			  break;
		  case 'p':
			  port = atoi(optarg);
			  break;
		  case 'n':
			  ns = strdup(optarg);
			  break;
		  case 's':
			  set = strdup(optarg);
			  break;
		  case 'v':
			  verbose = true;
			  break;
		  case 'f':
			  module_file = strdup(optarg);
			  break;
		  case 'P':
			  module_name = strdup(optarg);
			  break;
		  case 'b':
			  n_behaviors = atoi(optarg);
			  break;
		  case 'u':
			  n_users = atoi(optarg);
			  break;
		  default:
			  usage(argc, argv);
		}
	}
}

Ad_Udf::~Ad_Udf(void)
{
	aerospike_destroy(&as);

	cout << "\nFinished Ad UDF Example Program.\n";
}

int Ad_Udf::init(void)
{
	int rv = 0;
	as_error err;

	cout << "Startup: host " << host << " port " << port << " ns " << ns << " set " << set << " file \"" << module_file << "\"\n";

	as_config_init(&config);
	config.hosts[0].addr = host;
	config.hosts[0].port = port;
	config.policies.read.timeout = timeout_ms;
	config.policies.apply.timeout = timeout_ms;

	aerospike_init(&as, &config);

	if (AEROSPIKE_OK != aerospike_connect(&as, &err)) {
		cout << "aerospike_connect() failed with error: \"" << err.message << "\" (" << err.code << ")\n";
		return -1;
	}

	cout << "Connected to Aerospike cluster.\n";

	// Register module. 
	if ((rv = register_module())) {
		return rv;
	}

	cout << "Registered Ad UDF module.\n";

	return rv;
}

int Ad_Udf::register_module(void)
{ 
	cout << "Opening module file \"" << module_file << "\"\n";
	FILE *fptr = fopen(module_file, "r");
	if (!fptr) {
		cout << "Cannot open script file \"" << module_file << "\" : " <<  strerror(errno) << "\n";
		return -1;
	}
	int max_script_len = 1048576;
	uint8_t *script_code = (uint8_t *)malloc(max_script_len);
	if (script_code == NULL) {
		cout << "malloc failed\n";
		fclose(fptr);
		return -1;
	}

	uint8_t *script_ptr = script_code;
	int b_read = fread(script_ptr, 1, 512, fptr);
	int b_tot = 0;
	while (b_read) {
		b_tot      += b_read;
		script_ptr += b_read;
		b_read      = fread(script_ptr, 1, 512, fptr);
	}
	fclose(fptr);
	as_bytes udf_content;
	as_bytes_init_wrap(&udf_content, script_code, b_tot, true /*is_malloc*/);

	if (b_tot > 0) {
		as_error err;
		as_string base_string;
		const char* base = as_basename(&base_string, module_file);

		if (AEROSPIKE_OK != aerospike_udf_put(&as, &err, NULL, base, AS_UDF_TYPE_LUA, &udf_content)) {
			cout << "Unable to register module file \"" << module_file << "\" as \"" << module_name << "\" rv = " << err.code << "\n";
			return -1;
		}

		as_string_destroy(&base_string);

		cout << "Successfully registered module file \"" <<  module_file << "\" as \"" << module_name << "\"\n";
		// For now we need to wait a little to make sure the UDF package is
		// distributed through the cluster before we can use it.
		usleep(1000 * 100);
	} else {
		cout << "Unable to read module file \"" << module_file << "\" as \"" << module_name << "\" b_tot = " << b_tot << "\n";
		return -1;
	}
	as_bytes_destroy(&udf_content);

	return 0;
}

int Ad_Udf::run(void)
{
	as_error err;

	// Write behavior into the database.
	cout << "\n*** WRITING " << n_behaviors << " behavioral points for " << n_users << " users\n";
	for (int i = 0; i < n_behaviors; i++) {
		do_udf_user_write(rand() % n_users);
	}

	// For all possible users, do the first operation: read.
	cout << "\n*** READING behaviors for " << n_users << " users\n";
	for (int i = 0; i < n_users; i++) {
		do_udf_user_read(i);
	}

	if (AEROSPIKE_OK != aerospike_close(&as, &err)) {
		cout << "Error: aerospike_close() failed with (" << err.code << ") " << err.message << "\n";
	}

	return 0;
}

int Ad_Udf::dump_user_record(int user_id)
{
	cout << "Reading user(" << user_id << ")\n";

	// The key for the user record
	// Keys can contain either string, integer or binary values
	as_key key;
	as_key_init_int64(&key, ns, set, user_id);

	// Error to be populated
	as_error err;

	// Record to be populated with the data from the database.
	as_record *rec = NULL;

	if (AEROSPIKE_OK != aerospike_key_get(&as, &err, NULL, &key, &rec)) {
		cout << "Error: user(" << user_id << ") : get failed with (" << err.code << ") " << err.message << "\n";
		return -1;
	}

	cout << "user(" << user_id << ") : get returned " << as_record_numbins(rec) << " bins\n";

	// counter for the bins
	int i = 0;

	// iterator over bins
	as_record_iterator it;
	as_record_iterator_init(&it, rec);

	while (as_record_iterator_has_next(&it)) {
		// we expect the bins to contain a list of [action,timestamp]

		as_bin 	*bin 		= as_record_iterator_next(&it);
		char 	*bin_name 	= as_bin_get_name(bin);
		as_list *values 	= as_list_fromval((as_val *) as_bin_get_value(bin));

		if (values) {
			char * v = as_val_tostring(values);
			cout << "user(" << user_id << ") : bin[" << i << "] name=" << bin_name << " value=" << v+4 << "\n";
			free(v);
		}
		else {
			cout << "Error: user(" << user_id << ") : bin[" << i << "] name=" << bin_name << " has unexpected type " << as_bin_get_type(bin) << "\n";
		}

		i++;
	}

	// release the iterator
	as_record_iterator_destroy(&it);

	// release the record
	as_record_destroy(rec);

	return 0;
}

int Ad_Udf::do_udf_user_write(int user_id)
{
	if (verbose) {
		cout << "Writing user(" << user_id << ")\n";
	}

	// The key for the user record
	// Keys can contain either string, integer or binary values
	as_key key;
	as_key_init_int64(&key, ns, set, user_id);

	// The timestamp for the action
	time_t timestamp = time(0) - (rand() % (60 * 60 * 24));  // fake: last 1 day

	// The action that was taken
	const char *action = "imp";
	if (rand() % CLICK_RATE == 0) {
		action = "click";
	}

	// The campaign the action applies to
	int campaign_id = rand() % N_CAMPAIGNS;

	// Build the argument list for the UDF
	as_arraylist arglist;
	as_arraylist_inita(&arglist, 3);
	as_arraylist_append_int64(&arglist, campaign_id);
	as_arraylist_append_str(&arglist, action);
	as_arraylist_append_int64(&arglist, (int64_t) timestamp);

	// Error to be populated
	as_error err;

	// The result from the UDF call
	as_val *res = NULL;

	if (verbose) {
		// Print the UDF call.
		char * a = as_val_tostring(&arglist);
		cout << "user(" << user_id << ") : put_behavior" << a+4 << "\n";
		free(a);
	}

	if (AEROSPIKE_OK != aerospike_key_apply(&as, &err, NULL, &key, module_name, "put_behavior", (as_list *) &arglist, &res)) {
		cout << "Error: user(" << user_id << ") : put_behavior() failed with (" << err.code << ") " << err.message << ")\n";
		as_arraylist_destroy(&arglist);
		return -1;
	}

	if (verbose) {
		// Print the UDF call and result.
		char * a = as_val_tostring(&arglist);
		char * r = as_val_tostring(res);
		cout << "user(" << user_id << ") : put_behavior" << a+4 << " returned " << r << "\n";
		free(a);
		free(r);
	}

	// Done with the argument list.
	as_arraylist_destroy(&arglist);

	// Done with the result.
	as_val_destroy(res);

	if (verbose) {
		dump_user_record(user_id);
	}

	return 0;
}

int Ad_Udf::do_udf_user_read(int user_id)
{
	int rv = 0;

	if (verbose) {
		dump_user_record(user_id);
	}

	// The key for the user record
	// Keys can contain either string, integer or binary values
	as_key key;
	as_key_init_int64(&key, ns, set, user_id);

	// randomly choose which campaigns to read from
	int campaign1 = rand() % N_CAMPAIGNS;
	int campaign2 = 0;

	do {
		campaign2 = rand() % N_CAMPAIGNS;
	} while (campaign1 == campaign2);

	// Build the argument list for the UDF
	as_arraylist arglist;
	as_arraylist_inita(&arglist, 2);
	as_arraylist_append_int64(&arglist, campaign1);
	as_arraylist_append_int64(&arglist, campaign2);

	// Error to be populated
	as_error err;

	// Execute the UDF, print the result.
	as_val *res = NULL;

	if (verbose) {
		// Print the args.
		char * a = as_val_tostring(&arglist);
		cout << "user(" << user_id << ") : get_campaign" << a+4 << "\n";
		free(a);
	}

	if (AEROSPIKE_OK != aerospike_key_apply(&as, &err, NULL, &key, module_name, "get_campaign", (as_list *) &arglist, &res)) {
		cout << "Error: user(" << user_id << ") : get_campaign() failed with (" << err.code << ") " << err.message << "\n";
		as_arraylist_destroy(&arglist);
		return -1;
	}

	if (verbose) {
		// Print the UDF call and result.
		char * a = as_val_tostring(&arglist);
		char * r = as_val_tostring(res);
		cout << "user(" << user_id << ") : get_campaign" << a+4 << " returned " << r << "\n";
		free(a);
		free(r);
	}

	// Done with the argument list.
	as_arraylist_destroy(&arglist);

	// Check for the required number of campaigns.
	as_map *m = as_map_fromval(res);
	int map_size = as_map_size(m);
	
	if (map_size != 2) {
		cout << "Error: user(" << user_id << ") : expected 2 campaigns, got " << map_size << "\n";
		rv = -1;
	}

	// release the result
	as_val_destroy(res);

	return rv;
}

int main(int argc, char *argv[])
{
	int rv = 0;

	cout << "Starting Ad UDF Example Program:\n";

	Ad_Udf app(argc, argv);

	if ((rv = app.init())) {
		return rv;
	}

	return app.run();
}
