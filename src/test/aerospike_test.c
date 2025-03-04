/*
 * Copyright 2008-2024 Aerospike, Inc.
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

#if defined(_MSC_VER)
#undef _UNICODE  // Use ASCII getopt version on windows.
#endif
#include <getopt.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_info.h>
#include <aerospike/as_event.h>
#include <aerospike/as_metrics.h>
#include <aerospike/as_shm_cluster.h>

#include "test.h"
#include "aerospike_test.h"

//---------------------------------
// Macros
//---------------------------------

#define TIMEOUT 1000
#define SCRIPT_LEN_MAX 1048576

//---------------------------------
// Global Variables
//---------------------------------

aerospike * as = NULL;
int g_argc = 0;
char ** g_argv = NULL;
char g_host[MAX_HOST_SIZE];
int g_port = 3000;
static char g_user[AS_USER_SIZE];
static char g_password[AS_PASSWORD_SIZE];
as_config_tls g_tls = {0};
as_auth_mode g_auth_mode = AS_AUTH_INTERNAL;
bool g_enterprise_server = false;
bool g_has_ttl = false;
bool g_has_sc = false;

//---------------------------------
// Static Functions
//---------------------------------

static bool
as_client_log_callback(as_log_level level, const char * func, const char * file, uint32_t line, const char * fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	atf_logv(stderr, as_log_level_tostring(level), ATF_LOG_PREFIX, NULL, 0, fmt, ap);
	va_end(ap);
	return true;
}

static void
usage(void)
{
	fprintf(stderr, "Usage:\n");
	fprintf(stderr, "  -h, --host <host1>[:<tlsname1>][:<port1>],...  Default: 127.0.0.1\n");
	fprintf(stderr, "  Server seed hostnames or IP addresses.\n");
	fprintf(stderr, "  The tlsname is only used when connecting with a secure TLS enabled server.\n");
	fprintf(stderr, "  If the port is not specified, the default port is used. Examples:\n\n");
	fprintf(stderr, "  host1\n");
	fprintf(stderr, "  host1:3000,host2:3000\n");
	fprintf(stderr, "  192.168.1.10:cert1:3000,192.168.1.20:cert2:3000\n\n");

	fprintf(stderr, "  -p, --port <port>\n");
	fprintf(stderr, "  The default server port. Default: 3000.\n\n");

	fprintf(stderr, "  -U, --user <user>\n");
	fprintf(stderr, "  The user to connect as. Default: no user.\n\n");

	fprintf(stderr, "  -P[<password>], --password\n");
	fprintf(stderr, "  The user's password. If empty, a prompt is shown. Default: no password.\n\n");

	fprintf(stderr, "  -S, --suite <suite>\n");
	fprintf(stderr, "  The suite to be run. Default: all suites.\n\n");

	fprintf(stderr, "  -T, --testcase <testcase>\n");
	fprintf(stderr, "  The test case to run. Default: all test cases.\n\n");

	fprintf(stderr, "  --tlsEnable         # Default: TLS disabled\n");
	fprintf(stderr, "  Enable TLS.\n\n");

	fprintf(stderr, "  --tlsCaFile <path>\n");
	fprintf(stderr, "  Set the TLS certificate authority file.\n\n");

	fprintf(stderr, "  --tlsCaPath <path>\n");
	fprintf(stderr, "  Set the TLS certificate authority directory.\n\n");

	fprintf(stderr, "  --tlsProtocols <protocols>\n");
	fprintf(stderr, "  Set the TLS protocol selection criteria.\n\n");

	fprintf(stderr, "  --tlsCipherSuite <suite>\n");
	fprintf(stderr, "  Set the TLS cipher selection criteria.\n\n");

	fprintf(stderr, "  --tlsCrlCheck\n");
	fprintf(stderr, "  Enable CRL checking for leaf certs.\n\n");

	fprintf(stderr, "  --tlsCrlCheckAll\n");
	fprintf(stderr, "  Enable CRL checking for all certs.\n\n");

	fprintf(stderr, "  --tlsCertBlackList <path>\n");
	fprintf(stderr, "  Path to a certificate blacklist file.\n\n");

	fprintf(stderr, "  --tlsLogSessionInfo\n");
	fprintf(stderr, "  Log TLS connected session info.\n\n");

	fprintf(stderr, "  --tlsKeyFile <path>\n");
	fprintf(stderr, "  Set the TLS client key file for mutual authentication.\n\n");

	fprintf(stderr, "  --tlsCertFile <path>\n");
	fprintf(stderr, "  Set the TLS client certificate chain file for mutual authentication.\n\n");

	fprintf(stderr, "  --tlsLoginOnly\n");
	fprintf(stderr, "  Use TLS sockets on node login only.\n\n");

	fprintf(stderr, "  --auth {INTERNAL,EXTERNAL,EXTERNAL_SECURE,PKI} # Default: INTERNAL\n");
	fprintf(stderr, "  Set authentication mode when user/password is defined.\n\n");

	fprintf(stderr, "  -u --usage         # Default: usage not printed.\n");
	fprintf(stderr, "  Display program usage.\n\n");
}

static const char* short_options = "h:p:U:P::S:T:u";

static struct option long_options[] = {
	{"hosts",                required_argument, 0, 'h'},
	{"port",                 required_argument, 0, 'p'},
	{"user",                 required_argument, 0, 'U'},
	{"password",             optional_argument, 0, 'P'},
	{"suite",                required_argument, 0, 'S'},
	{"test",                 required_argument, 0, 'T'},
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
	{"tlsLoginOnly",         no_argument,       0, 'Y'},
	{"auth",                 required_argument, 0, 'e'},
	{"usage",                no_argument,       0, 'u'},
	{0, 0, 0, 0}
};

static bool parse_opts(int argc, char* argv[])
{
	int option_index = 0;
	int c;

	strcpy(g_host, "127.0.0.1");

	while ((c = getopt_long(argc, argv, short_options, long_options, &option_index)) != -1) {
		switch (c) {
		case 'h':
			if (strlen(optarg) >= sizeof(g_host)) {
				error("ERROR: host exceeds max length");
				return false;
			}
			strcpy(g_host, optarg);
			error("host:           %s", g_host);
			break;

		case 'p':
			g_port = atoi(optarg);
			break;

		case 'U':
			if (strlen(optarg) >= sizeof(g_user)) {
				error("ERROR: user exceeds max length");
				return false;
			}
			strcpy(g_user, optarg);
			error("user:           %s", g_user);
			break;

		case 'P':
			as_password_acquire(g_password, optarg, AS_PASSWORD_SIZE);
			break;

		case 'S':
			// Exclude all but the specified suite from the plan.
			atf_suite_filter(optarg);
			break;

		case 'T':
			// Exclude all but the specified test.
			atf_test_filter(optarg);
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

		case 'Y':
			g_tls.for_login_only = true;
			break;

		case 'e':
			if (! as_auth_mode_from_string(&g_auth_mode, optarg)) {
				error("ERROR: invalid authentication mode: %s", optarg);
				return false;
			}
			break;

		case 'u':
			usage();
			return false;

		default:
			error("unrecognized options");
			usage();
			return false;
		}
	}

	return true;
}

static bool before(atf_plan* plan)
{
	if ( as ) {
		error("aerospike was already initialized");
		return false;
	}

	// Initialize logging.
	as_log_set_level(AS_LOG_LEVEL_INFO);
	as_log_set_callback(as_client_log_callback);

#if AS_EVENT_LIB_DEFINED
	if (as_event_create_loops(1) == 0) {
		error("failed to create event loops");
		return false;
	}
#endif

	// Initialize global lua configuration.
	as_config_lua lua;
	as_config_lua_init(&lua);
	strcpy(lua.user_path, AS_START_DIR "src/test/lua");
	aerospike_init_lua(&lua);

	// Initialize cluster configuration.
	as_config config;
	as_config_init(&config);

	if (! as_config_add_hosts(&config, g_host, g_port)) {
		error("Invalid host(s) %s", g_host);
		as_event_close_loops();
		return false;
	}

	as_config_set_user(&config, g_user, g_password);

	// Transfer ownership of all heap allocated TLS fields via shallow copy.
	memcpy(&config.tls, &g_tls, sizeof(as_config_tls));
	config.auth_mode = g_auth_mode;

	as_error err;
	as_error_reset(&err);

	as = aerospike_new(&config);

	if (aerospike_connect(as, &err) != AEROSPIKE_OK) {
		error("%s @ %s[%s:%d]", err.message, err.func, err.file, err.line);
		aerospike_destroy(as);
		as_event_close_loops();
		return false;
	}

	char* result;
	if (aerospike_info_any(as, &err, NULL, "edition", &result) != AEROSPIKE_OK) {
		error("%s @ %s[%s:%d]", err.message, err.func, err.file, err.line);
		aerospike_close(as, &err);
		aerospike_destroy(as);
		as_event_close_loops();
		return false;
	}

	if (strstr(result, "Aerospike Enterprise Edition") != NULL) {
		g_enterprise_server = true;
	}

	cf_free(result);

	if (aerospike_info_any(as, &err, NULL, "get-config:context=namespace;id=test", &result)
		!= AEROSPIKE_OK) {
		error("%s @ %s[%s:%d]", err.message, err.func, err.file, err.line);
		aerospike_close(as, &err);
		aerospike_destroy(as);
		as_event_close_loops();
		return false;
	}

	// Run ttl tests if nsup-period or allow-ttl-without-nsup is defined.
	const char* search = "nsup-period=";
	char* p = strstr(result, search);

	if (p) {
		p += strlen(search);
		g_has_ttl = *p != '0';
	}
	else {
		error("Failed to find namespace config nsup-period");
		cf_free(result);
		return false;
	}

	if (! g_has_ttl) {
		search = "allow-ttl-without-nsup=";
		p = strstr(result, search);

		if (p) {
			p += strlen(search);
			g_has_ttl = (strncmp(p, "true;", 5) == 0 || strcmp(p, "true") == 0);
		}
		else {
			error("Failed to find namespace config allow-ttl-without-nsup");
			cf_free(result);
			return false;
		}
	}
	cf_free(result);

	// Determine if namespace is configured as strong consistency.
	as_cluster* cluster = as->cluster;
	const char* ns = "test";

	if (cluster->shm_info) {
		as_cluster_shm* cluster_shm = cluster->shm_info->cluster_shm;
		as_partition_table_shm* table = as_shm_find_partition_table(cluster_shm, ns);
		g_has_sc = table ? table->sc_mode : false;
	}
	else {
		as_partition_table* table = as_partition_tables_get(&cluster->partition_tables, ns);
		g_has_sc = table ? table->sc_mode : false;
	}

	/*
	// Test metrics
	as_metrics_policy policy;
	as_metrics_policy_init(&policy);

	as_status status = aerospike_enable_metrics(as, &err, &policy);

	if (status != AEROSPIKE_OK) {
		error("aerospike_enable_metrics() returned %d - %s", err.code, err.message);
	}
	*/
	
	return true;
}

static bool after(atf_plan* plan)
{
	if ( ! as ) {
		error("aerospike was not initialized");
		return false;
	}

	as_error err;
	as_error_reset(&err);

	as_status status = aerospike_close(as, &err);
	aerospike_destroy(as);

#if AS_EVENT_LIB_DEFINED
	as_event_close_loops();
#endif

	if (status == AEROSPIKE_OK) {
		return true;
	}
	else {
		error("%s @ %s[%s:%d]", err.message, err.func, err.file, err.line);
		return false;
	}
}

//---------------------------------
// Test Plan
//---------------------------------

PLAN(aerospike_test)
{
	if (! parse_opts(g_argc, g_argv)) {
		return;
	}

	plan_before(before);
	plan_after(after);

	plan_add(key_basics);
	plan_add(key_apply);
	plan_add(key_apply2);
	plan_add(key_operate);
	plan_add(list_basics);
	plan_add(map_basics);
	plan_add(map_udf);
	plan_add(map_index);
	plan_add(map_sort);
	plan_add(bit);
	plan_add(hll);
	plan_add(filter_exp);
	plan_add(exp_operate);
	plan_add(info_basics);
	plan_add(udf_basics);
	plan_add(udf_types);
	plan_add(udf_record);
	plan_add(index_basics);
	plan_add(query_foreach);
	plan_add(query_background);
	plan_add(query_geospatial);
	plan_add(scan_basics);
	plan_add(batch);
	plan_add(transaction);

#if AS_EVENT_LIB_DEFINED
	plan_add(key_basics_async);
	plan_add(list_basics_async);
	plan_add(map_basics_async);
	plan_add(key_apply_async);
	plan_add(key_pipeline);
	plan_add(batch_async);
	plan_add(scan_async);
	plan_add(query_async);
	plan_add(transaction_async);
#endif
}
