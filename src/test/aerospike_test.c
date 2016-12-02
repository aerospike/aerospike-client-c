/*
 * Copyright 2008-2016 Aerospike, Inc.
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
#include <getopt.h>

#include <aerospike/aerospike.h>
#include <aerospike/as_event.h>

#include "test.h"
#include "aerospike_test.h"

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define TIMEOUT 1000
#define SCRIPT_LEN_MAX 1048576

/******************************************************************************
 * VARIABLES
 *****************************************************************************/

aerospike * as = NULL;
int g_argc = 0;
char ** g_argv = NULL;
char g_host[MAX_HOST_SIZE];
int g_port = 3000;
static char g_user[AS_USER_SIZE];
static char g_password[AS_PASSWORD_HASH_SIZE];
bool g_tls_enable = false;
bool g_tls_encrypt_only = false;
char * g_tls_cafile = NULL;
char * g_tls_capath = NULL;
char * g_tls_protocol = NULL;
char * g_tls_cipher_suite = NULL;
bool g_tls_crl_check = false;
bool g_tls_crl_check_all = false;
char* g_tls_cert_blacklist = NULL;
bool g_tls_log_session_info = false;

#if defined(AS_USE_LIBEV) || defined(AS_USE_LIBUV) || defined(AS_USE_LIBEVENT)
static bool g_use_async = true;
#else
static bool g_use_async = false;
#endif

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

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
usage()
{
	fprintf(stderr, "Usage:\n");
	fprintf(stderr, "  -h, --host <host1>[:<tlsname1>][:<port1>],...  Default: 127.0.0.1\n");
	fprintf(stderr, "   Server seed hostnames or IP addresses.\n");
	fprintf(stderr, "   The tlsname is only used when connecting with a secure TLS enabled server.\n");
	fprintf(stderr, "   If the port is not specified, the default port is used. Examples:\n\n");
	fprintf(stderr, "   host1\n");
	fprintf(stderr, "   host1:3000,host2:3000\n");
	fprintf(stderr, "   192.168.1.10:cert1:3000,192.168.1.20:cert2:3000\n\n");

	fprintf(stderr, "  -p, --port <port>\n");
	fprintf(stderr, "    The default server port. Default: 3000.\n\n");

	fprintf(stderr, "  -U, --user <user>\n");
	fprintf(stderr, "    The user to connect as. Default: no user.\n\n");

	fprintf(stderr, "  -P[<password>], --password\n");
	fprintf(stderr, "    The user's password. If empty, a prompt is shown. Default: no password.\n\n");

	fprintf(stderr, "  -e, --tls-enable\n");
	fprintf(stderr, "    Enable TLS. Default: TLS disabled.\n\n");

	fprintf(stderr, "  -E, --tls-encrypt-only\n");
	fprintf(stderr, "    Disable TLS certificate verification. Default: enabled.\n\n");

	fprintf(stderr, "  -c, --tls-cafile <path>\n");
	fprintf(stderr, "    Set the TLS certificate authority file.\n\n");

	fprintf(stderr, "  -C, --tls-capath <path>\n");
	fprintf(stderr, "    Set the TLS certificate authority directory.\n\n");

	fprintf(stderr, "  -r, --tls-protocol\n");
	fprintf(stderr, "    Set the TLS protocol selection criteria.\n\n");

	fprintf(stderr, "  -t, --tls-cipher-suite\n");
	fprintf(stderr, "    Set the TLS cipher selection criteria.\n\n");

	fprintf(stderr, "  -Q, --tls-crl-check\n");
	fprintf(stderr, "    Enable CRL checking for leaf certs.\n\n");

	fprintf(stderr, "  -R, --tls-crl-check-all\n");
	fprintf(stderr, "    Enable CRL checking for all certs.\n\n");

	fprintf(stderr, "  -B, --tls-cert-blacklist\n");
	fprintf(stderr, "    Path to a certificate blacklist file.\n\n");

	fprintf(stderr, "  -L, --tls-log-session-info\n");
	fprintf(stderr, "    Log TLS connected session info.\n\n");

	fprintf(stderr, "  -S, --suite <suite>\n");
	fprintf(stderr, "    The suite to be run. Default: all suites.\n\n");

	fprintf(stderr, "  -T, --testcase <testcase>\n");
	fprintf(stderr, "    The test case to run. Default: all test cases.\n\n");
}

static const char* short_options = "h:p:U:uP::eEc:C:r:t:QRB:LS:T:";

static struct option long_options[] = {
	{"hosts",                  1, 0, 'h'},
	{"port",                   1, 0, 'p'},
	{"user",                   1, 0, 'U'},
	{"usage",     	           0, 0, 'u'},
	{"password",               2, 0, 'P'},
	{"tls-enable",             0, 0, 'e'},
	{"tls-encrypt-only",       0, 0, 'E'},
	{"tls-cafile",             1, 0, 'c'},
	{"tls-capath",             1, 0, 'C'},
	{"tls-protocol",           1, 0, 'r'},
	{"tls-cipher-suite",       1, 0, 't'},
	{"tls-crl-check",          0, 0, 'Q'},
	{"tls-crl-check-all",      0, 0, 'R'},
	{"tls-cert-blacklist",     1, 0, 'B'},
	{"tls-log-session-info",   0, 0, 'L'},
	{"suite",                  1, 0, 'S'},
	{"test",                   1, 0, 'T'},
	{0,                        0, 0,  0 }
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

		case 'u':
			usage();
			return false;

		case 'P':
			as_password_prompt_hash(optarg, g_password);
			break;
				
		case 'e':
			g_tls_enable = true;
			break;
				
		case 'E':
			g_tls_encrypt_only = true;
			break;
				
		case 'c':
			g_tls_cafile = strdup(optarg);
			break;
				
		case 'C':
			g_tls_capath = strdup(optarg);
			break;
				
		case 'r':
			g_tls_protocol = strdup(optarg);
			break;
				
		case 't':
			g_tls_cipher_suite = strdup(optarg);
			break;
				
		case 'Q':
			g_tls_crl_check = true;
			break;
				
		case 'R':
			g_tls_crl_check_all = true;
			break;
				
		case 'B':
			g_tls_cert_blacklist = strdup(optarg);
			break;
				
		case 'L':
			g_tls_log_session_info = true;
			break;
				
		case 'S':
			// Exclude all but the specified suite from the plan.
			atf_suite_filter(optarg);
			break;
				
		case 'T':
			// Exclude all but the specified test.
			atf_test_filter(optarg);
			break;
				
		default:
	        error("unrecognized options");
	        usage();
			return false;
		}
	}

	return true;
}

static bool before(atf_plan * plan) {

    if ( as ) {
        error("aerospike was already initialized");
        return false;
    }

	// Initialize logging.
	as_log_set_level(AS_LOG_LEVEL_INFO);
	as_log_set_callback(as_client_log_callback);
	
	if (g_use_async) {
		if (as_event_create_loops(1) == 0) {
			error("failed to create event loops");
			return false;
		}
	}
	
	// Initialize global lua configuration.
	as_config_lua lua;
	as_config_lua_init(&lua);
	strcpy(lua.system_path, "modules/lua-core/src");
	strcpy(lua.user_path, "src/test/lua");
	aerospike_init_lua(&lua);

	// Initialize cluster configuration.
	as_config config;
	as_config_init(&config);

	if (! as_config_add_hosts(&config, g_host, g_port)) {
		error("Invalid host(s) %s", g_host);
		return false;
	}

	as_config_set_user(&config, g_user, g_password);

	config.tls.enable = g_tls_enable;
	config.tls.encrypt_only = g_tls_encrypt_only;
	as_config_tls_set_cafile(&config, g_tls_cafile);
	as_config_tls_set_capath(&config, g_tls_capath);
	as_config_tls_set_protocol(&config, g_tls_protocol);
	as_config_tls_set_cipher_suite(&config, g_tls_cipher_suite);
	config.tls.crl_check = g_tls_crl_check;
	config.tls.crl_check_all = g_tls_crl_check_all;
	as_config_tls_set_cert_blacklist(&config, g_tls_cert_blacklist);
	config.tls.log_session_info = g_tls_log_session_info;
	
	as_error err;
	as_error_reset(&err);

	as = aerospike_new(&config);

	if ( aerospike_connect(as, &err) == AEROSPIKE_OK ) {
		debug("connected to %s %d", g_host, g_port);
    	return true;
	}
	else {
		error("%s @ %s[%s:%d]", err.message, err.func, err.file, err.line);
		return false;
	}
}

static bool after(atf_plan * plan) {

    if ( ! as ) {
        error("aerospike was not initialized");
        return false;
    }
	
	as_error err;
	as_error_reset(&err);
	
	as_status status = aerospike_close(as, &err);
	aerospike_destroy(as);

	if (g_use_async) {
		as_event_close_loops();
	}

	if (g_tls_cafile) {
		free(g_tls_cafile);
	}
	if (g_tls_capath) {
		free(g_tls_capath);
	}
	if (g_tls_protocol) {
		free(g_tls_protocol);
	}
	if (g_tls_cipher_suite) {
		free(g_tls_cipher_suite);
	}
	if (g_tls_cert_blacklist) {
		free(g_tls_cert_blacklist);
	}
	
	if (status == AEROSPIKE_OK) {
		debug("disconnected from %s %d", g_host, g_port);
		return true;
	}
	else {
		error("%s @ %s[%s:%d]", err.message, err.func, err.file, err.line);
		return false;
	}
}

/******************************************************************************
 * TEST PLAN
 *****************************************************************************/

PLAN(aerospike_test) {

	// This needs to be done before we add the tests.
    if (! parse_opts(g_argc, g_argv)) {
    	return;
    }
		
	plan_before(before);
	plan_after(after);

	// aerospike_key module
	plan_add(key_basics);
	plan_add(key_apply);
	plan_add(key_apply2);
	plan_add(key_operate);

	// cdt
	plan_add(list_basics);
	plan_add(map_basics);
	plan_add(map_udf);
	plan_add(map_index);

	// aerospike_info module
	plan_add(info_basics);

	// aerospike_info module
	plan_add(udf_basics);
	plan_add(udf_types);
	plan_add(udf_record);

	// aerospike_sindex module
	plan_add(index_basics);

	// aerospike_query module
	plan_add(query_foreach);
	plan_add(query_background);
    plan_add(query_geospatial);

	// aerospike_scan module
	plan_add(scan_basics);

	// aerospike_scan module
	plan_add(batch_get);

	// as_policy module
	plan_add(policy_read);
	plan_add(policy_scan);

	// as_ldt module
	plan_add(ldt_lmap);

	if (g_use_async) {
		plan_add(key_basics_async);
		plan_add(list_basics_async);
		plan_add(map_basics_async);
		plan_add(key_apply_async);
		plan_add(key_pipeline);
		plan_add(batch_async);
		plan_add(scan_async);
		plan_add(query_async);
	}
}
