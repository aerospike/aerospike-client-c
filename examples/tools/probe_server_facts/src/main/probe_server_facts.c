#include <getopt.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_info.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_config.h>
#include <aerospike/as_host.h>
#include <aerospike/as_node.h>
#include <aerospike/as_partition.h>
#include <aerospike/as_shm_cluster.h>
#include <aerospike/as_vector.h>
#include <aerospike/as_version.h>

typedef struct probe_options_s {
	const char* host;
	uint16_t port;
	const char* user;
	const char* password;
	as_auth_mode auth_mode;
	const char* namespace_name;
	bool tls_enable;
	const char* tls_ca_file;
	const char* tls_ca_path;
	const char* tls_protocols;
	const char* tls_cipher_suite;
	bool tls_crl_check;
	bool tls_crl_check_all;
	const char* tls_cert_blacklist;
	bool tls_log_session_info;
	const char* tls_key_file;
	const char* tls_cert_file;
	bool tls_login_only;
	const char* tls_name;
} probe_options;

typedef struct probe_results_s {
	char server_version[64];
	bool enterprise;
	bool ttl_support;
	bool strong_consistency;
} probe_results;

static void
usage(FILE* stream, const char* program)
{
	fprintf(stream,
			"Usage: %s [options]\n"
			"  --host <host>                Seed host list (default: 127.0.0.1)\n"
			"  --port <port>                Seed port (default: 3000)\n"
			"  --user <user>\n"
			"  --password <password>\n"
			"  --auth <mode>\n"
			"  --namespace <namespace>      Namespace to inspect (default: test)\n"
			"  --tls-enable\n"
			"  --tls-ca-file <path>\n"
			"  --tls-ca-path <path>\n"
			"  --tls-protocols <protocols>\n"
			"  --tls-cipher-suite <suite>\n"
			"  --tls-crl-check\n"
			"  --tls-crl-check-all\n"
			"  --tls-cert-blacklist <path>\n"
			"  --tls-log-session-info\n"
			"  --tls-key-file <path>\n"
			"  --tls-cert-file <path>\n"
			"  --tls-login-only\n"
			"  --tls-name <name>\n",
			program);
}

static void
failf(const char* format, ...)
{
	va_list ap;
	va_start(ap, format);
	vfprintf(stderr, format, ap);
	va_end(ap);
	fputc('\n', stderr);
}

static bool
parse_port(const char* value, uint16_t* out)
{
	char* end = NULL;
	long parsed = strtol(value, &end, 10);

	if (! value || *value == '\0' || ! end || *end != '\0') {
		return false;
	}

	if (parsed < 0 || parsed > 65535) {
		return false;
	}

	*out = (uint16_t)parsed;
	return true;
}

static bool
parse_args(int argc, char* argv[], probe_options* options)
{
	static const struct option long_options[] = {
		{"host", required_argument, NULL, 'h'},
		{"port", required_argument, NULL, 'p'},
		{"user", required_argument, NULL, 'U'},
		{"password", required_argument, NULL, 'P'},
		{"auth", required_argument, NULL, 'a'},
		{"namespace", required_argument, NULL, 'n'},
		{"tls-enable", no_argument, NULL, 't'},
		{"tls-ca-file", required_argument, NULL, 1000},
		{"tls-ca-path", required_argument, NULL, 1001},
		{"tls-protocols", required_argument, NULL, 1002},
		{"tls-cipher-suite", required_argument, NULL, 1003},
		{"tls-crl-check", no_argument, NULL, 1004},
		{"tls-crl-check-all", no_argument, NULL, 1005},
		{"tls-cert-blacklist", required_argument, NULL, 1006},
		{"tls-log-session-info", no_argument, NULL, 1007},
		{"tls-key-file", required_argument, NULL, 1008},
		{"tls-cert-file", required_argument, NULL, 1009},
		{"tls-login-only", no_argument, NULL, 1010},
		{"tls-name", required_argument, NULL, 1011},
		{"help", no_argument, NULL, 1012},
		{0, 0, 0, 0}
	};

	memset(options, 0, sizeof(*options));
	options->host = "127.0.0.1";
	options->port = 3000;
	options->namespace_name = "test";
	options->auth_mode = AS_AUTH_INTERNAL;

	for (;;) {
		int option_index = 0;
		int c = getopt_long(argc, argv, "h:p:U:P:a:n:", long_options, &option_index);

		if (c == -1) {
			break;
		}

		switch (c) {
		case 'h':
			options->host = optarg;
			break;

		case 'p':
			if (! parse_port(optarg, &options->port)) {
				failf("Invalid port value '%s'", optarg);
				return false;
			}
			break;

		case 'U':
			options->user = optarg;
			break;

		case 'P':
			options->password = optarg;
			break;

		case 'a':
			if (! as_auth_mode_from_string(&options->auth_mode, optarg)) {
				failf("Invalid auth mode '%s'", optarg);
				return false;
			}
			break;

		case 'n':
			options->namespace_name = optarg;
			break;

		case 't':
			options->tls_enable = true;
			break;

		case 1000:
			options->tls_ca_file = optarg;
			break;

		case 1001:
			options->tls_ca_path = optarg;
			break;

		case 1002:
			options->tls_protocols = optarg;
			break;

		case 1003:
			options->tls_cipher_suite = optarg;
			break;

		case 1004:
			options->tls_crl_check = true;
			break;

		case 1005:
			options->tls_crl_check_all = true;
			break;

		case 1006:
			options->tls_cert_blacklist = optarg;
			break;

		case 1007:
			options->tls_log_session_info = true;
			break;

		case 1008:
			options->tls_key_file = optarg;
			break;

		case 1009:
			options->tls_cert_file = optarg;
			break;

		case 1010:
			options->tls_login_only = true;
			break;

		case 1011:
			options->tls_name = optarg;
			break;

		case 1012:
			usage(stdout, argv[0]);
			exit(EXIT_SUCCESS);

		case '?':
		default:
			usage(stderr, argv[0]);
			return false;
		}
	}

	if (optind != argc) {
		failf("Unexpected trailing arguments");
		return false;
	}

	return true;
}

static void
configure_tls(as_config* config, const probe_options* options)
{
	config->tls.enable = options->tls_enable;

	if (options->tls_ca_file) {
		as_config_tls_set_cafile(config, options->tls_ca_file);
	}

	if (options->tls_ca_path) {
		as_config_tls_set_capath(config, options->tls_ca_path);
	}

	if (options->tls_protocols) {
		as_config_tls_set_protocols(config, options->tls_protocols);
	}

	if (options->tls_cipher_suite) {
		as_config_tls_set_cipher_suite(config, options->tls_cipher_suite);
	}

	if (options->tls_cert_blacklist) {
		as_config_tls_set_cert_blacklist(config, options->tls_cert_blacklist);
	}

	if (options->tls_key_file) {
		as_config_tls_set_keyfile(config, options->tls_key_file);
	}

	if (options->tls_cert_file) {
		as_config_tls_set_certfile(config, options->tls_cert_file);
	}

	config->tls.crl_check = options->tls_crl_check;
	config->tls.crl_check_all = options->tls_crl_check_all;
	config->tls.log_session_info = options->tls_log_session_info;
	config->tls.for_login_only = options->tls_login_only;
}

static bool
apply_seed_hosts(as_config* config, const probe_options* options)
{
	if (! as_config_add_hosts(config, options->host, options->port)) {
		failf("Invalid host list '%s'", options->host);
		return false;
	}

	if (! options->tls_name) {
		return true;
	}

	for (uint32_t i = 0; i < config->hosts->size; i++) {
		as_host* host = (as_host*)as_vector_get(config->hosts, i);

		if (! host->tls_name) {
			host->tls_name = strdup(options->tls_name);

			if (! host->tls_name) {
				failf("Failed to allocate TLS name");
				return false;
			}
		}
	}

	return true;
}

static bool
contains_ignore_case(const char* haystack, const char* needle)
{
	size_t needle_len;

	if (! haystack || ! needle) {
		return false;
	}

	needle_len = strlen(needle);

	if (needle_len == 0) {
		return true;
	}

	for (const char* p = haystack; *p; p++) {
		if (strncasecmp(p, needle, needle_len) == 0) {
			return true;
		}
	}

	return false;
}

static bool
query_enterprise(
	aerospike* as, as_error* err, as_node* node, probe_results* results
	)
{
	const char* command = (as_version_compare(&node->version, &as_server_version_8_1_1) >= 0) ?
		"release" : "edition";
	char* response = NULL;
	as_status status = aerospike_info_node(as, err, NULL, node, command, &response);

	if (status != AEROSPIKE_OK) {
		failf("Failed to query server edition: %s", err->message);
		return false;
	}

	results->enterprise = contains_ignore_case(response, "enterprise");
	cf_free(response);
	return true;
}

static bool
query_ttl_support(
	aerospike* as, as_error* err, as_node* node, const probe_options* options, probe_results* results
	)
{
	const char* ns_field_name;
	char command[1024];
	char* response = NULL;
	char* match;
	as_status status;

	if (as_version_compare(&node->version, &as_server_version_8_1) >= 0) {
		ns_field_name = "namespace";
	}
	else {
		ns_field_name = "id";
	}

	snprintf(command, sizeof(command), "get-config:context=namespace;%s=%s",
			ns_field_name, options->namespace_name);

	status = aerospike_info_node(as, err, NULL, node, command, &response);

	if (status != AEROSPIKE_OK) {
		failf("Failed to query namespace config for '%s': %s",
				options->namespace_name, err->message);
		return false;
	}

	match = strstr(response, "nsup-period=");

	if (match) {
		match += strlen("nsup-period=");
		results->ttl_support = *match != '0';
	}
	else {
		failf("Namespace config for '%s' is missing nsup-period", options->namespace_name);
		cf_free(response);
		return false;
	}

	if (! results->ttl_support) {
		match = strstr(response, "allow-ttl-without-nsup=");

		if (! match) {
			failf("Namespace config for '%s' is missing allow-ttl-without-nsup",
					options->namespace_name);
			cf_free(response);
			return false;
		}

		match += strlen("allow-ttl-without-nsup=");
		results->ttl_support = (strncmp(match, "true;", 5) == 0 || strcmp(match, "true") == 0);
	}

	cf_free(response);
	return true;
}

static bool
query_strong_consistency(
	aerospike* as, const probe_options* options, probe_results* results
	)
{
	as_cluster* cluster = as->cluster;

	if (cluster->shm_info) {
		as_cluster_shm* cluster_shm = cluster->shm_info->cluster_shm;
		as_partition_table_shm* table = as_shm_find_partition_table(
				cluster_shm, options->namespace_name);

		if (! table) {
			failf("Namespace '%s' was not found in shared-memory partition tables",
					options->namespace_name);
			return false;
		}

		results->strong_consistency = table->sc_mode != 0;
		return true;
	}

	as_partition_table* table = as_partition_tables_get(
			&cluster->partition_tables, options->namespace_name);

	if (! table) {
		failf("Namespace '%s' was not found in partition tables", options->namespace_name);
		return false;
	}

	results->strong_consistency = table->sc_mode;
	return true;
}

static bool
probe_server_facts(
	const probe_options* options, probe_results* results
	)
{
	aerospike as;
	as_config config;
	as_error err;
	as_node* node = NULL;
	bool ok = false;

	memset(results, 0, sizeof(*results));
	as_config_init(&config);

	if (! apply_seed_hosts(&config, options)) {
		return false;
	}

	as_config_set_user(&config, options->user, options->password);
	config.auth_mode = options->auth_mode;

	configure_tls(&config, options);
	aerospike_init(&as, &config);
	as_error_init(&err);

	if (aerospike_connect(&as, &err) != AEROSPIKE_OK) {
		failf("Failed to connect: %s", err.message);
		goto cleanup;
	}

	node = as_node_get_random(as.cluster);

	if (! node) {
		failf("Failed to fetch a cluster node after connecting");
		goto cleanup;
	}

	as_version_to_string(&node->version, results->server_version, sizeof(results->server_version));

	if (! query_enterprise(&as, &err, node, results)) {
		goto cleanup;
	}

	if (! query_ttl_support(&as, &err, node, options, results)) {
		goto cleanup;
	}

	if (! query_strong_consistency(&as, options, results)) {
		goto cleanup;
	}

	ok = true;

cleanup:
	if (node) {
		as_node_release(node);
	}

	if (ok) {
		if (aerospike_close(&as, &err) != AEROSPIKE_OK) {
			failf("Failed to close cluster connection: %s", err.message);
			ok = false;
		}
	}

	aerospike_destroy(&as);
	return ok;
}

static void
print_results(const probe_results* results)
{
	printf("SERVER_VERSION=%s\n", results->server_version);
	printf("SERVER_ENTERPRISE=%s\n", results->enterprise ? "true" : "false");
	printf("NAMESPACE_STRONG_CONSISTENCY=%s\n",
			results->strong_consistency ? "true" : "false");
	printf("NAMESPACE_TTL_SUPPORT=%s\n", results->ttl_support ? "true" : "false");
}

int
main(int argc, char* argv[])
{
	probe_options options;
	probe_results results;

	if (! parse_args(argc, argv, &options)) {
		return EXIT_FAILURE;
	}

	if (! probe_server_facts(&options, &results)) {
		return EXIT_FAILURE;
	}

	print_results(&results);
	return EXIT_SUCCESS;
}
