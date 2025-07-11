#include <errno.h>
#include <getopt.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <aerospike/aerospike.h>

////// Parse Command Line Arguments

// The result of parsing arguments from the CLI.
typedef struct program_options_s {
	char* hostname;
	uint16_t port;
	bool tls_options_given;
	char* cafile;
	char* cert_file;
	char* key_file;
	char* tls_name;
} program_options;

// If you alter any of these assignments, be sure to update
// `optstring` below as well.

enum {
	OPT_DONE = -1,
	OPT_HELP = 'h',
	OPT_HOST = 'r',
	OPT_PORT = 'p',
	OPT_CAFILE = 'a',
	OPT_CERTFILE = 'c',
	OPT_KEYFILE = 'k',
	OPT_TLS_NAME = 't',
};

static const struct option longopts[] = {
	{"help",         no_argument,       NULL, OPT_HELP},
	{"host",         required_argument, NULL, OPT_HOST},
	{"port",         required_argument, NULL, OPT_PORT},
	{"ca-file",      required_argument, NULL, OPT_CAFILE},
	{"cert-file",    required_argument, NULL, OPT_CERTFILE},
	{"key-file",     required_argument, NULL, OPT_KEYFILE},
	{"cluster-name", required_argument, NULL, OPT_TLS_NAME},
	{NULL,           0,                 NULL, 0},
};

static const char* optstring = "h?r:p:a:c:k:t:";

static void
program_options_init(program_options* po)
{
	if(po) {
		memset(po, 0, sizeof(program_options));

		po->hostname = "127.0.0.1";
		po->port = 3000;
	}
}

static void
print_usage(const char* cmdname)
{
	printf("%s [options] "
	       "-r|--host <remote host> "
	       "-p|--port <port>\n\n", cmdname);
	printf("where [options] can be one or more of:\n");
	printf("  -?,-h  --help              Displays this message and quits.\n");
	printf("  -a     --ca-file <path>    Gives path to CA Certificate file\n");
	printf("  -c     --cert-file <path>  Gives path to TLS certificate file\n");
	printf("  -k     --key-file <path>   Gives path to TLS key file\n");
	printf("  -t     --tls-name <name>   Gives the TLS cluster name\n");
	printf("\nNote that, at a minimum, -a and -t must be specified for "
	       "TLS to work.\n");
}

void
program_options_parse(program_options* po, int argc, char* argv[])
{
	program_options_init(po);

	for (int done = 0; !done; ) {
		int res = getopt_long(argc, argv, optstring, longopts, NULL);

		switch(res) {
		default:
			printf("Unknown getopt_long() result: %u\n", res);
			// fall through intended.

		case OPT_DONE:
			done++;
			break;

		case '?':
		case OPT_HELP:
			print_usage(argv[0]);
			exit(EXIT_SUCCESS);

		case OPT_HOST:
			po->hostname = strdup(optarg);
			break;

		case OPT_PORT:
			errno = 0;
			unsigned long p = strtoul(optarg, NULL, 10);
			if(errno != 0) {
				perror("strtoul");
				exit(EXIT_FAILURE);
			}

			if(!((0 <= p) && (p < 65536))) {
				fprintf(stderr,
				        "Port must fall between 0 "
				        "and 65535 inclusive.\n");
				exit(EXIT_FAILURE);
			}

			po->port = (uint16_t)p;
			break;

		case OPT_CAFILE:
			po->cafile = strdup(optarg);
			break;

		case OPT_CERTFILE:
			po->cert_file = strdup(optarg);
			break;

		case OPT_KEYFILE:
			po->key_file = strdup(optarg);
			break;

		case OPT_TLS_NAME:
			po->tls_name = strdup(optarg);
			break;
		}
	}

	// Set tls_options_given only if the minimum set of parameters
	// are given.
	po->tls_options_given = po->cafile && po->tls_name;
}

void
check_error(const char* operation, as_error* err)
{
	if (err->code != AEROSPIKE_OK) {
		fprintf(stderr,
		        "Aerospike client failed while %s: "
		        "err(%d) %s at [%s:%d]\n",
			operation,
			err->code, err->message, err->file, err->line);
		exit(EXIT_FAILURE);
	}
}

void
connect_to_aerospike(aerospike* as, program_options* po, as_error* err)
{
	as_config config;
	as_config_init(&config);

	// This adds the provided host as a seed.  This may yield a number
	// of additional hosts if the cluster size is greater than one.
	as_config_add_host(&config, po->hostname, po->port);

	if (po->tls_options_given) {
		config.tls.enable = true;

		as_config_tls_set_cafile(&config, po->cafile);
		as_config_tls_set_certfile(&config, po->cert_file);
		as_config_tls_set_keyfile(&config, po->key_file);

		// After discovering the complete set of hosts in the cluster,
		// we need to configure each of them with the TLS name we wish
		// to connect to.  Failing to do that will yield connection
		// failures when attempting to connect.

		for (int i = 0; i < config.hosts->capacity; i++) {
			as_host* host = (as_host*)as_vector_get(config.hosts, i);

			if( !host->tls_name) {
				host->tls_name = strdup(po->tls_name);
			}
		}
	}

	aerospike_init(as, &config);
	aerospike_connect(as, err);
	check_error("connecting", err);
}

int
main(int argc, char* argv[])
{
	program_options po;
	program_options_parse(&po, argc, argv);

	printf("Attempting to connect to host %s port %u\n", po.hostname, po.port);
	if (po.tls_options_given) {
		printf(" using TLS with the following settings:\n");
		printf("    CA File: %s\n", po.cafile);
		printf("  CERT File: %s\n", po.cert_file);
		printf("   KEY File: %s\n", po.key_file);
		printf("   TLS Name: %s\n", po.tls_name);
	}
	else {
		printf("  without using TLS.\n");
	}

	aerospike as;
	as_error err;
	as_error_init(&err);

	connect_to_aerospike(&as, &po, &err);
	printf("Connection successful.\n");

	// ...

	printf("Now closing connection.\n");
	aerospike_close(&as, &err);
	check_error("closing connection", &err);

	return 0;
}

