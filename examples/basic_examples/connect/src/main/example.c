#include <stdio.h>
#include <unistd.h>
#include <getopt.h>
#include <errno.h>

#include <aerospike/aerospike.h>

#define IMPLEMENT_PROGRAM_OPTIONS
#include "program_options.h"


void
check_error(const char *operation, as_error *err) {
	if(err->code != AEROSPIKE_OK) {
		fprintf(stderr,
		        "Aerospike client failed while %s: "
		        "err(%d) %s at [%s:%d]\n",
			operation,
			err->code, err->message, err->file, err->line);
		exit(EXIT_FAILURE);
	}
}


void
connect_to_aerospike(aerospike* as, program_options *po, as_error *err) {
	as_config config;
	as_config_init(&config);
	as_config_add_host(&config, po->hostname, po->port);

	if(po->tls_options_given) {
		as_config_tls_set_cafile(&config, po->cafile);
		as_config_tls_set_certfile(&config, po->cert_file);
		as_config_tls_set_keyfile(&config, po->key_file);

		config.tls.enable = true;
	}

	aerospike_init(as, &config);
	aerospike_connect(as, err);
	check_error("connecting", err);
}


int
main(int argc, char *argv[]) {
	program_options po;
	program_options_parse(&po, argc, argv);

	printf("Attempting to connect to host %s port %u\n", po.hostname, po.port);
	if(po.tls_options_given) {
		printf(" using TLS with the following settings:\n");
		printf("    CA File: %s\n", po.cafile);
		printf("  CERT File: %s\n", po.cert_file);
		printf("   KEY File: %s\n", po.key_file);
	}
	else {
		printf("  without using TLS.\n");
	}

	aerospike as;
	as_error err;
	as_error_init(&err);

	connect_to_aerospike(&as, &po, &err);
	printf("Connection successful.\n");

	printf("Now closing connection.\n");
	aerospike_close(&as, &err);
	check_error("closing connection", &err);

	return 0;
}

// vim:set ts=8 sw=8 noet ai :
