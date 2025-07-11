#pragma once

#include <stdint.h>
#include <stdlib.h>


typedef struct program_options_s {
	char *   hostname;
	uint16_t port;
} program_options;


program_options *program_options_new(void);
void             program_options_init(program_options *);


#ifdef IMPLEMENT_PROGRAM_OPTIONS
enum {
	OPT_DONE = -1,
	OPT_HELP = 'h',

	OPT_HOST = 'r',
	OPT_PORT = 'p',
};


static const struct option longopts[] = {
	{"help", no_argument,       NULL, OPT_HELP},
	{"host", required_argument, NULL, OPT_HOST},
	{"port", required_argument, NULL, OPT_PORT},
	{NULL,   0,                 NULL, 0},
};


static const char *optstring = "h?r:p:";


void
program_options_init(program_options *po) {
	if(po) {
		po->hostname = "127.0.0.1";
		po->port = 3000;
	}
}


static void
print_usage(const char *cmdname) {
	printf("%s [options] "
	       "-r|--host <remote host> "
	       "-p|--port <port>\n\n", cmdname);
	printf("where [options] can be one or more of:\n");
	printf("  -?,-h  --help  Displays this message and quits.\n");
}


void
program_options_parse(program_options *po, int argc, char *argv[]) {
	program_options_init(po);

	for(int done = 0; !done;) {
		int res = getopt_long(
			argc, argv, optstring, longopts, NULL
		);

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
			po->hostname = optarg;
			break;

		case OPT_PORT:
			errno = 0;
			unsigned long p = strtoul(optarg, NULL, 10);
			if(errno != 0) {
				perror("strtoul");
				exit(EXIT_FAILURE);
			}

			if(!((0 <= p) && (p < 65536))) {
				fprintf(stderr, "Port must fall between 0 and 65535 inclusive.\n");
				exit(EXIT_FAILURE);
			}

			po->port = (uint16_t)p;
			break;
		}
	}
}

#endif

