/******************************************************************************
 * Copyright 2008-2012 by Aerospike.  All rights reserved.
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 * ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 ******************************************************************************/

#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/udf.h>
#include <citrusleaf/as_hashmap.h>
#include <citrusleaf/as_buffer.h>
#include <citrusleaf/as_msgpack.h>
#include <stdio.h>

/******************************************************************************
 * CONSTANTS
 ******************************************************************************/

#define HOST    "127.0.0.1"
#define PORT    3000
#define TIMEOUT 100

/******************************************************************************
 * TYPES
 ******************************************************************************/

typedef struct config_s config;

struct config_s {
    char *  host;
    int     port;
    int     timeout;
};

/******************************************************************************
 * MACROS
 ******************************************************************************/

#define LOG(msg, ...) \
    { printf("%s:%d - ", __FILE__, __LINE__); printf(msg, ##__VA_ARGS__ ); printf("\n"); }

/******************************************************************************
 * STATIC FUNCTION DECLARATIONS
 ******************************************************************************/

static int usage(const char * program);
static int configure(config * c, int argc, char *argv[]);

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

int main(int argc, char ** argv) {
    
    int rc = 0;
    const char * program = argv[0];

    config c = {
        .host       = HOST,
        .port       = PORT,
        .timeout    = TIMEOUT
    };

    rc = configure(&c, argc, argv);

    if ( rc != 0 ) {
        return rc;
    }

    argv += optind;
    argc -= optind;

    if ( argc != 1 ) {
        LOG("invalid arguments.");
        usage(program);
        return 1;
    }

    char *          filename    = argv[0];
    cl_cluster *    cluster     = NULL;
    char *          content     = NULL;
    int             content_len = 0;
    char *          error       = NULL;

    citrusleaf_init();

    cluster = citrusleaf_cluster_create();
    citrusleaf_cluster_add_host(cluster, c.host, c.port, c.timeout);

    rc = citrusleaf_udf_get(cluster, filename, &content, &content_len, &error);
    
    if ( rc ) {
        printf("error: %s\n", error);
        free(error);
        error = NULL;
    }
    else {
        puts(content);
        free(content);
        content = NULL;
    }

    return rc;
}


static int usage(const char * program) {
    fprintf(stderr, "Usage %s:\n", program);
    fprintf(stderr, "-h host [default 127.0.0.1] \n");
    fprintf(stderr, "-p port [default 3000]\n");
    return 0;
}

static int configure(config * c, int argc, char *argv[]) {
    int optcase;
    while ((optcase = getopt(argc, argv, "h:p:")) != -1) {
        switch (optcase) {
            case 'h':   c->host = strdup(optarg); break;
            case 'p':   c->port = atoi(optarg); break;
            default:    return usage(argv[0]);
        }
    }
    return 0;
}
