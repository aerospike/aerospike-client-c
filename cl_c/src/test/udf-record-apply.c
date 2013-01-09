/******************************************************************************
 * Copyright 2008-2012 by Aerospike.  All rights reserved.
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 * ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 ******************************************************************************/

#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/cl_udf.h>
#include <citrusleaf/as_hashmap.h>
#include <citrusleaf/as_buffer.h>
#include <citrusleaf/as_msgpack.h>
#include <citrusleaf/as_linkedlist.h>
#include <citrusleaf/as_arraylist.h>
#include <citrusleaf/as_hashmap.h>
#include <stdio.h>
#include "as_json.h"

/******************************************************************************
 * CONSTANTS
 ******************************************************************************/

#define ADDR    "127.0.0.1"
#define PORT    3000
#define TIMEOUT 100

/******************************************************************************
 * TYPES
 ******************************************************************************/

typedef struct config_s config;

struct config_s {
    char *  addr;
    int     port;
    int     timeout;
};

/******************************************************************************
 * MACROS
 ******************************************************************************/

#define LOG(msg, ...) \
    { printf("%s:%d - ", __FILE__, __LINE__); printf(msg, ##__VA_ARGS__ ); printf("\n"); }

#define ERROR(msg, ...) \
    { fprintf(stderr,"error: "); fprintf(stderr,msg, ##__VA_ARGS__ ); fprintf(stderr, "\n"); }

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
        .addr       = ADDR,
        .port       = PORT,
        .timeout    = TIMEOUT
    };

    rc = configure(&c, argc, argv);

    if ( rc != 0 ) {
        return rc;
    }

    argv += optind;
    argc -= optind;

    if ( argc < 5 ) {
        ERROR("missing arguments.");
        usage(program);
        return 1;
    }

    char *          ns          = argv[0];
    char *          set         = argv[1];
    char *          key         = argv[2];
    char *          file        = argv[3];
    char *          func        = argv[4];

    cl_cluster *    cluster     = NULL;
    as_list *       arglist     = NULL;

    cl_object       okey;
    as_result       res;

    citrusleaf_init();

    cluster = citrusleaf_cluster_create();
    citrusleaf_cluster_add_host(cluster, c.addr, c.port, c.timeout);

    citrusleaf_object_init_str(&okey, key);
    
    arglist = as_json_arglist(argc-5,argv+5);

    rc = citrusleaf_udf_record_apply(cluster, ns, set, &okey, file, func, arglist, TIMEOUT, &res);

    if ( rc ) {
        ERROR("%d",rc);
    }
    else {
        printf("%s: ", res.is_success ? "SUCCESS" : "FAILURE");
        as_json_print(res.value);
        printf("\n");
    }

    return rc;
}

static int usage(const char * program) {
    fprintf(stderr, "\n");
    fprintf(stderr, "Usage: %s <namespace> <set> <key> <filename> <function> [args...]\n", basename(program));
    fprintf(stderr, "\n");
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "    -a remote address [default %s] \n", ADDR);
    fprintf(stderr, "    -p remote port [default %d]\n", PORT);
    fprintf(stderr, "\n");
    return 1;
}

static int configure(config * c, int argc, char *argv[]) {
    int optcase;
    while ((optcase = getopt(argc, argv, "ha:p:")) != -1) {
        switch (optcase) {
            case 'a':   c->addr = strdup(optarg); break;
            case 'p':   c->port = atoi(optarg); break;
            case 'h':
            default:    return usage(argv[0]);
        }
    }
    return 0;
}
