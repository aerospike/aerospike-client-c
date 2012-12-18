/******************************************************************************
 * Copyright 2008-2012 by Aerospike.  All rights reserved.
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 * ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 ******************************************************************************/

#include "as_json.h"
#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/udf.h>
#include <citrusleaf/as_hashmap.h>
#include <citrusleaf/as_buffer.h>
#include <citrusleaf/as_msgpack.h>
#include <stdio.h>

/******************************************************************************
 * CONSTANTS
 ******************************************************************************/

#define HOST "127.0.0.1"
#define PORT 3010
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
    // { printf("%s:%d - ", __FILE__, __LINE__); printf(msg, ##__VA_ARGS__ ); printf("\n"); }

#define ERROR(msg, ...) \
    { fprintf(stderr,"error: "); fprintf(stderr, msg, ##__VA_ARGS__ ); fprintf(stderr, "\n"); }

/******************************************************************************
 * STATIC FUNCTION DECLARATIONS
 ******************************************************************************/

static int usage(const char * program);
static int configure(config * c, int argc, char *argv[]);
static int record_to_json(int nbins, cl_bin * bins);

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

    if ( argc != 3 ) {
        ERROR("missing arguments.");
        usage(program);
        return 1;
    }

    char * n = argv[0];
    char * s = argv[1];
    char * k = argv[2];

    citrusleaf_init();

    cl_cluster * cluster = citrusleaf_cluster_create();
    citrusleaf_cluster_add_host(cluster, HOST, PORT, TIMEOUT);

    cl_object key;
    citrusleaf_object_init_str(&key, k);

    cl_bin * bins = NULL;
    int nbins = 0;
    uint32_t gen = 0;

    rc = citrusleaf_get_all(cluster, n, s, &key, &bins, &nbins, TIMEOUT, &gen);

    if ( rc ) {
        ERROR("%d",rc);
    }
    else {
        record_to_json(nbins, bins);
        printf("\n");
    }

    return rc;
}

static int record_to_json(int nbins, cl_bin * bins) {
    printf("{");
    for (int i = 0; i < nbins; i++) {
        printf("\"%s\": ",bins[i].bin_name);
        switch (bins[i].object.type) {
            case CL_STR:
                printf("\"%s\"", bins[i].object.u.str);
                break;
            case CL_INT:
                printf("%"PRId64"",bins[i].object.u.i64);
                break;
            case CL_LIST: 
            case CL_MAP: {
                as_serializer ser;
                as_msgpack_init(&ser);
                cl_object o = bins[i].object;
                as_val * val = NULL;
                as_buffer buf = {
                    .capacity = (uint32_t) o.sz,
                    .size = (uint32_t) o.sz,
                    .data = (char *) o.u.blob
                };
                as_serializer_deserialize(&ser, &buf, &val);
                as_json_print(val);
                as_serializer_destroy(&ser);
                break;
            }
            default:
                printf("<%d>",(int)bins[i].object.type);
                break;
        }
        if ( i < nbins-1 ) {
            printf(", ");
        }
    }
    printf("}");
    return 0;
}

static int usage(const char * program) {
    fprintf(stderr, "\n");
    fprintf(stderr, "Usage: %s <namespace> <set> <key>\n", basename(program));
    fprintf(stderr, "\n");
    fprintf(stderr, "Retrieves and prints the record.\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "    -h host [default %s] \n", HOST);
    fprintf(stderr, "    -p port [default %d]\n", PORT);
    fprintf(stderr, "\n");
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