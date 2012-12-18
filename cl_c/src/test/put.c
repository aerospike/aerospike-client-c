/******************************************************************************
 * Copyright 2008-2012 by Aerospike.  All rights reserved.
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 * ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 ******************************************************************************/

#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/udf.h>
#include <citrusleaf/as_hashmap.h>
#include <citrusleaf/as_msgpack.h>
#include <stdio.h>
#include "as_json.h"

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

#define ERROR(msg, ...) \
    { fprintf(stderr,"error: "); fprintf(stderr, msg, ##__VA_ARGS__ ); fprintf(stderr, "\n"); }

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

    if ( argc != 5 ) {
        ERROR("missing arguments.");
        usage(program);
        return 1;
    }

    char * n = argv[0];
    char * s = argv[1];
    char * k = argv[2];
    char * b = argv[3];
    char * v = argv[4];
    
    as_val * val = as_json_arg(v);

    if ( val == NULL ) {
        ERROR("invalid argument: %s",v);
        return 2;
    }

    citrusleaf_init();

    cl_cluster * cluster = citrusleaf_cluster_create();
    citrusleaf_cluster_add_host(cluster, c.host, c.port, c.timeout);

    cl_object key;
    citrusleaf_object_init_str(&key, k);

    cl_bin bin;
    strcpy(bin.bin_name, b);

    as_serializer ser;
    as_msgpack_init(&ser);

    as_buffer buf;
    as_buffer_init(&buf);

    switch ( val->type ) {
        case AS_INTEGER: {
            as_integer * i = as_integer_fromval(val);
            citrusleaf_object_init_int(&bin.object, as_integer_toint(i));
            break;
        }
        case AS_STRING: {
            as_string * s = as_string_fromval(val);
            citrusleaf_object_init_str(&bin.object, as_string_tostring(s));
            break;
        }
        case AS_LIST: {
            as_serializer_serialize(&ser, val, &buf);
            citrusleaf_object_init_blob2(&bin.object, buf.data, buf.size, CL_LIST);
            break;
        }
        case AS_MAP: {
            as_serializer_serialize(&ser, val, &buf);
            citrusleaf_object_init_blob2(&bin.object, buf.data, buf.size, CL_MAP);
            break;
        }
        default: {
            break;
        }
    }


    cl_write_parameters cl_wp;
    cl_write_parameters_set_default(&cl_wp);

    rc = citrusleaf_put(cluster, n, s, &key, &bin, 1, &cl_wp);

    as_buffer_destroy(&buf);

    if ( rc ) {
        ERROR("%d",rc);
    }

    return rc;
}

static int usage(const char * program) {
    fprintf(stderr, "\n");
    fprintf(stderr, "Usage: %s <namespace> <set> <key> <bin> <value>\n", basename(program));
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
