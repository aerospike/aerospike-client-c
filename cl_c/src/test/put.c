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

typedef int (* parameter_callback)(const char * key, const char * value, int index, void * context);

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

cl_bin * bin_from_pair(as_pair * p) {
    as_val * k = as_pair_1(p);
    as_val * v = as_pair_2(p);
    
    if ( k->type != AS_STRING ) {
        return NULL;
    }

    cl_bin * bin = (cl_bin *) malloc(sizeof(cl_bin));
    strcpy(bin->bin_name, as_string_tostring((as_string *) k));

    as_serializer ser;
    as_msgpack_init(&ser);

    as_buffer buf;
    as_buffer_init(&buf);

    switch ( v->type ) {
        case AS_INTEGER: {
            as_integer * i = as_integer_fromval(v);
            citrusleaf_object_init_int(&bin->object, as_integer_toint(i));
            break;
        }
        case AS_STRING: {
            as_string * s = as_string_fromval(v);
            citrusleaf_object_init_str(&bin->object, as_string_tostring(s));
            break;
        }
        case AS_LIST: {
            as_serializer_serialize(&ser, v, &buf);
            citrusleaf_object_init_blob2(&bin->object, buf.data, buf.size, CL_LIST);
            break;
        }
        case AS_MAP: {
            as_serializer_serialize(&ser, v, &buf);
            citrusleaf_object_init_blob2(&bin->object, buf.data, buf.size, CL_MAP);
            break;
        }
        default: {
            citrusleaf_object_init_null(&bin->object);
            break;
        }
    }

    return bin;
}

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

    if ( argc < 4 ) {
        ERROR("missing arguments.");
        usage(program);
        return 1;
    }

    char * n = argv[0];
    char * s = argv[1];
    char * k = argv[2];
    char * d = argv[3];

    as_val * val = as_json_arg((char *) d);

    if ( val->type != AS_MAP ) {
        ERROR("invalid document.");
        return 2;
    }


    as_map * doc = (as_map *) val;

    int nbins = as_map_size(doc);
    cl_bin * bins = (cl_bin *) malloc(nbins * sizeof(cl_bin));

    as_iterator * i = as_map_iterator(doc);
    for ( int j=0; as_iterator_has_next(i); j++ ) {
        cl_bin * bin = bin_from_pair((as_pair *) as_iterator_next(i));
        if ( bin == NULL ) {
            ERROR("invalid field.");
            return 3;
        }
        bins[j] = *bin;
    }

    citrusleaf_init();

    cl_cluster * cluster = citrusleaf_cluster_create();
    citrusleaf_cluster_add_host(cluster, c.addr, c.port, c.timeout);

    cl_object key;
    citrusleaf_object_init_str(&key, k);

    cl_write_parameters cl_wp;
    cl_write_parameters_set_default(&cl_wp);

    rc = citrusleaf_put(cluster, n, s, &key, bins, nbins, &cl_wp);

    if ( rc ) {
        ERROR("%d",rc);
    }

    return rc;
}

static int usage(const char * program) {
    fprintf(stderr, "\n");
    fprintf(stderr, "Usage: %s <namespace> <set> <key> <object> \n", basename(program));
    fprintf(stderr, "\n");
    fprintf(stderr, "Stores an object with specified key. The <object> is a JSON object.\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "    -a remote address [default %s] \n", ADDR);
    fprintf(stderr, "    -p remote port [default %d]\n", PORT);
    fprintf(stderr, "\n");
    fprintf(stderr, "Examples:\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "    %s test demo 1 '{ \"name\": \"Bob\", \"age\": 30 }' \n", basename(program));
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
