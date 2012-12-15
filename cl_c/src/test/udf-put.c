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
typedef struct linebuffer_s linebuffer;

struct config_s {
    char *  host;
    int     port;
    int     timeout;
};

struct linebuffer_s {
    int     capacity;
    int     line_len;
    int     size;
    char ** lines;
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

static char * readfile(char * filepath);

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
        ERROR("missing filepath.");
        usage(program);
        return 1;
    }

    char *          filename    = argv[0];
    cl_cluster *    cluster     = NULL;
    char *          content     = readfile(filename);
    char *          error       = NULL;

    citrusleaf_init();

    cluster = citrusleaf_cluster_create();
    citrusleaf_cluster_add_host(cluster, c.host, c.port, c.timeout);

    rc = citrusleaf_udf_put(cluster, basename(filename), content, &error);

    if ( rc ) {
        ERROR(error);
        free(error);
        error = NULL;
    }

    return rc;
}

static char * readfile(char * filepath) {

    FILE *  file        = NULL;
    char    line[1024]  = {0};

    linebuffer buffer = {
        .capacity   = 1024,
        .line_len   = 1024,
        .size       = 0,
        .lines      = (char **) malloc(sizeof(char *) * 1024)
    };

    file = fopen(filepath,"r");
    while( fgets(line, buffer.line_len, file) != NULL ) {
        buffer.lines[buffer.size] = strdup(line);
        buffer.size++;
    }

    fclose(file);
    file = NULL;

    char * content = (char *) malloc(buffer.size * buffer.line_len * sizeof(char));
    char * cp = content;
    int    cz = 0;
    for (int i = 0; i < buffer.size; i++ ) {
        int l = strlen(buffer.lines[i]);
        memcpy(cp, buffer.lines[i], l);
        cp += l;
        cz += l;
    }
    *cp = 0;
    
    free(buffer.lines);
    buffer.lines = NULL;

    return content;
}


static int usage(const char * program) {
    fprintf(stderr, "\n");
    fprintf(stderr, "Usage: %s <filepath>\n", basename(program));
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
