#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/udf.h>
#include <citrusleaf/as_hashmap.h>
#include <citrusleaf/as_buffer.h>
#include <citrusleaf/as_msgpack.h>
#include <stdio.h>


#define HOST "127.0.0.1"
#define PORT 3000
#define TIMEOUT 100

#define LOG(msg, ...) \
    { printf("%s:%d - ", __FILE__, __LINE__); printf(msg, ##__VA_ARGS__ ); printf("\n"); }

typedef struct linebuffer_s linebuffer;

struct linebuffer_s {
    int     capacity;
    int     line_len;
    int     size;
    char ** lines;
};

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

int main(int argc, char ** argv) {
    
    if ( argc != 2 ) {
        LOG("invalid arguments.");
        return 1;
    }

    char *          filename    = argv[1];
    cl_cluster *    cluster     = NULL;
    char *          content     = readfile(filename);
    char *          error       = NULL;
    int             rc          = 0;

    citrusleaf_init();

    cluster = citrusleaf_cluster_create();
    citrusleaf_cluster_add_host(cluster, HOST, PORT, TIMEOUT);

    rc = citrusleaf_udf_put(cluster, basename(filename), content, &error);

    if ( rc ) {
        printf("error: %s\n", error);
        free(error);
        error = NULL;
    }

    return rc;
}
