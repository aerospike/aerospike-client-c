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


int main(int argc, char ** argv) {
    
    cl_cluster *    cluster = NULL;
    char **         files   = NULL;
    int             count   = 0;
    char *          error   = NULL;
    int             rc      = 0;

    citrusleaf_init();

    cluster = citrusleaf_cluster_create();
    citrusleaf_cluster_add_host(cluster, HOST, PORT, TIMEOUT);

    citrusleaf_udf_list(cluster, &files, &count, &error);
    
    if ( rc ) {
        printf("error: %s\n", error);
        free(error);
        error = NULL;
    }
    else {
        for ( int i = 0; i < count; i++ ) {
            printf("%s\n",files[i]);
            free(files[i]);
            files[i] = NULL;
        }
        free(files);
        files = NULL;
    }

    return rc;
}
