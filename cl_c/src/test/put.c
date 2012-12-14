#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/udf.h>
#include <citrusleaf/as_hashmap.h>
#include <stdio.h>


#define HOST "127.0.0.1"
#define PORT 3000
#define TIMEOUT 100

#define LOG(msg, ...) \
    { printf("%s:%d - ", __FILE__, __LINE__); printf(msg, ##__VA_ARGS__ ); printf("\n"); }

int main(int argc, char ** argv) {
    
    if ( argc != 6 ) {
        LOG("invalid arguments.");
        return 1;
    }

    char * n = argv[1];
    char * s = argv[2];
    char * k = argv[3];
    char * b = argv[4];
    char * v = argv[5];

    citrusleaf_init();

    cl_cluster * c = citrusleaf_cluster_create();
    citrusleaf_cluster_add_host(c, HOST, PORT, TIMEOUT);

    cl_object key;
    citrusleaf_object_init_str(&key, k);

    cl_bin bin;
    strcpy(bin.bin_name, b);
    citrusleaf_object_init_str(&bin.object, v);

    cl_write_parameters cl_wp;
    cl_write_parameters_set_default(&cl_wp);

    int rc = citrusleaf_put(c, n, s, &key, &bin, 1, &cl_wp);
    if ( rc ) {
        printf("error: %d\n",rc);
    }
    return rc;
}
