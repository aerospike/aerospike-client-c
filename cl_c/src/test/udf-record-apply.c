#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/udf.h>
#include <as_hashmap.h>
#include <as_buffer.h>
#include <as_msgpack.h>
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

    char *          ns          = argv[1];
    char *          set         = argv[2];
    char *          key         = argv[3];
    char *          file        = argv[4];
    char *          func        = argv[5];

    cl_cluster *    cluster     = NULL;
    as_list *       list        = NULL;
    as_list *       arglist     = NULL;
    int             rc          = 0;

    cl_object       okey;
    as_result       res;

    citrusleaf_init();

    cluster = citrusleaf_cluster_create();
    citrusleaf_cluster_add_host(cluster, HOST, PORT, TIMEOUT);

    citrusleaf_object_init_str(&okey, key);

    list = as_arraylist_new(3,0);
    as_list_add_string(list, "alex");
    as_list_add_string(list, "bob");
    as_list_add_string(list, "chuck");
    
    arglist = as_arglist_new(1);
    as_list_add_list(arglist, list);

    rc = citrusleaf_udf_record_apply(cluster, ns, set, &okey, file, func, arglist, TIMEOUT, &res);

    as_list_free(arglist);

    if ( rc ) {
        printf("error: %d\n", rc);
    }
    else {
        printf("%s: %s\n", res.is_success ? "SUCCESS" : "FAILURE", as_val_tostring(res.value));
    }

    return rc;
}
