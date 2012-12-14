#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/udf.h>
#include <citrusleaf/as_hashmap.h>
#include <citrusleaf/as_buffer.h>
#include <citrusleaf/as_msgpack.h>
#include <citrusleaf/as_linkedlist.h>
#include <stdio.h>


#define HOST "127.0.0.1"
#define PORT 3000
#define TIMEOUT 100

#define LOG(msg, ...) \
    { printf("%s:%d - ", __FILE__, __LINE__); printf(msg, ##__VA_ARGS__ ); printf("\n"); }


as_list * getarglist(int argc, char ** argv) {
    if ( argc == 0 || argv == NULL ) return cons(NULL,NULL);
    return cons(as_string_new(argv[0]), getarglist(argc-1, argv+1));
}


int main(int argc, char ** argv) {
    
    if ( argc < 6 ) {
        LOG("invalid arguments.");
        return 1;
    }

    char *          ns          = argv[1];
    char *          set         = argv[2];
    char *          key         = argv[3];
    char *          file        = argv[4];
    char *          func        = argv[5];

    cl_cluster *    cluster     = NULL;
    as_list *       arglist     = NULL;
    int             rc          = 0;

    cl_object       okey;
    as_result       res;

    citrusleaf_init();

    cluster = citrusleaf_cluster_create();
    citrusleaf_cluster_add_host(cluster, HOST, PORT, TIMEOUT);

    citrusleaf_object_init_str(&okey, key);

    
    // arglist = as_arglist_new(1);
    // as_list_add_list(arglist, list);
    arglist = getarglist(argc-6,argv+=6);

    rc = citrusleaf_udf_record_apply(cluster, ns, set, &okey, file, func, arglist, TIMEOUT, &res);

    // as_list_free(arglist);

    if ( rc ) {
        printf("error: %d\n", rc);
    }
    else {
        printf("%s: %s\n", res.is_success ? "SUCCESS" : "FAILURE", as_val_tostring(res.value));
    }

    return rc;
}
