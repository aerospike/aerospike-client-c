#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/udf.h>
#include <citrusleaf/as_hashmap.h>
#include <citrusleaf/as_buffer.h>
#include <citrusleaf/as_msgpack.h>
#include <citrusleaf/as_linkedlist.h>
#include <citrusleaf/as_arraylist.h>
#include <citrusleaf/as_hashmap.h>
#include <stdio.h>

#include <jansson.h>


#define HOST "127.0.0.1"
#define PORT 3010
#define TIMEOUT 100

#define LOG(msg, ...) \
    { printf("%s:%d - ", __FILE__, __LINE__); printf(msg, ##__VA_ARGS__ ); printf("\n"); }

as_list * json_array_to_list(json_t * a);
as_map * json_object_to_map(json_t * o);
as_string * json_string_to_string(json_t * s);
as_integer * json_number_to_integer(json_t * n);
as_val * json_to_val(json_t * j);


as_list * json_array_to_list(json_t * a) {

    int size = json_array_size(a);
    as_list * l = as_arraylist_new(size,0);

    for (int i = 0; i < json_array_size(a); i++) {
        as_val * v = json_to_val(json_array_get(a,i));
        as_list_append(l, v);
    }

    return l;
}

as_map * json_object_to_map(json_t * o) {

    int size = json_array_size(o);
    as_map * m = as_hashmap_new(size);
    const char * key;
    json_t * value;

    json_object_foreach(o, key, value) {
        as_val * k = (as_val *) as_string_new(strdup(key));
        as_val * v = json_to_val(value);
        as_map_set(m, k, v);
    }

    return m;
}

as_string * json_string_to_string(json_t * s) {
    return as_string_new(strdup(json_string_value(s)));
}

as_integer * json_number_to_integer(json_t * n) {
    return as_integer_new((int64_t) json_integer_value(n));
}

as_val * json_to_val(json_t * j) {
    if ( json_is_array(j) )  return (as_val *) json_array_to_list(j);
    if ( json_is_object(j) ) return (as_val *) json_object_to_map(j);
    if ( json_is_string(j) ) return (as_val *) json_string_to_string(j);
    if ( json_is_number(j) ) return (as_val *) json_number_to_integer(j);
    return (as_val *) &as_nil;
}


as_list * getarglist(int argc, char ** argv) {
    if ( argc == 0 || argv == NULL ) return cons(NULL,NULL);

    as_val * val = NULL;
    json_t *root;
    json_error_t error;

    root = json_loads(argv[0], 0, &error);

    if ( !root ) {
        // then it is either a string or integer (i hope)
        char * end = NULL;
        uint64_t i = (uint64_t) strtol(argv[0], &end, 10);
        if ( *end == '\0' ) {
            val = (as_val *) as_integer_new(i);
        }
        else {
            val = (as_val *) as_string_new(argv[0]);
        }
    }
    else {
        val = json_to_val(root);
    }



    return cons(val, getarglist(argc-1, argv+1));
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
