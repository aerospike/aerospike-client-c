#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/udf.h>
#include <as_hashmap.h>
#include <stdio.h>


#define HOST "127.0.0.1"
#define PORT 3000
#define TIMEOUT 100

#define LOG(msg, ...) \
    { printf("%s:%d - ", __FILE__, __LINE__); printf(msg, ##__VA_ARGS__ ); printf("\n"); }

typedef void (* udf_test)(cl_cluster *, cl_object *, as_result *);

void lists_lappend(cl_cluster * c, cl_object * key, as_result * res) {

    as_list * list = as_arraylist_new(3,0);
    as_list_add_string(list, "alex");
    as_list_add_string(list, "bob");
    as_list_add_string(list, "chuck");

    as_list * arglist = as_arglist_new(1);
    as_list_add_list(arglist, list);

    citrusleaf_udf_record_apply(c, "test", "demo", key, "lists", "lappend", arglist, TIMEOUT, res);

    as_list_free(arglist);
}

void maps_mapput(cl_cluster * c, cl_object * key, as_result * res) {

    as_map * map = as_hashmap_new(32);
    as_map_set(map, (as_val *) as_string_new(strdup("A")), (as_val *) as_string_new(strdup("alex")));
    as_map_set(map, (as_val *) as_string_new(strdup("B")), (as_val *) as_string_new(strdup("bob")));
    as_map_set(map, (as_val *) as_string_new(strdup("C")), (as_val *) as_string_new(strdup("chuck")));

    as_list * arglist = as_arglist_new(1);
    as_list_add_map(arglist, map);
    as_list_add_string(arglist, "Z");
    as_list_add_string(arglist, "Zed");

    citrusleaf_udf_record_apply(c, "test", "demo", key, "maps", "mapput", arglist, TIMEOUT, res);

    as_list_free(arglist);
}

void maps_show(cl_cluster * c, cl_object * key, as_result * res) {

    as_map * map = as_hashmap_new(32);
    as_map_set(map, (as_val *) as_string_new(strdup("A")), (as_val *) as_string_new(strdup("alex")));
    as_map_set(map, (as_val *) as_string_new(strdup("B")), (as_val *) as_string_new(strdup("bob")));
    as_map_set(map, (as_val *) as_string_new(strdup("C")), (as_val *) as_string_new(strdup("chuck")));

    as_list * arglist = as_arglist_new(2);
    as_list_add_map(arglist, map);
    as_list_add_string(arglist, "B");

    citrusleaf_udf_record_apply(c, "test", "demo", key, "maps", "show", arglist, TIMEOUT, res);

    as_list_free(arglist);
}

void maps_putmap(cl_cluster * c, cl_object * key, as_result * res) {

    as_map * map = as_hashmap_new(32);
    as_map_set(map, (as_val *) as_string_new(strdup("A")), (as_val *) as_string_new(strdup("alex")));
    as_map_set(map, (as_val *) as_string_new(strdup("B")), (as_val *) as_string_new(strdup("bob")));
    as_map_set(map, (as_val *) as_string_new(strdup("C")), (as_val *) as_string_new(strdup("chuck")));

    as_list * arglist = as_arglist_new(2);
    as_list_add_string(arglist, "mapperito");
    as_list_add_map(arglist, map);

    citrusleaf_udf_record_apply(c, "test", "demo", key, "maps", "putmap", arglist, TIMEOUT, res);

    as_list_free(arglist);
}

void maps_getmap(cl_cluster * c, cl_object * key, as_result * res) {

    as_list * arglist = as_arglist_new(2);
    as_list_add_string(arglist, "mapperito");

    citrusleaf_udf_record_apply(c, "test", "demo", key, "maps", "getmap", arglist, TIMEOUT, res);

    as_list_free(arglist);
}

int main() {


    udf_test tests[] = {
        lists_lappend,
        maps_mapput,
        maps_show,
        maps_putmap,
        maps_getmap,
        NULL
    };

    citrusleaf_init();

    cl_cluster * c = citrusleaf_cluster_create();
    citrusleaf_cluster_add_host(c, HOST, PORT, TIMEOUT);

    cl_object key;
    citrusleaf_object_init_str(&key, "1");

    udf_test * test = tests;
    while ( *test != NULL ) {
        as_result res;
        (*test)(c, &key, &res);
        LOG("%s: %s", res.is_success ? "SUCCESS" : "FAILURE", as_val_tostring(res.value));
        test++;
    }

    return 0;
}
