#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/udf.h>
#include <stdio.h>


#define HOST "127.0.0.1"
#define PORT 3000
#define TIMEOUT 100

#define LOG(msg, ...) \
    { printf("%s:%d - ", __FILE__, __LINE__); printf(msg, ##__VA_ARGS__ ); printf("\n"); }

int main() {

    citrusleaf_init();

    cl_cluster * c = citrusleaf_cluster_create();
    citrusleaf_cluster_add_host(c, HOST, PORT, TIMEOUT);

    cl_object key;
    citrusleaf_object_init_str(&key, "1");

    as_list * list = as_arraylist_new(3,0);
    as_list_add_string(list, "alex");
    as_list_add_string(list, "bob");
    as_list_add_string(list, "chuck");

    as_list * arglist = as_arglist_new(1);
    as_list_add_list(arglist, list);

    as_result res;

    citrusleaf_udf_record_apply(c, "test", "demo", &key, "lists", "lappend", arglist, TIMEOUT, &res);

    LOG("%s: %s", res.is_success ? "SUCCESS" : "FAILURE", as_val_tostring(res.value));

    return 0;
}
