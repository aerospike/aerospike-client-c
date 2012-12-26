
*******************************
User-Defined Functions Tutorial
*******************************

Later...

::

    cl_cluster *    cluster     = NULL;
    as_list *       arglist     = NULL;

    cl_object       okey;
    as_result       res;

    citrusleaf_init();

    cluster = citrusleaf_cluster_create();
    citrusleaf_cluster_add_host(cluster, "127.0.0.1", 3000, 1000);

    citrusleaf_object_init_str(&okey, key);

    as_list * arglist = as_arglist_new(3);
    as_list_add_string(arglist, "a");
    as_list_add_string(arglist, "b");
    as_list_add_string(arglist, "c");

    cl_rv rc = citrusleaf_udf_record_apply(cluster, "test", "demo", &okey, "strings", "cat", arglist, 1000, &res);


