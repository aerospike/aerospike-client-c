**********************
User-Defined Functions
**********************

Functions
=========

..  function:: cl_rv citrusleaf_udf_record_apply(cl_cluster * cluster, const char * namespace, const char * set, const cl_object * key, const char * file, const char * function, as_list * arglist, int timeout, as_result * result)


    Apply a UDF to a record in the specified *cluster*. The record is identified the its *namespace*, *set* and *key* parameters. The function is identified via its *file* name and *function* name parameters. The arguments are specified via the *arglist* parameter. The *timeout* parameter is how long to wait for a response in milliseconds. The *result* parameter is the result of the function call. 

    A return value of 0 (zero) indicates a success. 

    ::

        as_list * arglist = as_arglist_new(2);
        as_list_add_string(arglist, "A");
        as_list_add_integer(arglist, 1);

        as_result result;
        as_result_init(&result);

        int rc = citrusleaf_udf_record_apply(cluster, "test", "demo", &key, "myudfs", "myudf", arglist, 1000, &result);
        if ( rc == 0 && result.is_success ) {
            ...
        }
        else {
            ...
        }

..  function:: cl_rv citrusleaf_udf_list(cl_cluster * cluster, char *** files, int * size, char ** error)

    List the user-defined function files on the server. Populates *files* with the list of file names and *size* with the number of files. The *error* is populated when an error occurs.

    A return value of 0 (zero) indicates a success. 

    ::

        char **     files = NULL;
        uint32_t    size  = 0;
        char *      error = NULL;

        int rc = citrusleaf_udf_list(cluster, &list, &size, &error);
        if ( rc == 0 && error == NULL ) {
            ...
        }
        else {
            ...
        }

..  function:: cl_rv citrusleaf_udf_get(cl_cluster * cluster, const char * filename, char ** contents, int * size, char ** error)

    Get the contents of a user-defined function file, specified by *filename*. Populates *contents* with the file contents, and *size* with the number of bytes contained in *contents*. The *error* is populated when an error occurs.

    A return value of 0 (zero) indicates a success. 

    ::

        char *      content = NULL;
        uint32_t    size    = 0;
        char *      error   = NULL;

        int rc = citrusleaf_udf_get(cluster, "myudfs.lua", &content, &size, &error);
        if ( rc == 0 && error == NULL ) {
            ...
        }
        else {
            ...
        }

..  function:: cl_rv citrusleaf_udf_get_with_gen(cl_cluster * cluster, const char * filename, char ** contents, int * size, char ** gen, char ** error) 

    Same as :c:func:`citrusleaf_udf_get`, except a *gen* value is populated, containing the generation value for the file.

    A return value of 0 (zero) indicates a success. 

    ::

        char *      content  = NULL;
        uint32_t    size     = 0;
        char        gen[160] = {0};
        char *      error    = NULL;

        int rc = citrusleaf_udf_get_gen(cluster, "myudfs.lua", &content, &size, &gen, &error);
        if ( rc == 0 && error == NULL ) {
            ...
        }
        else {
            ...
        }

..  function:: cl_rv citrusleaf_udf_put(cl_cluster * cluster, const char * filename, const char * contents, char ** error)

    Uploads a user-defined function file, with the given *filename* and *contents*. The *error* is populated when an error occurs.

    A return value of 0 (zero) indicates a success. 

    ::

        char *      content = NULL;
        char *      error   = NULL;

        int rc = citrusleaf_udf_put(cluster, "myudfs.lua", content, &error);
        if ( rc == 0 && error == NULL ) {
            ...
        }
        else {
            ...
        }

..  function:: cl_rv citrusleaf_udf_remove(cl_cluster * cluster, const char * filename, char ** error)
    
    Removes a user-defined function file, with the given *filename*. The *error* is populated when an error occurs.

    A return value of 0 (zero) indicates a success. 

    ::

        char * error = NULL;

        int rc = citrusleaf_udf_remove(cluster, "myudfs.lua", &error);
        if ( rc == 0 && error == NULL ) {
            ...
        }
        else {
            ...
        }