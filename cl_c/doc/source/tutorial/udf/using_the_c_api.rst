***************
Using the C API
***************

Before we begin, you should create a text file named ``my_functions.lua``, containing::

    function hello()
        return "Hello World!"
    end

This file will be used in the following examples.

Connecting to a Cluster
-----------------------

As with most Aerospike applications, you will first need to initialize the client environment::

    citrusleaf_init();

This will initialize a global citrusleaf client state.

Next, you will want to create a ``cl_cluster`` instance, that will be used as a handle to a cluster::
    
    cl_cluster * cluster = NULL;
    cluster = citrusleaf_cluster_create();

To establish a connection to a cluster::

    citrusleaf_cluster_add_host(cluster, "127.0.0.1", 3000, 1000);

You only need to add a single host from the cluster, then the client will discover the other servers. In this example, we assume you
are running a server on ``localhost`` on port ``3000``. 

Now, your application should be connected to a cluster and able to send commands to the cluster.

Upload a UDF file
-----------------

To upload a UDF file, use the ``citrusleaf_udf_put`` function::

    char * error = NULL;

    int rc = citrusleaf_udf_put(cluster, "my_functions.lua", my_functions, &error);

Where ``my_functions`` is the contents of a UDF file, named ``my_functions.lua``.

On success, ``rc`` will be zero (0) and ``error`` will remain NULL. On failure, ``rc`` will be non-zero, and ``error`` will be updated with an error message. 

You are responsible for freeing ``error``.

List UDF files
--------------

To list UDF files, use the ``citrusleaf_udf_put`` function::

    char *  files   = NULL;
    int     count   = 0;
    char *  error   = NULL;

    int rc = citrusleaf_udf_list(cluster, &files, &count, &error);

Where ``files`` will be populated with a list of filenames and ``count`` with the number of filenames.

On success, ``rc`` will be zero (0), ``error`` will remain NULL, while ``files`` and ``count`` are updated. On failure, ``rc`` will be non-zero, ``error`` will be updated with an error message, while ``files`` and ``count`` remain unchanged. 

You are responsible for freeing the ``error`` and ``files``.


Get a UDF file
--------------

To get a UDF file, use the ``citrusleaf_udf_get`` function::

    char *  content = NULL;
    int     size    = 0;
    char *  error   = NULL;

    int rc = citrusleaf_udf_get(cluster, "my_functions.lua", &content, size, &error);

Where ``content`` will be populated the file's contents and ``size`` is the number of bytes in ``content``.

On success, ``rc`` will be zero (0), ``error`` will remain NULL, while ``content`` and ``size`` will be updated. On failure, ``rc`` will be non-zero, ``error`` will be updated with an error messages, while ``content`` and ``size`` remain unchanged. 

You are responsible for freeing ``error`` and ``content``.

Call a UDF
----------

Currently, a UDF can be applied to a single record in the database. In order, to call a UDF, you have to specify which record it can be applied against::

First, we will define the namespace, set and key::
    
    char *      ns  = "test";
    char *      set = "demo";
    cl_object   key;

    citrusleaf_object_init_str(&key, "1");

The key currently is defined using a ``cl_object`` and is populated with the value "1". 

Next, we need to define the UDF file, function, arguments and return value::

    char *      file    = "greetings";
    char *      func    = "english";
    as_list *   arglist = as_arglist_new(0);
    as_result   result;

The ``arglist`` is an :type:`as_list` containing arguments for the function. It is initialized to size ``0`` because the function we are calling does not require any arguments.

The ``result`` is an :type:`as_result` which represents a success of a failure value. 

To call the UDF, you will use the ``citrusleaf_udf_record_apply`` function::

    int rc = citrusleaf_udf_record_apply(cluster, ns, set, &key, file, func, arglist, 1000, &result);

On success, ``rc`` will be zero (0) and ``result`` will be a ``success`` and contain the value. On failure, ``rc`` will be non-zero, and ``result`` will be a failure and contain an error value.

::

    if ( result->is_success ) {
        as_val * success = result->value;
    }
    else {
        as_val * failure = result->value;
    }

Finally, you will want to clean up::

    as_list_free(arglist);
    as_result_destroy(&result);

For ``arglist``, we call "free" because it was initialized as a pointer on the heap. For ``result``, we call "destroy" because it was initialized on the stack.

For the key, we need to call "free"::

    citrusleaf_object_free(&key);

Remove a UDF File
-----------------

To remove a UDF file, use the ``citrusleaf_udf_remove`` function::

    char * error = NULL;

    int rc = citrusleaf_udf_remove(cluster, "my_functions.lua", &error);

On success, ``rc`` will be zero (0) and ``error`` will remain NULL. On failure, ``rc`` will be non-zero, ``error`` will be updated with an error message.

You are responsible for freeing ``error``.

Disconnecting from a Cluster
----------------------------

Once your application is finsihed, you will want to shutdown you client connection to the cluster::

    citrusleaf_cluster_destroy(cluster);

And if your application is done using the citrusleaf client, then you will want to shut it down::

    citrusleaf_shutdown();
