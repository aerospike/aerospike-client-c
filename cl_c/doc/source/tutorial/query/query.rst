*******************************************
Querying Aerospike's secondary indexes in C
*******************************************

Summary
-------

Queries on Secondary indexes can be done on both of Aerospike's supported index types: [numeric, string]. Numeric index queries can be either equality or range lookups, whereas String index queries (currently) only support equality lookups.

A Query on a secondary index can lookup ALL bins in the records it evaluates to or a subset of all of the record's bins. The SELECTed bins must be declared in advance.

NOTE: Queries are only possible on completely-built indexes (built via background process).


Examples
--------

In this example, we assume 
1.) a numeric index has been put on the binname 'age'
2.) a string index has been put on the binname 'uuid'


Numeric Range Query
-------------------

The following C code will perform the SQL Query:
SQL: "SELECT fname, lname FROM nsname.setname WHERE age BETWEEN 10 AND 20"

C CODE
------
::

    // Create Aerospike Cluster
    cl_cluster *asc   = citrusleaf_cluster_create();
    // Initialize query object
    cl_query   *query = citrusleaf_query_create(NULL, 'setname');
    // SELECT 'fname'
    citrusleaf_query_add_binname(query, 'fname');
    // SELECT 'lname'
    citrusleaf_query_add_binname(query, 'lname');
    // Add in Where Clause predicate (range query: "age BETWEEN 10 and 20")
    citrusleaf_query_add_range_numeric(query, 'age', 10, 20);
    // Run Query, data processing handled by callback: 'cb'
    int rv = citrusleaf_query(asc, 'nsname', query, cb, NULL);
    if (rv) { /* ERROR */ }
    // Destroy query
    citrusleaf_query_destroy(query);


String Equality Query
---------------------

The following C code will perform the SQL Query:
SQL: "SELECT jobtitle, salary FROM nsname.setname WHERE uuid = '333AB6532EF879BC22'"

::

    // Create Aerospike Cluster
    cl_cluster *asc   = citrusleaf_cluster_create();
    // Initialize query object
    cl_query   *query = citrusleaf_query_create(NULL, 'setname');
    // SELECT 'fname'
    citrusleaf_query_add_binname(query, 'fname');
    // SELECT 'lname'
    citrusleaf_query_add_binname(query, 'lname');
    // Add in Where Clause predicate (equality query: "WHERE uuid = '333AB6532EF879BC22'"
    citrusleaf_query_add_equality_string(query, 'uuid', '333AB6532EF879BC22');
    // Run Query, data processing handled by callback: 'cb'
    int rv = citrusleaf_query(asc, 'nsname', query, cb, NULL);
    if (rv) { /* ERROR */ }
    // Destroy query
    citrusleaf_query_destroy(query);



Callback
--------

The call back functions have the following signature:

::

    int cb(char *ns, cf_digest *keyd, char *set, uint32_t gen,
           uint32_t record_ttl, cl_bin *bin, int n_bins,
           bool is_last, void *udata) {
        return 0;
    }

TODO: explanation
