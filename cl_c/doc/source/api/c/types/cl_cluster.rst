**********
cl_cluster
**********

A handle to an aerospike cluster. This handle maintains information about the cluster and a pool of connections to each node in a cluster.

Types
=====

..  type:: struct cl_cluster

    A handle to an aerospike cluster. This handle maintains information about the cluster and a pool of connections to each node in a cluster.

Functions
=========

..  function:: cl_cluster * citrusleaf_cluster_create()

    Allocates and initializes a :type:`cl_cluster`::

        cl_cluster * cluster = citrusleaf_cluster_create();

..  function:: cl_rv citrusleaf_cluster_add_host(cl_cluster * cluster, char const * host, short port, int timeout)

    Connect to the host and gather information about the host and the cluster in which it belongs.
    ::

        citursleaf_cluster_add_host(cluster, "127.0.0.1", 3000, 1000);

..  function:: void citrusleaf_cluster_destroy(cl_cluster * cluster)

    Release the resources utilizes by the :type:`cl_cluster` struct.


