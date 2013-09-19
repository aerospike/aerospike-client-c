#pragma once

#include <citrusleaf/cl_cluster.h>
#include <citrusleaf/citrusleaf.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_info.h>

/**
 * Given a particular query string and a key, return the value of the key
 * from all nodes in the cluster. It is the caller's responsibility to free
 * the resulting value array
 */
char **get_stats(char * query, char * key, cl_cluster * asc);
