/*
 *  Citrusleaf Tools: General Performance Test Config File
 *
 *  Copyright 2013 by Citrusleaf, Aerospike Inc.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */

#pragma once

#include <citrusleaf/cf_atomic.h> 
#include "test_counter.h"
#include "test.h"


/**
 * Hold the basic (default) information needed to configure and run
 * the general performance tests
 */
typedef struct test_config_s {
    char  *host;
    int    port;
    char  *ns;      // Namespace
    char  *set;
    bool  verbose; // Turn on/off client debug/trace printing
    bool  strict;
    bool  follow;
    cl_cluster *asc; // the Aerospike Cluster

    // Set up a cluster of machines.  If no cluster, default to
    // a server running at local host.
    int cluster_count; // when non-zero, we have a cluster
    char * cluster_name[32]; // Build a cluster this big
    int    cluster_port[32]; // Build a cluster this big

    uint32_t timeout_ms;
    uint32_t record_ttl;
    char * package_name; // Name of the File holding the UDF
    char * filter_name;  // Name of the "Inner UDF" for the UDF call


    unsigned int n_threads;     // Number of threads in this test
    unsigned int n_iterations;  // Number of iterations per thread
    unsigned int n_keys;        // Number of keys used
    unsigned int key_max;       // Integer Key range: 0 to key_max

    unsigned int key_len;
    unsigned int value_len;

    uint64_t    *values;  // array of uint64_t, size is the number of keys
    shash       *in_progress_hash;

    // Track success and fails
    atomic_int *success_counter; // Track successes and fails
    atomic_int *fail_counter;

    // Used for timing and histogram tracking
    atomic_int  *read_ops_counter;
    atomic_int  *read_vals_counter;
    atomic_int  *write_ops_counter;
    atomic_int  *write_vals_counter;
    atomic_int  *delete_ops_counter;
    atomic_int  *delete_vals_counter;
    atomic_int  *key_counter;
    unsigned int pseudo_seed; // Feed the Rand() Function
} test_config;


// Foward delare our config structure for all test files.
extern test_config * g_config;

// Forward declare the functions in the config module
extern int set_config_defaults ( test_config * config_ptr );
