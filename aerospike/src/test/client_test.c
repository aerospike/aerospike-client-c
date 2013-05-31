#include <errno.h>

#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/cl_udf.h>

#include "test.h"
#include "util/info_util.h"

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define HOST "127.0.0.1"
#define PORT 3000
#define TIMEOUT 1000

#define SCRIPT_LEN_MAX 1048576

/******************************************************************************
 * VARIABLES
 *****************************************************************************/

cl_cluster * cluster = NULL;
int cluster_size;
bool run_memory_tests;

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static bool before(atf_plan * plan) {

    if ( cluster ) {
        error("cluster already initialized");
        return false;
    }

    citrusleaf_init();

    cluster = citrusleaf_cluster_create();
    if ( !cluster ) { 
        info("could not create cluster"); 
        return false; 
    }

    if ( citrusleaf_cluster_add_host(cluster, HOST, PORT, TIMEOUT) != 0 ) {
        info("could not connect to host %s port %d", HOST, PORT);
        return false;
    }

    info("connected to %s:%d",HOST,PORT);

	// Find cluster size
	char ** cluster_size_str = get_stats( "statistics", "cluster_size", cluster);
	cluster_size = atol(cluster_size_str[0]);

	// Find if we need to run in memory specific tests or not
	// These tests work only when the namespace is in-memory or 
	// the storage is device based but data is in memory
	run_memory_tests 		= false;
	char * query 			= "namespace/test";
	char ** ns_type 		= get_stats( query, "type", cluster);
	char ** data_in_memory 	= NULL;
	if (strcmp(ns_type[0], "device") == 0) {
		data_in_memory = get_stats(query, "data-in-memory", cluster);
		if(strcmp(data_in_memory[0], "true") == 0) {
			run_memory_tests = true;
		}
	}
	else if(strcmp(ns_type[0], "memory") == 0) {
		run_memory_tests = true;
	}
	
	// Free stuff
	for (int i = 0; i < cluster_size; i++) {
		if (ns_type) free(ns_type[i]);
		if (data_in_memory) free(data_in_memory[i]);
		if (cluster_size_str) free(cluster_size_str[i]);
	}
	free(ns_type);
	free(data_in_memory);
	free(cluster_size_str);

    return true;
}

static bool after(atf_plan * plan) {

    if ( !cluster ) {
        error("cluster was not initialized");
        return false;
    }

    citrusleaf_cluster_destroy( cluster );
    citrusleaf_shutdown();

    info("disconnected from %s:%d",HOST,PORT);

    return true;
}

/******************************************************************************
 * TEST PLAN
 *****************************************************************************/
// NOTE: This is a DECLARING a set of methods; this is NOT an execution.
PLAN( client_test ) {

    plan_before( before );
    plan_after( after );
    
    /**
     * kv - kv tests
     */
    plan_add( kv_string );

    /**
     * ldt tests
     */
    plan_add( lstack_basics );
    plan_add( lset_basics ) ;

    /**
     * record - record tests
     */
    plan_add( record_basics );
    plan_add( record_lists );

    /**
     * stream - stream tests
     */
    plan_add( stream_simple );
    // plan_add( stream_ads );
}
