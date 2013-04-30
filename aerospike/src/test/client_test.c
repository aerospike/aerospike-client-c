#include "test.h"
#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/cl_udf.h>
#include <errno.h>

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
    // plan_add( kv_string );

    /**
     * ldt tests
     */
     

    /**
     * record - record tests
     */
    // plan_add( record_basics );
    // plan_add( record_lists );

    /**
     * stream - stream tests
     */
    // plan_add( stream_simple );
    // plan_add( stream_ads );
}
