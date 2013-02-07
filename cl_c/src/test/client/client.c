#include "../test.h"
#include "citrusleaf/citrusleaf.h"

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define HOST "127.0.0.1"
#define PORT 3000
#define TIMEOUT 1000

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

    return true;
}

static bool after(atf_plan * plan) {

    if ( !cluster ) {
        error("cluster was not initialized");
        return false;
    }

    citrusleaf_cluster_destroy( cluster );
    citrusleaf_shutdown();

    return true;
}

/******************************************************************************
 * TEST PLAN
 *****************************************************************************/

PLAN( client ) {

    plan_before( before );
    plan_after( after );

    plan_add( client_string );
}
