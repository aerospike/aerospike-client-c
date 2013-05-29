
// #include "../test.h"
// #include "../util/udf.h"
// #include <citrusleaf/citrusleaf.h>
// #include <citrusleaf/as_types.h>
// #include <citrusleaf/aerospike_lset.h>

#include "lset_test.h"  // Everything a growing test needs

// LSET UTILITIES: 
// Common methods used by the lset tests
// (*) insert()
// (*) search()
// (*) size()
// (*) config()

static char * MOD = "lset_util.c::13_04_26";


extern cl_cluster * cluster;

/******************************************************************************
 * Utility Functions
 *****************************************************************************/

/**
 * Define our mechanism for tracing/debugging.  Statically included or
 * excluded at compile time (see log.h).
 */

void lset__log_append(FILE * f, const char * prefix, const char * fmt, ...) {
    char msg[128] = {0};
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, 128, fmt, ap);
    va_end(ap);
    fprintf(f, "%s%s\n",prefix,msg);
}


/** 
 *  Initialize Test: Do the set up for a test so that the regular
 *  Aerospike functions can run.
 *
 *  @TODO: FIX REFERENCES TO UNDEFINED lset_g_config
 *
 */
int lset_setup_test() {
    // static char * meth = "setup_test()";
    // printf("ENTER<%s:%s> \n", MOD, meth );

    // int rc = 0;
    // char * host; // for each iteration of "add host"
    // int port; // for each iteration of "add host"
    // uint32_t timeout_ms;
    // int i = 0;

    // printf("[DEBUG]<%s:%s> About to config::\n", MOD, meth );
    lset_set_config_defaults( lset_g_config );

    // // show cluster setup
    // INFO("[DEBUG]:<%s:%s>Startup: host %s port %d ns %s set %s",
    // MOD, meth, lset_g_config->host, lset_g_config->port, lset_g_config->ns,
    // lset_g_config->set == NULL ? "" : lset_g_config->set);

    // citrusleaf_init();
    // citrusleaf_set_debug(true);

    // // create the cluster object
    //     cl_cluster *asc = citrusleaf_cluster_create();
    //     if (!asc) { 
    //     INFO("[ERROR]:<%s:%s>: Fail on citrusleaf_cluster_create()",MOD,meth);
    //     return(-1); 
    // }

    // // If we have "cluster" defined, then we'll go with that (manually
    // // set up in main.c: setup_cluster().  Otherwise, we will default
    // // to local host (also defined in lset_g_config).
    // if( lset_g_config->cluster_count <= 0 ) {
    //     lset_g_config->cluster_count = 1;
    //     lset_g_config->cluster_name[0] = lset_g_config->host; 
    //     lset_g_config->cluster_port[0] = lset_g_config->port; 
    // }
    // timeout_ms = lset_g_config->timeout_ms;
    // for( i = 0; i < lset_g_config->cluster_count; i++ ){
    //     host = lset_g_config->cluster_name[i];
    //     port = lset_g_config->cluster_port[i];
    //     INFO("[DEBUG]:<%s:%s>:Adding host(%s) port(%d)", MOD, meth, host, port);
    //     rc = citrusleaf_cluster_add_host(asc, host, port, timeout_ms);
    //     if ( rc != CITRUSLEAF_OK ) {
    //         INFO("[ERROR]:<%s:%s>:could not connect to host(%s) port(%d)",
    //         MOD, meth, host, port);
    //         INFO("[ERROR]:<%s:%s>:Trying more nodes", MOD, meth );
    //         return(-1);
    //     }
    // } // end for each cluster server

    lset_g_config->asc  = cluster;

    return 0;
} // end setup_test()


/**
 * Close up the shop.
 *
 *  @TODO: FIX REFERENCES TO UNDEFINED lset_g_config
 *
 */
int lset_shutdown_test() {
    // if (lset_g_config->asc) citrusleaf_cluster_destroy(lset_g_config->asc);
    // citrusleaf_shutdown();
    return 0;
} // end shutdown_test()
