
// #include "../test.h"
// #include "../util/udf.h"
// #include <citrusleaf/citrusleaf.h>
// #include <citrusleaf/as_types.h>
// #include <citrusleaf/aerospike_lstack.h>

#include "lstack_test.h"  // Everything a growing test needs

// LSTACK UTILITIES: 
// Common methods used by the lstack tests
// (*) push()
// (*) peek()
// (*) size()
// (*) config()

static char * MOD = "lstack_util.c::13_04_26";
/******************************************************************************
 * Utility Functions
 *****************************************************************************/

/**
 * Define our mechanism for tracing/debugging.  Statically included or
 * excluded at compile time (see log.h).
 */
void __log_append(FILE * f, const char * prefix, const char * fmt, ...) {
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
 */
int setup_test() {
    static char * meth = "setup_test()";
    int rc = 0;
    char * host; // for each iteration of "add host"
    int port; // for each iteration of "add host"
    uint32_t timeout_ms;
    int i = 0;

    // show cluster setup
    INFO("[DEBUG]:<%s:%s>Startup: host %s port %d ns %s set %s",
            MOD, meth, g_config->host, g_config->port, g_config->ns,
            g_config->set == NULL ? "" : g_config->set);

    citrusleaf_init();
    citrusleaf_set_debug(true);

    // create the cluster object
    cl_cluster *asc = citrusleaf_cluster_create();
    if (!asc) { 
        INFO("[ERROR]:<%s:%s>: Fail on citrusleaf_cluster_create()",MOD,meth);
        return(-1); 
    }

    // If we have "cluster" defined, then we'll go with that (manually
    // set up in main.c: setup_cluster().  Otherwise, we will default
    // to local host (also defined in g_config).
    if( g_config->cluster_count <= 0 ) {
        g_config->cluster_count = 1;
        g_config->cluster_name[0] = g_config->host; 
        g_config->cluster_port[0] = g_config->port; 
    }
    timeout_ms = g_config->timeout_ms;
    for( i = 0; i < g_config->cluster_count; i++ ){
        host = g_config->cluster_name[i];
        port = g_config->cluster_port[i];
        INFO("[DEBUG]:<%s:%s>:Adding host(%s) port(%d)", MOD, meth, host, port);
        rc = citrusleaf_cluster_add_host(asc, host, port, timeout_ms);
        if ( rc != CITRUSLEAF_OK ) {
            INFO("[ERROR]:<%s:%s>:could not connect to host(%s) port(%d)",
                    MOD, meth, host, port);

            INFO("[ERROR]:<%s:%s>:Trying more nodes", MOD, meth );
            // return(-1);
        }
    } // end for each cluster server

    g_config->asc  = asc;

    return 0;
} // end setup_test()


/**
 * Close up the shop.
 */
int shutdown_test() {
    if (g_config->asc) citrusleaf_cluster_destroy(g_config->asc);
    citrusleaf_shutdown();
    return 0;
} // end shutdown_test()
