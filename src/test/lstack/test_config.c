/*  Citrusleaf Large Object Stack Test Program
 *  config.c - Setting up configuration for Large Stack
 *
 *  Copyright 2013 by Citrusleaf, Aerospike Inc.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */

#include "test_config.h"
#include "../aerospike_test.h"

/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Set up the configuration for the LSO Routines.  The config structure
 *  was already built by the caller (malloc'd) so all we do here is fill
 *  in the appropriate fields.
 */
int set_config_defaults ( test_config * config_ptr ){
    static char * meth = "init_configuration()";

    config_ptr->host         = g_host;
    config_ptr->port         = 3000;
    config_ptr->ns           = "test";
    config_ptr->set          = "demo";
    config_ptr->timeout_ms   = 5000;
    config_ptr->record_ttl   = 864000;
    config_ptr->verbose      = true;
    config_ptr->package_name = "LSTACK";

    config_ptr->n_threads    = 5;
    config_ptr->n_iterations = 10;
    config_ptr->n_keys       = 100;
    config_ptr->key_max      = 20; // make it smaller than num keys so that
                                   // we get record reuse.
    config_ptr->peek_max     = 100; // Want semi-small default max value for
                                    // the max size peek we can generate.

//     config_ptr->strict       = false;

    return 0;
}


