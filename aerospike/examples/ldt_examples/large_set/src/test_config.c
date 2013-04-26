/*  Citrusleaf Large Object Stack Test Program
 *  config.c - Setting up configuration for Large Stack
 *
 *  Copyright 2013 by Citrusleaf, Aerospike Inc.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */

#include "test_config.h"

/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Set up the configuration for the LSO Routines.  The config structure
 *  was already built by the caller (malloc'd) so all we do here is fill
 *  in the appropriate fields.
 */
int set_config_defaults ( test_config * config_ptr ){
    static char * meth = "init_configuration()";

    config_ptr->host         = "127.0.0.1";
    config_ptr->port         = 3000;
    config_ptr->ns           = "test";
    config_ptr->set          = "demo";
    config_ptr->timeout_ms   = 5000;
    config_ptr->record_ttl   = 864000;
    config_ptr->verbose      = true;
    config_ptr->package_name = "LSET";

    config_ptr->n_threads    = 5;
    config_ptr->n_iterations = 10;
    config_ptr->n_keys       = 100;
    config_ptr->key_max      = 20; // make it smaller than num keys so that
                                   // we get record reuse.

    config_ptr->key_type = NUMBER_FORMAT; // default to just numbers
    config_ptr->key_len = 0;
    config_ptr->key_compare = NULL;  // Name of Key Compare (Lua) function

    config_ptr->value_len = 0;
    config_ptr->obj_type = NULL;     // Type of object for storage (and compare)
    config_ptr->obj_compare = NULL;  // Name of object compare Lua function

    config_ptr->strict       = false;

    return 0;
}


