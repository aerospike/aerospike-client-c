/*  Citrusleaf Large Object Stack Test Program
 *  as_lso_main.c - Simple LSO example
 *
 *  Copyright 2013 by Citrusleaf, Aerospike Inc.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <stdarg.h>
#include <stdbool.h>
#include <assert.h>
#include <ctype.h>

#include <fcntl.h>
#include <sys/stat.h>

#include "citrusleaf/citrusleaf.h"
#include "citrusleaf/as_lso.h"
#include "citrusleaf/cl_udf.h"
#include <citrusleaf/cf_random.h>
#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_hist.h>
#include <citrusleaf/citrusleaf.h>
#include "as_arraylist.h"

// Global Configuration object: holds client config data.
config *g_config = NULL;

// NOTE: INFO(), ERROR() and LOG() defined in as_lso.h
void __log_append(FILE * f, const char * prefix, const char * fmt, ...) {
	char msg[128] = {0};
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(msg, 128, fmt, ap);
	va_end(ap);
	fprintf(f, "%s%s\n",prefix,msg);
}

/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Show Usage
 */
void usage(int argc, char *argv[]) {
	INFO("Usage %s:", argv[0]);
	INFO("   -h host [default 127.0.0.1] ");
	INFO("   -p port [default 3000]");
	INFO("   -n namespace [default test]");
	INFO("   -s set [default *all*]");
}

/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Set up the configuration for the LSO Routines
 */
int init_configuration (int argc, char *argv[]) {
	static char * meth = "init_configuration()";
	INFO("[ENTER]:[%s]: Num Args (%d)\n", meth, argc );

	g_config = (config *)malloc(sizeof(config));
	memset(g_config, 0, sizeof(g_config));

	g_config->host         = "127.0.0.1";
	g_config->port         = 3000;
	g_config->ns           = "test";
	g_config->set          = "demo";
	g_config->timeout_ms   = 5000;
	g_config->record_ttl   = 864000;
	g_config->verbose      = false;
	g_config->package_name = "LsoStoneman";

	INFO("[DEBUG]:[%s]: Num Args (%d) g_config(%p)\n", meth, argc, g_config);

	INFO("[DEBUG]:[%s]: About to Process Args (%d)\n", meth, argc );
	int optcase;
	while ((optcase = getopt(argc, argv, "ckmh:p:n:s:P:f:v:x:r:t:i:j:")) != -1){
		INFO("[ENTER]:[%s]: Processings Arg(%d)\n", meth, optcase );
		switch (optcase) {
		case 'h': g_config->host    = strdup(optarg); break;
		case 'p': g_config->port    = atoi(optarg);   break;
		case 'n': g_config->ns      = strdup(optarg);
		break;
		case 's': g_config->set     = strdup(optarg); break;
		case 'v': g_config->verbose = true;           break;
		default:  usage(argc, argv);                  return(-1);
		}
	}
	return 0;
}

// |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Initialize Test: Do the set up for a test so that the regular
 *  Aerospike functions can run.
 */
int setup_test( int argc, char **argv ) {
	static char * meth = "setup_test()";
	int rc = 0;

	INFO("[ENTER]:[%s]: Args(%d) g_config(%p)\n", meth, argc, g_config );

	if (init_configuration(argc,argv) !=0 ) { // reading parameters
		return -1;
	}

	// show cluster setup
	INFO("[DEBUG]:[%s]Startup: host %s port %d ns %s set %s",
			meth, g_config->host, g_config->port, g_config->ns,
			g_config->set == NULL ? "" : g_config->set);

	citrusleaf_init();
	citrusleaf_set_debug(true);

	// create the cluster object
	cl_cluster *asc = citrusleaf_cluster_create();
	if (!asc) { 
		INFO("[ERROR]:[%s]: Fail on citrusleaf_cluster_create()");
		return(-1); 
	}

	rc = citrusleaf_cluster_add_host(asc, g_config->host, g_config->port,
									 g_config->timeout_ms);
	if (rc) {
		INFO("[ERROR]:[%s]:could not connect to host %s port %d",
				meth, g_config->host,g_config->port);
		return(-1);
	}

	g_config->asc  = asc;

	return 0;
} // end setup_test()

/// ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||

/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  LSO PUSH TEST
 *  For a single record, perform a series of STACK PUSHES.
 *  Create a new record, then repeatedly call stack push.
 */
int lso_push_test(char * keystr, char * lso_bin, int iterations) {
	static char * meth = "lso_push_test()";
	int rc = 0;

	INFO("[ENTER]:[%s]: It(%d) Key(%s) LSOBin(%s)\n",
			meth, iterations, keystr, lso_bin );

	// Create the LSO Bin
    // PageMode=List -> Overriding Default PageMode(Bytes)
    as_map *create_args = as_hashmap_new(1);
    as_map_set(create_args, (as_val *)as_string_new("PageMode", false),
						    (as_val *)as_string_new("List", false));
	rc = as_lso_create( g_config->asc, g_config->ns, g_config->set,
						keystr, lso_bin, create_args, g_config->package_name,
						g_config->timeout_ms);
	if( rc < 0 ){
		INFO("[ERROR]:[%s]: LSO Create Error: rc(%d)\n", meth, rc );
		return rc;
	}

	cl_cluster * c     = g_config->asc;
	char       * ns    = g_config->ns;
	char       * set   = g_config->set;
	char       * key   = keystr;
	char       * bname = lso_bin;

	INFO("[DEBUG]:[%s]: Run as_lso_push() iterations(%d)\n", meth, iterations );
	for ( int i = 0; i < iterations; i++ ) {
        int val = i * 10;
		as_list * listp = as_arraylist_new( 5, 5 );
        int64_t urlid   = val + 1; // Generate URL_ID
		as_list_add_integer( listp, urlid );
        int64_t created = val + 2; // Generate CREATED
		as_list_add_integer( listp, created );
        int64_t meth_a  = val + 3; // Generate first half of method
		as_list_add_integer( listp, meth_a );
        int64_t meth_b  = val + 4; // Generate 2nd half of method
		as_list_add_integer( listp, meth_b );
        int64_t status  = val + 5; // Generate status
		as_list_add_integer( listp, status );

		rc = as_lso_push( c, ns, set, key, bname, (as_val *)listp,
						  g_config->package_name, g_config->timeout_ms);
		if (rc) {
			INFO("[ERROR]:[%s]: LSO PUSH Error: i(%d) rc(%d)\n", meth, i, rc );
            return -1;
		}
		as_val_destroy( listp ); // must destroy every iteration.
		listp = NULL;
	} // end for

	return rc;
} // end lso_push_test()

// |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||

/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  LSO PEEK TEST
 *  For a single record, perform a series of STACK PEEKS.
 *  Using the previously created record, repeatedly call stack peek with
 *  varying numbers of peek counts.
 *  NOTE: We must EXPLICITLY FREE the result, as it is a malloc'd
 *  object that is handed to us.
 */
int lso_peek_test(char * keystr, char * lso_bin, int iterations ) {
	static char * meth = "lso_peek_test()";
	int rc = 0;
	as_result * resultp;

	INFO("[ENTER]:[%s]: Iterations(%d) Key(%s) LSOBin(%s)\n",
			meth, iterations, keystr, lso_bin );

	cl_cluster * c     = g_config->asc;
	char       * ns    = g_config->ns;
	char       * set   = g_config->set;
	char       * key   = keystr;
	char       * bname = lso_bin;

	INFO("[DEBUG]:[%s]: Run as_lso_peek() iterations(%d)\n", meth, iterations );

	int    peek_count = 1;
	char * valstr     = NULL; // Hold Temp results from as_val_tostring()
	// NOTE: Must FREE the result for EACH ITERATION.
	for ( int i = 0; i < iterations ; i ++ ){
		peek_count++;
		resultp = as_lso_peek( c, ns, set, key, bname, peek_count,
							   g_config->package_name, g_config->timeout_ms);
		if ( resultp->is_success ) {
			valstr = as_val_tostring( resultp->value );
			printf("LSO PEEK SUCCESS: peek_count(%d) Val(%s)\n",
				   peek_count, valstr);
			free( valstr );
		} else {
			INFO("[ERROR]:[%s]: LSO PEEK Error: i(%d) \n", meth, i );
			// Don't break (for now) just keep going.
		}
		// Clean up -- release the result object
		as_result_destroy( resultp );
	} // end for each peek iteration

	INFO("[EXIT]:[%s]: RC(%d)\n", meth, rc );
	return rc;
} // end lso_peek_test()

/// ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||

/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  LSO PUSH WITH_TRANSFORM TEST
 *  For a single record, perform a series of STACK PUSHES of BYTE-PACKED data.
 *  Create a new record, then repeatedly call stack push.
 */
int lso_push_with_transform_test(char * keystr, char * lso_bin,
								 char * compress_func, as_list * compress_args,
								 int iterations) {
	static char * meth = "lso_push_with_transform_test()";
	int rc = 0;

	INFO("[ENTER]:[%s]: It(%d) Key(%s) LSOBin(%s)\n",
			meth, iterations, keystr, lso_bin );

	// Abbreviate for simplicity.
	cl_cluster * c  = g_config->asc;
	char       * ns = g_config->ns;
	char       * set  = g_config->set;
	char       * key  = keystr;
	char       * bname  = lso_bin;

	INFO("[DEBUG]:[%s]: Run as_lso_push_with_transform() iterations(%d)\n",
		  meth, iterations );
	for ( int i = 0; i < iterations; i++ ) {
        int val         = i * 10;
		as_list * listp = as_arraylist_new( 5, 5 );
        int64_t urlid   = val + 1;
		as_list_add_integer( listp, urlid );
        int64_t created = val + 2;
		as_list_add_integer( listp, created );
        int64_t meth_a  = val + 3;
		as_list_add_integer( listp, meth_a );
        int64_t meth_b  = val + 4;
		as_list_add_integer( listp, meth_b );
        int64_t status  = val + 5;
		as_list_add_integer( listp, status );

		rc = as_lso_push_with_transform( c, ns, set, key, bname,
										 (as_val *)listp,
						  				 g_config->package_name,
										 compress_func, compress_args,
										 g_config->timeout_ms);
		if (rc) {
			INFO("[ERROR]:[%s]: LSO PUSH WITH TRANSFROM Error: i(%d) rc(%d)\n",
				  meth, i, rc );
            return -1;
		}
		as_val_destroy( listp ); // must destroy every iteration.
		listp = NULL;
	} // end for

	return rc;
} // end lso_push_with_transform_test()

/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  LSO PEEK WITH TRANSFORM TEST
 *  For a single record, perform a series of STACK PEEKS.
 *  and do a server side transform of the byte-packed data
 *  Using the previously created record, repeatedly call stack peek with
 *  varying numbers of peek counts.
 */
int lso_peek_with_transform_test(char * keystr, char * lso_bin,
								 char * uncompress_func,
								 as_list * uncompress_args,
								 int iterations ) {
	static char * meth = "lso_peek_with_transform_test()";
	int rc = 0;

	INFO("[ENTER]:[%s]: Iterations(%d) Key(%s) LSOBin(%s)\n",
			meth, iterations, keystr, lso_bin );

	cl_cluster * c     = g_config->asc;
	char       * ns    = g_config->ns;
	char       * set   = g_config->set;
	char       * key   = keystr;
	char       * bname = lso_bin;

	INFO("[DEBUG]:[%s]: Run as_lso_peek() iterations(%d)\n", meth, iterations );

	// NOTE: Must FREE the result for EACH ITERATION.
	int peek_count = 2; // Soon -- set by Random Number
	char * valstr = NULL; // Hold Temp results from as_val_tostring()
	for ( int i = 0; i < iterations ; i ++ ){
		peek_count++;
		as_result * resultp = as_lso_peek_with_transform( c, ns, set, key,
											bname, peek_count,
											g_config->package_name,
											uncompress_func, uncompress_args,
											g_config->timeout_ms);
		if ( resultp->is_success ) {
			valstr = as_val_tostring( resultp->value );
			printf("LSO PEEK WITH TRANSFORM SUCCESS: peek_count(%d) Val(%s)\n",
				   peek_count, valstr);
			free( valstr );
		} else {
			INFO("[ERROR]:[%s]: LSO PEEK WITH TRANSFORM Error: i(%d) \n",
				 meth, i );
			// Don't break (for now) just keep going.
		}
		// Clean up -- release the result object
		as_result_destroy( resultp );
	} // end for each peek iteration

	INFO("[EXIT]:[%s]: RC(%d)\n", meth, rc );
	return rc;
} // end lso_peek_with_transform_test()


/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  This file exercises the LSO Interface.
 *  We have the following choice
 *  (1) Do some simple "manual inserts"
 *  (2) Do some automatic generation (generate key, generate entry)
 *  (3) Do some generation from File (read file entry, insert)
 */
int main(int argc, char **argv) {
	static char * meth         = "main()";
	int           rc           = 0;

	char        * user_key     = "User_111";
    char        * lso_bin_name = "urlid_stack";

	INFO("[ENTER]:[%s]: Start in main()\n", meth );

	// Initialize everything
	INFO("[DEBUG]:[%s]: calling setup_test()\n", meth );
	setup_test( argc, argv );

    int iterations = 15;
	// (1) Push Test
	INFO("[DEBUG]:[%s]: calling lso_push_test()\n", meth );
	rc = lso_push_test( user_key, lso_bin_name, iterations );
	if (rc) {
		INFO("[ERROR]:[%s]: lso_push_test() RC(%d)\n", meth, rc );
		return( rc );
	}

	// (2) Peek Test
	INFO("[DEBUG]:[%s]: calling lso_peek_test()\n", meth );
	rc = lso_peek_test( user_key, lso_bin_name, iterations );
    if (rc) {
		INFO("[ERROR]:[%s]: lso_peek_test() RC(%d)\n", meth, rc );
		return( rc );
	}

    // Next 2 tests -> new user
	user_key = "User_222";

	char * compress_func   = "stumbleCompress5";
    as_list *compress_args = as_arraylist_new( 1, 1 );
	as_list_add_integer( compress_args, 1 ); // dummy argument

	// (3) Push Test With Transform
	INFO("[DEBUG]:[%s]: calling lso_push_with_transform_test()\n", meth );
	rc = lso_push_with_transform_test( user_key, lso_bin_name,
									   compress_func, compress_args,
									   iterations );
	if (rc) {
		INFO("[ERROR]:[%s]: lso_push_with_transform_test() RC(%d)\n", meth, rc);
		return( rc );
	}

	// (4) Peek Test With Transform
	char * uncompress_func = "stumbleUnCompress5";
    as_list *uncompress_args = as_arraylist_new( 1, 1 );
	as_list_add_integer( uncompress_args, 1 ); // dummy argument

	INFO("[DEBUG]:[%s]: calling lso_peek_with_transform_test()\n", meth );
	rc = lso_peek_with_transform_test( user_key, lso_bin_name,
									   uncompress_func, uncompress_args,
									   iterations );
	if (rc) {
		INFO("[ERROR]:[%s]: lso_peek_with_transform_test() RC(%d)\n", meth, rc);
		return( rc );
	}

	exit(0);
} // end main()
