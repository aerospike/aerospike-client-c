/* *  Citrusleaf/Aerospike Large Set Test Program
 *  as_lset_main.c - Validates AS Large Set stored procedure functionality
 *
 *  Copyright 2013 by Citrusleaf, Aerospike In.c  All rights reserved.
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
#include "citrusleaf/as_lset.h"
#include "citrusleaf/cl_udf.h"
#include <citrusleaf/cf_random.h>
#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_hist.h>
#include <citrusleaf/citrusleaf.h>
#include "as_arraylist.h"

// Global Configuration object: holds client config data.
config *g_config = NULL;

// NOTE: INFO(), ERROR() and LOG() defined in as_lset.h
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
 *  Set up the configuration for the AS Large Set Routines
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
	g_config->package_name = "LSET";

	INFO("[DEBUG]:[%s]: Num Args (%d) g_config(%p)\n", meth, argc, g_config);

	INFO("[DEBUG]:[%s]: About to Process Args (%d)\n", meth, argc );
	int optcase;
	while ((optcase = getopt(argc, argv, "ckmh:p:n:s:P:f:v:x:r:t:i:j:")) != -1){
		INFO("[ENTER]:[%s]: Processings Arg(%d)\n", meth, optcase );
		switch (optcase) {
		case 'h': g_config->host         = strdup(optarg); break;
		case 'p': g_config->port         = atoi(optarg);   break;
		case 'n': g_config->ns           = strdup(optarg);  break;
		case 's': g_config->set          = strdup(optarg); break;
		case 'v': g_config->verbose      = true;           break;
		case 'P': g_config->package_name = strdup(optarg); break;
		default:  usage(argc, argv);                      return(-1);
		}
	}
	return 0;
}

/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Initialize Test: Do the set up for a test so that the regular
 *  Aerospike functions can run.
 */
int test_setup( int argc, char **argv ) {
	static char * meth = "test_setup()";
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
	if ( rc != 0 ){
		INFO("[ERROR]:[%s]:could not connect to host %s port %d",
				meth, g_config->host,g_config->port);
		return(-1);
	}

	g_config->asc  = asc;

	return 0;

} // end test_setup()

/// ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||


/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  AS Large Set INSERT TEST
 *  For a single record, perform a series of Set Inserts
 *  Create a new record, then repeatedly call Set Insert
 */
int lset_insert_test(char * keystr, char * lset_bin, int iterations) {
	static char * meth = "lset_insert_test()";
	int rc = 0;

	INFO("[ENTER]:[%s]: It(%d) Key(%s) LSETBin(%s)\n",
			meth, iterations, keystr, lset_bin );

	cl_cluster * c     = g_config->asc;
	char       * ns    = g_config->ns;
	char       * set   = g_config->set;
	char       * key   = keystr;
	char       * bname = lset_bin;

	INFO("[DEBUG]:[%s]: as_lset_insert() iterations(%d)\n", meth, iterations );
	srand( 200 ); // Init our random number generator with a fixed seed.

	int num_ok   = 0;
	int num_errs = 0;
	for ( int i = 0; i < iterations; i++ ) {
		unsigned int   base       = rand() % 500;
		as_val       * newSetItem = (as_val *) as_integer_new( base );

		rc = as_lset_insert( c, ns, set, key, bname, newSetItem,
							 g_config->package_name, g_config->timeout_ms );
		if (rc) {
			INFO("[ERROR]:[%s]: LSET INSERT Error: i(%d) rc(%d)\n",
				 meth, i, rc );
			num_errs++;
		} else {
			num_ok++;
		}
		as_val_destroy( newSetItem ); // must destroy every iteration.
	} // end for

	fprintf(stderr, "[RESULTS]:<%s>Test Results: Success(%d) Errors(%d)\n", 
			meth, num_ok, num_errs );

	return rc;
} // end lset_insert_test()


/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  AS Large Set SEARCH TEST
 *  For a single record, perform a series of Set Searches
 *  Using the previously created record, repeatedly call Set Search with
 *  various values (some valid, some invalid)
 *  NOTE: We must EXPLICITLY FREE the result, as it is a malloc'd
 *  object that is handed to us.
 */
int lset_search_test(char * keystr, char * lset_bin, int iterations ) {
	static char * meth = "as_lset_search()";
	int rc = 0;

	INFO("[ENTER]:[%s]: Iterations(%d) Key(%s) LSETBin(%s)\n",
			meth, iterations, keystr, lset_bin );

	cl_cluster * c     = g_config->asc;
	char       * ns    = g_config->ns;
	char       * set   = g_config->set;
	char       * key   = keystr;
	char       * bname = lset_bin;

	INFO("[DEBUG]:[%s]: as_lset_search() iterations(%d)\n", meth, iterations );
	srand( 200 ); // Init our random number generator with a fixed seed.

	int num_ok   = 0; // Got what we were looking for
	int num_errs = 0; // Errors
	int num_miss = 0; // Query ok -- but did not find object
	for ( int i = 0; i < iterations; i++ ) {
		unsigned int base = rand() % 500;
		as_val    * newSetItem = (as_val *) as_integer_new( base ); 
		as_result * resultp    = as_lset_search( c, ns, set, key, bname,
												 newSetItem, false,
												 g_config->package_name,
												 g_config->timeout_ms );
		if ( resultp->is_success ) {
			char * valstr = as_val_tostring( resultp->value );
			if (!strcmp(valstr, "\"Not Found\"")) {
				INFO("[DEBUG]:[%s]: LSET SEARCH MISS: i(%d) base(%d) Val(%s)\n",
				 	 meth, i, base, valstr);
				num_miss++;
			} else {
				INFO("[DEBUG]:[%s]: LSET SEARCH HIT: i(%d) base(%d) Val(%s)\n",
				 	 meth, i, base, valstr);
				num_ok++;
			}
			free( valstr );
		} else {
			// Don't break (for now) just keep going.
			INFO("[ERROR]:[%s]: LSET SEARCH Error: i(%d) base(%d)\n",
				 meth, i, base);
			num_errs++;
		}
		as_result_destroy( resultp ); // Clean up -- release the result object
		as_val_destroy( newSetItem ); // must destroy every iteration.
	} // end for each iteration

	fprintf(stderr, "[RESULTS]:<%s>HIT_TEST: Results: " \
					"Success(%d) Miss(%d)\n", 
					meth, num_ok, num_miss );

    int miss = 5; // NEXT TEST: let's miss 5
	num_ok   = 0;
	num_errs = 0;
	num_miss = 0;
	for ( int i = 0; i < miss; i++ ) {
		unsigned int base = rand() % 500;
		as_val    * newSetItem = (as_val *) as_integer_new( base ); 
		as_result * resultp    = as_lset_search( c, ns, set, key, bname,
												 newSetItem, false,
												 g_config->package_name,
												 g_config->timeout_ms );
		if ( resultp->is_success ) {
			char * valstr = as_val_tostring( resultp->value );
			if (!strcmp(valstr, "\"Not Found\"")) {
				INFO("[DEBUG]:[%s]: LSET SEARCH MISS: i(%d) base(%d) Val(%s)\n",
				 	 meth, i, base, valstr);
				num_miss++;
			} else {
				INFO("[DEBUG]:[%s]: LSET SEARCH HIT: i(%d) base(%d) Val(%s)\n",
				 	 meth, i, base, valstr);
				num_ok++;
			}
			free( valstr );
		} else {
			// Don't break (for now) just keep going.
			INFO("[ERROR]:[%s]: LSET SEARCH Error: i(%d) base(%d)\n",
				 meth, i, base);
			num_errs++;
		}
		as_result_destroy( resultp ); // Clean up -- release the result object
		as_val_destroy( newSetItem ); // must destroy every iteration.
	} // end for each iteration

	fprintf(stderr, "[RESULTS]:<%s>MISS_TEST: Results: " \
					"Success(%d) Miss(%d)\n", 
					meth, num_ok, num_miss );

	INFO("[EXIT]:[%s]: RC(%d)\n", meth, rc );
	return rc;
} // end lset_search_test()

/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  This file exercises the AS Large Set Interface.
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
	INFO("[DEBUG]:[%s]: calling test_setup()\n", meth );
	test_setup( argc, argv );

	int iterations = 15;
	// (1) Insert Test
	INFO("[DEBUG]:[%s]: calling lset_insert_test()\n", meth );
	rc = lset_insert_test(user_key, lso_bin_name, iterations );
	if (rc) {
		INFO("[ERROR]:[%s]: lset_insert_test() RC(%d)\n", meth, rc );
		return( rc );
	}

	// (2) Search Test
	INFO("[DEBUG]:[%s]: calling lset_search_test()\n", meth );
	rc = lset_search_test( user_key, lso_bin_name, iterations );
    if (rc) {
		INFO("[ERROR]:[%s]: lset_search_test() RC(%d)\n", meth, rc );
		return( rc );
	}

	// (3) Delete Test
	//

	return 0;
} // end main()
