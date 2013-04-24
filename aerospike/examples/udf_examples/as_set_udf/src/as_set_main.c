/* *  Citrusleaf Large Object Stack Test Program
 *  as_lso_main.c - Validates LSO stored procedure functionality
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
#include "lso_udf.h"
#include "citrusleaf/cl_udf.h"
#include <citrusleaf/cf_random.h>
#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_hist.h>
#include <citrusleaf/citrusleaf.h>
#include "as_arraylist.h"

// Use this to turn on extra debugging prints and checks
#define TRA_DEBUG true

// Global Configuration object that holds ALL needed client data.
config *g_config = NULL;

// NOTE: INFO(), ERROR() and LOG() defined in lso_udf.h

/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 * Define the LOG append call: For tracing/debugging
 */
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
	INFO("   -f udf_file [default lua_files/udf_unit_test.lua]");
}

/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Set up the configuration for the LSO Routines
 */
int init_configuration (int argc, char *argv[])
{
	static char * meth = "init_configuration()";
	INFO("[ENTER]:[%s]: Num Args (%d)\n", meth, argc );

	g_config = (config *)malloc(sizeof(config));
	memset(g_config, 0, sizeof(g_config));

	g_config->host         = "127.0.0.1";
	g_config->port         = 3000;
	g_config->hot_ns       = "test";
	g_config->cold_ns      = "test";
	g_config->set          = "demo";
	g_config->timeout_ms   = 5000;
	g_config->record_ttl   = 864000;
	g_config->verbose      = false;
	g_config->package_file = "../../lua_files/LsoStrawman.lua";
	// g_config->package_file = "../../lua_files/LsoStickman.lua";
	g_config->filter_name  = "../../lua_files/LsoFilter.lua";
	g_config->package_name = "LsoStrawman";
	// g_config->package_name = "LsoStickman";

	INFO("[DEBUG]:[%s]: Num Args (%d) g_config(%p)\n", meth, argc, g_config);

	INFO("[DEBUG]:[%s]: About to Process Args (%d)\n", meth, argc );
	int optcase;
	while ((optcase = getopt(argc, argv, "ckmh:p:n:s:P:f:v:x:r:t:i:j:")) != -1) {
		INFO("[ENTER]:[%s]: Processings Arg(%d)\n", meth, optcase );
		switch (optcase) {
		case 'h': g_config->host         = strdup(optarg);          break;
		case 'p': g_config->port         = atoi(optarg);            break;
		case 'n': g_config->hot_ns = g_config->cold_ns = strdup(optarg);
		break;
		case 's': g_config->set          = strdup(optarg);          break;
		case 'v': g_config->verbose      = true;                    break;
		case 'f': g_config->package_file = strdup(optarg);          break;
		case 'P': g_config->package_name = strdup(optarg);          break;
		default:  usage(argc, argv);                      return(-1);
		}
	}
	return 0;
}


// =======================================================================
// Define the LSO functions (extern)
// =======================================================================

int as_lso_create(cl_cluster *, char *, char *, char *, char *);
int as_lso_push(cl_cluster *, char *, char *, char *, char *, as_val *);
int as_lso_push_with_transform(cl_cluster *, char *, char *, char *,
		char *, as_val *, char *, char *, as_list *);
as_result * as_lso_peek(cl_cluster *, char *, char *, char *, char *, int);
as_result * as_lso_peek_with_transform(cl_cluster *, char *, char *, char *,
		char *, int, char *, char *, as_list *);
int as_lso_trim(cl_cluster *, char *, char *, char *, char *, int);

// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
// Functions in this module:
// (00) UTILITY FUNCTIONS
// (02) main()
// (03) register_udf()
// (10) RECORD FUNCTIONS
// (11) record_put()
// (12) record_delete()
// (20) LSO FUNCTIONS
// (21) lso_push_test()
// (22) lso_peek_test()
// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Perform a simple record PUT with the supplied key.  Use the default
 *  config parameters for all other values.
 */
int record_put( char * keystr, char * binname, char * valstr ) {
	static char * meth = "record_put()";
	int rc = 0;

	cl_write_parameters cl_wp;
	cl_write_parameters_set_default(&cl_wp);
	cl_wp.timeout_ms = g_config->timeout_ms;
	cl_wp.record_ttl = 864000;

	// Do a regular insert record
	cl_object o_key;
	citrusleaf_object_init_str(&o_key,keystr);		
	cl_bin bins[1];
	strcpy(bins[0].bin_name, binname );
	citrusleaf_object_init_str(&bins[0].object, valstr );
	rc = citrusleaf_put(g_config->asc, g_config->hot_ns, g_config->set,
			&o_key, bins, 1, &cl_wp);
	citrusleaf_object_free(&bins[0].object);
	if( rc != CITRUSLEAF_OK ) {
		citrusleaf_object_free(&o_key);		
		INFO("[DEBUG]:[%s]:failed inserting test data rc(%d)",meth, rc);
		return -1;
	}
	return rc;
} // end record_put()

/// ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||


/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Perform a simple record DELETE with the supplied key.  Use the default
 *  config parameters for all other values.
 */
int record_delete( char * keystr, char * binname, char * valstr ) {
	static char * meth = "record_delete()";
	int rc = 0;
	if( TRA_DEBUG )
		INFO("[ENTER]:[%s]: Key(%s) Bin(%s)",meth, keystr, binname );

	cl_write_parameters cl_wp;
	cl_write_parameters_set_default(&cl_wp);
	cl_wp.timeout_ms = g_config->timeout_ms;
	cl_wp.record_ttl = 864000;

	// Do a regular delete record
	cl_object o_key;
	citrusleaf_object_init_str(&o_key,keystr);		

	rc = citrusleaf_delete(g_config->asc, g_config->hot_ns, g_config->set,
			&o_key, &cl_wp);
	if ( rc != CITRUSLEAF_OK &&  rc != CITRUSLEAF_FAIL_NOTFOUND ) {
		citrusleaf_object_free(&o_key);		
		INFO("[DEBUG]:[%s]:failed deleting test data rsp=%d",meth, rc);
		return -1;
	}
	if( TRA_DEBUG ) INFO("[EXIT]:[%s]: RC(%d)",meth, rc);
	return rc;
} // end record_delete()

/// ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
//  util_make_tuple: Create a list of values to store
void make_tuple( as_list * listp, int i ){
	as_list_add_integer( listp, (int64_t) (i + 1) );
	as_list_add_integer( listp, (int64_t) (i + 2) );
	as_list_add_integer( listp, (int64_t) (i + 3) );
	as_list_add_integer( listp, (int64_t) (i + 4) );
}

/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  LSO PUSH TEST
 *  For a single record, perform a series of STACK PUSHES.
 *  Create a new record, then repeatedly call stack push.
 */
int lso_push_test(int iterations, char * bin_name, char * keystr, char * val,
		char * lso_bin)
{
	static char * meth = "lso_push_test()";
	int rc = 0;
	as_list * listp;

	INFO("[ENTER]:[%s]: It(%d) UsrBin(%s) Key(%s) Val(%s) LSOBin(%s)\n",
			meth, iterations, bin_name, keystr, val, lso_bin );

	// Create the base record.
	if(( rc = record_put( keystr, bin_name, val )) < 0 ){
		INFO("[ERROR]:[%s]: Record Put Error: rc(%d)\n", meth, rc );
		return rc;
	}

	// Create the LSO Bin
	rc = as_lso_create( g_config->asc, g_config->hot_ns, g_config->set,
			keystr, lso_bin );
	if( rc < 0 ){
		INFO("[ERROR]:[%s]: LSO Create Error: rc(%d)\n", meth, rc );
		return rc;
	}

	// Abbreviate for simplicity.
	cl_cluster * c = g_config->asc;
	char * ns = g_config->hot_ns;
	char * s = g_config->set;
	char * k = keystr;
	char * b = lso_bin;

	INFO("[DEBUG]:[%s]: Run as_lso_push() iterations(%d)\n", meth, iterations );

	for( int i = 0; i < (iterations * 10); i += 10 ){
		listp = as_arraylist_new( 4, 4 );
		// make_tuple( listp, i);
		as_list_add_integer( listp, (int64_t) (i + 1) );
		as_list_add_integer( listp, (int64_t) (i + 2) );
		as_list_add_integer( listp, (int64_t) (i + 3) );
		as_list_add_integer( listp, (int64_t) (i + 4) );

		if( TRA_DEBUG ){
			char * valstr = as_val_tostring( listp );
			INFO("[DEBUG]:[%s]: Pushing (%s) \n", meth, valstr );
			free( valstr );
		}

		if(( rc = as_lso_push( c, ns, s, k, b,  (as_val *) listp )) < 0 ){
			INFO("[ERROR]:[%s]: LSO PUSH Error: i(%d) rc(%d)\n", meth, i, rc );
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
int lso_peek_test(char * keystr, char * lso_bin, int iterations )
{
	static char * meth = "lso_peek_test()";
	int rc = 0;
	// as_list stack_tuple;
	// as_list * listp;
	as_result * resultp;

	INFO("[ENTER]:[%s]: Iterations(%d) Key(%s) LSOBin(%s)\n",
			meth, iterations, keystr, lso_bin );

	// Abbreviate for simplicity.
	cl_cluster * c = g_config->asc;
	char * ns = g_config->hot_ns;
	char * s = g_config->set;
	char * k = keystr;
	char * b = lso_bin;

	INFO("[DEBUG]:[%s]: Run as_lso_peek() iterations(%d)\n", meth, iterations );

	// NOTE: Must FREE the result for EACH ITERATION.
	int peek_count = 5; // Soon -- set by Random Number
	char * valstr = NULL; // Hold Temp results from as_val_tostring()
	for( int i = 0; i < iterations ; i ++ ){
		peek_count += i;
		resultp = as_lso_peek( c, ns, s, k, b, peek_count );
		if( resultp->is_success ) {
			valstr = as_val_tostring( resultp->value );
			INFO("[DEBUG]:[%s]: LSO PEEK SUCCESS: i(%d) Val(%s)\n", meth, i, valstr);
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

// |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||

/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *
 */
int register_package() 
{ 
	INFO("Opening package file %s",g_config->package_file);  
	FILE *fptr = fopen(g_config->package_file,"r"); 
	if (!fptr) { 
		INFO("cannot open script file %s : %s",g_config->package_file,strerror(errno));  
		return(-1); 
	} 
	int max_script_len = 1048576; 
	byte *script_code = (byte *)malloc(max_script_len); 
	memset(script_code, 0, max_script_len);
	if (script_code == NULL) { 
		INFO("malloc failed"); return(-1); 
	}     

	byte *script_ptr = script_code; 
	int b_read = fread(script_ptr,1,512,fptr); 
	int b_tot = 0; 
	while (b_read) { 
		b_tot      += b_read; 
		script_ptr += b_read; 
		b_read      = fread(script_ptr,1,512,fptr); 
	}                        
	fclose(fptr); 

	char *err_str = NULL; 
	as_bytes udf_content = {
			.len = b_tot,
			.value = script_code
	}; 

	if (b_tot>0) { 
		int resp = citrusleaf_udf_put(g_config->asc, basename(g_config->package_file), &udf_content, AS_UDF_LUA, &err_str); 
		if (resp!=0) { 
			INFO("unable to register package file %s as %s resp = %d",g_config->package_file,g_config->package_name,resp); return(-1);
			INFO("%s",err_str); free(err_str);
			free(script_code);
			return(-1);
		}
		INFO("successfully registered package file %s as %s",g_config->package_file,g_config->package_name); 
	} else {   
		INFO("unable to read package file %s as %s b_tot = %d",g_config->package_file,g_config->package_name,b_tot); return(-1);    
	}
	free(script_code);
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

	// reading parameters
	if (init_configuration(argc,argv) !=0 ) {
		return -1;
	}

	// show cluster setup
	INFO("[DEBUG]:[%s]Startup: host %s port %d ns %s set %s file %s",
			meth, g_config->host, g_config->port, g_config->hot_ns,
			g_config->set == NULL ? "" : g_config->set, g_config->package_file);

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
	if( rc != 0 ){
		INFO("[ERROR]:[%s]:could not connect to host %s port %d",
				meth, g_config->host,g_config->port);
		return(-1);
	}

	g_config->asc  = asc;

	/****
	// register our package.  (maybe not needed?  Depends on config setup)
	INFO("[DEBUG]:[%s]: Do the UDF Package Register");
	if (register_package() !=0 ) {
		return -1;
	}
	 *****/
	return 0;

} // end test_setup()


/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  This file exercises the LSO Interface.
 *  We have the following choice
 *  (1) Do some simple "manual inserts"
 *  (2) Do some automatic generation (generate key, generate entry)
 *  (3) Do some generation from File (read file entry, insert)
 */
int main(int argc, char **argv) {
	static char * meth = "main()";
	int rc = 0;

	INFO("[ENTER]:[%s]: Start in main()\n", meth );

	// Initialize everything
	INFO("[DEBUG]:[%s]: calling test_setup()\n", meth );
	test_setup( argc, argv );

	INFO("[DEBUG]:[%s]: After test_setup(): g_config(%p)\n", meth, g_config);

	// Run some tests
	// (1) Push Test
	INFO("[DEBUG]:[%s]: calling lso_push_test()\n", meth );
	if(( rc = lso_push_test( 10, "UserBin","UKey", "UVal", "lso_bin" )) < 0 ){
		INFO("[ERROR]:[%s]: lso_push_test() RC(%d)\n", meth, rc );
		return( rc );
	}

	// (2) Peek Test
	INFO("[DEBUG]:[%s]: calling lso_peek_test()\n", meth );
	if(( rc = lso_peek_test( "UKey", "lso_bin", 10 )) < 0 ){
		INFO("[ERROR]:[%s]: lso_push_test() RC(%d)\n", meth, rc );
		return( rc );
	}

	// (3) Push Test With Transform
	//
	// (4) Peek Test With Transform
	//
	// (5) Trim Test
	//

	exit(0);
} // end main()
