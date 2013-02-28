/* *  Citrusleaf Large Object Stack Test Program
 *  lso_udf.c - Validates LSO stored procedure functionality
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

// Use this to turn on extra debugging prints and checks
#define TRA_DEBUG true

// Global Configuration object that holds ALL needed client data.
extern config * g_config;

void __log_append(FILE * f, const char * prefix, const char * fmt, ...);

/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Large Stack Object Create:
 *  Call the LSO Create() routine to create a Large Stack Object bin in
 *  a record.  The record corresponding to this key may or may not already
 *  exist (we either create a new record or update an existing one with
 *  the new LSO bin). The only error is if there is an existing bin with
 *  the supplied name.
 *  Parms:
 *  (*) asc: The Aerospike Cluster
 *  (*) namespace: The Namespace for record holding the LSO bin
 *  (*) set: The Set for the record holding the LSO bin
 *  (*) key: The Key that identifies this record
 *  (*) bin_name: Name of the new bin of type "LSO" (AS_MAP).
 */
int
as_lso_create( cl_cluster * asc, char * namespace, char * set,
		char * keystr, char * lso_bin_name )
{
	static char * meth = "as_lso_create()";
	int rc = 0; // ubiquitous return code

	if( TRA_DEBUG )
		INFO("[ENTER]:[%s]:NS(%s) Set(%s) Key(%s) Bin(%s) Config(%p)\n",
				meth, namespace, set, keystr, lso_bin_name, g_config );

	// Call the "apply udf" function (function "StackCreate") for this record to
	// create the LSO Bin. Here's the Doc for the UDF call
	// cl_rv citrusleaf_udf_record_apply(
	//    cl_cluster * cluster,
	//    const char * namespace,
	//    const char * set,
	//    const cl_object * key,
	//    const char * file,
	//    const char * function,
	//    as_list * arglist,
	//    int timeout,
	//    as_result * result)

	// In this function, we are returning an int (not the result), so we
	// can use the "stack allocated" result (as_result_init).
	as_result result;
	as_result_init(&result);

	// Set up the arglist that we'll use to pass in function parms
	// As is now the case with all UDF resources, we initialize the
	// arglist, use it then destroy it.
	as_list * arglist = NULL;
	arglist = as_arraylist_new(3, 8);	// We have 3 or 4 parms to pass
	// 2 for strawman, 3 for stickman
	as_list_add_string(arglist, namespace );
	// as_list_add_string(arglist, g_config->cold_ns ); // Not for strawman
	as_list_add_string(arglist, set );
	as_list_add_string(arglist, lso_bin_name );
	char * function_name = "stackCreate";

	// NOTE: All strings created by "as_val_tostring()" must be explicitly
	// freed after use.
	char * valstr = as_val_tostring(arglist);
	if( TRA_DEBUG ) INFO("[DEBUG]:[%s]:Created ArgList(%s)\n", meth, valstr );
	free(valstr);

	// Load up the key that we'll feed into the call (and we'll remember
	// to free it after we're done).
	cl_object o_key;
	citrusleaf_object_init_str( &o_key, keystr );

	if( TRA_DEBUG )
		INFO("[DEBUG]:[%s]Calling UDF Apply:NS(%s) Set(%s) Key(%s) Bin(%s) \n",
				meth, namespace,  set, keystr,  lso_bin_name );
	valstr = as_val_tostring(arglist);
	if( TRA_DEBUG ) INFO("[DEBUG]:[%s] Package(%s) Func(%s) Args(%s) \n",
			meth, g_config->package_name, function_name, valstr );
	free(valstr);

	rc = citrusleaf_udf_record_apply(g_config->asc, namespace,
			set, &o_key, g_config->package_name, function_name, arglist,
			g_config->timeout_ms, &result);

	if (rc != CITRUSLEAF_OK) {
		INFO("[ERROR]:[%s]:citrusleaf_udf_record_apply: Fail: RC(%d)",meth, rc);
		goto cleanup;
	}

	if ( result.is_success ) {
		INFO("[DEBUG]:[%s]:UDF Result SUCCESS\n", meth );
		if ( as_val_type(result.value) == AS_NIL ) {
			INFO("[ERROR]:[%s] Result type is NIL\n", meth );
			rc = CITRUSLEAF_FAIL_CLIENT; // not sure which error to use.
		} else {
			valstr = as_val_tostring(result.value);
			INFO("[DEBUG]:[%s]: udf_return_type(%s)", meth, valstr);
			free(valstr);
		}
	} else {
		INFO("[DEBUG]:[%s]:UDF Result FAIL\n", meth );
		rc = CITRUSLEAF_FAIL_CLIENT; // not sure which error to use.
	}

	cleanup:
	as_val_destroy(arglist);
	as_result_destroy(&result);
	citrusleaf_object_free(&o_key);		

	if( TRA_DEBUG ) INFO("[EXIT]:[%s]: RC(%d)\n", meth, rc );
	return rc;
} 	// end lso_create()


/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Large Stack Object Push:
 *  for the given record (associated with 'keyp'), push a value onto
 *  the LSO stack in the named LSO Bin.
 *  Parms:
 *  - keyp: Key for the record
 *  - new_valp: The new "value list" to be pushed on the stack
 *  - lso_binp: ptr to the LSO Bin Name
 *
 *  (*) asc: The Aerospike Cluster
 *  (*) namespace: The Namespace for record holding the LSO bin
 *  (*) set: The Set for the record holding the LSO bin
 *  (*) key: The Key that identifies this record
 *  (*) bin_name: Name of the new bin of type "LSO" (AS_MAP).
 *  (*) lso_valuep: Ptr to the as_val instance that is the new stack value
 */
int
as_lso_push(cl_cluster * asc, char * ns, char * set, char * keystr,
		char * lso_bin_name, as_val * lso_valuep)
{
	static char * meth = "as_lso_push()";
	int rc = 0; // ubiquitous return code
	char * function_name = "stackPush";

	// Note: results of as_val_tostring() must ALWAYS be explicitly freed
	char * valstr = as_val_tostring( lso_valuep );
	if( TRA_DEBUG ) INFO("[ENTER]:[%s]: NS(%s) Set(%s) Key(%s) Bin(%s) Val(%s)",
			meth, ns, set, keystr, lso_bin_name, valstr );
	free( valstr );

	// In this function, we are returning an int (not the result), so we
	// can use the "stack allocated" result (as_result_init).
	as_result result;
	as_result_init(&result);

	// Set up the arglist that we'll use to pass in function parms
	// As is now the case with all UDF resources, we initialize the
	// arglist, use it then destroy it.  Also, we MALLOC (vs using a
	// stack value), as the values are used via reference (I think).
	//
	// Note: lso_valuep is an as_val type that we are embedding in ANOTHER
	// as_val type, so we must increment the reference count (with
	// as_val_reserve) so that all of the free()/destroy() calls can match up.
	as_list * arglist = NULL;
	arglist = as_arraylist_new(1, 10);	// Just one item -- the new as_val
	as_val_reserve( lso_valuep ); // Increment the reference count
	as_list_append( arglist, lso_valuep );

	// Call the "apply udf" function (function "StackCreate") for this record to
	// push a new value onto the LSO Bin. Here's the Doc for the UDF call
	// cl_rv citrusleaf_udf_record_apply(
	//    cl_cluster * cluster,
	//    const char * namespace,
	//    const char * set,
	//    const cl_object * key,
	//    const char * file,
	//    const char * function,
	//    as_list * arglist,
	//    int timeout,
	//    as_result * result)
	// Load up the key that we'll feed into the call (and we'll remember
	// to free it after we're done).
	cl_object o_key;
	citrusleaf_object_init_str(&o_key, keystr);

	// NOTE: Have verified that the as_val (the list) passed to us was
	// created with "new", so we have a malloc'd value.

	if( TRA_DEBUG )
		INFO("[DEBUG]:[%s]Calling UDF Apply:NS(%s) Set(%s) Key(%s) Bin(%s) \n",
				meth, ns,  set, keystr,  lso_bin_name );

	if( TRA_DEBUG ) {
		valstr = as_val_tostring(arglist);
		INFO("[DEBUG]:[%s] Package(%s) Func(%s) \n",
				meth, g_config->package_name, function_name, valstr );
		free(valstr);
	}

	rc = citrusleaf_udf_record_apply(asc, ns, set, &o_key,
			g_config->package_name, function_name, arglist,
			g_config->timeout_ms, &result);

	if (rc != CITRUSLEAF_OK) {
		INFO("[ERROR]:[%s]:citrusleaf_udf_record_apply: Fail: RC(%d)",meth, rc);
		goto cleanup;
	}

	if ( result.is_success ) {
		INFO("[DEBUG]:[%s]:UDF Result SUCCESS\n", meth );
		if ( as_val_type(result.value) == AS_NIL ) {
			INFO("[ERROR]:[%s] Result type is NIL\n", meth );
			rc = CITRUSLEAF_FAIL_CLIENT; // not sure which error to use.
		} else {
			valstr = as_val_tostring(result.value);
			INFO("[DEBUG]:[%s]: udf_return_type(%s)", meth, valstr);
			free(valstr);
		}
	} else {
		INFO("[DEBUG]:[%s]:UDF Result FAIL\n", meth );
		rc = CITRUSLEAF_FAIL_CLIENT; // not sure which error to use.
	}

	cleanup:
	as_val_destroy(arglist);
	as_result_destroy(&result);
	citrusleaf_object_free(&o_key);

	if( TRA_DEBUG ) INFO("[EXIT]:[%s]: RC(%d)\n", meth, rc );

	return rc;
} // end as_lso_push()


/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Fill this in once the regular push works.
 */
int
as_lso_push_with_transform(cl_cluster * asc, char * ns, char * set,
		char * keystr, char * lso_bin_name, as_val * lso_valuep, char * udf_file,
		char * udf_name, as_list * function_args )
{
	static char * meth = "as_lso_push_with_transform()";
	int rc = 0;

	// Note: results of as_val_tostring() must ALWAYS be explicitly freed
	char * valstr = as_val_tostring( lso_valuep );
	if( TRA_DEBUG )
		INFO("[ENTER]:[%s]: NS(%s) Set(%s) Key(%s) Bin(%s) Val(%d) UDF(%s)",
				meth, ns, set, keystr, lso_bin_name, valstr, udf_name);
	free( valstr );

	// <<<<   Fill this in once the regular push works. >>>>>>

	if( TRA_DEBUG ) INFO("[EXIT]:[%s]: RC(%d)\n", meth, rc );
	return rc;
} // end as_lso_push_with_transform()


/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Large Stack Object Peek:
 *  For the given record (associated with 'keyp'), read the top N elements
 *  from the Large Stack Object (named by NS, Set, Key, LsoBinName)
 *
 *  Parms:
 *  (*) asc: The Aerospike Cluster
 *  (*) namespace: The Namespace for record holding the LSO bin
 *  (*) set: The Set for the record holding the LSO bin
 *  (*) key: The Key that identifies this record
 *  (*) bin_name: Name of the new bin of type "LSO" (AS_MAP).
 *  (*) peek_count: Number of items from the stack we'll retrieve
 *
 *  Return: 
 *  "as_result *", which is a mallod'c structure that must be manually
 *  freed (as_result_destroy(resultp) by the caller after it is done
 *  using the result.
 */
as_result  *
as_lso_peek(cl_cluster * asc, char * ns, char * set, char * keystr,
		char * lso_bin_name, int peek_count )
{
	static char * meth = "as_lso_peek()";
	int rc = 0; // ubiquitous return code
	char * function_name = "stackPeek";

	if( TRA_DEBUG )
		INFO("[ENTER]:[%s]: NS(%s) Set(%s) Key(%s) Bin(%s) Count(%d)",
				meth, ns, set, keystr, lso_bin_name, peek_count );

	// For Result, we are going to pass this back to the caller, so we
	// must FULLY DOCUMENT the fact that they must call "as_result_destroy()"
	// on the result after they are done with it.
	as_result * resultp;
	resultp = as_result_new();

	// Set up the arglist that we'll use to pass in function parms
	// As is now the case with all UDF resources, we initialize the
	// arglist, use it then destroy it.
	as_list * arglist = NULL;
	arglist = as_arraylist_new(1, 4);	// Just one item -- the new as_val
	as_list_add_integer( arglist, peek_count );

	// Call the "apply udf" function (function "StackCreate") for this record to
	// push a new value onto the LSO Bin. Here's the Doc for the UDF call
	// cl_rv citrusleaf_udf_record_apply(
	//    cl_cluster * cluster,
	//    const char * namespace,
	//    const char * set,
	//    const cl_object * key,
	//    const char * file,
	//    const char * function,
	//    as_list * arglist,
	//    int timeout,
	//    as_result * result)
	// Load up the key that we'll feed into the call (and we'll remember
	// to free it after we're done).
	cl_object o_key;
	citrusleaf_object_init_str(&o_key, keystr);

	INFO("[DEBUG]:[%s]Calling UDF Apply:NS(%s) Set(%s) Key(%s) Bin(%s) \n",
			meth, ns,  set, keystr,  lso_bin_name );
	char * valstr = as_val_tostring(arglist);
	INFO("[DEBUG]:[%s] Package(%s) Func(%s) Args(%s) \n",
			meth, g_config->package_name, function_name, valstr );
	free(valstr);

	rc = citrusleaf_udf_record_apply(asc, ns, set, &o_key,
			g_config->package_name, function_name, arglist,
			g_config->timeout_ms, resultp);

	if (rc != CITRUSLEAF_OK) {
		INFO("[ERROR]:[%s]:citrusleaf_udf_record_apply: Fail: RC(%d)",meth, rc);
		goto cleanup;
	}

	if ( resultp->is_success ) {
		INFO("[DEBUG]:[%s]:UDF Result SUCCESS\n", meth );
		if ( as_val_type(resultp->value) == AS_NIL ) {
			INFO("[ERROR]:[%s] Result type is NIL\n", meth );
			rc = CITRUSLEAF_FAIL_CLIENT; // not sure which error to use.
		} else {
			valstr = as_val_tostring(resultp->value);
			INFO("[DEBUG]:[%s]: udf_return_type(%s)", meth, valstr);
			free(valstr);
		}
	} else {
		INFO("[DEBUG]:[%s]:UDF Result FAIL\n", meth );
		rc = CITRUSLEAF_FAIL_CLIENT; // not sure which error to use.
	}

	cleanup:
	as_val_destroy(arglist);
	// NOTE: We do NOT destroy result: The caller has to do that
	citrusleaf_object_free(&o_key);		

	if( TRA_DEBUG ) INFO("[EXIT]:[%s]: RC(%d)\n", meth, rc );
	if( rc == CITRUSLEAF_OK ){
		return resultp;
	} else {
		return NULL;
	}
} // end as_lso_peek()


/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Peek the top of stack, looking at the top N elements, applying the
 *  transformation/filter UDF to each one.
 */
as_result *
as_lso_peek_with_transform(cl_cluster * asc, char * ns, char * set,
		char * keystr, char * lso_bin_name, int peek_count, char * udf_file,
		char * udf_name, as_list * function_args )
{
	static char * meth = "as_lso_peek_with_transform()";
	int rc = 0;
	as_result * resultp = NULL;

	if( TRA_DEBUG )
		INFO("[ENTER]:[%s]: NS(%s) Set(%s) Key(%s) Bin(%s) Cnt(%d) UDF(%s)",
				meth, ns, set, keystr, lso_bin_name, peek_count, udf_name);

	// <<<<   Fill this in once the regular peek works. >>>>>>

	if( TRA_DEBUG ) INFO("[EXIT]:[%s]: RC(%d)\n", meth, rc );

	return resultp;
} // end as_lso_peek_with_transform()


/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Large Stack Object Trim:
 *  For the given record (associated with 'keyp'), trim the LSO, leaving
 *  the top N elements.
 *
 *  Parms:
 *  (*) asc: The Aerospike Cluster
 *  (*) namespace: The Namespace for record holding the LSO bin
 *  (*) set: The Set for the record holding the LSO bin
 *  (*) key: The Key that identifies this record
 *  (*) bin_name: Name of the new bin of type "LSO" (AS_MAP).
 *  (*) trim_count: Number of items we will leave on the top of stack
 *
 *  Return: 
 *  0: success.   -1: Failure
 */
int
as_lso_trim(cl_cluster * asc, char * ns, char * set, char * keystr,
		char * lso_bin_name, int trim_count )
{
	static char * meth = "as_lso_trim()";
	int rc = 0; // ubiquitous return code
	char * function_name = "stackTrim";

	if( TRA_DEBUG )
		INFO("[ENTER]:[%s]: NS(%s) Set(%s) Key(%s) Bin(%s) Count(%d)",
				meth, ns, set, keystr, lso_bin_name, trim_count );

	// In this function, we are returning an int (not the result), so we
	// can use the "stack allocated" result (as_result_init).
	as_result result;
	as_result_init(&result);

	// Set up the arglist that we'll use to pass in function parms
	// As is now the case with all UDF resources, we initialize the
	// arglist, use it then destroy it.
	as_list * arglist = NULL;
	arglist = as_arraylist_new(1, 4);	// Just one item -- the trim count
	as_list_add_integer( arglist, trim_count );

	// Call the "apply udf" function (function "StackCreate") for this record to
	// push a new value onto the LSO Bin. Here's the Doc for the UDF call
	// cl_rv citrusleaf_udf_record_apply(
	//    cl_cluster * cluster,
	//    const char * namespace,
	//    const char * set,
	//    const cl_object * key,
	//    const char * file,
	//    const char * function,
	//    as_list * arglist,
	//    int timeout,
	//    as_result * result)
	// Load up the key that we'll feed into the call (and we'll remember
	// to free it after we're done).
	cl_object o_key;
	citrusleaf_object_init_str(&o_key, keystr);

	INFO("[DEBUG]:[%s]Calling UDF Apply:NS(%s) Set(%s) Key(%s) Bin(%s) \n",
			meth, ns,  set, keystr,  lso_bin_name );
	char * valstr = as_val_tostring(arglist);
	INFO("[DEBUG]:[%s] Package(%s) Func(%s) Args(%s) \n",
			meth, g_config->package_name, function_name, valstr );
	free(valstr);

	rc = citrusleaf_udf_record_apply(asc, ns, set, &o_key,
			g_config->package_name, function_name, arglist,
			g_config->timeout_ms, &result);

	if (rc != CITRUSLEAF_OK) {
		INFO("[ERROR]:[%s]:citrusleaf_udf_record_apply: Fail: RC(%d)",meth, rc);
		goto cleanup;
	}

	if ( result.is_success ) {
		INFO("[DEBUG]:[%s]:UDF Result SUCCESS\n", meth );
		if ( as_val_type(result.value) == AS_NIL ) {
			INFO("[ERROR]:[%s] Result type is NIL\n", meth );
			rc = CITRUSLEAF_FAIL_CLIENT; // not sure which error to use.
		} else {
			valstr = as_val_tostring(result.value);
			INFO("[DEBUG]:[%s]: udf_return_type(%s)", meth, valstr);
			free(valstr);
		}
	} else {
		INFO("[DEBUG]:[%s]:UDF Result FAIL\n", meth );
		rc = CITRUSLEAF_FAIL_CLIENT; // not sure which error to use.
	}

	cleanup:
	as_val_destroy(arglist);
	as_result_destroy( &result );
	citrusleaf_object_free(&o_key);		

	INFO("[EXIT]:[%s]: RC(%d)\n", meth, rc );
	return rc;
} // end as_lso_trim()


#if 0
/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Register the  Lua Package
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
			.size = b_tot,
			.data = script_code
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
} // end register_package() 

#endif 
