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

#include "citrusleaf.h"
#include "as_lso.h"
#include "cl_udf.h"
#include <citrusleaf/cf_random.h>
#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_hist.h>

// Use this to turn on extra debugging prints and checks
#define TRA_DEBUG true

// Global Configuration object that holds ALL needed client data.
extern config * g_config;

void __log_append(FILE * f, const char * prefix, const char * fmt, ...);

// |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
// Global Comment on the "apply udf" function call.
// |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
// Here's the Doc for the UDF call -- to reference down below
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
// |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||

/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
*  Large Stack Object Create:
 *  Call the LSO Create() routine to create a Large Stack Object bin in
 *  a record.  The record corresponding to this key may or may not already
 *  exist (we either create a new record or update an existing one with
 *  the new LSO bin). The only error is if there is an existing bin with
 *  the supplied name.
 *  Parms:
 *  (*) asc: The Aerospike Cluster
 *  (*) ns: The Namespace for record holding the LSO bin
 *  (*) set: The Set for the record holding the LSO bin
 *  (*) keystr: The Key that identifies this record
 *  (*) lso_bin_name: Name of the new bin of type "LSO" (AS_MAP).
 *  (*) lso_package: Package to use for the LSO functions (cannot be null)
 *  (*) timeout_ms: Timeout to wait (in MilliSecs). ZERO means forever.
 */
int
as_lso_create( cl_cluster * asc, char * namespace, char * set,
							 char * keystr, char * lso_bin_name, as_map * creation_args,
							 char * lso_package, uint32_t timeout_ms )
{
	static char * meth = "as_lso_create()";
	int rc = 0; // ubiquitous return code

	if( TRA_DEBUG )
		INFO("[ENTER]:[%s]:NS(%s) Set(%s) Key(%s) Bin(%s) Config(%p)\n",
				meth, namespace, set, keystr, lso_bin_name, g_config );
	// In this function, we are returning an int (not the result), so we
	// can use the "stack allocated" result (as_result_init).
	as_result result;
	as_result_init(&result);

	// Set up the arglist that we'll use to pass in function parms
	// As is now the case with all UDF resources, we initialize the
	// arglist, use it then destroy it.
	as_list * arglist = NULL;
	arglist = as_arraylist_new(2, 0);
	as_list_add_string(arglist, lso_bin_name );
	as_list_add_map(arglist, creation_args);

	// NOTE: All strings created by "as_val_tostring()" must be explicitly
	// freed after use.
	char * valstr = as_val_tostring(arglist);
	if( TRA_DEBUG ) INFO("[DEBUG]:[%s]:Created ArgList(%s)\n", meth, valstr );
	free(valstr);

	// Load up the key that we'll feed into the call (and we'll remember
	// to free it after we're done).
	cl_object o_key;
	citrusleaf_object_init_str( &o_key, keystr );

	char * function_name = "stackCreate";

	if( TRA_DEBUG )
		INFO("[DEBUG]:[%s]Calling UDF Apply:NS(%s) Set(%s) Key(%s) Bin(%s) \n",
				meth, namespace,  set, keystr,  lso_bin_name );
	valstr = as_val_tostring(arglist);
	if( TRA_DEBUG ) INFO("[DEBUG]:[%s] Package(%s) Func(%s) Args(%s) \n",
			meth, g_config->package_name, function_name, valstr );
	free(valstr);

  // Make the Citrusleaf UDF Record Apply call - with all of the stuff we
  // packaged up.
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
 *  LSO PUSH (Internal)
 *  Do the actual work of both LSO PUSH and LSO PUSH WITH UDF
 *  For the given record (associated with 'keyp'), push a value onto
 *  the LSO stack in the named LSO Bin.
 *  Parms:
 *  (*) asc: The Aerospike Cluster
 *  (*) ns: The Namespace for record holding the LSO bin
 *  (*) set: The Set for the record holding the LSO bin
 *  (*) keystr: The Key that identifies this record
 *  (*) lso_bin_name: Name of the new bin of type "LSO" (AS_MAP).
 *  (*) lso_valuep: Ptr to the as_val instance that is the new stack value
 *  (*) lso_package: Package to use for the LSO functions (cannot be null)
 *  (*) udf_name: Name of the inner UDF (null ok)
 *  (*) function_args: (as_list *) of the args to pass to the inner UDF(null ok)
 *  (*) timeout_ms: Timeout to wait (in MilliSecs). ZERO means forever.
 */
int
as_lso_push_internal(cl_cluster * asc, char * ns, char * set,
		char * keystr, char * lso_bin_name, as_val * lso_valuep,
    char * lso_package, char * udf_name, as_list * function_args,
    uint32_t timeout_ms )
{
	static char * meth = "as_lso_push_internal()";
	int rc = 0; // ubiquitous return code
  char * valstr = NULL;

	// Note: results of as_val_tostring() must ALWAYS be explicitly freed
	if( TRA_DEBUG ) {
    valstr = as_val_tostring( lso_valuep );
    INFO("[ENTER]:[%s]: NS(%s) Set(%s) Key(%s) Bin(%s) Val(%s)",
			meth, ns, set, keystr, lso_bin_name, valstr );
    free( valstr );
  }

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
	as_val_reserve( lso_valuep ); // Increment the reference count
	as_list * arglist = NULL;
	arglist = as_arraylist_new(4, 0);	// Value, plus inner UDF stuff
  as_list_add_string( arglist, lso_bin_name );
	as_list_append( arglist, lso_valuep );
  if( udf_name != NULL && function_args != NULL ){
    as_list_add_string( arglist, udf_name );
    as_val_reserve( function_args ); // Just as before, bump the ref count
    as_list_append( arglist, (as_val *) function_args );
  }

	// Load up the key that we'll feed into the call (and we'll remember
	// to free it after we're done).
	cl_object o_key;
	citrusleaf_object_init_str(&o_key, keystr);

	// NOTE: Have verified that the as_val (the list) passed to us was
	// created with "new", so we have a malloc'd value.

	// Call the "apply udf" function (e.g. StackPush()) for this record to
	// push a new value onto the LSO Bin.  Call the appropriate Lua function.
  char * function_name;
  if( udf_name != NULL && function_args != NULL ){
    function_name = "stackPushWithUDF";
  } else {
    function_name = "stackPush";
  }

	if( TRA_DEBUG ) {
		INFO("[DEBUG]:[%s]Calling UDF Apply:NS(%s) Set(%s) Key(%s) Bin(%s) \n",
				meth, ns,  set, keystr,  lso_bin_name );
		valstr = as_val_tostring(arglist);
		INFO("[DEBUG]:[%s] Package(%s) Func(%s) Fargs(%s)\n",
				meth, g_config->package_name, function_name, valstr );
		free(valstr);
	}

	rc = citrusleaf_udf_record_apply(asc, ns, set, &o_key,
			lso_package, function_name, arglist, timeout_ms, &result);

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
} // end as_lso_push_internal()


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
 *  (*) ns: The Namespace for record holding the LSO bin
 *  (*) set: The Set for the record holding the LSO bin
 *  (*) keystr: The Key that identifies this record
 *  (*) lso_bin_name: Name of the new bin of type "LSO" (AS_MAP).
 *  (*) lso_valuep: Ptr to the as_val instance that is the new stack value
 *  (*) lso_package: Name of the LSO Package (cannot be null)
 *  (*) timeout_ms: Timeout to wait (in MilliSecs). ZERO means forever.
 */
int
as_lso_push(cl_cluster * asc, char * ns, char * set, char * keystr,
		char * lso_bin_name, as_val * lso_valuep, char * lso_package, 
    uint32_t timeout_ms )
{
	int rc = 0; // ubiquitous return code

  // Call the inner function that does the real work.
  rc = as_lso_push_internal(asc, ns, set, keystr, lso_bin_name, lso_valuep,
          lso_package, NULL, NULL, timeout_ms );

	return rc;
} // end as_lso_push()


/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Large Stack Object Push -- with UDF
 *  For the given record (associated with 'keyp'), push a value onto
 *  the LSO stack in the named LSO Bin.
 *  Parms:
 *  - keyp: Key for the record
 *  - new_valp: The new "value list" to be pushed on the stack
 *  - lso_binp: ptr to the LSO Bin Name
 *
 *  (*) asc: The Aerospike Cluster
 *  (*) ns: The Namespace for record holding the LSO bin
 *  (*) set: The Set for the record holding the LSO bin
 *  (*) keystr: The Key that identifies this record
 *  (*) lso_bin_name: Name of the new bin of type "LSO" (AS_MAP).
 *  (*) lso_valuep: Ptr to the as_val instance that is the new stack value
 *  (*) lso_package: Package to use for the LSO functions (cannot be null)
 *  (*) udf_name: Name of the inner UDF (null ok)
 *  (*) function_args: (as_list *) of the args to pass to the inner UDF(null ok)
 *  (*) timeout_ms: Timeout to wait (in MilliSecs). ZERO means forever.
 */
int
as_lso_push_with_transform(cl_cluster * asc, char * ns, char * set,
		char * keystr, char * lso_bin_name, as_val * lso_valuep,
    char * lso_package, char * udf_name, as_list * function_args,
    uint32_t timeout_ms )
{
	int rc = 0; // ubiquitous return code

  // Call the inner function that does the real work.
  rc = as_lso_push_internal(asc, ns, set, keystr, lso_bin_name, lso_valuep,
          lso_package, udf_name, function_args, timeout_ms );

	return rc;
} // end as_lso_push_with_transform()


/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Large Stack Object Peek -- INTERNAL
 *  For the given record (associated with 'keyp'), read the top N elements
 *  from the Large Stack Object (named by NS, Set, Key, LsoBinName)
 *
 *  Parms:
 *  (*) asc: The Aerospike Cluster
 *  (*) ns: The Namespace for record holding the LSO bin
 *  (*) set: The Set for the record holding the LSO bin
 *  (*) keystr: The Key that identifies this record
 *  (*) lso_bin_name: Name of the new bin of type "LSO" (AS_MAP).
 *  (*) peek_count: Number of items from the stack we'll retrieve
 *  (*) lso_package: Package to use for the LSO functions (cannot be null)
 *  (*) udf_name: Name of the inner UDF (null ok)
 *  (*) function_args: (as_list *) of the args to pass to the inner UDF(null ok)
 *  (*) timeout_ms: Timeout to wait (in MilliSecs). ZERO means forever.
 *
 *  Return: 
 *  "as_result *", which is a mallod'c structure that must be manually
 *  freed (as_result_destroy(resultp) by the caller after it is done
 *  using the result.
 */
as_result  *
as_lso_peek_internal(cl_cluster * asc, char * ns, char * set, char * keystr,
		char * lso_bin_name, int peek_count,
    char * lso_package, char * udf_name, as_list * function_args,
    uint32_t timeout_ms )
{
	static char * meth = "as_lso_peek()";
	int rc = 0; // ubiquitous return code
  char * valstr = NULL;

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
  //
  // Note: function_args is an <as_val> type, so if present, it must be
  // protected (with as_reserve()) so that it doesn't get destroyed twice.
	as_list * arglist = NULL;
	arglist = as_arraylist_new(4, 4);	// Just one item -- the new as_val
  as_list_add_string( arglist, lso_bin_name );
	as_list_add_integer( arglist, peek_count );
  if( udf_name != NULL && function_args != NULL ){
    as_list_add_string( arglist, udf_name );
    as_val_reserve( function_args ); // Just as before, bump the ref count
    as_list_append( arglist, (as_val *) function_args );
  }

	// Load up the key that we'll feed into the call (and we'll remember
	// to free it after we're done).
	cl_object o_key;
	citrusleaf_object_init_str(&o_key, keystr);
	// Call the "apply udf" function (function "StackPeek") for this record to
	// extract "peek_count" items from the stack.
  char * function_name;
  if( udf_name != NULL && function_args != NULL ){
    function_name = "stackPeekWithUDF";
  } else {
    function_name = "stackPeek";
  }

  if( TRA_DEBUG ){
    INFO("[DEBUG]:[%s]Calling UDF Apply:NS(%s) Set(%s) Key(%s) Bin(%s) \n",
			meth, ns,  set, keystr,  lso_bin_name );
    valstr = as_val_tostring(arglist);
    INFO("[DEBUG]:[%s] Package(%s) Func(%s) Args(%s) \n",
        meth, g_config->package_name, function_name, valstr );
    free(valstr);
  }

	rc = citrusleaf_udf_record_apply(asc, ns, set, &o_key,
			lso_package, function_name, arglist, timeout_ms, resultp);

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
} // as_lso_peek_internal()


/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Large Stack Object Peek
 *  For the given record (associated with 'keyp'), read the top N elements
 *  from the Large Stack Object (named by NS, Set, Key, LsoBinName)
 *
 *  Parms:
 *  (*) asc: The Aerospike Cluster
 *  (*) ns: The Namespace for record holding the LSO bin
 *  (*) set: The Set for the record holding the LSO bin
 *  (*) keystr: The Key that identifies this record
 *  (*) lso_bin_name: Name of the new bin of type "LSO" (AS_MAP).
 *  (*) peek_count: Number of items from the stack we'll retrieve
 *  (*) lso_package: Package to use for the LSO functions (cannot be null)
 *  (*) timeout_ms: Timeout to wait (in MilliSecs). ZERO means forever.
 *
 *  Return: 
 *  "as_result *", which is a mallod'c structure that must be manually
 *  freed (as_result_destroy(resultp) by the caller after it is done
 *  using the result.
 */
as_result  *
as_lso_peek(cl_cluster * asc, char * ns, char * set, char * keystr,
		char * lso_bin_name, int peek_count, char * lso_package,
    uint32_t timeout_ms )
{
	as_result * resultp; // Return results

  // Call the inner function that does the real work.
  resultp = as_lso_peek_internal(asc, ns, set, keystr, lso_bin_name, peek_count,
          lso_package, NULL, NULL, timeout_ms );

	return resultp;
} // end as_lso_peek()


/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Large Stack Object Peek WITH UDF
 *  Peek the top of stack, looking at the top N elements, applying the
 *  transformation/filter UDF to each one.
 *
 *  Parms:
 *  (*) asc: The Aerospike Cluster
 *  (*) ns: The Namespace for record holding the LSO bin
 *  (*) set: The Set for the record holding the LSO bin
 *  (*) keystr: The Key that identifies this record
 *  (*) lso_bin_name: Name of the new bin of type "LSO" (AS_MAP).
 *  (*) peek_count: Number of items from the stack we'll retrieve
 *  (*) lso_package: Package to use for the LSO functions (cannot be null)
 *  (*) udf_name: Name of the inner UDF (null ok)
 *  (*) function_args: (as_list *) of the args to pass to the inner UDF(null ok)
 *  (*) timeout_ms: Timeout to wait (in MilliSecs). ZERO means forever.
 *
 *  Return: 
 *  "as_result *", which is a mallod'c structure that must be manually
 *  freed (as_result_destroy(resultp) by the caller after it is done
 *  using the result.
 */
as_result *
as_lso_peek_with_transform(cl_cluster * asc, char * ns, char * set,
		char * keystr, char * lso_bin_name, int peek_count,
    char * lso_package, char * udf_name, as_list * function_args,
    uint32_t timeout_ms )
{
	as_result * resultp; // Return results
  
  // Call the inner function that does the real work.
  resultp = as_lso_peek_internal(asc, ns, set, keystr, lso_bin_name,
          peek_count, lso_package, udf_name, function_args, timeout_ms );

	return resultp;
} // end as_lso_peek_with_transform()


/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Large Stack Object Trim:
 *  For the given record (associated with 'keyp'), trim the LSO, leaving
 *  the top N elements.
 *
 *
 *  Parms:
 *  (*) asc: The Aerospike Cluster
 *  (*) ns: The Namespace for record holding the LSO bin
 *  (*) set: The Set for the record holding the LSO bin
 *  (*) keystr: The Key that identifies this record
 *  (*) lso_bin_name: Name of the new bin of type "LSO" (AS_MAP).
 *  (*) remainder_count: Delete all but his many items
 *  (*) lso_package: Package to use for the LSO functions (cannot be null)
 *  (*) timeout_ms: Timeout to wait (in MilliSecs). ZERO means forever.
 *
 *  Return: 
 *  0: success.   -1: Failure
 */
int
as_lso_trim(cl_cluster * asc, char * ns, char * set, char * keystr,
		char * lso_bin_name, int remainder_count,
    char * lso_package, char * udf_name, as_list * function_args,
    uint32_t timeout_ms )
{
	static char * meth = "as_lso_trim()";
	int rc = 0; // ubiquitous return code
  char * valstr = NULL;

	if( TRA_DEBUG )
		INFO("[ENTER]:[%s]: NS(%s) Set(%s) Key(%s) Bin(%s) Count(%d)",
				meth, ns, set, keystr, lso_bin_name, remainder_count );

	// In this function, we are returning an int (not the result), so we
	// can use the "stack allocated" result (as_result_init).
	as_result result;
	as_result_init(&result);

	// Set up the arglist that we'll use to pass in function parms
	// As is now the case with all UDF resources, we initialize the
	// arglist, use it then destroy it.
	as_list * arglist = NULL;
	arglist = as_arraylist_new(1, 0);	// Just one item -- the trim count
	as_list_add_integer( arglist, remainder_count );

	// Load up the key that we'll feed into the call (and we'll remember
	// to free it after we're done).
	cl_object o_key;
	citrusleaf_object_init_str(&o_key, keystr);

	char * function_name = "stackTrim";

  if( TRA_DEBUG ) {
    INFO("[DEBUG]:[%s]Calling UDF Apply:NS(%s) Set(%s) Key(%s) Bin(%s) \n",
			meth, ns,  set, keystr,  lso_bin_name );
    valstr = as_val_tostring(arglist);
    INFO("[DEBUG]:[%s] Package(%s) Func(%s) Args(%s) \n",
			meth, g_config->package_name, function_name, valstr );
    free(valstr);
  }

  // Call the "apply udf" function (e.g. StackTrim()) for this record to
  // truncate the Stack to "Remainder_Count" items.
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
