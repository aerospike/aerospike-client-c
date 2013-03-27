/* *  Citrusleaf Large Object Stack C API and Validation Program
 *  aerospike_lstack.c - Validates Large Stack  procedure functionality
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
#include "aerospike_lstack.h"
#include "cl_udf.h"
#include <citrusleaf/cf_random.h>
#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_hist.h>

// Use this to turn on/off tracing/debugging prints and checks
// Comment out this next line to quiet the output.
#define DEBUG

#ifdef DEBUG
#define TRA_ENTER true   // show method ENTER values
#define TRA_EXIT true    // show method EXIT values
#define TRA_DEBUG true   // show various DEBUG prints
#define TRA_ERROR true   // show ERROR conditions
#else
#define TRA_ENTER false
#define TRA_EXIT false
#define TRA_DEBUG false
#define TRA_ERROR true   // Best to leave this ON
#endif

// |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
// Global Comment on the "apply udf" function call.
// |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
// Here's the Doc for the UDF call -- to reference down below.  This is
// the call that is used to invoke the appropriate Large Stack Lua function
// that performs the server-side Large Stack operation.
//
// cl_rv citrusleaf_udf_record_apply(
//    cl_cluster * cluster,
//    const char * namespace,
//    const char * set,
//    const cl_object * keyp,
//    const char * file,
//    const char * function,
//    as_list * arglist,
//    int timeout,
//    as_result * result)
// |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||

// =======================================================================
// ATTENTION!!!  :::   VERSION CHANGES (March 27, 2013)
// NOTE: We have changed the API for Large Stack Objects (LSO) to be
// more in line with the emerging Aerospike standard.  Here's a summary
// of the changes:
// (1) The package (LSTACK.lua) will be implicit, so it will no longer
//     be passed in as a parameter (was lso_package).
// (2) The key field will be a cl_object, but there will be "helper" 
//     functions that will allow users to pass in string keys directly.
// (3) The names of the large stack operations have changed to the standard
//     "C Style" of functions (e.g. lstack_create(), lstack_push()).
// (4) The names of the functions defined in this file change from as_lso_xx()
//     to aerospike_lstack_xx().
// (5) The return types are either "cl_rv" (for status results), or
//     "as_results" (for value results).
//     The status result values are:
//     CITRUSLEAF_FAIL_TIMEOUT = -2,   // Timeout before completion
//     CITRUSLEAF_FAIL_CLIENT = -1,    // an out of memory or similar locally
//     CITRUSLEAF_OK = 0,              // YAY!! SUCCESS!!
//     CITRUSLEAF_FAIL_UNKNOWN = 1,    // unknown failure on the server side
//     CITRUSLEAF_FAIL_NOTFOUND = 2,   // Did not find record for key
//     CITRUSLEAF_FAIL_GENERATION = 3, // write failed on CAS write
//     CITRUSLEAF_FAIL_PARAMETER = 4,  // bad parameters
//     CITRUSLEAF_FAIL_KEY_EXISTS = 5, // Fail overwriting existing key
//     CITRUSLEAF_FAIL_BIN_EXISTS = 6  // Fail overwriting existing bin
// =======================================================================

// ++==============++
// || Fixed Values ||
// ++==============++
// PackageName: This is now a fixed term, so users no longer pass it in.
// The LSTACK package is now IMPLICIT as a known system Lua file.
static char * s_lso_package = "LSTACK"; // System file:  LSTACK.lua

// The names of the Lua Functions that implement Large Stack Ops
static char * s_create           = "lstack_create";
static char * s_create_not_exist = "lstack_create_if_not_exist";
static char * s_push             = "lstack_push";
static char * s_push_w_udf       = "lstack_push_with_udf";
static char * s_peek             = "lstack_peek";
static char * s_peek_w_udf       = "lstack_peek_with_udf";
static char * s_trim             = "lstack_trim";
static char * s_size             = "lstack_size";
static char * s_set_config       = "lstack_set_config";
static char * s_get_config       = "lstack_get_config";

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
 *  (*) o_keyp: Ptr to the Key Object (cl_object) that identifies this record
 *  (*) lso_bin_name: Name of the new bin of type "LSO" (AS_MAP).
 *  (*) creation_args: A map (as_map) of creation settings (can be null)
 *  (*) timeout_ms: Timeout to wait (in MilliSecs). ZERO means forever.
 *
 * Return: a value of the type "cl_rv"
 *   Success:  CITRUSLEAF_OK: (0)
 *   Failure:  CITRUSLEAF_FAIL_CLIENT (-1);
 */
cl_rv
aerospike_lstack_create(
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const cl_object * o_keyp,
        const char * lso_bin_name,
        as_map * creation_args,
        uint32_t timeout_ms )
{
    static char * meth = "aerospike_lstack_create()";
    cl_rv rc = CITRUSLEAF_OK; // ubiquitous return code

    if( TRA_ENTER )
        printf("[ENTER]:[%s]:NS(%s) Set(%s) Bin(%s)\n",
                meth, namespace, set, lso_bin_name);
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
    char * valstr;
    if( TRA_DEBUG ) {
        printf("[DEBUG]:[%s]UDF Apply:NS(%s) Set(%s) Bin(%s) \n",
                meth, namespace,  set, lso_bin_name );
        char * valstr = as_val_tostring(arglist);
        printf("[DEBUG]:[%s] Package(%s) Func(%s) Args(%s) \n",
            meth, s_lso_package, s_create, valstr );
        free(valstr);
    }

    // Make the Citrusleaf UDF Record Apply call - with all of the stuff we
    // packaged up.
    rc = citrusleaf_udf_record_apply(asc, namespace,
            set, o_keyp, s_lso_package, s_create, arglist,
            timeout_ms, &result);

    if (rc != CITRUSLEAF_OK) {
        if( TRA_ERROR )
            printf("[ERROR]:[%s]:citrusleaf_udf_record_apply: Fail: RC(%d)",
                meth, rc);
        goto cleanup;
    }

    if ( result.is_success ) {
        if( TRA_DEBUG ) printf("[DEBUG]:[%s]:UDF Result SUCCESS\n", meth );
        if ( as_val_type(result.value) == AS_NIL ) {
            if( TRA_ERROR ) printf("[ERROR]:[%s] Result type is NIL\n", meth );
            rc = CITRUSLEAF_FAIL_CLIENT; // general client failure
        } else {
            if( TRA_DEBUG ) {
                valstr = as_val_tostring(result.value);
                printf("[DEBUG]:[%s]: udf_return_type(%s)", meth, valstr);
                free(valstr);
            }
        }
    } else {
        if( TRA_DEBUG ) printf("[DEBUG]:[%s]:UDF Result FAIL\n", meth );
        rc = CITRUSLEAF_FAIL_CLIENT; // general client failure
    }

cleanup:
    as_val_destroy(arglist);
    as_result_destroy(&result);

    if( TRA_EXIT ) printf("[EXIT]:[%s]: RC(%d)\n", meth, rc );
    return rc;
}     // end aerospike_lstack_create()


// ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
// CREATE HELPER FUNCTIONS
// (1) with key string: Convert user's key to cl_object
// ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
cl_rv
aerospike_lstack_create_with_keystring(
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const char * keystr,
        const char * lso_bin_name,
        as_map * creation_args,
        uint32_t timeout_ms )
{

    // Load up the key that we'll feed into the call (and we'll remember
    // to free it after we're done).
    cl_object o_key;
    citrusleaf_object_init_str( &o_key, keystr );

    cl_rv rc = aerospike_lstack_create( asc, namespace, set,
            &o_key, lso_bin_name, creation_args, timeout_ms );

    citrusleaf_object_free(&o_key);        

    return( rc );
} // end aerospike_lstack_create_with_keystring()


/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  LSO PUSH (Internal)
 *  Do the actual work of both LSO PUSH and LSO PUSH WITH UDF
 *  For the given record (associated with 'keyp'), push a value onto
 *  the LSO stack in the named LSO Bin.
 *  Parms:
 *  (*) asc: The Aerospike Cluster
 *  (*) namespace: The Namespace for record holding the LSO bin
 *  (*) set: The Set for the record holding the LSO bin
 *  (*) o_keyp: Ptr to the Key Object (cl_object) that identifies this record
 *  (*) lso_bin_name: Name of the new bin of type "LSO" (AS_MAP).
 *  (*) lso_valuep: Ptr to the as_val instance that is the new stack value
 *  (*) udf_name: Name of the inner UDF (null ok)
 *  (*) function_args: (as_list *) of the args to pass to the inner UDF(null ok)
 *  (*) timeout_ms: Timeout to wait (in MilliSecs). ZERO means forever.
 *
 * Return: a value of the type "cl_rv"
 *   Success:  CITRUSLEAF_OK: (0)
 *   Failure:  CITRUSLEAF_FAIL_CLIENT (-1);
 */
cl_rv
aerospike_lstack_push_internal(
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const cl_object * o_keyp,
        const char * lso_bin_name,
        as_val * lso_valuep,
        const char * udf_name,
        as_list * function_args,
        uint32_t timeout_ms )
{
    static char * meth = "aerospike_lstack_push_internal()";
    cl_rv rc; // ubiquitous return code
    char * valstr = NULL;

    // Note: results of as_val_tostring() must ALWAYS be explicitly freed
    if( TRA_ENTER ) {
        valstr = as_val_tostring( lso_valuep );
        printf("[ENTER]:[%s]: NS(%s) Set(%s) Bin(%s) Val(%s)",
                meth, namespace, set, lso_bin_name, valstr );
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
    arglist = as_arraylist_new(4, 0);    // Value, plus inner UDF stuff
    as_list_add_string( arglist, lso_bin_name );
    as_list_append( arglist, lso_valuep );
    if( udf_name != NULL && function_args != NULL ){
        as_list_add_string( arglist, udf_name );
        as_val_reserve( function_args ); // Just as before, bump the ref count
        as_list_append( arglist, (as_val *) function_args );
    }

    // NOTE: Have verified that the as_val (the list) passed to us was
    // created with "new", so we have a malloc'd value.

    // Call the "apply udf" function (e.g. StackPush()) for this record to
    // push a new value onto the LSO Bin.  Call the appropriate Lua function.
    char * function_name;
    if( udf_name != NULL && function_args != NULL ){
        function_name = s_push_w_udf;
    } else {
        function_name = s_push;
    }

    if( TRA_DEBUG ) {
        printf("[DEBUG]:[%s]UDF Apply:NS(%s) Set(%s) Bin(%s) \n",
                meth, namespace,  set, lso_bin_name );
        valstr = as_val_tostring(arglist);
        printf("[DEBUG]:[%s] Package(%s) Func(%s) Fargs(%s)\n",
                meth, s_lso_package, function_name, valstr );
        free(valstr);
    }

    rc = citrusleaf_udf_record_apply(asc, namespace, set, o_keyp,
            s_lso_package, function_name, arglist, timeout_ms, &result);

    if (rc != CITRUSLEAF_OK) {
        if( TRA_ERROR )
            printf("[ERROR]:[%s]:citrusleaf_udf_record_apply: Fail: RC(%d)",
                meth, rc);
        goto cleanup;
    }

    if ( result.is_success ) {
        if( TRA_DEBUG ) printf("[DEBUG]:[%s]:UDF Result SUCCESS\n", meth );
        if ( as_val_type(result.value) == AS_NIL ) {
            if( TRA_ERROR ) printf("[ERROR]:[%s] Result type is NIL\n", meth );
            rc = CITRUSLEAF_FAIL_CLIENT; // general client failure
        } else {
            valstr = as_val_tostring(result.value);
            if( TRA_DEBUG )
                printf("[DEBUG]:[%s]: udf_return_type(%s)", meth, valstr);
            free(valstr);
        }
    } else {
        if( TRA_ERROR ) printf("[ERROR]:[%s]:UDF Result FAIL\n", meth );
        rc = CITRUSLEAF_FAIL_CLIENT; // general client failure
    }

cleanup:
    as_val_destroy(arglist);
    as_result_destroy(&result);

    if( TRA_EXIT ) printf("[EXIT]:[%s]: RC(%d)\n", meth, rc );
    return rc;
} // end aerospike_lstack_push_internal()


/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Large Stack Object Push:
 *  for the given record (associated with 'o_keyp'), push a value onto
 *  the LSO stack in the named LSO Bin.
 *  Parms:
 *  (*) asc: The Aerospike Cluster
 *  (*) namespace: The Namespace for record holding the LSO bin
 *  (*) set: The Set for the record holding the LSO bin
 *  (*) o_keyp: Ptr to the Key Object (cl_object) that identifies this record
 *  (*) lso_bin_name: Name of the new bin of type "LSO" (AS_MAP).
 *  (*) lso_valuep: Ptr to the as_val instance that is the new stack value
 *  (*) timeout_ms: Timeout to wait (in MilliSecs). ZERO means forever.
 *
 * Return: a value of the type "cl_rv"
 *   Success:  CITRUSLEAF_OK: (0)
 *   Failure:  CITRUSLEAF_FAIL_CLIENT (-1);
 */
cl_rv
aerospike_lstack_push(
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const cl_object * o_keyp,
        const char * lso_bin_name,
        as_val * lso_valuep,
        uint32_t timeout_ms )
{

    // Call the inner function that does the real work.
    cl_rv rc = aerospike_lstack_push_internal(
            asc,namespace, set, o_keyp, lso_bin_name, lso_valuep,
            NULL, NULL, timeout_ms );

    return rc;
} // end aerospike_lstack_push()


// ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
// LSTACK PUSH HELPER FUNCTIONS
// (1) with key string: Convert user's key to cl_object
// ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
cl_rv
aerospike_lstack_push_with_keystring(
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const char * keystr,
        const char * lso_bin_name,
        as_val * lso_valuep,
        uint32_t timeout_ms )
{

    // Load up the key that we'll feed into the call (and we'll remember
    // to free it after we're done).
    cl_object o_key;
    citrusleaf_object_init_str( &o_key, keystr );

    cl_rv rc = aerospike_lstack_push_internal(
            asc, namespace, set, &o_key, lso_bin_name, lso_valuep,
            NULL, NULL, timeout_ms );

    citrusleaf_object_free(&o_key);        

    return( rc );
} // end aerospike_lstack_push_with_keystring()

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
 *  (*) namespace: The Namespace for record holding the LSO bin
 *  (*) set: The Set for the record holding the LSO bin
 *  (*) o_keyp: Ptr to the Key Object (cl_object) that identifies this record
 *  (*) lso_bin_name: Name of the new bin of type "LSO" (AS_MAP).
 *  (*) lso_valuep: Ptr to the as_val instance that is the new stack value
 *  (*) udf_name: Name of the inner UDF (null ok)
 *  (*) function_args: (as_list *) of the args to pass to the inner UDF(null ok)
 *  (*) timeout_ms: Timeout to wait (in MilliSecs). ZERO means forever.
 *
 * Return: a value of the type "cl_rv"
 *   Success:  CITRUSLEAF_OK: (0)
 *   Failure:  CITRUSLEAF_FAIL_CLIENT (-1);
 */
cl_rv
aerospike_lstack_push_with_transform(
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const cl_object * o_keyp,
        const char * lso_bin_name,
        as_val * lso_valuep,
        const char * udf_name,
        as_list * function_args,
        uint32_t timeout_ms )
{

    // Call the inner function that does the real work.
    cl_rv  rc = aerospike_lstack_push_internal(
            asc, namespace, set, o_keyp, lso_bin_name, lso_valuep,
            udf_name, function_args, timeout_ms );

    return rc;
} // end aerospike_lstack_push_with_transform()


// ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
// PUSH WITH TRANSFORM HELPER FUNCTIONS
// (1) with key string: Convert user's key to cl_object
// ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
cl_rv
aerospike_lstack_push_with_transform_with_keystring(
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const char * keystr,
        const char * lso_bin_name,
        as_val * lso_valuep,
        const char * udf_name,
        as_list * function_args,
        uint32_t timeout_ms )
{

    // Load up the key that we'll feed into the call (and we'll remember
    // to free it after we're done).
    cl_object o_key;
    citrusleaf_object_init_str( &o_key, keystr );

    cl_rv rc = aerospike_lstack_push_internal(
            asc, namespace, set, &o_key, lso_bin_name, lso_valuep,
            udf_name, function_args, timeout_ms );
        
    citrusleaf_object_free(&o_key);        

    return( rc );
} // end aerospike_lstack_push_with_transform_with_keystring()


/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Large Stack Object Peek -- INTERNAL
 *  For the given record (associated with 'keyp'), read the top N elements
 *  from the Large Stack Object (named by NS, Set, Key, LsoBinName)
 *
 *  Parms:
 *  (*) asc: The Aerospike Cluster
 *  (*) namespace: The Namespace for record holding the LSO bin
 *  (*) set: The Set for the record holding the LSO bin
 *  (*) o_keyp: Ptr to the Key Object (cl_object) that identifies this record
 *  (*) keystr: The Key that identifies this record
 *  (*) lso_bin_name: Name of the new bin of type "LSO" (AS_MAP).
 *  (*) peek_count: Number of items from the stack we'll retrieve
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
aerospike_lstack_peek_internal(
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const cl_object * o_keyp,
        const char * lso_bin_name,
        int peek_count,
        const char * udf_name,
        as_list * function_args,
        uint32_t timeout_ms )
{
    static char * meth = "aerospike_lstack_peek()";
    char * valstr = NULL;

    if( TRA_ENTER )
        printf("[ENTER]:[%s]: NS(%s) Set(%s) Bin(%s) Count(%d)\n",
                meth, namespace, set, lso_bin_name, peek_count );

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
    arglist = as_arraylist_new(4, 4);    // Just one item -- the new as_val
    as_list_add_string( arglist, lso_bin_name );
    as_list_add_integer( arglist, peek_count );
    if( udf_name != NULL && function_args != NULL ){
        as_list_add_string( arglist, udf_name );
        as_val_reserve( function_args ); // Just as before, bump the ref count
        as_list_append( arglist, (as_val *) function_args );
    }

    // Call the "apply udf" function (function "StackPeek") for this record to
    // extract "peek_count" items from the stack.
    char * function_name;
    if( udf_name != NULL && function_args != NULL ){
        function_name = s_peek_w_udf;
    } else {
        function_name = s_peek;
    }

    if( TRA_DEBUG ){
        printf("[DEBUG]:<%s>UDF Apply:NS(%s) Set(%s) Bin(%s) \n",
                meth, namespace,  set, lso_bin_name );
        valstr = as_val_tostring(arglist);
        printf("[DEBUG]:[%s] Package(%s) Func(%s) Args(%s) \n",
                meth, s_lso_package, function_name, valstr );
        free(valstr);
    }

    cl_rv rc = citrusleaf_udf_record_apply(asc, namespace, set, o_keyp,
            s_lso_package, function_name, arglist, timeout_ms, resultp);

    if (rc != CITRUSLEAF_OK) {
        if( TRA_ERROR )
            printf("[ERROR]:[%s]:citrusleaf_udf_record_apply: Fail: RC(%d)",
                meth, rc);
        goto cleanup;
    }

    if ( resultp->is_success ) {
        printf("[DEBUG]:[%s]:UDF Result SUCCESS\n", meth );
        if ( as_val_type(resultp->value) == AS_NIL ) {
            if( TRA_ERROR ) printf("[ERROR]:[%s] Result type is NIL\n", meth );
            rc = CITRUSLEAF_FAIL_CLIENT; // general client failure
        } else {
            valstr = as_val_tostring(resultp->value);
            printf("[DEBUG]:[%s]: udf_return_type(%s)", meth, valstr);
            free(valstr);
        }
    } else {
        printf("[DEBUG]:[%s]:UDF Result FAIL\n", meth );
        rc = CITRUSLEAF_FAIL_CLIENT; // general client failure
    }

cleanup:
    // NOTE: Clean up arglist, BUT,
    // We do NOT destroy result: The caller has to do that.
    as_val_destroy(arglist);

    if( TRA_EXIT ) printf("[EXIT]:[%s]: RC(%d)\n", meth, rc );
    if( rc == CITRUSLEAF_OK ){
        return resultp;
    } else {
        as_result_destroy(resultp);
        return NULL;
    }
} // aerospike_lstack_peek_internal()


/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Large Stack Object Peek
 *  For the given record (associated with 'keyp'), read the top N elements
 *  from the Large Stack Object (named by NS, Set, Key, LsoBinName)
 *
 *  Parms:
 *  (*) asc: The Aerospike Cluster
 *  (*) namespace: The Namespace for record holding the LSO bin
 *  (*) set: The Set for the record holding the LSO bin
 *  (*) o_keyp: Ptr to the Key Object (cl_object) that identifies this record
 *  (*) lso_bin_name: Name of the new bin of type "LSO" (AS_MAP).
 *  (*) peek_count: Number of items from the stack we'll retrieve
 *  (*) timeout_ms: Timeout to wait (in MilliSecs). ZERO means forever.
 *
 *  Return: 
 *  "as_result *", which is a mallod'c structure that must be manually
 *  freed (as_result_destroy(resultp) by the caller after it is done
 *  using the result.
 */
as_result  *
aerospike_lstack_peek(
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const cl_object * o_keyp,
        const char * lso_bin_name,
        int peek_count,
        uint32_t timeout_ms )
{
    as_result * resultp; // Return results

    // Call the inner function that does the real work.
    resultp = aerospike_lstack_peek_internal(asc, namespace, set, o_keyp,
            lso_bin_name, peek_count, NULL, NULL, timeout_ms );

    return resultp;
} // end aerospike_lstack_peek()


// ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
// CREATE HELPER FUNCTIONS
// (1) with key string: Convert user's key to cl_object
// ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
as_result *
aerospike_lstack_peek_with_keystring(
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const char * keystr,
        const char * lso_bin_name,
        int peek_count,
        uint32_t timeout_ms )
{

    // Load up the key that we'll feed into the call (and we'll remember
    // to free it after we're done).
    cl_object o_key;
    citrusleaf_object_init_str( &o_key, keystr );

    as_result * resultp; // Return results
    resultp = aerospike_lstack_peek_internal(asc, namespace, set, &o_key,
            lso_bin_name, peek_count, NULL, NULL, timeout_ms );

    citrusleaf_object_free(&o_key);        

    return( resultp );
} // end aerospike_lstack_create_with_keystring()

/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Large Stack Object Peek WITH UDF
 *  Peek the top of stack, looking at the top N elements, applying the
 *  transformation/filter UDF to each one.
 *
 *  Parms:
 *  (*) asc: The Aerospike Cluster
 *  (*) namespace: The Namespace for record holding the LSO bin
 *  (*) set: The Set for the record holding the LSO bin
 *  (*) o_keyp: Ptr to the Key Object (cl_object) that identifies this record
 *  (*) lso_bin_name: Name of the new bin of type "LSO" (AS_MAP).
 *  (*) peek_count: Number of items from the stack we'll retrieve
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
aerospike_lstack_peek_with_transform(
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const cl_object * o_keyp,
        const char * lso_bin_name,
        int peek_count,
        const char * udf_name,
        as_list * function_args,
        uint32_t timeout_ms )
{
    as_result * resultp; // Return results

    // Call the inner function that does the real work.
    resultp = aerospike_lstack_peek_internal(
            asc, namespace, set, o_keyp, lso_bin_name, peek_count,
            udf_name, function_args, timeout_ms );

    return resultp;
} // end aerospike_lstack_peek_with_transform()


// ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
// CREATE HELPER FUNCTIONS
// (1) with key string: Convert user's key to cl_object
// ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
as_result *
aerospike_lstack_peek_with_transform_with_keystring(
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const char * keystr,
        const char * lso_bin_name,
        int peek_count,
        const char * udf_name,
        as_list * function_args,
        uint32_t timeout_ms )
{

    // Load up the key that we'll feed into the call (and we'll remember
    // to free it after we're done).
    cl_object o_key;
    citrusleaf_object_init_str( &o_key, keystr );

    as_result * resultp; // Return results
    resultp = aerospike_lstack_peek_internal(
            asc, namespace, set, &o_key, lso_bin_name, peek_count,
            udf_name, function_args, timeout_ms );

    citrusleaf_object_free(&o_key);        

    return( resultp );

} // end aerospike_lstack_peek_with_transform_with_keystring()


/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Large Stack Object Trim:
 *  For the given record (associated with 'keyp'), trim the LSO, leaving
 *  the top N elements.  Notice that the lstack_size() function might
 *  be useful in conjunction with lstack_trim().
 *
 *  Parms:
 *  (*) asc: The Aerospike Cluster
 *  (*) namespace: The Namespace for record holding the LSO bin
 *  (*) set: The Set for the record holding the LSO bin
 *  (*) o_keyp: Ptr to the Key Object (cl_object) that identifies this record
 *  (*) lso_bin_name: Name of the new bin of type "LSO" (AS_MAP).
 *  (*) remainder_count: Delete all but his many items
 *  (*) timeout_ms: Timeout to wait (in MilliSecs). ZERO means forever.
 *
 * Return: a value of the type "cl_rv"
 *   Success:  CITRUSLEAF_OK: (0)
 *   Failure:  CITRUSLEAF_FAIL_CLIENT (-1);
 */
cl_rv
aerospike_lstack_trim(
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const cl_object * o_keyp,
        const char * lso_bin_name,
        int remainder_count,
        uint32_t timeout_ms )
{
    static char * meth = "aerospike_lstack_trim()";
    char * valstr = NULL;

    if( TRA_ENTER )
        printf("[ENTER]:[%s]: NS(%s) Set(%s) Bin(%s) Count(%d)",
                meth, namespace, set, lso_bin_name, remainder_count );

    // In this function, we are returning an int (not the result), so we
    // can use the "stack allocated" result (as_result_init).
    as_result result;
    as_result_init(&result);

    // Set up the arglist that we'll use to pass in function parms
    // As is now the case with all UDF resources, we initialize the
    // arglist, use it then destroy it.
    as_list * arglist = NULL;
    arglist = as_arraylist_new(1, 0); // Just one item -- the trim count
    as_list_add_integer( arglist, remainder_count );

    if( TRA_DEBUG ) {
        printf("[DEBUG]:[%s] UDF Apply:NS(%s) Set(%s) Bin(%s) \n",
                meth, namespace,  set, lso_bin_name );
        valstr = as_val_tostring(arglist);
        printf("[DEBUG]:[%s] Package(%s) Func(%s) Args(%s) \n",
                meth, s_lso_package, s_trim, valstr );
        free(valstr);
    }

    // Call the "apply udf" function (e.g. StackTrim()) for this record to
    // truncate the Stack to "Remainder_Count" items.
    cl_rv rc = citrusleaf_udf_record_apply(asc, namespace, set, o_keyp,
            s_lso_package, s_trim, arglist, timeout_ms, &result);

    if (rc != CITRUSLEAF_OK) {
        if( TRA_ERROR )
            printf("[ERROR]:[%s]:citrusleaf_udf_record_apply: Fail: RC(%d)",
                    meth, rc);
        goto cleanup;
    }

    if ( result.is_success ) {
        printf("[DEBUG]:[%s]:UDF Result SUCCESS\n", meth );
        if ( as_val_type(result.value) == AS_NIL ) {
            if( TRA_ERROR ) printf("[ERROR]:[%s] Result type is NIL\n", meth );
            rc = CITRUSLEAF_FAIL_CLIENT; // general client failure
        } else {
            valstr = as_val_tostring(result.value);
            printf("[DEBUG]:[%s]: udf_return_type(%s)", meth, valstr);
            free(valstr);
        }
    } else {
        printf("[DEBUG]:[%s]:UDF Result FAIL\n", meth );
        rc = CITRUSLEAF_FAIL_CLIENT; // general client failure
    }

cleanup:
    as_val_destroy(arglist);
    as_result_destroy( &result );

    if( TRA_EXIT ) printf("[EXIT]:[%s]: RC(%d)\n", meth, rc );
    return rc;
} // end aerospike_lstack_trim()


// ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
// Will we need a TRIM helper function???
// ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||

/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Large Stack Object Size:
 *  For the given record (associated with 'keyp'), return the size in terms
 *  if number of elements in the stack.
 *
 *  Parms:
 *  (*) asc: The Aerospike Cluster
 *  (*) namespace: The Namespace for record holding the LSO bin
 *  (*) set: The Set for the record holding the LSO bin
 *  (*) o_keyp: Ptr to the Key Object (cl_object) that identifies this record
 *  (*) lso_bin_name: Name of the new bin of type "LSO" (AS_MAP).
 *  (*) timeout_ms: Timeout to wait (in MilliSecs). ZERO means forever.
 *
 *  Return: integer
 *  Success: The size of the large stack
 *  Error: -1.
 */
int
aerospike_lstack_size(
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const cl_object * o_keyp,
        const char * lso_bin_name,
        uint32_t timeout_ms )
{
    static char * meth = "aerospike_lstack_size()";
    cl_rv rc;     // ubiquitous return code
    int size_result = 0;
    char * valstr = NULL;

    if( TRA_ENTER )
        printf("[ENTER]:[%s]: NS(%s) Set(%s) Bin(%s)\n", 
                meth, namespace, set, lso_bin_name );

    // In this function, we are returning an int (not the result), so we
    // can use the "stack allocated" result (as_result_init).
    as_result result;
    as_result_init(&result);

    // Call the "apply udf" function (e.g. lstack_size()) for this record to
    // return the size of the stack.
    rc = citrusleaf_udf_record_apply(asc, namespace, set, o_keyp,
            s_lso_package, s_size, NULL, timeout_ms, &result);

    if (rc != CITRUSLEAF_OK) {
        if( TRA_ERROR )
            printf("[ERROR]:[%s]:citrusleaf_udf_record_apply: Fail: RC(%d)",
                    meth, rc);
        goto cleanup;
    }

    if ( result.is_success ) {
        printf("[DEBUG]:[%s]:UDF Result SUCCESS\n", meth );
        if ( as_val_type(result.value) == AS_NIL ) {
            if( TRA_ERROR ) printf("[ERROR]:[%s] Result type is NIL\n", meth );
            rc = CITRUSLEAF_FAIL_CLIENT; // general client failure
        } else {
            valstr = as_val_tostring(result.value);
            printf("[DEBUG]:[%s]: udf_return_type(%s)", meth, valstr);
            free(valstr);
            // NOTE: May have to check TYPE first and do a conversion
            // to a real int.
            size_result = result.value; // should be an int.
        }
    } else {
        printf("[DEBUG]:[%s]:UDF Result FAIL\n", meth );
        rc = CITRUSLEAF_FAIL_CLIENT; // general client failure
    }

cleanup:
    as_result_destroy( &result );

    if( TRA_EXIT ) printf("[EXIT]:[%s]: RC(%d)\n", meth, rc );
    return rc;
} // end aerospike_lstack_size()()

// ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
// Will we need a SIZE helper function???
// ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
