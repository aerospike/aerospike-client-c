/******************************************************************************
 * Copyright 2008-2013 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to 
 * deal in the Software without restriction, including without limitation the 
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 * sell copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *****************************************************************************/

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

#include <citrusleaf/cf_random.h>
#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_hist.h>

#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/aerospike_lstack.h>
#include <citrusleaf/cl_udf.h>
#include <aerospike/as_types.h>

// Use this to turn on/off tracing/debugging prints and checks
// Comment out this next line to quiet the output.
// #define DEBUG
#undef DEBUG

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

// Define the current version of the C API file for Lstack
// April 25, 2013
#define MOD "Lstack C:4.25.1"

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
// ATTENTION!!!  :::   VERSION CHANGES (April 11, 2013)
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
// (5) The return types are either "cl_rv" (for status results), and the
//     any "content results" are passed back via parameter (address of
//     as_result *) rather than by function return value.  This was done to
//     process all error returns properly.
//
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
static char * s_ldt_package = "LSTACK"; // System file:  LSTACK.lua

// The names of the Lua Functions that implement Large Stack Ops
static char * s_create           = "lstack_create";
static char * s_push             = "lstack_push";
static char * s_create_and_push  = "lstack_create_and_push";
static char * s_peek             = "lstack_peek";
static char * s_peek_w_filter    = "lstack_peek_then_filter";
static char * s_trim             = "lstack_trim"; // not yet implemented
static char * s_size             = "lstack_size";
static char * s_config           = "lstack_config";

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
 *  (*) bin_name: Name of the new bin of type "LSO" (AS_MAP).
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
        const char * bin_name,
        as_map * creation_args,
        uint32_t timeout_ms )
{
    static char * meth = "aerospike_lstack_create()";
    cl_rv rc = CITRUSLEAF_OK; // ubiquitous return code

    if( TRA_ENTER )
        printf("[ENTER]<%s:%s>NS(%s) Set(%s) Bin(%s)\n",
               MOD, meth, namespace, set, bin_name);
    // In this function, we are returning an int (not the result), so we
    // can use the "stack allocated" result (as_result_init).
    as_result result;
    as_result_init(&result);

    // Set up the arglist that we'll use to pass in function parms
    // As is now the case with all UDF resources, we initialize the
    // arglist, use it then destroy it.
    as_list * arglist = NULL;
    arglist = as_arraylist_new(2, 0);
    as_list_add_string(arglist, bin_name );
    as_list_add_map(arglist, creation_args);

    // NOTE: All strings created by "as_val_tostring()" must be explicitly
    // freed after use.
    char * valstr;
    if( TRA_DEBUG ) {
        printf("[DEBUG]<%s:%s>UDF Apply:NS(%s) Set(%s) Bin(%s) \n",
               MOD, meth, namespace,  set, bin_name );
        char * valstr = as_val_tostring(arglist);
        printf("[DEBUG]<%s:%s> Package(%s) Func(%s) Args(%s) \n",
           MOD, meth, s_ldt_package, s_create, valstr );
        free(valstr);
    }

    // Make the Citrusleaf UDF Record Apply call - with all of the stuff we
    // packaged up.
    rc = citrusleaf_udf_record_apply(asc, namespace,
            set, o_keyp, s_ldt_package, s_create, arglist,
            timeout_ms, &result);

    if (rc != CITRUSLEAF_OK) {
        if( TRA_ERROR )
            printf("[ERROR]<%s:%s>:citrusleaf_udf_record_apply: Fail: RC(%d)",
               MOD, meth, rc);
        goto cleanup;
    }

    if ( result.is_success ) {
        if(TRA_DEBUG) printf("[DEBUG]<%s:%s>UDF Result SUCCESS\n", MOD, meth );
        if ( as_val_type(result.value) == AS_NIL ) {
            if(TRA_ERROR) printf("[ERROR]<%s:%s>Result TYPE NIL\n", MOD,meth );
            rc = CITRUSLEAF_FAIL_CLIENT; // general client failure
        } else {
            if( TRA_DEBUG ) {
                valstr = as_val_tostring(result.value);
                printf("[DEBUG]<%s:%s>udf_return_type(%s)", MOD, meth, valstr);
                free(valstr);
            }
        }
    } else {
        if( TRA_DEBUG ) printf("[DEBUG]<%s:%s>:UDF Result FAIL\n", MOD, meth );
        rc = CITRUSLEAF_FAIL_CLIENT; // general client failure
    }

cleanup:
    as_val_destroy(arglist);
    as_result_destroy(&result);

    if( TRA_EXIT ) printf("[EXIT]<%s:%s>: RC(%d)\n", MOD, meth, rc );
    return rc;
}     // end aerospike_lstack_create()


// ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
// CREATE HELPER FUNCTIONS
// (1) using key string: Convert user's key to cl_object
// ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
cl_rv
aerospike_lstack_create_using_keystring(
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const char * keystr,
        const char * bin_name,
        as_map * creation_args,
        uint32_t timeout_ms )
{

    // Load up the key that we'll feed into the call (and we'll remember
    // to free it after we're done).
    cl_object o_key;
    citrusleaf_object_init_str( &o_key, keystr );

    cl_rv rc = aerospike_lstack_create( asc, namespace, set,
            &o_key, bin_name, creation_args, timeout_ms );

    citrusleaf_object_free(&o_key);        

    return( rc );
} // end aerospike_lstack_create_using_keystring()


/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  LSTACK PUSH (Internal)
 *  Do the actual work of both LSTACK PUSH and LSTACK CREATE AND PUSH
 *  For the given record (associated with 'keyp'), push a value onto
 *  the LSTACK in the named LSTACK Bin.
 *  Parms:
 *  (*) asc: The Aerospike Cluster
 *  (*) namespace: The Namespace for record holding the LSO bin
 *  (*) set: The Set for the record holding the LSO bin
 *  (*) o_keyp: Ptr to the Key Object (cl_object) that identifies this record
 *  (*) bin_name: Name of the new bin of type "LSO" (AS_MAP).
 *  (*) lso_valuep: Ptr to the as_val instance that is the new stack value
 *  (*) creation_args: A map (as_map) of creation settings (can be null)
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
        const char * bin_name,
        as_val * lso_valuep,
        as_map * creation_spec,
        uint32_t timeout_ms )
{
    static char * meth = "aerospike_lstack_push_internal()";
    cl_rv rc; // ubiquitous return code
    char * valstr = NULL;

    // Note: results of as_val_tostring() must ALWAYS be explicitly freed
    if( TRA_ENTER ) {
        valstr = as_val_tostring( lso_valuep );
        printf("[ENTER]<%s:%s> NS(%s) Set(%s) Bin(%s) Val(%s)",
               MOD, meth, namespace, set, bin_name, valstr );
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
    as_list_add_string( arglist, bin_name );
    as_list_append( arglist, lso_valuep );
    if( creation_spec != NULL ){
        as_val_reserve( creation_spec ); // Just as before, bump the ref count
        as_list_append( arglist, (as_val *) creation_spec );
    }

    // NOTE: Have verified that the as_val (the list) passed to us was
    // created with "new", so we have a malloc'd value.

    // Call the "apply udf" function (e.g. StackPush()) for this record to
    // push a new value onto the LSO Bin.  Call the appropriate Lua function.
    // If the caller wants to invoke "create_and_push", she MUST pass in
    // a NON-NULL creation arg (valid or not). (Bad args are ignored.)
    char * function_name;
    if( creation_spec != NULL ){
        function_name = s_create_and_push;
    } else {
        function_name = s_push;
    }

    if( TRA_DEBUG ) {
        printf("[DEBUG]<%s:%s>UDF Apply:NS(%s) Set(%s) Bin(%s) \n",
               MOD, meth, namespace,  set, bin_name );
        valstr = as_val_tostring(arglist);
        printf("[DEBUG]<%s:%s> Package(%s) Func(%s) Fargs(%s)\n",
               MOD, meth, s_ldt_package, function_name, valstr );
        free(valstr);
    }

    rc = citrusleaf_udf_record_apply(asc, namespace, set, o_keyp,
            s_ldt_package, function_name, arglist, timeout_ms, &result);

    if (rc != CITRUSLEAF_OK) {
        if( TRA_ERROR )
            printf("[ERROR]<%s:%s>:citrusleaf_udf_record_apply: Fail: RC(%d)",
               MOD, meth, rc);
        goto cleanup;
    }

    if ( result.is_success ) {
        if( TRA_DEBUG ) printf("[DEBUG]<%s:%s>:UDF Result SUCCESS\n",MOD,meth);
        if ( as_val_type(result.value) == AS_NIL ) {
            if(TRA_ERROR) printf("[ERROR]<%s:%s> Result TYPE NIL\n",MOD,meth);
            rc = CITRUSLEAF_FAIL_CLIENT; // general client failure
        } else {
            valstr = as_val_tostring(result.value);
            if( TRA_DEBUG )
                printf("[DEBUG]<%s:%s>: udf_return_type(%s)",MOD, meth, valstr);
            free(valstr);
        }
    } else {
        if( TRA_ERROR ) printf("[ERROR]<%s:%s>:UDF Result FAIL\n", MOD, meth );
        rc = CITRUSLEAF_FAIL_CLIENT; // general client failure
    }

cleanup:
    as_val_destroy(arglist);
    as_result_destroy(&result);

    if( TRA_EXIT ) printf("[EXIT]<%s:%s>: RC(%d)\n",MOD, meth, rc );
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
 *  (*) bin_name: Name of the new bin of type "LSO" (AS_MAP).
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
        const char * bin_name,
        as_val * lso_valuep,
        uint32_t timeout_ms )
{

    // Call the inner function that does the real work.
    cl_rv rc = aerospike_lstack_push_internal(
            asc, namespace, set, o_keyp, bin_name, lso_valuep,
            NULL, timeout_ms );

    return rc;
} // end aerospike_lstack_push()


// ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
// LSTACK PUSH HELPER FUNCTIONS
// (1) using key string: Convert user's key to cl_object
// ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
cl_rv
aerospike_lstack_push_using_keystring(
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const char * keystr,
        const char * bin_name,
        as_val * lso_valuep,
        uint32_t timeout_ms )
{

    // Load up the key that we'll feed into the call (and we'll remember
    // to free it after we're done).
    cl_object o_key;
    citrusleaf_object_init_str( &o_key, keystr );

    cl_rv rc = aerospike_lstack_push_internal(
            asc, namespace, set, &o_key, bin_name, lso_valuep,
            NULL, timeout_ms );

    citrusleaf_object_free(&o_key);        

    return( rc );
} // end aerospike_lstack_push_using_keystring()

/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Large Stack Object Create And Push.
 *  For the given record (associated with 'keyp'), push a value onto
 *  the LSO stack in the named LSO Bin -- and do an implicit create of the
 *  LSO if it does not already exist.  Creation args are expected to be
 *  supplied (such as a package name).
 *  Parms:
 *  - keyp: Key for the record
 *  - new_valp: The new "value list" to be pushed on the stack
 *  - lso_binp: ptr to the LSO Bin Name
 *
 *  (*) asc: The Aerospike Cluster
 *  (*) namespace: The Namespace for record holding the LSO bin
 *  (*) set: The Set for the record holding the LSO bin
 *  (*) o_keyp: Ptr to the Key Object (cl_object) that identifies this record
 *  (*) bin_name: Name of the new bin of type "LSO" (AS_MAP).
 *  (*) lso_valuep: Ptr to the as_val instance that is the new stack value
 *  (*) creation_spec: A map (as_map) of creation settings (can be null)
 *  (*) timeout_ms: Timeout to wait (in MilliSecs). ZERO means forever.
 *
 * Return: a value of the type "cl_rv"
 *   Success:  CITRUSLEAF_OK: (0)
 *   Failure:  CITRUSLEAF_FAIL_CLIENT (-1);
 */
cl_rv
aerospike_lstack_create_and_push(
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const cl_object * o_keyp,
        const char * bin_name,
        as_val * lso_valuep,
        as_map * creation_spec,
        uint32_t timeout_ms )
{

    // Call the inner function that does the real work.
    cl_rv  rc = aerospike_lstack_push_internal(
            asc, namespace, set, o_keyp, bin_name, lso_valuep,
            creation_spec, timeout_ms );

    return rc;
} // end aerospike_lstack_create_and_push()


// ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
// PUSH WITH CREATE HELPER FUNCTIONS
// (1) with key string: Convert user's key to cl_object
// ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
cl_rv
aerospike_lstack_create_and_push_using_keystring(
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const char * keystr,
        const char * bin_name,
        as_val * lso_valuep,
        as_map * create_spec,
        uint32_t timeout_ms )
{

    // Load up the key that we'll feed into the call (and we'll remember
    // to free it after we're done).
    cl_object o_key;
    citrusleaf_object_init_str( &o_key, keystr );

    cl_rv rc = aerospike_lstack_push_internal(
            asc, namespace, set, &o_key, bin_name, lso_valuep,
            create_spec, timeout_ms );
        
    citrusleaf_object_free(&o_key);        

    return( rc );
} // end aerospike_lstack_create_and_push_using_keystring()


/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Large Stack Object Peek -- INTERNAL
 *  For the given record (associated with 'keyp'), read the top N elements
 *  from the Large Stack Object (named by NS, Set, Key, LsoBinName)
 *
 *  Parms:
 *  (*) resultpp: The pointer to the Peek Result (if no error).
 *  (*) asc: The Aerospike Cluster
 *  (*) namespace: The Namespace for record holding the LSO bin
 *  (*) set: The Set for the record holding the LSO bin
 *  (*) o_keyp: Ptr to the Key Object (cl_object) that identifies this record
 *  (*) keystr: The Key that identifies this record
 *  (*) bin_name: Name of the new bin of type "LSO" (AS_MAP).
 *  (*) peek_count: Number of items from the stack we'll retrieve
 *  (*) filter: Name of the inner UDF (null ok)
 *  (*) function_args: (as_list *) of the args to pass to the inner UDF(null ok)
 *  (*) timeout_ms: Timeout to wait (in MilliSecs). ZERO means forever.
 *
 *  Return: 
 *  rc = 0:  Success.  Results are put into user's resultp parameter.
 *  rc < 0:  Error Case.
 *  Note: Caller's "as_result * resultp" will be filled in with a malloc'd
 *  structure that must be manually freed (as_result_destroy(resultp) by the
 *  caller after it is done using the result.
 */
cl_rv
aerospike_lstack_peek_internal(
        as_result ** resultpp,
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const cl_object * o_keyp,
        const char * bin_name,
        int peek_count,
        const char * filter,
        as_list * function_args,
        uint32_t timeout_ms )
{
    static char * meth = "aerospike_lstack_peek()";
    char * valstr = NULL;

    if( TRA_ENTER )
        printf("[ENTER]<%s:%s>: NS(%s) Set(%s) Bin(%s) Count(%d)\n",
               MOD, meth, namespace, set, bin_name, peek_count );

    // For Result, we are going to pass this back to the caller, so we
    // must FULLY DOCUMENT the fact that they must call "as_result_destroy()"
    // on the result after they are done with it.
    as_result * resultp = NULL;
    resultp = as_result_new();
    if( resultp == NULL ){
        // Big trouble.  Client Failure -- can't allocate a result object.
        return( CITRUSLEAF_FAIL_CLIENT );
    }
    *resultpp = resultp; // This is the user's return value.
    cl_rv rc = CITRUSLEAF_OK; // This is the user's return code.

    // Set up the arglist that we'll use to pass in function parms
    // As is now the case with all UDF resources, we initialize the
    // arglist, use it then destroy it.
    //
    // Note: function_args is an <as_val> type, so if present, it must be
    // protected (with as_reserve()) so that it doesn't get destroyed twice.
    as_list * arglist = NULL;
    arglist = as_arraylist_new(4, 4);    // Just one item -- the new as_val
    as_list_add_string( arglist, bin_name );
    as_list_add_integer( arglist, peek_count );
    if( filter != NULL && function_args != NULL ){
        as_list_add_string( arglist, filter );
        as_val_reserve( function_args ); // Just as before, bump the ref count
        as_list_append( arglist, (as_val *) function_args );
    }

    // Call the "apply udf" function (function "StackPeek") for this record to
    // extract "peek_count" items from the stack.
    char * function_name;
    if( filter != NULL && function_args != NULL ){
        function_name = s_peek_w_filter;
    } else {
        function_name = s_peek;
    }

    if( TRA_DEBUG ){
        printf("[DEBUG]:<%s:%s>UDF Apply:NS(%s) Set(%s) Bin(%s) \n",
               MOD, meth, namespace,  set, bin_name );
        valstr = as_val_tostring(arglist);
        printf("[DEBUG]<%s:%s> Package(%s) Func(%s) Args(%s) \n",
               MOD, meth, s_ldt_package, function_name, valstr );
        free(valstr);
    }

    rc = citrusleaf_udf_record_apply(asc, namespace, set, o_keyp,
            s_ldt_package, function_name, arglist, timeout_ms, resultp);

    if (rc != CITRUSLEAF_OK) {
        if( TRA_ERROR )
            printf("[ERROR]<%s:%s>:citrusleaf_udf_record_apply: Fail: RC(%d)",
               MOD, meth, rc);
        goto cleanup;
    }

    if ( resultp->is_success ) {
        if( TRA_DEBUG ) printf("[DEBUG]<%s:%s>:UDF Result SUCCESS\n",MOD,meth );
        if ( as_val_type(resultp->value) == AS_NIL ) {
            if(TRA_ERROR) printf("[ERROR]<%s:%s>Result TYPE NIL\n",MOD,meth );
            rc = CITRUSLEAF_FAIL_CLIENT; // general client failure
        } else {
            valstr = as_val_tostring(resultp->value);
            if( TRA_DEBUG )
                printf("[DEBUG]<%s:%s>: udf_return_type(%s)",MOD, meth, valstr);
            free(valstr);
        }
    } else {
        if( TRA_ERROR )
            printf("[ERROR]<%s:%s>:UDF Result FAIL\n", MOD, meth );
        rc = CITRUSLEAF_FAIL_CLIENT; // general client failure
    }

cleanup:
    // NOTE: Clean up arglist, BUT,
    // We do NOT destroy result: The caller has to do that.
    as_val_destroy(arglist);

    if( TRA_EXIT ) printf("[EXIT]<%s:%s>: RC(%d)\n",MOD, meth, rc );
    if( rc != CITRUSLEAF_OK ){
        // Bad result, so contents of resultp are not reliable.
        // Note that this function needs to change so that the caller
        // passes in the resultp and we always return a return code.
        if( resultp != NULL ) {
            as_result_destroy(resultp);
        }
        *resultpp = NULL; // client gets NULL in her resultp var.
    }
    return( rc ); 
} // aerospike_lstack_peek_internal()


/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Large Stack Object Peek
 *  For the given record (associated with 'keyp'), read the top N elements
 *  from the Large Stack Object (named by NS, Set, Key, LsoBinName)
 *
 *  Parms:
 *  (*) resultpp: The pointer to the Peek Result (if no error).
 *  (*) asc: The Aerospike Cluster
 *  (*) namespace: The Namespace for record holding the LSO bin
 *  (*) set: The Set for the record holding the LSO bin
 *  (*) o_keyp: Ptr to the Key Object (cl_object) that identifies this record
 *  (*) bin_name: Name of the new bin of type "LSO" (AS_MAP).
 *  (*) peek_count: Number of items from the stack we'll retrieve
 *  (*) timeout_ms: Timeout to wait (in MilliSecs). ZERO means forever.
 *
 *  Return: 
 *  rc = 0:  Success.  Results are put into user's resultp parameter.
 *  rc < 0:  Error Case.
 *  Note: Caller's "as_result * resultp" will be filled in with a malloc'd
 *  structure that must be manually freed (as_result_destroy(resultp) by the
 *  caller after it is done using the result.
 */
cl_rv 
aerospike_lstack_peek(
        as_result ** resultpp,
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const cl_object * o_keyp,
        const char * bin_name,
        int peek_count,
        uint32_t timeout_ms )
{

    // Call the inner function that does the real work.
    cl_rv status = aerospike_lstack_peek_internal( resultpp,
            asc, namespace, set, o_keyp,
            bin_name, peek_count, NULL, NULL, timeout_ms );

    return status;
} // end aerospike_lstack_peek()


// ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
// CREATE HELPER FUNCTIONS
// (1) using key string: Convert user's key to cl_object
// ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
cl_rv
aerospike_lstack_peek_using_keystring(
        as_result ** resultpp,
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const char * keystr,
        const char * bin_name,
        int peek_count,
        uint32_t timeout_ms )
{

    // Load up the key that we'll feed into the call (and we'll remember
    // to free it after we're done).
    cl_object o_key;
    citrusleaf_object_init_str( &o_key, keystr );

    cl_rv status  = aerospike_lstack_peek_internal(
            resultpp, asc, namespace, set, &o_key,
            bin_name, peek_count, NULL, NULL, timeout_ms );

    citrusleaf_object_free(&o_key);        

    return( status );
} // end aerospike_lstack_create_using_keystring()

/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Large Stack Object Peek Then apply UDF (peek then filter)
 *  Peek the top of stack, looking at the top N elements, applying the
 *  transformation function (if it is defined at create time), then
 *  applying the filter (if present) to each peek item.
 *
 *  Parms:
    (*) resultpp: The address of the user's (as_result *) variable.
 *  (*) asc: The Aerospike Cluster
 *  (*) namespace: The Namespace for record holding the LSO bin
 *  (*) set: The Set for the record holding the LSO bin
 *  (*) o_keyp: Ptr to the Key Object (cl_object) that identifies this record
 *  (*) bin_name: Name of the new bin of type "LSO" (AS_MAP).
 *  (*) peek_count: Number of items from the stack we'll retrieve
 *  (*) filter: Name of the inner UDF (null ok)
 *  (*) function_args: (as_list *) of the args to pass to the inner UDF(null ok)
 *  (*) timeout_ms: Timeout to wait (in MilliSecs). ZERO means forever.
 *
 *  Return: 
 *  "as_result *", which is a mallod'c structure that must be manually
 *  freed (as_result_destroy(resultp) by the caller after it is done
 *  using the result.
 *  (*) NULL if there is an extreme client error
 */
cl_rv
aerospike_lstack_peek_then_filter(
        as_result ** resultpp,
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const cl_object * o_keyp,
        const char * bin_name,
        int peek_count,
        const char * filter,
        as_list * function_args,
        uint32_t timeout_ms )
{
    // Call the inner function that does the real work.
    cl_rv rc =  aerospike_lstack_peek_internal(
            resultpp, asc, namespace, set, o_keyp, bin_name, peek_count,
            filter, function_args, timeout_ms );

    return rc;
} // end aerospike_lstack_peek_then_filter()


// ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
// CREATE HELPER FUNCTIONS
// (1) using key string: Convert user's key to cl_object
// ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
cl_rv
aerospike_lstack_peek_then_filter_using_keystring(
        as_result ** resultpp,
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const char * keystr,
        const char * bin_name,
        int peek_count,
        const char * filter,
        as_list * function_args,
        uint32_t timeout_ms )
{

    // Load up the key that we'll feed into the call (and we'll remember
    // to free it after we're done).
    cl_object o_key;
    citrusleaf_object_init_str( &o_key, keystr );

    cl_rv rc = aerospike_lstack_peek_internal(
            resultpp, asc, namespace, set, &o_key, bin_name, peek_count,
            filter, function_args, timeout_ms );

    citrusleaf_object_free(&o_key);        

    return( rc );

} // end aerospike_lstack_peek_then_filter_using_keystring()


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
 *  (*) bin_name: Name of the new bin of type "LSO" (AS_MAP).
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
        const char * bin_name,
        int remainder_count,
        uint32_t timeout_ms )
{
    static char * meth = "aerospike_lstack_trim()";
    char * valstr = NULL;

    if( TRA_ENTER )
        printf("[ENTER]<%s:%s>: NS(%s) Set(%s) Bin(%s) Count(%d)",
               MOD, meth, namespace, set, bin_name, remainder_count );

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
        printf("[DEBUG]<%s:%s> UDF Apply:NS(%s) Set(%s) Bin(%s) \n",
               MOD, meth, namespace,  set, bin_name );
        valstr = as_val_tostring(arglist);
        printf("[DEBUG]<%s:%s> Package(%s) Func(%s) Args(%s) \n",
               MOD, meth, s_ldt_package, s_trim, valstr );
        free(valstr);
    }

    // Call the "apply udf" function (e.g. StackTrim()) for this record to
    // truncate the Stack to "Remainder_Count" items.
    cl_rv rc = citrusleaf_udf_record_apply(asc, namespace, set, o_keyp,
            s_ldt_package, s_trim, arglist, timeout_ms, &result);

    if (rc != CITRUSLEAF_OK) {
        if( TRA_ERROR )
            printf("[ERROR]<%s:%s>:citrusleaf_udf_record_apply: Fail: RC(%d)",
                   MOD, meth, rc);
        goto cleanup;
    }

    if ( result.is_success ) {
        printf("[DEBUG]<%s:%s>:UDF Result SUCCESS\n", MOD, meth );
        if ( as_val_type(result.value) == AS_NIL ) {
            if( TRA_ERROR )
                printf("[ERROR]<%s:%s> Result type is NIL\n", MOD, meth );
            rc = CITRUSLEAF_FAIL_CLIENT; // general client failure
        } else {
            valstr = as_val_tostring(result.value);
            if( TRA_DEBUG )
                printf("[DEBUG]<%s:%s>: udf_return_type(%s)",MOD, meth, valstr);
            free(valstr);
        }
    } else {
        if( TRA_ERROR ) printf("[ERROR]<%s:%s>:UDF Result FAIL\n", MOD, meth );
        rc = CITRUSLEAF_FAIL_CLIENT; // general client failure
    }

cleanup:
    as_val_destroy(arglist);
    as_result_destroy( &result );

    if( TRA_EXIT ) printf("[EXIT]<%s:%s>: RC(%d)\n", MOD, meth, rc );
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
 *  (*) size: The variable in which we return the size
 *  (*) asc: The Aerospike Cluster
 *  (*) namespace: The Namespace for record holding the LSO bin
 *  (*) set: The Set for the record holding the LSO bin
 *  (*) o_keyp: Ptr to the Key Object (cl_object) that identifies this record
 *  (*) bin_name: Name of the new bin of type "LSO" (AS_MAP).
 *  (*) timeout_ms: Timeout to wait (in MilliSecs). ZERO means forever.
 *
 *  Success: 
 *  rc = 0; The size of the large stack is returned in "size".
 *  rc < 0: Error Condition
 */
cl_rv
aerospike_lstack_size(
        uint32_t   * size,
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const cl_object * o_keyp,
        const char * bin_name,
        uint32_t timeout_ms )
{
    static char * meth = "aerospike_lstack_size()";
    cl_rv rc;     // ubiquitous return code
    uint32_t size_result = 0;
    char * valstr = NULL;

    if( TRA_ENTER )
        printf("[ENTER]<%s:%s>: NS(%s) Set(%s) Bin(%s)\n", 
               MOD, meth, namespace, set, bin_name );

    // In this function, we are returning an int (not the result), so we
    // can use the "stack allocated" result (as_result_init).
    as_result result;
    as_result_init(&result);

    // Lua Call: lstack_size( record, bin_name )
    as_list * arglist = NULL;
    arglist = as_arraylist_new(1, 0); // One item in the arglist
    as_list_add_string(arglist, bin_name);
    

    // Call the "apply udf" function (e.g. lstack_size()) for this record to
    // return the size of the stack.
    rc = citrusleaf_udf_record_apply(asc, namespace, set, o_keyp,
            s_ldt_package, s_size, arglist, timeout_ms, &result);

    if (rc != CITRUSLEAF_OK) {
        if( TRA_ERROR )
            printf("[ERROR]<%s:%s>:citrusleaf_udf_record_apply: Fail: RC(%d)",
                   MOD, meth, rc);
        goto cleanup;
    }

    if ( result.is_success ) {
        if(TRA_DEBUG) printf("[DEBUG]<%s:%s>:UDF Result SUCCESS\n", MOD, meth );
        if ( as_val_type(result.value) == AS_NIL ) {
            if( TRA_ERROR )
                printf("[ERROR]<%s:%s> Result type is NIL\n", MOD, meth );
            rc = CITRUSLEAF_FAIL_CLIENT; // general client failure
        } else {
            valstr = as_val_tostring(result.value);
            if( TRA_DEBUG )
                printf("[DEBUG]<%s:%s>udf_return_type(%s)", MOD, meth, valstr);
            free(valstr);
            // NOTE: May have to check TYPE first and do a conversion
            // to a real int.
            as_val * val_sizep = as_result_value( &result );
            as_integer * int_sizep = as_integer_fromval( val_sizep );
            size_result = as_integer_toint( int_sizep ); // should be an int.
            *size = (uint32_t) size_result;
        }
    } else {
        if( TRA_ERROR ) printf("[ERROR]<%s:%s>:UDF Result FAIL\n", MOD, meth );
        rc = CITRUSLEAF_FAIL_CLIENT; // general client failure
    }

cleanup:
    as_result_destroy( &result );
    as_val_destroy( arglist );

    if( TRA_EXIT ) printf("[EXIT]<%s:%s>: RC(%d)\n",MOD, meth, rc );
    return rc;
} // end aerospike_lstack_size()()


/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Large Stack Object config:
 *  For the given record (associated with 'keyp' and bin_name, return
 *  the config information.
 *
 *  Parms:
 *  (*) resultpp: The address of the user's (as_result *) variable.
 *  (*) asc: The Aerospike Cluster
 *  (*) namespace: The Namespace for record holding the LSO bin
 *  (*) set: The Set for the record holding the LSO bin
 *  (*) o_keyp: Ptr to the Key Object (cl_object) that identifies this record
 *  (*) bin_name: Name of the new bin of type "LSO" (AS_MAP).
 *  (*) timeout_ms: Timeout to wait (in MilliSecs). ZERO means forever.
 *
 *  Success: 
 *  rc = 0; The as_map, holding the config, is return in * resultpp
 *  rc < 0: Error Condition
 */
cl_rv
aerospike_lstack_config(
        as_result ** resultpp,
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const cl_object * o_keyp,
        const char * bin_name,
        uint32_t timeout_ms )
{
    static char * meth = "aerospike_lstack_config()";
    cl_rv rc;     // ubiquitous return code
    int size_result = 0;
    char * valstr = NULL;

    if( TRA_ENTER )
        printf("[ENTER]<%s:%s>: NS(%s) Set(%s) Bin(%s)\n", 
               MOD, meth, namespace, set, bin_name );

    // For Result, we are going to pass this back to the caller, so we
    // must FULLY DOCUMENT the fact that they must call "as_result_destroy()"
    // on the result after they are done with it.
    as_result * resultp = NULL;
    resultp = as_result_new();
    if( resultp == NULL ){
        // Big trouble.  Client Failure -- can't allocate a result object.
        return( CITRUSLEAF_FAIL_CLIENT );
    }
    *resultpp = resultp; // This is the user's return value.
    rc = CITRUSLEAF_OK; // This is the user's return code.

    // Lua Call: lstack_config( record, bin_name )
    as_list * arglist = NULL;
    arglist = as_arraylist_new(1, 0); // One item in the arglist
    as_list_add_string(arglist, bin_name);
    
    // Call the "apply udf" function (e.g. lstack_size()) for this record to
    // return the size of the stack.
    rc = citrusleaf_udf_record_apply(asc, namespace, set, o_keyp,
            s_ldt_package, s_config, arglist, timeout_ms, resultp);

    if (rc != CITRUSLEAF_OK) {
        if( TRA_ERROR )
            printf("[ERROR]<%s:%s>:citrusleaf_udf_record_apply: Fail: RC(%d)",
                   MOD, meth, rc);
        goto cleanup;
    }

cleanup:
    // We do NOT destroy result (in resultp): The caller has to do that.
    // However, if there were errors, then the contents of resultp are
    // undetermined, so we null it out when there are errors.
    as_val_destroy( arglist );

    if( TRA_EXIT ) printf("[EXIT]<%s:%s>: RC(%d)\n",MOD, meth, rc );
    if( rc != CITRUSLEAF_OK ){
        // Bad result, so contents of resultp are not reliable.
        // Note that this function needs to change so that the caller
        // passes in the resultp and we always return a return code.
        if( resultp != NULL ) {
            as_result_destroy(resultp);
        }
        *resultpp = NULL; // client gets NULL in her resultp var.
    }
    if( TRA_EXIT ) printf("[EXIT]<%s:%s>: RC(%d)\n", MOD, meth, rc );
    return rc;
} // end aerospike_lstack_config()()

// <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF>
