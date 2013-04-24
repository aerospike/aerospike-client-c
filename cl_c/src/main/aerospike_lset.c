/*  Citrusleaf Aerospike Large Set (LSET) API
 *  aerospike_lset.c - Validates AS SET stored procedure functionality
 *  and provides a C Language interface for the as_lset server type.
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
#include "aerospike_lset.h"
#include "cl_udf.h"
#include <citrusleaf/cf_random.h>
#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_hist.h>

// Use this to turn on/off tracing/debugging prints and checks
// Comment out this next line to quiet the output.
#define DEBUG
// #undef DEBUG

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

// Define the current version of the C API file for LSET
// April 18, 2013
#define MOD "Lset C_API:4.18"

// ATTENTION!!!  :::   VERSION CHANGES (April 11, 2013)
// NOTE: We have changed the API for Large SET Objects (LSET) to be
// more in line with the emerging Aerospike standard.  Here's a summary
// of the changes:
// (1) The package (LSET.lua) will be implicit, so it will no longer
//     be passed in as a parameter (was lset_package).
// (2) The key field will be a cl_object, not a string.
// (3) The names of the large Set operations have changed to the standard
//     "C Style" of functions (e.g. lset_create(), lset_insert()).
// (4) The names of the functions defined in this file change from as_lset_xx()
//     to aerospike_lset_xx().
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
//
// =======================================================================
// Documented here for all methods -- the "apply udf" call:
// =======================================================================
// Call the "apply udf" function (e.g. function "lset_create") for this
// record to create the lset Bin. Here's the Doc for the UDF call
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
// =======================================================================

// ++==============++
// || Fixed Values ||
// ++==============++
// PackageName: This is now a fixed term, so users no longer pass it in.
// The LSET package is now IMPLICIT as a known system Lua file.
static char * s_ldt_package = "LSET"; // System file:  LSET.lua

// The names of the Lua Functions that implement Large Set Ops
static char * s_create            = "lset_create";
static char * s_insert            = "lset_insert";
static char * s_create_and_insert = "lset_create_and_insert";
static char * s_search            = "lset_search";
static char * s_search_filter     = "lset_search_then_filter";
static char * s_exists            = "lset_exists";
static char * s_delete            = "lset_delete";
static char * s_size              = "lset_size";
static char * s_config            = "lset_config";

/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Large Set Create:
 *  Call the Large Set Create() routine to create a Large Set Object bin in
 *  a record.  The record corresponding to this key may or may not already
 *  exist (we either create a new record or update an existing one with
 *  the new LSet bin). The only error is if there is an existing bin with
 *  the supplied name.
 *  Parms:
 *  (*) asc: The Aerospike Cluster
 *  (*) namespace: The Namespace for record holding the LSet bin
 *  (*) set: The Set for the record holding the LSet bin
 *  (*) o_keyp: Ptr to the Key that identifies this record
 *  (*) bin_name: Name of the new bin of type "LSet" (AS_MAP).
 *  (*) create_spec: An as_map structure with creation settings
 *  (*) timeout_ms: The timeout in milliseconds to wait for the UDF op.
 */
cl_rv aerospike_lset_create(cl_cluster * asc, const char * namespace,
		const char * set, const cl_object *o_keyp, const char * bin_name,
		as_map * create_spec, // This will include the distribution value.
		uint32_t timeout_ms)
{
	static char * meth = "aerospike_lset_create()";
	cl_rv rc = 0; // ubiquitous return code
	// In this function, we are returning an int (not the result), so we
	// can use the "stack allocated" result (as_result_init).
	as_result result;
	as_result_init(&result);

	// Set up the arglist that we'll use to pass in function parms
	// As is now the case with all UDF resources, we initialize the
	// arglist, use it then destroy it.
	// LSET:lset_create(record, bin_name, create_spec)
	as_list * arglist = NULL;
	arglist = as_arraylist_new(2, 0); // We have 2 parms to pass
	as_list_add_string(arglist, bin_name);
	if (create_spec != NULL) {
		as_val_reserve( create_spec);
		as_list_append(arglist, (as_val *) create_spec);
	}

	rc = citrusleaf_udf_record_apply(asc, namespace, set, o_keyp, s_ldt_package,
			s_create, arglist, timeout_ms, &result);

	if (rc != CITRUSLEAF_OK) {
		if (TRA_DEBUG)
			printf("[ERROR]:<%s:%s>:citrusleaf_udf_record_apply: Fail: RC(%d)",
					MOD, meth, rc);
		goto cleanup;
	}

	if (result.is_success) {
		if (TRA_DEBUG)
			printf("[DEBUG]:<%s:%s>:UDF Result SUCCESS\n", MOD, meth);
		if (as_val_type(result.value) == AS_NIL) {
			if (TRA_DEBUG)
				printf("[ERROR]:<%s:%s> Result type is NIL\n", MOD, meth);
			rc = CITRUSLEAF_FAIL_CLIENT; // General Fail
		} else {
			char *valstr = as_val_tostring(result.value);
			if (TRA_DEBUG)
				printf("[DEBUG]:<%s:%s>: udf_return_type(%s)", MOD, meth,
						valstr);
			free(valstr);
		}
	} else {
		if (TRA_DEBUG)
			printf("[DEBUG]:<%s:%s>:UDF Result FAIL\n", MOD, meth);
		rc = CITRUSLEAF_FAIL_CLIENT; // General Fail
	}

	cleanup:
	as_val_destroy(arglist);
	as_result_destroy(&result);

	return rc;
} // end aerospike_lset_create()

// ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
// CREATE HELPER FUNCTIONS
// (1) using key string: Convert user's key to cl_object
// ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
cl_rv aerospike_lset_create_using_keystring(cl_cluster * asc,
		const char * namespace, const char * set, const char * keystr,
		const char * bin_name, as_map * creation_args, uint32_t timeout_ms)
{

	// Load up the key that we'll feed into the call (and we'll remember
	// to free it after we're done).
	cl_object o_key;
	citrusleaf_object_init_str(&o_key, keystr);

	cl_rv rc = aerospike_lset_create(asc, namespace, set, &o_key, bin_name,
			creation_args, timeout_ms);

	citrusleaf_object_free(&o_key);

	return (rc);
} // end aerospike_lset_create_using_keystring()


/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  AS Large Set Insert (INTERNAL)
 *  Do the work of both LSET INSERT and LSET CREATE AND INSERT.
 *  For the given record (associated with 'keyp'), insert a value in the
 *  Large Set in the named LSET Bin.
 *  Parms:
 *  (*) asc: The Aerospike Cluster
 *  (*) namespace: The Namespace for record holding the Large Set bin
 *  (*) set: The Set for the record holding the Large Set bin
 *  (*) key: The Key that identifies this record
 *  (*) bin_name: Name of the new bin of type "Large Set".
 *  (*) lset_valuep: Ptr to the as_val instance to be inserted in the set
 *  (*) creation_args: A map (as_map) of creation settings (can be null)
 *  (*) timeout_ms: Timeout to wait (in MilliSecs). ZERO means forever.
 *
 * Return: a value of the type "cl_rv"
 *   Success:  CITRUSLEAF_OK: (0)
 *   Failure:  CITRUSLEAF_FAIL_CLIENT (-1);
 */
cl_rv aerospike_lset_insert_internal(cl_cluster * asc, const char * namespace,
		const char * set, const cl_object * o_keyp, const char * bin_name,
		as_val * lset_valuep, as_map * creation_spec, uint32_t timeout_ms)
{
	static char * meth = "aerospike_lset_insert_internal()";
	cl_rv rc = 0; // ubiquitous return code

	// Call the "apply udf" function (e.g. lset_insert()) for this record to
	// insert a new value into the LDT Bin.  Call the appropriate Lua function.
	// If the caller wants to invoke "create_and_insert", she MUST pass in
	// a NON-NULL creation arg (valid or not). (Bad args are ignored.)
	char * function_name;
	if (creation_spec != NULL) {
		function_name = s_create_and_insert;
	} else {
		function_name = s_insert;
	}

	// Note: results of as_val_tostring() must ALWAYS be explicitly freed

	// In this function, we are returning an int (not the result), so we
	// can use the "stack allocated" result (as_result_init).
	as_result result;
	as_result_init(&result);

	// Set up the arglist that we'll use to pass in function parms
	// As is now the case with all UDF resources, we initialize the
	// arglist, use it then destroy it.  Also, we MALLOC (vs using a
	// stack value), as the values are used via reference (I think).
	//
	// Note: lset_valuep is an as_val type that we are embedding in ANOTHER
	// as_val type, so we must increment the reference count (with
	// as_val_reserve) so that all of the free()/destroy() calls can match up.

	as_list * arglist = NULL;
	arglist = as_arraylist_new(4, 0); // Two items
	as_list_add_string(arglist, bin_name);
	as_val_reserve( lset_valuep);
	// Increment the reference count for valuep
	as_list_append(arglist, lset_valuep);

	if (creation_spec != NULL) {
		as_val_reserve( creation_spec);
		// bump the ref count
		as_list_append(arglist, (as_val *) creation_spec);
	}

	// NOTE: Have verified that the as_val (the list) passed to us was
	// created with "new", so we have a malloc'd value.
	rc = citrusleaf_udf_record_apply(asc, namespace, set, o_keyp, s_ldt_package,
			function_name, arglist, timeout_ms, &result);

	if (rc != CITRUSLEAF_OK) {
		if (TRA_DEBUG)
			printf("[ERROR]:<%s:%s>:citrusleaf_udf_record_apply: Fail: RC(%d)",
					MOD, meth, rc);
		goto cleanup;
	}

	if (result.is_success) {
		if (TRA_DEBUG)
			printf("[DEBUG]:<%s:%s>:UDF Result SUCCESS\n", MOD, meth);
//		if (as_val_type(result.value) == AS_NIL) {
//			if (TRA_DEBUG)
//				printf("[ERROR]:<%s:%s> Result type NIL\n", MOD, meth);
//			rc = CITRUSLEAF_FAIL_CLIENT; // General Fail
//		} else {
//			char *valstr = as_val_tostring(result.value);
//			if (TRA_DEBUG)
//				printf("[DEBUG]:<%s:%s>: udf_return_type(%s)", MOD, meth,
//						valstr);
//			free(valstr);
//		}
	} else {
		if (TRA_DEBUG)
			printf("[DEBUG]:<%s:%s>:UDF Result FAIL\n", MOD, meth);
		rc = CITRUSLEAF_FAIL_CLIENT; // General Fail
	}

	cleanup:
	as_val_destroy(arglist);
	as_result_destroy(&result);
	// citrusleaf_object_free(o_keyp); // don't free caller's object.

	return rc;
} // end aerospike_lset_insert_internal()

/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Large Set Object Insert:
 *  For the given record (associated with 'o_keyp'), insert a value into
 *  the Large Set in the named Bin.
 *  Parms:
 *  (*) asc: The Aerospike Cluster
 *  (*) namespace: The Namespace for record holding the LDT bin
 *  (*) set: The Set for the record holding the LDT bin
 *  (*) o_keyp: Ptr to the Key Object (cl_object) that identifies this record
 *  (*) bin_name: Name of the new bin of type "LDT".
 *  (*) valuep: Ptr to the as_val instance that is the new lset value
 *  (*) timeout_ms: Timeout to wait (in MilliSecs). ZERO means forever.
 *
 * Return: a value of the type "cl_rv"
 *   Success:  CITRUSLEAF_OK: (0)
 *   Failure:  CITRUSLEAF_FAIL_CLIENT (-1);
 */
cl_rv aerospike_lset_insert(cl_cluster * asc, const char * namespace,
		const char * set, const cl_object * o_keyp, const char * bin_name,
		as_val * valuep, uint32_t timeout_ms)
{
	// Call the internal function that does the real work.
	cl_rv rc = aerospike_lset_insert_internal(asc, namespace, set, o_keyp,
			bin_name, valuep, NULL, timeout_ms);

	return rc;
} // end aerospike_lset_insert()

/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Large Set Object Create And Insert
 *  For the given record (associated with 'keyp'), insert a value into
 *  the large set in the named LDT Bin -- and do an implicit create of the
 *  LDT if it does not already exist.  Creation args are expected to be
 *  supplied (such as a package name).
 *  Parms:
 *  (*) asc: The Aerospike Cluster
 *  (*) namespace: The Namespace for record holding the LDT bin
 *  (*) set: The Set for the record holding the LDT bin
 *  (*) o_keyp: Ptr to the Key Object (cl_object) that identifies this record
 *  (*) bin_name: Name of the new bin of type "LDT".
 *  (*) valuep: Ptr to the as_val instance that is the new set value
 *  (*) creation_spec: A map (as_map) of creation settings (can be null)
 *  (*) timeout_ms: Timeout to wait (in MilliSecs). ZERO means forever.
 *
 * Return: a value of the type "cl_rv"
 *   Success:  CITRUSLEAF_OK: (0)
 *   Failure:  CITRUSLEAF_FAIL_CLIENT (-1);
 */
cl_rv aerospike_lset_create_and_insert(cl_cluster * asc, const char * namespace,
		const char * set, const cl_object * o_keyp, const char * bin_name,
		as_val * valuep, as_map * creation_spec, uint32_t timeout_ms)
{
	// Call the internal function that does the real work.
	cl_rv rc = aerospike_lset_insert_internal(asc, namespace, set, o_keyp,
			bin_name, valuep, creation_spec, timeout_ms);

	return rc;
} // end aerospike_lset_create_and_insert()

/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Large Set Search/Exists
 *  For the given record (associated with 'keyp'), locate the element
 *  associated with "searchValue".  In some cases, users may want to know
 *  only if the element exists.  In other cases, users may want to know
 *  the additional information that is associated with the SearchValue.
 *  The Large Set is named by NameSpace, Set, Key, LdtBinName)
 *
 *  Parms:
 *  (*) asc: The Aerospike Cluster
 *  (*) namespace: The Namespace for record holding the Large Set bin
 *  (*) set: The Set for the record holding the Large Set bin
 *  (*) key: The Key that identifies this record
 *  (*) bin_name: Name of the LDT Bin Name.
 *  (*) search_valuep: Ptr to the as_val that we're looking for
 *  (*) filter: Name of the inner UDF (null ok)
 *  (*) function_args: (as_list *) of the args to pass to the inner UDF(null ok)
 *  (*) exists: When true, just return (true=exists, false=not found)
 *              otherwise, return the value in as_result.
 *  (*) timeout_ms: Timeout to wait (in MilliSecs). ZERO means forever.
 *
 *  Return: 
 *  rc = 0:  Success.  Results are put into user's resultp parameter.
 *  rc < 0:  Error Case. (resultp is NULL)
 *
 *  Note: Caller's "as_result * resultp" will be filled in with a malloc'd
 *  structure that must be manually freed (as_result_destroy(resultp) by the
 *  caller after it is done using the result.
 *
 */
cl_rv aerospike_lset_search_internal(as_result ** resultpp, cl_cluster * asc,
		const char * namespace, const char * set, const cl_object * o_keyp,
		const char * bin_name, as_val * search_valuep, const char * filter,
		as_list * function_args, bool exists, uint32_t timeout_ms)
{
	static char * meth = "aerospike_lset_search_INT()";
	cl_rv rc = 0; // ubiquitous return code
	char * valstr = NULL;
	char * function_name;
	if (filter == NULL) {
		function_name = s_search;
	} else {
		function_name = s_search_filter;
	}

	// For Result, we are going to pass this back to the caller, so we
	// must FULLY DOCUMENT the fact that they must call "as_result_destroy()"
	// on the result after they are done with it.
	as_result * resultp = NULL;
	resultp = as_result_new();
	if (resultp == NULL) {
		// Big trouble.  Client Failure -- can't allocate a result object.
		// Ok to return here -- we can't do cleanup anyway.
		if (TRA_ERROR)
			printf("[ERROR]:<%s:%s>:as_result Alloc Fail:\n", MOD, meth);
		return CITRUSLEAF_FAIL_CLIENT;
	}
	*resultpp = resultp; // Return the result via parameter

	// Set up the arglist that we'll use to pass in function parms
	// As is now the case with all UDF resources, we initialize the
	// arglist, use it then destroy it.
	// NOTE: lset_valuep is an as_val object owned by the caller, so we
	// must bump the reference count so that both the caller's destroy
	// and the list destroy work properly.
	//
	// Lua Call: lset_search( record, binName, Value, filter, fargs )
	as_list * arglist = NULL;
	arglist = as_arraylist_new(4, 0); // two or four items to push
	as_list_add_string(arglist, bin_name);
	as_val_reserve( search_valuep);
	// Increment the reference count for valuep
	as_list_append(arglist, search_valuep);
	as_list_add_integer(arglist, exists);
	if (filter != NULL && function_args != NULL) {
		as_list_add_string(arglist, filter);
		as_val_reserve( function_args);
		// protect: Bump ref count
		as_list_append(arglist, (as_val *) function_args);
	}

	if (TRA_DEBUG) {
		printf("[DEBUG]:<%s:%s> UDF Apply:NS(%s) Set(%s) Bin(%s) \n",
                MOD, meth, namespace, set, bin_name);
		valstr = as_val_tostring(arglist);
		printf("[DEBUG]:<%s:%s> Package(%s) Func(%s) Args(%s) \n",
                MOD, meth, s_ldt_package, function_name, valstr);
		free(valstr);
	}

	rc = citrusleaf_udf_record_apply(asc, namespace, set, o_keyp, s_ldt_package,
			function_name, arglist, timeout_ms, resultp);

	// TODO: Need to distinquish between RECORD NOT FOUND and
	// LDT element not found.
	//
	//
	if (rc != CITRUSLEAF_OK) {
		if (TRA_DEBUG)
			printf("[ERROR]:<%s:%s>:citrusleaf_udf_record_apply: Fail: RC(%d)",
					MOD, meth, rc);
		goto cleanup;
	}

	if (resultp->is_success) {
		if (TRA_DEBUG)
			printf("[DEBUG]:<%s:%s>:UDF Result SUCCESS\n", MOD, meth);

// TODO: Remember to check on how we return NOT FOUND!!
// TODO: Remember to check on how we return NOT FOUND!!
// TODO: Remember to check on how we return NOT FOUND!!
//
		if (as_val_type(resultp->value) == AS_NIL) {
			if (TRA_ERROR)
				printf("[ERROR]:<%s:%s> Result type NIL\n", MOD, meth);
			rc = CITRUSLEAF_FAIL_NOTFOUND; // Not a bad error.
		} else {
			if (TRA_DEBUG) {
				valstr = as_val_tostring(resultp->value);
				printf("[DEBUG]:<%s:%s>:udf_return_val(%s)", MOD, meth, valstr);
				free(valstr);
			}
		}
	} else {
		if (TRA_DEBUG)
			printf("[DEBUG]:<%s:%s>:UDF Result FAIL\n", MOD, meth);
		rc = CITRUSLEAF_FAIL_CLIENT; // General Fail
	}

	cleanup:
	as_val_destroy(arglist);
	// NOTE: We do NOT destroy result: The caller has to do that

	if (rc != CITRUSLEAF_OK) {
		// Bad result, so contents of resultp are not reliable.
		// Note that this function needs to change so that the caller
		// passes in the resultp and we always return a return code.
		if (resultp != NULL)
			as_result_destroy(resultp);
		*resultpp = NULL;
	}
	if (TRA_EXIT)
		printf("[EXIT]:<%s:%s>:Search Result(%d)\n", MOD, meth, rc);
	return rc;
} // end aerospike_lset_search_internal()

/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Large Set Search: 
 *  Locate Set element(s).
 *  Search the set, applying the UN-transformation function (if it is
 *  defined at create time).
 *  Notice that there are only two cases for Sets:
 *  (*) Single Item result -- then apply filter.
 *  (*) ALL items result -- then apply filter.
 *
 *  Parms:
 (*) resultpp: The address of the user's (as_result *) variable.
 *  (*) asc: The Aerospike Cluster
 *  (*) namespace: The Namespace for record holding the LDT bin
 *  (*) set: The Set for the record holding the LDT bin
 *  (*) o_keyp: Ptr to the Key Object (cl_object) that identifies this record
 *  (*) bin_name: Name of the new bin of type LDT.
 *  (*) search_valuep: Ptr to the as_val that we're looking for.
 *  (*) timeout_ms: Timeout to wait (in MilliSecs). ZERO means forever.
 *
 *  Return: 
 *  rc = 0:  Success.  Results are put into user's resultp parameter.
 *  rc < 0:  Error Case. (resultp is NULL)
 *
 *  Note: Caller's "as_result * resultp" will be filled in with a malloc'd
 *  structure that must be manually freed (as_result_destroy(resultp) by the
 *  caller after it is done using the result.
 */
cl_rv aerospike_lset_search(as_result ** resultpp, cl_cluster * asc,
		const char * namespace, const char * set, const cl_object * o_keyp,
		const char * bin_name, as_val * search_valuep, uint32_t timeout_ms)
{
	// Call the internal function that does the real work.
	cl_rv rc = aerospike_lset_search_internal(resultpp, asc, namespace, set,
			o_keyp, bin_name, search_valuep, NULL, NULL, false, timeout_ms);

	return rc;
} // end aerospike_lset_search()

/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Large Set Search :: With Filter.
 *  Locate Set element(s), then  apply the filter (inner UDF).
 *  Search the set, applying the UN-transformation function (if it is
 *  defined at create time), then applying the filter (if present) to each
 *  qualifying item.  Notice that there are only two cases for Sets:
 *  (*) Single Item result -- then apply filter.
 *  (*) ALL items result -- then apply filter.
 *
 *  Parms:
 (*) resultpp: The address of the user's (as_result *) variable.
 *  (*) asc: The Aerospike Cluster
 *  (*) namespace: The Namespace for record holding the LDT bin
 *  (*) set: The Set for the record holding the LDT bin
 *  (*) o_keyp: Ptr to the Key Object (cl_object) that identifies this record
 *  (*) bin_name: Name of the new bin of type LDT.
 *  (*) search_valuep: Ptr to the as_val that we're looking for.
 *  (*) filter: Name of the inner UDF (null ok)
 *  (*) function_args: (as_list *) of the args to pass to the inner UDF(null ok)
 *  (*) timeout_ms: Timeout to wait (in MilliSecs). ZERO means forever.
 *
 *  Return: 
 *  rc = 0:  Success.  Results are put into user's resultp parameter.
 *  rc < 0:  Error Case. (resultp is NULL)
 *
 *  Note: Caller's "as_result * resultp" will be filled in with a malloc'd
 *  structure that must be manually freed (as_result_destroy(resultp) by the
 *  caller after it is done using the result.
 */
cl_rv aerospike_lset_search_then_filter(as_result ** resultpp, cl_cluster * asc,
		const char * namespace, const char * set, const cl_object * o_keyp,
		const char * bin_name, as_val * search_valuep, const char * filter,
		as_list * function_args, uint32_t timeout_ms) {
	// Call the internal function that does the real work.
	cl_rv rc = aerospike_lset_search_internal(resultpp, asc, namespace, set,
			o_keyp, bin_name, search_valuep, filter, function_args, false,
			timeout_ms);

	return rc;
} // end aerospike_lset_search_then_filter()

/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Large Set Delete
 *  For the given record (associated with 'keyp'), locate the search item
 *  (delete value) in the set and remove it.
 *
 *  Parms:
 *  (*) asc: The Aerospike Cluster
 *  (*) namespace: The Namespace for record holding the Large Set bin
 *  (*) set: The Set for the record holding the Large Set bin
 *  (*) key: The Key that identifies this record
 *  (*) bin_name: Name of the new bin of type "Large Set" (AS_MAP).
 *  (*) delete_valuep: the value to be removed from the set
 *
 *  Return: 
 *  0: success.   -1: Failure
 */
cl_rv aerospike_lset_delete(cl_cluster * asc, const char * namespace,
		const char * set, const cl_object * o_keyp, const char * bin_name,
		as_val * delete_valuep, uint32_t timeout_ms) {
	static char * meth = "aerospike_lset_delete()";
	int rc = 0; // ubiquitous return code
	char * valstr = NULL;

	// In this function, we are returning an int (not the result), so we
	// can use the "stack allocated" result (as_result_init).
	as_result result;
	as_result_init(&result);

	// Set up the arglist that we'll use to pass in function parms
	// As is now the case with all UDF resources, we initialize the
	// arglist, use it then destroy it.
	// NOTE: lset_valuep is an as_val object owned by the caller, so we
	// must bump the reference count so that both the caller's destroy
	// and the list destroy work properly.
	//
	// Lua Call: lset_delete( record, bin_name, value )
	as_list * arglist = NULL;
	arglist = as_arraylist_new(2, 0); // Two items to push
	as_list_add_string(arglist, bin_name);
	as_val_reserve( delete_valuep);
	// Increment the reference count for valuep
	as_list_append(arglist, delete_valuep);

	if (TRA_DEBUG) {
		printf("[DEBUG]:<%s:%s>Calling UDF:NS(%s) Set(%s) Bin(%s) \n", MOD,
				meth, namespace, set, bin_name);
		valstr = as_val_tostring(arglist);
		if (TRA_DEBUG)
			printf("[DEBUG]:<%s:%s> Package(%s) Func(%s) Args(%s) \n", MOD,
					meth, s_ldt_package, s_delete, valstr);
		free(valstr);
	}

	rc = citrusleaf_udf_record_apply(asc, namespace, set, o_keyp, s_ldt_package,
			s_delete, arglist, timeout_ms, &result);

	if (rc != CITRUSLEAF_OK) {
		if (TRA_ERROR)
			printf("[ERROR]:<%s:%s>:citrusleaf_udf_record_apply: Fail: RC(%d)",
					MOD, meth, rc);
		goto cleanup;
	}

	if (result.is_success) {
		if (TRA_DEBUG)
			printf("[DEBUG]:<%s:%s>:UDF Result SUCCESS\n", MOD, meth);
		if (as_val_type(result.value) == AS_NIL) {
			if (TRA_DEBUG)
				printf("[ERROR]:<%s:%s> Result type is NIL\n", MOD, meth);
			rc = CITRUSLEAF_FAIL_CLIENT; // General Fail
		} else {
			if (TRA_DEBUG) {
				valstr = as_val_tostring(result.value);
				printf("[DEBUG]:<%s:%s>:udf_return val(%s)", MOD, meth, valstr);
				free(valstr);
			}
		}
	} else {
		if (TRA_DEBUG)
			printf("[DEBUG]:<%s:%s>:UDF Result FAIL\n", MOD, meth);
		rc = CITRUSLEAF_FAIL_CLIENT; // General Fail
	}

	cleanup:
	as_val_destroy(arglist);
	as_result_destroy(&result);
	// citrusleaf_object_free(o_keyp); // don't free caller's object

	if (TRA_EXIT)
		printf("[EXIT]:<%s:%s>: RC(%d)\n", MOD, meth, rc);
	return rc;
} // end aerospike_lset_delete()

/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Large Set Size:
 *  For the given record (associated with 'keyp'), return the size in terms
 *  if number of elements in the set.
 *
 *  Parms:
 *  (*) size: The variable in which we return the size
 *  (*) asc: The Aerospike Cluster
 *  (*) namespace: The Namespace for record holding the LDT bin
 *  (*) set: The Set for the record holding the LDT bin
 *  (*) o_keyp: Ptr to the Key Object (cl_object) that identifies this record
 *  (*) bin_name: Name of the new bin of type LDT.
 *  (*) timeout_ms: Timeout to wait (in MilliSecs). ZERO means forever.
 *
 *  Success: 
 *  rc = 0; The size of the large set is returned in "size".
 *  rc < 0: Error Condition
 */
cl_rv aerospike_lset_size(uint32_t * size, cl_cluster * asc,
		const char * namespace, const char * set, const cl_object * o_keyp,
		const char * bin_name, uint32_t timeout_ms) {
	static char * meth = "aerospike_lset_size()";
	cl_rv rc; // ubiquitous return code
	uint32_t size_result = 0;
	char * valstr = NULL;

	if (TRA_ENTER)
		printf("[ENTER]<%s:%s>: NS(%s) Set(%s) Bin(%s)\n", MOD, meth, namespace,
				set, bin_name);

	// In this function, we are returning an int (not the result), so we
	// can use the "stack allocated" result (as_result_init).
	as_result result;
	as_result_init(&result);

	// Lua Call: lset_size( record, bin_name )
	as_list * arglist = NULL;
	arglist = as_arraylist_new(1, 0); // One item in the arglist
	as_list_add_string(arglist, bin_name);

	// Call the "apply udf" function (e.g. lset_size()) for this record to
	// return the size of the set.
	rc = citrusleaf_udf_record_apply(asc, namespace, set, o_keyp, s_ldt_package,
			s_size, NULL, timeout_ms, &result);

	if (rc != CITRUSLEAF_OK) {
		if (TRA_ERROR)
			printf("[ERROR]<%s:%s>:citrusleaf_udf_record_apply: Fail: RC(%d)",
					MOD, meth, rc);
		goto cleanup;
	}

	if (result.is_success) {
		if (TRA_DEBUG)
			printf("[DEBUG]<%s:%s>:UDF Result SUCCESS\n", MOD, meth);
		if (as_val_type(result.value) == AS_NIL) {
			if (TRA_ERROR)
				printf("[ERROR]<%s:%s> Result type is NIL\n", MOD, meth);
			rc = CITRUSLEAF_FAIL_CLIENT; // general client failure
		} else {
			if (TRA_DEBUG) {
				valstr = as_val_tostring(result.value);
				printf("[DEBUG]<%s:%s>udf_return_type(%s)", MOD, meth, valstr);
				free(valstr);
			}
			// NOTE: May have to check TYPE first and do a conversion
			// to a real int.
			as_val * val_sizep = as_result_value(&result);
			as_integer * int_sizep = as_integer_fromval(val_sizep);
			size_result = as_integer_toint(int_sizep); // should be an int.
			*size = (uint32_t) size_result;
		}
	} else {
		if (TRA_ERROR)
			printf("[ERROR]<%s:%s>:UDF Result FAIL\n", MOD, meth);
		rc = CITRUSLEAF_FAIL_CLIENT; // general client failure
	}

	cleanup:
	as_val_destroy(arglist);
	as_result_destroy(&result);

	if (TRA_EXIT)
		printf("[EXIT]<%s:%s>: RC(%d)\n", MOD, meth, rc);
	return rc;
} // end aerospike_lset_size()()

/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  Large Set Object config:
 *  For the given record (associated with 'keyp' and bin_name, return
 *  the size of the lset object.
 *
 *  Parms:
 *  (*) size: The variable in which we return the size
 *  (*) asc: The Aerospike Cluster
 *  (*) namespace: The Namespace for record holding the LDT bin
 *  (*) set: The Set for the record holding the LDT bin
 *  (*) o_keyp: Ptr to the Key Object (cl_object) that identifies this record
 *  (*) bin_name: Name of the new bin of type "LDT"
 *  (*) timeout_ms: Timeout to wait (in MilliSecs). ZERO means forever.
 *
 *  Success: 
 *  rc = 0; The as_map, holding the config, is return in * resultpp
 *  rc < 0: Error Condition
 */
cl_rv aerospike_lset_config(as_result ** resultpp, cl_cluster * asc,
		const char * namespace, const char * set, const cl_object * o_keyp,
		const char * bin_name, uint32_t timeout_ms) {
	static char * meth = "aerospike_lset_size()";
	cl_rv rc; // ubiquitous return code
	int size_result = 0;

	if (TRA_ENTER)
		printf("[ENTER]<%s:%s>: NS(%s) Set(%s) Bin(%s)\n", MOD, meth, namespace,
				set, bin_name);

	// For Result, we are going to pass this back to the caller, so we
	// must FULLY DOCUMENT the fact that they must call "as_result_destroy()"
	// on the result after they are done with it.
	as_result * resultp = NULL;
	resultp = as_result_new();
	if (resultp == NULL) {
		// Big trouble.  Client Failure -- can't allocate a result object.
		return (CITRUSLEAF_FAIL_CLIENT);
	}
	*resultpp = resultp; // This is the user's return value.
	rc = CITRUSLEAF_OK; // This is the user's return code.

	// Lua Call: lset_config( record, bin_name )
	as_list * arglist = NULL;
	arglist = as_arraylist_new(1, 0); // One item in the arglist
	as_list_add_string(arglist, bin_name);

	// Call the "apply udf" function (e.g. lset_config()) for this record to
	// return the size of the set.
	rc = citrusleaf_udf_record_apply(asc, namespace, set, o_keyp, s_ldt_package,
			s_config, NULL, timeout_ms, resultp);

	if (rc != CITRUSLEAF_OK) {
		if (TRA_ERROR)
			printf("[ERROR]<%s:%s>:citrusleaf_udf_record_apply: Fail: RC(%d)",
					MOD, meth, rc);
		goto cleanup;
	}

	cleanup:
	// We do NOT destroy result (in resultp): The caller has to do that.
	// However, if there were errors, then the contents of resultp are
	// undetermined, so we null it out when there are errors.

	if (TRA_EXIT)
		printf("[EXIT]<%s:%s>: RC(%d)\n", MOD, meth, rc);
	if (rc != CITRUSLEAF_OK) {
		// Bad result, so contents of resultp are not reliable.
		// Note that this function needs to change so that the caller
		// passes in the resultp and we always return a return code.
		if (resultp != NULL) {
			as_result_destroy(resultp);
		}
		*resultpp = NULL; // client gets NULL in her resultp var.
	}
	if (TRA_EXIT)
		printf("[EXIT]<%s:%s>: RC(%d)\n", MOD, meth, rc);
	return rc;
} // end aerospike_lset_size()()

// <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF> <EOF>
