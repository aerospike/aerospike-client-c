#include <aerospike/aerospike.h>
#include <aerospike/as_policy.h>

#include "../test.h"
#include "../../main/aerospike/_policy.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike * as;

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( policy_scan_init , "init" ) 
{
	as_policy_scan policy;
	as_policy_scan_init(&policy);

	assert_int_eq(policy.timeout, 0);
	assert_int_eq(policy.fail_on_cluster_change, AS_POLICY_BOOL_UNDEF);
}

TEST( policy_scan_resolve_1 , "resolve: global.scan (init)" )
{
	as_policies global;
	as_policies_init(&global);

	as_policy_scan resolved;

	// resolve values
	as_policy_scan_resolve(&resolved, &global, NULL);

	// check timeout
	assert_int_eq(resolved.timeout, global.timeout);
	assert_int_ne(resolved.timeout, global.scan.timeout);

	// check fail_on_cluster_change
	assert_int_eq(resolved.fail_on_cluster_change, true);
	assert_int_ne(resolved.fail_on_cluster_change, global.scan.fail_on_cluster_change);
}

TEST( policy_scan_resolve_2 , "resolve: global.scan.timeout=10, global.scan.fail_on_cluster_change=false" )
{
	as_policies global;
	as_policies_init(&global);

	global.scan.timeout = 10;
	global.scan.fail_on_cluster_change = false;

	as_policy_scan resolved;

	// resolve values
	as_policy_scan_resolve(&resolved, &global, NULL);

	// check timeout
	assert_int_eq(resolved.timeout, global.scan.timeout);
	assert_int_ne(resolved.timeout, global.timeout);

	// check fail_on_cluster_change
	assert_int_eq(resolved.fail_on_cluster_change, false);
	assert_int_eq(resolved.fail_on_cluster_change, global.scan.fail_on_cluster_change);
}

TEST( policy_scan_resolve_3 , "resolve: local.timeout=10, local.fail_on_cluster_change=false" )
{
	as_policies global;
	as_policies_init(&global);

	as_policy_scan local;
	as_policy_scan_init(&local);

	local.timeout = 10;
	local.fail_on_cluster_change = false;

	as_policy_scan resolved;

	// resolve values
	as_policy_scan_resolve(&resolved, &global, &local);

	// check timeout
	assert_int_eq(resolved.timeout, local.timeout);
	assert_int_ne(resolved.timeout, global.timeout);
	assert_int_ne(resolved.timeout, global.scan.timeout);

	// check fail_on_cluster_change
	assert_int_eq(resolved.fail_on_cluster_change, local.fail_on_cluster_change);
	assert_int_ne(resolved.fail_on_cluster_change, global.scan.fail_on_cluster_change);
}

TEST( policy_scan_resolve_4 , "resolve: global.scan.timeout=100, global.scan.fail_on_cluster_change=true, local.timeout=10, local.fail_on_cluster_change=false" )
{
	as_policies global;
	as_policies_init(&global);

	global.scan.timeout = 100;
	global.scan.fail_on_cluster_change = true;

	as_policy_scan local;
	as_policy_scan_init(&local);

	local.timeout = 10;
	local.fail_on_cluster_change = false;

	as_policy_scan resolved;

	// resolve values
	as_policy_scan_resolve(&resolved, &global, &local);

	// check timeout
	assert_int_eq(resolved.timeout, local.timeout);
	assert_int_ne(resolved.timeout, global.timeout);
	assert_int_ne(resolved.timeout, global.scan.timeout);

	// check fail_on_cluster_change
	assert_int_eq(resolved.fail_on_cluster_change, local.fail_on_cluster_change);
	assert_int_ne(resolved.fail_on_cluster_change, global.scan.fail_on_cluster_change);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( policy_scan, "as_policy_scan tests" )
{
	suite_add( policy_scan_init );
	suite_add( policy_scan_resolve_1 );
	suite_add( policy_scan_resolve_2 );
	suite_add( policy_scan_resolve_3 );
	suite_add( policy_scan_resolve_4 );
}
