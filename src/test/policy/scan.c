/*
 * Copyright 2008-2016 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
#include <aerospike/aerospike.h>
#include <aerospike/as_policy.h>

#include "../test.h"

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
	assert_int_eq(policy.fail_on_cluster_change, false);
}

TEST( policy_scan_resolve_1 , "resolve: global.scan (init)" )
{
	as_policies global;
	as_policies_init(&global);
	as_policies_resolve(&global);
	
	as_policy_scan resolved;
	as_policy_scan_copy(&global.scan, &resolved);
		
	// check timeout
	assert_int_ne(resolved.timeout, global.timeout);
	assert_int_eq(resolved.timeout, global.scan.timeout);
	
	// check fail_on_cluster_change
	assert_int_eq(resolved.fail_on_cluster_change, false);
	assert_int_eq(resolved.fail_on_cluster_change, global.scan.fail_on_cluster_change);
}

TEST( policy_scan_resolve_2 , "resolve: global.scan.timeout=10, global.scan.fail_on_cluster_change=false" )
{
	as_policies global;
	as_policies_init(&global);
	
	global.scan.timeout = 10;
	global.scan.fail_on_cluster_change = false;
	
	as_policies_resolve(&global);

	as_policy_scan resolved;
	as_policy_scan_copy(&global.scan, &resolved);
	
	// check timeout
	assert_int_eq(resolved.timeout, global.scan.timeout);
	
	// check fail_on_cluster_change
	assert_int_eq(resolved.fail_on_cluster_change, false);
	assert_int_eq(resolved.fail_on_cluster_change, global.scan.fail_on_cluster_change);
}

TEST( policy_scan_resolve_3 , "resolve: local.timeout=10, local.fail_on_cluster_change=false" )
{
	as_policies global;
	as_policies_init(&global);
	as_policies_resolve(&global);
	
	as_policy_scan local;
	as_policy_scan_init(&local);
	
	local.timeout = 10;
	local.fail_on_cluster_change = false;
		
	// check timeout
	assert_int_ne(local.timeout, global.scan.timeout);
	
	// check fail_on_cluster_change
	assert_int_eq(local.fail_on_cluster_change, global.scan.fail_on_cluster_change);
}

TEST( policy_scan_resolve_4 , "resolve: global.scan.timeout=100, global.scan.fail_on_cluster_change=true, local.timeout=10, local.fail_on_cluster_change=false" )
{
	as_policies global;
	as_policies_init(&global);
	
	global.scan.timeout = 100;
	global.scan.fail_on_cluster_change = true;
	
	as_policies_resolve(&global);

	as_policy_scan local;
	as_policy_scan_init(&local);
	
	local.timeout = 10;
	local.fail_on_cluster_change = false;
		
	// check timeout
	assert_int_ne(local.timeout, global.timeout);
	assert_int_ne(local.timeout, global.scan.timeout);
	
	// check fail_on_cluster_change
	assert_int_ne(local.fail_on_cluster_change, global.scan.fail_on_cluster_change);
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
