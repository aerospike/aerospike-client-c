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

TEST( policy_read_init , "init" )
{
	as_policy_read policy;
	as_policy_read_init(&policy);

	assert_int_eq(policy.timeout, 1000);
	assert_int_eq(policy.key, AS_POLICY_KEY_DIGEST);
}

TEST( policy_read_resolve_1 , "resolve: global.read (init)" )
{
	as_policies global;
	as_policies_init(&global);
	as_policies_resolve(&global);
	
	as_policy_read resolved;
	as_policy_read_copy(&global.read, &resolved);
	
	// check timeout
	assert_int_eq(resolved.timeout, global.timeout);
	assert_int_eq(resolved.timeout, global.read.timeout);
}

TEST( policy_read_resolve_2 , "resolve: global.read.timeout=10, global.read.key=AS_POLICY_KEY_SEND" )
{
	as_policies global;
	as_policies_init(&global);
	
	global.read.timeout = 10;
	global.read.key = AS_POLICY_KEY_SEND;
	
	as_policies_resolve(&global);

	as_policy_read resolved;
	as_policy_read_copy(&global.read, &resolved);

	// check timeout
	assert_int_eq(resolved.timeout, global.read.timeout);
	assert_int_ne(resolved.timeout, global.timeout);
	
	// check key
	assert_int_eq(resolved.key, global.read.key);
}

TEST( policy_read_resolve_3 , "resolve: local.timeout=10, local.key=AS_POLICY_KEY_SEND" )
{
	as_policies global;
	as_policies_init(&global);
	as_policies_resolve(&global);
	
	as_policy_read local;
	as_policy_read_init(&local);
	
	local.timeout = 10;
	local.key = AS_POLICY_KEY_SEND;
		
	// check timeout
	assert_int_ne(local.timeout, global.read.timeout);
	
	// check key
	assert_int_ne(local.key, global.read.key);
}

TEST( policy_read_resolve_4 , "resolve: global.read.timeout=100, global.read.key=AS_POLICY_KEY_SEND, local.timeout=10, local.key=AS_POLICY_KEY_DIGEST" )
{
	as_policies global;
	as_policies_init(&global);
	
	global.read.timeout = 100;
	global.read.key = AS_POLICY_KEY_SEND;
	
	as_policies_resolve(&global);

	as_policy_read local;
	as_policy_read_init(&local);
	
	local.timeout = 10;
	local.key = AS_POLICY_KEY_DIGEST;
		
	// check timeout
	assert_int_ne(local.timeout, global.timeout);
	assert_int_ne(local.timeout, global.read.timeout);
	
	// check key
	assert_int_ne(local.key, global.read.key);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( policy_read, "as_policy_read tests" )
{
	suite_add( policy_read_init );
	suite_add( policy_read_resolve_1 );
	suite_add( policy_read_resolve_2 );
	suite_add( policy_read_resolve_3 );
	suite_add( policy_read_resolve_4 );
}
