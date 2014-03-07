
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

TEST( policy_read_init , "init" )
{
	as_policy_read policy;
	as_policy_read_init(&policy);

	assert_int_eq(policy.timeout, 0);
	assert_int_eq(policy.key, AS_POLICY_KEY_UNDEF);
}

TEST( policy_read_resolve_1 , "resolve: global.read (init)" )
{
	as_policies global;
	as_policies_init(&global);

	as_policy_read resolved;

	// resolve values
	as_policy_read_resolve(&resolved, &global, NULL);

	// check timeout
	assert_int_eq(resolved.timeout, global.timeout);
	assert_int_ne(resolved.timeout, global.read.timeout);
}

TEST( policy_read_resolve_2 , "resolve: global.read.timeout=10, global.read.key=AS_POLICY_KEY_STORE" )
{
	as_policies global;
	as_policies_init(&global);

	global.read.timeout = 10;
	global.read.key = AS_POLICY_KEY_STORE;

	as_policy_read resolved;

	// resolve values
	as_policy_read_resolve(&resolved, &global, NULL);

	// check timeout
	assert_int_eq(resolved.timeout, global.read.timeout);
	assert_int_ne(resolved.timeout, global.timeout);

	// check key
	assert_int_eq(resolved.key, global.read.key);
}

TEST( policy_read_resolve_3 , "resolve: local.timeout=10, local.key=AS_POLICY_KEY_STORE" )
{
	as_policies global;
	as_policies_init(&global);

	as_policy_read local;
	as_policy_read_init(&local);

	local.timeout = 10;
	local.key = AS_POLICY_KEY_STORE;

	as_policy_read resolved;

	// resolve values
	as_policy_read_resolve(&resolved, &global, &local);

	// check timeout
	assert_int_eq(resolved.timeout, local.timeout);
	assert_int_ne(resolved.timeout, global.timeout);
	assert_int_ne(resolved.timeout, global.read.timeout);
	
	// check key
	assert_int_eq(resolved.key, local.key);
	assert_int_ne(resolved.key, global.read.key);
}

TEST( policy_read_resolve_4 , "resolve: global.read.timeout=100, global.read.key=AS_POLICY_KEY_SEND, local.timeout=10, local.key=AS_POLICY_KEY_STORE" )
{
	as_policies global;
	as_policies_init(&global);

	global.read.timeout = 100;
	global.read.key = AS_POLICY_KEY_SEND;

	as_policy_read local;
	as_policy_read_init(&local);

	local.timeout = 10;
	local.key = AS_POLICY_KEY_STORE;

	as_policy_read resolved;

	// resolve values
	as_policy_read_resolve(&resolved, &global, &local);

	// check timeout
	assert_int_eq(resolved.timeout, local.timeout);
	assert_int_ne(resolved.timeout, global.timeout);
	assert_int_ne(resolved.timeout, global.read.timeout);
	
	// check key
	assert_int_eq(resolved.key, local.key);
	assert_int_ne(resolved.key, global.read.key);
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
