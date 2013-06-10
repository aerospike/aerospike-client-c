
#include <aerospike/as_config.h>
#include <aerospike/as_policy.h>
#include <aerospike/mod_lua_config.h>

#include <stdbool.h>
#include <stdint.h>

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define MOD_LUA_CACHE_ENABLED true
#define MOD_LUA_SYSTEM_PATH "/opt/citrusleaf/sys/udf/lua"
#define MOD_LUA_USER_PATH "/opt/citrusleaf/usr/udf/lua"

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_config * as_config_init(as_config * c) 
{
	c->nonblocking = true;
	c->tend_frequency = 1000;
	as_policies_init(&c->policies);
	c->logger = NULL;
	memset(c->hosts, 0, sizeof(c->hosts));
	c->mod_lua.cache_enabled = MOD_LUA_CACHE_ENABLED;
	strcpy(c->mod_lua.system_path, MOD_LUA_SYSTEM_PATH);
	strcpy(c->mod_lua.user_path, MOD_LUA_USER_PATH);
	return c;
}