
#include <aerospike/as_config.h>
#include <aerospike/as_policy.h>
#include <aerospike/mod_lua_config.h>

#include <stdbool.h>
#include <stdint.h>

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define MOD_LUA_CACHE_ENABLED false

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_config * as_config_init(as_config * c) 
{
	c->tender_interval = 1000;
	as_policies_init(&c->policies);
	memset(c->hosts, 0, sizeof(c->hosts));
	c->lua.cache_enabled = MOD_LUA_CACHE_ENABLED;
	strcpy(c->lua.system_path, AS_CONFIG_LUA_SYSTEM_PATH);
	strcpy(c->lua.user_path, AS_CONFIG_LUA_USER_PATH);
	return c;
}
