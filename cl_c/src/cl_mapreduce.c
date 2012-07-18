/*
 * A good, basic C client for the Aerospike protocol
 * Creates a library which is linkable into a variety of systems
 *
 * First attempt is a very simple non-threaded blocking interface
 * currently coded to C99 - in our tree, GCC 4.2 and 4.3 are used
 *
 * Citrusleaf Inc.
 * All rights reserved
 */

#include <sys/types.h>
#include <sys/socket.h> // socket calls
#include <stdio.h>
#include <ctype.h>
#include <errno.h> //errno
#include <stdlib.h> //fprintf
#include <unistd.h> // close
#include <string.h>
#include <pthread.h>
#include <fcntl.h>
#include <arpa/inet.h> // inet_ntop
#include <signal.h>
#include <assert.h>

#include <netdb.h> //gethostbyname_r

#include "citrusleaf/citrusleaf.h"
#include "citrusleaf/citrusleaf-internal.h"
#include "citrusleaf/proto.h"
#include "citrusleaf/cf_rchash.h"
#include "citrusleaf/cf_alloc.h"
#include "citrusleaf/cf_b64.h"

//ALCHEMY
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>


// Key is a string
// Value is a pointer to mr_package

static cf_rchash *mr_package_hash = 0;

//
// map reduce structures and functions
//

typedef struct mr_package_s {
	
	char package_name[MAX_PACKAGE_NAME_SIZE];
	
	// "func" is the code, "name" is the symbol to invoke
	// generation is the server-returned value that equals this code's version
	char lang[MAX_PACKAGE_NAME_SIZE];
	char generation[MAX_PACKAGE_NAME_SIZE];
    char      *script; int script_len; 
    pthread_mutex_t script_lock;
    
    // Queue of mr_state pointers, anything in this queue  will have the above functions loaded
	cf_queue	*mr_state_q;

} mr_package;	


// forward define
void mr_package_release(mr_package *mrp_p);

//
// helpers
//
#define ISBLANK(c)           (c == 32 || c == 9)
#define SKIP_SPACES(tok)     while (ISBLANK(*tok)) tok++;


static char luaPredefinedFunctions[] = \
    "function AddTableToMapResults(hasrdc, k, v) " 				\
    "  local cmd; " 											\
    "  if (hasrdc) then " 										\
    "    cmd = 'MapResults[' .. k .. '] = ' .. v .. ';'; " 		\
    "  else " 													\
    "    cmd = 'table.insert(ReduceResults, ' .. v .. ');'; " 	\
    "    ReduceCount      = ReduceCount + 1; " 					\
    "  end " 													\
    "  assert(loadstring(cmd))() " 								\
    "end " 														\
    "function AddStringToMapResults(hasrdc, k, v) " 			\
    "  if (hasrdc) then " 										\
    "    MapResults[k] = v; " 									\
    "  else " 													\
    "    ReduceResults[k] = v; " 								\
    "  end " 													\
    "end "														\
    "local function GlobalCheck(tab, name, value) " 					\
    "  if (ReadOnly[name] == nil) then " 								\
    "    error(name ..' is a Global Variable, use \\'Sandbox\\'', 2); " \
    "  else " 															\
    "    rawset(tab, name, value) " 									\
    "  end " 															\
    "end " 																\
    "function PostFinalizeCleanup() " 									\
    "  setmetatable(_G, {}); " 											\
    "  ReadOnly = { " 													\
    "     MapResults    = {}; " 										\
    "     MapCount      =  0; " 										\
    "     ReduceResults = {}; " 										\
    "     ReduceCount   =  0; " 										\
    "     Sandbox       = {}; " 										\
    "  }; " 															\
    "  setmetatable(_G, {__index=ReadOnly, __newindex=GlobalCheck});  " \
    "end "																\
    "function ReduceWrapper(func) "  						\
    "  ReduceCount = 0; " 									\
    "  local res   = {}; " 									\
    "  for k, t in pairs(MapResults) do " 					\
    "    ReduceResults[k] = func(t); "						\
    "    ReduceCount = ReduceCount + 1; "					\
    "  end " 												\
    "  MapResults = {}; "									\
    "  MapCount   = 0; "				 					\
    "end "													\
    "function FinalizeWrapper(func) "						\
    "  ReduceResults = func(ReduceResults); "				\
    "  ReduceCount = 0; "					  				\
    "  for i, v in pairs(ReduceResults) do "				\
    "    ReduceCount = ReduceCount + 1; "					\
    "  end "												\
    "end ";

static char luaDebugWrapper[] = \
    "function DebugWrapper(func) "					\
    "  print('DebugWrapper'); "          			\
    "  for k, t in pairs(ReduceResults) do "        \
    "    func(k, t); "          					\
    "  end "          								\
    "end "          								\
    "function print_user_and_value(k, t) "          \
    "  print('v: ' .. t); "  						\
    "end ";

    //"  print('user: ' .. t.user_id .. ' score: ' .. t.score .. ' cats: ' .. t.cats); " 

static void assertOnLuaError(lua_State *lua, char *assert_string) {
    const char *lerr = lua_tostring(lua, -1);
    printf("lerr: %s assert: %s\n", lerr, assert_string);
    assert(!assert_string);
}

//
// Creates a new lua functions, and loads the predefined 
// This loads into the lua state table the DEFINEd code above,
// which only has to be done when the load 
//

int luaSendReduceObject(lua_State *lua);

static int 
mr_state_lua_create(cl_mr_state *mrs_p) {
	
	if (mrs_p->lua) {
		// fprintf(stderr, "closing lua %p\n",mrs_p->lua);
		lua_close(mrs_p->lua); mrs_p->lua = 0; 
	}
	
    mrs_p->lua       = lua_open();
    if (!mrs_p->lua) return(-1);
    lua_State *lua  = mrs_p->lua;
    luaL_openlibs(lua);

    int ret = luaL_dostring(lua, luaPredefinedFunctions);
//    int ret = luaL_loadbuffer(lua, luaPredefinedFunctions, sizeof(luaPredefinedFunctions)-1, "mr_wrapper");
    if (ret) {
    	assertOnLuaError(lua, "ERROR: adding(luaPredefinedFunctions)");
    	goto Cleanup;
    }
    
    // register C functions
    lua_pushcfunction(lua, luaSendReduceObject);
    lua_setglobal    (lua, "SendReduceObject");
    
    ret     = luaL_dostring(lua, luaDebugWrapper);
    if (ret) {
    	assertOnLuaError(lua, "ERROR: luaL_dostring(DebugWrapper)");
    	goto Cleanup; 
    }

	// fprintf(stderr, "lua state create success\n");    
    return(0);
    
Cleanup:
	fprintf(stderr, "lua state create failed\n");
	if (mrs_p->lua)  lua_close(mrs_p->lua);
	return(-1);
}

//
// Load in the dynamic functions necessary for this map reduce job
//

static int mr_state_load_package_lua(cl_mr_state *mrs_p, mr_package *mrp_p) {
	
    lua_State *lua  = mrs_p->lua;

    if (mrp_p->script) {
		int ret     = luaL_dostring(lua, mrp_p->script);
//		int ret     = luaL_loadbuffer(lua, mrp_p->script, mrp_p->script_len,mrp_p->package_name);
		if (ret) { assertOnLuaError(lua, "ERROR: luaL_dostring(map_func)"); return(-1); }
	}
	else {
		fprintf(stderr, "attempting to run package without registered script, failure\n");
		return(-1);
	}

    return(0);
}

void mr_state_destroy(cl_mr_state *mrs_p)
{
	if (mrs_p->lua) lua_close(mrs_p->lua);
	free(mrs_p);	
}

// input: an mrs_pd and the functions to register
// allocates copies the functions into the mrs_pd, creates the LUA universe, and loads all functions

cl_mr_state * mr_state_create(mr_package *mrp_p) {

	// some validation of the package - it should have code & generation
	if (mrp_p->generation[0] == 0) {
		fprintf(stderr, "creating mr_state from mr_package with no generation, illegal %s\n",mrp_p->generation);		
		return(0);
	}
	if (mrp_p->script == 0) {
		fprintf(stderr, "creating mr_state from mr_package with no code, illegal %s\n",mrp_p->package_name);
		return(0);
	}

	cl_mr_state *mrs_p = calloc(sizeof(cl_mr_state),1);
	if (!mrs_p) return(0);
		
	pthread_mutex_init(&mrs_p->lua_lock, 0);

	// create the lua universe and load in static funcs
    if (0 != mr_state_lua_create(mrs_p) ) {
    	mr_state_destroy(mrs_p);
    	return(0);
	}
    
    // load in the dynamic code, this can actually fail if the Lua
    // registered is bad, take a copy of the functions
    pthread_mutex_lock(&mrp_p->script_lock);

    if (0 != mr_state_load_package_lua(mrs_p, mrp_p) ) {
    	pthread_mutex_unlock(&mrp_p->script_lock);
    	mr_state_destroy(mrs_p);
    	return(0);
    }
    
    strncpy(mrs_p->package_name, mrp_p->package_name, MAX_PACKAGE_NAME_SIZE);
    strncpy(mrs_p->generation, mrp_p->generation, MAX_PACKAGE_NAME_SIZE);
    
    pthread_mutex_unlock(&mrp_p->script_lock);
    
    return(mrs_p);
}


cl_mr_state * 
cl_mr_state_get(const cl_mr_job *mrj) {

	// get the package with this name
	mr_package *mrp_p = 0;
	cf_rchash_get(mr_package_hash,mrj->package,strlen(mrj->package),(void **)&mrp_p);
	
	if (! mrp_p) {
		fprintf(stderr, "package %s has not been registered locally\n",mrj->package);
		return(0);
	}
	
	// try to pop a cached state
	cl_mr_state	*mrs_p = 0;
	
	int rv = cf_queue_pop(mrp_p->mr_state_q , (void *)&mrs_p, 0/*nowait*/);
	if (rv != CF_QUEUE_OK) {
		mrs_p = mr_state_create(mrp_p);
		if (!mrs_p) {
			fprintf(stderr, "could not create new state from package %s\n",mrj->package);
			mr_package_release(mrp_p);
			return(0);
		}
	}
	
	mr_package_release(mrp_p);
	
	mrs_p->mr_job = mrj;
	
	return(mrs_p);
}

void 
cl_mr_state_put(cl_mr_state *mrs_p) {
	
	mrs_p->mr_job = 0;
	
	// get the package with this name
	mr_package *mrp_p = 0;
	cf_rchash_get(mr_package_hash,mrs_p->package_name,strlen(mrs_p->package_name),(void **)&mrp_p);
	
	if (! mrp_p) {
		fprintf(stderr, "package %s has not been registered locally\n",mrs_p->package_name);
		mr_state_destroy(mrs_p);
		return;
	}
	
	// push the state
	fprintf(stderr, "pushing state %p onto queue, package %s ( %p )\n",mrs_p,mrs_p->package_name,mrp_p);
	int rv = cf_queue_push(mrp_p->mr_state_q , (void *)&mrs_p);
	if (rv != CF_QUEUE_OK) {
		// could not push for some reason, destroy I guess
		mr_state_destroy(mrs_p);
	}
	
	mr_package_release(mrp_p);
	
}


void mr_package_destroy(void *arg)
{
	mr_package *mrp_p = (mr_package *) arg;
	if (0 == cf_client_rc_release(mrp_p)) {
		
		fprintf(stderr, "mr_package_destroy: free %p\n",mrp_p);

		if (mrp_p->script)	free(mrp_p->script);

		cl_mr_state *mrs_p;
    	while (CF_QUEUE_OK == cf_queue_pop(mrp_p->mr_state_q, &mrs_p,0/*nowait*/)) {
    		mr_state_destroy(mrs_p);
    	}
		
		cf_client_rc_free(mrp_p);
	}
//	else {
//		fprintf(stderr, "mr_package_destroy: %p still a refcount\n",mrp_p);
//	}
	return;
}

void mr_package_release(mr_package *mrp_p) {
//	fprintf(stderr, "mr_package_release %p\n",mrp_p); 
	mr_package_destroy((void *)mrp_p);
}

//
// Todo: check if package exists on server, if so, load from there, if not, load to there
// char *script - must be malloc'ed, will be consumed / registered - must be null terminated
// script will be freed after this call

mr_package * mr_package_create(const char *package_name,  const char *lang, 
								char *script, int script_len, const char *generation )
{
	mr_package *mrp_p = 0;
	bool reusing = false;

	// already registered?
	int found = cf_rchash_get(mr_package_hash, (char *)package_name, strlen(package_name), (void **) &mrp_p);
	if (found == CF_RCHASH_OK) {
		reusing = true;
		pthread_mutex_lock(&mrp_p->script_lock);
		// bail if it hasn't changed
		if (0 == strcmp(generation, mrp_p->generation)) {
			free(script);
			pthread_mutex_unlock(&mrp_p->script_lock);
			return(mrp_p);
		}
		if (mrp_p->script)	{ free(mrp_p->script); mrp_p->script = 0; }
		reusing = true;
	}
	else {
		mrp_p = cf_client_rc_alloc(sizeof(mr_package));
		if (!mrp_p) goto Cleanup;
		memset(mrp_p, 0, sizeof(mr_package));

		strncpy(mrp_p->package_name, strdup(package_name), MAX_PACKAGE_NAME_SIZE);
		pthread_mutex_init(&mrp_p->script_lock, 0/*default addr*/);
		mrp_p->mr_state_q = cf_queue_create(sizeof(cl_mr_state *), true/*multithreaded*/);
	}
	
	mrp_p->script = script;
	mrp_p->script_len = script_len;
	
	strcpy(mrp_p->lang, lang);
	strcpy(mrp_p->generation, generation);
    
    if (reusing) {
    	pthread_mutex_unlock(&mrp_p->script_lock);
    	
    	// clean queue/cache
    	cl_mr_state *mrs_p = 0;
    	while (CF_QUEUE_OK == cf_queue_pop(mrp_p->mr_state_q, &mrs_p,0/*nowait*/))
    		mr_state_destroy(mrs_p);
    	
    }
    else {
    	// add to the rchash
    	cf_client_rc_reserve(mrp_p);
    	cf_rchash_put_unique(mr_package_hash, (char *)package_name, strlen(package_name), mrp_p);
	}
	return(mrp_p);
	
Cleanup:
	if (!mrp_p)	return(0);
	if (mrp_p->script)	{ free(mrp_p->script); mrp_p->script = 0; }
	return(0);
}


//
// grab the package from a server
// Not sure whether to do sync or async. Start with sync.
int
citrusleaf_sproc_package_get(cl_cluster *asc, const char *package_name, cl_script_lang_t lang_t)
{
//	fprintf(stderr, "citrusleaf mr package get %s\n",package_name);
	
	if (lang_t!=CL_SCRIPT_LANG_LUA) {
		fprintf(stderr, "unrecognized script language %d\n",lang_t);
		return(-1);
	}	
	const char lang = "lua";
	
	char info_query[512];
	if (sizeof(info_query) <= (size_t) snprintf(info_query, sizeof(info_query), "get-package:package=%s;lang=%s;",package_name,lang)) {
		return(-1);
	}
	char *values = 0;
	// shouldn't do this on a blocking thread --- todo, queue
	if (0 != citrusleaf_info_cluster(asc, info_query, &values, true/*asis*/, 100/*timeout*/)) {
		fprintf(stderr, "could not get package %s from cluster\n",package_name);
		return(-1);
	}
	if (0 == values) {
		fprintf(stderr, "info cluster success, but no package %s on server\n",package_name);
		return(-1);
	}
	
	// got response, add into cache
	// format: request\tresponse
	// response is gen=asdf;script=xxyefu
	// where gen is a simple string, and script
	// error is something else entirely
	
	char *value = strchr(values, '\t') + 1; // skip request, parse response 
	
	int n_tok=0;
	char *brkb = 0;
	char *words[20];
	do {
		words[n_tok] = strtok_r(value,"=",&brkb);
		if (0 == words[n_tok]) break;
		value = 0;
		words[n_tok+1] = strtok_r(value,";",&brkb);
		if (0 == words[n_tok+1]) break;
		char *newline = strchr(words[n_tok+1],'\n');
		if (newline) *newline = 0;
		n_tok += 2;
		if (n_tok >= 20) {
			fprintf(stderr, "too many tokens\n");
			free(values);
			return(-1);
		}
	} while(true);
	
	char *gen_str = 0;
	char *script64_str = 0;
	
	for (int i = 0; i < n_tok ; i += 2) {
		char *key = words[i];
		char *value = words[i+1];
		if (0 == strcmp(key,"gen")) {
			gen_str = value;
		}
		else if (0 == strcmp(key,"script")) {
			script64_str = value;
		} else {
			fprintf(stderr, "package get: unknown key %s value %s\n",key,value);
		}
	}
	if ( (!gen_str) || (!script64_str)) {
		fprintf(stderr, "get package did not return enough data\n");
		free(values);
		return(-1);
	}
	
	// unbase64
	int script_str_len = strlen(script64_str);
	char *script_str = malloc(script_str_len+1); // guarenteed to shrink it
	if (!script_str) {
		free(values);
		return(-1);
	}
	int rv = cf_base64_decode(script64_str, script_str, &script_str_len, true/*validate*/);
	if (rv != 0) {
		fprintf(stderr,"could not decode base64 from server %s\n",script64_str);
		free(script_str);
		free(values);
		return(-1);
	}
	script_str[script_str_len] = 0;
		
	mr_package *mrp_p = mr_package_create(package_name, lang, script_str, script_str_len, gen_str );
	if (!mrp_p) {
		fprintf(stderr, "could not create package: %s\n",package_name);
		free(values);
		return(-1);
	}
	script_str = 0;
	
	mr_package_release(mrp_p);

	free(values);
	
	return(0);
	
}

int
citrusleaf_sproc_package_set(cl_cluster *asc, const char *package_name, const char *script_str, cl_script_lang_t lang_t)
{	
	if (lang_t!=CL_SCRIPT_LANG_LUA) {
		fprintf(stderr, "unrecognized script language %d\n",lang_t);
		return(-1);
	}	
	const char lang[] = "lua";

	if (!package_name || !script_str) {
		fprintf(stderr, "package name and script required\n");
		return CITRUSLEAF_FAIL_CLIENT;
	}
	int script_str_len = strlen(script_str);
	
	int  info_query_len = cf_base64_encode_maxlen(script_str_len)+strlen(package_name)+strlen(lang)+100;
	char *info_query = malloc(info_query_len);	
	if (!info_query) {
		fprintf(stderr, "cannot malloc\n");
		return CITRUSLEAF_FAIL_CLIENT;
	}

	// put in the default stuff first
	snprintf(info_query, info_query_len, "set-package:package=%s;lang=%s;script=",package_name,lang);

	cf_base64_tostring((uint8_t *)script_str, (uint8_t *)(info_query+strlen(info_query)), &script_str_len);
		
	//fprintf(stderr, "**[%s]\n",info_query);

	char *values = 0;

	// shouldn't do this on a blocking thread --- todo, queue
	if (0 != citrusleaf_info_cluster_all(asc, info_query, &values, true/*asis*/, 5000/*timeout*/)) {
		fprintf(stderr, "could not set package %s from cluster\n",package_name);
		return CITRUSLEAF_FAIL_UNKNOWN;
	}
	if (0 == values) {
		fprintf(stderr, "info cluster success, but no response from server\n");
		return CITRUSLEAF_FAIL_UNKNOWN;
	}
	
	// got response, 
	// format: request\tresponse
	// response is a string "ok" or ???
	
	char *value = strchr(values, '\t') + 1; // skip request, parse response 
	
	int n_tok=0;
	char *brkb = 0;
	char *words[20];
	do {
		words[n_tok] = strtok_r(value,"=",&brkb);
		if (0 == words[n_tok]) break;
		value = 0;
		words[n_tok+1] = strtok_r(value,";",&brkb);
		if (0 == words[n_tok+1]) break;
		char *newline = strchr(words[n_tok+1],'\n');
		if (newline) *newline = 0;
		n_tok += 2;
		if (n_tok >= 20) {
			fprintf(stderr, "too many tokens\n");
			return CITRUSLEAF_FAIL_UNKNOWN;
		}
	} while(true);
	
	char *err_str = 0;
	
	for (int i = 0; i < n_tok ; i += 2) {
		char *key = words[i];
		char *value = words[i+1];
		if (0 == strcmp(key,"error")) {
			err_str = value;
		} else {
			//fprintf(stderr, "package set: unknown key %s value %s\n",key,value);
		}
	}
	if (err_str) {
		fprintf(stderr, "package set: server returned error %d\n",err_str);
		free(values);
		return CITRUSLEAF_FAIL_UNKNOWN;
	}	
	
	free(values);
	
	return(0);
	
}

//
// receiving a record from the server. Load it into the results structure.  
//
// ??? call the client ???
//

int cl_mr_state_row(cl_mr_state *mrs_p, char *ns, cf_digest *keyd, char *set, uint32_t generation,
           uint32_t record_ttl, cl_bin *bin, int n_bins,
           bool is_last, citrusleaf_get_many_cb cb, void *udata) 
{

    lua_State *lua   = mrs_p->lua;
    
    // fprintf(stderr, "cl_mr_state_row: received a row in response\n");
    
    pthread_mutex_lock( & mrs_p->lua_lock );

	mrs_p->responses++;
    
    int ret;
    
    if (mrs_p->responses == 1) {
        lua_getglobal(lua, "PostFinalizeCleanup");
        ret = lua_pcall(lua, 0, 0, 0);
        if (ret) {
            assertOnLuaError(lua, "ERROR: luaL_dostring(PostFinalizeCleanup)");
        }
    }
    
    for (int i=0;i<n_bins;i++) {
        char bin_name[32];
        strcpy(bin_name, bin[i].bin_name);
        if (bin_name[0] == 0) { bin_name[0] = '.'; bin_name[1] = 0; }
        cl_object *o = &bin[i].object;
        switch(o->type) {
          case CL_BLOB:        
          case CL_JAVA_BLOB: 
          case CL_CSHARP_BLOB: 
          case CL_PYTHON_BLOB: 
          case CL_RUBY_BLOB:
              fprintf(stderr, " UNHANDLED BLOB type response received\n"); 
              break;
          case CL_INT:
              fprintf(stderr, " UNHANDLED: integer type response received: %"PRIi64"\n", o->u.i64 ); 
              break;
          case CL_STR:
			  lua_getglobal(lua, "AddStringToMapResults");
			  lua_pushboolean(lua, mrs_p->mr_job->rdc_fname? 1 : 0); // true if a reduce will be called
			  lua_pushstring (lua, bin_name);
			  lua_pushstring (lua, o->u.str);
			  ret = lua_pcall(lua, 3, 0, 0);
			  if (ret) assertOnLuaError(lua, "ERR: AddStringToMapResults");
              break;
          case CL_LUA_BLOB:
			  lua_getglobal  (lua, "AddTableToMapResults");
			  lua_pushboolean(lua, mrs_p->mr_job->rdc_fname? 1 : 0); // true if a reduce will be called
			  lua_pushstring (lua, bin_name);
			  lua_pushstring (lua, o->u.str);
			  ret = lua_pcall(lua, 3, 0, 0);
			  if (ret) assertOnLuaError(lua, "ERR: AddTableToMapResults");
          	  break;
          default: 
          	  fprintf(stderr, " UNHANDLED BLOB\n"); 
          	  break;
        }
    }

    pthread_mutex_unlock( & mrs_p->lua_lock );
    
    return 0;
}

//
// All the rows are done. call map and reduce.
//

int cl_mr_state_done(cl_mr_state *mrs_p,  citrusleaf_get_many_cb cb, void *udata) 
{
    lua_State *lua   = mrs_p->lua;
    int ret;
    
	// call the reduce wrapper
	if (mrs_p->mr_job->rdc_fname) {
		lua_getglobal(lua, "ReduceWrapper");
		lua_getglobal(lua, mrs_p->mr_job->rdc_fname);
		int ret = lua_pcall(lua, 1, 0, 0);
		if (ret) {
			printf("ReduceWrapper: FAILED: msg: (%s)\n",
				   lua_tostring(lua, -1));
			return -1; //TODO throw an error
		}
	}
	// call the finalize - puts the answer in ReduceResults global
	if (mrs_p->mr_job->fnz_fname) {
		lua_getglobal(lua, "FinalizeWrapper");
		lua_getglobal(lua, mrs_p->mr_job->fnz_fname);
		int ret = lua_pcall(lua, 1, 0, 0);
		if (ret) {
			printf("FinalizeWrapper: FAILED: (%s)\n",
				   lua_tostring(lua, -1));
			return -1; //TODO throw an error
		}
	}
	// call the SendResults - takes ReduceResults global, call back into the row function
	lua_getglobal(lua, "SendReduceResults");
	lua_pushlightuserdata(lua, cb);
	lua_pushlightuserdata(lua, udata);
	ret = lua_pcall(lua, 2, 0, 0);
	if (ret) {
		printf("DebugWrapper: FAILED: (%s)\n",
			   lua_tostring(lua, -1));
		return -1; //TODO throw an error
	}

	
	lua_getglobal(lua, "DebugWrapper");
	lua_getglobal(lua, "print_user_and_value");
	ret = lua_pcall(lua, 1, 0, 0);
	if (ret) {
		printf("DebugWrapper: FAILED: (%s)\n",
			   lua_tostring(lua, -1));
		return -1; //TODO throw an error
	}
        
}


// parameter 1 is the key object
// parameter 2 is the value object
// parameter 3 is the (opaque) callback
// parameter 4 is the (opaque) userdata

int luaSendReduceObject(lua_State *lua) {

    int argc = lua_gettop(lua);
    if (argc != 4 || !lua_isuserdata(lua, 3) || !lua_isuserdata(lua, 4)) {
        lua_settop(lua, 0);
        // Todo: this was throwing a Redis error
        // luaPushError(lua, "Lua USAGE: SendReduceObject(k, v, cb, udata)");
        fprintf(stderr, "luaSendReduceObject USAGE: SendReduceObject(k, v, cb, udata)");
        return 1;
    }
    citrusleaf_get_many_cb cb = (citrusleaf_get_many_cb) lua_touserdata(lua, 3);
    void *udata     = (void *)      lua_touserdata(lua, 4);

    cl_bin	bins[2];
    strcpy(bins[0].bin_name,"key");
    strcpy(bins[1].bin_name,"value");
    
    // loop over the key and the value, which are 1 and 2 on the stack
    for (int i=1; i <= 2; i++) {
    	cl_object *o = &bins[i-1].object;
    	o->free = 0;

    	int ltype = lua_type(lua, i);
    	switch (ltype) {
    		case LUA_TNIL:
				o->type = CL_INT;
				o->sz = sizeof(o->u.i64);
				o->u.i64 = 0;
				break;
    		case LUA_TNUMBER: {
				uint64_t k = lua_tonumber(lua,i);
				o->type = CL_INT;
				o->sz = sizeof(k);
				o->u.i64 = k;
				}   break;
    		case LUA_TBOOLEAN: {
				bool b = lua_toboolean(lua, i);
				o->type = CL_INT;
				o->sz = sizeof(o->u.i64);
				o->u.i64 = b ? 1 : 0;
				}   break;
			case LUA_TSTRING: { 
				size_t k_len;
				char *k = (char *) lua_tolstring(lua,i,&k_len);
				o->type = CL_STR;
				o->sz = k_len;
				o->u.str = k;
				} break;
    		case LUA_TTABLE:
    		case LUA_TFUNCTION:
    		case LUA_TUSERDATA:
    		case LUA_TTHREAD:
    		case LUA_TLIGHTUSERDATA:
    		default:
    			fprintf(stderr, "map reduce: should not have tables\n");
    			break;
    	}
	}	
	
	(*cb) ( 0/*ns*/, 0/*keyd*/, 0/*set*/, 0/*gen*/, 0/*recordttl*/, bins, 2, false /*islast*/, udata);

	return(0);
}

#define MAX_LUA_SIZE    4096

// lousy helper
static char *trim_end_space(char *str) {
    char *end;
    end = str + strlen(str) - 1; // Trim trailing space
    while(end > str && isspace(*end)) end--;
    *(end + 1) = 0; // Write new null terminator
    return str;
}


#define BITS_IN_int     ( 32 )
#define THREE_QUARTERS  ((int) ((BITS_IN_int * 3) / 4))
#define ONE_EIGHTH      ((int) (BITS_IN_int / 8))
#define HIGH_BITS       ( ~((unsigned int)(~0) >> ONE_EIGHTH ))

// ignoring value_len - know it's null terminated
uint32_t cf_mr_string_hash_fn(void *value, uint32_t value_len)
{
	uint8_t *v = value;
    uint32_t hash_value = 0, i;

    while (*v) 
    {
        hash_value = ( hash_value << ONE_EIGHTH ) + *v;
        if (( i = hash_value & HIGH_BITS ) != 0 )
            hash_value =
                ( hash_value ^ ( i >> THREE_QUARTERS )) &
                        ~HIGH_BITS;
        v++;
    }
    return ( hash_value );
}

int citrusleaf_mr_init() {

	cf_rchash_create(&mr_package_hash, cf_mr_string_hash_fn, mr_package_destroy, 
		0 /*keylen*/, 100 /*sz*/, CF_RCHASH_CR_MT_BIGLOCK);
	return(0);
}

void citrusleaf_mr_shutdown() {
	
	cf_rchash_destroy(mr_package_hash);
	
}
