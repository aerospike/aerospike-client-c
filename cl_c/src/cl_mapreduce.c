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
	
	char *package_name;
	
	// "func" is the code, "name" is the symbol to invoke
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


#define DEFINE_ADD_TO_MAP_RESULTS								\
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
    "end "

#define DEFINE_POST_FINALIZE_CLEANUP                                    \
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
    "end                                                              "

#define DEFINE_REDUCE_WRAPPER                                 \
    "function ReduceWrapper(func)       "                     \
    "  print('ReduceWrapper');          "                     \
    "  ReduceCount = 0;                 "                     \
    "  local res   = {};                "                     \
    "  for k, t in pairs(MapResults) do "                     \
    "    ReduceResults[k] = func(t);    "                     \
    "    ReduceCount = ReduceCount + 1; "                     \
    "  end                              "                     \
    "  MapResults = {};                 "                     \
    "  MapCount   = 0;                  "                     \
    "end                                "

#define DEFINE_FINALIZE_WRAPPER               \
    "function FinalizeWrapper(func)         " \
    "  ReduceResults = func(ReduceResults); " \
    "  ReduceCount = 0; "					  \
    "  for i, v in pairs(ReduceResults) do " \
    "    ReduceCount = ReduceCount + 1; "     \
    "  end "								  \
    "end "

#define DEFINE_DEBUG_WRAPPER                        \
    "function DebugWrapper(func) "					\
    "  print('DebugWrapper'); "          			\
    "  for k, t in pairs(ReduceResults) do "        \
    "    func(k, t); "          					\
    "  end "          								\
    "end "          								\
    "function print_user_and_value(k, t) "          \
    "  print('v: ' .. t); "  						\
    "end "

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

static int 
mr_state_lua_create(cl_mr_state *mrs_p) {
	
	if (mrs_p->lua) { lua_close(mrs_p->lua); mrs_p->lua = 0; }
	
    mrs_p->lua       = lua_open();
    if (!mrs_p->lua) return(-1);
    lua_State *lua  = mrs_p->lua;
    luaL_openlibs(lua);

    int ret = luaL_dostring(lua, DEFINE_ADD_TO_MAP_RESULTS);
    if (ret) {
    	assertOnLuaError(lua, "ERROR: adding(AddTableToMapResults)");
    	goto Cleanup;
    }
    
    ret     = luaL_dostring(lua, DEFINE_POST_FINALIZE_CLEANUP);
    if (ret) {
    	assertOnLuaError(lua, "ERROR: luaL_dostring(PostFinalizeCleanup)");
    	goto Cleanup;
    }

    ret     = luaL_dostring(lua, DEFINE_REDUCE_WRAPPER);
    if (ret) {
    	assertOnLuaError(lua, "ERROR: luaL_dostring(ReduceWrapper)");
    	goto Cleanup;
    }
    
    ret     = luaL_dostring(lua, DEFINE_FINALIZE_WRAPPER);
    if (ret) {
    	assertOnLuaError(lua, "ERROR: luaL_dostring(FinalizeWrapper)");
    	goto Cleanup;
    }

    ret     = luaL_dostring(lua, DEFINE_DEBUG_WRAPPER);
    if (ret) {
    	assertOnLuaError(lua, "ERROR: luaL_dostring(DebugWrapper)");
    	goto Cleanup; 
    }

    ret     = luaL_dostring(lua, "PostFinalizeCleanup();");
    if (ret) { 
    	assertOnLuaError(lua, "ERROR: luaL_dostring(PostFinalizeCleanup)"); 
    	goto Cleanup; 
    }
    return(0);
    
Cleanup:
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
	if (mrs_p->package_name) free(mrs_p->package_name);
	free(mrs_p);	
}

// input: an mrs_pd and the functions to register
// allocates copies the functions into the mrs_pd, creates the LUA universe, and loads all functions

cl_mr_state * mr_state_create(mr_package *mrp_p) {

	cl_mr_state *mrs_p = calloc(sizeof(cl_mr_state),1);
	if (!mrs_p) return(0);

	// create the lua universe and load in static funcs
    if (! mr_state_lua_create(mrs_p) ) {
    	mr_state_destroy(mrs_p);
    	return(0);
	}
    
    // load in the dynamic code, this can actually fail if the Lua
    // registered is bad, take a copy of the functions
    pthread_mutex_lock(&mrp_p->script_lock);

	// copy the bits we need

    if (! mr_state_load_package_lua(mrs_p, mrp_p) ) {
    	pthread_mutex_unlock(&mrp_p->script_lock);
    	mr_state_destroy(mrs_p);
    	return(0);
    }
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
	
	return(mrs_p);
}

void 
cl_mr_state_put(cl_mr_state *mrs_p) {
	
	// get the package with this name
	mr_package *mrp_p = 0;
	cf_rchash_get(mr_package_hash,mrs_p->package_name,strlen(mrs_p->package_name),(void **)&mrp_p);
	
	if (! mrp_p) {
		fprintf(stderr, "package %s has not been registered locally\n",mrs_p->package_name);
		mr_state_destroy(mrs_p);
		return;
	}
	
	// push the state
	fprintf(stderr, "pushing state %p onto package %s ( %p )\n",mrs_p,mrs_p->package_name,mrp_p);
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
		
		if (mrp_p->script)	free(mrp_p->script);

		cl_mr_state *mrs_p;
    	while (CF_QUEUE_OK == cf_queue_pop(mrp_p->mr_state_q, &mrs_p,0/*nowait*/)) {
    		mr_state_destroy(mrs_p);
    	}
		
		cf_client_rc_free(mrp_p);
	}
	return;
}

void mr_package_release(mr_package *mrp_p) {
	mr_package_destroy((void *)mrp_p);
}

//
// Todo: check if package exists on server, if so, load from there, if not, load to there
//

mr_package * mr_package_create(const char *package_name, const char *script, int script_len )
{
	mr_package *mrp_p = 0;
	bool reusing = false;
	
	// already registered?
	int found = cf_rchash_get(mr_package_hash, package_name, strlen(package_name)+1, (void **) &mrp_p);
	if (found == CF_RCHASH_OK) {
		reusing = true;
		pthread_mutex_lock(&mrp_p->script_lock);
		if (mrp_p->script)	{ free(mrp_p->script); mrp_p->script = 0; }
		reusing = true;
	}
	else {
		mrp_p = cf_client_rc_alloc(sizeof(mr_package));
		if (!mrp_p) goto Cleanup;
		memset(mrp_p, 0, sizeof(mr_package));
	
		mrp_p->package_name = strdup(package_name);
		pthread_mutex_init(&mrp_p->script_lock, 0/*default addr*/);
		mrp_p->mr_state_q = cf_queue_create(sizeof(cl_mr_state *), true/*multithreaded*/);
	}
	
	if (script) {
		// trim ---
		while (script_len && (script[script_len - 1] == 0)) script_len--;
		// then allocate with a null on the end
		mrp_p->script                 = malloc(script_len + 1);
		if (!mrp_p->script)			goto Cleanup;
		memcpy(mrp_p->script, script, script_len);
		mrp_p->script[script_len]     = '\0';
	}
    
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
    	cf_rchash_put_unique(mr_package_hash, package_name, strlen(package_name)+1, mrp_p);
	}
	return(mrp_p);
Cleanup:
	if (!mrp_p)	return(0);
	if (mrp_p->script)	{ free(mrp_p->script); mrp_p->script = 0; }
	return(0);
}


int
citrusleaf_mr_package_register(const char *package_name, const char *script, size_t script_len)
{
	mr_package *package = mr_package_create(package_name, script, script_len );
	if (package == 0) {
		fprintf(stderr, "could not register package %s\n",package_name);
		return(-1);
	}
	mr_package_release(package);
	return(0);
}


//
// receiving a record from the server. Load it into the results structure.  
// If this seems like the last row, invoke Lua to do the Reduce/Finalize structures
//
// ??? call the client ???
//

int cl_mr_state_row(cl_mr_state *mrs_p, char *ns, cf_digest *keyd, char *set, uint32_t generation,
           uint32_t record_ttl, cl_bin *bin, int n_bins,
           bool is_last, citrusleaf_get_many_cb cb, void *udata) 
{

	mrs_p->responses++; // atomic? lock?
    lua_State *lua   = mrs_p->lua;
    
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

    return 0;
}

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

	lua_getglobal(lua, "DebugWrapper");
	lua_getglobal(lua, "print_user_and_value");
	ret = lua_pcall(lua, 1, 0, 0);
	if (ret) {
		printf("DebugWrapper: FAILED: (%s)\n",
			   lua_tostring(lua, -1));
		return -1; //TODO throw an error
	}
        
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

// Load from disk the ID mrsid, and fill out the lscripts structure
// structure (which has a pointer/memory for map, reduce, finalize snips)
// INPUT: mrsid
// OUTPUT: lscripts

#if 0
int citrusleaf_package_register_lua(int mrsid, citrusleaf_package_lua *lscripts) {
    char rpath[128] = "lua_files/";
    lscripts->mrsid = mrsid;

    char fname[512];
    sprintf(fname,"%s%d.map",rpath,mrsid);
    FILE *fmap = fopen(fname,"r"); // open the files
    fprintf(stderr,"loading map_reduce files %s\n",fname);
    if (fmap==NULL) {
        fprintf(stderr,"can't open map file %s\n",fname); return -1;
    } 
   
    sprintf(fname,"%s%d.reduce",rpath,mrsid);
    FILE *frd = fopen(fname,"r");
    if (frd==NULL) {
        fprintf(stderr,"can't open reduce file %s\n",fname); return -1;
    } 

    sprintf(fname,"%s%d.finalize",rpath,mrsid);
    FILE *ffn = fopen(fname,"r");
    if (ffn==NULL) {
        fprintf(stderr,"can't open finalize file %s\n",fname); return -1;
    } 
   
    lscripts->lua_map = malloc(MAX_LUA_SIZE); // allocate memory
    lscripts->lua_reduce = malloc(MAX_LUA_SIZE);
    lscripts->lua_finalize = malloc(MAX_LUA_SIZE);
    if (!lscripts->lua_map || !lscripts->lua_reduce || !lscripts->lua_finalize){
        fprintf(stderr, "can't allocate memory\n"); return -1;
    }

    fgets(lscripts->lua_map,MAX_LUA_SIZE,fmap); // read the data in   
    trim_end_space(lscripts->lua_map);
    fgets(lscripts->lua_reduce,MAX_LUA_SIZE,frd);
    trim_end_space(lscripts->lua_reduce);
    fgets(lscripts->lua_finalize,MAX_LUA_SIZE,ffn);
    trim_end_space(lscripts->lua_finalize);
    
    char tmpStr[1024];
    sprintf(tmpStr, "map.%d=[%s] %ld\n", mrsid, lscripts->lua_map,
                                        strlen(lscripts->lua_map));
    fprintf(stderr, tmpStr);
    sprintf(tmpStr, "reduce.%d=[%s] %ld\n", mrsid, lscripts->lua_reduce,
                                            strlen(lscripts->lua_reduce));
    fprintf(stderr,tmpStr);
    sprintf(tmpStr, "finalize.%d=[%s] %ld\n", mrsid, lscripts->lua_finalize,
                                              strlen(lscripts->lua_finalize));
    fprintf(stderr,tmpStr);
    fclose(fmap); fclose(frd); fclose(ffn);
    return 0;
}
#endif

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
