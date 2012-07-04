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

//ALCHEMY
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>




#define MAX_NUM_MR_PACKAGES 255

static mr_package *MR_packages[MAX_NUM_MR_PACKAGES];


//
// helpers
//
#define ISBLANK(c)           (c == 32 || c == 9)
#define SKIP_SPACES(tok)     while (ISBLANK(*tok)) tok++;

static char *getFuncNameFromFuncDecl(char *lfuncdecl) {
    if (strncmp(lfuncdecl, "function ", 9)) return NULL;
    char *fbeg = lfuncdecl + 9;
    if (ISBLANK(*fbeg)) SKIP_SPACES(fbeg)
    char *paren = strchr(fbeg, '(');
    if (!paren) return NULL;
    char *fname = malloc(paren - fbeg + 1);
    memcpy(fname, fbeg, paren - fbeg);
    fname[paren - fbeg] = '\0';
    return fname;
}

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

static int state_create_lua(mr_state *mrs_p) {
	
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

static int mr_state_load_package_lua(mr_state *mrs_p, mr_package *mrp_p) {
	
    lua_State *lua  = mrs_p->lua;

    if (mrp_p->map_func) {
		int ret     = luaL_dostring(lua, mrp_p->map_func);
		if (ret) { assertOnLuaError(lua, "ERROR: luaL_dostring(map_func)"); return(-1); }
	}

    if (mrp_p->rdc_func) {
//      printf("loadFuncMapReduceLua: rdcfunc: %s\n", mrp_p->rdcfunc);
      int ret   = luaL_dostring(lua, mrp_p->rdc_func);
      if (ret) { assertOnLuaError(lua, "ERROR: luaL_dostring(rdc_func)"); return(-1); }
    }

    if (mrp_p->fnz_func) {
//      printf("loadFuncMapReduceLua: fnzfunc: %s\n", mrp_p->fnzfunc);
      int ret   = luaL_dostring(lua, mrp_p->fnz_func);
      if (ret) { assertOnLuaError(lua, "ERROR: luaL_dostring(fnz_func)"); return(-1); }
    }
    return(0);
}

void mr_state_destroy(mr_state *mrs_p)
{
	if (mrs_p->lua) lua_close(mrs_p->lua);
	free(mrs_p);	
}

// input: an mrj_pd and the functions to register
// allocates copies the functions into the mrj_pd, creates the LUA universe, and loads all functions

mr_state * mr_state_create(mr_package *mrp_p) {

	mr_state *mrs_p = calloc(sizeof(mr_state),1);
	if (!mrs_p) return(0);

	// todo: increase reference count
	mrs_p->package_p = mrp_p;
	
	// create the lua universe and load in static funcs
    if (! state_create_lua(mrs_p) ) {
    	mr_state_destroy(mrs_p);
    	return(0);
	}
    
    // load in the dynamic functions, this can actually fail if the Lua
    // registered is bad
    if (! state_load_package_lua(mrs_p, mrp_p) ) {
    	mr_state_destroy(mrs_p);
    	return(0);
    }
    
    return(mrs_p);
}


//
//

void mr_package_destroy(mr_package *mrp_p)
{
	// TODO
	return;
}

mr_package * mr_package_create(int package_id, char *map_func, int map_func_len,
									char *rdc_func, int rdc_func_len,
									char *fnz_func, int fnz_func_len )
{
	if (package_id >= MAX_NUM_MR_PACKAGES) return(0);	
	mr_package *mrp_p = 0;
	bool reusing = false;
	
	// are we reusing? if so, need to lock, and clean cache later
	if (MR_packages[package_id]) {
		mrp_p = MR_packages[package_id];
		pthread_mutex_lock(&mrp_p->func_lock);
		if (mrp_p->map_func)	{ free(mrp_p->map_func); mrp_p->map_func = 0; } 
		if (mrp_p->rdc_func)	{ free(mrp_p->rdc_func); mrp_p->rdc_func = 0; } 
		if (mrp_p->fnz_func)	{ free(mrp_p->fnz_func); mrp_p->fnz_func = 0; }
		reusing = true;
	}
	else {
		mrp_p = cf_rc_alloc(sizeof(mr_package));
		if (!mrp_p) goto Cleanup;
		memset(mrp_p, 0, sizeof(mr_package));
	
		mrp_p->package_id = package_id;
		pthread_mutex_init(&mrp_p->func_lock, 0/*default addr*/);
		mrp_p->mr_states_q = cf_queue_create(sizeof(mr_state *), true/*multithreaded*/);
	}
	
	if (map_func) {
		mrp_p->map_func                 = malloc(map_func_len + 1);
		if (!mrp_p->map_func)			goto Cleanup;
		memcpy(mrp_p->map_func, map_func, map_func_len);
		mrp_p->map_func[map_func_len]     = '\0';
		mrp_p->map_name                 = getFuncNameFromFuncDecl(map_func);
	}
    
    if (rdc_func) {
        mrp_p->rdc_func             = malloc(rdc_func_len + 1);
        if (!mrp_p->rdc_func)	goto Cleanup;
        memcpy(mrp_p->rdc_func, rdc_func, rdc_func_len);
        mrp_p->rdc_func[rdc_func_len] = '\0';
        mrp_p->rdc_name             = getFuncNameFromFuncDecl(rdc_func);
    }
    
    if (fnz_func) {
        mrp_p->fnz_func             = malloc(fnz_func_len + 1);
        if (!mrp_p->fnz_func)	goto Cleanup;
        memcpy(mrp_p->fnz_func, fnz_func, fnz_func_len);
        mrp_p->fnz_func[fnz_func_len] = '\0';
        mrp_p->fnz_name             = getFuncNameFromFuncDecl(fnz_func);
    }
    
    if (reusing) {
    	pthread_mutex_unlock(&mrp_p->func_lock);
    	
    	// clean queue
    	mr_state *mrs_p = 0;
    	while (CF_QUEUE_OK == cf_queue_pop(mrp_p->mr_states_q, &mrs_p,0/*nowait*/))
    		mr_state_destroy(mrs_p);
    	
    }
    else {
		// stash package locally, open for business
		MR_packages[package_id] = mrp_p;
	}
   	cf_rc_reserve(mrp_p); // increase the reference count
	return(mrp_p);
Cleanup:
	if (!mrp_p)	return(0);
	if (mrp_p->map_func)	free(mrp_p->map_func);
	if (mrp_p->rdc_func)	free(mrp_p->rdc_func);
	if (mrp_p->fnz_func)	free(mrp_p->fnz_func);
	// TODO TRICKY - if reuse, possibly allow jobs to fail? reference count???
	return(0);
}

//
// going to run a particular map reduce job
// look up the name in the shared table, grab / make a new state
//

static int register_luafunc_cb(char *ns, cf_digest *keyd, char *set, uint32_t generation,
                  uint32_t record_ttl, cl_bin *bin, int n_bins,
                  bool is_last, void *udata) {

    int mrjid = (int)(long)udata;
    printf("cl_mapreduce: register_luafunc_cb: mrj_id: %d\n", mrjid);

	// see if this mrjid is registered, grab a state
	
	    
    
    char *mapfunc    = CurrentLuaMapFunc;
    int   mapfunclen = strlen(CurrentLuaMapFunc);
    
    char *rdcfunc    = strncmp(CurrentLuaRdcFunc, "NULL", 4) ? 
    								CurrentLuaRdcFunc : NULL;
    int   rdcfunclen = rdcfunc ? strlen(rdcfunc) : 0;
    
    char *fnzfunc    = strncmp(CurrentLuaFnzFunc, "NULL", 4) ?
    								CurrentLuaFnzFunc : NULL;
    int   fnzfunclen = fnzfunc ? strlen(fnzfunc) : 0;
    
    regMRJ(&MRJ[mrjid], mapfunc, mapfunclen, rdcfunc, rdcfunclen,
                        fnzfunc, fnzfunclen);

    return 0;
}

int ignore_cb(char *ns, cf_digest *keyd, char *set, uint32_t generation,
              uint32_t record_ttl, cl_bin *bin, int n_bins,
              bool is_last, void *udata) {
    return 0;
}

int createsecindx_cb(char *ns, cf_digest *keyd, char *set, uint32_t generation,
                     uint32_t record_ttl, cl_bin *bin, int n_bins,
                     bool is_last, void *udata) {
    printf("createsecindx_cb\n");
    return 0;
}

mrj_state *get_MRJ(int mrjid) { 
	if (mrjid >= MAX_NUM_MR_IDS) {
		printf("using mrj id greater than compiled maximum %d, will fail badly\n",MAX_NUM_MR_IDS);
		return(0);
	}
	return &MRJ[mrjid];
}

//
// receiving a record from the server. Load it into the results structure.  
// If this seems like the last row, invoke Lua to do the Reduce/Finalize structures
//
// ??? call the client ???
//

int mrj_record_cb(char *ns, cf_digest *keyd, char *set, uint32_t generation,
           uint32_t record_ttl, cl_bin *bin, int n_bins,
           bool is_last, void *udata) 
{

	mrs_p = (mr_state *) udata;

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
			  lua_pushboolean(lua, mrji->rdcname? 1 : 0);
			  lua_pushstring (lua, bin_name);
			  lua_pushstring (lua, o->u.str);
			  ret = lua_pcall(lua, 3, 0, 0);
			  if (ret) assertOnLuaError(lua, "ERR: AddStringToMapResults");
              break;
          case CL_LUA_BLOB:
			  lua_getglobal  (lua, "AddTableToMapResults");
			  lua_pushboolean(lua, mrji->rdcname? 1 : 0);
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

    if (mrs_p->responses == mrs_p->num_nodes) {

    	// call the reduce wrapper
        if (mrji->rdcname) {
            lua_getglobal(lua, "ReduceWrapper");
            lua_getglobal(lua, mrs_p->mrp_p->rdc_name);
            int ret = lua_pcall(lua, 1, 0, 0);
            if (ret) {
                printf("ReduceWrapper: FAILED: msg: (%s)\n",
                       lua_tostring(lua, -1));
                return -1; //TODO throw an error
            }
        }
        if (mrji->fnzname) {
            lua_getglobal(lua, "FinalizeWrapper");
            lua_getglobal(lua, mrs_p->mrp_p->fnz_name);
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
        
        // Need to call the actual client about the response
        
    }
    return 0;
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

// Load from disk the ID mrjid, and fill out the lscripts structure
// structure (which has a pointer/memory for map, reduce, finalize snips)
// INPUT: mrjid
// OUTPUT: lscripts

int citrusleaf_package_register_lua(int mrjid, citrusleaf_package_lua *lscripts) {
    char rpath[128] = "lua_files/";
    lscripts->mrjid = mrjid;

    char fname[512];
    sprintf(fname,"%s%d.map",rpath,mrjid);
    FILE *fmap = fopen(fname,"r"); // open the files
    fprintf(stderr,"loading map_reduce files %s\n",fname);
    if (fmap==NULL) {
        fprintf(stderr,"can't open map file %s\n",fname); return -1;
    } 
   
    sprintf(fname,"%s%d.reduce",rpath,mrjid);
    FILE *frd = fopen(fname,"r");
    if (frd==NULL) {
        fprintf(stderr,"can't open reduce file %s\n",fname); return -1;
    } 

    sprintf(fname,"%s%d.finalize",rpath,mrjid);
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
    sprintf(tmpStr, "map.%d=[%s] %ld\n", mrjid, lscripts->lua_map,
                                        strlen(lscripts->lua_map));
    fprintf(stderr, tmpStr);
    sprintf(tmpStr, "reduce.%d=[%s] %ld\n", mrjid, lscripts->lua_reduce,
                                            strlen(lscripts->lua_reduce));
    fprintf(stderr,tmpStr);
    sprintf(tmpStr, "finalize.%d=[%s] %ld\n", mrjid, lscripts->lua_finalize,
                                              strlen(lscripts->lua_finalize));
    fprintf(stderr,tmpStr);
    fclose(fmap); fclose(frd); fclose(ffn);
    return 0;
}


