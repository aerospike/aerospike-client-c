#include <iostream>
using namespace std;

#ifndef LUA_MODULE_PATH
#define LUA_MODULE_PATH "../udf_examples/rec_udf/src/lua"
#endif

extern "C" {
	
	//********************************************************
	// Citrusleaf Core Foundation Includes
	//********************************************************
	#include <citrusleaf/cf_random.h>
	#include <citrusleaf/cf_atomic.h>
	#include <citrusleaf/cf_hist.h>

	//********************************************************
	// Citrusleaf Mod-Lua Includes
	//********************************************************

	// as_val types (persistable)
	#include <citrusleaf/as_val.h>
	#include <citrusleaf/as_nil.h>
	#include <citrusleaf/as_boolean.h>
	#include <citrusleaf/as_integer.h>
	#include <citrusleaf/as_bytes.h>
	#include <citrusleaf/as_string.h>
	#include <citrusleaf/as_list.h>
	#include <citrusleaf/as_map.h>

	// as_val types (non-persisted)
	#include <citrusleaf/as_pair.h>
	#include <citrusleaf/as_rec.h>

	// implementations
	#include <citrusleaf/as_arraylist.h>
	#include <citrusleaf/as_linkedlist.h>
	#include <citrusleaf/as_hashmap.h>

	// others
	#include <citrusleaf/as_result.h>
	#include <citrusleaf/as_stream.h>
	#include <citrusleaf/as_aerospike.h>
	#include <citrusleaf/as_buffer.h>
	#include <citrusleaf/as_module.h>
	#include <citrusleaf/as_logger.h>
	#include <citrusleaf/as_serializer.h>

	// lua module
	#include <citrusleaf/mod_lua.h>
	#include <citrusleaf/mod_lua_config.h>

	//********************************************************
	// Citrusleaf Client Includes
	//********************************************************
	#include <citrusleaf/cl_async.h>
	#include <citrusleaf/cl_object.h>
	#include <citrusleaf/cl_cluster.h>
	#include <citrusleaf/cl_info.h>
	#include <citrusleaf/cl_kv.h>
	#include <citrusleaf/cl_lookup.h>
	#include <citrusleaf/cl_object.h>
	#include <citrusleaf/cl_partition.h>
	#include <citrusleaf/cl_query.h>
	#include <citrusleaf/cl_scan.h>
	#include <citrusleaf/cl_sindex.h>
	#include <citrusleaf/cl_types.h>
	#include <citrusleaf/cl_udf.h>
	#include <citrusleaf/cl_write.h>
	#include <citrusleaf/citrusleaf.h>

}

#include <stdarg.h>

#define INFO(fmt, args...) \
    __log_append(stderr,"", fmt, ## args);

#define ERROR(fmt, args...) \
    __log_append(stderr,"    ", fmt, ## args);

#define LOG(fmt, args...) \
    __log_append(stderr,"    ", fmt, ## args);

void __log_append(FILE * f, const char * prefix, const char * fmt, ...) {
    char msg[128] = {0};
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, 128, fmt, ap);
    va_end(ap);
    fprintf(f, "%s%s\n",prefix,msg);
}
typedef struct config_s {

        const char  *host;
        int    port;
        const char  *ns;
        const char  *set;
        uint32_t timeout_ms;
        uint32_t record_ttl;
        const char *package_file;
        const char *package_name;
        cl_cluster      *asc;
        bool    verbose;
        cf_atomic_int success;
        cf_atomic_int fail;
} config;

void usage(int argc, char *argv[]) {
    INFO("Usage %s:", argv[0]);
    INFO("   -h host [default 127.0.0.1] ");
    INFO("   -p port [default 3000]");
    INFO("   -n namespace [default test]");
    INFO("   -s set [default *all*]");
    INFO("   -f udf_file [default lua_files/udf_unit_test.lua]");
}

int do_udf_add_bin_test(config c) {

	int ret = 0;
	bool isGood = false;
	char * res_str;
	int     rsp_n_bins = 0;
	cl_bin *rsp_bins = NULL;
	cl_write_parameters cl_wp;
	cl_write_parameters_set_default(&cl_wp);
	cl_wp.timeout_ms = c.timeout_ms;
	cl_wp.record_ttl = 864000;

	// (0) delete old record to start afresh
	cl_object o_key;
	citrusleaf_object_init_str(&o_key,"addBin_key");		

	int rsp = citrusleaf_delete(c.asc, c.ns, c.set, &o_key, &cl_wp);
	if (rsp != CITRUSLEAF_OK && rsp != CITRUSLEAF_FAIL_NOTFOUND) {
		LOG("failed deleting test data rsp=%d",rsp);
		return -1;
	}

	// (1) insert data with one existing bin
	cl_bin bins[1];
	strcpy(bins[0].bin_name, "old_bin");
	citrusleaf_object_init_str(&bins[0].object, "old_val");
	rsp = citrusleaf_put(c.asc, c.ns, c.set, &o_key, bins, 1, &cl_wp);
	if (rsp != CITRUSLEAF_OK) {
		LOG("failed inserting test data rsp=%d",rsp);
		ret = -1;
		goto Cleanup;
	}
	else {
		LOG("citrusleaf put succeeded");
	}

	// (2) execute the udf 
	as_result res;
	as_result_init(&res);
	uint32_t cl_gen;

	rsp = citrusleaf_udf_record_apply(c.asc, c.ns, c.set, &o_key, 
			c.package_name, "do_new_bin", NULL, 
			c.timeout_ms, &res);  
	
	res_str = as_val_tostring(res.value); 
	LOG("%s: %s", res.is_success ? "SUCCESS" : "FAILURE", res_str);
	free(res_str);
	as_result_destroy(&res);
	
	if (rsp != CITRUSLEAF_OK) {
		LOG("failed running udf rsp=%d",rsp);
		ret = -1;
		goto Cleanup;
	}

	// (3) verify bin is added 
	rsp = citrusleaf_get_all(c.asc, c.ns, c.set, &o_key, &rsp_bins, &rsp_n_bins, c.timeout_ms, &cl_gen);  
	if (rsp != CITRUSLEAF_OK) {
		LOG("failed getting record_udf test data rsp=%d",rsp);
		ret = -1;
		goto Cleanup;
	}
	if (rsp_n_bins !=2 ) {
		LOG("num bin returned not 2 %d",rsp_n_bins);
		ret = -1;
		goto Cleanup;
	}

	for (int b=0; b<rsp_n_bins; b++) {
		if ( CL_STR == rsp_bins[b].object.type 
				&& 0 == strcmp(rsp_bins[b].bin_name,"new_bin")
				&& 0 == strcmp(rsp_bins[b].object.u.str,"new string")) {
			isGood = true;
		} 
		citrusleaf_object_free(&rsp_bins[b].object);		
	}  	
	free(rsp_bins);	
	ret = (isGood ? 0: -1);

Cleanup:    
	citrusleaf_object_free(&o_key);		
	citrusleaf_object_free(&bins[0].object);		

	return ret;
}

int do_udf_read_bins_test(config c) {

	cl_write_parameters cl_wp;
	cl_write_parameters_set_default(&cl_wp);
	cl_wp.timeout_ms = c.timeout_ms;
	cl_wp.record_ttl = 864000;

	// (0) reinsert record to start afresh
	const char *keyStr = "key_read1";
	cl_object o_key;
	citrusleaf_object_init_str(&o_key,keyStr);		

	cl_bin bins[3];
	strcpy(bins[0].bin_name, "bin1");
	citrusleaf_object_init_str(&bins[0].object, "val1");
	strcpy(bins[1].bin_name, "bin2");
	citrusleaf_object_init_str(&bins[1].object, "val2");
	strcpy(bins[2].bin_name, "bin3");
	citrusleaf_object_init_str(&bins[2].object, "val3");
	int rsp = citrusleaf_put(c.asc, c.ns, c.set, &o_key, bins, 3, &cl_wp);
	citrusleaf_object_free(&bins[0].object);
	if (rsp != CITRUSLEAF_OK) {
		citrusleaf_object_free(&o_key);		
		LOG("failed inserting test data rsp=%d",rsp);
		return -1;
	}
	else {
		LOG("citrusleaf put succeeded");
	}

	// (2) call udf_record_apply - "do_read1_record" function in udf_unit_test.lua 
	as_result res;
	as_result_init(&res);
	rsp = citrusleaf_udf_record_apply(c.asc, c.ns, c.set, &o_key, 
			c.package_name, "do_read1_record", NULL, 
			c.timeout_ms, &res); 
	char *res_str = as_val_tostring(res.value); 
	LOG("%s: %s", res.is_success ? "SUCCESS" : "FAILURE", res_str);
	if (rsp != CITRUSLEAF_OK) {
		LOG("failed citrusleaf_run_udf rsp=%d",rsp);
	}
	as_result_destroy(&res);
	free(res_str);
	citrusleaf_object_free(&o_key);		
	return 0;
}
typedef struct test_def_s {
	const char * name;
	int (*run)(config c);
} test_def;

#define test(func) {#func, func}

const test_def test_defs[] = {
	test(do_udf_read_bins_test),
	test(do_udf_add_bin_test),
	{ NULL, NULL }
};


int register_package(config c) 
{ 
	INFO("Opening package file %s", c.package_file);  
	FILE *fptr = fopen(c.package_file,"r"); 
	if (!fptr) { 
		LOG("cannot open script file %s", c.package_file);  
		return(-1); 
	} 
	int max_script_len = 1048576; 
	byte *script_code = new byte [max_script_len]; 
	memset(script_code, 0, max_script_len);
	if (script_code == NULL) { 
		LOG("malloc failed"); return(-1); 
	}     

	byte *script_ptr = script_code; 
	int b_read = fread(script_ptr,1,512,fptr); 
	int b_tot = 0; 
	while (b_read) { 
		b_tot      += b_read; 
		script_ptr += b_read; 
		b_read      = fread(script_ptr,1,512,fptr); 
	}                        
	fclose(fptr); 

	char *err_str = NULL; 
	as_bytes udf_content;
	as_bytes_init(&udf_content, script_code, b_tot, true);
	if (b_tot>0) { 
		int resp = citrusleaf_udf_put(c.asc, basename(c.package_file), &udf_content, AS_UDF_LUA, &err_str); 
		if (resp!=0) { 
			INFO("unable to register package file %s as %s resp = %d", c.package_file, c.package_name,resp); return(-1);
			INFO("%s",err_str); free(err_str);
			free(script_code);
			return(-1);
		}
		INFO("successfully registered package file %s as %s", c.package_file, c.package_name); 
	} else {   
		INFO("unable to read package file %s as %s b_tot = %d", c.package_file, c.package_name,b_tot); return(-1);    
	}
	free(script_code);
	return 0;
}
int main(int argc, char **argv) {
	// Setting configuration
	config c;
	memset(&c,0,sizeof(config));

	c.host = "127.0.0.1";
	c.port         = 3000;
	c.ns = "test";
	c.set = "demo";
	c.timeout_ms   = 1000;
	c.record_ttl   = 864000;
	c.verbose      = false;
	c.package_file = LUA_MODULE_PATH"/udf_unit_test.lua";
	c.package_name = "udf_unit_test";

	INFO("Starting Record stored-procedure Unit Tests");
	int optcase;
	while ((optcase = getopt(argc, argv, "ckmh:p:n:s:P:f:v:x:r:t:i:j:")) != -1) {
		switch (optcase) {
			case 'h': c.host         = strdup(optarg);          break;
			case 'p': c.port         = atoi(optarg);            break;
			case 'n': c.ns           = strdup(optarg);          break;
			case 's': c.set          = strdup(optarg);          break;
			case 'v': c.verbose      = true;                    break;
			case 'f': c.package_file = strdup(optarg);          break;
			case 'P': c.package_name = strdup(optarg);          break;
			default:  usage(argc, argv);                      return(-1);
		}
	}

	// setting up cluster
	INFO("Startup: host %s port %d ns %s set %s file %s",
			c.host, c.port, c.ns, c.set == NULL ? "" : c.set, c.package_file);

	citrusleaf_init();

	//citrusleaf_set_debug(true);

	// create the cluster object - attach
	cl_cluster *asc = citrusleaf_cluster_create();
	if (!asc) { 
		INFO("could not create cluster"); 
		return(-1); 
	}

	if (0 != citrusleaf_cluster_add_host(asc, c.host, c.port, c.timeout_ms)) {
		INFO("could not connect to host %s port %d", c.host, c.port);
		return(-1);
	}
	c.asc           = asc;

	// register our package. 
	if (register_package(c) !=0 ) {
		return -1;
	}

	INFO("");

	test_def ** failures = (test_def **) alloca(sizeof(test_defs));
	uint32_t nfailures = 0;

	test_def ** successes = (test_def **) alloca(sizeof(test_defs));
	uint32_t nsuccesses = 0;

	// Test passes
	test_def * test = (test_def *) test_defs;
	while( test->name != NULL ) {
		INFO("%s ::", test->name); 
		if ( test->run(c) ) {
			LOG("✘  FAILURE"); 
			failures[nfailures++] = test;
		} 
		else {
			LOG("✔  SUCCESS"); 
			successes[nsuccesses++] = test;
		}
		test++;
		LOG("");
	}
	
	citrusleaf_cluster_destroy(asc);
	citrusleaf_shutdown();
	
	INFO("###############################################################");
	INFO("");
	INFO("Test Summary: %d (success) %d (failures) %d (total)", nsuccesses, nfailures, nsuccesses + nfailures);
	INFO("");

	if ( nfailures > 0 ) {
		INFO("Failed Tests:");
		for( int i = 0; i<nfailures; i++) {
			INFO("    - %s", failures[i]->name);
		}
		INFO("");
	}

	return(0);
}
