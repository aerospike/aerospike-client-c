/* *  Citrusleaf Stored Procedure Test Program
 *  rec_udf.c - Validates stored procedure functionality
 *
 *  Copyright 2012 by Citrusleaf.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <stdarg.h>
#include <stdbool.h>
#include <assert.h>
#include <ctype.h>

#include <fcntl.h>
#include <sys/stat.h>

#include "citrusleaf/citrusleaf.h"
#include "rec_udf.h"
#include "citrusleaf/cl_udf.h"
#include <citrusleaf/cf_random.h>
#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_hist.h>
#include <citrusleaf/citrusleaf.h>

static config *g_config = NULL;

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

void usage(int argc, char *argv[]) {
    INFO("Usage %s:", argv[0]);
    INFO("   -h host [default 127.0.0.1] ");
    INFO("   -p port [default 3000]");
    INFO("   -n namespace [default test]");
    INFO("   -s set [default *all*]");
    INFO("   -f udf_file [default lua_files/udf_unit_test.lua]");
}


int init_configuration (int argc, char *argv[])
{
	g_config = (config *)malloc(sizeof(config));
	memset(g_config, 0, sizeof(g_config));

	g_config->host         = "127.0.0.1";
	g_config->port         = 3000;
	g_config->ns           = "test";
	g_config->set          = "demo";
	g_config->timeout_ms   = 1000;
	g_config->record_ttl   = 864000;
	g_config->verbose      = false;
	g_config->package_file = "../../lua_files/udf_unit_test.lua";
	g_config->package_name = "udf_unit_test";

	INFO("Starting Record stored-procedure Unit Tests");
	int optcase;
	while ((optcase = getopt(argc, argv, "ckmh:p:n:s:P:f:v:x:r:t:i:j:")) != -1) {
		switch (optcase) {
			case 'h': g_config->host         = strdup(optarg);          break;
			case 'p': g_config->port         = atoi(optarg);            break;
			case 'n': g_config->ns           = strdup(optarg);          break;
			case 's': g_config->set          = strdup(optarg);          break;
			case 'v': g_config->verbose      = true;                    break;
			case 'f': g_config->package_file = strdup(optarg);          break;
			case 'P': g_config->package_name = strdup(optarg);          break;
			default:  usage(argc, argv);                      return(-1);
		}
	}
	return 0;
}
int do_udf_bin_update_test() {

	cl_write_parameters cl_wp;
	cl_write_parameters_set_default(&cl_wp);
	cl_wp.timeout_ms = g_config->timeout_ms;
	cl_wp.record_ttl = 864000;

	// (0) delete & reinsert record to start afresh
	char *keyStr = "key_bin_update";
	cl_object o_key;
	citrusleaf_object_init_str(&o_key,keyStr);		
	int rsp = citrusleaf_delete(g_config->asc, g_config->ns, g_config->set, &o_key, &cl_wp);
	if (rsp != CITRUSLEAF_OK && rsp != CITRUSLEAF_FAIL_NOTFOUND) {
		citrusleaf_object_free(&o_key);		
		LOG("failed deleting test data rsp=%d",rsp);
		return -1;
	}

	// (1) put in values
	cl_bin bins[1];
	strcpy(bins[0].bin_name, "bin_to_change");
	citrusleaf_object_init_str(&bins[0].object, "original_bin_val");
	rsp = citrusleaf_put(g_config->asc, g_config->ns, g_config->set, &o_key, bins, 1, &cl_wp);
	citrusleaf_object_free(&bins[0].object);
	if (rsp != CITRUSLEAF_OK) {
		citrusleaf_object_free(&o_key);		
		LOG("failed inserting test data rsp=%d",rsp);
		return -1;
	}

	// (2) set up stored procedure to call
	//uint32_t cl_gen;
	cl_bin *rsp_bins = NULL;
	int     rsp_n_bins = 0;
	as_result res;
	as_result_init(&res);

	as_list * arglist = as_arraylist_new(3, 8);	
	// arg 1 -> bin name
	as_list_add_string(arglist, "bin_to_change");

	// arg #2 -> bin value
	as_list_add_string(arglist,"changed by lua");
	LOG("Bin value intially : original_bin_val");
	rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
			g_config->package_name,"do_update_bin", arglist, g_config->timeout_ms, &res);  
	
	if (rsp != CITRUSLEAF_OK) {
		citrusleaf_object_free(&o_key);		
		LOG("failed citrusleaf_run_udf rsp=%d",rsp);
		return -1;
	}
	char *res_str = as_val_tostring(res.value); 
	LOG("%s: %s", res.is_success ? "SUCCESS" : "FAILURE", res_str);
	free(res_str);

	as_result_destroy(&res);
	as_val_destroy(arglist);
	arglist = NULL;

	// (3) verify record is updated by reading 4 times 
	for (int i=0; i<4; i++) {
		uint32_t cl_gen;
		rsp_bins = NULL;
		rsp_n_bins = 0;

		int rsp = citrusleaf_get_all(g_config->asc, g_config->ns, g_config->set, &o_key, &rsp_bins, &rsp_n_bins, g_config->timeout_ms, &cl_gen);  

		if (rsp != CITRUSLEAF_OK) {
			LOG("failed citrusleaf_get_all %d rsp=%d",i,rsp);
			citrusleaf_object_free(&o_key);		
			return -1;
		}

		for (int b=0; b<rsp_n_bins; b++) {
			LOG("validation read returned %s = [%s]",rsp_bins[b].bin_name,rsp_bins[b].object.u.str);
			if (strcmp(rsp_bins[b].bin_name, "bin_to_change") == 0) {
				if ( rsp_bins[b].object.type != CL_STR || strncmp(rsp_bins[b].object.u.str,"changed by lua", strlen("changed by lua")) != 0) {
					LOG("data validation failed on round %i",i);
					citrusleaf_object_free(&rsp_bins[b].object);		
					free(rsp_bins);	
					return -1;
				} 
			} 
			citrusleaf_object_free(&rsp_bins[b].object);		
		}  	
		free(rsp_bins);	
	}

	citrusleaf_object_free(&o_key);		
	return 0;
}

int do_udf_trim_bin_test() {

	int num_records = 2;
	cl_write_parameters cl_wp;
	cl_write_parameters_set_default(&cl_wp);
	cl_wp.timeout_ms = g_config->timeout_ms;
	cl_wp.record_ttl = 864000;

	// (0) delete old record to start afresh
	for (int i=0; i<num_records; i++) {
		// creating the key object
		char *keyStr = (i==0 ? "key1": "key2") ;
		cl_object o_key;
		citrusleaf_object_init_str(&o_key,keyStr);		
		int rsp = citrusleaf_delete(g_config->asc, g_config->ns, g_config->set, &o_key, &cl_wp);
		if (rsp != CITRUSLEAF_OK && rsp != CITRUSLEAF_FAIL_NOTFOUND) {
			LOG("failed deleting test data %d rsp=%d",i,rsp);
			return -1;
		}
	}	

	// (1) inserting 2 records, one with short data and one with long data
	for (int i=0; i<num_records; i++) {
		// creating the key object
		char *keyStr = (i==0 ? "key1": "key2") ;
		cl_object o_key;
		citrusleaf_object_init_str(&o_key,keyStr);		
		// creating bins for the key
		int	num_bins = 2;
		cl_bin bins[num_bins];
		char *valStr = (i==0 ? "short line" : "longer than 10 character line");
		strcpy(bins[0].bin_name, "id");
		citrusleaf_object_init_str(&bins[0].object, keyStr);
		strcpy(bins[1].bin_name, "cats");
		citrusleaf_object_init_str(&bins[1].object, valStr);


		// inserting the data		
		int rsp = citrusleaf_put(g_config->asc, g_config->ns, g_config->set, &o_key, bins, num_bins, &cl_wp);
		// cleanup
		citrusleaf_object_free(&bins[0].object);
		citrusleaf_object_free(&bins[1].object);
		citrusleaf_object_free(&o_key);		
		if (rsp != CITRUSLEAF_OK) {
			LOG("failed inserting test data %d rsp=%d",i,rsp);
			return -1;
		}
	}

	// (3) calling each record to execute the storedproc 
	for (int i=0; i<2; i++) {
		cl_bin *rsp_bins = NULL;
		int     rsp_n_bins = 0;

		char *keyStr = (i==0 ? "key1": "key2") ;
		cl_object o_key;
		citrusleaf_object_init_str(&o_key,keyStr);	
		// (2) set up stored procedure to call
		as_list * arglist = as_arraylist_new(2, 8);

		if (!arglist) {
			LOG("can't create udf_params");
			return(-1);
		}
		// Send information about limit on the string len
		as_list_add_string(arglist, "20");

		as_result res;
		as_result_init(&res);
		int rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
				g_config->package_name, "do_trim_bin", arglist, g_config->timeout_ms, &res);  

		char *res_str = as_val_tostring(res.value); 
		LOG("%s: %s", res.is_success ? "SUCCESS" : "FAILURE", res_str);
		free(res_str);

		as_result_destroy(&res);


		if (rsp != CITRUSLEAF_OK) {
			citrusleaf_object_free(&o_key);	
			LOG("failed record_udf test data %d rsp=%d",i,rsp);
			return -1;
		}
		citrusleaf_object_free(&o_key);		

		for (int b=0; b<rsp_n_bins; b++) {
			if (rsp_bins[b].object.type == CL_STR) {
				LOG("udf returned record[%d] %s=%s",i,rsp_bins[b].bin_name,rsp_bins[b].object.u.str);
			} else {
				LOG("warning: expected string type but has object type %s=%d",rsp_bins[b].bin_name,rsp_bins[b].object.type);
			}
			citrusleaf_object_free(&rsp_bins[b].object);		
		}  	
		free(rsp_bins);	
		as_val_destroy(arglist);
		arglist = NULL;
	}

	// (4) verify record is updated 
	for (int i=0; i<num_records; i++) {
		uint32_t cl_gen;
		cl_bin *rsp_bins = NULL;
		int     rsp_n_bins = 0;

		char *keyStr = (i==0 ? "key1": "key2") ;
		cl_object o_key;
		citrusleaf_object_init_str(&o_key,keyStr);		

		int rsp = citrusleaf_get_all(g_config->asc, g_config->ns, g_config->set, &o_key, &rsp_bins, &rsp_n_bins, g_config->timeout_ms, &cl_gen);  

		if (rsp != CITRUSLEAF_OK) {
			LOG("failed record_udf test data %d rsp=%d",i,rsp);
			citrusleaf_object_free(&o_key);		
			return -1;
		}
		citrusleaf_object_free(&o_key);		

		uint8_t fail = 0;
		for (int b=0; b<rsp_n_bins; b++) {
			if ( CL_STR == rsp_bins[b].object.type && 0 == strcmp(rsp_bins[b].bin_name,"cats")) {
				LOG("checking record[%d] %s=[%s]",i,rsp_bins[b].bin_name,rsp_bins[b].object.u.str);
				if ( (i==0 && strcmp(rsp_bins[b].object.u.str,"short line")!=0)
						||(i==1 && strcmp(rsp_bins[b].object.u.str,"new string")!=0) ) {				
					fail = 1;
				}
			}
			citrusleaf_object_free(&rsp_bins[b].object);		
		}  	
		if (fail) {
			LOG("data failed");
			return -1;
		}
		free(rsp_bins);	
	}

	return 0;
}


int do_udf_add_bin_test() {

	int ret = 0;

	cl_write_parameters cl_wp;
	cl_write_parameters_set_default(&cl_wp);
	cl_wp.timeout_ms = g_config->timeout_ms;
	cl_wp.record_ttl = 864000;

	// (0) delete old record to start afresh
	cl_object o_key;
	citrusleaf_object_init_str(&o_key,"addBin_key");		

	int rsp = citrusleaf_delete(g_config->asc, g_config->ns, g_config->set, &o_key, &cl_wp);
	if (rsp != CITRUSLEAF_OK && rsp != CITRUSLEAF_FAIL_NOTFOUND) {
		LOG("failed deleting test data rsp=%d",rsp);
		return -1;
	}

	// (1) insert data with one existing bin
	cl_bin bins[1];
	strcpy(bins[0].bin_name, "old_bin");
	citrusleaf_object_init_str(&bins[0].object, "old_val");
	rsp = citrusleaf_put(g_config->asc, g_config->ns, g_config->set, &o_key, bins, 1, &cl_wp);
	if (rsp != CITRUSLEAF_OK) {
		LOG("failed inserting test data rsp=%d",rsp);
		ret = -1;
		goto Cleanup;
	}
	else {
		LOG("citrusleaf put succeeded");
	}

	// (2) execute the udf 
	cl_bin *rsp_bins = NULL;
	int     rsp_n_bins = 0;
	as_result res;
	as_result_init(&res);
	uint32_t cl_gen;

	rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
			g_config->package_name, "do_new_bin", NULL, 
			g_config->timeout_ms, &res);  
	
	char *res_str = as_val_tostring(res.value); 
	LOG("%s: %s", res.is_success ? "SUCCESS" : "FAILURE", res_str);
	free(res_str);
	as_result_destroy(&res);
	
	if (rsp != CITRUSLEAF_OK) {
		LOG("failed running udf rsp=%d",rsp);
		ret = -1;
		goto Cleanup;
	}

	// (3) verify bin is added 
	rsp = citrusleaf_get_all(g_config->asc, g_config->ns, g_config->set, &o_key, &rsp_bins, &rsp_n_bins, g_config->timeout_ms, &cl_gen);  
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

	bool isGood = false;
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

int do_udf_copy_record_test() {

	cl_write_parameters cl_wp;
	cl_write_parameters_set_default(&cl_wp);
	cl_wp.timeout_ms = g_config->timeout_ms;
	cl_wp.record_ttl = 123000;

	// (0) delete old record to start afresh
	char *keyStr = "key_copy_me";
	cl_object o_key;
	citrusleaf_object_init_str(&o_key,keyStr);				
	int rsp = citrusleaf_delete(g_config->asc, g_config->ns, g_config->set, &o_key, &cl_wp);
	if (rsp != CITRUSLEAF_OK && rsp != CITRUSLEAF_FAIL_NOTFOUND) {
		LOG("failed deleting test data rsp=%d",rsp);
		return -1;
	}

	// (1) reinsert record to start afresh
	cl_bin bins[2];
	strcpy(bins[0].bin_name, "a_bin");
	citrusleaf_object_init_str(&bins[0].object, "a_val");
	strcpy(bins[1].bin_name, "b_bin");
	citrusleaf_object_init_int(&bins[1].object, 22);

	rsp = citrusleaf_put(g_config->asc, g_config->ns, g_config->set, &o_key, bins, 2, &cl_wp);
	if (rsp != CITRUSLEAF_OK) {
		citrusleaf_object_free(&o_key);		
		LOG("failed inserting test data rsp=%d",rsp);
		return -1;
	}
	citrusleaf_object_free(&bins[0].object);
	citrusleaf_object_free(&bins[1].object);

	// (2) set up stored procedure to call
	cl_bin *rsp_bins = NULL;
	int     rsp_n_bins = 0;
	uint32_t cl_gen;
	as_result res;
	as_result_init(&res);
	rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
			g_config->package_name, "do_copy_record", NULL, 
			g_config->timeout_ms, &res);  
	
	char * res_str = as_val_tostring(res.value);	
	LOG("%s: %s", res.is_success ? "SUCCESS" : "FAILURE", res_str);
	free(res_str);
	as_result_destroy(&res);
	
	if (rsp != CITRUSLEAF_OK) {
		citrusleaf_object_free(&o_key);		
		LOG("failed citrusleaf_run_udf rsp=%d",rsp);
		return -1;
	}

	// (4) call second UDF which will add one bin, update one bin, and delete one bin
	rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
			g_config->package_name, "do_updated_copy", NULL, 
			g_config->timeout_ms, &res);  
	
	res_str = as_val_tostring(res.value);	
	LOG("%s: %s", res.is_success ? "SUCCESS" : "FAILURE", res_str);
	as_val_destroy(res.value);
	free(res_str);
	
	if (rsp != CITRUSLEAF_OK) {
		citrusleaf_object_free(&o_key);		
		LOG("failed citrusleaf_run_udf rsp=%d",rsp);
		return -1;
	}
	bool isBad = false;
	rsp = citrusleaf_get_all(g_config->asc, g_config->ns, g_config->set, &o_key, &rsp_bins, &rsp_n_bins, g_config->timeout_ms, &cl_gen);  
	if (rsp_n_bins !=2 ) {
		LOG("num bin returned not 2 %d",rsp_n_bins);
		citrusleaf_object_free(&o_key);		
	}
	for (int i=0; i<rsp_n_bins; i++) {
		if (strcmp(rsp_bins[i].bin_name,"c_bin")==0) {
			if (rsp_bins[i].object.type != CL_STR 
			    || strcmp(rsp_bins[i].object.u.str, "new_value") !=0) {
				LOG("bin %d isn't matching [%s]",i,rsp_bins[i].bin_name);
			    isBad = true;
			}
		} else if (strcmp(rsp_bins[i].bin_name,"b_bin")==0) {
			if (rsp_bins[i].object.type != CL_INT 
				|| rsp_bins[i].object.u.i64 != 22 ) {
				LOG("bin %d isn't matching [%s]",i,rsp_bins[i].bin_name);
		    	isBad = true;
			}
		} else {
			LOG("unexpected bin [%s]",rsp_bins[i].bin_name);
		    isBad = true;
		}
		citrusleaf_object_free(&rsp_bins[i].object);		
	}

	citrusleaf_object_free(&o_key);		


	return isBad;
}

int do_udf_create_record_test() {

	int ret = 0;
	cl_write_parameters cl_wp;
	cl_write_parameters_set_default(&cl_wp);
	cl_wp.timeout_ms = g_config->timeout_ms;
	cl_wp.record_ttl = 864000;

	// (0) delete old record to start afresh
	cl_object o_key;
	citrusleaf_object_init_str(&o_key,"udf_create_record_key");			
	int rsp = citrusleaf_delete(g_config->asc, g_config->ns, g_config->set, &o_key, &cl_wp);
	if (rsp != CITRUSLEAF_OK && rsp != CITRUSLEAF_FAIL_NOTFOUND) {
		LOG("failed deleting test data rsp=%d",rsp);
		return -1;
	}

	// (2) execute the storedproc 
	cl_bin *rsp_bins = NULL;
	int     rsp_n_bins = 0;
	uint32_t cl_gen;
	as_result res;
	as_result_init(&res);
	rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
			g_config->package_name, "do_add_record", NULL, 
			g_config->timeout_ms, &res);  
	
	char *res_str = as_val_tostring(res.value); 
	LOG("%s: %s", res.is_success ? "SUCCESS" : "FAILURE", res_str);
	free(res_str);
	as_result_destroy(&res);
	
	if (rsp != CITRUSLEAF_OK) {
		LOG("failed running udf = %d",rsp);
		ret = -1;
		goto Cleanup;
	}

	// (3) verify record and bin added 
	rsp = citrusleaf_get_all(g_config->asc, g_config->ns, g_config->set, &o_key, &rsp_bins, &rsp_n_bins, g_config->timeout_ms, &cl_gen);  
	if (rsp != CITRUSLEAF_OK) {
		LOG("failed adding record udf test data rsp=%d",rsp);
		ret = -1;
		goto Cleanup;
	}
	if (rsp_n_bins !=2 ) {
		LOG("num bin returned not 2 %d",rsp_n_bins);
		ret = -1;
		goto Cleanup;
	}

	bool isGood = true;
	if ( rsp_bins[1].object.type != CL_STR 
			|| strcmp(rsp_bins[1].bin_name,"second_bin") != 0
			|| strcmp(rsp_bins[1].object.u.str,"another_value") != 0 
			|| rsp_bins[0].object.type != CL_STR 
			|| strcmp(rsp_bins[0].bin_name,"lua_bin") != 0
			|| strcmp(rsp_bins[0].object.u.str,"new_value") != 0) {
		isGood = false;
		LOG("unexpected results");
		LOG("0 - %s %s",rsp_bins[0].bin_name, rsp_bins[0].object.u.str);
		LOG("1 - %s %s",rsp_bins[1].bin_name, rsp_bins[1].object.u.str);
	}  	
	LOG("0 - %s %s",rsp_bins[0].bin_name, rsp_bins[0].object.u.str);
	LOG("1 - %s %s",rsp_bins[1].bin_name, rsp_bins[1].object.u.str);
	citrusleaf_object_free(&rsp_bins[0].object);		
	citrusleaf_object_free(&rsp_bins[1].object);		
	free(rsp_bins);	
	ret = (isGood ? 0: -1);

Cleanup:    
	citrusleaf_object_free(&o_key);		

	return ret;
} 	

int do_udf_delete_record_test() {

	cl_write_parameters cl_wp;
	cl_write_parameters_set_default(&cl_wp);
	cl_wp.timeout_ms = g_config->timeout_ms;
	cl_wp.record_ttl = 864000;

	// (0) reinsert record to start afresh
	char *keyStr = "key_delete";
	cl_object o_key;
	citrusleaf_object_init_str(&o_key,keyStr);		

	cl_bin bins[1];
	strcpy(bins[0].bin_name, "a_bin");
	citrusleaf_object_init_str(&bins[0].object, "a_val");
	int rsp = citrusleaf_put(g_config->asc, g_config->ns, g_config->set, &o_key, bins, 1, &cl_wp);
	citrusleaf_object_free(&bins[0].object);
	if (rsp != CITRUSLEAF_OK) {
		citrusleaf_object_free(&o_key);		
		LOG("failed inserting test data rsp=%d",rsp);
		return -1;
	}

	// (1) set up stored procedure to call
	cl_bin *rsp_bins = NULL;
	int     rsp_n_bins = 0;
	as_result res;
	as_result_init(&res);
	rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
			g_config->package_name, "do_delete_record", NULL, 
			g_config->timeout_ms, &res);  
	char *res_str = as_val_tostring(res.value); 
	LOG("%s: %s", res.is_success ? "SUCCESS" : "FAILURE", res_str);
	free(res_str);
	as_result_destroy(&res);

	if (rsp != CITRUSLEAF_OK) {
		citrusleaf_object_free(&o_key);		
		LOG("failed citrusleaf_run_udf rsp=%d",rsp);
		return -1;
	}

	// (2) verify record does not exists by reading 4 times 
	for (int i=0; i<4; i++) {
		uint32_t cl_gen;
		rsp_bins = NULL;
		rsp_n_bins = 0;

		int rsp = citrusleaf_get_all(g_config->asc, g_config->ns, g_config->set, &o_key, &rsp_bins, &rsp_n_bins, g_config->timeout_ms, &cl_gen);  

		if (rsp != CITRUSLEAF_FAIL_NOTFOUND) {
			LOG("failed citrusleaf_get_all %d rsp=%d",i,rsp);
			citrusleaf_object_free(&o_key);		
			return -1;
		}

	}

	citrusleaf_object_free(&o_key);		
	return 0;
} 	

// THIS TEST FAILS WITH "FAILURE when calling udf_unit_test do_read1_record /home/sunanda/server_code/lua/udf_unit_test.lua:74:
// attempt to call global 'tostring' (a nil value)" error in the logs
int do_udf_read_bins_test() {

	cl_write_parameters cl_wp;
	cl_write_parameters_set_default(&cl_wp);
	cl_wp.timeout_ms = g_config->timeout_ms;
	cl_wp.record_ttl = 864000;

	// (0) reinsert record to start afresh
	char *keyStr = "key_read1";
	cl_object o_key;
	citrusleaf_object_init_str(&o_key,keyStr);		

	cl_bin bins[3];
	strcpy(bins[0].bin_name, "bin1");
	citrusleaf_object_init_str(&bins[0].object, "val1");
	strcpy(bins[1].bin_name, "bin2");
	citrusleaf_object_init_str(&bins[1].object, "val2");
	strcpy(bins[2].bin_name, "bin3");
	citrusleaf_object_init_str(&bins[2].object, "val3");
	int rsp = citrusleaf_put(g_config->asc, g_config->ns, g_config->set, &o_key, bins, 3, &cl_wp);
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
	rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
			g_config->package_name, "do_read1_record", NULL, 
			g_config->timeout_ms, &res); 
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

int do_udf_noop_test() {

	cl_write_parameters cl_wp;
	cl_write_parameters_set_default(&cl_wp);
	cl_wp.timeout_ms = g_config->timeout_ms;
	cl_wp.record_ttl = 864000;

	// (0) setup key
	char *keyStr = "key_noop";
	cl_object o_key;
	citrusleaf_object_init_str(&o_key,keyStr);		

	// (2) set up stored procedure to call
	uint32_t cl_gen;
	cl_bin *rsp_bins = NULL;
	int     rsp_n_bins = 0;
	as_result res;
	as_result_init(&res);
	int rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
			g_config->package_name, "do_noop_function", NULL, 
			g_config->timeout_ms, &res); 

	char *res_str = as_val_tostring(res.value); 
	LOG("%s: %s", res.is_success ? "SUCCESS" : "FAILURE", res_str);
	free(res_str);
	as_result_destroy(&res);
	if (rsp != CITRUSLEAF_OK) {
		LOG("failed citrusleaf_run_udf rsp=%d",rsp);
		return -1;
	} 

	// (3) verify key is still not found 
	rsp = citrusleaf_get_all(g_config->asc, g_config->ns, g_config->set, &o_key, &rsp_bins, &rsp_n_bins, g_config->timeout_ms, &cl_gen);  
	if (rsp != CITRUSLEAF_FAIL_NOTFOUND) {
		citrusleaf_object_free(&o_key);		
		LOG("failed getting record_udf test data rsp=%d",rsp);
		return -1;
	}

	citrusleaf_object_free(&o_key);		
	return 0;
} 	

int do_udf_delete_bin_test() {

	int ret = 0;
	cl_write_parameters cl_wp;
	cl_write_parameters_set_default(&cl_wp);
	cl_wp.timeout_ms = g_config->timeout_ms;
	cl_wp.record_ttl = 864000;

	// (0) delete old record to start afresh
	cl_object o_key;
	citrusleaf_object_init_str(&o_key,"udf_deleteBin_key");		
	int rsp = citrusleaf_delete(g_config->asc, g_config->ns, g_config->set, &o_key, &cl_wp);
	if (rsp != CITRUSLEAF_OK && rsp != CITRUSLEAF_FAIL_NOTFOUND) {
		LOG("failed deleting test data rsp=%d",rsp);
		return -1;
	}

	// (1) insert data with 4 bins
	cl_bin bins[4];
	for (int i=0; i<4; i++) {
		char bname[128], bval[128];
		sprintf(bname,"bin%d",i);
		strcpy(bins[i].bin_name, bname);
		sprintf(bval,"binval%d",i);
		citrusleaf_object_init_str(&bins[i].object, bval);
	}
	rsp = citrusleaf_put(g_config->asc, g_config->ns, g_config->set, &o_key, bins, 4, &cl_wp);
	if (rsp != CITRUSLEAF_OK) {
		LOG("failed inserting test data rsp=%d",rsp);
		return -1;
	}

	// (2) execute the udf 
	cl_bin *rsp_bins = NULL;
	int     rsp_n_bins = 0;
	uint32_t cl_gen;
	as_result res;
	as_result_init(&res);
	rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
			g_config->package_name, "do_delete_bin", NULL, 
			g_config->timeout_ms, &res);  
	char *res_str = as_val_tostring(res.value); 
	LOG("%s: %s", res.is_success ? "SUCCESS" : "FAILURE", res_str);
	free(res_str);
	if (rsp != CITRUSLEAF_OK) {
		LOG("failed running udf = %d",rsp);
		ret = -1;
		goto Cleanup;
	}
	as_result_destroy(&res);

	// (3) verify bin is deleted
	rsp = citrusleaf_get_all(g_config->asc, g_config->ns, g_config->set, &o_key, &rsp_bins, &rsp_n_bins, g_config->timeout_ms, &cl_gen);  
	if (rsp != CITRUSLEAF_OK) {
		LOG("failed getting record_udf test data rsp=%d",rsp);
		ret = -1;
		goto Cleanup;
	}
	if (rsp_n_bins !=3 ) {
		LOG("num bin returned not 3 %d",rsp_n_bins);
		ret = -1;
		goto Cleanup;
	}

	bool isGood = true;
	for (int b=0; b<rsp_n_bins; b++) {
		if ( CL_STR == rsp_bins[b].object.type 
				&& 0 == strcmp(rsp_bins[b].bin_name,"bin3")) {
			//LOG("got it! record[%d] %s=[%s]",i,rsp_bins[b].bin_name,rsp_bins[b].object.u.str);
			isGood = false;
		} 
		citrusleaf_object_free(&rsp_bins[b].object);		
	}  	
	free(rsp_bins);	
	ret = (isGood ? 0: -1);

Cleanup:    
	citrusleaf_object_free(&o_key);
	for (int i=0; i<4; i++) {	
		citrusleaf_object_free(&bins[i].object);		
	}	

	return ret;
} 	

int do_udf_bin_type_test() {

	int ret = 0;

	cl_write_parameters cl_wp;
	cl_write_parameters_set_default(&cl_wp);
	cl_wp.timeout_ms = g_config->timeout_ms;
	cl_wp.record_ttl = 864000;

	// (0) delete old record to start afresh
	cl_object o_key;
	citrusleaf_object_init_str(&o_key,"udf_bin_type_key");		

	int rsp = citrusleaf_delete(g_config->asc, g_config->ns, g_config->set, &o_key, &cl_wp);
	if (rsp != CITRUSLEAF_OK && rsp != CITRUSLEAF_FAIL_NOTFOUND) {
		LOG("failed deleting test data rsp=%d",rsp);
		return -1;
	}

	// (1) execute the storedproc 
	cl_bin *rsp_bins = NULL;
	int     rsp_n_bins = 0;
	uint32_t cl_gen;
	as_result res;
	as_result_init(&res);
	rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
			g_config->package_name, "do_bin_types", NULL, 
			g_config->timeout_ms, &res);  
	
	char *res_str = as_val_tostring(res.value); 
	LOG("%s: %s", res.is_success ? "SUCCESS" : "FAILURE", res_str);
	free(res_str);
	if (rsp != CITRUSLEAF_OK) {
		LOG("failed running udf rsp=%d",rsp);
		ret = -1;
		goto Cleanup;
	}
	as_result_destroy(&res);

	// (2) verify each bin type 
	rsp = citrusleaf_get_all(g_config->asc, g_config->ns, g_config->set, &o_key, &rsp_bins, &rsp_n_bins, g_config->timeout_ms, &cl_gen);  
	if (rsp != CITRUSLEAF_OK) {
		LOG("failed getting record_udf test data rsp=%d",rsp);
		ret = -1;
		goto Cleanup;
	}
	if (rsp_n_bins !=4 ) {
		LOG("num bin returned not 4 %d",rsp_n_bins);
		ret = -1;
		goto Cleanup;
	}

	bool isGood = true;
	for (int b=0; b<rsp_n_bins; b++) {
		if (strcmp(rsp_bins[b].bin_name,"p_int_b")==0) {
			if ( rsp_bins[b].object.type != CL_INT) {
				LOG("p_int unexpected type %d",rsp_bins[b].object.type);
				isGood = false;
			}
			if (rsp_bins[b].object.u.i64 != 5 ) {
				LOG("p_int unexpected value %ld",rsp_bins[b].object.u.i64);
				isGood = false;
			}
		} else if (strcmp(rsp_bins[b].bin_name,"big_int_b")==0) {
			if ( rsp_bins[b].object.type != CL_INT) {
				LOG("big_int unexpected type %d",rsp_bins[b].object.type);
				isGood = false;
			}
			if (rsp_bins[b].object.u.i64 != 1099511627776L ) {
				LOG("big_int unexpected value %ld",rsp_bins[b].object.u.i64);
				isGood = false;
			}
		} else if (strcmp(rsp_bins[b].bin_name,"n_int_b")==0) {
			if ( rsp_bins[b].object.type != CL_INT) {
				LOG("n_int unexpected type %d",rsp_bins[b].object.type);
				isGood = false;
			}
			if (rsp_bins[b].object.u.i64 != -1 ) {
				LOG("n_int unexpected value %ld",rsp_bins[b].object.u.i64);
				isGood = false;
			}
		} else if (strcmp(rsp_bins[b].bin_name,"str_b")==0) {
			if ( rsp_bins[b].object.type != CL_STR) {
				LOG("str unexpected type %d",rsp_bins[b].object.type);
				isGood = false;
			}
			if (strcmp(rsp_bins[b].object.u.str,"this is a string") !=0 ) {
				LOG("str unexpected value %s",rsp_bins[b].object.u.str);
				isGood = false;
			}
		} 
		citrusleaf_object_free(&rsp_bins[b].object);		
	}  	
	free(rsp_bins);	
	ret = (isGood ? 0: -1);

Cleanup:    
	citrusleaf_object_free(&o_key);	

	return ret;
} 	

int do_udf_long_bindata_test() {

	cl_write_parameters cl_wp;
	cl_write_parameters_set_default(&cl_wp);
	cl_wp.timeout_ms = g_config->timeout_ms;
	cl_wp.record_ttl = 864000;

	// (0) delete & reinsert record to start afresh
	char *keyStr = "key_long_bindata";
	cl_object o_key;
	citrusleaf_object_init_str(&o_key,keyStr);		
	int rsp = citrusleaf_delete(g_config->asc, g_config->ns, g_config->set, &o_key, &cl_wp);
	if (rsp != CITRUSLEAF_OK && rsp != CITRUSLEAF_FAIL_NOTFOUND) {
		citrusleaf_object_free(&o_key);		
		LOG("failed deleting test data rsp=%d",rsp);
		return -1;
	}
	// Our lua test function game_double_str, first checks if the record exists, if it already does
	// it appends the bin value with the previous value. If it does not, it makes a bin of value 'x'.  
	int curr_len=0, prev_len=0;

	// (1) set up stored procedure to call multiple times and build up the data

	for (int i=0;i<400;i++) {
		uint32_t cl_gen;
		cl_bin *rsp_bins = NULL;
		int     rsp_n_bins = 0;
		as_result res;
		as_result_init(&res);
		rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
				g_config->package_name,"game_double_str", NULL, g_config->timeout_ms, &res);  

		char *res_str = as_val_tostring(res.value); 
		LOG("Iteration %d: %s: %s", i, res.is_success ? "SUCCESS" : "FAILURE", res_str);
		as_result_destroy(&res);
		free(res_str);

		if (rsp != CITRUSLEAF_OK) {
			LOG("failed running udf rsp=%d",rsp);
			return -1;
		}
		rsp = citrusleaf_get_all(g_config->asc, g_config->ns, g_config->set, &o_key, &rsp_bins, &rsp_n_bins, g_config->timeout_ms, &cl_gen);  
		if (rsp == CITRUSLEAF_OK) {
			for (int b=0; b<rsp_n_bins; b++) {
				if (rsp_bins[b].object.type == CL_STR) {
					curr_len = strlen(rsp_bins[b].object.u.str);
					if(curr_len == prev_len) {
						LOG("String did not get appended, length = %d",curr_len);
						return -1;
					}
					LOG("udf returned %s=[%ld]",rsp_bins[b].bin_name,strlen(rsp_bins[b].object.u.str));
				} else if (rsp_bins[b].object.type == CL_INT) {
					LOG("udf returned %s=[%ld]",rsp_bins[b].bin_name,rsp_bins[b].object.u.i64);
				} else {
					LOG("warning: udf returned object type %s=%d",rsp_bins[b].bin_name,rsp_bins[b].object.type);
				}
				citrusleaf_object_free(&rsp_bins[b].object);		
			}
			prev_len = curr_len;  	
		}
		else {
			LOG("failed citrusleaf_run_udf on iteration %d rsp=%d",i, rsp);
			citrusleaf_object_free(&o_key);		
			return -1;
		}
		for (int i=0;i<rsp_n_bins;i++) {
			citrusleaf_object_free(&rsp_bins[i].object);		
		}
		free(rsp_bins);	
	}

	citrusleaf_object_free(&o_key);		
	return 0;
}

int do_udf_long_biname_test() {

	cl_write_parameters cl_wp;
	cl_write_parameters_set_default(&cl_wp);
	cl_wp.timeout_ms = g_config->timeout_ms;
	cl_wp.record_ttl = 864000;

	// (0) delete & reinsert record to start afresh
	char *keyStr = "key_long_binname";
	cl_object o_key;
	citrusleaf_object_init_str(&o_key,keyStr);		
	int rsp = citrusleaf_delete(g_config->asc, g_config->ns, g_config->set, &o_key, &cl_wp);
	if (rsp != CITRUSLEAF_OK && rsp != CITRUSLEAF_FAIL_NOTFOUND) {
		citrusleaf_object_free(&o_key);		
		LOG("failed deleting test data rsp=%d",rsp);
		return -1;
	}

	// (1) set up stored procedure which will insert a long named bin
	uint32_t cl_gen;
	cl_bin *rsp_bins = NULL;
	int     rsp_n_bins = 0;
	as_result res;
	as_result_init(&res);
	rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
			g_config->package_name,"do_long_binname", NULL, g_config->timeout_ms, &res);  

	char *res_str = as_val_tostring(res.value); 
	LOG("Citrusleaf udf apply %s: %s", res.is_success ? "SUCCESS" : "FAILURE", res_str);
	free(res_str);
	as_result_destroy(&res);

	if (rsp != CITRUSLEAF_OK) {
		LOG("failed citrusleaf_run_udf rsp=%d",rsp);
		return -1;
	}
	rsp = citrusleaf_get_all(g_config->asc, g_config->ns, g_config->set, &o_key, &rsp_bins, &rsp_n_bins, g_config->timeout_ms, &cl_gen);  
	if (rsp == CITRUSLEAF_OK) {
		LOG("Number of bins are %d",rsp_n_bins);
		if (rsp_n_bins!=2) {
			// debugging
			for (int b=0; b<rsp_n_bins; b++) {
				if (rsp_bins[b].object.type == CL_STR) {
					LOG("udf returned %s=[%ld]",rsp_bins[b].bin_name,strlen(rsp_bins[b].object.u.str));
				} else if (rsp_bins[b].object.type == CL_INT) {
					LOG("udf returned %s=[%ld]",rsp_bins[b].bin_name,rsp_bins[b].object.u.i64);
				} else {
					LOG("warning: udf returned object type %s=%d",rsp_bins[b].bin_name,rsp_bins[b].object.type);
				}
				citrusleaf_object_free(&rsp_bins[b].object);		
			}  	
			// end debugging
			citrusleaf_object_free(&o_key);		
			LOG("unexpected # of bins returned %d",rsp_n_bins);
			return -1;
		}
		for (int b=0; b<rsp_n_bins; b++) {
			citrusleaf_object_free(&rsp_bins[b].object);		
		}
	}
	else {
		LOG("Citrusleaf get all failed with %d",rsp);
	}
	free(rsp_bins);	

	citrusleaf_object_free(&o_key);		
	return 0;
}

int do_udf_too_many_bins_test() {

	cl_write_parameters cl_wp;
	cl_write_parameters_set_default(&cl_wp);
	cl_wp.timeout_ms = g_config->timeout_ms;
	cl_wp.record_ttl = 864000;

	// (0) delete & reinsert record to start afresh
	char *keyStr = "key_many_bins";
	cl_object o_key;
	citrusleaf_object_init_str(&o_key,keyStr);		
	int rsp = citrusleaf_delete(g_config->asc, g_config->ns, g_config->set, &o_key, &cl_wp);
	if (rsp != CITRUSLEAF_OK && rsp != CITRUSLEAF_FAIL_NOTFOUND) {
		citrusleaf_object_free(&o_key);		
		LOG("failed deleting test data rsp=%d",rsp);
		return -1;
	}

	// (1) set up stored procedure which will insert lot of bins
	as_result res;
	as_result_init(&res);
	rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
			g_config->package_name,"do_too_many_bins", NULL, g_config->timeout_ms, &res);  

	char *res_str = as_val_tostring(res.value); 
	LOG("Citrusleaf udf apply %s: %s", res.is_success ? "SUCCESS" : "FAILURE", res_str);
	free(res_str);
	as_result_destroy(&res);

	if (rsp != CITRUSLEAF_OK) {
		LOG("citrusleaf_run_udf failed as rsp=%d",rsp);
		return -1;
	}
	citrusleaf_object_free(&o_key);		
	return 0;
}

int do_udf_lua_functional_test() {

	// (1) Call a lua function that simply executes functional tests
	// lets try with a key that doesn't exist
	char *keyStr = "key_luafunc";
	cl_object o_key;
	citrusleaf_object_init_str(&o_key,keyStr);		
	as_result res;
	as_result_init(&res);
	int rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
			g_config->package_name, "do_lua_functional_test", NULL, 
			g_config->timeout_ms, &res);  

	char *res_str = as_val_tostring(res.value); 
	LOG("Citrusleaf udf apply %s: %s", res.is_success ? "SUCCESS" : "FAILURE", res_str);
	as_result_destroy(&res);
	free(res_str);

	if (rsp != CITRUSLEAF_OK) {
		LOG("citrusleaf_run_udf failed as rsp=%d",rsp);
		return -1;
	}
	citrusleaf_object_free(&o_key);	
	return 0;
} 	

int do_udf_return_type_test() {

	int errors = 0;

	cl_write_parameters cl_wp;
	cl_write_parameters_set_default(&cl_wp);
	cl_wp.timeout_ms = g_config->timeout_ms;
	cl_wp.record_ttl = 864000;

	// (0) set up key
	char *keyStr = "key_bin_return_type";
	cl_object o_key;
	citrusleaf_object_init_str(&o_key,keyStr);		

	as_list * arglist = NULL;

	as_result res;
	as_result_init(&res);

	/**
	 * NONE
	 */

	arglist = as_arraylist_new(1, 8);	
	as_list_add_string(arglist, "none");


	int rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
			g_config->package_name,"do_return_types", arglist, g_config->timeout_ms, &res); 

	if (rsp != CITRUSLEAF_OK) {
		LOG("citrusleaf_run_udf failed as rsp=%d",rsp);
		return -1;
	}


	LOG("nil: %s", res.is_success ? "SUCCESS" : "FAILURE");
	if ( res.is_success ) {
		if ( as_val_type(res.value) != AS_NIL ) {
			LOG("nil: invalid type (%d)", as_val_type(res.value));
			errors++;
		}
		char *str = as_val_tostring(res.value);
		LOG("do_udf_return_type: first return is %s",str);
		free(str);
	}

	as_val_destroy(arglist);

	as_result_destroy(&res);


	/**
	 * STRING
	 */

	arglist = as_arraylist_new(1, 8);	
	as_list_add_string(arglist, "string_primitive");

	as_result_init(&res);

	rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
			g_config->package_name,"do_return_types", arglist, g_config->timeout_ms, &res);  
	
	if (rsp != CITRUSLEAF_OK) {
		LOG("citrusleaf_run_udf failed as rsp=%d",rsp);
		return -1;
	}

	LOG("string: %s", res.is_success ? "SUCCESS" : "FAILURE");
	if ( res.is_success ) {
		if ( as_val_type(res.value) == AS_STRING ) {
			char * res_str = as_string_tostring((as_string *) res.value);
			if ( res_str ) {
				if (strcmp(res_str,"good") != 0) {
					LOG("string: expected=\"good\", actual=\"%s\"", res_str);
					errors++;
				}
			}
			else {
				LOG("string: NULL");
				errors++;
			}
		}
		else {
			LOG("string: invalid type (%d)", as_val_type(res.value));
			errors++;
		}
	}

	as_val_destroy(arglist);

	as_result_destroy(&res);
	

	/**
	 * POSITIVE INTEGER
	 */
	
	arglist = as_arraylist_new(1, 8);	
	as_list_add_string(arglist, "p_int_primitive");

	rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
			g_config->package_name,"do_return_types", arglist, g_config->timeout_ms, &res);  
	
	if (rsp != CITRUSLEAF_OK) {
		LOG("citrusleaf_run_udf failed as rsp=%d",rsp);
		return -1;
	}

	LOG("postive integer: %s", res.is_success ? "SUCCESS" : "FAILURE");
	if ( res.is_success ) {
		if ( as_val_type(res.value) == AS_INTEGER ) {
			int64_t ret_int = as_integer_toint((as_integer *) res.value);
			if ( ret_int != 5 ) {
				LOG("postive integer: expected=5, actual=%d", ret_int);
				errors++;
			}
		}
		else {
			LOG("postive integer: invalid type (%d)", as_val_type(res.value));
			errors++;
		}
	}

	as_val_destroy(arglist);
	as_val_destroy(res.value);
	as_result_destroy(&res);


	/**
	 * NAGATIVE INTEGER
	 */

	arglist = as_arraylist_new(1, 8);	
	as_list_add_string(arglist, "n_int_primitive");

	as_result_init(&res);

	rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
			g_config->package_name,"do_return_types", arglist, g_config->timeout_ms, &res);  
	
	if (rsp != CITRUSLEAF_OK) {
		LOG("citrusleaf_run_udf failed as rsp=%d",rsp);
		return -1;
	}

	LOG("negative integer: %s", res.is_success ? "SUCCESS" : "FAILURE");
	if ( res.is_success ) {
		if ( as_val_type(res.value) == AS_INTEGER ) {
			int64_t ret_int = as_integer_toint((as_integer *) res.value);
			if ( ret_int != -5 ) {
				LOG("negative integer: expected=-5, actual=%d", ret_int);
				errors++;
			}
		}
		else {
			LOG("postive integer: invalid type (%d)", as_val_type(res.value));
			errors++;
		}
	}

	as_val_destroy(arglist);

	as_result_destroy(&res);

	/**
	 * LIST
	 */

	arglist = as_arraylist_new(1, 8);	
	as_list_add_string(arglist, "bin_array");

	as_result_init(&res);

	rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
			g_config->package_name,"do_return_types", arglist, g_config->timeout_ms, &res);  
	
	if (rsp != CITRUSLEAF_OK) {
		LOG("citrusleaf_run_udf failed as rsp=%d",rsp);
		return -1;
	}

	LOG("list: %s", res.is_success ? "SUCCESS" : "FAILURE");
	if ( res.is_success ) {
		if ( as_val_type(res.value) == AS_LIST ) {
			as_list * ret_list = (as_list *) res.value;
			uint32_t sz = as_list_size(ret_list);
			if ( sz != 2 ) {
				LOG("list: expected=2 elements, actual=%d elements", sz);
				errors++;
			}
		}
		else {
			LOG("list: invalid type (%d)", as_val_type(res.value));
			errors++;
		}
	}

	as_val_destroy(arglist);

	as_result_destroy(&res);


	/**
	 * NESTED LIST
	 */

	arglist = as_arraylist_new(1, 8);	
	as_list_add_string(arglist, "bin_nested_list");
	as_result_init(&res);

	rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
			g_config->package_name,"do_return_types", arglist, g_config->timeout_ms, &res);  
	
	if (rsp != CITRUSLEAF_OK) {
		LOG("citrusleaf_run_udf failed as rsp=%d",rsp);
		return -1;
	}

	LOG("list: %s", res.is_success ? "SUCCESS" : "FAILURE");
	if ( res.is_success ) {
		if ( as_val_type(res.value) == AS_LIST ) {
			as_list * l1 = (as_list *) res.value;
			uint32_t l1_sz = as_list_size(l1);
			if ( l1_sz != 2 ) {
				LOG("list: expected=2 elements, actual=%d elements", l1_sz);
				errors++;
			}
			else {
				as_iterator l1_i;
				as_list_iterator_init(&l1_i, l1);
				bool l1_string = false;
				bool l1_list = false;
				while ( as_iterator_has_next(&l1_i) ) {
					as_val * l1_v = (as_val *) as_iterator_next(&l1_i);
					if ( as_val_type(l1_v) == AS_STRING ) {
						char * str = as_string_tostring((as_string *) l1_v);
						if ( strcmp(str,"string_resp") != 0 ) {
							LOG("list: expected=\"string_resp\", actual=\"\"", str);
							errors++;
						}
						else {
							l1_string = false;
						}
					}
					else if ( as_val_type(l1_v) == AS_LIST ) {
						as_list * l2 = (as_list *) l1_v;
						uint32_t l2_sz = as_list_size(l2);
						if ( l2_sz != 2 ) {
							LOG("list: expected=2 elements, actual=%d elements", l2_sz);
							errors++;
						}
						else {

							as_iterator l2_i; 
							as_list_iterator_init(&l2_i, l2);
							bool l2_string = false;
							bool l2_integer = false;
							while ( as_iterator_has_next(&l2_i) ) {
								as_val * l2_v = (as_val *) as_iterator_next(&l2_i);
								if ( as_val_type(l2_v) == AS_STRING ) {
									char * str = as_string_tostring((as_string *) l2_v);
									if ( strcmp(str,"yup") != 0 ) {
										LOG("list: expected=\"yup\", actual=\"\"", str);
										errors++;
									}
									else {
										l2_string = true;
									}
								}
								else if ( as_val_type(l2_v) == AS_INTEGER ) {
									int64_t i = as_integer_toint((as_integer *) l2_v);
									if ( i != 1 ) {
										LOG("list: expected=1, actual=%d", i);
										errors++;
									}
									else {
										l2_integer = true;
									}
								}
								else {
									LOG("list: unexpected type (%d)", as_val_type(l2_v));
									errors++;
								}
							}

							if ( l2_integer && l2_string ) {
								l1_list = true;
							}

							as_iterator_destroy(&l2_i);
						}
					}
					else {
						LOG("list: unexpected type (%d)", as_val_type(l1_v));
						errors++;
					}
				}
				as_iterator_destroy(&l1_i);
			}

			// do something with l1_string and l1_list
		}
		else {
			LOG("list: invalid type (%d)", as_val_type(res.value));
			errors++;
		}
	}

	as_val_destroy(arglist);

	as_result_destroy(&res);


	/**
	 * MAP
	 */

	arglist = as_arraylist_new(1, 8);	
	as_list_add_string(arglist, "bin_map");

	as_result_init(&res);

	rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
			g_config->package_name,"do_return_types", arglist, g_config->timeout_ms, &res);  
	
	if (rsp != CITRUSLEAF_OK) {
		LOG("citrusleaf_run_udf failed as rsp=%d",rsp);
		return -1;
	}

	LOG("map: %s", res.is_success ? "SUCCESS" : "FAILURE");
	if ( res.is_success ) {
		if ( as_val_type(res.value) == AS_MAP ) {
			as_map * m1 = (as_map *) res.value;
			uint32_t m1_sz = as_map_size(m1);
			if ( m1_sz != 4 ) {
				LOG("map: expected=2 entries, actual=%d entries", m1_sz);
				errors++;
			}
			else {
				as_string s;

				as_val * m1_s = as_map_get(m1,(as_val *) as_string_init(&s,"s",false));
				if ( as_val_type(m1_s) == AS_STRING ) {
					if ( strcmp(as_string_tostring((as_string*) m1_s), "abc") != 0 ) {
						LOG("map: expected=\"abc\", actual=\"%s\"", as_string_tostring((as_string*) m1_s));
						errors++;
					}
				}

				as_val * m1_i = as_map_get(m1,(as_val *) as_string_init(&s,"i", false));
				if ( as_val_type(m1_i) == AS_INTEGER ) {
					if ( as_integer_toint((as_integer *) m1_i) != 123 ) {
						LOG("map: expected=123, actual=%d", as_integer_toint((as_integer *) m1_i));
						errors++;
					}
				}

				as_val * m1_l = as_map_get(m1,(as_val *) as_string_init(&s,"l",false));
				if ( as_val_type(m1_l) == AS_LIST ) {
					if ( as_list_size((as_list *) m1_l) != 2 ) {
						LOG("map: expected=2 elements, actual=%d elements", as_list_size((as_list *) m1_l));
						errors++;
					}
				}

				as_val * m1_m = as_map_get(m1,(as_val *) as_string_init(&s,"m",false));
				if ( as_val_type(m1_m) == AS_MAP ) {
					if ( as_map_size((as_map *) m1_m) != 3 ) {
						LOG("map: expected=3 entries, actual=%d entries", as_map_size((as_map *) m1_m));
						errors++;
					}
					else {
						as_map * m2 = (as_map *) m1_m;

						as_val * m2_i = as_map_get(m2,(as_val *) as_string_init(&s,"i",false));
						if ( as_val_type(m2_i) == AS_INTEGER ) {
							if ( as_integer_toint((as_integer *) m2_i) != 456 ) {
								LOG("map: expected=456, actual=%d", as_integer_toint((as_integer *) m2_i));
								errors++;
							}
						}

						as_val * m2_s = as_map_get(m2,(as_val *) as_string_init(&s,"s",false));
						if ( as_val_type(m2_s) == AS_STRING ) {
							if ( strcmp(as_string_tostring((as_string*) m2_s), "def") != 0 ) {
								LOG("map: expected=\"def\", actual=\"%s\"", as_string_tostring((as_string*) m2_s));
								errors++;
							}
						}

						as_val * m2_l = as_map_get(m2,(as_val *) as_string_init(&s,"l",false));
						if ( as_val_type(m2_l) == AS_LIST ) {
							if ( as_list_size((as_list *) m2_l) != 3 ) {
								LOG("map: expected=3 elements, actual=%d elements", as_list_size((as_list *) m2_l));
								errors++;
							}
						}
					}
				}
			}
		}
		else {
			LOG("list: invalid type (%d)", as_val_type(res.value));
			errors++;
		}

	}

	as_val_destroy(arglist);
	as_result_destroy(&res);

	citrusleaf_object_free(&o_key);		
	
	return errors;
}

int do_udf_undefined_global() {

	// (0) let's try with an existing record
	char *keyStr = "key_badlua";
	cl_object o_key;
	citrusleaf_object_init_str(&o_key,keyStr);		

	cl_bin bins[1];
	strcpy(bins[0].bin_name, "a_bin");
	citrusleaf_object_init_str(&bins[0].object, "a_val");
	int rsp = citrusleaf_put(g_config->asc, g_config->ns, g_config->set, &o_key, bins, 1, 0 /*wp*/);
	citrusleaf_object_free(&bins[0].object);
	if (rsp != CITRUSLEAF_OK) {
		citrusleaf_object_free(&o_key);		
		LOG("failed inserting test data rsp=%d",rsp);
		return -1;
	}
	
	// (1) Call a lua function that generates a runtime error
	as_result res;
	as_result_init(&res);
	rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
		g_config->package_name, "do_undefined_global", NULL, 
		g_config->timeout_ms, &res);  
	if (rsp == CITRUSLEAF_OK) {
		LOG("failed: should return a failure but got %d instead",rsp);
		return -1;
	}
	char *res_str = as_val_tostring(res.value); 
	LOG("%s: %s", res.is_success ? "SUCCESS" : "FAILURE", res_str);
	free(res_str);

	as_result_destroy(&res);

	citrusleaf_object_free(&o_key);
		
    return 0;
} 	

int do_udf_blob_test() {

	char *keyStr = "key_blob1";
	cl_object o_key;
	citrusleaf_object_init_str(&o_key,keyStr);	

	as_list arglist;
	as_arraylist_init(&arglist, 3, 8);	
	// arg 1 -> bin name
	as_list_add_string(&arglist, "WRITE");
	as_list_add_string(&arglist, "bin1"); // bin to write
	as_list_add_integer(&arglist, 5); // len

	// (1) Call a lua function that writes this blob
	as_result res;
	as_result_init(&res);
	cl_rv rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
		g_config->package_name, "do_udf_blob", &arglist, 
		g_config->timeout_ms, &res);  

	if (rsp != CITRUSLEAF_OK) return -1;
	if (as_val_type(res.value) != AS_STRING) return(-1);
	char *res_str = as_string_tostring((as_string *) res.value); 
	LOG("%s: %s", res.is_success ? "SUCCESS" : "FAILURE", res_str);
	if (0 != strcmp("OK",res_str)) return(-1);

	as_result_destroy(&res);
	as_list_destroy(&arglist);

	// check that it got persisted

	as_arraylist_init(&arglist,3, 8);	
	// arg 1 -> bin name
	as_list_add_string(&arglist, "READ");
	as_list_add_string(&arglist, "bin1"); // bin to write
	as_list_add_integer(&arglist, 5); // len

	// (1) Call a lua function that writes this blob
	as_result_init(&res);
	rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
		g_config->package_name, "do_udf_blob", &arglist, 
		g_config->timeout_ms, &res);  

	if (rsp != CITRUSLEAF_OK) 	return -1;
	if (as_val_type(res.value) != AS_STRING) return(-1);
	res_str = as_string_tostring((as_string *)res.value); 
	LOG("%s: %s", res.is_success ? "SUCCESS" : "FAILURE", res_str);
	if (0 != strcmp("OK",res_str)) return(-1);

	as_result_destroy(&res);

	as_list_destroy(&arglist);

	return(0);
}


int do_udf_blob_unit_test() {

	char *keyStr = "key_blob_unit";
	cl_object o_key;
	citrusleaf_object_init_str(&o_key,keyStr);	

	as_list arglist;
	as_arraylist_init(&arglist, 3, 8);	
	// arg 1 -> bin name
	as_list_add_string(&arglist, "WRITE");

	// (1) Call a lua function that writes this blob
	as_result res;
	as_result_init(&res);
	cl_rv rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
		g_config->package_name, "do_udf_blob_unit", &arglist, 
		g_config->timeout_ms, &res);  

	if (rsp != CITRUSLEAF_OK) 	return -1;
	if (as_val_type(res.value) != AS_STRING) return(-1);
	char *res_str = as_string_tostring((as_string *)res.value); 
	LOG("%s: %s", res.is_success ? "SUCCESS" : "FAILURE", res_str);
	if (0 != strcmp("OK",res_str)) return(-1);

	as_result_destroy(&res);
	as_list_destroy(&arglist);

	// check that it got persisted

	as_arraylist_init(&arglist,3, 8);	
	// arg 1 -> bin name
	as_list_add_string(&arglist, "READ");

	// (1) Call a lua function that writes this blob
	as_result_init(&res);
	rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
		g_config->package_name, "do_udf_blob_unit", &arglist, 
		g_config->timeout_ms, &res);  

	if (rsp != CITRUSLEAF_OK) return(-1);
	if (as_val_type(res.value) != AS_STRING) return(-1);
	res_str = as_string_tostring((as_string *)res.value); 
	LOG("%s: %s", res.is_success ? "SUCCESS" : "FAILURE", res_str);
	if (0 != strcmp("OK",res_str)) return(-1);


	as_result_destroy(&res);

	as_list_destroy(&arglist);

	return(0);
}

uint8_t test_bytes1[] = { 0x45, 0x56, 0x67, 0x68, 0x89 };

uint8_t test_bytes2[] = { 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7};

int do_udf_blob_list_unit_test() {

	char *keyStr = "key_blob_list_unit";
	cl_object o_key;
	citrusleaf_object_init_str(&o_key,keyStr);	

	as_list arglist;
	as_arraylist_init(&arglist, 3, 8);	
	// arg 1 -> action
	as_list_add_string(&arglist, "WRITE");

	// arg 2 -> fancy list of bytes
    as_list * lob = as_arraylist_new(2, 0);
    as_bytes * b1 = as_bytes_new(test_bytes1,sizeof(test_bytes1),false/*ismalloc*/);
    as_bytes * b2 = as_bytes_new(test_bytes2,sizeof(test_bytes2),false/*ismalloc*/);
    as_list_set(lob,0,(as_val *)b1);
    as_list_set(lob,1,(as_val *)b2);

    as_list_set(&arglist, 1, (as_val *) lob);

    // arg 3 -> fancy map of bytes
    as_map * mob = as_hashmap_new(5);
    as_string * k1 = as_string_new("key1",false /*ismalloc*/);
    as_string * k2 = as_string_new("key2",false /*ismalloc*/);
    as_val_reserve(b1);
    as_map_set(mob,(as_val *)k1,(as_val *)b1);
    as_val_reserve(b2);
    as_map_set(mob,(as_val *)k2,(as_val *)b2);

    as_list_set(&arglist, 2, (as_val *) mob);

	// (1) Call a lua function that writes this blob
	as_result res;
	as_result_init(&res);
	cl_rv rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
		g_config->package_name, "do_udf_blob_list_unit", &arglist, 
		g_config->timeout_ms, &res);  

	if (rsp != CITRUSLEAF_OK) 	return -1;
	if (as_val_type(res.value) != AS_STRING) return(-1);
	char *res_str = as_string_tostring((as_string *)res.value); 
	LOG("%s: %s", res.is_success ? "SUCCESS" : "FAILURE", res_str);
	if (0 != strcmp("OK",res_str)) return(-1);

	as_result_destroy(&res);
	// as_list_destroy(&arglist); // reuse this later

	// check that it got persisted

	as_arraylist_init(&arglist,3, 8);	
	// arg 1 -> bin name
	as_list_set(&arglist, 0, (as_val *) as_string_new( "READ", false /*ismalloc*/ ) );

	// (1) Call a lua function that writes this blob
	as_result_init(&res);
	rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
		g_config->package_name, "do_udf_blob_list_unit", &arglist, 
		g_config->timeout_ms, &res);  

	if (rsp != CITRUSLEAF_OK) return(-1);
	if (as_val_type(res.value) != AS_STRING) return(-1);
	res_str = as_string_tostring((as_string *)res.value); 
	LOG("%s: %s", res.is_success ? "SUCCESS" : "FAILURE", res_str);
	if (0 != strcmp("OK",res_str)) return(-1);


	as_result_destroy(&res);

	as_list_destroy(&arglist);

	return(0);
}

int register_package() 
{ 
	INFO("Opening package file %s",g_config->package_file);  
	FILE *fptr = fopen(g_config->package_file,"r"); 
	if (!fptr) { 
		LOG("cannot open script file %s : %s",g_config->package_file,strerror(errno));  
		return(-1); 
	} 
	int max_script_len = 1048576; 
	byte *script_code = (byte *)malloc(max_script_len); 
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
		int resp = citrusleaf_udf_put(g_config->asc, basename(g_config->package_file), &udf_content, AS_UDF_LUA, &err_str); 
		if (resp!=0) { 
			INFO("unable to register package file %s as %s resp = %d",g_config->package_file,g_config->package_name,resp); return(-1);
			INFO("%s",err_str); free(err_str);
			free(script_code);
			return(-1);
		}
		INFO("successfully registered package file %s as %s",g_config->package_file,g_config->package_name); 
	} else {   
		INFO("unable to read package file %s as %s b_tot = %d",g_config->package_file,g_config->package_name,b_tot); return(-1);    
	}
	free(script_code);
	return 0;
}
//
// test cases created by a gaming customer
//
const char *ORDER_SET = "Order";
const int TEST_COUNT = 4;

const char *GREE_FUNCS = "udf_unit_test";
const char *MY_TEST = "game_my_test";
const char *MY_FOREACH = "game_foreach";
const char *MY_COPY = "game_copy";
const char *MY_ECHO = "game_echo";
const char *MY_META = "game_meta";
const char *MY_DOUBLE_STR = "game_double_str";
const char *MY_INC = "game_inc";

static int lastOrderID;

int game_next_order_id(){
	int 			nextID = -1;
	cl_rv 			rv;
	cl_object		key;
	cl_operation 	ops[1];
	uint32_t		generation;

	citrusleaf_object_init_str(&key, ORDER_SET);
	ops[0].op = CL_OP_INCR;
	strcpy(ops[0].bin.bin_name, "nextID");
	citrusleaf_object_init_int(&ops[0].bin.object, 1);
	if (0 != (rv = citrusleaf_operate(g_config->asc, g_config->ns, "IDtable", &key, ops, 1, 0, 0, &generation))) {
		LOG("get nextID failed: %d",rv);
		return(-1);
	}

	cl_bin bin;
	strcpy(&bin.bin_name[0], "nextID");
	citrusleaf_object_init(&bin.object);
	cl_write_parameters	cl_wp;
	cl_write_parameters_set_default(&cl_wp);
	cl_write_parameters_set_generation_gt(&cl_wp, generation);
	rv = citrusleaf_get(g_config->asc, g_config->ns, "IDtable", &key, &bin, 1, 0, &generation);
	nextID = bin.object.u.i64;
	LOG("got nextID of %d:", nextID);
	lastOrderID = nextID;
	return nextID;
}

cl_object game_make_order_key(int id){
	cl_object order_key;
	citrusleaf_object_init_int(&order_key, id);
	return order_key;
}

int game_create_order(char *customer_name, char *stock_name, char *type, int quantity, int price){
	int rv;

	cl_object	key;
	int orderID = game_next_order_id();
	if (orderID == -1)
		return -1;
	key = game_make_order_key(orderID);
	cl_bin bins[6];
	strcpy(&bins[0].bin_name[0], "OrderID");
	citrusleaf_object_init_int(&bins[0].object, orderID);

	strcpy(&bins[1].bin_name[0], "StockName");
	citrusleaf_object_init_str(&bins[1].object, stock_name);

	strcpy(&bins[2].bin_name[0], "CustomerName");
	citrusleaf_object_init_str(&bins[2].object, customer_name);

	strcpy(&bins[3].bin_name[0], "Price");
	citrusleaf_object_init_int(&bins[3].object, price);

	strcpy(&bins[4].bin_name[0], "Quantity");
	citrusleaf_object_init_int(&bins[4].object, quantity);

	strcpy(&bins[5].bin_name[0], "type");
	citrusleaf_object_init_str(&bins[5].object, type);


	cl_write_parameters cl_wp;
	cl_write_parameters_set_default(&cl_wp);
	cl_wp.timeout_ms = 1000;


	if (CITRUSLEAF_OK != (rv = citrusleaf_put(g_config->asc, g_config->ns, ORDER_SET, &key, bins, 6, &cl_wp))) {
		LOG("Create order failed: error %d",rv);
	} else {
		LOG("%s's %s Order for %d %s at %d submitted with id: %d", customer_name, type, quantity, stock_name, price, orderID);
	}
	return orderID;
}

int game_create_holding(char *customer_name, char *stock_name, int quantity, int price){
	int rv;

	cl_object	key;
	cl_bin bins[4];

	char holdingSetStr[50];
	strcpy(holdingSetStr, customer_name);
	strcat(holdingSetStr, "Holding");

	char holdingKeyStr[50];
	strcpy(holdingKeyStr, customer_name);
	strcat(holdingKeyStr, stock_name);
	citrusleaf_object_init_str(&key, holdingKeyStr);

	strcpy(&bins[0].bin_name[0], "CustomerName");
	citrusleaf_object_init_str(&bins[0].object, customer_name);

	strcpy(&bins[1].bin_name[0], "StockName");
	citrusleaf_object_init_str(&bins[1].object, stock_name);

	strcpy(&bins[2].bin_name[0], "Quantity");
	citrusleaf_object_init_int(&bins[2].object, quantity);

	strcpy(&bins[3].bin_name[0], "Price");
	citrusleaf_object_init_int(&bins[3].object, price);

	cl_write_parameters cl_wp;
	cl_write_parameters_set_default(&cl_wp);
	cl_wp.timeout_ms = 1000;


	if (CITRUSLEAF_OK != (rv = citrusleaf_put(g_config->asc, g_config->ns, holdingSetStr, &key, bins, 4, &cl_wp))) {
		LOG("Create Holding failed: error %d",rv);
	} else {
		LOG("%s's holding of %d %s at %d created in set %s", customer_name, quantity, stock_name, price, holdingSetStr);
	}
	return rv;
}


int *game_create_holdings()
{
	LOG("Create n holdings");

	game_create_holding( "Pat", "CostLess", 300, 25);
	game_create_holding( "Pat", "MacDonna", 300, 25);
	game_create_holding( "Pat", "PacBella", 300, 25);
	game_create_holding( "Pat", "UnSafeway", 300, 25);

	game_create_holding( "Bill", "CostLess", 300, 25);
	game_create_holding( "Bill", "MacDonna", 300, 25);
	game_create_holding( "Bill", "PacBella", 300, 25);
	game_create_holding( "Bill", "UnSafeway", 300, 25);

	return 0;
}


int *game_create_orders()
{
	LOG("Create n Buy/Sell orders");


	// Create sell orders
	game_create_order("Pat", "CostLess", "Sell", 10, 50);
	game_create_order("Pat", "MacDonna", "Sell", 10, 50);
	game_create_order("Pat", "PacBella", "Sell", 10, 50);
	game_create_order( "Pat", "UnSafeway", "Sell", 10, 50);

	// Create buy orders
	game_create_order("Bill", "CostLess", "Buy", 10, 50);
	game_create_order("Bill", "MacDonna", "Buy", 10, 50);
	game_create_order("Bill", "PacBella", "Buy", 10, 50);
	game_create_order("Bill", "UnSafeway", "Buy", 10, 50);

	return 0;
}

int game_execute_udf(cl_object *key, const char *udf_name){
	cl_rv 		rv;
	as_result res;
	as_result_init(&res);
	rv = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, ORDER_SET, key,
			GREE_FUNCS,
			udf_name,
			(void *)0,
			g_config->timeout_ms,
			&res);

	if (rv != CITRUSLEAF_OK){
		LOG("Could not execute %s on: %ld Return code %d", udf_name, key->u.i64, rv);
		LOG("%s: %s", res.is_success ? "SUCCESS" : "FAILURE", as_val_tostring(res.value));
	} else {
		LOG("Executed %s on: %ld Return code %d", udf_name, key->u.i64, rv);
		LOG("%s: %s", res.is_success ? "SUCCESS" : "FAILURE", as_val_tostring(res.value));
	}
	as_result_destroy(&res);
	return rv;
}

int test_game_funcs()
{
	game_create_holdings();
	game_create_orders();
	for (int orderID = 1; orderID <= lastOrderID; orderID++){
		cl_bin 		*buy_bins = 0;

		int 		buy_bins_len = 0;


		cl_object	buy_order_key;

		cl_rv 		rv;
		uint32_t	generation;

		// get the order
		buy_order_key = game_make_order_key(orderID);
		rv = citrusleaf_get_all(g_config->asc, g_config->ns, ORDER_SET, &buy_order_key, &buy_bins, &buy_bins_len, g_config->timeout_ms, &generation);
		if (rv != CITRUSLEAF_OK){
			LOG("Could not retrieve order: %d Return code %d", orderID, rv);
			continue;
		} else {
			/*
			LOG("\nRetrieved order: %d", orderID);

			LOG("Bins: %s=%ld %s=%s %s=%s %s=%ld %s=%ld %s=%s",
					buy_bins[0].bin_name,
					buy_bins[0].object.u.i64,
					buy_bins[1].bin_name,
					buy_bins[1].object.u.str,
					buy_bins[2].bin_name,
					buy_bins[2].object.u.str,
					buy_bins[3].bin_name,
					buy_bins[3].object.u.i64,
					buy_bins[4].bin_name,
					buy_bins[4].object.u.i64,
					buy_bins[5].bin_name,
					buy_bins[5].object.u.str
			);
			*/

			/*
			 * Execute my_meta
			 *
			 * Causes server to crash. Log entries:
			 * Sep 25 2012 08:00:37 GMT: WARNING (udf): (base/rw_sproc.c:37) SprocWrapper: FAILED: msg: (/var/cache/citrusleaf//lua_include/gree_funcs.lua:39: attempt to call global 'citrusleaf_meta_set' (a nil value))
			 * Sep 25 2012 08:00:37 GMT: INFO (udf): (base/rw_sproc.c:372) run_sproc_on_rec failure -1
			 * --- does not seem to crash, but the meta function is no longer available
			 */
			// rv = game_execute_udf(c, &buy_order_key, MY_META);

			/*
			 * Execute my_echo
			 * Currrently not supported, returns null every time
			 */
//			rv = game_execute_udf(&buy_order_key, MY_ECHO);

			/*
			 * Execute my_inc
			 */
			rv = game_execute_udf(&buy_order_key, MY_INC);

			/*
			 * Execute my_copy
			 * causes server to crash
			 * --- validated
			 */
//			rv = game_execute_udf( &buy_order_key, MY_COPY);

			/*
			 * Execute my_double_str -- crashes server if trying to return the bin data
			 * Crashes server also when you do not try to return (different traces for both)
			 */
			for (int i=0;i<20;i++) {
				rv = game_execute_udf(&buy_order_key, MY_DOUBLE_STR);
			}

			/*
			 * Execute my_double_str
			 * Crashes the server
			 */
			rv = game_execute_udf(&buy_order_key, MY_TEST);


			citrusleaf_object_free(&buy_order_key);
			free(buy_bins);
		}
	}
	return 0;
}

typedef struct test_def_s {
	const char * name;
	int (*run)();
} test_def;

#define test(func) {#func, func}

const test_def test_defs[] = {
	test(do_udf_read_bins_test),
	test(do_udf_bin_update_test),
	test(do_udf_trim_bin_test),
	test(do_udf_add_bin_test),
	test(do_udf_create_record_test),
	test(do_udf_noop_test),
	test(do_udf_copy_record_test),
	test(do_udf_return_type_test),
	test(do_udf_bin_type_test),
	// test(do_udf_long_bindata_test),
	// test(do_udf_long_biname_test),
	// test(do_udf_too_many_bins_test),
	test(do_udf_undefined_global),
	test(do_udf_lua_functional_test),
	test(do_udf_delete_bin_test),
	test(do_udf_delete_record_test),
	test(do_udf_blob_test),
	test(do_udf_blob_unit_test),
	test(do_udf_blob_list_unit_test),
	{ NULL, NULL }
};



int main(int argc, char **argv) {
	
	// reading parameters
	if (init_configuration(argc,argv) !=0 ) {
		return -1;
	}
	
	// setting up cluster
	INFO("Startup: host %s port %d ns %s set %s file %s",
			g_config->host, g_config->port, g_config->ns, g_config->set == NULL ? "" : g_config->set, g_config->package_file);

	citrusleaf_init();

	//citrusleaf_set_debug(true);

	// create the cluster object - attach
	cl_cluster *asc = citrusleaf_cluster_create();
	if (!asc) { 
		INFO("could not create cluster"); 
		return(-1); 
	}

	if (0 != citrusleaf_cluster_add_host(asc, g_config->host, g_config->port, g_config->timeout_ms)) {
		INFO("could not connect to host %s port %d",g_config->host,g_config->port);
		return(-1);
	}
	g_config->asc           = asc;

	// register our package. 
	if (register_package() !=0 ) {
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
		if ( test->run() ) {
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
	
	free(g_config);
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
