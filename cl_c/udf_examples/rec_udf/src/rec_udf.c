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
#include <citrusleaf/cl_udf.h>

static config *g_config = NULL;
void usage(int argc, char *argv[]) {
    fprintf(stderr, "Usage %s:\n", argv[0]);
    fprintf(stderr, "-h host [default 127.0.0.1] \n");
    fprintf(stderr, "-p port [default 3000]\n");
    fprintf(stderr, "-n namespace [default test]\n");
    fprintf(stderr, "-s set [default *all*]\n");
    fprintf(stderr, "-f udf_file [default lua_files/udf_unit_test.lua]\n");
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

	fprintf(stderr, "Starting Record stored-procedure Unit Tests\n");
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
		fprintf(stderr,"failed deleting test data rsp=%d\n",rsp);
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
		fprintf(stderr,"failed inserting test data rsp=%d\n",rsp);
		return -1;
	}

	// (2) set up stored procedure to call
	//uint32_t cl_gen;
	cl_bin *rsp_bins = NULL;
	int     rsp_n_bins = 0;
	as_result res;
	as_list * arglist = as_arglist_new(3);	
	// arg 1 -> bin name
	as_list_add_string(arglist, "bin_to_change");

	// arg #2 -> bin value
	as_list_add_string(arglist, "original_bin_val");

	rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
			g_config->package_name,"do_update_bin", arglist, g_config->timeout_ms, &res);  
	if (rsp != CITRUSLEAF_OK) {
		citrusleaf_object_free(&o_key);		
		fprintf(stderr,"failed citrusleaf_run_udf rsp=%d\n",rsp);
		return -1;
	}
	fprintf(stderr,"%s: %s\n", res.is_success ? "SUCCESS" : "FAILURE", as_val_tostring(res.value));
	as_list_free(arglist);
	arglist = NULL;

	// (3) verify record is updated by reading 4 times 
	for (int i=0; i<4; i++) {
		uint32_t cl_gen;
		rsp_bins = NULL;
		rsp_n_bins = 0;

		int rsp = citrusleaf_get_all(g_config->asc, g_config->ns, g_config->set, &o_key, &rsp_bins, &rsp_n_bins, g_config->timeout_ms, &cl_gen);  

		if (rsp != CITRUSLEAF_OK) {
			fprintf(stderr,"failed citrusleaf_get_all %d rsp=%d\n",i,rsp);
			citrusleaf_object_free(&o_key);		
			return -1;
		}

		for (int b=0; b<rsp_n_bins; b++) {
			fprintf(stderr,"validation read returned %s=[%s]\n",rsp_bins[b].bin_name,rsp_bins[b].object.u.str);
			if (strcmp(rsp_bins[b].bin_name,"bin_to_change") == 0) {
				if ( rsp_bins[b].object.type != CL_STR || strncmp(rsp_bins[b].object.u.str,"changed by lua", strlen("changed by lua")) != 0) {
					fprintf(stderr,"data validation failed on round %i\n",i);
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
			fprintf(stderr,"failed deleting test data %d rsp=%d\n",i,rsp);
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
			fprintf(stderr,"failed inserting test data %d rsp=%d\n",i,rsp);
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
		as_list * arglist = as_arglist_new(5);

		if (!arglist) {
			fprintf(stderr, "can't create udf_params\n");
			return(-1);
		}
		// Send information about limit on the string len
		as_list_add_string(arglist, "limits");
		as_list_add_string(arglist, "20");
		// Send the actual bin/value
		as_list_add_string(arglist, "id");
		as_list_add_string(arglist, keyStr);
		as_list_add_string(arglist, "cats");
		char *valStr = (i==0 ? "short line" : "longer than 10 character line");
		as_list_add_string(arglist, valStr);

		as_result res;
		int rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
				g_config->package_name, "do_trim_bin", arglist, g_config->timeout_ms, &res);  

		fprintf(stderr,"%s: %s\n", res.is_success ? "SUCCESS" : "FAILURE", as_val_tostring(res.value));
		if (rsp != CITRUSLEAF_OK) {
			citrusleaf_object_free(&o_key);		
			fprintf(stderr,"failed record_udf test data %d rsp=%d\n",i,rsp);
			return -1;
		}
		citrusleaf_object_free(&o_key);		

		for (int b=0; b<rsp_n_bins; b++) {
			if (rsp_bins[b].object.type == CL_STR) {
				fprintf(stderr,"udf returned record[%d] %s=%s\n",i,rsp_bins[b].bin_name,rsp_bins[b].object.u.str);
			} else {
				fprintf(stderr,"warning: expected string type but has object type %s=%d\n",rsp_bins[b].bin_name,rsp_bins[b].object.type);
			}
			citrusleaf_object_free(&rsp_bins[b].object);		
		}  	
		free(rsp_bins);	
		as_list_free(arglist);
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
			fprintf(stderr,"failed record_udf test data %d rsp=%d\n",i,rsp);
			citrusleaf_object_free(&o_key);		
			return -1;
		}
		citrusleaf_object_free(&o_key);		

		uint8_t fail = 0;
		for (int b=0; b<rsp_n_bins; b++) {
			if ( CL_STR == rsp_bins[b].object.type && 0 == strcmp(rsp_bins[b].bin_name,"cats")) {
				fprintf(stderr,"checking record[%d] %s=[%s]\n",i,rsp_bins[b].bin_name,rsp_bins[b].object.u.str);
				if ( (i==0 && strcmp(rsp_bins[b].object.u.str,"short line")!=0)
						||(i==1 && strcmp(rsp_bins[b].object.u.str,"new string")!=0) ) {				
					fail = 1;
				}
			}
			citrusleaf_object_free(&rsp_bins[b].object);		
		}  	
		if (fail) {
			fprintf(stderr,"data failed\n");
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
		fprintf(stderr,"failed deleting test data rsp=%d\n",rsp);
		return -1;
	}

	// (1) insert data with one existing bin
	cl_bin bins[1];
	strcpy(bins[0].bin_name, "old_bin");
	citrusleaf_object_init_str(&bins[0].object, "old_val");
	rsp = citrusleaf_put(g_config->asc, g_config->ns, g_config->set, &o_key, bins, 1, &cl_wp);
	if (rsp != CITRUSLEAF_OK) {
		fprintf(stderr,"failed inserting test data rsp=%d\n",rsp);
		ret = -1;
		goto Cleanup;
	}
	else {
		fprintf(stderr,"citrusleaf put succeeded\n");
	}

	// (2) execute the udf 
	cl_bin *rsp_bins = NULL;
	int     rsp_n_bins = 0;
	as_result res;
	uint32_t cl_gen;
	as_list * arglist = as_arglist_new(3);

	// arg 1 -> bin name
	as_list_add_string(arglist, "old_bin");

	// arg #2 -> bin value
	as_list_add_string(arglist, "old_val");

	rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
			g_config->package_name, "do_new_bin", NULL, 
			g_config->timeout_ms, &res);  
	
	fprintf(stderr,"%s: %s\n", res.is_success ? "SUCCESS" : "FAILURE", as_val_tostring(res.value));
	
	if (rsp != CITRUSLEAF_OK) {
		fprintf(stderr,"failed running udfrsp=%d\n",rsp);
		ret = -1;
		goto Cleanup;
	}
	as_list_free(arglist);
        arglist = NULL;

	// (3) verify bin is added 
	rsp = citrusleaf_get_all(g_config->asc, g_config->ns, g_config->set, &o_key, &rsp_bins, &rsp_n_bins, g_config->timeout_ms, &cl_gen);  
	if (rsp != CITRUSLEAF_OK) {
		fprintf(stderr,"failed getting record_udf test data rsp=%d\n",rsp);
		ret = -1;
		goto Cleanup;
	}
	if (rsp_n_bins !=2 ) {
		fprintf(stderr,"num bin returned not 2 %d\n",rsp_n_bins);
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
		fprintf(stderr,"failed deleting test data rsp=%d\n",rsp);
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
		fprintf(stderr,"failed inserting test data rsp=%d\n",rsp);
		return -1;
	}
	citrusleaf_object_free(&bins[0].object);
	citrusleaf_object_free(&bins[1].object);

	// (2) set up stored procedure to call
	cl_bin *rsp_bins = NULL;
	int     rsp_n_bins = 0;
	uint32_t cl_gen;
	as_result res;
	rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
			g_config->package_name, "do_copy_record", NULL, 
			g_config->timeout_ms, &res);  
	fprintf(stderr,"%s: %s\n", res.is_success ? "SUCCESS" : "FAILURE", as_val_tostring(res.value));
	
	if (rsp != CITRUSLEAF_OK) {
		citrusleaf_object_free(&o_key);		
		fprintf(stderr,"failed citrusleaf_run_udf rsp=%d\n",rsp);
		return -1;
	}

	// (4) call second UDF which will add one bin, update one bin, and delete one bin
	rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
			g_config->package_name, "do_updated_copy", NULL, 
			g_config->timeout_ms, &res);  
	if (rsp != CITRUSLEAF_OK) {
		citrusleaf_object_free(&o_key);		
		fprintf(stderr,"failed citrusleaf_run_udf rsp=%d\n",rsp);
		return -1;
	}
	bool isBad = false;
	rsp = citrusleaf_get_all(g_config->asc, g_config->ns, g_config->set, &o_key, &rsp_bins, &rsp_n_bins, g_config->timeout_ms, &cl_gen);  
	if (rsp_n_bins !=2 ) {
		fprintf(stderr,"num bin returned not 2 %d\n",rsp_n_bins);
		citrusleaf_object_free(&o_key);		
	}
	for (int i=0; i<rsp_n_bins; i++) {
		if (strcmp(rsp_bins[i].bin_name,"c_bin")==0) {
			if (rsp_bins[i].object.type != CL_STR 
			    || strcmp(rsp_bins[i].object.u.str, "new_value") !=0) {
				fprintf(stderr,"bin %d isn't matching [%s]\n",i,rsp_bins[i].bin_name);
			    isBad = true;
			}
		} else if (strcmp(rsp_bins[i].bin_name,"b_bin")==0) {
			if (rsp_bins[i].object.type != CL_INT 
				|| rsp_bins[i].object.u.i64 != 22 ) {
				fprintf(stderr,"bin %d isn't matching [%s]\n",i,rsp_bins[i].bin_name);
		    	isBad = true;
			}
		} else {
			fprintf(stderr,"unexpected bin [%s]\n",rsp_bins[i].bin_name);
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
		fprintf(stderr,"failed deleting test data rsp=%d\n",rsp);
		return -1;
	}

	// (2) execute the storedproc 
	cl_bin *rsp_bins = NULL;
	int     rsp_n_bins = 0;
	uint32_t cl_gen;
	as_result res;
	rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
			g_config->package_name, "do_add_record", NULL, 
			g_config->timeout_ms, &res);  
	fprintf(stderr,"%s: %s\n", res.is_success ? "SUCCESS" : "FAILURE", as_val_tostring(res.value));
	
	if (rsp != CITRUSLEAF_OK) {
		fprintf(stderr,"failed running udf = %d\n",rsp);
		ret = -1;
		goto Cleanup;
	}

	// (3) verify record and bin added 
	rsp = citrusleaf_get_all(g_config->asc, g_config->ns, g_config->set, &o_key, &rsp_bins, &rsp_n_bins, g_config->timeout_ms, &cl_gen);  
	if (rsp != CITRUSLEAF_OK) {
		fprintf(stderr,"failed adding record udf test data rsp=%d\n",rsp);
		ret = -1;
		goto Cleanup;
	}
	if (rsp_n_bins !=2 ) {
		fprintf(stderr,"num bin returned not 2 %d\n",rsp_n_bins);
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
		fprintf(stderr,"unexpected results\n");
		fprintf(stderr,"0 - %s %s\n",rsp_bins[0].bin_name, rsp_bins[0].object.u.str);
		fprintf(stderr,"1 - %s %s\n",rsp_bins[1].bin_name, rsp_bins[1].object.u.str);
	}  	
	fprintf(stderr,"0 - %s %s\n",rsp_bins[0].bin_name, rsp_bins[0].object.u.str);
	fprintf(stderr,"1 - %s %s\n",rsp_bins[1].bin_name, rsp_bins[1].object.u.str);
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
		fprintf(stderr,"failed inserting test data rsp=%d\n",rsp);
		return -1;
	}

	// (1) set up stored procedure to call
	cl_bin *rsp_bins = NULL;
	int     rsp_n_bins = 0;
	as_result res;
	rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
			g_config->package_name, "do_delete_record", NULL, 
			g_config->timeout_ms, &res);  
	fprintf(stderr,"%s: %s\n", res.is_success ? "SUCCESS" : "FAILURE", as_val_tostring(res.value));
	if (rsp != CITRUSLEAF_OK) {
		citrusleaf_object_free(&o_key);		
		fprintf(stderr,"failed citrusleaf_run_udf rsp=%d\n",rsp);
		return -1;
	}

	// (2) verify record does not exists by reading 4 times 
	for (int i=0; i<4; i++) {
		uint32_t cl_gen;
		rsp_bins = NULL;
		rsp_n_bins = 0;

		int rsp = citrusleaf_get_all(g_config->asc, g_config->ns, g_config->set, &o_key, &rsp_bins, &rsp_n_bins, g_config->timeout_ms, &cl_gen);  

		if (rsp != CITRUSLEAF_FAIL_NOTFOUND) {
			fprintf(stderr,"failed citrusleaf_get_all %d rsp=%d\n",i,rsp);
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
		fprintf(stderr,"failed inserting test data rsp=%d\n",rsp);
		return -1;
	}
	else {
		fprintf(stderr,"citrusleaf put succeeded\n");
	}

	// Check if the three bins that we inserted are present
	as_list * arglist = as_arglist_new(7);

	// arg 1 -> bin name
	as_list_add_string(arglist, "bin1");

	// arg #2 -> bin value
	as_list_add_string(arglist, "val1");
	as_list_add_string(arglist, "bin2");
	as_list_add_string(arglist, "val2");
	as_list_add_string(arglist, "bin3");
	as_list_add_string(arglist, "val3");

	// (2) call udf_record_apply - "do_read1_record" function in udf_unit_test.lua 
	as_result res;
	rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
			g_config->package_name, "do_read1_record", arglist, 
			g_config->timeout_ms, &res);  
	fprintf(stderr,"%s: %s\n", res.is_success ? "SUCCESS" : "FAILURE", as_val_tostring(res.value));
	as_list_free(arglist);
	arglist = NULL;
	if (rsp != CITRUSLEAF_OK) {
		fprintf(stderr,"failed citrusleaf_run_udf rsp=%d\n",rsp);
		cf_atomic_int_incr(&g_config->fail);
	} else {
		cf_atomic_int_incr(&g_config->success);
	}
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
	int rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
			g_config->package_name, "do_noop_function", NULL, 
			g_config->timeout_ms, &res); 

	fprintf(stderr,"%s: %s\n", res.is_success ? "SUCCESS" : "FAILURE", as_val_tostring(res.value));
	
	// (3) verify key is still not found 
	rsp = citrusleaf_get_all(g_config->asc, g_config->ns, g_config->set, &o_key, &rsp_bins, &rsp_n_bins, g_config->timeout_ms, &cl_gen);  
	if (rsp != CITRUSLEAF_FAIL_NOTFOUND) {
		citrusleaf_object_free(&o_key);		
		fprintf(stderr,"failed getting record_udf test data rsp=%d\n",rsp);
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
		fprintf(stderr,"failed deleting test data rsp=%d\n",rsp);
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
		fprintf(stderr,"failed inserting test data rsp=%d\n",rsp);
		return -1;
	}

	// (2) execute the udf 
	cl_bin *rsp_bins = NULL;
	int     rsp_n_bins = 0;
	uint32_t cl_gen;
	as_result res;
	rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
			g_config->package_name, "do_delete_bin", NULL, 
			g_config->timeout_ms, &res);  
	fprintf(stderr,"%s: %s\n", res.is_success ? "SUCCESS" : "FAILURE", as_val_tostring(res.value));
	if (rsp != CITRUSLEAF_OK) {
		fprintf(stderr,"failed running udf = %d\n",rsp);
		ret = -1;
		goto Cleanup;
	}

	// (3) verify bin is deleted
	rsp = citrusleaf_get_all(g_config->asc, g_config->ns, g_config->set, &o_key, &rsp_bins, &rsp_n_bins, g_config->timeout_ms, &cl_gen);  
	if (rsp != CITRUSLEAF_OK) {
		fprintf(stderr,"failed getting record_udf test data rsp=%d\n",rsp);
		ret = -1;
		goto Cleanup;
	}
	if (rsp_n_bins !=3 ) {
		fprintf(stderr,"num bin returned not 3 %d\n",rsp_n_bins);
		ret = -1;
		goto Cleanup;
	}

	bool isGood = true;
	for (int b=0; b<rsp_n_bins; b++) {
		if ( CL_STR == rsp_bins[b].object.type 
				&& 0 == strcmp(rsp_bins[b].bin_name,"bin3")) {
			//fprintf(stderr,"got it! record[%d] %s=[%s]\n",i,rsp_bins[b].bin_name,rsp_bins[b].object.u.str);
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

int register_package() 
{ 
	fprintf(stderr, "Opening package file %s\n",g_config->package_file);  
	FILE *fptr = fopen(g_config->package_file,"r"); 
	if (!fptr) { 
		fprintf(stderr, "cannot open script file %s : %s\n",g_config->package_file,strerror(errno));  
		return(-1); 
	} 
	int max_script_len = 1048576; 
	char *script_code = malloc(max_script_len); 
	if (script_code == NULL) { 
		fprintf(stderr, "malloc failed"); return(-1); 
	}     

	char *script_ptr = script_code; 
	int b_read = fread(script_ptr,1,512,fptr); 
	int b_tot = 0; 
	while (b_read) { 
		b_tot      += b_read; 
		script_ptr += b_read; 
		b_read      = fread(script_ptr,1,512,fptr); 
	}                        
	fclose(fptr); 

	char *err_str = NULL; 
	if (b_tot>0) { 
		int resp = citrusleaf_udf_put(g_config->asc, basename(g_config->package_file), script_code, &err_str); 
		if (resp!=0) { 
			fprintf(stderr, "unable to register package file %s as %s resp = %d\n",g_config->package_file,g_config->package_name,resp); return(-1);
			fprintf(stderr, "%s\n",err_str); free(err_str);
			return(-1);
		}
		fprintf(stderr, "successfully registered package file %s as %s\n",g_config->package_file,g_config->package_name); 
	} else {   
		fprintf(stderr, "unable to read package file %s as %s b_tot = %d\n",g_config->package_file,g_config->package_name,b_tot); return(-1);    
	}

	return 0;
}

int main(int argc, char **argv) {
	// reading parameters
	if (init_configuration(argc,argv) !=0 ) {
		return -1;
	}
	
	// setting up cluster
	fprintf(stderr, "Startup: host %s port %d ns %s set %s file %s\n",
			g_config->host, g_config->port, g_config->ns, g_config->set == NULL ? "" : g_config->set, g_config->package_file);

	citrusleaf_init();

	//citrusleaf_set_debug(true);

	// create the cluster object - attach
	cl_cluster *asc = citrusleaf_cluster_create();
	if (!asc) { fprintf(stderr, "could not create cluster\n"); return(-1); }
	if (0 != citrusleaf_cluster_add_host(asc, g_config->host, g_config->port, g_config->timeout_ms)) {
		fprintf(stderr, "could not connect to host %s port %d\n",g_config->host,g_config->port);
		return(-1);
	}
	g_config->asc           = asc;

	// register our package. 
	if (register_package() !=0 ) {
		return -1;
	}

	fprintf(stderr, "\n*** do_udf_read_bins_test started\n"); 
	if (do_udf_read_bins_test()) {
		fprintf(stderr, "*** do_udf_read_bins_test failed\n"); return(-1);
	} else {
		fprintf(stderr, "*** do_udf_read_bins_test succeeded\n"); 
	}

	fprintf(stderr, "\n*** do_udf_bin_update_test started\n"); 
	if (do_udf_bin_update_test()) {
		fprintf(stderr, "*** do_udf_bin_update_test failed\n"); return(-1);
	} else {
		fprintf(stderr, "*** do_udf_bin_update_test succeeded\n"); 
	}
	fprintf(stderr, "\n*** do_udf_trim_test started\n"); 
	if (do_udf_trim_bin_test()) {
		fprintf(stderr, "*** do_udf_trim_test failed\n"); //return(-1);
	} else {
		fprintf(stderr, "*** do_udf_trim_test succeeded\n"); 
	}

	fprintf(stderr, "\n*** do_udf_add_bin_test started\n"); 
	if (do_udf_add_bin_test()) {
		fprintf(stderr, "*** do_udf_add_bin_test failed\n"); return(-1);
	} else {
		fprintf(stderr, "*** do_udf_add_bin_test succeeded\n"); 
	}

	fprintf(stderr, "\n*** do_udf_create_record_test started\n"); 
	if (do_udf_create_record_test()) {
		fprintf(stderr, "do_udf_create_record_test failed\n"); return(-1);
	} else {
		fprintf(stderr, "*** do_udf_create_record_test succeeded\n"); 
	}

	fprintf(stderr, "\n*** do_udf_noop_test started\n"); 
	if (do_udf_noop_test()) {
		fprintf(stderr, "*** do_udf_noop_test failed\n"); return(-1);
	} else {
		fprintf(stderr, "*** do_udf_noop_test succeeded\n"); 
	}

	fprintf(stderr, "\n*** do_udf_copy_record_test started\n"); 
	if (do_udf_copy_record_test()) {
		fprintf(stderr, "do_udf_copy_record_test failed\n"); return(-1);
	} else {
		fprintf(stderr, "*** do_udf_copy_record_test succeeded\n"); 
	}

	fprintf(stderr, "\n*** do_udf_delete_bin_test started\n"); 
	if (do_udf_delete_bin_test()) {
		fprintf(stderr, "*** do_udf_delete_bin_test failed\n"); return(-1);
	} else {
		fprintf(stderr, "*** do_udf_delete_bin_test succeeded\n"); 
	}

	fprintf(stderr, "\n*** do_udf_delete_record_test started\n"); 
	if (do_udf_delete_record_test()) {
		fprintf(stderr, "do_udf_delete_record_test failed\n"); return(-1);
	} else {
		fprintf(stderr, "*** do_udf_delete_record_test succeeded\n"); 
	}
	
	citrusleaf_cluster_destroy(asc);
	citrusleaf_shutdown();

	fprintf(stderr, "\n\nFinished Record stored-procedure Unit Tests\n");
	return(0);
}
