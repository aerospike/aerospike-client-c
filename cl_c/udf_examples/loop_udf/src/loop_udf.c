/*
 *      loop_udf.c
 *
 * Copyright 2008-2013 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdbool.h>
#include <pthread.h>
#include <signal.h>

#include <citrusleaf/cf_random.h>
#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_hist.h>
#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/cl_udf.h>
#include <citrusleaf/cf_log_internal.h>

//#define DEBUG_VERBOSE 1
// #define PRINT_KEY 1

typedef struct config_s {
	
	char  *host;
	int    port;
	char  *ns;
	char  *set;
	uint32_t timeout_ms;
	uint32_t record_ttl; 
		
	char *package_file;
	char *package_name;
	char *f_name;
	
	int n_threads;	
	int start_key;     // where to start in the keyspace 
	int n_keys;        // keys picked will be [start_key,start_key+n_keys]

	int value_type;    // a CL type - integer or string or blob
	int key_type;      // a CL type - integer or string
	int key_len;
	int value_len;
	int rw_ratio;
	
	cl_cluster	*asc;		
		
	bool 	verbose; // verbose for our system
	bool	debug; // debug for the citrusleaf library
	int 	delay;

	cf_atomic_int success;
	cf_atomic_int fail;
	cf_atomic_int transactions;
			
} config;


static cf_histogram* g_read_histogram = NULL;
static cf_histogram* g_write_histogram = NULL;
static config *g_config = NULL;


// helper
static inline uint64_t safe_delta_ms(uint64_t start_ms, uint64_t stop_ms) {
    return start_ms > stop_ms ? 0 : stop_ms - start_ms;
}

/*
static void
dump_buf(char *msg, uint8_t *buf, size_t buf_len)
{
	fprintf(stderr, "dump_buf: %s\n",msg);
	uint i;
	for (i=0;i<buf_len;i++) {
		if (i % 16 == 8)
			fprintf(stderr, " : ");
		if (i && (i % 16 == 0))
			fprintf(stderr, "\n");
		fprintf(stderr, "%02x ",buf[i]);
	}
	fprintf(stderr, "\n");
}
*/

void *
counter_fn(void *arg)
{
	uint64_t t = 0;
	while (1) {
		sleep(1);
		cf_info("Transactions in the last second %ld",g_config->transactions - t);	
		cf_debug("Every sec check: total success %ld fail %ld",g_config->success,g_config->fail);
		cf_histogram_dump(g_read_histogram); 
		cf_histogram_dump(g_write_histogram); 
		t = g_config->transactions;
	}
	return(0);
}

void *
start_counter_thread()
{
	pthread_t tt;
	pthread_create(&tt, 0, counter_fn, NULL);
	
	return(NULL);
}

typedef struct key_value_s {
	
	uint64_t key_int;
	
	char 	*key_str;
	
	char *value_str; // this points to the area in memory allocated to the key_value (no free)
	
	int   value_blob_len;
	void *value_blob; // this points to the area in memory allocated to the key_value (no free)
	
	uint64_t value_int;
	
} key_value;


// Make a key and value
// When you're finished with it, just free the structure - the following strings
// are allocated as part of the structure
//
// Set up both value and integer ---
// 
static key_value *make_key_value(uint seed, uint key_len, uint value_len)
{
	int i;
	size_t	sz = sizeof(key_value) + key_len + value_len + 2 + value_len;
	key_value *kv = malloc(sz);
	if (!kv)	return(0);

	// save before molesting
	kv->key_int = kv->value_int = seed;
	
	// a little hinky, pointing internally to local memory, but so much more efficient
	kv->key_str = ((char *)kv)+ sizeof(key_value);
	kv->value_str = ((char *)kv)+ sizeof(key_value) + key_len + 1;
	kv->value_blob = kv->value_str + value_len + 1;
	kv->value_blob_len = value_len;
	
	// generate the string key
	kv->key_str[key_len] = 0;
	for (i = key_len-1; i >= 0; i--) {
		kv->key_str[i] = (seed % 10) + '0';
		seed = seed / 10;
	}
	
	// build a string out of the value, might be different from key
	// (optimization: don't build if integer test, 
	char value_str_short[key_len];
	seed = kv->value_int;
	value_str_short[key_len] = 0;
	for (i = key_len-1; i >= 0; i--) {
		value_str_short[i] = (seed % 10) + '0';
		seed = seed / 10;
	}
	
	// longer value likely required: multiple copies of value_str_short
	i=0;
	while (i < value_len) {
		if (value_len - i > key_len) {
			memcpy(&kv->value_str[i], value_str_short, key_len);
			i += key_len;
		}
		else {
			memcpy(&kv->value_str[i], value_str_short, value_len - i);
			i += (value_len - i);
		}
	}	
	kv->value_str[value_len] = 0;

	// And how's about a fine blob!
	uint64_t t = kv->value_int;
	memcpy(kv->value_blob, &t, sizeof(t)); // this will make sure some nulls, always a good idea
	memcpy(kv->value_blob + sizeof(t), kv->value_str,kv->value_blob_len - sizeof(t));

	
#ifdef DEBUG_VERBOSE
	fprintf(stderr, "make key value: key_len %d key %s value_len %d value_str %s value_int %"PRIu64"\n",key_len,kv->key_str,value_len,kv->value_str,kv->value_int);
#endif
	
	return(kv);	
}


void *
worker_fn(void *udata)
{
	// default write parameters	    
	cl_write_parameters cl_wp;
	cl_write_parameters_set_default(&cl_wp);
	cl_wp.timeout_ms = g_config->timeout_ms;
	cl_wp.record_ttl = g_config->record_ttl;
		
	uint32_t delay_factor = 0;
			
	while (1) {
	
		uint64_t rnumber = cf_get_rand64();
		uint key_int = ( rnumber % g_config->n_keys) + g_config->start_key;
		key_value *kv = make_key_value(key_int, g_config->key_len, g_config->value_len);
	
		// Create a key for accessing
		cl_object o_key;
		if (g_config->key_type == CL_STR) {
			citrusleaf_object_init_str(&o_key,kv->key_str);
		} else {
			citrusleaf_object_init_int(&o_key,kv->key_int);
		}

		// figure out if we should do a read or write based on read/write ratio
		bool isRead = true;
		if (rnumber % 100 > g_config->rw_ratio) {
			isRead = false;
		} 
		
        as_list * arglist = as_arglist_new(3);

        // arg 1 -> bin name
        as_list_add_string(arglist, "bin1");

        if ( !isRead ) {
            // arg #2 -> bin value
            as_list_add_string(arglist, kv->value_str);
        }

		// do the actual work
        as_result res;

        uint64_t start_time = cf_getms();
    	cl_rv rsp = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, g_config->package_name, g_config->f_name, arglist, g_config->timeout_ms, &res);

#ifdef DEBUG_VERBOSE
	fprintf(stderr,"%s: %s\n", res.is_success ? "SUCCESS" : "FAILURE", as_val_tostring(res.value));
#endif
        as_list_free(arglist);
        arglist = NULL;

		cf_histogram_insert_data_point(isRead ? g_read_histogram : g_write_histogram, start_time);		
		
		if (rsp != CITRUSLEAF_OK) {
			//fprintf(stderr,"failed citrusleaf_run_udf rsp=%d\n",rsp);
			fprintf(stderr,"Key_str is %s, key_int %ld\n",kv->key_str,kv->key_int);
			cf_atomic_int_incr(&g_config->fail);
		} else {
			cf_atomic_int_incr(&g_config->success);
		}
		cf_atomic_int_incr(&g_config->transactions);
		citrusleaf_object_free(&o_key);		
		free(kv);
		
		// do delay if any
		if (g_config->delay) {
			if (g_config->delay >= 1000) {
				usleep( (g_config->delay - 1000) * 1000 );
			}
			else {
				if ( (delay_factor++ % (1000 - g_config->delay)) == 0) {
					usleep( 1000 );
				}
			}
		}		
	}	
	return(0);
}



void usage(int argc, char *argv[]) {
    fprintf(stderr, "Usage %s:\n", argv[0]);
    fprintf(stderr, "-h host [default 127.0.0.1] \n");
    fprintf(stderr, "-p port [default 3000]\n");
    fprintf(stderr, "-n namespace [default test]\n");
    fprintf(stderr, "-s set [default *all*]\n");
    fprintf(stderr, "-f udf_file [default lua_files/udf_loop_test.lua]\n");
    fprintf(stderr, "-x f_name [default udf_loop_test] \n");    
    fprintf(stderr, "-v is verbose\n");
    fprintf(stderr, "-r read/write ratio (0-100) [default 80]\n");
    fprintf(stderr, "-t thread_count [default 8]\n");
    fprintf(stderr, "-i start_key [default 0]\n");
    fprintf(stderr, "-j n_keys [default 1000]\n");
	fprintf(stderr, "-d debug [default false]\n");
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
	g_config->debug		   = false;
	g_config->package_file = "../lua_files/udf_loop_test.lua";
	g_config->package_name = "udf_loop_test";
	g_config->n_threads    = 8;
	g_config->f_name       = "do_loop_test";
	g_config->value_type   = CL_STR;
	g_config->key_type     = CL_STR;
	g_config->start_key    = 0;
	g_config->n_keys       = 1000;
	g_config->key_len      = 100;
	g_config->value_len    = 128;
	g_config->rw_ratio     = 80;
	g_config->delay        = 0;
   	g_config->transactions = 0;             
	fprintf(stderr, "Starting Loop Test Record Sproc\n");
	int optcase;
	while ((optcase = getopt(argc, argv, "ckmh:p:n:s:P:f:v:x:r:t:i:j:d")) != -1) {
		switch (optcase) {
		case 'h': g_config->host         = strdup(optarg);          break;
		case 'p': g_config->port         = atoi(optarg);            break;
		case 'n': g_config->ns           = strdup(optarg);          break;
		case 's': g_config->set          = strdup(optarg);          break;
		case 'v': g_config->verbose      = true;                    break;
		case 'f': g_config->package_file = strdup(optarg);          break;
		case 'P': g_config->package_name = strdup(optarg);          break;
		case 'x': g_config->f_name       = strdup(optarg);          break;
		case 't': g_config->n_threads    = atoi(optarg);            break;
		case 'r': g_config->rw_ratio     = atoi(optarg);      
        	if (g_config->rw_ratio>100 || g_config->rw_ratio<0) {
			    fprintf(stderr, "rw_ratio must be within 0-100\n");
			    return -1;
        	}    
        	break;
		case 'i': g_config->start_key    = atoi(optarg);            break;
		case 'j': g_config->n_keys       = atoi(optarg);            break;
		case 'd': g_config->debug = true; break;
		default:  usage(argc, argv);                      return(-1);
        }
    }
    return 0;
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
    byte *script_code = (byte*) malloc(max_script_len);
    if (script_code == NULL) {
        fprintf(stderr, "malloc failed"); return(-1);
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
    as_bytes udf_content = {
	.size = b_tot,
	.data = script_code
    };
    char *err_str = NULL;
    if (b_tot>0) {
	    int resp = citrusleaf_udf_put(g_config->asc, basename(g_config->package_file), &udf_content, AS_UDF_LUA, &err_str);
	    if (resp!=0) {
		    fprintf(stderr, "unable to register package file %s as %s resp = %d\n",g_config->package_file,g_config->package_name,resp); return(-1);
		    fprintf(stderr, "%s\n",err_str); free(err_str);
		    free(script_code);
		    return(-1);
	    }
	    fprintf(stderr, "successfully registered package file %s as %s\n",g_config->package_file,g_config->package_name); 
    } else {   
	    fprintf(stderr, "unable to read package file %s as %s b_tot = %d\n",g_config->package_file,g_config->package_name,b_tot); return(-1);    
    }
    free(script_code); 
    return 0;
}


int main(int argc, char **argv) {

	// reading parameters
	if (init_configuration(argc,argv) !=0 ) {
		return -1;
	}
	if (g_config->debug) {
    	citrusleaf_set_debug(true);
	}
	
	// setting up cluster
    fprintf(stderr, "Startup: host %s port %d ns %s set %s file %s\n",
            g_config->host, g_config->port, g_config->ns, g_config->set == NULL ? "" : g_config->set, g_config->package_file);
    fprintf(stderr, "Run: n_threads %d start %d n_keys %d rw_ratio %d\n",
            g_config->n_threads, g_config->start_key, g_config->n_keys, g_config->rw_ratio);
    citrusleaf_init();
    
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
	
    // create and fire off a stats thread 
	start_counter_thread();		

    // create and fire off n worker threads
	pthread_t	*thr_array = malloc(sizeof(pthread_t) * g_config->n_threads);

    g_read_histogram = cf_histogram_create("r_udf");
    g_write_histogram = cf_histogram_create("w_udf");
    if (g_read_histogram==NULL || g_write_histogram==NULL) {
        fprintf(stderr," cannot create histograms \n");
        return -1;
    }    
	for (uint i=0;i<g_config->n_threads;i++) {			
        fprintf(stderr, "starting thread %d of %d\n",i, g_config->n_threads);
		pthread_create(&thr_array[i], 0, worker_fn, NULL); 
	}
	for (uint i=0;i<g_config->n_threads;i++) {
		void *value_ptr;
		pthread_join(thr_array[i], &value_ptr);
		if ( ((int)(ssize_t)value_ptr) )
			fprintf(stderr, "thread %d returned value %d\n",i,(int) (ssize_t) value_ptr);
	}

	citrusleaf_cluster_destroy(asc);
	
	free(thr_array);
	free(g_config);
	
}
