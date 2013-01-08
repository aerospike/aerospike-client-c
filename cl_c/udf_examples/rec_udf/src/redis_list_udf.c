/* *  Citrusleaf Stored Procedure Test Program
 *  rlist.c - Validates stored procedure functionality
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

#include "redis_list_sproc.h"

static int run_redis_list_sproc(config *c, char *funcname,
                                char *binname, char *arg1, char *arg2) {
    uint32_t  cl_gen;
    cl_bin   *rsp_bins   = NULL;
    int       rsp_n_bins = 0;
    char     *keyStr     = "key1";
    cl_sproc_params *sproc_params = citrusleaf_sproc_params_create();
    if (!sproc_params) { printf("can't create sproc_params\n"); return -1; }
    citrusleaf_sproc_params_add_string(sproc_params, "binname", binname);
    if (arg1) citrusleaf_sproc_params_add_string(sproc_params, "arg1", arg1);
    if (arg2) citrusleaf_sproc_params_add_string(sproc_params, "arg2", arg2);
    cl_object o_key; citrusleaf_object_init_str(&o_key, keyStr);
    int rsp = citrusleaf_sproc_execute(c->asc, c->ns, c->set, &o_key, 
                                       c->package_name, funcname,
                                       sproc_params,    &rsp_bins,
                                       &rsp_n_bins,     c->timeout_ms,
                                       &cl_gen);  
    if (rsp != CITRUSLEAF_OK) {
        citrusleaf_object_free(&o_key);        
        printf("failed record_sproc test rsp=%d\n", rsp);
        return -1;
    }
    citrusleaf_object_free(&o_key);        
    printf("rsp_n_bins: %d\n", rsp_n_bins);
    for (int b = 0; b < rsp_n_bins; b++) {
        if (rsp_bins[b].object.type == CL_STR) {
            printf("sproc returned record %s=%s\n",
                    rsp_bins[b].bin_name, rsp_bins[b].object.u.str);
        } else {
            printf("warning: expected string type but object type %s=%d\n",
                   rsp_bins[b].bin_name, rsp_bins[b].object.type);
        }
        citrusleaf_object_free(&rsp_bins[b].object);        
    }
    free(rsp_bins);    
    citrusleaf_sproc_params_destroy(sproc_params);
    return 0;
}

static int do_rlist_test(config *c) {
    cl_write_parameters cl_wp;
    cl_write_parameters_set_default(&cl_wp);
    cl_wp.timeout_ms    = c->timeout_ms;
    cl_wp.record_ttl    = 864000;

    {
        char *keyStr = "key1";
        cl_object o_key;
        citrusleaf_object_init_str(&o_key,keyStr);        
        int rsp = citrusleaf_delete(c->asc, c->ns, c->set, &o_key, &cl_wp);
        if (rsp != CITRUSLEAF_OK && rsp != CITRUSLEAF_FAIL_NOTFOUND) {
            printf("failed deleting test rsp=%d\n", rsp); return -1;
        }
    }    
        
    {
        char *keyStr   = "key1";
        cl_object o_key; citrusleaf_object_init_str(&o_key,keyStr);        
        int   num_bins = 2;
        cl_bin bins[num_bins];
        strcpy(bins[0].bin_name, "id");
        citrusleaf_object_init_str(&bins[0].object, keyStr);
        char *elist    = "elist";
        strcpy(bins[1].bin_name, "elist");
        citrusleaf_object_init_str(&bins[1].object, elist);

        // inserting the data        
        int rsp = citrusleaf_put(c->asc, c->ns, c->set, &o_key, bins, num_bins,
                                 &cl_wp);
        // cleanup
        citrusleaf_object_free(&bins[0].object);
        citrusleaf_object_free(&bins[1].object);
        citrusleaf_object_free(&o_key);        
        if (rsp != CITRUSLEAF_OK) {
            printf("failed inserting test rsp=%d\n", rsp); return -1;
        }
    }
    
    if (run_redis_list_sproc(c, "RPUSH", "rlist", "1", NULL)  == -1) return -1;
    if (run_redis_list_sproc(c, "RPUSH", "rlist", "2", NULL)  == -1) return -1;
    if (run_redis_list_sproc(c, "RPUSH", "rlist", "3", NULL)  == -1) return -1;
    if (run_redis_list_sproc(c, "RPUSH", "rlist", "4", NULL)  == -1) return -1;
    if (run_redis_list_sproc(c, "RPUSH", "rlist", "5", NULL)  == -1) return -1;
    if (run_redis_list_sproc(c, "RPUSH", "rlist", "5", NULL)  == -1) return -1;
    if (run_redis_list_sproc(c, "RPUSH", "rlist", "5", NULL)  == -1) return -1;
    if (run_redis_list_sproc(c, "RPUSH", "rlist", "5", NULL)  == -1) return -1;
    if (run_redis_list_sproc(c, "RPUSH", "rlist", "6", NULL)  == -1) return -1;
    if (run_redis_list_sproc(c, "RPUSH", "rlist", "7", NULL)  == -1) return -1;
    if (run_redis_list_sproc(c, "RPUSH", "rlist", "8", NULL)  == -1) return -1;

    if (run_redis_list_sproc(c, "LPUSH", "rlist", "0", NULL)  == -1) return -1;
    if (run_redis_list_sproc(c, "LPUSH", "rlist", "-1", NULL) == -1) return -1;

    if (run_redis_list_sproc(c, "LLEN",  "rlist", NULL, NULL) == -1) return -1;

    if (run_redis_list_sproc(c, "LPOP",  "rlist", NULL, NULL) == -1) return -1;
    if (run_redis_list_sproc(c, "LPOP",  "rlist", NULL, NULL) == -1) return -1;
    if (run_redis_list_sproc(c, "RPOP",  "rlist", NULL, NULL) == -1) return -1;

    if (run_redis_list_sproc(c, "LREM",  "rlist", "2",  "5")  == -1) return -1;
    if (run_redis_list_sproc(c, "LSET",  "rlist", "3", "99")  == -1) return -1;
    if (run_redis_list_sproc(c, "LTRIM", "rlist", "5",  "2")  == -1) return -1;

/*
    for (int i = 0; i < num_sproc_calls; i++) {
        cl_sproc_params *sproc_params = citrusleaf_sproc_params_create();
        if (!sproc_params) { printf("can't create sproc_params\n"); return -1; }
        citrusleaf_sproc_params_add_string(sproc_params, "limits",
                                                          i ? "10" : "20");
           
        cl_object o_key; citrusleaf_object_init_str(&o_key,keyStr);        
        int rsp = citrusleaf_sproc_execute(c->asc, c->ns, c->set, &o_key, 
                                           c->package_name, "sp_doc_test",
                                           sproc_params, &rsp_bins,
                                           &rsp_n_bins, c->timeout_ms,
                                           &cl_gen);  
        if (rsp != CITRUSLEAF_OK) {
            citrusleaf_object_free(&o_key);        
            printf("failed record_sproc test data %d rsp=%d\n", i, rsp);
            return -1;
        }
        citrusleaf_object_free(&o_key);        
        for (int b = 0; b < rsp_n_bins; b++) {
            if (rsp_bins[b].object.type == CL_STR) {
                printf("sproc returned record[%d] %s=%s\n",
                        i, rsp_bins[b].bin_name,rsp_bins[b].object.u.str);
            } else {
                printf("warning: expected string type but object type %s=%d\n",
                       rsp_bins[b].bin_name,rsp_bins[b].object.type);
            }
            citrusleaf_object_free(&rsp_bins[b].object);        
        }      
        free(rsp_bins);    
        citrusleaf_sproc_params_destroy(sproc_params);
    }
    
    // (4) verify record is updated 
    for (int i = 0; i < num_records; i++) {
        uint32_t  cl_gen;
        cl_bin   *rsp_bins   = NULL;
        int       rsp_n_bins = 0;
        char     *keyStr     = "key1";
        cl_object o_key; citrusleaf_object_init_str(&o_key,keyStr);        
        int rsp = citrusleaf_get_all(c->asc, c->ns, c->set, &o_key, &rsp_bins,
                                     &rsp_n_bins, c->timeout_ms, &cl_gen);  
        if (rsp != CITRUSLEAF_OK) {
            printf("failed record_sproc test data %d rsp=%d\n", i, rsp);
            citrusleaf_object_free(&o_key); return -1;
        }
        citrusleaf_object_free(&o_key);        

        for (int b = 0; b < rsp_n_bins; b++) {
            char buf[rsp_bins[b].object.sz + 1];
            memcpy(buf, rsp_bins[b].object.u.str, rsp_bins[b].object.sz);
            buf[rsp_bins[b].object.sz] = '\0';
            if        (CL_LUA_BLOB  == rsp_bins[b].object.type) {
                printf("FOUND LUA: (%s)\n", buf);
            } else if (CL_STR       == rsp_bins[b].object.type) {
                printf("FOUND STRING: (%s)\n", buf);
            }
            citrusleaf_object_free(&rsp_bins[b].object);        
        }      
        free(rsp_bins);    
    }
*/
    return 0;
}

void usage(int argc, char *argv[]) {
    printf("Usage %s:\n", argv[0]);
    printf("-h host [default 127.0.0.1] \n");
    printf("-p port [default 3000]\n");
    printf("-n namespace [test]\n");
    printf("-s set [default *all*]\n");
    printf("-f package_file [lua_packages/sproc_unit_test.lua]\n");
    printf("-P package_name [sproc_unit_test] \n");
    printf("-v is verbose\n");
}

int main(int argc, char **argv) {
    config c; memset(&c, 0, sizeof(c));
    c.host         = "127.0.0.1";
    c.port         = 3000;
    c.ns           = "test";
    c.set          = 0;
    c.timeout_ms   = 1000;
    c.verbose      = true;
    c.package_file = "lua_packages/PackageRedisList.lua";
    c.package_name = "redis_list";
        
    printf("Starting DocumentStore stored-procedure Unit Tests\n");
    int optcase;
    while ((optcase = getopt(argc, argv, "ckmh:p:n:s:P:f:v:")) != -1) {
        switch (optcase) {
        case 'h': c.host         = strdup(optarg);          break;
        case 'p': c.port         = atoi(optarg);            break;
        case 'n': c.ns           = strdup(optarg);          break;
        case 's': c.set          = strdup(optarg);          break;
        case 'v': c.verbose      = true;                    break;
        case 'f': c.package_file = strdup(optarg);          break;
        case 'P': c.package_name = strdup(optarg);          break;
        default:  usage(argc, argv);                        return -1;
        }
    }

    printf( "Startup: host %s port %d ns %s set %s file %s\n",
            c.host, c.port, c.ns, c.set, c.package_file);
    citrusleaf_init();
    
    // create the cluster object - attach
    cl_cluster *asc = citrusleaf_cluster_create();
    if (!asc) { printf( "could not create cluster\n"); return -1; }
    if (citrusleaf_cluster_add_host(asc, c.host, c.port, c.timeout_ms)) {
        printf( "could not connect to host %s port %d\n", c.host, c.port);
        return -1;
    }
    c.asc           = asc;
    
    // register our package. 
    printf("Opening package file %s\n", c.package_file); 
    FILE *fptr = fopen(c.package_file,"r");
    if (!fptr) {
        printf("cannot open script file %s : %s\n",
               c.package_file, strerror(errno)); 
        return -1;
    }
    int   max_script_len = 1048576;
    char *script_code    = malloc(max_script_len);
    if (script_code == NULL) { printf( "malloc failed"); return -1; }    
    
    char *script_ptr = script_code;
    int   b_read     = fread(script_ptr,1,512,fptr);
    int   b_tot      = 0;
    while (b_read) {
        b_tot      += b_read;
        script_ptr += b_read;
        b_read      = fread(script_ptr,1,512,fptr);
    }            
    fclose(fptr);
    if (b_tot > 0) {
    	char *err_str = 0;
        int resp = citrusleaf_sproc_package_set(asc, c.package_name,
                                               script_code, &err_str, CL_SCRIPT_LANG_LUA);
        if (resp) {
            printf("unable to register package file %s as %s resp = %d\n",
                   c.package_file,c.package_name,resp);
            if (*err_str) { fprintf(stderr, "%s\n",err_str); free(err_str); }
            return -1;
        }
        printf("successfully registered package file %s as %s\n",
               c.package_file,c.package_name); 
        if (*err_str) free(*err_str);
    } else {   
        printf("unable to read package file %s as %s b_tot = %d\n",
               c.package_file,c.package_name,b_tot);
        return -1;    
    }
    if (do_rlist_test(&c)) {
        printf("FAILED: do_rlist_test\n"); return -1;
    } else {
        printf("SUCCESS: do_rlist_test\n");
    }
    citrusleaf_cluster_destroy(asc);
    printf("\n\nFinished DocumentStore stored-procedure Unit Tests\n");
    return 0;
}
