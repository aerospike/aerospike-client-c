/* *  Citrusleaf Stored Procedure Test Program
 *  rec_udf.c - Validates stored procedure functionality
 *
 *  Copyright 2012 by Citrusleaf.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */
#include <stdio.h>
#include "citrusleaf/cl_udf.h"
#include <citrusleaf/cf_random.h>
#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_log_internal.h>
#include <citrusleaf/cl_parsers.h>
#include "info_util.h"

typedef struct key_value_s {
	char key[128];
	char ** value;
	int elements;
} key_value;

// Sets the key value in the hashmap
void * set_cb(char * key, char * value, void * context) {
	
	// Make as_val for key and value
	as_val * k = (as_val *) as_string_new((char *) key, false);
	as_val * v = (as_val *) as_string_new(strdup(value), true);

	// Set k, v in the map
	as_map_set((as_map*)context, k, v);

	// Return the updated map again
	return context;
}

// Splits all 'key=value' strings into key and value pairs and call set_cb on them
void * split_and_set_cb(char * data, void * context) {

	// Split and call map set callback	
	cl_parameters_parser parser = {
		.delim = ',',
		.context = context,
		.callback = set_cb
	};
	cl_parameters_parse(data, &parser);
	
	// Return the updated map back
	return context;
}

// Parses the response received for the command at each node and splits the resulting string
// into 'key=value' strings and calls split_and_set_cb on them
static bool parse_response( const as_node * cn, const char * query, char * value, void * udata) {

	key_value * kv = (key_value *)udata;

	// Define a map to store the key, value pairs thus parsed
	as_map *map = (as_map *) as_hashmap_new(1024);

	// Skip the query and only take the response
	char * response = strchr(value, '\t') + 1;

	// Split the response string and set the key value pairs in the map
	cl_seq_parser parser = {
		.delim = ';',
		.context = map,
		.callback = split_and_set_cb
	};
	cl_seq_parse(response, &parser);

	// Make value out of key
	as_val *  k = (as_val*) as_string_new(kv->key, false);
	char * r = as_val_tostring(as_map_get(map, k));
	char * d = r;

	// Strip quotes off the string
    int len = (int)strlen(r);
    char * buf = (char*)calloc(1, len - 1);
    char * v = buf;
    for(int i = 0; i<len; i++) {
        if(*r == '"') {
            r++;
        }
        else {
            *buf++ = *r++;
        }
    }

	// update the key value structure to be sent back
	(kv->value)[kv->elements] = (char*) calloc(1, len - 1);
	memcpy((kv->value)[kv->elements], v, len - 1);
	kv->elements++;
	
	// Free memory used
	free(value);
	free(v);
	free(d);
	as_val_destroy(k);
	as_map_destroy(map);
	return true;	
}

// Get stats from all the nodes in the cluster and returns an array of strings
char **get_stats(char * query, char * key, as_cluster * asc) {
	
	// Initialize the key_value structure
	key_value kv;
	memset(&kv, 0, sizeof(key_value));
	memcpy(kv.key, key, strlen(key));
		
	// Allocate space assuming 64 nodes
	kv.value = (char**)calloc(1, 64 * sizeof(char*));

	// Info the cluster with the query for the result
	char* error = 0;
	int rc = citrusleaf_info_cluster_foreach(asc, query, true, false, 0, &kv, &error, parse_response);

	if (rc) {
		cf_warn("Error get_stats (%d): %s", rc, error);
		free(error);
	}
	// Caller's responsibility to free value
	return kv.value;
}
