/*
 * Copyright 2008-2016 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
#include <aerospike/aerospike_index.h>
#include <aerospike/aerospike_info.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_log.h>
#include <citrusleaf/alloc.h>

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_status
aerospike_index_create_complex(
	aerospike* as, as_error* err, as_index_task* task, const as_policy_info* policy,
	const as_namespace ns, const as_set set, const as_index_position position, const char* name,
	as_index_type itype, as_index_datatype dtype)
{
	as_error_reset(err);
	
	const char* dtype_string;
    switch (dtype) {
		case AS_INDEX_NUMERIC:
			dtype_string = "NUMERIC";
			break;
		case AS_INDEX_GEO2DSPHERE:
			dtype_string = "GEO2DSPHERE";
			break;
		default:
		case AS_INDEX_STRING:
			dtype_string = "STRING";
			break;
    }
	
	const char* itype_string;
	switch (itype) {
		default:
		case AS_INDEX_TYPE_DEFAULT: {
			itype_string = "DEFAULT";
			break;
		}
		case AS_INDEX_TYPE_LIST: {
			itype_string = "LIST";
			break;
		}
		case AS_INDEX_TYPE_MAPKEYS: {
			itype_string = "MAPKEYS";
			break;
		}
		case AS_INDEX_TYPE_MAPVALUES: {
			itype_string = "MAPVALUES";
			break;
		}
	}
	
    char command[1024];
	int count;
    
	if (itype == AS_INDEX_TYPE_DEFAULT) {
		// Use old format, so command can work with older servers.
		count = snprintf(command, sizeof(command),
						 "sindex-create:ns=%s%s%s;indexname=%s;"
						 "numbins=1;indexdata=%s,%s;priority=normal\n",
						 ns, set ? ";set=" : "", set ? set : "",
						 name, position, dtype_string
						 );
	}
	else {
		// Use new format.
		count = snprintf(command, sizeof(command),
						 "sindex-create:ns=%s%s%s;indexname=%s;"
						 "numbins=1;indextype=%s;indexdata=%s,%s;priority=normal\n",
						 ns, set ? ";set=" : "", set ? set : "",
						 name, itype_string, position, dtype_string
						 );
	}
	
	if (++count >= sizeof(command)) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Index create buffer overflow: %d", count);
	}
	
	char* response = NULL;
	as_status status = aerospike_info_any(as, err, policy, command, &response);
	
	switch (status) {
		case AEROSPIKE_OK:
			// Return task that could optionally be polled for completion.
			if (task) {
				task->as = as;
				as_strncpy(task->ns, ns, sizeof(task->ns));
				as_strncpy(task->name, name, sizeof(task->name));
				task->done = false;
			}
			cf_free(response);
			break;
			
		case AEROSPIKE_ERR_INDEX_FOUND:
			// Index has already been created.  Do not need to poll for completion.
			if (task) {
				task->done = true;
			}
			status = AEROSPIKE_OK;
			as_error_reset(err);
			break;
			
		default:
			break;
	}
	return status;
}

static bool
aerospike_index_create_is_done(aerospike* as, as_error* err, as_policy_info* policy, char* command)
{
	// Index is not done if any node reports percent completed < 100.
	// Errors are ignored and considered done.
	bool done = true;
	as_nodes* nodes = as_nodes_reserve(as->cluster);
	
	for (uint32_t i = 0; i < nodes->size && done; i++) {
		as_node* node = nodes->array[i];
		
		char* response = 0;
		as_status status = aerospike_info_node(as, err, policy, node, command, &response);
		
		if (status == AEROSPIKE_OK) {
			char* find = "load_pct=";
			char* p = strstr(response, find);
			
			if (p) {
				p += strlen(find);
				char* q = strchr(p, ';');
				
				if (q) {
					*q = 0;
				}
				
				int pct = atoi(p);
				
				if (pct >= 0 && pct < 100) {
					done = false;
				}
			}
			cf_free(response);
		}
	}
	as_nodes_release(nodes);
	return done;
}

as_status
aerospike_index_create_wait(as_error* err, as_index_task* task, uint32_t interval_ms)
{
	if (task->done) {
		return AEROSPIKE_OK;
	}
	
	as_policy_info policy;
	policy.timeout = 1000;
	policy.send_as_is = false;
	policy.check_bounds = true;
	
	char command[1024];
	snprintf(command, sizeof(command), "sindex/%s/%s" , task->ns, task->name);
	
	uint32_t interval_micros = (interval_ms <= 0)? 1000 * 1000 : interval_ms * 1000;
	
	while (! task->done) {
		usleep(interval_micros);
		task->done = aerospike_index_create_is_done(task->as, err, &policy, command);
	}
	return AEROSPIKE_OK;
}

as_status
aerospike_index_remove(
	aerospike* as, as_error* err, const as_policy_info* policy, const char* ns, const char* name)
{
	as_error_reset(err);
	
	char command[1024];
	int count = snprintf(command, sizeof(command), "sindex-delete:ns=%s;indexname=%s", ns, name);
	
	if (++count >= sizeof(command)) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Index remove buffer overflow: %d", count);
	}
	
	char* response = NULL;
	as_status status = aerospike_info_any(as, err, policy, command, &response);
	
	switch (status) {
		case AEROSPIKE_OK:
			cf_free(response);
			break;
			
		case AEROSPIKE_ERR_INDEX_NOT_FOUND:
			status = AEROSPIKE_OK;
			as_error_reset(err);
			break;
			
		default:
			break;
	}
	return status;
}
