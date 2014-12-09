/*
 * Copyright 2008-2014 Aerospike, Inc.
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
#include <aerospike/as_log.h>
#include <citrusleaf/cl_sindex.h>
#include "_shim.h"

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 *	Create secondary index.
 *
 *	This asynchronous server call will return before the command is complete.
 *	The user can optionally wait for command completion by using a task instance.
 *
 *	~~~~~~~~~~{.c}
 *	as_index_task task;
 *	if ( aerospike_index_create(&as, &err, &task, NULL, "test", "demo", "bin1", "idx_test_demo_bin1") == AEROSPIKE_OK ) {
 *		aerospike_index_create_wait(&err, &task, 0);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param task			The optional task data used to poll for completion.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param ns			The namespace to be indexed.
 *	@param set			The set to be indexed.
 *	@param bin			The bin to be indexed.
 *	@param name			The name of the index.
 *
 *	@return AEROSPIKE_OK if successful or index already exists. Otherwise an error.
 *
 *	@ingroup index_operations
 */
as_status aerospike_index_create(
	aerospike * as, as_error * err, as_index_task * task, const as_policy_info * policy,
	const as_namespace ns, const as_set set, const as_index_position position, const char * name, as_index_type itype, as_index_datatype dtype)
{
	as_error_reset(err);
	
	const char* dtype_string = (dtype == AS_INDEX_DATA_NUMERIC)? "NUMERIC" : "STRING";
	const char* itype_string = NULL;
	switch (itype) {
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
		default:
			break;
	}

	char * response = NULL;
	int rc = citrusleaf_secondary_index_create(as->cluster, ns, set, name, position, itype_string, dtype_string, &response);
	
	switch (rc) {
		case AEROSPIKE_OK:
			// Return task that could optionally be polled for completion.
			if (task) {
				task->as = as;
				as_strncpy(task->ns, ns, sizeof(task->ns));
				as_strncpy(task->name, name, sizeof(task->name));
				task->done = false;
			}
			break;
			
		case AEROSPIKE_ERR_INDEX_FOUND:
			// Index has already been created.  Do not need to poll for completion.
			if (task) {
				task->done = true;
			}
			break;
			
		default:
			as_strncpy(err->message, response, sizeof(err->message));
			as_error_fromrc(err, rc);
			break;
	}
	
	free(response);
	return err->code;
}

static bool
aerospike_index_create_is_done(as_cluster* cluster, char* command)
{
	// Index is not done if any node reports percent completed < 100.
	// Errors are ignored and considered done.
	bool done = true;
	as_nodes* nodes = as_nodes_reserve(cluster);
	
	for (uint32_t i = 0; i < nodes->size && done; i++) {
		as_node* node = nodes->array[i];
		struct sockaddr_in* sa_in = as_node_get_address(node);
		
		char* response = 0;
		int rc = citrusleaf_info_host_auth(cluster, sa_in, command, &response, 1000, false, true);
		
		if (rc == AEROSPIKE_OK) {
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
		}
		free(response);
	}
	as_nodes_release(nodes);
	return done;
}

/**
 *	Wait for asynchronous task to complete using given polling interval.
 *
 *	@param err			The as_error to be populated if an error occurs.
 *	@param task			The task data used to poll for completion.
 *	@param interval_ms	The polling interval in milliseconds. If zero, 1000 ms is used.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup index_operations
 */
as_status
aerospike_index_create_wait(as_error * err, as_index_task * task, uint32_t interval_ms)
{
	if (task->done) {
		return AEROSPIKE_OK;
	}
	
	char command[256];
	snprintf(command, sizeof(command), "sindex/%s/%s" , task->ns, task->name);
	
	as_cluster* cluster = task->as->cluster;
	uint32_t interval_micros = (interval_ms <= 0)? 1000 * 1000 : interval_ms * 1000;
	
	while (! task->done) {
		usleep(interval_micros);
		task->done = aerospike_index_create_is_done(cluster, command);
	}
	return AEROSPIKE_OK;
}

/**
 *	Removes (drops) a secondary index.
 *
 *	~~~~~~~~~~{.c}
 *	if ( aerospike_index_remove(&as, &err, NULL, "test", idx_test_demo_bin1") != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param ns			The namespace containing the index to be removed.
 *	@param name			The name of the index to be removed.
 *
 *	@return AEROSPIKE_OK if successful or index does not exist. Otherwise an error.
 *
 *	@ingroup index_operations
 */
as_status aerospike_index_remove(
	aerospike * as, as_error * err, const as_policy_info * policy, 
	const char * ns, const char * name)
{
	as_error_reset(err);

	char * response = NULL;
	int rc = citrusleaf_secondary_index_drop(as->cluster, ns, name, &response);

	switch (rc) {
		case AEROSPIKE_OK:
		case AEROSPIKE_ERR_INDEX_NOT_FOUND:
			break;

		default:
			as_strncpy(err->message, response, sizeof(err->message));
			as_error_fromrc(err, rc);
			break;
	}
	
	free(response);
	return err->code;
}
