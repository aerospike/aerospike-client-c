/*
 * Copyright 2008-2023 Aerospike, Inc.
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
#include <aerospike/as_cdt_internal.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_log.h>
#include <aerospike/as_sleep.h>
#include <aerospike/as_string_builder.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_b64.h>
#include <stdlib.h>

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_status
aerospike_index_create_ctx(
	aerospike* as, as_error* err, as_index_task* task, const as_policy_info* policy, const char* ns,
	const char* set, const char* bin_name, const char* index_name, as_index_type itype,
	as_index_datatype dtype, as_cdt_ctx* ctx
	)
{
	as_error_reset(err);

	if (! policy) {
		policy = &as->config.policies.info;
	}

	const char* dtype_string;
	switch (dtype) {
		case AS_INDEX_NUMERIC:
			dtype_string = "NUMERIC";
			break;
		case AS_INDEX_BLOB:
			dtype_string = "BLOB";
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

	as_string_builder sb;
	as_string_builder_inita(&sb, 4096, false);
	as_string_builder_append(&sb, "sindex-create:ns=");
	as_string_builder_append(&sb, ns);

	if (set) {
		as_string_builder_append(&sb, ";set=");
		as_string_builder_append(&sb, set);
	}

	as_string_builder_append(&sb, ";indexname=");
	as_string_builder_append(&sb, index_name);

	if (ctx) {
		as_packer pk = {.buffer = NULL, .capacity = UINT32_MAX};

		if (as_cdt_ctx_pack(ctx, &pk) == 0) {
			return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to pack ctx");
		}

		char* context = cf_malloc(pk.offset);
		uint32_t b64_sz = cf_b64_encoded_len(pk.offset);

		char* b64 = cf_malloc(b64_sz + 1);
		pk.buffer = (uint8_t*)context;
		pk.offset = 0;
		as_cdt_ctx_pack(ctx, &pk);
		cf_b64_encode(pk.buffer, pk.offset, b64);
		b64[b64_sz] = 0;
		cf_free(context);

		as_string_builder_append(&sb, ";context=");
		as_string_builder_append(&sb, b64);
		cf_free(b64);
	}

	as_string_builder_append(&sb, ";indextype=");
	as_string_builder_append(&sb, itype_string);

	as_string_builder_append(&sb, ";indexdata=");
	as_string_builder_append(&sb, bin_name);
	as_string_builder_append_char(&sb, ',');
	as_string_builder_append(&sb, dtype_string);
	as_string_builder_append_newline(&sb);

	if (sb.length + 1 >= sb.capacity) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Index create buffer overflow: %d",
			sb.length);
	}

	char* response = NULL;
	as_status status = aerospike_info_any(as, err, policy, sb.data, &response);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	// Return task that could optionally be polled for completion.
	if (task) {
		task->as = as;
		as_strncpy(task->ns, ns, sizeof(task->ns));
		as_strncpy(task->name, index_name, sizeof(task->name));
		task->socket_timeout = policy->timeout;
		task->total_timeout = 30000;
		task->done = false;
	}
	cf_free(response);
	return status;
}

static as_status
aerospike_index_get_status(as_index_task* task, as_error* err, as_policy_info* policy, char* command)
{
	// Index is not done if any node reports percent completed < 100.
	as_nodes* nodes = as_nodes_reserve(task->as->cluster);

	if (nodes->size == 0) {
		as_nodes_release(nodes);
		return AEROSPIKE_OK;
	}
	
	for (uint32_t i = 0; i < nodes->size; i++) {
		as_node* node = nodes->array[i];
		
		char* response = NULL;
		as_status status = aerospike_info_node(task->as, err, policy, node, command, &response);

		if (status != AEROSPIKE_OK) {
			as_nodes_release(nodes);
			return status;
		}

		char* find = "load_pct=";
		char* p = strstr(response, find);

		if (!p) {
			as_error_update(err, AEROSPIKE_ERR_REQUEST_INVALID,
				"Create index error: %s", response);
			cf_free(response);
			as_nodes_release(nodes);
			return err->code;
		}

		p += strlen(find);
		char* q = strchr(p, ';');
		
		if (q) {
			*q = 0;
		}
		
		int pct = atoi(p);
		cf_free(response);

		if (pct < 100) {
			// Create index not complete. Stop checking other nodes.
			as_nodes_release(nodes);
			return status;
		}
	}

	task->done = true;
	as_nodes_release(nodes);
	return AEROSPIKE_OK;
}

as_status
aerospike_index_create_wait(as_error* err, as_index_task* task, uint32_t interval_ms)
{
	if (task->done) {
		return AEROSPIKE_OK;
	}
	
	as_policy_info policy;
	policy.timeout = task->socket_timeout;
	policy.send_as_is = false;
	policy.check_bounds = true;
	
	char command[1024];
	snprintf(command, sizeof(command), "sindex/%s/%s" , task->ns, task->name);
	
	if (! interval_ms) {
		interval_ms = 1000;
	}

	uint64_t deadline = as_socket_deadline(task->total_timeout);

	do {
		// Sleep first to give task a chance to complete.
		as_sleep(interval_ms);

		as_status status = aerospike_index_get_status(task, err, &policy, command);

		if (status != AEROSPIKE_OK || task->done) {
			return status;
		}

		// Check for timeout.
		if (deadline && cf_getms() + interval_ms > deadline) {
			// Timeout has been reached or will be reached after next sleep.
			return as_error_update(err, AEROSPIKE_ERR_TIMEOUT, "Timeout: %u", task->total_timeout);
		}
	} while (true);
}

as_status
aerospike_index_remove(
	aerospike* as, as_error* err, const as_policy_info* policy, const char* ns,
	const char* index_name
	)
{
	as_error_reset(err);
	
	char command[1024];
	int count = snprintf(command, sizeof(command), "sindex-delete:ns=%s;indexname=%s", ns, index_name);
	
	if (++count >= sizeof(command)) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Index remove buffer overflow: %d", count);
	}
	
	char* response = NULL;
	as_status status = aerospike_info_any(as, err, policy, command, &response);
	
	if (status != AEROSPIKE_OK) {
		return status;
	}

	cf_free(response);
	return status;
}
