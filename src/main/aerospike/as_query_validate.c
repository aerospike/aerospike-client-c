/*
 * Copyright 2008-2022 Aerospike, Inc.
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
#include <aerospike/as_query_validate.h>
#include <aerospike/as_async.h>
#include <aerospike/as_event.h>
#include <aerospike/as_event_internal.h>
#include <aerospike/as_info.h>
#include <aerospike/as_node.h>
#include <limits.h>

uint32_t
as_query_get_info_timeout(as_event_executor* executor);

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static inline void
as_write_cluster_stable(char* cmd, size_t size, const char* ns)
{
	snprintf(cmd, size, "cluster-stable:namespace=%s\n", ns);
}

static inline as_status
as_parse_error(as_error* err, const char* response)
{
	return as_error_update(err, AEROSPIKE_ERR_CLIENT,
						   "Failed to parse cluster-stable results: %s", response);
}

static inline as_status
as_cluster_key_error(as_error* err, uint64_t expected_key, uint64_t cluster_key)
{
	return as_error_update(err, AEROSPIKE_ERR_CLUSTER_CHANGE,
						   "Cluster is in migration: %" PRIu64 ", %" PRIu64,
						   expected_key, cluster_key);
}

static bool
as_parse_cluster_key(char* response, uint64_t* cluster_key)
{
	char* value = NULL;
	as_status status = as_info_parse_single_response(response, &value);

	if (status != AEROSPIKE_OK) {
		return false;
	}

	errno = 0;
	*cluster_key = strtoull(value, NULL, 16);

	if (*cluster_key == 0 || (*cluster_key == ULLONG_MAX && errno)) {
		return false;
	}
	return true;
}

static void
as_validate_begin_listener(as_error* err, char* response, void* udata, as_event_loop* event_loop)
{
	as_event_command* cmd = udata;
	as_event_executor* executor = cmd->udata;

	if (err) {
		as_event_command_destroy(cmd);
		as_event_executor_error(executor, err, executor->max);
		return;
	}

	as_error e;

	if (! as_parse_cluster_key(response, &executor->cluster_key)) {
		as_parse_error(&e, response);
		as_event_command_destroy(cmd);
		as_event_executor_error(executor, &e, executor->max);
		return;
	}

	uint32_t max_concurrent = executor->max_concurrent;
	as_status status = as_event_command_execute(cmd, &e);

	if (status != AEROSPIKE_OK) {
		// Command already destroyed.
		as_event_executor_error(executor, &e, executor->max);
		return;
	}

	for (uint32_t i = 1; i < max_concurrent; i++) {
		if (as_query_validate_next_async(executor, i) != AEROSPIKE_OK) {
			return;
		}
	}
}

static void
as_validate_next_listener(as_error* err, char* response, void* udata, as_event_loop* event_loop)
{
	as_event_command* cmd = udata;
	as_event_executor* executor = cmd->udata;

	if (err) {
		as_event_command_destroy(cmd);
		as_event_executor_error(executor, err, 1);
		return;
	}

	as_error e;
	uint64_t cluster_key;

	if (! as_parse_cluster_key(response, &cluster_key)) {
		as_parse_error(&e, response);
		as_event_command_destroy(cmd);
		as_event_executor_error(executor, &e, 1);
		return;
	}

	if (executor->cluster_key != cluster_key) {
		as_cluster_key_error(&e, executor->cluster_key, cluster_key);
		as_event_command_destroy(cmd);
		as_event_executor_error(executor, &e, 1);
		return;
	}

	as_status status = as_event_command_execute(cmd, &e);

	if (status != AEROSPIKE_OK) {
		// Command already destroyed.
		as_event_executor_error(executor, &e, 1);
	}
}

static void
as_validate_end_listener(as_error* err, char* response, void* udata, as_event_loop* event_loop)
{
	as_event_executor* executor = udata;

	if (err) {
		as_event_executor_error(executor, err, 1);
		return;
	}

	uint64_t cluster_key;

	if (! as_parse_cluster_key(response, &cluster_key)) {
		as_error e;
		as_parse_error(&e, response);
		as_event_executor_error(executor, &e, 1);
		return;
	}

	if (executor->cluster_key != cluster_key) {
		as_error e;
		as_cluster_key_error(&e, executor->cluster_key, cluster_key);
		as_event_executor_error(executor, &e, 1);
		return;
	}

	as_event_executor_complete(executor);
}

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_status
as_query_validate_begin(
	as_error* err, as_node* node, const char* ns, uint32_t timeout, uint64_t* cluster_key
	)
{
	char cmd[256];
	as_write_cluster_stable(cmd, sizeof(cmd), ns);

	// Fail when cluster is in migration.
	uint64_t deadline = as_socket_deadline(timeout);
	char* response;
	as_status status = as_info_command_node(err, node, cmd, true, deadline, &response);

	if (status) {
		*cluster_key = 0;
		return status;
	}

	if (! as_parse_cluster_key(response, cluster_key)) {
		status = as_parse_error(err, response);
	}

	cf_free(response);
	return status;
}

as_status
as_query_validate(
	as_error* err, as_node* node, const char* ns, uint32_t timeout, uint64_t expected_key
	)
{
	if (expected_key == 0) {
		return AEROSPIKE_OK;
	}

	// Fail when cluster is in migration.
	uint64_t cluster_key;
	as_status status = as_query_validate_begin(err, node, ns, timeout, &cluster_key);

	if (status) {
		return status;
	}

	if (cluster_key != expected_key) {
		return as_cluster_key_error(err, expected_key, cluster_key);
	}
	return status;
}

as_status
as_query_validate_begin_async(as_event_executor* executor, const char* ns, as_error* err)
{
	as_policy_info policy;
	as_policy_info_init(&policy);
	policy.timeout = as_query_get_info_timeout(executor);

	executor->ns = cf_strdup(ns);
	executor->queued++;

	char info_cmd[256];
	as_write_cluster_stable(info_cmd, sizeof(info_cmd), ns);

	as_event_command* cmd = executor->commands[0];

	// Reserve node again because the node will be released at end of async info processing.
	// Node must be available for query.
	as_node_reserve(cmd->node);

	as_status status = as_info_command_node_async(NULL, err, &policy, cmd->node, info_cmd,
												  as_validate_begin_listener, cmd, cmd->event_loop);

	if (status != AEROSPIKE_OK) {
		as_event_command_destroy(cmd);
		as_event_executor_cancel(executor, 0);
	}
	return status;
}

as_status
as_query_validate_next_async(as_event_executor* executor, uint32_t index)
{
	as_error err;
	as_policy_info policy;
	as_policy_info_init(&policy);
	policy.timeout = as_query_get_info_timeout(executor);

	executor->queued++;

	char info_cmd[256];
	as_write_cluster_stable(info_cmd, sizeof(info_cmd), executor->ns);

	as_event_command* cmd = executor->commands[index];

	// Reserve node again because the node will be released at end of async info processing.
	// Node must be available for query.
	as_node_reserve(cmd->node);

	as_status status = as_info_command_node_async(NULL, &err, &policy, cmd->node, info_cmd,
									  as_validate_next_listener, cmd, cmd->event_loop);

	if (status != AEROSPIKE_OK) {
		as_event_command_destroy(cmd);
		as_event_executor_error(executor, &err, executor->max - index);
		return status;
	}
	return status;
}

void
as_query_validate_end_async(as_event_executor* executor, as_node* node, as_event_loop* event_loop)
{
	as_error err;
	as_policy_info policy;
	as_policy_info_init(&policy);
	policy.timeout = as_query_get_info_timeout(executor);

	char info_cmd[256];
	as_write_cluster_stable(info_cmd, sizeof(info_cmd), executor->ns);

	// node will be released at end of async info processing. This is okay because the
	// node is not referenced after this async info command is complete.
	as_status status = as_info_command_node_async(NULL, &err, &policy, node, info_cmd,
									  as_validate_end_listener, executor, event_loop);

	if (status != AEROSPIKE_OK) {
		as_event_executor_error(executor, &err, 1);
	}
}
