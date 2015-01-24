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
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_batch.h>
#include <aerospike/as_command.h>
#include <aerospike/as_error.h>
#include <aerospike/as_key.h>
#include <aerospike/as_list.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_record.h>
#include <aerospike/as_socket.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>
#include <citrusleaf/cf_clock.h>

/************************************************************************
 * 	TYPES
 ************************************************************************/

typedef struct as_batch_node_s {
	as_node* node;
	as_vector offsets;
} as_batch_node;

typedef struct as_batch_task_s {
	as_node* node;
	as_vector offsets;
	
	as_cluster* cluster;
	const char* ns;
	as_error* err;
	cf_queue* complete_q;
	as_batch_read* results;
	uint32_t* error_mutex;
	as_key* keys;
	
	uint32_t n_keys;
	uint32_t timeout_ms;
	uint32_t index;
	as_policy_retry retry;
	uint8_t read_attr;
} as_batch_task;

typedef struct as_batch_complete_task_s {
	as_node* node;
	as_status result;
} as_batch_complete_task;

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

static uint8_t*
as_batch_parse_fields(uint8_t* p, uint32_t n_fields, uint8_t** digest)
{
	uint32_t len;
	
	for (uint32_t i = 0; i < n_fields; i++) {
		len = cf_swap_from_be32(*(uint32_t*)p);
		p += 4;
		
		if (*p++ == AS_FIELD_DIGEST) {
			*digest = p;
		}
		p += len - 1;
	}
	return p;
}

static as_status
as_batch_parse_records(uint8_t* buf, size_t size, as_batch_task* task)
{
	uint8_t* p = buf;
	uint8_t* end = buf + size;
	
	while (p < end) {
		as_msg* msg = (as_msg*)p;
		as_msg_swap_header_from_be(msg);
		
		if (msg->result_code && msg->result_code != AEROSPIKE_ERR_RECORD_NOT_FOUND) {
			return msg->result_code;
		}
		p += sizeof(as_msg);
		
		if (msg->info3 & AS_MSG_INFO3_LAST) {
			return AEROSPIKE_NO_MORE_RECORDS;
		}

		uint32_t offset = *(uint32_t*)as_vector_get(&task->offsets, task->index++);
		
		uint8_t* digest = 0;
		p = as_batch_parse_fields(p, msg->n_fields, &digest);
		
		if (digest && memcmp(digest, task->keys[offset].digest.value, AS_DIGEST_VALUE_SIZE) == 0) {
			as_batch_read* result = &task->results[offset];
			result->result = msg->result_code;
			
			if (msg->result_code == AEROSPIKE_OK) {
				as_record* rec = &result->record;
				as_record_init(rec, msg->n_ops);
				rec->gen = msg->generation;
				rec->ttl = cf_server_void_time_to_ttl(msg->record_ttl);
				p = as_command_parse_bins(rec, p, msg->n_ops, true);
			}
		}
		else {
			char digest_string[64];
			cf_digest_string((cf_digest*)digest, digest_string);
			as_log_warn("Unexpected batch key returned: %s,%s,%u,%u", task->ns, digest_string, task->index, offset);
		}
	}
	return AEROSPIKE_OK;
}

static as_status
as_batch_parse(as_error* err, int fd, uint64_t deadline_ms, void* udata)
{
	as_batch_task* task = udata;
	as_status status = AEROSPIKE_OK;
	uint8_t* buf = 0;
	size_t capacity = 0;
	
	while (true) {
		// Read header
		as_proto proto;
		status = as_socket_read_deadline(err, fd, (uint8_t*)&proto, sizeof(as_proto), deadline_ms);
		
		if (status) {
			break;
		}
		as_proto_swap_from_be(&proto);
		size_t size = proto.sz;
		
		if (size > 0) {
			// Prepare buffer
			if (size > capacity) {
				as_command_free(buf, capacity);
				capacity = size;
				buf = as_command_init(capacity);
			}
			
			// Read remaining message bytes in group
			status = as_socket_read_deadline(err, fd, buf, size, deadline_ms);
			
			if (status) {
				break;
			}
			
			status = as_batch_parse_records(buf, size, task);
			
			if (status != AEROSPIKE_OK) {
				if (status == AEROSPIKE_NO_MORE_RECORDS) {
					status = AEROSPIKE_OK;
				}
				break;
			}
		}
		else {
			status = as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Received zero sized data packet from server.");
			break;
		}
	}
	as_command_free(buf, capacity);
	return status;
}

static as_status
as_batch_command_execute(as_batch_task* task)
{
	size_t size = AS_HEADER_SIZE;
	size += as_command_string_field_size(task->ns);
	
	uint32_t n_offsets = task->offsets.size;
	uint32_t byte_size = n_offsets * AS_DIGEST_VALUE_SIZE;
	size += as_command_field_size(byte_size);
	
	// TODO: support bin name filters.
	
	uint8_t* cmd = as_command_init(size);
	uint8_t* p = as_command_write_header_read(cmd, task->read_attr, AS_POLICY_CONSISTENCY_LEVEL_ONE, task->timeout_ms, 2, 0);
	p = as_command_write_field_string(p, AS_FIELD_NAMESPACE, task->ns);
	p = as_command_write_field_header(p, AS_FIELD_DIGEST_ARRAY, byte_size);
	
	for (uint32_t i = 0; i < n_offsets; i++) {
		uint32_t offset = *(uint32_t*)as_vector_get(&task->offsets, i);
		as_key* key = &task->keys[offset];
		memcpy(p, key->digest.value, AS_DIGEST_VALUE_SIZE);
		p += AS_DIGEST_VALUE_SIZE;
	}
	size = as_command_write_end(cmd, p);

	as_command_node cn;
	cn.node = task->node;

	as_error err;
	as_status status = as_command_execute(&err, &cn, cmd, size, task->timeout_ms, task->retry, as_batch_parse, task);
	
	as_command_free(cmd, size);
	
	if (status) {
		// Copy error to main error only once.
		if (ck_pr_fas_32(task->error_mutex, 1) == 0) {
			memcpy(task->err, &err, sizeof(as_error));
		}
	}
	return status;
}

static void*
as_batch_worker_fn(void* data)
{
	as_cluster* cluster = (as_cluster*)data;
	as_batch_task task;
	
	while (cf_queue_pop(cluster->batch_q, &task, CF_QUEUE_FOREVER) == CF_QUEUE_OK) {
		// This is how batch shutdown signals we're done.
		if (! task.cluster) {
			break;
		}
		
		as_batch_complete_task complete_task;
		complete_task.node = task.node;
		complete_task.result = as_batch_command_execute(&task);
		
		cf_queue_push(task.complete_q, &complete_task);
	}
	return 0;
}

static void
as_batch_threads_init(as_cluster* cluster)
{
	// We do this lazily, during the first batch request, so make sure it's only
	// done once.
	
	// Quicker than pulling a lock, handles everything except first race:
	if (ck_pr_load_32(&cluster->batch_initialized) == 1) {
		return;
	}
	
	// Handle first race - losers must wait for winner to create dispatch queue.
	pthread_mutex_lock(&cluster->batch_init_lock);
	
	if (ck_pr_load_32(&cluster->batch_initialized) == 1) {
		// Lost race - another thread got here first.
		pthread_mutex_unlock(&cluster->batch_init_lock);
		return;
	}
	
	// Create dispatch queue.
	cluster->batch_q = cf_queue_create(sizeof(as_batch_task), true);
	
	// It's now safe to push to the queue.
	ck_pr_store_32(&cluster->batch_initialized, 1);
	
	pthread_mutex_unlock(&cluster->batch_init_lock);
	
	// Create thread pool.
	for (int i = 0; i < AS_NUM_BATCH_THREADS; i++) {
		pthread_create(&cluster->batch_threads[i], 0, as_batch_worker_fn, (void*)cluster);
	}
}

void
as_batch_threads_shutdown(as_cluster* cluster)
{
	// Note - we assume this doesn't race as_batch_threads_init(), i.e. that no
	// threads are initiating batch transactions while we're shutting down the
	// cluster.
	
	// Check whether we ever (lazily) initialized batch machinery.
	if (ck_pr_load_32(&cluster->batch_initialized) == 0) {
		return;
	}
	
	// This tells the worker threads to stop. We do this (instead of using a
	// "running" flag) to allow the workers to "wait forever" on processing the
	// work dispatch queue, which has minimum impact when the queue is empty.
	// This also means all queued requests get processed when shutting down.
	for (int i = 0; i < AS_NUM_BATCH_THREADS; i++) {
		as_batch_task task;
		task.cluster = NULL;
		cf_queue_push(cluster->batch_q, &task);
	}
	
	for (int i = 0; i < AS_NUM_BATCH_THREADS; i++) {
		pthread_join(cluster->batch_threads[i], NULL);
	}
	
	cf_queue_destroy(cluster->batch_q);
	cluster->batch_q = NULL;
	ck_pr_store_32(&cluster->batch_initialized, 0);
}

static as_batch_node*
as_batch_node_find(as_batch_node* batch_nodes, uint32_t n_batch_nodes, as_node* node)
{
	as_batch_node* batch_node = batch_nodes;
	
	for (uint32_t i = 0; i < n_batch_nodes; i++) {
		if (batch_node->node == node) {
			return batch_node;
		}
		batch_node++;
	}
	return 0;
}

static void
as_batch_release_nodes(as_batch_node* batch_nodes, uint32_t n_batch_nodes)
{
	as_batch_node* batch_node = batch_nodes;
	
	for (uint32_t i = 0; i < n_batch_nodes; i++) {
		as_node_release(batch_node->node);
		as_vector_destroy(&batch_node->offsets);
		batch_node++;
	}
}

static as_status
as_batch_execute(
	aerospike* as, as_error* err, const as_policy_batch* policy, const as_batch* batch,
	aerospike_batch_read_callback callback, void* udata, int read_attr)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.batch;
	}
	
	uint32_t n_keys = batch->keys.size;
	
	if (n_keys <= 0) {
		callback(0, 0, udata);
		return AEROSPIKE_OK;
	}
	
	as_cluster* cluster = as->cluster;
	as_nodes* nodes = as_nodes_reserve(cluster);
	uint32_t n_nodes = nodes->size;
	
	if (n_nodes == 0) {
		as_nodes_release(nodes);
		return as_error_set_message(err, AEROSPIKE_ERR_SERVER, "Batch command failed because cluster is empty.");
	}
	
	// Allocate results array on stack.  May be an issue for huge batch.
	size_t size = sizeof(as_batch_read) * n_keys;
	as_batch_read* results = (as_batch_read*)alloca(size);
	
	as_batch_node* batch_nodes = alloca(sizeof(as_batch_node) * n_nodes);
	char* ns = batch->keys.entries[0].ns;
	uint32_t n_batch_nodes = 0;
	as_status status = AEROSPIKE_OK;
	
	// Create initial key capacity for each node as average + 25%.
	uint32_t offsets_capacity = n_keys / n_nodes;
	offsets_capacity += offsets_capacity >> 2;
	
	// Map keys to server nodes.
	for (uint32_t i = 0; i < n_keys; i++) {
		as_key* key = &batch->keys.entries[i];
		as_batch_read* result = &results[i];
		result->key = key;
		result->result = AEROSPIKE_ERR_RECORD_NOT_FOUND;
		
		// Only support batch commands with all keys in the same namespace.
		if (strcmp(ns, key->ns)) {
			as_batch_release_nodes(batch_nodes, n_batch_nodes);
			as_nodes_release(nodes);
			return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "Batch keys must all be in the same namespace.");
		}
		
		status = as_key_set_digest(err, key);
		
		if (status != AEROSPIKE_OK) {
			as_batch_release_nodes(batch_nodes, n_batch_nodes);
			as_nodes_release(nodes);
			return status;
		}
		
		as_node* node = as_node_get(cluster, key->ns, (cf_digest*)key->digest.value, false, AS_POLICY_REPLICA_MASTER);
		as_batch_node* batch_node = as_batch_node_find(batch_nodes, n_batch_nodes, node);
		
		if (batch_node) {
			// Release duplicate node
			as_node_release(node);
		}
		else {
			// Add batch node.
			batch_node = &batch_nodes[n_batch_nodes++];
			batch_node->node = node;  // Transfer node
			as_vector_inita(&batch_node->offsets, sizeof(uint32_t), offsets_capacity);
		}
		as_vector_append(&batch_node->offsets, &i);
	}
	as_nodes_release(nodes);
	
	// Initialize batch worker threads.
	as_batch_threads_init(cluster);
	uint32_t error_mutex;

	// Initialize task.
	as_batch_task task;
	task.cluster = cluster;
	task.ns = ns;
	task.err = err;
	task.complete_q = cf_queue_create(sizeof(as_batch_complete_task), true);
	task.results = results;
	task.error_mutex = &error_mutex;
	task.n_keys = n_keys;
	task.keys = batch->keys.entries;
	task.timeout_ms = policy->timeout;
	task.index = 0;
	task.retry = AS_POLICY_RETRY_NONE;
	task.read_attr = read_attr;
	
	// Run task for each node.
	for (uint32_t i = 0; i < n_batch_nodes; i++) {
		as_batch_node* batch_node = &batch_nodes[i];
		task.node = batch_node->node;
		memcpy(&task.offsets, &batch_node->offsets, sizeof(as_vector));
		cf_queue_push(cluster->batch_q, &task);
	}
	
	// Wait for tasks to complete.
	for (uint32_t i = 0; i < n_batch_nodes; i++) {
		as_batch_complete_task complete;
		cf_queue_pop(task.complete_q, &complete, CF_QUEUE_FOREVER);
		
		if (complete.result != AEROSPIKE_OK && status == AEROSPIKE_OK) {
			status = complete.result;
		}
	}
	
	// Release temporary queue.
	cf_queue_destroy(task.complete_q);

	// Release each node.
	as_batch_release_nodes(batch_nodes, n_batch_nodes);

	// Call user defined function with results.
	callback(task.results, n_keys, udata);

	// Destroy records. User is responsible for destroying keys with as_batch_destroy().
	for (uint32_t i = 0; i < n_keys; i++) {
		as_record_destroy(&task.results[i].record);
	}
	return status;
}

/**
 *	Look up multiple records by key, then return all bins.
 */
as_status
aerospike_batch_get(
	aerospike* as, as_error* err, const as_policy_batch* policy, const as_batch* batch,
	aerospike_batch_read_callback callback, void* udata
	)
{
	return as_batch_execute(as, err, policy, batch, callback, udata, AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_ALL);
}

/**
 *	Test whether multiple records exist in the cluster.
 */
as_status
aerospike_batch_exists(
	aerospike* as, as_error* err, const as_policy_batch* policy, const as_batch* batch,
	aerospike_batch_read_callback callback, void* udata
	)
{
	return as_batch_execute(as, err, policy, batch, callback, udata, AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_NOBINDATA);
}
