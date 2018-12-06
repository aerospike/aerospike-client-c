/*
 * Copyright 2008-2018 Aerospike, Inc.
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
#include <aerospike/aerospike_key.h>
#include <aerospike/as_async.h>
#include <aerospike/as_bin.h>
#include <aerospike/as_buffer.h>
#include <aerospike/as_command.h>
#include <aerospike/as_error.h>
#include <aerospike/as_event.h>
#include <aerospike/as_key.h>
#include <aerospike/as_list.h>
#include <aerospike/as_log.h>
#include <aerospike/as_msgpack.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_partition.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_random.h>
#include <aerospike/as_record.h>
#include <aerospike/as_serializer.h>
#include <aerospike/as_shm_cluster.h>
#include <aerospike/as_status.h>
#include <citrusleaf/cf_clock.h>

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

static inline void
as_command_node_init(
	as_command_node* cn, const char* ns, const uint8_t* digest, as_policy_replica replica
	)
{
	cn->node = 0;
	cn->ns = ns;
	cn->digest = digest;
	cn->replica = replica;
}

static as_status
as_event_command_init(as_cluster* cluster, as_error* err, const as_key* key, const char** ns, void** partition)
{
	as_error_reset(err);

	as_status status = as_key_set_digest(err, (as_key*)key);

	if (status != AEROSPIKE_OK) {
		*partition = NULL;
		return status;
	}

	if (cluster->shm_info) {
		as_cluster_shm* cluster_shm = cluster->shm_info->cluster_shm;
		as_partition_table_shm* table = as_shm_find_partition_table(cluster_shm, key->ns);

		if (! table) {
			*partition = NULL;

			as_nodes* nodes = as_nodes_reserve(cluster);
			uint32_t n_nodes = nodes->size;
			as_nodes_release(nodes);

			if (n_nodes == 0) {
				return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Cluster is empty");
			}
			return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid namespace: %s", key->ns);
		}

		uint32_t partition_id = as_partition_getid(key->digest.value, cluster_shm->n_partitions);
		*partition = &table->partitions[partition_id];
		*ns = table->ns;
	}
	else {
		as_partition_table* table = as_cluster_get_partition_table(cluster, key->ns);

		if (! table) {
			*partition = NULL;

			as_nodes* nodes = as_nodes_reserve(cluster);
			uint32_t n_nodes = nodes->size;
			as_nodes_release(nodes);

			if (n_nodes == 0) {
				return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Cluster is empty");
			}
			return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid namespace: %s", key->ns);
		}

		uint32_t partition_id = as_partition_getid(key->digest.value, cluster->n_partitions);
		*partition = &table->partitions[partition_id];
		*ns = table->ns;
	}
	return AEROSPIKE_OK;
}

as_status
aerospike_key_get(
	aerospike* as, as_error* err, const as_policy_read* policy, const as_key* key, as_record** rec
	)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.read;
	}

	int status = as_key_set_digest(err, (as_key*)key);
	
	if (status != AEROSPIKE_OK) {
		return status;
	}
	
	uint16_t n_fields;
	size_t size = as_command_key_size(policy->key, key, &n_fields);
		
	uint8_t* cmd = as_command_init(size);
	uint8_t* p = as_command_write_header_read(cmd, AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_ALL,
		policy->consistency_level, policy->linearize_read, policy->base.total_timeout, n_fields, 0);

	p = as_command_write_key(p, policy->key, key);
	size = as_command_write_end(cmd, p);
	
	as_command_node cn;
	as_command_node_init(&cn, key->ns, key->digest.value, policy->replica);

	as_command_parse_result_data data;
	data.record = rec;
	data.deserialize = policy->deserialize;
	
	status = as_command_execute(as->cluster, err, &policy->base, &cn, cmd, size, as_command_parse_result, &data, true);
	
	as_command_free(cmd, size);
	return status;
}

as_status
aerospike_key_get_async(
	aerospike* as, as_error* err, const as_policy_read* policy, const as_key* key,
	as_async_record_listener listener, void* udata, as_event_loop* event_loop,
	as_pipe_listener pipe_listener
	)
{
	if (! policy) {
		policy = &as->config.policies.read;
	}
	
	const char* ns;
	void* partition;
	as_status status = as_event_command_init(as->cluster, err, key, &ns, &partition);

	if (status != AEROSPIKE_OK) {
		return status;
	}
		
	uint16_t n_fields;
	size_t size = as_command_key_size(policy->key, key, &n_fields);
	
	as_event_command* cmd = as_async_record_command_create(
		as->cluster, &policy->base, policy->replica, ns, partition, policy->deserialize,
		AS_ASYNC_FLAGS_MASTER | AS_ASYNC_FLAGS_READ, listener, udata, event_loop, pipe_listener,
		size, as_event_command_parse_result);

	uint8_t* p = as_command_write_header_read(cmd->buf, AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_ALL,
		policy->consistency_level, policy->linearize_read, policy->base.total_timeout, n_fields, 0);

	p = as_command_write_key(p, policy->key, key);
	cmd->write_len = (uint32_t)as_command_write_end(cmd->buf, p);
	return as_event_command_execute(cmd, err);
}

as_status
aerospike_key_select(
	aerospike* as, as_error* err, const as_policy_read* policy, const as_key* key,
	const char* bins[], as_record** rec
	)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.read;
	}
	
	int status = as_key_set_digest(err, (as_key*)key);
	
	if (status != AEROSPIKE_OK) {
		return status;
	}
	
	uint16_t n_fields;
	size_t size = as_command_key_size(policy->key, key, &n_fields);
	int nvalues = 0;
	
	for (nvalues = 0; bins[nvalues] != NULL && bins[nvalues][0] != '\0'; nvalues++) {
		status = as_command_bin_name_size(err, bins[nvalues], &size);
		
		if (status != AEROSPIKE_OK) {
			return status;
		}
	}
	
	uint8_t* cmd = as_command_init(size);
	uint8_t* p = as_command_write_header_read(cmd, AS_MSG_INFO1_READ, policy->consistency_level,
		policy->linearize_read, policy->base.total_timeout, n_fields, nvalues);

	p = as_command_write_key(p, policy->key, key);
	
	for (int i = 0; i < nvalues; i++) {
		p = as_command_write_bin_name(p, bins[i]);
	}
	size = as_command_write_end(cmd, p);

	as_command_node cn;
	as_command_node_init(&cn, key->ns, key->digest.value, policy->replica);
	
	as_command_parse_result_data data;
	data.record = rec;
	data.deserialize = policy->deserialize;

	status = as_command_execute(as->cluster, err, &policy->base, &cn, cmd, size, as_command_parse_result, &data, true);
	
	as_command_free(cmd, size);
	return status;
}

as_status
aerospike_key_select_async(
	aerospike* as, as_error* err, const as_policy_read* policy, const as_key* key, const char* bins[],
	as_async_record_listener listener, void* udata, as_event_loop* event_loop, as_pipe_listener pipe_listener
	)
{
	if (! policy) {
		policy = &as->config.policies.read;
	}
	
	const char* ns;
	void* partition;
	as_status status = as_event_command_init(as->cluster, err, key, &ns, &partition);

	if (status != AEROSPIKE_OK) {
		return status;
	}
	
	uint16_t n_fields;
	size_t size = as_command_key_size(policy->key, key, &n_fields);
	int nvalues = 0;
	
	for (nvalues = 0; bins[nvalues] != NULL && bins[nvalues][0] != '\0'; nvalues++) {
		status = as_command_bin_name_size(err, bins[nvalues], &size);
		
		if (status != AEROSPIKE_OK) {
			return status;
		}
	}
	
	as_event_command* cmd = as_async_record_command_create(
		as->cluster, &policy->base, policy->replica, ns, partition, policy->deserialize,
		AS_ASYNC_FLAGS_MASTER | AS_ASYNC_FLAGS_READ, listener, udata, event_loop, pipe_listener,
		size, as_event_command_parse_result);

	uint8_t* p = as_command_write_header_read(cmd->buf, AS_MSG_INFO1_READ, policy->consistency_level,
		policy->linearize_read, policy->base.total_timeout, n_fields, nvalues);

	p = as_command_write_key(p, policy->key, key);
	
	for (int i = 0; i < nvalues; i++) {
		p = as_command_write_bin_name(p, bins[i]);
	}
	cmd->write_len = (uint32_t)as_command_write_end(cmd->buf, p);
	return as_event_command_execute(cmd, err);
}

as_status
aerospike_key_exists(
	aerospike* as, as_error* err, const as_policy_read* policy, const as_key* key, as_record** rec
	)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.read;
	}
	
	as_status status = as_key_set_digest(err, (as_key*)key);
	
	if (status != AEROSPIKE_OK) {
		return status;
	}

	uint16_t n_fields;
	size_t size = as_command_key_size(policy->key, key, &n_fields);
	
	uint8_t* cmd = as_command_init(size);
	uint8_t* p = as_command_write_header_read(cmd, AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_NOBINDATA,
		policy->consistency_level, policy->linearize_read, policy->base.total_timeout, n_fields, 0);

	p = as_command_write_key(p, policy->key, key);
	size = as_command_write_end(cmd, p);

	as_command_node cn;
	as_command_node_init(&cn, key->ns, key->digest.value, policy->replica);
	
	as_proto_msg msg;
	status = as_command_execute(as->cluster, err, &policy->base, &cn, cmd, size, as_command_parse_header, &msg, true);
	
	as_command_free(cmd, size);

	if (rec) {
		if (status == AEROSPIKE_OK) {
			as_record* r = *rec;
			
			if (r == 0) {
				r = as_record_new(0);
				*rec = r;
			}
			r->gen = (uint16_t)msg.m.generation;
			r->ttl = cf_server_void_time_to_ttl(msg.m.record_ttl);
		}
		else {
			*rec = 0;
		}
	}
	return status;
}

as_status
aerospike_key_exists_async(
	aerospike* as, as_error* err, const as_policy_read* policy, const as_key* key,
	as_async_record_listener listener, void* udata, as_event_loop* event_loop,
	as_pipe_listener pipe_listener
	)
{
	if (! policy) {
		policy = &as->config.policies.read;
	}
	
	const char* ns;
	void* partition;
	as_status status = as_event_command_init(as->cluster, err, key, &ns, &partition);

	if (status != AEROSPIKE_OK) {
		return status;
	}
	
	uint16_t n_fields;
	size_t size = as_command_key_size(policy->key, key, &n_fields);
	
	as_event_command* cmd = as_async_record_command_create(
		as->cluster, &policy->base, policy->replica, ns, partition, false,
		AS_ASYNC_FLAGS_MASTER | AS_ASYNC_FLAGS_READ, listener, udata, event_loop, pipe_listener,
		size, as_event_command_parse_result);

	uint8_t* p = as_command_write_header_read(cmd->buf, AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_NOBINDATA,
		policy->consistency_level, policy->linearize_read, policy->base.total_timeout, n_fields, 0);

	p = as_command_write_key(p, policy->key, key);
	cmd->write_len = (uint32_t)as_command_write_end(cmd->buf, p);
	return as_event_command_execute(cmd, err);
}

as_status
aerospike_key_put(
	aerospike* as, as_error* err, const as_policy_write* policy, const as_key* key, as_record* rec
	)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.write;
	}
	
	as_status status = as_key_set_digest(err, (as_key*)key);
	
	if (status != AEROSPIKE_OK) {
		return status;
	}
	
	as_bin* bins = rec->bins.entries;
	uint32_t n_bins = rec->bins.size;
	as_buffer* buffers = (as_buffer*)alloca(sizeof(as_buffer) * n_bins);
	
	uint16_t n_fields;
	size_t size = as_command_key_size(policy->key, key, &n_fields);

	memset(buffers, 0, sizeof(as_buffer) * n_bins);
	
	for (uint32_t i = 0; i < n_bins; i++) {
		size += as_command_bin_size(&bins[i], &buffers[i]);
	}

	uint8_t* cmd = as_command_init(size);
	uint8_t* p = as_command_write_header(cmd, 0, AS_MSG_INFO2_WRITE, policy->commit_level, 0, false,
					policy->exists, policy->gen, rec->gen, rec->ttl, policy->base.total_timeout, n_fields,
					n_bins, policy->durable_delete);
		
	p = as_command_write_key(p, policy->key, key);

	for (uint32_t i = 0; i < n_bins; i++) {
		p = as_command_write_bin(p, AS_OPERATOR_WRITE, &bins[i], &buffers[i]);
	}
	size = as_command_write_end(cmd, p);

	as_command_node cn;
	as_command_node_init(&cn, key->ns, key->digest.value, policy->replica);
	as_proto_msg msg;
	
	if (policy->compression_threshold == 0 || (size <= policy->compression_threshold)) {
		// Send uncompressed command.
		status = as_command_execute(as->cluster, err, &policy->base, &cn, cmd, size, as_command_parse_header, &msg, false);
	}
	else {
		// Send compressed command.
		size_t comp_size = as_command_compress_max_size(size);
		uint8_t* comp_cmd = as_command_init(comp_size);
		status = as_command_compress(err, cmd, size, comp_cmd, &comp_size);
		
		if (status == AEROSPIKE_OK) {
			status = as_command_execute(as->cluster, err, &policy->base, &cn, comp_cmd, comp_size, as_command_parse_header, &msg, false);
		}
		as_command_free(comp_cmd, comp_size);
	}
	as_command_free(cmd, size);
	return status;
}

as_status
aerospike_key_put_async_ex(
	aerospike* as, as_error* err, const as_policy_write* policy, const as_key* key, as_record* rec,
	as_async_write_listener listener, void* udata, as_event_loop* event_loop, as_pipe_listener pipe_listener,
	size_t* length, size_t* comp_length
	)
{
	if (! policy) {
		policy = &as->config.policies.write;
	}

	const char* ns;
	void* partition;
	as_status status = as_event_command_init(as->cluster, err, key, &ns, &partition);

	if (status != AEROSPIKE_OK) {
		return status;
	}
		
	as_bin* bins = rec->bins.entries;
	uint32_t n_bins = rec->bins.size;
	as_buffer* buffers = (as_buffer*)alloca(sizeof(as_buffer) * n_bins);
	
	uint16_t n_fields;
	size_t size = as_command_key_size(policy->key, key, &n_fields);
	memset(buffers, 0, sizeof(as_buffer) * n_bins);
	
	for (uint32_t i = 0; i < n_bins; i++) {
		size += as_command_bin_size(&bins[i], &buffers[i]);
	}
	
	if (policy->compression_threshold == 0 || (size <= policy->compression_threshold)) {
		// Send uncompressed command.
		as_event_command* cmd = as_async_write_command_create(
				as->cluster, &policy->base, policy->replica, ns, partition, AS_ASYNC_FLAGS_MASTER,
				listener, udata, event_loop, pipe_listener, size, as_event_command_parse_header);
		
		uint8_t* p = as_command_write_header(cmd->buf, 0, AS_MSG_INFO2_WRITE, policy->commit_level, 0,
				false, policy->exists, policy->gen, rec->gen, rec->ttl, policy->base.total_timeout,
				n_fields, n_bins, policy->durable_delete);
		
		p = as_command_write_key(p, policy->key, key);
		
		for (uint32_t i = 0; i < n_bins; i++) {
			p = as_command_write_bin(p, AS_OPERATOR_WRITE, &bins[i], &buffers[i]);
		}
		cmd->write_len = (uint32_t)as_command_write_end(cmd->buf, p);

		if (length != NULL) {
			*length = size;
		}

		if (comp_length != NULL) {
			*comp_length = size;
		}

		return as_event_command_execute(cmd, err);
	}
	else {
		// Send compressed command.
		// First write uncompressed buffer.
		uint8_t* cmd = as_command_init(size);
		uint8_t* p = as_command_write_header(cmd, 0, AS_MSG_INFO2_WRITE, policy->commit_level, 0,
				false, policy->exists, policy->gen, rec->gen, rec->ttl, policy->base.total_timeout,
				n_fields, n_bins, policy->durable_delete);
		
		p = as_command_write_key(p, policy->key, key);
		
		for (uint32_t i = 0; i < n_bins; i++) {
			p = as_command_write_bin(p, AS_OPERATOR_WRITE, &bins[i], &buffers[i]);
		}
		size = as_command_write_end(cmd, p);
		
		// Allocate command with compressed upper bound.
		size_t comp_size = as_command_compress_max_size(size);
		
		as_event_command* comp_cmd = as_async_write_command_create(
				as->cluster, &policy->base, policy->replica, ns, partition, AS_ASYNC_FLAGS_MASTER,
				listener, udata, event_loop, pipe_listener, comp_size, as_event_command_parse_header);

		// Compress buffer and execute.
		status = as_command_compress(err, cmd, size, comp_cmd->buf, &comp_size);
		as_command_free(cmd, size);
		
		if (status == AEROSPIKE_OK) {
			comp_cmd->write_len = (uint32_t)comp_size;

			if (length != NULL) {
				*length = size;
			}

			if (comp_length != NULL) {
				*comp_length = comp_size;
			}

			return as_event_command_execute(comp_cmd, err);
		}
		else {
			cf_free(comp_cmd);
			return status;
		}
	}
}

as_status
aerospike_key_put_async(
	aerospike* as, as_error* err, const as_policy_write* policy, const as_key* key, as_record* rec,
	as_async_write_listener listener, void* udata, as_event_loop* event_loop, as_pipe_listener pipe_listener
	)
{
	return aerospike_key_put_async_ex(as, err, policy, key, rec, listener, udata, event_loop, pipe_listener, NULL, NULL);
}

as_status
aerospike_key_remove(
	aerospike* as, as_error* err, const as_policy_remove* policy, const as_key* key
	)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.remove;
	}
	
	as_status status = as_key_set_digest(err, (as_key*)key);
	
	if (status != AEROSPIKE_OK) {
		return status;
	}

	uint16_t n_fields;
	size_t size = as_command_key_size(policy->key, key, &n_fields);
		
	uint8_t* cmd = as_command_init(size);
	uint8_t* p = as_command_write_header(cmd, 0, AS_MSG_INFO2_WRITE | AS_MSG_INFO2_DELETE,
					policy->commit_level, 0, false, AS_POLICY_EXISTS_IGNORE, policy->gen,
					policy->generation, 0, policy->base.total_timeout, n_fields, 0,
					policy->durable_delete);

	p = as_command_write_key(p, policy->key, key);
	size = as_command_write_end(cmd, p);

	as_command_node cn;
	as_command_node_init(&cn, key->ns, key->digest.value, policy->replica);
	
	as_proto_msg msg;
	status = as_command_execute(as->cluster, err, &policy->base, &cn, cmd, size, as_command_parse_header, &msg, false);
	
	as_command_free(cmd, size);
	return status;
}

as_status
aerospike_key_remove_async_ex(
	aerospike* as, as_error* err, const as_policy_remove* policy, const as_key* key,
	as_async_write_listener listener, void* udata, as_event_loop* event_loop,
	as_pipe_listener pipe_listener, size_t* length
	)
{
	if (! policy) {
		policy = &as->config.policies.remove;
	}

	const char* ns;
	void* partition;
	as_status status = as_event_command_init(as->cluster, err, key, &ns, &partition);

	if (status != AEROSPIKE_OK) {
		return status;
	}
	
	uint16_t n_fields;
	size_t size = as_command_key_size(policy->key, key, &n_fields);
	
	as_event_command* cmd = as_async_write_command_create(
			as->cluster, &policy->base, policy->replica, ns, partition, AS_ASYNC_FLAGS_MASTER,
			listener, udata, event_loop, pipe_listener, size, as_event_command_parse_header);

	uint8_t* p = as_command_write_header(cmd->buf, 0, AS_MSG_INFO2_WRITE | AS_MSG_INFO2_DELETE,
						policy->commit_level, 0, false, AS_POLICY_EXISTS_IGNORE, policy->gen,
						policy->generation, 0, policy->base.total_timeout, n_fields, 0,
						policy->durable_delete);
	
	p = as_command_write_key(p, policy->key, key);
	cmd->write_len = (uint32_t)as_command_write_end(cmd->buf, p);

	if (length != NULL) {
		*length = size;
	}

	return as_event_command_execute(cmd, err);
}

as_status
aerospike_key_remove_async(
	aerospike* as, as_error* err, const as_policy_remove* policy, const as_key* key,
	as_async_write_listener listener, void* udata, as_event_loop* event_loop,
	as_pipe_listener pipe_listener
	)
{
	return aerospike_key_remove_async_ex(as, err, policy, key, listener, udata, event_loop, pipe_listener, NULL);
}

static size_t
as_operate_set_attr(const as_operations* ops, as_buffer* buffers, uint8_t* rattr, uint8_t* wattr)
{
	size_t size = 0;
	uint32_t n_operations = ops->binops.size;
	uint8_t read_attr = 0;
	uint8_t write_attr = 0;
	bool respond_all_ops = false;
	
	for (uint32_t i = 0; i < n_operations; i++) {
		as_binop* op = &ops->binops.entries[i];
		
		switch (op->op)	{
			case AS_OPERATOR_MAP_READ:
				// Map operations require respond_all_ops to be true.
				respond_all_ops = true;
				op->op = AS_OPERATOR_CDT_READ;
				// Fall through to read.
			case AS_OPERATOR_CDT_READ:
			case AS_OPERATOR_READ:
				read_attr |= AS_MSG_INFO1_READ;
				break;
				
			case AS_OPERATOR_MAP_MODIFY:
				// Map operations require respond_all_ops to be true.
				respond_all_ops = true;
				op->op = AS_OPERATOR_CDT_MODIFY;
				// Fall through to write.
			default:
				write_attr |= AS_MSG_INFO2_WRITE;
				break;
		}
		size += as_command_bin_size(&op->bin, &buffers[i]);
	}
	
	if (respond_all_ops) {
		write_attr |= AS_MSG_INFO2_RESPOND_ALL_OPS;
	}
	*rattr = read_attr;
	*wattr = write_attr;
	return size;
}

as_status
aerospike_key_operate(
	aerospike* as, as_error* err, const as_policy_operate* policy, const as_key* key,
	const as_operations* ops, as_record** rec
	)
{
	as_error_reset(err);

	int status = as_key_set_digest(err, (as_key*)key);
	
	if (status != AEROSPIKE_OK) {
		return status;
	}

	uint32_t n_operations = ops->binops.size;
	
	if (n_operations == 0) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "No operations defined");
	}
	
	as_buffer* buffers = (as_buffer*)alloca(sizeof(as_buffer) * n_operations);
	memset(buffers, 0, sizeof(as_buffer) * n_operations);

	uint8_t read_attr;
	uint8_t write_attr;
	size_t size = as_operate_set_attr(ops, buffers, &read_attr, &write_attr);
	
	as_policy_operate policy_local;

	if (! policy) {
		if (write_attr & AS_MSG_INFO2_WRITE) {
			// Write operations should not retry by default.
			policy = &as->config.policies.operate;
		}
		else {
			// Read operations should retry by default.
			as_policy_operate_copy(&as->config.policies.operate, &policy_local);
			policy_local.base.max_retries = 2;
			policy = &policy_local;
		}
	}

	uint16_t n_fields;
	size += as_command_key_size(policy->key, key, &n_fields);

	uint8_t* cmd = as_command_init(size);
	uint8_t* p = as_command_write_header(cmd, read_attr, write_attr, policy->commit_level,
				policy->consistency_level, policy->linearize_read, policy->exists,
				policy->gen, ops->gen, ops->ttl, policy->base.total_timeout, n_fields, n_operations,
				policy->durable_delete);

	p = as_command_write_key(p, policy->key, key);
	
	for (uint32_t i = 0; i < n_operations; i++) {
		as_binop* op = &ops->binops.entries[i];
		p = as_command_write_bin(p, op->op, &op->bin, &buffers[i]);
	}

	size = as_command_write_end(cmd, p);

	as_command_node cn;
	as_command_node_init(&cn, key->ns, key->digest.value, policy->replica);
	
	as_command_parse_result_data data;
	data.record = rec;
	data.deserialize = policy->deserialize;

	status = as_command_execute(as->cluster, err, &policy->base, &cn, cmd, size, as_command_parse_result, &data, false);
	
	as_command_free(cmd, size);
	return status;
}

as_status
aerospike_key_operate_async(
	aerospike* as, as_error* err, const as_policy_operate* policy, const as_key* key, const as_operations* ops,
	as_async_record_listener listener, void* udata, as_event_loop* event_loop, as_pipe_listener pipe_listener
	)
{
	uint32_t n_operations = ops->binops.size;
	
	if (n_operations == 0) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "No operations defined");
	}
	
	as_buffer* buffers = (as_buffer*)alloca(sizeof(as_buffer) * n_operations);
	memset(buffers, 0, sizeof(as_buffer) * n_operations);
	
	uint8_t read_attr;
	uint8_t write_attr;
	size_t size = as_operate_set_attr(ops, buffers, &read_attr, &write_attr);

	as_policy_operate policy_local;

	if (! policy) {
		if (write_attr & AS_MSG_INFO2_WRITE) {
			// Write operations should not retry by default.
			policy = &as->config.policies.operate;
		}
		else {
			// Read operations should retry by default.
			as_policy_operate_copy(&as->config.policies.operate, &policy_local);
			policy_local.base.max_retries = 2;
			policy = &policy_local;
		}
	}

	uint16_t n_fields;
	size += as_command_key_size(policy->key, key, &n_fields);

	const char* ns;
	void* partition;
	as_status status = as_event_command_init(as->cluster, err, key, &ns, &partition);

	if (status != AEROSPIKE_OK) {
		for (uint32_t i = 0; i < n_operations; i++) {
			as_buffer* buffer = &buffers[i];
			
			if (buffer->data) {
				cf_free(buffer->data);
			}
		}
		return status;
	}

	as_event_command* cmd = as_async_record_command_create(
		as->cluster, &policy->base, policy->replica, ns, partition, policy->deserialize,
		AS_ASYNC_FLAGS_MASTER, listener, udata, event_loop, pipe_listener, size,
		as_event_command_parse_result);

	uint8_t* p = as_command_write_header(cmd->buf, read_attr, write_attr, policy->commit_level,
		policy->consistency_level, policy->linearize_read, policy->exists, policy->gen,
		ops->gen, ops->ttl, policy->base.total_timeout, n_fields, n_operations,
		policy->durable_delete);

	p = as_command_write_key(p, policy->key, key);
	
	for (uint32_t i = 0; i < n_operations; i++) {
		as_binop* op = &ops->binops.entries[i];
		p = as_command_write_bin(p, op->op, &op->bin, &buffers[i]);
	}
	cmd->write_len = (uint32_t)as_command_write_end(cmd->buf, p);
	return as_event_command_execute(cmd, err);
}

as_status
aerospike_key_apply(
	aerospike* as, as_error* err, const as_policy_apply* policy, const as_key* key,
	const char* module, const char* function, as_list* arglist, as_val** result
	)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.apply;
	}

	int status = as_key_set_digest(err, (as_key*)key);
	
	if (status != AEROSPIKE_OK) {
		return status;
	}
	
	uint16_t n_fields;
	size_t size = as_command_key_size(policy->key, key, &n_fields);
	size += as_command_string_field_size(module);
	size += as_command_string_field_size(function);
	
	as_serializer ser;
	as_msgpack_init(&ser);
	as_buffer args;
	as_buffer_init(&args);
	as_serializer_serialize(&ser, (as_val*)arglist, &args);
	size += as_command_field_size(args.size);
	n_fields += 3;

	uint8_t* cmd = as_command_init(size);
	uint8_t* p = as_command_write_header(cmd, 0, AS_MSG_INFO2_WRITE, policy->commit_level, 0,
		policy->linearize_read, 0, policy->gen, policy->gen_value, policy->ttl,
		policy->base.total_timeout, n_fields, 0, policy->durable_delete);

	p = as_command_write_key(p, policy->key, key);
	p = as_command_write_field_string(p, AS_FIELD_UDF_PACKAGE_NAME, module);
	p = as_command_write_field_string(p, AS_FIELD_UDF_FUNCTION, function);
	p = as_command_write_field_buffer(p, AS_FIELD_UDF_ARGLIST, &args);
	size = as_command_write_end(cmd, p);
	
	as_command_node cn;
	as_command_node_init(&cn, key->ns, key->digest.value, policy->replica);
	
	status = as_command_execute(as->cluster, err, &policy->base, &cn, cmd, size, as_command_parse_success_failure, result, false);
	
	as_command_free(cmd, size);
	as_buffer_destroy(&args);
	as_serializer_destroy(&ser);
	return status;
}

as_status
aerospike_key_apply_async(
	aerospike* as, as_error* err, const as_policy_apply* policy, const as_key* key,
	const char* module, const char* function, as_list* arglist,
	as_async_value_listener listener, void* udata, as_event_loop* event_loop,
	as_pipe_listener pipe_listener
	)
{
	if (! policy) {
		policy = &as->config.policies.apply;
	}
	
	const char* ns;
	void* partition;
	as_status status = as_event_command_init(as->cluster, err, key, &ns, &partition);

	if (status != AEROSPIKE_OK) {
		return status;
	}
	
	uint16_t n_fields;
	size_t size = as_command_key_size(policy->key, key, &n_fields);
	size += as_command_string_field_size(module);
	size += as_command_string_field_size(function);
	
	as_serializer ser;
	as_msgpack_init(&ser);
	as_buffer args;
	as_buffer_init(&args);
	as_serializer_serialize(&ser, (as_val*)arglist, &args);
	size += as_command_field_size(args.size);
	n_fields += 3;
	
	as_event_command* cmd = as_async_value_command_create(
		as->cluster, &policy->base, policy->replica, ns, partition, AS_ASYNC_FLAGS_MASTER, listener,
		udata, event_loop, pipe_listener, size, as_event_command_parse_success_failure);

	uint8_t* p = as_command_write_header(cmd->buf, 0, AS_MSG_INFO2_WRITE, policy->commit_level, 0,
		policy->linearize_read, 0, policy->gen, policy->gen_value, policy->ttl,
		policy->base.total_timeout, n_fields, 0, policy->durable_delete);

	p = as_command_write_key(p, policy->key, key);
	p = as_command_write_field_string(p, AS_FIELD_UDF_PACKAGE_NAME, module);
	p = as_command_write_field_string(p, AS_FIELD_UDF_FUNCTION, function);
	p = as_command_write_field_buffer(p, AS_FIELD_UDF_ARGLIST, &args);
	cmd->write_len = (uint32_t)as_command_write_end(cmd->buf, p);
	as_buffer_destroy(&args);
	as_serializer_destroy(&ser);
	return as_event_command_execute(cmd, err);
}

bool
aerospike_has_geo(aerospike* as)
{
	as_nodes* nodes = as_nodes_reserve(as->cluster);
	
	if (nodes->size == 0) {
		as_nodes_release(nodes);
		return false;
	}
	
	for (uint32_t i = 0; i < nodes->size; i++) {
		if (! (nodes->array[i]->features & AS_FEATURES_GEO)) {
			as_nodes_release(nodes);
			return false;
		}
	}
	as_nodes_release(nodes);
	return true;
}
