/*
 * Copyright 2008-2019 Aerospike, Inc.
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
#include <aerospike/as_predexp.h>
#include <aerospike/as_random.h>
#include <aerospike/as_record.h>
#include <aerospike/as_serializer.h>
#include <aerospike/as_shm_cluster.h>
#include <aerospike/as_status.h>
#include <citrusleaf/cf_clock.h>

/******************************************************************************
 * TYPES
 *****************************************************************************/

typedef struct as_read_info_s {
	as_policy_replica replica;
	uint8_t flags;
} as_read_info;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

static inline as_status
as_key_partition_init(as_cluster* cluster, as_error* err, const as_key* key, as_partition_info* pi)
{
	as_error_reset(err);

	as_status status = as_key_set_digest(err, (as_key*)key);

	if (status != AEROSPIKE_OK) {
		return status;
	}
	return as_partition_info_init(pi, cluster, err, key);
}

static inline void
as_command_init_read(
	as_command* cmd, as_cluster* cluster, const as_policy_base* policy,
	as_policy_replica replica, as_policy_read_mode_sc read_mode_sc, const as_key* key,
	uint8_t* buf, size_t size, as_partition_info* pi, const as_parse_results_fn fn, void* udata
	)
{
	cmd->cluster = cluster;
	cmd->policy = policy;
	cmd->node = NULL;
	cmd->ns = pi->ns;
	cmd->partition = pi->partition;
	cmd->parse_results_fn = fn;
	cmd->udata = udata;
	cmd->buf = buf;
	cmd->buf_size = size;
	cmd->partition_id = pi->partition_id;

	if (pi->sc_mode) {
		switch (read_mode_sc) {
			case AS_POLICY_READ_MODE_SC_SESSION:
				cmd->replica = AS_POLICY_REPLICA_MASTER;
				cmd->flags = AS_COMMAND_FLAGS_READ;
				break;

			case AS_POLICY_READ_MODE_SC_LINEARIZE:
				cmd->replica = (replica != AS_POLICY_REPLICA_PREFER_RACK) ?
								replica : AS_POLICY_REPLICA_SEQUENCE;
				cmd->flags = AS_COMMAND_FLAGS_READ | AS_COMMAND_FLAGS_LINEARIZE;
				break;

			default:
				cmd->replica = replica;
				cmd->flags = AS_COMMAND_FLAGS_READ;
				break;
		}
	}
	else {
		cmd->replica = replica;
		cmd->flags = AS_COMMAND_FLAGS_READ;
	}
	as_command_start_timer(cmd);
}

static inline void
as_command_init_write(
	as_command* cmd, as_cluster* cluster, const as_policy_base* policy, as_policy_replica replica,
	const as_key* key, uint8_t* buf, size_t size, as_partition_info* pi,
	const as_parse_results_fn fn, void* udata
	)
{
	cmd->cluster = cluster;
	cmd->policy = policy;
	cmd->node = NULL;
	cmd->ns = pi->ns;
	cmd->partition = pi->partition;
	cmd->parse_results_fn = fn;
	cmd->udata = udata;
	cmd->buf = buf;
	cmd->buf_size = size;
	cmd->partition_id = pi->partition_id;
	cmd->replica = replica;
	cmd->flags = 0;
	as_command_start_timer(cmd);
}

static inline void
as_event_command_init_read(
	as_policy_replica replica, as_policy_read_mode_sc read_mode_sc, bool sc_mode, as_read_info* ri
	)
{
	if (sc_mode) {
		switch (read_mode_sc) {
			case AS_POLICY_READ_MODE_SC_SESSION:
				ri->replica = AS_POLICY_REPLICA_MASTER;
				ri->flags = AS_ASYNC_FLAGS_MASTER | AS_ASYNC_FLAGS_READ;
				break;

			case AS_POLICY_READ_MODE_SC_LINEARIZE:
				ri->replica = (replica != AS_POLICY_REPLICA_PREFER_RACK) ?
							   replica : AS_POLICY_REPLICA_SEQUENCE;
				ri->flags = AS_ASYNC_FLAGS_MASTER | AS_ASYNC_FLAGS_READ | AS_ASYNC_FLAGS_LINEARIZE;
				break;

			default:
				ri->replica = replica;
				ri->flags = AS_ASYNC_FLAGS_MASTER | AS_ASYNC_FLAGS_READ;
				break;
		}
	}
	else {
		ri->replica = replica;
		ri->flags = AS_ASYNC_FLAGS_MASTER | AS_ASYNC_FLAGS_READ;
	}
}


as_status
aerospike_key_get(
	aerospike* as, as_error* err, const as_policy_read* policy, const as_key* key, as_record** rec
	)
{
	if (! policy) {
		policy = &as->config.policies.read;
	}

	as_cluster* cluster = as->cluster;
	as_partition_info pi;
	as_status status = as_key_partition_init(cluster, err, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	uint16_t n_fields;
	size_t size = as_command_key_size(policy->key, key, &n_fields);
	uint32_t pred_size = 0;

	if (policy->base.predexp) {
		size += as_predexp_list_size(policy->base.predexp, &pred_size);
		n_fields++;
	}
		
	uint8_t* buf = as_command_buffer_init(size);
	uint8_t* p = as_command_write_header_read(buf, AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_ALL,
		policy->read_mode_ap, policy->read_mode_sc, policy->base.total_timeout, n_fields, 0);

	p = as_command_write_key(p, policy->key, key);

	if (policy->base.predexp) {
		p = as_predexp_list_write(policy->base.predexp, pred_size, p);
	}

	size = as_command_write_end(buf, p);

	as_command_parse_result_data data;
	data.record = rec;
	data.deserialize = policy->deserialize;

	as_command cmd;
	as_command_init_read(&cmd, cluster, &policy->base, policy->replica, policy->read_mode_sc,
						 key, buf, size, &pi, as_command_parse_result, &data);

	status = as_command_execute(&cmd, err);
	as_cluster_release_partitions(cluster);
	as_command_buffer_free(buf, size);
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

	as_cluster* cluster = as->cluster;
	as_partition_info pi;
	as_status status = as_key_partition_init(cluster, err, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_read_info ri;
	as_event_command_init_read(policy->replica, policy->read_mode_sc, pi.sc_mode, &ri);

	uint16_t n_fields;
	size_t size = as_command_key_size(policy->key, key, &n_fields);
	uint32_t pred_size = 0;

	if (policy->base.predexp) {
		size += as_predexp_list_size(policy->base.predexp, &pred_size);
		n_fields++;
	}

	as_event_command* cmd = as_async_record_command_create(
		cluster, &policy->base, ri.replica, pi.ns, pi.partition, policy->deserialize,
		ri.flags, listener, udata, event_loop, pipe_listener,
		size, as_event_command_parse_result);

	uint8_t* p = as_command_write_header_read(cmd->buf, AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_ALL,
		policy->read_mode_ap, policy->read_mode_sc, policy->base.total_timeout, n_fields, 0);

	p = as_command_write_key(p, policy->key, key);

	if (policy->base.predexp) {
		p = as_predexp_list_write(policy->base.predexp, pred_size, p);
	}

	cmd->write_len = (uint32_t)as_command_write_end(cmd->buf, p);
	return as_event_command_execute(cmd, err);
}

as_status
aerospike_key_select(
	aerospike* as, as_error* err, const as_policy_read* policy, const as_key* key,
	const char* bins[], as_record** rec
	)
{
	if (! policy) {
		policy = &as->config.policies.read;
	}
	
	as_cluster* cluster = as->cluster;
	as_partition_info pi;
	as_status status = as_key_partition_init(cluster, err, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	uint16_t n_fields;
	size_t size = as_command_key_size(policy->key, key, &n_fields);
	uint32_t pred_size = 0;

	if (policy->base.predexp) {
		size += as_predexp_list_size(policy->base.predexp, &pred_size);
		n_fields++;
	}

	int nvalues = 0;
	
	for (nvalues = 0; bins[nvalues] != NULL && bins[nvalues][0] != '\0'; nvalues++) {
		status = as_command_bin_name_size(err, bins[nvalues], &size);
		
		if (status != AEROSPIKE_OK) {
			as_cluster_release_partitions(cluster);
			return status;
		}
	}
	
	uint8_t* buf = as_command_buffer_init(size);
	uint8_t* p = as_command_write_header_read(buf, AS_MSG_INFO1_READ, policy->read_mode_ap,
					policy->read_mode_sc, policy->base.total_timeout, n_fields, nvalues);

	p = as_command_write_key(p, policy->key, key);

	if (policy->base.predexp) {
		p = as_predexp_list_write(policy->base.predexp, pred_size, p);
	}

	for (int i = 0; i < nvalues; i++) {
		p = as_command_write_bin_name(p, bins[i]);
	}
	size = as_command_write_end(buf, p);

	as_command_parse_result_data data;
	data.record = rec;
	data.deserialize = policy->deserialize;

	as_command cmd;
	as_command_init_read(&cmd, cluster, &policy->base, policy->replica, policy->read_mode_sc,
						 key, buf, size, &pi, as_command_parse_result, &data);

	status = as_command_execute(&cmd, err);
	as_cluster_release_partitions(cluster);
	as_command_buffer_free(buf, size);
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
	
	as_cluster* cluster = as->cluster;
	as_partition_info pi;
	as_status status = as_key_partition_init(cluster, err, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}
	
	as_read_info ri;
	as_event_command_init_read(policy->replica, policy->read_mode_sc, pi.sc_mode, &ri);

	uint16_t n_fields;
	size_t size = as_command_key_size(policy->key, key, &n_fields);
	uint32_t pred_size = 0;

	if (policy->base.predexp) {
		size += as_predexp_list_size(policy->base.predexp, &pred_size);
		n_fields++;
	}

	int nvalues = 0;
	
	for (nvalues = 0; bins[nvalues] != NULL && bins[nvalues][0] != '\0'; nvalues++) {
		status = as_command_bin_name_size(err, bins[nvalues], &size);
		
		if (status != AEROSPIKE_OK) {
			as_cluster_release_partitions(cluster);
			return status;
		}
	}

	as_event_command* cmd = as_async_record_command_create(
		cluster, &policy->base, ri.replica, pi.ns, pi.partition, policy->deserialize,
		ri.flags, listener, udata, event_loop, pipe_listener,
		size, as_event_command_parse_result);

	uint8_t* p = as_command_write_header_read(cmd->buf, AS_MSG_INFO1_READ, policy->read_mode_ap,
					policy->read_mode_sc, policy->base.total_timeout, n_fields, nvalues);

	p = as_command_write_key(p, policy->key, key);

	if (policy->base.predexp) {
		p = as_predexp_list_write(policy->base.predexp, pred_size, p);
	}

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
	if (! policy) {
		policy = &as->config.policies.read;
	}
	
	as_cluster* cluster = as->cluster;
	as_partition_info pi;
	as_status status = as_key_partition_init(cluster, err, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	uint16_t n_fields;
	size_t size = as_command_key_size(policy->key, key, &n_fields);
	uint32_t pred_size = 0;

	if (policy->base.predexp) {
		size += as_predexp_list_size(policy->base.predexp, &pred_size);
		n_fields++;
	}

	uint8_t* buf = as_command_buffer_init(size);
	uint8_t* p = as_command_write_header_read(buf, AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_NOBINDATA,
		policy->read_mode_ap, policy->read_mode_sc, policy->base.total_timeout, n_fields, 0);

	p = as_command_write_key(p, policy->key, key);

	if (policy->base.predexp) {
		p = as_predexp_list_write(policy->base.predexp, pred_size, p);
	}

	size = as_command_write_end(buf, p);

	as_proto_msg msg;
	as_command cmd;
	as_command_init_read(&cmd, cluster, &policy->base, policy->replica, policy->read_mode_sc,
						 key, buf, size, &pi, as_command_parse_header, &msg);

	status = as_command_execute(&cmd, err);

	as_cluster_release_partitions(cluster);
	as_command_buffer_free(buf, size);

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
	
	as_cluster* cluster = as->cluster;
	as_partition_info pi;
	as_status status = as_key_partition_init(cluster, err, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}
	
	as_read_info ri;
	as_event_command_init_read(policy->replica, policy->read_mode_sc, pi.sc_mode, &ri);

	uint16_t n_fields;
	size_t size = as_command_key_size(policy->key, key, &n_fields);
	uint32_t pred_size = 0;

	if (policy->base.predexp) {
		size += as_predexp_list_size(policy->base.predexp, &pred_size);
		n_fields++;
	}

	as_event_command* cmd = as_async_record_command_create(
		cluster, &policy->base, ri.replica, pi.ns, pi.partition, false,
		ri.flags, listener, udata, event_loop, pipe_listener,
		size, as_event_command_parse_result);

	uint8_t* p = as_command_write_header_read(cmd->buf, AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_NOBINDATA,
		policy->read_mode_ap, policy->read_mode_sc, policy->base.total_timeout, n_fields, 0);

	p = as_command_write_key(p, policy->key, key);

	if (policy->base.predexp) {
		p = as_predexp_list_write(policy->base.predexp, pred_size, p);
	}

	cmd->write_len = (uint32_t)as_command_write_end(cmd->buf, p);
	return as_event_command_execute(cmd, err);
}

as_status
aerospike_key_put(
	aerospike* as, as_error* err, const as_policy_write* policy, const as_key* key, as_record* rec
	)
{
	if (! policy) {
		policy = &as->config.policies.write;
	}
	
	as_cluster* cluster = as->cluster;
	as_partition_info pi;
	as_status status = as_key_partition_init(cluster, err, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_bin* bins = rec->bins.entries;
	uint32_t n_bins = rec->bins.size;
	as_buffer* buffers = (as_buffer*)alloca(sizeof(as_buffer) * n_bins);
	
	uint16_t n_fields;
	size_t size = as_command_key_size(policy->key, key, &n_fields);
	uint32_t pred_size = 0;

	if (policy->base.predexp) {
		size += as_predexp_list_size(policy->base.predexp, &pred_size);
		n_fields++;
	}

	memset(buffers, 0, sizeof(as_buffer) * n_bins);
	
	for (uint32_t i = 0; i < n_bins; i++) {
		size += as_command_bin_size(&bins[i], &buffers[i]);
	}

	uint8_t* buf = as_command_buffer_init(size);
	uint8_t* p = as_command_write_header(buf, 0, AS_MSG_INFO2_WRITE, 0, policy->commit_level,
					policy->exists, policy->gen, rec->gen, rec->ttl, policy->base.total_timeout,
					n_fields, n_bins, policy->durable_delete);
		
	p = as_command_write_key(p, policy->key, key);

	if (policy->base.predexp) {
		p = as_predexp_list_write(policy->base.predexp, pred_size, p);
	}

	for (uint32_t i = 0; i < n_bins; i++) {
		p = as_command_write_bin(p, AS_OPERATOR_WRITE, &bins[i], &buffers[i]);
	}
	size = as_command_write_end(buf, p);

	as_proto_msg msg;
	as_command cmd;

	if (policy->compression_threshold == 0 || (size <= policy->compression_threshold)) {
		// Send uncompressed command.
		as_command_init_write(&cmd, cluster, &policy->base, policy->replica, key, buf, size,
			&pi, as_command_parse_header, &msg);

		status = as_command_execute(&cmd, err);
	}
	else {
		// Send compressed command.
		size_t comp_size = as_command_compress_max_size(size);
		uint8_t* comp_cmd = as_command_buffer_init(comp_size);
		status = as_command_compress(err, buf, size, comp_cmd, &comp_size);
		
		if (status == AEROSPIKE_OK) {
			as_command_init_write(&cmd, cluster, &policy->base, policy->replica, key, comp_cmd,
				comp_size, &pi, as_command_parse_header, &msg);

			status = as_command_execute(&cmd, err);
		}
		as_command_buffer_free(comp_cmd, comp_size);
	}
	as_cluster_release_partitions(cluster);
	as_command_buffer_free(buf, size);
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

	as_cluster* cluster = as->cluster;
	as_partition_info pi;
	as_status status = as_key_partition_init(cluster, err, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}
		
	as_bin* bins = rec->bins.entries;
	uint32_t n_bins = rec->bins.size;
	as_buffer* buffers = (as_buffer*)alloca(sizeof(as_buffer) * n_bins);
	
	uint16_t n_fields;
	size_t size = as_command_key_size(policy->key, key, &n_fields);
	uint32_t pred_size = 0;

	if (policy->base.predexp) {
		size += as_predexp_list_size(policy->base.predexp, &pred_size);
		n_fields++;
	}

	memset(buffers, 0, sizeof(as_buffer) * n_bins);
	
	for (uint32_t i = 0; i < n_bins; i++) {
		size += as_command_bin_size(&bins[i], &buffers[i]);
	}
	
	if (policy->compression_threshold == 0 || (size <= policy->compression_threshold)) {
		// Send uncompressed command.
		as_event_command* cmd = as_async_write_command_create(
				cluster, &policy->base, policy->replica, pi.ns, pi.partition, AS_ASYNC_FLAGS_MASTER,
				listener, udata, event_loop, pipe_listener, size, as_event_command_parse_header);
		
		uint8_t* p = as_command_write_header(cmd->buf, 0, AS_MSG_INFO2_WRITE, 0, policy->commit_level,
				policy->exists, policy->gen, rec->gen, rec->ttl, policy->base.total_timeout,
				n_fields, n_bins, policy->durable_delete);
		
		p = as_command_write_key(p, policy->key, key);

		if (policy->base.predexp) {
			p = as_predexp_list_write(policy->base.predexp, pred_size, p);
		}

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
		uint8_t* cmd = as_command_buffer_init(size);
		uint8_t* p = as_command_write_header(cmd, 0, AS_MSG_INFO2_WRITE, 0, policy->commit_level,
				policy->exists, policy->gen, rec->gen, rec->ttl, policy->base.total_timeout,
				n_fields, n_bins, policy->durable_delete);
		
		p = as_command_write_key(p, policy->key, key);

		if (policy->base.predexp) {
			p = as_predexp_list_write(policy->base.predexp, pred_size, p);
		}

		for (uint32_t i = 0; i < n_bins; i++) {
			p = as_command_write_bin(p, AS_OPERATOR_WRITE, &bins[i], &buffers[i]);
		}
		size = as_command_write_end(cmd, p);
		
		// Allocate command with compressed upper bound.
		size_t comp_size = as_command_compress_max_size(size);
		
		as_event_command* comp_cmd = as_async_write_command_create(
				cluster, &policy->base, policy->replica, pi.ns, pi.partition, AS_ASYNC_FLAGS_MASTER,
				listener, udata, event_loop, pipe_listener, comp_size, as_event_command_parse_header);

		// Compress buffer and execute.
		status = as_command_compress(err, cmd, size, comp_cmd->buf, &comp_size);
		as_command_buffer_free(cmd, size);
		
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
			as_cluster_release_partitions(cluster);
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
	if (! policy) {
		policy = &as->config.policies.remove;
	}
	
	as_cluster* cluster = as->cluster;
	as_partition_info pi;
	as_status status = as_key_partition_init(cluster, err, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	uint16_t n_fields;
	size_t size = as_command_key_size(policy->key, key, &n_fields);
	uint32_t pred_size = 0;

	if (policy->base.predexp) {
		size += as_predexp_list_size(policy->base.predexp, &pred_size);
		n_fields++;
	}

	uint8_t* buf = as_command_buffer_init(size);
	uint8_t* p = as_command_write_header(buf, 0, AS_MSG_INFO2_WRITE | AS_MSG_INFO2_DELETE, 0,
					policy->commit_level, AS_POLICY_EXISTS_IGNORE, policy->gen, policy->generation,
					0, policy->base.total_timeout, n_fields, 0, policy->durable_delete);

	p = as_command_write_key(p, policy->key, key);

	if (policy->base.predexp) {
		p = as_predexp_list_write(policy->base.predexp, pred_size, p);
	}

	size = as_command_write_end(buf, p);

	as_proto_msg msg;
	as_command cmd;
	as_command_init_write(&cmd, cluster, &policy->base, policy->replica, key, buf, size, &pi,
						  as_command_parse_header, &msg);

	status = as_command_execute(&cmd, err);

	as_cluster_release_partitions(cluster);
	as_command_buffer_free(buf, size);
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

	as_cluster* cluster = as->cluster;
	as_partition_info pi;
	as_status status = as_key_partition_init(cluster, err, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}
	
	uint16_t n_fields;
	size_t size = as_command_key_size(policy->key, key, &n_fields);
	uint32_t pred_size = 0;

	if (policy->base.predexp) {
		size += as_predexp_list_size(policy->base.predexp, &pred_size);
		n_fields++;
	}

	as_event_command* cmd = as_async_write_command_create(
		cluster, &policy->base, policy->replica, pi.ns, pi.partition, AS_ASYNC_FLAGS_MASTER,
		listener, udata, event_loop, pipe_listener, size, as_event_command_parse_header);

	uint8_t* p = as_command_write_header(cmd->buf, 0, AS_MSG_INFO2_WRITE | AS_MSG_INFO2_DELETE, 0,
		policy->commit_level, AS_POLICY_EXISTS_IGNORE, policy->gen, policy->generation, 0,
		policy->base.total_timeout, n_fields, 0, policy->durable_delete);
	
	p = as_command_write_key(p, policy->key, key);

	if (policy->base.predexp) {
		p = as_predexp_list_write(policy->base.predexp, pred_size, p);
	}

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
				op->op = AS_OPERATOR_CDT_READ;
			case AS_OPERATOR_BIT_READ:
				// Map operations require respond_all_ops to be true.
				respond_all_ops = true;
				// Fall through to read.
			case AS_OPERATOR_CDT_READ:
			case AS_OPERATOR_READ:
				read_attr |= AS_MSG_INFO1_READ;
				break;
				
			case AS_OPERATOR_MAP_MODIFY:
				op->op = AS_OPERATOR_CDT_MODIFY;
			case AS_OPERATOR_BIT_MODIFY:
				// Map operations require respond_all_ops to be true.
				respond_all_ops = true;
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
	uint32_t n_operations = ops->binops.size;

	if (n_operations == 0) {
		as_error_reset(err);
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "No operations defined");
	}

	as_cluster* cluster = as->cluster;
	as_partition_info pi;
	as_status status = as_key_partition_init(cluster, err, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_buffer* buffers = (as_buffer*)alloca(sizeof(as_buffer) * n_operations);
	memset(buffers, 0, sizeof(as_buffer) * n_operations);

	uint8_t read_attr;
	uint8_t write_attr;
	size_t size = as_operate_set_attr(ops, buffers, &read_attr, &write_attr);
	
	as_policy_operate policy_local;
	uint8_t info_attr = 0;

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

			as_command_set_attr_read(policy->read_mode_ap, policy->read_mode_sc, &read_attr,
									 &info_attr);
		}
	}

	uint16_t n_fields;
	size += as_command_key_size(policy->key, key, &n_fields);

	uint32_t pred_size = 0;

	if (policy->base.predexp) {
		size += as_predexp_list_size(policy->base.predexp, &pred_size);
		n_fields++;
	}

	uint8_t* buf = as_command_buffer_init(size);
	uint8_t* p = as_command_write_header(buf, read_attr, write_attr, info_attr, policy->commit_level,
										 policy->exists, policy->gen, ops->gen, ops->ttl,
										 policy->base.total_timeout, n_fields, n_operations,
										 policy->durable_delete);

	p = as_command_write_key(p, policy->key, key);

	if (policy->base.predexp) {
		p = as_predexp_list_write(policy->base.predexp, pred_size, p);
	}

	for (uint32_t i = 0; i < n_operations; i++) {
		as_binop* op = &ops->binops.entries[i];
		p = as_command_write_bin(p, op->op, &op->bin, &buffers[i]);
	}

	size = as_command_write_end(buf, p);

	as_command_parse_result_data data;
	data.record = rec;
	data.deserialize = policy->deserialize;

	as_command cmd;

	if (write_attr & AS_MSG_INFO2_WRITE) {
		as_command_init_write(&cmd, cluster, &policy->base, policy->replica, key, buf, size,
							  &pi, as_command_parse_result, &data);
	}
	else {
		as_command_init_read(&cmd, cluster, &policy->base, policy->replica,
							 policy->read_mode_sc, key, buf, size, &pi, as_command_parse_result,
							 &data);
	}

	status = as_command_execute(&cmd, err);

	as_cluster_release_partitions(cluster);
	as_command_buffer_free(buf, size);
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
		as_error_reset(err);
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "No operations defined");
	}
	
	as_buffer* buffers = (as_buffer*)alloca(sizeof(as_buffer) * n_operations);
	memset(buffers, 0, sizeof(as_buffer) * n_operations);
	
	uint8_t read_attr;
	uint8_t write_attr;
	size_t size = as_operate_set_attr(ops, buffers, &read_attr, &write_attr);

	as_policy_operate policy_local;
	uint8_t info_attr = 0;

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

			as_command_set_attr_read(policy->read_mode_ap, policy->read_mode_sc, &read_attr,
									 &info_attr);
		}
	}

	uint16_t n_fields;
	size += as_command_key_size(policy->key, key, &n_fields);

	uint32_t pred_size = 0;

	if (policy->base.predexp) {
		size += as_predexp_list_size(policy->base.predexp, &pred_size);
		n_fields++;
	}

	as_cluster* cluster = as->cluster;
	as_partition_info pi;
	as_status status = as_key_partition_init(cluster, err, key, &pi);

	if (status != AEROSPIKE_OK) {
		for (uint32_t i = 0; i < n_operations; i++) {
			as_buffer* buffer = &buffers[i];
			
			if (buffer->data) {
				cf_free(buffer->data);
			}
		}
		return status;
	}

	as_event_command* cmd;
	uint8_t* p;

	if (write_attr & AS_MSG_INFO2_WRITE) {
		cmd = as_async_record_command_create(
			cluster, &policy->base, policy->replica, pi.ns, pi.partition, policy->deserialize,
			AS_ASYNC_FLAGS_MASTER, listener, udata, event_loop, pipe_listener, size,
			as_event_command_parse_result);
	}
	else {
		as_read_info ri;
		as_event_command_init_read(policy->replica, policy->read_mode_sc, pi.sc_mode, &ri);

		cmd = as_async_record_command_create(
			cluster, &policy->base, ri.replica, pi.ns, pi.partition, policy->deserialize,
			ri.flags, listener, udata, event_loop, pipe_listener, size,
			as_event_command_parse_result);
	}

	p = as_command_write_header(cmd->buf, read_attr, write_attr, info_attr, policy->commit_level,
								policy->exists, policy->gen, ops->gen, ops->ttl,
								policy->base.total_timeout, n_fields, n_operations,
								policy->durable_delete);

	p = as_command_write_key(p, policy->key, key);

	if (policy->base.predexp) {
		p = as_predexp_list_write(policy->base.predexp, pred_size, p);
	}

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
	if (! policy) {
		policy = &as->config.policies.apply;
	}

	as_cluster* cluster = as->cluster;
	as_partition_info pi;
	as_status status = as_key_partition_init(cluster, err, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}
	
	uint16_t n_fields;
	size_t size = as_command_key_size(policy->key, key, &n_fields);
	uint32_t pred_size = 0;

	if (policy->base.predexp) {
		size += as_predexp_list_size(policy->base.predexp, &pred_size);
		n_fields++;
	}

	size += as_command_string_field_size(module);
	size += as_command_string_field_size(function);
	
	as_serializer ser;
	as_msgpack_init(&ser);
	as_buffer args;
	as_buffer_init(&args);
	as_serializer_serialize(&ser, (as_val*)arglist, &args);
	size += as_command_field_size(args.size);
	n_fields += 3;

	uint8_t* buf = as_command_buffer_init(size);
	uint8_t* p = as_command_write_header(buf, 0, AS_MSG_INFO2_WRITE, 0, policy->commit_level, 0,
		policy->gen, policy->gen_value, policy->ttl, policy->base.total_timeout, n_fields, 0,
		policy->durable_delete);

	p = as_command_write_key(p, policy->key, key);

	if (policy->base.predexp) {
		p = as_predexp_list_write(policy->base.predexp, pred_size, p);
	}

	p = as_command_write_field_string(p, AS_FIELD_UDF_PACKAGE_NAME, module);
	p = as_command_write_field_string(p, AS_FIELD_UDF_FUNCTION, function);
	p = as_command_write_field_buffer(p, AS_FIELD_UDF_ARGLIST, &args);
	size = as_command_write_end(buf, p);
	
	as_command cmd;
	as_command_init_write(&cmd, cluster, &policy->base, policy->replica, key, buf, size, &pi,
						  as_command_parse_success_failure, result);

	status = as_command_execute(&cmd, err);

	as_cluster_release_partitions(cluster);
	as_command_buffer_free(buf, size);
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
	
	as_cluster* cluster = as->cluster;
	as_partition_info pi;
	as_status status = as_key_partition_init(cluster, err, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}
	
	uint16_t n_fields;
	size_t size = as_command_key_size(policy->key, key, &n_fields);
	uint32_t pred_size = 0;

	if (policy->base.predexp) {
		size += as_predexp_list_size(policy->base.predexp, &pred_size);
		n_fields++;
	}

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
		cluster, &policy->base, policy->replica, pi.ns, pi.partition, AS_ASYNC_FLAGS_MASTER, listener,
		udata, event_loop, pipe_listener, size, as_event_command_parse_success_failure);

	uint8_t* p = as_command_write_header(
		cmd->buf, 0, AS_MSG_INFO2_WRITE, 0, policy->commit_level, 0, policy->gen, policy->gen_value,
		policy->ttl, policy->base.total_timeout, n_fields, 0, policy->durable_delete);

	p = as_command_write_key(p, policy->key, key);

	if (policy->base.predexp) {
		p = as_predexp_list_write(policy->base.predexp, pred_size, p);
	}

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
