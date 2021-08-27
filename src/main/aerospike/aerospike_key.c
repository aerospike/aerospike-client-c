/*
 * Copyright 2008-2021 Aerospike, Inc.
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
#include <aerospike/as_exp.h>
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
	as_command* cmd, as_cluster* cluster, const as_policy_base* policy, as_policy_replica replica,
	as_policy_read_mode_sc read_mode_sc, size_t size, as_partition_info* pi,
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
}

static inline as_status
as_command_execute_read(
	as_cluster* cluster, as_error* err, const as_policy_base* policy, as_policy_replica replica,
	as_policy_read_mode_sc read_mode_sc, uint8_t* buf, size_t size, as_partition_info* pi,
	const as_parse_results_fn fn, void* udata
	)
{
	as_command cmd;
	as_command_init_read(&cmd, cluster, policy, replica, read_mode_sc, size, pi,
						 fn, udata);

	cmd.buf = buf;
	as_command_start_timer(&cmd);
	return as_command_execute(&cmd, err);
}

static inline void
as_command_init_write(
	as_command* cmd, as_cluster* cluster, const as_policy_base* policy, as_policy_replica replica,
	size_t size, as_partition_info* pi, const as_parse_results_fn fn, void* udata
	)
{
	cmd->cluster = cluster;
	cmd->policy = policy;
	cmd->node = NULL;
	cmd->ns = pi->ns;
	cmd->partition = pi->partition;
	cmd->parse_results_fn = fn;
	cmd->udata = udata;
	cmd->buf_size = size;
	cmd->partition_id = pi->partition_id;
	cmd->flags = 0;

	switch (replica) {
		case AS_POLICY_REPLICA_PREFER_RACK:
			// Writes must always go to master node via sequence algorithm.
			cmd->replica = AS_POLICY_REPLICA_SEQUENCE;
			break;

		case AS_POLICY_REPLICA_ANY:
			// Writes must always go to master node.
			cmd->replica = AS_POLICY_REPLICA_MASTER;
			break;

		default:
			cmd->replica = replica;
			break;
	}
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

static inline uint32_t
as_command_filter_size(const as_policy_base* policy, uint16_t* n_fields)
{
	if (policy->filter_exp) {
		(*n_fields)++;
		return AS_FIELD_HEADER_SIZE + policy->filter_exp->packed_sz;
	}

	if (policy->predexp) {
		(*n_fields)++;
		uint32_t tmp = 0;
		return (uint32_t)as_predexp_list_size(policy->predexp, &tmp);
	}
	return 0;
}

static inline uint8_t*
as_command_write_filter(const as_policy_base* policy, uint32_t filter_size, uint8_t* p)
{
	if (policy->filter_exp) {
		return as_exp_write(policy->filter_exp, p);
	}

	if (policy->predexp) {
		// filter_size includes header size, so subtract that out.
		return as_predexp_list_write(policy->predexp, filter_size - AS_FIELD_HEADER_SIZE, p);
	}
	return p;
}

/******************************************************************************
 * GET
 *****************************************************************************/

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
	uint32_t filter_size = as_command_filter_size(&policy->base, &n_fields);
	size += filter_size;

	uint8_t* buf = as_command_buffer_init(size);
	uint32_t timeout = as_command_server_timeout(&policy->base);
	uint8_t* p = as_command_write_header_read(buf, &policy->base, policy->read_mode_ap,
		policy->read_mode_sc, timeout, n_fields, 0, AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_ALL);

	p = as_command_write_key(p, policy->key, key);
	p = as_command_write_filter(&policy->base, filter_size, p);
	size = as_command_write_end(buf, p);

	as_command_parse_result_data data;
	data.record = rec;
	data.deserialize = policy->deserialize;

	status = as_command_execute_read(cluster, err, &policy->base, policy->replica,
				policy->read_mode_sc, buf, size, &pi, as_command_parse_result, &data);

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
	uint32_t filter_size = as_command_filter_size(&policy->base, &n_fields);
	size += filter_size;

	as_event_command* cmd = as_async_record_command_create(
		cluster, &policy->base, ri.replica, pi.ns, pi.partition, policy->deserialize,
		policy->async_heap_rec, ri.flags, listener, udata, event_loop, pipe_listener,
		size, as_event_command_parse_result);

	uint32_t timeout = as_command_server_timeout(&policy->base);
	uint8_t* p = as_command_write_header_read(cmd->buf, &policy->base, policy->read_mode_ap,
		policy->read_mode_sc, timeout, n_fields, 0, AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_ALL);

	p = as_command_write_key(p, policy->key, key);
	p = as_command_write_filter(&policy->base, filter_size, p);
	cmd->write_len = (uint32_t)as_command_write_end(cmd->buf, p);
	return as_event_command_execute(cmd, err);
}

/******************************************************************************
 * SELECT
 *****************************************************************************/

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
	uint32_t filter_size = as_command_filter_size(&policy->base, &n_fields);
	size += filter_size;

	int nvalues = 0;

	for (nvalues = 0; bins[nvalues] != NULL && bins[nvalues][0] != '\0'; nvalues++) {
		status = as_command_bin_name_size(err, bins[nvalues], &size);

		if (status != AEROSPIKE_OK) {
			return status;
		}
	}

	uint8_t* buf = as_command_buffer_init(size);
	uint32_t timeout = as_command_server_timeout(&policy->base);
	uint8_t* p = as_command_write_header_read(buf, &policy->base, policy->read_mode_ap,
				policy->read_mode_sc, timeout, n_fields, nvalues, AS_MSG_INFO1_READ);

	p = as_command_write_key(p, policy->key, key);
	p = as_command_write_filter(&policy->base, filter_size, p);

	for (int i = 0; i < nvalues; i++) {
		p = as_command_write_bin_name(p, bins[i]);
	}
	size = as_command_write_end(buf, p);

	as_command_parse_result_data data;
	data.record = rec;
	data.deserialize = policy->deserialize;

	status = as_command_execute_read(cluster, err, &policy->base, policy->replica,
				policy->read_mode_sc, buf, size, &pi, as_command_parse_result, &data);

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
	uint32_t filter_size = as_command_filter_size(&policy->base, &n_fields);
	size += filter_size;

	int nvalues = 0;

	for (nvalues = 0; bins[nvalues] != NULL && bins[nvalues][0] != '\0'; nvalues++) {
		status = as_command_bin_name_size(err, bins[nvalues], &size);

		if (status != AEROSPIKE_OK) {
			return status;
		}
	}

	as_event_command* cmd = as_async_record_command_create(
		cluster, &policy->base, ri.replica, pi.ns, pi.partition, policy->deserialize,
		policy->async_heap_rec, ri.flags, listener, udata, event_loop, pipe_listener,
		size, as_event_command_parse_result);

	uint32_t timeout = as_command_server_timeout(&policy->base);
	uint8_t* p = as_command_write_header_read(cmd->buf, &policy->base, policy->read_mode_ap,
					policy->read_mode_sc, timeout, n_fields, nvalues, AS_MSG_INFO1_READ);

	p = as_command_write_key(p, policy->key, key);
	p = as_command_write_filter(&policy->base, filter_size, p);

	for (int i = 0; i < nvalues; i++) {
		p = as_command_write_bin_name(p, bins[i]);
	}
	cmd->write_len = (uint32_t)as_command_write_end(cmd->buf, p);
	return as_event_command_execute(cmd, err);
}

/******************************************************************************
 * EXISTS
 *****************************************************************************/

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
	uint32_t filter_size = as_command_filter_size(&policy->base, &n_fields);
	size += filter_size;

	uint8_t* buf = as_command_buffer_init(size);
	uint8_t* p = as_command_write_header_read_header(buf, &policy->base, policy->read_mode_ap,
		policy->read_mode_sc, n_fields, 0, AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_NOBINDATA);

	p = as_command_write_key(p, policy->key, key);
	p = as_command_write_filter(&policy->base, filter_size, p);
	size = as_command_write_end(buf, p);

	status = as_command_execute_read(cluster, err, &policy->base, policy->replica,
				policy->read_mode_sc, buf, size, &pi, as_command_parse_header, rec);

	as_command_buffer_free(buf, size);

	if (status != AEROSPIKE_OK && rec) {
		*rec = NULL;
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
	uint32_t filter_size = as_command_filter_size(&policy->base, &n_fields);
	size += filter_size;

	as_event_command* cmd = as_async_record_command_create(
		cluster, &policy->base, ri.replica, pi.ns, pi.partition, false, policy->async_heap_rec,
		ri.flags, listener, udata, event_loop, pipe_listener,
		size, as_event_command_parse_result);

	uint8_t* p = as_command_write_header_read_header(cmd->buf, &policy->base, policy->read_mode_ap,
		policy->read_mode_sc, n_fields, 0, AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_NOBINDATA);

	p = as_command_write_key(p, policy->key, key);
	p = as_command_write_filter(&policy->base, filter_size, p);
	cmd->write_len = (uint32_t)as_command_write_end(cmd->buf, p);
	return as_event_command_execute(cmd, err);
}

/******************************************************************************
 * PUT
 *****************************************************************************/

typedef struct as_put_s {
	const as_policy_write* policy;
	const as_key* key;
	as_record* rec;
	as_queue* buffers;
	uint32_t filter_size;
	uint16_t n_fields;
	uint16_t n_bins;
} as_put;

static size_t
as_put_init(
	as_put* put, const as_policy_write* policy, const as_key* key, as_record* rec, as_queue* buffers
	)
{
	put->policy = policy;
	put->key = key;
	put->rec = rec;
	put->buffers = buffers;

	size_t size = as_command_key_size(policy->key, key, &put->n_fields);

	uint16_t n_bins = rec->bins.size;
	put->n_bins = n_bins;

	put->filter_size = as_command_filter_size(&policy->base, &put->n_fields);
	size += put->filter_size;

	as_bin* bins = rec->bins.entries;

	for (uint16_t i = 0; i < n_bins; i++) {
		size += as_command_bin_size(&bins[i], buffers);
	}
	return size;
}

static size_t
as_put_write(void* udata, uint8_t* buf)
{
	as_put* put = udata;
	const as_policy_write* policy = put->policy;
	as_record* rec = put->rec;

	uint8_t* p = as_command_write_header_write(buf, &policy->base, policy->commit_level,
		policy->exists, policy->gen, rec->gen, rec->ttl, put->n_fields, put->n_bins,
		policy->durable_delete, 0, AS_MSG_INFO2_WRITE, 0);

	p = as_command_write_key(p, policy->key, put->key);
	p = as_command_write_filter(&policy->base, put->filter_size, p);

	as_bin* bins = rec->bins.entries;
	uint16_t n_bins = put->n_bins;
	as_queue* buffers = put->buffers;

	for (uint16_t i = 0; i < n_bins; i++) {
		p = as_command_write_bin(p, AS_OPERATOR_WRITE, &bins[i], buffers);
	}
	as_buffers_destroy(buffers);
	return as_command_write_end(buf, p);
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

	as_queue buffers;
	as_queue_inita(&buffers, sizeof(as_buffer), rec->bins.size);

	as_put put;
	size_t size = as_put_init(&put, policy, key, rec, &buffers);

	// Support new compress while still being compatible with old XDR compression_threshold.
	uint32_t compression_threshold = policy->compression_threshold;

	if (policy->base.compress && compression_threshold == 0) {
		compression_threshold = AS_COMPRESS_THRESHOLD;
	}

	as_command cmd;
	as_command_init_write(&cmd, cluster, &policy->base, policy->replica, size, &pi,
						  as_command_parse_header, NULL);

	status = as_command_send(&cmd, err, compression_threshold, as_put_write, &put);
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

	as_queue buffers;
	as_queue_inita(&buffers, sizeof(as_buffer), rec->bins.size);

	as_put put;
	size_t size = as_put_init(&put, policy, key, rec, &buffers);

	// Support new compress while still being compatible with old XDR compression_threshold.
	uint32_t compression_threshold = policy->compression_threshold;

	if (policy->base.compress && compression_threshold == 0) {
		compression_threshold = AS_COMPRESS_THRESHOLD;
	}

	if (compression_threshold == 0 || (size <= compression_threshold)) {
		// Send uncompressed command.
		as_event_command* cmd = as_async_write_command_create(
				cluster, &policy->base, policy->replica, pi.ns, pi.partition, AS_ASYNC_FLAGS_MASTER,
				listener, udata, event_loop, pipe_listener, size, as_event_command_parse_header);

		cmd->write_len = (uint32_t)as_put_write(&put, cmd->buf);

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
		size_t capacity = size;
		uint8_t* buf = as_command_buffer_init(capacity);
		size = as_put_write(&put, buf);

		// Allocate command with compressed upper bound.
		size_t comp_size = as_command_compress_max_size(size);
		as_event_command* cmd = as_async_write_command_create(
				cluster, &policy->base, policy->replica, pi.ns, pi.partition, AS_ASYNC_FLAGS_MASTER,
				listener, udata, event_loop, pipe_listener, comp_size, as_event_command_parse_header);

		// Compress buffer and execute.
		status = as_command_compress(err, buf, size, cmd->buf, &comp_size);
		as_command_buffer_free(buf, capacity);
		
		if (status == AEROSPIKE_OK) {
			cmd->write_len = (uint32_t)comp_size;

			if (length != NULL) {
				*length = size;
			}

			if (comp_length != NULL) {
				*comp_length = comp_size;
			}

			return as_event_command_execute(cmd, err);
		}
		else {
			cf_free(cmd);
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

/******************************************************************************
 * REMOVE
 *****************************************************************************/

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
	uint32_t filter_size = as_command_filter_size(&policy->base, &n_fields);
	size += filter_size;

	uint8_t* buf = as_command_buffer_init(size);
	uint8_t* p = as_command_write_header_write(buf, &policy->base, policy->commit_level,
					AS_POLICY_EXISTS_IGNORE, policy->gen, policy->generation, 0, n_fields, 0,
					policy->durable_delete, 0, AS_MSG_INFO2_WRITE | AS_MSG_INFO2_DELETE, 0);

	p = as_command_write_key(p, policy->key, key);
	p = as_command_write_filter(&policy->base, filter_size, p);
	size = as_command_write_end(buf, p);

	as_command cmd;
	as_command_init_write(&cmd, cluster, &policy->base, policy->replica, size, &pi,
						  as_command_parse_header, NULL);

	cmd.buf = buf;
	as_command_start_timer(&cmd);
	status = as_command_execute(&cmd, err);

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
	uint32_t filter_size = as_command_filter_size(&policy->base, &n_fields);
	size += filter_size;

	as_event_command* cmd = as_async_write_command_create(
		cluster, &policy->base, policy->replica, pi.ns, pi.partition, AS_ASYNC_FLAGS_MASTER,
		listener, udata, event_loop, pipe_listener, size, as_event_command_parse_header);

	uint8_t* p = as_command_write_header_write(cmd->buf, &policy->base, policy->commit_level,
					AS_POLICY_EXISTS_IGNORE, policy->gen, policy->generation, 0, n_fields, 0,
					policy->durable_delete, 0, AS_MSG_INFO2_WRITE | AS_MSG_INFO2_DELETE, 0);

	p = as_command_write_key(p, policy->key, key);
	p = as_command_write_filter(&policy->base, filter_size, p);
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

/******************************************************************************
 * OPERATE
 *****************************************************************************/

typedef struct as_operate_s {
	const as_policy_operate* policy;
	const as_key* key;
	const as_operations* ops;
	as_queue* buffers;
	uint32_t filter_size;
	uint16_t n_fields;
	uint16_t n_operations;
	uint8_t read_attr;
	uint8_t write_attr;
	uint8_t info_attr;
} as_operate;

static size_t
as_operate_set_attr(const as_operations* ops, as_queue* buffers, uint8_t* rattr, uint8_t* wattr)
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
			case AS_OPERATOR_EXP_READ:
			case AS_OPERATOR_BIT_READ:
			case AS_OPERATOR_HLL_READ:
				// Map operations require respond_all_ops to be true.
				respond_all_ops = true;
				// Fall through to read.
			case AS_OPERATOR_CDT_READ:
			case AS_OPERATOR_READ:
				read_attr |= AS_MSG_INFO1_READ;
				break;
				
			case AS_OPERATOR_MAP_MODIFY:
			case AS_OPERATOR_EXP_MODIFY:
			case AS_OPERATOR_BIT_MODIFY:
			case AS_OPERATOR_HLL_MODIFY:
				// Map operations require respond_all_ops to be true.
				respond_all_ops = true;
				// Fall through to write.
			default:
				write_attr |= AS_MSG_INFO2_WRITE;
				break;
		}
		size += as_command_bin_size(&op->bin, buffers);
	}
	
	if (respond_all_ops) {
		write_attr |= AS_MSG_INFO2_RESPOND_ALL_OPS;
	}
	*rattr = read_attr;
	*wattr = write_attr;
	return size;
}

static size_t
as_operate_init(
	as_operate* oper, aerospike* as, const as_policy_operate* policy,
	as_policy_operate* policy_local, const as_key* key, const as_operations* ops, as_queue* buffers
	)
{
	oper->n_operations = ops->binops.size;
	oper->buffers = buffers;

	size_t size = as_operate_set_attr(ops, buffers, &oper->read_attr, &oper->write_attr);
	oper->info_attr = 0;

	if (! policy) {
		if (oper->write_attr & AS_MSG_INFO2_WRITE) {
			// Write operations should not retry by default.
			policy = &as->config.policies.operate;
		}
		else {
			// Read operations should retry by default.
			as_policy_operate_copy(&as->config.policies.operate, policy_local);
			policy_local->base.max_retries = 2;
			policy = policy_local;
		}
	}

	oper->policy = policy;
	oper->key = key;
	oper->ops = ops;

	as_command_set_attr_read(policy->read_mode_ap, policy->read_mode_sc, policy->base.compress,
							 &oper->read_attr, &oper->info_attr);

	size += as_command_key_size(policy->key, key, &oper->n_fields);
	oper->filter_size = as_command_filter_size(&policy->base, &oper->n_fields);
	size += oper->filter_size;
	return size;
}

static size_t
as_operate_write(void* udata, uint8_t* buf)
{
	as_operate* oper = udata;
	const as_policy_operate* policy = oper->policy;
	const as_operations* ops = oper->ops;

	uint8_t* p = as_command_write_header_write(buf, &policy->base, policy->commit_level,
		policy->exists, policy->gen, ops->gen, ops->ttl, oper->n_fields,
		oper->n_operations, policy->durable_delete, oper->read_attr, oper->write_attr,
		oper->info_attr);

	p = as_command_write_key(p, policy->key, oper->key);
	p = as_command_write_filter(&policy->base, oper->filter_size, p);

	uint16_t n_operations = oper->n_operations;
	as_queue* buffers = oper->buffers;

	for (uint16_t i = 0; i < n_operations; i++) {
		as_binop* op = &ops->binops.entries[i];
		p = as_command_write_bin(p, op->op, &op->bin, buffers);
	}
	as_buffers_destroy(buffers);
	return as_command_write_end(buf, p);
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

	as_queue buffers;
	as_queue_inita(&buffers, sizeof(as_buffer), n_operations);

	as_policy_operate policy_local;
	as_operate oper;
	size_t size = as_operate_init(&oper, as, policy, &policy_local, key, ops, &buffers);
	policy = oper.policy;

	as_command_parse_result_data data;
	data.record = rec;
	data.deserialize = policy->deserialize;

	as_command cmd;

	if (oper.write_attr & AS_MSG_INFO2_WRITE) {
		as_command_init_write(&cmd, cluster, &policy->base, policy->replica, size, &pi,
							  as_command_parse_result, &data);
	}
	else {
		as_command_init_read(&cmd, cluster, &policy->base, policy->replica, policy->read_mode_sc,
							 size, &pi, as_command_parse_result, &data);
	}

	uint32_t compression_threshold = policy->base.compress ? AS_COMPRESS_THRESHOLD : 0;

	status = as_command_send(&cmd, err, compression_threshold, as_operate_write, &oper);

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
	
	as_cluster* cluster = as->cluster;
	as_partition_info pi;
	as_status status = as_key_partition_init(cluster, err, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_queue buffers;
	as_queue_inita(&buffers, sizeof(as_buffer), n_operations);

	as_policy_operate policy_local;
	as_operate oper;
	size_t size = as_operate_init(&oper, as, policy, &policy_local, key, ops, &buffers);
	policy = oper.policy;

	as_event_command* cmd;

	if (! (policy->base.compress && size > AS_COMPRESS_THRESHOLD)) {
		// Send uncompressed command.
		if (oper.write_attr & AS_MSG_INFO2_WRITE) {
			cmd = as_async_record_command_create(
				cluster, &policy->base, policy->replica, pi.ns, pi.partition, policy->deserialize,
				policy->async_heap_rec, AS_ASYNC_FLAGS_MASTER, listener, udata, event_loop,
				pipe_listener, size, as_event_command_parse_result);
		}
		else {
			as_read_info ri;
			as_event_command_init_read(policy->replica, policy->read_mode_sc, pi.sc_mode, &ri);

			cmd = as_async_record_command_create(
				cluster, &policy->base, ri.replica, pi.ns, pi.partition, policy->deserialize,
				policy->async_heap_rec, ri.flags, listener, udata, event_loop, pipe_listener, size,
				as_event_command_parse_result);
		}

		cmd->write_len = (uint32_t)as_operate_write(&oper, cmd->buf);
	}
	else {
		// Send compressed command.
		// First write uncompressed buffer.
		size_t capacity = size;
		uint8_t* buf = as_command_buffer_init(capacity);
		size = as_operate_write(&oper, buf);

		// Allocate command with compressed upper bound.
		size_t comp_size = as_command_compress_max_size(size);

		if (oper.write_attr & AS_MSG_INFO2_WRITE) {
			cmd = as_async_record_command_create(
				cluster, &policy->base, policy->replica, pi.ns, pi.partition, policy->deserialize,
				policy->async_heap_rec, AS_ASYNC_FLAGS_MASTER, listener, udata, event_loop,
				pipe_listener, comp_size, as_event_command_parse_result);
		}
		else {
			as_read_info ri;
			as_event_command_init_read(policy->replica, policy->read_mode_sc, pi.sc_mode, &ri);

			cmd = as_async_record_command_create(
				cluster, &policy->base, ri.replica, pi.ns, pi.partition, policy->deserialize,
				policy->async_heap_rec, ri.flags, listener, udata, event_loop, pipe_listener,
				comp_size, as_event_command_parse_result);
		}

		// Compress buffer and execute.
		status = as_command_compress(err, buf, size, cmd->buf, &comp_size);
		as_command_buffer_free(buf, capacity);

		if (status != AEROSPIKE_OK) {
			cf_free(cmd);
			return status;
		}

		cmd->write_len = (uint32_t)comp_size;
	}
	return as_event_command_execute(cmd, err);
}

/******************************************************************************
 * APPLY
 *****************************************************************************/

typedef struct as_apply_s {
	const as_policy_apply* policy;
	const as_key* key;
	const char* module;
	const char* function;
	as_serializer ser;
	as_buffer args;
	uint32_t filter_size;
	uint16_t n_fields;
	uint8_t read_attr;
} as_apply;

static size_t
as_apply_init(
	as_apply* ap, const as_policy_apply* policy, const as_key* key, const char* module,
	const char* function, as_list* arglist
	)
{
	ap->policy = policy;
	ap->key = key;
	ap->module = module;
	ap->function = function;
	ap->read_attr = 0;

	size_t size = as_command_key_size(policy->key, key, &ap->n_fields);

	ap->filter_size = as_command_filter_size(&policy->base, &ap->n_fields);
	size += ap->filter_size;

	size += as_command_string_field_size(module);
	size += as_command_string_field_size(function);
	
	as_msgpack_init(&ap->ser);
	as_buffer_init(&ap->args);
	as_serializer_serialize(&ap->ser, (as_val*)arglist, &ap->args);
	size += as_command_field_size(ap->args.size);
	ap->n_fields += 3;

	as_command_set_attr_compress(policy->base.compress, &ap->read_attr);
	return size;
}

static size_t
as_apply_write(void* udata, uint8_t* buf)
{
	as_apply* ap = udata;
	const as_policy_apply* policy = ap->policy;

	uint8_t* p = as_command_write_header_write(buf, &policy->base, policy->commit_level, 0,
		AS_POLICY_GEN_IGNORE, 0, policy->ttl, ap->n_fields, 0, policy->durable_delete,
		ap->read_attr, AS_MSG_INFO2_WRITE, 0);

	p = as_command_write_key(p, policy->key, ap->key);
	p = as_command_write_filter(&policy->base, ap->filter_size, p);
	p = as_command_write_field_string(p, AS_FIELD_UDF_PACKAGE_NAME, ap->module);
	p = as_command_write_field_string(p, AS_FIELD_UDF_FUNCTION, ap->function);
	p = as_command_write_field_buffer(p, AS_FIELD_UDF_ARGLIST, &ap->args);
	return as_command_write_end(buf, p);
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

	as_apply ap;
	size_t size = as_apply_init(&ap, policy, key, module, function, arglist);

	as_command cmd;
	as_command_init_write(&cmd, cluster, &policy->base, policy->replica, size, &pi,
						  as_command_parse_success_failure, result);

	uint32_t compression_threshold = policy->base.compress ? AS_COMPRESS_THRESHOLD : 0;

	status = as_command_send(&cmd, err, compression_threshold, as_apply_write, &ap);

	as_buffer_destroy(&ap.args);
	as_serializer_destroy(&ap.ser);
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
	
	as_apply ap;
	size_t size = as_apply_init(&ap, policy, key, module, function, arglist);

	if (! (policy->base.compress && size > AS_COMPRESS_THRESHOLD)) {
		// Send uncompressed command.
		as_event_command* cmd = as_async_value_command_create(cluster, &policy->base,
			policy->replica, pi.ns, pi.partition, AS_ASYNC_FLAGS_MASTER, listener, udata,
			event_loop, pipe_listener, size, as_event_command_parse_success_failure);

		cmd->write_len = (uint32_t)as_apply_write(&ap, cmd->buf);

		as_buffer_destroy(&ap.args);
		as_serializer_destroy(&ap.ser);
		return as_event_command_execute(cmd, err);
	}
	else {
		// Send compressed command.
		// First write uncompressed buffer.
		size_t capacity = size;
		uint8_t* buf = as_command_buffer_init(capacity);
		size = as_apply_write(&ap, buf);

		as_buffer_destroy(&ap.args);
		as_serializer_destroy(&ap.ser);

		// Allocate command with compressed upper bound.
		size_t comp_size = as_command_compress_max_size(size);

		as_event_command* cmd = as_async_value_command_create(cluster, &policy->base,
			policy->replica, pi.ns, pi.partition, AS_ASYNC_FLAGS_MASTER, listener, udata,
			event_loop, pipe_listener, comp_size, as_event_command_parse_success_failure);

		// Compress buffer and execute.
		status = as_command_compress(err, buf, size, cmd->buf, &comp_size);
		as_command_buffer_free(buf, capacity);

		if (status != AEROSPIKE_OK) {
			cf_free(cmd);
			return status;
		}

		cmd->write_len = (uint32_t)comp_size;
		return as_event_command_execute(cmd, err);
	}
}
