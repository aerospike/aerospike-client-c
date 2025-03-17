/*
 * Copyright 2008-2025 Aerospike, Inc.
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
#include <aerospike/as_random.h>
#include <aerospike/as_record.h>
#include <aerospike/as_serializer.h>
#include <aerospike/as_shm_cluster.h>
#include <aerospike/as_status.h>
#include <aerospike/as_txn.h>
#include <aerospike/as_txn_monitor.h>
#include <citrusleaf/cf_clock.h>
#include <citrusleaf/cf_digest.h>

//---------------------------------
// Types
//---------------------------------

typedef struct as_read_info_s {
	as_policy_replica replica;
	uint8_t flags;
	uint8_t replica_index;
} as_read_info;

//---------------------------------
// Functions
//---------------------------------

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

static as_status
as_command_prepare(
	as_cluster* cluster, as_error* err, const as_policy_base* policy, const as_key* key,
	as_partition_info* pi
	)
{
	as_status status = as_key_partition_init(cluster, err, key, pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	if (policy->txn) {
		status = as_txn_verify_command(policy->txn, err);

		if (status != AEROSPIKE_OK) {
			return status;
		}

		status = as_txn_set_ns(policy->txn, key->ns, err);
	}

	return status;
}

static as_status
as_command_prepare_write(
	aerospike* as, as_error* err, const as_policy_base* policy, const as_key* key,
	as_partition_info* pi
	)
{
	as_status status = as_key_partition_init(as->cluster, err, key, pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	if (policy->txn) {
		status = as_txn_verify_command(policy->txn, err);

		if (status != AEROSPIKE_OK) {
			return status;
		}

		status = as_txn_set_ns(policy->txn, key->ns, err);

		if (status != AEROSPIKE_OK) {
			return status;
		}

		status = as_txn_monitor_add_key(as, policy, key, err);
	}

	return status;
}

static inline void
as_command_init_read(
	as_command* cmd, as_cluster* cluster, const as_policy_base* policy, as_policy_replica replica,
	as_policy_read_mode_sc read_mode_sc, const as_key* key, size_t size, as_partition_info* pi,
	const as_parse_results_fn fn, void* udata
	)
{
	cmd->cluster = cluster;
	cmd->policy = policy;
	cmd->node = NULL;
	cmd->key = key;
	cmd->partition = pi->partition;
	cmd->parse_results_fn = fn;
	cmd->udata = udata;
	cmd->buf_size = size;
	cmd->partition_id = pi->partition_id;
	cmd->latency_type = AS_LATENCY_TYPE_READ;
	as_cluster_add_command_count(cluster);

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
	cmd->replica_size = pi->replica_size;
	cmd->replica_index = as_replica_index_init_read(cmd->replica);
}

static inline as_status
as_command_execute_read(
	as_cluster* cluster, as_error* err, const as_policy_base* policy, as_policy_replica replica,
	as_policy_read_mode_sc read_mode_sc, const as_key* key, uint8_t* buf, size_t size,
	as_partition_info* pi, const as_parse_results_fn fn, void* udata
	)
{
	as_command cmd;
	as_command_init_read(&cmd, cluster, policy, replica, read_mode_sc, key, size, pi,
						 fn, udata);

	cmd.buf = buf;
	as_command_start_timer(&cmd);
	return as_command_execute(&cmd, err);
}

static inline void
as_command_init_write(
	as_command* cmd, as_cluster* cluster, const as_policy_base* policy, as_policy_replica replica,
	const as_key* key, size_t size, as_partition_info* pi, const as_parse_results_fn fn, void* udata
	)
{
	cmd->cluster = cluster;
	cmd->policy = policy;
	cmd->node = NULL;
	cmd->key = key;
	cmd->partition = pi->partition;
	cmd->parse_results_fn = fn;
	cmd->udata = udata;
	cmd->buf_size = size;
	cmd->partition_id = pi->partition_id;
	cmd->flags = 0;
	cmd->replica = as_command_write_replica(replica);
	cmd->replica_size = pi->replica_size;
	cmd->replica_index = 0;
	cmd->latency_type = AS_LATENCY_TYPE_WRITE;
	as_cluster_add_command_count(cluster);
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
				ri->flags = AS_ASYNC_FLAGS_READ;
				break;

			case AS_POLICY_READ_MODE_SC_LINEARIZE:
				ri->replica = (replica != AS_POLICY_REPLICA_PREFER_RACK) ?
							   replica : AS_POLICY_REPLICA_SEQUENCE;
				ri->flags = AS_ASYNC_FLAGS_READ | AS_ASYNC_FLAGS_LINEARIZE;
				break;

			default:
				ri->replica = replica;
				ri->flags = AS_ASYNC_FLAGS_READ;
				break;
		}
	}
	else {
		ri->replica = replica;
		ri->flags =  AS_ASYNC_FLAGS_READ;
	}

	ri->replica_index = as_replica_index_init_read(ri->replica);
}

static inline uint32_t
as_command_filter_size(const as_policy_base* policy, uint16_t* n_fields)
{
	if (policy->filter_exp) {
		(*n_fields)++;
		return AS_FIELD_HEADER_SIZE + policy->filter_exp->packed_sz;
	}
	return 0;
}

static inline uint8_t*
as_command_write_filter(const as_policy_base* policy, uint32_t filter_size, uint8_t* p)
{
	if (policy->filter_exp) {
		return as_exp_write(policy->filter_exp, p);
	}
	return p;
}

static void
as_write_command_notify(as_error* err, as_event_command* cmd, as_event_loop* event_loop)
{
	switch (cmd->type) {
		case AS_ASYNC_TYPE_WRITE:
			((as_async_write_command*)cmd)->listener(err, cmd->udata, cmd->event_loop);
			break;
		case AS_ASYNC_TYPE_RECORD:
			((as_async_record_command*)cmd)->listener(err, 0, cmd->udata, cmd->event_loop);
			break;
		case AS_ASYNC_TYPE_VALUE:
			((as_async_value_command*)cmd)->listener(err, 0, cmd->udata, cmd->event_loop);
			break;
	}
}

static void
as_txn_monitor_callback(as_error* err, as_record* rec, void* udata, as_event_loop* event_loop)
{
	as_event_command* cmd = udata;

	if (err) {
		as_write_command_notify(err, cmd, event_loop);
		as_event_command_destroy(cmd);
		return;
	}

	if (cmd->ubuf) {
		// Set deadline in command buffer and compress.
		// Uncompressed size is located in the overloaded cmd->len.
		// Deadline offset is located in the overloaded cmd->pos.
		*(uint32_t*)(cmd->ubuf + cmd->pos) = cmd->txn->deadline;

		size_t comp_size = cmd->write_len;
		as_status status = as_command_compress(err, cmd->ubuf, cmd->len, cmd->buf, &comp_size);

		if (status == AEROSPIKE_OK) {
			cmd->write_len = (uint32_t)comp_size;
		}
		else {
			as_write_command_notify(err, cmd, event_loop);
			as_event_command_destroy(cmd);
			return;
		}
	}
	else {
		// Set deadline in command buffer.
		// Deadline offset is located in the overloaded cmd->pos.
		*(uint32_t*)(cmd->buf + cmd->pos) = cmd->txn->deadline;
	}

	// Run original command.
	as_status status = as_event_command_execute(cmd, err);

	if (status != AEROSPIKE_OK) {
		as_write_command_notify(err, cmd, event_loop);
		// Do not destroy command because as_event_command_execute() does that on error.
	}
}

static as_status
as_event_command_execute_txn(
	aerospike* as, as_error* err, const as_policy_base* cmd_policy, const as_key* key,
	as_event_command* cmd
	)
{
	as_status status = as_txn_monitor_add_key_async(as, err, cmd_policy, key, as_txn_monitor_callback,
		cmd, cmd->event_loop);

	if (status != AEROSPIKE_OK) {
		as_event_command_destroy(cmd);
	}
	return status;
}

static inline bool
as_txn_key_add(as_txn* txn, const as_key* key)
{
	return txn && !as_txn_writes_contain(txn, key);
}

static inline as_status
as_async_command_execute(
	aerospike* as, as_error* err, const as_policy_base* policy, const as_key* key,
	as_event_command* cmd, as_command_txn_data* tdata
	)
{
	if (as_txn_key_add(policy->txn, key)) {
		// Use overloaded pos to store deadline offset.
		cmd->pos = tdata->deadline_offset;
		return as_event_command_execute_txn(as, err, policy, key, cmd);
	}
	else {
		return as_event_command_execute(cmd, err);
	}
}

static as_status
as_async_compress_command_execute(
	aerospike* as, as_error* err, const as_policy_base* policy, const as_key* key,
	as_event_command* cmd, as_command_txn_data* tdata, uint8_t* ubuf, size_t size,
	size_t comp_size, size_t* length, size_t* comp_length
	)
{
	if (as_txn_key_add(policy->txn, key)) {
		// Delay compression until key is added to txn monitor and txn deadline is returned.
		// Use overloaded len to store uncompressed size.
		// Use overloaded pos to store deadline offset.
		cmd->write_len = (uint32_t)comp_size;
		cmd->len = (uint32_t)size;
		cmd->pos = tdata->deadline_offset;
		return as_event_command_execute_txn(as, err, policy, key, cmd);
	}
	else {
		// Compress buffer and execute.
		as_status status = as_command_compress(err, ubuf, size, cmd->buf, &comp_size);

		if (status != AEROSPIKE_OK) {
			as_event_command_destroy(cmd);
			return status;
		}

		cmd->write_len = (uint32_t)comp_size;

		if (length != NULL) {
			*length = size;
		}

		if (comp_length != NULL) {
			*comp_length = comp_size;
		}

		return as_event_command_execute(cmd, err);
	}
}

//---------------------------------
// Read All
//---------------------------------

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
	as_status status = as_command_prepare(cluster, err, &policy->base, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_command_txn_data tdata;
	size_t size = as_command_key_size(&policy->base, policy->key, key, false, &tdata);
	uint32_t filter_size = as_command_filter_size(&policy->base, &tdata.n_fields);
	size += filter_size;

	uint8_t* buf = as_command_buffer_init(size);
	uint32_t timeout = as_command_server_timeout(&policy->base);
	uint8_t* p = as_command_write_header_read(buf, &policy->base, policy->read_mode_ap,
		policy->read_mode_sc, policy->read_touch_ttl_percent, timeout, tdata.n_fields, 0,
		AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_ALL, 0, 0);

	p = as_command_write_key(p, &policy->base, policy->key, key, &tdata);
	p = as_command_write_filter(&policy->base, filter_size, p);
	size = as_command_write_end(buf, p);

	as_command_parse_result_data data;
	data.record = rec;
	data.deserialize = policy->deserialize;

	status = as_command_execute_read(cluster, err, &policy->base, policy->replica,
				policy->read_mode_sc, key, buf, size, &pi, as_command_parse_result, &data);

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
	as_status status = as_command_prepare(cluster, err, &policy->base, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_read_info ri;
	as_event_command_init_read(policy->replica, policy->read_mode_sc, pi.sc_mode, &ri);

	as_command_txn_data tdata;
	size_t size = as_command_key_size(&policy->base, policy->key, key, false, &tdata);
	uint32_t filter_size = as_command_filter_size(&policy->base, &tdata.n_fields);
	size += filter_size;

	as_event_command* cmd = as_async_record_command_create(
		cluster, &policy->base, &pi, ri.replica, ri.replica_index, policy->deserialize,
		policy->async_heap_rec, ri.flags, listener, udata, event_loop, pipe_listener, size,
		as_event_command_parse_result, AS_ASYNC_TYPE_RECORD, AS_LATENCY_TYPE_READ, NULL, 0);

	uint32_t timeout = as_command_server_timeout(&policy->base);
	uint8_t* p = as_command_write_header_read(cmd->buf, &policy->base, policy->read_mode_ap,
		policy->read_mode_sc, policy->read_touch_ttl_percent, timeout, tdata.n_fields, 0,
		AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_ALL, 0, 0);

	p = as_command_write_key(p, &policy->base, policy->key, key, &tdata);
	p = as_command_write_filter(&policy->base, filter_size, p);
	cmd->write_len = (uint32_t)as_command_write_end(cmd->buf, p);
	return as_event_command_execute(cmd, err);
}

//---------------------------------
// Read Selected Bins
//---------------------------------

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
	as_status status = as_command_prepare(cluster, err, &policy->base, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_command_txn_data tdata;
	size_t size = as_command_key_size(&policy->base, policy->key, key, false, &tdata);
	uint32_t filter_size = as_command_filter_size(&policy->base, &tdata.n_fields);
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
				policy->read_mode_sc, policy->read_touch_ttl_percent, timeout, tdata.n_fields, nvalues,
				AS_MSG_INFO1_READ, 0, 0);

	p = as_command_write_key(p, &policy->base, policy->key, key, &tdata);
	p = as_command_write_filter(&policy->base, filter_size, p);

	for (int i = 0; i < nvalues; i++) {
		p = as_command_write_bin_name(p, bins[i]);
	}
	size = as_command_write_end(buf, p);

	as_command_parse_result_data data;
	data.record = rec;
	data.deserialize = policy->deserialize;

	status = as_command_execute_read(cluster, err, &policy->base, policy->replica,
				policy->read_mode_sc, key, buf, size, &pi, as_command_parse_result, &data);

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
	as_status status = as_command_prepare(cluster, err, &policy->base, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}
	
	as_read_info ri;
	as_event_command_init_read(policy->replica, policy->read_mode_sc, pi.sc_mode, &ri);

	as_command_txn_data tdata;
	size_t size = as_command_key_size(&policy->base, policy->key, key, false, &tdata);
	uint32_t filter_size = as_command_filter_size(&policy->base, &tdata.n_fields);
	size += filter_size;

	int nvalues = 0;

	for (nvalues = 0; bins[nvalues] != NULL && bins[nvalues][0] != '\0'; nvalues++) {
		status = as_command_bin_name_size(err, bins[nvalues], &size);

		if (status != AEROSPIKE_OK) {
			return status;
		}
	}

	as_event_command* cmd = as_async_record_command_create(
		cluster, &policy->base, &pi, ri.replica, ri.replica_index, policy->deserialize,
		policy->async_heap_rec, ri.flags, listener, udata, event_loop, pipe_listener, size,
		as_event_command_parse_result, AS_ASYNC_TYPE_RECORD, AS_LATENCY_TYPE_READ, NULL, 0);

	uint32_t timeout = as_command_server_timeout(&policy->base);
	uint8_t* p = as_command_write_header_read(cmd->buf, &policy->base, policy->read_mode_ap,
					policy->read_mode_sc, policy->read_touch_ttl_percent, timeout, tdata.n_fields, nvalues,
					AS_MSG_INFO1_READ, 0, 0);

	p = as_command_write_key(p, &policy->base, policy->key, key, &tdata);
	p = as_command_write_filter(&policy->base, filter_size, p);

	for (int i = 0; i < nvalues; i++) {
		p = as_command_write_bin_name(p, bins[i]);
	}
	cmd->write_len = (uint32_t)as_command_write_end(cmd->buf, p);
	return as_event_command_execute(cmd, err);
}

as_status
aerospike_key_select_bins(
	aerospike* as, as_error* err, const as_policy_read* policy, const as_key* key,
	const char* bins[], uint32_t n_bins, as_record** rec
	)
{
	if (! policy) {
		policy = &as->config.policies.read;
	}
	
	as_cluster* cluster = as->cluster;
	as_partition_info pi;
	as_status status = as_command_prepare(cluster, err, &policy->base, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_command_txn_data tdata;
	size_t size = as_command_key_size(&policy->base, policy->key, key, false, &tdata);
	uint32_t filter_size = as_command_filter_size(&policy->base, &tdata.n_fields);
	size += filter_size;

	for (uint32_t i = 0; i < n_bins; i++) {
		status = as_command_bin_name_size(err, bins[i], &size);

		if (status != AEROSPIKE_OK) {
			return status;
		}
	}

	uint8_t* buf = as_command_buffer_init(size);
	uint32_t timeout = as_command_server_timeout(&policy->base);
	uint8_t* p = as_command_write_header_read(buf, &policy->base, policy->read_mode_ap,
				policy->read_mode_sc, policy->read_touch_ttl_percent, timeout, tdata.n_fields, n_bins,
				AS_MSG_INFO1_READ, 0, 0);

	p = as_command_write_key(p, &policy->base, policy->key, key, &tdata);
	p = as_command_write_filter(&policy->base, filter_size, p);

	for (uint32_t i = 0; i < n_bins; i++) {
		p = as_command_write_bin_name(p, bins[i]);
	}
	size = as_command_write_end(buf, p);

	as_command_parse_result_data data;
	data.record = rec;
	data.deserialize = policy->deserialize;

	status = as_command_execute_read(cluster, err, &policy->base, policy->replica,
				policy->read_mode_sc, key, buf, size, &pi, as_command_parse_result, &data);

	as_command_buffer_free(buf, size);
	return status;
}

as_status
aerospike_key_select_bins_async(
	aerospike* as, as_error* err, const as_policy_read* policy, const as_key* key, const char* bins[],
	uint32_t n_bins, as_async_record_listener listener, void* udata, as_event_loop* event_loop,
	as_pipe_listener pipe_listener
	)
{
	if (! policy) {
		policy = &as->config.policies.read;
	}
	
	as_cluster* cluster = as->cluster;
	as_partition_info pi;
	as_status status = as_command_prepare(cluster, err, &policy->base, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}
	
	as_read_info ri;
	as_event_command_init_read(policy->replica, policy->read_mode_sc, pi.sc_mode, &ri);

	as_command_txn_data tdata;
	size_t size = as_command_key_size(&policy->base, policy->key, key, false, &tdata);
	uint32_t filter_size = as_command_filter_size(&policy->base, &tdata.n_fields);
	size += filter_size;

	for (uint32_t i = 0; i < n_bins; i++) {
		status = as_command_bin_name_size(err, bins[i], &size);

		if (status != AEROSPIKE_OK) {
			return status;
		}
	}

	as_event_command* cmd = as_async_record_command_create(
		cluster, &policy->base, &pi, ri.replica, ri.replica_index, policy->deserialize,
		policy->async_heap_rec, ri.flags, listener, udata, event_loop, pipe_listener, size,
		as_event_command_parse_result, AS_ASYNC_TYPE_RECORD, AS_LATENCY_TYPE_READ, NULL, 0);

	uint32_t timeout = as_command_server_timeout(&policy->base);
	uint8_t* p = as_command_write_header_read(cmd->buf, &policy->base, policy->read_mode_ap,
					policy->read_mode_sc, policy->read_touch_ttl_percent, timeout, tdata.n_fields, n_bins,
					AS_MSG_INFO1_READ, 0, 0);

	p = as_command_write_key(p, &policy->base, policy->key, key, &tdata);
	p = as_command_write_filter(&policy->base, filter_size, p);

	for (uint32_t i = 0; i < n_bins; i++) {
		p = as_command_write_bin_name(p, bins[i]);
	}
	cmd->write_len = (uint32_t)as_command_write_end(cmd->buf, p);
	return as_event_command_execute(cmd, err);
}

//---------------------------------
// Exists
//---------------------------------

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
	as_status status = as_command_prepare(cluster, err, &policy->base, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_command_txn_data tdata;
	size_t size = as_command_key_size(&policy->base, policy->key, key, false, &tdata);
	uint32_t filter_size = as_command_filter_size(&policy->base, &tdata.n_fields);
	size += filter_size;

	uint8_t* buf = as_command_buffer_init(size);
	uint8_t* p = as_command_write_header_read_header(buf, &policy->base, policy->read_mode_ap,
		policy->read_mode_sc, policy->read_touch_ttl_percent, tdata.n_fields, 0,
		AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_NOBINDATA);

	p = as_command_write_key(p, &policy->base, policy->key, key, &tdata);
	p = as_command_write_filter(&policy->base, filter_size, p);
	size = as_command_write_end(buf, p);

	status = as_command_execute_read(cluster, err, &policy->base, policy->replica,
				policy->read_mode_sc, key, buf, size, &pi, as_command_parse_header, rec);

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
	as_status status = as_command_prepare(cluster, err, &policy->base, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}
	
	as_read_info ri;
	as_event_command_init_read(policy->replica, policy->read_mode_sc, pi.sc_mode, &ri);

	as_command_txn_data tdata;
	size_t size = as_command_key_size(&policy->base, policy->key, key, false, &tdata);
	uint32_t filter_size = as_command_filter_size(&policy->base, &tdata.n_fields);
	size += filter_size;

	as_event_command* cmd = as_async_record_command_create(
		cluster, &policy->base, &pi, ri.replica, ri.replica_index, false, policy->async_heap_rec,
		ri.flags, listener, udata, event_loop, pipe_listener, size, as_event_command_parse_result,
		AS_ASYNC_TYPE_RECORD, AS_LATENCY_TYPE_READ, NULL, 0);

	uint8_t* p = as_command_write_header_read_header(cmd->buf, &policy->base, policy->read_mode_ap,
		policy->read_mode_sc, policy->read_touch_ttl_percent, tdata.n_fields, 0,
		AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_NOBINDATA);

	p = as_command_write_key(p, &policy->base, policy->key, key, &tdata);
	p = as_command_write_filter(&policy->base, filter_size, p);
	cmd->write_len = (uint32_t)as_command_write_end(cmd->buf, p);
	return as_event_command_execute(cmd, err);
}

//---------------------------------
// Put
//---------------------------------

typedef struct as_put_s {
	const as_policy_write* policy;
	const as_key* key;
	as_record* rec;
	as_queue* buffers;
	size_t size;
	as_command_txn_data tdata;
	uint32_t filter_size;
	uint16_t n_bins;
} as_put;

static as_status
as_put_init(
	as_put* put, const as_policy_write* policy, const as_key* key, as_record* rec,
	as_queue* buffers, as_error* err
	)
{
	put->policy = policy;
	put->key = key;
	put->rec = rec;
	put->buffers = buffers;
	put->size = as_command_key_size(&policy->base, policy->key, key, true, &put->tdata);
	put->filter_size = as_command_filter_size(&policy->base, &put->tdata.n_fields);
	put->size += put->filter_size;
	put->n_bins = rec->bins.size;

	as_bin* bins = rec->bins.entries;

	for (uint16_t i = 0; i < put->n_bins; i++) {
		as_status status = as_command_bin_size(&bins[i], buffers, &put->size, err);

		if (status != AEROSPIKE_OK) {
			return status;
		}
	}
	return AEROSPIKE_OK;
}

static size_t
as_put_write(void* udata, uint8_t* buf)
{
	as_put* put = udata;
	const as_policy_write* policy = put->policy;
	as_record* rec = put->rec;
	uint32_t ttl = (rec->ttl == AS_RECORD_CLIENT_DEFAULT_TTL)? policy->ttl : rec->ttl;

	uint8_t* p = as_command_write_header_write(buf, &policy->base, policy->commit_level,
		policy->exists, policy->gen, rec->gen, ttl, put->tdata.n_fields, put->n_bins,
		policy->durable_delete, policy->on_locking_only, 0, AS_MSG_INFO2_WRITE, 0);

	p = as_command_write_key(p, &policy->base, policy->key, put->key, &put->tdata);
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
	
	as_partition_info pi;
	as_status status = as_command_prepare_write(as, err, &policy->base, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_queue buffers;
	as_queue_inita(&buffers, sizeof(as_buffer), rec->bins.size);

	as_put put;
	status = as_put_init(&put, policy, key, rec, &buffers, err);

	if (status != AEROSPIKE_OK) {
		as_buffers_destroy(&buffers);
		return status;
	}

	// Support new compress while still being compatible with old XDR compression_threshold.
	uint32_t compression_threshold = policy->compression_threshold;

	if (policy->base.compress && compression_threshold == 0) {
		compression_threshold = AS_COMPRESS_THRESHOLD;
	}

	as_command cmd;
	as_command_init_write(&cmd, as->cluster, &policy->base, policy->replica, key, put.size, &pi,
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

	as_partition_info pi;
	as_status status = as_command_prepare(as->cluster, err, &policy->base, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_queue buffers;
	as_queue_inita(&buffers, sizeof(as_buffer), rec->bins.size);

	as_put put;
	status = as_put_init(&put, policy, key, rec, &buffers, err);

	if (status != AEROSPIKE_OK) {
		as_buffers_destroy(&buffers);
		return status;
	}

	// Support new compress while still being compatible with old XDR compression_threshold.
	uint32_t compression_threshold = policy->compression_threshold;

	if (policy->base.compress && compression_threshold == 0) {
		compression_threshold = AS_COMPRESS_THRESHOLD;
	}

	if (compression_threshold == 0 || (put.size <= compression_threshold)) {
		// Send uncompressed command.
		as_event_command* cmd = as_async_write_command_create(
			as->cluster, &policy->base, &pi, policy->replica, listener, udata, event_loop,
			pipe_listener, put.size, as_event_command_parse_header, NULL, 0);

		cmd->write_len = (uint32_t)as_put_write(&put, cmd->buf);

		if (length != NULL) {
			*length = cmd->write_len;
		}

		if (comp_length != NULL) {
			*comp_length = cmd->write_len;
		}

		return as_async_command_execute(as, err, &policy->base, key, cmd, &put.tdata);
	}
	else {
		// Send compressed command.
		// First write uncompressed buffer.
		size_t capacity = put.size;
		uint8_t* ubuf = cf_malloc(capacity);
		size_t size = as_put_write(&put, ubuf);

		// Allocate command with compressed upper bound.
		size_t comp_size = as_command_compress_max_size(size);
		as_event_command* cmd = as_async_write_command_create(
			as->cluster, &policy->base, &pi, policy->replica, listener, udata, event_loop,
			pipe_listener, comp_size, as_event_command_parse_header, ubuf, (uint32_t)size);

		return as_async_compress_command_execute(as, err, &policy->base, key, cmd, &put.tdata,
			ubuf, size, comp_size, length, comp_length);
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

//---------------------------------
// Remove
//---------------------------------

as_status
aerospike_key_remove(
	aerospike* as, as_error* err, const as_policy_remove* policy, const as_key* key
	)
{
	if (! policy) {
		policy = &as->config.policies.remove;
	}

	as_partition_info pi;
	as_status status = as_command_prepare_write(as, err, &policy->base, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_command_txn_data tdata;
	size_t size = as_command_key_size(&policy->base, policy->key, key, true, &tdata);
	uint32_t filter_size = as_command_filter_size(&policy->base, &tdata.n_fields);
	size += filter_size;

	uint8_t* buf = as_command_buffer_init(size);
	uint8_t* p = as_command_write_header_write(buf, &policy->base, policy->commit_level,
		AS_POLICY_EXISTS_IGNORE, policy->gen, policy->generation, 0, tdata.n_fields, 0,
		policy->durable_delete, false, 0, AS_MSG_INFO2_WRITE | AS_MSG_INFO2_DELETE, 0);

	p = as_command_write_key(p, &policy->base, policy->key, key, &tdata);
	p = as_command_write_filter(&policy->base, filter_size, p);
	size = as_command_write_end(buf, p);

	as_command cmd;
	as_command_init_write(&cmd, as->cluster, &policy->base, policy->replica, key, size, &pi,
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

	as_partition_info pi;
	as_status status = as_command_prepare(as->cluster, err, &policy->base, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_command_txn_data tdata;
	size_t size = as_command_key_size(&policy->base, policy->key, key, true, &tdata);
	uint32_t filter_size = as_command_filter_size(&policy->base, &tdata.n_fields);
	size += filter_size;

	as_event_command* cmd = as_async_write_command_create(
		as->cluster, &policy->base, &pi, policy->replica, listener, udata, event_loop,
		pipe_listener, size, as_event_command_parse_header, NULL, 0);

	uint8_t* p = as_command_write_header_write(cmd->buf, &policy->base, policy->commit_level,
		AS_POLICY_EXISTS_IGNORE, policy->gen, policy->generation, 0, tdata.n_fields, 0,
		policy->durable_delete, false, 0, AS_MSG_INFO2_WRITE | AS_MSG_INFO2_DELETE, 0);

	p = as_command_write_key(p, &policy->base, policy->key, key, &tdata);
	p = as_command_write_filter(&policy->base, filter_size, p);
	cmd->write_len = (uint32_t)as_command_write_end(cmd->buf, p);

	if (length != NULL) {
		*length = size;
	}

	return as_async_command_execute(as, err, &policy->base, key, cmd, &tdata);
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

//---------------------------------
// Operate
//---------------------------------

typedef struct as_operate_s {
	const as_policy_operate* policy;
	const as_key* key;
	const as_operations* ops;
	as_queue* buffers;
	size_t size;
	as_command_txn_data tdata;
	uint32_t filter_size;
	uint16_t n_operations;
	uint8_t read_attr;
	uint8_t write_attr;
	uint8_t info_attr;
} as_operate;

static as_status
as_operate_init(
	as_operate* oper, aerospike* as, const as_policy_operate* policy,
	as_policy_operate* policy_local, const as_key* key, const as_operations* ops, as_queue* buffers,
	as_error* err
	)
{
	oper->key = key;
	oper->ops = ops;
	oper->buffers = buffers;
	oper->size = 0;
	oper->n_operations = ops->binops.size;
	oper->read_attr = 0;
	oper->write_attr = 0;
	oper->info_attr = 0;

	bool respond_all_ops = false;

	for (uint32_t i = 0; i < oper->n_operations; i++) {
		as_binop* op = &oper->ops->binops.entries[i];
		
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
				oper->read_attr |= AS_MSG_INFO1_READ;

				if (op->bin.name[0] == 0) {
					oper->read_attr |= AS_MSG_INFO1_GET_ALL;
				}
				break;
				
			case AS_OPERATOR_MAP_MODIFY:
			case AS_OPERATOR_EXP_MODIFY:
			case AS_OPERATOR_BIT_MODIFY:
			case AS_OPERATOR_HLL_MODIFY:
				// Map operations require respond_all_ops to be true.
				respond_all_ops = true;
				// Fall through to write.
			default:
				oper->write_attr |= AS_MSG_INFO2_WRITE;
				break;
		}

		as_status status = as_command_bin_size(&op->bin, oper->buffers, &oper->size, err);

		if (status != AEROSPIKE_OK) {
			return status;
		}
	}

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

	// When GET_ALL is specified, RESPOND_ALL_OPS must be disabled.
	if ((respond_all_ops || policy->respond_all_ops) && !(oper->read_attr & AS_MSG_INFO1_GET_ALL)) {
		oper->write_attr |= AS_MSG_INFO2_RESPOND_ALL_OPS;
	}

	as_command_set_attr_read(policy->read_mode_ap, policy->read_mode_sc, policy->base.compress,
							 &oper->read_attr, &oper->info_attr);

	return AEROSPIKE_OK;
}

static void
as_operate_size(as_operate* oper)
{
	const as_policy_operate* policy = oper->policy;

	oper->size += as_command_key_size(&policy->base, policy->key, oper->key,
		oper->write_attr & AS_MSG_INFO2_WRITE, &oper->tdata);
	oper->filter_size = as_command_filter_size(&policy->base, &oper->tdata.n_fields);
	oper->size += oper->filter_size;
}

static size_t
as_operate_write(void* udata, uint8_t* buf)
{
	as_operate* oper = udata;
	const as_policy_operate* policy = oper->policy;
	const as_operations* ops = oper->ops;
	uint32_t ttl;
	
	if (oper->write_attr & AS_MSG_INFO2_WRITE) {
		ttl = (ops->ttl == AS_RECORD_CLIENT_DEFAULT_TTL)? policy->ttl : ops->ttl;
	}
	else {
		// ttl is an unsigned 32 bit integer in the wire protocol, but it still
		// works if a negative read_touch_ttl_percent is used. The server casts
		// ttl back to a signed integer when all operations are read operations.
		ttl = (uint32_t)policy->read_touch_ttl_percent;
	}

	uint8_t* p = as_command_write_header_write(buf, &policy->base, policy->commit_level,
		policy->exists, policy->gen, ops->gen, ttl, oper->tdata.n_fields,
		oper->n_operations, policy->durable_delete, policy->on_locking_only, oper->read_attr,
		oper->write_attr, oper->info_attr);

	p = as_command_write_key(p, &policy->base, policy->key, oper->key, &oper->tdata);
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
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "No operations defined");
	}

	as_queue buffers;
	as_queue_inita(&buffers, sizeof(as_buffer), n_operations);

	as_policy_operate policy_local;
	as_operate oper;

	as_status status = as_operate_init(&oper, as, policy, &policy_local, key, ops, &buffers, err);

	if (status != AEROSPIKE_OK) {
		as_buffers_destroy(&buffers);
		return status;
	}

	policy = oper.policy;

	as_partition_info pi;
	status = as_command_prepare(as->cluster, err, &policy->base, key, &pi);

	if (status != AEROSPIKE_OK) {
		as_buffers_destroy(&buffers);
		return status;
	}

	if (policy->base.txn && (oper.write_attr & AS_MSG_INFO2_WRITE)) {
		status = as_txn_monitor_add_key(as, &policy->base, key, err);

		if (status != AEROSPIKE_OK) {
			as_buffers_destroy(&buffers);
			return status;
		}
	}

	as_operate_size(&oper);

	as_command_parse_result_data data;
	data.record = rec;
	data.deserialize = policy->deserialize;

	as_command cmd;

	if (oper.write_attr & AS_MSG_INFO2_WRITE) {
		as_command_init_write(&cmd, as->cluster, &policy->base, policy->replica, key, oper.size, &pi,
							  as_command_parse_result, &data);
	}
	else {
		as_command_init_read(&cmd, as->cluster, &policy->base, policy->replica, policy->read_mode_sc, key,
							 oper.size, &pi, as_command_parse_result, &data);
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
	
	as_queue buffers;
	as_queue_inita(&buffers, sizeof(as_buffer), n_operations);

	as_policy_operate policy_local;
	as_operate oper;

	as_status status = as_operate_init(&oper, as, policy, &policy_local, key, ops, &buffers, err);

	if (status != AEROSPIKE_OK) {
		as_buffers_destroy(&buffers);
		return status;
	}

	policy = oper.policy;

	as_partition_info pi;
	status = as_command_prepare(as->cluster, err, &policy->base, key, &pi);

	if (status != AEROSPIKE_OK) {
		as_buffers_destroy(&buffers);
		return status;
	}

	as_operate_size(&oper);

	as_event_command* cmd;

	if (oper.write_attr & AS_MSG_INFO2_WRITE) {
		// Write command
		if (! (policy->base.compress && oper.size > AS_COMPRESS_THRESHOLD)) {
			// Send uncompressed command.
			cmd = as_async_record_command_create(
				as->cluster, &policy->base, &pi, policy->replica, 0, policy->deserialize,
				policy->async_heap_rec, 0, listener, udata, event_loop, pipe_listener, oper.size,
				as_event_command_parse_result, AS_ASYNC_TYPE_RECORD, AS_LATENCY_TYPE_WRITE, NULL, 0);

			cmd->write_len = (uint32_t)as_operate_write(&oper, cmd->buf);

			return as_async_command_execute(as, err, &policy->base, key, cmd, &oper.tdata);
		}
		else {
			// Send compressed command.
			// First write uncompressed buffer.
			size_t capacity = oper.size;
			uint8_t* ubuf = cf_malloc(capacity);
			size_t size = as_operate_write(&oper, ubuf);

			// Allocate command with compressed upper bound.
			size_t comp_size = as_command_compress_max_size(size);

			cmd = as_async_record_command_create(
				as->cluster, &policy->base, &pi, policy->replica, 0, policy->deserialize,
				policy->async_heap_rec, 0, listener, udata, event_loop, pipe_listener, comp_size,
				as_event_command_parse_result, AS_ASYNC_TYPE_RECORD, AS_LATENCY_TYPE_WRITE, ubuf, (uint32_t)size);

			return as_async_compress_command_execute(as, err, &policy->base, key, cmd, &oper.tdata,
				ubuf, size, comp_size, NULL, NULL);
		}
	}
	else {
		// Read command
		if (! (policy->base.compress && oper.size > AS_COMPRESS_THRESHOLD)) {
			// Send uncompressed command.
			as_read_info ri;
			as_event_command_init_read(policy->replica, policy->read_mode_sc, pi.sc_mode, &ri);

			cmd = as_async_record_command_create(
				as->cluster, &policy->base, &pi, ri.replica, ri.replica_index, policy->deserialize,
				policy->async_heap_rec, ri.flags, listener, udata, event_loop, pipe_listener,
				oper.size, as_event_command_parse_result, AS_ASYNC_TYPE_RECORD, AS_LATENCY_TYPE_READ, NULL, 0);

			cmd->write_len = (uint32_t)as_operate_write(&oper, cmd->buf);
		}
		else {
			// Send compressed command.
			// First write uncompressed buffer.
			size_t capacity = oper.size;
			uint8_t* ubuf = cf_malloc(capacity);
			size_t size = as_operate_write(&oper, ubuf);

			// Allocate command with compressed upper bound.
			size_t comp_size = as_command_compress_max_size(size);

			as_read_info ri;
			as_event_command_init_read(policy->replica, policy->read_mode_sc, pi.sc_mode, &ri);

			cmd = as_async_record_command_create(
				as->cluster, &policy->base, &pi, ri.replica, ri.replica_index, policy->deserialize,
				policy->async_heap_rec, ri.flags, listener, udata, event_loop, pipe_listener,
				comp_size, as_event_command_parse_result, AS_ASYNC_TYPE_RECORD, AS_LATENCY_TYPE_READ, ubuf, (uint32_t)size);

			// Compress buffer and execute.
			status = as_command_compress(err, ubuf, size, cmd->buf, &comp_size);

			if (status != AEROSPIKE_OK) {
				as_event_command_destroy(cmd);
				return status;
			}

			cmd->write_len = (uint32_t)comp_size;
		}

		// Call normal execute since readonly commands do not add keys to the transaction monitor.
		return as_event_command_execute(cmd, err);
	}
}

//---------------------------------
// Apply
//---------------------------------

typedef struct as_apply_s {
	const as_policy_apply* policy;
	const as_key* key;
	const char* module;
	const char* function;
	as_serializer ser;
	as_buffer args;
	as_command_txn_data tdata;
	uint32_t filter_size;
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

	size_t size = as_command_key_size(&policy->base, policy->key, key, true, &ap->tdata);

	ap->filter_size = as_command_filter_size(&policy->base, &ap->tdata.n_fields);
	size += ap->filter_size;

	size += as_command_string_field_size(module);
	size += as_command_string_field_size(function);
	
	as_msgpack_init(&ap->ser);
	as_buffer_init(&ap->args);
	as_serializer_serialize(&ap->ser, (as_val*)arglist, &ap->args);
	size += as_command_field_size(ap->args.size);
	ap->tdata.n_fields += 3;

	as_command_set_attr_compress(policy->base.compress, &ap->read_attr);
	return size;
}

static size_t
as_apply_write(void* udata, uint8_t* buf)
{
	as_apply* ap = udata;
	const as_policy_apply* policy = ap->policy;

	uint8_t* p = as_command_write_header_write(buf, &policy->base, policy->commit_level, 0,
		AS_POLICY_GEN_IGNORE, 0, policy->ttl, ap->tdata.n_fields, 0, policy->durable_delete,
		policy->on_locking_only, ap->read_attr, AS_MSG_INFO2_WRITE, 0);

	p = as_command_write_key(p, &policy->base, policy->key, ap->key, &ap->tdata);
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
	
	as_partition_info pi;
	as_status status = as_command_prepare_write(as, err, &policy->base, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_apply ap;
	size_t size = as_apply_init(&ap, policy, key, module, function, arglist);

	as_command cmd;
	as_command_init_write(&cmd, as->cluster, &policy->base, policy->replica, key, size, &pi,
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
	
	as_partition_info pi;
	as_status status = as_command_prepare(as->cluster, err, &policy->base, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}
	
	as_apply ap;
	size_t size = as_apply_init(&ap, policy, key, module, function, arglist);

	if (! (policy->base.compress && size > AS_COMPRESS_THRESHOLD)) {
		// Send uncompressed command.
		as_event_command* cmd = as_async_value_command_create(as->cluster, &policy->base, &pi,
			policy->replica, listener, udata, event_loop, pipe_listener, size,
			as_event_command_parse_success_failure, NULL, 0);

		cmd->write_len = (uint32_t)as_apply_write(&ap, cmd->buf);

		as_buffer_destroy(&ap.args);
		as_serializer_destroy(&ap.ser);

		return as_async_command_execute(as, err, &policy->base, key, cmd, &ap.tdata);
	}
	else {
		// Send compressed command.
		// First write uncompressed buffer.
		size_t capacity = size;
		uint8_t* ubuf = cf_malloc(capacity);
		size = as_apply_write(&ap, ubuf);

		as_buffer_destroy(&ap.args);
		as_serializer_destroy(&ap.ser);

		// Allocate command with compressed upper bound.
		size_t comp_size = as_command_compress_max_size(size);

		as_event_command* cmd = as_async_value_command_create(as->cluster, &policy->base, &pi,
			policy->replica, listener, udata, event_loop, pipe_listener, comp_size,
			as_event_command_parse_success_failure, ubuf, (uint32_t)size);

		return as_async_compress_command_execute(as, err, &policy->base, key, cmd, &ap.tdata,
			ubuf, size, comp_size, NULL, NULL);
	}
}

//---------------------------------
// Txn Monitor Operations
//---------------------------------

static as_status
as_txn_monitor_parse_header(as_error* err, as_command* cmd, as_node* node, uint8_t* buf, size_t size)
{
	as_msg* msg = (as_msg*)buf;
	as_status status = as_msg_parse(err, msg, size);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	if (msg->result_code == AEROSPIKE_OK || msg->result_code == AEROSPIKE_MRT_COMMITTED) {
		return AEROSPIKE_OK;
	}

	return as_error_set_message(err, msg->result_code, as_error_string(msg->result_code));
}

as_status
as_txn_monitor_mark_roll_forward(
	aerospike* as, as_error* err, const as_policy_base* base_policy, as_key* key
	)
{
	as_policy_write policy;
	as_policy_write_init(&policy);
	policy.base.socket_timeout = base_policy->socket_timeout;
	policy.base.total_timeout = base_policy->total_timeout;
	policy.base.max_retries = base_policy->max_retries;
	policy.base.sleep_between_retries = base_policy->sleep_between_retries;

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_bool(&rec, "fwd", true);

	as_partition_info pi;
	as_status status = as_key_partition_init(as->cluster, err, key, &pi);

	if (status != AEROSPIKE_OK) {
		as_record_destroy(&rec);
		return status;
	}

	as_queue buffers;
	as_queue_inita(&buffers, sizeof(as_buffer), rec.bins.size);

	as_put put;
	status = as_put_init(&put, &policy, key, &rec, &buffers, err);

	if (status != AEROSPIKE_OK) {
		as_buffers_destroy(&buffers);
		as_record_destroy(&rec);
		return status;
	}

	as_command cmd;
	as_command_init_write(&cmd, as->cluster, &policy.base, policy.replica, key, put.size, &pi,
						  as_txn_monitor_parse_header, NULL);

	status = as_command_send(&cmd, err, 0, as_put_write, &put);

	as_record_destroy(&rec);
	return status;
}

static bool
as_txn_monitor_parse_header_async(as_event_command* cmd)
{
	uint8_t* p = cmd->buf + cmd->pos;
	as_msg* msg = (as_msg*)p;
	as_msg_swap_header_from_be(msg);
	p += sizeof(as_msg);

	if (msg->result_code == AEROSPIKE_OK || msg->result_code == AEROSPIKE_MRT_COMMITTED) {
		as_event_response_complete(cmd);
		((as_async_write_command*)cmd)->listener(0, cmd->udata, cmd->event_loop);
		as_event_command_release(cmd);
	}
	else {
		as_error err;
		as_error_set_message(&err, msg->result_code, as_error_string(msg->result_code));
		as_event_response_error(cmd, &err);
	}
	return true;
}

as_status
as_txn_monitor_mark_roll_forward_async(
	aerospike* as, as_error* err, const as_policy_base* base_policy, as_key* key,
	as_async_write_listener listener, void* udata, as_event_loop* event_loop
	)
{
	as_policy_write policy;
	as_policy_write_init(&policy);
	policy.base.socket_timeout = base_policy->socket_timeout;
	policy.base.total_timeout = base_policy->total_timeout;
	policy.base.max_retries = base_policy->max_retries;
	policy.base.sleep_between_retries = base_policy->sleep_between_retries;

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_bool(&rec, "fwd", true);

	as_partition_info pi;
	as_status status = as_key_partition_init(as->cluster, err, key, &pi);

	if (status != AEROSPIKE_OK) {
		as_record_destroy(&rec);
		return status;
	}

	as_queue buffers;
	as_queue_inita(&buffers, sizeof(as_buffer), rec.bins.size);

	as_put put;
	status = as_put_init(&put, &policy, key, &rec, &buffers, err);

	if (status != AEROSPIKE_OK) {
		as_buffers_destroy(&buffers);
		as_record_destroy(&rec);
		return status;
	}

	as_event_command* cmd = as_async_write_command_create(
		as->cluster, &policy.base, &pi, policy.replica, listener, udata, event_loop,
		NULL, put.size, as_txn_monitor_parse_header_async, NULL, 0);

	cmd->write_len = (uint32_t)as_put_write(&put, cmd->buf);

	status = as_async_command_execute(as, err, &policy.base, key, cmd, &put.tdata);
	
	as_record_destroy(&rec);
	return status;
}

as_status
as_txn_monitor_operate(
	aerospike* as, as_error* err, as_txn* txn, const as_policy_operate* policy, const as_key* key,
	const as_operations* ops
	)
{
	uint32_t n_operations = ops->binops.size;

	as_partition_info pi;
	as_status status = as_key_partition_init(as->cluster, err, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_queue buffers;
	as_queue_inita(&buffers, sizeof(as_buffer), n_operations);

	as_policy_operate policy_local;
	as_operate oper;

	status = as_operate_init(&oper, as, policy, &policy_local, key, ops, &buffers, err);

	if (status != AEROSPIKE_OK) {
		as_buffers_destroy(&buffers);
		return status;
	}

	as_operate_size(&oper);
	policy = oper.policy;

	as_command cmd;
	as_command_init_write(&cmd, as->cluster, &policy->base, policy->replica, key, oper.size, &pi,
		as_command_parse_deadline, txn);
	cmd.flags |= AS_COMMAND_FLAGS_TXN_MONITOR;

	uint32_t compression_threshold = policy->base.compress ? AS_COMPRESS_THRESHOLD : 0;

	return as_command_send(&cmd, err, compression_threshold, as_operate_write, &oper);
}

as_status
as_txn_monitor_operate_async(
	aerospike* as, as_error* err, as_txn* txn, const as_policy_operate* policy, const as_key* key,
	const as_operations* ops, as_async_record_listener listener, void* udata, as_event_loop* event_loop
	)
{
	uint32_t n_operations = ops->binops.size;
	
	as_partition_info pi;
	as_status status = as_key_partition_init(as->cluster, err, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}
	
	as_queue buffers;
	as_queue_inita(&buffers, sizeof(as_buffer), n_operations);

	as_policy_operate policy_local;
	as_operate oper;

	status = as_operate_init(&oper, as, policy, &policy_local, key, ops, &buffers, err);

	if (status != AEROSPIKE_OK) {
		as_buffers_destroy(&buffers);
		return status;
	}

	as_operate_size(&oper);
	policy = oper.policy;

	as_event_command* cmd;

	if (! (policy->base.compress && oper.size > AS_COMPRESS_THRESHOLD)) {
		// Send uncompressed command.
		cmd = as_async_record_command_create(
			as->cluster, &policy->base, &pi, policy->replica, 0, policy->deserialize,
			policy->async_heap_rec, 0, listener, udata, event_loop, NULL, oper.size,
			as_event_command_parse_deadline, AS_ASYNC_TYPE_TXN_MONITOR, AS_LATENCY_TYPE_WRITE,
			NULL, 0);

		cmd->txn = txn;
		cmd->write_len = (uint32_t)as_operate_write(&oper, cmd->buf);
		return as_event_command_execute(cmd, err);
	}
	else {
		// Send compressed command.
		// First write uncompressed buffer.
		size_t capacity = oper.size;
		uint8_t* ubuf = cf_malloc(capacity);
		size_t size = as_operate_write(&oper, ubuf);

		// Allocate command with compressed upper bound.
		size_t comp_size = as_command_compress_max_size(size);

		cmd = as_async_record_command_create(
			as->cluster, &policy->base, &pi, policy->replica, 0, policy->deserialize,
			policy->async_heap_rec, 0, listener, udata, event_loop, NULL, comp_size,
			as_event_command_parse_deadline, AS_ASYNC_TYPE_TXN_MONITOR, AS_LATENCY_TYPE_WRITE,
			ubuf, (uint32_t)size);

		// Compress buffer and execute.
		status = as_command_compress(err, ubuf, size, cmd->buf, &comp_size);

		if (status != AEROSPIKE_OK) {
			as_event_command_destroy(cmd);
			return status;
		}

		cmd->txn = txn;
		cmd->write_len = (uint32_t)comp_size;
		return as_event_command_execute(cmd, err);
	}
}

//---------------------------------
// Txn Verify
//---------------------------------

static as_status
parse_result_code(as_error* err, as_command* cmd, as_node* node, uint8_t* buf, size_t size)
{
	as_msg* msg = (as_msg*)buf;
	as_status status = as_msg_parse(err, msg, size);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	if (msg->result_code) {
		return as_error_set_message(err, msg->result_code, as_error_string(msg->result_code));
	}
	return AEROSPIKE_OK;
}

as_status
as_txn_verify_single(
	aerospike* as, as_error* err, const as_policy_txn_verify* policy, const as_key* key, uint64_t ver
	)
{
	as_cluster* cluster = as->cluster;
	as_partition_info pi;
	as_status status = as_key_partition_init(cluster, err, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	uint16_t n_fields = 4;
	size_t size = strlen(key->ns) + strlen(key->set) + sizeof(cf_digest) + 45;
	size += 7 + AS_FIELD_HEADER_SIZE; // Version field

	uint8_t* buf = as_command_buffer_init(size);
	uint32_t timeout = as_command_server_timeout(&policy->base);

	buf[8] = 22;
	buf[9] = AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_NOBINDATA;
	buf[10] = 0;
	buf[11] = AS_MSG_INFO3_SC_READ_TYPE;
	buf[12] = AS_MSG_INFO4_TXN_VERIFY_READ;
	buf[13] = 0;
	*(uint32_t*)&buf[14] = 0;
	*(int*)&buf[18] = 0;
	*(uint32_t*)&buf[22] = cf_swap_to_be32(timeout);
	*(uint16_t*)&buf[26] = cf_swap_to_be16(n_fields);
	*(uint16_t*)&buf[28] = 0;

	uint8_t* p = &buf[30];

	p = as_command_write_field_string(p, AS_FIELD_NAMESPACE, key->ns);
	p = as_command_write_field_string(p, AS_FIELD_SETNAME, key->set);
	p = as_command_write_field_digest(p, &key->digest);
	p = as_command_write_field_version(p, ver);
	size = as_command_write_end(buf, p);

	status = as_command_execute_read(cluster, err, &policy->base, policy->replica,
			policy->read_mode_sc, key, buf, size, &pi, parse_result_code, NULL);

	as_command_buffer_free(buf, size);
	return status;
}

static bool
txn_verify_parse(as_event_command* cmd)
{
	uint8_t* p = cmd->buf + cmd->pos;
	as_msg* msg = (as_msg*)p;
	as_msg_swap_header_from_be(msg);
	p += sizeof(as_msg);

	if (msg->result_code == AEROSPIKE_OK) {
		as_event_response_complete(cmd);
		((as_async_record_command*)cmd)->listener(0, 0, cmd->udata, cmd->event_loop);
		as_event_command_release(cmd);
	}
	else {
		as_error err;
		as_error_set_message(&err, msg->result_code, as_error_string(msg->result_code));
		as_event_response_error(cmd, &err);
	}
	return true;
}

as_status
as_txn_verify_single_async(
	aerospike* as, as_error* err, const as_policy_txn_verify* policy, const as_key* key, uint64_t ver,
	as_async_record_listener listener, void* udata, as_event_loop* event_loop
	)
{
	as_cluster* cluster = as->cluster;
	as_partition_info pi;
	as_status status = as_key_partition_init(cluster, err, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_read_info ri;
	as_event_command_init_read(policy->replica, policy->read_mode_sc, pi.sc_mode, &ri);

	uint16_t n_fields = 4;
	size_t size = strlen(key->ns) + strlen(key->set) + sizeof(cf_digest) + 45;
	size += 7 + AS_FIELD_HEADER_SIZE; // Version field

	as_event_command* cmd = as_async_record_command_create(
		cluster, &policy->base, &pi, ri.replica, ri.replica_index, policy->deserialize,
		false, ri.flags, listener, udata, event_loop, NULL, size,
		txn_verify_parse, AS_ASYNC_TYPE_RECORD, AS_LATENCY_TYPE_READ, NULL, 0);

	uint32_t timeout = as_command_server_timeout(&policy->base);
	uint8_t* buf = cmd->buf;

	buf[8] = 22;
	buf[9] = AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_NOBINDATA;
	buf[10] = 0;
	buf[11] = AS_MSG_INFO3_SC_READ_TYPE;
	buf[12] = AS_MSG_INFO4_TXN_VERIFY_READ;
	buf[13] = 0;
	*(uint32_t*)&buf[14] = 0;
	*(int*)&buf[18] = 0;
	*(uint32_t*)&buf[22] = cf_swap_to_be32(timeout);
	*(uint16_t*)&buf[26] = cf_swap_to_be16(n_fields);
	*(uint16_t*)&buf[28] = 0;

	uint8_t* p = &buf[30];

	p = as_command_write_field_string(p, AS_FIELD_NAMESPACE, key->ns);
	p = as_command_write_field_string(p, AS_FIELD_SETNAME, key->set);
	p = as_command_write_field_digest(p, &key->digest);
	p = as_command_write_field_version(p, ver);
	cmd->write_len = (uint32_t)as_command_write_end(buf, p);

	return as_event_command_execute(cmd, err);
}

//---------------------------------
// Txn Roll
//---------------------------------

as_status
as_txn_roll_single(
	aerospike* as, as_error* err, as_txn* txn, const as_policy_txn_roll* policy, const as_key* key,
	uint64_t ver, uint8_t roll_attr
	)
{
	as_cluster* cluster = as->cluster;
	as_partition_info pi;
	as_status status = as_key_partition_init(cluster, err, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	uint16_t n_fields = 4;
	size_t size = strlen(key->ns) + strlen(key->set) + sizeof(cf_digest) + 45;

	// Transaction ID
	size += AS_FIELD_HEADER_SIZE + sizeof(uint64_t);

	// Transaction version
	if (ver) {
		size += 7 + AS_FIELD_HEADER_SIZE;
		n_fields++;
	}

	uint8_t* buf = as_command_buffer_init(size);
	uint32_t timeout = as_command_server_timeout(&policy->base);

	buf[8] = 22;
	buf[9] = 0;
	buf[10] = AS_MSG_INFO2_WRITE | AS_MSG_INFO2_DURABLE_DELETE;
	buf[11] = 0;
	buf[12] = roll_attr;
	buf[13] = 0;
	*(uint32_t*)&buf[14] = 0;
	*(int*)&buf[18] = 0;
	*(uint32_t*)&buf[22] = cf_swap_to_be32(timeout);
	*(uint16_t*)&buf[26] = cf_swap_to_be16(n_fields);
	*(uint16_t*)&buf[28] = 0;

	uint8_t* p = &buf[30];
	p = as_command_write_field_string(p, AS_FIELD_NAMESPACE, key->ns);
	p = as_command_write_field_string(p, AS_FIELD_SETNAME, key->set);
	p = as_command_write_field_digest(p, &key->digest);
	p = as_command_write_field_uint64_le(p, AS_FIELD_TXN_ID, txn->id);

	if (ver) {
		p = as_command_write_field_version(p, ver);
	}
	size = as_command_write_end(buf, p);

	as_command cmd;
	as_command_init_write(&cmd, as->cluster, &policy->base, policy->replica, key, size, &pi,
		parse_result_code, NULL);
	cmd.buf = buf;
	as_command_start_timer(&cmd);

	status = as_command_execute(&cmd, err);

	as_command_buffer_free(buf, size);
	return status;
}

static bool
txn_roll_parse(as_event_command* cmd)
{
	uint8_t* p = cmd->buf + cmd->pos;
	as_msg* msg = (as_msg*)p;
	as_msg_swap_header_from_be(msg);
	p += sizeof(as_msg);

	if (msg->result_code == AEROSPIKE_OK) {
		as_event_response_complete(cmd);
		((as_async_write_command*)cmd)->listener(0, cmd->udata, cmd->event_loop);
		as_event_command_release(cmd);
	}
	else {
		as_error err;
		as_error_set_message(&err, msg->result_code, as_error_string(msg->result_code));
		as_event_response_error(cmd, &err);
	}
	return true;
}

as_status
as_txn_roll_single_async(
	aerospike* as, as_error* err, as_txn* txn, const as_policy_txn_roll* policy, const as_key* key,
	uint64_t ver, uint8_t roll_attr, as_async_write_listener listener, void* udata,
	as_event_loop* event_loop
	)
{
	as_cluster* cluster = as->cluster;
	as_partition_info pi;
	as_status status = as_key_partition_init(cluster, err, key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	uint16_t n_fields = 4;
	size_t size = strlen(key->ns) + strlen(key->set) + sizeof(cf_digest) + 45;

	// Transaction ID
	size += AS_FIELD_HEADER_SIZE + sizeof(uint64_t);

	// Transaction version
	if (ver) {
		size += 7 + AS_FIELD_HEADER_SIZE;
		n_fields++;
	}

	as_event_command* cmd = as_async_write_command_create(
		as->cluster, &policy->base, &pi, policy->replica, listener, udata, event_loop,
		NULL, size, txn_roll_parse, NULL, 0);

	uint32_t timeout = as_command_server_timeout(&policy->base);
	uint8_t* buf = cmd->buf;

	buf[8] = 22;
	buf[9] = 0;
	buf[10] = AS_MSG_INFO2_WRITE | AS_MSG_INFO2_DURABLE_DELETE;
	buf[11] = 0;
	buf[12] = roll_attr;
	buf[13] = 0;
	*(uint32_t*)&buf[14] = 0;
	*(int*)&buf[18] = 0;
	*(uint32_t*)&buf[22] = cf_swap_to_be32(timeout);
	*(uint16_t*)&buf[26] = cf_swap_to_be16(n_fields);
	*(uint16_t*)&buf[28] = 0;

	uint8_t* p = &buf[30];

	p = as_command_write_field_string(p, AS_FIELD_NAMESPACE, key->ns);
	p = as_command_write_field_string(p, AS_FIELD_SETNAME, key->set);
	p = as_command_write_field_digest(p, &key->digest);
	p = as_command_write_field_uint64_le(p, AS_FIELD_TXN_ID, txn->id);

	if (ver) {
		p = as_command_write_field_version(p, ver);
	}
	cmd->write_len = (uint32_t)as_command_write_end(buf, p);

	return as_event_command_execute(cmd, err);
}
