/*
 * Copyright 2008-2015 Aerospike, Inc.
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
#include <aerospike/as_async.h>
#include <aerospike/as_admin.h>
#include <aerospike/as_command.h>
#include <aerospike/as_error.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_node.h>
#include <aerospike/as_socket.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_byte_order.h>
#include <errno.h>

/******************************************************************************
 * STATIC VARIABLES
 *****************************************************************************/

static uint32_t as_event_loop_current = 0;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

static inline void
as_async_put_connection(as_async_command* cmd)
{
	as_queue* q = &cmd->node->async_conn_qs[cmd->event_loop->index];
	
	if (! as_queue_push_limit(q, &cmd->fd)) {
		close(cmd->fd);
		cmd->fd = -1;
	}
}

static inline void
as_async_finish(as_async_command* cmd, void* result)
{
	if (cmd->release_node) {
		as_node_release(cmd->node);
	}
	
	// Command buffer may have been allocated separately from command allocation.
	if (cmd->buf != cmd->space) {
		cf_free(cmd->buf);
	}
	
	cmd->ucb(0, result, cmd->udata, cmd->event_loop);
	cf_free(cmd);
}

void
as_async_error(as_async_command* cmd, as_error* err)
{
	if (cmd->release_node) {
		as_node_release(cmd->node);
	}
	
	// Buffer may have been allocated separately from command allocation.
	if (cmd->buf != cmd->space) {
		cf_free(cmd->buf);
	}
	
	cmd->ucb(err, 0, cmd->udata, cmd->event_loop);
	cf_free(cmd);
}

static inline void
as_async_server_error(as_async_command* cmd, as_error* err)
{
	// Put connection back in pool.
	as_async_put_connection(cmd);
	as_async_error(cmd, err);
}

static void
as_async_server_error_code(as_async_command* cmd, as_status error_code)
{
	// Put connection back in pool.
	as_async_put_connection(cmd);
	
	as_error err;
	as_error_set_message(&err, error_code, as_error_string(error_code));
	as_async_error(cmd, &err);
}

static void
as_async_socket_error(as_async_command* cmd, as_error* err)
{
	// Stop watcher if it has been initialized.
	if (cmd->state > AS_ASYNC_STATE_UNREGISTERED) {
		as_event_unregister(cmd);
	}
	
	// Stop timer.
	if (cmd->timeout_ms) {
		as_event_stop_timer(cmd);
	}
	
	// Do not put connection back in pool.
	close(cmd->fd);
	cmd->fd = -1;
	
	as_async_error(cmd, err);
}

void
as_async_timeout(as_async_command* cmd)
{
	// Stop watcher if it has been initialized.
	if (cmd->state > AS_ASYNC_STATE_UNREGISTERED) {
		as_event_unregister(cmd);
	}
	
	// Assume timer has already been stopped.
	// Do not put connection back in pool.
	// Timeouts can occur before connection is created, so check fd.
	if (cmd->fd >= 0) {
		close(cmd->fd);
		cmd->fd = -1;
	}
	
	as_error err;
	as_error_update(&err, AEROSPIKE_ERR_TIMEOUT, as_error_string(AEROSPIKE_ERR_TIMEOUT));
	as_async_error(cmd, &err);
}

static void
as_async_conn_error(as_async_command* cmd, as_error* err)
{
	// Socket never connected or registered, so just close.
	if (cmd->fd >= 0) {
		close(cmd->fd);
		cmd->fd = -1;
	}
	as_async_error(cmd, err);
}

static int
as_async_command_write(as_async_command* cmd)
{
	ssize_t bytes;
	
	do {
		bytes = write(cmd->fd, cmd->buf + cmd->pos, cmd->len - cmd->pos);
		
		if (bytes > 0) {
			cmd->pos += bytes;
			continue;
		}
		
		if (bytes < 0) {
			if (errno == EWOULDBLOCK) {
				return 1;
			}
			
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Socket %d write failed: %d", cmd->fd, errno);
			as_async_socket_error(cmd, &err);
			return 2;
		}
		else {
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Socket %d write closed by peer", cmd->fd);
			as_async_socket_error(cmd, &err);
			return 3;
		}
	} while (cmd->pos < cmd->len);
	
	return 0;
}

static inline void
as_async_set_auth_write(as_async_command* cmd)
{
	// The command buffer was already allocated with enough space for max authentication size,
	// so just use the end of the write buffer for authentication bytes.
	cmd->pos = cmd->len;
	cmd->auth_len = as_authenticate_set(cmd->cluster->user, cmd->cluster->password, &cmd->buf[cmd->pos]);
	cmd->len = cmd->pos + cmd->auth_len;
}

static inline void
as_async_set_auth_read_header(as_async_command* cmd)
{
	// Authenticate response buffer is at end of write buffer.
	cmd->pos = cmd->len - cmd->auth_len;
	cmd->len = cmd->pos + 8;
	cmd->state = AS_ASYNC_STATE_AUTH_READ_HEADER;
}

static void
as_async_connected_auth(as_async_command* cmd)
{
	as_async_set_auth_write(cmd);
	
	int ret = as_async_command_write(cmd);
	
	if (ret == 0) {
		// Done with write. Register for read.
		as_async_set_auth_read_header(cmd);
		as_event_register_read(cmd);
		return;
	}
	
	if (ret == 1) {
		// Got would-block. Register for write.
		cmd->state = AS_ASYNC_STATE_AUTH_WRITE;
		as_event_register_write(cmd);
	}
}

static inline bool
as_async_connected(as_async_command* cmd)
{
	if (cmd->cluster->user) {
		as_async_connected_auth(cmd);
		return false;
	}
	return true;
}

static inline void
as_async_connect_in_progress(as_async_command* cmd)
{
	if (cmd->cluster->user) {
		as_async_set_auth_write(cmd);
		cmd->state = AS_ASYNC_STATE_AUTH_WRITE;
	}
	else {
		cmd->state = AS_ASYNC_STATE_WRITE;
	}
	as_event_register_write(cmd);
}

static bool
as_async_create_connection(as_async_command* cmd)
{
	// Create a non-blocking socket.
	cmd->fd = as_socket_create_nb();
	
	if (cmd->fd < 0) {
		as_error err;
		as_error_set_message(&err, AEROSPIKE_ERR_CLIENT, "Failed to create non-blocking socket");
		as_async_conn_error(cmd, &err);
		return false;
	}
	
	// Try primary address.
	as_node* node = cmd->node;
	as_address* primary = as_vector_get(&node->addresses, node->address_index);
	
	// Attempt non-blocking connection.
	if (connect(cmd->fd, (struct sockaddr*)&primary->addr, sizeof(struct sockaddr)) == 0) {
		return as_async_connected(cmd);
	}
	
	// Check if connection is in progress.
	if (errno == EINPROGRESS) {
		// Connection hasn't finished, so register new fd for write.
		as_async_connect_in_progress(cmd);
		return false;
	}
	
	// Try other addresses.
	as_vector* addresses = &node->addresses;
	for (uint32_t i = 0; i < addresses->size; i++) {
		as_address* address = as_vector_get(addresses, i);
		
		// Address points into alias array, so pointer comparison is sufficient.
		if (address != primary) {
			if (connect(cmd->fd, (struct sockaddr*)&address->addr, sizeof(struct sockaddr)) == 0) {
				// Replace invalid primary address with valid alias.
				// Other threads may not see this change immediately.
				// It's just a hint, not a requirement to try this new address first.
				as_log_debug("Change node address %s %s:%d", node->name, address->name, (int)cf_swap_from_be16(address->addr.sin_port));
				ck_pr_store_32(&node->address_index, i);
				return as_async_connected(cmd);
			}
			
			// Check if connection is in progress.
			if (errno == EINPROGRESS) {
				// Replace invalid primary address with valid alias.
				// Other threads may not see this change immediately.
				// It's just a hint, not a requirement to try this new address first.
				as_log_debug("Change node address %s %s:%d", node->name, address->name, (int)cf_swap_from_be16(address->addr.sin_port));
				ck_pr_store_32(&node->address_index, i);
				
				// Connection hasn't finished, so register new fd for write.
				as_async_connect_in_progress(cmd);
				return false;
			}
		}
	}
	
	// Failed to start a connection on any socket address.
	as_error err;
	as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Failed to connect: %s %s:%d",
					node->name, primary->name, (int)cf_swap_from_be16(primary->addr.sin_port));
	as_async_conn_error(cmd, &err);
	return false;
}

static bool
as_async_get_connection(as_async_command* cmd)
{
	as_queue* q = &cmd->node->async_conn_qs[cmd->event_loop->index];
	
	while (as_queue_pop(q, &cmd->fd)) {
		if (as_socket_validate(cmd->fd, true)) {
			return true;
		}
	}
	return false;
}

static void
as_async_command_begin(as_async_command* cmd)
{
	// Always initialize timer first when timeouts are specified.
	if (cmd->timeout_ms) {
		as_event_init_timer(cmd);
	}
	
	if (! as_async_get_connection(cmd)) {
		if (! as_async_create_connection(cmd)) {
			// Waiting on connection, authentication or error occurred.
			return;
		}
	}
		
	// Try non-blocking write.
	int ret = as_async_command_write(cmd);
	
	if (ret == 0) {
		// Done with write. Register for read.
		cmd->pos = 0;
		cmd->len = 8;
		cmd->state = AS_ASYNC_STATE_READ_HEADER;
		as_event_register_read(cmd);
		return;
	}
	
	if (ret == 1) {
		// Got would-block. Register for write.
		cmd->state = AS_ASYNC_STATE_WRITE;
		as_event_register_write(cmd);
	}
}

void
as_async_command_execute(as_async_command* cmd)
{
	// Check if command timed out after coming off queue.
	if (cmd->timeout_ms && (cf_getms() - *(uint64_t*)cmd) > cmd->timeout_ms) {
		as_error err;
		as_error_set_message(&err, AEROSPIKE_ERR_TIMEOUT, as_error_string(AEROSPIKE_ERR_TIMEOUT));
		as_async_error(cmd, &err);
		return;
	}
	
	// Start processing.
	as_async_command_begin(cmd);
}

void
as_async_command_assign(as_async_command* cmd, size_t size)
{
	cmd->len = (uint32_t)size;
	
	if (! cmd->event_loop) {
		// Assign event loop using round robin distribution.
		// Not atomic because doesn't need to be exactly accurate.
		uint32_t current = as_event_loop_current++;
		cmd->event_loop = &as_event_loops[current % as_event_loop_size];
	}
	
	// Use pointer comparison for performance.
	// If portability becomes an issue, use "pthread_equal(event_loop->thread, pthread_self())"
	// instead.
	if (cmd->event_loop->thread == pthread_self()) {
		// We are already in event loop thread, so start processing.
		as_async_command_begin(cmd);
	}
	else {
		if (cmd->timeout_ms) {
			// Store current time in first 8 bytes which is not used yet.
			*(uint64_t*)cmd = cf_getms();
		}
		
		// Send command through queue so it can be executed in event loop thread.
		if (! as_event_send(cmd)) {
			as_error err;
			as_error_set_message(&err, AEROSPIKE_ERR_CLIENT, "Failed to queue command");
			as_async_error(cmd, &err);
		}
	}
}

void
as_async_command_send(as_async_command* cmd)
{
	int ret = as_async_command_write(cmd);
	
	if (ret == 0) {
		// Done with write. Register for read.
		if (cmd->state == AS_ASYNC_STATE_AUTH_WRITE) {
			as_async_set_auth_read_header(cmd);
		}
		else {
			cmd->pos = 0;
			cmd->len = 8;
			cmd->state = AS_ASYNC_STATE_READ_HEADER;
		}
		as_event_set_read(cmd);
	}
}

static bool
as_async_read(as_async_command* cmd)
{
	ssize_t bytes;
	
	do {
		bytes = read(cmd->fd, cmd->buf + cmd->pos, cmd->len - cmd->pos);
		
		if (bytes > 0) {
			cmd->pos += bytes;
			continue;
		}
		
		if (bytes < 0) {
			if (errno != EWOULDBLOCK) {
				as_error err;
				as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Socket %d read failed: %d", cmd->fd, errno);
				as_async_socket_error(cmd, &err);
			}
			return false;
		}
		else {
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Socket %d read closed by peer", cmd->fd);
			as_async_socket_error(cmd, &err);
			return false;
		}
	} while (cmd->pos < cmd->len);
	
	return true;
}

static void
as_async_command_parse_authentication(as_async_command* cmd)
{
	if (cmd->state == AS_ASYNC_STATE_AUTH_READ_HEADER) {
		// Read response length
		if (! as_async_read(cmd)) {
			return;
		}
		
		// Authenticate response buffer is at end of write buffer.
		cmd->pos = cmd->len - 8;
		as_proto_msg* msg = (as_proto_msg*)&cmd->buf[cmd->pos];
		as_proto_swap_from_be(&msg->proto);
		cmd->auth_len = (uint32_t)msg->proto.sz;
		cmd->len = cmd->pos + cmd->auth_len;
		
		if (cmd->len > cmd->capacity) {
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Authenticate response size is corrupt: %u", cmd->auth_len);
			as_async_socket_error(cmd, &err);
			return;
		}
		cmd->state = AS_ASYNC_STATE_AUTH_READ_BODY;
	}

	if (! as_async_read(cmd)) {
		return;
	}

	// Parse authentication response.
	cmd->len -= cmd->auth_len;
	uint8_t code = cmd->buf[cmd->len + 1];

	if (code) {
		// Can't authenticate socket, so must close it.
		as_error err;
		as_error_set_message(&err, code, as_error_string(code));
		as_async_socket_error(cmd, &err);
		return;
	}
	cmd->pos = 0;
	
	// Try non-blocking command write.
	int ret = as_async_command_write(cmd);
	
	if (ret == 0) {
		// Done with write. Set command read.
		cmd->pos = 0;
		cmd->len = 8;
		cmd->state = AS_ASYNC_STATE_READ_HEADER;
		as_event_set_read(cmd);
		return;
	}

	if (ret == 1) {
		// Got would-block. Set command write.
		cmd->state = AS_ASYNC_STATE_WRITE;
		as_event_set_write(cmd);
	}
}

void
as_async_command_receive(as_async_command* cmd)
{
	// Check for authenticate read-header or read-body.
	if (cmd->state & AS_ASYNC_STATE_AUTH_READ_HEADER) {
		as_async_command_parse_authentication(cmd);
		return;
	}
	
	if (cmd->state == AS_ASYNC_STATE_READ_HEADER) {
		// Read response length
		if (! as_async_read(cmd)) {
			return;
		}
		
		as_proto_msg* msg = (as_proto_msg*)cmd->buf;
		as_proto_swap_from_be(&msg->proto);
		size_t size = msg->proto.sz;
		cmd->len = (uint32_t)size;
		cmd->pos = 0;
		
		if (cmd->len > cmd->capacity) {
			cmd->buf = cf_malloc(size);
			cmd->capacity = cmd->len;
		}
		cmd->state = AS_ASYNC_STATE_READ_BODY;
	}
	
	if (! as_async_read(cmd)) {
		return;
	}
	
	as_event_unregister(cmd);
	
	if (cmd->timeout_ms) {
		as_event_stop_timer(cmd);
	}
	
	cmd->parse_results(cmd);
}

void
as_async_command_parse_header(as_async_command* cmd)
{
	uint8_t ret = cmd->buf[5];
	
	if (ret == AEROSPIKE_OK) {
		as_async_finish(cmd, 0);
	}
	else {
		as_async_server_error_code(cmd, ret);
	}
}

void
as_async_command_parse_result(as_async_command* cmd)
{
	as_msg* msg = (as_msg*)cmd->buf;
	as_msg_swap_header_from_be(msg);
	uint8_t* p = cmd->buf + sizeof(as_msg);
	as_status status = msg->result_code;
	
	switch (status) {
		case AEROSPIKE_OK: {
			as_record rec;
			
			if (msg->n_ops < 1000) {
				as_record_inita(&rec, msg->n_ops);
			}
			else {
				as_record_init(&rec, msg->n_ops);
			}
			
			rec.gen = msg->generation;
			rec.ttl = cf_server_void_time_to_ttl(msg->record_ttl);
			
			p = as_command_ignore_fields(p, msg->n_fields);
			as_command_parse_bins(&rec, p, msg->n_ops, cmd->deserialize);
		
			as_async_finish(cmd, &rec);
			as_record_destroy(&rec);
			break;
		}
			
		case AEROSPIKE_ERR_UDF: {
			as_error err;
			as_command_parse_udf_failure(p, &err, msg, status);
			as_async_server_error(cmd, &err);
			break;
		}
			
		default: {
			as_async_server_error_code(cmd, status);
			break;
		}
	}
}

void
as_async_command_parse_success_failure(as_async_command* cmd)
{
	as_msg* msg = (as_msg*)cmd->buf;
	as_msg_swap_header_from_be(msg);
	uint8_t* p = cmd->buf + sizeof(as_msg);
	as_status status = msg->result_code;

	switch (status) {
		case AEROSPIKE_OK: {
			as_error err;
			as_val* val = 0;
			status = as_command_parse_success_failure_bins(&p, &err, msg, &val);
			
			if (status == AEROSPIKE_OK) {
				as_async_finish(cmd, val);
				as_val_destroy(val);
			}
			else {
				as_async_server_error(cmd, &err);
			}
			break;
		}
			
		case AEROSPIKE_ERR_UDF: {
			as_error err;
			as_command_parse_udf_failure(p, &err, msg, status);
			as_async_server_error(cmd, &err);
			break;
		}

		default:
			as_async_server_error_code(cmd, status);
			break;
	}
}
