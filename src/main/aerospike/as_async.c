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
 * FUNCTIONS
 *****************************************************************************/

static inline void
as_async_unregister(as_async_command* cmd)
{
	as_event_unregister(&cmd->event);

	if (cmd->event.timeout_ms) {
		as_event_stop_timer(&cmd->event);
	}
}

static inline void
as_async_put_connection(as_async_command* cmd)
{
	as_queue* q = &cmd->node->async_conn_qs[cmd->event.event_loop->index];
	
	if (! as_queue_push_limit(q, &cmd->event.fd)) {
		close(cmd->event.fd);
		cmd->event.fd = -1;
	}
}

static inline void
as_async_command_free(as_async_command* cmd)
{
	if (cmd->free_buf) {
		cf_free(cmd->buf);
	}
	cf_free(cmd);
}

static void
as_async_error_callback(as_async_command* cmd, as_error* err)
{
	as_node_release(cmd->node);

	switch (cmd->type) {
		case AS_ASYNC_TYPE_WRITE:
			((as_async_write_command*)cmd)->listener(err, cmd->udata, cmd->event.event_loop);
			break;
		case AS_ASYNC_TYPE_RECORD:
			((as_async_record_command*)cmd)->listener(err, 0, cmd->udata, cmd->event.event_loop);
			break;
		case AS_ASYNC_TYPE_VALUE:
			((as_async_value_command*)cmd)->listener(err, 0, cmd->udata, cmd->event.event_loop);
			break;
		
		default: {
			// Handle command that is part of a group (batch, scan, query).
			// Commands are issued on same event loop, so we can assume single threaded behavior.
			as_async_executor* executor = cmd->udata;
			
			// Notify user of error only once.
			if (executor->valid) {
				executor->complete_fn(executor, err);
				executor->valid = false;
			}
			
			// Only free executor if all outstanding commands are complete.
			if (++executor->count == executor->max) {
				cf_free(executor->commands);
				cf_free(executor);
			}
		}
	}
	as_async_command_free(cmd);
}

static inline void
as_async_conn_error(as_async_command* cmd, as_error* err)
{
	// Only timer needs to be released on socket connection failure.
	// Watcher has not been registered yet.
	if (cmd->event.timeout_ms) {
		as_event_stop_timer(&cmd->event);
	}
	as_async_error_callback(cmd, err);
}

static void
as_async_socket_error(as_async_command* cmd, as_error* err)
{
	// Socket read/write failure.
	// Stop watcher only if it has been initialized.
	if (cmd->state > AS_ASYNC_STATE_UNREGISTERED) {
		as_event_unregister(&cmd->event);
	}
	
	// Stop timer.
	if (cmd->event.timeout_ms) {
		as_event_stop_timer(&cmd->event);
	}
	
	// Do not put connection back in pool.
	as_event_close(&cmd->event);
	as_async_error_callback(cmd, err);
}

void
as_async_timeout(as_async_command* cmd)
{
	as_error err;
	as_error_set_message(&err, AEROSPIKE_ERR_TIMEOUT, as_error_string(AEROSPIKE_ERR_TIMEOUT));
	
	// Command has timed out.
	// Stop watcher if it has been initialized.
	if (cmd->state > AS_ASYNC_STATE_UNREGISTERED) {
		as_event_unregister(&cmd->event);
	}
	
	// Assume timer has already been stopped.
	// Do not put connection back in pool.
	as_event_close(&cmd->event);
	as_async_error_callback(cmd, &err);
}

void
as_async_response_error(as_async_command* cmd, as_error* err)
{
	// Server sent back error.
	// Release resources, make callback and free command.
	as_async_unregister(cmd);
	
	// Close socket on errors that can leave unread data in socket.
	switch (err->code) {
		case AEROSPIKE_ERR_QUERY_ABORTED:
		case AEROSPIKE_ERR_SCAN_ABORTED:
		case AEROSPIKE_ERR_CLIENT_ABORT:
		case AEROSPIKE_ERR_CLIENT:
			as_event_close(&cmd->event);
			break;
			
		default:
			as_async_put_connection(cmd);
			break;
	}
	as_async_error_callback(cmd, err);
}

static inline void
as_async_response_complete(as_async_command* cmd)
{
	as_async_unregister(cmd);
	as_async_put_connection(cmd);
	as_node_release(cmd->node);
}

void
as_async_executor_complete(as_async_command* cmd)
{
	as_async_response_complete(cmd);
	
	// Only invoke user callback after all node commands have completed.
	as_async_executor* executor = cmd->udata;
	
	if (++executor->count == executor->max) {
		// All commands completed.
		if (executor->valid) {
			executor->complete_fn(executor, 0);
		}
		cf_free(executor->commands);
		cf_free(executor);
	}
	else {
		// Determine if a new command needs to be started.
		if (executor->valid) {
			int next = executor->count + executor->max_concurrent - 1;
		
			if (next < executor->max) {
				// Start new command.
				as_async_command_execute(executor->commands[next]);
			}
		}
	}
	as_async_command_free(cmd);
}

#define AS_ASYNC_WRITE_COMPLETE 0
#define AS_ASYNC_WRITE_INCOMPLETE 1
#define AS_ASYNC_WRITE_ERROR 2

static int
as_async_command_write(as_async_command* cmd)
{
	ssize_t bytes;
	
	do {
#if defined(__linux__)
		bytes = send(cmd->event.fd, cmd->buf + cmd->pos, cmd->len - cmd->pos, MSG_NOSIGNAL);
#else
		bytes = write(cmd->event.fd, cmd->buf + cmd->pos, cmd->len - cmd->pos);
#endif
		
		if (bytes > 0) {
			cmd->pos += bytes;
			continue;
		}
		
		if (bytes < 0) {
			if (errno == EWOULDBLOCK) {
				return AS_ASYNC_WRITE_INCOMPLETE;
			}
			
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Socket %d write failed: %d", cmd->event.fd, errno);
			as_async_socket_error(cmd, &err);
			return AS_ASYNC_WRITE_ERROR;
		}
		else {
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Socket %d write closed by peer", cmd->event.fd);
			as_async_socket_error(cmd, &err);
			return AS_ASYNC_WRITE_ERROR;
		}
	} while (cmd->pos < cmd->len);
	
	return AS_ASYNC_WRITE_COMPLETE;
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
	cmd->len = cmd->pos + sizeof(as_proto);
	cmd->state = AS_ASYNC_STATE_AUTH_READ_HEADER;
}

static void
as_async_connected_auth(as_async_command* cmd)
{
	as_async_set_auth_write(cmd);
	
	int ret = as_async_command_write(cmd);
	
	if (ret == AS_ASYNC_WRITE_COMPLETE) {
		// Done with write. Register for read.
		as_async_set_auth_read_header(cmd);
		as_event_register_read(&cmd->event);
		return;
	}
	
	if (ret == AS_ASYNC_WRITE_INCOMPLETE) {
		// Got would-block. Register for write.
		cmd->state = AS_ASYNC_STATE_AUTH_WRITE;
		as_event_register_write(&cmd->event);
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
	as_event_register_write(&cmd->event);
}

static bool
as_async_create_connection(as_async_command* cmd)
{
	// Create a non-blocking socket.
	cmd->event.fd = as_socket_create_nb();
	
	if (cmd->event.fd < 0) {
		as_error err;
		as_error_set_message(&err, AEROSPIKE_ERR_CLIENT, "Failed to create non-blocking socket");
		as_async_conn_error(cmd, &err);
		return false;
	}
	
	// Try primary address.
	as_node* node = cmd->node;
	as_address* primary = as_vector_get(&node->addresses, node->address_index);
	
	// Attempt non-blocking connection.
	if (connect(cmd->event.fd, (struct sockaddr*)&primary->addr, sizeof(struct sockaddr)) == 0) {
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
			if (connect(cmd->event.fd, (struct sockaddr*)&address->addr, sizeof(struct sockaddr)) == 0) {
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
	close(cmd->event.fd);
	as_error err;
	as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Failed to connect: %s %s:%d",
					node->name, primary->name, (int)cf_swap_from_be16(primary->addr.sin_port));
	as_async_conn_error(cmd, &err);
	return false;
}

static bool
as_async_get_connection(as_async_command* cmd)
{
	as_queue* q = &cmd->node->async_conn_qs[cmd->event.event_loop->index];
	
	while (as_queue_pop(q, &cmd->event.fd)) {
		if (as_socket_validate(cmd->event.fd, true)) {
			return true;
		}
	}
	return false;
}

static void
as_async_command_begin(as_async_command* cmd)
{
	// Always initialize timer first when timeouts are specified.
	if (cmd->event.timeout_ms) {
		as_event_init_timer(&cmd->event);
	}
	
	if (! as_async_get_connection(cmd)) {
		if (! as_async_create_connection(cmd)) {
			// Waiting on connection, authentication or error occurred.
			return;
		}
	}
		
	// Try non-blocking write.
	int ret = as_async_command_write(cmd);
	
	if (ret == AS_ASYNC_WRITE_COMPLETE) {
		// Done with write. Register for read.
		cmd->pos = 0;
		cmd->len = sizeof(as_proto);
		cmd->state = AS_ASYNC_STATE_READ_HEADER;
		as_event_register_read(&cmd->event);
		return;
	}
	
	if (ret == AS_ASYNC_WRITE_INCOMPLETE) {
		// Got would-block. Register for write.
		cmd->state = AS_ASYNC_STATE_WRITE;
		as_event_register_write(&cmd->event);
	}
}

void
as_async_command_thread_execute(as_async_command* cmd)
{
	// Check if command timed out after coming off queue.
	if (cmd->event.timeout_ms && (cf_getms() - *(uint64_t*)cmd) > cmd->event.timeout_ms) {
		as_error err;
		as_error_set_message(&err, AEROSPIKE_ERR_TIMEOUT, as_error_string(AEROSPIKE_ERR_TIMEOUT));
		as_async_error_callback(cmd, &err);
		return;
	}
	
	// Start processing.
	as_async_command_begin(cmd);
}

void
as_async_command_execute(as_async_command* cmd)
{
	// Use pointer comparison for performance.
	// If portability becomes an issue, use "pthread_equal(event_loop->thread, pthread_self())"
	// instead.
	if (cmd->event.event_loop->thread == pthread_self()) {
		// We are already in event loop thread, so start processing.
		as_async_command_begin(cmd);
	}
	else {
		if (cmd->event.timeout_ms) {
			// Store current time in first 8 bytes which is not used yet.
			*(uint64_t*)cmd = cf_getms();
		}
		
		// Send command through queue so it can be executed in event loop thread.
		if (! as_event_send(&cmd->event)) {
			as_error err;
			as_error_set_message(&err, AEROSPIKE_ERR_CLIENT, "Failed to queue command");
			as_async_error_callback(cmd, &err);
		}
	}
}

void
as_async_command_send(as_async_command* cmd)
{
	int ret = as_async_command_write(cmd);
	
	if (ret == AS_ASYNC_WRITE_COMPLETE) {
		// Done with write. Register for read.
		if (cmd->state == AS_ASYNC_STATE_AUTH_WRITE) {
			as_async_set_auth_read_header(cmd);
		}
		else {
			cmd->pos = 0;
			cmd->len = sizeof(as_proto);
			cmd->state = AS_ASYNC_STATE_READ_HEADER;
		}
		as_event_set_read(&cmd->event);
	}
}

static bool
as_async_read(as_async_command* cmd)
{
	ssize_t bytes;
	
	do {
		bytes = read(cmd->event.fd, cmd->buf + cmd->pos, cmd->len - cmd->pos);
		
		if (bytes > 0) {
			cmd->pos += bytes;
			continue;
		}
		
		if (bytes < 0) {
			if (errno != EWOULDBLOCK) {
				as_error err;
				as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Socket %d read failed: %d", cmd->event.fd, errno);
				as_async_socket_error(cmd, &err);
			}
			return false;
		}
		else {
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Socket %d read closed by peer", cmd->event.fd);
			as_async_socket_error(cmd, &err);
			return false;
		}
	} while (cmd->pos < cmd->len);
	
	return true;
}

#define AS_ASYNC_AUTH_RETURN_CODE 1

static void
as_async_command_parse_authentication(as_async_command* cmd)
{
	if (cmd->state == AS_ASYNC_STATE_AUTH_READ_HEADER) {
		// Read response length
		if (! as_async_read(cmd)) {
			return;
		}
		
		// Authenticate response buffer is at end of write buffer.
		cmd->pos = cmd->len - sizeof(as_proto);
		as_proto* proto = (as_proto*)&cmd->buf[cmd->pos];
		as_proto_swap_from_be(proto);
		cmd->auth_len = (uint32_t)proto->sz;
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
	uint8_t code = cmd->buf[cmd->len + AS_ASYNC_AUTH_RETURN_CODE];

	if (code) {
		// Can't authenticate socket, so must close it.
		as_error err;
		as_error_update(&err, code, "Authentication failed: %s", as_error_string(code));
		as_async_socket_error(cmd, &err);
		return;
	}
	cmd->pos = 0;
	
	// Try non-blocking command write.
	int ret = as_async_command_write(cmd);
	
	if (ret == AS_ASYNC_WRITE_COMPLETE) {
		// Done with write. Set command read.
		cmd->pos = 0;
		cmd->len = sizeof(as_proto);
		cmd->state = AS_ASYNC_STATE_READ_HEADER;
		as_event_set_read(&cmd->event);
		return;
	}

	if (ret == AS_ASYNC_WRITE_INCOMPLETE) {
		// Got would-block. Set command write.
		cmd->state = AS_ASYNC_STATE_WRITE;
		as_event_set_write(&cmd->event);
	}
}

static void
as_async_command_receive_multi(as_async_command* cmd)
{
	// Batch, scan, query may be waiting on end block.
	// Prepare for next message block.
	cmd->len = sizeof(as_proto);
	cmd->pos = 0;
	cmd->state = AS_ASYNC_STATE_READ_HEADER;
	
	if (! as_async_read(cmd)) {
		return;
	}
	
	as_proto* proto = (as_proto*)cmd->buf;
	as_proto_swap_from_be(proto);
	size_t size = proto->sz;
	
	cmd->len = (uint32_t)size;
	cmd->pos = 0;
	cmd->state = AS_ASYNC_STATE_READ_BODY;

	// Check for end block size.
	if (cmd->len == sizeof(as_msg)) {
		// Look like we received end block.  Read and parse to make sure.
		if (! as_async_read(cmd)) {
			return;
		}

		if (! cmd->parse_results(cmd)) {
			// We did not finish after all. Prepare to read next header.
			cmd->len = sizeof(as_proto);
			cmd->pos = 0;
			cmd->state = AS_ASYNC_STATE_READ_HEADER;
		}
	}
	else {
		// Received normal data block.  Stop reading for fairness reasons and wait
		// till next iteration.
		if (cmd->len > cmd->capacity) {
			if (cmd->free_buf) {
				cf_free(cmd->buf);
			}
			cmd->buf = cf_malloc(size);
			cmd->capacity = cmd->len;
			cmd->free_buf = true;
		}
	}
}

void
as_async_command_receive(as_async_command* cmd)
{
	// Check for authenticate read-header or read-body.
	if (cmd->state & (AS_ASYNC_STATE_AUTH_READ_HEADER | AS_ASYNC_STATE_AUTH_READ_BODY)) {
		as_async_command_parse_authentication(cmd);
		return;
	}
	
	if (cmd->state == AS_ASYNC_STATE_READ_HEADER) {
		// Read response length
		if (! as_async_read(cmd)) {
			return;
		}
		
		as_proto* proto = (as_proto*)cmd->buf;
		as_proto_swap_from_be(proto);
		size_t size = proto->sz;
		
		cmd->len = (uint32_t)size;
		cmd->pos = 0;
		cmd->state = AS_ASYNC_STATE_READ_BODY;
		
		if (cmd->len > cmd->capacity) {
			if (cmd->free_buf) {
				cf_free(cmd->buf);
			}
			cmd->buf = cf_malloc(size);
			cmd->capacity = cmd->len;
			cmd->free_buf = true;
		}
	}
	
	// Read response body
	if (! as_async_read(cmd)) {
		return;
	}
	
	if (! cmd->parse_results(cmd)) {
		// Batch, scan, query is not finished.
		as_async_command_receive_multi(cmd);
	}
}

bool
as_async_command_parse_header(as_async_command* cmd)
{
	if (cmd->len < sizeof(as_msg)) {
		as_error err;
		as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Invalid record header size: %u", cmd->len);
		as_async_socket_error(cmd, &err);
		return true;
	}
	
	as_msg* msg = (as_msg*)cmd->buf;
	
	if (msg->result_code == AEROSPIKE_OK) {
		as_async_response_complete(cmd);
		((as_async_write_command*)cmd)->listener(0, cmd->udata, cmd->event.event_loop);
		as_async_command_free(cmd);
	}
	else {
		as_error err;
		as_error_set_message(&err, msg->result_code, as_error_string(msg->result_code));
		as_async_response_error(cmd, &err);
	}
	return true;
}

bool
as_async_command_parse_result(as_async_command* cmd)
{
	if (cmd->len < sizeof(as_msg)) {
		as_error err;
		as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Invalid record header size: %u", cmd->len);
		as_async_socket_error(cmd, &err);
		return true;
	}
	
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
		
			as_async_response_complete(cmd);
			((as_async_record_command*)cmd)->listener(0, &rec, cmd->udata, cmd->event.event_loop);
			as_async_command_free(cmd);
			as_record_destroy(&rec);
			break;
		}
			
		case AEROSPIKE_ERR_UDF: {
			as_error err;
			as_command_parse_udf_failure(p, &err, msg, status);
			as_async_response_error(cmd, &err);
			break;
		}
			
		default: {
			as_error err;
			as_error_set_message(&err, status, as_error_string(status));
			as_async_response_error(cmd, &err);
			break;
		}
	}
	return true;
}

bool
as_async_command_parse_success_failure(as_async_command* cmd)
{
	if (cmd->len < sizeof(as_msg)) {
		as_error err;
		as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Invalid record header size: %u", cmd->len);
		as_async_socket_error(cmd, &err);
		return true;
	}

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
				as_async_response_complete(cmd);
				((as_async_value_command*)cmd)->listener(0, val, cmd->udata, cmd->event.event_loop);
				as_async_command_free(cmd);
				as_val_destroy(val);
			}
			else {
				as_async_response_error(cmd, &err);
			}
			break;
		}
			
		case AEROSPIKE_ERR_UDF: {
			as_error err;
			as_command_parse_udf_failure(p, &err, msg, status);
			as_async_response_error(cmd, &err);
			break;
		}

		default: {
			as_error err;
			as_error_set_message(&err, status, as_error_string(status));
			as_async_response_error(cmd, &err);
			break;
		}
	}
	return true;
}
