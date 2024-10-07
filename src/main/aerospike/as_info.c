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
#include <aerospike/as_info.h>
#include <aerospike/as_admin.h>
#include <aerospike/as_async.h>
#include <aerospike/as_command.h>
#include <aerospike/as_event_internal.h>
#include <aerospike/as_proto.h>
#include <aerospike/as_socket.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_b64.h>
#include <stdlib.h>

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static void
as_info_decode_error(char* begin)
{
	// Decode base64 message in place.
	// UDF error format: <error message>;file=<file>;line=<line>;message=<base64 message>\n
	char* msg = strstr(begin, "message=");

	if (msg) {
		msg += 8;
		
		uint32_t src_len = (uint32_t)strlen(msg);

		if (msg[src_len-1] == '\n') {
			src_len--; // Ignore newline '\n' at the end
		}

		uint32_t trg_len = 0;
		
		if (cf_b64_validate_and_decode_in_place((uint8_t*)msg, src_len, &trg_len)) {
			msg[trg_len] = 0;
		}
	}
}

static as_status
as_info_parse_error(char* begin, char** message)
{
	// Parse error format: [<code>][:<message>][\t|\n]
	// Terminate tab.
	char* p = strchr(begin, '\t');
	int rc;

	if (p) {
		*p = 0;
	}
	else {
		// Terminate newline.
		p = strchr(begin, '\n');

		if (p) {
			*p = 0;
		}
	}

	p = strchr(begin, ':');

	if (p) {
		*p++ = 0;
		*message = p;

		rc = atoi(begin);

		if (rc == 0) {
			rc = AEROSPIKE_ERR_SERVER;
		}

		// Handle new error format for "udf-put" command.
		as_info_decode_error(p);
	}
	else {
		*message = begin;
		rc = AEROSPIKE_ERR_SERVER;
	}
	return rc;
}

static bool
as_info_keep_connection(as_status status)
{
	switch (status) {
		case AEROSPIKE_ERR_CONNECTION:
		case AEROSPIKE_ERR_TIMEOUT:
		case AEROSPIKE_ERR_CLIENT:
		case AEROSPIKE_ERR_CLIENT_ABORT:
		case AEROSPIKE_NOT_AUTHENTICATED:
		case AEROSPIKE_ERR_TLS_ERROR:
			return false;

		default:
			return true;
	}
}

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_status
as_info_command_node(
	as_error* err, as_node* node, char* command, bool send_asis, uint64_t deadline_ms,
	char** response
	)
{
	as_socket socket;
	as_status status = as_node_get_connection(err, node, 0, deadline_ms, &socket);
	
	if (status != AEROSPIKE_OK) {
		return status;
	}
	
	status = as_info_command(err, &socket, node, command, send_asis, deadline_ms, 0, response);

	if (status != AEROSPIKE_OK) {
		if (as_info_keep_connection(status)) {
			as_node_put_conn_error(node, &socket);
		}
		else {
			as_node_close_conn_error(node, &socket, socket.pool);
		}

		// Add node address to error message.
		char str[512];
		snprintf(str, sizeof(str), " from %s", as_node_get_address_string(node));
		as_error_append(err, str);
		return status;
	}

	as_node_put_connection(node, &socket);
	return status;
}

as_status
as_info_command_node_async(
	aerospike* as, as_error* err, as_policy_info* policy, as_node* node, const char* command,
	as_async_info_listener listener, void* udata, as_event_loop* event_loop
	)
{
	as_error_reset(err);

	if (! policy) {
		policy = &as->config.policies.info;
	}

	size_t size = strlen(command);
	as_event_command* cmd = as_async_info_command_create(node, policy, listener, udata, event_loop, size);
	uint8_t* p = cmd->buf + sizeof(uint64_t);

	memcpy(p, command, size);
	p += size;
	size = p - cmd->buf;
	uint64_t proto = (size - 8) | ((uint64_t)AS_PROTO_VERSION << 56) | ((uint64_t)AS_INFO_MESSAGE_TYPE << 48);
	*(uint64_t*)cmd->buf = cf_swap_to_be64(proto);
	cmd->write_len = (uint32_t)size;

	return as_event_command_execute(cmd, err);
}

as_status
as_info_command_random_node(aerospike* as, as_error* err, as_policy_info* policy, char* command)
{
	as_error_reset(err);

	if (! policy) {
		policy = &as->config.policies.info;
	}

	uint64_t deadline = as_socket_deadline(policy->timeout);
	as_node* node = as_node_get_random(as->cluster);

	if (! node) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to find server node.");
	}

	char* response;
	as_status status = as_info_command_node(err, node, command, true, deadline, &response);

	if (status == AEROSPIKE_OK) {
		cf_free(response);
	}

	as_node_release(node);
	return status;
}

as_status
as_info_command_host(
	as_cluster* cluster, as_error* err, struct sockaddr* addr, char* command,
	bool send_asis, uint64_t deadline_ms, char** response, const char* tls_name
	)
{
	as_socket sock;
	as_status status = as_info_create_socket(cluster, err, addr, deadline_ms, tls_name, &sock);
	
	if (status) {
		*response = 0;
		return status;
	}
		
	status = as_info_command(err, &sock, NULL, command, send_asis, deadline_ms, 0, response);

	if (status) {
		as_socket_error_append(err, addr);
	}
	as_socket_close(&sock);
	return status;
}

as_status
as_info_command(
	as_error* err, as_socket* sock, as_node* node, char* names, bool send_asis, uint64_t deadline_ms,
	uint64_t max_response_length, char** values
	)
{
	*values = 0;
	
	size_t size = 8;  // header size.
	uint32_t slen = 0;
	bool add_newline = false;
	
	// Deal with the incoming 'names' parameter
	// Translate interior ';' in the passed-in names to \n
	if (names) {
		if (send_asis) {
			slen = (uint32_t)strlen(names);
		}
		else {
			char *p = names;
			while (*p) {
				slen++;
				if ((*p == ';') || (*p == ':') || (*p == ',')) {
					*p = '\n';
				}
				p++;
			}
		}
		size += slen;

		// Sometimes people forget to/cannot add the trailing '\n'. Be nice and
		// add it for them using a stack allocated variable so we don't have to clean up.
		if (slen && names[slen-1] != '\n') {
			add_newline = true;
			size++;
		}
	}
	else {
		names = "";
	}

	uint8_t* cmd = as_command_buffer_init(size);
	
	// Write request
	uint8_t* p = cmd + 8;
	memcpy(p, names, slen);
	p += slen;
	
	if (add_newline) {
		*p++ = '\n';
	}
	
	// Write header
	size = p - cmd;
	uint64_t proto = (size - 8) | ((uint64_t)AS_PROTO_VERSION << 56) | ((uint64_t)AS_INFO_MESSAGE_TYPE << 48);
	*(uint64_t*)cmd = cf_swap_to_be64(proto);
	
	// Write command
	as_status status = as_socket_write_deadline(err, sock, node, cmd, size, 0, deadline_ms);
	as_command_buffer_free(cmd, size);
	
	if (status) {
		return status;
	}
	
	// Read response
	as_proto header;
	status = as_socket_read_deadline(err, sock, node, (uint8_t*)&header, sizeof(as_proto), 0, deadline_ms);
	
	if (status) {
		return status;
	}
	
	status = as_proto_parse_type(err, &header, AS_INFO_MESSAGE_TYPE);

	if (status) {
		return status;
	}

	if (header.sz) {
		if (max_response_length > 0 && header.sz > max_response_length) {
			// Response buffer is too big.  Read a few bytes just to see what the buffer contains.
			// Reuse command buffer.
			int read_len = 100;
			uint8_t* buf = alloca(read_len + 1);
			status = as_socket_read_deadline(err, sock, node, buf, read_len, 0, deadline_ms);
			
			if (status) {
				return status;
			}
			
			buf[read_len] = 0;
			return as_error_update(err, AEROSPIKE_ERR_CLIENT,
								   "Info request '%s' failed. Response buffer length %lu is excessive. Buffer: %s",
								   names, (uint64_t)header.sz, buf);
		}
		
		char* response = cf_malloc(header.sz + 1);
		status = as_socket_read_deadline(err, sock, node, (uint8_t*)response, header.sz, 0, deadline_ms);
		
		if (status) {
			cf_free(response);
			*values = 0;
			return status;
		}
		response[header.sz] = 0;
		
		char* error = 0;
		status = as_info_validate(response, &error);
		
		if (status) {
			as_error_set_message(err, status, error);
			cf_free(response);
			*values = 0;
			return status;
		}
		
		*values = response;
	}
	return status;
}

as_status
as_info_create_socket(
	as_cluster* cluster, as_error* err, struct sockaddr* addr, uint64_t deadline_ms,
	const char* tls_name, as_socket* sock
	)
{
	// This function can't authenticate because node and session token are not specified.
	as_tls_context* ctx = as_socket_get_tls_context(cluster->tls_ctx);
	return as_socket_create_and_connect(sock, err, addr, ctx, tls_name, deadline_ms);	
}

as_status
as_info_validate(char* response, char** message)
{
	char* p = response;

	if (p) {
		// Check for errors embedded in the response.
		// ERROR: may appear at beginning of string.
		if (strncmp(p, "ERROR:", 6) == 0) {
			return as_info_parse_error(p + 6, message);
		}

		// ERROR: or FAIL: may appear after a tab.
		while ((p = strchr(p, '\t'))) {
			p++;

			if (strncmp(p, "ERROR:", 6) == 0) {
				return as_info_parse_error(p + 6, message);
			}

			if (strncmp(p, "FAIL:", 5) == 0) {
				return as_info_parse_error(p + 5, message);
			}

			if (strncmp(p, "error=", 6) == 0) {
				*message = p;
				as_info_decode_error(p + 6);
				return AEROSPIKE_ERR_UDF;
			}
		}
	}
	return AEROSPIKE_OK;
}

as_status
as_info_validate_item(as_error* err, char* response)
{
	if (strncmp(response, "ERROR:", 6) == 0) {
		char* msg = NULL;
		as_status status = as_info_parse_error(response + 6, &msg);
		return as_error_set_message(err, status, msg);
	}
	return AEROSPIKE_OK;
}

as_status
as_info_parse_single_response(char *values, char **value)
{
	while (*values && (*values != '\t')) {
		values++;
	}
	
	if (*values == 0) {
		return AEROSPIKE_ERR_CLIENT;
	}
	values++;
	*value = values;
	
	while (*values && (*values != '\n')) {
		values++;
	}
	
	if (*values == 0) {
		return AEROSPIKE_ERR_CLIENT;
	}
	*values = 0;
	return AEROSPIKE_OK;
}

void
as_info_parse_multi_response(char* buf, as_vector* /* <as_name_value> */ values)
{
	// Info buffer format: name1\tvalue1\nname2\tvalue2\n...
	char* p = buf;
	char* begin = p;
	
	as_name_value nv;
	
	while (*p) {
		if (*p == '\t') {
			// Found end of name. Null terminate it.
			*p = 0;
			nv.name = begin;
			begin = ++p;
			
			// Parse value.
			while (*p) {
				if (*p == '\n') {
					*p = 0;
					break;
				}
				p++;
			}
			nv.value = begin;
			as_vector_append(values, &nv);
			begin = ++p;
		}
		else if (*p == '\n') {
			// Found new line before tab.
			*p = 0;
			
			if (p > begin) {
				// Name returned without value.
				nv.name = begin;
				nv.value = p;
				as_vector_append(values, &nv);
			}
			begin = ++p;
		}
		else {
			p++;
		}
	}
	
	if (p > begin) {
		// Name returned without value.
		nv.name = begin;
		nv.value = p;
		as_vector_append(values, &nv);
	}
}
