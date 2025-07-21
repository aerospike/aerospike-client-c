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
#include <aerospike/as_admin.h>
#include <aerospike/as_command.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_proto.h>
#include <aerospike/as_socket.h>
#include <aerospike/as_status.h>
#include <citrusleaf/cf_byte_order.h>
#include <citrusleaf/cf_clock.h>
#include <string.h>

//---------------------------------
// Types
//---------------------------------

typedef as_status (*as_admin_parse_fn) (as_error* err, uint8_t* buffer, size_t size, as_vector* list);

//---------------------------------
// Macros
//---------------------------------

// Commands
#define AUTHENTICATE 0
#define CREATE_USER 1
#define DROP_USER 2
#define SET_PASSWORD 3
#define CHANGE_PASSWORD 4
#define GRANT_ROLES 5
#define REVOKE_ROLES 6
#define QUERY_USERS 9
#define CREATE_ROLE 10
#define DROP_ROLE 11
#define GRANT_PRIVILEGES 12
#define REVOKE_PRIVILEGES 13
#define SET_WHITELIST 14
#define SET_QUOTAS 15
#define QUERY_ROLES 16
#define LOGIN 20

// Field IDs
#define USER 0
#define PASSWORD 1
#define OLD_PASSWORD 2
#define CREDENTIAL 3
#define CLEAR_PASSWORD 4
#define SESSION_TOKEN 5
#define SESSION_TTL 6
#define ROLES 10
#define ROLE 11
#define PRIVILEGES 12
#define WHITELIST 13
#define READ_QUOTA 14
#define WRITE_QUOTA 15
#define READ_INFO 16
#define WRITE_INFO 17
#define CONNECTIONS 18

// Misc
#define FIELD_HEADER_SIZE 5
#define HEADER_SIZE 24
#define HEADER_REMAINING 16
#define RESULT_CODE 9
#define DEFAULT_TIMEOUT 60000  // one minute

//---------------------------------
// Static Functions
//---------------------------------

static uint8_t*
as_admin_write_header(uint8_t* p, uint8_t command, uint8_t field_count)
{
	memset(p, 0, HEADER_REMAINING);
	p[2] = command;
	p[3] = field_count;
	return p + HEADER_REMAINING;;
}

static uint8_t*
as_admin_write_field_header(uint8_t* p, uint8_t id, int size)
{
	*(int*)p = cf_swap_to_be32(size+1);
	p += 4;
	*p++ = id;
	return p;
}

static uint8_t*
as_admin_write_field_int(uint8_t* p, uint8_t id, int val)
{
	p = as_admin_write_field_header(p, id, 4);
	*(int*)p = cf_swap_to_be32(val);
	p += 4;
	return p;
}

static uint8_t*
as_admin_write_field_string(uint8_t* p, uint8_t id, const char* val)
{
	// Copy string, but do not transfer null byte.
	uint8_t* q = p + FIELD_HEADER_SIZE;
	while (*val) {
		*q++ = *val++;
	}
	as_admin_write_field_header(p, id, (int)(q - p - FIELD_HEADER_SIZE));
	return q;
}

static uint8_t*
as_admin_write_field_bytes(uint8_t* p, uint8_t id, const uint8_t* bytes, uint32_t len)
{
	p = as_admin_write_field_header(p, id, len);
	memcpy(p, bytes, len);
	return p + len;
}

static uint8_t*
as_admin_write_roles(uint8_t* p, const char** roles, int length)
{
	uint8_t* q = p + FIELD_HEADER_SIZE;
	*q++ = (uint8_t)length;
	
	for (int i = 0; i < length; i++) {
		// Copy string, but do not transfer null byte.
		const char* role = roles[i];
		uint8_t* r = q + 1;
		while (*role) {
			*r++ = *role++;
		}
		*q = (uint8_t)(r - q - 1);
		q = r;
	}
	as_admin_write_field_header(p, ROLES, (int)(q - p - FIELD_HEADER_SIZE));
	return q;
}

static uint8_t*
as_admin_write_string(uint8_t* p, const char* str)
{
	// Copy string, but do not transfer null byte.
	uint8_t* q = p + 1;
	while (*str) {
		*q++ = *str++;
	}
	*p = (uint8_t)(q - p - 1);
	return q;
}

static as_status
as_admin_write_privileges(uint8_t** p, as_error* err, as_privilege** privileges, int privileges_size)
{
	uint8_t* q = *p + FIELD_HEADER_SIZE;
	*q++ = (uint8_t)privileges_size;
	
	for (int i = 0; i < privileges_size; i++) {
		as_privilege* priv = privileges[i];
		*q++ = (uint8_t)priv->code;
		
		if (priv->code >= AS_PRIVILEGE_READ) {
			q = as_admin_write_string(q, priv->ns);
			q = as_admin_write_string(q, priv->set);
		}
		else {
			if (priv->ns[0] || priv->set[0]) {
				return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
									"Admin privilege has namespace/set scope which is invalid.");
			}
		}
	}
	as_admin_write_field_header(*p, PRIVILEGES, (int)(q - *p - FIELD_HEADER_SIZE));
	*p = q;
	return AEROSPIKE_OK;
}

static as_status
as_admin_write_whitelist(uint8_t** p, as_error* err, const char** whitelist, int whitelist_size)
{
	uint8_t* q = *p + FIELD_HEADER_SIZE;

	for (int i = 0; i < whitelist_size; i++) {
		if (i > 0) {
			*q++ = ',';
		}

		const char* val = whitelist[i];

		while (*val) {
			*q++ = *val++;
		}
	}
	as_admin_write_field_header(*p, WHITELIST, (int)(q - *p - FIELD_HEADER_SIZE));
	*p = q;
	return AEROSPIKE_OK;
}

static as_status
as_admin_send(
	as_error* err, as_socket* sock, as_node* node, uint8_t* buffer, uint8_t* end,
	uint32_t socket_timeout, uint64_t deadline_ms
	)
{
	uint64_t len = end - buffer;
	uint64_t proto = (len - 8) | ((uint64_t)AS_PROTO_VERSION << 56) | ((uint64_t)AS_ADMIN_MESSAGE_TYPE << 48);
	*(uint64_t*)buffer = cf_swap_to_be64(proto);
	
	as_status status = as_socket_write_deadline(err, sock, node, buffer, len, socket_timeout, deadline_ms);

	if (status == AEROSPIKE_OK && node && node->cluster->metrics_enabled) {
		as_ns_metrics* metrics = as_node_prepare_metrics(node, NULL);

		if (metrics) {
			as_node_add_bytes_out(metrics, len);
		}
	}
	return status;
}

static inline as_status
as_admin_receive(
	as_error* err, as_socket* sock, as_node* node, uint8_t* buffer, uint64_t len,
	uint32_t socket_timeout, uint64_t deadline_ms
	)
{
	as_status status = as_socket_read_deadline(err, sock, node, buffer, len, socket_timeout, deadline_ms);

	if (status == AEROSPIKE_OK && node && node->cluster->metrics_enabled) {
		as_ns_metrics* metrics = as_node_prepare_metrics(node, NULL);

		if (metrics) {
			as_node_add_bytes_in(metrics, len);
		}
	}
	return status;
}

static uint32_t
as_policy_admin_get_timeout(aerospike* as)
{
	as_config* config = aerospike_load_config(as);
	return config->policies.admin.timeout;
}

static as_status
as_admin_execute_node(
	aerospike* as, as_node* node, as_error* err, const as_policy_admin* policy, uint8_t* buffer,
	uint8_t* end
	)
{
	uint32_t timeout_ms = (policy)? policy->timeout : as_policy_admin_get_timeout(as);
	if (timeout_ms == 0) {
		timeout_ms = DEFAULT_TIMEOUT;
	}
	uint64_t deadline_ms = as_socket_deadline(timeout_ms);

	as_socket socket;
	as_status status = as_node_get_connection(err, node, NULL, 0, deadline_ms, &socket);

	if (status) {
		return status;
	}

	status = as_admin_send(err, &socket, node, buffer, end, 0, deadline_ms);
	
	if (status) {
		as_node_close_conn_error(node, &socket, socket.pool);
		return status;
	}

	status = as_admin_receive(err, &socket, node, buffer, HEADER_SIZE, 0, deadline_ms);

	if (status) {
		as_node_close_conn_error(node, &socket, socket.pool);
		return status;
	}
	
	as_node_put_connection(node, &socket);

	status = buffer[RESULT_CODE];
	
	if (status) {
		return as_error_set_message(err, status, as_error_string(status));
	}
	return status;
}

static inline as_status
as_admin_execute(
	aerospike* as, as_error* err, const as_policy_admin* policy, uint8_t* buffer, uint8_t* end
	)
{
	as_node* node = as_node_get_random(as->cluster);
	
	if (! node) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to find server node");
	}

	as_status status = as_admin_execute_node(as, node, err, policy, buffer, end);
	as_node_release(node);
	return status;
}

static as_status
as_admin_read_blocks(
	as_error* err, as_socket* sock, as_node* node, uint64_t deadline_ms, as_admin_parse_fn parse_fn,
	as_vector* list
	)
{
	as_status status = AEROSPIKE_OK;
	uint8_t* buf = 0;
	size_t capacity = 0;
	
	while (true) {
		// Read header
		as_proto proto;
		status = as_admin_receive(err, sock, node, (uint8_t*)&proto, sizeof(as_proto), 0, deadline_ms);

		if (status) {
			break;
		}

		status = as_proto_parse_type(err, &proto, AS_ADMIN_MESSAGE_TYPE);

		if (status) {
			break;
		}

		size_t size = proto.sz;
		
		if (size > 0) {
			// Prepare buffer
			if (size > capacity) {
				as_command_buffer_free(buf, capacity);
				capacity = size;
				buf = as_command_buffer_init(capacity);
			}
			
			// Read remaining message bytes in group
			status = as_admin_receive(err, sock, node, buf, size, 0, deadline_ms);
			
			if (status) {
				break;
			}
			
			status = parse_fn(err, buf, size, list);
			
			if (status != AEROSPIKE_OK) {
				if (status == AEROSPIKE_QUERY_END) {
					status = AEROSPIKE_OK;
				}
				else {
					as_error_set_message(err, status, as_error_string(status));
				}
				break;
			}
		}
	}
	as_command_buffer_free(buf, capacity);
	return status;
}

static as_status
as_admin_read_list(
	aerospike* as, as_error* err, const as_policy_admin* policy, uint8_t* command, uint8_t* end,
	as_admin_parse_fn parse_fn, as_vector* list
	)
{
	int timeout_ms = (policy)? policy->timeout : as_policy_admin_get_timeout(as);
	if (timeout_ms <= 0) {
		timeout_ms = DEFAULT_TIMEOUT;
	}
	uint64_t deadline_ms = as_socket_deadline(timeout_ms);
	as_cluster* cluster = as->cluster;
	as_node* node = as_node_get_random(cluster);
	
	if (! node) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to find server node");
	}

	as_socket socket;
	as_status status = as_node_get_connection(err, node, NULL, 0, deadline_ms, &socket);
	
	if (status) {
		as_node_release(node);
		return status;
	}
	
	status = as_admin_send(err, &socket, node, command, end, 0, deadline_ms);
	
	if (status) {
		as_node_close_conn_error(node, &socket, socket.pool);
		as_node_release(node);
		return status;
	}
	
	status = as_admin_read_blocks(err, &socket, node, deadline_ms, parse_fn, list);
	
	if (status) {
		as_node_close_conn_error(node, &socket, socket.pool);
		as_node_release(node);
		return status;
	}

	as_node_put_connection(node, &socket);
	as_node_release(node);
	return status;
}

static void
as_admin_modify_password_error(as_status status, as_error* err)
{
	if (status == AEROSPIKE_FORBIDDEN_PASSWORD) {
		as_strncpy(err->message, "PKI user password not changeable", sizeof(err->message));
	}
}

//---------------------------------
// Functions
//---------------------------------

as_status
as_cluster_login(
	as_cluster* cluster, as_error* err, as_socket* sock, uint64_t deadline_ms,
	as_node_info* node_info
)
{
	node_info->session = NULL;

	uint8_t buffer[AS_STACK_BUF_SIZE];
	uint8_t* p = buffer + 8;

	if (cluster->auth_mode == AS_AUTH_INTERNAL) {
		p = as_admin_write_header(p, LOGIN, 2);
		p = as_admin_write_field_string(p, USER, cluster->user);
		p = as_admin_write_field_string(p, CREDENTIAL, cluster->password_hash);
	}
	else if (cluster->auth_mode == AS_AUTH_PKI) {
		p = as_admin_write_header(p, LOGIN, 0);
	}
	else {
		p = as_admin_write_header(p, LOGIN, 3);
		p = as_admin_write_field_string(p, USER, cluster->user);
		p = as_admin_write_field_string(p, CREDENTIAL, cluster->password_hash);
		p = as_admin_write_field_string(p, CLEAR_PASSWORD, cluster->password);
	}

	as_status status = as_admin_send(err, sock, NULL, buffer, p, 0, deadline_ms);

	if (status) {
		return status;
	}

	status = as_admin_receive(err, sock, NULL, buffer, HEADER_SIZE, 0, deadline_ms);

	if (status) {
		return status;
	}

	status = buffer[RESULT_CODE];

	if (status) {
		if (status == AEROSPIKE_SECURITY_NOT_ENABLED) {
			// Server does not require login.
			return AEROSPIKE_OK;
		}
		return as_error_set_message(err, status, as_error_string(status));
	}

	// Read session token.
	as_proto* proto = (as_proto*)buffer;

	status = as_proto_parse_type(err, proto, AS_ADMIN_MESSAGE_TYPE);

	if (status) {
		return status;
	}

	int64_t receive_size = proto->sz - HEADER_REMAINING;
	int field_count = buffer[11];

	if (receive_size <= 0 || receive_size > AS_STACK_BUF_SIZE || field_count <= 0) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to retrieve session token");
	}

	// Read remaining message bytes in group
	status = as_admin_receive(err, sock, NULL, buffer, receive_size, 0, deadline_ms);

	if (status) {
		return status;
	}

	as_session* session = NULL;
	uint64_t expiration = 0;
	int len;
	uint8_t id;
	p = buffer;

	for (int i = 0; i < field_count; i++) {
		len = cf_swap_from_be32(*(int*)p);
		p += 4;
		id = *p++;
		len--;

		if (id == SESSION_TOKEN && !session) {
			if (len > 0 && len < AS_STACK_BUF_SIZE) {
				session = cf_malloc(sizeof(as_session) + len);
				session->ref_count = 1;
				session->token_length = len;
				memcpy(session->token, p, len);
			}
			else {
				return as_error_update(err, AEROSPIKE_ERR_CLIENT,
									   "Invalid session token length %d", len);
			}
		}
		else if (id == SESSION_TTL) {
			// Subtract 60 seconds from ttl so client session expires before server session.
			int64_t seconds = (int64_t)cf_swap_from_be32(*(uint32_t*)p) - 60;

			if (seconds > 0) {
				expiration = cf_getns() + (seconds * 1000 * 1000 * 1000);
			}
			else {
				as_log_warn("Invalid session TTL: %" PRIi64, seconds);
			}
		}
		p += len;
	}

	if (session == NULL) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to retrieve session token");
	}

	session->expiration = expiration;
	node_info->session = session;
	return AEROSPIKE_OK;
}

uint32_t
as_authenticate_set(as_cluster* cluster, as_session* session, uint8_t* buffer)
{
	uint8_t* p = buffer + 8;
	
	if (cluster->auth_mode != AS_AUTH_PKI) {
		p = as_admin_write_header(p, AUTHENTICATE, 2);
		p = as_admin_write_field_string(p, USER, cluster->user);
	}
	else {
		p = as_admin_write_header(p, AUTHENTICATE, 1);
	}

	p = as_admin_write_field_bytes(p, SESSION_TOKEN, session->token, session->token_length);

	uint64_t len = p - buffer;
	uint64_t proto = (len - 8) | ((uint64_t)AS_PROTO_VERSION << 56) | ((uint64_t)AS_ADMIN_MESSAGE_TYPE << 48);
	*(uint64_t*)buffer = cf_swap_to_be64(proto);
	return (uint32_t)len;
}

as_status
as_authenticate(
	as_cluster* cluster, as_error* err, as_socket* sock, as_node* node, as_session* session,
	uint32_t socket_timeout, uint64_t deadline_ms
	)
{
	uint8_t buffer[AS_STACK_BUF_SIZE];
	uint8_t* p = buffer + 8;

	if (cluster->auth_mode != AS_AUTH_PKI) {
		p = as_admin_write_header(p, AUTHENTICATE, 2);
		p = as_admin_write_field_string(p, USER, cluster->user);
	}
	else {
		p = as_admin_write_header(p, AUTHENTICATE, 1);
	}

	p = as_admin_write_field_bytes(p, SESSION_TOKEN, session->token, session->token_length);

	as_status status = as_admin_send(err, sock, node, buffer, p, socket_timeout, deadline_ms);
	
	if (status) {
		return status;
	}

	status = as_admin_receive(err, sock, node, buffer, HEADER_SIZE, socket_timeout, deadline_ms);
	
	if (status) {
		return status;
	}
	
	status = buffer[RESULT_CODE];

	if (status) {
		if (status == AEROSPIKE_SECURITY_NOT_ENABLED) {
			return AEROSPIKE_OK;
		}
	
		as_error_set_message(err, status, as_error_string(status));
	}
	return status;
}

as_status
aerospike_create_user(
	aerospike* as, as_error* err, const as_policy_admin* policy, const char* user,
	const char* password, const char** roles, int roles_size
	)
{
	as_error_reset(err);

	int len = (int)strlen(user);

	if (len >= AS_USER_SIZE) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Max user length %d exceeded: %d",
							   AS_USER_SIZE - 1, len)
	}

	len = (int)strlen(password);

	if (len >= AS_PASSWORD_SIZE) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Max password length %d exceeded: %d",
							   AS_PASSWORD_SIZE - 1, len)
	}

	char hash[AS_PASSWORD_HASH_SIZE];
	as_password_get_constant_hash(password, hash);
	
	uint8_t buffer[AS_STACK_BUF_SIZE];
	uint8_t* p = buffer + 8;
	
	p = as_admin_write_header(p, CREATE_USER, 3);
	p = as_admin_write_field_string(p, USER, user);
	p = as_admin_write_field_string(p, PASSWORD, hash);
	p = as_admin_write_roles(p, roles, roles_size);
	return as_admin_execute(as, err, policy, buffer, p);
}

as_status
aerospike_create_pki_user(
	aerospike* as, as_error* err, const as_policy_admin* policy, const char* user,
	const char** roles, int roles_size
	)
{
	as_node* node = as_node_get_random(as->cluster);
	
	if (! node) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to find server node");
	}

	as_version min = {8, 1, 0, 0};

	if (as_version_compare(&node->version, &min) < 0) {
		char ver_str[32], min_str[32];
		as_version_to_string(&node->version, ver_str, sizeof(ver_str));
		as_version_to_string(&min, min_str, sizeof(min_str));
		as_node_release(node);
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
			"Node version %s is less than required minimum version %s", ver_str, min_str);
	}

	as_error_reset(err);

	int len = (int)strlen(user);

	if (len >= AS_USER_SIZE) {
		as_node_release(node);
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Max user length %d exceeded: %d",
							   AS_USER_SIZE - 1, len)
	}

	char hash[AS_PASSWORD_HASH_SIZE];

	// nopassword is a special keyword used by server versions 8.1+ to indicate that password
	// authentication is not allowed.
	as_password_get_constant_hash("nopassword", hash);

	uint8_t buffer[AS_STACK_BUF_SIZE];
	uint8_t* p = buffer + 8;
	
	p = as_admin_write_header(p, CREATE_USER, 3);
	p = as_admin_write_field_string(p, USER, user);
	p = as_admin_write_field_string(p, PASSWORD, hash);
	p = as_admin_write_roles(p, roles, roles_size);

	as_status status = as_admin_execute_node(as, node, err, policy, buffer, p);
	as_node_release(node);
	return status;
}

as_status
aerospike_drop_user(aerospike* as, as_error* err, const as_policy_admin* policy, const char* user)
{
	as_error_reset(err);

	uint8_t buffer[AS_STACK_BUF_SIZE];
	uint8_t* p = buffer + 8;
	
	p = as_admin_write_header(p, DROP_USER, 1);
	p = as_admin_write_field_string(p, USER, user);
	return as_admin_execute(as, err, policy, buffer, p);
}

as_status
aerospike_set_password(
	aerospike* as, as_error* err, const as_policy_admin* policy, const char* user,
	const char* password
	)
{
	as_error_reset(err);

	if (! user) {
		user = as->cluster->user;
	}

	int len = (int)strlen(user);

	if (len >= AS_USER_SIZE) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Max user length %d exceeded: %d",
							   AS_USER_SIZE - 1, len)
	}

	len = (int)strlen(password);

	if (len >= AS_PASSWORD_SIZE) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Max password length %d exceeded: %d",
							   AS_PASSWORD_SIZE - 1, len)
	}

	char hash[AS_PASSWORD_HASH_SIZE];
	as_password_get_constant_hash(password, hash);
	
	uint8_t buffer[AS_STACK_BUF_SIZE];
	uint8_t* p = buffer + 8;
	
	p = as_admin_write_header(p, SET_PASSWORD, 2);
	p = as_admin_write_field_string(p, USER, user);
	p = as_admin_write_field_string(p, PASSWORD, hash);
	int status = as_admin_execute(as, err, policy, buffer, p);
	
	if (status == AEROSPIKE_OK) {
		as_cluster_change_password(as->cluster, user, password, hash);
	}
	else {
		as_admin_modify_password_error(status, err);
	}
	return status;
}

as_status
aerospike_change_password(
	aerospike* as, as_error* err, const as_policy_admin* policy, const char* user,
	const char* password
	)
{
	as_error_reset(err);

	if (! user) {
		user = as->cluster->user;
	}

	int len = (int)strlen(user);

	if (len >= AS_USER_SIZE) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Max user length %d exceeded: %d", AS_USER_SIZE - 1, len)
	}

	len = (int)strlen(password);

	if (len >= AS_PASSWORD_SIZE) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Max password length %d exceeded: %d", AS_PASSWORD_SIZE - 1, len)
	}

	if (! as->cluster->password_hash) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "Current hashed password is invalid");
	}

	char hash[AS_PASSWORD_HASH_SIZE];
	as_password_get_constant_hash(password, hash);
	
	uint8_t buffer[AS_STACK_BUF_SIZE];
	uint8_t* p = buffer + 8;
	
	p = as_admin_write_header(p, CHANGE_PASSWORD, 3);
	p = as_admin_write_field_string(p, USER, user);
	p = as_admin_write_field_string(p, OLD_PASSWORD, as->cluster->password_hash);
	p = as_admin_write_field_string(p, PASSWORD, hash);
	int status = as_admin_execute(as, err, policy, buffer, p);
	
	if (status == AEROSPIKE_OK) {
		as_cluster_change_password(as->cluster, user, password, hash);
	}
	else {
		as_admin_modify_password_error(status, err);
	}
	return status;
}

as_status
aerospike_grant_roles(
	aerospike* as, as_error* err, const as_policy_admin* policy, const char* user,
	const char** roles, int roles_size
	)
{
	as_error_reset(err);
	
	uint8_t buffer[AS_STACK_BUF_SIZE];
	uint8_t* p = buffer + 8;
	
	p = as_admin_write_header(p, GRANT_ROLES, 2);
	p = as_admin_write_field_string(p, USER, user);
	p = as_admin_write_roles(p, roles, roles_size);
	return as_admin_execute(as, err, policy, buffer, p);
}

as_status
aerospike_revoke_roles(
	aerospike* as, as_error* err, const as_policy_admin* policy, const char* user,
	const char** roles, int roles_size
	)
{
	as_error_reset(err);
	
	uint8_t buffer[AS_STACK_BUF_SIZE];
	uint8_t* p = buffer + 8;
	
	p = as_admin_write_header(p, REVOKE_ROLES, 2);
	p = as_admin_write_field_string(p, USER, user);
	p = as_admin_write_roles(p, roles, roles_size);
	return as_admin_execute(as, err, policy, buffer, p);
}

as_status
aerospike_create_role(
	aerospike* as, as_error* err, const as_policy_admin* policy, const char* role,
	as_privilege** privileges, int privileges_size
	)
{
	as_error_reset(err);

	uint8_t buffer[AS_STACK_BUF_SIZE];
	uint8_t* p = buffer + 8;
	
	p = as_admin_write_header(p, CREATE_ROLE, 2);
	p = as_admin_write_field_string(p, ROLE, role);
	as_status status = as_admin_write_privileges(&p, err, privileges, privileges_size);
	
	if (status) {
		return status;
	}
	return as_admin_execute(as, err, policy, buffer, p);
}

as_status
aerospike_create_role_whitelist(
	aerospike* as, as_error* err, const as_policy_admin* policy, const char* role,
	as_privilege** privileges, int privileges_size, const char** whitelist, int whitelist_size
	)
{
	as_error_reset(err);

	uint8_t buffer[AS_STACK_BUF_SIZE];
	uint8_t* p = buffer + 8;
	int field_count = 1;

	if (privileges_size > 0) {
		field_count++;
	}

	if (whitelist_size > 0) {
		field_count++;
	}

	p = as_admin_write_header(p, CREATE_ROLE, field_count);
	p = as_admin_write_field_string(p, ROLE, role);

	if (privileges_size > 0) {
		as_status status = as_admin_write_privileges(&p, err, privileges, privileges_size);

		if (status != AEROSPIKE_OK) {
			return status;
		}
	}

	if (whitelist_size > 0) {
		as_status status = as_admin_write_whitelist(&p, err, whitelist, whitelist_size);

		if (status != AEROSPIKE_OK) {
			return status;
		}
	}
	return as_admin_execute(as, err, policy, buffer, p);
}

as_status
aerospike_create_role_quotas(
	aerospike* as, as_error* err, const as_policy_admin* policy, const char* role,
	as_privilege** privileges, int privileges_size, const char** whitelist, int whitelist_size,
	int read_quota, int write_quota
	)
{
	as_error_reset(err);

	uint8_t buffer[AS_STACK_BUF_SIZE];
	uint8_t* p = buffer + 8;
	int field_count = 1;

	if (privileges_size > 0) {
		field_count++;
	}

	if (whitelist_size > 0) {
		field_count++;
	}

	if (read_quota > 0) {
		field_count++;
	}

	if (write_quota > 0) {
		field_count++;
	}

	p = as_admin_write_header(p, CREATE_ROLE, field_count);
	p = as_admin_write_field_string(p, ROLE, role);

	if (privileges_size > 0) {
		as_status status = as_admin_write_privileges(&p, err, privileges, privileges_size);

		if (status != AEROSPIKE_OK) {
			return status;
		}
	}

	if (whitelist_size > 0) {
		as_status status = as_admin_write_whitelist(&p, err, whitelist, whitelist_size);

		if (status != AEROSPIKE_OK) {
			return status;
		}
	}

	if (read_quota > 0) {
		p = as_admin_write_field_int(p, READ_QUOTA, read_quota);
	}

	if (write_quota > 0) {
		p = as_admin_write_field_int(p, WRITE_QUOTA, write_quota);
	}
	return as_admin_execute(as, err, policy, buffer, p);
}

as_status
aerospike_drop_role(aerospike* as, as_error* err, const as_policy_admin* policy, const char* role)
{
	as_error_reset(err);

	uint8_t buffer[AS_STACK_BUF_SIZE];
	uint8_t* p = buffer + 8;
	
	p = as_admin_write_header(p, DROP_ROLE, 1);
	p = as_admin_write_field_string(p, ROLE, role);
	return as_admin_execute(as, err, policy, buffer, p);
}

as_status
aerospike_grant_privileges(
	aerospike* as, as_error* err, const as_policy_admin* policy, const char* role,
	as_privilege** privileges, int privileges_size
	)
{
	as_error_reset(err);

	uint8_t buffer[AS_STACK_BUF_SIZE];
	uint8_t* p = buffer + 8;
	
	p = as_admin_write_header(p, GRANT_PRIVILEGES, 2);
	p = as_admin_write_field_string(p, ROLE, role);
	as_status status = as_admin_write_privileges(&p, err, privileges, privileges_size);
	
	if (status) {
		return status;
	}
	return as_admin_execute(as, err, policy, buffer, p);
}

as_status
aerospike_revoke_privileges(
	aerospike* as, as_error* err, const as_policy_admin* policy, const char* role,
	as_privilege** privileges, int privileges_size
	)
{
	as_error_reset(err);

	uint8_t buffer[AS_STACK_BUF_SIZE];
	uint8_t* p = buffer + 8;
	
	p = as_admin_write_header(p, REVOKE_PRIVILEGES, 2);
	p = as_admin_write_field_string(p, ROLE, role);
	as_status status = as_admin_write_privileges(&p, err, privileges, privileges_size);
	
	if (status) {
		return status;
	}
	return as_admin_execute(as, err, policy, buffer, p);
}

as_status
aerospike_set_whitelist(
	aerospike* as, as_error* err, const as_policy_admin* policy, const char* role,
	const char** whitelist, int whitelist_size
	)
{
	as_error_reset(err);

	uint8_t buffer[AS_STACK_BUF_SIZE];
	uint8_t* p = buffer + 8;
	int field_count = (whitelist_size > 0) ? 2 : 1;

	p = as_admin_write_header(p, SET_WHITELIST, field_count);
	p = as_admin_write_field_string(p, ROLE, role);

	if (whitelist_size > 0) {
		as_status status = as_admin_write_whitelist(&p, err, whitelist, whitelist_size);

		if (status != AEROSPIKE_OK) {
			return status;
		}
	}
	return as_admin_execute(as, err, policy, buffer, p);
}

as_status
aerospike_set_quotas(
	aerospike* as, as_error* err, const as_policy_admin* policy, const char* role,
	int read_quota, int write_quota
	)
{
	as_error_reset(err);

	uint8_t buffer[AS_STACK_BUF_SIZE];
	uint8_t* p = buffer + 8;

	p = as_admin_write_header(p, SET_QUOTAS, 3);
	p = as_admin_write_field_string(p, ROLE, role);
	p = as_admin_write_field_int(p, READ_QUOTA, read_quota);
	p = as_admin_write_field_int(p, WRITE_QUOTA, write_quota);

	return as_admin_execute(as, err, policy, buffer, p);
}

//---------------------------------
// Query Users
//---------------------------------

static uint8_t*
as_parse_users_roles(uint8_t* p, as_user** user_out)
{
	uint8_t size = *p++;
	as_user* user = cf_malloc(sizeof(as_user) + (size * AS_ROLE_SIZE));
	user->roles_size = size;
	
	char* role;
	uint8_t len;
	uint8_t sz;
	for (uint8_t i = 0; i < size; i++) {
		role = user->roles[i];
		len = *p++;
		sz = (len <= (AS_ROLE_SIZE-1))? len : (AS_ROLE_SIZE-1);
		memcpy(role, p, sz);
		role[sz] = 0;
		p += len;
	}
	*user_out = user;
	return p;
}

static uint8_t*
as_parse_info(uint8_t* p, int* info_size, uint32_t** info)
{
	uint8_t size = *p++;

	if (size == 0) {
		*info_size = 0;
		*info = NULL;
		return p;
	}

	uint32_t* array = cf_malloc(sizeof(uint32_t) * size);

	for (uint8_t i = 0; i < size; i++) {
		array[i] = cf_swap_from_be32(*(uint32_t*)p);
		p += sizeof(uint32_t);
	}
	*info_size = size;
	*info = array;
	return p;
}

static as_status
as_parse_users(as_error* err, uint8_t* buffer, size_t size, as_vector* /*<as_user*>*/ users)
{
	uint8_t* p = buffer;
	uint8_t* end = buffer + size;
	
	as_user* user;
	char user_name[AS_USER_SIZE];
	uint32_t* read_info;
	uint32_t* write_info;
	int read_info_size;
	int write_info_size;
	int conns_in_use;
	int len;
	int sz;
	uint8_t id;
	uint8_t field_count;
	uint8_t result;
	
	while (p < end) {
		result = p[1];
		
		if (result != 0) {
			return result;
		}
		
		field_count = p[3];
		p += HEADER_REMAINING;
		
		user_name[0] = 0;
		user = 0;
		conns_in_use = 0;
		read_info = NULL;
		read_info_size = 0;
		write_info = NULL;
		write_info_size = 0;

		for (uint8_t b = 0; b < field_count; b++) {
			len = cf_swap_from_be32(*(int*)p);
			p += 4;
			id = *p++;
			len--;

			switch (id) {
			case USER:
				sz = (len <= (AS_USER_SIZE-1))? len : (AS_USER_SIZE-1);
				memcpy(user_name, p, sz);
				user_name[sz] = 0;
				p += len;
				break;

			case ROLES:
				p = as_parse_users_roles(p, &user);
				break;

			case READ_INFO:
				p = as_parse_info(p, &read_info_size, &read_info);
				break;

			case WRITE_INFO:
				p = as_parse_info(p, &write_info_size, &write_info);
				break;

			case CONNECTIONS:
				conns_in_use = cf_swap_from_be32(*(int*)p);
				p += len;
				break;

			default:
				p += len;
				break;
			}
		}
		
		if (user_name[0] == 0 && user == 0) {
			continue;
		}
		
		if (! user) {
			user = cf_malloc(sizeof(as_user));
			user->roles_size = 0;
		}
		strcpy(user->name, user_name);
		user->read_info = read_info;
		user->read_info_size = read_info_size;
		user->write_info = write_info;
		user->write_info_size = write_info_size;
		user->conns_in_use = conns_in_use;
		as_vector_append(users, &user);
	}
	return 0;
}

static void
as_free_users(as_vector* users, int offset)
{
	for (uint32_t i = offset; i < users->size; i++) {
		as_user* user = as_vector_get_ptr(users, i);
		as_user_destroy(user);
	}
	as_vector_destroy(users);
}

as_status
aerospike_query_user(
	aerospike* as, as_error* err, const as_policy_admin* policy, const char* user_name,
	as_user** user
	)
{
	as_error_reset(err);

	if (! user_name) {
		user_name = as->cluster->user;
		
		if (! user_name) {
			return AEROSPIKE_INVALID_USER;
		}
	}
	uint8_t buffer[AS_STACK_BUF_SIZE];
	uint8_t* p = buffer + 8;
	
	p = as_admin_write_header(p, QUERY_USERS, 1);
	p = as_admin_write_field_string(p, USER, user_name);
	
	as_vector list;
	as_vector_inita(&list, sizeof(as_user*), 1);
	as_status status = as_admin_read_list(as, err, policy, buffer, p, as_parse_users, &list);
	
	if (status == AEROSPIKE_OK) {
		if (list.size == 1) {
			*user = as_vector_get_ptr(&list, 0);
		}
		else if (list.size <= 0) {
			*user = 0;
			as_free_users(&list, 0);
		}
		else {
			*user = as_vector_get_ptr(&list, 0);
			// Delete excess users.
			as_free_users(&list, 1);
		}
	}
	else {
		*user = 0;
		as_free_users(&list, 0);
	}
	return status;
}

void
as_user_destroy(as_user* user)
{
	if (user->read_info_size > 0) {
		cf_free(user->read_info);
	}
	if (user->write_info_size > 0) {
		cf_free(user->write_info);
	}
	cf_free(user);
}

as_status
aerospike_query_users(
	aerospike* as, as_error* err, const as_policy_admin* policy, as_user*** users, int* users_size
	)
{
	as_error_reset(err);

	uint8_t buffer[AS_STACK_BUF_SIZE];
	uint8_t* p = buffer + 8;
	
	p = as_admin_write_header(p, QUERY_USERS, 0);
	
	as_vector list;
	as_vector_init(&list, sizeof(as_user*), 100);
	as_status status = as_admin_read_list(as, err, policy, buffer, p, as_parse_users, &list);
	
	if (status == AEROSPIKE_OK) {
		// Transfer array to output argument. Do not destroy vector.
		*users_size = list.size;
		*users = list.list;
	}
	else {
		*users_size = 0;
		*users = 0;
		as_free_users(&list, 0);
	}
	return status;
}

void
as_users_destroy(as_user** users, int users_size)
{
	for (int i = 0; i < users_size; i++) {
		as_user_destroy(users[i]);
	}
	cf_free(users);
}

//---------------------------------
// Query Roles
//---------------------------------

static uint8_t*
as_privileges_parse(uint8_t* p, as_role** role_out)
{
	uint8_t size = *p++;
	as_role* role = cf_malloc(sizeof(as_role) + (sizeof(as_privilege) * size));
	role->privileges_size = size;
	
	as_privilege* priv;
	uint8_t len;
	uint8_t sz;
	
	for (uint8_t i = 0; i < size; i++) {
		priv = &role->privileges[i];
		priv->code = *p++;
		
		if (priv->code >= AS_PRIVILEGE_READ) {
			len = *p++;
			sz = (len <= (AS_NAMESPACE_MAX_SIZE-1))? len : (AS_NAMESPACE_MAX_SIZE-1);
			memcpy(priv->ns, p, sz);
			priv->ns[sz] = 0;
			p += len;
			
			len = *p++;
			sz = (len <= (AS_SET_MAX_SIZE-1))? len : (AS_SET_MAX_SIZE-1);
			memcpy(priv->set, p, sz);
			priv->set[sz] = 0;
			p += len;
		}
		else {
			priv->ns[0] = 0;
			priv->set[0] = 0;
		}
	}
	*role_out = role;
	return p;
}

static void
as_whitelist_append(as_vector* v, char* src_whitelist, int begin, int i)
{
	int len = i - begin;

	if (len > 0) {
		char* address = cf_malloc(len + 1);
		memcpy(address, &src_whitelist[begin], len);
		address[len] = 0;
		as_vector_append(v, &address);
	}
}

static void
as_whitelist_parse(char* src_whitelist, int src_len, char*** trg_whitelist, int* trg_size)
{
	int count = 1;

	for (int i = 0; i < src_len; i++) {
		char p = src_whitelist[i];

		if (p == ',') {
			count++;
		}
	}

	as_vector v;
	as_vector_init(&v, sizeof(char*), count);

	int begin = 0;

	for (int i = 0; i < src_len; i++) {
		char p = src_whitelist[i];

		if (p == ',') {
			as_whitelist_append(&v, src_whitelist, begin, i);
			begin = i + 1;
			continue;
		}
	}
	as_whitelist_append(&v, src_whitelist, begin, src_len);
	*trg_whitelist = v.list;
	*trg_size = v.size;
}

static void
as_whitelist_destroy(char** whitelist, int whitelist_size)
{
	for (int i = 0; i < whitelist_size; i++) {
		cf_free(whitelist[i]);
	}
	cf_free(whitelist);
}

static as_status
as_parse_roles(as_error* err, uint8_t* buffer, size_t size, as_vector* /*<as_role*>*/ roles)
{
	uint8_t* p = buffer;
	uint8_t* end = buffer + size;
	
	as_role* role;
	char role_name[AS_ROLE_SIZE];
	char** whitelist;
	int whitelist_size;
	int read_quota;
	int write_quota;
	int len;
	int sz;
	uint8_t id;
	uint8_t field_count;
	uint8_t result;
	
	while (p < end) {
		result = p[1];
		
		if (result != 0) {
			return result;
		}
		
		field_count = p[3];
		p += HEADER_REMAINING;
		
		role_name[0] = 0;
		role = NULL;
		whitelist = NULL;
		whitelist_size = 0;
		read_quota = 0;
		write_quota = 0;
		
		for (uint8_t b = 0; b < field_count; b++) {
			len = cf_swap_from_be32(*(int*)p);
			p += 4;
			id = *p++;
			len--;

			switch (id) {
			case ROLE:
				sz = (len <= (AS_ROLE_SIZE-1))? len : (AS_ROLE_SIZE-1);
				memcpy(role_name, p, sz);
				role_name[sz] = 0;
				p += len;
				break;

			case PRIVILEGES:
				p = as_privileges_parse(p, &role);
				break;

			case WHITELIST:
				as_whitelist_parse((char*)p, len, &whitelist, &whitelist_size);
				p += len;
				break;

			case READ_QUOTA:
				read_quota = cf_swap_from_be32(*(int*)p);
				p += len;
				break;

			case WRITE_QUOTA:
				write_quota = cf_swap_from_be32(*(int*)p);
				p += len;
				break;

			default:
				p += len;
				break;
			}
		}

		if (! role) {
			role = cf_malloc(sizeof(as_role));
			role->privileges_size = 0;
		}
		strcpy(role->name, role_name);
		role->whitelist = whitelist;
		role->whitelist_size = whitelist_size;
		role->read_quota = read_quota;
		role->write_quota = write_quota;
		as_vector_append(roles, &role);
	}
	return AEROSPIKE_OK;
}

static void
as_free_roles(as_vector* roles, int offset)
{
	for (uint32_t i = offset; i < roles->size; i++) {
		as_role* role = as_vector_get_ptr(roles, i);
		as_role_destroy(role);
	}
	as_vector_destroy(roles);
}

as_status
aerospike_query_role(
	aerospike* as, as_error* err, const as_policy_admin* policy, const char* role_name,
	as_role** role
	)
{
	uint8_t buffer[AS_STACK_BUF_SIZE];
	uint8_t* p = buffer + 8;
	
	p = as_admin_write_header(p, QUERY_ROLES, 1);
	p = as_admin_write_field_string(p, ROLE, role_name);
	
	as_vector list;
	as_vector_inita(&list, sizeof(as_role*), 1);
	as_status status = as_admin_read_list(as, err, policy, buffer, p, as_parse_roles, &list);
	
	if (status == AEROSPIKE_OK) {
		if (list.size == 1) {
			*role = as_vector_get_ptr(&list, 0);
		}
		else if (list.size <= 0) {
			*role = 0;
			as_free_roles(&list, 0);
		}
		else {
			*role = as_vector_get_ptr(&list, 0);
			// Delete excess users.
			as_free_roles(&list, 1);
		}
	}
	else {
		*role = 0;
		as_free_roles(&list, 0);
	}
	return status;
}

void
as_role_destroy(as_role* role)
{
	as_whitelist_destroy(role->whitelist, role->whitelist_size);
	cf_free(role);
}

as_status
aerospike_query_roles(
	aerospike* as, as_error* err, const as_policy_admin* policy, as_role*** roles, int* roles_size
	)
{
	uint8_t buffer[AS_STACK_BUF_SIZE];
	uint8_t* p = buffer + 8;
	
	p = as_admin_write_header(p, QUERY_ROLES, 0);
	
	as_vector list;
	as_vector_init(&list, sizeof(as_role*), 100);
	as_status status = as_admin_read_list(as, err, policy, buffer, p, as_parse_roles, &list);
	
	if (status == AEROSPIKE_OK) {
		// Transfer array to output argument. Do not destroy vector.
		*roles_size = list.size;
		*roles = list.list;
	}
	else {
		*roles_size = 0;
		*roles = 0;
		as_free_roles(&list, 0);
	}
	return status;
}

void
as_roles_destroy(as_role** roles, int roles_size)
{
	for (int i = 0; i < roles_size; i++) {
		as_role_destroy(roles[i]);
	}
	cf_free(roles);
}
