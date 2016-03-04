/*
 * Copyright 2008-2016 Aerospike, Inc.
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
#include <aerospike/as_proto.h>
#include <aerospike/as_socket.h>
#include <citrusleaf/cf_byte_order.h>
#include <citrusleaf/cf_clock.h>
#include <string.h>

/******************************************************************************
 *	TYPES
 *****************************************************************************/

typedef as_status (*as_admin_parse_fn) (as_error* err, uint8_t* buffer, size_t size, as_vector* list);

/******************************************************************************
 *	MACROS
 *****************************************************************************/

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
#define QUERY_ROLES 16

// Field IDs
#define USER 0
#define PASSWORD 1
#define OLD_PASSWORD 2
#define CREDENTIAL 3
#define ROLES 10
#define ROLE 11
#define PRIVILEGES 12

// Misc
#define MSG_VERSION 0L
#define MSG_TYPE 2L
#define FIELD_HEADER_SIZE 5
#define HEADER_SIZE 24
#define HEADER_REMAINING 16
#define RESULT_CODE 9
#define DEFAULT_TIMEOUT 60000  // one minute

/******************************************************************************
 *	STATIC FUNCTIONS
 *****************************************************************************/

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
				return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "Admin privilege has namespace/set scope which is invalid.");
			}
		}
	}
	as_admin_write_field_header(*p, PRIVILEGES, (int)(q - *p - FIELD_HEADER_SIZE));
	*p = q;
	return AEROSPIKE_OK;
}

static as_status
as_admin_send(as_error* err, int fd, uint8_t* buffer, uint8_t* end, uint64_t deadline_ms)
{
	uint64_t len = end - buffer;
	uint64_t proto = (len - 8) | (MSG_VERSION << 56) | (MSG_TYPE << 48);
	*(uint64_t*)buffer = cf_swap_to_be64(proto);
	
	return as_socket_write_deadline(err, fd, buffer, len, deadline_ms);
}

static as_status
as_admin_execute(aerospike* as, as_error* err, const as_policy_admin* policy, uint8_t* buffer, uint8_t* end)
{
	uint32_t timeout_ms = (policy)? policy->timeout : as->config.policies.admin.timeout;
	if (timeout_ms <= 0) {
		timeout_ms = DEFAULT_TIMEOUT;
	}
	uint64_t deadline_ms = as_socket_deadline(timeout_ms);
	as_cluster* cluster = as->cluster;
	as_node* node = as_node_get_random(cluster);
	
	if (! node) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to find server node.");
	}
	
	int fd;
	as_status status = as_node_get_connection(err, node, deadline_ms, &fd);
	
	if (status) {
		as_node_release(node);
		return status;
	}

	status = as_admin_send(err, fd, buffer, end, deadline_ms);
	
	if (status) {
		as_node_close_connection(node, fd);
		as_node_release(node);
		return status;
	}
	
	status = as_socket_read_deadline(err, fd, buffer, HEADER_SIZE, deadline_ms);
	
	if (status) {
		as_node_close_connection(node, fd);
		as_node_release(node);
		return status;
	}
	
	as_node_put_connection(node, fd);
	as_node_release(node);
	
	status = buffer[RESULT_CODE];
	
	if (status) {
		return as_error_set_message(err, status, as_error_string(status));
	}
	return status;
}

static as_status
as_admin_read_blocks(as_error* err, int fd, uint64_t deadline_ms, as_admin_parse_fn parse_fn, as_vector* list)
{
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
	as_command_free(buf, capacity);
	return status;
}

static as_status
as_admin_read_list(aerospike* as, as_error* err, const as_policy_admin* policy, uint8_t* command, uint8_t* end, as_admin_parse_fn parse_fn, as_vector* list)
{
	int timeout_ms = (policy)? policy->timeout : as->config.policies.admin.timeout;
	if (timeout_ms <= 0) {
		timeout_ms = DEFAULT_TIMEOUT;
	}
	uint64_t deadline_ms = as_socket_deadline(timeout_ms);
	as_cluster* cluster = as->cluster;
	as_node* node = as_node_get_random(cluster);
	
	if (! node) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to find server node.");
	}
	
	int fd;
	as_status status = as_node_get_connection(err, node, deadline_ms, &fd);
	
	if (status) {
		as_node_release(node);
		return status;
	}
	
	status = as_admin_send(err, fd, command, end, deadline_ms);
	
	if (status) {
		as_node_close_connection(node, fd);
		as_node_release(node);
		return status;
	}
	
	status = as_admin_read_blocks(err, fd, deadline_ms, parse_fn, list);
	
	if (status) {
		as_node_close_connection(node, fd);
		as_node_release(node);
		return status;
	}

	as_node_put_connection(node, fd);
	as_node_release(node);
	return status;
}

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

uint32_t
as_authenticate_set(const char* user, const char* credential, uint8_t* buffer)
{
	uint8_t* p = buffer + 8;
	
	p = as_admin_write_header(p, AUTHENTICATE, 2);
	p = as_admin_write_field_string(p, USER, user);
	p = as_admin_write_field_string(p, CREDENTIAL, credential);

	uint64_t len = p - buffer;
	uint64_t proto = (len - 8) | (MSG_VERSION << 56) | (MSG_TYPE << 48);
	*(uint64_t*)buffer = cf_swap_to_be64(proto);
	return (uint32_t)len;
}

as_status
as_authenticate(as_error* err, int fd, const char* user, const char* credential, uint64_t deadline_ms)
{
	uint8_t buffer[AS_STACK_BUF_SIZE];
	uint8_t* p = buffer + 8;

	p = as_admin_write_header(p, AUTHENTICATE, 2);
	p = as_admin_write_field_string(p, USER, user);
	p = as_admin_write_field_string(p, CREDENTIAL, credential);
	
	as_status status = as_admin_send(err, fd, buffer, p, deadline_ms);
	
	if (status) {
		return status;
	}

	status = as_socket_read_deadline(err, fd, buffer, HEADER_SIZE, deadline_ms);
	
	if (status) {
		return status;
	}
	
	status = buffer[RESULT_CODE];
	
	if (status) {
		as_error_set_message(err, status, as_error_string(status));
	}
	return status;
}

as_status
aerospike_create_user(aerospike* as, as_error* err, const as_policy_admin* policy, const char* user, const char* password, const char** roles, int roles_size)
{
	as_error_reset(err);
	
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
aerospike_set_password(aerospike* as, as_error* err, const as_policy_admin* policy, const char* user, const char* password)
{
	as_error_reset(err);

	char hash[AS_PASSWORD_HASH_SIZE];
	as_password_get_constant_hash(password, hash);
	
	if (! user) {
		user = as->cluster->user;
	}
	uint8_t buffer[AS_STACK_BUF_SIZE];
	uint8_t* p = buffer + 8;
	
	p = as_admin_write_header(p, SET_PASSWORD, 2);
	p = as_admin_write_field_string(p, USER, user);
	p = as_admin_write_field_string(p, PASSWORD, hash);
	int status = as_admin_execute(as, err, policy, buffer, p);
	
	if (status == 0) {
		as_cluster_change_password(as->cluster, user, hash);
	}
	return status;
}

as_status
aerospike_change_password(aerospike* as, as_error* err, const as_policy_admin* policy, const char* user, const char* password)
{
	as_error_reset(err);

	char hash[AS_PASSWORD_HASH_SIZE];
	as_password_get_constant_hash(password, hash);
	
	if (! user) {
		user = as->cluster->user;
	}
	uint8_t buffer[AS_STACK_BUF_SIZE];
	uint8_t* p = buffer + 8;
	
	p = as_admin_write_header(p, CHANGE_PASSWORD, 3);
	p = as_admin_write_field_string(p, USER, user);
	p = as_admin_write_field_string(p, OLD_PASSWORD, as->cluster->password);
	p = as_admin_write_field_string(p, PASSWORD, hash);
	int status = as_admin_execute(as, err, policy, buffer, p);
	
	if (status == 0) {
		as_cluster_change_password(as->cluster, user, hash);
	}
	return status;
}

as_status
aerospike_grant_roles(aerospike* as, as_error* err, const as_policy_admin* policy, const char* user, const char** roles, int roles_size)
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
aerospike_revoke_roles(aerospike* as, as_error* err, const as_policy_admin* policy, const char* user, const char** roles, int roles_size)
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
aerospike_create_role(aerospike* as, as_error* err, const as_policy_admin* policy, const char* role, as_privilege** privileges, int privileges_size)
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
aerospike_grant_privileges(aerospike* as, as_error* err, const as_policy_admin* policy, const char* role, as_privilege** privileges, int privileges_size)
{
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
aerospike_revoke_privileges(aerospike* as, as_error* err, const as_policy_admin* policy, const char* role, as_privilege** privileges, int privileges_size)
{
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

/******************************************************************************
 *	QUERY USERS
 *****************************************************************************/

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

static as_status
as_parse_users(as_error* err, uint8_t* buffer, size_t size, as_vector* /*<as_user*>*/ users)
{
	uint8_t* p = buffer;
	uint8_t* end = buffer + size;
	
	as_user* user;
	char user_name[AS_USER_SIZE];
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
		
		for (uint8_t b = 0; b < field_count; b++) {
			len = cf_swap_from_be32(*(int*)p);
			p += 4;
			id = *p++;
			len--;
			
			if (id == USER) {
				sz = (len <= (AS_USER_SIZE-1))? len : (AS_USER_SIZE-1);
				memcpy(user_name, p, sz);
				user_name[sz] = 0;
				p += len;
			}
			else if (id == ROLES) {
				p = as_parse_users_roles(p, &user);
			}
			else {
				p += len;
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
		as_vector_append(users, &user);
	}
	return 0;
}

static void
as_free_users(as_vector* users, int offset)
{
	for (uint32_t i = offset; i < users->size; i++) {
		as_user* user = as_vector_get_ptr(users, i);
		cf_free(user);
	}
	as_vector_destroy(users);
}

as_status
aerospike_query_user(aerospike* as, as_error* err, const as_policy_admin* policy, const char* user_name, as_user** user)
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
	cf_free(user);
}

as_status
aerospike_query_users(aerospike* as, as_error* err, const as_policy_admin* policy, as_user*** users, int* users_size)
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
		cf_free(users[i]);
	}
	cf_free(users);
}

/******************************************************************************
 *	QUERY ROLES
 *****************************************************************************/

static uint8_t*
as_parse_privileges(uint8_t* p, as_role** role_out)
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

static as_status
as_parse_roles(as_error* err, uint8_t* buffer, size_t size, as_vector* /*<as_role*>*/ roles)
{
	uint8_t* p = buffer;
	uint8_t* end = buffer + size;
	
	as_role* role;
	char role_name[AS_ROLE_SIZE];
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
		role = 0;
		
		for (uint8_t b = 0; b < field_count; b++) {
			len = cf_swap_from_be32(*(int*)p);
			p += 4;
			id = *p++;
			len--;
			
			if (id == ROLE) {
				sz = (len <= (AS_ROLE_SIZE-1))? len : (AS_ROLE_SIZE-1);
				memcpy(role_name, p, sz);
				role_name[sz] = 0;
				p += len;
			}
			else if (id == PRIVILEGES) {
				p = as_parse_privileges(p, &role);
			}
			else {
				p += len;
			}
		}
		
		if (role_name[0] == 0 && role == 0) {
			continue;
		}
		
		if (! role) {
			role = cf_malloc(sizeof(as_role));
			role->privileges_size = 0;
		}
		strcpy(role->name, role_name);
		as_vector_append(roles, &role);
	}
	return AEROSPIKE_OK;
}

static void
as_free_roles(as_vector* roles, int offset)
{
	for (uint32_t i = offset; i < roles->size; i++) {
		as_role* role = as_vector_get_ptr(roles, i);
		cf_free(role);
	}
	as_vector_destroy(roles);
}

as_status
aerospike_query_role(aerospike* as, as_error* err, const as_policy_admin* policy, const char* role_name, as_role** role)
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
	cf_free(role);
}

as_status
aerospike_query_roles(aerospike* as, as_error* err, const as_policy_admin* policy, as_role*** roles, int* roles_size)
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
	for (uint32_t i = 0; i < roles_size; i++) {
		cf_free(roles[i]);
	}
	cf_free(roles);
}
