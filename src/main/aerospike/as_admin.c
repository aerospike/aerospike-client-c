/******************************************************************************
 * Copyright 2008-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to 
 * deal in the Software without restriction, including without limitation the 
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 * sell copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *****************************************************************************/
#include <aerospike/as_admin.h>
#include <aerospike/as_cluster.h>
#include <citrusleaf/cf_byte_order.h>
#include <citrusleaf/cf_proto.h>
#include <citrusleaf/cf_socket.h>
#include <citrusleaf/cl_types.h>
#include <string.h>

// Commands
#define AUTHENTICATE 0
#define CREATE_USER 1
#define DROP_USER 2
#define SET_PASSWORD 3
#define CHANGE_PASSWORD 4
#define GRANT_ROLES 5
#define REVOKE_ROLES 6
#define REPLACE_ROLES 7
#define CREATE_ROLE 8
#define QUERY_USERS 9
#define QUERY_ROLES 10

// Field IDs
#define USER 0
#define PASSWORD 1
#define OLD_PASSWORD 2
#define CREDENTIAL 3
#define ROLES 10
#define PRIVILEGES 11

// Misc
#define MSG_VERSION 0L
#define MSG_TYPE 2L
#define FIELD_HEADER_SIZE 5
#define HEADER_SIZE 24
#define HEADER_REMAINING 16
#define RESULT_CODE 9
#define QUERY_END 50
#define DEFAULT_TIMEOUT 60000  // one minute

static uint8_t*
write_header(uint8_t* p, uint8_t command, uint8_t field_count)
{
	memset(p, 0, HEADER_REMAINING);
	p[2] = command;
	p[3] = field_count;
	return p + HEADER_REMAINING;;
}

static uint8_t*
write_field_header(uint8_t* p, uint8_t id, int size)
{
	*(int*)p = cf_swap_to_be32(size+1);
	p += 4;
	*p++ = id;
	return p;
}

static uint8_t*
write_field_string(uint8_t* p, uint8_t id, const char* val)
{
	uint8_t* q = (uint8_t*)stpcpy((char*)p + FIELD_HEADER_SIZE, val);
	write_field_header(p, id, (int)(q - p - FIELD_HEADER_SIZE));
	return q;
}

static uint8_t*
write_roles(uint8_t* p, const char** roles, int length)
{
	uint8_t* q = p + FIELD_HEADER_SIZE;
	*q++ = (uint8_t)length;
	
	uint8_t* r;
	for (uint32_t i = 0; i < length; i++) {
		r = (uint8_t*)stpcpy((char*)q + 1, roles[i]);
		*q = (uint8_t)(r - q - 1);
		q = r;
	}
	
	write_field_header(p, ROLES, (int)(q - p - FIELD_HEADER_SIZE));
	return q;
}

static int
as_send(int fd, uint8_t* buffer, uint8_t* end, uint64_t deadline_ms, int timeout_ms)
{
	uint64_t len = end - buffer;
	uint64_t proto = (len - 8) | (MSG_VERSION << 56) | (MSG_TYPE << 48);
	*(uint64_t*)buffer = cf_swap_to_be64(proto);
	
	return cf_socket_write_timeout(fd, buffer, len, deadline_ms, timeout_ms);
}

static int
as_execute(aerospike* as, const as_policy_admin* policy, uint8_t* buffer, uint8_t* end)
{
	int timeout_ms = (policy)? policy->timeout : as->config.policies.admin.timeout;
	if (timeout_ms <= 0) {
		timeout_ms = DEFAULT_TIMEOUT;
	}
	uint64_t deadline_ms = cf_getms() + timeout_ms;
	as_node* node = as_node_get_random(as->cluster);
	
	if (! node) {
		return CITRUSLEAF_FAIL_CLIENT;
	}
	
	int fd;
	int status = as_node_get_connection(node, &fd);
	
	if (status) {
		as_node_release(node);
		return status;
	}

	if (as_send(fd, buffer, end, deadline_ms, timeout_ms)) {
		cf_close(fd);
		as_node_release(node);
		return CITRUSLEAF_FAIL_TIMEOUT;
	}
	
	if (cf_socket_read_timeout(fd, buffer, HEADER_SIZE, deadline_ms, timeout_ms)) {
		cf_close(fd);
		as_node_release(node);
		return CITRUSLEAF_FAIL_TIMEOUT;
	}
	
	as_node_put_connection(node, fd);
	as_node_release(node);
	return buffer[RESULT_CODE];
}

int
as_authenticate(int fd, const char* user, const char* credential, int timeout_ms)
{
	uint8_t buffer[STACK_BUF_SZ];
	uint8_t* p = buffer + 8;

	p = write_header(p, AUTHENTICATE, 2);
	p = write_field_string(p, USER, user);
	p = write_field_string(p, CREDENTIAL, credential);
	
	if (timeout_ms == 0) {
		timeout_ms = DEFAULT_TIMEOUT;
	}
	uint64_t deadline_ms = cf_getms() + timeout_ms;
	
	if (as_send(fd, buffer, p, deadline_ms, timeout_ms)) {
		return CITRUSLEAF_FAIL_TIMEOUT;
	}

	if (cf_socket_read_timeout(fd, buffer, HEADER_SIZE, deadline_ms, timeout_ms)) {
		return CITRUSLEAF_FAIL_TIMEOUT;
	}
	return buffer[RESULT_CODE];
}

int
aerospike_create_user(aerospike* as, const as_policy_admin* policy, const char* user, const char* password, const char** roles, int roles_size)
{
	char hash[AS_PASSWORD_HASH_SIZE];
	as_password_get_constant_hash(password, hash);
	
	uint8_t buffer[STACK_BUF_SZ];
	uint8_t* p = buffer + 8;
	
	p = write_header(p, CREATE_USER, 3);
	p = write_field_string(p, USER, user);
	p = write_field_string(p, PASSWORD, hash);
	p = write_roles(p, roles, roles_size);
	return as_execute(as, policy, buffer, p);
}

int
aerospike_drop_user(aerospike* as, const as_policy_admin* policy, const char* user)
{
	uint8_t buffer[STACK_BUF_SZ];
	uint8_t* p = buffer + 8;
	
	p = write_header(p, DROP_USER, 1);
	p = write_field_string(p, USER, user);
	return as_execute(as, policy, buffer, p);
}

int
aerospike_set_password(aerospike* as, const as_policy_admin* policy, const char* user, const char* password)
{
	char hash[AS_PASSWORD_HASH_SIZE];
	as_password_get_constant_hash(password, hash);
	
	if (! user) {
		user = as->cluster->user;
	}
	uint8_t buffer[STACK_BUF_SZ];
	uint8_t* p = buffer + 8;
	
	p = write_header(p, SET_PASSWORD, 2);
	p = write_field_string(p, USER, user);
	p = write_field_string(p, PASSWORD, hash);
	int status = as_execute(as, policy, buffer, p);
	
	if (status == 0) {
		as_cluster_change_password(as->cluster, user, hash);
	}
	return status;
}

int
aerospike_change_password(aerospike* as, const as_policy_admin* policy, const char* user, const char* password)
{
	char hash[AS_PASSWORD_HASH_SIZE];
	as_password_get_constant_hash(password, hash);
	
	if (! user) {
		user = as->cluster->user;
	}
	uint8_t buffer[STACK_BUF_SZ];
	uint8_t* p = buffer + 8;
	
	p = write_header(p, CHANGE_PASSWORD, 3);
	p = write_field_string(p, USER, user);
	p = write_field_string(p, OLD_PASSWORD, as->cluster->password);
	p = write_field_string(p, PASSWORD, hash);
	int status = as_execute(as, policy, buffer, p);
	
	if (status == 0) {
		as_cluster_change_password(as->cluster, user, hash);
	}
	return status;
}

int
aerospike_grant_roles(aerospike* as, const as_policy_admin* policy, const char* user, const char** roles, int roles_size)
{
	uint8_t buffer[STACK_BUF_SZ];
	uint8_t* p = buffer + 8;
	
	p = write_header(p, GRANT_ROLES, 2);
	p = write_field_string(p, USER, user);
	p = write_roles(p, roles, roles_size);
	return as_execute(as, policy, buffer, p);
}

int
aerospike_revoke_roles(aerospike* as, const as_policy_admin* policy, const char* user, const char** roles, int roles_size)
{
	uint8_t buffer[STACK_BUF_SZ];
	uint8_t* p = buffer + 8;
	
	p = write_header(p, REVOKE_ROLES, 2);
	p = write_field_string(p, USER, user);
	p = write_roles(p, roles, roles_size);
	return as_execute(as, policy, buffer, p);
}

int
aerospike_replace_roles(aerospike* as, const as_policy_admin* policy, const char* user, const char** roles, int roles_size)
{
	uint8_t buffer[STACK_BUF_SZ];
	uint8_t* p = buffer + 8;
	
	p = write_header(p, REPLACE_ROLES, 2);
	p = write_field_string(p, USER, user);
	p = write_roles(p, roles, roles_size);
	return as_execute(as, policy, buffer, p);
}

static uint8_t*
as_parse_roles(uint8_t* p, as_user_roles** user_roles)
{
	uint8_t size = *p++;
	as_user_roles* user = cf_malloc(sizeof(as_user_roles) + (size * AS_ROLE_SIZE));
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
	*user_roles = user;
	return p;
}

static int
as_parse_users(uint8_t* buffer, int size, as_vector* /*<as_user_roles*>*/ users)
{
	uint8_t* p = buffer;
	uint8_t* end = buffer + size;
	
	as_user_roles* user_roles;
	char user[AS_USER_SIZE];
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
		
		user[0] = 0;
		user_roles = 0;
		
		for (uint8_t b = 0; b < field_count; b++) {
			len = cf_swap_from_be32(*(int*)p);
			p += 4;
			id = *p++;
			len--;
			
			if (id == USER) {
				sz = (len <= (AS_USER_SIZE-1))? len : (AS_USER_SIZE-1);
				memcpy(user, p, sz);
				user[sz] = 0;
				p += len;
			}
			else if (id == ROLES) {
				p = as_parse_roles(p, &user_roles);
			}
			else {
				p += len;
			}
		}
		
		if (user[0] == 0 && user_roles == 0) {
			continue;
		}
		
		if (! user_roles) {
			user_roles = cf_malloc(sizeof(as_user_roles));
			user_roles->roles_size = 0;
		}
		strcpy(user_roles->user, user);
		as_vector_append(users, &user_roles);
	}
	return 0;
}

static int
as_read_user_blocks(int fd, uint8_t* buffer, uint64_t deadline_ms, int timeout_ms, as_vector* /*<as_user_roles*>*/ users)
{
	int buffer_size = STACK_BUF_SZ;
	uint8_t* buf = buffer;
	uint64_t proto;
	int status = 0;
	int size;
	
	while (status == 0) {
		if (cf_socket_read_timeout(fd, buf, 8, deadline_ms, timeout_ms)) {
			status = -1;
			break;
		}
		proto = cf_swap_from_be64(*(uint64_t*)buf);
		size = (int)(proto & 0xFFFFFFFFFFFFL);
		
		if (size > 0) {
			if (size > buffer_size) {
				buffer_size = size;
				
				if (buf != buffer) {
					cf_free(buf);
				}
				buf = cf_malloc(size);
			}
			
			if (cf_socket_read_timeout(fd, buf, size, deadline_ms, timeout_ms)) {
				status = -1;
				break;
			}
			
			status = as_parse_users(buf, size, users);
		}
		else {
			break;
		}
	}
	
	if (buf != buffer) {
		cf_free(buf);
	}
	return (status == QUERY_END)? 0 : status;
}

static int
as_read_users(aerospike* as, const as_policy_admin* policy, uint8_t* buffer, uint8_t* end, as_vector* /*<as_user_roles*>*/ users)
{
	int timeout_ms = (policy)? policy->timeout : as->config.policies.admin.timeout;
	if (timeout_ms <= 0) {
		timeout_ms = DEFAULT_TIMEOUT;
	}
	uint64_t deadline_ms = cf_getms() + timeout_ms;
	as_node* node = as_node_get_random(as->cluster);
	
	if (! node) {
		return CITRUSLEAF_FAIL_CLIENT;
	}
	
	int fd;
	int status = as_node_get_connection(node, &fd);
	
	if (status) {
		as_node_release(node);
		return status;
	}
	
	if (as_send(fd, buffer, end, deadline_ms, timeout_ms)) {
		cf_close(fd);
		as_node_release(node);
		return CITRUSLEAF_FAIL_TIMEOUT;
	}
	
	status = as_read_user_blocks(fd, buffer, deadline_ms, timeout_ms, users);
	
	if (status >= 0) {
		as_node_put_connection(node, fd);
	}
	else {
		cf_close(fd);
	}
	as_node_release(node);
	return status;
}

static void
as_free_users(as_vector* users, int offset)
{
	for (uint32_t i = offset; i < users->size; i++) {
		as_user_roles* item = as_vector_get(users, i);
		cf_free(item);
	}
	as_vector_destroy(users);
}

#if 0
static void print_users(as_vector* /*<as_user_roles*>*/ users)
{
	for (uint32_t i = 0; i < users->size; i++) {
		as_user_roles* urs = as_vector_get_ptr(users, i);
		printf("User %s Roles ", urs->user);
		
		for (uint32_t j = 0; j < urs->roles_size; j++) {
			printf("%s", urs->roles[j]);
		}
		printf("\n");
	}
}

static void print_users2(as_user_roles** users, int size)
{
	for (uint32_t i = 0; i < size; i++) {
		as_user_roles* urs = users[i];
		printf("User %s Roles ", urs->user);
		
		for (uint32_t j = 0; j < urs->roles_size; j++) {
			printf("%s", urs->roles[j]);
		}
		printf("\n");
	}
}
#endif

int
aerospike_query_user(aerospike* as, const as_policy_admin* policy, const char* user, as_user_roles** user_roles)
{
	if (! user) {
		user = as->cluster->user;
	}
	uint8_t buffer[STACK_BUF_SZ];
	uint8_t* p = buffer + 8;
	
	p = write_header(p, QUERY_USERS, 1);
	p = write_field_string(p, USER, user);
	
	as_vector users;
	as_vector_inita(&users, sizeof(as_user_roles*), 1);
	int status = as_read_users(as, policy, buffer, p, &users);
	
	if (status == 0) {
		if (users.size == 1) {
			*user_roles = as_vector_get_ptr(&users, 0);
		}
		else if (users.size <= 0) {
			*user_roles = 0;
			as_free_users(&users, 0);
		}
		else {
			*user_roles = as_vector_get_ptr(&users, 0);
			// Delete excess users.
			as_free_users(&users, 1);
		}
	}
	else {
		*user_roles = 0;
		as_free_users(&users, 0);
	}
	return status;
}

void
as_user_roles_destroy(as_user_roles* user_roles)
{
	cf_free(user_roles);
}

int
aerospike_query_users(aerospike* as, const as_policy_admin* policy, as_user_roles*** user_roles, int* user_roles_size)
{
	uint8_t buffer[STACK_BUF_SZ];
	uint8_t* p = buffer + 8;
	
	p = write_header(p, QUERY_USERS, 0);
	
	as_vector users;
	as_vector_init(&users, sizeof(as_user_roles*), 100);
	int status = as_read_users(as, policy, buffer, p, &users);
	
	if (status == 0) {
		// Transfer array to output argument. Do not destroy vector.
		*user_roles_size = users.size;
		*user_roles = users.list;
	}
	else {
		*user_roles_size = 0;
		*user_roles = 0;
		as_free_users(&users, 0);
	}
	return status;
}

void
as_user_roles_destroy_array(as_user_roles** user_roles, int user_roles_size)
{
	for (uint32_t i = 0; i < user_roles_size; i++) {
		cf_free(user_roles[i]);
	}
	cf_free(user_roles);
}
