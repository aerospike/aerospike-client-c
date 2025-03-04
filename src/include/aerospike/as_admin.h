/*
 * Copyright 2008-2024 Aerospike, Inc.
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
#pragma once 

/**
 * @defgroup admin_operations Admin Operations
 * @ingroup client_operations
 *
 * User administration operations.
 */

#include <aerospike/aerospike.h>
#include <aerospike/as_config.h>
#include <aerospike/as_key.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * MACROS
 *****************************************************************************/

/**
 * Maximum size of role string including null byte.
 * @ingroup admin_operations
 */
#define AS_ROLE_SIZE 64

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * Permission codes define the type of permission granted for a user's role.
 * @ingroup admin_operations
 */
typedef enum as_privilege_code_e {
	/**
	 * User can edit/remove other users.  Global scope only.
	 */
	AS_PRIVILEGE_USER_ADMIN = 0,
	
	/**
	 * User can perform systems administration functions on a database that do not involve user
	 * administration.  Examples include setting dynamic server configuration.
	 * Global scope only.
	 */
	AS_PRIVILEGE_SYS_ADMIN = 1,
	
	/**
	 * User can perform UDF and SINDEX administration actions. Global scope only.
	 */
	AS_PRIVILEGE_DATA_ADMIN = 2,

	/**
	 * User can perform user defined function(UDF) administration actions.
	 * Examples include create/drop UDF. Global scope only.
	 * Requires server version 6.0+
	 */
	AS_PRIVILEGE_UDF_ADMIN = 3,

	/**
	 * User can perform secondary index administration actions.
	 * Examples include create/drop index. Global scope only.
	 * Requires server version 6.0+
	 */
	AS_PRIVILEGE_SINDEX_ADMIN = 4,

	/**
	 * User can read data only.
	 */
	AS_PRIVILEGE_READ = 10,
	
	/**
	 * User can read and write data.
	 */
	AS_PRIVILEGE_READ_WRITE = 11,
	
	/**
	 * User can read and write data through user defined functions.
	 */
	AS_PRIVILEGE_READ_WRITE_UDF = 12,

	/**
	 * User can write data only.
	 */
	AS_PRIVILEGE_WRITE = 13,

	/**
	 * User can truncate data only.
	 * Requires server version 6.0+
	 */
	AS_PRIVILEGE_TRUNCATE = 14
} as_privilege_code;

/**
 * User privilege.
 * @ingroup admin_operations
 */
typedef struct as_privilege_s {
	/**
	 * Namespace scope.  Apply permission to this null terminated namespace only.
	 * If string length is zero, the privilege applies to all namespaces.
	 */
	as_namespace ns;
	
	/**
	 * Set name scope.  Apply permission to this null terminated set within namespace only.
	 * If string length is zero, the privilege applies to all sets within namespace.
	 */
	as_set set;
	
	/**
	 * Privilege code.
	 */
	as_privilege_code code;
} as_privilege;

/**
 * Role definition.
 * @ingroup admin_operations
 */
typedef struct as_role_s {
	/**
	 * Role name.
	 */
	char name[AS_ROLE_SIZE];

	/**
	 * Maximum reads per second limit.
	 */
	int read_quota;

	/**
	 * Maximum writes per second limit.
	 */
	int write_quota;

	/**
	 * Array of allowable IP address strings.
	 */
	char** whitelist;

	/**
	 * Length of whitelist array.
	 */
	int whitelist_size;

	/**
	 * Length of privileges array.
	 */
	int privileges_size;
	
	/**
	 * Array of assigned privileges.
	 */
	as_privilege privileges[];
} as_role;

/**
 * User and assigned roles.
 * @ingroup admin_operations
 */
typedef struct as_user_s {
	/**
	 * User name.
	 */
	char name[AS_USER_SIZE];

	/**
	 * Array of read statistics. Array may be null.
	 * Current statistics by offset are:
	 * <ul>
	 * <li>0: read quota in records per second</li>
	 * <li>1: single record read command rate (TPS)</li>
	 * <li>2: read scan/query record per second rate (RPS)</li>
	 * <li>3: number of limitless read scans/queries</li>
	 * </ul>
	 * Future server releases may add additional statistics.
	 */
	uint32_t* read_info;

	/**
	 * Array of write statistics. Array may be null.
	 * Current statistics by offset are:
	 * <ul>
	 * <li>0: write quota in records per second</li>
	 * <li>1: single record write command rate (TPS)</li>
	 * <li>2: write scan/query record per second rate (RPS)</li>
	 * <li>3: number of limitless write scans/queries</li>
	 * </ul>
	 * Future server releases may add additional statistics.
	 */
	uint32_t* write_info;

	/**
	 * Length of read info array.
	 */
	int read_info_size;

	/**
	 * Length of write info array.
	 */
	int write_info_size;

	/**
	 * Number of currently open connections.
	 */
	int conns_in_use;

	/**
	 * Length of roles array.
	 */
	int roles_size;

	/**
	 * Array of assigned role names.
	 */
	char roles[][AS_ROLE_SIZE];
} as_user;

struct as_node_s;
struct as_socket_s;

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

/**
 * Create user with password and roles.  Clear-text password will be hashed using bcrypt before 
 * sending to server.
 * @ingroup admin_operations
 */
AS_EXTERN as_status
aerospike_create_user(
	aerospike* as, as_error* err, const as_policy_admin* policy, const char* user_name,
	const char* password, const char** roles, int roles_size
	);

/**
 * Remove user from cluster.
 * @ingroup admin_operations
 */
AS_EXTERN as_status
aerospike_drop_user(
	aerospike* as, as_error* err, const as_policy_admin* policy, const char* user_name
	);

/**
 * Set user's password by user administrator.  Clear-text password will be hashed using bcrypt
 * before sending to server.
 * @ingroup admin_operations
 */
AS_EXTERN as_status
aerospike_set_password(
	aerospike* as, as_error* err, const as_policy_admin* policy, const char* user_name,
	const char* password
	);

/**
 * Change user's password by user.  Clear-text password will be hashed using bcrypt before
 * sending to server.
 * @ingroup admin_operations
 */
AS_EXTERN as_status
aerospike_change_password(
	aerospike* as, as_error* err, const as_policy_admin* policy, const char* user_name,
	const char* password
	);

/**
 * Add role to user's list of roles.
 * @ingroup admin_operations
 */
AS_EXTERN as_status
aerospike_grant_roles(
	aerospike* as, as_error* err, const as_policy_admin* policy, const char* user_name,
	const char** roles, int roles_size
	);

/**
 * Remove role from user's list of roles.
 * @ingroup admin_operations
 */
AS_EXTERN as_status
aerospike_revoke_roles(
	aerospike* as, as_error* err, const as_policy_admin* policy, const char* user_name,
	const char** roles, int roles_size
	);

/**
 * Create user defined role.
 * @ingroup admin_operations
 */
AS_EXTERN as_status
aerospike_create_role(
	aerospike* as, as_error* err, const as_policy_admin* policy, const char* role,
	as_privilege** privileges, int privileges_size
	);

/**
 * Create user defined role with optional privileges and whitelist.
 * Whitelist IP addresses can contain wildcards (ie. 10.1.2.0/24).
 * @ingroup admin_operations
 */
AS_EXTERN as_status
aerospike_create_role_whitelist(
	aerospike* as, as_error* err, const as_policy_admin* policy, const char* role,
	as_privilege** privileges, int privileges_size, const char** whitelist, int whitelist_size
	);

/**
 * Create user defined role with optional privileges, whitelist and quotas.
 * Whitelist IP addresses can contain wildcards (ie. 10.1.2.0/24).
 * Quotas are maximum reads/writes per second limit, pass in zero for no limit.
 * Quotas require server security configuration "enable-quotas" to be set to true.
 * @ingroup admin_operations
 */
AS_EXTERN as_status
aerospike_create_role_quotas(
	aerospike* as, as_error* err, const as_policy_admin* policy, const char* role,
	as_privilege** privileges, int privileges_size, const char** whitelist, int whitelist_size,
	int read_quota, int write_quota
	);

/**
 * Delete user defined role.
 * @ingroup admin_operations
 */
AS_EXTERN as_status
aerospike_drop_role(aerospike* as, as_error* err, const as_policy_admin* policy, const char* role);

/**
 * Add specified privileges to user.
 * @ingroup admin_operations
 */
AS_EXTERN as_status
aerospike_grant_privileges(
	aerospike* as, as_error* err, const as_policy_admin* policy, const char* role,
	as_privilege** privileges, int privileges_size
	);

/**
 * Remove specified privileges from user.
 * @ingroup admin_operations
 */
AS_EXTERN as_status
aerospike_revoke_privileges(
	aerospike* as, as_error* err, const as_policy_admin* policy, const char* role,
	as_privilege** privileges, int privileges_size
	);

/**
 * Set IP address whitelist for a role.
 * If whitelist is NULL or empty, remove existing whitelist from role.
 * IP addresses can contain wildcards (ie. 10.1.2.0/24).
 * @ingroup admin_operations
 */
AS_EXTERN as_status
aerospike_set_whitelist(
	aerospike* as, as_error* err, const as_policy_admin* policy, const char* role,
	const char** whitelist, int whitelist_size
	);

/**
 * Set maximum reads/writes per second limits for a role. If a quota is zero, the limit is removed.
 * @ingroup admin_operations
 */
AS_EXTERN as_status
aerospike_set_quotas(
	aerospike* as, as_error* err, const as_policy_admin* policy, const char* role,
	int read_quota, int write_quota
	);

/**
 * Retrieve roles for a given user.
 * When successful, as_user_destroy() must be called to free resources.
 * @ingroup admin_operations
 */
AS_EXTERN as_status
aerospike_query_user(
	aerospike* as, as_error* err, const as_policy_admin* policy, const char* user_name,
	as_user** user
	);

/**
 * Release as_user_roles memory.
 * @ingroup admin_operations
 */
AS_EXTERN void
as_user_destroy(as_user* user);

/**
 * Retrieve all users and their roles.
 * When successful, as_users_destroy() must be called to free resources.
 * @ingroup admin_operations
 */
AS_EXTERN as_status
aerospike_query_users(
	aerospike* as, as_error* err, const as_policy_admin* policy, as_user*** users, int* users_size
	);

/**
 * Release memory for as_user_roles array.
 * @ingroup admin_operations
 */
AS_EXTERN void
as_users_destroy(as_user** users, int users_size);

/**
 * Retrieve role definition for a given role name.
 * When successful, as_role_destroy() must be called to free resources.
 * @ingroup admin_operations
 */
AS_EXTERN as_status
aerospike_query_role(
	aerospike* as, as_error* err, const as_policy_admin* policy, const char* role_name,
	as_role** role
	);

/**
 * Release as_role memory.
 * @ingroup admin_operations
 */
AS_EXTERN void
as_role_destroy(as_role* role);

/**
 * Retrieve all roles and their privileges.
 * When successful, as_roles_destroy() must be called to free resources.
 * @ingroup admin_operations
 */
AS_EXTERN as_status
aerospike_query_roles(
	aerospike* as, as_error* err, const as_policy_admin* policy, as_role*** roles, int* roles_size
	);

/**
 * Release memory for as_role array.
 * @ingroup admin_operations
 */
AS_EXTERN void
as_roles_destroy(as_role** roles, int roles_size);

struct as_cluster_s;
struct as_node_info_s;
struct as_session_s;

/**
 * @private
 * Login to node on node discovery.  Do not use this method directly.
 */
as_status
as_cluster_login(
	struct as_cluster_s* cluster, as_error* err, struct as_socket_s* sock, uint64_t deadline_ms,
	struct as_node_info_s* node_info
	);

/**
 * @private
 * Authenticate user with a server node.  This is done automatically after socket open.
 * Do not use this method directly.
 */
as_status
as_authenticate(
	struct as_cluster_s* cluster, as_error* err, struct as_socket_s* sock, struct as_node_s* node,
	struct as_session_s* session, uint32_t socket_timeout, uint64_t deadline_ms
	);

/**
 * @private
 * Write authentication command to buffer.  Return buffer length.
 */
uint32_t
as_authenticate_set(struct as_cluster_s* cluster, struct as_session_s* session, uint8_t* buffer);

#ifdef __cplusplus
} // end extern "C"
#endif
