/*
 * Copyright 2008-2014 Aerospike, Inc.
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

#include <aerospike/aerospike.h>
#include <aerospike/as_config.h>

/******************************************************************************
 *	MACROS
 *****************************************************************************/

/**
 *	Maximum size of role string including null byte.
 */
#define AS_ROLE_SIZE 32

/******************************************************************************
 *	TYPES
 *****************************************************************************/

/**
 *	User and assigned roles.
 */
typedef struct as_user_roles_s {
	/**
	 *	User name.
	 */
	char user[AS_USER_SIZE];
	
	/**
	 *	Length of roles array.
	 */
	int roles_size;

	/**
	 *	Array of assigned roles.
	 */
	char roles[][AS_ROLE_SIZE];
} as_user_roles;

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

/**
 *	Create user with password and roles.  Clear-text password will be hashed using bcrypt before 
 *	sending to server.  Return zero on success.
 */
int
aerospike_create_user(aerospike* as, const as_policy_admin* policy, const char* user, const char* password, const char** roles, int roles_size);

/**
 *	Remove user from cluster.  Return zero on success.
 */
int
aerospike_drop_user(aerospike* as, const as_policy_admin* policy, const char* user);

/**
 *	Set user's password by user administrator.  Clear-text password will be hashed using bcrypt before sending to server.
 *	Return zero on success.
 */
int
aerospike_set_password(aerospike* as, const as_policy_admin* policy, const char* user, const char* password);

/**
 *	Change user's password by user.  Clear-text password will be hashed using bcrypt before sending to server.
 *	Return zero on success.
 */
int
aerospike_change_password(aerospike* as, const as_policy_admin* policy, const char* user, const char* password);

/**
 *	Add role to user's list of roles.  Return zero on success.
 */
int
aerospike_grant_roles(aerospike* as, const as_policy_admin* policy, const char* user, const char** roles, int roles_size);

/**
 *	Remove role from user's list of roles.  Return zero on success.
 */
int
aerospike_revoke_roles(aerospike* as, const as_policy_admin* policy, const char* user, const char** roles, int roles_size);

/**
 *	Replace user's list of roles with a new list of roles.  Return zero on success.
 */
int
aerospike_replace_roles(aerospike* as, const as_policy_admin* policy, const char* user, const char** roles, int roles_size);

/**
 *	Retrieve roles for a given user.  Return zero on success.
 *	When successful, as_user_roles_destroy() must be called to free resources.
 */
int
aerospike_query_user(aerospike* as, const as_policy_admin* policy, const char* user, as_user_roles** user_roles);

/**
 *	Release as_user_roles memory.
 */
void
as_user_roles_destroy(as_user_roles* user_roles);

/**
 *	Retrieve all users and their roles.  Return zero on success.
 *	When successful, as_user_roles_destroy_array() must be called to free resources.
 */
int
aerospike_query_users(aerospike* as, const as_policy_admin* policy, as_user_roles*** user_roles, int* user_roles_size);

/**
 *	Release memory for as_user_roles array.
 */
void
as_user_roles_destroy_array(as_user_roles** user_roles, int user_roles_size);

/**
 *	@private
 *	Authenticate user with a server node.  This is done automatically after socket open.
 *  Do not use this method directly.
 */
int
as_authenticate(int fd, const char* user, const char* credential, int timeout_ms);
