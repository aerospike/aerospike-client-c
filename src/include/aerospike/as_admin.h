/******************************************************************************
 *	Copyright 2008-2014 by Aerospike.
 *
 *	Permission is hereby granted, free of charge, to any person obtaining a copy 
 *	of this software and associated documentation files (the "Software"), to 
 *	deal in the Software without restriction, including without limitation the 
 *	rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 *	sell copies of the Software, and to permit persons to whom the Software is 
 *	furnished to do so, subject to the following conditions:
 *	
 *	The above copyright notice and this permission notice shall be included in 
 *	all copies or substantial portions of the Software.
 *	
 *	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 *	FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 *	IN THE SOFTWARE.
 *****************************************************************************/
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
