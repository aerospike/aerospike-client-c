/******************************************************************************
 *	Copyright 2008-2013 by Aerospike.
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

/**
 *	@defgroup client_policies Client Policies
 *	
 *  Policies define the behavior of database operations. 
 *
 *  Policies fall into two groups: policy values and operation policies.
 *  A policy value is a single value which defines how the client behaves. An
 *  operation policy is a group of policy values which affect an operation.
 *
 *  ## Policy Values
 *
 *  The following are the policy values. For details, please see the documentation
 *  for each policy value
 *
 *  - as_policy_key
 *  - as_policy_gen
 *  - as_policy_retry
 *  - as_policy_exists
 *  
 *  ## Operation Policies
 *
 *  The following are the operation policies. Operation policies are groups of
 *  policy values for a type of operation.
 *
 *  - as_policy_batch
 *  - as_policy_info
 *  - as_policy_operate
 *  - as_policy_read
 *  - as_policy_remove
 *  - as_policy_query
 *  - as_policy_scan
 *  - as_policy_write
 *
 */

#pragma once 

#include <stdbool.h>
#include <stdint.h>

/******************************************************************************
 *	MACROS
 *****************************************************************************/

/**
 *	Default timeout value
 *
 *	@ingroup client_policies
 */
#define AS_POLICY_TIMEOUT_DEFAULT 1000

/**
 *	Default as_policy_retry value
 *
 *	@ingroup client_policies
 */
#define AS_POLICY_RETRY_DEFAULT AS_POLICY_RETRY_NONE

/**
 *	Default as_policy_gen value
 *
 *	@ingroup client_policies
 */
#define AS_POLICY_GEN_DEFAULT AS_POLICY_GEN_IGNORE

/**
 *	Default as_policy_key value
 *
 *	@ingroup client_policies
 */
#define AS_POLICY_KEY_DEFAULT AS_POLICY_KEY_DIGEST

/**
 *	Default as_policy_exists value
 *
 *	@ingroup client_policies
 */
#define AS_POLICY_EXISTS_DEFAULT AS_POLICY_EXISTS_IGNORE

/******************************************************************************
 *	TYPES
 *****************************************************************************/

/**
 *	Retry Policy
 *
 *	Specifies the behavior of failed operations. 
 *
 *	@ingroup client_policies
 */
typedef enum as_policy_retry_e {

	/**
	 *	The policy is undefined.
	 *
	 *	If set, then the value will default to
	 *	either as_config.policies.retry
	 *	or `AS_POLICY_RETRY_DEFAULT`.
	 */
	AS_POLICY_RETRY_UNDEF, 

	/**
	 *	Only attempt an operation once.
	 */
	AS_POLICY_RETRY_NONE, 

	/**
	 *	If an operation fails, attempt the operation
	 *	one more time.
	 */
	AS_POLICY_RETRY_ONCE, 

} as_policy_retry;

/**
 *	Generation Policy
 *
 *	Specifies the behavior of record modifications with regard to the 
 *	generation value.
 *
 *	@ingroup client_policies
 */
typedef enum as_policy_gen_e {

	/**
	 *	The policy is undefined.
	 *
	 *	If set, then the value will default to
	 *	either as_config.policies.gen
	 *	or `AS_POLICY_GEN_DEFAULT`.
	 */
	AS_POLICY_GEN_UNDEF,

	/**
	 *	Write a record, regardless of generation.
	 */
	AS_POLICY_GEN_IGNORE,

	/**
	 *	Write a record, ONLY if generations are equal
	 */
	AS_POLICY_GEN_EQ,

	/**
	 *	Write a record, ONLY if local generation is 
	 *	greater-than remote generation
	 */
	AS_POLICY_GEN_GT,

	/**
	 *	Write a record creating a duplicate, ONLY if
	 *	the generation collides (?)
	 */
	AS_POLICY_GEN_DUP

} as_policy_gen;

/**
 *	Key Policy
 *
 *	Specifies the behavior for whether keys or digests
 *	should be sent to the cluster.
 *
 *	@ingroup client_policies
 */
typedef enum as_policy_key_e {

	/**
	 *	The policy is undefined.
	 *
	 *	If set, then the value will default to either as_config.policies.key
	 *	or `AS_POLICY_KEY_DEFAULT`.
	 */
	AS_POLICY_KEY_UNDEF,

	/**
	 *	Send the digest value of the key.
	 *
	 *	This is the recommended mode of operation. This calculates the digest
	 *	and send the digest to the server. The digest is only calculated on
	 *	the client, and not on the server.
	 */
	AS_POLICY_KEY_DIGEST,

	/**
	 *	Send the key, but do not store it.
	 *	
	 *	This policy is ideal if you want to reduce the number of bytes sent
	 *	over the network. This will only work if the combination the set and
	 *	key value are less than 20 bytes, which is the size of the digest.
	 *
	 *	This will also cause the digest to be computer once on the client
	 * 	and once on the server. 
	 *	
	 *	If your values are not less than 20 bytes, then you should just 
	 *	use AS_POLICY_KEY_DIGEST.
	 */
	AS_POLICY_KEY_SEND,

	/**
	 *	Store the key.
	 *	@warning Not yet implemented
	 */
	AS_POLICY_KEY_STORE

} as_policy_key;

/**
 *	Existence Policy.
 *	
 *	Specifies the behavior for writing the record
 *	depending whether or not it exists.
 *
 *	@ingroup client_policies
 */
typedef enum as_policy_exists_e {

	/**
	 *	The policy is undefined.
	 *
	 *	If set, then the value will default to
	 *	either as_config.policies.exists
	 *	or `AS_POLICY_EXISTS_DEFAULT`.
	 */
	AS_POLICY_EXISTS_UNDEF,

	/**
	 *	Write the record, regardless of existence. (i.e. create or update.)
	 */
	AS_POLICY_EXISTS_IGNORE,

	/**
	 *	Create a record, ONLY if it doesn't exist.
	 */
	AS_POLICY_EXISTS_CREATE,

	/**
	 *	Update a record, ONLY if it exists.
	 */
	AS_POLICY_EXISTS_UPDATE,

	/**
	 *	Completely replace a record, ONLY if it exists.
	 */
	AS_POLICY_EXISTS_REPLACE,

	/**
	 *	Completely replace a record if it exists, otherwise create it.
	 */
	AS_POLICY_EXISTS_CREATE_OR_REPLACE

} as_policy_exists;

/**
 *	Boolean Policy.
 *
 *	This enum provides boolean values (true,false) and an
 *	undefined value for the boolean.
 *
 *	@ingroup client_policies
 */
typedef enum as_policy_bool_e {

	/**
	 *	If the value is neither true or false,
	 * 	then it is undefined. This is used for cases
	 *	where we initialize a variable, but do not want
	 *  it to have a value.
	 */
	AS_POLICY_BOOL_UNDEF = -1,

	/**
	 *	This value is interchangable with `false`.
	 */
	AS_POLICY_BOOL_FALSE = false,

	/**
	 *	This value is interchangable with `true`.
	 */
	AS_POLICY_BOOL_TRUE = true

} as_policy_bool;


/**
 *	Write Policy
 *
 *	@ingroup client_policies
 */
typedef struct as_policy_write_s {

	/**
	 *	Maximum time in milliseconds to wait for 
	 *	the operation to complete.
	 *
	 *	If 0 (zero), then the value will default to
	 *	either as_config.policies.timeout
	 *	or `AS_POLICY_TIMEOUT_DEFAULT`.
	 */
	uint32_t timeout;

	/**
	 *	Specifies the behavior for failed operations.
	 */
	as_policy_retry retry;

	/**
	 *	Specifies the behavior for the key.
	 */
	as_policy_key key;

	/**
	 *	Specifies the behavior for the generation
	 *	value.
	 */
	as_policy_gen gen;

	/**
	 *	Specifies the behavior for the existence 
	 *	of the record.
	 */
	as_policy_exists exists;

} as_policy_write;

/**
 *	Read Policy
 *
 *	@ingroup client_policies
 */
typedef struct as_policy_read_s {

	/**
	 *	Maximum time in milliseconds to wait for 
	 *	the operation to complete.
	 *
	 *	If 0 (zero), then the value will default to
	 *	either as_config.policies.timeout
	 *	or `AS_POLICY_TIMEOUT_DEFAULT`.
	 */
	uint32_t timeout;

	/**
	 *	Specifies the behavior for the key.
	 */
	as_policy_key key;

} as_policy_read;

/**
 *	Key Apply Policy
 *
 *	@ingroup client_policies
 */
typedef struct as_policy_apply_s {

	/**
	 *	Maximum time in milliseconds to wait for 
	 *	the operation to complete.
	 *
	 *	If 0 (zero), then the value will default to
	 *	either as_config.policies.timeout
	 *	or `AS_POLICY_TIMEOUT_DEFAULT`.
	 */
	uint32_t timeout;

	/**
	 *	Specifies the behavior for the key.
	 */
	as_policy_key key;

} as_policy_apply;

/**
 *	Operate Policy
 *
 *	@ingroup client_policies
 */
typedef struct as_policy_operate_s {

	/**
	 *	Maximum time in milliseconds to wait for 
	 *	the operation to complete.
	 *
	 *	If 0 (zero), then the value will default to
	 *	either as_config.policies.timeout
	 *	or `AS_POLICY_TIMEOUT_DEFAULT`.
	 */
	uint32_t timeout;

	/**
	 *	Specifies the behavior for failed operations.
	 */
	as_policy_retry retry;
	
	/**
	 *	Specifies the behavior for the key.
	 */
	as_policy_key key;

	/**
	 *	Specifies the behavior for the generation
	 *	value.
	 */
	as_policy_gen gen;

} as_policy_operate;

/**
 *	Remove Policy
 *
 *	@ingroup client_policies
 */
typedef struct as_policy_remove_s {

	/**
	 *	Maximum time in milliseconds to wait for 
	 *	the operation to complete.
	 *
	 *	If 0 (zero), then the value will default to
	 *	either as_config.policies.timeout
	 *	or `AS_POLICY_TIMEOUT_DEFAULT`.
	 */
	uint32_t timeout;

	/**
	 *	The generation of the record.
	 */
	uint16_t generation;

	/**
	 *	Specifies the behavior of failed operations.
	 */
	as_policy_retry retry;
	
	/**
	 *	Specifies the behavior for the key.
	 */
	as_policy_key key;

	/**
	 *	Specifies the behavior for the generation
	 *	value.
	 */
	as_policy_gen gen;

} as_policy_remove;

/**
 *	Query Policy
 *
 *	@ingroup client_policies
 */
typedef struct as_policy_query_s {

	/**
	 *	Maximum time in milliseconds to wait for 
	 *	the operation to complete.
	 *
	 *	If 0 (zero), then the value will default to
	 *	either as_config.policies.timeout
	 *	or Aerospike's recommended default.
	 */
	uint32_t timeout;

} as_policy_query;

/**
 *	Scan Policy
 *
 *	@ingroup client_policies
 */
typedef struct as_policy_scan_s {

	/**
	 *	Maximum time in milliseconds to wait for the operation to complete.
	 *
	 *	If 0 (zero), then the value will default to
	 *	either as_config.policies.timeout
	 *	or `AS_POLICY_TIMEOUT_DEFAULT`.
	 */
	uint32_t timeout;

	/**
	 *	Abort the scan if the cluster is not in a 
	 *	stable state.
	 */
	as_policy_bool fail_on_cluster_change;

} as_policy_scan;

/**
 *	Info Policy
 *
 *	@ingroup client_policies
 */
typedef struct as_policy_info_s {

	/**
	 *	Maximum time in milliseconds to wait for 
	 *	the operation to complete.
	 *
	 *	If 0 (zero), then the value will default to
	 *	either as_config.policies.timeout
	 *	or `AS_POLICY_TIMEOUT_DEFAULT`.
	 */
	uint32_t timeout;

	/**
	 *	Send request without any further processing.
	 */
	as_policy_bool send_as_is;

	/**
	 *	Ensure the request is within allowable size limits.
	 */
	as_policy_bool check_bounds;

} as_policy_info;

/**
 *	Batch Policy
 *
 *	@ingroup client_policies
 */
typedef struct as_policy_batch_s {

	/**
	 *	Maximum time in milliseconds to wait for 
	 *	the operation to complete.
	 *
	 *	If 0 (zero), then the value will default to
	 *	either as_config.policies.timeout
	 *	or `AS_POLICY_TIMEOUT_DEFAULT`.
	 */
	uint32_t timeout;

} as_policy_batch;

/**
 *	Struct of all policy values and operation policies. 
 *	
 *	This is utilizes by as_config, to define global and default values
 *	for policies.
 *
 *	@ingroup as_config_t
 */
typedef struct as_policies_s {

	/***************************************************************************
	 *	DEFAULT VALUES, IF SPECIFIC POLICY IS UNDEFINED
	 **************************************************************************/

	/**
	 *	Default timeout in milliseconds.
	 *
	 *	Will be used if specific policies have a timeout of 0 (zero).
	 *
	 *	The default value is `AS_POLICY_TIMEOUT_DEFAULT`.
	 */
	uint32_t timeout;

	/**
	 *	Specifies the behavior for failed operations.
	 *	
	 *	The default value is `AS_POLICY_RETRY_DEFAULT`.
	 */
	as_policy_retry retry;
	
	/**
	 *	Specifies the behavior for the key.
	 *	
	 *	The default value is `AS_POLICY_KEY_DEFAULT`.
	 */
	as_policy_key key;

	/**
	 *	Specifies the behavior for the generation
	 *	value.
	 *	
	 *	The default value is `AS_POLICY_GEN_DEFAULT`.
	 */
	as_policy_gen gen;

	/**
	 *	Specifies the behavior for the existence 
	 *	of the record.
	 *	
	 *	The default value is `AS_POLICY_EXISTS_DEFAULT`.
	 */
	as_policy_exists exists;

	/***************************************************************************
	 *	SPECIFIC POLICIES
	 **************************************************************************/

	/**
	 *	The default read policy.
	 */
	as_policy_read read;

	/**
	 *	The default write policy.
	 */
	as_policy_write write;

	/**
	 *	The default operate policy.
	 */
	as_policy_operate operate;

	/**
	 *	The default remove policy.
	 */
	as_policy_remove remove;

	/**
	 *	The default apply policy.
	 */
	as_policy_apply apply;

	/**
	 *	The default query policy.
	 */
	as_policy_query query;

	/**
	 *	The default scan policy.
	 */
	as_policy_scan scan;

	/**
	 *	The default info policy.
	 */
	as_policy_info info;

	/**
	 *	The default batch policy.
	 */
	as_policy_batch batch;

} as_policies;

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/**
 *	Initialize as_policy_read to default values.
 *
 *	@param p	The policy to initialize
 *	@return The initialized policy.
 *
 *	@relates as_policy_read
 */
as_policy_read * as_policy_read_init(as_policy_read * p);

/**
 *	Initialize as_policy_apply to default values.
 *
 *	@param p	The policy to initialize
 *	@return The initialized policy.
 *
 *	@relates as_policy_apply
 */
as_policy_apply * as_policy_apply_init(as_policy_apply * p);

/**
 *	Initialize as_policy_write to default values.
 *
 *	@param p	The policy to initialize
 *	@return The initialized policy.
 *
 *	@relates as_policy_write
 */
as_policy_write * as_policy_write_init(as_policy_write * p);

/**
 *	Initialize as_policy_operate to default values.
 *
 *	@param p	The policy to initialize
 *	@return The initialized policy.
 *
 *	@relates as_policy_operate
 */
as_policy_operate * as_policy_operate_init(as_policy_operate * p);

/**
 *	Initialize as_policy_scan to default values.
 *
 *	@param p	The policy to initialize
 *	@return The initialized policy.
 *
 *	@relates as_policy_scan
 */
as_policy_scan * as_policy_scan_init(as_policy_scan * p);

/**
 *	Initialize as_policy_query to default values.
 *
 *	@param p	The policy to initialize
 *	@return The initialized policy.
 *
 *	@relates as_policy_query
 */
as_policy_query * as_policy_query_init(as_policy_query * p);

/**
 *	Initialize as_policy_info to default values.
 *
 *	@param p	The policy to initialize
 *	@return The initialized policy.
 *
 *	@relates as_policy_info
 */
as_policy_info * as_policy_info_init(as_policy_info * p);

/**
 *	Initialize as_policy_remove to default values.
 *
 *	@param p	The policy to initialize
 *	@return The initialized policy.
 *
 *	@relates as_policy_remove
 */
as_policy_remove * as_policy_remove_init(as_policy_remove * p);

/**
 *	Initialize as_policy_batch to default values.
 *
 *	@param p	The policy to initialize
 *	@return The initialized policy.
 *
 *	@relates as_policy_batch
 */
as_policy_batch * as_policy_batch_init(as_policy_batch * p);

/**
 *	Initialize as_policies to default values.
 *
 *	@param p	The policies to initialize
 *	@return The initialized policies.
 *
 *	@relates as_policies
 */
as_policies * as_policies_init(as_policies * p);


