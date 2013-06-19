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

#pragma once 

#include <stdbool.h>
#include <stdint.h>

/******************************************************************************
 *	MACROS
 *****************************************************************************/

/**
 *	Default timeout value
 */
#define AS_POLICY_TIMEOUT_DEFAULT 1000

/**
 *	Default as_policy_writemode value
 */
#define AS_POLICY_WRITEMODE_DEFAULT AS_POLICY_WRITEMODE_RETRY

/**
 *	Default as_policy_gen value
 */
#define AS_POLICY_GEN_DEFAULT AS_POLICY_GEN_IGNORE

/**
 *	Default as_policy_key value
 */
#define AS_POLICY_KEY_DEFAULT AS_POLICY_KEY_DIGEST

/**
 *	Default as_policy_exists value
 */
#define AS_POLICY_EXISTS_DEFAULT AS_POLICY_EXISTS_IGNORE

/******************************************************************************
 *	TYPES
 *****************************************************************************/

/**
 *	Write Mode Policy
 */
typedef enum as_policy_writemode_e {

	/**
	 *	The policy is undefined.
	 *	This will mean the value will be defaulted to
	 *	value either defined in the as_config.policies
	 *	or Aerospike's recommended default.
	 */
	AS_POLICY_WRITEMODE_UNDEF, 

	/**
	 * Asynchronous write mode.
	 */
	AS_POLICY_WRITEMODE_ASYNC, 

	/**
	 * Attempt write once or fail.
	 */
	AS_POLICY_WRITEMODE_ONESHOT, 

	/**
	 * Attempt write until success.
	 */
	AS_POLICY_WRITEMODE_RETRY

} as_policy_writemode;

/**
 *	Generation Policy
 *
 *	Specifies the behavior of record modifications
 *	with regard to the generation value.
 */
typedef enum as_policy_gen_e {

	/**
	 *	The policy is undefined.
	 *
	 *	If set, then the value will default to
	 *	either as_config.policies.gen
	 *	or Aerospike's recommended default: 
	 *	AS_POLICY_KEY_DIGEST
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
 */
typedef enum as_policy_key_e {

	/**
	 *	The policy is undefined.
	 *
	 *	If set, then the value will default to
	 *	either as_config.policies.key
	 *	or Aerospike's recommended default: 
	 *	AS_POLICY_KEY_DIGEST
	 */
	AS_POLICY_KEY_UNDEF,

	/**
	 *	Send the digest value of the key.
	 */
	AS_POLICY_KEY_DIGEST,

	/**
	 *	Send the key, but do not store it.
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
 */
typedef enum as_policy_exists_e {

	/**
	 *	The policy is undefined.
	 *
	 *	If set, then the value will default to
	 *	either as_config.policies.exists
	 *	or Aerospike's recommended default: 
	 *	AS_POLICY_KEY_DIGEST
	 */
	AS_POLICY_EXISTS_UNDEF,

	/**
	 *	Write the record, regardless of existence.
	 */
	AS_POLICY_EXISTS_IGNORE,

	/**
	 *	Create a record, ONLY if it doesn't exist.
	 */
	AS_POLICY_EXISTS_CREATE,

	/**
	 *	Update a record, ONLY if it exists.
	 *	@warning Not yet implemented
	 */
	AS_POLICY_EXISTS_UPDATE

} as_policy_exists;

/**
 *	Write Policy
 */
typedef struct as_policy_write_s {

	/**
	 *	Maximum time in milliseconds to wait for 
	 *	the operation to complete.
	 *
	 *	If 0 (zero), then the value will default to
	 *	either as_config.policies.timeout
	 *	or Aerospike's recommended default.
	 */
	uint32_t timeout;

	/**
	 *	The write mode defines the behavior 
	 *	for writing data to the cluster.
	 */
	as_policy_writemode mode;

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
 */
typedef struct as_policy_read_s {

	/**
	 *	Maximum time in milliseconds to wait for 
	 *	the operation to complete.
	 *
	 *	If 0 (zero), then the value will default to
	 *	either as_config.policies.timeout
	 *	or Aerospike's recommended default.
	 */
	uint32_t timeout;

	/**
	 *	Specifies the behavior for the key.
	 */
	as_policy_key key;

} as_policy_read;

/**
 *	Operate Policy
 */
typedef struct as_policy_operate_s {

	/**
	 *	Maximum time in milliseconds to wait for 
	 *	the operation to complete.
	 *
	 *	If 0 (zero), then the value will default to
	 *	either as_config.policies.timeout
	 *	or Aerospike's recommended default.
	 */
	uint32_t timeout;

	/**
	 *	The generation of the record.
	 */
	uint16_t generation;

	/**
	 *	The write mode defines the behavior 
	 *	for writing data to the cluster.
	 */
	as_policy_writemode mode;
	
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
 *	Query Policy
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
 */
typedef struct as_policy_scan_s {

	/**
	 *	Maximum time in milliseconds to wait for 
	 *	the operation to complete.
	 *
	 *	If 0 (zero), then the value will default to
	 *	either as_config.policies.timeout
	 *	or Aerospike's recommended default.
	 */
	uint32_t timeout;

	/**
	 *	Abort the scan if the cluster is not in a 
	 *	stable state.
	 */
	bool fail_on_cluster_change;

} as_policy_scan;

/**
 *	Info Policy
 */
typedef struct as_policy_info_s {

	/**
	 *	Maximum time in milliseconds to wait for 
	 *	the operation to complete.
	 *
	 *	If 0 (zero), then the value will default to
	 *	either as_config.policies.timeout
	 *	or Aerospike's recommended default.
	 */
	uint32_t timeout;

	/**
	 *	Send request without any further processing.
	 */
	bool send_as_is;

	/**
	 *	Ensure the request is within allowable size limits.
	 */
	bool check_bounds;

} as_policy_info;

/**
 *	struct of all the policies.
 *	This is utilizes for defining defaults within an aerospike 
 *	client or configuration.
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
	 *	If 0 (zero), then the value will default to
	 *	or Aerospike's recommended default: 1000 ms
	 */
	uint32_t timeout;

	/**
	 *	The write mode defines the behavior for writing data to the cluster.
	 *	
	 *	If AS_POLICY_WRITEMODE_UNDEF, then the value will default to
	 *	or Aerospike's recommended default: AS_POLICY_WRITEMODE_RETRY.
	 */
	as_policy_writemode mode;
	
	/**
	 *	Specifies the behavior for the key.
	 *	
	 *	If AS_POLICY_KEY_UNDEF, then the value will default to
	 *	or Aerospike's recommended default: AS_POLICY_KEY_DIGEST.
	 */
	as_policy_key key;

	/**
	 *	Specifies the behavior for the generation
	 *	value.
	 *	
	 *	If AS_POLICY_GEN_UNDEF, then the value will default to
	 *	or Aerospike's recommended default: AS_POLICY_GEN_IGNORE.
	 */
	as_policy_gen gen;

	/**
	 *	Specifies the behavior for the existence 
	 *	of the record.
	 *	
	 *	If AS_POLICY_EXISTS_UNDEF, then the value will default to
	 *	or Aerospike's recommended default: AS_POLICY_EXISTS_IGNORE.
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

} as_policies;

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/**
 *	Initialize as_policy_read to default values.
 *
 *	@param p	The policy to initialize
 *	@return The initialized policy.
 */
as_policy_read * as_policy_read_init(as_policy_read * p);

/**
 *	Initialize as_policy_write to default values.
 *
 *	@param p	The policy to initialize
 *	@return The initialized policy.
 */
as_policy_write * as_policy_write_init(as_policy_write * p);

/**
 *	Initialize as_policy_operate to default values.
 *
 *	@param p	The policy to initialize
 *	@return The initialized policy.
 */
as_policy_operate * as_policy_operate_init(as_policy_operate * p);

/**
 *	Initialize as_policy_scan to default values.
 *
 *	@param p	The policy to initialize
 *	@return The initialized policy.
 */
as_policy_scan * as_policy_scan_init(as_policy_scan * p);

/**
 *	Initialize as_policy_query to default values.
 *
 *	@param p	The policy to initialize
 *	@return The initialized policy.
 */
as_policy_query * as_policy_query_init(as_policy_query * p);

/**
 *	Initialize as_policy_info to default values.
 *
 *	@param p	The policy to initialize
 *	@return The initialized policy.
 */
as_policy_info * as_policy_info_init(as_policy_info * p);

/**
 *	Initialize as_policies to default values.
 *
 *	@param p	The policies to initialize
 *	@return The initialized policies.
 */
as_policies * as_policies_init(as_policies * p);


