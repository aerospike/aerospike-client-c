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
 *  - as_policy_replica
 *  - as_policy_consistency_level
 *  - as_policy_commit_level
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
 */

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
#define AS_POLICY_RETRY_DEFAULT AS_POLICY_RETRY_ONCE

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

/**
 *	Default as_policy_replica value
 *
 *	@ingroup client_policies
 */
#define AS_POLICY_REPLICA_DEFAULT AS_POLICY_REPLICA_MASTER

/**
 *	Default as_policy_consistency_level value for read
 *
 *	@ingroup client_policies
 */
#define AS_POLICY_CONSISTENCY_LEVEL_DEFAULT AS_POLICY_CONSISTENCY_LEVEL_ONE

/**
 *	Default as_policy_commit_level value for write
 *
 *	@ingroup client_policies
 */
#define AS_POLICY_COMMIT_LEVEL_DEFAULT AS_POLICY_COMMIT_LEVEL_ALL

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
	 *	Send the digest value of the key.
	 *
	 *	This is the recommended mode of operation. This calculates the digest
	 *	and send the digest to the server. The digest is only calculated on
	 *	the client, and not on the server.
	 */
	AS_POLICY_KEY_DIGEST,

	/**
	 *	Send the key, in addition to the digest value.
	 *
	 *	If you want keys to be returned when scanning or querying, the keys must
	 *	be stored on the server. This policy causes a write operation to store
	 *	the key. Once a key is stored, the server will keep it - there is no
	 *	need to use this policy on subsequent updates of the record.
	 *
	 *	If this policy is used on read or delete operations, or on subsequent
	 *	updates of a record with a stored key, the key sent will be compared
	 *	with the key stored on the server. A mismatch will cause
	 *	AEROSPIKE_ERR_RECORD_KEY_MISMATCH to be returned.
	 */
	AS_POLICY_KEY_SEND,

} as_policy_key;

/**
 *	Existence Policy
 *	
 *	Specifies the behavior for writing the record
 *	depending whether or not it exists.
 *
 *	@ingroup client_policies
 */
typedef enum as_policy_exists_e {

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
 *  Replica Policy
 *
 *  Specifies which partition replica to read from.
 *
 *  @ingroup client_policies
 */
typedef enum as_policy_replica_e {

	/**
	 *  Read from the partition master replica node.
	 */
	AS_POLICY_REPLICA_MASTER,

	/**
	 *  Read from an unspecified replica node.
	 */
	AS_POLICY_REPLICA_ANY

} as_policy_replica;

/**
 *  Consistency Level
 *
 *  Specifies the number of replicas to be consulted
 *  in a read operation to provide the desired
 *  consistency guarantee.
 *
 *  @ingroup client_policies
 */
typedef enum as_policy_consistency_level_e {

	/**
	 *  Involve a single replica in the operation.
	 */
	AS_POLICY_CONSISTENCY_LEVEL_ONE,

	/**
	 *  Involve all replicas in the operation.
	 */
	AS_POLICY_CONSISTENCY_LEVEL_ALL,

} as_policy_consistency_level;

/**
 *  Commit Level
 *
 *  Specifies the number of replicas required to be successfully
 *  committed before returning success in a write operation
 *  to provide the desired consistency guarantee.
 *
 *  @ingroup client_policies
 */
typedef enum as_policy_commit_level_e {

	/**
	 *  Return succcess only after successfully committing all replicas.
	 */
	AS_POLICY_COMMIT_LEVEL_ALL,

	/**
	 *  Return succcess after successfully committing the master replica.
	 */
	AS_POLICY_COMMIT_LEVEL_MASTER,

} as_policy_commit_level;

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
	 *	If undefined (-1), then the value will default to
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

	/**
	 *  Specifies the number of replicas required
	 *  to be committed successfully when writing
	 *  before returning transaction succeeded.
	 */
	as_policy_commit_level commit_level;

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
	 *	If undefined (-1), then the value will default to
	 *	either as_config.policies.timeout
	 *	or `AS_POLICY_TIMEOUT_DEFAULT`.
	 */
	uint32_t timeout;

	/**
	 *	Specifies the behavior for the key.
	 */
	as_policy_key key;

	/**
	 *  Specifies the replica to be consulted for the read.
	 */
	as_policy_replica replica;

	/**
	 *  Specifies the number of replicas consulted
	 *  when reading for the desired consistency guarantee.
	 */
	as_policy_consistency_level consistency_level;

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
	 *	If undefined (-1), then the value will default to
	 *	either as_config.policies.timeout
	 *	or `AS_POLICY_TIMEOUT_DEFAULT`.
	 */
	uint32_t timeout;

	/**
	 *	Specifies the behavior for the key.
	 */
	as_policy_key key;

	/**
	 *  Specifies the number of replicas required
	 *  to be committed successfully when writing
	 *  before returning transaction succeeded.
	 */
	as_policy_commit_level commit_level;

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
	 *	If undefined (-1), then the value will default to
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
	 *  Specifies the replica to be consulted for the read.
	 */
	as_policy_replica replica;

	/**
	 *  Specifies the number of replicas consulted
	 *  when reading for the desired consistency guarantee.
	 */
	as_policy_consistency_level consistency_level;

	/**
	 *  Specifies the number of replicas required
	 *  to be committed successfully when writing
	 *  before returning transaction succeeded.
	 */
	as_policy_commit_level commit_level;

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
	 *	If undefined (-1), then the value will default to
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

	/**
	 *  Specifies the number of replicas required
	 *  to be committed successfully when writing
	 *  before returning transaction succeeded.
	 */
	as_policy_commit_level commit_level;

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
	 *	The default (0) means do not timeout.
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
	 *	The default (0) means do not timeout.
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
 *
 *	@ingroup client_policies
 */
typedef struct as_policy_info_s {

	/**
	 *	Maximum time in milliseconds to wait for 
	 *	the operation to complete.
	 *
	 *	If undefined (-1), then the value will default to
	 *	either as_config.policies.timeout
	 *	or `AS_POLICY_TIMEOUT_DEFAULT`.
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
 *	Batch Policy
 *
 *	@ingroup client_policies
 */
typedef struct as_policy_batch_s {

	/**
	 *	Maximum time in milliseconds to wait for 
	 *	the operation to complete.
	 *
	 *	If undefined (-1), then the value will default to
	 *	either as_config.policies.timeout
	 *	or `AS_POLICY_TIMEOUT_DEFAULT`.
	 */
	uint32_t timeout;

} as_policy_batch;

/**
 *	Administration Policy
 *
 *	@ingroup client_policies
 */
typedef struct as_policy_admin_s {
	
	/**
	 *	Maximum time in milliseconds to wait for
	 *	the operation to complete.
	 *
	 *	If undefined (-1), then the value will default to
	 *	either as_config.policies.timeout
	 *	or `AS_POLICY_TIMEOUT_DEFAULT`.
	 */
	uint32_t timeout;
	
} as_policy_admin;

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

	/**
	 *	Specifies which replica to read.
	 *
	 *	The default value is `AS_POLICY_REPLICA_MASTER`.
	 */
	as_policy_replica replica;

	/**
	 *	Specifies the consistency level for reading.
	 *
	 *	The default value is `AS_POLICY_CONSISTENCY_LEVEL_ONE`.
	 */
	as_policy_consistency_level consistency_level;

	/**
	 *	Specifies the commit level for writing.
	 *
	 *	The default value is `AS_POLICY_COMMIT_LEVEL_ALL`.
	 */
	as_policy_commit_level commit_level;

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
	
	/**
	 *	The default administration policy.
	 */
	as_policy_admin admin;

} as_policies;

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/**
 *	Initialize as_policy_read to default values.
 *
 *	@param p	The policy to initialize.
 *	@return		The initialized policy.
 *
 *	@relates as_policy_read
 */
static inline as_policy_read*
as_policy_read_init(as_policy_read* p)
{
	p->timeout = AS_POLICY_TIMEOUT_DEFAULT;
	p->key = AS_POLICY_KEY_DEFAULT;
	p->replica = AS_POLICY_REPLICA_DEFAULT;
	p->consistency_level = AS_POLICY_CONSISTENCY_LEVEL_DEFAULT;
	return p;
}

/**
 *	Copy as_policy_read values.
 *
 *	@param src	The source policy.
 *	@param trg	The target policy.
 *
 *	@relates as_policy_read
 */
static inline void
as_policy_read_copy(as_policy_read* src, as_policy_read* trg)
{
	trg->timeout = src->timeout;
	trg->key = src->key;
	trg->replica = src->replica;
	trg->consistency_level = src->consistency_level;
}

/**
 *	Initialize as_policy_write to default values.
 *
 *	@param p	The policy to initialize.
 *	@return		The initialized policy.
 *
 *	@relates as_policy_write
 */
static inline as_policy_write*
as_policy_write_init(as_policy_write* p)
{
	p->timeout = AS_POLICY_TIMEOUT_DEFAULT;
	p->retry = AS_POLICY_RETRY_DEFAULT;
	p->key = AS_POLICY_KEY_DEFAULT;
	p->gen = AS_POLICY_GEN_DEFAULT;
	p->exists = AS_POLICY_EXISTS_DEFAULT;
	p->commit_level = AS_POLICY_COMMIT_LEVEL_DEFAULT;
	return p;
}

/**
 *	Copy as_policy_write values.
 *
 *	@param src	The source policy.
 *	@param trg	The target policy.
 *
 *	@relates as_policy_write
 */
static inline void
as_policy_write_copy(as_policy_write* src, as_policy_write* trg)
{
	trg->timeout = src->timeout;
	trg->retry = src->retry;
	trg->key = src->key;
	trg->gen = src->gen;
	trg->exists = src->exists;
	trg->commit_level = src->commit_level;
}

/**
 *	Initialize as_policy_operate to default values.
 *
 *	@param p	The policy to initialize.
 *	@return		The initialized policy.
 *
 *	@relates as_policy_operate
 */
static inline as_policy_operate*
as_policy_operate_init(as_policy_operate* p)
{
	p->timeout = AS_POLICY_TIMEOUT_DEFAULT;
	p->retry = AS_POLICY_RETRY_DEFAULT;
	p->key = AS_POLICY_KEY_DEFAULT;
	p->gen = AS_POLICY_GEN_DEFAULT;
	p->replica = AS_POLICY_REPLICA_DEFAULT;
	p->consistency_level = AS_POLICY_CONSISTENCY_LEVEL_DEFAULT;
	p->commit_level = AS_POLICY_COMMIT_LEVEL_DEFAULT;
	return p;
}

/**
 *	Copy as_policy_operate values.
 *
 *	@param src	The source policy.
 *	@param trg	The target policy.
 *
 *	@relates as_policy_operate
 */
static inline void
as_policy_operate_copy(as_policy_operate* src, as_policy_operate* trg)
{
	trg->timeout = src->timeout;
	trg->retry = src->retry;
	trg->key = src->key;
	trg->gen = src->gen;
	trg->replica = src->replica;
	trg->consistency_level = src->consistency_level;
	trg->commit_level = src->commit_level;
}

/**
 *	Initialize as_policy_remove to default values.
 *
 *	@param p	The policy to initialize.
 *	@return		The initialized policy.
 *
 *	@relates as_policy_remove
 */
static inline as_policy_remove*
as_policy_remove_init(as_policy_remove* p)
{
	p->timeout = AS_POLICY_TIMEOUT_DEFAULT;
	p->retry = AS_POLICY_RETRY_DEFAULT;
	p->key = AS_POLICY_KEY_DEFAULT;
	p->gen = AS_POLICY_GEN_DEFAULT;
	p->generation = 0;
	p->commit_level = AS_POLICY_COMMIT_LEVEL_DEFAULT;
	return p;
}

/**
 *	Copy as_policy_remove values.
 *
 *	@param src	The source policy.
 *	@param trg	The target policy.
 *
 *	@relates as_policy_remove
 */
static inline void
as_policy_remove_copy(as_policy_remove* src, as_policy_remove* trg)
{
	trg->timeout = src->timeout;
	trg->retry = src->retry;
	trg->key = src->key;
	trg->gen = src->gen;
	trg->generation = src->generation;
	trg->commit_level = src->commit_level;
}

/**
 *	Initialize as_policy_apply to default values.
 *
 *	@param p	The policy to initialize.
 *	@return		The initialized policy.
 *
 *	@relates as_policy_apply
 */
static inline as_policy_apply*
as_policy_apply_init(as_policy_apply* p)
{
	p->timeout = AS_POLICY_TIMEOUT_DEFAULT;
	p->key = AS_POLICY_KEY_DEFAULT;
	p->commit_level = AS_POLICY_COMMIT_LEVEL_DEFAULT;
	return p;
}

/**
 *	Copy as_policy_apply values.
 *
 *	@param src	The source policy.
 *	@param trg	The target policy.
 *
 *	@relates as_policy_apply
 */
static inline void
as_policy_apply_copy(as_policy_apply* src, as_policy_apply* trg)
{
	trg->timeout = src->timeout;
	trg->key = src->key;
	trg->commit_level = src->commit_level;
}

/**
 *	Initialize as_policy_info to default values.
 *
 *	@param p	The policy to initialize.
 *	@return		The initialized policy.
 *
 *	@relates as_policy_info
 */
static inline as_policy_info*
as_policy_info_init(as_policy_info* p)
{
	p->timeout = AS_POLICY_TIMEOUT_DEFAULT;
	p->send_as_is = true;
	p->check_bounds	= true;
	return p;
}

/**
 *	Copy as_policy_info values.
 *
 *	@param src	The source policy.
 *	@param trg	The target policy.
 *
 *	@relates as_policy_info
 */
static inline void
as_policy_info_copy(as_policy_info* src, as_policy_info* trg)
{
	trg->timeout = src->timeout;
	trg->send_as_is = src->send_as_is;
	trg->check_bounds = src->check_bounds;
}

/**
 *	Initialize as_policy_batch to default values.
 *
 *	@param p	The policy to initialize.
 *	@return		The initialized policy.
 *
 *	@relates as_policy_batch
 */
static inline as_policy_batch*
as_policy_batch_init(as_policy_batch* p)
{
	p->timeout = AS_POLICY_TIMEOUT_DEFAULT;
	return p;
}

/**
 *	Copy as_policy_batch values.
 *
 *	@param src	The source policy.
 *	@param trg	The target policy.
 *
 *	@relates as_policy_batch
 */
static inline void
as_policy_batch_copy(as_policy_batch* src, as_policy_batch* trg)
{
	trg->timeout = src->timeout;
}

/**
 *	Initialize as_policy_admin to default values.
 *
 *	@param p	The policy to initialize.
 *	@return		The initialized policy.
 *
 *	@relates as_policy_admin
 */
static inline as_policy_admin*
as_policy_admin_init(as_policy_admin* p)
{
	p->timeout = AS_POLICY_TIMEOUT_DEFAULT;
	return p;
}

/**
 *	Copy as_policy_admin values.
 *
 *	@param src	The source policy.
 *	@param trg	The target policy.
 *
 *	@relates as_policy_admin
 */
static inline void
as_policy_admin_copy(as_policy_admin* src, as_policy_admin* trg)
{
	trg->timeout = src->timeout;
}

/**
 *	Initialize as_policy_scan to default values.
 *
 *	@param p	The policy to initialize.
 *	@return		The initialized policy.
 *
 *	@relates as_policy_scan
 */
static inline as_policy_scan*
as_policy_scan_init(as_policy_scan* p)
{
	p->timeout = 0;
	p->fail_on_cluster_change = false;
	return p;
}

/**
 *	Copy as_policy_scan values.
 *
 *	@param src	The source policy.
 *	@param trg	The target policy.
 *
 *	@relates as_policy_scan
 */
static inline void
as_policy_scan_copy(as_policy_scan* src, as_policy_scan* trg)
{
	trg->timeout = src->timeout;
	trg->fail_on_cluster_change = src->fail_on_cluster_change;
}

/**
 *	Initialize as_policy_query to default values.
 *
 *	@param p	The policy to initialize.
 *	@return		The initialized policy.
 *
 *	@relates as_policy_query
 */
static inline as_policy_query*
as_policy_query_init(as_policy_query* p)
{
	p->timeout = 0;
	return p;
}

/**
 *	Copy as_policy_query values.
 *
 *	@param src	The source policy.
 *	@param trg	The target policy.
 *
 *	@relates as_policy_query
 */
static inline void
as_policy_query_copy(as_policy_query* src, as_policy_query* trg)
{
	trg->timeout = src->timeout;
}

/**
 *	Initialize as_policies to undefined values.
 *  as_policies_resolve() will later be called resolve undefined values to global defaults.
 *
 *	@param p	The policies to undefine
 *	@return		The undefined policies.
 *
 *	@relates as_policies
 */
as_policies*
as_policies_init(as_policies* p);

/**
 *	Resolve global policies (like timeout) with operational policies (like as_policy_read).
 *
 *	@param p	The policies to resolve
 *
 *	@relates as_policies
 */
void
as_policies_resolve(as_policies* p);
