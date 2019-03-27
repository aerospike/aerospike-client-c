/*
 * Copyright 2008-2019 Aerospike, Inc.
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
 * @defgroup client_policies Client Policies
 * 
 * Policies define the behavior of database operations. 
 *
 * Policies fall into two groups: policy values and operation policies.
 * A policy value is a single value which defines how the client behaves. An
 * operation policy is a group of policy values which affect an operation.
 *
 * ## Policy Values
 *
 * The following are the policy values. For details, please see the documentation
 * for each policy value
 *
 * - as_policy_key
 * - as_policy_gen
 * - as_policy_exists
 * - as_policy_replica
 * - as_policy_read_mode_ap
 * - as_policy_read_mode_sc
 * - as_policy_commit_level
 *
 * ## Operation Policies
 *
 * The following are the operation policies. Operation policies are groups of
 * policy values for a type of operation.
 *
 * - as_policy_batch
 * - as_policy_info
 * - as_policy_operate
 * - as_policy_read
 * - as_policy_remove
 * - as_policy_query
 * - as_policy_scan
 * - as_policy_write
 */

#include <aerospike/as_std.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * MACROS
 *****************************************************************************/

/**
 * Default socket idle timeout value
 *
 * @ingroup client_policies
 */
#define AS_POLICY_SOCKET_TIMEOUT_DEFAULT 30000

/**
 * Default total timeout value
 *
 * @ingroup client_policies
 */
#define AS_POLICY_TOTAL_TIMEOUT_DEFAULT 1000

/**
 * Default value for compression threshold
 *
 * @ingroup client_policies
 */
#define AS_POLICY_COMPRESSION_THRESHOLD_DEFAULT 0

/**
 * Default as_policy_gen value
 *
 * @ingroup client_policies
 */
#define AS_POLICY_GEN_DEFAULT AS_POLICY_GEN_IGNORE

/**
 * Default as_policy_key value
 *
 * @ingroup client_policies
 */
#define AS_POLICY_KEY_DEFAULT AS_POLICY_KEY_DIGEST

/**
 * Default as_policy_exists value
 *
 * @ingroup client_policies
 */
#define AS_POLICY_EXISTS_DEFAULT AS_POLICY_EXISTS_IGNORE

/**
 * Default as_policy_replica value
 *
 * @ingroup client_policies
 */
#define AS_POLICY_REPLICA_DEFAULT AS_POLICY_REPLICA_SEQUENCE

/**
 * Default as_policy_read_mode_ap value
 *
 * @ingroup client_policies
 */
#define AS_POLICY_READ_MODE_AP_DEFAULT AS_POLICY_READ_MODE_AP_ONE

/**
 * Default as_policy_read_mode_sc value
 *
 * @ingroup client_policies
 */
#define AS_POLICY_READ_MODE_SC_DEFAULT AS_POLICY_READ_MODE_SC_SESSION

/**
 * Default as_policy_commit_level value for write
 *
 * @ingroup client_policies
 */
#define AS_POLICY_COMMIT_LEVEL_DEFAULT AS_POLICY_COMMIT_LEVEL_ALL

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * Retry Policy
 *
 * Specifies the behavior of failed operations. 
 *
 * @ingroup client_policies
 */
typedef enum as_policy_retry_e {

	/**
	 * Only attempt an operation once.
	 */
	AS_POLICY_RETRY_NONE, 

	/**
	 * If an operation fails, attempt the operation
	 * one more time.
	 */
	AS_POLICY_RETRY_ONCE, 

} as_policy_retry;

/**
 * Generation Policy
 *
 * Specifies the behavior of record modifications with regard to the 
 * generation value.
 *
 * @ingroup client_policies
 */
typedef enum as_policy_gen_e {

	/**
	 * Write a record, regardless of generation.
	 */
	AS_POLICY_GEN_IGNORE,

	/**
	 * Write a record, ONLY if generations are equal
	 */
	AS_POLICY_GEN_EQ,

	/**
	 * Write a record, ONLY if local generation is 
	 * greater-than remote generation
	 */
	AS_POLICY_GEN_GT

} as_policy_gen;

/**
 * Key Policy
 *
 * Specifies the behavior for whether keys or digests
 * should be sent to the cluster.
 *
 * @ingroup client_policies
 */
typedef enum as_policy_key_e {

	/**
	 * Send the digest value of the key.
	 *
	 * This is the recommended mode of operation. This calculates the digest
	 * and send the digest to the server. The digest is only calculated on
	 * the client, and not on the server.
	 */
	AS_POLICY_KEY_DIGEST,

	/**
	 * Send the key, in addition to the digest value.
	 *
	 * If you want keys to be returned when scanning or querying, the keys must
	 * be stored on the server. This policy causes a write operation to store
	 * the key. Once a key is stored, the server will keep it - there is no
	 * need to use this policy on subsequent updates of the record.
	 *
	 * If this policy is used on read or delete operations, or on subsequent
	 * updates of a record with a stored key, the key sent will be compared
	 * with the key stored on the server. A mismatch will cause
	 * AEROSPIKE_ERR_RECORD_KEY_MISMATCH to be returned.
	 */
	AS_POLICY_KEY_SEND,

} as_policy_key;

/**
 * Existence Policy
 * 
 * Specifies the behavior for writing the record
 * depending whether or not it exists.
 *
 * @ingroup client_policies
 */
typedef enum as_policy_exists_e {

	/**
	 * Write the record, regardless of existence. (i.e. create or update.)
	 */
	AS_POLICY_EXISTS_IGNORE,

	/**
	 * Create a record, ONLY if it doesn't exist.
	 */
	AS_POLICY_EXISTS_CREATE,

	/**
	 * Update a record, ONLY if it exists.
	 */
	AS_POLICY_EXISTS_UPDATE,

	/**
	 * Completely replace a record, ONLY if it exists.
	 */
	AS_POLICY_EXISTS_REPLACE,

	/**
	 * Completely replace a record if it exists, otherwise create it.
	 */
	AS_POLICY_EXISTS_CREATE_OR_REPLACE

} as_policy_exists;

/**
 * Replica Policy
 *
 * Specifies which partition replica to read from.
 *
 * @ingroup client_policies
 */
typedef enum as_policy_replica_e {

	/**
	 * Read from the partition master replica node.
	 */
	AS_POLICY_REPLICA_MASTER,

	/**
	 * Distribute reads across nodes containing key's master and replicated partition
	 * in round-robin fashion.  Currently restricted to master and one prole.
	 */
	AS_POLICY_REPLICA_ANY,

	/**
	 * Always try node containing master partition first. If connection fails and
	 * `retry_on_timeout` is true, try node containing prole partition.
	 * Currently restricted to master and one prole.
	 */
	AS_POLICY_REPLICA_SEQUENCE,

	/**
	 * Try node on the same rack as the client first.  If there are no nodes on the
	 * same rack, use SEQUENCE instead.
	 *
	 * as_config.rack_aware, as_config.rack_id, and server rack configuration must also
	 * be set to enable this functionality.
	 */
	AS_POLICY_REPLICA_PREFER_RACK

} as_policy_replica;

/**
 * Read policy for AP (availability) namespaces.
 *
 * How duplicates should be consulted in a read operation.
 * Only makes a difference during migrations and only applicable in AP mode.
 *
 * @ingroup client_policies
 */
typedef enum as_policy_read_mode_ap_e {

	/**
	 * Involve single node in the read operation.
	 */
	AS_POLICY_READ_MODE_AP_ONE,

	/**
	 * Involve all duplicates in the read operation.
	 */
	AS_POLICY_READ_MODE_AP_ALL,

} as_policy_read_mode_ap;

/**
 * Read policy for SC (strong consistency) namespaces.
 *
 * Determines SC read consistency options.
 *
 * @ingroup client_policies
 */
typedef enum as_policy_read_mode_sc_e {

	/**
	 * Ensures this client will only see an increasing sequence of record versions.
	 * Server only reads from master.  This is the default.
	 */
	AS_POLICY_READ_MODE_SC_SESSION,

	/**
	 * Ensures ALL clients will only see an increasing sequence of record versions.
	 * Server only reads from master.
	 */
	AS_POLICY_READ_MODE_SC_LINEARIZE,

	/**
	 * Server may read from master or any full (non-migrating) replica.
	 * Increasing sequence of record versions is not guaranteed.
	 */
	AS_POLICY_READ_MODE_SC_ALLOW_REPLICA,

	/**
	 * Server may read from master or any full (non-migrating) replica or from unavailable
	 * partitions.  Increasing sequence of record versions is not guaranteed.
	 */
	AS_POLICY_READ_MODE_SC_ALLOW_UNAVAILABLE,

} as_policy_read_mode_sc;

/**
 * Commit Level
 *
 * Specifies the number of replicas required to be successfully
 * committed before returning success in a write operation
 * to provide the desired consistency guarantee.
 *
 * @ingroup client_policies
 */
typedef enum as_policy_commit_level_e {

	/**
	 * Return succcess only after successfully committing all replicas.
	 */
	AS_POLICY_COMMIT_LEVEL_ALL,

	/**
	 * Return succcess after successfully committing the master replica.
	 */
	AS_POLICY_COMMIT_LEVEL_MASTER,

} as_policy_commit_level;

/**
 * Generic policy fields shared among all policies.
 *
 * @ingroup client_policies
 */
typedef struct as_policy_base_s {

	/**
	 * Socket idle timeout in milliseconds when processing a database command.
	 *
	 * If socket_timeout is zero and total_timeout is non-zero, then socket_timeout will be set
	 * to total_timeout.  If both socket_timeout and total_timeout are non-zero and
	 * socket_timeout > total_timeout, then socket_timeout will be set to total_timeout. If both
	 * socket_timeout and total_timeout are zero, then there will be no socket idle limit.
	 *
	 * If socket_timeout is non-zero and the socket has been idle for at least socket_timeout,
	 * both max_retries and total_timeout are checked.  If max_retries and total_timeout are not
	 * exceeded, the transaction is retried.
	 *
	 * Default: 30000ms
	 */
	uint32_t socket_timeout;

	/**
	 * Total transaction timeout in milliseconds.
	 *
	 * The total_timeout is tracked on the client and sent to the server along with
	 * the transaction in the wire protocol.  The client will most likely timeout
	 * first, but the server also has the capability to timeout the transaction.
	 *
	 * If total_timeout is not zero and total_timeout is reached before the transaction
	 * completes, the transaction will return error AEROSPIKE_ERR_TIMEOUT.
	 * If totalTimeout is zero, there will be no total time limit.
	 *
	 * Default: 1000
	 */
	uint32_t total_timeout;

	/**
	 * Maximum number of retries before aborting the current transaction.
	 * The initial attempt is not counted as a retry.
	 *
	 * If max_retries is exceeded, the transaction will return error AEROSPIKE_ERR_TIMEOUT.
	 *
	 * WARNING: Database writes that are not idempotent (such as "add")
	 * should not be retried because the write operation may be performed
	 * multiple times if the client timed out previous transaction attempts.
	 * It's important to use a distinct write policy for non-idempotent
	 * writes which sets max_retries = 0;
	 *
	 * Default for read: 2 (initial attempt + 2 retries = 3 attempts)
	 * Default for write/query/scan: 0 (no retries)
	 */
	uint32_t max_retries;

	/**
	 * Milliseconds to sleep between retries.  Enter zero to skip sleep.
	 * This field is ignored when max_retries is zero.  
	 * This field is also ignored in async mode.
	 *
	 * Reads do not have to sleep when a node goes down because the cluster
	 * does not shut out reads during cluster reformation.  The default for
	 * reads is zero.
	 *
	 * The default for writes is also zero because writes are not retried by default.
	 * Writes need to wait for the cluster to reform when a node goes down.
	 * Immediate write retries on node failure have been shown to consistently
	 * result in errors.  If max_retries is greater than zero on a write, then
	 * sleep_between_retries should be set high enough to allow the cluster to
	 * reform (>= 500ms).
	 *
	 * Default: 0 (do not sleep between retries).
	 */
	uint32_t sleep_between_retries;

} as_policy_base;

/**
 * Read Policy
 *
 * @ingroup client_policies
 */
typedef struct as_policy_read_s {

	/**
	 * Generic policy fields.
	 */
	as_policy_base base;

	/**
	 * Specifies the behavior for the key.
	 */
	as_policy_key key;

	/**
	 * Specifies the replica to be consulted for the read.
	 */
	as_policy_replica replica;

	/**
	 * Read policy for AP (availability) namespaces.
	 * Default: AS_POLICY_READ_MODE_AP_ONE
	 */
	as_policy_read_mode_ap read_mode_ap;

	/**
	 * Read policy for SC (strong consistency) namespaces.
	 * Default: AS_POLICY_READ_MODE_SC_SESSION
	 */
	as_policy_read_mode_sc read_mode_sc;

	/**
	 * Should raw bytes representing a list or map be deserialized to as_list or as_map.
	 * Set to false for backup programs that just need access to raw bytes.
	 * Default: true
	 */
	bool deserialize;

} as_policy_read;
	
/**
 * Write Policy
 *
 * @ingroup client_policies
 */
typedef struct as_policy_write_s {

	/**
	 * Generic policy fields.
	 */
	as_policy_base base;

	/**
	 * Specifies the behavior for the key.
	 */
	as_policy_key key;

	/**
	 * Specifies the replica to be consulted.
	 */
	as_policy_replica replica;

	/**
	 * Specifies the number of replicas required to be committed successfully when writing
	 * before returning transaction succeeded.
	 */
	as_policy_commit_level commit_level;

	/**
	 * Specifies the behavior for the generation value.
	 */
	as_policy_gen gen;

	/**
	 * Specifies the behavior for the existence of the record.
	 */
	as_policy_exists exists;
	
	/**
	 * Minimum record size beyond which it is compressed and sent to the server.
	 */
	uint32_t compression_threshold;

	/**
	 * If the transaction results in a record deletion, leave a tombstone for the record.
	 * This prevents deleted records from reappearing after node failures.
	 * Valid for Aerospike Server Enterprise Edition only.
	 *
	 * Default: false (do not tombstone deleted records).
	 */
	bool durable_delete;

} as_policy_write;

/**
 * Key Apply Policy
 *
 * @ingroup client_policies
 */
typedef struct as_policy_apply_s {

	/**
	 * Generic policy fields.
	 */
	as_policy_base base;

	/**
	 * Specifies the behavior for the key.
	 */
	as_policy_key key;

	/**
	 * Specifies the replica to be consulted for the read.
	 */
	as_policy_replica replica;

	/**
	 * Specifies the number of replicas required to be committed successfully when writing
	 * before returning transaction succeeded.
	 */
	as_policy_commit_level commit_level;

	/**
	 * The time-to-live (expiration) of the record in seconds.
	 * There are also special values that can be set in the record TTL:
	 * (*) ZERO (defined as AS_RECORD_DEFAULT_TTL), which means that the
	 *    record will adopt the default TTL value from the namespace.
	 * (*) 0xFFFFFFFF (also, -1 in a signed 32 bit int)
	 *    (defined as AS_RECORD_NO_EXPIRE_TTL), which means that the record
	 *    will get an internal "void_time" of zero, and thus will never expire.
	 * (*) 0xFFFFFFFE (also, -2 in a signed 32 bit int)
	 *    (defined as AS_RECORD_NO_CHANGE_TTL), which means that the record
	 *    ttl will not change when the record is updated.
	 *
	 * Note that the TTL value will be employed ONLY on write/update calls.
	 */
	uint32_t ttl;

	/**
	 * Specifies the behavior for the generation value.
	 */
	as_policy_gen gen;

	/**
	 * The expected generation of the record.
	 */
	uint16_t gen_value;

	/**
	 * If the transaction results in a record deletion, leave a tombstone for the record.
	 * This prevents deleted records from reappearing after node failures.
	 * Valid for Aerospike Server Enterprise Edition only.
	 *
	 * Default: false (do not tombstone deleted records).
	 */
	bool durable_delete;

} as_policy_apply;

/**
 * Operate Policy
 *
 * @ingroup client_policies
 */
typedef struct as_policy_operate_s {

	/**
	 * Generic policy fields.
	 */
	as_policy_base base;

	/**
	 * Specifies the behavior for the key.
	 */
	as_policy_key key;

	/**
	 * Specifies the replica to be consulted for the read.
	 */
	as_policy_replica replica;

	/**
	 * Read policy for AP (availability) namespaces.
	 * Default: AS_POLICY_READ_MODE_AP_ONE
	 */
	as_policy_read_mode_ap read_mode_ap;

	/**
	 * Read policy for SC (strong consistency) namespaces.
	 * Default: AS_POLICY_READ_MODE_SC_SESSION
	 */
	as_policy_read_mode_sc read_mode_sc;

	/**
	 * Specifies the number of replicas required to be committed successfully when writing
	 * before returning transaction succeeded.
	 */
	as_policy_commit_level commit_level;

	/**
	 * Specifies the behavior for the generation value.
	 */
	as_policy_gen gen;

	/**
	 * Specifies the behavior for the existence of the record.
	 */
	as_policy_exists exists;

	/**
	 * Should raw bytes representing a list or map be deserialized to as_list or as_map.
	 * Set to false for backup programs that just need access to raw bytes.
	 * Default: true
	 */
	bool deserialize;

	/**
	 * If the transaction results in a record deletion, leave a tombstone for the record.
	 * This prevents deleted records from reappearing after node failures.
	 * Valid for Aerospike Server Enterprise Edition only.
	 *
	 * Default: false (do not tombstone deleted records).
	 */
	bool durable_delete;

} as_policy_operate;

/**
 * Remove Policy
 *
 * @ingroup client_policies
 */
typedef struct as_policy_remove_s {

	/**
	 * Generic policy fields.
	 */
	as_policy_base base;

	/**
	 * Specifies the behavior for the key.
	 */
	as_policy_key key;

	/**
	 * Specifies the replica to be consulted for the read.
	 */
	as_policy_replica replica;

	/**
	 * Specifies the number of replicas required to be committed successfully when writing
	 * before returning transaction succeeded.
	 */
	as_policy_commit_level commit_level;

	/**
	 * Specifies the behavior for the generation value.
	 */
	as_policy_gen gen;

	/**
	 * The generation of the record.
	 */
	uint16_t generation;

	/**
	 * If the transaction results in a record deletion, leave a tombstone for the record.
	 * This prevents deleted records from reappearing after node failures.
	 * Valid for Aerospike Server Enterprise Edition only.
	 *
	 * Default: false (do not tombstone deleted records).
	 */
	bool durable_delete;

} as_policy_remove;

/**
 * Batch Policy
 *
 * @ingroup client_policies
 */
typedef struct as_policy_batch_s {

	/**
	 * Generic policy fields.
	 */
	as_policy_base base;

	/**
	 * Specifies the replica to be consulted for the read.
	 */
	as_policy_replica replica;

	/**
	 * Read policy for AP (availability) namespaces.
	 * Default: AS_POLICY_READ_MODE_AP_ONE
	 */
	as_policy_read_mode_ap read_mode_ap;

	/**
	 * Read policy for SC (strong consistency) namespaces.
	 * Default: AS_POLICY_READ_MODE_SC_SESSION
	 */
	as_policy_read_mode_sc read_mode_sc;

	/**
	 * Determine if batch commands to each server are run in parallel threads.
	 *
	 * Values:
	 * <ul>
	 * <li>
	 * false: Issue batch commands sequentially.  This mode has a performance advantage for small
	 * to medium sized batch sizes because commands can be issued in the main transaction thread.
	 * This is the default.
	 * </li>
	 * <li>
	 * true: Issue batch commands in parallel threads.  This mode has a performance
	 * advantage for large batch sizes because each node can process the command immediately.
	 * The downside is extra threads will need to be created (or taken from
	 * a thread pool).
	 * </li>
	 * </ul>
	 */
	bool concurrent;

	/**
	 * Allow batch to be processed immediately in the server's receiving thread when the server
	 * deems it to be appropriate.  If false, the batch will always be processed in separate
	 * transaction threads.  This field is only relevant for the new batch index protocol.
	 *
	 * For batch exists or batch reads of smaller sized records (<= 1K per record), inline
	 * processing will be significantly faster on "in memory" namespaces.  The server disables
	 * inline processing on disk based namespaces regardless of this policy field.
	 *
	 * Inline processing can introduce the possibility of unfairness because the server
	 * can process the entire batch before moving onto the next command.
	 * Default: true
	 */
	bool allow_inline;

	/**
	 * Send set name field to server for every key in the batch for batch index protocol.
	 * This is only necessary when authentication is enabled and security roles are defined
	 * on a per set basis.
	 * Default: false
	 */
	bool send_set_name;

	/**
	 * Should raw bytes be deserialized to as_list or as_map. Set to false for backup programs that
	 * just need access to raw bytes.
	 * Default: true
	 */
	bool deserialize;

} as_policy_batch;
	
/**
 * Query Policy
 *
 * @ingroup client_policies
 */
typedef struct as_policy_query_s {

	/**
	 * Generic policy fields.
	 */
	as_policy_base base;

	/**
	 * Terminate query if cluster is in migration state.
	 */
	bool fail_on_cluster_change;

	/**
	 * Should raw bytes representing a list or map be deserialized to as_list or as_map.
	 * Set to false for backup programs that just need access to raw bytes.
	 * Default: true
	 */
	bool deserialize;

} as_policy_query;

/**
 * Scan Policy
 *
 * @ingroup client_policies
 */
typedef struct as_policy_scan_s {

	/**
	 * Generic policy fields.
	 */
	as_policy_base base;

	/**
	 * Terminate scan if cluster is in migration state.
	 */
	bool fail_on_cluster_change;

	/**
	 * If the transaction results in a record deletion, leave a tombstone for the record.
	 * This prevents deleted records from reappearing after node failures.
	 * Valid for Aerospike Server Enterprise Edition only.
	 *
	 * Default: false (do not tombstone deleted records).
	 */
	bool durable_delete;

} as_policy_scan;

/**
 * Info Policy
 *
 * @ingroup client_policies
 */
typedef struct as_policy_info_s {

	/**
	 * Maximum time in milliseconds to wait for the operation to complete.
	 */
	uint32_t timeout;

	/**
	 * Send request without any further processing.
	 */
	bool send_as_is;

	/**
	 * Ensure the request is within allowable size limits.
	 */
	bool check_bounds;
	
} as_policy_info;
	
/**
 * Administration Policy
 *
 * @ingroup client_policies
 */
typedef struct as_policy_admin_s {
	
	/**
	 * Maximum time in milliseconds to wait for the operation to complete.
	 */
	uint32_t timeout;
	
} as_policy_admin;

/**
 * Struct of all policy values and operation policies. 
 * 
 * This is utilized by as_config to define default values for policies.
 *
 * @ingroup as_config_t
 */
typedef struct as_policies_s {

	/**
	 * The default read policy.
	 */
	as_policy_read read;

	/**
	 * The default write policy.
	 */
	as_policy_write write;

	/**
	 * The default operate policy.
	 */
	as_policy_operate operate;

	/**
	 * The default remove policy.
	 */
	as_policy_remove remove;

	/**
	 * The default apply policy.
	 */
	as_policy_apply apply;

	/**
	 * The default batch policy.
	 */
	as_policy_batch batch;

	/**
	 * The default scan policy.
	 */
	as_policy_scan scan;

	/**
	 * The default query policy.
	 */
	as_policy_query query;

	/**
	 * The default info policy.
	 */
	as_policy_info info;

	/**
	 * The default administration policy.
	 */
	as_policy_admin admin;

} as_policies;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * Initialize as_policy_read to default values.
 *
 * @param p	The policy to initialize.
 * @return	The initialized policy.
 *
 * @relates as_policy_read
 */
static inline as_policy_read*
as_policy_read_init(as_policy_read* p)
{
	p->base.socket_timeout = AS_POLICY_SOCKET_TIMEOUT_DEFAULT;
	p->base.total_timeout = AS_POLICY_TOTAL_TIMEOUT_DEFAULT;
	p->base.max_retries = 2;
	p->base.sleep_between_retries = 0;
	p->key = AS_POLICY_KEY_DEFAULT;
	p->replica = AS_POLICY_REPLICA_DEFAULT;
	p->read_mode_ap = AS_POLICY_READ_MODE_AP_DEFAULT;
	p->read_mode_sc = AS_POLICY_READ_MODE_SC_DEFAULT;
	p->deserialize = true;
	return p;
}

/**
 * Copy as_policy_read values.
 *
 * @param src	The source policy.
 * @param trg	The target policy.
 *
 * @relates as_policy_read
 */
static inline void
as_policy_read_copy(const as_policy_read* src, as_policy_read* trg)
{
	*trg = *src;
}

/**
 * Initialize as_policy_write to default values.
 *
 * @param p	The policy to initialize.
 * @return	The initialized policy.
 *
 * @relates as_policy_write
 */
static inline as_policy_write*
as_policy_write_init(as_policy_write* p)
{
	p->base.socket_timeout = AS_POLICY_SOCKET_TIMEOUT_DEFAULT;
	p->base.total_timeout = AS_POLICY_TOTAL_TIMEOUT_DEFAULT;
	p->base.max_retries = 0;
	p->base.sleep_between_retries = 0;
	p->key = AS_POLICY_KEY_DEFAULT;
	p->replica = AS_POLICY_REPLICA_DEFAULT;
	p->commit_level = AS_POLICY_COMMIT_LEVEL_DEFAULT;
	p->gen = AS_POLICY_GEN_DEFAULT;
	p->exists = AS_POLICY_EXISTS_DEFAULT;
	p->compression_threshold = AS_POLICY_COMPRESSION_THRESHOLD_DEFAULT;
	p->durable_delete = false;
	return p;
}

/**
 * Copy as_policy_write values.
 *
 * @param src	The source policy.
 * @param trg	The target policy.
 *
 * @relates as_policy_write
 */
static inline void
as_policy_write_copy(const as_policy_write* src, as_policy_write* trg)
{
	*trg = *src;
}

/**
 * Initialize as_policy_operate to default values.
 *
 * @param p	The policy to initialize.
 * @return	The initialized policy.
 *
 * @relates as_policy_operate
 */
static inline as_policy_operate*
as_policy_operate_init(as_policy_operate* p)
{
	p->base.socket_timeout = AS_POLICY_SOCKET_TIMEOUT_DEFAULT;
	p->base.total_timeout = AS_POLICY_TOTAL_TIMEOUT_DEFAULT;
	p->base.max_retries = 0;
	p->base.sleep_between_retries = 0;
	p->key = AS_POLICY_KEY_DEFAULT;
	p->replica = AS_POLICY_REPLICA_DEFAULT;
	p->read_mode_ap = AS_POLICY_READ_MODE_AP_DEFAULT;
	p->read_mode_sc = AS_POLICY_READ_MODE_SC_DEFAULT;
	p->commit_level = AS_POLICY_COMMIT_LEVEL_DEFAULT;
	p->gen = AS_POLICY_GEN_DEFAULT;
	p->exists = AS_POLICY_EXISTS_DEFAULT;
	p->deserialize = true;
	p->durable_delete = false;
	return p;
}

/**
 * Copy as_policy_operate values.
 *
 * @param src	The source policy.
 * @param trg	The target policy.
 *
 * @relates as_policy_operate
 */
static inline void
as_policy_operate_copy(const as_policy_operate* src, as_policy_operate* trg)
{
	*trg = *src;
}

/**
 * Initialize as_policy_remove to default values.
 *
 * @param p	The policy to initialize.
 * @return	The initialized policy.
 *
 * @relates as_policy_remove
 */
static inline as_policy_remove*
as_policy_remove_init(as_policy_remove* p)
{
	p->base.socket_timeout = AS_POLICY_SOCKET_TIMEOUT_DEFAULT;
	p->base.total_timeout = AS_POLICY_TOTAL_TIMEOUT_DEFAULT;
	p->base.max_retries = 0;
	p->base.sleep_between_retries = 0;
	p->key = AS_POLICY_KEY_DEFAULT;
	p->replica = AS_POLICY_REPLICA_DEFAULT;
	p->commit_level = AS_POLICY_COMMIT_LEVEL_DEFAULT;
	p->gen = AS_POLICY_GEN_DEFAULT;
	p->generation = 0;
	p->durable_delete = false;
	return p;
}

/**
 * Copy as_policy_remove values.
 *
 * @param src	The source policy.
 * @param trg	The target policy.
 *
 * @relates as_policy_remove
 */
static inline void
as_policy_remove_copy(const as_policy_remove* src, as_policy_remove* trg)
{
	*trg = *src;
}

/**
 * Initialize as_policy_apply to default values.
 *
 * @param p	The policy to initialize.
 * @return	The initialized policy.
 *
 * @relates as_policy_apply
 */
static inline as_policy_apply*
as_policy_apply_init(as_policy_apply* p)
{
	p->base.socket_timeout = AS_POLICY_SOCKET_TIMEOUT_DEFAULT;
	p->base.total_timeout = AS_POLICY_TOTAL_TIMEOUT_DEFAULT;
	p->base.max_retries = 0;
	p->base.sleep_between_retries = 0;
	p->key = AS_POLICY_KEY_DEFAULT;
	p->replica = AS_POLICY_REPLICA_DEFAULT;
	p->commit_level = AS_POLICY_COMMIT_LEVEL_DEFAULT;
	p->ttl = 0; // AS_RECORD_DEFAULT_TTL
	p->gen = AS_POLICY_GEN_DEFAULT;
	p->gen_value = 0;
	p->durable_delete = false;
	return p;
}

/**
 * Copy as_policy_apply values.
 *
 * @param src	The source policy.
 * @param trg	The target policy.
 *
 * @relates as_policy_apply
 */
static inline void
as_policy_apply_copy(const as_policy_apply* src, as_policy_apply* trg)
{
	*trg = *src;
}

/**
 * Initialize as_policy_batch to default values.
 *
 * @param p	The policy to initialize.
 * @return	The initialized policy.
 *
 * @relates as_policy_batch
 */
static inline as_policy_batch*
as_policy_batch_init(as_policy_batch* p)
{
	p->base.socket_timeout = AS_POLICY_SOCKET_TIMEOUT_DEFAULT;
	p->base.total_timeout = AS_POLICY_TOTAL_TIMEOUT_DEFAULT;
	p->base.max_retries = 2;
	p->base.sleep_between_retries = 0;
	p->replica = AS_POLICY_REPLICA_SEQUENCE;
	p->read_mode_ap = AS_POLICY_READ_MODE_AP_DEFAULT;
	p->read_mode_sc = AS_POLICY_READ_MODE_SC_DEFAULT;
	p->concurrent = false;
	p->allow_inline = true;
	p->send_set_name = false;
	p->deserialize = true;
	return p;
}

/**
 * Copy as_policy_batch values.
 *
 * @param src	The source policy.
 * @param trg	The target policy.
 *
 * @relates as_policy_batch
 */
static inline void
as_policy_batch_copy(const as_policy_batch* src, as_policy_batch* trg)
{
	*trg = *src;
}

/**
 * Initialize as_policy_scan to default values.
 *
 * @param p	The policy to initialize.
 * @return	The initialized policy.
 *
 * @relates as_policy_scan
 */
static inline as_policy_scan*
as_policy_scan_init(as_policy_scan* p)
{
	p->base.socket_timeout = AS_POLICY_SOCKET_TIMEOUT_DEFAULT;
	p->base.total_timeout = 0;
	p->base.max_retries = 0;
	p->base.sleep_between_retries = 0;
	p->fail_on_cluster_change = false;
	p->durable_delete = false;
	return p;
}

/**
 * Copy as_policy_scan values.
 *
 * @param src	The source policy.
 * @param trg	The target policy.
 *
 * @relates as_policy_scan
 */
static inline void
as_policy_scan_copy(const as_policy_scan* src, as_policy_scan* trg)
{
	*trg = *src;
}

/**
 * Initialize as_policy_query to default values.
 *
 * @param p	The policy to initialize.
 * @return	The initialized policy.
 *
 * @relates as_policy_query
 */
static inline as_policy_query*
as_policy_query_init(as_policy_query* p)
{
	p->base.socket_timeout = AS_POLICY_SOCKET_TIMEOUT_DEFAULT;
	p->base.total_timeout = 0;
	p->base.max_retries = 0;
	p->base.sleep_between_retries = 0;
	p->fail_on_cluster_change = false;
	p->deserialize = true;
	return p;
}

/**
 * Copy as_policy_query values.
 *
 * @param src	The source policy.
 * @param trg	The target policy.
 *
 * @relates as_policy_query
 */
static inline void
as_policy_query_copy(const as_policy_query* src, as_policy_query* trg)
{
	*trg = *src;
}

/**
 * Initialize as_policy_info to default values.
 *
 * @param p	The policy to initialize.
 * @return	The initialized policy.
 *
 * @relates as_policy_info
 */
static inline as_policy_info*
as_policy_info_init(as_policy_info* p)
{
	p->timeout = AS_POLICY_TOTAL_TIMEOUT_DEFAULT;
	p->send_as_is = true;
	p->check_bounds	= true;
	return p;
}

/**
 * Copy as_policy_info values.
 *
 * @param src	The source policy.
 * @param trg	The target policy.
 *
 * @relates as_policy_info
 */
static inline void
as_policy_info_copy(const as_policy_info* src, as_policy_info* trg)
{
	*trg = *src;
}
	
/**
 * Initialize as_policy_admin to default values.
 *
 * @param p	The policy to initialize.
 * @return	The initialized policy.
 *
 * @relates as_policy_admin
 */
static inline as_policy_admin*
as_policy_admin_init(as_policy_admin* p)
{
	p->timeout = AS_POLICY_TOTAL_TIMEOUT_DEFAULT;
	return p;
}

/**
 * Copy as_policy_admin values.
 *
 * @param src	The source policy.
 * @param trg	The target policy.
 *
 * @relates as_policy_admin
 */
static inline void
as_policy_admin_copy(const as_policy_admin* src, as_policy_admin* trg)
{
	*trg = *src;
}
	
/**
 * Initialize as_policies.
 *
 * @relates as_policies
 */
as_policies*
as_policies_init(as_policies* p);

#ifdef __cplusplus
} // end extern "C"
#endif
