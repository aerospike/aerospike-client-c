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

//---------------------------------
// Macros
//---------------------------------

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

//---------------------------------
// Types
//---------------------------------

struct as_exp;
struct as_txn;

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
	 * Do not use record generation to restrict writes.
	 */
	AS_POLICY_GEN_IGNORE,

	/**
	 * Update/delete record if expected generation is equal to server generation. Otherwise, fail.
	 */
	AS_POLICY_GEN_EQ,

	/**
	 * Update/delete record if expected generation greater than the server generation. 
	 * Otherwise, fail. This is useful for restore after backup.
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
 * Defines algorithm used to determine the target node for a command.
 *
 * @ingroup client_policies
 */
typedef enum as_policy_replica_e {

	/**
	 * Use node containing key's master partition.
	 */
	AS_POLICY_REPLICA_MASTER,

	/**
	 * Distribute reads across nodes containing key's master and replicated partition
	 * in round-robin fashion.
	 */
	AS_POLICY_REPLICA_ANY,

	/**
	 * Try node containing master partition first.
	 * If connection fails, all commands try nodes containing replicated partitions.
	 * If socketTimeout is reached, reads also try nodes containing replicated partitions,
	 * but writes remain on master node.
	 */
	AS_POLICY_REPLICA_SEQUENCE,

	/**
	 * For reads, try node on preferred racks first. If there are no nodes on preferred racks,
	 * use SEQUENCE instead. Also use SEQUENCE for writes.
	 *
	 * as_config.rack_aware, as_config.rack_id or as_config.rack_ids, and server rack 
	 * configuration must also be set to enable this functionality.
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
	 * Client only reads from master.  This is the default.
	 */
	AS_POLICY_READ_MODE_SC_SESSION,

	/**
	 * Ensures all clients will only see an increasing sequence of record versions.
	 * Client only reads from master.
	 */
	AS_POLICY_READ_MODE_SC_LINEARIZE,

	/**
	 * Client may read from master or any full (non-migrating) replica.
	 * Increasing sequence of record versions is not guaranteed.
	 */
	AS_POLICY_READ_MODE_SC_ALLOW_REPLICA,

	/**
	 * Client may read from master or any full (non-migrating) replica or from unavailable
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
 * Expected query duration. The server treats the query in different ways depending on the expected duration.
 * This enum is ignored for aggregation queries, background queries and server versions &lt; 6.0.
 *
 * @ingroup client_policies
 */
typedef enum as_query_duration_e {

	/**
	 * The query is expected to return more than 100 records per node. The server optimizes for a
	 * large record set in the following ways:
	 * <ul>
	 * <li>Allow query to be run in multiple threads using the server's query threading configuration.</li>
	 * <li>Do not relax read consistency for AP namespaces.</li>
	 * <li>Add the query to the server's query monitor.</li>
	 * <li>Do not add the overall latency to the server's latency histogram.</li>
	 * <li>Do not allow server timeouts.</li>
	 * </ul>
	 */
	AS_QUERY_DURATION_LONG,

	/**
	 * The query is expected to return less than 100 records per node. The server optimizes for a
	 * small record set in the following ways:
	 * <ul>
	 * <li>Always run the query in one thread and ignore the server's query threading configuration.</li>
	 * <li>Allow query to be inlined directly on the server's service thread.</li>
	 * <li>Relax read consistency for AP namespaces.</li>
	 * <li>Do not add the query to the server's query monitor.</li>
	 * <li>Add the overall latency to the server's latency histogram.</li>
	 * <li>Allow server timeouts. The default server timeout for a short query is 1 second.</li>
	 * </ul>
	 */
	AS_QUERY_DURATION_SHORT,
	
	/**
	 * Treat query as a LONG query, but relax read consistency for AP namespaces.
	 * This value is treated exactly like LONG for server versions &lt; 7.1.
	 */
	AS_QUERY_DURATION_LONG_RELAX_AP

} as_query_duration;

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
	 * exceeded, the command is retried.
	 *
	 * Default: 30000ms
	 */
	uint32_t socket_timeout;

	/**
	 * Total command timeout in milliseconds.
	 *
	 * The total_timeout is tracked on the client and sent to the server along with
	 * the command in the wire protocol.  The client will most likely timeout
	 * first, but the server also has the capability to timeout the command.
	 *
	 * If total_timeout is not zero and total_timeout is reached before the command
	 * completes, the command will return error AEROSPIKE_ERR_TIMEOUT.
	 * If totalTimeout is zero, there will be no total time limit.
	 *
	 * Default: 1000
	 */
	uint32_t total_timeout;

	/**
	 * Maximum number of retries before aborting the current command.
	 * The initial attempt is not counted as a retry.
	 *
	 * If max_retries is exceeded, the command will return error AEROSPIKE_ERR_TIMEOUT.
	 *
	 * WARNING: Database writes that are not idempotent (such as "add")
	 * should not be retried because the write operation may be performed
	 * multiple times if the client timed out previous command attempts.
	 * It's important to use a distinct write policy for non-idempotent
	 * writes which sets max_retries = 0;
	 *
	 * Default for read: 2 (initial attempt + 2 retries = 3 attempts)
	 *
	 * Default for write: 0 (no retries)
     *
	 * Default for partition scan or query with null filter: 5
	 *
	 * No default for legacy scan/query. No retries are allowed for these commands.
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
	 * reform (>= 3000ms).
	 *
	 * Default: 0 (do not sleep between retries).
	 */
	uint32_t sleep_between_retries;

	/**
	 * Optional expression filter. If filter_exp exists and evaluates to false, the
	 * command is ignored. This can be used to eliminate a client/server roundtrip
	 * in some cases.
	 *
	 * aerospike_destroy() automatically calls as_exp_destroy() on all global default 
	 * policy filter expression instances. The user is responsible for calling as_exp_destroy()
	 * on filter expressions when setting temporary command policies.
	 *
	 * ~~~~~~~~~~{.c}
	 * as_exp_build(filter,
	 *   as_exp_cmp_eq(as_exp_bin_int("a"), as_exp_int(10)));
	 *
	 * as_policy_read p;
	 * as_policy_read_init(&p);
	 * p.filter_exp = filter;
	 * ...
	 * as_exp_destroy(filter);
	 * ~~~~~~~~~~
	 *
	 * Default: NULL
	 */
	struct as_exp* filter_exp;
	
	/**
	 * Transaction identifier. If set for an async command,  the source txn instance must
	 * be allocated on the heap using as_txn_create() or as_txn_create_capacity().
	 *
	 * Default: NULL
	 */
	struct as_txn* txn;

	/**
	 * Use zlib compression on write or batch read commands when the command buffer size is greater
	 * than 128 bytes.  In addition, tell the server to compress it's response on read commands.
	 * The server response compression threshold is also 128 bytes.
	 *
	 * This option will increase cpu and memory usage (for extra compressed buffers), but
	 * decrease the size of data sent over the network.
	 *
	 * This compression feature requires the Enterprise Edition Server.
	 *
	 * Default: false
	 */
	bool compress;

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
	 * Algorithm used to determine target node.
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
	 * Determine how record TTL (time to live) is affected on reads. When enabled, the server can
	 * efficiently operate as a read-based LRU cache where the least recently used records are expired.
	 * The value is expressed as a percentage of the TTL sent on the most recent write such that a read
	 * within this interval of the record’s end of life will generate a touch.
	 *
	 * For example, if the most recent write had a TTL of 10 hours and read_touch_ttl_percent is set to
	 * 80, the next read within 8 hours of the record's end of life (equivalent to 2 hours after the most
	 * recent write) will result in a touch, resetting the TTL to another 10 hours.
	 *
	 * Values:
	 * <ul>
	 * <li> 0 : Use server config default-read-touch-ttl-pct for the record's namespace/set.</li>
	 * <li>-1 : Do not reset record TTL on reads.</li>
	 * <li>1 - 100 : Reset record TTL on reads when within this percentage of the most recent write TTL.</li>
	 * </ul>
	 *
	 * Default: 0
	 */
	int read_touch_ttl_percent;
	
	/**
	 * Should raw bytes representing a list or map be deserialized to as_list or as_map.
	 * Set to false for backup programs that just need access to raw bytes.
	 *
	 * Default: true
	 */
	bool deserialize;

	/**
	 * Should as_record instance be allocated on the heap before user listener is called in
	 * async commands. If true, the user is responsible for calling as_record_destroy() when done
	 * with the record. If false, as_record_destroy() is automatically called by the client after
	 * the user listener function completes. This field is ignored for sync commands.
	 *
	 * Default: false
	 */
	bool async_heap_rec;

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
	 * Algorithm used to determine target node.
	 */
	as_policy_replica replica;

	/**
	 * Specifies the number of replicas required to be committed successfully when writing
	 * before returning command succeeded.
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
	 * The default time-to-live (expiration) of the record in seconds. This field will 
	 * only be used if "as_record.ttl" is set to AS_RECORD_CLIENT_DEFAULT_TTL. The
	 * as_record instance is passed in to write functions along with as_policy_write.
	 *
	 * There are also special values that can be set in the record ttl:
	 * <ul>
	 * <li>AS_RECORD_DEFAULT_TTL: Use the server default ttl from the namespace.</li>
	 * <li>AS_RECORD_NO_EXPIRE_TTL: Do not expire the record.</li>
	 * <li>AS_RECORD_NO_CHANGE_TTL: Keep the existing record ttl when the record is updated.</li>
	 * </ul>
	 */
	uint32_t ttl;

	/**
	 * Minimum record size beyond which it is compressed and sent to the server.
	 */
	uint32_t compression_threshold;

	/**
	 * If the command results in a record deletion, leave a tombstone for the record.
	 * This prevents deleted records from reappearing after node failures.
	 * Valid for Aerospike Server Enterprise Edition only.
	 *
	 * Default: false (do not tombstone deleted records).
	 */
	bool durable_delete;

	/**
	 * Execute the write command only if the record is not already locked by this transaction.
	 * If this field is true and the record is already locked by this transaction, the command will
	 * return AEROSPIKE_MRT_ALREADY_LOCKED.
	 *
	 * This field is useful for safely retrying non-idempotent writes as an alternative to simply
	 * aborting the transaction.
	 *
	 * Default: false.
	 */
	bool on_locking_only;

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
	 * Algorithm used to determine target node.
	 */
	as_policy_replica replica;

	/**
	 * Specifies the number of replicas required to be committed successfully when writing
	 * before returning command succeeded.
	 */
	as_policy_commit_level commit_level;

	/**
	 * The time-to-live (expiration) of the record in seconds. Note that ttl
	 * is only used on write/update calls.
	 *
	 * There are also special values that can be set in the record ttl:
	 * <ul>
	 * <li>AS_RECORD_DEFAULT_TTL: Use the server default ttl from the namespace.</li>
	 * <li>AS_RECORD_NO_EXPIRE_TTL: Do not expire the record.</li>
	 * <li>AS_RECORD_NO_CHANGE_TTL: Keep the existing record ttl when the record is updated.</li>
	 * </ul>
	 */
	uint32_t ttl;

	/**
	 * If the command results in a record deletion, leave a tombstone for the record.
	 * This prevents deleted records from reappearing after node failures.
	 * Valid for Aerospike Server Enterprise Edition only.
	 *
	 * Default: false (do not tombstone deleted records).
	 */
	bool durable_delete;

	/**
	 * Execute the write command only if the record is not already locked by this transaction.
	 * If this field is true and the record is already locked by this transaction, the command will
	 * return AEROSPIKE_MRT_ALREADY_LOCKED.
	 *
	 * This field is useful for safely retrying non-idempotent writes as an alternative to simply
	 * aborting the transaction.
	 *
	 * Default: false.
	 */
	bool on_locking_only;

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
	 * Algorithm used to determine target node.
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
	 * before returning command succeeded.
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
	 * The default time-to-live (expiration) of the record in seconds. This field will 
	 * only be used if one or more of the  operations is a write operation and  if "as_operations.ttl"
	 * is set to AS_RECORD_CLIENT_DEFAULT_TTL. The as_operations instance is passed in to
	 * operate functions along with as_policy_operate.
	 *
	 * There are also special values that can be set in the record ttl:
	 * <ul>
	 * <li>AS_RECORD_DEFAULT_TTL: Use the server default ttl from the namespace.</li>
	 * <li>AS_RECORD_NO_EXPIRE_TTL: Do not expire the record.</li>
	 * <li>AS_RECORD_NO_CHANGE_TTL: Keep the existing record ttl when the record is updated.</li>
	 * </ul>
	 */
	uint32_t ttl;

	/**
	 * Determine how record TTL (time to live) is affected on reads. When enabled, the server can
	 * efficiently operate as a read-based LRU cache where the least recently used records are expired.
	 * The value is expressed as a percentage of the TTL sent on the most recent write such that a read
	 * within this interval of the record’s end of life will generate a touch.
	 *
	 * For example, if the most recent write had a TTL of 10 hours and read_touch_ttl_percent is set to
	 * 80, the next read within 8 hours of the record's end of life (equivalent to 2 hours after the most
	 * recent write) will result in a touch, resetting the TTL to another 10 hours.
	 *
	 * Values:
	 * <ul>
	 * <li> 0 : Use server config default-read-touch-ttl-pct for the record's namespace/set.</li>
	 * <li>-1 : Do not reset record TTL on reads.</li>
	 * <li>1 - 100 : Reset record TTL on reads when within this percentage of the most recent write TTL.</li>
	 * </ul>
	 *
	 * Default: 0
	 */
	int read_touch_ttl_percent;

	/**
	 * Should raw bytes representing a list or map be deserialized to as_list or as_map.
	 * Set to false for backup programs that just need access to raw bytes.
	 *
	 * Default: true
	 */
	bool deserialize;

	/**
	 * If the command results in a record deletion, leave a tombstone for the record.
	 * This prevents deleted records from reappearing after node failures.
	 * Valid for Aerospike Server Enterprise Edition only.
	 *
	 * Default: false (do not tombstone deleted records).
	 */
	bool durable_delete;

	/**
	 * Execute the write command only if the record is not already locked by this transaction.
	 * If this field is true and the record is already locked by this transaction, the command will
	 * return AEROSPIKE_MRT_ALREADY_LOCKED.
	 *
	 * This field is useful for safely retrying non-idempotent writes as an alternative to simply
	 * aborting the transaction.
	 *
	 * Default: false.
	 */
	bool on_locking_only;

	/**
	 * Should as_record instance be allocated on the heap before user listener is called in
	 * async commands. If true, the user is responsible for calling as_record_destroy() when done
	 * with the record. If false, as_record_destroy() is automatically called by the client after
	 * the user listener function completes. This field is ignored for sync commands.
	 *
	 * Default: false
	 */
	bool async_heap_rec;

	/**
	 * Should the client return a result for every operation.
	 *
	 * Some operations do not return a result by default. This can make it difficult to determine the
	 * result offset in the returned bin's result list. Setting this field to true makes it easier to identify
	 * the desired result offset.
	 *
	 * This field defaults to false for older operations (basic read/write/incr/touch and list) to
	 * preserve legacy behavior. Newer operations (map, expression, bit or HLL and batch
	 * write operations) force respond_all_ops to be true regardless of it's initial setting.
	 */
	bool respond_all_ops;

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
	 * Algorithm used to determine target node.
	 */
	as_policy_replica replica;

	/**
	 * Specifies the number of replicas required to be committed successfully when writing
	 * before returning command succeeded.
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
	 * If the command results in a record deletion, leave a tombstone for the record.
	 * This prevents deleted records from reappearing after node failures.
	 * Valid for Aerospike Server Enterprise Edition only.
	 *
	 * Default: false (do not tombstone deleted records).
	 */
	bool durable_delete;

} as_policy_remove;

/**
 * Batch parent policy.
 *
 * @ingroup client_policies
 */
typedef struct as_policy_batch_s {

	/**
	 * Generic policy fields.
	 */
	as_policy_base base;

	/**
	 * Algorithm used to determine target node.
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
	 * Determine how record TTL (time to live) is affected on reads. When enabled, the server can
	 * efficiently operate as a read-based LRU cache where the least recently used records are expired.
	 * The value is expressed as a percentage of the TTL sent on the most recent write such that a read
	 * within this interval of the record’s end of life will generate a touch.
	 *
	 * For example, if the most recent write had a TTL of 10 hours and read_touch_ttl_percent is set to
	 * 80, the next read within 8 hours of the record's end of life (equivalent to 2 hours after the most
	 * recent write) will result in a touch, resetting the TTL to another 10 hours.
	 *
	 * Values:
	 * <ul>
	 * <li> 0 : Use server config default-read-touch-ttl-pct for the record's namespace/set.</li>
	 * <li>-1 : Do not reset record TTL on reads.</li>
	 * <li>1 - 100 : Reset record TTL on reads when within this percentage of the most recent write TTL.</li>
	 * </ul>
	 *
	 * Default: 0
	 */
	int read_touch_ttl_percent;

	/**
	 * Determine if batch commands to each server are run in parallel threads.
	 *
	 * Values:
	 * <ul>
	 * <li>
	 * false: Issue batch commands sequentially.  This mode has a performance advantage for small
	 * to medium sized batch sizes because commands can be issued in the main command thread.
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
	 * Allow batch to be processed immediately in the server's receiving thread for in-memory
	 * namespaces. If false, the batch will always be processed in separate service threads.
	 *
	 * For batch commands with smaller sized records (&lt;= 1K per record), inline
	 * processing will be significantly faster on in-memory namespaces.
	 *
	 * Inline processing can introduce the possibility of unfairness because the server
	 * can process the entire batch before moving onto the next command.
	 *
	 * Default: true
	 */
	bool allow_inline;

	/**
	 * Allow batch to be processed immediately in the server's receiving thread for SSD
	 * namespaces. If false, the batch will always be processed in separate service threads.
	 * Server versions &lt; 6.0 ignore this field.
	 *
	 * Inline processing can introduce the possibility of unfairness because the server
	 * can process the entire batch before moving onto the next command.
	 *
	 * Default: false
	 */
	bool allow_inline_ssd;

	/**
	 * Should all batch keys be attempted regardless of errors. This field is used on both
	 * the client and server. The client handles node specific errors and the server handles
	 * key specific errors.
	 *
	 * If true, every batch key is attempted regardless of previous key specific errors.
	 * Node specific errors such as timeouts stop keys to that node, but keys directed at
	 * other nodes will continue to be processed.
	 *
	 * If false, the server will stop the batch to its node on most key specific errors.
	 * The exceptions are AEROSPIKE_ERR_RECORD_NOT_FOUND and AEROSPIKE_FILTERED_OUT
	 * which never stop the batch. The client will stop the entire batch on node specific
	 * errors for sync commands that are run in sequence (concurrent == false). The client
	 * will not stop the entire batch for async commands or sync commands run in parallel.
	 *
	 * Server versions &lt; 6.0 do not support this field and treat this value as false
	 * for key specific errors.
	 *
	 * Default: true
	 */
	bool respond_all_keys;

	/**
	 * This method is deprecated and will eventually be removed.
	 * The set name is now always sent for every distinct namespace/set in the batch.
	 *
	 * Send set name field to server for every key in the batch for batch index protocol.
	 * This is necessary for batch writes and batch reads when authentication is enabled and
	 * security roles are defined on a per set basis.
	 *
	 * @deprecated Set name always sent.
	 */
	bool send_set_name;

	/**
	 * Should raw bytes be deserialized to as_list or as_map. Set to false for backup programs that
	 * just need access to raw bytes.
	 *
	 * Default: true
	 */
	bool deserialize;

} as_policy_batch;

/**
 * Policy attributes used in batch read commands.
 * @ingroup client_policies
 */
typedef struct as_policy_batch_read_s {
	/**
	 * Optional expression filter. If filter_exp exists and evaluates to false, the
	 * command is ignored. This can be used to eliminate a client/server roundtrip
	 * in some cases.
	 *
	 * aerospike_destroy() automatically calls as_exp_destroy() on all global default
	 * policy filter expression instances. The user is responsible for calling as_exp_destroy()
	 * on filter expressions when setting temporary command policies.
	 *
	 * Default: NULL
	 */
	struct as_exp* filter_exp;

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
	 * Determine how record TTL (time to live) is affected on reads. When enabled, the server can
	 * efficiently operate as a read-based LRU cache where the least recently used records are expired.
	 * The value is expressed as a percentage of the TTL sent on the most recent write such that a read
	 * within this interval of the record’s end of life will generate a touch.
	 *
	 * For example, if the most recent write had a TTL of 10 hours and read_touch_ttl_percent is set to
	 * 80, the next read within 8 hours of the record's end of life (equivalent to 2 hours after the most
	 * recent write) will result in a touch, resetting the TTL to another 10 hours.
	 *
	 * Values:
	 * <ul>
	 * <li> 0 : Use server config default-read-touch-ttl-pct for the record's namespace/set.</li>
	 * <li>-1 : Do not reset record TTL on reads.</li>
	 * <li>1 - 100 : Reset record TTL on reads when within this percentage of the most recent write TTL.</li>
	 * </ul>
	 *
	 * Default: 0
	 */
	int read_touch_ttl_percent;

} as_policy_batch_read;

/**
 * Policy attributes used in batch write commands.
 * @ingroup client_policies
 */
typedef struct as_policy_batch_write_s {
	/**
	 * Optional expression filter. If filter_exp exists and evaluates to false, the
	 * command is ignored. This can be used to eliminate a client/server roundtrip
	 * in some cases.
	 *
	 * aerospike_destroy() automatically calls as_exp_destroy() on all global default
	 * policy filter expression instances. The user is responsible for calling as_exp_destroy()
	 * on filter expressions when setting temporary command policies.
	 *
	 * Default: NULL
	 */
	struct as_exp* filter_exp;

	/**
	 * Specifies the behavior for the key.
	 */
	as_policy_key key;

	/**
	 * Specifies the number of replicas required to be committed successfully when writing
	 * before returning command succeeded.
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
	 * The default time-to-live (expiration) of the record in seconds. This field will only be
	 * used if "as_operations.ttl" is set to AS_RECORD_CLIENT_DEFAULT_TTL. The as_operations
	 * instance is passed in to batch write functions along with as_policy_batch_write.
	 *
	 * There are also special values that can be set in the record ttl:
	 * <ul>
	 * <li>AS_RECORD_DEFAULT_TTL: Use the server default ttl from the namespace.</li>
	 * <li>AS_RECORD_NO_EXPIRE_TTL: Do not expire the record.</li>
	 * <li>AS_RECORD_NO_CHANGE_TTL: Keep the existing record ttl when the record is updated.</li>
	 * </ul>
	 */
	uint32_t ttl;

	/**
	 * If the command results in a record deletion, leave a tombstone for the record.
	 * This prevents deleted records from reappearing after node failures.
	 * Valid for Aerospike Server Enterprise Edition only.
	 *
	 * Default: false (do not tombstone deleted records).
	 */
	bool durable_delete;

	/**
	 * Execute the write command only if the record is not already locked by this transaction.
	 * If this field is true and the record is already locked by this transaction, the command will
	 * return AEROSPIKE_MRT_ALREADY_LOCKED.
	 *
	 * This field is useful for safely retrying non-idempotent writes as an alternative to simply
	 * aborting the transaction.
	 *
	 * Default: false.
	 */
	bool on_locking_only;

} as_policy_batch_write;

/**
 * Policy attributes used in batch UDF apply commands.
 * @ingroup client_policies
 */
typedef struct as_policy_batch_apply_s {
	/**
	 * Optional expression filter. If filter_exp exists and evaluates to false, the
	 * command is ignored. This can be used to eliminate a client/server roundtrip
	 * in some cases.
	 *
	 * aerospike_destroy() automatically calls as_exp_destroy() on all global default 
	 * policy filter expression instances. The user is responsible for calling as_exp_destroy()
	 * on filter expressions when setting temporary command policies.
	 *
	 * Default: NULL
	 */
	struct as_exp* filter_exp;

	/**
	 * Specifies the behavior for the key.
	 */
	as_policy_key key;

	/**
	 * Specifies the number of replicas required to be committed successfully when writing
	 * before returning command succeeded.
	 */
	as_policy_commit_level commit_level;

	/**
	 * The time-to-live (expiration) of the record in seconds. Note that ttl
	 * is only used on write/update calls.
	 *
	 * There are also special values that can be set in the record ttl:
	 * <ul>
	 * <li>AS_RECORD_DEFAULT_TTL: Use the server default ttl from the namespace.</li>
	 * <li>AS_RECORD_NO_EXPIRE_TTL: Do not expire the record.</li>
	 * <li>AS_RECORD_NO_CHANGE_TTL: Keep the existing record ttl when the record is updated.</li>
	 * </ul>
	 */
	uint32_t ttl;

	/**
	 * If the command results in a record deletion, leave a tombstone for the record.
	 * This prevents deleted records from reappearing after node failures.
	 * Valid for Aerospike Server Enterprise Edition only.
	 *
	 * Default: false (do not tombstone deleted records).
	 */
	bool durable_delete;

	/**
	 * Execute the write command only if the record is not already locked by this transaction.
	 * If this field is true and the record is already locked by this transaction, the command will
	 * return AEROSPIKE_MRT_ALREADY_LOCKED.
	 *
	 * This field is useful for safely retrying non-idempotent writes as an alternative to simply
	 * aborting the transaction.
	 *
	 * Default: false.
	 */
	bool on_locking_only;

} as_policy_batch_apply;

/**
 * Policy attributes used in batch remove commands.
 * @ingroup client_policies
 */
typedef struct as_policy_batch_remove_s {
	/**
	 * Optional expression filter. If filter_exp exists and evaluates to false, the
	 * command is ignored. This can be used to eliminate a client/server roundtrip
	 * in some cases.
	 *
	 * aerospike_destroy() automatically calls as_exp_destroy() on all global default 
	 * policy filter expression instances. The user is responsible for calling as_exp_destroy()
	 * on filter expressions when setting temporary command policies.
	 *
	 * Default: NULL
	 */
	struct as_exp* filter_exp;

	/**
	 * Specifies the behavior for the key.
	 */
	as_policy_key key;

	/**
	 * Specifies the number of replicas required to be committed successfully when writing
	 * before returning command succeeded.
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
	 * If the command results in a record deletion, leave a tombstone for the record.
	 * This prevents deleted records from reappearing after node failures.
	 * Valid for Aerospike Server Enterprise Edition only.
	 *
	 * Default: false (do not tombstone deleted records).
	 */
	bool durable_delete;

} as_policy_batch_remove;

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
	 * Timeout used when info command is used that checks for cluster changes before and 
	 * after the query.  This timeout is only used when fail_on_cluster_change is enabled.
	 *
	 * Default: 10000 ms
	 */
	uint32_t info_timeout;

	/**
	 * Algorithm used to determine target node.
	 */
	as_policy_replica replica;
	
	/**
	 * Expected query duration. The server treats the query in different ways depending on the expected duration.
	 * This field is ignored for aggregation queries, background queries and server versions &lt; 6.0.
	 *
	 * Default: AS_QUERY_DURATION_LONG
	 */
	as_query_duration expected_duration;

	/**
	 * Terminate query if cluster is in migration state. If the server supports partition
	 * queries or the query filter is null (scan), this field is ignored.
	 *
	 * Default: false
	 */
	bool fail_on_cluster_change;

	/**
	 * Should raw bytes representing a list or map be deserialized to as_list or as_map.
	 * Set to false for backup programs that just need access to raw bytes.
	 *
	 * Default: true
	 */
	bool deserialize;

	/**
	 * This field is deprecated and will eventually be removed. Use expected_duration instead.
	 *
	 * For backwards compatibility: If short_query is true, the query is treated as a short query and
	 * expected_duration is ignored. If short_query is false, expected_duration is used
	 * and defaults to AS_QUERY_DURATION_LONG.
	 *
	 * Is query expected to return less than 100 records per node.
	 * If true, the server will optimize the query for a small record set.
	 * This field is ignored for aggregation queries, background queries
	 * and server versions &lt; 6.0.
	 *
	 * Default: false
	 *
	 * @deprecated Use expected_duration instead.
	 */
	bool short_query;

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
	 * Approximate number of records to return to client. This number is divided by the
	 * number of nodes involved in the scan.  The actual number of records returned
	 * may be less than max_records if node record counts are small and unbalanced across
	 * nodes.
	 *
	 * Default: 0 (do not limit record count)
	 */
	uint64_t max_records;

	/**
	 * Limit returned records per second (rps) rate for each server.
	 * Do not apply rps limit if records_per_second is zero.
	 *
	 * Default: 0
	 */
	uint32_t records_per_second;

	/**
	 * Algorithm used to determine target node.
	 */
	as_policy_replica replica;

	/**
	 * The default time-to-live (expiration) of the record in seconds. This field will only be
	 * used on background scan writes if "as_scan.ttl" is set to AS_RECORD_CLIENT_DEFAULT_TTL.
	 *
	 * There are also special values that can be set in the record ttl:
	 * <ul>
	 * <li>AS_RECORD_DEFAULT_TTL: Use the server default ttl from the namespace.</li>
	 * <li>AS_RECORD_NO_EXPIRE_TTL: Do not expire the record.</li>
	 * <li>AS_RECORD_NO_CHANGE_TTL: Keep the existing record ttl when the record is updated.</li>
	 * </ul>
	 */
	uint32_t ttl;

	/**
	 * If the command results in a record deletion, leave a tombstone for the record.
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
 * Transaction policy fields used to batch verify record versions on commit.
 * Used a placeholder for now as there are no additional fields beyond as_policy_batch.
 */
typedef as_policy_batch as_policy_txn_verify;

/**
 * Transaction policy fields used to batch roll forward/backward records on
 * commit or abort. Used a placeholder for now as there are no additional fields beyond as_policy_batch.
 */
typedef as_policy_batch as_policy_txn_roll;

/**
 * Struct of all policy values and operation policies. 
 * 
 * This is utilized by as_config to define default values for policies.
 *
 * @ingroup client_policies
 */
typedef struct as_policies_s {

	/**
	 * Default read policy.
	 */
	as_policy_read read;

	/**
	 * Default write policy.
	 */
	as_policy_write write;

	/**
	 * Default operate policy.
	 */
	as_policy_operate operate;

	/**
	 * Default remove policy.
	 */
	as_policy_remove remove;

	/**
	 * Default apply policy.
	 */
	as_policy_apply apply;

	/**
	 * Default parent policy used in batch read commands.
	 */
	as_policy_batch batch;

	/**
	 * Default parent policy used in batch write commands.
	 */
	as_policy_batch batch_parent_write;

	/**
	 * Default write policy used in batch operate commands.
	 */
	as_policy_batch_write batch_write;

	/**
	 * Default user defined function policy used in batch UDF apply commands.
	 */
	as_policy_batch_apply batch_apply;

	/**
	 * Default delete policy used in batch remove commands.
	 */
	as_policy_batch_remove batch_remove;

	/**
	 * Default scan policy.
	 */
	as_policy_scan scan;

	/**
	 * Default query policy.
	 */
	as_policy_query query;

	/**
	 * Default info policy.
	 */
	as_policy_info info;

	/**
	 * Default administration policy.
	 */
	as_policy_admin admin;

	/**
	 * Default transaction policy when verifying record versions in a batch.
	 */
	as_policy_txn_verify txn_verify;

	/**
	 * Default transaction policy when rolling the transaction records forward (commit)
	 * or back (abort) in a batch.
	 */
	as_policy_txn_roll txn_roll;

} as_policies;

//---------------------------------
// Functions
//---------------------------------

/**
 * Initialize base defaults for reads.
 */
static inline void
as_policy_base_read_init(as_policy_base* p)
{
	p->socket_timeout = AS_POLICY_SOCKET_TIMEOUT_DEFAULT;
	p->total_timeout = AS_POLICY_TOTAL_TIMEOUT_DEFAULT;
	p->max_retries = 2;
	p->sleep_between_retries = 0;
	p->filter_exp = NULL;
	p->txn = NULL;
	p->compress = false;
}

/**
 * Initialize base defaults for writes.
 */
static inline void
as_policy_base_write_init(as_policy_base* p)
{
	p->socket_timeout = AS_POLICY_SOCKET_TIMEOUT_DEFAULT;
	p->total_timeout = AS_POLICY_TOTAL_TIMEOUT_DEFAULT;
	p->max_retries = 0;
	p->sleep_between_retries = 0;
	p->filter_exp = NULL;
	p->txn = NULL;
	p->compress = false;
}

/**
 * Initialize base defaults for scan/query.
 *
 * Set max_retries for scans and non-aggregation queries with a null filter.
 * All other queries are not retried.
 *
 * The latest servers support retries on individual data partitions.
 * This feature is useful when a cluster is migrating and partition(s)
 * are missed or incomplete on the first query (with null filter) attempt.
 *
 * If the first query attempt misses 2 of 4096 partitions, then only
 * those 2 partitions are retried in the next query attempt from the
 * last key digest received for each respective partition. A higher
 * default max_retries is used because it's wasteful to invalidate
 * all query results because a single partition was missed.
 */
static inline void
as_policy_base_query_init(as_policy_base* p)
{
	p->socket_timeout = AS_POLICY_SOCKET_TIMEOUT_DEFAULT;
	p->total_timeout = 0;
	p->max_retries = 5;
	p->sleep_between_retries = 0;
	p->filter_exp = NULL;
	p->txn = NULL;
	p->compress = false;
}

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
	as_policy_base_read_init(&p->base);
	p->key = AS_POLICY_KEY_DEFAULT;
	p->replica = AS_POLICY_REPLICA_DEFAULT;
	p->read_mode_ap = AS_POLICY_READ_MODE_AP_DEFAULT;
	p->read_mode_sc = AS_POLICY_READ_MODE_SC_DEFAULT;
	p->read_touch_ttl_percent = 0;
	p->deserialize = true;
	p->async_heap_rec = false;
	return p;
}

/**
 * Shallow copy as_policy_read values.
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
	as_policy_base_write_init(&p->base);
	p->key = AS_POLICY_KEY_DEFAULT;
	p->replica = AS_POLICY_REPLICA_DEFAULT;
	p->commit_level = AS_POLICY_COMMIT_LEVEL_DEFAULT;
	p->gen = AS_POLICY_GEN_DEFAULT;
	p->exists = AS_POLICY_EXISTS_DEFAULT;
	p->ttl = 0; // AS_RECORD_DEFAULT_TTL
	p->compression_threshold = AS_POLICY_COMPRESSION_THRESHOLD_DEFAULT;
	p->durable_delete = false;
	p->on_locking_only = false;
	return p;
}

/**
 * Shallow copy as_policy_write values.
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
	as_policy_base_write_init(&p->base);
	p->key = AS_POLICY_KEY_DEFAULT;
	p->replica = AS_POLICY_REPLICA_DEFAULT;
	p->read_mode_ap = AS_POLICY_READ_MODE_AP_DEFAULT;
	p->read_mode_sc = AS_POLICY_READ_MODE_SC_DEFAULT;
	p->commit_level = AS_POLICY_COMMIT_LEVEL_DEFAULT;
	p->gen = AS_POLICY_GEN_DEFAULT;
	p->exists = AS_POLICY_EXISTS_DEFAULT;
	p->ttl = 0; // AS_RECORD_DEFAULT_TTL
	p->read_touch_ttl_percent = 0;
	p->deserialize = true;
	p->durable_delete = false;
	p->on_locking_only = false;
	p->async_heap_rec = false;
	p->respond_all_ops = false;
	return p;
}

/**
 * Shallow copy as_policy_operate values.
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
	as_policy_base_write_init(&p->base);
	p->key = AS_POLICY_KEY_DEFAULT;
	p->replica = AS_POLICY_REPLICA_DEFAULT;
	p->commit_level = AS_POLICY_COMMIT_LEVEL_DEFAULT;
	p->gen = AS_POLICY_GEN_DEFAULT;
	p->generation = 0;
	p->durable_delete = false;
	return p;
}

/**
 * Shallow copy as_policy_remove values.
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
	as_policy_base_write_init(&p->base);
	p->key = AS_POLICY_KEY_DEFAULT;
	p->replica = AS_POLICY_REPLICA_DEFAULT;
	p->commit_level = AS_POLICY_COMMIT_LEVEL_DEFAULT;
	p->ttl = 0; // AS_RECORD_DEFAULT_TTL
	p->durable_delete = false;
	p->on_locking_only = false;
	return p;
}

/**
 * Shallow copy as_policy_apply values.
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
	as_policy_base_read_init(&p->base);
	p->replica = AS_POLICY_REPLICA_SEQUENCE;
	p->read_mode_ap = AS_POLICY_READ_MODE_AP_DEFAULT;
	p->read_mode_sc = AS_POLICY_READ_MODE_SC_DEFAULT;
	p->read_touch_ttl_percent = 0;
	p->concurrent = false;
	p->allow_inline = true;
	p->allow_inline_ssd = false;
	p->respond_all_keys = true;
	p->send_set_name = true;
	p->deserialize = true;
	return p;
}

/**
 * Initialize as_policy_batch to default values when writes may occur.
 *
 * @param p	The policy to initialize.
 * @return	The initialized policy.
 *
 * @relates as_policy_batch
 */
static inline as_policy_batch*
as_policy_batch_parent_write_init(as_policy_batch* p)
{
	as_policy_batch_init(p);
	p->base.max_retries = 0;
	return p;
}

/**
 * Shallow copy as_policy_batch values.
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
 * Initialize as_policy_batch_read to default values.
 * @relates as_policy_batch_read
 */
static inline as_policy_batch_read*
as_policy_batch_read_init(as_policy_batch_read* p)
{
	p->filter_exp = NULL;
	p->read_mode_ap = AS_POLICY_READ_MODE_AP_DEFAULT;
	p->read_mode_sc = AS_POLICY_READ_MODE_SC_DEFAULT;
	p->read_touch_ttl_percent = 0;
	return p;
}

/**
 * Initialize as_policy_batch_write to default values.
 * @relates as_policy_batch_write
 */
static inline as_policy_batch_write*
as_policy_batch_write_init(as_policy_batch_write* p)
{
	p->filter_exp = NULL;
	p->key = AS_POLICY_KEY_DEFAULT;
	p->commit_level = AS_POLICY_COMMIT_LEVEL_DEFAULT;
	p->gen = AS_POLICY_GEN_DEFAULT;
	p->exists = AS_POLICY_EXISTS_DEFAULT;
	p->ttl = 0; // AS_RECORD_DEFAULT_TTL
	p->durable_delete = false;
	p->on_locking_only = false;
	return p;
}

/**
 * Initialize as_policy_batch_apply to default values.
 * @relates as_policy_batch_apply
 */
static inline as_policy_batch_apply*
as_policy_batch_apply_init(as_policy_batch_apply* p)
{
	p->filter_exp = NULL;
	p->key = AS_POLICY_KEY_DEFAULT;
	p->commit_level = AS_POLICY_COMMIT_LEVEL_DEFAULT;
	p->ttl = 0; // AS_RECORD_DEFAULT_TTL
	p->durable_delete = false;
	p->on_locking_only = false;
	return p;
}

/**
 * Initialize as_policy_batch_remove to default values.
 * @relates as_policy_batch_remove
 */
static inline as_policy_batch_remove*
as_policy_batch_remove_init(as_policy_batch_remove* p)
{
	p->filter_exp = NULL;
	p->key = AS_POLICY_KEY_DEFAULT;
	p->commit_level = AS_POLICY_COMMIT_LEVEL_DEFAULT;
	p->gen = AS_POLICY_GEN_DEFAULT;
	p->generation = 0;
	p->durable_delete = false;
	return p;
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
	as_policy_base_query_init(&p->base);
	p->max_records = 0;
	p->records_per_second = 0;
	p->replica = AS_POLICY_REPLICA_SEQUENCE;
	p->ttl = 0; // AS_RECORD_DEFAULT_TTL
	p->durable_delete = false;
	return p;
}

/**
 * Shallow copy as_policy_scan values.
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
	as_policy_base_query_init(&p->base);
	p->info_timeout = 10000;
	p->replica = AS_POLICY_REPLICA_SEQUENCE;
	p->expected_duration = AS_QUERY_DURATION_LONG;
	p->fail_on_cluster_change = false;
	p->deserialize = true;
	p->short_query = false;
	return p;
}

/**
 * Shallow copy as_policy_query values.
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
 * Initialize as_policy_txn_verify to default values.
 *
 * @param p	The policy to initialize.
 * @return	The initialized policy.
 *
 * @relates as_policy_txn_verify
 */
static inline as_policy_txn_verify*
as_policy_txn_verify_init(as_policy_txn_verify* p)
{
	p->base.socket_timeout = 3000;
	p->base.total_timeout = 10000;
	p->base.max_retries = 5;
	p->base.sleep_between_retries = 1000;
	p->base.filter_exp = NULL;
	p->base.txn = NULL;
	p->base.compress = false;
	p->replica = AS_POLICY_REPLICA_MASTER;
	p->read_mode_ap = AS_POLICY_READ_MODE_AP_DEFAULT;
	p->read_mode_sc = AS_POLICY_READ_MODE_SC_LINEARIZE;
	p->read_touch_ttl_percent = 0;
	p->concurrent = false;
	p->allow_inline = true;
	p->allow_inline_ssd = false;
	p->respond_all_keys = true;
	p->send_set_name = true;
	p->deserialize = true;
	return p;
}

/**
 * Copy as_policy_txn_verify values.
 *
 * @param src	The source policy.
 * @param trg	The target policy.
 *
 * @relates as_policy_txn_verify
 */
static inline void
as_policy_txn_verify_copy(const as_policy_txn_verify* src, as_policy_txn_verify* trg)
{
	*trg = *src;
}

/**
 * Initialize as_policy_txn_roll_ to default values.
 *
 * @param p	The policy to initialize.
 * @return	The initialized policy.
 *
 * @relates as_policy_txn_roll_
 */
static inline as_policy_txn_roll*
as_policy_txn_roll_init(as_policy_txn_roll* p)
{
	p->base.socket_timeout = 3000;
	p->base.total_timeout = 10000;
	p->base.max_retries = 5;
	p->base.sleep_between_retries = 1000;
	p->base.filter_exp = NULL;
	p->base.txn = NULL;
	p->base.compress = false;
	p->replica = AS_POLICY_REPLICA_MASTER;
	p->read_mode_ap = AS_POLICY_READ_MODE_AP_DEFAULT;
	p->read_mode_sc = AS_POLICY_READ_MODE_SC_DEFAULT;
	p->read_touch_ttl_percent = 0;
	p->concurrent = false;
	p->allow_inline = true;
	p->allow_inline_ssd = false;
	p->respond_all_keys = true;
	p->send_set_name = true;
	p->deserialize = true;
	return p;
}

/**
 * Copy as_policy_txn_roll values.
 *
 * @param src	The source policy.
 * @param trg	The target policy.
 *
 * @relates as_policy_txn_roll
 */
static inline void
as_policy_txn_roll_copy(const as_policy_txn_roll* src, as_policy_txn_roll* trg)
{
	*trg = *src;
}

/**
 * @private
 * Initialize as_policies.
 *
 * @relates as_policies
 */
as_policies*
as_policies_init(as_policies* p);

/**
 * @private
 * Destroy as_policies.
 *
 * @relates as_policies
 */
void
as_policies_destroy(as_policies* p);

#ifdef __cplusplus
} // end extern "C"
#endif
