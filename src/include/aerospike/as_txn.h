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

#include <aerospike/as_std.h>
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_status.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Macros
//---------------------------------

/**
 * Default number of transaction read hash buckets.
 */
#define AS_TXN_READ_CAPACITY_DEFAULT 128

/**
 * Default number of transaction write hash buckets.
 */
#define AS_TXN_WRITE_CAPACITY_DEFAULT 128

//---------------------------------
// Types
//---------------------------------

/**
 * Transaction state.
 */
typedef enum {
	AS_TXN_STATE_OPEN,
	AS_TXN_STATE_VERIFIED,
	AS_TXN_STATE_COMMITTED,
	AS_TXN_STATE_ABORTED
} as_txn_state;

/**
 * Transaction key.
 */
typedef struct as_txn_key {
	as_digest_value digest;
	char set[64];
	uint64_t version;
	struct as_txn_key* next;
} as_txn_key;

/**
 * Transaction hash map row.
 */
typedef struct {
	bool used;
	as_txn_key head;
} as_txn_hash_row;

/**
 * Transaction hash map.
 */
typedef struct {
	pthread_mutex_t lock;
	uint32_t n_eles;
	uint32_t n_rows;
	as_txn_hash_row* table;
} as_txn_hash;

/**
 * Transaction. Each command in the transaction must use the same namespace.
 */
typedef struct as_txn {
	uint64_t id;
	as_namespace ns;
	as_txn_hash reads;
	as_txn_hash writes;
	uint32_t timeout;
	uint32_t deadline;
	as_txn_state state;
	bool write_in_doubt;
	bool in_doubt;
	bool free;
} as_txn;

/**
 * Transaction iterator.
 */
typedef struct {
	as_txn_hash* khash;
	as_txn_hash_row* row;
	as_txn_key* ele;
	uint32_t idx;
} as_txn_iter;

//---------------------------------
// Functions
//---------------------------------

/**
 * Initialize transaction,  assign random transaction id and initialize
 * reads/writes hashmaps with default capacities.
 *
 * The default client transaction timeout is zero. This means use the server configuration mrt-duration
 * as the transaction timeout. The default mrt-duration is 10 seconds.
 *
 * Call this function or as_txn_init_capacity(), but not both. Do not use this function for async
 * commands (use as_txn_create() instead).
 *
 * @param txn		Transaction.
 */
AS_EXTERN void
as_txn_init(as_txn* txn);

/**
 * Initialize transaction, assign random transaction id and initialize
 * reads/writes hashmaps with given capacities.
 *
 * The default client transaction timeout is zero. This means use the server configuration mrt-duration
 * as the transaction timeout. The default mrt-duration is 10 seconds.
 *
 * Call this function or as_txn_init(), but not both. Do not use this function for async commands
 * (use as_txn_create_capacity() instead).
 *
 * @param txn				Transaction.
 * @param reads_capacity	expected number of record reads in the transaction. Minimum value is 16.
 * @param writes_capacity	expected number of record writes in the transaction. Minimum value is 16.
 */
AS_EXTERN void
as_txn_init_capacity(as_txn* txn, uint32_t reads_capacity, uint32_t writes_capacity);

/**
 * Create transaction on heap, assign random transaction id and initialize
 * reads/writes hashmaps with default capacities.
 *
 * The default client transaction timeout is zero. This means use the server configuration mrt-duration
 * as the transaction timeout. The default mrt-duration is 10 seconds.
 */
AS_EXTERN as_txn*
as_txn_create(void);

/**
 * Create transaction on heap, assign random transaction id and initialize
 * reads/writes hashmaps with given capacities.
 *
 * The default client transaction timeout is zero. This means use the server configuration mrt-duration
 * as the transaction timeout. The default mrt-duration is 10 seconds.
 *
 * @param reads_capacity	expected number of record reads in the transaction. Minimum value is 16.
 * @param writes_capacity	expected number of record writes in the transaction. Minimum value is 16.
 */
AS_EXTERN as_txn*
as_txn_create_capacity(uint32_t reads_capacity, uint32_t writes_capacity);

/**
 * Destroy transaction.
 */
AS_EXTERN void
as_txn_destroy(as_txn* txn);

/**
 * Set transaction timeout in seconds. The timer starts when the transaction monitor record is created.
 * This occurs when the first command in the transaction is executed. If the timeout is reached before
 * an aerospike_commit() or aerospike_abort() is called, the server will expire and rollback the transaction.
 *
 * If the transaction timeout is zero, the server configuration mrt-duration is used.
 * The default mrt-duration is 10 seconds.
 */
static inline void
as_txn_set_timeout(as_txn* txn, uint32_t timeout)
{
	txn->timeout = timeout;
}

/**
 * Return read hash size.
 */
static inline uint32_t
as_txn_reads_size(as_txn* txn)
{
	return txn->reads.n_eles;
}

/**
 * Process the results of a record read. For internal use only.
 */
AS_EXTERN void
as_txn_on_read(as_txn* txn, const uint8_t* digest, const char* set, uint64_t version);

/**
 * Get record version for a given key. For internal use only.
 */
AS_EXTERN uint64_t
as_txn_get_read_version(as_txn* txn, const uint8_t* digest);

/**
 * Return write hash size.
 */
static inline uint32_t
as_txn_writes_size(as_txn* txn)
{
	return txn->writes.n_eles;
}

/**
 * Process the results of a record write. For internal use only.
 */
AS_EXTERN void
as_txn_on_write(as_txn* txn, const uint8_t* digest, const char* set, uint64_t version, int rc);

/**
 * Add key to write hash when write command is in doubt (usually caused by timeout). For internal use only.
 */
AS_EXTERN void
as_txn_on_write_in_doubt(as_txn* txn, const uint8_t* digest, const char* set);

/**
 * Return if writes hashmap contains the given key.
 */
AS_EXTERN bool
as_txn_writes_contain(as_txn* txn, const as_key* key);

/**
 * Verify that the transaction state allows future commands.
 */
AS_EXTERN as_status
as_txn_verify_command(as_txn* txn, as_error* err);

/**
 * Set transaction namespace only if doesn't already exist.
 * If namespace already exists, verify new namespace is the same.
 * For internal use only.
 */
AS_EXTERN as_status
as_txn_set_ns(as_txn* txn, const char* ns, as_error* err);

/**
 * Return if the transaction monitor record should be closed/deleted. For internal use only.
 */
static inline bool
as_txn_close_monitor(as_txn* txn)
{
	return txn->deadline != 0 && !txn->write_in_doubt;
}

/**
 * Does transaction monitor record exist.
 */
static inline bool
as_txn_monitor_exists(as_txn* txn)
{
	return txn->deadline != 0;
}

/**
 * Clear transaction. Remove all tracked keys. For internal use only.
 */
AS_EXTERN void
as_txn_clear(as_txn* txn);

/**
 * Initialize read keys iterator.
 */
static inline void
as_txn_iter_reads(as_txn_iter* iter, as_txn* txn)
{
	iter->khash = &txn->reads;
	iter->row = txn->reads.table;
	iter->ele = NULL;
	iter->idx = 0;
}

/**
 * Initialize write keys iterator.
 */
static inline void
as_txn_iter_writes(as_txn_iter* iter, as_txn* txn)
{
	iter->khash = &txn->writes;
	iter->row = txn->writes.table;
	iter->ele = NULL;
	iter->idx = 0;
}

/**
 * Return next available hash element or NULL if no more elements are available.
 */
AS_EXTERN as_txn_key*
as_txn_iter_next(as_txn_iter* iter);

#ifdef __cplusplus
} // end extern "C"
#endif
