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

#include <aerospike/as_std.h>
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_status.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Types
//---------------------------------

/**
 * Hash map element.
 * @private
 */
typedef struct as_khash_ele_s {
	as_digest_value keyd;
	char set[64];
	uint64_t version;
	struct as_khash_ele_s* next;
} as_khash_ele;

/**
 * Hash map row.
 * @private
 */
typedef struct as_khash_row_s {
	bool used;
	as_khash_ele head;
} as_khash_row;

/**
 * Hashmap.
 * @private
 */
typedef struct as_khash_s {
	pthread_mutex_t lock;
	uint32_t n_eles;
	uint32_t n_rows;
	as_khash_row* table;
} as_khash;

/**
 * Multi-record transaction (MRT). Each command in the MRT must use the same namespace.
 */
typedef struct as_txn {
	uint64_t id;
	as_namespace ns;
	as_khash reads;
	as_khash writes;
	uint32_t deadline;
	bool monitor_in_doubt;
	bool roll_attempted;
	bool free;
} as_txn;

//---------------------------------
// Functions
//---------------------------------

/**
 * Initialize multi-record transaction (MRT),  assign random transaction id and initialize
 * reads/writes hashmaps with default capacities. Call this function or as_txn_init_capacity(),
 * but not both. Do not use thie function for async commands (use as_txn_create() instead).
 *
 * @param txn		Multi-record transaction.
 */
AS_EXTERN void
as_txn_init(as_txn* txn);

/**
 * Initialize multi-record transaction (MRT), assign random transaction id and initialize
 * reads/writes hashmaps with given capacities. Call this function or as_txn_init(),
 * but not both. Do not use thie function for async commands (use as_txn_create_capacity() instead).
 *
 * @param txn				Multi-record transaction.
 * @param reads_capacity	expected number of record reads in the MRT. Minimum value is 16.
 * @param writes_capacity	expected number of record writes in the MRT. Minimum value is 16.
 */
AS_EXTERN void
as_txn_init_capacity(as_txn* txn, uint32_t reads_capacity, uint32_t writes_capacity);

/**
 * Create multi-record transaction (MRT) on heap, assign random transaction id and initialize
 * reads/writes hashmaps with default capacities.
 */
AS_EXTERN as_txn*
as_txn_create(void);

/**
 * Create multi-record transaction (MRT) on heap, assign random transaction id and initialize
 * reads/writes hashmaps with given capacities.
 *
 * @param reads_capacity	expected number of record reads in the MRT. Minimum value is 16.
 * @param writes_capacity	expected number of record writes in the MRT. Minimum value is 16.
 */
AS_EXTERN as_txn*
as_txn_create_capacity(uint32_t reads_capacity, uint32_t writes_capacity);

/**
 * Destroy MRT.
 */
AS_EXTERN void
as_txn_destroy(as_txn* txn);

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
 * Set MRT namespace only if doesn't already exist.
 * If namespace already exists, verify new namespace is the same.
 * For internal use only.
 */
AS_EXTERN as_status
as_txn_set_ns(as_txn* txn, const char* ns, as_error* err);

/**
 * Verify that commit/abort is only attempted once. For internal use only.
 */
AS_EXTERN bool
as_txn_set_roll_attempted(as_txn* txn);

/**
 * Does MRT monitor record exist or is in doubt.
 */
static inline bool
as_txn_monitor_might_exist(as_txn* txn)
{
	return txn->deadline != 0 || txn->monitor_in_doubt;
}

/**
 * Does MRT monitor record exist.
 */
static inline bool
as_txn_monitor_exists(as_txn* txn)
{
	return txn->deadline != 0;
}

/**
 * Clear MRT. Remove all tracked keys. For internal use only.
 */
AS_EXTERN void
as_txn_clear(as_txn* txn);

#ifdef __cplusplus
} // end extern "C"
#endif
