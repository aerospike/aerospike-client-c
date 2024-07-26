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
typedef struct as_tran {
	uint64_t id;
	as_namespace ns;
	as_khash reads;
	as_khash writes;
	uint32_t deadline;
	bool roll_attempted;
	bool free;
} as_tran;

struct as_batch_s;

//---------------------------------
// Functions
//---------------------------------

/**
 * Initialize multi-record transaction (MRT),  assign random transaction id and initialize
 * reads/writes hashmaps with default capacities. Call this function or as_tran_init_capacity(),
 * but not both. Do not use thie function for async commands (use as_tran_create() instead).
 *
 * @param tran		Multi-record transaction.
 */
AS_EXTERN void
as_tran_init(as_tran* tran);

/**
 * Initialize multi-record transaction (MRT), assign random transaction id and initialize
 * reads/writes hashmaps with given capacities. Call this function or as_tran_init(),
 * but not both. Do not use thie function for async commands (use as_tran_create_capacity() instead).
 *
 * @param tran				Multi-record transaction.
 * @param reads_capacity	expected number of record reads in the MRT. Minimum value is 16.
 * @param writes_capacity	expected number of record writes in the MRT. Minimum value is 16.
 */
AS_EXTERN void
as_tran_init_capacity(as_tran* tran, uint32_t reads_capacity, uint32_t writes_capacity);

/**
 * Create multi-record transaction (MRT) on heap, assign random transaction id and initialize
 * reads/writes hashmaps with default capacities.
 */
AS_EXTERN as_tran*
as_tran_create(void);

/**
 * Create multi-record transaction (MRT) on heap, assign random transaction id and initialize
 * reads/writes hashmaps with given capacities.
 *
 * @param reads_capacity	expected number of record reads in the MRT. Minimum value is 16.
 * @param writes_capacity	expected number of record writes in the MRT. Minimum value is 16.
 */
AS_EXTERN as_tran*
as_tran_create_capacity(uint32_t reads_capacity, uint32_t writes_capacity);

/**
 * Destroy MRT.
 */
AS_EXTERN void
as_tran_destroy(as_tran* tran);

/**
 * Process the results of a record read. For internal use only.
 */
AS_EXTERN void
as_tran_on_read(as_tran* tran, const uint8_t* digest, const char* set, uint64_t version);

/**
 * Get record version for a given key. For internal use only.
 */
AS_EXTERN uint64_t
as_tran_get_read_version(as_tran* tran, const as_key* key);

/**
 * Process the results of a record write. For internal use only.
 */
AS_EXTERN void
as_tran_on_write(as_tran* tran, const uint8_t* digest, const char* set, uint64_t version, int rc);

/**
 * Return if writes hashmap contains the given key.
 */
AS_EXTERN bool
as_tran_writes_contain(as_tran* tran, const as_key* key);

/**
 * Set MRT namespace only if doesn't already exist.
 * If namespace already exists, verify new namespace is the same.
 * For internal use only.
 */
AS_EXTERN as_status
as_tran_set_ns(as_tran* tran, const char* ns, as_error* err);

/**
 * Set MRT namespaces only if the don't already exist from a batch.
 * If namespaces already exist, verify new namespaces are the same.
 * For internal use only.
 */
AS_EXTERN as_status
as_tran_set_ns_batch(as_tran* tran, const struct as_batch_s* batch, as_error* err);

/**
 * Verify that commit/abort is only attempted once. For internal use only.
 */
AS_EXTERN bool
as_tran_set_roll_attempted(as_tran* tran);

/**
 * Clear MRT. Remove all tracked keys. For internal use only.
 */
AS_EXTERN void
as_tran_clear(as_tran* tran);

#ifdef __cplusplus
} // end extern "C"
#endif
