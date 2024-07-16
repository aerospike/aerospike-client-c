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
typedef struct as_tran_s {
	uint64_t id;
	as_namespace ns;
	as_khash reads;
	as_khash writes;
	uint32_t deadline;
} as_tran;

//---------------------------------
// Functions
//---------------------------------

/**
 * Initialize multi-record transaction using defaults and assign random transaction id.
 *
 * @param tran		Multi-record transaction.
 */
AS_EXTERN void
as_tran_init(as_tran* tran);

/**
 * Initialize multi-record transaction with given hash bucket sizes.
 *
 * @param tran			Multi-record transaction.
 * @param read_buckets	Fixed number of hash buckets for record reads.
 * @param write_buckets	Fixed number of hash buckets for record writes.
 */
AS_EXTERN void
as_tran_init_buckets(as_tran* tran, uint32_t read_buckets, uint32_t write_buckets);

/**
 * Initialize multi-record transaction with given transaction id and hash bucket sizes.
 *
 * @param tran			Multi-record transaction.
 * @param id			Multi-record transaction identifier. Must be unique and non-zero.
 * @param read_buckets	Fixed number of hash buckets for record reads.
 * @param write_buckets	Fixed number of hash buckets for record writes.
 */
AS_EXTERN void
as_tran_init_all(as_tran* tran, uint64_t id, uint32_t read_buckets, uint32_t write_buckets);

/**
 * Set MRT namespace only if doesn't already exist.
 * If namespace already exists, verify new namespace is the same.
 * For internal use only.
 */
AS_EXTERN bool
as_tran_set_ns(as_tran* tran, const char* ns);

/**
 * Process the results of a record read. For internal use only.
 * TODO: Key will probably be a custom structure.
 */
AS_EXTERN bool
as_tran_on_read(as_tran* tran, as_key* key, uint64_t version);

/**
 * Get record version for a given key. For internal use only.
 */
AS_EXTERN uint64_t
as_tran_get_read_version(as_tran* tran, as_key* key);

/**
 * Process the results of a record write. For internal use only.
 */
AS_EXTERN bool
as_tran_on_write(as_tran* tran, as_key* key, uint64_t version, int rc);

/**
 * Close transaction. Remove all tracked keys.
 */
AS_EXTERN void
as_tran_close(as_tran* tran);

#ifdef __cplusplus
} // end extern "C"
#endif
