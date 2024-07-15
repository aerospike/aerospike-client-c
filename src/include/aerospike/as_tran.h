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

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Types
//---------------------------------

/**
 * Multi-record transaction (MRT). Each command in the MRT must use the same namespace.
 */
typedef struct as_tran_s {
	uint64_t id;
	as_namespace ns;
	uint32_t deadline;
	// TODO: Declare reads and writes hashmap.
	// Each key requires digest and setname. setname could alternatively be part of the value.
	// The uint64_t value is the record version.
	// hashmap<key,uint64_t> writes
	// hashset<key> reads
} as_tran;

//---------------------------------
// Functions
//---------------------------------

/**
 * Create transaction and assign random transaction id.
 *
 * @param tran		Multi-record transaction.
 */
AS_EXTERN void
as_tran_init(as_tran* tran);

/**
 * Create transaction with given transaction id. Transaction id must be unique and non-zero.
 *
 * @param tran		Multi-record transaction.
 * @param id		Multi-record transaction identifier.
 */
AS_EXTERN void
as_tran_init_uint64(as_tran* tran, uint64_t id);

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
