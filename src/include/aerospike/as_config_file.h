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

#include <aerospike/as_config.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Macros
//---------------------------------

#define AS_CONFIG_INTERVAL 0
#define AS_MAX_CONNS_PER_NODE 1
#define AS_MIN_CONNS_PER_NODE 2
#define AS_ASYNC_MAX_CONNS_PER_NODE 3
#define AS_ASYNC_MIN_CONNS_PER_NODE 4
#define AS_APP_ID 5
#define AS_TEND_TIMEOUT 6
#define AS_MAX_ERROR_RATE 7
#define AS_ERROR_RATE_WINDOW 8
#define AS_LOGIN_TIMEOUT 9
#define AS_MAX_SOCKET_IDLE 10
#define AS_TEND_INTERVAL 11
#define AS_FAIL_IF_NOT_CONNECTED 12
#define AS_USE_SERVICE_ALTERNATIVE 13
#define AS_RACK_AWARE 14
#define AS_RACK_IDS 15
#define AS_READ_SOCKET_TIMEOUT 16
#define AS_READ_TOTAL_TIMEOUT 17
#define AS_READ_MAX_RETRIES 18
#define AS_READ_SLEEP_BETWEEN_RETRIES 19
#define AS_READ_READ_MODE_AP 20
#define AS_READ_READ_MODE_SC 21
#define AS_READ_REPLICA 22
#define AS_WRITE_SOCKET_TIMEOUT 23
#define AS_WRITE_TOTAL_TIMEOUT 24
#define AS_WRITE_MAX_RETRIES 25
#define AS_WRITE_SLEEP_BETWEEN_RETRIES 26
#define AS_WRITE_REPLICA 27
#define AS_WRITE_DURABLE_DELETE 28
#define AS_WRITE_SEND_KEY 29
#define AS_SCAN_SOCKET_TIMEOUT 30
#define AS_SCAN_TOTAL_TIMEOUT 31
#define AS_SCAN_MAX_RETRIES 32
#define AS_SCAN_SLEEP_BETWEEN_RETRIES 33
#define AS_SCAN_REPLICA 34
#define AS_QUERY_SOCKET_TIMEOUT 35
#define AS_QUERY_TOTAL_TIMEOUT 36
#define AS_QUERY_MAX_RETRIES 37
#define AS_QUERY_SLEEP_BETWEEN_RETRIES 38
#define AS_QUERY_REPLICA 39
#define AS_QUERY_INFO_TIMEOUT 40
#define AS_QUERY_EXPECTED_DURATION 41
#define AS_METRICS_ENABLE 42
#define AS_METRICS_LATENCY_COLUMNS 43
#define AS_METRICS_LATENCY_SHIFT 44
#define AS_METRICS_LABELS 45
#define AS_BATCH_WRITE_DURABLE_DELETE 46
#define AS_BATCH_WRITE_SEND_KEY 47
#define AS_BATCH_UDF_DURABLE_DELETE 48
#define AS_BATCH_UDF_SEND_KEY 49
#define AS_BATCH_DELETE_DURABLE_DELETE 50
#define AS_BATCH_DELETE_SEND_KEY 51

#define AS_BATCH_PARENT_READ 52
#define AS_BATCH_PARENT_WRITE 63
#define AS_TXN_VERIFY 74
#define AS_TXN_ROLL 85

#define AS_BATCH_SOCKET_TIMEOUT 0
#define AS_BATCH_TOTAL_TIMEOUT 1
#define AS_BATCH_MAX_RETRIES 2
#define AS_BATCH_SLEEP_BETWEEN_RETRIES 3
#define AS_BATCH_READ_MODE_AP 4
#define AS_BATCH_READ_MODE_SC 5
#define AS_BATCH_REPLICA 6
#define AS_BATCH_CONCURRENT 7
#define AS_BATCH_ALLOW_INLINE 8
#define AS_BATCH_ALLOW_INLINE_SSD 9
#define AS_BATCH_RESPOND_ALL_KEYS 10

// 96 total bits / 8 = 12 bytes
#define AS_CONFIG_BITMAP_SIZE 12

//---------------------------------
// Types
//---------------------------------

struct aerospike_s;

//---------------------------------
// Functions
//---------------------------------

/**
 * Read dynamic configuration file and populate config. Call this function before creating the cluster.
 */
as_status
as_config_file_init(struct aerospike_s* as, as_error* err);

/**
 * Read dynamic configuration file and update cluster with new values.
 */
as_status
as_config_file_update(struct aerospike_s* as, as_error* err);

/**
 * Mark field as modified.
 */
static inline void
as_field_set(uint8_t* bitmap, uint32_t offset)
{
	bitmap[offset >> 3] |= (1 << (offset & 7));
}

/**
 * Return if field was modified.
 */
static inline bool
as_field_is_set(uint8_t* bitmap, uint32_t offset)
{
	return bitmap[offset >> 3] & (1 << (offset & 7));
}

#ifdef __cplusplus
} // end extern "C"
#endif
