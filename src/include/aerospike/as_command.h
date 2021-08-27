/*
 * Copyright 2008-2021 Aerospike, Inc.
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

#include <aerospike/as_bin.h>
#include <aerospike/as_buffer.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_key.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_proto.h>
#include <aerospike/as_random.h>
#include <aerospike/as_record.h>
#include <citrusleaf/cf_byte_order.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * MACROS
 *****************************************************************************/

// Command Flags
#define AS_COMMAND_FLAGS_READ 1
#define AS_COMMAND_FLAGS_BATCH 2
#define AS_COMMAND_FLAGS_LINEARIZE 4

// Field IDs
#define AS_FIELD_NAMESPACE 0
#define AS_FIELD_SETNAME 1
#define AS_FIELD_KEY 2
#define AS_FIELD_DIGEST 4
#define AS_FIELD_TASK_ID 7
#define AS_FIELD_SCAN_TIMEOUT 9
#define AS_FIELD_SCAN_RPS 10
#define AS_FIELD_PID_ARRAY 11
#define AS_FIELD_DIGEST_ARRAY 12
#define AS_FIELD_SCAN_MAX_RECORDS 13
#define AS_FIELD_INDEX_RANGE 22
#define AS_FIELD_INDEX_FILTER 23
#define AS_FIELD_INDEX_LIMIT 24
#define AS_FIELD_INDEX_ORDER 25
#define AS_FIELD_INDEX_TYPE 26
#define AS_FIELD_UDF_PACKAGE_NAME 30
#define AS_FIELD_UDF_FUNCTION 31
#define AS_FIELD_UDF_ARGLIST 32
#define AS_FIELD_UDF_OP 33
#define AS_FIELD_QUERY_BINS 40
#define AS_FIELD_BATCH_INDEX 41
#define AS_FIELD_BATCH_INDEX_WITH_SET 42
#define AS_FIELD_FILTER 43

// Message info1 bits
#define AS_MSG_INFO1_READ				(1 << 0) // contains a read operation
#define AS_MSG_INFO1_GET_ALL			(1 << 1) // get all bins, period
// (Note:  Bit 2 is unused.)
#define AS_MSG_INFO1_BATCH_INDEX		(1 << 3) // batch read
#define AS_MSG_INFO1_XDR				(1 << 4) // operation is being performed by XDR
#define AS_MSG_INFO1_GET_NOBINDATA		(1 << 5) // do not get information about bins and its data
#define AS_MSG_INFO1_READ_MODE_AP_ALL	(1 << 6) // read mode all for AP namespaces.
#define AS_MSG_INFO1_COMPRESS_RESPONSE	(1 << 7) // tell server to compress it's response.

// Message info2 bits
#define AS_MSG_INFO2_WRITE				(1 << 0) // contains a write semantic
#define AS_MSG_INFO2_DELETE				(1 << 1) // delete record
#define AS_MSG_INFO2_GENERATION			(1 << 2) // pay attention to the generation
#define AS_MSG_INFO2_GENERATION_GT		(1 << 3) // apply write if new generation >= old, good for restore
#define AS_MSG_INFO2_DURABLE_DELETE		(1 << 4) // transaction resulting in record deletion leaves tombstone (Enterprise only).
#define AS_MSG_INFO2_CREATE_ONLY		(1 << 5) // write record only if it doesn't exist
// (Note:  Bit 6 is unused.)
#define AS_MSG_INFO2_RESPOND_ALL_OPS	(1 << 7) // return a result for every operation.

// Message info3 bits
#define AS_MSG_INFO3_LAST				(1 << 0) // this is the last of a multi-part message
#define AS_MSG_INFO3_COMMIT_MASTER  	(1 << 1) // write commit level - bit 0
#define AS_MSG_INFO3_PARTITION_DONE  	(1 << 2) // Partition is complete response in scan.
#define AS_MSG_INFO3_UPDATE_ONLY		(1 << 3) // update existing record only, do not create new record
#define AS_MSG_INFO3_CREATE_OR_REPLACE	(1 << 4) // completely replace existing record, or create new record
#define AS_MSG_INFO3_REPLACE_ONLY		(1 << 5) // completely replace existing record, do not create new record
#define AS_MSG_INFO3_SC_READ_TYPE		(1 << 6) // see below
#define AS_MSG_INFO3_SC_READ_RELAX		(1 << 7) // see below
// Interpret SC_READ bits in info3.
//
// RELAX   TYPE
//                strict
//                ------
//   0      0     sequential (default)
//   0      1     linearize
//
//                relaxed
//                -------
//   1      0     allow prole
//   1      1     allow unavailable

// Misc
#define AS_HEADER_SIZE 30
#define AS_FIELD_HEADER_SIZE 5
#define AS_OPERATION_HEADER_SIZE 8

#define AS_STACK_BUF_SIZE (1024 * 16)
#define AS_COMPRESS_THRESHOLD 128

/**
 * @private
 * Macros use these stand-ins for cf_malloc() / cf_free(), so that
 * instrumentation properly substitutes them.
 */
static inline void*
local_malloc(size_t size)
{
	return cf_malloc(size);
}

static inline void
local_free(void* memory)
{
	cf_free(memory);
}

/**
 * @private
 * Allocate command buffer on stack or heap depending on given size.
 */
#define as_command_buffer_init(_sz) (_sz > AS_STACK_BUF_SIZE) ? (uint8_t*)local_malloc(_sz) : (uint8_t*)alloca(_sz)

/**
 * @private
 * Free command buffer.
 */
#define as_command_buffer_free(_buf, _sz) if (_sz > AS_STACK_BUF_SIZE) {local_free(_buf);}

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * @private
 * Write buffer callback used in as_command_send().
 */
typedef size_t (*as_write_fn) (void* udata, uint8_t* buf);

/**
 * @private
 * Parse results callback used in as_command_execute().
 */
typedef as_status (*as_parse_results_fn) (
	as_error* err, as_node* node, uint8_t* buf, size_t size, void* user_data
	);

/**
 * @private
 * Synchronous command data.
 */
typedef struct as_command_s {
	as_cluster* cluster;
	const as_policy_base* policy;
	as_node* node;
	const char* ns;
	void* partition;
	as_parse_results_fn parse_results_fn;
	void* udata;
	uint8_t* buf;
	size_t buf_size;
	uint32_t partition_id;
	as_policy_replica replica;
	uint64_t deadline_ms;
	uint32_t socket_timeout;
	uint32_t total_timeout;
	uint32_t iteration;
	uint8_t flags;
	bool master;
	bool master_sc; // Used in batch only.
} as_command;

/**
 * @private
 * Data used in as_command_parse_result().
 */
typedef struct as_command_parse_result_data_s {
	as_record** record;
	bool deserialize;
} as_command_parse_result_data;

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

/**
 * @private
 * Destroy buffers when error occurs before bins have been written.
 */
static inline void
as_buffers_destroy(as_queue* buffers)
{
	as_buffer b;

	while (as_queue_pop(buffers, &b)) {
		cf_free(b.data);
	}
	as_queue_destroy(buffers);
}

/**
 * @private
 * Calculate size of command header plus key fields.
 */
size_t
as_command_key_size(as_policy_key policy, const as_key* key, uint16_t* n_fields);

/**
 * @private
 * Calculate size of string field.
 */
static inline size_t
as_command_string_field_size(const char* value)
{
	return strlen(value) + AS_FIELD_HEADER_SIZE;
}

/**
 * @private
 * Calculate size of field structure given field value size.
 */
static inline size_t
as_command_field_size(size_t size)
{
	return size + AS_FIELD_HEADER_SIZE;
}

/**
 * @private
 * Calculate size of as_val field.
 */
size_t
as_command_value_size(as_val* val, as_queue* buffers);

/**
 * @private
 * Calculate size of bin name and value combined.
 */
static inline size_t
as_command_bin_size(const as_bin* bin, as_queue* buffers)
{
	return strlen(bin->name) + as_command_value_size((as_val*)bin->valuep, buffers) + 8;
}

/**
 * @private
 * Calculate size of bin name. Return error is bin name greater than AS_BIN_NAME_MAX_LEN characters.
 */
static inline as_status
as_command_bin_name_size(as_error* err, const char* name, size_t* size)
{
	size_t s = strlen(name);
	
	if (s > AS_BIN_NAME_MAX_LEN) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Bin name too long: %s", name);
	}
	(*size) += s + AS_OPERATION_HEADER_SIZE;
	return AEROSPIKE_OK;
}

/**
 * @private
 * Calculate size of string operation.
 */
static inline size_t
as_command_string_operation_size(const char* value)
{
	return strlen(value) + AS_OPERATION_HEADER_SIZE;
}

/**
 * @private
 * Set read attributes for read header commands.
 */
static inline void
as_command_set_attr_read_header(
	as_policy_read_mode_ap read_mode_ap, as_policy_read_mode_sc read_mode_sc,
	uint8_t* read_attr, uint8_t* info_attr
	)
{
	switch (read_mode_sc) {
		default:
		case AS_POLICY_READ_MODE_SC_SESSION:
			break;

		case AS_POLICY_READ_MODE_SC_LINEARIZE:
			*info_attr |= AS_MSG_INFO3_SC_READ_TYPE;
			break;

		case AS_POLICY_READ_MODE_SC_ALLOW_REPLICA:
			*info_attr |= AS_MSG_INFO3_SC_READ_RELAX;
			break;

		case AS_POLICY_READ_MODE_SC_ALLOW_UNAVAILABLE:
			*info_attr |= AS_MSG_INFO3_SC_READ_TYPE | AS_MSG_INFO3_SC_READ_RELAX;
			break;
	}

	if (read_mode_ap == AS_POLICY_READ_MODE_AP_ALL) {
		*read_attr |= AS_MSG_INFO1_READ_MODE_AP_ALL;
	}
}

/**
 * @private
 * Set compress attributes when compress is true.
 */
static inline void
as_command_set_attr_compress(bool compress, uint8_t* read_attr)
{
	if (compress) {
		*read_attr |= AS_MSG_INFO1_COMPRESS_RESPONSE;
	}
}

/**
 * @private
 * Set read attributes for read commands.
 */
static inline void
as_command_set_attr_read(
	as_policy_read_mode_ap read_mode_ap, as_policy_read_mode_sc read_mode_sc, bool compress,
	uint8_t* read_attr, uint8_t* info_attr
	)
{
	as_command_set_attr_read_header(read_mode_ap, read_mode_sc, read_attr, info_attr);
	as_command_set_attr_compress(compress, read_attr);
}
	
/**
 * @private
 * Write command header for write commands.
 */
uint8_t*
as_command_write_header_write(
	uint8_t* cmd, const as_policy_base* policy, as_policy_commit_level commit_level,
	as_policy_exists exists, as_policy_gen gen_policy, uint32_t gen, uint32_t ttl,
	uint16_t n_fields, uint16_t n_bins, bool durable_delete, uint8_t read_attr, uint8_t write_attr,
	uint8_t info_attr
	);

/**
 * @private
 * Write command header for read commands.
 */
uint8_t*
as_command_write_header_read(
	uint8_t* cmd, const as_policy_base* policy, as_policy_read_mode_ap read_mode_ap,
	as_policy_read_mode_sc read_mode_sc, uint32_t timeout, uint16_t n_fields, uint16_t n_bins,
	uint8_t read_attr
	);

/**
 * @private
 * Write command header for read header commands.
 */
uint8_t*
as_command_write_header_read_header(
	uint8_t* cmd, const as_policy_base* policy, as_policy_read_mode_ap read_mode_ap,
	as_policy_read_mode_sc read_mode_sc, uint16_t n_fields, uint16_t n_bins, uint8_t read_attr
	);

/**
 * @private
 * Write field header.
 */
static inline uint8_t*
as_command_write_field_header(uint8_t* p, uint8_t id, uint32_t size)
{
	*(uint32_t*)p = cf_swap_to_be32(size+1);
	p += 4;
	*p++ = id;
	return p;
}

/**
 * @private
 * Write string field.
 */
static inline uint8_t*
as_command_write_field_string(uint8_t* begin, uint8_t id, const char* val)
{
	uint8_t* p = begin + AS_FIELD_HEADER_SIZE;
	
	// Copy string, but do not transfer null byte.
	while (*val) {
		*p++ = *val++;
	}
	as_command_write_field_header(begin, id, (uint32_t)(p - begin - AS_FIELD_HEADER_SIZE));
	return p;
}

/**
 * @private
 * Write uint32_t field.
 */
static inline uint8_t*
as_command_write_field_uint32(uint8_t* p, uint8_t id, uint32_t val)
{
	p = as_command_write_field_header(p, id, sizeof(uint32_t));
	*(uint32_t*)p = cf_swap_to_be32(val);
	return p + sizeof(uint32_t);
}

/**
 * @private
 * Write uint64_t field.
 */
static inline uint8_t*
as_command_write_field_uint64(uint8_t* p, uint8_t id, uint64_t val)
{
	p = as_command_write_field_header(p, id, sizeof(uint64_t));
	*(uint64_t*)p = cf_swap_to_be64(val);
	return p + sizeof(uint64_t);
}

/**
 * @private
 * Write as_buffer field.
 */
static inline uint8_t*
as_command_write_field_buffer(uint8_t* p, uint8_t id, as_buffer* buffer)
{
	p = as_command_write_field_header(p, id, buffer->size);
	memcpy(p, buffer->data, buffer->size);
	return p + buffer->size;
}

/**
 * @private
 * Write digest field.
 */
static inline uint8_t*
as_command_write_field_digest(uint8_t* p, const as_digest* val)
{
	p = as_command_write_field_header(p, AS_FIELD_DIGEST, AS_DIGEST_VALUE_SIZE);
	memcpy(p, val->value, AS_DIGEST_VALUE_SIZE);
	return p + AS_DIGEST_VALUE_SIZE;
}

/**
 * @private
 * Write key structure.
 */
uint8_t*
as_command_write_key(uint8_t* p, as_policy_key policy, const as_key* key);

/**
 * @private
 * Write bin header and bin name.
 */
uint8_t*
as_command_write_bin_name(uint8_t* cmd, const char* name);

/**
 * @private
 * Write bin.
 */
uint8_t*
as_command_write_bin(
	uint8_t* begin, as_operator operation_type, const as_bin* bin, as_queue* buffers
	);

/**
 * @private
 * Finish writing command.
 */
static inline size_t
as_command_write_end(uint8_t* begin, uint8_t* end)
{
	uint64_t len = end - begin;
	uint64_t proto = (len - 8) | ((uint64_t)AS_PROTO_VERSION << 56) | ((uint64_t)AS_MESSAGE_TYPE << 48);
	*(uint64_t*)begin = cf_swap_to_be64(proto);
	return len;
}

/**
 * @private
 * Finish writing compressed command.
 */
static inline size_t
as_command_compress_write_end(uint8_t* begin, uint8_t* end, uint64_t uncompressed_sz)
{
	uint64_t len = end - begin;
	uint64_t proto = (len - 8) | ((uint64_t)AS_PROTO_VERSION << 56) | ((uint64_t)AS_COMPRESSED_MESSAGE_TYPE << 48);
	*(uint64_t*)begin = cf_swap_to_be64(proto);
	((as_compressed_proto *)begin)->uncompressed_sz = cf_swap_to_be64(uncompressed_sz);
	return len;
}

/**
 * @private
 * Calculate max size the compressed command buffer.
 */
size_t
as_command_compress_max_size(size_t cmd_sz);

/**
 * @private
 * Compress command buffer.
 */
as_status
as_command_compress(as_error* err, uint8_t* cmd, size_t cmd_sz, uint8_t* compressed_cmd, size_t* compressed_size);

/**
 * @private
 * Return timeout to be sent to server for single record transactions.
 */
static inline uint32_t
as_command_server_timeout(const as_policy_base* policy)
{
	return (policy->socket_timeout < policy->total_timeout && policy->socket_timeout != 0)?
			policy->socket_timeout : policy->total_timeout;
}

/**
 * @private
 * Start command timer.
 */
static inline void
as_command_start_timer(as_command* cmd)
{
	cmd->iteration = 0;
	cmd->master = true;

	const as_policy_base* policy = cmd->policy;

	if (policy->total_timeout > 0) {
		cmd->socket_timeout = (policy->socket_timeout == 0 ||
							   policy->socket_timeout > policy->total_timeout)?
								   policy->total_timeout : policy->socket_timeout;

		cmd->total_timeout = policy->total_timeout;
		cmd->deadline_ms = cf_getms() + policy->total_timeout;
	}
	else {
		cmd->socket_timeout = policy->socket_timeout;
		cmd->total_timeout = policy->total_timeout;
		cmd->deadline_ms = 0;
	}
}

/**
 * @private
 * Write buffer and send command to the server.
 */
as_status
as_command_send(
	as_command* cmd, as_error* err, uint32_t comp_threshold, as_write_fn write_fn, void* udata
	);

/**
 * @private
 * Send command to the server.
 */
as_status
as_command_execute(as_command* cmd, as_error* err);

/**
 * @private
 * Parse header of server response.
 */
as_status
as_command_parse_header(as_error* err, as_node* node, uint8_t* buf, size_t size, void* udata);

/**
 * @private
 * Parse server record.  Used for reads.
 */
as_status
as_command_parse_result(as_error* err, as_node* node, uint8_t* buf, size_t size, void* udata);

/**
 * @private
 * Parse server success or failure result.
 */
as_status
as_command_parse_success_failure(as_error* err, as_node* node, uint8_t* buf, size_t size, void* udata);

/**
 * @private
 * Parse server success or failure bins.
 */
as_status
as_command_parse_success_failure_bins(uint8_t** pp, as_error* err, as_msg* msg, as_val** value);

/**
 * @private
 * Parse bins received from the server.
 */
as_status
as_command_parse_bins(uint8_t** pp, as_error* err, as_record* rec, uint32_t n_bins, bool deserialize);

/**
 * @private
 * Parse user defined function error.
 */
as_status
as_command_parse_udf_failure(uint8_t* p, as_error* err, as_msg* msg, as_status status);

/**
 * @private
 * Skip over fields section in returned data.
 */
uint8_t*
as_command_ignore_fields(uint8_t* p, uint32_t n_fields);

/**
 * @private
 * Skip over bins in returned data.
 */
uint8_t*
as_command_ignore_bins(uint8_t* p, uint32_t n_bins);

/**
 * @private
 * Parse key fields received from server.  Used for reads.
 */
uint8_t*
as_command_parse_key(uint8_t* p, uint32_t n_fields, as_key* key);

/**
 * @private
 * Return random task id if not specified.
 */
static inline uint64_t
as_task_id_resolve(uint64_t* task_id_ptr)
{
	if (! task_id_ptr) {
		return as_random_get_uint64();
	}

	if (*task_id_ptr == 0) {
		*task_id_ptr = as_random_get_uint64();
	}
	return *task_id_ptr;
}

#ifdef __cplusplus
} // end extern "C"
#endif
