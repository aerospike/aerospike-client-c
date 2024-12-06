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
#include <aerospike/as_error.h>
#include <citrusleaf/cf_byte_order.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * MACROS
 *****************************************************************************/

// Proto header version
#define AS_PROTO_VERSION 2

// Proto message types
#define AS_INFO_MESSAGE_TYPE 1
#define AS_ADMIN_MESSAGE_TYPE 2
#define AS_MESSAGE_TYPE 3
#define AS_COMPRESSED_MESSAGE_TYPE 4
#define PROTO_SIZE_MAX (128 * 1024 * 1024)

/******************************************************************************
 * TYPES
 *****************************************************************************/

#if defined(__APPLE__) || defined(_MSC_VER)

#pragma pack(push, 1) // packing is now 1
typedef struct as_proto_s {
	uint64_t	version	:8;
	uint64_t	type	:8;
	uint64_t	sz		:48;
} as_proto;
#pragma pack(pop) // packing is back to what it was

#pragma pack(push, 1) // packing is now 1
typedef struct as_compressed_proto_s {
	as_proto	proto;
	uint64_t	uncompressed_sz;
} as_compressed_proto;
#pragma pack(pop) // packing is back to what it was

#pragma pack(push, 1) // packing is now 1
typedef struct as_msg_s {
/*00*/	uint8_t		header_sz;			// number of uint8_ts in this header
/*01*/	uint8_t		info1;				// bitfield about this request
/*02*/	uint8_t		info2;
/*03*/	uint8_t		info3;
/*04*/	uint8_t		unused;
/*05*/	uint8_t		result_code;
/*06*/	uint32_t	generation;
/*10*/	uint32_t	record_ttl;
/*14*/	uint32_t	transaction_ttl;	// command time to live in milliseconds
/*18*/	uint16_t	n_fields;			// size in uint8_ts
/*20*/	uint16_t	n_ops;				// number of operations
/*22*/	uint8_t		data[0];			// data contains first the fields, then the ops
} as_msg;
#pragma pack(pop) // packing is back to what it was

#pragma pack(push, 1) // packing is now 1
typedef struct as_proto_msg_s {
	as_proto  	proto;
	as_msg		m;
} as_proto_msg;
#pragma pack(pop) // packing is back to what it was

#else

typedef struct as_proto_s {
	uint8_t		version;
	uint8_t		type;
	uint64_t	sz :48;
	uint8_t		data[0];
} __attribute__ ((__packed__)) as_proto;

typedef struct as_compressed_proto_s {
	as_proto	proto;
	uint64_t	uncompressed_sz;
	uint8_t		data[0];				// compressed bytes
} __attribute__((__packed__)) as_compressed_proto;

typedef struct as_msg_s {
/*00*/	uint8_t		header_sz;			// number of uint8_ts in this header
/*01*/	uint8_t		info1;				// bitfield about this request
/*02*/	uint8_t		info2;
/*03*/	uint8_t		info3;
/*04*/	uint8_t		unused;
/*05*/	uint8_t		result_code;
/*06*/	uint32_t	generation;
/*10*/	uint32_t	record_ttl;
/*14*/	uint32_t	transaction_ttl;	// command time to live in milliseconds
/*18*/	uint16_t	n_fields;			// size in uint8_ts
/*20*/	uint16_t	n_ops;				// number of operations
/*22*/	uint8_t		data[0];			// data contains first the fields, then the ops
} __attribute__((__packed__)) as_msg;

typedef struct as_proto_msg_s {
	as_proto  	proto;
	as_msg		m;
} __attribute__((__packed__)) as_proto_msg;

#endif

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

void as_proto_swap_to_be(as_proto *m);
void as_proto_swap_from_be(as_proto *m);
void as_msg_swap_header_from_be(as_msg *m);
as_status as_proto_version_error(as_error* err, as_proto* proto);
as_status as_proto_type_error(as_error* err, as_proto* proto, uint8_t expected);
as_status as_proto_size_error(as_error* err, size_t size);
as_status as_compressed_size_error(as_error* err, size_t size);
as_status as_proto_parse(as_error* err, as_proto* proto);
as_status as_proto_decompress(as_error* err, uint8_t* trg, size_t trg_sz, uint8_t* src, size_t src_sz);

static inline as_status
as_proto_parse_type(as_error* err, as_proto* proto, uint8_t expected_type)
{
	if (proto->type != expected_type) {
		return as_proto_type_error(err, proto, expected_type);
	}
	return as_proto_parse(err, proto);
}

static inline as_status
as_msg_parse(as_error* err, as_msg* msg, size_t size)
{
	if (size < sizeof(as_msg)) {
		return as_proto_size_error(err, size);
	}
	as_msg_swap_header_from_be(msg);
	return AEROSPIKE_OK;
}

static inline as_status
as_compressed_size_parse(as_error* err, const uint8_t* buf, size_t* size)
{
	*size = (size_t)cf_swap_from_be64(*(uint64_t*)buf);

	if (*size > PROTO_SIZE_MAX) {
		return as_compressed_size_error(err, *size);
	}
	return AEROSPIKE_OK;
}

#ifdef __cplusplus
} // end extern "C"
#endif
