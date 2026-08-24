/*
 * Copyright 2008-2026 Aerospike, Inc.
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
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_command.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>

#include "../test.h"

enum {
	AS_ERROR_DETAIL_KEY_SUBCODE = 1,
	AS_ERROR_DETAIL_KEY_MESSAGE = 2,
	AS_ERROR_DETAIL_KEY_EXP_TRACE = 3,

	AS_EXP_TRACE_KEY_PHASE = 1,
	AS_EXP_TRACE_KEY_BYTE_OFFSET = 2,
	AS_EXP_TRACE_KEY_OP = 3,
	AS_EXP_TRACE_KEY_DEPTH = 4,
	AS_EXP_TRACE_KEY_PATH = 5,
	AS_EXP_TRACE_KEY_SNIPPET = 6,
	AS_EXP_TRACE_KEY_OUTCOME = 7,
	AS_EXP_TRACE_KEY_LANG = 8,
	AS_EXP_TRACE_KEY_AEL_OFFSET = 9,
	AS_EXP_TRACE_KEY_AEL_SPAN = 10,
	AS_EXP_TRACE_KEY_OPERANDS = 13,

	AS_EXP_TRACE_PHASE_BUILD = 1,
	AS_EXP_TRACE_PHASE_EVAL = 2,
	AS_EXP_TRACE_OUTCOME_FAULT = 1,
	AS_EXP_TRACE_OUTCOME_FALSE = 2,
	AS_EXP_TRACE_OUTCOME_ABSENT = 3,
	AS_EXP_TRACE_LANG_MSGPACK = 1,
	AS_EXP_TRACE_LANG_AEL = 2,

	AS_EXP_TRACE_OP_MAX_SIZE = 32,
	AS_EXP_TRACE_PATH_MAX_DEPTH = 16,
	AS_EXP_TRACE_SNIPPET_MAX_SIZE = 256
};

extern aerospike* as;

//-------------------------------------
// Helpers: msgpack buffer construction
//-------------------------------------

static uint32_t
write_fixmap(uint8_t* buf, uint8_t count)
{
	buf[0] = 0x80 | (count & 0x0f);
	return 1;
}

static uint32_t
write_map16(uint8_t* buf, uint16_t count)
{
	buf[0] = 0xde;
	buf[1] = (uint8_t)(count >> 8);
	buf[2] = (uint8_t)(count);
	return 3;
}

static uint32_t
write_map32(uint8_t* buf, uint32_t count)
{
	buf[0] = 0xdf;
	buf[1] = (uint8_t)(count >> 24);
	buf[2] = (uint8_t)(count >> 16);
	buf[3] = (uint8_t)(count >> 8);
	buf[4] = (uint8_t)(count);
	return 5;
}

static uint32_t
write_fixint(uint8_t* buf, uint8_t val)
{
	buf[0] = val & 0x7f;
	return 1;
}

static uint32_t
write_uint8(uint8_t* buf, uint8_t val)
{
	buf[0] = 0xcc;
	buf[1] = val;
	return 2;
}

static uint32_t
write_uint16(uint8_t* buf, uint16_t val)
{
	buf[0] = 0xcd;
	buf[1] = (uint8_t)(val >> 8);
	buf[2] = (uint8_t)(val);
	return 3;
}

static uint32_t
write_uint32(uint8_t* buf, uint32_t val)
{
	buf[0] = 0xce;
	buf[1] = (uint8_t)(val >> 24);
	buf[2] = (uint8_t)(val >> 16);
	buf[3] = (uint8_t)(val >> 8);
	buf[4] = (uint8_t)(val);
	return 5;
}

static uint32_t
write_uint64(uint8_t* buf, uint64_t val)
{
	buf[0] = 0xcf;
	buf[1] = (uint8_t)(val >> 56);
	buf[2] = (uint8_t)(val >> 48);
	buf[3] = (uint8_t)(val >> 40);
	buf[4] = (uint8_t)(val >> 32);
	buf[5] = (uint8_t)(val >> 24);
	buf[6] = (uint8_t)(val >> 16);
	buf[7] = (uint8_t)(val >> 8);
	buf[8] = (uint8_t)(val);
	return 9;
}

static uint32_t
write_int8(uint8_t* buf, int8_t val)
{
	buf[0] = 0xd0;
	buf[1] = (uint8_t)val;
	return 2;
}

static uint32_t
write_fixstr(uint8_t* buf, const char* str, uint32_t len)
{
	buf[0] = 0xa0 | (len & 0x1f);
	memcpy(buf + 1, str, len);
	return 1 + len;
}

static uint32_t
write_str8(uint8_t* buf, const char* str, uint32_t len)
{
	buf[0] = 0xd9;
	buf[1] = (uint8_t)len;
	memcpy(buf + 2, str, len);
	return 2 + len;
}

static uint32_t
write_str16(uint8_t* buf, const char* str, uint32_t len)
{
	buf[0] = 0xda;
	buf[1] = (uint8_t)(len >> 8);
	buf[2] = (uint8_t)(len);
	memcpy(buf + 3, str, len);
	return 3 + len;
}

static uint32_t
write_fixarray(uint8_t* buf, uint8_t count)
{
	buf[0] = 0x90 | (count & 0x0f);
	return 1;
}

static uint32_t
write_array16(uint8_t* buf, uint16_t count)
{
	buf[0] = 0xdc;
	buf[1] = (uint8_t)(count >> 8);
	buf[2] = (uint8_t)(count);
	return 3;
}

static uint32_t
write_true(uint8_t* buf)
{
	buf[0] = 0xc3;
	return 1;
}

static uint32_t
write_fixext1(uint8_t* buf, int8_t type, uint8_t val)
{
	buf[0] = 0xd4;
	buf[1] = (uint8_t)type;
	buf[2] = val;
	return 3;
}

static uint32_t
write_ext8(uint8_t* buf, int8_t type, const uint8_t* val, uint8_t len)
{
	buf[0] = 0xc7;
	buf[1] = len;
	buf[2] = (uint8_t)type;
	memcpy(buf + 3, val, len);
	return 3 + len;
}

static uint32_t
write_error_detail_field(uint8_t* buf, const uint8_t* detail, uint32_t detail_len)
{
	uint32_t p = 0;
	*(uint32_t*)(buf + p) = cf_swap_to_be32(detail_len + 1);
	p += 4;
	buf[p++] = AS_FIELD_ERROR_DETAILS;
	memcpy(buf + p, detail, detail_len);
	return p + detail_len;
}

static uint32_t
write_op_string(uint8_t* buf, const char* name, const char* value)
{
	uint8_t name_len = (uint8_t)strlen(name);
	uint32_t value_len = (uint32_t)strlen(value);
	uint32_t op_size = name_len + value_len + 4;

	*(uint32_t*)buf = cf_swap_to_be32(op_size);
	buf[4] = 0;
	buf[5] = AS_BYTES_STRING;
	buf[6] = 0;
	buf[7] = name_len;
	memcpy(buf + 8, name, name_len);
	memcpy(buf + 8 + name_len, value, value_len);
	return op_size + 4;
}

static inline void
command_parse_error_details(as_error* err, uint8_t* buf, uint32_t len)
{
	as_command_parse_error_details(err, NULL, buf, len, AEROSPIKE_OK);
}

//---------------------------------
// Section 3: Parser Unit Tests
//---------------------------------

// 3.1 Empty map yields no error detail
TEST(error_detail_parser_empty_map, "3.1 empty map yields no error detail")
{
	as_error err;
	as_error_init(&err);

	uint8_t buf[1];
	uint32_t len = write_fixmap(buf, 0);

	command_parse_error_details(&err, buf, len);

	assert_string_eq(err.message, "AEROSPIKE_OK");
	assert_int_eq(err.subcode, AS_SUB_NONE);
}

// 3.2 Subcode only (fixint key 1)
TEST(error_detail_parser_subcode_only, "3.2 subcode only fixint")
{
	as_error err;
	as_error_init(&err);

	uint8_t buf[16];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 1);
	p += write_fixint(buf + p, 1);
	p += write_fixint(buf + p, 42);

	command_parse_error_details(&err, buf, p);

	assert_string_eq(err.message, "AEROSPIKE_OK");
	assert_int_eq(err.subcode, 42);
}

// 3.3 Message only (fixstr key 2)
TEST(error_detail_parser_message_only, "3.3 message only fixstr")
{
	as_error err;
	as_error_init(&err);

	const char* msg = "record not found";
	uint32_t msg_len = (uint32_t)strlen(msg);

	uint8_t buf[64];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 1);
	p += write_fixint(buf + p, 2);
	p += write_fixstr(buf + p, msg, msg_len);

	command_parse_error_details(&err, buf, p);

	assert_string_eq(err.message, "record not found");
	assert_int_eq(err.subcode, AS_SUB_NONE);
}

// 3.4 Subcode and message together
TEST(error_detail_parser_subcode_and_message, "3.4 subcode and message together")
{
	as_error err;
	as_error_init(&err);

	const char* msg = "delete generation mismatch";
	uint32_t msg_len = (uint32_t)strlen(msg);

	uint8_t buf[64];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 2);
	p += write_fixint(buf + p, 1);
	p += write_uint16(buf + p, 5001);
	p += write_fixint(buf + p, 2);
	p += write_fixstr(buf + p, msg, msg_len);

	command_parse_error_details(&err, buf, p);

	assert_string_eq(err.message, "delete generation mismatch");
	assert_int_eq(err.subcode, 5001);
}

// 3.5 Reversed key order (message before subcode)
TEST(error_detail_parser_reversed_keys, "3.5 reversed key order")
{
	as_error err;
	as_error_init(&err);

	const char* msg = "type mismatch";
	uint32_t msg_len = (uint32_t)strlen(msg);

	uint8_t buf[64];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 2);
	p += write_fixint(buf + p, 2);
	p += write_fixstr(buf + p, msg, msg_len);
	p += write_fixint(buf + p, 1);
	p += write_uint16(buf + p, 1100);

	command_parse_error_details(&err, buf, p);

	assert_string_eq(err.message, "type mismatch");
	assert_int_eq(err.subcode, 1100);
}

// 3.6 Unknown keys are skipped
TEST(error_detail_parser_unknown_keys, "3.6 unknown keys are skipped")
{
	as_error err;
	as_error_init(&err);

	const char* future_val = "future-key-value";
	const char* msg = "some message";

	uint8_t buf[128];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 4);
	// key 1 -> subcode 99
	p += write_fixint(buf + p, 1);
	p += write_fixint(buf + p, 99);
	// key 99 -> fixstr (unknown, must be skipped)
	p += write_fixint(buf + p, 99);
	p += write_fixstr(buf + p, future_val, (uint32_t)strlen(future_val));
	// key 2 -> message
	p += write_fixint(buf + p, 2);
	p += write_fixstr(buf + p, msg, (uint32_t)strlen(msg));

	command_parse_error_details(&err, buf, p);

	assert_string_eq(err.message, "some message");
	assert_int_eq(err.subcode, 99);
}

// 3.7 Unknown key with nested container value is skipped
TEST(error_detail_parser_nested_unknown, "3.7 unknown key with nested container")
{
	as_error err;
	as_error_init(&err);

	const char* nested_str = "nested";
	const char* msg = "msg";

	uint8_t buf[128];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 3);
	// key 1 -> 200
	p += write_fixint(buf + p, 1);
	p += write_uint8(buf + p, 200);
	// key 50 -> [1, "nested", {3: true}]
	p += write_fixint(buf + p, 50);
	p += write_fixarray(buf + p, 3);
	p += write_fixint(buf + p, 1);
	p += write_fixstr(buf + p, nested_str, (uint32_t)strlen(nested_str));
	p += write_fixmap(buf + p, 1);
	p += write_fixint(buf + p, 3);
	p += write_true(buf + p);
	// key 2 -> "msg"
	p += write_fixint(buf + p, 2);
	p += write_fixstr(buf + p, msg, (uint32_t)strlen(msg));

	command_parse_error_details(&err, buf, p);

	assert_string_eq(err.message, "msg");
	assert_int_eq(err.subcode, 200);
}

// 3.8 Truncated buffer returns null (no crash)
TEST(error_detail_parser_truncated, "3.8 truncated buffer no crash")
{
	as_error err;
	as_error_init(&err);

	uint8_t buf[2];
	buf[0] = 0x82; // fixmap 2 entries
	buf[1] = 0x01; // key 1, but value is missing

	command_parse_error_details(&err, buf, 2);

	assert_string_eq(err.message, "AEROSPIKE_OK");
	assert_int_eq(err.subcode, AS_SUB_NONE);
}

// 3.9 Zero-length buffer returns null
TEST(error_detail_parser_zero_length, "3.9 zero-length buffer")
{
	as_error err;
	as_error_init(&err);

	command_parse_error_details(&err, NULL, 0);

	assert_string_eq(err.message, "");
	assert_int_eq(err.subcode, AS_SUB_NONE);
}

// 3.10 Subcode encoded as uint8 (0xCC)
TEST(error_detail_parser_uint8_subcode, "3.10 subcode uint8")
{
	as_error err;
	as_error_init(&err);

	uint8_t buf[16];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 1);
	p += write_fixint(buf + p, 1);
	p += write_uint8(buf + p, 200);

	command_parse_error_details(&err, buf, p);

	assert_int_eq(err.subcode, 200);
	assert_string_eq(err.message, "AEROSPIKE_OK");
}

// 3.11 Subcode encoded as uint16 (0xCD)
TEST(error_detail_parser_uint16_subcode, "3.11 subcode uint16")
{
	as_error err;
	as_error_init(&err);

	uint8_t buf[16];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 1);
	p += write_fixint(buf + p, 1);
	p += write_uint16(buf + p, 3001);

	command_parse_error_details(&err, buf, p);

	assert_int_eq(err.subcode, 3001);
	assert_string_eq(err.message, "AEROSPIKE_OK");
}

// 3.12 Subcode encoded as uint32 (0xCE)
TEST(error_detail_parser_uint32_subcode, "3.12 subcode uint32")
{
	as_error err;
	as_error_init(&err);

	uint8_t buf[16];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 1);
	p += write_fixint(buf + p, 1);
	p += write_uint32(buf + p, 70000);

	command_parse_error_details(&err, buf, p);

	assert_int_eq(err.subcode, 70000);
	assert_string_eq(err.message, "AEROSPIKE_OK");
}

// 3.13 Subcode encoded as uint64 (0xCF)
TEST(error_detail_parser_uint64_subcode, "3.13 subcode uint64")
{
	as_error err;
	as_error_init(&err);

	uint8_t buf[16];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 1);
	p += write_fixint(buf + p, 1);
	p += write_uint64(buf + p, 100000);

	command_parse_error_details(&err, buf, p);

	assert_int_eq(err.subcode, 100000);
	assert_string_eq(err.message, "AEROSPIKE_OK");
}

// 3.14 Message encoded as str8 (0xD9)
TEST(error_detail_parser_str8_message, "3.14 message str8")
{
	as_error err;
	as_error_init(&err);

	const char* msg = "this is a 35 byte str8 message!!!!";
	uint32_t msg_len = (uint32_t)strlen(msg);

	uint8_t buf[128];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 1);
	p += write_fixint(buf + p, 2);
	p += write_str8(buf + p, msg, msg_len);

	command_parse_error_details(&err, buf, p);

	assert_string_eq(err.message, msg);
}

// 3.15 Message encoded as str16 (0xDA)
TEST(error_detail_parser_str16_message, "3.15 message str16")
{
	as_error err;
	as_error_init(&err);

	char msg[301];
	memset(msg, 'A', 300);
	msg[300] = '\0';

	uint8_t buf[512];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 1);
	p += write_fixint(buf + p, 2);
	p += write_str16(buf + p, msg, 300);

	command_parse_error_details(&err, buf, p);

	assert_string_eq(err.message, msg);
}

// 3.16 Unicode message (multi-byte UTF-8) round-trips
TEST(error_detail_parser_unicode, "3.16 unicode message round-trips")
{
	as_error err;
	as_error_init(&err);

	const char* msg = "Fehler: Schl\xc3\xbcsselkonflikt \xe2\x80\x94 \xe9\x94\xae\xe5\x86\xb2\xe7\xaa\x81";
	uint32_t msg_len = (uint32_t)strlen(msg);

	uint8_t buf[128];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 1);
	p += write_fixint(buf + p, 2);
	p += write_str8(buf + p, msg, msg_len);

	command_parse_error_details(&err, buf, p);

	assert_string_eq(err.message, msg);
}

// 3.17 map16 header parses correctly
TEST(error_detail_parser_map16, "3.17 map16 header")
{
	as_error err;
	as_error_init(&err);

	const char* msg = "map16 test";
	uint32_t msg_len = (uint32_t)strlen(msg);

	uint8_t buf[64];
	uint32_t p = 0;
	p += write_map16(buf + p, 2);
	p += write_fixint(buf + p, 1);
	p += write_uint16(buf + p, 500);
	p += write_fixint(buf + p, 2);
	p += write_fixstr(buf + p, msg, msg_len);

	command_parse_error_details(&err, buf, p);

	assert_string_eq(err.message, "map16 test");
	assert_int_eq(err.subcode, 500);
}

// 3.18 map32 header parses correctly
TEST(error_detail_parser_map32, "3.18 map32 header")
{
	as_error err;
	as_error_init(&err);

	const char* msg = "map32 test";
	uint32_t msg_len = (uint32_t)strlen(msg);

	uint8_t buf[64];
	uint32_t p = 0;
	p += write_map32(buf + p, 2);
	p += write_fixint(buf + p, 1);
	p += write_uint16(buf + p, 600);
	p += write_fixint(buf + p, 2);
	p += write_fixstr(buf + p, msg, msg_len);

	command_parse_error_details(&err, buf, p);

	assert_string_eq(err.message, "map32 test");
	assert_int_eq(err.subcode, 600);
}

// 3.19 Subcode zero is treated as valid
TEST(error_detail_parser_subcode_zero, "3.19 subcode zero is valid")
{
	as_error err;
	as_error_init(&err);

	const char* msg = "zero subcode";
	uint32_t msg_len = (uint32_t)strlen(msg);

	uint8_t buf[64];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 2);
	p += write_fixint(buf + p, 1);
	p += write_fixint(buf + p, 0);
	p += write_fixint(buf + p, 2);
	p += write_fixstr(buf + p, msg, msg_len);

	command_parse_error_details(&err, buf, p);

	assert_string_eq(err.message, "zero subcode");
	assert_int_eq(err.subcode, AS_SUB_NONE);
}

// 3.20 Large subcode value (boundary)
TEST(error_detail_parser_large_subcode, "3.20 large subcode boundary")
{
	as_error err;
	as_error_init(&err);

	const char* msg = "cross-cutting";
	uint32_t msg_len = (uint32_t)strlen(msg);

	uint8_t buf[64];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 2);
	p += write_fixint(buf + p, 1);
	p += write_uint32(buf + p, 99999);
	p += write_fixint(buf + p, 2);
	p += write_fixstr(buf + p, msg, msg_len);

	command_parse_error_details(&err, buf, p);

	assert_int_eq(err.subcode, 99999);
	assert_string_eq(err.message, "cross-cutting");
}

// 3.21 Message near AS_ERROR_MESSAGE_MAX_LEN with subcode suffix
TEST(error_detail_parser_near_max_len, "3.21 message near max length")
{
	as_error err;
	as_error_init(&err);

	char msg[1001];
	memset(msg, 'X', 1000);
	msg[1000] = '\0';

	uint8_t buf[2048];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 2);
	p += write_fixint(buf + p, 1);
	p += write_uint32(buf + p, 99999);
	p += write_fixint(buf + p, 2);
	p += write_str16(buf + p, msg, 1000);

	command_parse_error_details(&err, buf, p);

	assert_int_eq(err.subcode, 99999);
	assert_true(err.message[0] != '\0');
	assert_not_null(memchr(err.message, '\0', AS_ERROR_MESSAGE_MAX_SIZE));
	assert_true(strlen(err.message) <= 1023);
}

// 3.22 Message exceeding buffer capacity is safely truncated
TEST(error_detail_parser_overflow_truncation, "3.22 overflow is truncated")
{
	as_error err;
	as_error_init(&err);

	char msg[1021];
	memset(msg, 'Y', 1020);
	msg[1020] = '\0';

	uint8_t buf[2048];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 2);
	p += write_fixint(buf + p, 1);
	p += write_fixint(buf + p, 1);
	p += write_fixint(buf + p, 2);
	p += write_str16(buf + p, msg, 1020);

	command_parse_error_details(&err, buf, p);

	assert_int_eq(err.subcode, 1);
	assert_true(err.message[0] != '\0');
	assert_not_null(memchr(err.message, '\0', AS_ERROR_MESSAGE_MAX_SIZE));
	assert_true(strlen(err.message) <= 1023);
}

// 3.23 Full expression trace is parsed into bounded public fields
TEST(error_detail_parser_exp_trace_full, "3.23 full expression trace parse")
{
	as_error err;
	as_error_init(&err);

	const char* msg = "expression failed";
	const char* op = "cmp_eq";
	const char* path0 = "root";
	const char* path1 = "bin";
	const char* path2 = "leaf";
	const char* snippet = "ibin == 99999";

	uint8_t buf[512];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 3);
	p += write_fixint(buf + p, AS_ERROR_DETAIL_KEY_SUBCODE);
	p += write_uint16(buf + p, 4242);
	p += write_fixint(buf + p, AS_ERROR_DETAIL_KEY_MESSAGE);
	p += write_fixstr(buf + p, msg, (uint32_t)strlen(msg));
	p += write_fixint(buf + p, AS_ERROR_DETAIL_KEY_EXP_TRACE);
	p += write_fixmap(buf + p, 11);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_PHASE);
	p += write_fixint(buf + p, AS_EXP_TRACE_PHASE_EVAL);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_BYTE_OFFSET);
	p += write_uint16(buf + p, 1234);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_OP);
	p += write_fixstr(buf + p, op, (uint32_t)strlen(op));
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_DEPTH);
	p += write_fixint(buf + p, 3);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_PATH);
	p += write_fixarray(buf + p, 3);
	p += write_fixstr(buf + p, path0, (uint32_t)strlen(path0));
	p += write_fixstr(buf + p, path1, (uint32_t)strlen(path1));
	p += write_fixstr(buf + p, path2, (uint32_t)strlen(path2));
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_SNIPPET);
	p += write_fixstr(buf + p, snippet, (uint32_t)strlen(snippet));
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_OUTCOME);
	p += write_fixint(buf + p, AS_EXP_TRACE_OUTCOME_FALSE);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_LANG);
	p += write_fixint(buf + p, AS_EXP_TRACE_LANG_AEL);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_AEL_OFFSET);
	p += write_fixint(buf + p, 10);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_AEL_SPAN);
	p += write_fixint(buf + p, 4);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_OPERANDS);
	p += write_fixarray(buf + p, 2);
	p += write_fixstr(buf + p, "100", 3);
	p += write_fixstr(buf + p, "99999", 5);

	command_parse_error_details(&err, buf, p);

	assert_int_eq(err.subcode, 4242);
	assert_true(strstr(err.message, "expression failed; exp_trace={") == err.message);
	assert_true(strstr(err.message, "phase=\"eval\"") != NULL);
	assert_true(strstr(err.message, "byte_offset=1234") != NULL);
	assert_true(strstr(err.message, "op=\"cmp_eq\"") != NULL);
	assert_true(strstr(err.message, "depth=3") != NULL);
	assert_true(strstr(err.message, "path=[\"root\",\"bin\",\"leaf\"]") != NULL);
	assert_true(strstr(err.message, "snippet=\"ibin == 99999\"") != NULL);
	assert_true(strstr(err.message, "outcome=\"false\"") != NULL);
	assert_true(strstr(err.message, "lang=\"ael\"") != NULL);
	assert_true(strstr(err.message, "ael_offset=10") != NULL);
	assert_true(strstr(err.message, "ael_span=4") != NULL);
	assert_true(strstr(err.message, "operands=[\"100\",\"99999\"]") != NULL);
}

// 3.24 Oversized trace arrays and strings are truncated into bounded storage
TEST(error_detail_parser_exp_trace_truncation, "3.24 expression trace truncation")
{
	as_error err;
	as_error_init(&err);

	char long_op[80];
	char long_path[80];
	char long_snippet[400];

	memset(long_op, 'O', sizeof(long_op) - 1);
	long_op[sizeof(long_op) - 1] = '\0';
	memset(long_path, 'P', sizeof(long_path) - 1);
	long_path[sizeof(long_path) - 1] = '\0';
	memset(long_snippet, 'S', sizeof(long_snippet) - 1);
	long_snippet[sizeof(long_snippet) - 1] = '\0';

	uint8_t buf[4096];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 1);
	p += write_fixint(buf + p, AS_ERROR_DETAIL_KEY_EXP_TRACE);
	p += write_fixmap(buf + p, 3);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_OP);
	p += write_str8(buf + p, long_op, (uint32_t)strlen(long_op));
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_PATH);
	p += write_array16(buf + p, AS_EXP_TRACE_PATH_MAX_DEPTH + 2);
	for (uint32_t i = 0; i < AS_EXP_TRACE_PATH_MAX_DEPTH + 2; i++) {
		p += write_str8(buf + p, long_path, (uint32_t)strlen(long_path));
	}
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_SNIPPET);
	p += write_str16(buf + p, long_snippet, (uint32_t)strlen(long_snippet));

	command_parse_error_details(&err, buf, p);

	assert_true(strstr(err.message, "AEROSPIKE_OK; exp_trace={") == err.message);
	assert_true(strstr(err.message, "op=\"OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO\"") != NULL);
	assert_true(strstr(err.message, "path=[\"PPPPPPPPPPPPPPPPPPPPPPPPPPPPPPP\"") != NULL);
	assert_true(strstr(err.message, "snippet=\"SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS") != NULL);
	assert_not_null(memchr(err.message, '\0', AS_ERROR_MESSAGE_MAX_SIZE));
	assert_true(strlen(err.message) <= AS_ERROR_MESSAGE_MAX_LEN);
}

// 3.25 Reserved server keys are skipped and operands are kept without blocking later fields.
TEST(error_detail_parser_exp_trace_skips_dropped_keys,
	"3.25 expression trace skips reserved keys and keeps operands")
{
	as_error err;
	as_error_init(&err);

	uint8_t buf[256];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 1);
	p += write_fixint(buf + p, AS_ERROR_DETAIL_KEY_EXP_TRACE);
	p += write_fixmap(buf + p, 4);
	p += write_fixint(buf + p, 11);
	p += write_fixint(buf + p, 2);
	p += write_fixint(buf + p, 12);
	p += write_fixint(buf + p, 8);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_OPERANDS);
	p += write_fixarray(buf + p, 2);
	p += write_fixstr(buf + p, "ibin", 4);
	p += write_fixstr(buf + p, "99999", 5);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_OP);
	p += write_fixstr(buf + p, "cmp_eq", 6);

	command_parse_error_details(&err, buf, p);

	assert_true(strstr(err.message, "AEROSPIKE_OK; exp_trace={") == err.message);
	assert_true(strstr(err.message, "op=\"cmp_eq\"") != NULL);
	assert_true(strstr(err.message, "operands=[\"ibin\",\"99999\"]") != NULL);
}

// 3.25.6 Operand strings longer than the client buffer are clipped.
TEST(error_detail_parser_exp_trace_operand_clipping, "3.25.6 expression trace operand clipping")
{
	as_error err;
	as_error_init(&err);

	char long_lhs[56];
	memset(long_lhs, 'a', 55);
	long_lhs[55] = '\0';

	char clipped[49];
	memset(clipped, 'a', 48);
	clipped[48] = '\0';

	char expect[96];
	snprintf(expect, sizeof(expect), "operands=[\"%s\",\"ok\"]", clipped);

	uint8_t buf[512];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 1);
	p += write_fixint(buf + p, AS_ERROR_DETAIL_KEY_EXP_TRACE);
	p += write_fixmap(buf + p, 2);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_OPERANDS);
	p += write_fixarray(buf + p, 2);
	p += write_str8(buf + p, long_lhs, 55);
	p += write_fixstr(buf + p, "ok", 2);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_OUTCOME);
	p += write_fixint(buf + p, AS_EXP_TRACE_OUTCOME_FALSE);

	command_parse_error_details(&err, buf, p);

	assert_true(strstr(err.message, expect) != NULL);

	char too_long[50];
	memset(too_long, 'a', 49);
	too_long[49] = '\0';
	assert_true(strstr(err.message, too_long) == NULL);
}

// 3.26 Unknown/dropped-only trace maps do not surface as public exp_trace data.
TEST(error_detail_parser_exp_trace_dropped_only_ignored,
	"3.26 dropped-only expression trace is ignored")
{
	as_error err;
	as_error_init(&err);

	uint8_t buf[256];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 1);
	p += write_fixint(buf + p, AS_ERROR_DETAIL_KEY_EXP_TRACE);
	p += write_fixmap(buf + p, 3);
	p += write_fixint(buf + p, 11);
	p += write_fixint(buf + p, 2);
	p += write_fixint(buf + p, 12);
	p += write_fixint(buf + p, 8);
	p += write_fixint(buf + p, 99);
	p += write_fixstr(buf + p, "future", 6);

	command_parse_error_details(&err, buf, p);

	assert_int_eq(err.subcode, 0);
	assert_string_eq(err.message, "AEROSPIKE_OK");
}

// 3.25.1 Outcome and operands are decoded together.
TEST(error_detail_parser_exp_trace_outcome_operands, "3.25.1 outcome and operands parse")
{
	as_error err;
	as_error_init(&err);

	uint8_t buf[256];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 1);
	p += write_fixint(buf + p, AS_ERROR_DETAIL_KEY_EXP_TRACE);
	p += write_fixmap(buf + p, 4);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_PHASE);
	p += write_fixint(buf + p, AS_EXP_TRACE_PHASE_EVAL);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_OP);
	p += write_fixstr(buf + p, "cmp_gt", 6);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_OUTCOME);
	p += write_fixint(buf + p, AS_EXP_TRACE_OUTCOME_FALSE);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_OPERANDS);
	p += write_fixarray(buf + p, 2);
	p += write_fixstr(buf + p, "15", 2);
	p += write_fixstr(buf + p, "18", 2);

	command_parse_error_details(&err, buf, p);

	assert_true(strstr(err.message, "phase=\"eval\"") != NULL);
	assert_true(strstr(err.message, "outcome=\"false\"") != NULL);
	assert_true(strstr(err.message, "operands=[\"15\",\"18\"]") != NULL);
}

// 3.25.2 Fault and absent outcomes carry no operands.
TEST(error_detail_parser_exp_trace_outcome_no_operands,
	"3.25.2 fault and absent outcomes omit operands")
{
	as_error err;
	as_error_init(&err);

	const uint8_t outcomes[] = {
		AS_EXP_TRACE_OUTCOME_FAULT,
		AS_EXP_TRACE_OUTCOME_ABSENT
	};

	for (size_t i = 0; i < sizeof(outcomes); i++) {
		as_error_init(&err);

		uint8_t buf[256];
		uint32_t p = 0;
		p += write_fixmap(buf + p, 1);
		p += write_fixint(buf + p, AS_ERROR_DETAIL_KEY_EXP_TRACE);
		p += write_fixmap(buf + p, 2);
		p += write_fixint(buf + p, AS_EXP_TRACE_KEY_PHASE);
		p += write_fixint(buf + p, AS_EXP_TRACE_PHASE_EVAL);
		p += write_fixint(buf + p, AS_EXP_TRACE_KEY_OUTCOME);
		p += write_fixint(buf + p, outcomes[i]);

		command_parse_error_details(&err, buf, p);

		assert_true(strstr(err.message, "operands=") == NULL);
		if (outcomes[i] == AS_EXP_TRACE_OUTCOME_FAULT) {
			assert_true(strstr(err.message, "outcome=\"fault\"") != NULL);
		}
		else {
			assert_true(strstr(err.message, "outcome=\"absent\"") != NULL);
		}
	}
}

// 3.25.3 FALSE outcome does not guarantee operands when the server drops them.
TEST(error_detail_parser_exp_trace_false_without_operands,
	"3.25.3 false outcome without operands")
{
	as_error err;
	as_error_init(&err);

	uint8_t buf[256];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 1);
	p += write_fixint(buf + p, AS_ERROR_DETAIL_KEY_EXP_TRACE);
	p += write_fixmap(buf + p, 2);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_PHASE);
	p += write_fixint(buf + p, AS_EXP_TRACE_PHASE_EVAL);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_OUTCOME);
	p += write_fixint(buf + p, AS_EXP_TRACE_OUTCOME_FALSE);

	command_parse_error_details(&err, buf, p);

	assert_true(strstr(err.message, "outcome=\"false\"") != NULL);
	assert_true(strstr(err.message, "operands=") == NULL);
}

// 3.25.4 Build traces omit eval-phase explainer fields.
TEST(error_detail_parser_exp_trace_build_without_outcome,
	"3.25.4 build trace omits outcome and operands")
{
	as_error err;
	as_error_init(&err);

	uint8_t buf[256];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 1);
	p += write_fixint(buf + p, AS_ERROR_DETAIL_KEY_EXP_TRACE);
	p += write_fixmap(buf + p, 2);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_PHASE);
	p += write_fixint(buf + p, AS_EXP_TRACE_PHASE_BUILD);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_BYTE_OFFSET);
	p += write_fixint(buf + p, 3);

	command_parse_error_details(&err, buf, p);

	assert_true(strstr(err.message, "phase=\"build\"") != NULL);
	assert_true(strstr(err.message, "byte_offset=3") != NULL);
	assert_true(strstr(err.message, "outcome=") == NULL);
	assert_true(strstr(err.message, "operands=") == NULL);
}

// 3.25.5 Path arrays past fixarray size retain the server truncation sentinel.
TEST(error_detail_parser_exp_trace_max_path, "3.25.5 expression trace max path frames")
{
	as_error err;
	as_error_init(&err);

	uint8_t buf[1024];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 1);
	p += write_fixint(buf + p, AS_ERROR_DETAIL_KEY_EXP_TRACE);
	p += write_fixmap(buf + p, 2);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_DEPTH);
	p += write_fixint(buf + p, 40);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_PATH);
	p += write_array16(buf + p, AS_EXP_TRACE_PATH_MAX_DEPTH);
	for (uint32_t i = 0; i < 14; i++) {
		p += write_fixstr(buf + p, "and", 3);
	}
	p += write_fixstr(buf + p, "...", 3);
	p += write_fixstr(buf + p, "eq", 2);

	command_parse_error_details(&err, buf, p);

	assert_true(strstr(err.message, "depth=40") != NULL);
	assert_true(strstr(err.message, "\"...\"") != NULL);
	assert_true(strstr(err.message, "\"eq\"") != NULL);
}

// 3.26.0 Truncated path markers are preserved as ordinary path frames.
TEST(error_detail_parser_exp_trace_path_truncation_marker,
	"3.26.0 expression trace path marker is preserved")
{
	as_error err;
	as_error_init(&err);

	uint8_t buf[256];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 1);
	p += write_fixint(buf + p, AS_ERROR_DETAIL_KEY_EXP_TRACE);
	p += write_fixmap(buf + p, 1);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_PATH);
	p += write_fixarray(buf + p, 3);
	p += write_fixstr(buf + p, "root", 4);
	p += write_fixstr(buf + p, "...", 3);
	p += write_fixstr(buf + p, "leaf", 4);

	command_parse_error_details(&err, buf, p);

	assert_true(strstr(err.message, "path=[\"root\",\"...\",\"leaf\"]") != NULL);
}

// 3.26.1 Malformed operands are dropped without losing other trace fields.
TEST(error_detail_parser_exp_trace_malformed_operands,
	"3.26.1 malformed operands preserve trace fields")
{
	as_error err;
	as_error_init(&err);

	uint8_t buf[256];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 1);
	p += write_fixint(buf + p, AS_ERROR_DETAIL_KEY_EXP_TRACE);
	p += write_fixmap(buf + p, 3);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_OPERANDS);
	p += write_fixarray(buf + p, 2);
	p += write_fixstr(buf + p, "ibin", 4);
	p += write_fixint(buf + p, 99);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_OP);
	p += write_fixstr(buf + p, "cmp_eq", 6);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_OUTCOME);
	p += write_fixint(buf + p, AS_EXP_TRACE_OUTCOME_FALSE);

	command_parse_error_details(&err, buf, p);

	assert_true(strstr(err.message, "operands=") == NULL);
	assert_true(strstr(err.message, "op=\"cmp_eq\"") != NULL);
	assert_true(strstr(err.message, "outcome=\"false\"") != NULL);
}

// 3.26.2 Unknown signed integers and extension values do not block later fields.
TEST(error_detail_parser_unknown_signed_and_ext,
	"3.26.2 unknown signed and ext values are skipped")
{
	as_error err;
	as_error_init(&err);

	uint8_t ext[] = {0xaa, 0xbb};
	uint8_t buf[256];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 5);
	p += write_fixint(buf + p, 50);
	p += write_int8(buf + p, -7);
	p += write_fixint(buf + p, 51);
	p += write_fixext1(buf + p, 1, 0xee);
	p += write_fixint(buf + p, 52);
	p += write_ext8(buf + p, 2, ext, sizeof(ext));
	p += write_fixint(buf + p, AS_ERROR_DETAIL_KEY_SUBCODE);
	p += write_fixint(buf + p, 17);
	p += write_fixint(buf + p, AS_ERROR_DETAIL_KEY_MESSAGE);
	p += write_fixstr(buf + p, "after unknowns", 14);

	command_parse_error_details(&err, buf, p);

	assert_int_eq(err.subcode, 17);
	assert_string_eq(err.message, "after unknowns");
}

// 3.27 Invalid expression trace payload is ignored without losing other details
TEST(error_detail_parser_invalid_exp_trace, "3.25 invalid expression trace is ignored")
{
	as_error err;
	as_error_init(&err);

	const char* msg = "detail survives";
	const char* invalid = "not-a-map";

	uint8_t buf[128];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 3);
	p += write_fixint(buf + p, AS_ERROR_DETAIL_KEY_SUBCODE);
	p += write_fixint(buf + p, 7);
	p += write_fixint(buf + p, AS_ERROR_DETAIL_KEY_MESSAGE);
	p += write_fixstr(buf + p, msg, (uint32_t)strlen(msg));
	p += write_fixint(buf + p, AS_ERROR_DETAIL_KEY_EXP_TRACE);
	p += write_fixstr(buf + p, invalid, (uint32_t)strlen(invalid));

	command_parse_error_details(&err, buf, p);

	assert_int_eq(err.subcode, 7);
	assert_string_eq(err.message, "detail survives");
}

// 3.28 UDF FAILURE text preserves trace suffix in err.message.
TEST(error_detail_udf_message_preserves_server_detail,
	"3.28 UDF message preserves parsed server detail")
{
	as_error err;
	as_error_init(&err);

	uint8_t detail_buf[128];
	uint32_t detail_len = 0;
	detail_len += write_fixmap(detail_buf + detail_len, 2);
	detail_len += write_fixint(detail_buf + detail_len, AS_ERROR_DETAIL_KEY_SUBCODE);
	detail_len += write_fixint(detail_buf + detail_len, 7);
	detail_len += write_fixint(detail_buf + detail_len, AS_ERROR_DETAIL_KEY_EXP_TRACE);
	detail_len += write_fixmap(detail_buf + detail_len, 2);
	detail_len += write_fixint(detail_buf + detail_len, AS_EXP_TRACE_KEY_PHASE);
	detail_len += write_fixint(detail_buf + detail_len, AS_EXP_TRACE_PHASE_BUILD);
	detail_len += write_fixint(detail_buf + detail_len, AS_EXP_TRACE_KEY_OP);
	detail_len += write_fixstr(detail_buf + detail_len, "cmp_eq", 6);

	command_parse_error_details(&err, detail_buf, detail_len);

	as_msg msg;
	memset(&msg, 0, sizeof(msg));
	msg.n_ops = 1;

	uint8_t udf_buf[128];
	write_op_string(udf_buf, "FAILURE", "test failure from error_detail_udf");

	as_node* node = as_node_get_random(as->cluster);
	as_status status = as_command_parse_udf_failure(udf_buf, &err, node, &msg, AEROSPIKE_ERR_UDF);
	as_node_release(node);

	assert_int_eq(status, AEROSPIKE_ERR_UDF);
	assert_int_eq(err.subcode, 7);

	// UDF messages override server detailed messages, so the expression trace will not be included.
	assert_true(strstr(err.message, "test failure from error_detail_udf") != NULL);
	assert_true(strstr(err.message, "exp_trace") == NULL);
}

// 3.28.1 UDF fallback text also preserves parsed server detail.
TEST(error_detail_udf_fallback_preserves_server_detail,
	"3.28.1 UDF fallback preserves parsed server detail")
{
	as_error err;
	as_error_init(&err);

	uint8_t detail_buf[128];
	uint32_t detail_len = 0;
	detail_len += write_fixmap(detail_buf + detail_len, 2);
	detail_len += write_fixint(detail_buf + detail_len, AS_ERROR_DETAIL_KEY_SUBCODE);
	detail_len += write_fixint(detail_buf + detail_len, 7);
	detail_len += write_fixint(detail_buf + detail_len, AS_ERROR_DETAIL_KEY_EXP_TRACE);
	detail_len += write_fixmap(detail_buf + detail_len, 2);
	detail_len += write_fixint(detail_buf + detail_len, AS_EXP_TRACE_KEY_PHASE);
	detail_len += write_fixint(detail_buf + detail_len, AS_EXP_TRACE_PHASE_BUILD);
	detail_len += write_fixint(detail_buf + detail_len, AS_EXP_TRACE_KEY_OP);
	detail_len += write_fixstr(detail_buf + detail_len, "cmp_eq", 6);

	as_command_parse_error_details(&err, NULL, detail_buf, detail_len, AEROSPIKE_ERR_UDF);

	as_msg msg;
	memset(&msg, 0, sizeof(msg));
	msg.n_ops = 1;

	uint8_t udf_buf[128];
	write_op_string(udf_buf, "OTHER", "not the failure bin");

	as_node* node = as_node_get_random(as->cluster);
	as_status status = as_command_parse_udf_failure(udf_buf, &err, node, &msg, AEROSPIKE_ERR_UDF);
	as_node_release(node);

	assert_int_eq(status, AEROSPIKE_ERR_UDF);
	assert_int_eq(err.subcode, 7);
	assert_true(strstr(err.message, as_error_string(AEROSPIKE_ERR_UDF)) != NULL);
	assert_true(strstr(err.message, "; exp_trace={") != NULL);
	assert_true(strstr(err.message, "phase=\"build\"") != NULL);
	assert_true(strstr(err.message, "op=\"cmp_eq\"") != NULL);
}

// 3.29 Trace-only detail still receives generic status text when surfaced
TEST(error_detail_parser_exp_trace_only_status_fallback,
	"3.29 trace-only detail keeps fallback status text")
{
	as_error err;
	as_error_init(&err);

	uint8_t buf[128];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 1);
	p += write_fixint(buf + p, AS_ERROR_DETAIL_KEY_EXP_TRACE);
	p += write_fixmap(buf + p, 2);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_PHASE);
	p += write_fixint(buf + p, AS_EXP_TRACE_PHASE_BUILD);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_OP);
	p += write_fixstr(buf + p, "cmp_eq", 6);

	as_command_parse_error_details(&err, NULL, buf, p, AEROSPIKE_ERR_REQUEST_INVALID);

	assert_true(strstr(err.message, "; exp_trace={") != NULL);

	as_error_set_node(&err, NULL, AEROSPIKE_ERR_REQUEST_INVALID);

	assert_int_eq(err.code, AEROSPIKE_ERR_REQUEST_INVALID);
	assert_true(strstr(err.message, as_error_string(AEROSPIKE_ERR_REQUEST_INVALID)) != NULL);
	assert_true(strstr(err.message, "; exp_trace={") != NULL);
	assert_true(strstr(err.message, "phase=\"build\"") != NULL);
	assert_true(strstr(err.message, "op=\"cmp_eq\"") != NULL);
}

// 3.30 Trace-only detail still receives address-formatted fallback text
TEST(error_detail_parser_exp_trace_only_address_fallback,
	"3.30 trace-only detail keeps fallback address text")
{
	as_error err;
	as_error_init(&err);

	uint8_t buf[128];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 1);
	p += write_fixint(buf + p, AS_ERROR_DETAIL_KEY_EXP_TRACE);
	p += write_fixmap(buf + p, 2);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_PHASE);
	p += write_fixint(buf + p, AS_EXP_TRACE_PHASE_BUILD);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_OP);
	p += write_fixstr(buf + p, "cmp_eq", 6);

	as_node* node = as_node_get_random(as->cluster);
	as_command_parse_error_details(&err, node, buf, p, AEROSPIKE_ERR_REQUEST_INVALID);

	assert_true(strstr(err.message, "; exp_trace={") != NULL);

	as_error_set_node(&err, node, AEROSPIKE_ERR_REQUEST_INVALID);

	char expected[AS_ERROR_MESSAGE_MAX_SIZE];
	snprintf(expected, sizeof(expected), "%s %s", as_node_get_address_string(node),
		as_error_string(AEROSPIKE_ERR_REQUEST_INVALID));

	as_node_release(node);

	assert_int_eq(err.code, AEROSPIKE_ERR_REQUEST_INVALID);
	assert_true(strstr(err.message, expected) == err.message);
	assert_true(strstr(err.message, "; exp_trace={") != NULL);
	assert_true(strstr(err.message, "phase=\"build\"") != NULL);
	assert_true(strstr(err.message, "op=\"cmp_eq\"") != NULL);
}

// 3.31 Omitted lang is not rendered in trace text
TEST(error_detail_parser_exp_trace_omitted_lang_defaults_msgpack,
	"3.31 expression trace omitted lang is absent")
{
	as_error err;
	as_error_init(&err);

	uint8_t buf[128];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 1);
	p += write_fixint(buf + p, AS_ERROR_DETAIL_KEY_EXP_TRACE);
	p += write_fixmap(buf + p, 2);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_PHASE);
	p += write_fixint(buf + p, AS_EXP_TRACE_PHASE_EVAL);
	p += write_fixint(buf + p, AS_EXP_TRACE_KEY_OP);
	p += write_fixstr(buf + p, "unknown", 7);

	command_parse_error_details(&err, buf, p);

	assert_true(strstr(err.message, "phase=\"eval\"") != NULL);
	assert_true(strstr(err.message, "op=\"unknown\"") != NULL);
	assert_true(strstr(err.message, "lang=") == NULL);
}

// 3.32 Field-section parsing extracts the field-45 body written by the server.
TEST(error_detail_parser_fields_err_extracts_detail,
	"3.32 field parser extracts error detail")
{
	as_error err;
	as_error_init(&err);

	uint8_t detail[128];
	uint32_t detail_len = 0;
	detail_len += write_fixmap(detail + detail_len, 2);
	detail_len += write_fixint(detail + detail_len, AS_ERROR_DETAIL_KEY_SUBCODE);
	detail_len += write_fixint(detail + detail_len, 7);
	detail_len += write_fixint(detail + detail_len, AS_ERROR_DETAIL_KEY_MESSAGE);
	detail_len += write_fixstr(detail + detail_len, "fatal detail", 12);

	uint8_t fields[160];
	uint32_t p = 0;
	*(uint32_t*)(fields + p) = cf_swap_to_be32(detail_len + 1);
	p += 4;
	fields[p++] = AS_FIELD_ERROR_DETAILS;
	memcpy(fields + p, detail, detail_len);
	p += detail_len;

	as_msg msg;
	msg.result_code = AEROSPIKE_ERR_RECORD_NOT_FOUND;
	msg.n_fields = 1;

	uint8_t* end = as_command_parse_fields_err(fields, &err, NULL, &msg);

	assert_true(end == fields + p);
	assert_int_eq(err.subcode, 7);
	assert_string_eq(err.message, "fatal detail");
}

// 3.33 Batch command-level LAST errors parse field 45 before applying status text.
TEST(error_detail_parser_batch_last_error_fields,
	"3.33 batch LAST fatal error parses field 45")
{
	as_error err;
	as_error_init(&err);

	uint8_t detail[128];
	uint32_t detail_len = 0;
	detail_len += write_fixmap(detail + detail_len, 2);
	detail_len += write_fixint(detail + detail_len, AS_ERROR_DETAIL_KEY_SUBCODE);
	detail_len += write_fixint(detail + detail_len, 7);
	detail_len += write_fixint(detail + detail_len, AS_ERROR_DETAIL_KEY_MESSAGE);
	detail_len += write_fixstr(detail + detail_len, "batch fatal detail", 18);

	uint8_t fields[160];
	uint32_t fields_len = write_error_detail_field(fields, detail, detail_len);

	as_msg msg;
	memset(&msg, 0, sizeof(msg));
	msg.info3 = AS_MSG_INFO3_LAST;
	msg.result_code = AEROSPIKE_ERR_BATCH_MAX_REQUESTS_EXCEEDED;
	msg.n_fields = 1;

	as_status status = as_command_parse_error(&err, NULL, &msg, fields);

	assert_int_eq(fields_len, 4 + 1 + detail_len);
	assert_int_eq(status, AEROSPIKE_ERR_BATCH_MAX_REQUESTS_EXCEEDED);
	assert_int_eq(err.code, AEROSPIKE_ERR_BATCH_MAX_REQUESTS_EXCEEDED);
	assert_int_eq(err.subcode, 7);
	assert_string_eq(err.message, "batch fatal detail");
}

// 3.34 Sync transaction verify/roll status paths preserve field 45 details.
TEST(error_detail_parser_sync_txn_status_error_fields,
	"3.34 sync txn status error parses field 45")
{
	as_error err;
	as_error_init(&err);

	uint8_t detail[128];
	uint32_t detail_len = 0;
	detail_len += write_fixmap(detail + detail_len, 2);
	detail_len += write_fixint(detail + detail_len, AS_ERROR_DETAIL_KEY_SUBCODE);
	detail_len += write_fixint(detail + detail_len, 3);
	detail_len += write_fixint(detail + detail_len, AS_ERROR_DETAIL_KEY_MESSAGE);
	detail_len += write_fixstr(detail + detail_len, "txn verify detail", 17);

	uint8_t fields[160];
	write_error_detail_field(fields, detail, detail_len);

	as_msg msg;
	memset(&msg, 0, sizeof(msg));
	msg.result_code = AEROSPIKE_MRT_VERSION_MISMATCH;
	msg.n_fields = 1;

	as_status status = as_command_parse_error(&err, NULL, &msg, fields);

	assert_int_eq(status, AEROSPIKE_MRT_VERSION_MISMATCH);
	assert_int_eq(err.code, AEROSPIKE_MRT_VERSION_MISMATCH);
	assert_true(err.subcode != 0 || err.message[0] != '\0');
	assert_int_eq(err.subcode, 3);
	assert_string_eq(err.message, "txn verify detail");
}

// 3.35 Async transaction verify/roll status paths use the same field 45 preservation.
TEST(error_detail_parser_async_txn_status_error_fields,
	"3.35 async txn status error parses field 45")
{
	as_error err;
	as_error_init(&err);

	uint8_t detail[128];
	uint32_t detail_len = 0;
	detail_len += write_fixmap(detail + detail_len, 2);
	detail_len += write_fixint(detail + detail_len, AS_ERROR_DETAIL_KEY_SUBCODE);
	detail_len += write_fixint(detail + detail_len, 4);
	detail_len += write_fixint(detail + detail_len, AS_ERROR_DETAIL_KEY_MESSAGE);
	detail_len += write_fixstr(detail + detail_len, "txn roll detail", 15);

	uint8_t fields[160];
	write_error_detail_field(fields, detail, detail_len);

	as_msg msg;
	memset(&msg, 0, sizeof(msg));
	msg.result_code = AEROSPIKE_MRT_EXPIRED;
	msg.n_fields = 1;

	as_status status = as_command_parse_error(&err, NULL, &msg, fields);

	assert_int_eq(status, AEROSPIKE_MRT_EXPIRED);
	assert_int_eq(err.code, AEROSPIKE_MRT_EXPIRED);
	assert_true(err.subcode != 0 || err.message[0] != '\0');
	assert_int_eq(err.subcode, 4);
	assert_string_eq(err.message, "txn roll detail");
}

//-------------------------------------------------------
// Section 4: Policy Field and Header Serialisation Tests
//-------------------------------------------------------

// 4.1 Default policy has verbosity 0
TEST(error_detail_policy_default, "4.1 default policy has verbosity 0")
{
	as_policy_read pr;
	as_policy_read_init(&pr);
	assert_int_eq(pr.base.error_detail_verbosity, 0);

	as_policy_write pw;
	as_policy_write_init(&pw);
	assert_int_eq(pw.base.error_detail_verbosity, 0);

	as_policy_remove prm;
	as_policy_remove_init(&prm);
	assert_int_eq(prm.base.error_detail_verbosity, 0);

	as_policy_operate po;
	as_policy_operate_init(&po);
	assert_int_eq(po.base.error_detail_verbosity, 0);

	as_policy_apply pa;
	as_policy_apply_init(&pa);
	assert_int_eq(pa.base.error_detail_verbosity, 0);

	as_policy_txn_verify ptv;
	as_policy_txn_verify_init(&ptv);
	assert_int_eq(ptv.base.error_detail_verbosity, 0);

	as_policy_txn_roll ptr;
	as_policy_txn_roll_init(&ptr);
	assert_int_eq(ptr.base.error_detail_verbosity, 0);
}

// 4.2 Policy copy preserves verbosity
TEST(error_detail_policy_copy, "4.2 policy copy preserves verbosity")
{
	as_policy_write src;
	as_policy_write_init(&src);
	src.base.error_detail_verbosity = AS_ERROR_DETAIL_MESSAGE;

	as_policy_write dst;
	as_policy_write_copy(&src, &dst);
	assert_int_eq(dst.base.error_detail_verbosity, 2);
}

// 4.3 Verbosity 0 writes zero bits in info4
TEST(error_detail_header_v0, "4.3 verbosity 0 clears info4 bits")
{
	as_policy_base policy;
	as_policy_base_write_init(&policy);
	policy.error_detail_verbosity = AS_ERROR_DETAIL_NONE;

	uint8_t cmd[30];
	memset(cmd, 0, sizeof(cmd));

	as_command_write_header_write(cmd, &policy, AS_POLICY_COMMIT_LEVEL_ALL,
		AS_POLICY_EXISTS_IGNORE, AS_POLICY_GEN_IGNORE, 0, 0, 1, 1, false, false, 0, 0, 0);

	assert_int_eq(cmd[12] & 0x60, 0x00);
}

// 4.4 Verbosity 1 sets correct info4 bits
TEST(error_detail_header_v1, "4.4 verbosity 1 sets info4 0x20")
{
	as_policy_base policy;
	as_policy_base_write_init(&policy);
	policy.error_detail_verbosity = AS_ERROR_DETAIL_SUBCODE;

	uint8_t cmd[30];
	memset(cmd, 0, sizeof(cmd));

	as_command_write_header_write(cmd, &policy, AS_POLICY_COMMIT_LEVEL_ALL,
		AS_POLICY_EXISTS_IGNORE, AS_POLICY_GEN_IGNORE, 0, 0, 1, 1, false, false, 0, 0, 0);

	assert_int_eq(cmd[12] & 0x60, 0x20);
}

// 4.5 Verbosity 2 sets correct info4 bits
TEST(error_detail_header_v2, "4.5 verbosity 2 sets info4 0x40")
{
	as_policy_base policy;
	as_policy_base_write_init(&policy);
	policy.error_detail_verbosity = AS_ERROR_DETAIL_MESSAGE;

	uint8_t cmd[30];
	memset(cmd, 0, sizeof(cmd));

	as_command_write_header_write(cmd, &policy, AS_POLICY_COMMIT_LEVEL_ALL,
		AS_POLICY_EXISTS_IGNORE, AS_POLICY_GEN_IGNORE, 0, 0, 1, 1, false, false, 0, 0, 0);

	assert_int_eq(cmd[12] & 0x60, 0x40);
}

// 4.6 Verbosity 3 sets correct info4 bits
TEST(error_detail_header_v3, "4.6 verbosity 3 sets info4 0x60")
{
	as_policy_base policy;
	as_policy_base_write_init(&policy);
	policy.error_detail_verbosity = AS_ERROR_DETAIL_EXP_TRACE;

	uint8_t cmd[30];
	memset(cmd, 0, sizeof(cmd));

	as_command_write_header_write(cmd, &policy, AS_POLICY_COMMIT_LEVEL_ALL,
		AS_POLICY_EXISTS_IGNORE, AS_POLICY_GEN_IGNORE, 0, 0, 1, 1, false, false, 0, 0, 0);

	assert_int_eq(cmd[12] & 0x60, 0x60);
}

// 4.7 Verbosity > 3 is clamped to the maximum public trace level
TEST(error_detail_header_overflow, "4.7 verbosity > 3 clamps to trace")
{
	as_policy_base policy;
	as_policy_base_write_init(&policy);

	uint8_t cmd[30];

	policy.error_detail_verbosity = 4;
	memset(cmd, 0, sizeof(cmd));
	as_command_write_header_write(cmd, &policy, AS_POLICY_COMMIT_LEVEL_ALL,
		AS_POLICY_EXISTS_IGNORE, AS_POLICY_GEN_IGNORE, 0, 0, 1, 1, false, false, 0, 0, 0);
	assert_int_eq(cmd[12] & 0x60, 0x60);

	policy.error_detail_verbosity = 5;
	memset(cmd, 0, sizeof(cmd));
	as_command_write_header_write(cmd, &policy, AS_POLICY_COMMIT_LEVEL_ALL,
		AS_POLICY_EXISTS_IGNORE, AS_POLICY_GEN_IGNORE, 0, 0, 1, 1, false, false, 0, 0, 0);
	assert_int_eq(cmd[12] & 0x60, 0x60);

	policy.error_detail_verbosity = 7;
	memset(cmd, 0, sizeof(cmd));
	as_command_write_header_write(cmd, &policy, AS_POLICY_COMMIT_LEVEL_ALL,
		AS_POLICY_EXISTS_IGNORE, AS_POLICY_GEN_IGNORE, 0, 0, 1, 1, false, false, 0, 0, 0);
	assert_int_eq(cmd[12] & 0x60, 0x60);

	policy.error_detail_verbosity = 255;
	memset(cmd, 0, sizeof(cmd));
	as_command_write_header_write(cmd, &policy, AS_POLICY_COMMIT_LEVEL_ALL,
		AS_POLICY_EXISTS_IGNORE, AS_POLICY_GEN_IGNORE, 0, 0, 1, 1, false, false, 0, 0, 0);
	assert_int_eq(cmd[12] & 0x60, 0x60);
}

// 4.8 Verbosity bits do not clobber existing info4 (txn flags)
TEST(error_detail_header_no_clobber, "4.8 verbosity does not clobber txn flags")
{
	as_policy_base policy;
	as_policy_base_write_init(&policy);
	policy.error_detail_verbosity = AS_ERROR_DETAIL_MESSAGE;

	uint8_t cmd[30];
	memset(cmd, 0, sizeof(cmd));

	// on_locking_only=true sets AS_MSG_INFO4_TXN_ON_LOCKING_ONLY (bit 4 = 0x10)
	as_command_write_header_write(cmd, &policy, AS_POLICY_COMMIT_LEVEL_ALL,
		AS_POLICY_EXISTS_IGNORE, AS_POLICY_GEN_IGNORE, 0, 0, 1, 1, false, true, 0, 0, 0);

	assert_int_eq(cmd[12] & 0x10, 0x10);
	assert_int_eq(cmd[12] & 0x60, 0x40);
}

// 4.12 Public OP_NOT_APPLICABLE subcodes match the server protocol values.
TEST(error_detail_opnot_string_subcodes, "4.12 OP_NOT string subcode constants")
{
	assert_int_eq(AS_SUB_OPNOT_STRING_CONVERSION_FAILED, 10);
	assert_int_eq(AS_SUB_OPNOT_STRING_UTF8_INVALID, 11);
	assert_int_eq(AS_SUB_OPNOT_STRING_B64_INVALID, 13);
}

// 4.13 Verbosity is applied in read header builder
TEST(error_detail_header_read, "4.13 verbosity in read header")
{
	as_policy_base policy;
	as_policy_base_read_init(&policy);
	policy.error_detail_verbosity = AS_ERROR_DETAIL_MESSAGE;

	uint8_t cmd[30];
	memset(cmd, 0, sizeof(cmd));

	as_command_write_header_read(cmd, &policy, AS_POLICY_READ_MODE_AP_ONE,
		AS_POLICY_READ_MODE_SC_SESSION, 0, 1000, 1, 1, 0, 0, 0);

	assert_int_eq(cmd[12] & 0x60, 0x40);
}

// 4.14 Verbosity is applied in write header builder
TEST(error_detail_header_write, "4.14 verbosity in write header")
{
	as_policy_base policy;
	as_policy_base_write_init(&policy);
	policy.error_detail_verbosity = AS_ERROR_DETAIL_SUBCODE;

	uint8_t cmd[30];
	memset(cmd, 0, sizeof(cmd));

	as_command_write_header_write(cmd, &policy, AS_POLICY_COMMIT_LEVEL_ALL,
		AS_POLICY_EXISTS_IGNORE, AS_POLICY_GEN_IGNORE, 0, 0, 1, 1, false, false, 0, 0, 0);

	assert_int_eq(cmd[12] & 0x60, 0x20);
}

// 4.15 Verbosity is applied in read-header (exists) builder
TEST(error_detail_header_read_header, "4.15 verbosity in exists header")
{
	as_policy_base policy;
	as_policy_base_read_init(&policy);
	policy.error_detail_verbosity = AS_ERROR_DETAIL_MESSAGE;

	uint8_t cmd[30];
	memset(cmd, 0, sizeof(cmd));

	as_command_write_header_read_header(cmd, &policy, AS_POLICY_READ_MODE_AP_ONE,
		AS_POLICY_READ_MODE_SC_SESSION, 0, 1, 0, AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_NOBINDATA);

	assert_int_eq(cmd[12] & 0x60, 0x40);
}

//-----------------------------------
// Section 7: Edge-case Tests (local)
//-----------------------------------

// 7.6 as_error_reset clears subcode
TEST(error_detail_reset_clears_subcode, "7.6 as_error_reset clears subcode")
{
	as_error err;
	as_error_init(&err);
	err.subcode = 5001;
	snprintf(err.message, sizeof(err.message), "some error (subcode=5001)");

	as_error_reset(&err);

	assert_int_eq(err.subcode, AS_SUB_NONE);
	assert_string_eq(err.message, "");
}

// 7.7 as_error_copy preserves subcode
TEST(error_detail_copy_preserves_subcode, "7.7 as_error_copy preserves subcode")
{
	as_error src;
	as_error_init(&src);
	src.subcode = 3042;
	snprintf(src.message, sizeof(src.message), "list bounds");

	as_error trg;
	as_error_init(&trg);

	as_error_copy(&trg, &src);

	assert_int_eq(trg.subcode, 3042);
	assert_string_eq(trg.message, "list bounds");
}

// 7.8 Successful response with spurious field 45 -- parser populates but
// the result_code==OK path should not surface it. We test that the parser
// itself populates the data (the priority logic in parse_result handles
// ignoring it on success).
TEST(error_detail_parser_populates_on_any_call, "7.8 parser populates regardless")
{
	as_error err;
	as_error_init(&err);

	const char* msg = "spurious";
	uint32_t msg_len = (uint32_t)strlen(msg);

	uint8_t buf[64];
	uint32_t p = 0;
	p += write_fixmap(buf + p, 2);
	p += write_fixint(buf + p, 1);
	p += write_uint16(buf + p, 999);
	p += write_fixint(buf + p, 2);
	p += write_fixstr(buf + p, msg, msg_len);

	command_parse_error_details(&err, buf, p);

	// The parser itself always writes subcode and message.
	// The calling code (parse_result) is responsible for not surfacing it on success.
	assert_int_eq(err.subcode, 999);
	assert_true(strstr(err.message, "spurious") != NULL);
}

//---------------------------------
// Test Suites
//---------------------------------

SUITE(error_detail_parser, "error detail msgpack parser tests")
{
	suite_add(error_detail_parser_empty_map);
	suite_add(error_detail_parser_subcode_only);
	suite_add(error_detail_parser_message_only);
	suite_add(error_detail_parser_subcode_and_message);
	suite_add(error_detail_parser_reversed_keys);
	suite_add(error_detail_parser_unknown_keys);
	suite_add(error_detail_parser_nested_unknown);
	suite_add(error_detail_parser_truncated);
	suite_add(error_detail_parser_zero_length);
	suite_add(error_detail_parser_uint8_subcode);
	suite_add(error_detail_parser_uint16_subcode);
	suite_add(error_detail_parser_uint32_subcode);
	suite_add(error_detail_parser_uint64_subcode);
	suite_add(error_detail_parser_str8_message);
	suite_add(error_detail_parser_str16_message);
	suite_add(error_detail_parser_unicode);
	suite_add(error_detail_parser_map16);
	suite_add(error_detail_parser_map32);
	suite_add(error_detail_parser_subcode_zero);
	suite_add(error_detail_parser_large_subcode);
	suite_add(error_detail_parser_near_max_len);
	suite_add(error_detail_parser_overflow_truncation);
	suite_add(error_detail_parser_exp_trace_full);
	suite_add(error_detail_parser_exp_trace_truncation);
	suite_add(error_detail_parser_exp_trace_skips_dropped_keys);
	suite_add(error_detail_parser_exp_trace_outcome_operands);
	suite_add(error_detail_parser_exp_trace_outcome_no_operands);
	suite_add(error_detail_parser_exp_trace_false_without_operands);
	suite_add(error_detail_parser_exp_trace_build_without_outcome);
	suite_add(error_detail_parser_exp_trace_max_path);
	suite_add(error_detail_parser_exp_trace_operand_clipping);
	suite_add(error_detail_parser_exp_trace_dropped_only_ignored);
	suite_add(error_detail_parser_exp_trace_path_truncation_marker);
	suite_add(error_detail_parser_exp_trace_malformed_operands);
	suite_add(error_detail_parser_unknown_signed_and_ext);
	suite_add(error_detail_parser_invalid_exp_trace);
	suite_add(error_detail_udf_message_preserves_server_detail);
	suite_add(error_detail_udf_fallback_preserves_server_detail);
	suite_add(error_detail_parser_exp_trace_only_status_fallback);
	suite_add(error_detail_parser_exp_trace_only_address_fallback);
	suite_add(error_detail_parser_exp_trace_omitted_lang_defaults_msgpack);
	suite_add(error_detail_parser_fields_err_extracts_detail);
	suite_add(error_detail_parser_batch_last_error_fields);
	suite_add(error_detail_parser_sync_txn_status_error_fields);
	suite_add(error_detail_parser_async_txn_status_error_fields);
}

SUITE(error_detail_policy, "error detail policy and header tests")
{
	suite_add(error_detail_policy_default);
	suite_add(error_detail_policy_copy);
	suite_add(error_detail_header_v0);
	suite_add(error_detail_header_v1);
	suite_add(error_detail_header_v2);
	suite_add(error_detail_header_v3);
	suite_add(error_detail_header_overflow);
	suite_add(error_detail_header_no_clobber);
	suite_add(error_detail_header_read);
	suite_add(error_detail_header_write);
	suite_add(error_detail_header_read_header);
	suite_add(error_detail_opnot_string_subcodes);
	suite_add(error_detail_reset_clears_subcode);
	suite_add(error_detail_copy_preserves_subcode);
	suite_add(error_detail_parser_populates_on_any_call);
}
