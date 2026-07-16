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
write_true(uint8_t* buf)
{
	buf[0] = 0xc3;
	return 1;
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

	as_command_parse_error_details(&err, buf, len);

	assert_string_eq(err.message, "");
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

	as_command_parse_error_details(&err, buf, p);

	assert_string_eq(err.message, "error subcode=42");
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

	as_command_parse_error_details(&err, buf, p);

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

	as_command_parse_error_details(&err, buf, p);

	assert_string_eq(err.message, "delete generation mismatch (subcode=5001)");
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

	as_command_parse_error_details(&err, buf, p);

	assert_string_eq(err.message, "type mismatch (subcode=1100)");
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
	p += write_fixmap(buf + p, 3);
	// key 1 -> subcode 99
	p += write_fixint(buf + p, 1);
	p += write_fixint(buf + p, 99);
	// key 99 -> fixstr (unknown, must be skipped)
	p += write_fixint(buf + p, 99);
	p += write_fixstr(buf + p, future_val, (uint32_t)strlen(future_val));
	// key 2 -> message
	p += write_fixint(buf + p, 2);
	p += write_fixstr(buf + p, msg, (uint32_t)strlen(msg));

	as_command_parse_error_details(&err, buf, p);

	assert_string_eq(err.message, "some message (subcode=99)");
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

	as_command_parse_error_details(&err, buf, p);

	assert_string_eq(err.message, "msg (subcode=200)");
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

	as_command_parse_error_details(&err, buf, 2);

	assert_string_eq(err.message, "");
	assert_int_eq(err.subcode, AS_SUB_NONE);
}

// 3.9 Zero-length buffer returns null
TEST(error_detail_parser_zero_length, "3.9 zero-length buffer")
{
	as_error err;
	as_error_init(&err);

	as_command_parse_error_details(&err, NULL, 0);

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

	as_command_parse_error_details(&err, buf, p);

	assert_int_eq(err.subcode, 200);
	assert_string_eq(err.message, "error subcode=200");
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

	as_command_parse_error_details(&err, buf, p);

	assert_int_eq(err.subcode, 3001);
	assert_string_eq(err.message, "error subcode=3001");
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

	as_command_parse_error_details(&err, buf, p);

	assert_int_eq(err.subcode, 70000);
	assert_string_eq(err.message, "error subcode=70000");
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

	as_command_parse_error_details(&err, buf, p);

	assert_int_eq(err.subcode, 100000);
	assert_string_eq(err.message, "error subcode=100000");
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

	as_command_parse_error_details(&err, buf, p);

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

	as_command_parse_error_details(&err, buf, p);

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

	as_command_parse_error_details(&err, buf, p);

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

	as_command_parse_error_details(&err, buf, p);

	assert_string_eq(err.message, "map16 test (subcode=500)");
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

	as_command_parse_error_details(&err, buf, p);

	assert_string_eq(err.message, "map32 test (subcode=600)");
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

	as_command_parse_error_details(&err, buf, p);

	assert_string_eq(err.message, "zero subcode (subcode=0)");
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

	as_command_parse_error_details(&err, buf, p);

	assert_int_eq(err.subcode, 99999);
	assert_string_eq(err.message, "cross-cutting (subcode=99999)");
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

	as_command_parse_error_details(&err, buf, p);

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

	as_command_parse_error_details(&err, buf, p);

	assert_int_eq(err.subcode, 1);
	assert_true(err.message[0] != '\0');
	assert_not_null(memchr(err.message, '\0', AS_ERROR_MESSAGE_MAX_SIZE));
	assert_true(strlen(err.message) <= 1023);
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

// 4.6 Verbosity 3 (reserved) sets correct info4 bits
TEST(error_detail_header_v3, "4.6 verbosity 3 sets info4 0x60")
{
	as_policy_base policy;
	as_policy_base_write_init(&policy);
	policy.error_detail_verbosity = 3;

	uint8_t cmd[30];
	memset(cmd, 0, sizeof(cmd));

	as_command_write_header_write(cmd, &policy, AS_POLICY_COMMIT_LEVEL_ALL,
		AS_POLICY_EXISTS_IGNORE, AS_POLICY_GEN_IGNORE, 0, 0, 1, 1, false, false, 0, 0, 0);

	assert_int_eq(cmd[12] & 0x60, 0x60);
}

// 4.7 Verbosity > 3 is naturally truncated by the 2-bit mask
TEST(error_detail_header_overflow, "4.7 verbosity > 3 mask truncation")
{
	as_policy_base policy;
	as_policy_base_write_init(&policy);

	uint8_t cmd[30];

	// verbosity 4 -> (4 << 5) & 0x60 = 0x80 & 0x60 = 0x00
	policy.error_detail_verbosity = 4;
	memset(cmd, 0, sizeof(cmd));
	as_command_write_header_write(cmd, &policy, AS_POLICY_COMMIT_LEVEL_ALL,
		AS_POLICY_EXISTS_IGNORE, AS_POLICY_GEN_IGNORE, 0, 0, 1, 1, false, false, 0, 0, 0);
	assert_int_eq(cmd[12] & 0x60, 0x00);

	// verbosity 5 -> (5 << 5) & 0x60 = 0xA0 & 0x60 = 0x20
	policy.error_detail_verbosity = 5;
	memset(cmd, 0, sizeof(cmd));
	as_command_write_header_write(cmd, &policy, AS_POLICY_COMMIT_LEVEL_ALL,
		AS_POLICY_EXISTS_IGNORE, AS_POLICY_GEN_IGNORE, 0, 0, 1, 1, false, false, 0, 0, 0);
	assert_int_eq(cmd[12] & 0x60, 0x20);

	// verbosity 7 -> (7 << 5) & 0x60 = 0xE0 & 0x60 = 0x60
	policy.error_detail_verbosity = 7;
	memset(cmd, 0, sizeof(cmd));
	as_command_write_header_write(cmd, &policy, AS_POLICY_COMMIT_LEVEL_ALL,
		AS_POLICY_EXISTS_IGNORE, AS_POLICY_GEN_IGNORE, 0, 0, 1, 1, false, false, 0, 0, 0);
	assert_int_eq(cmd[12] & 0x60, 0x60);

	// verbosity 255 -> (255 << 5) & 0x60 = 0xE0 & 0x60 = 0x60
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

// 4.9 Verbosity is applied in read header builder
TEST(error_detail_header_read, "4.9 verbosity in read header")
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

// 4.10 Verbosity is applied in write header builder
TEST(error_detail_header_write, "4.10 verbosity in write header")
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

// 4.11 Verbosity is applied in read-header (exists) builder
TEST(error_detail_header_read_header, "4.11 verbosity in exists header")
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

	as_command_parse_error_details(&err, buf, p);

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
	suite_add(error_detail_reset_clears_subcode);
	suite_add(error_detail_copy_preserves_subcode);
	suite_add(error_detail_parser_populates_on_any_call);
}
