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
#include <aerospike/as_config_yaml.h>
#include <aerospike/as_cluster.h>
#include <ctype.h>
#include <stdio.h>
#include <yaml.h>

//---------------------------------
// Types
//---------------------------------

typedef struct {
	yaml_parser_t parser;
	yaml_event_t event;
	as_config* config;
	bool init;
	as_error err;
} as_yaml;

typedef enum {
	AS_POLICY_TYPE_ALL,
	AS_POLICY_TYPE_READ,
	AS_POLICY_TYPE_WRITE,
	AS_POLICY_TYPE_REMOVE,
	AS_POLICY_TYPE_APPLY,
	AS_POLICY_TYPE_OPERATE,
	AS_POLICY_TYPE_BATCH_READ,
	AS_POLICY_TYPE_BATCH_WRITE,
	AS_POLICY_TYPE_TXN_VERIFY,
	AS_POLICY_TYPE_TXN_ROLL,
	AS_POLICY_TYPE_QUERY,
	AS_POLICY_TYPE_SCAN
} as_policy_type;

//---------------------------------
// Static Functions
//---------------------------------

static bool
as_skip_value(as_yaml* yaml)
{
	if (!yaml_parser_parse(&yaml->parser, &yaml->event)) {
		return false;
	}

	if (yaml->event.type == YAML_SCALAR_EVENT) {
		yaml_event_delete(&yaml->event);
		return true;
	}
	else if (yaml->event.type == YAML_SEQUENCE_START_EVENT) {
		yaml_event_delete(&yaml->event);

		while (as_skip_value(yaml)) {
		}
		return true;
	}
	else {
		yaml_event_delete(&yaml->event);
		return false;
	}
}

static void
as_skip_sequence(as_yaml* yaml)
{
	while (as_skip_value(yaml)) {
	}
}

static inline bool
as_parse_next(as_yaml* yaml)
{
	if (!yaml_parser_parse(&yaml->parser, &yaml->event)) {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "yaml_parser_parse failure at offset %zu", yaml->parser.offset);
		return false;
	}
	return true;
}

static inline void
as_expected_error(as_yaml* yaml, yaml_event_type_t type)
{
	as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Expected event %d. Received event: %d Offset: %zu",
		type, yaml->event.type, yaml->parser.offset);
}

static bool
as_expect_event(as_yaml* yaml, yaml_event_type_t type)
{
	if (!as_parse_next(yaml)) {
		return false;
	}

	if (yaml->event.type != type) {
		as_expected_error(yaml, type);
		yaml_event_delete(&yaml->event);
		return false;
	}

	yaml_event_delete(&yaml->event);
	return true;
}

static bool
as_prepare_scalar(as_yaml* yaml)
{
	if (!as_parse_next(yaml)) {
		return false;
	}

	if (yaml->event.type != YAML_SCALAR_EVENT) {
		as_expected_error(yaml, YAML_SCALAR_EVENT);
		yaml_event_delete(&yaml->event);
		return false;
	}

	// Caller is expected to delete event.
	return true;
}

static bool
as_parse_scalar(as_yaml* yaml, char* out, int size)
{
	if (!as_prepare_scalar(yaml)) {
		return false;
	}

	as_strncpy(out, (const char*)yaml->event.data.scalar.value, size);
	yaml_event_delete(&yaml->event);
	return true;
}

static bool
as_skip_mapping(as_yaml* yaml)
{
	if (!as_expect_event(yaml, YAML_MAPPING_START_EVENT)) {
		return false;
	}

	char name[256];

	while (as_parse_scalar(yaml, name, sizeof(name))) {
		if (!as_skip_value(yaml)) {
			return false;
		}
	}
	return true;
}

static bool
as_parse_int32(as_yaml* yaml, const char* name, const char* value, int32_t* out)
{
    char* end = NULL;
    errno = 0;
	long v = strtol(value, &end, 10);

	if (end == value || (errno != 0)) {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid int for %s: %s", name, value);
		return false;
	}

	*out = (int32_t)v;
	return true;
}

static bool
as_parse_uint32(as_yaml* yaml, const char* name, const char* value, uint32_t* out)
{
    char* end = NULL;
    errno = 0;
	unsigned long v = strtoul(value, &end, 10);

	if (end == value || (errno != 0)) {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid uint for %s: %s", name, value);
		return false;
	}

	*out = (uint32_t)v;
	return true;
}

static bool
as_parse_bool(as_yaml* yaml, const char* name, const char* value, bool* out)
{
	if (strcmp(value, "false") == 0) {
		*out = false;
	}
	else if (strcmp(value, "true") == 0) {
		*out = true;
	}
	else {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid bool for %s: %s", name, value);
		return false;
	}
	return true;
}

static bool
as_parse_vector_int32(as_yaml* yaml, const char* name, as_vector** out)
{
	*out = as_vector_create(sizeof(int), 8);

	int32_t v;

	while (as_prepare_scalar(yaml)) {
		const char* value = (const char*)yaml->event.data.scalar.value;
		bool rv = as_parse_int32(yaml, name, value, &v);

		yaml_event_delete(&yaml->event);

		if (rv) {
			as_vector_append(*out, &v);
		}
		else {
			return false;
		}
	}
	return true;
}

static bool
as_parse_policy_base(as_yaml* yaml, const char* name, const char* value, as_policy_type type)
{
	as_policies* p = &yaml->config->policies;

	if (strcmp(name, "read_mode_ap") == 0) {
		printf("read_mode_ap=%s\n", value);
		as_policy_read_mode_ap read_mode_ap;

		if (strcmp(value, "ONE") == 0) {
			read_mode_ap = AS_POLICY_READ_MODE_AP_ONE;
		}
		else if (strcmp(value, "ALL") == 0) {
			read_mode_ap = AS_POLICY_READ_MODE_AP_ALL;
		}
		else {
			as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid %s: %s", "read_mode_ap", value);
			return false;
		}

		switch (type) {
			case AS_POLICY_TYPE_ALL:
				p->read.read_mode_ap = read_mode_ap;
				p->operate.read_mode_ap = read_mode_ap;
				p->batch.read_mode_ap = read_mode_ap;
				p->txn_verify.read_mode_ap = read_mode_ap;
				break;
			case AS_POLICY_TYPE_READ:
				p->read.read_mode_ap = read_mode_ap;
				break;
			case AS_POLICY_TYPE_OPERATE:
				p->operate.read_mode_ap = read_mode_ap;
				break;
			case AS_POLICY_TYPE_BATCH_READ:
				p->batch.read_mode_ap = read_mode_ap;
				break;
			case AS_POLICY_TYPE_TXN_VERIFY:
				p->txn_verify.read_mode_ap = read_mode_ap;
				break;
			default:
				break;
		}
	}
	else if (strcmp(name, "read_mode_sc") == 0) {
		printf("read_mode_sc=%s\n", value);
		as_policy_read_mode_sc read_mode_sc;

		if (strcmp(value, "SESSION") == 0) {
			read_mode_sc = AS_POLICY_READ_MODE_SC_SESSION;
		}
		else if (strcmp(value, "LINEARIZE") == 0) {
			read_mode_sc = AS_POLICY_READ_MODE_SC_LINEARIZE;
		}
		else if (strcmp(value, "ALLOW_REPLICA") == 0) {
			read_mode_sc = AS_POLICY_READ_MODE_SC_ALLOW_REPLICA;
		}
		else if (strcmp(value, "ALLOW_UNAVAILABLE") == 0) {
			read_mode_sc = AS_POLICY_READ_MODE_SC_ALLOW_UNAVAILABLE;
		}
		else {
			as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid %s: %s", "read_mode_sc", value);
			return false;
		}

		switch (type) {
			case AS_POLICY_TYPE_ALL:
				p->read.read_mode_sc = read_mode_sc;
				p->operate.read_mode_sc = read_mode_sc;
				p->batch.read_mode_sc = read_mode_sc;
				p->txn_verify.read_mode_sc = read_mode_sc;
				break;
			case AS_POLICY_TYPE_READ:
				p->read.read_mode_sc = read_mode_sc;
				break;
			case AS_POLICY_TYPE_OPERATE:
				p->operate.read_mode_sc = read_mode_sc;
				break;
			case AS_POLICY_TYPE_BATCH_READ:
				p->batch.read_mode_sc = read_mode_sc;
				break;
			case AS_POLICY_TYPE_TXN_VERIFY:
				p->txn_verify.read_mode_sc = read_mode_sc;
				break;
			default:
				break;
		}
	}
	else if (strcmp(name, "replica") == 0) {
		printf("replica=%s\n", value);
		as_policy_replica replica;

		if (strcmp(value, "MASTER") == 0) {
			replica = AS_POLICY_REPLICA_MASTER;
		}
		else if (strcmp(value, "MASTER_PROLES") == 0) {
			replica = AS_POLICY_REPLICA_ANY;
		}
		else if (strcmp(value, "SEQUENCE") == 0) {
			replica = AS_POLICY_REPLICA_SEQUENCE;
		}
		else if (strcmp(value, "PREFER_RACK") == 0) {
			replica = AS_POLICY_REPLICA_PREFER_RACK;
		}
		else {
			as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid %s: %s", "replica", value);
			return false;
		}

		switch (type) {
			case AS_POLICY_TYPE_ALL:
				p->read.replica = replica;
				p->write.replica = replica;
				p->operate.replica = replica;
				p->remove.replica = replica;
				p->apply.replica = replica;
				p->batch.replica = replica;
				p->batch_parent_write.replica = replica;
				p->txn_verify.replica = replica;
				p->txn_roll.replica = replica;
				p->query.replica = replica;
				p->scan.replica = replica;
				break;
			case AS_POLICY_TYPE_READ:
				p->read.replica = replica;
				break;
			case AS_POLICY_TYPE_WRITE:
				p->write.replica = replica;
				break;
			case AS_POLICY_TYPE_OPERATE:
				p->operate.replica = replica;
				break;
			case AS_POLICY_TYPE_REMOVE:
				p->remove.replica = replica;
				break;
			case AS_POLICY_TYPE_APPLY:
				p->apply.replica = replica;
				break;
			case AS_POLICY_TYPE_BATCH_READ:
				p->batch.replica = replica;
				break;
			case AS_POLICY_TYPE_BATCH_WRITE:
				p->batch_parent_write.replica = replica;
				break;
			case AS_POLICY_TYPE_TXN_VERIFY:
				p->txn_verify.replica = replica;
				break;
			case AS_POLICY_TYPE_TXN_ROLL:
				p->txn_roll.replica = replica;
				break;
			case AS_POLICY_TYPE_QUERY:
				p->query.replica = replica;
				break;
			case AS_POLICY_TYPE_SCAN:
				p->scan.replica = replica;
				break;
			default:
				break;
		}
	}
	else if (strcmp(name, "send_key") == 0) {
		printf("send_key=%s\n", value);
		bool send_key;

		if (!as_parse_bool(yaml, name, value, &send_key)) {
			return false;
		}

		as_policy_key key = send_key ? AS_POLICY_KEY_SEND : AS_POLICY_KEY_DIGEST;

		switch (type) {
			case AS_POLICY_TYPE_ALL:
				p->read.key = key;
				p->write.key = key;
				p->operate.key = key;
				p->remove.key = key;
				p->apply.key = key;
				break;
			case AS_POLICY_TYPE_READ:
				p->read.key = key;
				break;
			case AS_POLICY_TYPE_WRITE:
				p->write.key = key;
				break;
			case AS_POLICY_TYPE_OPERATE:
				p->operate.key = key;
				break;
			case AS_POLICY_TYPE_REMOVE:
				p->remove.key = key;
				break;
			case AS_POLICY_TYPE_APPLY:
				p->apply.key = key;
				break;
			default:
				break;
		}
	}
	else if (strcmp(name, "sleep_between_retries") == 0) {
		printf("sleep_between_retries=%s\n", value);
		uint32_t sleep_between_retries;

		if (!as_parse_uint32(yaml, name, value, &sleep_between_retries)) {
			return false;
		}

		switch (type) {
			case AS_POLICY_TYPE_ALL:
				p->read.base.sleep_between_retries = sleep_between_retries;
				p->write.base.sleep_between_retries = sleep_between_retries;
				p->operate.base.sleep_between_retries = sleep_between_retries;
				p->remove.base.sleep_between_retries = sleep_between_retries;
				p->apply.base.sleep_between_retries = sleep_between_retries;
				p->batch.base.sleep_between_retries = sleep_between_retries;
				p->batch_parent_write.base.sleep_between_retries = sleep_between_retries;
				p->txn_verify.base.sleep_between_retries = sleep_between_retries;
				p->txn_roll.base.sleep_between_retries = sleep_between_retries;
				p->query.base.sleep_between_retries = sleep_between_retries;
				p->scan.base.sleep_between_retries = sleep_between_retries;
				break;
			case AS_POLICY_TYPE_READ:
				p->read.base.sleep_between_retries = sleep_between_retries;
				break;
			case AS_POLICY_TYPE_WRITE:
				p->write.base.sleep_between_retries = sleep_between_retries;
				break;
			case AS_POLICY_TYPE_OPERATE:
				p->operate.base.sleep_between_retries = sleep_between_retries;
				break;
			case AS_POLICY_TYPE_REMOVE:
				p->remove.base.sleep_between_retries = sleep_between_retries;
				break;
			case AS_POLICY_TYPE_APPLY:
				p->apply.base.sleep_between_retries = sleep_between_retries;
				break;
			case AS_POLICY_TYPE_BATCH_READ:
				p->batch.base.sleep_between_retries = sleep_between_retries;
				break;
			case AS_POLICY_TYPE_BATCH_WRITE:
				p->batch_parent_write.base.sleep_between_retries = sleep_between_retries;
				break;
			case AS_POLICY_TYPE_TXN_VERIFY:
				p->txn_verify.base.sleep_between_retries = sleep_between_retries;
				break;
			case AS_POLICY_TYPE_TXN_ROLL:
				p->txn_roll.base.sleep_between_retries = sleep_between_retries;
				break;
			case AS_POLICY_TYPE_QUERY:
				p->query.base.sleep_between_retries = sleep_between_retries;
				break;
			case AS_POLICY_TYPE_SCAN:
				p->scan.base.sleep_between_retries = sleep_between_retries;
				break;
			default:
				break;
		}
	}
	else if (strcmp(name, "socket_timeout") == 0) {
		printf("socket_timeout=%s\n", value);
		uint32_t socket_timeout;

		if (!as_parse_uint32(yaml, name, value, &socket_timeout)) {
			return false;
		}

		switch (type) {
			case AS_POLICY_TYPE_ALL:
				p->read.base.socket_timeout = socket_timeout;
				p->write.base.socket_timeout = socket_timeout;
				p->operate.base.socket_timeout = socket_timeout;
				p->remove.base.socket_timeout = socket_timeout;
				p->apply.base.socket_timeout = socket_timeout;
				p->batch.base.socket_timeout = socket_timeout;
				p->batch_parent_write.base.socket_timeout = socket_timeout;
				p->txn_verify.base.socket_timeout = socket_timeout;
				p->txn_roll.base.socket_timeout = socket_timeout;
				p->query.base.socket_timeout = socket_timeout;
				p->scan.base.socket_timeout = socket_timeout;
				break;
			case AS_POLICY_TYPE_READ:
				p->read.base.socket_timeout = socket_timeout;
				break;
			case AS_POLICY_TYPE_WRITE:
				p->write.base.socket_timeout = socket_timeout;
				break;
			case AS_POLICY_TYPE_OPERATE:
				p->operate.base.socket_timeout = socket_timeout;
				break;
			case AS_POLICY_TYPE_REMOVE:
				p->remove.base.socket_timeout = socket_timeout;
				break;
			case AS_POLICY_TYPE_APPLY:
				p->apply.base.socket_timeout = socket_timeout;
				break;
			case AS_POLICY_TYPE_BATCH_READ:
				p->batch.base.socket_timeout = socket_timeout;
				break;
			case AS_POLICY_TYPE_BATCH_WRITE:
				p->batch_parent_write.base.socket_timeout = socket_timeout;
				break;
			case AS_POLICY_TYPE_TXN_VERIFY:
				p->txn_verify.base.socket_timeout = socket_timeout;
				break;
			case AS_POLICY_TYPE_TXN_ROLL:
				p->txn_roll.base.socket_timeout = socket_timeout;
				break;
			case AS_POLICY_TYPE_QUERY:
				p->query.base.socket_timeout = socket_timeout;
				break;
			case AS_POLICY_TYPE_SCAN:
				p->scan.base.socket_timeout = socket_timeout;
				break;
			default:
				break;
		}
	}
	else if (strcmp(name, "total_timeout") == 0) {
		printf("total_timeout=%s\n", value);
		uint32_t total_timeout;

		if (!as_parse_uint32(yaml, name, value, &total_timeout)) {
			return false;
		}

		switch (type) {
			case AS_POLICY_TYPE_ALL:
				p->read.base.total_timeout = total_timeout;
				p->write.base.total_timeout = total_timeout;
				p->operate.base.total_timeout = total_timeout;
				p->remove.base.total_timeout = total_timeout;
				p->apply.base.total_timeout = total_timeout;
				p->batch.base.total_timeout = total_timeout;
				p->batch_parent_write.base.total_timeout = total_timeout;
				p->txn_verify.base.total_timeout = total_timeout;
				p->txn_roll.base.total_timeout = total_timeout;
				p->query.base.total_timeout = total_timeout;
				p->scan.base.total_timeout = total_timeout;
				break;
			case AS_POLICY_TYPE_READ:
				p->read.base.total_timeout = total_timeout;
				break;
			case AS_POLICY_TYPE_WRITE:
				p->write.base.total_timeout = total_timeout;
				break;
			case AS_POLICY_TYPE_OPERATE:
				p->operate.base.total_timeout = total_timeout;
				break;
			case AS_POLICY_TYPE_REMOVE:
				p->remove.base.total_timeout = total_timeout;
				break;
			case AS_POLICY_TYPE_APPLY:
				p->apply.base.total_timeout = total_timeout;
				break;
			case AS_POLICY_TYPE_BATCH_READ:
				p->batch.base.total_timeout = total_timeout;
				break;
			case AS_POLICY_TYPE_BATCH_WRITE:
				p->batch_parent_write.base.total_timeout = total_timeout;
				break;
			case AS_POLICY_TYPE_TXN_VERIFY:
				p->txn_verify.base.total_timeout = total_timeout;
				break;
			case AS_POLICY_TYPE_TXN_ROLL:
				p->txn_roll.base.total_timeout = total_timeout;
				break;
			case AS_POLICY_TYPE_QUERY:
				p->query.base.total_timeout = total_timeout;
				break;
			case AS_POLICY_TYPE_SCAN:
				p->scan.base.total_timeout = total_timeout;
				break;
			default:
				break;
		}
	}
	else if (strcmp(name, "max_retries") == 0) {
		printf("max_retries=%s\n", value);
		uint32_t max_retries;

		if (!as_parse_uint32(yaml, name, value, &max_retries)) {
			return false;
		}

		switch (type) {
			case AS_POLICY_TYPE_ALL:
				p->read.base.max_retries = max_retries;
				p->write.base.max_retries = max_retries;
				p->operate.base.max_retries = max_retries;
				p->remove.base.max_retries = max_retries;
				p->apply.base.max_retries = max_retries;
				p->batch.base.max_retries = max_retries;
				p->batch_parent_write.base.max_retries = max_retries;
				p->txn_verify.base.max_retries = max_retries;
				p->txn_roll.base.max_retries = max_retries;
				p->query.base.max_retries = max_retries;
				p->scan.base.max_retries = max_retries;
				break;
			case AS_POLICY_TYPE_READ:
				p->read.base.max_retries = max_retries;
				break;
			case AS_POLICY_TYPE_WRITE:
				p->write.base.max_retries = max_retries;
				break;
			case AS_POLICY_TYPE_OPERATE:
				p->operate.base.max_retries = max_retries;
				break;
			case AS_POLICY_TYPE_REMOVE:
				p->remove.base.max_retries = max_retries;
				break;
			case AS_POLICY_TYPE_APPLY:
				p->apply.base.max_retries = max_retries;
				break;
			case AS_POLICY_TYPE_BATCH_READ:
				p->batch.base.max_retries = max_retries;
				break;
			case AS_POLICY_TYPE_BATCH_WRITE:
				p->batch_parent_write.base.max_retries = max_retries;
				break;
			case AS_POLICY_TYPE_TXN_VERIFY:
				p->txn_verify.base.max_retries = max_retries;
				break;
			case AS_POLICY_TYPE_TXN_ROLL:
				p->txn_roll.base.max_retries = max_retries;
				break;
			case AS_POLICY_TYPE_QUERY:
				p->query.base.max_retries = max_retries;
				break;
			case AS_POLICY_TYPE_SCAN:
				p->scan.base.max_retries = max_retries;
				break;
			default:
				break;
		}
	}
	/* Not supported in C client.
	else if (strcmp(name, "connect_timeout") == 0) {
		// connect_timeout not supported in the C client.
	}
	else if (strcmp(name, "timeout_delay") == 0) {
		// timeout_delay not supported in the C client.
	}
	else if (strcmp(name, "fail_on_filtered_out") == 0) {
		// fail_on_filtered_out not supported in the C client.
	}
	*/
	return true;
}

static bool
as_parse_write_scalar(as_yaml* yaml, const char* name, const char* value)
{
	as_policies* p = &yaml->config->policies;

	if (strcmp(name, "durable_delete") == 0) {
		printf("durable_delete=%s\n", value);
		bool durable_delete;

		if (!as_parse_bool(yaml, name, value, &durable_delete)) {
			return false;
		}

		p->write.durable_delete = durable_delete;
	}
	else {
		return as_parse_policy_base(yaml, name, value, AS_POLICY_TYPE_WRITE);
	}
	return true;
}

static bool
as_parse_write(as_yaml* yaml)
{
	char name[256];

	while (as_parse_scalar(yaml, name, sizeof(name))) {
		if (!as_parse_next(yaml)) {
			return false;
		}

		if (yaml->event.type == YAML_SCALAR_EVENT) {
			char* value = (char*)yaml->event.data.scalar.value;
			bool rv = as_parse_write_scalar(yaml, name, value);

			yaml_event_delete(&yaml->event);

			if (!rv) {
				return false;
			}
		}
		else {
			as_expected_error(yaml, YAML_SCALAR_EVENT);
			yaml_event_delete(&yaml->event);
			return false;
		}
	}
	return true;
}

static bool
as_parse_batch_scalar(as_yaml* yaml, const char* name, const char* value, as_policy_type type)
{
	as_policies* p = &yaml->config->policies;

	if (strcmp(name, "max_concurrent_threads") == 0) {
		// C client only supports all concurrent threads or not.
		printf("max_concurrent_threads=%s\n", value);
		uint32_t max_concurrent_threads;

		if (!as_parse_uint32(yaml, name, value, &max_concurrent_threads)) {
			return false;
		}

		bool concurrent = max_concurrent_threads != 1;

		switch (type) {
			case AS_POLICY_TYPE_BATCH_READ:
				p->batch.concurrent = concurrent;
				break;
			case AS_POLICY_TYPE_BATCH_WRITE:
				p->batch_parent_write.concurrent = concurrent;
				break;
			case AS_POLICY_TYPE_TXN_VERIFY:
				p->txn_verify.concurrent = concurrent;
				break;
			case AS_POLICY_TYPE_TXN_ROLL:
				p->txn_roll.concurrent = concurrent;
				break;
			default:
				break;
		}
	}
	else if (strcmp(name, "allow_inline") == 0) {
		printf("allow_inline=%s\n", value);
		bool allow_inline;

		if (!as_parse_bool(yaml, name, value, &allow_inline)) {
			return false;
		}

		switch (type) {
			case AS_POLICY_TYPE_BATCH_READ:
				p->batch.allow_inline = allow_inline;
				break;
			case AS_POLICY_TYPE_BATCH_WRITE:
				p->batch_parent_write.allow_inline = allow_inline;
				break;
			case AS_POLICY_TYPE_TXN_VERIFY:
				p->txn_verify.allow_inline = allow_inline;
				break;
			case AS_POLICY_TYPE_TXN_ROLL:
				p->txn_roll.allow_inline = allow_inline;
				break;
			default:
				break;
		}
	}
	else if (strcmp(name, "allow_inline_ssd") == 0) {
		printf("allow_inline_ssd=%s\n", value);
		bool allow_inline_ssd;

		if (!as_parse_bool(yaml, name, value, &allow_inline_ssd)) {
			return false;
		}

		switch (type) {
			case AS_POLICY_TYPE_BATCH_READ:
				p->batch.allow_inline_ssd = allow_inline_ssd;
				break;
			case AS_POLICY_TYPE_BATCH_WRITE:
				p->batch_parent_write.allow_inline_ssd = allow_inline_ssd;
				break;
			case AS_POLICY_TYPE_TXN_VERIFY:
				p->txn_verify.allow_inline_ssd = allow_inline_ssd;
				break;
			case AS_POLICY_TYPE_TXN_ROLL:
				p->txn_roll.allow_inline_ssd = allow_inline_ssd;
				break;
			default:
				break;
		}
	}
	else if (strcmp(name, "respond_all_keys") == 0) {
		printf("respond_all_keys=%s\n", value);
		bool respond_all_keys;

		if (!as_parse_bool(yaml, name, value, &respond_all_keys)) {
			return false;
		}

		switch (type) {
			case AS_POLICY_TYPE_BATCH_READ:
				p->batch.respond_all_keys = respond_all_keys;
				break;
			case AS_POLICY_TYPE_BATCH_WRITE:
				p->batch_parent_write.respond_all_keys = respond_all_keys;
				break;
			case AS_POLICY_TYPE_TXN_VERIFY:
				p->txn_verify.respond_all_keys = respond_all_keys;
				break;
			case AS_POLICY_TYPE_TXN_ROLL:
				p->txn_roll.respond_all_keys = respond_all_keys;
				break;
			default:
				break;
		}
	}
	else {
		return as_parse_policy_base(yaml, name, value, type);
	}
	return true;
}

static bool
as_parse_batch(as_yaml* yaml, as_policy_type type)
{
	char name[256];

	while (as_parse_scalar(yaml, name, sizeof(name))) {
		if (!as_parse_next(yaml)) {
			return false;
		}

		if (yaml->event.type == YAML_SCALAR_EVENT) {
			char* value = (char*)yaml->event.data.scalar.value;
			bool rv = as_parse_batch_scalar(yaml, name, value, type);

			yaml_event_delete(&yaml->event);

			if (!rv) {
				return false;
			}
		}
		else {
			as_expected_error(yaml, YAML_SCALAR_EVENT);
			yaml_event_delete(&yaml->event);
			return false;
		}
	}
	return true;
}

static bool
as_parse_query_scalar(as_yaml* yaml, const char* name, const char* value)
{
	as_policies* p = &yaml->config->policies;

	if (strcmp(name, "info_timeout") == 0) {
		printf("info_timeout=%s\n", value);
		uint32_t info_timeout;

		if (!as_parse_uint32(yaml, name, value, &info_timeout)) {
			return false;
		}

		p->query.info_timeout = info_timeout;
	}
	else if (strcmp(name, "expected_duration") == 0) {
		printf("expected_duration=%s\n", value);
		as_query_duration expected_duration;

		if (strcmp(value, "LONG") == 0) {
			expected_duration = AS_QUERY_DURATION_LONG;
		}
		else if (strcmp(value, "SHORT") == 0) {
			expected_duration = AS_QUERY_DURATION_SHORT;
		}
		else if (strcmp(value, "LONG_RELAX_AP") == 0) {
			expected_duration = AS_QUERY_DURATION_LONG_RELAX_AP;
		}
		else {
			as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid %s: %s", name, value);
			return false;
		}

		p->query.expected_duration = expected_duration;
	}
	else if (strcmp(name, "include_bin_data") == 0) {
		// Not supported in C client.
	}
	else if (strcmp(name, "record_queue_size") == 0) {
		// Not supported in C client.
	}
	else {
		return as_parse_policy_base(yaml, name, value, AS_POLICY_TYPE_SCAN);
	}
	return true;
}

static bool
as_parse_query(as_yaml* yaml)
{
	char name[256];

	while (as_parse_scalar(yaml, name, sizeof(name))) {
		if (!as_parse_next(yaml)) {
			return false;
		}

		if (yaml->event.type == YAML_SCALAR_EVENT) {
			char* value = (char*)yaml->event.data.scalar.value;
			bool rv = as_parse_query_scalar(yaml, name, value);

			yaml_event_delete(&yaml->event);

			if (!rv) {
				return false;
			}
		}
		else {
			as_expected_error(yaml, YAML_SCALAR_EVENT);
			yaml_event_delete(&yaml->event);
			return false;
		}
	}
	return true;
}

static bool
as_parse_scan_scalar(as_yaml* yaml, const char* name, const char* value)
{
	if (strcmp(name, "concurrent_nodes") == 0) {
		// Not supported in C client.
	}
	else if (strcmp(name, "max_concurrent_nodes") == 0) {
		// Not supported in C client.
	}
	else {
		return as_parse_policy_base(yaml, name, value, AS_POLICY_TYPE_SCAN);
	}
	return true;
}

static bool
as_parse_scan(as_yaml* yaml)
{
	char name[256];

	while (as_parse_scalar(yaml, name, sizeof(name))) {
		if (!as_parse_next(yaml)) {
			return false;
		}

		if (yaml->event.type == YAML_SCALAR_EVENT) {
			char* value = (char*)yaml->event.data.scalar.value;
			bool rv = as_parse_scan_scalar(yaml, name, value);

			yaml_event_delete(&yaml->event);

			if (!rv) {
				return false;
			}
		}
		else {
			as_expected_error(yaml, YAML_SCALAR_EVENT);
			yaml_event_delete(&yaml->event);
			return false;
		}
	}
	return true;
}

static bool
as_parse_policy(as_yaml* yaml)
{
	if (!as_expect_event(yaml, YAML_MAPPING_START_EVENT)) {
		return false;
	}

	char name[256];

	while (as_parse_scalar(yaml, name, sizeof(name))) {
		if (!as_parse_next(yaml)) {
			return false;
		}

		bool rv;

		if (yaml->event.type == YAML_SCALAR_EVENT) {
			char* value = (char*)yaml->event.data.scalar.value;

			rv = as_parse_policy_base(yaml, name, value, AS_POLICY_TYPE_ALL);
		}
		else if (yaml->event.type == YAML_MAPPING_START_EVENT) {
			yaml_event_delete(&yaml->event);

			if (strcmp(name, "write") == 0) {
				printf("write\n");
				rv = as_parse_write(yaml);
			}
			else if (strcmp(name, "batch") == 0) {
				printf("batch\n");
				rv = as_parse_batch(yaml, AS_POLICY_TYPE_BATCH_READ);
			}
			else if (strcmp(name, "batch_write") == 0) {
				printf("batch_write\n");
				rv = as_parse_batch(yaml, AS_POLICY_TYPE_BATCH_WRITE);
			}
			else if (strcmp(name, "txn_verify") == 0) {
				printf("txn_verify\n");
				rv = as_parse_batch(yaml, AS_POLICY_TYPE_TXN_VERIFY);
			}
			else if (strcmp(name, "txn_roll") == 0) {
				printf("txn_roll\n");
				rv = as_parse_batch(yaml, AS_POLICY_TYPE_TXN_ROLL);
			}
			else if (strcmp(name, "query") == 0) {
				printf("query\n");
				rv = as_parse_query(yaml);
			}
			else if (strcmp(name, "scan") == 0) {
				printf("scan\n");
				rv = as_parse_scan(yaml);
			}
			else {
				as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Unknown label: %s", name);
				rv = false;
			}
		}
		else {
			as_expected_error(yaml, YAML_SCALAR_EVENT);
			rv = false;
		}

		yaml_event_delete(&yaml->event);

		if (!rv) {
			return rv;
		}
	}
	return true;
}

static bool
as_parse_static_client(as_yaml* yaml)
{
	if (!as_expect_event(yaml, YAML_MAPPING_START_EVENT)) {
		return false;
	}

	char name[256];

	while (as_parse_scalar(yaml, name, sizeof(name))) {
		if (!as_parse_next(yaml)) {
			return false;
		}

		if (yaml->event.type == YAML_SCALAR_EVENT) {
			const char* value = (const char*)yaml->event.data.scalar.value;
			bool rv;

			if (strcmp(name, "min_connections_per_node") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->min_conns_per_node);
				printf("min_connections_per_node=%u\n", yaml->config->min_conns_per_node);
			}
			else if (strcmp(name, "max_connections_per_node") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->max_conns_per_node);
				printf("max_connections_per_node=%u\n", yaml->config->max_conns_per_node);
			}
			else if (strcmp(name, "async_min_connections_per_node") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->async_min_conns_per_node);
				printf("async_min_connections_per_node=%u\n", yaml->config->async_min_conns_per_node);
			}
			else if (strcmp(name, "async_max_connections_per_node") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->async_max_conns_per_node);
				printf("async_max_connections_per_node=%u\n", yaml->config->async_max_conns_per_node);
			}
			else if (strcmp(name, "timeout") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->conn_timeout_ms);
				printf("timeout=%u\n", yaml->config->conn_timeout_ms);
			}
			else {
				rv = true; // Skip unknown scalars.
			}

			yaml_event_delete(&yaml->event);

			if (!rv) {
				return rv;
			}
		}
		else {
			as_expected_error(yaml, YAML_SCALAR_EVENT);
			yaml_event_delete(&yaml->event);
			return false;
		}
	}
	return true;
}

static bool
as_parse_dynamic_client(as_yaml* yaml)
{
	if (!as_expect_event(yaml, YAML_MAPPING_START_EVENT)) {
		return false;
	}

	char name[256];

	while (as_parse_scalar(yaml, name, sizeof(name))) {
		if (!as_parse_next(yaml)) {
			return false;
		}

		if (yaml->event.type == YAML_SCALAR_EVENT) {
			const char* value = (const char*)yaml->event.data.scalar.value;
			bool rv;

			if (strcmp(name, "error_rate_window") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->error_rate_window);
				printf("error_rate_window=%u\n", yaml->config->error_rate_window);
			}
			else if (strcmp(name, "max_error_rate") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->max_error_rate);
				printf("max_error_rate=%u\n", yaml->config->max_error_rate);
			}
			else if (strcmp(name, "login_timeout") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->login_timeout_ms);
				printf("login_timeout=%u\n", yaml->config->login_timeout_ms);
			}
			else if (strcmp(name, "max_socket_idle") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->max_socket_idle);
				printf("max_socket_idle=%u\n", yaml->config->max_socket_idle);
			}
			else if (strcmp(name, "tend_interval") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->tender_interval);
				printf("tend_interval=%u\n", yaml->config->tender_interval);
			}
			else if (strcmp(name, "fail_if_not_connected") == 0) {
				rv = as_parse_bool(yaml, name, value, &yaml->config->fail_if_not_connected);
				printf("fail_if_not_connected=%d\n", yaml->config->fail_if_not_connected);
			}
			else if (strcmp(name, "use_service_alternative") == 0) {
				rv = as_parse_bool(yaml, name, value, &yaml->config->use_services_alternate);
				printf("use_service_alternative=%d\n", yaml->config->use_services_alternate);
			}
			else if (strcmp(name, "rack_aware") == 0) {
				rv = as_parse_bool(yaml, name, value, &yaml->config->rack_aware);
				printf("rack_aware=%d\n", yaml->config->rack_aware);
			}
			else {
				rv = true; // Skip unknown scalars.
			}

			yaml_event_delete(&yaml->event);

			if (!rv) {
				return rv;
			}
		}
		else if (yaml->event.type == YAML_SEQUENCE_START_EVENT) {
			yaml_event_delete(&yaml->event);

			bool rv;

			if (strcmp(name, "rack_ids") == 0) {
				rv = as_parse_vector_int32(yaml, name, &yaml->config->rack_ids);

				for (uint32_t i = 0; i < yaml->config->rack_ids->size; i++) {
					int32_t* v = as_vector_get(yaml->config->rack_ids, i);
					printf("rack_id=%d\n", *v);
				}
			}
			else {
				as_skip_sequence(yaml);
				rv = true;
			}

			if (!rv) {
				return rv;
			}
		}
		else {
			as_expected_error(yaml, YAML_SCALAR_EVENT);
			yaml_event_delete(&yaml->event);
			return false;
		}
	}
	return true;
}

static bool
as_parse_static(as_yaml* yaml)
{
	if (!as_expect_event(yaml, YAML_MAPPING_START_EVENT)) {
		return false;
	}

	char name[256];

	while (as_parse_scalar(yaml, name, sizeof(name))) {
		if (yaml->init && strcmp(name, "client") == 0) {
			printf("client\n");
			as_parse_static_client(yaml);
		}
		else {
			printf("Skip %s\n", name);
			as_skip_mapping(yaml);
		}
	}
	return true;
}

static bool
as_parse_dynamic(as_yaml* yaml)
{
	if (!as_expect_event(yaml, YAML_MAPPING_START_EVENT)) {
		return false;
	}

	char name[256];

	while (as_parse_scalar(yaml, name, sizeof(name))) {
		if (strcmp(name, "client") == 0) {
			printf("client\n");
			as_parse_dynamic_client(yaml);
		}
		else if (strcmp(name, "policy") == 0) {
			printf("policy\n");
			as_parse_policy(yaml);
		}
		else {
			printf("Skip %s\n", name);
			as_skip_mapping(yaml);
		}
	}
	return true;
}

static bool
as_parse_yaml(as_yaml* yaml)
{
	if (!as_expect_event(yaml, YAML_STREAM_START_EVENT)) {
		return false;
	}

	if (!as_expect_event(yaml, YAML_DOCUMENT_START_EVENT)) {
		return false;
	}

	if (!as_expect_event(yaml, YAML_MAPPING_START_EVENT)) {
		return false;
	}

	char name[256];

	while (as_parse_scalar(yaml, name, sizeof(name))) {
		if (strcmp(name, "static") == 0) {
			printf("static\n");
			as_parse_static(yaml);
		}
		else if (strcmp(name, "dynamic") == 0) {
			printf("dynamic\n");
			as_parse_dynamic(yaml);
		}
		else {
			printf("Skip %s\n", name);
			as_skip_mapping(yaml);
		}
	}

	return true;
}

#if 0
// Used for debugging purposes.
static bool
parse_debug(yaml_parser_t* parser)
{
	char name[256];
	yaml_event_t event;
	bool valid = true;

	while (valid) {
		if (!yaml_parser_parse(parser, &event)) {
			return false;
		}

		switch(event.type)
		{
		case YAML_NO_EVENT:
			puts("YAML_NO_EVENT");
			break;

		case YAML_STREAM_START_EVENT:
			puts("YAML_STREAM_START_EVENT");
			break;

		case YAML_STREAM_END_EVENT:
			puts("YAML_STREAM_END_EVENT");
			valid = false;
			break;

		case YAML_DOCUMENT_START_EVENT:
			puts("YAML_DOCUMENT_START_EVENT");
			break;

		case YAML_DOCUMENT_END_EVENT:
			puts("YAML_DOCUMENT_END_EVENT");
			break;

		case YAML_SEQUENCE_START_EVENT:
			puts("YAML_SEQUENCE_START_EVENT");
			break;

		case YAML_SEQUENCE_END_EVENT:
			puts("YAML_SEQUENCE_END_EVENT");
			break;

		case YAML_MAPPING_START_EVENT:
			puts("YAML_MAPPING_START_EVENT");
			break;

		case YAML_MAPPING_END_EVENT:
			puts("YAML_MAPPING_END_EVENT");
			break;

		case YAML_ALIAS_EVENT:
			printf("YAML_ALIAS_EVENT (anchor %s)\n", event.data.alias.anchor);
			break;

		case YAML_SCALAR_EVENT:
			printf("YAML_SCALAR_EVENT (value %s)\n", event.data.scalar.value);
			break;
		}

		yaml_event_delete(&event);
	}
	return true;
}
#endif

static as_status
as_config_yaml_read(const char* path, as_config* config, bool init, as_error* err)
{
	as_error_reset(err);

	FILE* fp = fopen(path, "r");

	if (!fp) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to open: %s", path);
	}

	as_yaml yaml;
	yaml.config = config;
	yaml.init = init;

	if (!yaml_parser_initialize(&yaml.parser)) {
		fclose(fp);
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to initialize yaml parser");
	}

	yaml_parser_set_input_file(&yaml.parser, fp);

	bool rv = as_parse_yaml(&yaml);

	yaml_parser_delete(&yaml.parser);
	fclose(fp);

	if (!rv) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to parse: %s\n%s",
			path, yaml.err.message);
	}
	return AEROSPIKE_OK;
}

static bool
as_rack_ids_equal(as_vector* r1, as_vector* r2)
{
	if (r1->size != r2->size) {
		return false;
	}

	for (uint32_t i = 0; i < r1->size; i++) {
		int id1 = *(int*)as_vector_get(r1, i);
		int id2 = *(int*)as_vector_get(r2, i);

		if (id1 != id2) {
			return false;
		}
	}
	return true;
}

static void
as_release_rack_ids(as_vector* rack_ids)
{
	as_vector_destroy(rack_ids);
}

//---------------------------------
// Functions
//---------------------------------

as_status
as_config_yaml_init(as_config* config, as_error* err)
{
	return as_config_yaml_read(config->config_provider.yaml_path, config, true, err);
}

void
as_config_destroy(as_config* config);

void
as_cluster_set_max_socket_idle(as_cluster* cluster, uint32_t max_socket_idle_sec);

as_status
as_config_yaml_update(aerospike* as, as_error* err)
{
	as_config* config = cf_malloc(sizeof(as_config));
	memcpy(config, &as->config, sizeof(as_config));
	config->rack_ids = NULL;

	as_status status = as_config_yaml_read(config->config_provider.yaml_path, config, false, err);

	if (status != AEROSPIKE_OK) {
		as_config_destroy(config);
		cf_free(config);
		return status;
	}

	// Apply config to cluster.
	as_cluster* cluster = as->cluster;

	cluster->max_error_rate = config->max_error_rate;
	cluster->error_rate_window = config->error_rate_window;
	cluster->login_timeout_ms = config->login_timeout_ms;
	cluster->tend_interval = (config->tender_interval < 250)? 250 : config->tender_interval;
	cluster->use_services_alternate = config->use_services_alternate;
	cluster->fail_if_not_connected = config->fail_if_not_connected;
	as_cluster_set_max_socket_idle(cluster, config->max_socket_idle);

	cluster->rack_aware = config->rack_aware;

	if (config->rack_ids && !as_rack_ids_equal(config->rack_ids, cluster->rack_ids)) {
		as_vector* old = cluster->rack_ids;

		as_store_ptr_rls((void**)&cluster->rack_ids, config->rack_ids);
		config->rack_ids = NULL;

		as_gc_item item;
		item.data = old;
		item.release_fn = (as_release_fn)as_release_rack_ids;
		as_vector_append(cluster->gc, &item);
	}

	as_config_destroy(config);
	cf_free(config);
	return AEROSPIKE_OK;
}
