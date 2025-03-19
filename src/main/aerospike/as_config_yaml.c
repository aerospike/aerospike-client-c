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
#include <aerospike/as_log_macros.h>
#include <aerospike/as_string_builder.h>
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
	const char* name;
	as_error err;
	bool init;
} as_yaml;

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
parse_uint32(as_yaml* yaml, const char* name, const char* value, uint32_t* out)
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
as_parse_uint32(as_yaml* yaml, const char* name, const char* value, uint32_t* out)
{
	uint32_t val;

	if (!parse_uint32(yaml, name, value, &val)) {
		return false;
	}

	if (*out != val) {
		as_log_info("Set %s.%s = %s", yaml->name, name, value);
		*out = val;
	}
	return true;
}

static bool
parse_bool(as_yaml* yaml, const char* name, const char* value, bool* out)
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
as_parse_bool(as_yaml* yaml, const char* name, const char* value, bool* out)
{
	bool val;

	if (!parse_bool(yaml, name, value, &val)) {
		return false;
	}

	if (*out != val) {
		as_log_info("Set %s.%s = %s", yaml->name, name, value);
		*out = val;
	}
	return true;
}

static bool
as_vector_int32_equal(as_vector* r1, as_vector* r2)
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

static bool
as_parse_vector_int32(as_yaml* yaml, const char* name, as_vector** out)
{
	as_vector* list = as_vector_create(sizeof(int), 8);
	int32_t v;

	while (as_prepare_scalar(yaml)) {
		const char* value = (const char*)yaml->event.data.scalar.value;
		bool rv = as_parse_int32(yaml, name, value, &v);

		yaml_event_delete(&yaml->event);

		if (rv) {
			as_vector_append(list, &v);
		}
		else {
			as_vector_destroy(list);
			return false;
		}
	}

	if (!as_vector_int32_equal(*out, list)) {
		as_string_builder sb;
		as_string_builder_inita(&sb, 512, false);

		as_string_builder_append(&sb, "Set ");
		as_string_builder_append(&sb, yaml->name);
		as_string_builder_append_char(&sb, '.');
		as_string_builder_append(&sb, name);
		as_string_builder_append(&sb, " = [");

		for (uint32_t i = 0; i < list->size; i++) {
			int id = *(int*)as_vector_get(list, i);

			if (i > 0) {
				as_string_builder_append_char(&sb, ',');
			}
			as_string_builder_append_int(&sb, id);
		}

		as_string_builder_append_char(&sb, ']');
		as_log_info(sb.data);

		// On init, the original config is set directly, so the old vector must be destroyed.
		// On update, a shallow copy config is set and the original vector will be destroyed
		// after the update succeeds.
		if (yaml->init) {
			as_vector_destroy(*out);
		}
		*out = list;
	}
	else {
		as_vector_destroy(list);
	}
	return true;
}

static bool
as_parse_read_mode_ap(as_yaml* yaml, const char* name, const char* value, as_policy_read_mode_ap* read_mode_ap)
{
	as_policy_read_mode_ap val;

	if (strcmp(value, "ONE") == 0) {
		val = AS_POLICY_READ_MODE_AP_ONE;
	}
	else if (strcmp(value, "ALL") == 0) {
		val = AS_POLICY_READ_MODE_AP_ALL;
	}
	else {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid %s: %s", name, value);
		return false;
	}

	if (*read_mode_ap != val) {
		as_log_info("Set %s.%s = %s", yaml->name, name, value);
		*read_mode_ap = val;
	}
	return true;
}

static bool
as_parse_read_mode_sc(as_yaml* yaml, const char* name, const char* value, as_policy_read_mode_sc* read_mode_sc)
{
	as_policy_read_mode_sc val;

	if (strcmp(value, "SESSION") == 0) {
		val = AS_POLICY_READ_MODE_SC_SESSION;
	}
	else if (strcmp(value, "LINEARIZE") == 0) {
		val = AS_POLICY_READ_MODE_SC_LINEARIZE;
	}
	else if (strcmp(value, "ALLOW_REPLICA") == 0) {
		val = AS_POLICY_READ_MODE_SC_ALLOW_REPLICA;
	}
	else if (strcmp(value, "ALLOW_UNAVAILABLE") == 0) {
		val = AS_POLICY_READ_MODE_SC_ALLOW_UNAVAILABLE;
	}
	else {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid %s: %s", name, value);
		return false;
	}

	if (*read_mode_sc != val) {
		as_log_info("Set %s.%s = %s", yaml->name, name, value);
		*read_mode_sc = val;
	}
	return true;
}

static bool
as_parse_replica(as_yaml* yaml, const char* name, const char* value, as_policy_replica* replica)
{
	as_policy_replica val;

	if (strcmp(value, "MASTER") == 0) {
		val = AS_POLICY_REPLICA_MASTER;
	}
	else if (strcmp(value, "MASTER_PROLES") == 0) {
		val = AS_POLICY_REPLICA_ANY;
	}
	else if (strcmp(value, "SEQUENCE") == 0) {
		val = AS_POLICY_REPLICA_SEQUENCE;
	}
	else if (strcmp(value, "PREFER_RACK") == 0) {
		val = AS_POLICY_REPLICA_PREFER_RACK;
	}
	else {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid %s: %s", name, value);
		return false;
	}

	if (*replica != val) {
		as_log_info("Set %s.%s = %s", yaml->name, name, value);
		*replica = val;
	}
	return true;
}

static bool
as_parse_expected_duration(as_yaml* yaml, const char* name, const char* value, as_query_duration* expected_duration)
{
	as_query_duration val;

	if (strcmp(value, "LONG") == 0) {
		val = AS_QUERY_DURATION_LONG;
	}
	else if (strcmp(value, "SHORT") == 0) {
		val = AS_QUERY_DURATION_SHORT;
	}
	else if (strcmp(value, "LONG_RELAX_AP") == 0) {
		val = AS_QUERY_DURATION_LONG_RELAX_AP;
	}
	else {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid %s: %s", name, value);
		return false;
	}

	if (*expected_duration != val) {
		as_log_info("Set %s.%s = %s", yaml->name, name, value);
		*expected_duration = val;
	}
	return true;
}

#define as_str(x) #x
#define as_xstr(x) as_str(x)

static bool
as_parse_send_key(as_yaml* yaml, const char* name, const char* value, as_policy_key* key)
{
	bool send_key;

	if (!parse_bool(yaml, name, value, &send_key)) {
		return false;
	}

	as_policy_key val = send_key ? AS_POLICY_KEY_SEND : AS_POLICY_KEY_DIGEST;

	if (*key != val) {
		const char* str = send_key ? as_xstr(AS_POLICY_KEY_SEND) : as_xstr(AS_POLICY_KEY_DIGEST);
		as_log_info("Set %s.%s = %s", yaml->name, name, str);
		*key = val;
	}
	return true;
}

static bool
as_parse_max_concurrent_threads(as_yaml* yaml, const char* name, const char* value, bool* concurrent)
{
	uint32_t max_concurrent_threads;

	if (!parse_uint32(yaml, name, value, &max_concurrent_threads)) {
		return false;
	}

	bool val = max_concurrent_threads != 1;

	if (*concurrent != val) {
		const char* str = val ? "true" : "false";
		as_log_info("Set %s.concurrent = %s", yaml->name, str);
		*concurrent = val;
	}
	return true;
}

static bool
as_parse_read(as_yaml* yaml, const char* name, const char* value, as_policies* base)
{
	as_policy_read* policy = &base->read;
	yaml->name = "read";

	if (strcmp(name, "read_mode_ap") == 0) {
		return as_parse_read_mode_ap(yaml, name, value, &policy->read_mode_ap);
	}

	if (strcmp(name, "read_mode_sc") == 0) {
		return as_parse_read_mode_sc(yaml, name, value, &policy->read_mode_sc);
	}

	if (strcmp(name, "replica") == 0) {
		return as_parse_replica(yaml, name, value, &policy->replica);
	}

	if (strcmp(name, "socket_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.socket_timeout);
	}

	if (strcmp(name, "total_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.total_timeout);
	}

	if (strcmp(name, "max_retries") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.max_retries);
	}

	if (strcmp(name, "sleep_between_retries") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.sleep_between_retries);
	}

	if (strcmp(name, "connect_timeout") == 0) {
		// Not supported.
		return true;
	}

	if (strcmp(name, "timeout_delay") == 0) {
		// Not supported.
		return true;
	}

	if (strcmp(name, "fail_on_filtered_out") == 0) {
		// Not supported.
		return true;
	}

	as_log_info("Unexpected field: %s.%s", yaml->name, name);
	return true;
}

static bool
as_parse_write(as_yaml* yaml, const char* name, const char* value, as_policies* base)
{
	as_policy_write* policy = &base->write;
	yaml->name = "write";

	if (strcmp(name, "replica") == 0) {
		return as_parse_replica(yaml, name, value, &policy->replica);
	}

	if (strcmp(name, "socket_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.socket_timeout);
	}

	if (strcmp(name, "total_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.total_timeout);
	}

	if (strcmp(name, "max_retries") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.max_retries);
	}

	if (strcmp(name, "sleep_between_retries") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.sleep_between_retries);
	}

	if (strcmp(name, "send_key") == 0) {
		return as_parse_send_key(yaml, name, value, &policy->key);
	}

	if (strcmp(name, "durable_delete") == 0) {
		return as_parse_bool(yaml, name, value, &policy->durable_delete);
	}

	if (strcmp(name, "connect_timeout") == 0) {
		// Not supported.
		return true;
	}

	if (strcmp(name, "timeout_delay") == 0) {
		// Not supported.
		return true;
	}

	if (strcmp(name, "fail_on_filtered_out") == 0) {
		// Not supported.
		return true;
	}

	as_log_info("Unexpected field: %s.%s", yaml->name, name);
	return true;
}

static bool
as_parse_scan(as_yaml* yaml, const char* name, const char* value, as_policies* base)
{
	as_policy_scan* policy = &base->scan;
	yaml->name = "scan";

	if (strcmp(name, "replica") == 0) {
		return as_parse_replica(yaml, name, value, &policy->replica);
	}

	if (strcmp(name, "socket_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.socket_timeout);
	}

	if (strcmp(name, "total_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.total_timeout);
	}

	if (strcmp(name, "max_retries") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.max_retries);
	}

	if (strcmp(name, "sleep_between_retries") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.sleep_between_retries);
	}

	if (strcmp(name, "connect_timeout") == 0) {
		// Not supported.
		return true;
	}

	if (strcmp(name, "timeout_delay") == 0) {
		// Not supported.
		return true;
	}

	if (strcmp(name, "read_mode_ap") == 0) {
		// Not supported.
		return true;
	}

	if (strcmp(name, "read_mode_sc") == 0) {
		// Not supported.
		return true;
	}

	if (strcmp(name, "concurrent_nodes") == 0) {
		// Not supported.
		return true;
	}

	if (strcmp(name, "max_concurrent_nodes") == 0) {
		// concurrent_nodes is supported in as_scan, but there no policy defaults for that.
		return true;
	}

	as_log_info("Unexpected field: %s.%s", yaml->name, name);
	return true;
}

static bool
as_parse_query(as_yaml* yaml, const char* name, const char* value, as_policies* base)
{
	as_policy_query* policy = &base->query;
	yaml->name = "query";

	if (strcmp(name, "replica") == 0) {
		return as_parse_replica(yaml, name, value, &policy->replica);
	}

	if (strcmp(name, "socket_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.socket_timeout);
	}

	if (strcmp(name, "total_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.total_timeout);
	}

	if (strcmp(name, "max_retries") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.max_retries);
	}

	if (strcmp(name, "sleep_between_retries") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.sleep_between_retries);
	}

	if (strcmp(name, "info_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->info_timeout);
	}

	if (strcmp(name, "expected_duration") == 0) {
		return as_parse_expected_duration(yaml, name, value, &policy->expected_duration);
	}

	if (strcmp(name, "connect_timeout") == 0) {
		// Not supported.
		return true;
	}

	if (strcmp(name, "timeout_delay") == 0) {
		// Not supported.
		return true;
	}

	if (strcmp(name, "read_mode_ap") == 0) {
		// Not supported.
		return true;
	}

	if (strcmp(name, "read_mode_sc") == 0) {
		// Not supported.
		return true;
	}

	if (strcmp(name, "include_bin_data") == 0) {
		// Not supported.
		return true;
	}

	if (strcmp(name, "record_queue_size") == 0) {
		// Not supported.
		return true;
	}

	as_log_info("Unexpected field: %s.%s", yaml->name, name);
	return true;
}

static bool
as_parse_batch_shared(as_yaml* yaml, const char* name, const char* value, as_policy_batch* policy)
{
	if (strcmp(name, "read_mode_ap") == 0) {
		return as_parse_read_mode_ap(yaml, name, value, &policy->read_mode_ap);
	}

	if (strcmp(name, "read_mode_sc") == 0) {
		return as_parse_read_mode_sc(yaml, name, value, &policy->read_mode_sc);
	}

	if (strcmp(name, "replica") == 0) {
		return as_parse_replica(yaml, name, value, &policy->replica);
	}

	if (strcmp(name, "socket_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.socket_timeout);
	}

	if (strcmp(name, "total_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.total_timeout);
	}

	if (strcmp(name, "max_retries") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.max_retries);
	}

	if (strcmp(name, "sleep_between_retries") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.sleep_between_retries);
	}

	if (strcmp(name, "max_concurrent_threads") == 0) {
		return as_parse_max_concurrent_threads(yaml, name, value, &policy->concurrent);
	}

	if (strcmp(name, "allow_inline") == 0) {
		return as_parse_bool(yaml, name, value, &policy->allow_inline);
	}

	if (strcmp(name, "allow_inline_ssd") == 0) {
		return as_parse_bool(yaml, name, value, &policy->allow_inline_ssd);
	}

	if (strcmp(name, "respond_all_keys") == 0) {
		return as_parse_bool(yaml, name, value, &policy->respond_all_keys);
	}

	if (strcmp(name, "connect_timeout") == 0) {
		// Not supported.
		return true;
	}

	if (strcmp(name, "timeout_delay") == 0) {
		// Not supported.
		return true;
	}

	if (strcmp(name, "fail_on_filtered_out") == 0) {
		// Not supported.
		return true;
	}

	as_log_info("Unexpected field: %s.%s", yaml->name, name);
	return true;
}

static bool
as_parse_batch_read(as_yaml* yaml, const char* name, const char* value, as_policies* base)
{
	yaml->name = "batch_read";
	return as_parse_batch_shared(yaml, name, value, &base->batch);
}

static bool
as_parse_batch_write(as_yaml* yaml, const char* name, const char* value, as_policies* base)
{
	as_policy_batch_write* bw = &base->batch_write;
	yaml->name = "batch_write";

	if (strcmp(name, "durable_delete") == 0) {
		return as_parse_bool(yaml, name, value, &bw->durable_delete);
	}

	if (strcmp(name, "send_key") == 0) {
		return as_parse_send_key(yaml, name, value, &bw->key);
	}

	return as_parse_batch_shared(yaml, name, value, &base->batch_parent_write);
}

static bool
as_parse_batch_udf(as_yaml* yaml, const char* name, const char* value, as_policies* base)
{
	as_policy_batch_apply* policy = &base->batch_apply;
	yaml->name = "batch_udf";

	if (strcmp(name, "durable_delete") == 0) {
		return as_parse_bool(yaml, name, value, &policy->durable_delete);
	}

	if (strcmp(name, "send_key") == 0) {
		return as_parse_send_key(yaml, name, value, &policy->key);
	}

	as_log_info("Unexpected field: %s.%s", yaml->name, name);
	return true;
}

static bool
as_parse_batch_delete(as_yaml* yaml, const char* name, const char* value, as_policies* base)
{
	as_policy_batch_remove* policy = &base->batch_remove;
	yaml->name = "batch_delete";

	if (strcmp(name, "durable_delete") == 0) {
		return as_parse_bool(yaml, name, value, &policy->durable_delete);
	}

	if (strcmp(name, "send_key") == 0) {
		return as_parse_send_key(yaml, name, value, &policy->key);
	}

	as_log_info("Unexpected field: %s.%s", yaml->name, name);
	return true;
}

static bool
as_parse_txn_verify(as_yaml* yaml, const char* name, const char* value, as_policies* base)
{
	yaml->name = "txn_verify";
	return as_parse_batch_shared(yaml, name, value, &base->txn_verify);
}

static bool
as_parse_txn_roll(as_yaml* yaml, const char* name, const char* value, as_policies* base)
{
	yaml->name = "txn_roll";
	return as_parse_batch_shared(yaml, name, value, &base->txn_roll);
}

typedef bool (*as_parse_policy_fn) (as_yaml* yaml, const char* name, const char* value, as_policies* base);

static bool
as_parse_policy(as_yaml* yaml, as_parse_policy_fn fn)
{
	as_policies* base = &yaml->config->policies;

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

			rv = fn(yaml, name, value, base);
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

	yaml->name = "client";

	char name[256];

	while (as_parse_scalar(yaml, name, sizeof(name))) {
		if (!as_parse_next(yaml)) {
			return false;
		}

		if (yaml->event.type == YAML_SCALAR_EVENT) {
			const char* value = (const char*)yaml->event.data.scalar.value;
			bool rv;

			if (strcmp(name, "config_tend_count") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->config_provider.config_tend_count);
			}
			else if (strcmp(name, "max_connections_per_node") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->max_conns_per_node);
			}
			else if (strcmp(name, "min_connections_per_node") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->min_conns_per_node);
			}
			else if (strcmp(name, "async_max_connections_per_node") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->async_max_conns_per_node);
			}
			else if (strcmp(name, "async_min_connections_per_node") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->async_min_conns_per_node);
			}
			else {
				as_log_info("Unexpected field: %s.%s", yaml->name, name);
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

	yaml->name = "client";

	char name[256];

	while (as_parse_scalar(yaml, name, sizeof(name))) {
		if (!as_parse_next(yaml)) {
			return false;
		}

		if (yaml->event.type == YAML_SCALAR_EVENT) {
			const char* value = (const char*)yaml->event.data.scalar.value;
			bool rv;

			if (strcmp(name, "timeout") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->conn_timeout_ms);
			}
			else if (strcmp(name, "error_rate_window") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->error_rate_window);
			}
			else if (strcmp(name, "max_error_rate") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->max_error_rate);
			}
			else if (strcmp(name, "login_timeout") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->login_timeout_ms);
			}
			else if (strcmp(name, "max_socket_idle") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->max_socket_idle);
			}
			else if (strcmp(name, "tend_interval") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->tender_interval);
			}
			else if (strcmp(name, "fail_if_not_connected") == 0) {
				rv = as_parse_bool(yaml, name, value, &yaml->config->fail_if_not_connected);
			}
			else if (strcmp(name, "use_service_alternative") == 0) {
				rv = as_parse_bool(yaml, name, value, &yaml->config->use_services_alternate);
			}
			else if (strcmp(name, "rack_aware") == 0) {
				rv = as_parse_bool(yaml, name, value, &yaml->config->rack_aware);
			}
			else {
				as_log_info("Unexpected field: %s.%s", yaml->name, name);
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
			}
			else {
				as_log_info("Unexpected sequence: %s.%s", yaml->name, name);
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
		if (! yaml->init) {
			// Do not process static fields on a dynamic update.
			as_skip_mapping(yaml);
		}
		else if (strcmp(name, "client") == 0) {
			as_parse_static_client(yaml);
		}
		else {
			as_log_info("Unexpected section: %s", name);
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
			as_parse_dynamic_client(yaml);
		}
		else if (strcmp(name, "read") == 0) {
			as_parse_policy(yaml, as_parse_read);
		}
		else if (strcmp(name, "write") == 0) {
			as_parse_policy(yaml, as_parse_write);
		}
		else if (strcmp(name, "query") == 0) {
			as_parse_policy(yaml, as_parse_query);
		}
		else if (strcmp(name, "scan") == 0) {
			as_parse_policy(yaml, as_parse_scan);
		}
		else if (strcmp(name, "batch_read") == 0) {
			as_parse_policy(yaml, as_parse_batch_read);
		}
		else if (strcmp(name, "batch_write") == 0) {
			as_parse_policy(yaml, as_parse_batch_write);
		}
		else if (strcmp(name, "batch_udf") == 0) {
			as_parse_policy(yaml, as_parse_batch_udf);
		}
		else if (strcmp(name, "batch_delete") == 0) {
			as_parse_policy(yaml, as_parse_batch_delete);
		}
		else if (strcmp(name, "txn_verify") == 0) {
			as_parse_policy(yaml, as_parse_txn_verify);
		}
		else if (strcmp(name, "txn_roll") == 0) {
			as_parse_policy(yaml, as_parse_txn_roll);
		}
		else {
			as_log_info("Unexpected section: %s", name);
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
			as_parse_static(yaml);
		}
		else if (strcmp(name, "dynamic") == 0) {
			as_parse_dynamic(yaml);
		}
		else if (strcmp(name, "metadata") == 0) {
			as_skip_mapping(yaml);
		}
		else {
			as_log_info("Unexpected section: %s", name);
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
as_config_yaml_read(as_config* config, bool init, as_error* err)
{
	as_error_reset(err);

	const char* path = config->config_provider.yaml_path;
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

void
as_cluster_set_max_socket_idle(as_cluster* cluster, uint32_t max_socket_idle_sec);

static void
as_release_rack_ids(as_vector* rack_ids)
{
	as_vector_destroy(rack_ids);
}

static void
as_cluster_update(as_cluster* cluster, as_config* orig, as_config* config)
{
	// Set original config.
	orig->max_error_rate = config->max_error_rate;
	orig->error_rate_window = config->error_rate_window;
	orig->login_timeout_ms = config->login_timeout_ms;
	orig->tender_interval = config->tender_interval;
	orig->conn_timeout_ms = config->conn_timeout_ms;
	orig->use_services_alternate = config->use_services_alternate;
	orig->fail_if_not_connected = config->fail_if_not_connected;
	orig->max_socket_idle = config->max_socket_idle;
	orig->rack_aware = config->rack_aware;

	if (orig->rack_ids != config->rack_ids) {
		// Can be destroyed now since all rack_ids access is done through
		// cluster rack_ids and not config rack_ids.
		as_vector_destroy(orig->rack_ids);
		orig->rack_ids = config->rack_ids;
	}

	// Set cluster.
	cluster->max_error_rate = config->max_error_rate;
	cluster->error_rate_window = config->error_rate_window;
	cluster->login_timeout_ms = (config->login_timeout_ms == 0) ? 5000 : config->login_timeout_ms;
	cluster->tend_interval = (config->tender_interval < 250)? 250 : config->tender_interval;
	cluster->conn_timeout_ms = (config->conn_timeout_ms == 0) ? 1000 : config->conn_timeout_ms;
	cluster->use_services_alternate = config->use_services_alternate;
	cluster->fail_if_not_connected = config->fail_if_not_connected;
	as_cluster_set_max_socket_idle(cluster, config->max_socket_idle);
	cluster->rack_aware = config->rack_aware;

	if (!as_vector_int32_equal(config->rack_ids, cluster->rack_ids)) {
		as_vector* old = cluster->rack_ids;

		// Make full copy of rack_ids
		uint32_t max = config->rack_ids->size;
		as_vector* rack_ids = as_vector_create(sizeof(int), max);

		for (uint32_t i = 0; i < max; i++) {
			int id = *(int*)as_vector_get(config->rack_ids, i);
			as_vector_append(rack_ids, &id);
		}

		// Update cluster rack_ids.
		as_store_ptr_rls((void**)&cluster->rack_ids, rack_ids);

		// Eventually destroy old cluster rack_ids.
		as_gc_item item;
		item.data = old;
		item.release_fn = (as_release_fn)as_release_rack_ids;
		as_vector_append(cluster->gc, &item);
	}

	// Copy new policy values to the cluster one field at a time.
	// Do not perform memcpy because that byte protocol might temporarily
	// corrupt multi-byte values which are being read in parallel threads.
	as_policies* src = &config->policies;
	as_policies* trg = &orig->policies;

	trg->read.base.socket_timeout = src->read.base.socket_timeout;
	trg->read.base.total_timeout = src->read.base.total_timeout;
	trg->read.base.max_retries = src->read.base.max_retries;
	trg->read.base.sleep_between_retries = src->read.base.sleep_between_retries;
	trg->read.read_mode_ap = src->read.read_mode_ap;
	trg->read.read_mode_sc = src->read.read_mode_sc;
	trg->read.replica = src->read.replica;

	trg->write.base.socket_timeout = src->write.base.socket_timeout;
	trg->write.base.total_timeout = src->write.base.total_timeout;
	trg->write.base.max_retries = src->write.base.max_retries;
	trg->write.base.sleep_between_retries = src->write.base.sleep_between_retries;
	trg->write.replica = src->write.replica;
	trg->write.durable_delete = src->write.durable_delete;
	trg->write.key = src->write.key;

	trg->scan.base.socket_timeout = src->scan.base.socket_timeout;
	trg->scan.base.total_timeout = src->scan.base.total_timeout;
	trg->scan.base.max_retries = src->scan.base.max_retries;
	trg->scan.base.sleep_between_retries = src->scan.base.sleep_between_retries;
	trg->scan.replica = src->scan.replica;

	trg->query.base.socket_timeout = src->query.base.socket_timeout;
	trg->query.base.total_timeout = src->query.base.total_timeout;
	trg->query.base.max_retries = src->query.base.max_retries;
	trg->query.base.sleep_between_retries = src->query.base.sleep_between_retries;
	trg->query.replica = src->query.replica;
	trg->query.info_timeout = src->query.info_timeout;
	trg->query.expected_duration = src->query.expected_duration;

	trg->batch.base.socket_timeout = src->batch.base.socket_timeout;
	trg->batch.base.total_timeout = src->batch.base.total_timeout;
	trg->batch.base.max_retries = src->batch.base.max_retries;
	trg->batch.base.sleep_between_retries = src->batch.base.sleep_between_retries;
	trg->batch.read_mode_ap = src->batch.read_mode_ap;
	trg->batch.read_mode_sc = src->batch.read_mode_sc;
	trg->batch.replica = src->batch.replica;
	trg->batch.concurrent = src->batch.concurrent;
	trg->batch.allow_inline = src->batch.allow_inline;
	trg->batch.allow_inline_ssd = src->batch.allow_inline_ssd;
	trg->batch.respond_all_keys = src->batch.respond_all_keys;

	trg->batch_parent_write.base.socket_timeout = src->batch_parent_write.base.socket_timeout;
	trg->batch_parent_write.base.total_timeout = src->batch_parent_write.base.total_timeout;
	trg->batch_parent_write.base.max_retries = src->batch_parent_write.base.max_retries;
	trg->batch_parent_write.base.sleep_between_retries = src->batch_parent_write.base.sleep_between_retries;
	trg->batch_parent_write.read_mode_ap = src->batch_parent_write.read_mode_ap;
	trg->batch_parent_write.read_mode_sc = src->batch_parent_write.read_mode_sc;
	trg->batch_parent_write.replica = src->batch_parent_write.replica;
	trg->batch_parent_write.concurrent = src->batch_parent_write.concurrent;
	trg->batch_parent_write.allow_inline = src->batch_parent_write.allow_inline;
	trg->batch_parent_write.allow_inline_ssd = src->batch_parent_write.allow_inline_ssd;
	trg->batch_parent_write.respond_all_keys = src->batch_parent_write.respond_all_keys;

	trg->batch_write.durable_delete = src->batch_write.durable_delete;
	trg->batch_write.key = src->batch_write.key;

	trg->batch_apply.durable_delete = src->batch_apply.durable_delete;
	trg->batch_apply.key = src->batch_apply.key;

	trg->batch_remove.durable_delete = src->batch_remove.durable_delete;
	trg->batch_remove.key = src->batch_remove.key;

	trg->txn_verify.base.socket_timeout = src->txn_verify.base.socket_timeout;
	trg->txn_verify.base.total_timeout = src->txn_verify.base.total_timeout;
	trg->txn_verify.base.max_retries = src->txn_verify.base.max_retries;
	trg->txn_verify.base.sleep_between_retries = src->txn_verify.base.sleep_between_retries;
	trg->txn_verify.read_mode_ap = src->txn_verify.read_mode_ap;
	trg->txn_verify.read_mode_sc = src->txn_verify.read_mode_sc;
	trg->txn_verify.replica = src->txn_verify.replica;
	trg->txn_verify.concurrent = src->txn_verify.concurrent;
	trg->txn_verify.allow_inline = src->txn_verify.allow_inline;
	trg->txn_verify.allow_inline_ssd = src->txn_verify.allow_inline_ssd;
	trg->txn_verify.respond_all_keys = src->txn_verify.respond_all_keys;

	trg->txn_roll.base.socket_timeout = src->txn_roll.base.socket_timeout;
	trg->txn_roll.base.total_timeout = src->txn_roll.base.total_timeout;
	trg->txn_roll.base.max_retries = src->txn_roll.base.max_retries;
	trg->txn_roll.base.sleep_between_retries = src->txn_roll.base.sleep_between_retries;
	trg->txn_roll.read_mode_ap = src->txn_roll.read_mode_ap;
	trg->txn_roll.read_mode_sc = src->txn_roll.read_mode_sc;
	trg->txn_roll.replica = src->txn_roll.replica;
	trg->txn_roll.concurrent = src->txn_roll.concurrent;
	trg->txn_roll.allow_inline = src->txn_roll.allow_inline;
	trg->txn_roll.allow_inline_ssd = src->txn_roll.allow_inline_ssd;
	trg->txn_roll.respond_all_keys = src->txn_roll.respond_all_keys;
}

//---------------------------------
// Functions
//---------------------------------

as_status
as_config_yaml_init(as_config* config, as_error* err)
{
	if (!config->rack_ids) {
		// Add config rack_id to rack_ids so it can be compared with yaml file rack_ids.
		config->rack_ids = as_vector_create(sizeof(int), 1);
		as_vector_append(config->rack_ids, &config->rack_id);
	}
	
	return as_config_yaml_read(config, true, err);
}

as_status
as_config_yaml_update(as_cluster* cluster, as_config* orig, as_error* err)
{
	as_config config;
	memcpy(&config, orig, sizeof(as_config));

	as_status status = as_config_yaml_read(&config, false, err);

	if (status != AEROSPIKE_OK) {
		// Destroy new rack_ids vector if changed before update fails.
		if (config.rack_ids != orig->rack_ids) {
			as_vector_destroy(config.rack_ids);
		}
		return status;
	}

	as_log_info("Update dynamic config");
	as_cluster_update(cluster, orig, &config);
	return AEROSPIKE_OK;
}
