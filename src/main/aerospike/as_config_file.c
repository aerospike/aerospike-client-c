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
#include <aerospike/as_config_file.h>
#include <aerospike/aerospike.h>
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
	uint8_t* bitmap;
	as_config* config;
	const char* name;
	as_error err;
	bool init;
} as_yaml;

//---------------------------------
// Static Functions
//---------------------------------

static inline bool
as_parse_next(as_yaml* yaml)
{
	if (!yaml_parser_parse(&yaml->parser, &yaml->event)) {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "yaml_parser_parse failure at offset %zu", yaml->parser.offset);
		return false;
	}
	return true;
}

static bool
as_skip_section(as_yaml* yaml);

static bool
as_skip_value(as_yaml* yaml)
{
	switch (yaml->event.type) {
		case YAML_MAPPING_START_EVENT:
			yaml_event_delete(&yaml->event);

			// Parse name/value pairs.
			while (as_parse_next(yaml)) {
				if (yaml->event.type == YAML_MAPPING_END_EVENT) {
					yaml_event_delete(&yaml->event);
					break;
				}
				yaml_event_delete(&yaml->event);

				if (!as_parse_next(yaml)) {
					return false;
				}

				if (!as_skip_value(yaml)) {
					return false;
				}
			}
			return true;

		case YAML_SEQUENCE_START_EVENT:
			yaml_event_delete(&yaml->event);

			while (as_skip_section(yaml)) {
			}
			return true;

		case YAML_SCALAR_EVENT:
			yaml_event_delete(&yaml->event);
			return true;

		default:
			yaml_event_delete(&yaml->event);
			return false;
	}
}

static bool
as_skip_section(as_yaml* yaml)
{
	if (!as_parse_next(yaml)) {
		return false;
	}

	if (!as_skip_value(yaml)) {
		return false;
	}

	return true;
}

static void
as_skip_sequence(as_yaml* yaml)
{
	while (as_skip_section(yaml)) {
	}
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
as_parse_int32(as_yaml* yaml, const char* name, const char* value, int32_t* out)
{
    char* end = NULL;
    errno = 0;
	long v = strtol(value, &end, 10);

	if (end == value || (errno != 0)) {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid int %s: %s", name, value);
		return false;
	}

	*out = (int32_t)v;
	return true;
}

static inline void
as_assign_uint32(
	const char* section, const char* name, const char* value, uint32_t src, uint32_t* trg
	)
{
	if (*trg != src) {
		as_log_info("Set %s.%s = %s", section, name, value);
		*trg = src;
	}
}

static bool
parse_uint32(
	as_yaml* yaml, const char* name, const char* value, uint32_t min, uint32_t max, uint32_t* out
	)
{
    char* end = NULL;
    errno = 0;
	unsigned long v = strtoul(value, &end, 10);

	if (end == value || (errno != 0)) {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid uint %s: %s", name, value);
		return false;
	}

	if (v < min || v > max) {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid uint %s: %s. valid range: %u - %u",
			name, value, min, max);
		return false;
	}

	*out = (uint32_t)v;
	return true;
}

static bool
as_parse_uint32_range(
	as_yaml* yaml, const char* name, const char* value, uint32_t min, uint32_t max, uint32_t* out,
	uint32_t field
	)
{
	uint32_t val;

	if (!parse_uint32(yaml, name, value, min, max, &val)) {
		return false;
	}

	as_assign_uint32(yaml->name, name, value, val, out);
	as_field_set(yaml->bitmap, field);
	return true;
}

static bool
as_parse_uint32(as_yaml* yaml, const char* name, const char* value, uint32_t* out, uint32_t field)
{
	uint32_t val;

	if (!parse_uint32(yaml, name, value, 0, UINT32_MAX, &val)) {
		return false;
	}

	as_assign_uint32(yaml->name, name, value, val, out);
	as_field_set(yaml->bitmap, field);
	return true;
}

static inline void
as_assign_uint8(
	const char* section, const char* name, const char* value, uint8_t src, uint8_t* trg
	)
{
	if (*trg != src) {
		as_log_info("Set %s.%s = %s", section, name, value);
		*trg = src;
	}
}

static bool
parse_uint8(as_yaml* yaml, const char* name, const char* value, uint8_t* out)
{
    char* end = NULL;
    errno = 0;
	unsigned long v = strtoul(value, &end, 10);

	if (end == value || (errno != 0) || v > 255) {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid uint8 %s: %s", name, value);
		return false;
	}

	*out = (uint8_t)v;
	return true;
}

static bool
as_parse_uint8(as_yaml* yaml, const char* name, const char* value, uint8_t* out, uint32_t field)
{
	uint8_t val;

	if (!parse_uint8(yaml, name, value, &val)) {
		return false;
	}

	as_assign_uint8(yaml->name, name, value, val, out);
	as_field_set(yaml->bitmap, field);
	return true;
}

static inline void
as_assign_bool(
	const char* section, const char* name, const char* value, bool src, bool* trg
	)
{
	if (*trg != src) {
		as_log_info("Set %s.%s = %s", section, name, value);
		*trg = src;
	}
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
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid bool %s: %s", name, value);
		return false;
	}
	return true;
}

static bool
as_parse_bool(as_yaml* yaml, const char* name, const char* value, bool* out, uint32_t field)
{
	bool val;

	if (!parse_bool(yaml, name, value, &val)) {
		return false;
	}

	as_assign_bool(yaml->name, name, value, val, out);
	as_field_set(yaml->bitmap, field);
	return true;
}

static inline void
as_assign_string(const char* section, const char* name, char* src, char** trg)
{
	if (*trg == NULL || strcmp(*trg, src) != 0) {
		as_log_info("Set %s.%s = %s", section, name, src);
		*trg = src;
	}
}

static bool
as_parse_string(as_yaml* yaml, const char* name, const char* value, char** out, uint32_t field)
{
	char* val = cf_strdup(value);

	as_assign_string(yaml->name, name, val, out);
	as_field_set(yaml->bitmap, field);
	return true;
}

static bool
as_rack_ids_equal(as_cluster* c1, as_config* c2)
{
	// Cluster racks_ids will never be NULL.
	// Config rack_ids can be NULL and rack_id exists.
	if (c2->rack_ids == NULL) {
		if (c1->rack_ids->size == 1) {
			int rack_id = *(int*)as_vector_get(c1->rack_ids, 0);
			return rack_id == c2->rack_id;
		}
		else {
			return false;
		}
	}

	as_vector* v1 = c1->rack_ids;
	as_vector* v2 = c2->rack_ids;

	if (v1->size != v2->size) {
		return false;
	}

	for (uint32_t i = 0; i < v1->size; i++) {
		int id1 = *(int*)as_vector_get(v1, i);
		int id2 = *(int*)as_vector_get(v2, i);

		if (id1 != id2) {
			return false;
		}
	}
	return true;
}

static bool
as_parse_vector_int32(as_yaml* yaml, const char* name, as_vector** out, uint32_t field)
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

	// Original vector will still exist and will be restored if
	// there is an error while parsing the file.
	*out = list;

	as_field_set(yaml->bitmap, field);
	return true;
}

static inline void
as_assign_read_mode_ap(
	const char* section, const char* name, const char* value, as_policy_read_mode_ap src,
	as_policy_read_mode_ap* trg
	)
{
	if (*trg != src) {
		as_log_info("Set %s.%s = %s", section, name, value);
		*trg = src;
	}
}

static bool
as_parse_read_mode_ap(
	as_yaml* yaml, const char* name, const char* value, as_policy_read_mode_ap* read_mode_ap,
	uint32_t field
	)
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

	as_assign_read_mode_ap(yaml->name, name, value, val, read_mode_ap);
	as_field_set(yaml->bitmap, field);
	return true;
}

static inline void
as_assign_read_mode_sc(
	const char* section, const char* name, const char* value, as_policy_read_mode_sc src,
	as_policy_read_mode_sc* trg
	)
{
	if (*trg != src) {
		as_log_info("Set %s.%s = %s", section, name, value);
		*trg = src;
	}
}

static bool
as_parse_read_mode_sc(
	as_yaml* yaml, const char* name, const char* value, as_policy_read_mode_sc* read_mode_sc,
	uint32_t field
	)
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

	as_assign_read_mode_sc(yaml->name, name, value, val, read_mode_sc);
	as_field_set(yaml->bitmap, field);
	return true;
}

static inline void
as_assign_replica(
	const char* section, const char* name, const char* value, as_policy_replica src,
	as_policy_replica* trg
	)
{
	if (*trg != src) {
		as_log_info("Set %s.%s = %s", section, name, value);
		*trg = src;
	}
}

static bool
as_parse_replica(
	as_yaml* yaml, const char* name, const char* value, as_policy_replica* replica, uint32_t field
	)
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
	else if (strcmp(value, "RANDOM") == 0) {
		val = AS_POLICY_REPLICA_RANDOM;
	}
	else {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid %s: %s", name, value);
		return false;
	}

	as_assign_replica(yaml->name, name, value, val, replica);
	as_field_set(yaml->bitmap, field);
	return true;
}

static bool
as_parse_expected_duration(
	as_yaml* yaml, const char* name, const char* value, as_query_duration* expected_duration,
	uint32_t field
	)
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

	as_field_set(yaml->bitmap, field);
	return true;
}

#define as_str(x) #x
#define as_xstr(x) as_str(x)

static void
as_assign_send_key(
	const char* section, const char* name, const char* value, as_policy_key src, as_policy_key* trg
	)
{
	if (*trg != src) {
		const char* str = (src == AS_POLICY_KEY_SEND) ? as_xstr(AS_POLICY_KEY_SEND) : as_xstr(AS_POLICY_KEY_DIGEST);
		as_log_info("Set %s.%s = %s", section, name, str);
		*trg = src;
	}
}

static bool
as_parse_send_key(
	as_yaml* yaml, const char* name, const char* value, as_policy_key* key, uint32_t field
	)
{
	bool send_key;

	if (!parse_bool(yaml, name, value, &send_key)) {
		return false;
	}

	as_policy_key val = send_key ? AS_POLICY_KEY_SEND : AS_POLICY_KEY_DIGEST;

	as_assign_send_key(yaml->name, name, value, val, key);
	as_field_set(yaml->bitmap, field);
	return true;
}

static bool
as_parse_max_concurrent_threads(
	as_yaml* yaml, const char* name, const char* value, bool* concurrent, uint32_t field
	)
{
	uint32_t max_concurrent_threads;

	if (!parse_uint32(yaml, name, value, 0, UINT32_MAX, &max_concurrent_threads)) {
		return false;
	}

	bool val = max_concurrent_threads != 1;

	if (*concurrent != val) {
		const char* str = val ? "true" : "false";
		as_log_info("Set %s.concurrent = %s", yaml->name, str);
		*concurrent = val;
	}

	as_field_set(yaml->bitmap, field);
	return true;
}

static bool
as_parse_read(as_yaml* yaml, const char* name, const char* value, as_policies* base)
{
	as_policy_read* policy = &base->read;
	yaml->name = "read";

	// The dynamic configuration file schema contains a read policy, but the C client
	// has as_policy_read and as_policy_operate. The file read policy must be applied
	// to all of these policies.
	const char* operate_section = "operate";

	if (strcmp(name, "read_mode_ap") == 0) {
		if (!as_parse_read_mode_ap(yaml, name, value, &policy->read_mode_ap, AS_READ_READ_MODE_AP)) {
			return false;
		}
		as_assign_read_mode_ap(operate_section, name, value, policy->read_mode_ap, &base->operate.read_mode_ap);
		return true;
	}

	if (strcmp(name, "read_mode_sc") == 0) {
		if (!as_parse_read_mode_sc(yaml, name, value, &policy->read_mode_sc, AS_READ_READ_MODE_SC)) {
			return false;
		}
		as_assign_read_mode_sc(operate_section, name, value, policy->read_mode_sc, &base->operate.read_mode_sc);
		return true;
	}

	if (strcmp(name, "replica") == 0) {
		return as_parse_replica(yaml, name, value, &policy->replica, AS_READ_REPLICA);
	}

	if (strcmp(name, "socket_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.socket_timeout, AS_READ_SOCKET_TIMEOUT);
	}

	if (strcmp(name, "total_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.total_timeout, AS_READ_TOTAL_TIMEOUT);
	}

	if (strcmp(name, "max_retries") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.max_retries, AS_READ_MAX_RETRIES);
	}

	if (strcmp(name, "sleep_between_retries") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.sleep_between_retries, AS_READ_SLEEP_BETWEEN_RETRIES);
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

	// The dynamic configuration file schema contains a write policy, but the C client
	// has as_policy_write, as_policy_operate, as_policy_remove and as_policy_apply.
	// The file write policy must be applied to all of these policies.
	const char* operate_section = "operate";
	const char* apply_section = "apply";
	const char* remove_section = "remove";

	if (strcmp(name, "replica") == 0) {
		if (!as_parse_replica(yaml, name, value, &policy->replica, AS_WRITE_REPLICA)) {
			return false;
		}
		as_assign_replica(operate_section, name, value, policy->replica, &base->operate.replica);
		as_assign_replica(apply_section, name, value, policy->replica, &base->apply.replica);
		as_assign_replica(remove_section, name, value, policy->replica, &base->remove.replica);
		return true;
	}

	if (strcmp(name, "socket_timeout") == 0) {
		if (!as_parse_uint32(yaml, name, value, &policy->base.socket_timeout, AS_WRITE_SOCKET_TIMEOUT)) {
			return false;
		}
		as_assign_uint32(operate_section, name, value, policy->base.socket_timeout, &base->operate.base.socket_timeout);
		as_assign_uint32(apply_section, name, value, policy->base.socket_timeout, &base->apply.base.socket_timeout);
		as_assign_uint32(remove_section, name, value, policy->base.socket_timeout, &base->remove.base.socket_timeout);
		return true;
	}

	if (strcmp(name, "total_timeout") == 0) {
		if (!as_parse_uint32(yaml, name, value, &policy->base.total_timeout, AS_WRITE_TOTAL_TIMEOUT)) {
			return false;
		}
		as_assign_uint32(operate_section, name, value, policy->base.total_timeout, &base->operate.base.total_timeout);
		as_assign_uint32(apply_section, name, value, policy->base.total_timeout, &base->apply.base.total_timeout);
		as_assign_uint32(remove_section, name, value, policy->base.total_timeout, &base->remove.base.total_timeout);
		return true;
	}

	if (strcmp(name, "max_retries") == 0) {
		if (!as_parse_uint32(yaml, name, value, &policy->base.max_retries, AS_WRITE_MAX_RETRIES)) {
			return false;
		}
		as_assign_uint32(operate_section, name, value, policy->base.max_retries, &base->operate.base.max_retries);
		as_assign_uint32(apply_section, name, value, policy->base.max_retries, &base->apply.base.max_retries);
		as_assign_uint32(remove_section, name, value, policy->base.max_retries, &base->remove.base.max_retries);
		return true;
	}

	if (strcmp(name, "sleep_between_retries") == 0) {
		if (!as_parse_uint32(yaml, name, value, &policy->base.sleep_between_retries, AS_WRITE_SLEEP_BETWEEN_RETRIES)) {
			return false;
		}
		as_assign_uint32(operate_section, name, value, policy->base.sleep_between_retries, &base->operate.base.max_retries);
		as_assign_uint32(apply_section, name, value, policy->base.sleep_between_retries, &base->apply.base.max_retries);
		as_assign_uint32(remove_section, name, value, policy->base.sleep_between_retries, &base->remove.base.max_retries);
		return true;
	}

	if (strcmp(name, "send_key") == 0) {
		if (!as_parse_send_key(yaml, name, value, &policy->key, AS_WRITE_SEND_KEY)) {
			return false;
		}
		as_assign_send_key(operate_section, name, value, policy->key, &base->operate.key);
		as_assign_send_key(apply_section, name, value, policy->key, &base->apply.key);
		as_assign_send_key(remove_section, name, value, policy->key, &base->remove.key);
		return true;
	}

	if (strcmp(name, "durable_delete") == 0) {
		if (!as_parse_bool(yaml, name, value, &policy->durable_delete, AS_WRITE_DURABLE_DELETE)) {
			return false;
		}
		as_assign_bool(operate_section, name, value, policy->durable_delete, &base->operate.durable_delete);
		as_assign_bool(apply_section, name, value, policy->durable_delete, &base->apply.durable_delete);
		as_assign_bool(remove_section, name, value, policy->durable_delete, &base->remove.durable_delete);
		return true;
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
		return as_parse_replica(yaml, name, value, &policy->replica, AS_SCAN_REPLICA);
	}

	if (strcmp(name, "socket_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.socket_timeout, AS_SCAN_SOCKET_TIMEOUT);
	}

	if (strcmp(name, "total_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.total_timeout, AS_SCAN_TOTAL_TIMEOUT);
	}

	if (strcmp(name, "max_retries") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.max_retries, AS_SCAN_MAX_RETRIES);
	}

	if (strcmp(name, "sleep_between_retries") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.sleep_between_retries, AS_SCAN_SLEEP_BETWEEN_RETRIES);
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
		return as_parse_replica(yaml, name, value, &policy->replica, AS_QUERY_REPLICA);
	}

	if (strcmp(name, "socket_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.socket_timeout, AS_QUERY_SOCKET_TIMEOUT);
	}

	if (strcmp(name, "total_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.total_timeout, AS_QUERY_TOTAL_TIMEOUT);
	}

	if (strcmp(name, "max_retries") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.max_retries, AS_QUERY_MAX_RETRIES);
	}

	if (strcmp(name, "sleep_between_retries") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.sleep_between_retries, AS_QUERY_SLEEP_BETWEEN_RETRIES);
	}

	if (strcmp(name, "info_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->info_timeout, AS_QUERY_INFO_TIMEOUT);
	}

	if (strcmp(name, "expected_duration") == 0) {
		return as_parse_expected_duration(yaml, name, value, &policy->expected_duration, AS_QUERY_EXPECTED_DURATION);
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
as_parse_batch_shared(
	as_yaml* yaml, const char* name, const char* value, as_policy_batch* policy, uint32_t offset
	)
{
	if (strcmp(name, "read_mode_ap") == 0) {
		return as_parse_read_mode_ap(yaml, name, value, &policy->read_mode_ap, offset + AS_BATCH_READ_MODE_AP);
	}

	if (strcmp(name, "read_mode_sc") == 0) {
		return as_parse_read_mode_sc(yaml, name, value, &policy->read_mode_sc, offset + AS_BATCH_READ_MODE_SC);
	}

	if (strcmp(name, "replica") == 0) {
		return as_parse_replica(yaml, name, value, &policy->replica, offset + AS_BATCH_REPLICA);
	}

	if (strcmp(name, "socket_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.socket_timeout, offset + AS_BATCH_SOCKET_TIMEOUT);
	}

	if (strcmp(name, "total_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.total_timeout, offset + AS_BATCH_TOTAL_TIMEOUT);
	}

	if (strcmp(name, "max_retries") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.max_retries, offset + AS_BATCH_MAX_RETRIES);
	}

	if (strcmp(name, "sleep_between_retries") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.sleep_between_retries, offset + AS_BATCH_SLEEP_BETWEEN_RETRIES);
	}

	if (strcmp(name, "max_concurrent_threads") == 0) {
		return as_parse_max_concurrent_threads(yaml, name, value, &policy->concurrent, offset + AS_BATCH_CONCURRENT);
	}

	if (strcmp(name, "allow_inline") == 0) {
		return as_parse_bool(yaml, name, value, &policy->allow_inline, offset + AS_BATCH_ALLOW_INLINE);
	}

	if (strcmp(name, "allow_inline_ssd") == 0) {
		return as_parse_bool(yaml, name, value, &policy->allow_inline_ssd, offset + AS_BATCH_ALLOW_INLINE_SSD);
	}

	if (strcmp(name, "respond_all_keys") == 0) {
		return as_parse_bool(yaml, name, value, &policy->respond_all_keys, offset + AS_BATCH_RESPOND_ALL_KEYS);
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
	return as_parse_batch_shared(yaml, name, value, &base->batch, AS_BATCH_PARENT_READ);
}

static bool
as_parse_batch_write(as_yaml* yaml, const char* name, const char* value, as_policies* base)
{
	as_policy_batch_write* bw = &base->batch_write;
	yaml->name = "batch_write";

	if (strcmp(name, "durable_delete") == 0) {
		return as_parse_bool(yaml, name, value, &bw->durable_delete, AS_BATCH_WRITE_DURABLE_DELETE);
	}

	if (strcmp(name, "send_key") == 0) {
		return as_parse_send_key(yaml, name, value, &bw->key, AS_BATCH_WRITE_SEND_KEY);
	}

	return as_parse_batch_shared(yaml, name, value, &base->batch_parent_write, AS_BATCH_PARENT_WRITE);
}

static bool
as_parse_batch_udf(as_yaml* yaml, const char* name, const char* value, as_policies* base)
{
	as_policy_batch_apply* policy = &base->batch_apply;
	yaml->name = "batch_udf";

	if (strcmp(name, "durable_delete") == 0) {
		return as_parse_bool(yaml, name, value, &policy->durable_delete, AS_BATCH_UDF_DURABLE_DELETE);
	}

	if (strcmp(name, "send_key") == 0) {
		return as_parse_send_key(yaml, name, value, &policy->key, AS_BATCH_UDF_SEND_KEY);
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
		return as_parse_bool(yaml, name, value, &policy->durable_delete, AS_BATCH_DELETE_DURABLE_DELETE);
	}

	if (strcmp(name, "send_key") == 0) {
		return as_parse_send_key(yaml, name, value, &policy->key, AS_BATCH_DELETE_SEND_KEY);
	}

	as_log_info("Unexpected field: %s.%s", yaml->name, name);
	return true;
}

static bool
as_parse_txn_verify(as_yaml* yaml, const char* name, const char* value, as_policies* base)
{
	yaml->name = "txn_verify";
	return as_parse_batch_shared(yaml, name, value, &base->txn_verify, AS_TXN_VERIFY);
}

static bool
as_parse_txn_roll(as_yaml* yaml, const char* name, const char* value, as_policies* base)
{
	yaml->name = "txn_roll";
	return as_parse_batch_shared(yaml, name, value, &base->txn_roll, AS_TXN_ROLL);
}

static bool
as_parse_labels(as_yaml* yaml, as_metrics_policy* policy, uint32_t field)
{
	// Original labels will still exist and they will be restored if
	// there is an error while parsing the file.
	policy->labels = NULL;

	char name[256];
	char value[256];

	while (as_parse_scalar(yaml, name, sizeof(name))) {
		if (!as_parse_scalar(yaml, value, sizeof(value))) {
			as_metrics_policy_destroy_labels(policy);
			return false;
		}
		as_metrics_policy_add_label(policy, name, value);
	}

	as_string_builder sb;
	as_string_builder_inita(&sb, 512, false);

	as_string_builder_append(&sb, "Set ");
	as_string_builder_append(&sb, yaml->name);
	as_string_builder_append_char(&sb, '.');
	as_string_builder_append(&sb, "labels");
	as_string_builder_append(&sb, " = [");

	as_vector* list = policy->labels;

	if (list) {
		for (uint32_t i = 0; i < list->size; i++) {
			as_metrics_label* label = as_vector_get(list, i);

			if (i > 0) {
				as_string_builder_append_char(&sb, ',');
			}
			as_string_builder_append_char(&sb, '[');
			as_string_builder_append(&sb, label->name);
			as_string_builder_append_char(&sb, ',');
			as_string_builder_append(&sb, label->value);
			as_string_builder_append_char(&sb, ']');
		}
	}

	as_string_builder_append_char(&sb, ']');
	as_log_info(sb.data);
	as_field_set(yaml->bitmap, field);
	return true;
}

static bool
as_parse_metrics(as_yaml* yaml, const char* name, const char* value, as_policies* base)
{
	as_metrics_policy* policy = &base->metrics;
	yaml->name = "metrics";

	if (strcmp(name, "enable") == 0) {
		return as_parse_bool(yaml, name, value, &policy->enable, AS_METRICS_ENABLE);
	}

	if (strcmp(name, "latency_columns") == 0) {
		return as_parse_uint8(yaml, name, value, &policy->latency_columns, AS_METRICS_LATENCY_COLUMNS);
	}

	if (strcmp(name, "latency_shift") == 0) {
		return as_parse_uint8(yaml, name, value, &policy->latency_shift, AS_METRICS_LATENCY_SHIFT);
	}

	if (strcmp(name, "labels") == 0) {
		return as_parse_labels(yaml, policy, AS_METRICS_LABELS);
	}

	as_log_info("Unexpected field: %s.%s", yaml->name, name);
	return true;
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

		switch (yaml->event.type) {
			case YAML_SCALAR_EVENT: {
				char* value = (char*)yaml->event.data.scalar.value;

				rv = fn(yaml, name, value, base);
				yaml_event_delete(&yaml->event);
				break;
			}

			case YAML_SEQUENCE_START_EVENT: {
				yaml_event_delete(&yaml->event);
				rv = fn(yaml, name, "", base);
				break;
			}

			case YAML_MAPPING_START_EVENT: {
				yaml_event_delete(&yaml->event);
				rv = fn(yaml, name, "", base);
				break;
			}

			default: {
				as_expected_error(yaml, YAML_SCALAR_EVENT);
				yaml_event_delete(&yaml->event);
				rv = false;
				break;
			}
		}

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

			if (strcmp(name, "config_interval") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->config_provider.interval, AS_CONFIG_INTERVAL);
			}
			else if (strcmp(name, "max_connections_per_node") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->max_conns_per_node, AS_MAX_CONNS_PER_NODE);
			}
			else if (strcmp(name, "min_connections_per_node") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->min_conns_per_node, AS_MIN_CONNS_PER_NODE);
			}
			else if (strcmp(name, "async_max_connections_per_node") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->async_max_conns_per_node, AS_ASYNC_MAX_CONNS_PER_NODE);
			}
			else if (strcmp(name, "async_min_connections_per_node") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->async_min_conns_per_node, AS_ASYNC_MIN_CONNS_PER_NODE);
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

			if (strcmp(name, "app_id") == 0) {
				rv = as_parse_string(yaml, name, value, &yaml->config->app_id, AS_APP_ID);
			}
			else if (strcmp(name, "timeout") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->conn_timeout_ms, AS_TEND_TIMEOUT);
			}
			else if (strcmp(name, "max_error_rate") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->max_error_rate, AS_MAX_ERROR_RATE);
			}
			else if (strcmp(name, "error_rate_window") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->error_rate_window, AS_ERROR_RATE_WINDOW);
			}
			else if (strcmp(name, "login_timeout") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->login_timeout_ms, AS_LOGIN_TIMEOUT);
			}
			else if (strcmp(name, "max_socket_idle") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->max_socket_idle, AS_MAX_SOCKET_IDLE);
			}
			else if (strcmp(name, "tend_interval") == 0) {
				rv = as_parse_uint32_range(yaml, name, value, AS_TEND_INTERVAL_MIN, UINT32_MAX,
					&yaml->config->tender_interval, AS_TEND_INTERVAL);
			}
			else if (strcmp(name, "fail_if_not_connected") == 0) {
				rv = as_parse_bool(yaml, name, value, &yaml->config->fail_if_not_connected, AS_FAIL_IF_NOT_CONNECTED);
			}
			else if (strcmp(name, "use_service_alternative") == 0) {
				rv = as_parse_bool(yaml, name, value, &yaml->config->use_services_alternate, AS_USE_SERVICE_ALTERNATIVE);
			}
			else if (strcmp(name, "rack_aware") == 0) {
				rv = as_parse_bool(yaml, name, value, &yaml->config->rack_aware, AS_RACK_AWARE);
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
				rv = as_parse_vector_int32(yaml, name, &yaml->config->rack_ids, AS_RACK_IDS);
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
	bool rv;

	while (as_parse_scalar(yaml, name, sizeof(name))) {
		if (! yaml->init) {
			// Do not process static fields on a dynamic update.
			rv = as_skip_section(yaml);
		}
		else if (strcmp(name, "client") == 0) {
			rv = as_parse_static_client(yaml);
		}
		else {
			as_log_info("Unexpected section: %s", name);
			rv = as_skip_section(yaml);
		}

		if (!rv) {
			return false;
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
	bool rv;

	while (as_parse_scalar(yaml, name, sizeof(name))) {
		if (strcmp(name, "client") == 0) {
			rv = as_parse_dynamic_client(yaml);
		}
		else if (strcmp(name, "read") == 0) {
			rv = as_parse_policy(yaml, as_parse_read);
		}
		else if (strcmp(name, "write") == 0) {
			rv = as_parse_policy(yaml, as_parse_write);
		}
		else if (strcmp(name, "query") == 0) {
			rv = as_parse_policy(yaml, as_parse_query);
		}
		else if (strcmp(name, "scan") == 0) {
			rv = as_parse_policy(yaml, as_parse_scan);
		}
		else if (strcmp(name, "batch_read") == 0) {
			rv = as_parse_policy(yaml, as_parse_batch_read);
		}
		else if (strcmp(name, "batch_write") == 0) {
			rv = as_parse_policy(yaml, as_parse_batch_write);
		}
		else if (strcmp(name, "batch_udf") == 0) {
			rv = as_parse_policy(yaml, as_parse_batch_udf);
		}
		else if (strcmp(name, "batch_delete") == 0) {
			rv = as_parse_policy(yaml, as_parse_batch_delete);
		}
		else if (strcmp(name, "txn_verify") == 0) {
			rv = as_parse_policy(yaml, as_parse_txn_verify);
		}
		else if (strcmp(name, "txn_roll") == 0) {
			rv = as_parse_policy(yaml, as_parse_txn_roll);
		}
		else if (strcmp(name, "metrics") == 0) {
			rv = as_parse_policy(yaml, as_parse_metrics);
		}
		else {
			as_log_info("Unexpected section: %s", name);
			rv = as_skip_section(yaml);
		}

		if (!rv) {
			return false;
		}
	}
	return true;
}

static bool
as_parse_version(as_yaml* yaml)
{
	char version[256];

	if (!as_parse_scalar(yaml, version, sizeof(version))) {
		return false;
	}

	if (strcmp(version, "1.0.0") != 0) {
		// Log warning and continue parsing on version mismatch.
		as_log_warn("Unexpected dynamic config file version: %s expected: 1.0.0", version);
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
	bool rv;

	while (as_parse_scalar(yaml, name, sizeof(name))) {
		if (strcmp(name, "version") == 0) {
			rv = as_parse_version(yaml);
		}
		else if (strcmp(name, "static") == 0) {
			rv = as_parse_static(yaml);
		}
		else if (strcmp(name, "dynamic") == 0) {
			rv = as_parse_dynamic(yaml);
		}
		else {
			as_log_info("Unexpected section: %s", name);
			rv = as_skip_section(yaml);
		}

		if (!rv) {
			return false;
		}
	}
	return true;
}

#if 0
// Used for debugging purposes.
static bool
parse_debug(yaml_parser_t* parser)
{
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
as_config_file_read(aerospike* as, as_config* config, uint8_t* bitmap, bool init, as_error* err)
{
	as_error_reset(err);

	const char* path = as->config.config_provider.path;
	FILE* fp = fopen(path, "r");

	if (!fp) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to open: %s", path);
	}

	as_yaml yaml;
	yaml.bitmap = bitmap;
	yaml.config = config;
	yaml.init = init;

	if (!yaml_parser_initialize(&yaml.parser)) {
		fclose(fp);
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to initialize yaml parser");
	}

	yaml_parser_set_input_file(&yaml.parser, fp);

	bool rv = as_parse_yaml(&yaml);
	//bool rv = parse_debug(&yaml.parser);

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
as_rack_ids_copy(as_vector* src, as_vector** trg)
{
	as_vector* rack_ids = as_vector_create(sizeof(int), src->size);

	for (uint32_t i = 0; i < src->size; i++) {
		int id = *(int*)as_vector_get(src, i);
		as_vector_append(rack_ids, &id);
	}
	*trg = rack_ids;
}

static void
as_cluster_update_policies(as_policies* orig, as_policies* src, as_policies* trg, uint8_t* bitmap)
{
	// Copy new policy values to the cluster one field at a time.
	trg->read.base.socket_timeout = as_field_is_set(bitmap, AS_READ_SOCKET_TIMEOUT)?
		src->read.base.socket_timeout : orig->read.base.socket_timeout;
	trg->read.base.total_timeout = as_field_is_set(bitmap, AS_READ_TOTAL_TIMEOUT)?
		src->read.base.total_timeout : orig->read.base.total_timeout;
	trg->read.base.max_retries = as_field_is_set(bitmap, AS_READ_MAX_RETRIES)?
		src->read.base.max_retries : orig->read.base.max_retries;
	trg->read.base.sleep_between_retries = as_field_is_set(bitmap, AS_READ_SLEEP_BETWEEN_RETRIES)?
		src->read.base.sleep_between_retries : orig->read.base.sleep_between_retries;
	trg->read.read_mode_ap = as_field_is_set(bitmap, AS_READ_READ_MODE_AP)?
		src->read.read_mode_ap : orig->read.read_mode_ap;
	trg->read.read_mode_sc = as_field_is_set(bitmap, AS_READ_READ_MODE_SC)?
		src->read.read_mode_sc : orig->read.read_mode_sc;
	trg->read.replica = as_field_is_set(bitmap, AS_READ_REPLICA)?
		src->read.replica : orig->read.replica;

	trg->write.base.socket_timeout = as_field_is_set(bitmap, AS_WRITE_SOCKET_TIMEOUT)?
		src->write.base.socket_timeout : orig->write.base.socket_timeout;
	trg->write.base.total_timeout = as_field_is_set(bitmap, AS_WRITE_TOTAL_TIMEOUT)?
		src->write.base.total_timeout : orig->write.base.total_timeout;
	trg->write.base.max_retries = as_field_is_set(bitmap, AS_WRITE_MAX_RETRIES)?
		src->write.base.max_retries : orig->write.base.max_retries;
	trg->write.base.sleep_between_retries = as_field_is_set(bitmap, AS_WRITE_SLEEP_BETWEEN_RETRIES)?
		src->write.base.sleep_between_retries : orig->write.base.sleep_between_retries;
	trg->write.replica = as_field_is_set(bitmap, AS_WRITE_REPLICA)?
		src->write.replica : orig->write.replica;
	trg->write.durable_delete = as_field_is_set(bitmap, AS_WRITE_DURABLE_DELETE)?
		src->write.durable_delete : orig->write.durable_delete;
	trg->write.key = as_field_is_set(bitmap, AS_WRITE_SEND_KEY)?
		src->write.key : orig->write.key;

	trg->operate.base.socket_timeout = as_field_is_set(bitmap, AS_WRITE_SOCKET_TIMEOUT)?
		src->operate.base.socket_timeout : orig->operate.base.socket_timeout;
	trg->operate.base.total_timeout = as_field_is_set(bitmap, AS_WRITE_TOTAL_TIMEOUT)?
		src->operate.base.total_timeout : orig->operate.base.total_timeout;
	trg->operate.base.max_retries = as_field_is_set(bitmap, AS_WRITE_MAX_RETRIES)?
		src->operate.base.max_retries : orig->operate.base.max_retries;
	trg->operate.base.sleep_between_retries = as_field_is_set(bitmap, AS_WRITE_SLEEP_BETWEEN_RETRIES)?
		src->operate.base.sleep_between_retries : orig->operate.base.sleep_between_retries;
	trg->operate.replica = as_field_is_set(bitmap, AS_WRITE_REPLICA)?
		src->operate.replica : orig->operate.replica;
	trg->operate.durable_delete = as_field_is_set(bitmap, AS_WRITE_DURABLE_DELETE)?
		src->operate.durable_delete : orig->operate.durable_delete;
	trg->operate.key = as_field_is_set(bitmap, AS_WRITE_SEND_KEY)?
		src->operate.key : orig->operate.key;
	trg->operate.read_mode_ap = as_field_is_set(bitmap, AS_READ_READ_MODE_AP)?
		src->operate.read_mode_ap : orig->operate.read_mode_ap;
	trg->operate.read_mode_sc = as_field_is_set(bitmap, AS_READ_READ_MODE_SC)?
		src->operate.read_mode_sc : orig->operate.read_mode_sc;

	trg->apply.base.socket_timeout = as_field_is_set(bitmap, AS_WRITE_SOCKET_TIMEOUT)?
		src->apply.base.socket_timeout : orig->apply.base.socket_timeout;
	trg->apply.base.total_timeout = as_field_is_set(bitmap, AS_WRITE_TOTAL_TIMEOUT)?
		src->apply.base.total_timeout : orig->apply.base.total_timeout;
	trg->apply.base.max_retries = as_field_is_set(bitmap, AS_WRITE_MAX_RETRIES)?
		src->apply.base.max_retries : orig->apply.base.max_retries;
	trg->apply.base.sleep_between_retries = as_field_is_set(bitmap, AS_WRITE_SLEEP_BETWEEN_RETRIES)?
		src->apply.base.sleep_between_retries : orig->apply.base.sleep_between_retries;
	trg->apply.replica = as_field_is_set(bitmap, AS_WRITE_REPLICA)?
		src->apply.replica : orig->apply.replica;
	trg->apply.durable_delete = as_field_is_set(bitmap, AS_WRITE_DURABLE_DELETE)?
		src->apply.durable_delete : orig->apply.durable_delete;
	trg->apply.key = as_field_is_set(bitmap, AS_WRITE_SEND_KEY)?
		src->apply.key : orig->apply.key;

	trg->remove.base.socket_timeout = as_field_is_set(bitmap, AS_WRITE_SOCKET_TIMEOUT)?
		src->remove.base.socket_timeout : orig->remove.base.socket_timeout;
	trg->remove.base.total_timeout = as_field_is_set(bitmap, AS_WRITE_TOTAL_TIMEOUT)?
		src->remove.base.total_timeout : orig->remove.base.total_timeout;
	trg->remove.base.max_retries = as_field_is_set(bitmap, AS_WRITE_MAX_RETRIES)?
		src->remove.base.max_retries : orig->remove.base.max_retries;
	trg->remove.base.sleep_between_retries = as_field_is_set(bitmap, AS_WRITE_SLEEP_BETWEEN_RETRIES)?
		src->remove.base.sleep_between_retries : orig->remove.base.sleep_between_retries;
	trg->remove.replica = as_field_is_set(bitmap, AS_WRITE_REPLICA)?
		src->remove.replica : orig->remove.replica;
	trg->remove.durable_delete = as_field_is_set(bitmap, AS_WRITE_DURABLE_DELETE)?
		src->remove.durable_delete : orig->remove.durable_delete;
	trg->remove.key = as_field_is_set(bitmap, AS_WRITE_SEND_KEY)?
		src->remove.key : orig->remove.key;

	trg->scan.base.socket_timeout = as_field_is_set(bitmap, AS_SCAN_SOCKET_TIMEOUT)?
		src->scan.base.socket_timeout : orig->scan.base.socket_timeout;
	trg->scan.base.total_timeout = as_field_is_set(bitmap, AS_SCAN_TOTAL_TIMEOUT)?
		src->scan.base.total_timeout : orig->scan.base.total_timeout;
	trg->scan.base.max_retries = as_field_is_set(bitmap, AS_SCAN_MAX_RETRIES)?
		src->scan.base.max_retries : orig->scan.base.max_retries;
	trg->scan.base.sleep_between_retries = as_field_is_set(bitmap, AS_SCAN_SLEEP_BETWEEN_RETRIES)?
		src->scan.base.sleep_between_retries : orig->scan.base.sleep_between_retries;
	trg->scan.replica = as_field_is_set(bitmap, AS_SCAN_REPLICA)?
		src->scan.replica : orig->scan.replica;

	trg->query.base.socket_timeout = as_field_is_set(bitmap, AS_QUERY_SOCKET_TIMEOUT)?
		src->query.base.socket_timeout : orig->query.base.socket_timeout;
	trg->query.base.total_timeout = as_field_is_set(bitmap, AS_QUERY_TOTAL_TIMEOUT)?
		src->query.base.total_timeout : orig->query.base.total_timeout;
	trg->query.base.max_retries = as_field_is_set(bitmap, AS_QUERY_MAX_RETRIES)?
		src->query.base.max_retries : orig->query.base.max_retries;
	trg->query.base.sleep_between_retries = as_field_is_set(bitmap, AS_QUERY_SLEEP_BETWEEN_RETRIES)?
		src->query.base.sleep_between_retries : orig->query.base.sleep_between_retries;
	trg->query.replica = as_field_is_set(bitmap, AS_QUERY_REPLICA)?
		src->query.replica : orig->query.replica;
	trg->query.info_timeout = as_field_is_set(bitmap, AS_QUERY_INFO_TIMEOUT)?
		src->query.info_timeout : orig->query.info_timeout;
	trg->query.expected_duration = as_field_is_set(bitmap, AS_QUERY_EXPECTED_DURATION)?
		src->query.expected_duration : orig->query.expected_duration;

	trg->batch.base.socket_timeout = as_field_is_set(bitmap, AS_BATCH_PARENT_READ + AS_BATCH_SOCKET_TIMEOUT)?
		src->batch.base.socket_timeout : orig->batch.base.socket_timeout;
	trg->batch.base.total_timeout = as_field_is_set(bitmap, AS_BATCH_PARENT_READ + AS_BATCH_TOTAL_TIMEOUT)?
		src->batch.base.total_timeout : orig->batch.base.total_timeout;
	trg->batch.base.max_retries = as_field_is_set(bitmap, AS_BATCH_PARENT_READ + AS_BATCH_MAX_RETRIES)?
		src->batch.base.max_retries : orig->batch.base.max_retries;
	trg->batch.base.sleep_between_retries = as_field_is_set(bitmap, AS_BATCH_PARENT_READ + AS_BATCH_SLEEP_BETWEEN_RETRIES)?
		src->batch.base.sleep_between_retries : orig->batch.base.sleep_between_retries;
	trg->batch.read_mode_ap = as_field_is_set(bitmap, AS_BATCH_PARENT_READ + AS_BATCH_READ_MODE_AP)?
		src->batch.read_mode_ap : orig->batch.read_mode_ap;
	trg->batch.read_mode_sc = as_field_is_set(bitmap, AS_BATCH_PARENT_READ + AS_BATCH_READ_MODE_SC)?
		src->batch.read_mode_sc : orig->batch.read_mode_sc;
	trg->batch.replica = as_field_is_set(bitmap, AS_BATCH_PARENT_READ + AS_BATCH_REPLICA)?
		src->batch.replica : orig->batch.replica;
	trg->batch.concurrent = as_field_is_set(bitmap, AS_BATCH_PARENT_READ + AS_BATCH_CONCURRENT)?
		src->batch.concurrent : orig->batch.concurrent;
	trg->batch.allow_inline = as_field_is_set(bitmap, AS_BATCH_PARENT_READ + AS_BATCH_ALLOW_INLINE)?
		src->batch.allow_inline : orig->batch.allow_inline;
	trg->batch.allow_inline_ssd = as_field_is_set(bitmap, AS_BATCH_PARENT_READ + AS_BATCH_ALLOW_INLINE_SSD)?
		src->batch.allow_inline_ssd : orig->batch.allow_inline_ssd;
	trg->batch.respond_all_keys = as_field_is_set(bitmap, AS_BATCH_PARENT_READ + AS_BATCH_RESPOND_ALL_KEYS)?
		src->batch.respond_all_keys : orig->batch.respond_all_keys;

	trg->batch_parent_write.base.socket_timeout = as_field_is_set(bitmap, AS_BATCH_PARENT_WRITE + AS_BATCH_SOCKET_TIMEOUT)?
		src->batch_parent_write.base.socket_timeout : orig->batch_parent_write.base.socket_timeout;
	trg->batch_parent_write.base.total_timeout = as_field_is_set(bitmap, AS_BATCH_PARENT_WRITE + AS_BATCH_TOTAL_TIMEOUT)?
		src->batch_parent_write.base.total_timeout : orig->batch_parent_write.base.total_timeout;
	trg->batch_parent_write.base.max_retries = as_field_is_set(bitmap, AS_BATCH_PARENT_WRITE + AS_BATCH_MAX_RETRIES)?
		src->batch_parent_write.base.max_retries : orig->batch_parent_write.base.max_retries;
	trg->batch_parent_write.base.sleep_between_retries =
		as_field_is_set(bitmap, AS_BATCH_PARENT_WRITE + AS_BATCH_SLEEP_BETWEEN_RETRIES)?
		src->batch_parent_write.base.sleep_between_retries : orig->batch_parent_write.base.sleep_between_retries;
	trg->batch_parent_write.read_mode_ap = as_field_is_set(bitmap, AS_BATCH_PARENT_WRITE + AS_BATCH_READ_MODE_AP)?
		src->batch_parent_write.read_mode_ap : orig->batch_parent_write.read_mode_ap;
	trg->batch_parent_write.read_mode_sc = as_field_is_set(bitmap, AS_BATCH_PARENT_WRITE + AS_BATCH_READ_MODE_SC)?
		src->batch_parent_write.read_mode_sc : orig->batch_parent_write.read_mode_sc;
	trg->batch_parent_write.replica = as_field_is_set(bitmap, AS_BATCH_PARENT_WRITE + AS_BATCH_REPLICA)?
		src->batch_parent_write.replica : orig->batch_parent_write.replica;
	trg->batch_parent_write.concurrent = as_field_is_set(bitmap, AS_BATCH_PARENT_WRITE + AS_BATCH_CONCURRENT)?
		src->batch_parent_write.concurrent : orig->batch_parent_write.concurrent;
	trg->batch_parent_write.allow_inline = as_field_is_set(bitmap, AS_BATCH_PARENT_WRITE + AS_BATCH_ALLOW_INLINE)?
		src->batch_parent_write.allow_inline : orig->batch_parent_write.allow_inline;
	trg->batch_parent_write.allow_inline_ssd = as_field_is_set(bitmap, AS_BATCH_PARENT_WRITE + AS_BATCH_ALLOW_INLINE_SSD)?
		src->batch_parent_write.allow_inline_ssd : orig->batch_parent_write.allow_inline_ssd;
	trg->batch_parent_write.respond_all_keys = as_field_is_set(bitmap, AS_BATCH_PARENT_WRITE + AS_BATCH_RESPOND_ALL_KEYS)?
		src->batch_parent_write.respond_all_keys : orig->batch_parent_write.respond_all_keys;

	trg->batch_write.durable_delete = as_field_is_set(bitmap, AS_BATCH_WRITE_DURABLE_DELETE)?
		src->batch_write.durable_delete : orig->batch_write.durable_delete;
	trg->batch_write.key = as_field_is_set(bitmap, AS_BATCH_WRITE_SEND_KEY)?
		src->batch_write.key : orig->batch_write.key;

	trg->batch_apply.durable_delete = as_field_is_set(bitmap, AS_BATCH_UDF_DURABLE_DELETE)?
		src->batch_apply.durable_delete : orig->batch_apply.durable_delete;
	trg->batch_apply.key = as_field_is_set(bitmap, AS_BATCH_UDF_SEND_KEY)?
		src->batch_apply.key : orig->batch_apply.key;

	trg->batch_remove.durable_delete = as_field_is_set(bitmap, AS_BATCH_DELETE_DURABLE_DELETE)?
		src->batch_remove.durable_delete : orig->batch_remove.durable_delete;
	trg->batch_remove.key = as_field_is_set(bitmap, AS_BATCH_DELETE_SEND_KEY)?
		src->batch_remove.key : orig->batch_remove.key;

	trg->txn_verify.base.socket_timeout = as_field_is_set(bitmap, AS_TXN_VERIFY + AS_BATCH_SOCKET_TIMEOUT)?
		src->txn_verify.base.socket_timeout : orig->txn_verify.base.socket_timeout;
	trg->txn_verify.base.total_timeout = as_field_is_set(bitmap, AS_TXN_VERIFY + AS_BATCH_TOTAL_TIMEOUT)?
		src->txn_verify.base.total_timeout : orig->txn_verify.base.total_timeout;
	trg->txn_verify.base.max_retries = as_field_is_set(bitmap, AS_TXN_VERIFY + AS_BATCH_MAX_RETRIES)?
		src->txn_verify.base.max_retries : orig->txn_verify.base.max_retries;
	trg->txn_verify.base.sleep_between_retries = as_field_is_set(bitmap, AS_TXN_VERIFY + AS_BATCH_SLEEP_BETWEEN_RETRIES)?
		src->txn_verify.base.sleep_between_retries : orig->txn_verify.base.sleep_between_retries;
	trg->txn_verify.read_mode_ap = as_field_is_set(bitmap, AS_TXN_VERIFY + AS_BATCH_READ_MODE_AP)?
		src->txn_verify.read_mode_ap : orig->txn_verify.read_mode_ap;
	trg->txn_verify.read_mode_sc = as_field_is_set(bitmap, AS_TXN_VERIFY + AS_BATCH_READ_MODE_SC)?
		src->txn_verify.read_mode_sc : orig->txn_verify.read_mode_sc;
	trg->txn_verify.replica = as_field_is_set(bitmap, AS_TXN_VERIFY + AS_BATCH_REPLICA)?
		src->txn_verify.replica : orig->txn_verify.replica;
	trg->txn_verify.concurrent = as_field_is_set(bitmap, AS_TXN_VERIFY + AS_BATCH_CONCURRENT)?
		src->txn_verify.concurrent : orig->txn_verify.concurrent;
	trg->txn_verify.allow_inline = as_field_is_set(bitmap, AS_TXN_VERIFY + AS_BATCH_ALLOW_INLINE)?
		src->txn_verify.allow_inline : orig->txn_verify.allow_inline;
	trg->txn_verify.allow_inline_ssd = as_field_is_set(bitmap, AS_TXN_VERIFY + AS_BATCH_ALLOW_INLINE_SSD)?
		src->txn_verify.allow_inline_ssd : orig->txn_verify.allow_inline_ssd;
	trg->txn_verify.respond_all_keys = as_field_is_set(bitmap, AS_TXN_VERIFY + AS_BATCH_RESPOND_ALL_KEYS)?
		src->txn_verify.respond_all_keys : orig->txn_verify.respond_all_keys;

	trg->txn_roll.base.socket_timeout = as_field_is_set(bitmap, AS_TXN_ROLL + AS_BATCH_SOCKET_TIMEOUT)?
		src->txn_roll.base.socket_timeout : orig->txn_roll.base.socket_timeout;
	trg->txn_roll.base.total_timeout = as_field_is_set(bitmap, AS_TXN_ROLL + AS_BATCH_TOTAL_TIMEOUT)?
		src->txn_roll.base.total_timeout : orig->txn_roll.base.total_timeout;
	trg->txn_roll.base.max_retries = as_field_is_set(bitmap, AS_TXN_ROLL + AS_BATCH_MAX_RETRIES)?
		src->txn_roll.base.max_retries : orig->txn_roll.base.max_retries;
	trg->txn_roll.base.sleep_between_retries = as_field_is_set(bitmap, AS_TXN_ROLL + AS_BATCH_SLEEP_BETWEEN_RETRIES)?
		src->txn_roll.base.sleep_between_retries : orig->txn_roll.base.sleep_between_retries;
	trg->txn_roll.read_mode_ap = as_field_is_set(bitmap, AS_TXN_ROLL + AS_BATCH_READ_MODE_AP)?
		src->txn_roll.read_mode_ap : orig->txn_roll.read_mode_ap;
	trg->txn_roll.read_mode_sc = as_field_is_set(bitmap, AS_TXN_ROLL + AS_BATCH_READ_MODE_SC)?
		src->txn_roll.read_mode_sc : orig->txn_roll.read_mode_sc;
	trg->txn_roll.replica = as_field_is_set(bitmap, AS_TXN_ROLL + AS_BATCH_REPLICA)?
		src->txn_roll.replica : orig->txn_roll.replica;
	trg->txn_roll.concurrent = as_field_is_set(bitmap, AS_TXN_ROLL + AS_BATCH_CONCURRENT)?
		src->txn_roll.concurrent : orig->txn_roll.concurrent;
	trg->txn_roll.allow_inline = as_field_is_set(bitmap, AS_TXN_ROLL + AS_BATCH_ALLOW_INLINE)?
		src->txn_roll.allow_inline : orig->txn_roll.allow_inline;
	trg->txn_roll.allow_inline_ssd = as_field_is_set(bitmap, AS_TXN_ROLL + AS_BATCH_ALLOW_INLINE_SSD)?
		src->txn_roll.allow_inline_ssd : orig->txn_roll.allow_inline_ssd;
	trg->txn_roll.respond_all_keys = as_field_is_set(bitmap, AS_TXN_ROLL + AS_BATCH_RESPOND_ALL_KEYS)?
		src->txn_roll.respond_all_keys : orig->txn_roll.respond_all_keys;
}

static as_status
as_cluster_update_metrics(
	as_cluster* cluster, as_error* err, as_metrics_policy* orig, as_metrics_policy* src,
	as_metrics_policy* trg, uint8_t* bitmap
	)
{
	pthread_mutex_lock(&cluster->metrics_lock);

	bool enable_metrics = false;

	trg->enable = as_field_is_set(bitmap, AS_METRICS_ENABLE)?
		src->enable : orig->enable;
	trg->latency_columns = as_field_is_set(bitmap, AS_METRICS_LATENCY_COLUMNS)?
		src->latency_columns : orig->latency_columns;
	trg->latency_shift = as_field_is_set(bitmap, AS_METRICS_LATENCY_SHIFT)?
		src->latency_shift : orig->latency_shift;

	if (as_field_is_set(bitmap, AS_METRICS_LABELS)) {
		if (!as_metrics_labels_equal(trg->labels, src->labels)) {
			as_metrics_policy_set_labels(trg, src->labels);
			enable_metrics = true;
		}
		else {
			as_metrics_labels_destroy(src->labels);
		}
		src->labels = NULL;
	}
	else {
		if (!as_metrics_labels_equal(trg->labels, orig->labels)) {
			as_metrics_policy_copy_labels(trg, orig->labels);
			enable_metrics = true;
		}
	}

	as_status status = AEROSPIKE_OK;

	if (trg->enable) {
		if (!cluster->metrics_enabled || !(cluster->metrics_latency_columns == trg->latency_columns &&
			  cluster->metrics_latency_shift == trg->latency_shift)) {
			enable_metrics = true;
		}

		if (enable_metrics) {
			as_log_info("Enable metrics");
			status = as_cluster_enable_metrics(err, cluster, trg);
		}
	}
	else {
		if (cluster->metrics_enabled) {
			as_log_info("Disable metrics");
			status = as_cluster_disable_metrics(err, cluster);
		}
	}

	pthread_mutex_unlock(&cluster->metrics_lock);
	return status;
}

static bool
as_str_eq(const char* s1, const char* s2)
{
	if (s1 == NULL) {
		return s2 == NULL;
	}
	else if (s2 == NULL) {
		return false;
	}
	return strcmp(s1, s2) == 0;
}

static as_status
as_cluster_update(
	aerospike* as, as_config* orig, as_config* src, uint8_t* bitmap, as_error* err
	)
{
	as_config* config = &as->config;
	as_cluster* cluster = as->cluster;

	// Set config.
	config->conn_timeout_ms = as_field_is_set(bitmap, AS_TEND_TIMEOUT)?
		src->conn_timeout_ms : orig->conn_timeout_ms;
	config->max_error_rate = as_field_is_set(bitmap, AS_MAX_ERROR_RATE)?
		src->max_error_rate : orig->max_error_rate;
	config->error_rate_window = as_field_is_set(bitmap, AS_ERROR_RATE_WINDOW)?
		src->error_rate_window : orig->error_rate_window;
	config->login_timeout_ms = as_field_is_set(bitmap, AS_LOGIN_TIMEOUT)?
		src->login_timeout_ms : orig->login_timeout_ms;
	config->max_socket_idle = as_field_is_set(bitmap, AS_MAX_SOCKET_IDLE)?
		src->max_socket_idle : orig->max_socket_idle;
	config->tender_interval = as_field_is_set(bitmap, AS_TEND_INTERVAL)?
		src->tender_interval : orig->tender_interval;
	config->fail_if_not_connected = as_field_is_set(bitmap, AS_FAIL_IF_NOT_CONNECTED)?
		src->fail_if_not_connected : orig->fail_if_not_connected;
	config->use_services_alternate = as_field_is_set(bitmap, AS_USE_SERVICE_ALTERNATIVE)?
		src->use_services_alternate : orig->use_services_alternate;
	config->rack_aware = as_field_is_set(bitmap, AS_RACK_AWARE)?
		src->rack_aware : orig->rack_aware;

	as_config_massage_error_rate(config);

	if (cluster->config_interval < config->tender_interval) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
			"Dynamic config interval %u must be greater or equal to the tend interval %u",
			cluster->config_interval, config->tender_interval);
	}

	if (as_field_is_set(bitmap, AS_RACK_IDS)) {
		if (config->rack_ids != src->rack_ids) {
			// Can be destroyed now since all rack_ids access is done through
			// cluster rack_ids and not config rack_ids.
			as_vector_destroy(config->rack_ids);
			config->rack_ids = src->rack_ids;
		}
	}
	else {
		if (config->rack_ids != orig->rack_ids) {
			as_vector_destroy(config->rack_ids);
			as_rack_ids_copy(orig->rack_ids, &config->rack_ids);
		}
	}

	if (as_field_is_set(bitmap, AS_APP_ID)) {
		if (!as_str_eq(config->app_id, src->app_id)) {
			// app_id is referenced in metrics and user agent on tend connection.
			// metrics is covered under the metrics lock.
			// user agent is referenced is on the tend thread, so an extra lock is not necessary.
			pthread_mutex_lock(&cluster->metrics_lock);
			char* old = config->app_id;
			config->app_id = src->app_id;
			cluster->app_id = config->app_id;
			cf_free(old);
			pthread_mutex_unlock(&cluster->metrics_lock);
		}
		else {
			cf_free(src->app_id);
		}
	}
	else {
		if (!as_str_eq(config->app_id, orig->app_id)) {
			pthread_mutex_lock(&cluster->metrics_lock);
			char* old = config->app_id;
			config->app_id = orig->app_id ? cf_strdup(orig->app_id) : NULL;
			cluster->app_id = config->app_id;
			cf_free(old);
			pthread_mutex_unlock(&cluster->metrics_lock);
		}
	}

	// Set cluster.
	cluster->conn_timeout_ms = (config->conn_timeout_ms == 0) ? 1000 : config->conn_timeout_ms;
	cluster->login_timeout_ms = (config->login_timeout_ms == 0) ? 5000 : config->login_timeout_ms;
	as_cluster_set_max_socket_idle(cluster, config->max_socket_idle);
	cluster->tend_interval = config->tender_interval;
	cluster->fail_if_not_connected = config->fail_if_not_connected;
	cluster->use_services_alternate = config->use_services_alternate;
	cluster->rack_aware = config->rack_aware;
	cluster->error_rate_window = config->error_rate_window;

	if (cluster->max_error_rate != config->max_error_rate) {
		cluster->max_error_rate = config->max_error_rate;

		// Update max_error_rate for all nodes.
		as_nodes* nodes = as_nodes_reserve(cluster);
		for (uint32_t i = 0; i < nodes->size; i++) {
			as_node* node = nodes->array[i];
			node->max_error_rate = config->max_error_rate;
		}
		as_nodes_release(nodes);
	}

	if (!as_rack_ids_equal(cluster, config)) {
		as_vector* old = cluster->rack_ids;
		as_vector* rack_ids;

		// Make full copy of rack_ids;
		as_rack_ids_copy(config->rack_ids, &rack_ids);

		// Update cluster rack_ids.
		as_store_ptr_rls((void**)&cluster->rack_ids, rack_ids);

		// Eventually destroy old cluster rack_ids.
		as_gc_item item;
		item.data = old;
		item.release_fn = (as_release_fn)as_release_rack_ids;
		as_vector_append(cluster->gc, &item);
	}

	as_cluster_update_policies(&orig->policies, &src->policies, &config->policies, bitmap);
	memcpy(as->config_bitmap, bitmap, sizeof(AS_CONFIG_BITMAP_SIZE));

	return as_cluster_update_metrics(cluster, err, &orig->policies.metrics,
		&src->policies.metrics, &config->policies.metrics, bitmap);
}

//---------------------------------
// Functions
//---------------------------------

as_status
as_config_file_init(aerospike* as, as_error* err)
{
	// Make shallow copy of the original user config.
	as->config_orig = cf_malloc(sizeof(as_config));
	memcpy(as->config_orig, &as->config, sizeof(as_config));

	// Initialize field bitmap.
	as->config_bitmap = cf_malloc(AS_CONFIG_BITMAP_SIZE);
	memset(as->config_bitmap, 0, AS_CONFIG_BITMAP_SIZE);

	as_config* config = &as->config;
	as_status status = as_config_file_read(as, config, as->config_bitmap, true, err);

	if (status != AEROSPIKE_OK) {
		// Destroy heap allocated fields if changed before update fails.
		if (config->rack_ids != as->config_orig->rack_ids) {
			as_vector_destroy(config->rack_ids);
		}

		if (config->app_id != as->config_orig->app_id) {
			cf_free(config->app_id);
		}

		if (config->policies.metrics.labels != as->config_orig->policies.metrics.labels) {
			as_metrics_policy_destroy_labels(&config->policies.metrics);
		}

		// Restore original config.
		memcpy(&as->config, as->config_orig, sizeof(as_config));
	}
	return status;
}

as_status
as_config_file_update(aerospike* as, as_error* err)
{
	as_config* current = &as->config;

	as_config config;
	memcpy(&config, current, sizeof(as_config));

	// Start with empty vectors.
	config.app_id = NULL;
	config.rack_ids = NULL;
	config.policies.metrics.labels = NULL;

	uint8_t bitmap[AS_CONFIG_BITMAP_SIZE];
	memset(bitmap, 0, AS_CONFIG_BITMAP_SIZE);

	as_status status = as_config_file_read(as, &config, bitmap, false, err);

	if (status != AEROSPIKE_OK) {
		// Destroy heap allocated fields if changed before update fails.
		if (config.rack_ids) {
			as_vector_destroy(config.rack_ids);
		}

		if (config.app_id) {
			cf_free(config.app_id);
		}

		if (config.policies.metrics.labels) {
			as_metrics_policy_destroy_labels(&config.policies.metrics);
		}
		return status;
	}

	as_log_info("Update dynamic config");
	return as_cluster_update(as, as->config_orig, &config, bitmap, err);
}
