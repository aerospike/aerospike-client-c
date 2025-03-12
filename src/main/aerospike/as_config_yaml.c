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
as_parse_read_mode_ap(as_yaml* yaml, const char* name, const char* value, as_policy_read_mode_ap* read_mode_ap)
{
	if (strcmp(value, "ONE") == 0) {
		*read_mode_ap = AS_POLICY_READ_MODE_AP_ONE;
	}
	else if (strcmp(value, "ALL") == 0) {
		*read_mode_ap = AS_POLICY_READ_MODE_AP_ALL;
	}
	else {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid %s: %s", name, value);
		return false;
	}
	return true;
}

static bool
as_parse_read_mode_sc(as_yaml* yaml, const char* name, const char* value, as_policy_read_mode_sc* read_mode_sc)
{
	if (strcmp(value, "SESSION") == 0) {
		*read_mode_sc = AS_POLICY_READ_MODE_SC_SESSION;
	}
	else if (strcmp(value, "LINEARIZE") == 0) {
		*read_mode_sc = AS_POLICY_READ_MODE_SC_LINEARIZE;
	}
	else if (strcmp(value, "ALLOW_REPLICA") == 0) {
		*read_mode_sc = AS_POLICY_READ_MODE_SC_ALLOW_REPLICA;
	}
	else if (strcmp(value, "ALLOW_UNAVAILABLE") == 0) {
		*read_mode_sc = AS_POLICY_READ_MODE_SC_ALLOW_UNAVAILABLE;
	}
	else {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid %s: %s", name, value);
		return false;
	}
	return true;
}

static bool
as_parse_replica(as_yaml* yaml, const char* name, const char* value, as_policy_replica* replica)
{
	if (strcmp(value, "MASTER") == 0) {
		*replica = AS_POLICY_REPLICA_MASTER;
	}
	else if (strcmp(value, "MASTER_PROLES") == 0) {
		*replica = AS_POLICY_REPLICA_ANY;
	}
	else if (strcmp(value, "SEQUENCE") == 0) {
		*replica = AS_POLICY_REPLICA_SEQUENCE;
	}
	else if (strcmp(value, "PREFER_RACK") == 0) {
		*replica = AS_POLICY_REPLICA_PREFER_RACK;
	}
	else {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid %s: %s", name, value);
		return false;
	}
	return true;
}

static bool
as_parse_expected_duration(as_yaml* yaml, const char* name, const char* value, as_query_duration* expected_duration)
{
	if (strcmp(value, "LONG") == 0) {
		*expected_duration = AS_QUERY_DURATION_LONG;
	}
	else if (strcmp(value, "SHORT") == 0) {
		*expected_duration = AS_QUERY_DURATION_SHORT;
	}
	else if (strcmp(value, "LONG_RELAX_AP") == 0) {
		*expected_duration = AS_QUERY_DURATION_LONG_RELAX_AP;
	}
	else {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid %s: %s", name, value);
		return false;
	}
	return true;
}

static bool
as_parse_send_key(as_yaml* yaml, const char* name, const char* value, as_policy_key* key)
{
	bool send_key;

	if (as_parse_bool(yaml, name, value, &send_key)) {
		*key = send_key ? AS_POLICY_KEY_SEND : AS_POLICY_KEY_DIGEST;
		return true;
	}
	else {
		return false;
	}
}

static bool
as_parse_max_concurrent_threads(as_yaml* yaml, const char* name, const char* value, bool* concurrent)
{
	uint32_t max_concurrent_threads;

	if (as_parse_uint32(yaml, name, value, &max_concurrent_threads)) {
		*concurrent = max_concurrent_threads != 1;
		return true;
	}
	else {
		return false;
	}
}

static bool
as_parse_read(as_yaml* yaml, const char* name, const char* value, as_policies* base)
{
	as_policy_read* policy = &base->read;

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

	// Not supported: connect_timeout, fail_on_filtered_out, timeout_delay
	return true;
}

static bool
as_parse_write(as_yaml* yaml, const char* name, const char* value, as_policies* base)
{
	as_policy_write* policy = &base->write;

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

	// Not supported: connect_timeout, fail_on_filtered_out, timeout_delay
	return true;
}

static bool
as_parse_query(as_yaml* yaml, const char* name, const char* value, as_policies* base)
{
	as_policy_query* policy = &base->query;

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

	// Not supported: connect_timeout, timeout_delay, read_mode_ap, read_mode_sc, include_bin_data
	//                record_queue_size
	return true;
}

static bool
as_parse_scan(as_yaml* yaml, const char* name, const char* value, as_policies* base)
{
	as_policy_scan* policy = &base->scan;

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

	// Not supported: connect_timeout, timeout_delay, read_mode_ap, read_mode_sc, concurrent_nodes
	//                max_concurrent_nodes
	// concurrent_nodes is supported in as_scan, but there no policy defaults for that.
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

	// Not supported: connect_timeout, fail_on_filtered_out, timeout_delay
	return true;
}

static bool
as_parse_batch_read(as_yaml* yaml, const char* name, const char* value, as_policies* base)
{
	return as_parse_batch_shared(yaml, name, value, &base->batch);
}

static bool
as_parse_batch_write(as_yaml* yaml, const char* name, const char* value, as_policies* base)
{
	as_policy_batch_write* bw = &base->batch_write;

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

	if (strcmp(name, "durable_delete") == 0) {
		return as_parse_bool(yaml, name, value, &policy->durable_delete);
	}

	if (strcmp(name, "send_key") == 0) {
		return as_parse_send_key(yaml, name, value, &policy->key);
	}

	return true;
}

static bool
as_parse_batch_delete(as_yaml* yaml, const char* name, const char* value, as_policies* base)
{
	as_policy_batch_remove* policy = &base->batch_remove;

	if (strcmp(name, "durable_delete") == 0) {
		return as_parse_bool(yaml, name, value, &policy->durable_delete);
	}

	if (strcmp(name, "send_key") == 0) {
		return as_parse_send_key(yaml, name, value, &policy->key);
	}

	return true;
}

static bool
as_parse_txn_verify(as_yaml* yaml, const char* name, const char* value, as_policies* base)
{
	return as_parse_batch_shared(yaml, name, value, &base->txn_verify);
}

static bool
as_parse_txn_roll(as_yaml* yaml, const char* name, const char* value, as_policies* base)
{
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
				printf("config_tend_count=%u\n", yaml->config->config_provider.config_tend_count);
			}
			else if (strcmp(name, "max_connections_per_node") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->max_conns_per_node);
				printf("max_connections_per_node=%u\n", yaml->config->max_conns_per_node);
			}
			else if (strcmp(name, "min_connections_per_node") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->min_conns_per_node);
				printf("min_connections_per_node=%u\n", yaml->config->min_conns_per_node);
			}
			else if (strcmp(name, "async_max_connections_per_node") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->async_max_conns_per_node);
				printf("async_max_connections_per_node=%u\n", yaml->config->async_max_conns_per_node);
			}
			else if (strcmp(name, "async_min_connections_per_node") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->async_min_conns_per_node);
				printf("async_min_connections_per_node=%u\n", yaml->config->async_min_conns_per_node);
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

			if (strcmp(name, "timeout") == 0) {
				rv = as_parse_uint32(yaml, name, value, &yaml->config->conn_timeout_ms);
				printf("timeout=%u\n", yaml->config->conn_timeout_ms);
			}
			else if (strcmp(name, "error_rate_window") == 0) {
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
		else if (strcmp(name, "read") == 0) {
			printf("read\n");
			as_parse_policy(yaml, as_parse_read);
		}
		else if (strcmp(name, "write") == 0) {
			printf("write\n");
			as_parse_policy(yaml, as_parse_write);
		}
		else if (strcmp(name, "query") == 0) {
			printf("query\n");
			as_parse_policy(yaml, as_parse_query);
		}
		else if (strcmp(name, "scan") == 0) {
			printf("scan\n");
			as_parse_policy(yaml, as_parse_scan);
		}
		else if (strcmp(name, "batch_read") == 0) {
			printf("batch\n");
			as_parse_policy(yaml, as_parse_batch_read);
		}
		else if (strcmp(name, "batch_write") == 0) {
			printf("batch_write\n");
			as_parse_policy(yaml, as_parse_batch_write);
		}
		else if (strcmp(name, "batch_udf") == 0) {
			printf("batch_udf\n");
			as_parse_policy(yaml, as_parse_batch_udf);
		}
		else if (strcmp(name, "batch_delete") == 0) {
			printf("batch_delete\n");
			as_parse_policy(yaml, as_parse_batch_delete);
		}
		else if (strcmp(name, "txn_verify") == 0) {
			printf("txn_verify\n");
			as_parse_policy(yaml, as_parse_txn_verify);
		}
		else if (strcmp(name, "txn_roll") == 0) {
			printf("txn_roll\n");
			as_parse_policy(yaml, as_parse_txn_roll);
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
	return as_config_yaml_read(config, true, err);
}

void
as_config_destroy(as_config* config);

void
as_cluster_set_max_socket_idle(as_cluster* cluster, uint32_t max_socket_idle_sec);

as_status
as_config_yaml_update(aerospike* as, as_error* err)
{
	// No need to be pointer.
	as_config config;
	memcpy(&config, &as->config, sizeof(as_config));
	config.rack_ids = NULL;

	as_status status = as_config_yaml_read(&config, false, err);

	if (status != AEROSPIKE_OK) {
		as_config_destroy(&config);
		return status;
	}

	// Apply config to cluster.
	as_cluster* cluster = as->cluster;

	cluster->max_error_rate = config.max_error_rate;
	cluster->error_rate_window = config.error_rate_window;
	cluster->login_timeout_ms = config.login_timeout_ms;
	cluster->tend_interval = (config.tender_interval < 250)? 250 : config.tender_interval;
	cluster->use_services_alternate = config.use_services_alternate;
	cluster->fail_if_not_connected = config.fail_if_not_connected;
	as_cluster_set_max_socket_idle(cluster, config.max_socket_idle);

	cluster->rack_aware = config.rack_aware;

	if (config.rack_ids && !as_rack_ids_equal(config.rack_ids, cluster->rack_ids)) {
		as_vector* old = cluster->rack_ids;

		as_store_ptr_rls((void**)&cluster->rack_ids, config.rack_ids);
		config.rack_ids = NULL;

		as_gc_item item;
		item.data = old;
		item.release_fn = (as_release_fn)as_release_rack_ids;
		as_vector_append(cluster->gc, &item);
	}

	as_config_destroy(&config);
	return AEROSPIKE_OK;
}
