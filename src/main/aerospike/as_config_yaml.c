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
#include <stdio.h>
#include <yaml.h>

//---------------------------------
// Types
//---------------------------------

typedef struct {
	yaml_parser_t parser;
	yaml_event_t event;
	as_config* config;
} as_yaml;

//---------------------------------
// Static Functions
//---------------------------------

static bool
as_expect_event(as_yaml* yaml, yaml_event_type_t type)
{
	if (!yaml_parser_parse(&yaml->parser, &yaml->event)) {
		return false;
	}

	if (yaml->event.type != type) {
		yaml_event_delete(&yaml->event);
		return false;
	}

	yaml_event_delete(&yaml->event);
	return true;
}

static bool
as_parse_scalar(as_yaml* yaml, char* value, int size)
{
	if (!yaml_parser_parse(&yaml->parser, &yaml->event)) {
		return false;
	}

	if (yaml->event.type != YAML_SCALAR_EVENT) {
		yaml_event_delete(&yaml->event);
		return false;
	}

	as_strncpy(value, (const char*)yaml->event.data.scalar.value, size);
	yaml_event_delete(&yaml->event);
	return true;
}

static bool
as_parse_name_value(as_yaml* yaml, char* name, char* value, int size)
{
	if (!as_parse_scalar(yaml, name, size)) {
		return false;
	}

	if (!as_parse_scalar(yaml, value, size)) {
		return false;
	}

	return true;
}

static bool
as_parse_metadata(as_yaml* yaml)
{
	if (!as_expect_event(yaml, YAML_MAPPING_START_EVENT)) {
		return false;
	}

	char name[256];
	char value[256];

	while (as_parse_name_value(yaml, name, value, sizeof(name))) {
		if (strcmp(name, "app_name") == 0) {
			printf("app_name=%s\n", value);
		}
		else if (strcmp(name, "generation") == 0) {
			printf("generation=%s\n", value);
		}
	}
	return true;
}

static bool
as_parse_client(as_yaml* yaml)
{
	if (!as_expect_event(yaml, YAML_MAPPING_START_EVENT)) {
		return false;
	}

	char name[256];
	char value[256];

	while (as_parse_name_value(yaml, name, value, sizeof(name))) {
		if (strcmp(name, "min_connections_per_node") == 0) {
			printf("min_connections_per_node=%s\n", value);
		}
		else if (strcmp(name, "max_connections_per_node") == 0) {
			printf("max_connections_per_node=%s\n", value);
		}
		if (strcmp(name, "async_min_connections_per_node") == 0) {
			printf("async_min_connections_per_node=%s\n", value);
		}
		else if (strcmp(name, "async_max_connections_per_node") == 0) {
			printf("async_max_connections_per_node=%s\n", value);
		}
		else if (strcmp(name, "timeout") == 0) {
			printf("timeout=%s\n", value);
		}
		else if (strcmp(name, "error_rate_window") == 0) {
			printf("error_rate_window=%s\n", value);
		}
		else if (strcmp(name, "max_error_rate") == 0) {
			printf("max_error_rate=%s\n", value);
		}
		else if (strcmp(name, "faile_if_not_connected") == 0) {
			printf("faile_if_not_connected=%s\n", value);
		}
		else if (strcmp(name, "ip_map") == 0) {
			printf("ip_map=%s\n", value);
		}
		else if (strcmp(name, "login_timeout") == 0) {
			printf("login_timeout=%s\n", value);
		}
		else if (strcmp(name, "max_socket_idle") == 0) {
			printf("max_socket_idle=%s\n", value);
		}
		else if (strcmp(name, "rack_aware") == 0) {
			printf("rack_aware=%s\n", value);
		}
		else if (strcmp(name, "tend_interval") == 0) {
			printf("tend_interval=%s\n", value);
		}
		else if (strcmp(name, "use_service_alternative") == 0) {
			printf("use_service_alternative=%s\n", value);
		}
	}
	return true;
}

static bool
as_parse_batch(as_yaml* yaml)
{
	char name[256];
	char value[256];

	while (as_parse_name_value(yaml, name, value, sizeof(name))) {
		if (strcmp(name, "max_concurrent_threads") == 0) {
			printf("max_concurrent_threads=%s\n", value);
		}
		else if (strcmp(name, "allow_inline") == 0) {
			printf("allow_inline=%s\n", value);
		}
		else if (strcmp(name, "allow_inline_ssd") == 0) {
			printf("allow_inline_ssd=%s\n", value);
		}
		else if (strcmp(name, "async_max_connections_per_node") == 0) {
			printf("async_max_connections_per_node=%s\n", value);
		}
		else if (strcmp(name, "respond_all_keys") == 0) {
			printf("respond_all_keys=%s\n", value);
		}
	}
	return true;
}

static bool
as_parse_query(as_yaml* yaml)
{
	char name[256];
	char value[256];

	while (as_parse_name_value(yaml, name, value, sizeof(name))) {
		if (strcmp(name, "include_bin_data") == 0) {
			printf("include_bin_data=%s\n", value);
		}
		else if (strcmp(name, "info_timeout") == 0) {
			printf("info_timeout=%s\n", value);
		}
		else if (strcmp(name, "record_queue_size") == 0) {
			printf("record_queue_size=%s\n", value);
		}
		else if (strcmp(name, "expected_duration") == 0) {
			printf("expected_duration=%s\n", value);
		}
	}
	return true;
}

static bool
as_parse_scan(as_yaml* yaml)
{
	char name[256];
	char value[256];

	while (as_parse_name_value(yaml, name, value, sizeof(name))) {
		if (strcmp(name, "concurrent_nodes") == 0) {
			printf("concurrent_nodes=%s\n", value);
		}
		else if (strcmp(name, "max_concurrent_nodes") == 0) {
			printf("max_concurrent_nodes=%s\n", value);
		}
	}
	return true;
}

static bool
as_parse_write(as_yaml* yaml)
{
	char name[256];
	char value[256];

	while (as_parse_name_value(yaml, name, value, sizeof(name))) {
		if (strcmp(name, "durable_delete") == 0) {
			printf("durable_delete=%s\n", value);
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
		if (!yaml_parser_parse(&yaml->parser, &yaml->event)) {
			return false;
		}

		if (yaml->event.type == YAML_SCALAR_EVENT) {
			char* value = (char*)yaml->event.data.scalar.value;

			if (strcmp(name, "read_mode_ap") == 0) {
				printf("read_mode_ap=%s\n", value);
			}
			else if (strcmp(name, "read_mode_sc") == 0) {
				printf("read_mode_sc=%s\n", value);
			}
			if (strcmp(name, "connect_timeout") == 0) {
				printf("connect_timeout=%s\n", value);
			}
			else if (strcmp(name, "fail_on_filtered_out") == 0) {
				printf("fail_on_filtered_out=%s\n", value);
			}
			else if (strcmp(name, "replica") == 0) {
				printf("replica=%s\n", value);
			}
			else if (strcmp(name, "send_key") == 0) {
				printf("send_key=%s\n", value);
			}
			else if (strcmp(name, "sleep_between_retries") == 0) {
				printf("sleep_between_retries=%s\n", value);
			}
			else if (strcmp(name, "socket_timeout") == 0) {
				printf("socket_timeout=%s\n", value);
			}
			else if (strcmp(name, "timeout_delay") == 0) {
				printf("ip_map=%s\n", value);
			}
			else if (strcmp(name, "total_timeout") == 0) {
				printf("login_timeout=%s\n", value);
			}
			else if (strcmp(name, "max_retries") == 0) {
				printf("max_retries=%s\n", value);
			}
			yaml_event_delete(&yaml->event);
		}
		else if (yaml->event.type == YAML_MAPPING_START_EVENT) {
			yaml_event_delete(&yaml->event);

			if (strcmp(name, "batch") == 0) {
				printf("batch\n");
				as_parse_batch(yaml);
			}
			else if (strcmp(name, "query") == 0) {
				printf("query\n");
				as_parse_query(yaml);
			}
			else if (strcmp(name, "scan") == 0) {
				printf("scan\n");
				as_parse_scan(yaml);
			}
			else if (strcmp(name, "write") == 0) {
				printf("write\n");
				as_parse_write(yaml);
			}
		}
		else {
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
		if (strcmp(name, "client") == 0) {
			printf("client\n");
			as_parse_client(yaml);
		}
		else if (strcmp(name, "policy") == 0) {
			printf("policy\n");
			as_parse_policy(yaml);
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
			as_parse_client(yaml);
		}
		else if (strcmp(name, "policy") == 0) {
			printf("policy\n");
			as_parse_policy(yaml);
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
		if (strcmp(name, "metadata") == 0) {
			printf("metadata\n");
			as_parse_metadata(yaml);
		}
		else if (strcmp(name, "static") == 0) {
			printf("static\n");
			as_parse_static(yaml);
		}
		else if (strcmp(name, "dynamic") == 0) {
			printf("dynamic\n");
			as_parse_dynamic(yaml);
		}
	}

	return true;
}

#if 0
static bool
old_parse(yaml_parser_t* parser)
{
	char name[256];
	yaml_event_t event;
	bool valid = true;

	while (valid) {
		if (!yaml_parser_parse(parser, &event)) {
			return false;
			//return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Yaml parser error: %d", parser.error);
		}

		switch(event.type)
		{
		case YAML_NO_EVENT:
			puts("No event!");
			break;

		case YAML_STREAM_START_EVENT:
			puts("STREAM START");
			break;

		case YAML_STREAM_END_EVENT:
			puts("STREAM END");
			valid = false;
			break;

		case YAML_DOCUMENT_START_EVENT:
			puts("<b>Start Document</b>");
			break;

		case YAML_DOCUMENT_END_EVENT:
			puts("<b>End Document</b>");
			break;

		case YAML_SEQUENCE_START_EVENT:
			puts("<b>Start Sequence</b>");
			break;

		case YAML_SEQUENCE_END_EVENT:
			puts("<b>End Sequence</b>");
			break;

		case YAML_MAPPING_START_EVENT:
			puts("<b>Start Mapping</b>");
			break;

		case YAML_MAPPING_END_EVENT:
			puts("<b>End Mapping</b>");
			break;

		case YAML_ALIAS_EVENT:
			printf("Got alias (anchor %s)\n", event.data.alias.anchor);
			as_strncpy(name, (char*)event.data.alias.anchor, sizeof(name));
			break;

		case YAML_SCALAR_EVENT:
			printf("Got scalar (value %s)\n", event.data.scalar.value);
			const char* name = (const char*)event.data.scalar.value;

			if (strcmp(name, "metadata") == 0) {

			}
			//printf("Got scalar (value %s %s %s)\n", event.data.scalar.value, event.data.scalar.anchor, event.data.scalar.tag);
			break;
		}

		yaml_event_delete(&event);
	}
	return true;
}
#endif

//---------------------------------
// Functions
//---------------------------------

as_status
as_config_yaml_init(as_cluster* cluster, const char* path, as_error* err)
{
	FILE* fp = fopen(path, "r");

	if (!fp) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to open: %s", path);
	}

	as_yaml yaml;

	if (!yaml_parser_initialize(&yaml.parser)) {
		fclose(fp);
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to initialize yaml parser");
	}

	yaml_parser_set_input_file(&yaml.parser, fp);
	bool rv = as_parse_yaml(&yaml);
	yaml_parser_delete(&yaml.parser);
	fclose(fp);

	if (!rv) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to parse: %s", path);
	}
	return AEROSPIKE_OK;
}
