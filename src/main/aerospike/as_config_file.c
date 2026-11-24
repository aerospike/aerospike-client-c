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
#include <aerospike/as_config_file.h>
#include <aerospike/aerospike.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_config_internal.h>
#include <aerospike/as_discovery.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_metrics_internal.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_boolean.h>
#include <aerospike/as_double.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_nil.h>
#include <aerospike/as_orderedmap.h>
#include <aerospike/as_string.h>
#include <aerospike/as_string_builder.h>
#include <ctype.h>
#include <errno.h>
#include <math.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <yaml.h>

//---------------------------------
// Macros
//---------------------------------

// Keys of static.client staged in as_yaml_discovery.set.
#define AS_YAML_SET_IP_MAP (1u << 0)
#define AS_YAML_SET_USE_SERVICES_ALTERNATE (1u << 1)
#define AS_YAML_SET_MERGE_POLICY (1u << 2)
#define AS_YAML_SET_EXECUTION_MODE (1u << 3)
#define AS_YAML_SET_REFRESH_INTERVAL (1u << 4)
#define AS_YAML_SET_REFRESH_TIMEOUT (1u << 5)
#define AS_YAML_SET_TEND_DEADLINE (1u << 6)
#define AS_YAML_SET_DNS (1u << 7)
#define AS_YAML_SET_AWS (1u << 8)
#define AS_YAML_SET_ON_MISS (1u << 9)
#define AS_YAML_SET_STATIC_MAP (1u << 10)
#define AS_YAML_SET_PORT_MAP (1u << 11)

#define AS_YAML_PATH_SIZE 256

// SnakeYAML's LoaderOptions defaults: a node may open with at most 50 collections open around it,
// and a file may have at most 50 aliases to collections.
#define AS_YAML_DEPTH_MAX 50
#define AS_YAML_ALIASES_MAX 50

//---------------------------------
// Types
//---------------------------------

typedef enum {
	AS_YAML_PROVIDER_UNSET,
	AS_YAML_PROVIDER_STATIC,
	AS_YAML_PROVIDER_DNS,
	AS_YAML_PROVIDER_AWS,
	AS_YAML_PROVIDER_REGISTERED
} as_yaml_provider;

typedef enum {
	AS_YAML_TRANSLATOR_UNSET,
	AS_YAML_TRANSLATOR_STATIC_MAP,
	AS_YAML_TRANSLATOR_PORT_MAP,
	AS_YAML_TRANSLATOR_PASS_THROUGH,
	AS_YAML_TRANSLATOR_REGISTERED
} as_yaml_translator;

typedef struct {
	char* name;
	as_vector values; // char*, owned
	bool has_values;
} as_yaml_filter;

// Heap data and plugins of static.client, staged until the whole file is valid. Scalar
// discovery and translation keys are written directly to the config instead.
typedef struct {
	as_vector ip_map;      // as_addr_map, strings owned
	as_vector static_map;  // as_addr_map, strings owned
	as_vector port_map;    // as_addr_map, strings owned
	as_vector dns_names;   // char*, owned
	as_vector dns_servers; // char*, owned
	as_vector aws_filters; // as_yaml_filter, owned
	as_dns_seed_provider_config dns; // tls_name owned
	as_aws_seed_provider_config aws; // region, tls_name and endpoint owned
	as_yaml_provider provider;
	as_yaml_translator translator;
	char* provider_name;   // AS_YAML_PROVIDER_REGISTERED, owned
	char* translator_name; // AS_YAML_TRANSLATOR_REGISTERED, owned
	uint32_t provider_index;   // registry index of provider_name
	uint32_t translator_index; // registry index of translator_name
	as_map* provider_options;   // factory options of a registered provider, owned
	as_map* translator_options; // factory options of a registered translator, owned
	bool use_services_alternate;
	uint32_t set;
} as_yaml_discovery;

// Factory options captured while a discovery or translation block is parsed. They are kept only
// when the block's provider is a registered name.
typedef struct {
	as_orderedmap* map;
	as_vector unknown; // char*, owned: top-level keys logged as unexpected for a built-in
	as_vector names;   // char*, owned: the block's own string keys
	as_vector sources; // as_orderedmap*, reserved: maps merged into the block
	char deferred[AS_ERROR_MESSAGE_MAX_SIZE]; // first error that only a registered name raises
	char cur[AS_YAML_PATH_SIZE]; // path of the value being captured, ending in ... when cut
	uint32_t len;                // length of cur
} as_yaml_options;

// Where an anchored collection is defined, which decides whether an alias can reuse its value.
typedef enum {
	AS_YAML_ORIGIN_OUTSIDE, // outside the discovery and translation blocks
	AS_YAML_ORIGIN_BLOCK,   // a whole discovery or translation block
	AS_YAML_ORIGIN_SECTION, // in dns, aws, static_map or port_map
	AS_YAML_ORIGIN_OPTIONS  // in the factory options of a block
} as_yaml_origin;

// An anchor of the file. A redefined anchor replaces the earlier one.
typedef struct {
	char* name;             // owned
	uint32_t id;            // collection id, 0 for a scalar
	bool scalar;
	bool open;              // the collection has not ended, so an alias to it is recursive
	as_yaml_origin origin;
	as_val* val;            // the collection as a factory option, owned
	yaml_char_t* value;     // scalar copy, owned
	size_t length;
	yaml_char_t* tag;       // owned, NULL when untagged
	int plain_implicit;
	int quoted_implicit;
	yaml_scalar_style_t style;
} as_yaml_anchor;

typedef struct {
	yaml_parser_t parser;
	yaml_event_t event;
	uint8_t* bitmap;
	as_config* config;
	const char* name;
	as_yaml_discovery* disc; // NULL on reload
	as_yaml_options* opts;   // set while a discovery or translation block is parsed
	as_error err;
	as_vector anchors;       // as_yaml_anchor
	as_yaml_anchor* alias;   // anchor of the current collection alias event, until the next event
	uint32_t anchor_id;      // id of the anchored collection whose start event is current, else 0
	uint32_t ids;            // collection ids given out
	uint32_t open[AS_YAML_DEPTH_MAX + 1]; // id of each open collection by depth, 0 if not anchored
	uint32_t aliases;        // aliases to collections so far
	uint32_t depth;          // open collections
	bool section;            // dns, aws, static_map or port_map is being parsed
	bool failed;             // as_parse_next raised an error; every later call fails
	bool init;
} as_yaml;

//---------------------------------
// Static Functions
//---------------------------------

static void
as_parser_error(as_yaml* yaml)
{
	yaml_parser_t* parser = &yaml->parser;

	if (!parser->problem) {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "yaml_parser_parse failure at offset %zu",
			parser->offset);
		return;
	}

	// libyaml sets problem_offset only for reader errors; scanner and parser errors set problem_mark.
	if (parser->error == YAML_READER_ERROR) {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "yaml_parser_parse failure at offset %zu: %s",
			parser->problem_offset, parser->problem);
		return;
	}

	as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM,
		"yaml_parser_parse failure at line %zu, column %zu: %s",
		parser->problem_mark.line + 1, parser->problem_mark.column + 1, parser->problem);
}

static as_yaml_anchor*
as_yaml_anchor_find(as_yaml* yaml, const char* name)
{
	for (uint32_t i = 0; i < yaml->anchors.size; i++) {
		as_yaml_anchor* a = as_vector_get(&yaml->anchors, i);

		if (strcmp(a->name, name) == 0) {
			return a;
		}
	}
	return NULL;
}

static as_yaml_anchor*
as_yaml_anchor_by_id(as_yaml* yaml, uint32_t id)
{
	for (uint32_t i = 0; id && i < yaml->anchors.size; i++) {
		as_yaml_anchor* a = as_vector_get(&yaml->anchors, i);

		if (a->id == id) {
			return a;
		}
	}
	return NULL;
}

// Free what the anchor holds besides its name.
static void
as_yaml_anchor_clear(as_yaml_anchor* a)
{
	as_val_destroy(a->val);
	cf_free(a->value);
	cf_free(a->tag);
	a->val = NULL;
	a->value = NULL;
	a->tag = NULL;
}

static void
as_yaml_anchors_destroy(as_yaml* yaml)
{
	for (uint32_t i = 0; i < yaml->anchors.size; i++) {
		as_yaml_anchor* a = as_vector_get(&yaml->anchors, i);
		as_yaml_anchor_clear(a);
		cf_free(a->name);
	}
	as_vector_destroy(&yaml->anchors);
}

// Record the anchor of the current scalar or collection start event, if it has one.
static void
as_yaml_anchor_define(as_yaml* yaml)
{
	yaml_event_t* e = &yaml->event;
	const char* name = (const char*)(e->type == YAML_SCALAR_EVENT ? e->data.scalar.anchor :
		e->type == YAML_SEQUENCE_START_EVENT ? e->data.sequence_start.anchor :
		e->data.mapping_start.anchor);

	if (!name) {
		return;
	}

	as_yaml_anchor* a = as_yaml_anchor_find(yaml, name);

	if (a) {
		as_yaml_anchor_clear(a);
	}
	else {
		a = as_vector_reserve(&yaml->anchors);
		a->name = cf_strdup(name);
	}

	a->origin = !yaml->opts ? AS_YAML_ORIGIN_OUTSIDE :
		yaml->section ? AS_YAML_ORIGIN_SECTION : AS_YAML_ORIGIN_OPTIONS;

	if (e->type != YAML_SCALAR_EVENT) {
		a->id = ++yaml->ids;
		a->scalar = false;
		a->open = true;
		yaml->anchor_id = a->id;
		return;
	}

	a->id = 0;
	a->scalar = true;
	a->open = false;
	a->length = e->data.scalar.length;
	a->value = cf_malloc(a->length + 1);
	memcpy(a->value, e->data.scalar.value, a->length + 1);
	a->tag = e->data.scalar.tag ? (yaml_char_t*)cf_strdup((const char*)e->data.scalar.tag) : NULL;
	a->plain_implicit = e->data.scalar.plain_implicit;
	a->quoted_implicit = e->data.scalar.quoted_implicit;
	a->style = e->data.scalar.style;
}

static bool
as_yaml_event_error(as_yaml* yaml, const char* msg)
{
	as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "%s: line %zu, column %zu", msg,
		yaml->event.start_mark.line + 1, yaml->event.start_mark.column + 1);
	yaml_event_delete(&yaml->event);
	yaml->failed = true;
	return false;
}

// An alias to a scalar becomes a copy of the scalar, so every parser takes it as written there.
// An alias to a collection stays, with its anchor in yaml->alias.
static bool
as_yaml_alias(as_yaml* yaml)
{
	yaml_event_t* e = &yaml->event;
	as_yaml_anchor* a = as_yaml_anchor_find(yaml, (const char*)e->data.alias.anchor);
	char msg[AS_YAML_PATH_SIZE];

	if (!a) {
		snprintf(msg, sizeof(msg), "Undefined YAML alias *%.200s", e->data.alias.anchor);
		return as_yaml_event_error(yaml, msg);
	}

	if (!a->scalar) {
		if (++yaml->aliases > AS_YAML_ALIASES_MAX) {
			snprintf(msg, sizeof(msg), "More than %u YAML aliases to collections",
				AS_YAML_ALIASES_MAX);
			return as_yaml_event_error(yaml, msg);
		}
		yaml->alias = a;
		return true;
	}

	yaml_mark_t start = e->start_mark;
	yaml_mark_t end = e->end_mark;
	yaml_event_delete(e);

	if (!yaml_scalar_event_initialize(e, NULL, a->tag, a->value, (int)a->length,
		a->plain_implicit, a->quoted_implicit, a->style)) {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "YAML alias *%s: copy failed", a->name);
		yaml->failed = true;
		return false;
	}
	e->start_mark = start;
	e->end_mark = end;
	return true;
}

// Read the next event. An error here fails the whole file, even when the caller only stops a
// loop on it: as_config_file_read() checks yaml->failed.
static bool
as_parse_next(as_yaml* yaml)
{
	if (yaml->failed) {
		return false;
	}

	if (!yaml_parser_parse(&yaml->parser, &yaml->event)) {
		as_parser_error(yaml);
		yaml->failed = true;
		return false;
	}

	yaml->alias = NULL;
	yaml->anchor_id = 0;

	switch (yaml->event.type) {
		case YAML_SCALAR_EVENT:
		case YAML_SEQUENCE_START_EVENT:
		case YAML_MAPPING_START_EVENT:
			if (yaml->depth > AS_YAML_DEPTH_MAX) {
				char msg[64];
				snprintf(msg, sizeof(msg), "YAML nesting deeper than %u levels", AS_YAML_DEPTH_MAX);
				return as_yaml_event_error(yaml, msg);
			}
			as_yaml_anchor_define(yaml);

			if (yaml->event.type != YAML_SCALAR_EVENT) {
				yaml->open[yaml->depth++] = yaml->anchor_id;
			}
			break;

		case YAML_SEQUENCE_END_EVENT:
		case YAML_MAPPING_END_EVENT: {
			as_yaml_anchor* a = as_yaml_anchor_by_id(yaml, yaml->open[--yaml->depth]);

			if (a) {
				a->open = false;
			}
			break;
		}

		case YAML_ALIAS_EVENT:
			return as_yaml_alias(yaml);

		default:
			break;
	}
	return true;
}

static const char*
as_event_kind(yaml_event_type_t type)
{
	switch (type) {
		case YAML_SCALAR_EVENT:
			return "a scalar";
		case YAML_MAPPING_START_EVENT:
			return "a map";
		case YAML_SEQUENCE_START_EVENT:
			return "a sequence";
		case YAML_ALIAS_EVENT:
			return "an alias";
		case YAML_MAPPING_END_EVENT:
			return "the end of a map";
		case YAML_SEQUENCE_END_EVENT:
			return "the end of a sequence";
		default:
			return "an unsupported value";
	}
}

// An alias to a collection where a client setting is parsed. path names the value that holds the
// alias, or is NULL when the caller has no path.
static void
as_alias_error(as_yaml* yaml, const char* path)
{
	if (path) {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM,
			"Invalid %s: YAML aliases to collections are not supported for client settings", path);
		return;
	}

	as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM,
		"YAML aliases to collections are not supported for client settings: line %zu, column %zu",
		yaml->event.start_mark.line + 1, yaml->event.start_mark.column + 1);
}

static bool
as_skip_value(as_yaml* yaml);

// Skip the pairs of a mapping whose start event is consumed, through its end event.
static bool
as_skip_open_mapping(as_yaml* yaml)
{
	while (as_parse_next(yaml)) {
		if (yaml->event.type == YAML_MAPPING_END_EVENT) {
			yaml_event_delete(&yaml->event);
			return true;
		}

		// Key, which may itself be a collection.
		if (!as_skip_value(yaml)) {
			return false;
		}

		if (!as_parse_next(yaml)) {
			return false;
		}

		if (!as_skip_value(yaml)) {
			return false;
		}
	}
	return false;
}

// Skip the items of a sequence whose start event is consumed, through its end event.
static bool
as_skip_sequence(as_yaml* yaml)
{
	while (as_parse_next(yaml)) {
		if (yaml->event.type == YAML_SEQUENCE_END_EVENT) {
			yaml_event_delete(&yaml->event);
			return true;
		}

		if (!as_skip_value(yaml)) {
			return false;
		}
	}
	return false;
}

// Skip the value whose first event is current.
static bool
as_skip_value(as_yaml* yaml)
{
	switch (yaml->event.type) {
		case YAML_MAPPING_START_EVENT:
			yaml_event_delete(&yaml->event);
			return as_skip_open_mapping(yaml);

		case YAML_SEQUENCE_START_EVENT:
			yaml_event_delete(&yaml->event);
			return as_skip_sequence(yaml);

		case YAML_SCALAR_EVENT:
		case YAML_ALIAS_EVENT:
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

static inline void
as_expected_error(as_yaml* yaml, yaml_event_type_t type)
{
	if (yaml->event.type == YAML_ALIAS_EVENT) {
		as_alias_error(yaml, NULL);
		return;
	}

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
as_yaml_is_merge(const yaml_event_t* event);

// A merge key, whose scalar event is current, in a mapping the client parses itself (path NULL
// for the top level). Java applies the merge there and the client does not, so the file fails.
static bool
as_merge_error(as_yaml* yaml, const char* path)
{
	const char* key = (const char*)yaml->event.data.scalar.value;

	if (path) {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM,
			"Invalid %s.%s: YAML merge keys (<<) are only supported in factory options", path, key);
	}
	else {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM,
			"Invalid %s: YAML merge keys (<<) are only supported in factory options", key);
	}
	yaml_event_delete(&yaml->event);
	return false;
}

// Read the next key of the mapping at path (NULL for the top level) into name. Returns false at
// the end of the mapping with *end set, or on an error with *end clear.
static bool
as_parse_key(as_yaml* yaml, const char* path, char* name, int size, bool* end)
{
	*end = false;

	if (!as_parse_next(yaml)) {
		return false;
	}

	if (yaml->event.type == YAML_MAPPING_END_EVENT) {
		yaml_event_delete(&yaml->event);
		*end = true;
		return false;
	}

	if (yaml->event.type != YAML_SCALAR_EVENT) {
		if (path) {
			as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM,
				"Invalid %s: expected scalar keys, not %s", path, as_event_kind(yaml->event.type));
		}
		else {
			as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM,
				"Invalid top-level key: expected a scalar, not %s", as_event_kind(yaml->event.type));
		}
		yaml_event_delete(&yaml->event);
		return false;
	}

	if (as_yaml_is_merge(&yaml->event)) {
		return as_merge_error(yaml, path);
	}

	as_strncpy(name, (const char*)yaml->event.data.scalar.value, size);
	yaml_event_delete(&yaml->event);
	return true;
}

static bool
as_yaml_known(const char* name, const char* const* names);

static bool
as_kind_error(as_yaml* yaml, const char* path, const char* name, const char* expected);

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
as_assign_uint64(
	const char* section, const char* name, const char* value, uint64_t src, uint64_t* trg
	)
{
	if (*trg != src) {
		as_log_info("Set %s.%s = %s", section, name, value);
		*trg = src;
	}
}

static bool
as_parse_uint64(as_yaml* yaml, const char* name, const char* value, uint64_t* out, uint32_t field)
{
	char* end = NULL;
	errno = 0;
	unsigned long long parsed = strtoull(value, &end, 10);

	if (end == value || *end != '\0' || errno != 0) {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM,
			"Invalid dynamic configuration %s.%s: %s", yaml->name, name, value);
		return false;
	}

	as_assign_uint64(yaml->name, name, value, (uint64_t)parsed, out);
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
as_parse_vector_int32(
	as_yaml* yaml, const char* path, const char* name, as_vector** out, uint32_t field
	)
{
	as_vector* list = as_vector_create(sizeof(int), 8);
	int32_t v;

	for (uint32_t i = 0; ; i++) {
		if (!as_parse_next(yaml)) {
			as_vector_destroy(list);
			return false;
		}

		if (yaml->event.type == YAML_SEQUENCE_END_EVENT) {
			yaml_event_delete(&yaml->event);
			break;
		}

		if (yaml->event.type != YAML_SCALAR_EVENT) {
			char item[AS_YAML_PATH_SIZE];
			snprintf(item, sizeof(item), "%.100s.%.100s[%u]", path, name, i);

			if (yaml->event.type == YAML_ALIAS_EVENT) {
				as_alias_error(yaml, item);
			}
			else {
				as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid %s: expected a scalar, not %s",
					item, as_event_kind(yaml->event.type));
			}
			yaml_event_delete(&yaml->event);
			as_vector_destroy(list);
			return false;
		}

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
	// send_key can only be enabled in dynamic config.
	if (src == AS_POLICY_KEY_SEND && *trg != src) {
		const char* str = as_xstr(AS_POLICY_KEY_SEND);
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

	// send_key can only be enabled in dynamic config.
	if (send_key) {
		as_policy_key val = send_key ? AS_POLICY_KEY_SEND : AS_POLICY_KEY_DIGEST;

		as_assign_send_key(yaml->name, name, value, val, key);
		as_field_set(yaml->bitmap, field);
	}
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
as_parse_timeout_delay(as_yaml* yaml, const char* name, const char* value, uint32_t* out, uint32_t field)
{
	uint32_t val;

	if (!parse_uint32(yaml, name, value, 0, UINT32_MAX, &val)) {
		return false;
	}

	if (val > 0 && val < 1000) {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM,
			"Invalid %s: %s. valid values are 0 or >= 1000", name, value);
		return false;
	}

	as_assign_uint32(yaml->name, name, value, val, out);
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

	if (strcmp(name, "connect_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.connect_timeout, AS_READ_CONNECT_TIMEOUT);
	}

	if (strcmp(name, "socket_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.socket_timeout, AS_READ_SOCKET_TIMEOUT);
	}

	if (strcmp(name, "total_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.total_timeout, AS_READ_TOTAL_TIMEOUT);
	}

	if (strcmp(name, "timeout_delay") == 0) {
		return as_parse_timeout_delay(yaml, name, value, &policy->base.timeout_delay, AS_READ_TIMEOUT_DELAY);
	}

	if (strcmp(name, "max_retries") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.max_retries, AS_READ_MAX_RETRIES);
	}

	if (strcmp(name, "sleep_between_retries") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.sleep_between_retries, AS_READ_SLEEP_BETWEEN_RETRIES);
	}

	if (strcmp(name, "error_detail_verbosity") == 0) {
		return as_parse_uint8(yaml, name, value, &policy->base.error_detail_verbosity, AS_READ_ERROR_DETAIL);
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

	if (strcmp(name, "connect_timeout") == 0) {
		if (!as_parse_uint32(yaml, name, value, &policy->base.connect_timeout, AS_WRITE_CONNECT_TIMEOUT)) {
			return false;
		}
		as_assign_uint32(operate_section, name, value, policy->base.connect_timeout, &base->operate.base.connect_timeout);
		as_assign_uint32(apply_section, name, value, policy->base.connect_timeout, &base->apply.base.connect_timeout);
		as_assign_uint32(remove_section, name, value, policy->base.connect_timeout, &base->remove.base.connect_timeout);
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

	if (strcmp(name, "timeout_delay") == 0) {
		if (!as_parse_timeout_delay(yaml, name, value, &policy->base.timeout_delay, AS_WRITE_TIMEOUT_DELAY)) {
			return false;
		}
		as_assign_uint32(operate_section, name, value, policy->base.timeout_delay, &base->operate.base.timeout_delay);
		as_assign_uint32(apply_section, name, value, policy->base.timeout_delay, &base->apply.base.timeout_delay);
		as_assign_uint32(remove_section, name, value, policy->base.timeout_delay, &base->remove.base.timeout_delay);
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
		as_assign_uint32(operate_section, name, value, policy->base.sleep_between_retries, &base->operate.base.sleep_between_retries);
		as_assign_uint32(apply_section, name, value, policy->base.sleep_between_retries, &base->apply.base.sleep_between_retries);
		as_assign_uint32(remove_section, name, value, policy->base.sleep_between_retries, &base->remove.base.sleep_between_retries);
		return true;
	}

	if (strcmp(name, "error_detail_verbosity") == 0) {
		if (!as_parse_uint8(yaml, name, value, &policy->base.error_detail_verbosity, AS_WRITE_ERROR_DETAIL)) {
			return false;
		}
		as_assign_uint8(operate_section, name, value, policy->base.error_detail_verbosity, &base->operate.base.error_detail_verbosity);
		as_assign_uint8(apply_section, name, value, policy->base.error_detail_verbosity, &base->apply.base.error_detail_verbosity);
		as_assign_uint8(remove_section, name, value, policy->base.error_detail_verbosity, &base->remove.base.error_detail_verbosity);
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

	if (strcmp(name, "connect_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.connect_timeout, AS_SCAN_CONNECT_TIMEOUT);
	}

	if (strcmp(name, "socket_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.socket_timeout, AS_SCAN_SOCKET_TIMEOUT);
	}

	if (strcmp(name, "total_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.total_timeout, AS_SCAN_TOTAL_TIMEOUT);
	}

	if (strcmp(name, "timeout_delay") == 0) {
		return as_parse_timeout_delay(yaml, name, value, &policy->base.timeout_delay, AS_SCAN_TIMEOUT_DELAY);
	}

	if (strcmp(name, "max_retries") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.max_retries, AS_SCAN_MAX_RETRIES);
	}

	if (strcmp(name, "sleep_between_retries") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.sleep_between_retries, AS_SCAN_SLEEP_BETWEEN_RETRIES);
	}

	if (strcmp(name, "error_detail_verbosity") == 0) {
		return as_parse_uint8(yaml, name, value, &policy->base.error_detail_verbosity, AS_SCAN_ERROR_DETAIL);
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

	if (strcmp(name, "connect_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.connect_timeout, AS_QUERY_CONNECT_TIMEOUT);
	}

	if (strcmp(name, "socket_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.socket_timeout, AS_QUERY_SOCKET_TIMEOUT);
	}

	if (strcmp(name, "total_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.total_timeout, AS_QUERY_TOTAL_TIMEOUT);
	}

	if (strcmp(name, "timeout_delay") == 0) {
		return as_parse_timeout_delay(yaml, name, value, &policy->base.timeout_delay, AS_QUERY_TIMEOUT_DELAY);
	}

	if (strcmp(name, "max_retries") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.max_retries, AS_QUERY_MAX_RETRIES);
	}

	if (strcmp(name, "sleep_between_retries") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.sleep_between_retries, AS_QUERY_SLEEP_BETWEEN_RETRIES);
	}

	if (strcmp(name, "error_detail_verbosity") == 0) {
		return as_parse_uint8(yaml, name, value, &policy->base.error_detail_verbosity, AS_QUERY_ERROR_DETAIL);
	}

	if (strcmp(name, "info_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->info_timeout, AS_QUERY_INFO_TIMEOUT);
	}

	if (strcmp(name, "expected_duration") == 0) {
		return as_parse_expected_duration(yaml, name, value, &policy->expected_duration, AS_QUERY_EXPECTED_DURATION);
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

	if (strcmp(name, "connect_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.connect_timeout, offset + AS_BATCH_CONNECT_TIMEOUT);
	}

	if (strcmp(name, "socket_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.socket_timeout, offset + AS_BATCH_SOCKET_TIMEOUT);
	}

	if (strcmp(name, "total_timeout") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.total_timeout, offset + AS_BATCH_TOTAL_TIMEOUT);
	}

	if (strcmp(name, "timeout_delay") == 0) {
		return as_parse_timeout_delay(yaml, name, value, &policy->base.timeout_delay, offset + AS_BATCH_TIMEOUT_DELAY);
	}

	if (strcmp(name, "max_retries") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.max_retries, offset + AS_BATCH_MAX_RETRIES);
	}

	if (strcmp(name, "sleep_between_retries") == 0) {
		return as_parse_uint32(yaml, name, value, &policy->base.sleep_between_retries, offset + AS_BATCH_SLEEP_BETWEEN_RETRIES);
	}

	if (strcmp(name, "error_detail_verbosity") == 0) {
		return as_parse_uint8(yaml, name, value, &policy->base.error_detail_verbosity, offset + AS_BATCH_ERROR_DETAIL);
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
	bool end;

	while (as_parse_key(yaml, "dynamic.metrics.labels", name, sizeof(name), &end)) {
		if (!as_parse_next(yaml)) {
			as_metrics_policy_destroy_labels(policy);
			return false;
		}

		if (yaml->event.type != YAML_SCALAR_EVENT) {
			as_kind_error(yaml, "dynamic.metrics.labels", name, "a scalar");
			as_metrics_policy_destroy_labels(policy);
			return false;
		}

		as_strncpy(value, (const char*)yaml->event.data.scalar.value, sizeof(value));
		yaml_event_delete(&yaml->event);
		as_metrics_policy_add_label(policy, name, value);
	}

	if (!end) {
		as_metrics_policy_destroy_labels(policy);
		return false;
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
as_parse_export_interval(as_yaml* yaml, const char* name, const char* value, as_metrics_policy* policy)
{
	uint64_t ms = 0;

	// "" is valid and matches the policy field. Enable then uses interval.
	if (!as_metrics_export_interval_to_ms(value, &ms)) {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM,
			"Invalid dynamic configuration metrics.export_interval: %s", value);
		return false;
	}

	if (strlen(value) >= sizeof(policy->export_interval)) {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM,
			"Invalid dynamic configuration metrics.export_interval: %s", value);
		return false;
	}

	if (strcmp(policy->export_interval, value) != 0) {
		as_log_info("Set %s.%s = %s", yaml->name, name, value);
		as_strncpy(policy->export_interval, value, sizeof(policy->export_interval));
	}

	as_field_set(yaml->bitmap, AS_METRICS_EXPORT_INTERVAL);
	return true;
}

static bool
as_parse_builtin_exporter(as_yaml* yaml, const char* name, const char* value)
{
	as_metrics_builtin_exporter kind;

	if (strcmp(value, "file") == 0) {
		kind = AS_METRICS_BUILTIN_EXPORTER_FILE;
	}
	else if (strcmp(value, "none") == 0) {
		kind = AS_METRICS_BUILTIN_EXPORTER_NONE;
	}
	else if (strcmp(value, "custom") == 0) {
		kind = AS_METRICS_BUILTIN_EXPORTER_CUSTOM;
	}
	else {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM,
			"Invalid dynamic configuration metrics.exporter: %s. valid values: file, none, custom", value);
		return false;
	}

	as_metrics_policy* policy = &yaml->config->policies.metrics;

	if (policy->metrics_exporter != kind) {
		as_log_info("Set %s.%s = %s", yaml->name, name, value);
		policy->metrics_exporter = kind;
	}

	as_field_set(yaml->bitmap, AS_METRICS_EXPORTER);
	return true;
}

static bool
as_parse_report_dir(as_yaml* yaml, const char* name, const char* value, as_metrics_policy* policy)
{
	if (strcmp(policy->report_dir, value) != 0) {
		as_log_info("Set %s.%s = %s", yaml->name, name, value);
		as_strncpy(policy->report_dir, value, sizeof(policy->report_dir));
	}

	as_field_set(yaml->bitmap, AS_METRICS_REPORT_DIR);
	return true;
}

static bool
as_parse_latency_unit(as_yaml* yaml, const char* name, const char* value, as_metrics_policy* policy)
{
	as_metrics_latency_unit unit;

	if (strcmp(value, "milliseconds") == 0) {
		unit = AS_METRICS_LATENCY_MILLISECONDS;
	}
	else if (strcmp(value, "microseconds") == 0) {
		unit = AS_METRICS_LATENCY_MICROSECONDS;
	}
	else {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM,
			"Invalid dynamic configuration metrics.extended.operational.latency_unit: %s", value);
		return false;
	}

	if (policy->latency_unit != unit) {
		as_log_info("Set %s.%s = %s", yaml->name, name, value);
		policy->latency_unit = unit;
	}

	as_field_set(yaml->bitmap, AS_METRICS_LATENCY_UNIT);
	return true;
}

static bool
as_parse_sampler_field(as_yaml* yaml, const char* name, as_metrics_policy* policy)
{
	if (yaml->event.type == YAML_MAPPING_START_EVENT) {
		yaml_event_delete(&yaml->event);
		as_log_info("Unexpected field: metrics.extended.operational.sampler.%s", name);
		return as_skip_open_mapping(yaml);
	}

	if (yaml->event.type != YAML_SCALAR_EVENT) {
		as_expected_error(yaml, YAML_SCALAR_EVENT);
		yaml_event_delete(&yaml->event);
		return false;
	}

	const char* value = (const char*)yaml->event.data.scalar.value;
	const char* saved = yaml->name;
	bool rv = true;

	yaml->name = "metrics.extended.operational.sampler";

	if (strcmp(name, "range") == 0) {
		rv = as_parse_uint32(yaml, name, value, &policy->sampler_range, AS_METRICS_SAMPLER_RANGE);
	}
	else if (strcmp(name, "threshold") == 0) {
		rv = as_parse_uint32(yaml, name, value, &policy->sampler_threshold, AS_METRICS_SAMPLER_THRESHOLD);
	}
	else {
		as_log_info("Unexpected field: metrics.extended.operational.sampler.%s", name);
	}

	yaml->name = saved;
	yaml_event_delete(&yaml->event);
	return rv;
}

typedef bool (*as_parse_metrics_field_fn)(as_yaml* yaml, const char* name, as_metrics_policy* policy);

static bool
as_parse_open_mapping(
	as_yaml* yaml, const char* path, as_parse_metrics_field_fn fn, as_metrics_policy* policy
	);

static bool
as_parse_operational_field(as_yaml* yaml, const char* name, as_metrics_policy* policy)
{
	if (yaml->event.type == YAML_MAPPING_START_EVENT) {
		if (strcmp(name, "sampler") == 0) {
			yaml_event_delete(&yaml->event);

			if (!as_parse_open_mapping(yaml, "dynamic.metrics.extended.operational.sampler",
				as_parse_sampler_field, policy)) {
				return false;
			}

			if (policy->sampler_range < 1 || policy->sampler_threshold > policy->sampler_range) {
				as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM,
					"Invalid metrics.extended.operational.sampler: range must be >= 1 and threshold must be <= range");
				return false;
			}
			return true;
		}

		yaml_event_delete(&yaml->event);
		as_log_info("Unexpected field: metrics.extended.operational.%s", name);
		return as_skip_open_mapping(yaml);
	}

	if (yaml->event.type != YAML_SCALAR_EVENT) {
		as_expected_error(yaml, YAML_SCALAR_EVENT);
		yaml_event_delete(&yaml->event);
		return false;
	}

	const char* value = (const char*)yaml->event.data.scalar.value;
	const char* saved = yaml->name;
	bool rv = true;

	yaml->name = "metrics.extended.operational";

	if (strcmp(name, "enabled") == 0) {
		rv = as_parse_bool(yaml, name, value, &policy->operational_enabled, AS_METRICS_OPERATIONAL);
	}
	else if (strcmp(name, "latency_unit") == 0) {
		rv = as_parse_latency_unit(yaml, name, value, policy);
	}
	else if (strcmp(name, "latency_columns") == 0) {
		rv = as_parse_uint8(yaml, name, value, &policy->latency_columns, AS_METRICS_LATENCY_COLUMNS);
	}
	else if (strcmp(name, "latency_shift") == 0) {
		rv = as_parse_uint8(yaml, name, value, &policy->latency_shift, AS_METRICS_LATENCY_SHIFT);
	}
	else {
		as_log_info("Unexpected field: metrics.extended.operational.%s", name);
	}

	yaml->name = saved;
	yaml_event_delete(&yaml->event);
	return rv;
}

// Parse the pairs of the mapping at path whose start event is consumed, through its end event.
static bool
as_parse_open_mapping(
	as_yaml* yaml, const char* path, as_parse_metrics_field_fn fn, as_metrics_policy* policy
	)
{
	while (as_parse_next(yaml)) {
		if (yaml->event.type == YAML_MAPPING_END_EVENT) {
			yaml_event_delete(&yaml->event);
			return true;
		}

		if (yaml->event.type != YAML_SCALAR_EVENT) {
			as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid %s: expected scalar keys, not %s",
				path, as_event_kind(yaml->event.type));
			yaml_event_delete(&yaml->event);
			return false;
		}

		if (as_yaml_is_merge(&yaml->event)) {
			return as_merge_error(yaml, path);
		}

		char name[256];
		as_strncpy(name, (const char*)yaml->event.data.scalar.value, sizeof(name));
		yaml_event_delete(&yaml->event);

		if (!as_parse_next(yaml)) {
			return false;
		}

		if (!fn(yaml, name, policy)) {
			return false;
		}
	}
	return false;
}

static bool
as_parse_usage_field(as_yaml* yaml, const char* name, as_metrics_policy* policy)
{
	if (yaml->event.type == YAML_MAPPING_START_EVENT) {
		yaml_event_delete(&yaml->event);
		as_log_info("Unexpected field: metrics.extended.usage.%s", name);
		return as_skip_open_mapping(yaml);
	}

	if (yaml->event.type != YAML_SCALAR_EVENT) {
		as_expected_error(yaml, YAML_SCALAR_EVENT);
		yaml_event_delete(&yaml->event);
		return false;
	}

	const char* value = (const char*)yaml->event.data.scalar.value;
	const char* saved = yaml->name;
	bool rv = true;

	yaml->name = "metrics.extended.usage";

	if (strcmp(name, "enabled") == 0) {
		rv = as_parse_bool(yaml, name, value, &policy->usage_enabled, AS_METRICS_USAGE);
	}
	else {
		as_log_info("Unexpected field: metrics.extended.usage.%s", name);
	}

	yaml->name = saved;
	yaml_event_delete(&yaml->event);
	return rv;
}

static bool
as_parse_extended_field(as_yaml* yaml, const char* name, as_metrics_policy* policy)
{
	if (yaml->event.type != YAML_MAPPING_START_EVENT) {
		as_log_info("Unexpected field: metrics.extended.%s", name);
		return as_skip_value(yaml);
	}

	yaml_event_delete(&yaml->event);

	if (strcmp(name, "operational") == 0) {
		return as_parse_open_mapping(yaml, "dynamic.metrics.extended.operational",
			as_parse_operational_field, policy);
	}

	if (strcmp(name, "usage") == 0) {
		return as_parse_open_mapping(yaml, "dynamic.metrics.extended.usage", as_parse_usage_field,
			policy);
	}

	as_log_info("Unexpected field: metrics.extended.%s", name);
	return as_skip_open_mapping(yaml);
}

static bool
as_parse_metrics(as_yaml* yaml, const char* name, const char* value, as_policies* base)
{
	as_metrics_policy* policy = &base->metrics;
	yaml->name = "metrics";

	// metrics.enable remains the legacy name. metrics.enabled is the tech spec key.
	// The legacy key maps to operational metrics when that key is absent.
	// See as_cluster_update_metrics().
	if (strcmp(name, "enable") == 0) {
		as_field_set(yaml->bitmap, AS_METRICS_LEGACY_ENABLE);
		return as_parse_bool(yaml, name, value, &policy->enable, AS_METRICS_ENABLE);
	}

	if (strcmp(name, "enabled") == 0) {
		return as_parse_bool(yaml, name, value, &policy->enable, AS_METRICS_ENABLE);
	}

	if (strcmp(name, "extended") == 0) {
		return as_parse_open_mapping(yaml, "dynamic.metrics.extended", as_parse_extended_field,
			policy);
	}

	if (strcmp(name, "export_interval") == 0) {
		return as_parse_export_interval(yaml, name, value, policy);
	}

	if (strcmp(name, "exporter") == 0) {
		return as_parse_builtin_exporter(yaml, name, value);
	}

	if (strcmp(name, "report_dir") == 0) {
		return as_parse_report_dir(yaml, name, value, policy);
	}

	if (strcmp(name, "report_size_limit") == 0) {
		return as_parse_uint64(yaml, name, value, &policy->report_size_limit, AS_METRICS_REPORT_SIZE_LIMIT);
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

// Keys of dynamic.metrics whose value as_parse_metrics() parses as a whole map.
static const char* const as_metrics_maps[] = {"labels", "extended", NULL};

// maps lists the keys whose value fn consumes as a whole map, or is NULL.
static bool
as_parse_policy(as_yaml* yaml, const char* section, as_parse_policy_fn fn, const char* const* maps)
{
	as_policies* base = &yaml->config->policies;

	if (!as_expect_event(yaml, YAML_MAPPING_START_EVENT)) {
		return false;
	}

	char path[AS_YAML_PATH_SIZE];
	snprintf(path, sizeof(path), "dynamic.%.200s", section);

	char name[256];
	bool end;

	while (as_parse_key(yaml, path, name, sizeof(name), &end)) {
		if (!as_parse_next(yaml)) {
			return false;
		}

		bool rv;

		if (maps && as_yaml_known(name, maps)) {
			if (yaml->event.type != YAML_MAPPING_START_EVENT) {
				return as_kind_error(yaml, path, name, "a map");
			}
			yaml_event_delete(&yaml->event);

			if (!fn(yaml, name, "", base)) {
				return false;
			}
			continue;
		}

		switch (yaml->event.type) {
			case YAML_SCALAR_EVENT: {
				char* value = (char*)yaml->event.data.scalar.value;

				rv = fn(yaml, name, value, base);
				yaml_event_delete(&yaml->event);
				break;
			}

			case YAML_SEQUENCE_START_EVENT:
			case YAML_MAPPING_START_EVENT: {
				bool map = yaml->event.type == YAML_MAPPING_START_EVENT;
				yaml_event_delete(&yaml->event);
				rv = fn(yaml, name, "", base);

				if (rv) {
					rv = map ? as_skip_open_mapping(yaml) : as_skip_sequence(yaml);
				}
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
	return end;
}

//---------------------------------
// Static Functions: discovery and translation keys
//---------------------------------

#define AS_YAML_COUNT(a) (sizeof(a) / sizeof((a)[0]))

static const char* const as_yaml_merge_policies[] = {"merge", "replace", "discovery_only"};
static const char* const as_yaml_execution_modes[] = {"thread", "tend"};
static const char* const as_yaml_record_types[] = {"a", "srv"};
static const char* const as_yaml_address_fields[] = {
	"private_ip", "public_ip", "private_dns", "public_dns"
};
static const char* const as_yaml_miss_policies[] = {"pass_through", "strict"};

static const char* const as_yaml_static_client_scalars[] = {
	"config_interval", "max_connections_per_node", "min_connections_per_node",
	"async_max_connections_per_node", "async_min_connections_per_node", "use_services_alternate",
	NULL
};
static const char* const as_yaml_discovery_scalars[] = {
	"provider", "merge_policy", "execution_mode", "refresh_interval_ms", "refresh_timeout_ms",
	"tend_refresh_deadline_ms", NULL
};
static const char* const as_yaml_dns_scalars[] = {
	"record_type", "port", "tls_name", "timeout_ms", NULL
};
static const char* const as_yaml_aws_scalars[] = {
	"region", "address_field", "port", "tls_name", "timeout_ms", "init_timeout_ms", "endpoint",
	NULL
};
static const char* const as_yaml_translation_scalars[] = {"provider", "on_miss", NULL};

typedef bool (*as_parse_block_fn)(as_yaml* yaml, const char* path, const char* name);

static bool
as_yaml_known(const char* name, const char* const* names)
{
	for (const char* const* p = names; *p; p++) {
		if (strcmp(name, *p) == 0) {
			return true;
		}
	}
	return false;
}

static inline const char*
as_yaml_scalar(as_yaml* yaml)
{
	return (const char*)yaml->event.data.scalar.value;
}

static void
as_yaml_path(char* out, const char* path, const char* name)
{
	as_string_builder sb;
	as_string_builder_assign(&sb, AS_YAML_PATH_SIZE, out);
	as_string_builder_append(&sb, path);
	as_string_builder_append_char(&sb, '.');
	as_string_builder_append(&sb, name);
}

// A known key whose value has the wrong kind. Deletes the event.
static bool
as_kind_error(as_yaml* yaml, const char* path, const char* name, const char* expected)
{
	char key[AS_YAML_PATH_SIZE];
	as_yaml_path(key, path, name);

	if (yaml->event.type == YAML_ALIAS_EVENT) {
		as_alias_error(yaml, key);
	}
	else {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid %s: expected %s, not %s",
			key, expected, as_event_kind(yaml->event.type));
	}
	yaml_event_delete(&yaml->event);
	return false;
}

// Log an unknown key and skip its value, whatever its kind.
static bool
as_skip_field(as_yaml* yaml, const char* path, const char* name)
{
	as_log_info("Unexpected field: %s.%s", path, name);
	return as_skip_value(yaml);
}

static bool
parse_uint32_strict(
	as_yaml* yaml, const char* path, const char* value, uint32_t min, uint32_t max, uint32_t* out
	)
{
	char* end = NULL;
	errno = 0;
	unsigned long long v = strtoull(value, &end, 10);

	if (!isdigit((unsigned char)value[0]) || *end != '\0' || errno != 0 || v > UINT32_MAX) {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid uint %s: %s", path, value);
		return false;
	}

	if (v < min || v > max) {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid uint %s: %s. valid range: %u - %u",
			path, value, min, max);
		return false;
	}

	*out = (uint32_t)v;
	return true;
}

static bool
as_parse_enum(
	as_yaml* yaml, const char* path, const char* value, const char* const* names, uint32_t size,
	uint32_t* out
	)
{
	for (uint32_t i = 0; i < size; i++) {
		if (strcmp(value, names[i]) == 0) {
			*out = i;
			return true;
		}
	}

	as_string_builder sb;
	as_string_builder_inita(&sb, 256, false);

	for (uint32_t i = 0; i < size; i++) {
		if (i > 0) {
			as_string_builder_append(&sb, ", ");
		}
		as_string_builder_append(&sb, names[i]);
	}

	as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid %s: %s. valid values: %s",
		path, value, sb.data);
	return false;
}

static void
as_yaml_set_string(const char** trg, const char* value)
{
	cf_free((void*)*trg);
	*trg = cf_strdup(value);
}

static void
as_yaml_strings_clear(as_vector* v)
{
	for (uint32_t i = 0; i < v->size; i++) {
		cf_free(*(char**)as_vector_get(v, i));
	}
	as_vector_clear(v);
}

static void
as_yaml_addr_map_clear(as_vector* v)
{
	for (uint32_t i = 0; i < v->size; i++) {
		as_addr_map* entry = as_vector_get(v, i);
		cf_free(entry->orig);
		cf_free(entry->alt);
	}
	as_vector_clear(v);
}

static void
as_yaml_filters_clear(as_vector* v)
{
	for (uint32_t i = 0; i < v->size; i++) {
		as_yaml_filter* filter = as_vector_get(v, i);
		cf_free(filter->name);
		as_yaml_strings_clear(&filter->values);
		as_vector_destroy(&filter->values);
	}
	as_vector_clear(v);
}

static void
as_yaml_dns_reset(as_yaml_discovery* d)
{
	as_yaml_strings_clear(&d->dns_names);
	as_yaml_strings_clear(&d->dns_servers);
	cf_free((void*)d->dns.tls_name);
	as_dns_seed_provider_config_init(&d->dns);
}

static void
as_yaml_aws_reset(as_yaml_discovery* d)
{
	as_yaml_filters_clear(&d->aws_filters);
	cf_free((void*)d->aws.region);
	cf_free((void*)d->aws.tls_name);
	cf_free((void*)d->aws.endpoint);
	as_aws_seed_provider_config_init(&d->aws);
}

static void
as_yaml_discovery_init(as_yaml_discovery* d)
{
	memset(d, 0, sizeof(as_yaml_discovery));
	as_vector_init(&d->ip_map, sizeof(as_addr_map), 4);
	as_vector_init(&d->static_map, sizeof(as_addr_map), 4);
	as_vector_init(&d->port_map, sizeof(as_addr_map), 4);
	as_vector_init(&d->dns_names, sizeof(char*), 4);
	as_vector_init(&d->dns_servers, sizeof(char*), 4);
	as_vector_init(&d->aws_filters, sizeof(as_yaml_filter), 2);
	as_dns_seed_provider_config_init(&d->dns);
	as_aws_seed_provider_config_init(&d->aws);
}

static void
as_yaml_discovery_destroy(as_yaml_discovery* d)
{
	as_yaml_dns_reset(d);
	as_yaml_aws_reset(d);
	as_yaml_addr_map_clear(&d->ip_map);
	as_yaml_addr_map_clear(&d->static_map);
	as_yaml_addr_map_clear(&d->port_map);
	as_vector_destroy(&d->ip_map);
	as_vector_destroy(&d->static_map);
	as_vector_destroy(&d->port_map);
	as_vector_destroy(&d->dns_names);
	as_vector_destroy(&d->dns_servers);
	as_vector_destroy(&d->aws_filters);
	cf_free(d->provider_name);
	cf_free(d->translator_name);
	as_val_destroy(d->provider_options);
	as_val_destroy(d->translator_options);
}

//---------------------------------
// Static Functions: factory options
//---------------------------------

// Factory options are typed by the resolver and tag constructors of SnakeYAML 2.6, which Java
// loads the file with, so a file shared with Java gives a factory the same values, aliases and
// merge keys included. Where they differ: a timestamp is its string, not a date; an integer
// outside int64 is its string, not a BigInteger; a map key must be a string or an integer;
// !!binary, !!set, !!omap, !!pairs and local tags are rejected; an alias to a collection reuses
// only a collection captured as an option, so a recursive one is rejected; an inline merge
// source keeps its last duplicate key and its own keys over its merges, where SnakeYAML keeps
// the first.

#define AS_YAML_TAG_PREFIX "tag:yaml.org,2002:"
#define AS_YAML_REASON_SIZE 256

// Exponent magnitude a float is clamped to; far past the range of a double.
#define AS_YAML_EXP_MAX 1000000000

typedef enum {
	AS_YAML_TYPE_STR,
	AS_YAML_TYPE_NULL,
	AS_YAML_TYPE_BOOL,
	AS_YAML_TYPE_INT,
	AS_YAML_TYPE_FLOAT,
	AS_YAML_TYPE_TIMESTAMP,
	AS_YAML_TYPE_MERGE
} as_yaml_type;

static inline bool
as_yaml_is_dec(char c)
{
	return c >= '0' && c <= '9';
}

static inline bool
as_yaml_is_digit(char c, uint32_t base)
{
	switch (base) {
		case 2:
			return c == '0' || c == '1';
		case 8:
			return c >= '0' && c <= '7';
		case 10:
			return as_yaml_is_dec(c);
		default:
			return as_yaml_is_dec(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
	}
}

static inline const char*
as_yaml_skip_sign(const char* p)
{
	return (*p == '-' || *p == '+') ? p + 1 : p;
}

// [digit][digit_]* in base. Returns the end, or NULL when p does not start with a digit.
static const char*
as_yaml_match_run(const char* p, uint32_t base)
{
	if (!as_yaml_is_digit(*p, base)) {
		return NULL;
	}

	do {
		p++;
	} while (as_yaml_is_digit(*p, base) || *p == '_');
	return p;
}

// [0-9_]*
static const char*
as_yaml_skip_dec(const char* p)
{
	while (as_yaml_is_dec(*p) || *p == '_') {
		p++;
	}
	return p;
}

// (?::[0-5]?[0-9])+ Returns the end, or NULL.
static const char*
as_yaml_match_sexagesimal(const char* p)
{
	const char* start = p;

	while (*p == ':') {
		p++;

		if (*p >= '0' && *p <= '5' && as_yaml_is_dec(p[1])) {
			p += 2;
		}
		else if (as_yaml_is_dec(*p)) {
			p++;
		}
		else {
			return NULL;
		}
	}
	return p == start ? NULL : p;
}

// [eE][-+]?[0-9]+ through the end.
static bool
as_yaml_match_exponent(const char* p)
{
	if (*p != 'e' && *p != 'E') {
		return false;
	}

	p = as_yaml_skip_sign(p + 1);

	if (!as_yaml_is_dec(*p)) {
		return false;
	}

	while (as_yaml_is_dec(*p)) {
		p++;
	}
	return *p == '\0';
}

// Resolver.INT: [-+]?0b_*[0-1][0-1_]* | [-+]?0_*[0-7][0-7_]* | [-+]?(?:0|[1-9][0-9_]*) |
// [-+]?0x_*[0-9a-fA-F][0-9a-fA-F_]* | [-+]?[1-9][0-9_]*(?::[0-5]?[0-9])+
static bool
as_yaml_match_int(const char* value)
{
	const char* p = as_yaml_skip_sign(value);
	const char* e;

	if (p[0] == '0' && (p[1] == 'b' || p[1] == 'x')) {
		const char* q = p + 2;

		while (*q == '_') {
			q++;
		}
		e = as_yaml_match_run(q, p[1] == 'b' ? 2 : 16);
		return e && *e == '\0';
	}

	if (p[0] == '0') {
		if (p[1] == '\0') {
			return true;
		}

		const char* q = p + 1;

		while (*q == '_') {
			q++;
		}
		e = as_yaml_match_run(q, 8);
		return e && *e == '\0';
	}

	if (*p < '1' || *p > '9') {
		return false;
	}

	e = as_yaml_match_run(p, 10);

	if (*e == '\0') {
		return true;
	}

	e = as_yaml_match_sexagesimal(e);
	return e && *e == '\0';
}

// Resolver.FLOAT: [-+]?[0-9][0-9_]*\.[0-9_]*(?:[eE][-+]?[0-9]+)? |
// [-+]?[0-9][0-9_]*[eE][-+]?[0-9]+ | [-+]?\.[0-9_]+(?:[eE][-+]?[0-9]+)? |
// [-+]?[0-9][0-9_]*(?::[0-5]?[0-9])+\.[0-9_]* | [-+]?\.(?:inf|Inf|INF) | \.(?:nan|NaN|NAN)
static bool
as_yaml_match_float(const char* value)
{
	if (strcmp(value, ".nan") == 0 || strcmp(value, ".NaN") == 0 || strcmp(value, ".NAN") == 0) {
		return true;
	}

	const char* p = as_yaml_skip_sign(value);

	if (strcmp(p, ".inf") == 0 || strcmp(p, ".Inf") == 0 || strcmp(p, ".INF") == 0) {
		return true;
	}

	if (*p == '.') {
		p++;

		if (!as_yaml_is_dec(*p) && *p != '_') {
			return false;
		}
		p = as_yaml_skip_dec(p);
		return *p == '\0' || as_yaml_match_exponent(p);
	}

	if (!as_yaml_is_dec(*p)) {
		return false;
	}

	const char* q = as_yaml_skip_dec(p + 1);

	if (*q == '.') {
		const char* r = as_yaml_skip_dec(q + 1);

		if (*r == '\0' || as_yaml_match_exponent(r)) {
			return true;
		}
	}

	if (as_yaml_match_exponent(q)) {
		return true;
	}

	q = as_yaml_match_sexagesimal(q);

	if (!q || *q != '.') {
		return false;
	}
	return *as_yaml_skip_dec(q + 1) == '\0';
}

static bool
as_yaml_match_bool(const char* value, bool* out)
{
	static const char* const names[] = {
		"yes", "Yes", "YES", "true", "True", "TRUE", "on", "On", "ON",
		"no", "No", "NO", "false", "False", "FALSE", "off", "Off", "OFF"
	};

	for (uint32_t i = 0; i < AS_YAML_COUNT(names); i++) {
		if (strcmp(value, names[i]) == 0) {
			*out = i < 9;
			return true;
		}
	}
	return false;
}

static bool
as_yaml_match_null(const char* value)
{
	return value[0] == '\0' || strcmp(value, "~") == 0 || strcmp(value, "null") == 0 ||
		strcmp(value, "Null") == 0 || strcmp(value, "NULL") == 0;
}

// Implicit type of a plain scalar. SnakeYAML tries a pattern only up to a length: 10 for bool,
// null and merge, 1024 for int and float.
static as_yaml_type
as_yaml_resolve(const char* value, size_t len)
{
	bool b;

	if (len <= 10 && as_yaml_match_bool(value, &b)) {
		return AS_YAML_TYPE_BOOL;
	}

	if (len <= 1024 && as_yaml_match_int(value)) {
		return AS_YAML_TYPE_INT;
	}

	if (len <= 1024 && as_yaml_match_float(value)) {
		return AS_YAML_TYPE_FLOAT;
	}

	if (strcmp(value, "<<") == 0) {
		return AS_YAML_TYPE_MERGE;
	}

	if (len <= 10 && as_yaml_match_null(value)) {
		return AS_YAML_TYPE_NULL;
	}
	return AS_YAML_TYPE_STR;
}

// Copy value without the '_' separators.
static char*
as_yaml_strip(const char* value)
{
	char* out = cf_malloc(strlen(value) + 1);
	char* q = out;

	for (const char* p = value; *p; p++) {
		if (*p != '_') {
			*q++ = *p;
		}
	}
	*q = '\0';
	return out;
}

static bool
as_yaml_accumulate(uint64_t* acc, uint64_t base, uint64_t digit)
{
	if (*acc > (UINT64_MAX - digit) / base) {
		return false;
	}
	*acc = *acc * base + digit;
	return true;
}

// Java's int: the low 32 bits, two's complement.
static inline int32_t
as_yaml_int32(uint32_t v)
{
	return v <= INT32_MAX ? (int32_t)v : (int32_t)(v - 2147483648u) - INT32_MAX - 1;
}

static bool
as_yaml_to_int64(uint64_t mag, bool neg, int64_t* out)
{
	if (neg) {
		if (mag > (uint64_t)INT64_MAX + 1) {
			return false;
		}
		*out = mag == (uint64_t)INT64_MAX + 1 ? INT64_MIN : -(int64_t)mag;
		return true;
	}

	if (mag > (uint64_t)INT64_MAX) {
		return false;
	}
	*out = (int64_t)mag;
	return true;
}

// Long.parseLong of [p, end): an optional sign, then decimal digits that fit int64.
static bool
as_yaml_parse_long(const char* p, const char* end, int64_t* out)
{
	bool neg = false;

	if (p < end && (*p == '-' || *p == '+')) {
		neg = *p == '-';
		p++;
	}

	if (p == end) {
		return false;
	}

	uint64_t mag = 0;

	for (; p < end; p++) {
		if (!as_yaml_is_dec(*p) || !as_yaml_accumulate(&mag, 10, (uint64_t)(*p - '0'))) {
			return false;
		}
	}
	return as_yaml_to_int64(mag, neg, out);
}

// SnakeYAML's createNumber(): Java parses the digits p in base, with "-" before them when neg, so
// p may carry a sign of its own only when neg is clear. *big is set for a value outside int64,
// which Java gives as a BigInteger.
static bool
as_yaml_create_number(const char* p, bool neg, uint32_t base, int64_t* out, bool* big)
{
	if (*p == '-' || *p == '+') {
		if (neg) {
			return false;
		}
		neg = *p == '-';
		p++;
	}

	if (*p == '\0') {
		return false;
	}

	uint64_t mag = 0;
	bool over = false;

	for (; *p; p++) {
		if (!as_yaml_is_digit(*p, base)) {
			return false;
		}

		uint64_t d = as_yaml_is_dec(*p) ? (uint64_t)(*p - '0') : (uint64_t)((*p | 0x20) - 'a' + 10);

		if (!over && !as_yaml_accumulate(&mag, base, d)) {
			over = true;
		}
	}
	*big = over || !as_yaml_to_int64(mag, neg, out);
	return true;
}

// ConstructYamlInt base 60. Java parses each part with Long.parseLong and adds it times a power
// of 60 into a 32-bit int, so the result wraps. String.split(":") drops trailing empty parts.
static bool
as_yaml_int_sexagesimal(const char* p, bool neg, int64_t* out)
{
	const char* end = p + strlen(p);

	while (end > p && end[-1] == ':') {
		end--;
	}

	uint32_t acc = 0;

	while (p < end) {
		const char* q = memchr(p, ':', (size_t)(end - p));

		if (!q) {
			q = end;
		}

		int64_t part;

		if (!as_yaml_parse_long(p, q, &part)) {
			return false;
		}
		acc = acc * 60u + (uint32_t)part;
		p = q < end ? q + 1 : end;
	}

	int32_t v = as_yaml_int32(acc);

	// Java then parses "-" and the int's digits, which fails on a second sign.
	if (neg && v < 0) {
		return false;
	}
	*out = neg ? -(int64_t)v : v;
	return true;
}

// ConstructYamlInt. False where Java fails; *big is set for a value outside int64.
static bool
as_yaml_construct_int(const char* value, int64_t* out, bool* big)
{
	char* s = as_yaml_strip(value);
	const char* p = s;
	bool neg = false;
	bool ok;

	*big = false;

	if (*p == '-' || *p == '+') {
		neg = *p == '-';
		p++;
	}

	if (s[0] == '\0') {
		ok = false;
	}
	else if (strcmp(p, "0") == 0) {
		*out = 0;
		ok = true;
	}
	else if (p[0] == '0' && p[1] == 'b') {
		ok = as_yaml_create_number(p + 2, neg, 2, out, big);
	}
	else if (p[0] == '0' && p[1] == 'x') {
		ok = as_yaml_create_number(p + 2, neg, 16, out, big);
	}
	else if (p[0] == '0') {
		ok = as_yaml_create_number(p + 1, neg, 8, out, big);
	}
	else if (strchr(p, ':')) {
		ok = as_yaml_int_sexagesimal(p, neg, out);
	}
	else {
		ok = as_yaml_create_number(p, neg, 10, out, big);
	}
	cf_free(s);
	return ok;
}

static inline bool
as_yaml_is_space(char c)
{
	return (unsigned char)c <= ' ';
}

// Java's Double.parseDouble of [value, value + len): trimmed, an optional sign, then NaN,
// Infinity, a decimal or a hex float with a p exponent, and an optional f, F, d or D. strtod gets
// the digits without the point, so the locale's decimal point does not matter.
static bool
as_yaml_parse_double(const char* value, size_t len, double* out)
{
	const char* p = value;
	const char* end = value + len;

	while (p < end && as_yaml_is_space(*p)) {
		p++;
	}

	while (end > p && as_yaml_is_space(end[-1])) {
		end--;
	}

	bool neg = false;

	if (p < end && (*p == '-' || *p == '+')) {
		neg = *p == '-';
		p++;
	}

	size_t n = (size_t)(end - p);

	if (n == 3 && memcmp(p, "NaN", 3) == 0) {
		*out = NAN;
		return true;
	}

	if (n == 8 && memcmp(p, "Infinity", 8) == 0) {
		*out = neg ? -INFINITY : INFINITY;
		return true;
	}

	bool hex = n >= 2 && p[0] == '0' && (p[1] == 'x' || p[1] == 'X');
	uint32_t base = hex ? 16 : 10;

	if (hex) {
		p += 2;
	}

	const char* int_start = p;

	while (p < end && as_yaml_is_digit(*p, base)) {
		p++;
	}

	const char* int_end = p;
	const char* frac_start = p;
	const char* frac_end = p;

	if (p < end && *p == '.') {
		frac_start = ++p;

		while (p < end && as_yaml_is_digit(*p, base)) {
			p++;
		}
		frac_end = p;
	}

	if (int_end == int_start && frac_end == frac_start) {
		return false;
	}

	int64_t exp = 0;

	if (p < end && (*p | 0x20) == (hex ? 'p' : 'e')) {
		p++;

		bool exp_neg = false;

		if (p < end && (*p == '-' || *p == '+')) {
			exp_neg = *p == '-';
			p++;
		}

		if (p == end || !as_yaml_is_dec(*p)) {
			return false;
		}

		for (; p < end && as_yaml_is_dec(*p); p++) {
			if (exp < AS_YAML_EXP_MAX) {
				exp = exp * 10 + (*p - '0');
			}
		}

		if (exp > AS_YAML_EXP_MAX) {
			exp = AS_YAML_EXP_MAX;
		}

		if (exp_neg) {
			exp = -exp;
		}
	}
	else if (hex) {
		return false;
	}

	if (p < end && (*p == 'f' || *p == 'F' || *p == 'd' || *p == 'D')) {
		p++;
	}

	if (p != end) {
		return false;
	}

	size_t int_len = (size_t)(int_end - int_start);
	size_t frac_len = (size_t)(frac_end - frac_start);
	int64_t shift = frac_len > AS_YAML_EXP_MAX ? AS_YAML_EXP_MAX : (int64_t)frac_len;

	// Each hex digit is 4 bits of a binary exponent.
	exp -= hex ? 4 * shift : shift;

	size_t size = int_len + frac_len + 32;
	char* buf = cf_malloc(size);
	char* q = buf;

	if (hex) {
		*q++ = '0';
		*q++ = 'x';
	}
	memcpy(q, int_start, int_len);
	q += int_len;
	memcpy(q, frac_start, frac_len);
	q += frac_len;
	snprintf(q, size - (size_t)(q - buf), "%c%lld", hex ? 'p' : 'e', (long long)exp);

	// Out of range is not an error: the result is then infinite or zero, as in Java.
	char* stop;
	double d = strtod(buf, &stop);
	bool ok = *stop == '\0';
	cf_free(buf);

	if (!ok) {
		return false;
	}
	*out = neg ? -d : d;
	return true;
}

// ASCII case-insensitive equality with lower, which is lowercase.
static bool
as_yaml_ieq(const char* value, const char* lower)
{
	for (; *value && *lower; value++, lower++) {
		char c = *value >= 'A' && *value <= 'Z' ? (char)(*value | 0x20) : *value;

		if (c != *lower) {
			return false;
		}
	}
	return *value == *lower;
}

// ConstructYamlFloat. False where Java fails.
static bool
as_yaml_construct_float(const char* value, double* out)
{
	char* s = as_yaml_strip(value);
	const char* p = s;
	double sign = 1.0;
	double v = 0.0;
	bool ok = true;

	if (*p == '-' || *p == '+') {
		sign = *p == '-' ? -1.0 : 1.0;
		p++;
	}

	if (s[0] == '\0') {
		ok = false;
	}
	else if (as_yaml_ieq(p, ".inf")) {
		v = INFINITY;
	}
	else if (as_yaml_ieq(p, ".nan")) {
		v = NAN;
	}
	else if (strchr(p, ':')) {
		// Base 60: Java adds the parts from the last, each times a 32-bit int power of 60.
		const char* end = p + strlen(p);

		while (end > p && end[-1] == ':') {
			end--;
		}

		uint32_t bes = 1;
		const char* q = end;

		while (end > p) {
			const char* start = q;

			while (start > p && start[-1] != ':') {
				start--;
			}

			double part;

			if (!as_yaml_parse_double(start, (size_t)(q - start), &part)) {
				ok = false;
				break;
			}
			v += part * (double)as_yaml_int32(bes);
			bes *= 60u;

			if (start == p) {
				break;
			}
			q = start - 1;
		}
	}
	else {
		ok = as_yaml_parse_double(p, strlen(p), &v);
	}
	cf_free(s);

	if (!ok) {
		return false;
	}
	*out = sign * v;
	return true;
}

// ConstructYamlBool: any case, and null for another value.
static as_val*
as_yaml_construct_bool(const char* value)
{
	static const char* const names[] = {"yes", "true", "on", "no", "false", "off"};

	for (uint32_t i = 0; i < AS_YAML_COUNT(names); i++) {
		if (as_yaml_ieq(value, names[i])) {
			return (as_val*)as_boolean_new(i < 3);
		}
	}
	return (as_val*)&as_nil;
}

// Between min and max decimal digits, as many as there are.
static bool
as_yaml_skip_digits(const char** p, uint32_t min, uint32_t max)
{
	uint32_t n = 0;

	while (n < max && as_yaml_is_dec(**p)) {
		(*p)++;
		n++;
	}
	return n >= min;
}

// SnakeYAML's timestamp constructor pattern: [0-9]{4}-[0-9]{1,2}-[0-9]{1,2}, optionally then
// (?:[Tt]|[ \t]+)[0-9]{1,2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]*)? and
// (?:[ \t]*(?:Z|[-+][0-9]{1,2}(?::(?:[0-9]{2})?)?))?
static bool
as_yaml_match_timestamp(const char* p)
{
	if (!as_yaml_skip_digits(&p, 4, 4) || *p != '-') {
		return false;
	}
	p++;

	if (!as_yaml_skip_digits(&p, 1, 2) || *p != '-') {
		return false;
	}
	p++;

	if (!as_yaml_skip_digits(&p, 1, 2)) {
		return false;
	}

	if (*p == '\0') {
		return true;
	}

	if (*p == 'T' || *p == 't') {
		p++;
	}
	else if (*p == ' ' || *p == '\t') {
		while (*p == ' ' || *p == '\t') {
			p++;
		}
	}
	else {
		return false;
	}

	if (!as_yaml_skip_digits(&p, 1, 2) || *p != ':') {
		return false;
	}
	p++;

	if (!as_yaml_skip_digits(&p, 2, 2) || *p != ':') {
		return false;
	}
	p++;

	if (!as_yaml_skip_digits(&p, 2, 2)) {
		return false;
	}

	if (*p == '.') {
		p++;

		while (as_yaml_is_dec(*p)) {
			p++;
		}
	}

	if (*p == '\0') {
		return true;
	}

	// Blanks only before a time zone.
	while (*p == ' ' || *p == '\t') {
		p++;
	}

	if (*p == 'Z') {
		return p[1] == '\0';
	}

	if (*p != '-' && *p != '+') {
		return false;
	}
	p++;

	if (!as_yaml_skip_digits(&p, 1, 2)) {
		return false;
	}

	if (*p == ':') {
		p++;

		if (as_yaml_is_dec(p[0]) && as_yaml_is_dec(p[1])) {
			p += 2;
		}
	}
	return *p == '\0';
}

// Tag as written: !!int for a core tag.
static void
as_yaml_tag_name(const char* tag, char* out, size_t size)
{
	size_t len = sizeof(AS_YAML_TAG_PREFIX) - 1;

	if (strncmp(tag, AS_YAML_TAG_PREFIX, len) == 0) {
		snprintf(out, size, "!!%s", tag + len);
		return;
	}
	snprintf(out, size, "%s", tag);
}

// Core tag name after the prefix, or NULL for another tag.
static const char*
as_yaml_core_tag(const char* tag)
{
	size_t len = sizeof(AS_YAML_TAG_PREFIX) - 1;
	return strncmp(tag, AS_YAML_TAG_PREFIX, len) == 0 ? tag + len : NULL;
}

// A collection may carry no tag, the non-specific tag or its own core tag.
static bool
as_yaml_collection_tag(const yaml_event_t* event, bool map, char* reason, size_t size)
{
	const char* tag = (const char*)(map ? event->data.mapping_start.tag :
		event->data.sequence_start.tag);

	if (!tag || strcmp(tag, "!") == 0) {
		return true;
	}

	const char* core = as_yaml_core_tag(tag);

	if (core && strcmp(core, map ? "map" : "seq") == 0) {
		return true;
	}

	char name[128];
	as_yaml_tag_name(tag, name, sizeof(name));
	snprintf(reason, size, "unsupported tag %s", name);
	return false;
}

// Type of a scalar event: an untagged plain scalar, or a scalar of any style with the
// non-specific tag !, by the resolver; another untagged scalar is a string; a core tag by its
// name. Returns false for another tag.
static bool
as_yaml_scalar_type(const yaml_event_t* event, as_yaml_type* type)
{
	const char* tag = (const char*)event->data.scalar.tag;

	if (!tag || strcmp(tag, "!") == 0) {
		bool resolve = tag || event->data.scalar.style == YAML_PLAIN_SCALAR_STYLE;
		*type = resolve ? as_yaml_resolve((const char*)event->data.scalar.value,
			event->data.scalar.length) : AS_YAML_TYPE_STR;
		return true;
	}

	static const struct {
		const char* name;
		as_yaml_type type;
	} tags[] = {
		{"str", AS_YAML_TYPE_STR},
		{"null", AS_YAML_TYPE_NULL},
		{"bool", AS_YAML_TYPE_BOOL},
		{"int", AS_YAML_TYPE_INT},
		{"float", AS_YAML_TYPE_FLOAT},
		{"timestamp", AS_YAML_TYPE_TIMESTAMP},
		{"merge", AS_YAML_TYPE_MERGE}
	};
	const char* core = as_yaml_core_tag(tag);

	for (uint32_t i = 0; core && i < AS_YAML_COUNT(tags); i++) {
		if (strcmp(core, tags[i].name) == 0) {
			*type = tags[i].type;
			return true;
		}
	}
	return false;
}

static bool
as_yaml_is_merge(const yaml_event_t* event)
{
	as_yaml_type type;
	return event->type == YAML_SCALAR_EVENT && as_yaml_scalar_type(event, &type) &&
		type == AS_YAML_TYPE_MERGE;
}

// Type a scalar event as an option value, with SnakeYAML's constructor for its type. Returns NULL
// with reason where Java fails: another tag, a value its constructor rejects, or a merge key.
static as_val*
as_yaml_option_scalar(const yaml_event_t* event, char* reason, size_t size)
{
	const char* value = (const char*)event->data.scalar.value;
	const char* tag = (const char*)event->data.scalar.tag;
	as_yaml_type type;
	char name[128];

	if (!as_yaml_scalar_type(event, &type)) {
		as_yaml_tag_name(tag, name, sizeof(name));
		snprintf(reason, size, "unsupported tag %s", name);
		return NULL;
	}

	switch (type) {
		case AS_YAML_TYPE_NULL:
			return (as_val*)&as_nil;

		case AS_YAML_TYPE_BOOL:
			return as_yaml_construct_bool(value);

		case AS_YAML_TYPE_INT: {
			int64_t i;
			bool big;

			if (!as_yaml_construct_int(value, &i, &big)) {
				snprintf(reason, size, "'%.100s' is not a valid !!int", value);
				return NULL;
			}

			if (big) {
				return (as_val*)as_string_new_strdup(value);
			}
			return (as_val*)as_integer_new(i);
		}

		case AS_YAML_TYPE_FLOAT: {
			double d;

			if (!as_yaml_construct_float(value, &d)) {
				snprintf(reason, size, "'%.100s' is not a valid !!float", value);
				return NULL;
			}
			return (as_val*)as_double_new(d);
		}

		case AS_YAML_TYPE_TIMESTAMP:
			if (!as_yaml_match_timestamp(value)) {
				snprintf(reason, size, "'%.100s' is not a valid !!timestamp", value);
				return NULL;
			}
			return (as_val*)as_string_new_strdup(value);

		case AS_YAML_TYPE_MERGE:
			snprintf(reason, size, "a YAML merge key (<<) is not a value");
			return NULL;

		default:
			return (as_val*)as_string_new_strdup(value);
	}
}

// Record the first error that makes the options invalid. It is raised at the end of the block,
// and only when the block's provider is a registered name.
static void
as_options_defer(as_yaml_options* o, const char* fmt, ...)
{
	if (o->deferred[0]) {
		return;
	}

	va_list ap;
	va_start(ap, fmt);
	vsnprintf(o->deferred, sizeof(o->deferred), fmt, ap);
	va_end(ap);
}

// Append prefix and segment to the path of the value being captured. Returns the length to
// restore with as_options_pop(). A path that does not fit ends in "...", and stays so.
static uint32_t
as_options_push(as_yaml_options* o, const char* prefix, const char* segment)
{
	const uint32_t limit = AS_YAML_PATH_SIZE - 4;
	uint32_t len = o->len;

	if (len > limit) {
		return len;
	}

	int n = snprintf(o->cur + len, AS_YAML_PATH_SIZE - len, "%s%s", prefix, segment);

	if (n < 0 || len + (uint32_t)n > limit) {
		memcpy(o->cur + limit, "...", 4);
		o->len = AS_YAML_PATH_SIZE - 1;
	}
	else {
		o->len = len + (uint32_t)n;
	}
	return len;
}

static inline void
as_options_pop(as_yaml_options* o, uint32_t len)
{
	o->cur[len] = '\0';
	o->len = len;
}

static bool
as_yaml_capture(as_yaml* yaml, as_val** out);

// Keep a captured collection for the aliases to its anchor.
static void
as_yaml_anchor_store(as_yaml* yaml, uint32_t id, as_val* val)
{
	as_yaml_anchor* a = as_yaml_anchor_by_id(yaml, id);

	if (a) {
		as_val_destroy(a->val);
		a->val = as_val_reserve(val);
	}
}

// Why an alias to a collection has no value to reuse.
static void
as_yaml_alias_reason(const as_yaml_anchor* a, char* reason, size_t size)
{
	if (a->open) {
		snprintf(reason, size, "alias *%.100s is recursive", a->name);
		return;
	}

	switch (a->origin) {
		case AS_YAML_ORIGIN_OUTSIDE:
			snprintf(reason, size, "alias *%.100s is to a collection outside the discovery and "
				"translation blocks", a->name);
			break;

		case AS_YAML_ORIGIN_BLOCK:
			snprintf(reason, size, "alias *%.100s is to a whole discovery or translation block",
				a->name);
			break;

		case AS_YAML_ORIGIN_SECTION:
			snprintf(reason, size, "alias *%.100s is to a collection in a built-in section",
				a->name);
			break;

		default:
			snprintf(reason, size, "alias *%.100s is to a collection that is not an option",
				a->name);
			break;
	}
}

static void
as_yaml_sources_destroy(as_vector* sources)
{
	for (uint32_t i = 0; i < sources->size; i++) {
		as_val_destroy(*(as_val**)as_vector_get(sources, i));
	}
	as_vector_destroy(sources);
}

// Add the value of a merge key, whose first event is current, to sources (as_orderedmap*,
// reserved): a map, or each map of a list. Anything else gets SnakeYAML's error, deferred.
static bool
as_yaml_merge_sources(as_yaml* yaml, as_vector* sources)
{
	as_yaml_options* o = yaml->opts;
	as_val* val;

	if (!as_yaml_capture(yaml, &val)) {
		return false;
	}

	switch (as_val_type(val)) {
		case AS_UNDEF:
			return true;

		case AS_MAP:
			as_vector_append(sources, &val);
			return true;

		case AS_LIST: {
			as_list* list = (as_list*)val;
			uint32_t size = as_list_size(list);

			for (uint32_t i = 0; i < size; i++) {
				as_val* item = as_list_get(list, i);

				if (as_val_type(item) != AS_MAP) {
					as_options_defer(o, "Invalid %s: expected a mapping for merging, but found %s",
						o->cur, as_val_type(item) == AS_LIST ? "sequence" : "scalar");
					continue;
				}
				as_val_reserve(item);
				as_vector_append(sources, &item);
			}
			as_val_destroy(val);
			return true;
		}

		default:
			as_options_defer(o, "Invalid %s: expected a mapping or list of mappings for merging, "
				"but found scalar", o->cur);
			as_val_destroy(val);
			return true;
	}
}

// Add the pairs of sources whose keys map lacks, so explicit keys win, then earlier sources.
static void
as_yaml_merge(as_orderedmap* map, as_vector* sources)
{
	for (uint32_t i = 0; i < sources->size; i++) {
		as_orderedmap* src = *(as_orderedmap**)as_vector_get(sources, i);
		as_orderedmap_iterator it;
		as_orderedmap_iterator_init(&it, src);

		while (as_orderedmap_iterator_has_next(&it)) {
			as_pair* p = (as_pair*)as_orderedmap_iterator_next(&it);
			as_val* key = as_pair_1(p);
			as_val* val = as_pair_2(p);

			if (as_orderedmap_get(map, key)) {
				continue;
			}
			as_val_reserve(key);
			as_val_reserve(val);

			if (as_orderedmap_set(map, key, val) != 0) {
				as_val_destroy(key);
				as_val_destroy(val);
			}
		}
		as_orderedmap_iterator_destroy(&it);
	}
}

// Capture the pairs of a mapping whose start event is consumed, through its end event. Merge keys
// apply at the end, so the anchor id keeps the merged map.
static bool
as_yaml_capture_map(as_yaml* yaml, uint32_t id, bool keep, as_val** out)
{
	as_yaml_options* o = yaml->opts;
	as_orderedmap* map = as_orderedmap_new(8);
	as_vector sources;
	as_vector_inita(&sources, sizeof(as_val*), 4);

	while (as_parse_next(yaml)) {
		if (yaml->event.type == YAML_MAPPING_END_EVENT) {
			yaml_event_delete(&yaml->event);
			as_yaml_merge(map, &sources);
			as_yaml_sources_destroy(&sources);

			if (keep) {
				as_yaml_anchor_store(yaml, id, (as_val*)map);
				*out = (as_val*)map;
			}
			else {
				as_orderedmap_destroy(map);
			}
			return true;
		}

		if (as_yaml_is_merge(&yaml->event)) {
			yaml_event_delete(&yaml->event);

			if (!as_parse_next(yaml) || !as_yaml_merge_sources(yaml, &sources)) {
				break;
			}
			continue;
		}

		// The value's path names it by the key as written; a collection key is [?].
		char segment[AS_YAML_PATH_SIZE];
		bool scalar = yaml->event.type == YAML_SCALAR_EVENT;
		as_val* key;

		if (scalar) {
			as_strncpy(segment, (const char*)yaml->event.data.scalar.value, sizeof(segment));
		}

		if (!as_yaml_capture(yaml, &key)) {
			break;
		}

		if (key && as_val_type(key) != AS_STRING && as_val_type(key) != AS_INTEGER) {
			as_options_defer(o, "Invalid %s: map keys must be strings or integers", o->cur);
			as_val_destroy(key);
			key = NULL;
		}

		as_val* val = NULL;
		uint32_t len = scalar ? as_options_push(o, ".", segment) : as_options_push(o, "", "[?]");
		bool ok = as_parse_next(yaml) && as_yaml_capture(yaml, &val);
		as_options_pop(o, len);

		if (!ok) {
			as_val_destroy(key);
			break;
		}

		// A repeated key keeps the last value.
		if (!key || !val || as_orderedmap_set(map, key, val) != 0) {
			as_val_destroy(key);
			as_val_destroy(val);
		}
	}
	as_yaml_sources_destroy(&sources);
	as_orderedmap_destroy(map);
	return false;
}

// Capture the items of a sequence whose start event is consumed, through its end event.
static bool
as_yaml_capture_seq(as_yaml* yaml, uint32_t id, bool keep, as_val** out)
{
	as_yaml_options* o = yaml->opts;
	as_arraylist* list = as_arraylist_new(8, 8);

	for (uint32_t i = 0; as_parse_next(yaml); i++) {
		if (yaml->event.type == YAML_SEQUENCE_END_EVENT) {
			yaml_event_delete(&yaml->event);

			if (keep) {
				as_yaml_anchor_store(yaml, id, (as_val*)list);
				*out = (as_val*)list;
			}
			else {
				as_arraylist_destroy(list);
			}
			return true;
		}

		char segment[16];
		snprintf(segment, sizeof(segment), "[%u]", i);

		as_val* val;
		uint32_t len = as_options_push(o, "", segment);
		bool ok = as_yaml_capture(yaml, &val);
		as_options_pop(o, len);

		if (!ok) {
			break;
		}

		if (val && as_arraylist_append(list, val) != AS_ARRAYLIST_OK) {
			as_val_destroy(val);
		}
	}
	as_arraylist_destroy(list);
	return false;
}

// Capture the value whose first event is current into out: as_string, as_integer, as_double,
// as_boolean, as_nil, as_arraylist or as_orderedmap. An alias to a collection reuses its value.
// Problems that only matter for a registered name are deferred, and out is then NULL.
static bool
as_yaml_capture(as_yaml* yaml, as_val** out)
{
	as_yaml_options* o = yaml->opts;
	char reason[AS_YAML_REASON_SIZE];
	*out = NULL;

	switch (yaml->event.type) {
		case YAML_SCALAR_EVENT:
			*out = as_yaml_option_scalar(&yaml->event, reason, sizeof(reason));

			if (!*out) {
				as_options_defer(o, "Invalid %s: %s", o->cur, reason);
			}
			yaml_event_delete(&yaml->event);
			return true;

		case YAML_MAPPING_START_EVENT:
		case YAML_SEQUENCE_START_EVENT: {
			bool map = yaml->event.type == YAML_MAPPING_START_EVENT;
			bool keep = as_yaml_collection_tag(&yaml->event, map, reason, sizeof(reason));
			uint32_t id = yaml->anchor_id;

			if (!keep) {
				as_options_defer(o, "Invalid %s: %s", o->cur, reason);
			}
			yaml_event_delete(&yaml->event);

			if (map) {
				return as_yaml_capture_map(yaml, id, keep, out);
			}
			return as_yaml_capture_seq(yaml, id, keep, out);
		}

		case YAML_ALIAS_EVENT:
			if (yaml->alias->val) {
				*out = as_val_reserve(yaml->alias->val);
			}
			else {
				as_yaml_alias_reason(yaml->alias, reason, sizeof(reason));
				as_options_defer(o, "Invalid %s: %s", o->cur, reason);
			}
			yaml_event_delete(&yaml->event);
			return true;

		default:
			as_expected_error(yaml, YAML_SCALAR_EVENT);
			yaml_event_delete(&yaml->event);
			return false;
	}
}

// Add a top-level option. Takes val.
static void
as_options_set(as_yaml_options* o, const char* name, as_val* val)
{
	as_string* key = as_string_new_strdup(name);

	if (as_orderedmap_set(o->map, (as_val*)key, val) != 0) {
		as_string_destroy(key);
		as_val_destroy(val);
	}
}

// Parse a provider or translator name: a built-in, else a name registered in code. index is -1
// for a built-in, whose position is in builtin.
static bool
as_parse_plugin_name(
	as_yaml* yaml, const char* path, const char* value, const char* const* builtins,
	uint32_t size, const struct as_plugin_registry_s* registry, uint32_t* builtin,
	int32_t* index
	)
{
	for (uint32_t i = 0; i < size; i++) {
		if (strcmp(value, builtins[i]) == 0) {
			*builtin = i;
			*index = -1;
			return true;
		}
	}

	*index = as_plugin_registry_find(registry, value);

	if (*index >= 0) {
		return true;
	}

	as_string_builder sb;
	as_string_builder_init(&sb, 256, true);

	for (uint32_t i = 0; i < size; i++) {
		if (i > 0) {
			as_string_builder_append(&sb, ", ");
		}
		as_string_builder_append(&sb, builtins[i]);
	}

	uint32_t n = as_plugin_registry_size(registry);

	for (uint32_t i = 0; i < n; i++) {
		as_string_builder_append(&sb, ", ");
		as_string_builder_append(&sb, as_plugin_registry_name(registry, i));
	}

	as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid %s: %s. valid values: %s",
		path, value, sb.data);
	as_string_builder_destroy(&sb);
	return false;
}

static const char*
as_yaml_provider_label(const as_yaml_discovery* d)
{
	return d->provider == AS_YAML_PROVIDER_REGISTERED ? d->provider_name :
		as_config_builtin_providers[d->provider - 1];
}

static const char*
as_yaml_translator_label(const as_yaml_discovery* d)
{
	return d->translator == AS_YAML_TRANSLATOR_REGISTERED ? d->translator_name :
		as_config_builtin_translators[d->translator - 1];
}

// Parse the name/value pairs of a mapping whose start event is consumed, through the end event.
// fn receives each value's first event and consumes the whole value.
static bool
as_parse_block(as_yaml* yaml, const char* path, as_parse_block_fn fn)
{
	while (as_parse_next(yaml)) {
		if (yaml->event.type == YAML_MAPPING_END_EVENT) {
			yaml_event_delete(&yaml->event);
			return true;
		}

		if (yaml->event.type == YAML_ALIAS_EVENT) {
			as_alias_error(yaml, path);
			yaml_event_delete(&yaml->event);
			return false;
		}

		if (yaml->event.type != YAML_SCALAR_EVENT) {
			as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid %s: expected scalar keys, not %s",
				path, as_event_kind(yaml->event.type));
			yaml_event_delete(&yaml->event);
			return false;
		}

		if (as_yaml_is_merge(&yaml->event)) {
			return as_merge_error(yaml, path);
		}

		char name[256];
		as_strncpy(name, as_yaml_scalar(yaml), sizeof(name));
		yaml_event_delete(&yaml->event);

		if (!as_parse_next(yaml)) {
			return false;
		}

		if (yaml->event.type == YAML_ALIAS_EVENT) {
			char key[AS_YAML_PATH_SIZE];
			as_yaml_path(key, path, name);
			as_alias_error(yaml, key);
			yaml_event_delete(&yaml->event);
			return false;
		}

		if (!fn(yaml, path, name)) {
			return false;
		}
	}
	return false;
}

// Parse a sequence of scalars whose start event is consumed into out (char*, owned).
static bool
as_parse_string_seq(as_yaml* yaml, const char* path, as_vector* out)
{
	as_string_builder sb;
	as_string_builder_inita(&sb, 512, false);
	as_string_builder_append(&sb, "Set ");
	as_string_builder_append(&sb, path);
	as_string_builder_append(&sb, " = [");

	for (uint32_t i = 0; as_parse_next(yaml); i++) {
		if (yaml->event.type == YAML_SEQUENCE_END_EVENT) {
			yaml_event_delete(&yaml->event);
			as_string_builder_append_char(&sb, ']');
			as_log_info("%s", sb.data);
			return true;
		}

		if (yaml->event.type == YAML_ALIAS_EVENT) {
			char item[AS_YAML_PATH_SIZE];
			snprintf(item, sizeof(item), "%.200s[%u]", path, i);
			as_alias_error(yaml, item);
			yaml_event_delete(&yaml->event);
			return false;
		}

		if (yaml->event.type != YAML_SCALAR_EVENT) {
			as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid %s[%u]: expected a scalar, not %s",
				path, i, as_event_kind(yaml->event.type));
			yaml_event_delete(&yaml->event);
			return false;
		}

		char* value = cf_strdup(as_yaml_scalar(yaml));
		as_vector_append(out, &value);

		if (i > 0) {
			as_string_builder_append_char(&sb, ',');
		}
		as_string_builder_append(&sb, value);
		yaml_event_delete(&yaml->event);
	}
	return false;
}

// Parse a map of scalars whose start event is consumed into out (as_addr_map, owned).
static bool
as_parse_addr_map(as_yaml* yaml, const char* path, as_vector* out)
{
	while (as_parse_next(yaml)) {
		if (yaml->event.type == YAML_MAPPING_END_EVENT) {
			yaml_event_delete(&yaml->event);
			return true;
		}

		if (yaml->event.type == YAML_ALIAS_EVENT) {
			as_alias_error(yaml, path);
			yaml_event_delete(&yaml->event);
			return false;
		}

		if (yaml->event.type != YAML_SCALAR_EVENT) {
			as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid %s: expected scalar keys, not %s",
				path, as_event_kind(yaml->event.type));
			yaml_event_delete(&yaml->event);
			return false;
		}

		if (as_yaml_is_merge(&yaml->event)) {
			return as_merge_error(yaml, path);
		}

		char* orig = cf_strdup(as_yaml_scalar(yaml));
		yaml_event_delete(&yaml->event);

		if (!as_parse_next(yaml)) {
			cf_free(orig);
			return false;
		}

		if (yaml->event.type == YAML_ALIAS_EVENT) {
			char item[AS_YAML_PATH_SIZE];
			snprintf(item, sizeof(item), "%.200s[%.50s]", path, orig);
			as_alias_error(yaml, item);
			yaml_event_delete(&yaml->event);
			cf_free(orig);
			return false;
		}

		if (yaml->event.type != YAML_SCALAR_EVENT) {
			as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid %s[%s]: expected a scalar, not %s",
				path, orig, as_event_kind(yaml->event.type));
			yaml_event_delete(&yaml->event);
			cf_free(orig);
			return false;
		}

		as_addr_map* entry = as_vector_reserve(out);
		entry->orig = orig;
		entry->alt = cf_strdup(as_yaml_scalar(yaml));
		as_log_info("Set %s[%s] = %s", path, entry->orig, entry->alt);
		yaml_event_delete(&yaml->event);
	}
	return false;
}

// Parse a map value into one of the staged address maps; a repeated key keeps the last value.
static bool
as_parse_staged_map(as_yaml* yaml, const char* path, const char* name, as_vector* out, uint32_t flag)
{
	if (yaml->event.type != YAML_MAPPING_START_EVENT) {
		return as_kind_error(yaml, path, name, "a map");
	}
	yaml_event_delete(&yaml->event);

	as_yaml_discovery* d = yaml->disc;
	as_yaml_addr_map_clear(out);
	d->set &= ~flag;

	char key[AS_YAML_PATH_SIZE];
	as_yaml_path(key, path, name);

	if (!as_parse_addr_map(yaml, key, out)) {
		return false;
	}
	d->set |= flag;
	return true;
}

// Parse a sequence value into a staged string list; a repeated key keeps the last value.
static bool
as_parse_staged_seq(as_yaml* yaml, const char* path, const char* name, as_vector* out)
{
	if (yaml->event.type != YAML_SEQUENCE_START_EVENT) {
		return as_kind_error(yaml, path, name, "a sequence");
	}
	yaml_event_delete(&yaml->event);
	as_yaml_strings_clear(out);

	char key[AS_YAML_PATH_SIZE];
	as_yaml_path(key, path, name);
	return as_parse_string_seq(yaml, key, out);
}

static bool
as_parse_dns_field(as_yaml* yaml, const char* path, const char* name)
{
	as_yaml_discovery* d = yaml->disc;

	if (strcmp(name, "names") == 0) {
		return as_parse_staged_seq(yaml, path, name, &d->dns_names);
	}

	if (strcmp(name, "servers") == 0) {
		return as_parse_staged_seq(yaml, path, name, &d->dns_servers);
	}

	if (!as_yaml_known(name, as_yaml_dns_scalars)) {
		return as_skip_field(yaml, path, name);
	}

	if (yaml->event.type != YAML_SCALAR_EVENT) {
		return as_kind_error(yaml, path, name, "a scalar");
	}

	const char* value = as_yaml_scalar(yaml);
	char key[AS_YAML_PATH_SIZE];
	as_yaml_path(key, path, name);
	uint32_t v = 0;
	bool rv = true;

	if (strcmp(name, "record_type") == 0) {
		if ((rv = as_parse_enum(yaml, key, value, as_yaml_record_types,
				AS_YAML_COUNT(as_yaml_record_types), &v))) {
			d->dns.record_type = (as_dns_record_type)v;
		}
	}
	else if (strcmp(name, "port") == 0) {
		if ((rv = parse_uint32_strict(yaml, key, value, 1, UINT16_MAX, &v))) {
			d->dns.port = (uint16_t)v;
		}
	}
	else if (strcmp(name, "tls_name") == 0) {
		as_yaml_set_string(&d->dns.tls_name, value);
	}
	else {
		rv = parse_uint32_strict(yaml, key, value, 1, UINT32_MAX, &d->dns.timeout_ms);
	}

	if (rv) {
		as_log_info("Set %s = %s", key, value);
	}
	yaml_event_delete(&yaml->event);
	return rv;
}

static bool
as_parse_filter_field(as_yaml* yaml, const char* path, const char* name)
{
	as_vector* filters = &yaml->disc->aws_filters;
	as_yaml_filter* filter = as_vector_get(filters, filters->size - 1);

	if (strcmp(name, "values") == 0) {
		filter->has_values = false;

		if (!as_parse_staged_seq(yaml, path, name, &filter->values)) {
			return false;
		}
		filter->has_values = true;
		return true;
	}

	if (strcmp(name, "name") != 0) {
		return as_skip_field(yaml, path, name);
	}

	if (yaml->event.type != YAML_SCALAR_EVENT) {
		return as_kind_error(yaml, path, name, "a scalar");
	}

	cf_free(filter->name);
	filter->name = cf_strdup(as_yaml_scalar(yaml));
	as_log_info("Set %s.%s = %s", path, name, filter->name);
	yaml_event_delete(&yaml->event);
	return true;
}

// Parse the filters sequence whose start event is consumed.
static bool
as_parse_aws_filters(as_yaml* yaml, const char* path)
{
	as_vector* filters = &yaml->disc->aws_filters;
	as_yaml_filters_clear(filters);

	char key[AS_YAML_PATH_SIZE];

	for (uint32_t i = 0; as_parse_next(yaml); i++) {
		if (yaml->event.type == YAML_SEQUENCE_END_EVENT) {
			yaml_event_delete(&yaml->event);
			return true;
		}

		as_string_builder sb;
		as_string_builder_assign(&sb, sizeof(key), key);
		as_string_builder_append(&sb, path);
		as_string_builder_append_char(&sb, '[');
		as_string_builder_append_uint(&sb, i);
		as_string_builder_append_char(&sb, ']');

		if (yaml->event.type != YAML_MAPPING_START_EVENT) {
			as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid %s: expected a map, not %s",
				key, as_event_kind(yaml->event.type));
			yaml_event_delete(&yaml->event);
			return false;
		}
		yaml_event_delete(&yaml->event);

		as_yaml_filter* filter = as_vector_reserve(filters);
		as_vector_init(&filter->values, sizeof(char*), 4);

		if (!as_parse_block(yaml, key, as_parse_filter_field)) {
			return false;
		}

		filter = as_vector_get(filters, filters->size - 1);

		if (!filter->name) {
			as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid %s: name is required", key);
			return false;
		}

		if (!filter->has_values) {
			as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid %s: values is required", key);
			return false;
		}
	}
	return false;
}

static bool
as_parse_aws_field(as_yaml* yaml, const char* path, const char* name)
{
	as_yaml_discovery* d = yaml->disc;
	char key[AS_YAML_PATH_SIZE];
	as_yaml_path(key, path, name);

	if (strcmp(name, "filters") == 0) {
		if (yaml->event.type != YAML_SEQUENCE_START_EVENT) {
			return as_kind_error(yaml, path, name, "a sequence");
		}
		yaml_event_delete(&yaml->event);
		return as_parse_aws_filters(yaml, key);
	}

	if (!as_yaml_known(name, as_yaml_aws_scalars)) {
		return as_skip_field(yaml, path, name);
	}

	if (yaml->event.type != YAML_SCALAR_EVENT) {
		return as_kind_error(yaml, path, name, "a scalar");
	}

	const char* value = as_yaml_scalar(yaml);
	uint32_t v = 0;
	bool rv = true;

	if (strcmp(name, "region") == 0) {
		as_yaml_set_string(&d->aws.region, value);
	}
	else if (strcmp(name, "address_field") == 0) {
		if ((rv = as_parse_enum(yaml, key, value, as_yaml_address_fields,
				AS_YAML_COUNT(as_yaml_address_fields), &v))) {
			d->aws.address_field = (as_aws_address_field)v;
		}
	}
	else if (strcmp(name, "port") == 0) {
		if ((rv = parse_uint32_strict(yaml, key, value, 1, UINT16_MAX, &v))) {
			d->aws.port = (uint16_t)v;
		}
	}
	else if (strcmp(name, "tls_name") == 0) {
		as_yaml_set_string(&d->aws.tls_name, value);
	}
	else if (strcmp(name, "timeout_ms") == 0) {
		rv = parse_uint32_strict(yaml, key, value, 1, UINT32_MAX, &d->aws.timeout_ms);
	}
	else if (strcmp(name, "init_timeout_ms") == 0) {
		if ((rv = parse_uint32_strict(yaml, key, value, 1, UINT32_MAX, &v))) {
			as_log_info("Ignore %s = %s: the first refresh is bounded by "
				"static.client.discovery.refresh_timeout_ms", key, value);
		}
		yaml_event_delete(&yaml->event);
		return rv;
	}
	else {
		as_yaml_set_string(&d->aws.endpoint, value);
	}

	if (rv) {
		as_log_info("Set %s = %s", key, value);
	}
	yaml_event_delete(&yaml->event);
	return rv;
}

static bool
as_parse_discovery_field(as_yaml* yaml, const char* path, const char* name)
{
	as_yaml_discovery* d = yaml->disc;
	char key[AS_YAML_PATH_SIZE];
	as_yaml_path(key, path, name);

	if (strcmp(name, "dns") == 0) {
		if (yaml->event.type != YAML_MAPPING_START_EVENT) {
			return as_kind_error(yaml, path, name, "a map");
		}
		yaml_event_delete(&yaml->event);
		as_yaml_dns_reset(d);
		d->set |= AS_YAML_SET_DNS;
		return as_parse_block(yaml, key, as_parse_dns_field);
	}

	if (strcmp(name, "aws") == 0) {
		if (yaml->event.type != YAML_MAPPING_START_EVENT) {
			return as_kind_error(yaml, path, name, "a map");
		}
		yaml_event_delete(&yaml->event);
		as_yaml_aws_reset(d);
		d->set |= AS_YAML_SET_AWS;
		return as_parse_block(yaml, key, as_parse_aws_field);
	}

	if (yaml->event.type != YAML_SCALAR_EVENT) {
		return as_kind_error(yaml, path, name, "a scalar");
	}

	as_discovery_config* discovery = &yaml->config->discovery;
	const char* value = as_yaml_scalar(yaml);
	uint32_t v = 0;
	bool rv;

	if (strcmp(name, "provider") == 0) {
		int32_t index;

		if ((rv = as_parse_plugin_name(yaml, key, value, as_config_builtin_providers,
				AS_CONFIG_BUILTIN_PROVIDERS, discovery->registry, &v, &index))) {
			cf_free(d->provider_name);
			d->provider_name = NULL;

			if (index >= 0) {
				d->provider = AS_YAML_PROVIDER_REGISTERED;
				d->provider_name = cf_strdup(value);
				d->provider_index = (uint32_t)index;
			}
			else {
				d->provider = (as_yaml_provider)(v + 1);
			}
		}
	}
	else if (strcmp(name, "merge_policy") == 0) {
		if ((rv = as_parse_enum(yaml, key, value, as_yaml_merge_policies,
				AS_YAML_COUNT(as_yaml_merge_policies), &v))) {
			discovery->merge_policy = (as_seed_merge_policy)v;
			d->set |= AS_YAML_SET_MERGE_POLICY;
		}
	}
	else if (strcmp(name, "execution_mode") == 0) {
		if ((rv = as_parse_enum(yaml, key, value, as_yaml_execution_modes,
				AS_YAML_COUNT(as_yaml_execution_modes), &v))) {
			discovery->execution_mode = (as_discovery_execution_mode)v;
			d->set |= AS_YAML_SET_EXECUTION_MODE;
		}
	}
	else if (strcmp(name, "refresh_interval_ms") == 0) {
		if ((rv = parse_uint32_strict(yaml, key, value, 0, UINT32_MAX,
				&discovery->refresh_interval_ms))) {
			d->set |= AS_YAML_SET_REFRESH_INTERVAL;
		}
	}
	else if (strcmp(name, "refresh_timeout_ms") == 0) {
		if ((rv = parse_uint32_strict(yaml, key, value, 0, UINT32_MAX,
				&discovery->refresh_timeout_ms))) {
			d->set |= AS_YAML_SET_REFRESH_TIMEOUT;
		}
	}
	else {
		if ((rv = parse_uint32_strict(yaml, key, value, 0, UINT32_MAX,
				&discovery->tend_refresh_deadline_ms))) {
			d->set |= AS_YAML_SET_TEND_DEADLINE;
		}
	}

	if (rv) {
		as_log_info("Set %s = %s", key, value);
	}
	yaml_event_delete(&yaml->event);
	return rv;
}

static bool
as_parse_translation_field(as_yaml* yaml, const char* path, const char* name)
{
	as_yaml_discovery* d = yaml->disc;

	if (strcmp(name, "static_map") == 0) {
		return as_parse_staged_map(yaml, path, name, &d->static_map, AS_YAML_SET_STATIC_MAP);
	}

	if (strcmp(name, "port_map") == 0) {
		return as_parse_staged_map(yaml, path, name, &d->port_map, AS_YAML_SET_PORT_MAP);
	}

	if (yaml->event.type != YAML_SCALAR_EVENT) {
		return as_kind_error(yaml, path, name, "a scalar");
	}

	const char* value = as_yaml_scalar(yaml);
	char key[AS_YAML_PATH_SIZE];
	as_yaml_path(key, path, name);
	uint32_t v = 0;
	bool rv;

	if (strcmp(name, "provider") == 0) {
		int32_t index;

		if ((rv = as_parse_plugin_name(yaml, key, value, as_config_builtin_translators,
				AS_CONFIG_BUILTIN_TRANSLATORS, yaml->config->translation.registry, &v, &index))) {
			cf_free(d->translator_name);
			d->translator_name = NULL;

			if (index >= 0) {
				d->translator = AS_YAML_TRANSLATOR_REGISTERED;
				d->translator_name = cf_strdup(value);
				d->translator_index = (uint32_t)index;
			}
			else {
				d->translator = (as_yaml_translator)(v + 1);
			}
		}
	}
	else {
		if ((rv = as_parse_enum(yaml, key, value, as_yaml_miss_policies,
				AS_YAML_COUNT(as_yaml_miss_policies), &v))) {
			yaml->config->translation.on_miss = (as_translation_miss_policy)v;
			d->set |= AS_YAML_SET_ON_MISS;
		}
	}

	if (rv) {
		as_log_info("Set %s = %s", key, value);
	}
	yaml_event_delete(&yaml->event);
	return rv;
}

static bool
as_yaml_section(const char* name, bool discovery)
{
	return discovery ? strcmp(name, "dns") == 0 || strcmp(name, "aws") == 0 :
		strcmp(name, "static_map") == 0 || strcmp(name, "port_map") == 0;
}

// A key the client parses itself: provider, a setting or a built-in section.
static bool
as_yaml_block_key(const char* name, bool discovery)
{
	return as_yaml_section(name, discovery) || as_yaml_known(name,
		discovery ? as_yaml_discovery_scalars : as_yaml_translation_scalars);
}

static bool
as_yaml_names_has(const as_vector* names, const char* name)
{
	for (uint32_t i = 0; i < names->size; i++) {
		if (strcmp(*(char**)as_vector_get((as_vector*)names, i), name) == 0) {
			return true;
		}
	}
	return false;
}

// A key of the discovery or translation block whose value's first event is current. Every key
// except provider and the built-in sections is captured as a factory option.
static bool
as_parse_plugin_field(as_yaml* yaml, const char* path, const char* name, bool discovery)
{
	as_yaml_options* o = yaml->opts;
	char key[AS_YAML_PATH_SIZE];
	as_yaml_path(key, path, name);

	bool section = as_yaml_section(name, discovery);
	bool known = as_yaml_block_key(name, discovery);

	if (known && yaml->event.type == YAML_ALIAS_EVENT) {
		as_alias_error(yaml, key);
		yaml_event_delete(&yaml->event);
		return false;
	}

	if (!known) {
		as_val* val;
		uint32_t len = as_options_push(o, ".", name);
		bool rv = as_yaml_capture(yaml, &val);
		as_options_pop(o, len);

		if (!rv) {
			return false;
		}

		if (val) {
			as_options_set(o, name, val);
		}

		char* unknown = cf_strdup(name);
		as_vector_append(&o->unknown, &unknown);
		return true;
	}

	if (!section && strcmp(name, "provider") != 0 && yaml->event.type == YAML_SCALAR_EVENT) {
		// Typed here, then parsed by the client as usual.
		char reason[AS_YAML_REASON_SIZE];
		as_val* val = as_yaml_option_scalar(&yaml->event, reason, sizeof(reason));

		if (val) {
			as_options_set(o, name, val);
		}
		else {
			as_options_defer(o, "Invalid %s: %s", key, reason);
		}
	}

	if (!section) {
		return discovery ? as_parse_discovery_field(yaml, path, name) :
			as_parse_translation_field(yaml, path, name);
	}

	yaml->section = true;
	bool rv = discovery ? as_parse_discovery_field(yaml, path, name) :
		as_parse_translation_field(yaml, path, name);
	yaml->section = false;
	return rv;
}

// The value of a merge key directly under a block, whose first event is current. Any problem with
// it fails the file, whatever the provider: the merged keys may be client settings.
static bool
as_parse_block_merge(as_yaml* yaml)
{
	as_yaml_options* o = yaml->opts;
	char saved[sizeof(o->deferred)];
	memcpy(saved, o->deferred, sizeof(saved));
	o->deferred[0] = '\0';

	bool rv = as_yaml_merge_sources(yaml, &o->sources);

	if (rv && o->deferred[0]) {
		as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "%s", o->deferred);
		rv = false;
	}
	memcpy(o->deferred, saved, sizeof(saved));
	return rv;
}

// Apply the block's merge keys after its own keys, which win, as do earlier sources. Java applies
// a merged provider, setting or section to the client; the client does not, so it fails the file.
static bool
as_apply_block_merges(as_yaml* yaml, const char* path, bool discovery)
{
	as_yaml_options* o = yaml->opts;

	for (uint32_t i = 0; i < o->sources.size; i++) {
		as_orderedmap* src = *(as_orderedmap**)as_vector_get(&o->sources, i);
		as_orderedmap_iterator it;
		as_orderedmap_iterator_init(&it, src);
		bool rv = true;

		while (rv && as_orderedmap_iterator_has_next(&it)) {
			as_pair* p = (as_pair*)as_orderedmap_iterator_next(&it);
			as_val* key = as_pair_1(p);

			if (as_val_type(key) != AS_STRING) {
				char* s = as_val_tostring(key);
				as_options_defer(o, "Invalid key '%s' in %s: expected a string", s, path);

				if (as_yaml_names_has(&o->unknown, s)) {
					cf_free(s);
				}
				else {
					as_vector_append(&o->unknown, &s);
				}
				continue;
			}

			const char* name = as_string_get((as_string*)key);

			if (as_yaml_names_has(&o->names, name) || as_orderedmap_get(o->map, key)) {
				continue;
			}

			if (as_yaml_block_key(name, discovery)) {
				as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM,
					"Invalid %s.%s: client settings from YAML merge keys (<<) are not supported",
					path, name);
				rv = false;
				continue;
			}

			as_options_set(o, name, as_val_reserve(as_pair_2(p)));
			char* unknown = cf_strdup(name);
			as_vector_append(&o->unknown, &unknown);
		}
		as_orderedmap_iterator_destroy(&it);

		if (!rv) {
			return false;
		}
	}
	return true;
}

static bool
as_parse_plugin_pairs(as_yaml* yaml, const char* path, bool discovery)
{
	as_yaml_options* o = yaml->opts;

	while (as_parse_next(yaml)) {
		if (yaml->event.type == YAML_MAPPING_END_EVENT) {
			yaml_event_delete(&yaml->event);
			return as_apply_block_merges(yaml, path, discovery);
		}

		if (yaml->event.type != YAML_SCALAR_EVENT) {
			as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "Invalid %s: expected scalar keys, not %s",
				path, as_event_kind(yaml->event.type));
			yaml_event_delete(&yaml->event);
			return false;
		}

		if (as_yaml_is_merge(&yaml->event)) {
			yaml_event_delete(&yaml->event);

			if (!as_parse_next(yaml) || !as_parse_block_merge(yaml)) {
				return false;
			}
			continue;
		}

		// Full key: a factory receives it whole.
		char* name = cf_strdup(as_yaml_scalar(yaml));
		char reason[AS_YAML_REASON_SIZE];
		as_val* k = as_yaml_option_scalar(&yaml->event, reason, sizeof(reason));
		bool string_key = as_val_type(k) == AS_STRING;

		if (!k) {
			as_options_defer(o, "Invalid %s: %s", path, reason);
		}
		else if (!string_key) {
			as_options_defer(o, "Invalid key '%s' in %s: expected a string", name, path);
		}
		as_val_destroy(k);
		yaml_event_delete(&yaml->event);

		if (!as_parse_next(yaml)) {
			cf_free(name);
			return false;
		}

		bool rv;

		if (string_key) {
			rv = as_parse_plugin_field(yaml, path, name, discovery);
			as_vector_append(&o->names, &name);
		}
		else {
			// An unexpected key for a built-in provider.
			as_vector_append(&o->unknown, &name);
			rv = as_skip_value(yaml);
		}

		if (!rv) {
			return false;
		}
	}
	return false;
}

// Parse a discovery or translation block whose start event is consumed. Its options are kept
// for a registered provider; for a built-in one the unexpected keys are logged instead.
static bool
as_parse_plugin_block(as_yaml* yaml, const char* path, bool discovery)
{
	as_yaml_options o;
	o.map = as_orderedmap_new(8);
	as_vector_init(&o.unknown, sizeof(char*), 4);
	as_vector_init(&o.names, sizeof(char*), 8);
	as_vector_init(&o.sources, sizeof(as_orderedmap*), 2);
	o.deferred[0] = '\0';
	o.len = 0;
	o.cur[0] = '\0';
	as_options_push(&o, "", path);

	// The caller consumed the block's start event, so an anchor on it is the innermost open one.
	as_yaml_anchor* block = as_yaml_anchor_by_id(yaml, yaml->open[yaml->depth - 1]);

	if (block) {
		block->origin = AS_YAML_ORIGIN_BLOCK;
	}

	yaml->opts = &o;
	bool rv = as_parse_plugin_pairs(yaml, path, discovery);
	yaml->opts = NULL;

	as_yaml_discovery* d = yaml->disc;
	bool registered = discovery ? d->provider == AS_YAML_PROVIDER_REGISTERED :
		d->translator == AS_YAML_TRANSLATOR_REGISTERED;

	if (rv && registered) {
		if (o.deferred[0]) {
			as_error_update(&yaml->err, AEROSPIKE_ERR_PARAM, "%s", o.deferred);
			rv = false;
		}
		else {
			as_map** options = discovery ? &d->provider_options : &d->translator_options;
			as_val_destroy(*options);
			*options = (as_map*)o.map;
			o.map = NULL;
		}
	}

	for (uint32_t i = 0; i < o.unknown.size; i++) {
		char* name = *(char**)as_vector_get(&o.unknown, i);

		if (rv && !registered) {
			as_log_info("Unexpected field: %s.%s", path, name);
		}
		cf_free(name);
	}
	as_vector_destroy(&o.unknown);

	for (uint32_t i = 0; i < o.names.size; i++) {
		cf_free(*(char**)as_vector_get(&o.names, i));
	}
	as_vector_destroy(&o.names);
	as_yaml_sources_destroy(&o.sources);

	if (o.map) {
		as_orderedmap_destroy(o.map);
	}
	return rv;
}

static bool
as_parse_static_client_scalar(as_yaml* yaml, const char* path, const char* name)
{
	const char* value = as_yaml_scalar(yaml);
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
	else if (strcmp(name, "use_services_alternate") == 0) {
		char key[AS_YAML_PATH_SIZE];
		as_yaml_path(key, path, name);
		as_yaml_discovery* d = yaml->disc;

		if ((rv = parse_bool(yaml, key, value, &d->use_services_alternate))) {
			d->set |= AS_YAML_SET_USE_SERVICES_ALTERNATE;
			as_log_info("Set %s = %s", key, value);
		}
	}
	else {
		as_log_info("Unexpected field: %s.%s", yaml->name, name);
		rv = true; // Skip unknown scalars.
	}

	yaml_event_delete(&yaml->event);
	return rv;
}

static bool
as_parse_static_client_field(as_yaml* yaml, const char* path, const char* name)
{
	as_yaml_discovery* d = yaml->disc;
	char key[AS_YAML_PATH_SIZE];
	as_yaml_path(key, path, name);

	if (strcmp(name, "ip_map") == 0) {
		return as_parse_staged_map(yaml, path, name, &d->ip_map, AS_YAML_SET_IP_MAP);
	}

	if (strcmp(name, "discovery") == 0 || strcmp(name, "translation") == 0) {
		if (yaml->event.type != YAML_MAPPING_START_EVENT) {
			return as_kind_error(yaml, path, name, "a map");
		}
		yaml_event_delete(&yaml->event);
		return as_parse_plugin_block(yaml, key, name[0] == 'd');
	}

	switch (yaml->event.type) {
		case YAML_SCALAR_EVENT:
			return as_parse_static_client_scalar(yaml, path, name);

		case YAML_MAPPING_START_EVENT:
		case YAML_SEQUENCE_START_EVENT: {
			if (as_yaml_known(name, as_yaml_static_client_scalars)) {
				return as_kind_error(yaml, path, name, "a scalar");
			}

			bool map = yaml->event.type == YAML_MAPPING_START_EVENT;
			yaml_event_delete(&yaml->event);

			if (map) {
				as_log_info("Unexpected section: %s.%s", yaml->name, name);
				return as_skip_open_mapping(yaml);
			}
			as_log_info("Unexpected sequence: %s.%s", yaml->name, name);
			return as_skip_sequence(yaml);
		}

		default:
			as_expected_error(yaml, YAML_SCALAR_EVENT);
			yaml_event_delete(&yaml->event);
			return false;
	}
}

static bool
as_parse_static_client(as_yaml* yaml)
{
	if (!as_expect_event(yaml, YAML_MAPPING_START_EVENT)) {
		return false;
	}

	yaml->name = "client";
	return as_parse_block(yaml, "static.client", as_parse_static_client_field);
}

static bool
as_parse_dynamic_client(as_yaml* yaml)
{
	if (!as_expect_event(yaml, YAML_MAPPING_START_EVENT)) {
		return false;
	}

	yaml->name = "client";

	char name[256];
	bool end;

	while (as_parse_key(yaml, "dynamic.client", name, sizeof(name), &end)) {
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
				rv = as_parse_vector_int32(yaml, "dynamic.client", name, &yaml->config->rack_ids,
					AS_RACK_IDS);
			}
			else {
				as_log_info("Unexpected sequence: %s.%s", yaml->name, name);
				rv = as_skip_sequence(yaml);
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
	return end;
}

static bool
as_parse_static(as_yaml* yaml)
{
	if (!as_expect_event(yaml, YAML_MAPPING_START_EVENT)) {
		return false;
	}

	char name[256];
	bool rv;
	bool end;

	while (as_parse_key(yaml, "static", name, sizeof(name), &end)) {
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
	return end;
}

static bool
as_parse_dynamic(as_yaml* yaml)
{
	if (!as_expect_event(yaml, YAML_MAPPING_START_EVENT)) {
		return false;
	}

	char name[256];
	bool rv;
	bool end;

	while (as_parse_key(yaml, "dynamic", name, sizeof(name), &end)) {
		if (strcmp(name, "client") == 0) {
			rv = as_parse_dynamic_client(yaml);
		}
		else if (strcmp(name, "read") == 0) {
			rv = as_parse_policy(yaml, name, as_parse_read, NULL);
		}
		else if (strcmp(name, "write") == 0) {
			rv = as_parse_policy(yaml, name, as_parse_write, NULL);
		}
		else if (strcmp(name, "query") == 0) {
			rv = as_parse_policy(yaml, name, as_parse_query, NULL);
		}
		else if (strcmp(name, "scan") == 0) {
			rv = as_parse_policy(yaml, name, as_parse_scan, NULL);
		}
		else if (strcmp(name, "batch_read") == 0) {
			rv = as_parse_policy(yaml, name, as_parse_batch_read, NULL);
		}
		else if (strcmp(name, "batch_write") == 0) {
			rv = as_parse_policy(yaml, name, as_parse_batch_write, NULL);
		}
		else if (strcmp(name, "batch_udf") == 0) {
			rv = as_parse_policy(yaml, name, as_parse_batch_udf, NULL);
		}
		else if (strcmp(name, "batch_delete") == 0) {
			rv = as_parse_policy(yaml, name, as_parse_batch_delete, NULL);
		}
		else if (strcmp(name, "txn_verify") == 0) {
			rv = as_parse_policy(yaml, name, as_parse_txn_verify, NULL);
		}
		else if (strcmp(name, "txn_roll") == 0) {
			rv = as_parse_policy(yaml, name, as_parse_txn_roll, NULL);
		}
		else if (strcmp(name, "metrics") == 0) {
			rv = as_parse_policy(yaml, name, as_parse_metrics, as_metrics_maps);
		}
		else {
			as_log_info("Unexpected section: %s", name);
			rv = as_skip_section(yaml);
		}

		if (!rv) {
			return false;
		}
	}
	return end;
}

static bool
as_parse_version(as_yaml* yaml)
{
	char version[256];

	if (!as_parse_scalar(yaml, version, sizeof(version))) {
		return false;
	}

	if (strcmp(version, "1.0.0") != 0 && strcmp(version, "1.1.0") != 0 &&
			strcmp(version, "1.2.0") != 0) {
		// Log warning and continue parsing on version mismatch.
		as_log_warn("Unexpected dynamic config file version: %s expected: 1.0.0, 1.1.0, or 1.2.0", version);
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
	bool end;

	while (as_parse_key(yaml, NULL, name, sizeof(name), &end)) {
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
	return end;
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
as_config_file_read(
	aerospike* as, as_config* config, uint8_t* bitmap, bool init, as_yaml_discovery* disc,
	as_error* err
	)
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
	yaml.name = NULL;
	yaml.disc = disc;
	yaml.opts = NULL;
	yaml.alias = NULL;
	yaml.anchor_id = 0;
	yaml.ids = 0;
	yaml.aliases = 0;
	yaml.depth = 0;
	yaml.section = false;
	yaml.failed = false;
	yaml.init = init;
	as_error_init(&yaml.err);

	if (!yaml_parser_initialize(&yaml.parser)) {
		fclose(fp);
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to initialize yaml parser");
	}

	yaml_parser_set_input_file(&yaml.parser, fp);
	as_vector_init(&yaml.anchors, sizeof(as_yaml_anchor), 8);

	bool rv = as_parse_yaml(&yaml);
	//bool rv = parse_debug(&yaml.parser);

	// A syntax error can surface as an unrelated event error. The parser problem is the cause.
	if (yaml.parser.error != YAML_NO_ERROR) {
		as_parser_error(&yaml);
		rv = false;
	}

	if (yaml.failed) {
		rv = false;
	}

	yaml_parser_delete(&yaml.parser);
	as_yaml_anchors_destroy(&yaml);
	fclose(fp);

	if (!rv) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to parse: %s\n%s",
			path, yaml.err.message);
	}

	// metrics.exporter file overrides an empty application report_dir only when
	// metrics.report_dir is absent. An explicit empty metrics.report_dir stays
	// empty and does not install the file writer.
	as_metrics_policy* metrics = &config->policies.metrics;

	if (as_field_is_set(bitmap, AS_METRICS_EXPORTER) &&
			!as_field_is_set(bitmap, AS_METRICS_REPORT_DIR) &&
			metrics->metrics_exporter == AS_METRICS_BUILTIN_EXPORTER_FILE &&
			metrics->report_dir[0] == '\0') {
		as_strncpy(metrics->report_dir, ".", sizeof(metrics->report_dir));
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
	trg->read.base.connect_timeout = as_field_is_set(bitmap, AS_READ_CONNECT_TIMEOUT)?
		src->read.base.connect_timeout : orig->read.base.connect_timeout;
	trg->read.base.socket_timeout = as_field_is_set(bitmap, AS_READ_SOCKET_TIMEOUT)?
		src->read.base.socket_timeout : orig->read.base.socket_timeout;
	trg->read.base.total_timeout = as_field_is_set(bitmap, AS_READ_TOTAL_TIMEOUT)?
		src->read.base.total_timeout : orig->read.base.total_timeout;
	trg->read.base.timeout_delay = as_field_is_set(bitmap, AS_READ_TIMEOUT_DELAY)?
		src->read.base.timeout_delay : orig->read.base.timeout_delay;
	trg->read.base.max_retries = as_field_is_set(bitmap, AS_READ_MAX_RETRIES)?
		src->read.base.max_retries : orig->read.base.max_retries;
	trg->read.base.sleep_between_retries = as_field_is_set(bitmap, AS_READ_SLEEP_BETWEEN_RETRIES)?
		src->read.base.sleep_between_retries : orig->read.base.sleep_between_retries;
	trg->read.base.error_detail_verbosity = as_field_is_set(bitmap, AS_READ_ERROR_DETAIL)?
		src->read.base.error_detail_verbosity : orig->read.base.error_detail_verbosity;
	trg->read.read_mode_ap = as_field_is_set(bitmap, AS_READ_READ_MODE_AP)?
		src->read.read_mode_ap : orig->read.read_mode_ap;
	trg->read.read_mode_sc = as_field_is_set(bitmap, AS_READ_READ_MODE_SC)?
		src->read.read_mode_sc : orig->read.read_mode_sc;
	trg->read.replica = as_field_is_set(bitmap, AS_READ_REPLICA)?
		src->read.replica : orig->read.replica;

	trg->write.base.connect_timeout = as_field_is_set(bitmap, AS_WRITE_CONNECT_TIMEOUT)?
		src->write.base.connect_timeout : orig->write.base.connect_timeout;
	trg->write.base.socket_timeout = as_field_is_set(bitmap, AS_WRITE_SOCKET_TIMEOUT)?
		src->write.base.socket_timeout : orig->write.base.socket_timeout;
	trg->write.base.total_timeout = as_field_is_set(bitmap, AS_WRITE_TOTAL_TIMEOUT)?
		src->write.base.total_timeout : orig->write.base.total_timeout;
	trg->write.base.timeout_delay = as_field_is_set(bitmap, AS_WRITE_TIMEOUT_DELAY)?
		src->write.base.timeout_delay : orig->write.base.timeout_delay;
	trg->write.base.max_retries = as_field_is_set(bitmap, AS_WRITE_MAX_RETRIES)?
		src->write.base.max_retries : orig->write.base.max_retries;
	trg->write.base.sleep_between_retries = as_field_is_set(bitmap, AS_WRITE_SLEEP_BETWEEN_RETRIES)?
		src->write.base.sleep_between_retries : orig->write.base.sleep_between_retries;
	trg->write.base.error_detail_verbosity = as_field_is_set(bitmap, AS_WRITE_ERROR_DETAIL)?
		src->write.base.error_detail_verbosity : orig->write.base.error_detail_verbosity;
	trg->write.replica = as_field_is_set(bitmap, AS_WRITE_REPLICA)?
		src->write.replica : orig->write.replica;
	trg->write.durable_delete = as_field_is_set(bitmap, AS_WRITE_DURABLE_DELETE)?
		src->write.durable_delete : orig->write.durable_delete;
	trg->write.key = as_field_is_set(bitmap, AS_WRITE_SEND_KEY)?
		src->write.key : orig->write.key;

	trg->operate.base.connect_timeout = as_field_is_set(bitmap, AS_WRITE_CONNECT_TIMEOUT)?
		src->operate.base.connect_timeout : orig->operate.base.connect_timeout;
	trg->operate.base.socket_timeout = as_field_is_set(bitmap, AS_WRITE_SOCKET_TIMEOUT)?
		src->operate.base.socket_timeout : orig->operate.base.socket_timeout;
	trg->operate.base.total_timeout = as_field_is_set(bitmap, AS_WRITE_TOTAL_TIMEOUT)?
		src->operate.base.total_timeout : orig->operate.base.total_timeout;
	trg->operate.base.timeout_delay = as_field_is_set(bitmap, AS_WRITE_TIMEOUT_DELAY)?
		src->operate.base.timeout_delay : orig->operate.base.timeout_delay;
	trg->operate.base.max_retries = as_field_is_set(bitmap, AS_WRITE_MAX_RETRIES)?
		src->operate.base.max_retries : orig->operate.base.max_retries;
	trg->operate.base.sleep_between_retries = as_field_is_set(bitmap, AS_WRITE_SLEEP_BETWEEN_RETRIES)?
		src->operate.base.sleep_between_retries : orig->operate.base.sleep_between_retries;
	trg->operate.base.error_detail_verbosity = as_field_is_set(bitmap, AS_WRITE_ERROR_DETAIL)?
		src->operate.base.error_detail_verbosity : orig->operate.base.error_detail_verbosity;
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

	trg->apply.base.connect_timeout = as_field_is_set(bitmap, AS_WRITE_CONNECT_TIMEOUT)?
		src->apply.base.connect_timeout : orig->apply.base.connect_timeout;
	trg->apply.base.socket_timeout = as_field_is_set(bitmap, AS_WRITE_SOCKET_TIMEOUT)?
		src->apply.base.socket_timeout : orig->apply.base.socket_timeout;
	trg->apply.base.total_timeout = as_field_is_set(bitmap, AS_WRITE_TOTAL_TIMEOUT)?
		src->apply.base.total_timeout : orig->apply.base.total_timeout;
	trg->apply.base.timeout_delay = as_field_is_set(bitmap, AS_WRITE_TIMEOUT_DELAY)?
		src->apply.base.timeout_delay : orig->apply.base.timeout_delay;
	trg->apply.base.max_retries = as_field_is_set(bitmap, AS_WRITE_MAX_RETRIES)?
		src->apply.base.max_retries : orig->apply.base.max_retries;
	trg->apply.base.sleep_between_retries = as_field_is_set(bitmap, AS_WRITE_SLEEP_BETWEEN_RETRIES)?
		src->apply.base.sleep_between_retries : orig->apply.base.sleep_between_retries;
	trg->apply.base.error_detail_verbosity = as_field_is_set(bitmap, AS_WRITE_ERROR_DETAIL)?
		src->apply.base.error_detail_verbosity : orig->apply.base.error_detail_verbosity;
	trg->apply.replica = as_field_is_set(bitmap, AS_WRITE_REPLICA)?
		src->apply.replica : orig->apply.replica;
	trg->apply.durable_delete = as_field_is_set(bitmap, AS_WRITE_DURABLE_DELETE)?
		src->apply.durable_delete : orig->apply.durable_delete;
	trg->apply.key = as_field_is_set(bitmap, AS_WRITE_SEND_KEY)?
		src->apply.key : orig->apply.key;

	trg->remove.base.connect_timeout = as_field_is_set(bitmap, AS_WRITE_CONNECT_TIMEOUT)?
		src->remove.base.connect_timeout : orig->remove.base.connect_timeout;
	trg->remove.base.socket_timeout = as_field_is_set(bitmap, AS_WRITE_SOCKET_TIMEOUT)?
		src->remove.base.socket_timeout : orig->remove.base.socket_timeout;
	trg->remove.base.total_timeout = as_field_is_set(bitmap, AS_WRITE_TOTAL_TIMEOUT)?
		src->remove.base.total_timeout : orig->remove.base.total_timeout;
	trg->remove.base.timeout_delay = as_field_is_set(bitmap, AS_WRITE_TIMEOUT_DELAY)?
		src->remove.base.timeout_delay : orig->remove.base.timeout_delay;
	trg->remove.base.max_retries = as_field_is_set(bitmap, AS_WRITE_MAX_RETRIES)?
		src->remove.base.max_retries : orig->remove.base.max_retries;
	trg->remove.base.sleep_between_retries = as_field_is_set(bitmap, AS_WRITE_SLEEP_BETWEEN_RETRIES)?
		src->remove.base.sleep_between_retries : orig->remove.base.sleep_between_retries;
	trg->remove.base.error_detail_verbosity = as_field_is_set(bitmap, AS_WRITE_ERROR_DETAIL)?
		src->remove.base.error_detail_verbosity : orig->remove.base.error_detail_verbosity;
	trg->remove.replica = as_field_is_set(bitmap, AS_WRITE_REPLICA)?
		src->remove.replica : orig->remove.replica;
	trg->remove.durable_delete = as_field_is_set(bitmap, AS_WRITE_DURABLE_DELETE)?
		src->remove.durable_delete : orig->remove.durable_delete;
	trg->remove.key = as_field_is_set(bitmap, AS_WRITE_SEND_KEY)?
		src->remove.key : orig->remove.key;

	trg->scan.base.connect_timeout = as_field_is_set(bitmap, AS_SCAN_CONNECT_TIMEOUT)?
		src->scan.base.connect_timeout : orig->scan.base.connect_timeout;
	trg->scan.base.socket_timeout = as_field_is_set(bitmap, AS_SCAN_SOCKET_TIMEOUT)?
		src->scan.base.socket_timeout : orig->scan.base.socket_timeout;
	trg->scan.base.total_timeout = as_field_is_set(bitmap, AS_SCAN_TOTAL_TIMEOUT)?
		src->scan.base.total_timeout : orig->scan.base.total_timeout;
	trg->scan.base.timeout_delay = as_field_is_set(bitmap, AS_SCAN_TIMEOUT_DELAY)?
		src->scan.base.timeout_delay : orig->scan.base.timeout_delay;
	trg->scan.base.max_retries = as_field_is_set(bitmap, AS_SCAN_MAX_RETRIES)?
		src->scan.base.max_retries : orig->scan.base.max_retries;
	trg->scan.base.sleep_between_retries = as_field_is_set(bitmap, AS_SCAN_SLEEP_BETWEEN_RETRIES)?
		src->scan.base.sleep_between_retries : orig->scan.base.sleep_between_retries;
	trg->scan.base.error_detail_verbosity = as_field_is_set(bitmap, AS_SCAN_ERROR_DETAIL)?
		src->scan.base.error_detail_verbosity : orig->scan.base.error_detail_verbosity;
	trg->scan.replica = as_field_is_set(bitmap, AS_SCAN_REPLICA)?
		src->scan.replica : orig->scan.replica;

	trg->query.base.connect_timeout = as_field_is_set(bitmap, AS_QUERY_CONNECT_TIMEOUT)?
		src->query.base.connect_timeout : orig->query.base.connect_timeout;
	trg->query.base.socket_timeout = as_field_is_set(bitmap, AS_QUERY_SOCKET_TIMEOUT)?
		src->query.base.socket_timeout : orig->query.base.socket_timeout;
	trg->query.base.total_timeout = as_field_is_set(bitmap, AS_QUERY_TOTAL_TIMEOUT)?
		src->query.base.total_timeout : orig->query.base.total_timeout;
	trg->query.base.timeout_delay = as_field_is_set(bitmap, AS_QUERY_TIMEOUT_DELAY)?
		src->query.base.timeout_delay : orig->query.base.timeout_delay;
	trg->query.base.max_retries = as_field_is_set(bitmap, AS_QUERY_MAX_RETRIES)?
		src->query.base.max_retries : orig->query.base.max_retries;
	trg->query.base.sleep_between_retries = as_field_is_set(bitmap, AS_QUERY_SLEEP_BETWEEN_RETRIES)?
		src->query.base.sleep_between_retries : orig->query.base.sleep_between_retries;
	trg->query.base.error_detail_verbosity = as_field_is_set(bitmap, AS_QUERY_ERROR_DETAIL)?
		src->query.base.error_detail_verbosity : orig->query.base.error_detail_verbosity;
	trg->query.replica = as_field_is_set(bitmap, AS_QUERY_REPLICA)?
		src->query.replica : orig->query.replica;
	trg->query.info_timeout = as_field_is_set(bitmap, AS_QUERY_INFO_TIMEOUT)?
		src->query.info_timeout : orig->query.info_timeout;
	trg->query.expected_duration = as_field_is_set(bitmap, AS_QUERY_EXPECTED_DURATION)?
		src->query.expected_duration : orig->query.expected_duration;

	trg->batch.base.connect_timeout = as_field_is_set(bitmap, AS_BATCH_PARENT_READ + AS_BATCH_CONNECT_TIMEOUT)?
		src->batch.base.connect_timeout : orig->batch.base.connect_timeout;
	trg->batch.base.socket_timeout = as_field_is_set(bitmap, AS_BATCH_PARENT_READ + AS_BATCH_SOCKET_TIMEOUT)?
		src->batch.base.socket_timeout : orig->batch.base.socket_timeout;
	trg->batch.base.total_timeout = as_field_is_set(bitmap, AS_BATCH_PARENT_READ + AS_BATCH_TOTAL_TIMEOUT)?
		src->batch.base.total_timeout : orig->batch.base.total_timeout;
	trg->batch.base.timeout_delay = as_field_is_set(bitmap, AS_BATCH_PARENT_READ + AS_BATCH_TIMEOUT_DELAY)?
		src->batch.base.timeout_delay : orig->batch.base.timeout_delay;
	trg->batch.base.max_retries = as_field_is_set(bitmap, AS_BATCH_PARENT_READ + AS_BATCH_MAX_RETRIES)?
		src->batch.base.max_retries : orig->batch.base.max_retries;
	trg->batch.base.sleep_between_retries = as_field_is_set(bitmap, AS_BATCH_PARENT_READ + AS_BATCH_SLEEP_BETWEEN_RETRIES)?
		src->batch.base.sleep_between_retries : orig->batch.base.sleep_between_retries;
	trg->batch.base.error_detail_verbosity = as_field_is_set(bitmap, AS_BATCH_PARENT_READ + AS_BATCH_ERROR_DETAIL)?
		src->batch.base.error_detail_verbosity : orig->batch.base.error_detail_verbosity;
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

	trg->batch_parent_write.base.connect_timeout = as_field_is_set(bitmap, AS_BATCH_PARENT_WRITE + AS_BATCH_CONNECT_TIMEOUT)?
		src->batch_parent_write.base.connect_timeout : orig->batch_parent_write.base.connect_timeout;
	trg->batch_parent_write.base.socket_timeout = as_field_is_set(bitmap, AS_BATCH_PARENT_WRITE + AS_BATCH_SOCKET_TIMEOUT)?
		src->batch_parent_write.base.socket_timeout : orig->batch_parent_write.base.socket_timeout;
	trg->batch_parent_write.base.total_timeout = as_field_is_set(bitmap, AS_BATCH_PARENT_WRITE + AS_BATCH_TOTAL_TIMEOUT)?
		src->batch_parent_write.base.total_timeout : orig->batch_parent_write.base.total_timeout;
	trg->batch_parent_write.base.timeout_delay = as_field_is_set(bitmap, AS_BATCH_PARENT_WRITE + AS_BATCH_TIMEOUT_DELAY)?
		src->batch_parent_write.base.timeout_delay : orig->batch_parent_write.base.timeout_delay;
	trg->batch_parent_write.base.max_retries = as_field_is_set(bitmap, AS_BATCH_PARENT_WRITE + AS_BATCH_MAX_RETRIES)?
		src->batch_parent_write.base.max_retries : orig->batch_parent_write.base.max_retries;
	trg->batch_parent_write.base.sleep_between_retries =
		as_field_is_set(bitmap, AS_BATCH_PARENT_WRITE + AS_BATCH_SLEEP_BETWEEN_RETRIES)?
		src->batch_parent_write.base.sleep_between_retries : orig->batch_parent_write.base.sleep_between_retries;
	trg->batch_parent_write.base.error_detail_verbosity =
		as_field_is_set(bitmap, AS_BATCH_PARENT_WRITE + AS_BATCH_ERROR_DETAIL)?
		src->batch_parent_write.base.error_detail_verbosity : orig->batch_parent_write.base.error_detail_verbosity;
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

	trg->txn_verify.base.connect_timeout = as_field_is_set(bitmap, AS_TXN_VERIFY + AS_BATCH_CONNECT_TIMEOUT)?
		src->txn_verify.base.connect_timeout : orig->txn_verify.base.connect_timeout;
	trg->txn_verify.base.socket_timeout = as_field_is_set(bitmap, AS_TXN_VERIFY + AS_BATCH_SOCKET_TIMEOUT)?
		src->txn_verify.base.socket_timeout : orig->txn_verify.base.socket_timeout;
	trg->txn_verify.base.total_timeout = as_field_is_set(bitmap, AS_TXN_VERIFY + AS_BATCH_TOTAL_TIMEOUT)?
		src->txn_verify.base.total_timeout : orig->txn_verify.base.total_timeout;
	trg->txn_verify.base.timeout_delay = as_field_is_set(bitmap, AS_TXN_VERIFY + AS_BATCH_TIMEOUT_DELAY)?
		src->txn_verify.base.timeout_delay : orig->txn_verify.base.timeout_delay;
	trg->txn_verify.base.max_retries = as_field_is_set(bitmap, AS_TXN_VERIFY + AS_BATCH_MAX_RETRIES)?
		src->txn_verify.base.max_retries : orig->txn_verify.base.max_retries;
	trg->txn_verify.base.sleep_between_retries = as_field_is_set(bitmap, AS_TXN_VERIFY + AS_BATCH_SLEEP_BETWEEN_RETRIES)?
		src->txn_verify.base.sleep_between_retries : orig->txn_verify.base.sleep_between_retries;
	trg->txn_verify.base.error_detail_verbosity = as_field_is_set(bitmap, AS_TXN_VERIFY + AS_BATCH_ERROR_DETAIL)?
		src->txn_verify.base.error_detail_verbosity : orig->txn_verify.base.error_detail_verbosity;
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

	trg->txn_roll.base.connect_timeout = as_field_is_set(bitmap, AS_TXN_ROLL + AS_BATCH_CONNECT_TIMEOUT)?
		src->txn_roll.base.connect_timeout : orig->txn_roll.base.connect_timeout;
	trg->txn_roll.base.socket_timeout = as_field_is_set(bitmap, AS_TXN_ROLL + AS_BATCH_SOCKET_TIMEOUT)?
		src->txn_roll.base.socket_timeout : orig->txn_roll.base.socket_timeout;
	trg->txn_roll.base.total_timeout = as_field_is_set(bitmap, AS_TXN_ROLL + AS_BATCH_TOTAL_TIMEOUT)?
		src->txn_roll.base.total_timeout : orig->txn_roll.base.total_timeout;
	trg->txn_roll.base.timeout_delay = as_field_is_set(bitmap, AS_TXN_ROLL + AS_BATCH_TIMEOUT_DELAY)?
		src->txn_roll.base.timeout_delay : orig->txn_roll.base.timeout_delay;
	trg->txn_roll.base.max_retries = as_field_is_set(bitmap, AS_TXN_ROLL + AS_BATCH_MAX_RETRIES)?
		src->txn_roll.base.max_retries : orig->txn_roll.base.max_retries;
	trg->txn_roll.base.sleep_between_retries = as_field_is_set(bitmap, AS_TXN_ROLL + AS_BATCH_SLEEP_BETWEEN_RETRIES)?
		src->txn_roll.base.sleep_between_retries : orig->txn_roll.base.sleep_between_retries;
	trg->txn_roll.base.error_detail_verbosity = as_field_is_set(bitmap, AS_TXN_ROLL + AS_BATCH_ERROR_DETAIL)?
		src->txn_roll.base.error_detail_verbosity : orig->txn_roll.base.error_detail_verbosity;
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

	// Save the export settings currently in effect. trg is overwritten below, and
	// export_changed compares these copies with the values this reload installs.
	uint64_t prev_report_size_limit = trg->report_size_limit;
	char prev_report_dir[256];
	char prev_export_interval[32];
	as_strncpy(prev_report_dir, trg->report_dir, sizeof(prev_report_dir));
	as_strncpy(prev_export_interval, trg->export_interval, sizeof(prev_export_interval));

	// Read the bitmap already in effect before as_cluster_update replaces it.
	// metrics_exporter is "file" when the key is absent and when the key is "file".
	// An absent key calls added exporters, or the file writer when none were
	// added. An explicit "file" does not call added exporters. Compare whether
	// the key is set as well as the mode. Restart when the key appears or
	// disappears, or when it stays set and the mode changes.
	bool prev_exporter_set = cluster->as && cluster->as->config_bitmap &&
		as_field_is_set(cluster->as->config_bitmap, AS_METRICS_EXPORTER);

	uint8_t prev_exporter = trg->metrics_exporter;
	bool next_exporter_set = as_field_is_set(bitmap, AS_METRICS_EXPORTER);
	trg->metrics_exporter = next_exporter_set ? src->metrics_exporter : orig->metrics_exporter;

	bool exporter_changed = prev_exporter_set != next_exporter_set ||
		(next_exporter_set && prev_exporter != trg->metrics_exporter);

	trg->enable = as_field_is_set(bitmap, AS_METRICS_ENABLE)?
		src->enable : orig->enable;
	trg->latency_columns = as_field_is_set(bitmap, AS_METRICS_LATENCY_COLUMNS)?
		src->latency_columns : orig->latency_columns;
	trg->latency_shift = as_field_is_set(bitmap, AS_METRICS_LATENCY_SHIFT)?
		src->latency_shift : orig->latency_shift;
	trg->latency_unit = as_field_is_set(bitmap, AS_METRICS_LATENCY_UNIT) ?
		src->latency_unit : orig->latency_unit;
	trg->operational_enabled = as_field_is_set(bitmap, AS_METRICS_OPERATIONAL) ?
		src->operational_enabled : orig->operational_enabled;

	// Legacy metrics.enable, without extended.operational.enabled, is master
	// plus operational. metrics.enabled does not turn operational on.
	// aerospike_enable_metrics() applies the same mapping.
	if (as_field_is_set(bitmap, AS_METRICS_LEGACY_ENABLE) && src->enable &&
			!as_field_is_set(bitmap, AS_METRICS_OPERATIONAL)) {
		trg->operational_enabled = true;
	}
	trg->usage_enabled = as_field_is_set(bitmap, AS_METRICS_USAGE) ?
		src->usage_enabled : orig->usage_enabled;
	trg->sampler_range = as_field_is_set(bitmap, AS_METRICS_SAMPLER_RANGE) ?
		src->sampler_range : orig->sampler_range;
	trg->sampler_threshold = as_field_is_set(bitmap, AS_METRICS_SAMPLER_THRESHOLD) ?
		src->sampler_threshold : orig->sampler_threshold;

	if (trg->sampler_range < 1 || trg->sampler_threshold > trg->sampler_range) {
		pthread_mutex_unlock(&cluster->metrics_lock);
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"Invalid metrics.extended.operational.sampler: range must be >= 1 and threshold must be <= range");
	}
	trg->report_size_limit = as_field_is_set(bitmap, AS_METRICS_REPORT_SIZE_LIMIT) ?
		src->report_size_limit : orig->report_size_limit;

	if (as_field_is_set(bitmap, AS_METRICS_EXPORT_INTERVAL)) {
		as_strncpy(trg->export_interval, src->export_interval, sizeof(trg->export_interval));
	}
	else {
		as_strncpy(trg->export_interval, orig->export_interval, sizeof(trg->export_interval));
	}

	if (as_field_is_set(bitmap, AS_METRICS_REPORT_DIR)) {
		as_strncpy(trg->report_dir, src->report_dir, sizeof(trg->report_dir));
	}
	else if (as_field_is_set(bitmap, AS_METRICS_EXPORTER) &&
			trg->metrics_exporter == AS_METRICS_BUILTIN_EXPORTER_FILE &&
			orig->report_dir[0] == '\0') {
		// The file selected the writer and did not set metrics.report_dir.
		// An explicit empty metrics.report_dir is copied above and installs nothing.
		as_strncpy(trg->report_dir, ".", sizeof(trg->report_dir));
	}
	else {
		as_strncpy(trg->report_dir, orig->report_dir, sizeof(trg->report_dir));
	}

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
		// Restart the metrics thread when an export setting changed. Latency
		// bucket layout is stored on the cluster, so compare that separately.
		bool export_changed = strcmp(trg->export_interval, prev_export_interval) != 0 ||
			trg->report_size_limit != prev_report_size_limit ||
			exporter_changed ||
			strcmp(trg->report_dir, prev_report_dir) != 0;
		bool latency_changed = cluster->metrics_latency_columns != trg->latency_columns ||
			cluster->metrics_latency_shift != trg->latency_shift ||
			cluster->metrics_latency_unit != trg->latency_unit;
		bool tier_changed = cluster->metrics_operational_enabled != trg->operational_enabled ||
			cluster->metrics_usage_enabled != trg->usage_enabled;

		if (!cluster->metrics_enabled || export_changed || latency_changed || tier_changed) {
			enable_metrics = true;
		}
		else if (as_load_uint32(&cluster->metrics_sampler_range) != trg->sampler_range ||
				as_load_uint32(&cluster->metrics_sampler_threshold) != trg->sampler_threshold) {
			// Sampler changes apply to the next command. They do not restart export.
			as_store_uint32(&cluster->metrics_sampler_range, trg->sampler_range);
			as_store_uint32(&cluster->metrics_sampler_threshold, trg->sampler_threshold);
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

	// Update metrics before replacing the bitmap so the previous exporter key is visible.
	as_status status = as_cluster_update_metrics(cluster, err, &orig->policies.metrics,
		&src->policies.metrics, &config->policies.metrics, bitmap);
	memcpy(as->config_bitmap, bitmap, AS_CONFIG_BITMAP_SIZE);
	return status;
}

static as_status
as_apply_error(as_error* err, const char* path, const char* fmt, ...)
{
	char detail[AS_ERROR_MESSAGE_MAX_SIZE];
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(detail, sizeof(detail), fmt, ap);
	va_end(ap);
	return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to parse: %s\n%s", path, detail);
}

static as_status
as_apply_validate(as_yaml_discovery* d, const char* path, as_error* err)
{
	bool dns = (d->set & AS_YAML_SET_DNS) != 0;
	bool aws = (d->set & AS_YAML_SET_AWS) != 0;
	bool static_map = (d->set & AS_YAML_SET_STATIC_MAP) != 0;
	bool port_map = (d->set & AS_YAML_SET_PORT_MAP) != 0;
	const char* translator = d->translator == AS_YAML_TRANSLATOR_UNSET ? NULL :
		as_yaml_translator_label(d);

	if (d->provider == AS_YAML_PROVIDER_DNS && !dns) {
		return as_apply_error(err, path, "static.client.discovery.dns is required with provider dns");
	}

	if (dns && d->provider != AS_YAML_PROVIDER_DNS) {
		return as_apply_error(err, path, "static.client.discovery.dns is only valid with provider dns");
	}

	if (d->provider == AS_YAML_PROVIDER_AWS && !aws) {
		return as_apply_error(err, path,
			"static.client.discovery.aws is required with provider aws-tags");
	}

	if (aws && d->provider != AS_YAML_PROVIDER_AWS) {
		return as_apply_error(err, path,
			"static.client.discovery.aws is only valid with provider aws-tags");
	}

	if (static_map && translator && d->translator != AS_YAML_TRANSLATOR_STATIC_MAP) {
		return as_apply_error(err, path,
			"static.client.translation.static_map is only valid with provider static-map, not '%s'",
			translator);
	}

	if (port_map && d->translator != AS_YAML_TRANSLATOR_PORT_MAP) {
		if (translator) {
			return as_apply_error(err, path,
				"static.client.translation.port_map is only valid with provider port-map, not '%s'",
				translator);
		}
		return as_apply_error(err, path,
			"static.client.translation.port_map is only valid with provider port-map");
	}

	if (d->translator == AS_YAML_TRANSLATOR_PORT_MAP && !port_map) {
		return as_apply_error(err, path,
			"static.client.translation.port_map is required with provider port-map");
	}

	if (static_map && (d->set & AS_YAML_SET_IP_MAP)) {
		return as_apply_error(err, path,
			"static.client.translation.static_map and static.client.ip_map are both set");
	}
	return AEROSPIKE_OK;
}

static as_status
as_apply_recipe_failed(as_seed_provider* provider, const char* path, as_error* err)
{
	provider->destroy(provider->udata);
	return as_apply_error(err, path, "Failed to allocate seed provider recipe");
}

static as_status
as_apply_build_provider(
	as_yaml_discovery* d, const char* path, as_seed_provider* provider, as_plugin_recipe** recipe,
	as_error* err
	)
{
	as_error local;
	as_status status;

	if (d->provider == AS_YAML_PROVIDER_DNS) {
		as_dns_seed_provider_config dns = d->dns;
		dns.names = d->dns_names.size ? (const char* const*)d->dns_names.list : NULL;
		dns.names_size = d->dns_names.size;
		dns.servers = d->dns_servers.size ? (const char* const*)d->dns_servers.list : NULL;
		dns.servers_size = d->dns_servers.size;

		status = as_discovery_dns_seed_provider(&local, &dns, provider);

		if (status != AEROSPIKE_OK) {
			return as_apply_error(err, path, "Invalid static.client.discovery.dns: %s",
				local.message);
		}

		if (!(*recipe = as_plugin_recipe_dns(&dns))) {
			return as_apply_recipe_failed(provider, path, err);
		}
		return AEROSPIKE_OK;
	}

	uint32_t size = d->aws_filters.size;
	as_aws_filter* filters = NULL;

	if (size > 0) {
		filters = cf_malloc(sizeof(as_aws_filter) * size);

		if (!filters) {
			return as_apply_error(err, path, "Failed to allocate static.client.discovery.aws.filters");
		}

		for (uint32_t i = 0; i < size; i++) {
			as_yaml_filter* src = as_vector_get(&d->aws_filters, i);
			filters[i].name = src->name;
			filters[i].values = src->values.size ? (const char* const*)src->values.list : NULL;
			filters[i].values_size = src->values.size;
		}
	}

	as_aws_seed_provider_config aws = d->aws;
	aws.filters = filters;
	aws.filters_size = size;

	status = as_discovery_aws_seed_provider(&local, &aws, provider);

	if (status != AEROSPIKE_OK) {
		cf_free(filters);
		return as_apply_error(err, path, "Invalid static.client.discovery.aws: %s", local.message);
	}

	// The file cannot set a credentials hook, so the recipe has no hook holder to adopt.
	*recipe = as_plugin_recipe_aws(&aws);
	cf_free(filters);

	if (!*recipe) {
		return as_apply_recipe_failed(provider, path, err);
	}
	return AEROSPIKE_OK;
}

static as_addr_map*
as_apply_copy_map(as_vector* src)
{
	as_addr_map* map = cf_malloc(sizeof(as_addr_map) * src->size);

	if (!map) {
		return NULL;
	}

	for (uint32_t i = 0; i < src->size; i++) {
		as_addr_map* entry = as_vector_get(src, i);
		map[i].orig = cf_strdup(entry->orig);
		map[i].alt = cf_strdup(entry->alt);

		if (!map[i].orig || !map[i].alt) {
			for (uint32_t j = 0; j <= i; j++) {
				cf_free(map[j].orig);
				cf_free(map[j].alt);
			}
			cf_free(map);
			return NULL;
		}
	}
	return map;
}

// Register the staged discovery and translation keys of a valid file. Nothing in as->config
// changes unless every part is built, so a failure keeps the code registrations.
static as_status
as_config_file_apply_discovery(aerospike* as, as_yaml_discovery* d, as_error* err)
{
	const char* path = as->config.config_provider.path;
	as_status status = as_apply_validate(d, path, err);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	// Build.
	as_config* config = &as->config;

	as_seed_provider provider;
	memset(&provider, 0, sizeof(provider));
	bool registered_provider = d->provider == AS_YAML_PROVIDER_REGISTERED;

	// The file has already set execution_mode. Outside thread mode aerospike_connect() fails
	// before it builds a binding provider, so only its recipe is kept and the factory, which
	// enters the binding runtime, is not called.
	bool deferred_provider = registered_provider &&
		as_plugin_registry_binding(config->discovery.registry, d->provider_index) &&
		config->discovery.execution_mode != AS_DISCOVERY_EXECUTION_THREAD;
	bool has_provider = (d->provider == AS_YAML_PROVIDER_DNS ||
		d->provider == AS_YAML_PROVIDER_AWS || registered_provider) && !deferred_provider;
	char* provider_name = NULL;
	as_plugin_recipe* provider_recipe = NULL;

	as_address_translator translator;
	memset(&translator, 0, sizeof(translator));
	bool has_translator = false;
	bool registered_translator = false;
	char* translator_name = NULL;
	as_plugin_recipe* translator_recipe = NULL;

	as_vector* entries = NULL;
	as_addr_map* map = NULL;

	if (registered_provider) {
		as_error local;

		if (has_provider) {
			if (as_plugin_build_seed_provider(&local, config->discovery.registry,
					d->provider_index, d->provider_options, &provider) != AEROSPIKE_OK) {
				return as_apply_error(err, path,
					"Invalid static.client.discovery.provider: %s. %s", d->provider_name,
					local.message);
			}
			// The build copied the name.
			provider_name = (char*)provider.name;
		}

		if (!(provider_recipe = as_plugin_recipe_factory(config->discovery.registry,
				d->provider_index, d->provider_options, false))) {
			status = as_apply_error(err, path, "Failed to allocate seed provider recipe");
			goto Release;
		}
	}
	else if (has_provider) {
		status = as_apply_build_provider(d, path, &provider, &provider_recipe, err);

		if (status != AEROSPIKE_OK) {
			return status;
		}
	}

	if (d->translator == AS_YAML_TRANSLATOR_REGISTERED) {
		as_error local;

		if (as_plugin_build_translator(&local, config->translation.registry, d->translator_index,
				d->translator_options, &translator) != AEROSPIKE_OK) {
			status = as_apply_error(err, path, "Invalid static.client.translation.provider: %s. %s",
				d->translator_name, local.message);
			goto Release;
		}
		has_translator = true;
		registered_translator = true;
		translator_name = (char*)translator.name;
		translator_recipe = as_plugin_recipe_factory(config->translation.registry,
			d->translator_index, d->translator_options, true);
	}
	else if (d->translator == AS_YAML_TRANSLATOR_PORT_MAP) {
		as_error local;

		if (as_discovery_port_map_translator(&local, d->port_map.list, d->port_map.size,
				&translator) != AEROSPIKE_OK) {
			status = as_apply_error(err, path, "Invalid static.client.translation.port_map: %s",
				local.message);
			goto Release;
		}
		has_translator = true;
		translator_recipe = as_plugin_recipe_port_map(d->port_map.list, d->port_map.size);
	}
	else if (d->translator == AS_YAML_TRANSLATOR_PASS_THROUGH) {
		as_discovery_pass_through_translator(&translator);
		has_translator = true;
		translator_recipe = as_plugin_recipe_pass_through();
	}

	if (has_translator && !translator_recipe) {
		status = as_apply_error(err, path, "Failed to allocate address translator recipe");
		goto Release;
	}

	if (has_provider && !registered_provider && !(provider_name = cf_strdup(provider.name))) {
		status = as_apply_error(err, path, "Failed to copy seed provider name");
		goto Release;
	}

	if (has_translator && !registered_translator &&
			!(translator_name = cf_strdup(translator.name))) {
		status = as_apply_error(err, path, "Failed to copy address translator name");
		goto Release;
	}

	if (d->set & AS_YAML_SET_STATIC_MAP) {
		entries = &d->static_map;
	}
	else if (d->set & AS_YAML_SET_IP_MAP) {
		entries = &d->ip_map;
	}

	if (entries && entries->size > 0 && !(map = as_apply_copy_map(entries))) {
		status = as_apply_error(err, path, "Failed to copy static.client.ip_map");
		goto Release;
	}

	// Commit. Nothing below can fail.
	if (d->set & AS_YAML_SET_USE_SERVICES_ALTERNATE) {
		// dynamic.client.use_service_alternative wins. as_cluster_update() reverts to
		// config_orig when the dynamic key is absent, so the static value goes there too.
		if (!as_field_is_set(as->config_bitmap, AS_USE_SERVICE_ALTERNATIVE)) {
			config->use_services_alternate = d->use_services_alternate;
		}
		as->config_orig->use_services_alternate = d->use_services_alternate;
	}

	if (entries) {
		config->ip_map = map;
		config->ip_map_size = entries->size;
		config->translation.file_ip_map = map;
		config->translation.file_ip_map_size = map ? entries->size : 0;
	}

	if (d->provider != AS_YAML_PROVIDER_UNSET) {
		if (config->discovery.provider_registered) {
			as_log_warn("Seed provider %s registered in code is replaced by "
				"static.client.discovery.provider %s in %s", config->discovery.provider.name,
				as_yaml_provider_label(d), path);
		}

		if (has_provider) {
			provider.name = provider_name;
		}
		as_config_install_seed_provider(config, has_provider ? &provider : NULL, provider_recipe);
	}

	if (d->translator != AS_YAML_TRANSLATOR_UNSET || (d->set & AS_YAML_SET_STATIC_MAP)) {
		if (config->translation.provider_registered) {
			// static_map without provider implies static-map.
			as_log_warn("Address translator %s registered in code is replaced by "
				"static.client.translation.provider %s in %s", config->translation.provider.name,
				d->translator == AS_YAML_TRANSLATOR_UNSET ? "static-map" :
				as_yaml_translator_label(d), path);
		}

		if (has_translator) {
			translator.name = translator_name;
		}
		as_config_install_translator(config, has_translator ? &translator : NULL,
			translator_recipe);
	}
	return AEROSPIKE_OK;

Release:
	if (has_provider && provider.destroy) {
		provider.destroy(provider.udata);
	}

	if (has_translator && translator.destroy) {
		translator.destroy(translator.udata);
	}
	cf_free(provider_name);
	cf_free(translator_name);
	as_plugin_recipe_destroy(provider_recipe);
	as_plugin_recipe_destroy(translator_recipe);
	return status;
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

	as_yaml_discovery disc;
	as_yaml_discovery_init(&disc);

	as_config* config = &as->config;
	as_status status = as_config_file_read(as, config, as->config_bitmap, true, &disc, err);

	if (status == AEROSPIKE_OK) {
		status = as_config_file_apply_discovery(as, &disc, err);
	}

	if (status == AEROSPIKE_OK &&
			as_field_is_set(as->config_bitmap, AS_METRICS_LEGACY_ENABLE) &&
			config->policies.metrics.enable &&
			!as_field_is_set(as->config_bitmap, AS_METRICS_OPERATIONAL)) {
		// Same mapping as as_cluster_update_metrics(). The first read has no cluster yet.
		config->policies.metrics.operational_enabled = true;
	}

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
		memset(as->config_bitmap, 0, AS_CONFIG_BITMAP_SIZE);
	}

	as_yaml_discovery_destroy(&disc);
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

	as_status status = as_config_file_read(as, &config, bitmap, false, NULL, err);

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
