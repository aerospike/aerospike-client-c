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
#include <aerospike/as_config.h>
#include <aerospike/as_discovery.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_vector.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_clock.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "as_aws_private.h"

//---------------------------------
// Macros
//---------------------------------

#define AWS_API_VERSION "2016-11-15"
#define AWS_STATE_FILTER "instance-state-name"
#define AWS_FORM "application/x-www-form-urlencoded; charset=utf-8"
#define AWS_SERVICE "ec2"
#define AWS_ACTION "DescribeInstances"
#define AWS_ADDRESS_MAX 255

//---------------------------------
// Static Functions: validation
//---------------------------------

static const char*
aws_str(const char* s)
{
	return s ? s : "(null)";
}

static const char*
aws_element(as_aws_address_field field)
{
	switch (field) {
	case AS_AWS_ADDRESS_PUBLIC_IP:
		return "ipAddress";
	case AS_AWS_ADDRESS_PRIVATE_DNS:
		return "privateDnsName";
	case AS_AWS_ADDRESS_PUBLIC_DNS:
		return "dnsName";
	default:
		return "privateIpAddress";
	}
}

// [a-z0-9]+(-[a-z0-9]+)+
static bool
aws_valid_region(const char* region)
{
	if (!region) {
		return false;
	}

	uint32_t parts = 0;
	const char* p = region;

	while (true) {
		const char* start = p;

		while ((*p >= 'a' && *p <= 'z') || (*p >= '0' && *p <= '9')) {
			p++;
		}

		if (p == start) {
			return false;
		}
		parts++;

		if (*p == 0) {
			return parts >= 2;
		}

		if (*p != '-') {
			return false;
		}
		p++;
	}
}

static bool
aws_present(const char* s)
{
	return s && s[0];
}

static bool
aws_hook_present(const as_aws_credentials_hook* hook)
{
	return hook->get || hook->destroy || hook->udata;
}

static as_status
aws_validate_credentials(as_error* err, const as_aws_seed_provider_config* config)
{
	bool key = aws_present(config->access_key_id);
	bool secret = aws_present(config->secret_access_key);
	bool token = aws_present(config->session_token);
	bool hook = aws_hook_present(&config->credentials_hook);

	if (secret && !key) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"Invalid AWS EC2 seed provider credentials: access_key_id is empty");
	}

	if (key && !secret) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"Invalid AWS EC2 seed provider credentials: secret_access_key is empty");
	}

	if (token && !key) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"Invalid AWS EC2 seed provider credentials: session_token requires access_key_id and "
			"secret_access_key");
	}

	if (hook && (key || token)) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"Invalid AWS EC2 seed provider credentials: static keys and credentials_hook cannot "
			"both be set");
	}

	if (hook && !config->credentials_hook.get) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"Invalid AWS EC2 seed provider credentials_hook: get is required");
	}

	if (key && !as_aws_valid_key(config->access_key_id)) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"Invalid AWS EC2 seed provider access_key_id: expected letters and digits");
	}

	if (secret && !as_aws_visible(config->secret_access_key)) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"Invalid AWS EC2 seed provider secret_access_key: invalid character");
	}

	if (token && !as_aws_visible(config->session_token)) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"Invalid AWS EC2 seed provider session_token: invalid character");
	}
	return AEROSPIKE_OK;
}

static as_status
aws_invalid_endpoint(as_error* err, const char* endpoint)
{
	return as_error_update(err, AEROSPIKE_ERR_PARAM,
		"Invalid AWS EC2 seed provider endpoint: '%s': expected http or https URL with a host and "
		"no query", endpoint);
}

static as_status
aws_validate_filters(as_error* err, const as_aws_seed_provider_config* config)
{
	if (!config->filters || config->filters_size == 0) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"Invalid AWS EC2 seed provider filters: at least one filter is required");
	}

	for (uint32_t i = 0; i < config->filters_size; i++) {
		const as_aws_filter* f = &config->filters[i];

		if (!f->name || !f->name[0]) {
			return as_error_update(err, AEROSPIKE_ERR_PARAM,
				"Invalid AWS EC2 seed provider filter name: '%s'", aws_str(f->name));
		}

		if (strcmp(f->name, AWS_STATE_FILTER) == 0) {
			return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
				"Invalid AWS EC2 seed provider filter name: '" AWS_STATE_FILTER
				"': the provider adds " AWS_STATE_FILTER "=running");
		}

		for (uint32_t j = 0; j < i; j++) {
			if (strcmp(config->filters[j].name, f->name) == 0) {
				return as_error_update(err, AEROSPIKE_ERR_PARAM,
					"Invalid AWS EC2 seed provider filter name: '%s': duplicate", f->name);
			}
		}

		if (!f->values || f->values_size == 0) {
			return as_error_update(err, AEROSPIKE_ERR_PARAM,
				"Invalid AWS EC2 seed provider filter '%s' values: empty", f->name);
		}

		for (uint32_t j = 0; j < f->values_size; j++) {
			const char* value = f->values[j];

			if (!value || !value[0]) {
				return as_error_update(err, AEROSPIKE_ERR_PARAM,
					"Invalid AWS EC2 seed provider filter '%s' value: '%s'", f->name,
					aws_str(value));
			}
		}
	}
	return AEROSPIKE_OK;
}

static as_status
aws_validate(as_error* err, const as_aws_seed_provider_config* config)
{
	if (!aws_valid_region(config->region)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
			"Invalid AWS EC2 seed provider region: '%s'", aws_str(config->region));
	}

	as_status status = aws_validate_filters(err, config);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	if ((int)config->address_field < (int)AS_AWS_ADDRESS_PRIVATE_IP ||
		(int)config->address_field > (int)AS_AWS_ADDRESS_PUBLIC_DNS) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
			"Invalid AWS EC2 seed provider address_field: %d", (int)config->address_field);
	}

	if (config->port == 0) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"Invalid AWS EC2 seed provider port: 0");
	}

	if (config->tls_name && !config->tls_name[0]) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"Invalid AWS EC2 seed provider tls_name: empty");
	}

	if (config->timeout_ms == 0) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"Invalid AWS EC2 seed provider timeout_ms: 0");
	}

	if (config->endpoint) {
		as_aws_url url;

		if (!as_aws_url_parse(config->endpoint, &url)) {
			return aws_invalid_endpoint(err, config->endpoint);
		}

		bool query = url.query != NULL;
		as_aws_url_destroy(&url);

		if (query) {
			return aws_invalid_endpoint(err, config->endpoint);
		}
	}

	if (config->ca_file && !config->ca_file[0]) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"Invalid AWS EC2 seed provider ca_file: empty");
	}

	if (config->ca_path && !config->ca_path[0]) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"Invalid AWS EC2 seed provider ca_path: empty");
	}
	return aws_validate_credentials(err, config);
}

//---------------------------------
// Static Functions: state
//---------------------------------

static void
aws_append_filter(as_aws_buf* buf, uint32_t n, const char* name, const char* const* values,
	uint32_t values_size)
{
	char* encoded = as_aws_encode(name);

	if (!encoded) {
		buf->failed = true;
		return;
	}
	as_aws_buf_printf(buf, "&Filter.%u.Name=%s", n, encoded);
	cf_free(encoded);

	for (uint32_t i = 0; i < values_size; i++) {
		encoded = as_aws_encode(values[i]);

		if (!encoded) {
			buf->failed = true;
			return;
		}
		as_aws_buf_printf(buf, "&Filter.%u.Value.%u=%s", n, i + 1, encoded);
		cf_free(encoded);
	}
}

static char*
aws_query(const as_aws_seed_provider_config* config)
{
	static const char* const running[] = {"running"};

	as_aws_buf buf;
	as_aws_buf_init(&buf);
	as_aws_buf_append_str(&buf, "Action=" AWS_ACTION "&Version=" AWS_API_VERSION);

	uint32_t n = 0;

	for (uint32_t i = 0; i < config->filters_size; i++) {
		const as_aws_filter* f = &config->filters[i];
		aws_append_filter(&buf, ++n, f->name, f->values, f->values_size);
	}
	aws_append_filter(&buf, ++n, AWS_STATE_FILTER, running, 1);
	return as_aws_buf_detach(&buf);
}

static void
aws_destroy(void* udata)
{
	as_aws_state* st = udata;

	if (!st) {
		return;
	}

	cf_free(st->region);
	cf_free(st->tls_name);
	cf_free(st->query);
	as_aws_url_destroy(&st->endpoint);
	as_aws_tls_destroy(&st->tls);
	as_aws_free_secret(st->access_key_id);
	as_aws_free_secret(st->secret_access_key);
	as_aws_free_secret(st->session_token);
	as_aws_chain_destroy(st);

	if (st->hook.destroy) {
		st->hook.destroy(st->hook.udata);
	}
	st->magic = 0;
	cf_free(st);
}

// Leave the hook udata with the caller when a constructed state is destroyed unregistered.
static void
aws_release_hook(as_aws_state* st)
{
	memset(&st->hook, 0, sizeof(as_aws_credentials_hook));
}

static as_status
aws_alloc_failed(as_error* err)
{
	return as_error_set_message(err, AEROSPIKE_ERR_CLIENT,
		"Failed to allocate AWS EC2 seed provider");
}

static as_status
aws_create(as_error* err, const as_aws_seed_provider_config* config, const as_aws_env* env,
	as_aws_state** out)
{
	as_status status = aws_validate(err, config);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_aws_state* st = cf_calloc(1, sizeof(as_aws_state));

	if (!st) {
		return aws_alloc_failed(err);
	}

	st->magic = AS_AWS_STATE_MAGIC;
	st->address_field = config->address_field;
	st->port = config->port;
	st->timeout_ms = config->timeout_ms;
	st->region = cf_strdup(config->region);
	st->query = aws_query(config);

	bool ok = st->region && st->query;

	if (aws_present(config->access_key_id)) {
		ok = ok && (st->access_key_id = cf_strdup(config->access_key_id)) &&
			(st->secret_access_key = cf_strdup(config->secret_access_key));
	}

	if (config->tls_name) {
		ok = ok && (st->tls_name = cf_strdup(config->tls_name));
	}

	if (aws_present(config->session_token)) {
		ok = ok && (st->session_token = cf_strdup(config->session_token));
	}

	if (!ok) {
		aws_destroy(st);
		return aws_alloc_failed(err);
	}

	if (config->endpoint) {
		ok = as_aws_url_parse(config->endpoint, &st->endpoint);
	}
	else {
		char* endpoint = as_aws_ec2_endpoint(config->region);
		ok = endpoint && as_aws_url_parse(endpoint, &st->endpoint);
		cf_free(endpoint);
	}

	// Validation accepted the endpoint, so only an allocation can fail here.
	if (!ok) {
		aws_destroy(st);
		return aws_alloc_failed(err);
	}

	status = as_aws_chain_init(err, st, config, env);

	if (status != AEROSPIKE_OK) {
		aws_destroy(st);
		return status;
	}

	status = as_aws_tls_init(err, config->ca_file, config->ca_path, &st->tls);

	if (status != AEROSPIKE_OK) {
		aws_destroy(st);
		return status;
	}

	// After the last failure path, so that no failed construction runs the hook destroy.
	st->hook = config->credentials_hook;
	*out = st;
	return AEROSPIKE_OK;
}

//---------------------------------
// Static Functions: refresh
//---------------------------------

static bool
aws_valid_address(const char* address)
{
	if (!address || !address[0]) {
		return false;
	}

	size_t len = 0;

	for (const unsigned char* p = (const unsigned char*)address; *p; p++, len++) {
		if (*p <= ' ' || *p == 0x7f || len >= AWS_ADDRESS_MAX) {
			return false;
		}
	}
	return true;
}

static int
aws_compare(const void* a, const void* b)
{
	return strcmp(*(char* const*)a, *(char* const*)b);
}

// Send one DescribeInstances page and parse its body.
static as_status
aws_describe(as_error* err, const as_aws_state* st, const as_aws_credentials* credentials,
	const char* token, uint64_t deadline, as_aws_xml** out)
{
	as_aws_buf body;
	as_aws_buf_init(&body);
	as_aws_buf_append_str(&body, st->query);

	if (token) {
		char* encoded = as_aws_encode(token);

		if (encoded) {
			as_aws_buf_printf(&body, "&NextToken=%s", encoded);
			cf_free(encoded);
		}
		else {
			body.failed = true;
		}
	}

	char amz_date[AS_AWS_AMZ_DATE_SIZE];

	if (!as_aws_buf_ok(&body) || !as_aws_amz_date(as_aws_now_ms(st), amz_date)) {
		as_aws_buf_destroy(&body);
		return as_error_set_message(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
			"AWS EC2 seed provider: failed to build the DescribeInstances request");
	}

	// Sorted by name, as sent.
	as_aws_header headers[5] = {
		{"authorization", NULL},
		{"content-type", AWS_FORM},
		{"host", st->endpoint.host_header},
		{"x-amz-date", amz_date},
		{"x-amz-security-token", credentials->session_token}
	};
	uint32_t headers_size = credentials->session_token ? 5 : 4;

	char* authorization = as_aws_sigv4_authorization(credentials->access_key_id,
		credentials->secret_access_key, "POST", st->endpoint.path, "", headers + 1,
		headers_size - 1, (const uint8_t*)body.data, body.size, st->region, AWS_SERVICE, amz_date);

	if (!authorization) {
		as_aws_buf_destroy(&body);
		return as_error_set_message(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
			"AWS EC2 seed provider: failed to sign the DescribeInstances request");
	}
	headers[0].value = authorization;

	const char* secrets[] = {
		credentials->secret_access_key, credentials->session_token,
		strrchr(authorization, '=') + 1
	};
	as_aws_http_response response;
	as_aws_http_rc rc = as_aws_http_request(err, &st->tls, AWS_ACTION, "POST", &st->endpoint,
		headers, headers_size, (const uint8_t*)body.data, body.size, deadline, secrets, 3,
		&response);
	as_aws_buf_destroy(&body);

	as_status status = AEROSPIKE_OK;

	if (rc == AS_AWS_HTTP_TIMEOUT) {
		status = as_error_update(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
			"AWS EC2 seed provider: " AWS_ACTION " timed out after %ums", st->timeout_ms);
	}
	else if (rc != AS_AWS_HTTP_OK) {
		status = err->code;
	}
	else if (response.status != 200) {
		status = as_aws_xml_error(err, AWS_ACTION, response.status, response.body,
			response.body_size, response.request_id, secrets, 3);
		as_aws_http_response_destroy(&response, true);
	}
	else {
		char reason[128];
		*out = as_aws_xml_parse(response.body, response.body_size, reason, sizeof(reason));

		if (!*out) {
			status = as_aws_errorf(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED, secrets, 3,
				"AWS EC2 seed provider: invalid " AWS_ACTION " response: %s", reason);
		}
		else if (strcmp(as_aws_xml_name(as_aws_xml_root(*out)), AWS_ACTION "Response") != 0) {
			as_aws_xml_destroy(*out);
			*out = NULL;
			status = as_error_set_message(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
				"AWS EC2 seed provider: invalid " AWS_ACTION " response: the root element is not "
				AWS_ACTION "Response");
		}
		as_aws_http_response_destroy(&response, false);
	}
	as_aws_free_secret(authorization);
	return status;
}

static void
aws_free_addresses(as_vector* addresses)
{
	for (uint32_t i = 0; i < addresses->size; i++) {
		cf_free(*(char**)as_vector_get(addresses, i));
	}
	as_vector_destroy(addresses);
}

static as_status
aws_refresh(as_error* err, const as_seed_candidate_context* context,
	as_seed_candidates* candidates, void* udata)
{
	(void)context;
	as_aws_state* st = udata;

	if (!st || st->magic != AS_AWS_STATE_MAGIC || !candidates || !candidates->hosts) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"Invalid AWS EC2 seed refresh arguments");
	}

	uint64_t deadline = cf_getns() + (uint64_t)st->timeout_ms * 1000 * 1000;
	const as_aws_credentials* credentials;
	as_status status = as_aws_credentials_get(err, st, deadline, &credentials);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	const char* element = aws_element(st->address_field);
	as_vector addresses;
	as_vector_init(&addresses, sizeof(char*), 16);

	char* token = NULL;
	uint32_t instances = 0;
	uint32_t skipped = 0;
	uint32_t pages = 0;

	do {
		as_aws_xml* doc = NULL;
		status = aws_describe(err, st, credentials, token, deadline, &doc);

		if (status != AEROSPIKE_OK) {
			break;
		}
		pages++;

		const as_aws_xml_node* root = as_aws_xml_root(doc);
		const as_aws_xml_node* reservation =
			as_aws_xml_child(as_aws_xml_child(root, "reservationSet"), "item");

		for (; reservation; reservation = as_aws_xml_next(reservation)) {
			const as_aws_xml_node* instance =
				as_aws_xml_child(as_aws_xml_child(reservation, "instancesSet"), "item");

			for (; instance; instance = as_aws_xml_next(instance)) {
				instances++;
				const char* address = as_aws_xml_text(instance, element);

				if (!aws_valid_address(address)) {
					skipped++;
					continue;
				}

				char* copy = cf_strdup(address);

				if (!copy) {
					status = as_error_set_message(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
						"AWS EC2 seed provider: failed to allocate an address");
					break;
				}
				as_vector_append(&addresses, &copy);
			}

			if (status != AEROSPIKE_OK) {
				break;
			}
		}

		const char* next = as_aws_xml_text(root, "nextToken");

		if (status == AEROSPIKE_OK && as_aws_xml_failed(doc)) {
			status = as_error_set_message(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
				"AWS EC2 seed provider: failed to allocate " AWS_ACTION " response text");
		}

		if (status == AEROSPIKE_OK && next && next[0]) {
			if (token && strcmp(token, next) == 0) {
				status = as_error_set_message(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
					"AWS EC2 seed provider: invalid " AWS_ACTION
					" response: pagination did not advance");
			}
			else {
				cf_free(token);
				token = cf_strdup(next);

				if (!token) {
					status = as_error_set_message(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
						"AWS EC2 seed provider: failed to allocate a page token");
				}
			}
		}
		else {
			cf_free(token);
			token = NULL;
		}
		as_aws_xml_destroy(doc);
	} while (status == AEROSPIKE_OK && token);

	cf_free(token);

	if (status != AEROSPIKE_OK) {
		aws_free_addresses(&addresses);
		return status;
	}

	if (addresses.size > 1) {
		qsort(addresses.list, addresses.size, sizeof(char*), aws_compare);
	}

	uint32_t hosts = 0;

	for (uint32_t i = 0; i < addresses.size; i++) {
		const char* address = *(char**)as_vector_get(&addresses, i);

		if (i > 0 && strcmp(address, *(char**)as_vector_get(&addresses, i - 1)) == 0) {
			continue;
		}

		if (!as_seed_candidates_append(candidates, address, st->tls_name, st->port)) {
			aws_free_addresses(&addresses);
			return as_error_set_message(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
				"AWS EC2 seed provider: failed to append a seed candidate");
		}
		hosts++;
	}
	aws_free_addresses(&addresses);

	if (skipped > 0) {
		as_log_debug("AWS EC2 seed provider skipped %u instances without %s", skipped, element);
	}
	as_log_debug("AWS EC2 seed provider %s returned %u hosts from %u instances in %u pages",
		st->region, hosts, instances, pages);
	return AEROSPIKE_OK;
}

static bool
aws_true(void* udata)
{
	(void)udata;
	return true;
}

static bool
aws_false(void* udata)
{
	(void)udata;
	return false;
}

//---------------------------------
// Functions
//---------------------------------

bool
as_aws_valid_key(const char* s)
{
	if (!s || !s[0]) {
		return false;
	}

	for (; *s; s++) {
		if (!((*s >= 'A' && *s <= 'Z') || (*s >= 'a' && *s <= 'z') || (*s >= '0' && *s <= '9'))) {
			return false;
		}
	}
	return true;
}

bool
as_aws_visible(const char* s)
{
	for (const unsigned char* p = (const unsigned char*)s; *p; p++) {
		if (*p < 0x21 || *p > 0x7e) {
			return false;
		}
	}
	return true;
}

char*
as_aws_ec2_endpoint(const char* region)
{
	as_aws_buf buf;
	as_aws_buf_init(&buf);
	as_aws_buf_printf(&buf, "https://ec2.%s.amazonaws.com%s", region,
		strncmp(region, "cn-", 3) == 0 ? ".cn" : "");
	return as_aws_buf_detach(&buf);
}

void
as_aws_seed_provider_config_init(as_aws_seed_provider_config* config)
{
	memset(config, 0, sizeof(as_aws_seed_provider_config));
	config->address_field = AS_AWS_ADDRESS_PRIVATE_IP;
	config->port = AS_AWS_DEFAULT_PORT;
	config->timeout_ms = AS_AWS_DEFAULT_TIMEOUT_MS;
}

as_status
as_discovery_aws_seed_provider(as_error* err, const as_aws_seed_provider_config* config,
	as_seed_provider* provider)
{
	return as_discovery_aws_seed_provider_env(err, config, NULL, provider);
}

as_status
as_discovery_aws_seed_provider_env(as_error* err, const as_aws_seed_provider_config* config,
	const as_aws_env* env, as_seed_provider* provider)
{
	as_error_reset(err);

	if (!config || !provider) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "Invalid AWS EC2 seed provider");
	}

	as_aws_state* st = NULL;
	as_status status = aws_create(err, config, env, &st);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	memset(provider, 0, sizeof(as_seed_provider));
	provider->size = sizeof(as_seed_provider);
	provider->api_version = AS_DISCOVERY_API_VERSION;
	provider->name = "aws-tags";
	provider->refresh = aws_refresh;
	provider->needs_periodic_refresh = aws_true;
	provider->supports_deadline = aws_false;
	provider->destroy = aws_destroy;
	provider->udata = st;
	return AEROSPIKE_OK;
}

as_status
as_config_set_aws_seed_provider(as_config* config, const as_aws_seed_provider_config* aws,
	as_error* err)
{
	if (!config) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "Invalid seed provider");
	}

	if (config->discovery.registration_locked) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"Seed provider cannot be replaced while the client is connected");
	}

	if (config->discovery.execution_mode != AS_DISCOVERY_EXECUTION_THREAD) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"AWS EC2 seed provider requires thread execution mode");
	}

	as_seed_provider provider;
	as_status status = as_discovery_aws_seed_provider(err, aws, &provider);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = as_config_set_seed_provider(config, &provider, err);

	if (status != AEROSPIKE_OK) {
		aws_release_hook(provider.udata);
		provider.destroy(provider.udata);
	}
	return status;
}

bool
as_aws_seed_provider_timeout(const as_seed_provider* provider, uint32_t* timeout_ms)
{
	if (!provider || provider->refresh != aws_refresh || !provider->udata) {
		return false;
	}

	const as_aws_state* st = provider->udata;

	if (st->magic != AS_AWS_STATE_MAGIC) {
		return false;
	}
	*timeout_ms = st->timeout_ms;
	return true;
}
