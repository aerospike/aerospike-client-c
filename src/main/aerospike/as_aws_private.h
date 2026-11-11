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
#pragma once

// Private to the client library. Not installed.

#include <aerospike/as_aws_internal.h>
#include <aerospike/as_discovery.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Macros
//---------------------------------

#define AS_AWS_STATE_MAGIC 0x41575331

// Cap on every byte read for one response, headers included.
#define AS_AWS_HTTP_MAX_RESPONSE (16 * 1024 * 1024)

#define AS_AWS_HTTP_REQUEST_ID_SIZE 128

//---------------------------------
// Types
//---------------------------------

struct ssl_ctx_st;

/**
 * Growable byte buffer, always terminated. A failed allocation is sticky: later appends do
 * nothing and as_aws_buf_ok() returns false. Every superseded allocation is cleansed.
 */
typedef struct as_aws_buf_s {
	char* data;
	size_t size;
	size_t capacity;
	bool failed;
} as_aws_buf;

/**
 * Parsed http or https URL.
 */
typedef struct as_aws_url_s {
	bool tls;

	// Unbracketed host, used for name resolution, SNI and the certificate identity check.
	char* host;

	uint16_t port;

	// Host header value: the host, bracketed for IPv6, with ":port" when the URL has a port.
	char* host_header;

	// Raw path, "/" when the URL has none.
	char* path;

	// Raw query without the '?', or NULL.
	char* query;
} as_aws_url;

/**
 * Private TLS client context of one provider.
 */
typedef struct as_aws_tls_s {
	struct ssl_ctx_st* ctx;

	// Trust comes from the OpenSSL default verify paths (no ca_file or ca_path).
	bool default_trust;

	// System CA bundle also loaded because the OpenSSL default certificate file is missing, or
	// NULL. Static storage.
	const char* trust_bundle;
} as_aws_tls;

typedef enum as_aws_http_rc_e {
	AS_AWS_HTTP_OK,
	AS_AWS_HTTP_TIMEOUT,
	AS_AWS_HTTP_ERROR
} as_aws_http_rc;

typedef struct as_aws_http_response_s {
	int status;

	// x-amzn-requestid header, empty when absent.
	char request_id[AS_AWS_HTTP_REQUEST_ID_SIZE];

	// Terminated after body_size bytes. NULL when the response has no body.
	uint8_t* body;
	size_t body_size;
} as_aws_http_response;

// Immutable after creation except source_logged, which only refresh writes.
typedef struct as_aws_state_s {
	uint32_t magic;
	char* region;
	as_aws_address_field address_field;
	uint16_t port;
	char* tls_name;
	uint32_t timeout_ms;

	// DescribeInstances form body without NextToken.
	char* query;

	as_aws_url endpoint;
	as_aws_tls tls;
	char* access_key_id;
	char* secret_access_key;
	char* session_token;
	bool source_logged;
} as_aws_state;

//---------------------------------
// Functions
//---------------------------------

void
as_aws_buf_init(as_aws_buf* buf);

bool
as_aws_buf_append(as_aws_buf* buf, const void* data, size_t size);

bool
as_aws_buf_append_str(as_aws_buf* buf, const char* text);

bool
as_aws_buf_printf(as_aws_buf* buf, const char* fmt, ...);

static inline bool
as_aws_buf_ok(const as_aws_buf* buf)
{
	return !buf->failed;
}

/**
 * Return the terminated data and leave buf empty, or NULL (after cleansing) when an allocation
 * failed.
 */
char*
as_aws_buf_detach(as_aws_buf* buf);

/**
 * Cleanse and free the data.
 */
void
as_aws_buf_destroy(as_aws_buf* buf);

/**
 * Cleanse and free a heap string. NULL is ignored.
 */
void
as_aws_free_secret(char* text);

/**
 * Format a message in a heap buffer, redact secrets, then set err with code and the result.
 */
as_status
as_aws_errorf(as_error* err, as_status code, const char* const* secrets, uint32_t secrets_size,
	const char* fmt, ...);

/**
 * Parse "scheme://host[:port][/path][?query]" with an http or https scheme. Userinfo, a
 * fragment, an unbracketed IPv6 host and a port outside 1..65535 are rejected.
 */
bool
as_aws_url_parse(const char* text, as_aws_url* url);

void
as_aws_url_destroy(as_aws_url* url);

/**
 * Create the TLS context: TLS 1.2 or later, peer verification against ca_file and ca_path when
 * either is set, otherwise the OpenSSL default verify paths plus the system CA bundle chosen by
 * as_aws_trust_bundle() (not on Windows). Ignores SIGPIPE when it has the default handler.
 * Fails with AEROSPIKE_ERR_TLS_ERROR.
 */
as_status
as_aws_tls_init(as_error* err, const char* ca_file, const char* ca_path, as_aws_tls* tls);

void
as_aws_tls_destroy(as_aws_tls* tls);

/**
 * Send one HTTP/1.1 request on a new connection and read the response, every step bounded by
 * deadline_ns (cf_getns()). Header names must be lowercase; host is added when absent. body NULL
 * sends no content-length. On AS_AWS_HTTP_OK the caller destroys response. Otherwise err holds
 * "AWS EC2 seed provider: <action> timed out" or "AWS EC2 seed provider: <action> failed:
 * <reason>", redacted against secrets.
 */
as_aws_http_rc
as_aws_http_request(as_error* err, const as_aws_tls* tls, const char* action, const char* method,
	const as_aws_url* url, const as_aws_header* headers, uint32_t headers_size,
	const uint8_t* body, size_t body_size, uint64_t deadline_ns, const char* const* secrets,
	uint32_t secrets_size, as_aws_http_response* response);

/**
 * Free the body, cleansing it first when sensitive.
 */
void
as_aws_http_response_destroy(as_aws_http_response* response, bool sensitive);

/**
 * Return true and set timeout_ms to the configured refresh timeout when provider is the built-in
 * AWS EC2 seed provider. Identification uses the function table and state, never the provider
 * name.
 */
bool
as_aws_seed_provider_timeout(const as_seed_provider* provider, uint32_t* timeout_ms);

#ifdef __cplusplus
} // end extern "C"
#endif
