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

// Private to the client library and its tests. Not installed.

#include <aerospike/as_discovery.h>
#include <aerospike/as_error.h>
#include <aerospike/as_std.h>

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Macros
//---------------------------------

/**
 * @private
 * Size of a lowercase hex SHA-256 digest with its terminator.
 */
#define AS_AWS_SHA256_HEX_SIZE 65

/**
 * @private
 * Size of an x-amz-date value ("20150830T123600Z") with its terminator.
 */
#define AS_AWS_AMZ_DATE_SIZE 17

/**
 * @private
 * Replacement of each secret in messages.
 */
#define AS_AWS_REDACTED "<redacted>"

//---------------------------------
// Types
//---------------------------------

/**
 * @private
 * One request header. The name must be lowercase.
 */
typedef struct as_aws_header_s {
	const char* name;
	const char* value;
} as_aws_header;

/**
 * @private
 * Parsed XML document. Element names keep any namespace prefix.
 */
typedef struct as_aws_xml_s as_aws_xml;

/**
 * @private
 * Element of a parsed XML document, valid until the document is destroyed.
 */
typedef struct as_aws_xml_node_s as_aws_xml_node;

/**
 * @private
 * Environment of the AWS EC2 seed provider's credential chain: the injection seam that points
 * the chain at loopback servers, a fake environment and a fake clock. Production passes NULL,
 * which is the same as every member NULL. The strings are copied at construction.
 */
typedef struct as_aws_env_s {
	/**
	 * Return the value of environment variable name, or NULL. Called only during construction.
	 * NULL reads the process environment.
	 */
	const char* (*getenv)(const char* name, void* udata);

	/**
	 * Home directory. NULL reads HOME (USERPROFILE on Windows) through getenv; only when getenv
	 * is NULL does an unset or empty HOME fall back to the passwd entry (POSIX).
	 */
	const char* home;

	/**
	 * STS base URL, which may carry a path. NULL: https://sts.<region>.amazonaws.com[.cn].
	 */
	const char* sts_base;

	/**
	 * Base URL that AWS_CONTAINER_CREDENTIALS_RELATIVE_URI is appended to. NULL:
	 * http://169.254.170.2.
	 */
	const char* ecs_base;

	/**
	 * IMDS base URL. NULL: http://169.254.169.254.
	 */
	const char* imds_base;

	/**
	 * Return the wall-clock time in milliseconds since the epoch. NULL: cf_clock_getabsolute().
	 */
	uint64_t (*now_ms)(void* udata);

	/**
	 * Passed to getenv and now_ms. Must outlive the provider when now_ms is set.
	 */
	void* udata;
} as_aws_env;

/**
 * @private
 * One key of a flat JSON object.
 */
typedef struct as_aws_json_entry_s {
	char* key;
	char* value;
} as_aws_json_entry;

/**
 * @private
 * Flat JSON object: entries in the order their keys first appeared.
 */
typedef struct as_aws_json_s {
	as_aws_json_entry* entries;
	uint32_t size;
	uint32_t capacity;
} as_aws_json;

/**
 * @private
 * Static keys of one profile of a shared credentials file. NULL when absent.
 */
typedef struct as_aws_profile_s {
	char* access_key_id;
	char* secret_access_key;
	char* session_token;
} as_aws_profile;

//---------------------------------
// Functions: construction
//---------------------------------

/**
 * @private
 * as_discovery_aws_seed_provider() with an injected environment. env NULL is production.
 */
AS_EXTERN as_status
as_discovery_aws_seed_provider_env(as_error* err, const as_aws_seed_provider_config* config,
	const as_aws_env* env, as_seed_provider* provider);

//---------------------------------
// Functions: SigV4 and encoding
//---------------------------------

/**
 * @private
 * Write size bytes of data as lowercase hex followed by a terminator; out holds 2 * size + 1.
 */
AS_EXTERN void
as_aws_hex(const uint8_t* data, size_t size, char* out);

/**
 * @private
 * Write the lowercase hex SHA-256 digest of data. Returns false if the digest fails.
 */
AS_EXTERN bool
as_aws_sha256_hex(const void* data, size_t size, char out[AS_AWS_SHA256_HEX_SIZE]);

/**
 * @private
 * Percent-encode every byte except the RFC 3986 unreserved characters (A-Z a-z 0-9 - _ . ~),
 * with uppercase hex. Returns a heap string the caller frees with cf_free(), or NULL when the
 * allocation fails.
 */
AS_EXTERN char*
as_aws_encode(const char* value);

/**
 * @private
 * Format epoch milliseconds as an x-amz-date value (yyyyMMdd'T'HHmmss'Z', UTC).
 */
AS_EXTERN bool
as_aws_amz_date(uint64_t epoch_ms, char out[AS_AWS_AMZ_DATE_SIZE]);

/**
 * @private
 * Derive the SigV4 signing key of date (yyyyMMdd), region and service.
 */
AS_EXTERN bool
as_aws_sigv4_signing_key(const char* secret_access_key, const char* date, const char* region,
	const char* service, uint8_t key[32]);

/**
 * @private
 * Return the canonical request. Headers are sorted by name and their values trimmed, without
 * collapsing inner spaces. The caller cleanses and frees the heap result; NULL on allocation
 * failure.
 */
AS_EXTERN char*
as_aws_sigv4_canonical_request(const char* method, const char* path, const char* query,
	const as_aws_header* headers, uint32_t headers_size, const char* payload_hash);

/**
 * @private
 * Return the string to sign of a canonical request. The caller frees the heap result; NULL on
 * failure.
 */
AS_EXTERN char*
as_aws_sigv4_string_to_sign(const char* amz_date, const char* scope, const char* canonical_request);

/**
 * @private
 * Return the AWS4-HMAC-SHA256 Authorization header value of one request, signing every header
 * in headers. The caller frees the heap result; NULL on failure.
 */
AS_EXTERN char*
as_aws_sigv4_authorization(const char* access_key_id, const char* secret_access_key,
	const char* method, const char* path, const char* query, const as_aws_header* headers,
	uint32_t headers_size, const uint8_t* payload, size_t payload_size, const char* region,
	const char* service, const char* amz_date);

/**
 * @private
 * Return the regional EC2 endpoint: https://ec2.<region>.amazonaws.com, with a .cn suffix for
 * cn-* regions. The caller frees the heap result with cf_free(); NULL on allocation failure.
 */
AS_EXTERN char*
as_aws_ec2_endpoint(const char* region);

//---------------------------------
// Functions: TLS trust
//---------------------------------

/**
 * @private
 * Choose the system CA bundle to load besides the OpenSSL default verify paths when neither
 * ca_file nor ca_path is set. Returns NULL when env_file (SSL_CERT_FILE) or env_dir
 * (SSL_CERT_DIR) is non-empty or default_file (the default certificate file of the OpenSSL
 * build) is a regular file; otherwise the first of candidates that is a regular file, or NULL.
 */
AS_EXTERN const char*
as_aws_trust_bundle(const char* env_file, const char* env_dir, const char* default_file,
	const char* const* candidates, uint32_t candidates_size);

//---------------------------------
// Functions: messages
//---------------------------------

/**
 * @private
 * Return a heap copy of text with every non-empty secret replaced by AS_AWS_REDACTED. NULL
 * secrets are skipped. The caller frees the result; NULL on allocation failure.
 */
AS_EXTERN char*
as_aws_redact(const char* text, const char* const* secrets, uint32_t secrets_size);

/**
 * @private
 * Set err to AEROSPIKE_ERR_SEED_DISCOVERY_FAILED with an AWS Query API error message:
 * "AWS EC2 seed provider: <action> failed: HTTP <status>[ <Code>][: <Message>][ (request id <id>)]".
 * Code, Message and RequestID (else RequestId) are the first descendants of those names in body;
 * the request id falls back to header_request_id. A body that is not XML is ignored. The full
 * message is redacted before it is copied into err.
 */
AS_EXTERN as_status
as_aws_xml_error(as_error* err, const char* action, int status, const uint8_t* body,
	size_t body_size, const char* header_request_id, const char* const* secrets,
	uint32_t secrets_size);

//---------------------------------
// Functions: XML
//---------------------------------

/**
 * @private
 * Parse a complete document. DOCTYPE and ENTITY declarations, NUL characters, invalid character
 * references, nesting deeper than 64 elements and more than 1,000,000 elements are rejected.
 * Attributes, comments and processing instructions are skipped. Returns NULL with a reason on
 * failure.
 */
AS_EXTERN as_aws_xml*
as_aws_xml_parse(const uint8_t* data, size_t size, char* reason, size_t reason_size);

/**
 * @private
 * Release a document and all of its elements.
 */
AS_EXTERN void
as_aws_xml_destroy(as_aws_xml* doc);

/**
 * @private
 * Return the root element.
 */
AS_EXTERN const as_aws_xml_node*
as_aws_xml_root(const as_aws_xml* doc);

/**
 * @private
 * Return the element name.
 */
AS_EXTERN const char*
as_aws_xml_name(const as_aws_xml_node* node);

/**
 * @private
 * Return the first direct child element of parent named name, or NULL. parent may be NULL.
 */
AS_EXTERN const as_aws_xml_node*
as_aws_xml_child(const as_aws_xml_node* parent, const char* name);

/**
 * @private
 * Return the next sibling element of node with the same name, or NULL. With as_aws_xml_child()
 * this iterates the direct children of one name.
 */
AS_EXTERN const as_aws_xml_node*
as_aws_xml_next(const as_aws_xml_node* node);

/**
 * @private
 * Return the trimmed concatenation of every text and CDATA section inside node, or NULL when the
 * allocation fails, which as_aws_xml_failed() then reports. The string is owned by the document.
 */
AS_EXTERN const char*
as_aws_xml_content(const as_aws_xml_node* node);

/**
 * @private
 * Return true when a content allocation of doc failed. A lookup that returned NULL may then
 * have found its element, so results read from doc are incomplete.
 */
AS_EXTERN bool
as_aws_xml_failed(const as_aws_xml* doc);

/**
 * @private
 * Return the content of the first direct child element of parent named name, or NULL.
 */
AS_EXTERN const char*
as_aws_xml_text(const as_aws_xml_node* parent, const char* name);

/**
 * @private
 * Return the content of the first descendant element of node named name, in document order and
 * excluding node itself, or NULL.
 */
AS_EXTERN const char*
as_aws_xml_find(const as_aws_xml_node* node, const char* name);

//---------------------------------
// Functions: JSON, INI and ISO-8601
//---------------------------------

/**
 * @private
 * Parse one flat JSON object into string values: strings, numbers, true and false; null is
 * omitted and nested objects and arrays (up to 64 levels) are skipped, so they leave an earlier
 * value of a repeated key in place; otherwise a repeated key keeps the last value. \u0000, lone
 * surrogates, short \u sequences, other unknown escapes, NUL bytes, truncation and anything but
 * whitespace after the object are rejected. Returns false on invalid input or allocation
 * failure, with json empty.
 */
AS_EXTERN bool
as_aws_json_parse(const uint8_t* data, size_t size, as_aws_json* json);

/**
 * @private
 * Return the value of key, or NULL.
 */
AS_EXTERN const char*
as_aws_json_get(const as_aws_json* json, const char* key);

/**
 * @private
 * Cleanse and release every entry.
 */
AS_EXTERN void
as_aws_json_destroy(as_aws_json* json);

/**
 * @private
 * Read the static keys of profile from a shared credentials file. A leading UTF-8 byte order mark
 * is skipped. Lines end at LF or CR and are trimmed; blank lines and lines starting with '#' or
 * ';' are skipped. A line starting with '[' ends the current section; "[name]", optionally
 * followed by a '#' or ';' comment, starts section name (trimmed; a repeated section is merged),
 * and any other '[' line starts no section. "key = value" lines split at the first '=', both
 * sides trimmed, inline comments kept. A repeated key keeps the last value. Only
 * aws_access_key_id, aws_secret_access_key and aws_session_token are kept. Returns false on
 * allocation failure, with out empty.
 */
AS_EXTERN bool
as_aws_ini_profile(const char* text, size_t size, const char* profile, as_aws_profile* out);

/**
 * @private
 * Cleanse and release the keys.
 */
AS_EXTERN void
as_aws_profile_destroy(as_aws_profile* profile);

/**
 * @private
 * Parse YYYY-MM-DDTHH:MM:SS with an optional fraction of 1 to 9 digits (truncated to
 * milliseconds) and Z or a +HH:MM or -HH:MM offset into milliseconds since the epoch. Fields
 * out of range and times at or before 1970-01-01T00:00:00Z are rejected.
 */
AS_EXTERN bool
as_aws_iso8601(const char* text, uint64_t* epoch_ms);

#ifdef __cplusplus
} // end extern "C"
#endif
