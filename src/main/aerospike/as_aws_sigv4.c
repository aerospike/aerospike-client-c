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
#include <openssl/opensslv.h>

#if OPENSSL_VERSION_NUMBER < 0x10100000L
#error "the aws-tags seed provider requires OpenSSL 1.1.0 or later"
#endif

#include <aerospike/as_aws_internal.h>
#include <citrusleaf/alloc.h>

#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>

#include <string.h>
#include <time.h>

#include "as_aws_private.h"

//---------------------------------
// Macros
//---------------------------------

#define AWS_ALGORITHM "AWS4-HMAC-SHA256"

//---------------------------------
// Static Functions
//---------------------------------

static bool
sigv4_hmac(const uint8_t* key, size_t key_size, const char* data, uint8_t out[32])
{
	unsigned int len = 0;
	return HMAC(EVP_sha256(), key, (int)key_size, (const unsigned char*)data, strlen(data), out,
		&len) != NULL && len == 32;
}

static bool
sigv4_is_space(char c)
{
	// Java String.trim() removes every character up to and including the space.
	return (unsigned char)c <= ' ';
}

// Sort header pointers by name. The header count is small.
static void
sigv4_sort(const as_aws_header** sorted, uint32_t size)
{
	for (uint32_t i = 1; i < size; i++) {
		const as_aws_header* h = sorted[i];
		uint32_t j = i;

		while (j > 0 && strcmp(sorted[j - 1]->name, h->name) > 0) {
			sorted[j] = sorted[j - 1];
			j--;
		}
		sorted[j] = h;
	}
}

static bool
sigv4_append_signed(as_aws_buf* buf, const as_aws_header** sorted, uint32_t size)
{
	for (uint32_t i = 0; i < size; i++) {
		if (i > 0) {
			as_aws_buf_append(buf, ";", 1);
		}
		as_aws_buf_append_str(buf, sorted[i]->name);
	}
	return as_aws_buf_ok(buf);
}

static const as_aws_header**
sigv4_sorted(const as_aws_header* headers, uint32_t size)
{
	const as_aws_header** sorted = cf_malloc((size ? size : 1) * sizeof(as_aws_header*));

	if (!sorted) {
		return NULL;
	}

	for (uint32_t i = 0; i < size; i++) {
		sorted[i] = &headers[i];
	}
	sigv4_sort(sorted, size);
	return sorted;
}

//---------------------------------
// Functions
//---------------------------------

void
as_aws_hex(const uint8_t* data, size_t size, char* out)
{
	static const char hex[] = "0123456789abcdef";

	for (size_t i = 0; i < size; i++) {
		out[i * 2] = hex[data[i] >> 4];
		out[i * 2 + 1] = hex[data[i] & 0xf];
	}
	out[size * 2] = 0;
}

bool
as_aws_sha256_hex(const void* data, size_t size, char out[AS_AWS_SHA256_HEX_SIZE])
{
	uint8_t md[EVP_MAX_MD_SIZE];
	unsigned int len = 0;

	if (!EVP_Digest(data, size, md, &len, EVP_sha256(), NULL) || len != 32) {
		return false;
	}
	as_aws_hex(md, 32, out);
	return true;
}

char*
as_aws_encode(const char* value)
{
	static const char hex[] = "0123456789ABCDEF";
	size_t len = strlen(value);
	char* out = cf_malloc(len * 3 + 1);

	if (!out) {
		return NULL;
	}

	char* p = out;

	for (const unsigned char* s = (const unsigned char*)value; *s; s++) {
		unsigned char c = *s;

		if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
			c == '-' || c == '_' || c == '.' || c == '~') {
			*p++ = (char)c;
		}
		else {
			*p++ = '%';
			*p++ = hex[c >> 4];
			*p++ = hex[c & 0xf];
		}
	}
	*p = 0;
	return out;
}

bool
as_aws_amz_date(uint64_t epoch_ms, char out[AS_AWS_AMZ_DATE_SIZE])
{
	time_t t = (time_t)(epoch_ms / 1000);
	struct tm tm;

#if defined(_MSC_VER)
	if (gmtime_s(&tm, &t) != 0) {
		return false;
	}
#else
	if (!gmtime_r(&t, &tm)) {
		return false;
	}
#endif
	return strftime(out, AS_AWS_AMZ_DATE_SIZE, "%Y%m%dT%H%M%SZ", &tm) == AS_AWS_AMZ_DATE_SIZE - 1;
}

bool
as_aws_sigv4_signing_key(const char* secret_access_key, const char* date, const char* region,
	const char* service, uint8_t key[32])
{
	size_t secret_len = strlen(secret_access_key);
	uint8_t* first = cf_malloc(secret_len + 4);

	if (!first) {
		return false;
	}

	memcpy(first, "AWS4", 4);
	memcpy(first + 4, secret_access_key, secret_len);

	uint8_t k1[32];
	uint8_t k2[32];
	uint8_t k3[32];
	bool ok = sigv4_hmac(first, secret_len + 4, date, k1) && sigv4_hmac(k1, 32, region, k2) &&
		sigv4_hmac(k2, 32, service, k3) && sigv4_hmac(k3, 32, "aws4_request", key);

	OPENSSL_cleanse(first, secret_len + 4);
	cf_free(first);
	OPENSSL_cleanse(k1, sizeof(k1));
	OPENSSL_cleanse(k2, sizeof(k2));
	OPENSSL_cleanse(k3, sizeof(k3));

	if (!ok) {
		OPENSSL_cleanse(key, 32);
	}
	return ok;
}

char*
as_aws_sigv4_canonical_request(const char* method, const char* path, const char* query,
	const as_aws_header* headers, uint32_t headers_size, const char* payload_hash)
{
	const as_aws_header** sorted = sigv4_sorted(headers, headers_size);

	if (!sorted) {
		return NULL;
	}

	as_aws_buf buf;
	as_aws_buf_init(&buf);
	as_aws_buf_printf(&buf, "%s\n%s\n%s\n", method, path, query ? query : "");

	for (uint32_t i = 0; i < headers_size; i++) {
		const char* v = sorted[i]->value;
		size_t len = strlen(v);

		while (len > 0 && sigv4_is_space(*v)) {
			v++;
			len--;
		}

		while (len > 0 && sigv4_is_space(v[len - 1])) {
			len--;
		}

		as_aws_buf_append_str(&buf, sorted[i]->name);
		as_aws_buf_append(&buf, ":", 1);
		as_aws_buf_append(&buf, v, len);
		as_aws_buf_append(&buf, "\n", 1);
	}

	as_aws_buf_append(&buf, "\n", 1);
	sigv4_append_signed(&buf, sorted, headers_size);
	as_aws_buf_append(&buf, "\n", 1);
	as_aws_buf_append_str(&buf, payload_hash);
	cf_free(sorted);
	return as_aws_buf_detach(&buf);
}

char*
as_aws_sigv4_string_to_sign(const char* amz_date, const char* scope, const char* canonical_request)
{
	char hash[AS_AWS_SHA256_HEX_SIZE];

	if (!as_aws_sha256_hex(canonical_request, strlen(canonical_request), hash)) {
		return NULL;
	}

	as_aws_buf buf;
	as_aws_buf_init(&buf);
	as_aws_buf_printf(&buf, AWS_ALGORITHM "\n%s\n%s\n%s", amz_date, scope, hash);
	return as_aws_buf_detach(&buf);
}

char*
as_aws_sigv4_authorization(const char* access_key_id, const char* secret_access_key,
	const char* method, const char* path, const char* query, const as_aws_header* headers,
	uint32_t headers_size, const uint8_t* payload, size_t payload_size, const char* region,
	const char* service, const char* amz_date)
{
	if (strlen(amz_date) != AS_AWS_AMZ_DATE_SIZE - 1) {
		return NULL;
	}

	char date[9];
	memcpy(date, amz_date, 8);
	date[8] = 0;

	char payload_hash[AS_AWS_SHA256_HEX_SIZE];

	if (!as_aws_sha256_hex(payload ? (const void*)payload : (const void*)"", payload_size,
		payload_hash)) {
		return NULL;
	}

	as_aws_buf scope;
	as_aws_buf_init(&scope);
	as_aws_buf_printf(&scope, "%s/%s/%s/aws4_request", date, region, service);

	if (!as_aws_buf_ok(&scope)) {
		as_aws_buf_destroy(&scope);
		return NULL;
	}

	// The canonical request carries x-amz-security-token, so it is cleansed.
	char* canonical = as_aws_sigv4_canonical_request(method, path, query, headers, headers_size,
		payload_hash);
	char* to_sign = canonical ? as_aws_sigv4_string_to_sign(amz_date, scope.data, canonical) : NULL;
	as_aws_free_secret(canonical);

	uint8_t key[32];
	uint8_t signature[32];
	bool ok = to_sign && as_aws_sigv4_signing_key(secret_access_key, date, region, service, key);

	if (ok) {
		ok = sigv4_hmac(key, sizeof(key), to_sign, signature);
		OPENSSL_cleanse(key, sizeof(key));
	}
	cf_free(to_sign);

	if (!ok) {
		as_aws_buf_destroy(&scope);
		return NULL;
	}

	char signature_hex[65];
	as_aws_hex(signature, sizeof(signature), signature_hex);

	const as_aws_header** sorted = sigv4_sorted(headers, headers_size);

	if (!sorted) {
		as_aws_buf_destroy(&scope);
		return NULL;
	}

	as_aws_buf buf;
	as_aws_buf_init(&buf);
	as_aws_buf_printf(&buf, AWS_ALGORITHM " Credential=%s/%s, SignedHeaders=", access_key_id,
		scope.data);
	sigv4_append_signed(&buf, sorted, headers_size);
	as_aws_buf_printf(&buf, ", Signature=%s", signature_hex);
	cf_free(sorted);
	as_aws_buf_destroy(&scope);
	return as_aws_buf_detach(&buf);
}
