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
#include <aerospike/as_log_macros.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_clock.h>

#include <openssl/crypto.h>

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#if defined(_MSC_VER)
#include <io.h>
#else
#include <unistd.h>
#endif

#if !defined(_WIN32)
#include <pwd.h>
#endif

#include "as_aws_private.h"

//---------------------------------
// Macros
//---------------------------------

#define AWS_PREFIX "AWS EC2 seed provider: "
#define AWS_NO_SOURCE "no AWS credentials found in config, environment, shared credentials " \
	"file, web identity or ECS, and "
#define AWS_FORM "application/x-www-form-urlencoded; charset=utf-8"
#define AWS_STS "STS AssumeRoleWithWebIdentity"
#define AWS_ECS "ECS credentials"
#define AWS_IMDS_TOKEN "IMDS token"
#define AWS_IMDS_ROLE "IMDS role"
#define AWS_IMDS_CREDENTIALS "IMDS credentials"
#define AWS_IMDS_ROLES_PATH "/latest/meta-data/iam/security-credentials/"
#define AWS_ECS_BASE "http://169.254.170.2"
#define AWS_IMDS_BASE "http://169.254.169.254"
#define AWS_REFRESH_MARGIN_MS 300000
#define AWS_FILE_MAX (1024 * 1024)
#define AWS_ROLE_MAX 128

#if defined(_WIN32)
#define AWS_HOME_VAR "USERPROFILE"
#define AWS_PATH_SEP "\\"
#else
#define AWS_HOME_VAR "HOME"
#define AWS_PATH_SEP "/"
#endif

//---------------------------------
// Types
//---------------------------------

typedef enum aws_file_rc_e {
	AWS_FILE_OK,
	AWS_FILE_OPEN_FAILED,
	AWS_FILE_NOT_REGULAR,
	AWS_FILE_FAILED
} aws_file_rc;

//---------------------------------
// Globals
//---------------------------------

static const char* const aws_var_names[AS_AWS_VAR_SIZE] = {
	"AWS_ACCESS_KEY_ID",
	"AWS_SECRET_ACCESS_KEY",
	"AWS_SESSION_TOKEN",
	"AWS_SHARED_CREDENTIALS_FILE",
	"AWS_PROFILE",
	"AWS_WEB_IDENTITY_TOKEN_FILE",
	"AWS_ROLE_ARN",
	"AWS_ROLE_SESSION_NAME",
	"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
	"AWS_CONTAINER_CREDENTIALS_FULL_URI",
	"AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
	"AWS_CONTAINER_AUTHORIZATION_TOKEN",
	"AWS_EC2_METADATA_DISABLED"
};

static const char* const aws_source_labels[] = {
	"none",
	"config",
	"environment",
	"profile",
	"web-identity",
	"ecs",
	"imds",
	"hook"
};

//---------------------------------
// Static Functions: helpers
//---------------------------------

static as_status
aws_alloc_failed(as_error* err)
{
	return as_error_set_message(err, AEROSPIKE_ERR_CLIENT,
		"Failed to allocate AWS EC2 seed provider");
}

static as_status
aws_fail(as_error* err, const char* message)
{
	return as_error_set_message(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED, message);
}

static bool
aws_present(const char* s)
{
	return s && s[0];
}

static void
aws_error_text(int e, char* out, size_t size)
{
#if !defined(_MSC_VER)
	snprintf(out, size, "%s", strerror(e));
#else
	snprintf(out, size, "error %d", e);
#endif
}

// Trim as Java String.trim() in place: every byte up to and including the space, at both ends.
static char*
aws_trim(char* s)
{
	while (*s && (unsigned char)*s <= ' ') {
		s++;
	}

	size_t len = strlen(s);

	while (len > 0 && (unsigned char)s[len - 1] <= ' ') {
		s[--len] = 0;
	}
	return s;
}

// Trimmed text of a terminated file or response buffer, edited in place. "" for no buffer, which
// callers never write to; NULL when the buffer holds a NUL byte.
static char*
aws_text(char* data, size_t size)
{
	static char empty[1];

	if (!data) {
		return empty;
	}

	if (memchr(data, 0, size)) {
		return NULL;
	}
	return aws_trim(data);
}

static bool
aws_true(const char* s)
{
	const char* word = "true";

	for (; *word; s++, word++) {
		char c = *s;

		if (c >= 'A' && c <= 'Z') {
			c = (char)(c - 'A' + 'a');
		}

		if (c != *word) {
			return false;
		}
	}
	return *s == 0;
}

// [A-Za-z0-9+=,.@_-]{1,128}, neither "." nor "..".
static bool
aws_valid_role(const char* s)
{
	size_t len = strlen(s);

	if (len == 0 || len > AWS_ROLE_MAX || strcmp(s, ".") == 0 || strcmp(s, "..") == 0) {
		return false;
	}

	for (size_t i = 0; i < len; i++) {
		char c = s[i];

		if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
			strchr("+=,.@_-", c))) {
			return false;
		}
	}
	return true;
}

// Printable ASCII, space included: the ECS authorization token may carry a scheme such as
// "Bearer <token>".
static bool
aws_printable(const char* s)
{
	for (const unsigned char* p = (const unsigned char*)s; *p; p++) {
		if (*p < 0x20 || *p > 0x7e) {
			return false;
		}
	}
	return true;
}

static char*
aws_concat(const char* a, const char* b, const char* c)
{
	as_aws_buf buf;
	as_aws_buf_init(&buf);
	as_aws_buf_append_str(&buf, a);
	as_aws_buf_append_str(&buf, b);

	if (c) {
		as_aws_buf_append_str(&buf, c);
	}
	return as_aws_buf_detach(&buf);
}

static void
aws_endpoint_init(as_aws_endpoint* endpoint, const char* text)
{
	endpoint->valid = as_aws_url_parse(text, &endpoint->url);
}

static void
aws_credentials_clear(as_aws_credentials* credentials)
{
	as_aws_free_secret(credentials->access_key_id);
	as_aws_free_secret(credentials->secret_access_key);
	as_aws_free_secret(credentials->session_token);
	memset(credentials, 0, sizeof(as_aws_credentials));
}

// Copy into out, an empty token becoming NULL. False, with out zeroed, on allocation failure.
static bool
aws_credentials_copy(as_aws_credentials* out, const char* access_key_id,
	const char* secret_access_key, const char* session_token, uint64_t expiration_ms)
{
	memset(out, 0, sizeof(as_aws_credentials));
	out->access_key_id = cf_strdup(access_key_id);
	out->secret_access_key = cf_strdup(secret_access_key);
	out->expiration_ms = expiration_ms;

	bool token = aws_present(session_token);

	if (token) {
		out->session_token = cf_strdup(session_token);
	}

	if (!out->access_key_id || !out->secret_access_key || (token && !out->session_token)) {
		aws_credentials_clear(out);
		return false;
	}
	return true;
}

//---------------------------------
// Static Functions: construction
//---------------------------------

static const char*
aws_process_getenv(const char* name, void* udata)
{
	(void)udata;
	return getenv(name);
}

#if !defined(_WIN32)
// Home directory of the passwd entry of the process user. False on allocation failure only.
static bool
aws_passwd_home(char** home)
{
	long max = sysconf(_SC_GETPW_R_SIZE_MAX);
	size_t size = max > 0 ? (size_t)max : 16384;

	while (true) {
		char* buf = cf_malloc(size);

		if (!buf) {
			return false;
		}

		struct passwd pw;
		struct passwd* result = NULL;
		int rv = getpwuid_r(getuid(), &pw, buf, size, &result);

		if (rv == ERANGE && size < AWS_FILE_MAX) {
			cf_free(buf);
			size *= 2;
			continue;
		}

		bool ok = true;

		if (rv == 0 && result && aws_present(result->pw_dir)) {
			*home = cf_strdup(result->pw_dir);
			ok = *home != NULL;
		}
		cf_free(buf);
		return ok;
	}
}
#endif

static char*
aws_sts_url(const char* region)
{
	const char* suffix = strncmp(region, "cn-", 3) == 0 ? ".amazonaws.com.cn" : ".amazonaws.com";
	return aws_concat("https://sts.", region, suffix);
}

//---------------------------------
// Static Functions: file reads
//---------------------------------

// Read a regular file of at most 1 MiB into buf, which the caller always destroys. The open never
// blocks, so a FIFO is rejected instead of waiting for a writer.
static aws_file_rc
aws_read_file(const char* path, as_aws_buf* buf, char* reason, size_t reason_size)
{
	as_aws_buf_init(buf);

	char chunk[4096];
	aws_file_rc rc = AWS_FILE_OK;

#if !defined(_MSC_VER)
	int flags = O_RDONLY | O_NONBLOCK;
#if defined(O_CLOEXEC)
	flags |= O_CLOEXEC;
#endif
	int fd = open(path, flags);

	if (fd < 0) {
		aws_error_text(errno, reason, reason_size);
		return AWS_FILE_OPEN_FAILED;
	}

	struct stat st;

	if (fstat(fd, &st) != 0) {
		aws_error_text(errno, reason, reason_size);
		close(fd);
		return AWS_FILE_FAILED;
	}
#else
	int fd = _open(path, _O_RDONLY | _O_BINARY);

	if (fd < 0) {
		aws_error_text(errno, reason, reason_size);
		return AWS_FILE_OPEN_FAILED;
	}

	struct _stat st;

	if (_fstat(fd, &st) != 0) {
		aws_error_text(errno, reason, reason_size);
		_close(fd);
		return AWS_FILE_FAILED;
	}
#endif

	if ((st.st_mode & S_IFMT) != S_IFREG) {
		snprintf(reason, reason_size, "not a regular file");
		rc = AWS_FILE_NOT_REGULAR;
	}
	else if (st.st_size > AWS_FILE_MAX) {
		snprintf(reason, reason_size, "larger than 1 MiB");
		rc = AWS_FILE_FAILED;
	}

	while (rc == AWS_FILE_OK) {
#if !defined(_MSC_VER)
		ssize_t n = read(fd, chunk, sizeof(chunk));
#else
		int n = _read(fd, chunk, (unsigned int)sizeof(chunk));
#endif

		if (n < 0) {
			if (errno == EINTR) {
				continue;
			}
			aws_error_text(errno, reason, reason_size);
			rc = AWS_FILE_FAILED;
		}
		else if (n == 0) {
			break;
		}
		else if (buf->size + (size_t)n > AWS_FILE_MAX) {
			snprintf(reason, reason_size, "larger than 1 MiB");
			rc = AWS_FILE_FAILED;
		}
		else if (!as_aws_buf_append(buf, chunk, (size_t)n)) {
			snprintf(reason, reason_size, "out of memory");
			rc = AWS_FILE_FAILED;
		}
	}

	OPENSSL_cleanse(chunk, sizeof(chunk));

#if !defined(_MSC_VER)
	close(fd);
#else
	_close(fd);
#endif
	return rc;
}

static as_status
aws_read_token_file(as_error* err, const char* name, const char* path, as_aws_buf* buf)
{
	char reason[256];

	if (aws_read_file(path, buf, reason, sizeof(reason)) != AWS_FILE_OK) {
		return as_aws_errorf(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED, NULL, 0,
			AWS_PREFIX "failed to read %s: %s", name, reason);
	}
	return AEROSPIKE_OK;
}

//---------------------------------
// Static Functions: sources
//---------------------------------

static as_status
aws_static(as_error* err, const char* action, const char* access_key_id,
	const char* secret_access_key, const char* session_token, as_aws_credentials* out)
{
	if (!as_aws_valid_key(access_key_id)) {
		return as_error_update(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
			AWS_PREFIX "%s returned an invalid access key id", action);
	}

	if (!as_aws_visible(secret_access_key)) {
		return as_error_update(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
			AWS_PREFIX "%s returned an invalid secret access key", action);
	}

	if (session_token && !as_aws_visible(session_token)) {
		return as_error_update(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
			AWS_PREFIX "%s returned an invalid session token", action);
	}

	if (!aws_credentials_copy(out, access_key_id, secret_access_key, session_token, 0)) {
		return aws_fail(err, AWS_PREFIX "failed to allocate credentials");
	}
	return AEROSPIKE_OK;
}

static as_status
aws_temporary(as_error* err, const char* action, const char* access_key_id,
	const char* secret_access_key, const char* session_token, const char* expiration,
	as_aws_credentials* out)
{
	if (!aws_present(access_key_id) || !aws_present(secret_access_key) || !expiration) {
		return as_error_update(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
			AWS_PREFIX "%s response has no credentials", action);
	}

	if (!as_aws_valid_key(access_key_id)) {
		return as_error_update(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
			AWS_PREFIX "%s returned an invalid access key id", action);
	}

	if (!as_aws_visible(secret_access_key)) {
		return as_error_update(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
			AWS_PREFIX "%s returned an invalid secret access key", action);
	}

	if (session_token && !as_aws_visible(session_token)) {
		return as_error_update(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
			AWS_PREFIX "%s returned an invalid session token", action);
	}

	uint64_t expiration_ms;

	if (!as_aws_iso8601(expiration, &expiration_ms)) {
		return as_error_update(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
			AWS_PREFIX "%s returned an invalid expiration", action);
	}

	if (!aws_credentials_copy(out, access_key_id, secret_access_key, session_token,
		expiration_ms)) {
		return aws_fail(err, AWS_PREFIX "failed to allocate credentials");
	}
	return AEROSPIKE_OK;
}

static as_status
aws_json_credentials(as_error* err, const char* action, const char* invalid,
	const as_aws_http_response* response, as_aws_credentials* out)
{
	as_aws_json json;

	if (!as_aws_json_parse(response->body, response->body_size, &json)) {
		return as_error_update(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED, AWS_PREFIX "%s",
			invalid);
	}

	as_status status = aws_temporary(err, action, as_aws_json_get(&json, "AccessKeyId"),
		as_aws_json_get(&json, "SecretAccessKey"), as_aws_json_get(&json, "Token"),
		as_aws_json_get(&json, "Expiration"), out);
	as_aws_json_destroy(&json);
	return status;
}

static as_status
aws_send(as_error* err, const as_aws_state* st, const char* action, const char* method,
	const as_aws_endpoint* endpoint, const as_aws_header* headers, uint32_t headers_size,
	const uint8_t* body, size_t body_size, uint64_t deadline_ns, const char* const* secrets,
	uint32_t secrets_size, as_aws_http_response* response)
{
	memset(response, 0, sizeof(as_aws_http_response));

	if (!endpoint->valid) {
		return as_error_update(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
			AWS_PREFIX "%s failed: invalid URL", action);
	}

	if (as_aws_http_request(err, &st->tls, action, method, &endpoint->url, headers,
		headers_size, body, body_size, deadline_ns, secrets, secrets_size, response) !=
		AS_AWS_HTTP_OK) {
		return err->code;
	}
	return AEROSPIKE_OK;
}

static as_status
aws_environment(as_error* err, const as_aws_state* st, as_aws_credentials* out, bool* found)
{
	const char* access_key_id = st->vars[AS_AWS_VAR_ACCESS_KEY_ID];
	const char* secret_access_key = st->vars[AS_AWS_VAR_SECRET_ACCESS_KEY];

	*found = access_key_id && secret_access_key;

	if (!*found) {
		return AEROSPIKE_OK;
	}
	return aws_static(err, "environment", access_key_id, secret_access_key,
		st->vars[AS_AWS_VAR_SESSION_TOKEN], out);
}

static as_status
aws_profile(as_error* err, const as_aws_state* st, as_aws_credentials* out, bool* found)
{
	*found = false;

	const char* file = st->vars[AS_AWS_VAR_SHARED_CREDENTIALS_FILE];
	char* path;

	if (!file) {
		if (!st->home) {
			return AEROSPIKE_OK;
		}
		path = aws_concat(st->home, AWS_PATH_SEP ".aws" AWS_PATH_SEP "credentials", NULL);
	}
	else if (strncmp(file, "~/", 2) == 0) {
		if (!st->home) {
			return AEROSPIKE_OK;
		}
		path = aws_concat(st->home, AWS_PATH_SEP, file + 2);
	}
	else {
		path = cf_strdup(file);
	}

	if (!path) {
		return aws_fail(err, AWS_PREFIX "failed to allocate credentials");
	}

	as_aws_buf buf;
	char reason[256];
	aws_file_rc rc = aws_read_file(path, &buf, reason, sizeof(reason));

	if (rc != AWS_FILE_OK) {
		as_status status = AEROSPIKE_OK;

		if (rc == AWS_FILE_FAILED) {
			status = as_aws_errorf(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED, NULL, 0,
				AWS_PREFIX "failed to read %s: %s", path, reason);
		}
		as_aws_buf_destroy(&buf);
		cf_free(path);
		return status;
	}
	cf_free(path);

	const char* name = st->vars[AS_AWS_VAR_PROFILE] ? st->vars[AS_AWS_VAR_PROFILE] : "default";
	as_aws_profile profile;
	bool ok = as_aws_ini_profile(buf.data, buf.size, name, &profile);

	as_aws_buf_destroy(&buf);

	if (!ok) {
		return aws_fail(err, AWS_PREFIX "failed to allocate credentials");
	}

	as_status status = AEROSPIKE_OK;

	if (aws_present(profile.access_key_id) && aws_present(profile.secret_access_key)) {
		*found = true;
		status = aws_static(err, "shared credentials file", profile.access_key_id,
			profile.secret_access_key,
			aws_present(profile.session_token) ? profile.session_token : NULL, out);
	}
	as_aws_profile_destroy(&profile);
	return status;
}

static as_status
aws_sts_result(as_error* err, const as_aws_http_response* response, as_aws_credentials* out)
{
	char reason[128];
	as_aws_xml* doc = as_aws_xml_parse(response->body, response->body_size, reason,
		sizeof(reason));

	if (!doc) {
		return aws_fail(err, AWS_PREFIX "invalid " AWS_STS " response");
	}

	const as_aws_xml_node* credentials = as_aws_xml_child(
		as_aws_xml_child(as_aws_xml_root(doc), "AssumeRoleWithWebIdentityResult"), "Credentials");
	as_status status;

	if (!credentials) {
		status = aws_fail(err, AWS_PREFIX AWS_STS " response has no credentials");
	}
	else {
		const char* access_key_id = as_aws_xml_text(credentials, "AccessKeyId");
		const char* secret_access_key = as_aws_xml_text(credentials, "SecretAccessKey");
		const char* session_token = as_aws_xml_text(credentials, "SessionToken");
		const char* expiration = as_aws_xml_text(credentials, "Expiration");

		if (as_aws_xml_failed(doc)) {
			status = aws_fail(err, AWS_PREFIX "failed to allocate " AWS_STS " response text");
		}
		else {
			status = aws_temporary(err, AWS_STS, access_key_id, secret_access_key,
				session_token, expiration, out);
		}
	}
	as_aws_xml_destroy(doc);
	return status;
}

static as_status
aws_web_identity(as_error* err, as_aws_state* st, uint64_t deadline_ns, as_aws_credentials* out)
{
	as_aws_buf file;
	as_status status = aws_read_token_file(err, "AWS_WEB_IDENTITY_TOKEN_FILE",
		st->vars[AS_AWS_VAR_WEB_IDENTITY_TOKEN_FILE], &file);

	if (status != AEROSPIKE_OK) {
		as_aws_buf_destroy(&file);
		return status;
	}

	char* token = aws_text(file.data, file.size);

	if (!token || !aws_present(token) || !as_aws_visible(token)) {
		as_aws_buf_destroy(&file);
		return aws_fail(err, AWS_PREFIX AWS_STS " failed: invalid web identity token");
	}

	char session[64];
	const char* session_name = st->vars[AS_AWS_VAR_ROLE_SESSION_NAME];

	if (!session_name) {
		snprintf(session, sizeof(session), "aerospike-client-%llu",
			(unsigned long long)as_aws_now_ms(st));
		session_name = session;
	}

	char* role_arn = as_aws_encode(st->vars[AS_AWS_VAR_ROLE_ARN]);
	char* session_encoded = as_aws_encode(session_name);
	char* token_encoded = as_aws_encode(token);
	as_aws_buf body;
	as_aws_buf_init(&body);

	if (role_arn && session_encoded && token_encoded) {
		as_aws_buf_printf(&body, "Action=AssumeRoleWithWebIdentity&Version=2011-06-15"
			"&RoleArn=%s&RoleSessionName=%s&WebIdentityToken=%s", role_arn, session_encoded,
			token_encoded);
	}
	cf_free(role_arn);
	cf_free(session_encoded);
	as_aws_free_secret(token_encoded);

	if (!role_arn || !session_encoded || !token_encoded || !as_aws_buf_ok(&body)) {
		as_aws_buf_destroy(&body);
		as_aws_buf_destroy(&file);
		return aws_fail(err, AWS_PREFIX "failed to allocate " AWS_STS " request");
	}

	as_aws_header header = {"content-type", AWS_FORM};
	const char* secrets[] = {token};
	as_aws_http_response response;

	status = aws_send(err, st, AWS_STS, "POST", &st->sts, &header, 1, (const uint8_t*)body.data,
		body.size, deadline_ns, secrets, 1, &response);
	as_aws_buf_destroy(&body);

	if (status == AEROSPIKE_OK) {
		if (response.status != 200) {
			status = as_aws_xml_error(err, AWS_STS, response.status, response.body,
				response.body_size, response.request_id, secrets, 1);
		}
		else {
			status = aws_sts_result(err, &response, out);
		}
		as_aws_http_response_destroy(&response, true);
	}
	as_aws_buf_destroy(&file);
	return status;
}

static as_status
aws_ecs(as_error* err, as_aws_state* st, uint64_t deadline_ns, as_aws_credentials* out)
{
	if (!st->ecs.valid) {
		return aws_fail(err, AWS_PREFIX AWS_ECS " failed: invalid URL");
	}

	as_aws_buf file;
	char* copy = NULL;
	char* token = NULL;

	as_aws_buf_init(&file);

	if (st->vars[AS_AWS_VAR_CONTAINER_TOKEN_FILE]) {
		as_status status = aws_read_token_file(err, "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
			st->vars[AS_AWS_VAR_CONTAINER_TOKEN_FILE], &file);

		if (status != AEROSPIKE_OK) {
			as_aws_buf_destroy(&file);
			return status;
		}
		token = aws_text(file.data, file.size);
	}
	else if (st->vars[AS_AWS_VAR_CONTAINER_TOKEN]) {
		copy = cf_strdup(st->vars[AS_AWS_VAR_CONTAINER_TOKEN]);

		if (!copy) {
			return aws_fail(err, AWS_PREFIX "failed to allocate credentials");
		}
		token = aws_trim(copy);
	}
	else {
		token = aws_text(NULL, 0);
	}

	if (!token || !aws_printable(token)) {
		as_aws_buf_destroy(&file);
		as_aws_free_secret(copy);
		return aws_fail(err, AWS_PREFIX AWS_ECS " failed: invalid authorization token");
	}

	as_aws_header header = {"authorization", token};
	const char* secrets[] = {token};
	as_aws_http_response response;
	as_status status = aws_send(err, st, AWS_ECS, "GET", &st->ecs, &header, token[0] ? 1 : 0,
		NULL, 0, deadline_ns, secrets, 1, &response);

	as_aws_buf_destroy(&file);
	as_aws_free_secret(copy);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	if (response.status != 200) {
		status = as_error_update(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
			AWS_PREFIX AWS_ECS " failed: HTTP %d", response.status);
	}
	else {
		status = aws_json_credentials(err, AWS_ECS, "invalid ECS credentials response",
			&response, out);
	}
	as_aws_http_response_destroy(&response, true);
	return status;
}

// Discover the role through the role list into st->imds_role.
static as_status
aws_imds_role(as_error* err, as_aws_state* st, const as_aws_header* auth,
	const char* const* secrets, uint64_t deadline_ns)
{
	as_aws_http_response response;
	as_status status = aws_send(err, st, AWS_IMDS_ROLE, "GET", &st->imds_roles, auth, 1, NULL, 0,
		deadline_ns, secrets, 1, &response);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	if (response.status != 200) {
		status = as_error_update(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
			AWS_PREFIX AWS_IMDS_ROLE " failed: HTTP %d (no instance profile attached?)",
			response.status);
		as_aws_http_response_destroy(&response, true);
		return status;
	}

	char* role = aws_text((char*)response.body, response.body_size);

	if (role) {
		char* eol = strchr(role, '\n');

		if (eol) {
			*eol = 0;
			role = aws_trim(role);
		}
	}

	if (role && !role[0]) {
		status = aws_fail(err, AWS_PREFIX "IMDS returned no instance profile role");
	}
	else if (!role || !aws_valid_role(role)) {
		status = aws_fail(err, AWS_PREFIX AWS_IMDS_ROLE " returned an invalid role name");
	}
	else if (!(st->imds_role = cf_strdup(role))) {
		status = aws_fail(err, AWS_PREFIX "failed to allocate credentials");
	}
	as_aws_http_response_destroy(&response, true);
	return status;
}

static as_status
aws_imds_credentials(as_error* err, as_aws_state* st, const as_aws_header* auth,
	const char* const* secrets, uint64_t deadline_ns, as_aws_credentials* out)
{
	char* text = aws_concat(st->imds_roles_text, st->imds_role, NULL);

	if (!text) {
		return aws_fail(err, AWS_PREFIX "failed to allocate credentials");
	}

	as_aws_endpoint endpoint;
	memset(&endpoint, 0, sizeof(endpoint));
	aws_endpoint_init(&endpoint, text);
	cf_free(text);

	as_aws_http_response response;
	as_status status = aws_send(err, st, AWS_IMDS_CREDENTIALS, "GET", &endpoint, auth, 1, NULL,
		0, deadline_ns, secrets, 1, &response);

	as_aws_url_destroy(&endpoint.url);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	if (response.status == 404) {
		cf_free(st->imds_role);
		st->imds_role = NULL;
	}

	if (response.status != 200) {
		status = as_error_update(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
			AWS_PREFIX AWS_IMDS_CREDENTIALS " failed: HTTP %d", response.status);
		as_aws_http_response_destroy(&response, true);
		return status;
	}

	as_aws_json json;

	if (!as_aws_json_parse(response.body, response.body_size, &json)) {
		as_aws_http_response_destroy(&response, true);
		return aws_fail(err, AWS_PREFIX "invalid IMDS credentials response");
	}

	const char* code = as_aws_json_get(&json, "Code");

	if (code && strcmp(code, "Success") != 0) {
		status = as_aws_errorf(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED, secrets, 1,
			AWS_PREFIX AWS_IMDS_CREDENTIALS " failed: %s", code);
	}
	else {
		status = aws_temporary(err, AWS_IMDS_CREDENTIALS, as_aws_json_get(&json, "AccessKeyId"),
			as_aws_json_get(&json, "SecretAccessKey"), as_aws_json_get(&json, "Token"),
			as_aws_json_get(&json, "Expiration"), out);
	}
	as_aws_json_destroy(&json);
	as_aws_http_response_destroy(&response, true);
	return status;
}

static as_status
aws_imds(as_error* err, as_aws_state* st, uint64_t deadline_ns, as_aws_credentials* out)
{
	as_aws_header ttl = {"x-aws-ec2-metadata-token-ttl-seconds", "21600"};
	as_aws_http_response response;
	as_status status = aws_send(err, st, AWS_IMDS_TOKEN, "PUT", &st->imds_token, &ttl, 1,
		(const uint8_t*)"", 0, deadline_ns, NULL, 0, &response);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	if (response.status != 200) {
		status = as_error_update(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
			AWS_PREFIX AWS_IMDS_TOKEN " failed: HTTP %d", response.status);
		as_aws_http_response_destroy(&response, true);
		return status;
	}

	char* token = aws_text((char*)response.body, response.body_size);

	if (!token || !token[0] || !as_aws_visible(token)) {
		as_aws_http_response_destroy(&response, true);
		return aws_fail(err, AWS_PREFIX AWS_IMDS_TOKEN " returned an invalid token");
	}

	as_aws_header auth = {"x-aws-ec2-metadata-token", token};
	const char* secrets[] = {token};

	if (!st->imds_role) {
		status = aws_imds_role(err, st, &auth, secrets, deadline_ns);
	}

	if (status == AEROSPIKE_OK) {
		status = aws_imds_credentials(err, st, &auth, secrets, deadline_ns, out);
	}
	as_aws_http_response_destroy(&response, true);
	return status;
}

static as_status
aws_hook(as_error* err, as_aws_state* st, uint64_t deadline_ns, as_aws_credentials* out,
	bool* late)
{
	uint64_t now = cf_getns();
	uint64_t left_ms = deadline_ns > now ? (deadline_ns - now) / 1000000 : 0;
	uint32_t timeout_ms = left_ms == 0 ? 1 : left_ms > UINT32_MAX ? UINT32_MAX :
		(uint32_t)left_ms;

	as_aws_credentials result;
	memset(&result, 0, sizeof(result));

	as_error hook_err;
	as_error_init(&hook_err);

	as_status rv = st->hook.get(&hook_err, timeout_ms, &result, st->hook.udata);

	*late = cf_getns() >= deadline_ns;

	bool complete = result.access_key_id && result.secret_access_key;
	as_status status;

	if (*late) {
		if (rv == AEROSPIKE_OK && complete) {
			*out = result;
			memset(&result, 0, sizeof(result));
		}
		status = as_error_update(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
			AWS_PREFIX "credentials hook timed out after %ums", timeout_ms);
	}
	else if (rv != AEROSPIKE_OK) {
		hook_err.message[sizeof(hook_err.message) - 1] = 0;

		if (hook_err.message[0]) {
			const char* secrets[] = {st->cache.secret_access_key, st->cache.session_token,
				result.secret_access_key, result.session_token};
			status = as_aws_errorf(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED, secrets, 4,
				AWS_PREFIX "credentials hook failed: %s", hook_err.message);
		}
		else {
			status = as_error_update(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
				AWS_PREFIX "credentials hook failed: status %d", (int)rv);
		}
	}
	else if (!complete) {
		status = aws_fail(err, AWS_PREFIX "credentials hook returned no credentials");
	}
	else {
		*out = result;
		memset(&result, 0, sizeof(result));
		status = AEROSPIKE_OK;
	}

	aws_credentials_clear(&result);
	OPENSSL_cleanse(hook_err.message, sizeof(hook_err.message));
	return status;
}

//---------------------------------
// Static Functions: resolution
//---------------------------------

// Config, environment and profile credentials never expire, so only resolution loads them.
static as_status
aws_load(as_error* err, as_aws_state* st, as_aws_source source, uint64_t deadline_ns,
	as_aws_credentials* out, bool* late)
{
	switch (source) {
	case AS_AWS_SOURCE_WEB_IDENTITY:
		return aws_web_identity(err, st, deadline_ns, out);
	case AS_AWS_SOURCE_ECS:
		return aws_ecs(err, st, deadline_ns, out);
	case AS_AWS_SOURCE_IMDS:
		return aws_imds(err, st, deadline_ns, out);
	case AS_AWS_SOURCE_HOOK:
		return aws_hook(err, st, deadline_ns, out, late);
	default:
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
			AWS_PREFIX "no loader for credentials from %s", aws_source_labels[source]);
	}
}

static as_status
aws_resolve(as_error* err, as_aws_state* st, uint64_t deadline_ns, as_aws_credentials* out,
	as_aws_source* source, bool* late)
{
	if (st->hook.get) {
		*source = AS_AWS_SOURCE_HOOK;
		return aws_hook(err, st, deadline_ns, out, late);
	}

	if (st->access_key_id) {
		*source = AS_AWS_SOURCE_CONFIG;

		if (!aws_credentials_copy(out, st->access_key_id, st->secret_access_key,
			st->session_token, 0)) {
			return aws_fail(err, AWS_PREFIX "failed to allocate credentials");
		}
		return AEROSPIKE_OK;
	}

	bool found;
	as_status status = aws_environment(err, st, out, &found);

	if (status != AEROSPIKE_OK || found) {
		*source = AS_AWS_SOURCE_ENVIRONMENT;
		return status;
	}

	status = aws_profile(err, st, out, &found);

	if (status != AEROSPIKE_OK || found) {
		*source = AS_AWS_SOURCE_PROFILE;
		return status;
	}

	if (st->vars[AS_AWS_VAR_WEB_IDENTITY_TOKEN_FILE] && st->vars[AS_AWS_VAR_ROLE_ARN]) {
		*source = AS_AWS_SOURCE_WEB_IDENTITY;
		return aws_web_identity(err, st, deadline_ns, out);
	}

	if (st->vars[AS_AWS_VAR_CONTAINER_RELATIVE_URI] || st->vars[AS_AWS_VAR_CONTAINER_FULL_URI]) {
		*source = AS_AWS_SOURCE_ECS;
		return aws_ecs(err, st, deadline_ns, out);
	}

	const char* disabled = st->vars[AS_AWS_VAR_EC2_METADATA_DISABLED];

	if (disabled && aws_true(disabled)) {
		*source = AS_AWS_SOURCE_NONE;
		return aws_fail(err, AWS_PREFIX AWS_NO_SOURCE "IMDS is disabled");
	}

	*source = AS_AWS_SOURCE_IMDS;
	status = aws_imds(err, st, deadline_ns, out);

	if (status != AEROSPIKE_OK) {
		const char* reason = err->message;

		if (strncmp(reason, AWS_PREFIX, sizeof(AWS_PREFIX) - 1) == 0) {
			reason += sizeof(AWS_PREFIX) - 1;
		}
		status = as_aws_errorf(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED, NULL, 0,
			AWS_PREFIX AWS_NO_SOURCE "%s", reason);
	}
	return status;
}

//---------------------------------
// Functions
//---------------------------------

uint64_t
as_aws_now_ms(const as_aws_state* st)
{
	return st->now_ms ? st->now_ms(st->now_udata) : cf_clock_getabsolute();
}

as_status
as_aws_chain_init(as_error* err, as_aws_state* st, const as_aws_seed_provider_config* config,
	const as_aws_env* env)
{
	static const as_aws_env none;

	if (!env) {
		env = &none;
	}

	st->now_ms = env->now_ms;
	st->now_udata = env->udata;

	if (st->access_key_id || config->credentials_hook.get) {
		return AEROSPIKE_OK;
	}

	const char* (*get)(const char*, void*) = env->getenv ? env->getenv : aws_process_getenv;

	for (uint32_t i = 0; i < AS_AWS_VAR_SIZE; i++) {
		const char* value = get(aws_var_names[i], env->udata);

		if (aws_present(value) && !(st->vars[i] = cf_strdup(value))) {
			return aws_alloc_failed(err);
		}
	}

	if (env->home) {
		if (aws_present(env->home) && !(st->home = cf_strdup(env->home))) {
			return aws_alloc_failed(err);
		}
	}
	else {
		const char* home = get(AWS_HOME_VAR, env->udata);

		if (aws_present(home)) {
			if (!(st->home = cf_strdup(home))) {
				return aws_alloc_failed(err);
			}
		}
#if !defined(_WIN32)
		else if (!env->getenv && !aws_passwd_home(&st->home)) {
			return aws_alloc_failed(err);
		}
#endif
	}

	char* text = env->sts_base ? cf_strdup(env->sts_base) : aws_sts_url(st->region);

	if (!text) {
		return aws_alloc_failed(err);
	}
	aws_endpoint_init(&st->sts, text);
	cf_free(text);

	const char* relative = st->vars[AS_AWS_VAR_CONTAINER_RELATIVE_URI];

	if (relative) {
		// Without the leading '/' the relative URI could extend the host, redirecting the token.
		if (relative[0] == '/') {
			text = aws_concat(env->ecs_base ? env->ecs_base : AWS_ECS_BASE, relative, NULL);

			if (!text) {
				return aws_alloc_failed(err);
			}
			aws_endpoint_init(&st->ecs, text);
			cf_free(text);
		}
	}
	else if (st->vars[AS_AWS_VAR_CONTAINER_FULL_URI]) {
		aws_endpoint_init(&st->ecs, st->vars[AS_AWS_VAR_CONTAINER_FULL_URI]);
	}

	const char* imds = env->imds_base ? env->imds_base : AWS_IMDS_BASE;

	text = aws_concat(imds, "/latest/api/token", NULL);

	if (!text) {
		return aws_alloc_failed(err);
	}
	aws_endpoint_init(&st->imds_token, text);
	cf_free(text);

	if (!(st->imds_roles_text = aws_concat(imds, AWS_IMDS_ROLES_PATH, NULL))) {
		return aws_alloc_failed(err);
	}
	aws_endpoint_init(&st->imds_roles, st->imds_roles_text);
	return AEROSPIKE_OK;
}

void
as_aws_chain_destroy(as_aws_state* st)
{
	for (uint32_t i = 0; i < AS_AWS_VAR_SIZE; i++) {
		as_aws_free_secret(st->vars[i]);
		st->vars[i] = NULL;
	}
	cf_free(st->home);
	st->home = NULL;
	as_aws_url_destroy(&st->sts.url);
	as_aws_url_destroy(&st->ecs.url);
	as_aws_url_destroy(&st->imds_token.url);
	as_aws_url_destroy(&st->imds_roles.url);
	cf_free(st->imds_roles_text);
	st->imds_roles_text = NULL;
	aws_credentials_clear(&st->cache);
	cf_free(st->imds_role);
	st->imds_role = NULL;
}

as_status
as_aws_credentials_get(as_error* err, as_aws_state* st, uint64_t deadline_ns,
	const as_aws_credentials** out)
{
	as_aws_credentials* cache = &st->cache;
	uint64_t now = as_aws_now_ms(st);

	// Written without subtraction: expiration_ms - margin would underflow for an early expiry.
	if (cache->access_key_id &&
		(cache->expiration_ms == 0 || now + AWS_REFRESH_MARGIN_MS < cache->expiration_ms)) {
		*out = cache;
		return AEROSPIKE_OK;
	}

	as_aws_credentials fresh;
	memset(&fresh, 0, sizeof(fresh));

	as_aws_source source = st->source;
	bool late = false;
	as_status status = source == AS_AWS_SOURCE_NONE ?
		aws_resolve(err, st, deadline_ns, &fresh, &source, &late) :
		aws_load(err, st, source, deadline_ns, &fresh, &late);

	if (status == AEROSPIKE_OK || (late && fresh.access_key_id)) {
		aws_credentials_clear(cache);
		*cache = fresh;
		memset(&fresh, 0, sizeof(fresh));

		if (st->source == AS_AWS_SOURCE_NONE) {
			st->source = source;
			as_log_info("AWS EC2 seed provider credentials from %s", aws_source_labels[source]);
		}
	}
	aws_credentials_clear(&fresh);

	if (status == AEROSPIKE_OK) {
		*out = cache;
		return AEROSPIKE_OK;
	}

	if (!late && cache->access_key_id && now < cache->expiration_ms) {
		as_log_warn("AWS EC2 seed provider: credentials refresh from %s failed, using cached "
			"credentials until they expire: %s", aws_source_labels[st->source], err->message);
		as_error_reset(err);
		*out = cache;
		return AEROSPIKE_OK;
	}
	return status;
}

bool
as_aws_credentials_set(as_aws_credentials* credentials, const char* access_key_id,
	const char* secret_access_key, const char* session_token, uint64_t expiration_ms)
{
	if (!credentials || !as_aws_valid_key(access_key_id) || !aws_present(secret_access_key) ||
		!as_aws_visible(secret_access_key) ||
		(session_token && !as_aws_visible(session_token))) {
		return false;
	}

	as_aws_credentials copy;

	if (!aws_credentials_copy(&copy, access_key_id, secret_access_key, session_token,
		expiration_ms)) {
		return false;
	}
	aws_credentials_clear(credentials);
	*credentials = copy;
	return true;
}
