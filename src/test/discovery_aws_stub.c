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
#include "discovery_aws_stub.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef DISCOVERY_FAKE_SERVERS
#include <aerospike/as_atomic.h>
#include <aerospike/as_sleep.h>
#include <citrusleaf/cf_clock.h>

#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509v3.h>

#include <arpa/inet.h>
#include <dirent.h>
#include <netinet/in.h>
#include <poll.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#if defined(MSG_NOSIGNAL)
#define STUB_SEND_FLAGS MSG_NOSIGNAL
#else
#define STUB_SEND_FLAGS 0
#endif
#endif

//---------------------------------
// Functions: documents
//---------------------------------

char*
aws_strf(const char* fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	int len = vsnprintf(NULL, 0, fmt, ap);
	va_end(ap);

	char* out = malloc((size_t)len + 1);

	va_start(ap, fmt);
	vsnprintf(out, (size_t)len + 1, fmt, ap);
	va_end(ap);
	return out;
}

static char*
str_cat(char* s, const char* add)
{
	size_t a = s ? strlen(s) : 0;
	size_t b = strlen(add);
	char* out = realloc(s, a + b + 1);
	memcpy(out + a, add, b + 1);
	return out;
}

static char*
str_field(char* s, const char* name, const char* value)
{
	if (!value) {
		return s;
	}

	char* line = aws_strf("          <%s>%s</%s>\n", name, value, name);
	s = str_cat(s, line);
	free(line);
	return s;
}

char*
aws_xml_instance(const char* id, const char* private_ip, const char* public_ip,
	const char* private_dns, const char* public_dns)
{
	char* s = aws_strf("        <item>\n          <instanceId>%s</instanceId>\n"
		"          <imageId>ami-0abcdef1234567890</imageId>\n"
		"          <instanceState>\n            <code>16</code>\n            <name>running</name>\n"
		"          </instanceState>\n", id);
	s = str_field(s, "privateDnsName", private_dns);
	s = str_field(s, "dnsName", public_dns);
	s = str_cat(s, "          <instanceType>c5.large</instanceType>\n");
	s = str_field(s, "privateIpAddress", private_ip);
	s = str_field(s, "ipAddress", public_ip);
	s = str_cat(s, "          <networkInterfaceSet>\n            <item>\n"
		"              <privateIpAddress>192.0.2.254</privateIpAddress>\n"
		"            </item>\n          </networkInterfaceSet>\n"
		"          <tagSet>\n            <item>\n              <key>aerospike-cluster</key>\n"
		"              <value>prod-a</value>\n            </item>\n          </tagSet>\n"
		"        </item>\n");
	return s;
}

char*
aws_xml_describe(const char* next_token, const char* const* instances, uint32_t instances_size)
{
	char* s = aws_strf("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
		"<DescribeInstancesResponse xmlns=\"http://ec2.amazonaws.com/doc/2016-11-15/\">\n"
		"  <requestId>8f7724cf-496f-496e-8fe3-example</requestId>\n"
		"  <reservationSet>\n");

	if (instances_size > 0) {
		s = str_cat(s, "    <item>\n      <reservationId>r-1234567890abcdef0</reservationId>\n"
			"      <ownerId>123456789012</ownerId>\n      <groupSet/>\n      <instancesSet>\n");

		for (uint32_t i = 0; i < instances_size; i++) {
			s = str_cat(s, instances[i]);
		}
		s = str_cat(s, "      </instancesSet>\n    </item>\n");
	}
	s = str_cat(s, "  </reservationSet>\n");

	if (next_token) {
		char* line = aws_strf("  <nextToken>%s</nextToken>\n", next_token);
		s = str_cat(s, line);
		free(line);
	}
	return str_cat(s, "</DescribeInstancesResponse>\n");
}

char*
aws_xml_error(const char* code, const char* message, const char* request_id)
{
	return aws_strf("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Response><Errors><Error><Code>%s"
		"</Code><Message>%s</Message></Error></Errors><RequestID>%s</RequestID></Response>", code,
		message, request_id);
}

char*
aws_xml_sts(const char* access_key_id, const char* secret_access_key, const char* session_token,
	const char* expiration)
{
	return aws_strf("<AssumeRoleWithWebIdentityResponse "
		"xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\n"
		"  <AssumeRoleWithWebIdentityResult>\n"
		"    <SubjectFromWebIdentityToken>system:serviceaccount:aerospike:client"
		"</SubjectFromWebIdentityToken>\n"
		"    <Audience>sts.amazonaws.com</Audience>\n"
		"    <AssumedRoleUser>\n"
		"      <Arn>arn:aws:sts::123456789012:assumed-role/seed-role/aerospike</Arn>\n"
		"      <AssumedRoleId>AROACLKWSDQRAOEXAMPLE:aerospike</AssumedRoleId>\n"
		"    </AssumedRoleUser>\n"
		"    <Credentials>\n"
		"      <SessionToken>%s</SessionToken>\n"
		"      <SecretAccessKey>%s</SecretAccessKey>\n"
		"      <Expiration>%s</Expiration>\n"
		"      <AccessKeyId>%s</AccessKeyId>\n"
		"    </Credentials>\n"
		"    <Provider>oidc.eks.us-east-1.amazonaws.com</Provider>\n"
		"  </AssumeRoleWithWebIdentityResult>\n"
		"  <ResponseMetadata>\n"
		"    <RequestId>ad4156e9-bce1-11e2-82e6-6b6efEXAMPLE</RequestId>\n"
		"  </ResponseMetadata>\n"
		"</AssumeRoleWithWebIdentityResponse>\n", session_token, secret_access_key, expiration,
		access_key_id);
}

static char*
json_field(char* s, const char* name, const char* value)
{
	if (!value) {
		return s;
	}

	char* field = aws_strf(",\"%s\":\"%s\"", name, value);
	s = str_cat(s, field);
	free(field);
	return s;
}

char*
aws_json_credentials(const char* access_key_id, const char* secret_access_key,
	const char* session_token, const char* expiration)
{
	char* s = aws_strf("{\"Code\":\"Success\",\"LastUpdated\":\"2012-04-26T16:39:16Z\","
		"\"Type\":\"AWS-HMAC\"");
	s = json_field(s, "AccessKeyId", access_key_id);
	s = json_field(s, "SecretAccessKey", secret_access_key);
	s = json_field(s, "Token", session_token);
	s = json_field(s, "Expiration", expiration);
	return str_cat(s, "}");
}

#ifdef DISCOVERY_FAKE_SERVERS

//---------------------------------
// Static Functions: connection
//---------------------------------

static bool
stub_stopping(aws_stub* s)
{
	return as_load_uint8(&s->stop) != 0;
}

// Sleep against a deadline: short sleeps overshoot, so a count of them would add up late.
static void
stub_sleep(aws_stub* s, uint32_t ms)
{
	uint64_t end = cf_getms() + ms;

	while (!stub_stopping(s)) {
		uint64_t now = cf_getms();

		if (now >= end) {
			break;
		}
		as_sleep(end - now < 5 ? (uint32_t)(end - now) : 5);
	}
}

static int
stub_read(int fd, SSL* ssl, char* buf, size_t size)
{
	if (ssl) {
		return SSL_read(ssl, buf, (int)size);
	}
	return (int)recv(fd, buf, size, 0);
}

static bool
stub_write(int fd, SSL* ssl, const char* data, size_t size)
{
	size_t pos = 0;

	while (pos < size) {
		int n = ssl ? SSL_write(ssl, data + pos, (int)(size - pos)) :
			(int)send(fd, data + pos, size - pos, STUB_SEND_FLAGS);

		if (n <= 0) {
			return false;
		}
		pos += (size_t)n;
	}
	return true;
}

static const char*
find_crlf2(const char* buf, size_t len)
{
	for (size_t i = 0; i + 4 <= len; i++) {
		if (memcmp(buf + i, "\r\n\r\n", 4) == 0) {
			return buf + i;
		}
	}
	return NULL;
}

uint32_t
aws_request_header(const aws_request* r, const char* name, char* out, size_t size)
{
	size_t name_len = strlen(name);
	uint32_t count = 0;
	const char* line = r->head;

	if (out && size) {
		out[0] = 0;
	}

	while (*line) {
		const char* end = strstr(line, "\r\n");

		if (!end) {
			break;
		}

		const char* colon = memchr(line, ':', (size_t)(end - line));

		if (colon && (size_t)(colon - line) == name_len && strncasecmp(line, name, name_len) == 0) {
			if (count == 0 && out && size) {
				const char* v = colon + 1;

				while (*v == ' ') {
					v++;
				}
				snprintf(out, size, "%.*s", (int)(end - v), v);
			}
			count++;
		}
		line = end + 2;
	}
	return count;
}

// Read one request into r. Returns false when the client sent no complete request.
static bool
stub_read_request(int fd, SSL* ssl, aws_request* r)
{
	static const size_t cap = AWS_STUB_HEAD_MAX + AWS_STUB_BODY_MAX;
	char* buf = malloc(cap);
	size_t len = 0;
	const char* end;

	while (!(end = find_crlf2(buf, len))) {
		if (len == cap) {
			free(buf);
			return false;
		}

		int n = stub_read(fd, ssl, buf + len, cap - len);

		if (n <= 0) {
			free(buf);
			return false;
		}
		len += (size_t)n;
	}

	size_t head_len = (size_t)(end - buf) + 4;
	const char* line_end = strstr(buf, "\r\n");
	char line[600];
	snprintf(line, sizeof(line), "%.*s", (int)(line_end - buf), buf);
	sscanf(line, "%15s %511s", r->method, r->target);

	size_t lines = head_len - (size_t)(line_end + 2 - buf) - 2;

	if (lines >= sizeof(r->head)) {
		lines = sizeof(r->head) - 1;
	}
	memcpy(r->head, line_end + 2, lines);
	r->head[lines] = 0;

	char value[32];
	size_t body_size = 0;

	if (aws_request_header(r, "content-length", value, sizeof(value)) > 0) {
		body_size = (size_t)strtoul(value, NULL, 10);
	}

	if (body_size > sizeof(r->body) - 1) {
		free(buf);
		return false;
	}

	while (len - head_len < body_size) {
		int n = stub_read(fd, ssl, buf + len, cap - len);

		if (n <= 0) {
			free(buf);
			return false;
		}
		len += (size_t)n;
	}
	memcpy(r->body, buf + head_len, body_size);
	r->body[body_size] = 0;
	r->body_size = body_size;
	free(buf);
	return true;
}

static void
stub_hold(aws_stub* s, int fd)
{
	pthread_mutex_lock(&s->lock);

	if (s->held_size < AWS_STUB_MAX_HELD) {
		s->held[s->held_size++] = fd;
		fd = -1;
	}
	pthread_mutex_unlock(&s->lock);

	if (fd >= 0) {
		close(fd);
	}
}

static void
stub_serve(aws_stub* s, int fd)
{
	pthread_mutex_lock(&s->lock);
	s->accepts++;

	aws_reply reply = {.mode = AWS_REPLY_CLOSE};

	if (s->replies_size > 0) {
		uint32_t i = s->next_reply < s->replies_size ? s->next_reply++ : s->replies_size - 1;
		reply = s->replies[i];
		reply.data = malloc(reply.size + 1);
		memcpy(reply.data, s->replies[i].data, reply.size);
	}

	SSL_CTX* tls = s->tls;
	pthread_mutex_unlock(&s->lock);

	if (reply.mode == AWS_REPLY_SILENT) {
		free(reply.data);
		stub_hold(s, fd);
		return;
	}

	struct timeval tv = {3, 0};
	setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
	setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

	SSL* ssl = NULL;
	aws_request* r = calloc(1, sizeof(aws_request));
	bool ok = true;

	if (tls) {
		ssl = SSL_new(tls);
		SSL_set_fd(ssl, fd);
		ok = SSL_accept(ssl) == 1;

		if (ok) {
			const char* sni = SSL_get_servername(ssl, TLSEXT_NAMETYPE_host_name);
			snprintf(r->sni, sizeof(r->sni), "%s", sni ? sni : "");
		}
	}

	if (ok && stub_read_request(fd, ssl, r)) {
		pthread_mutex_lock(&s->lock);

		if (s->requests_size < AWS_STUB_MAX_REQUESTS) {
			s->requests[s->requests_size++] = *r;
		}
		pthread_mutex_unlock(&s->lock);

		if (reply.mode != AWS_REPLY_CLOSE) {
			stub_sleep(s, reply.delay_ms);

			if (reply.mode == AWS_REPLY_TRICKLE) {
				for (size_t i = 0; i < reply.size && !stub_stopping(s); i++) {
					if (!stub_write(fd, ssl, reply.data + i, 1)) {
						break;
					}
					stub_sleep(s, reply.trickle_ms);
				}
			}
			else {
				ok = stub_write(fd, ssl, reply.data, reply.size);
			}

			if (ssl && ok && !reply.no_close_notify) {
				SSL_shutdown(ssl);
			}
		}
	}

	if (ssl) {
		SSL_free(ssl);
	}
	ERR_clear_error();
	close(fd);
	free(r);
	free(reply.data);
}

static void*
stub_run(void* arg)
{
	aws_stub* s = arg;

	while (!stub_stopping(s)) {
		struct pollfd pfd = {s->listen_fd, POLLIN, 0};

		if (poll(&pfd, 1, 20) <= 0 || !(pfd.revents & POLLIN)) {
			continue;
		}

		int fd = accept(s->listen_fd, NULL, NULL);

		if (fd >= 0) {
			stub_serve(s, fd);
		}
	}
	return NULL;
}

//---------------------------------
// Functions: stub
//---------------------------------

bool
aws_stub_start(aws_stub* s, bool ipv6)
{
	memset(s, 0, sizeof(aws_stub));
	pthread_mutex_init(&s->lock, NULL);
	s->listen_fd = -1;
	s->requests = calloc(AWS_STUB_MAX_REQUESTS, sizeof(aws_request));

	struct sockaddr_storage ss;
	memset(&ss, 0, sizeof(ss));
	socklen_t len;

	if (ipv6) {
		struct sockaddr_in6* a = (struct sockaddr_in6*)&ss;
		a->sin6_family = AF_INET6;
		a->sin6_addr = in6addr_loopback;
		len = sizeof(*a);
	}
	else {
		struct sockaddr_in* a = (struct sockaddr_in*)&ss;
		a->sin_family = AF_INET;
		a->sin_addr.s_addr = inet_addr("127.0.0.1");
		len = sizeof(*a);
	}

	int fd = socket(ss.ss_family, SOCK_STREAM, 0);
	int one = 1;

	if (fd < 0) {
		return false;
	}
	setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

	if (bind(fd, (struct sockaddr*)&ss, len) != 0 || listen(fd, 16) != 0 ||
		getsockname(fd, (struct sockaddr*)&ss, &len) != 0) {
		close(fd);
		return false;
	}

	s->listen_fd = fd;
	s->port = ntohs(ipv6 ? ((struct sockaddr_in6*)&ss)->sin6_port :
		((struct sockaddr_in*)&ss)->sin_port);
	s->running = pthread_create(&s->thread, NULL, stub_run, s) == 0;
	return s->running;
}

static void
stub_clear(aws_stub* s)
{
	for (uint32_t i = 0; i < s->replies_size; i++) {
		free(s->replies[i].data);
	}
	s->replies_size = 0;
	s->next_reply = 0;
	s->requests_size = 0;
	s->accepts = 0;

	for (uint32_t i = 0; i < s->held_size; i++) {
		close(s->held[i]);
	}
	s->held_size = 0;
}

void
aws_stub_stop(aws_stub* s)
{
	if (s->running) {
		as_store_uint8(&s->stop, 1);
		pthread_join(s->thread, NULL);
		s->running = false;
	}

	stub_clear(s);

	if (s->listen_fd >= 0) {
		close(s->listen_fd);
		s->listen_fd = -1;
	}
	free(s->requests);
	s->requests = NULL;
	pthread_mutex_destroy(&s->lock);
}

void
aws_stub_reset(aws_stub* s)
{
	pthread_mutex_lock(&s->lock);
	stub_clear(s);
	pthread_mutex_unlock(&s->lock);
}

void
aws_stub_set_tls(aws_stub* s, SSL_CTX* tls)
{
	pthread_mutex_lock(&s->lock);
	s->tls = tls;
	pthread_mutex_unlock(&s->lock);
}

void
aws_stub_reply(aws_stub* s, const aws_reply* reply)
{
	pthread_mutex_lock(&s->lock);

	if (s->replies_size < AWS_STUB_MAX_REPLIES) {
		aws_reply* r = &s->replies[s->replies_size++];
		*r = *reply;
		r->data = malloc(reply->size + 1);

		if (reply->size) {
			memcpy(r->data, reply->data, reply->size);
		}
		r->data[reply->size] = 0;
	}
	pthread_mutex_unlock(&s->lock);
}

static char*
stub_http(int status, const char* headers, const char* body)
{
	return aws_strf("HTTP/1.1 %d %s\r\ncontent-type: text/xml;charset=UTF-8\r\n%s"
		"content-length: %zu\r\n\r\n%s", status, status == 200 ? "OK" : "Error",
		headers ? headers : "", strlen(body), body);
}

void
aws_stub_http(aws_stub* s, int status, const char* headers, const char* body)
{
	char* data = stub_http(status, headers, body);
	aws_reply reply = {.mode = AWS_REPLY_SEND, .data = data, .size = strlen(data)};
	aws_stub_reply(s, &reply);
	free(data);
}

void
aws_stub_set_http(aws_stub* s, int status, const char* headers, const char* body)
{
	char* data = stub_http(status, headers, body);
	pthread_mutex_lock(&s->lock);

	for (uint32_t i = 0; i < s->replies_size; i++) {
		free(s->replies[i].data);
	}
	s->replies[0] = (aws_reply){.mode = AWS_REPLY_SEND, .data = data, .size = strlen(data)};
	s->replies_size = 1;
	s->next_reply = 0;
	pthread_mutex_unlock(&s->lock);
}

void
aws_stub_describe(aws_stub* s, const char* next_token, const char* const* instances,
	uint32_t instances_size)
{
	char* body = aws_xml_describe(next_token, instances, instances_size);
	aws_stub_http(s, 200, NULL, body);
	free(body);
}

void
aws_stub_imds(aws_stub* s, const char* role, const char* credentials)
{
	aws_stub_http(s, 200, NULL, AWS_STUB_IMDS_TOKEN);

	if (role) {
		aws_stub_http(s, 200, NULL, role);
	}
	aws_stub_http(s, 200, NULL, credentials);
}

uint32_t
aws_stub_requests(aws_stub* s)
{
	pthread_mutex_lock(&s->lock);
	uint32_t n = s->requests_size;
	pthread_mutex_unlock(&s->lock);
	return n;
}

uint32_t
aws_stub_accepts(aws_stub* s)
{
	pthread_mutex_lock(&s->lock);
	uint32_t n = s->accepts;
	pthread_mutex_unlock(&s->lock);
	return n;
}

bool
aws_stub_request(aws_stub* s, uint32_t i, aws_request* out)
{
	pthread_mutex_lock(&s->lock);
	bool found = i < s->requests_size;

	if (found) {
		*out = s->requests[i];
	}
	pthread_mutex_unlock(&s->lock);
	return found;
}

//---------------------------------
// Functions: certificates
//---------------------------------

static EVP_PKEY*
cert_key(void)
{
	EVP_PKEY* key = NULL;
	EVP_PKEY_CTX* ctx = EVP_PKEY_CTX_new_id(EVP_PKEY_EC, NULL);

	if (ctx && EVP_PKEY_keygen_init(ctx) == 1 &&
		EVP_PKEY_CTX_set_ec_paramgen_curve_nid(ctx, NID_X9_62_prime256v1) == 1) {
		EVP_PKEY_keygen(ctx, &key);
	}
	EVP_PKEY_CTX_free(ctx);
	return key;
}

static bool
cert_ext(X509* cert, X509* issuer, int nid, const char* value)
{
	X509V3_CTX ctx;
	X509V3_set_ctx_nodb(&ctx);
	X509V3_set_ctx(&ctx, issuer, cert, NULL, NULL, 0);

	X509_EXTENSION* ext = X509V3_EXT_conf_nid(NULL, &ctx, nid, (char*)value);

	if (!ext) {
		return false;
	}

	bool ok = X509_add_ext(cert, ext, -1) == 1;
	X509_EXTENSION_free(ext);
	return ok;
}

// A CA certificate when issuer is NULL (self-signed), otherwise a server certificate.
static X509*
cert_make(EVP_PKEY* key, const char* cn, X509* issuer, EVP_PKEY* issuer_key, const char* san,
	long serial)
{
	X509* cert = X509_new();

	if (!cert) {
		return NULL;
	}

	X509_set_version(cert, 2);
	ASN1_INTEGER_set(X509_get_serialNumber(cert), serial);
	X509_gmtime_adj(X509_getm_notBefore(cert), -3600);
	X509_gmtime_adj(X509_getm_notAfter(cert), 86400);
	X509_set_pubkey(cert, key);

	X509_NAME* name = X509_get_subject_name(cert);
	X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC, (const unsigned char*)cn, -1, -1, 0);
	X509_set_issuer_name(cert, issuer ? X509_get_subject_name(issuer) : name);

	X509* signer = issuer ? issuer : cert;
	bool ok = cert_ext(cert, signer, NID_subject_key_identifier, "hash");

	if (issuer) {
		ok = ok && cert_ext(cert, signer, NID_basic_constraints, "CA:FALSE") &&
			cert_ext(cert, signer, NID_key_usage, "critical,digitalSignature") &&
			cert_ext(cert, signer, NID_ext_key_usage, "serverAuth") &&
			cert_ext(cert, signer, NID_subject_alt_name, san);
	}
	else {
		ok = ok && cert_ext(cert, signer, NID_basic_constraints, "critical,CA:TRUE") &&
			cert_ext(cert, signer, NID_key_usage, "critical,keyCertSign,cRLSign");
	}

	if (!ok || X509_sign(cert, issuer_key ? issuer_key : key, EVP_sha256()) <= 0) {
		X509_free(cert);
		return NULL;
	}
	return cert;
}

static SSL_CTX*
cert_server(X509* issuer, EVP_PKEY* issuer_key, const char* san, long serial)
{
	EVP_PKEY* key = cert_key();
	X509* cert = key ? cert_make(key, "aws-stub", issuer, issuer_key, san, serial) : NULL;
	SSL_CTX* ctx = cert ? SSL_CTX_new(TLS_server_method()) : NULL;

	if (ctx && (SSL_CTX_use_certificate(ctx, cert) != 1 || SSL_CTX_use_PrivateKey(ctx, key) != 1 ||
		SSL_CTX_check_private_key(ctx) != 1)) {
		SSL_CTX_free(ctx);
		ctx = NULL;
	}
	X509_free(cert);
	EVP_PKEY_free(key);
	return ctx;
}

static bool
cert_write(const char* path, X509* cert)
{
	FILE* f = fopen(path, "w");

	if (!f) {
		return false;
	}

	bool ok = PEM_write_X509(f, cert) == 1;
	return fclose(f) == 0 && ok;
}

bool
aws_certs_create(aws_certs* c)
{
	memset(c, 0, sizeof(aws_certs));
	snprintf(c->dir, sizeof(c->dir), "/tmp/as-aws-certs-XXXXXX");

	if (!mkdtemp(c->dir)) {
		c->dir[0] = 0;
		return false;
	}

	EVP_PKEY* ca_key = cert_key();
	EVP_PKEY* other_key = cert_key();
	X509* ca = ca_key ? cert_make(ca_key, "aws-stub CA", NULL, NULL, NULL, 1) : NULL;
	X509* other = other_key ? cert_make(other_key, "aws-stub other CA", NULL, NULL, NULL, 2) : NULL;
	bool ok = ca && other;

	if (ok) {
		snprintf(c->ca_file, sizeof(c->ca_file), "%s/ca.pem", c->dir);
		snprintf(c->ca_path, sizeof(c->ca_path), "%s/hashed", c->dir);

		char hashed[400];
		snprintf(hashed, sizeof(hashed), "%s/%08lx.0", c->ca_path, X509_subject_name_hash(ca));
		ok = cert_write(c->ca_file, ca) && mkdir(c->ca_path, 0700) == 0 && cert_write(hashed, ca);
	}

	if (ok) {
		const char* san = "IP:127.0.0.1,IP:::1,DNS:localhost";
		c->good = cert_server(ca, ca_key, san, 10);
		c->wrong_dns = cert_server(ca, ca_key, "DNS:wrong.example.com", 11);
		c->no_ip = cert_server(ca, ca_key, "DNS:localhost", 12);
		c->untrusted = cert_server(other, other_key, san, 13);
		ok = c->good && c->wrong_dns && c->no_ip && c->untrusted;
	}

	X509_free(ca);
	X509_free(other);
	EVP_PKEY_free(ca_key);
	EVP_PKEY_free(other_key);
	ERR_clear_error();

	if (!ok) {
		aws_certs_destroy(c);
	}
	return ok;
}

void
aws_certs_destroy(aws_certs* c)
{
	SSL_CTX_free(c->good);
	SSL_CTX_free(c->wrong_dns);
	SSL_CTX_free(c->no_ip);
	SSL_CTX_free(c->untrusted);
	c->good = c->wrong_dns = c->no_ip = c->untrusted = NULL;

	if (c->dir[0]) {
		char path[640];

		if (c->ca_path[0]) {
			DIR* dir = opendir(c->ca_path);

			if (dir) {
				struct dirent* entry;

				while ((entry = readdir(dir)) != NULL) {
					if (entry->d_name[0] != '.') {
						snprintf(path, sizeof(path), "%s/%s", c->ca_path, entry->d_name);
						unlink(path);
					}
				}
				closedir(dir);
			}
			rmdir(c->ca_path);
		}

		if (c->ca_file[0]) {
			unlink(c->ca_file);
		}
		rmdir(c->dir);
		c->dir[0] = 0;
	}
}

#endif // DISCOVERY_FAKE_SERVERS
