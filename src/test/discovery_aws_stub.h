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

// Loopback HTTP and HTTPS stub of the EC2 Query API for the aws-tags tests, with generated
// certificates and builders of DescribeInstances and error documents.

#include "discovery_fake_node.h"

#include <stdint.h>

// Heap string the caller frees with free().
char*
aws_strf(const char* fmt, ...);

// One instancesSet item; NULL fields are omitted. The network interface carries 192.0.2.254,
// which must never be returned.
char*
aws_xml_instance(const char* id, const char* private_ip, const char* public_ip,
	const char* private_dns, const char* public_dns);

char*
aws_xml_describe(const char* next_token, const char* const* instances, uint32_t instances_size);

char*
aws_xml_error(const char* code, const char* message, const char* request_id);

// STS AssumeRoleWithWebIdentity response, as Java's StubAwsServer.assumeRoleWithWebIdentity().
char*
aws_xml_sts(const char* access_key_id, const char* secret_access_key, const char* session_token,
	const char* expiration);

// ECS and IMDS credentials document, as Java's StubAwsServer.credentialsJson(); NULL fields are
// omitted.
char*
aws_json_credentials(const char* access_key_id, const char* secret_access_key,
	const char* session_token, const char* expiration);

#ifdef DISCOVERY_FAKE_SERVERS

#include <openssl/ssl.h>
#include <openssl/x509.h>

#define AWS_STUB_MAX_REPLIES 16
#define AWS_STUB_MAX_REQUESTS 32
#define AWS_STUB_MAX_HELD 16
#define AWS_STUB_HEAD_MAX 16384
#define AWS_STUB_BODY_MAX 16384

typedef enum {
	// Read the request, then send the reply.
	AWS_REPLY_SEND,

	// Accept the connection and do nothing else: no TLS handshake, no read, no reply.
	AWS_REPLY_SILENT,

	// Read the request, then close without a reply (and without TLS close_notify).
	AWS_REPLY_CLOSE,

	// Read the request, then send the reply one byte every trickle_ms.
	AWS_REPLY_TRICKLE
} aws_reply_mode;

typedef struct aws_reply_s {
	aws_reply_mode mode;

	// Complete HTTP response, status line included.
	char* data;
	size_t size;

	// Wait before sending.
	uint32_t delay_ms;

	uint32_t trickle_ms;

	// TLS: close without sending close_notify.
	bool no_close_notify;
} aws_reply;

typedef struct aws_request_s {
	char method[16];

	// Request target: path and query.
	char target[512];

	// Header lines after the request line, each terminated by "\r\n".
	char head[AWS_STUB_HEAD_MAX];

	char body[AWS_STUB_BODY_MAX];
	size_t body_size;

	// TLS server name sent by the client, empty when none.
	char sni[256];
} aws_request;

typedef struct aws_stub_s {
	int listen_fd;
	uint16_t port;
	pthread_t thread;
	pthread_mutex_t lock;
	uint8_t stop;
	bool running;

	// Serve HTTPS with this context; NULL serves plain HTTP. Not owned.
	SSL_CTX* tls;

	// Replies in request order; the last one repeats. No reply closes the connection.
	aws_reply replies[AWS_STUB_MAX_REPLIES];
	uint32_t replies_size;
	uint32_t next_reply;

	aws_request* requests;
	uint32_t requests_size;
	uint32_t accepts;

	// Connections kept open by AWS_REPLY_SILENT until reset or stop.
	int held[AWS_STUB_MAX_HELD];
	uint32_t held_size;
} aws_stub;

// Listen on 127.0.0.1 (or ::1 with ipv6) on an ephemeral port.
bool
aws_stub_start(aws_stub* s, bool ipv6);

void
aws_stub_stop(aws_stub* s);

// Forget replies and requests and close held connections.
void
aws_stub_reset(aws_stub* s);

void
aws_stub_set_tls(aws_stub* s, SSL_CTX* tls);

// Queue a reply; data is copied.
void
aws_stub_reply(aws_stub* s, const aws_reply* reply);

// Queue a Content-Length framed reply with optional extra header lines (each ending in "\r\n").
void
aws_stub_http(aws_stub* s, int status, const char* headers, const char* body);

// Atomically replace every queued reply with one Content-Length framed reply.
void
aws_stub_set_http(aws_stub* s, int status, const char* headers, const char* body);

// Queue a 200 DescribeInstances reply.
void
aws_stub_describe(aws_stub* s, const char* next_token, const char* const* instances,
	uint32_t instances_size);

#define AWS_STUB_IMDS_TOKEN "imds-session-token"
#define AWS_STUB_IMDS_ROLE "aerospike-seed-role"

// Queue the replies of one IMDS load: the token, the role list when role is not NULL, and the
// credentials document.
void
aws_stub_imds(aws_stub* s, const char* role, const char* credentials);

uint32_t
aws_stub_requests(aws_stub* s);

uint32_t
aws_stub_accepts(aws_stub* s);

// Copy request i. Returns false when there is none.
bool
aws_stub_request(aws_stub* s, uint32_t i, aws_request* out);

// Copy the value of the first header named name (case-insensitive) and return the number of
// headers with that name.
uint32_t
aws_request_header(const aws_request* r, const char* name, char* out, size_t size);

// Generated EC P-256 certificates, valid for a day.
typedef struct aws_certs_s {
	char dir[256];

	// The trusted CA, as a PEM file and as a hashed directory.
	char ca_file[320];
	char ca_path[320];

	// Issued by the trusted CA: SAN IP:127.0.0.1, IP:::1 and DNS:localhost.
	SSL_CTX* good;

	// Issued by the trusted CA: SAN DNS:wrong.example.com.
	SSL_CTX* wrong_dns;

	// Issued by the trusted CA: SAN DNS:localhost.
	SSL_CTX* no_ip;

	// Issued by another CA, with the SAN of good.
	SSL_CTX* untrusted;
} aws_certs;

bool
aws_certs_create(aws_certs* c);

// Free the contexts and remove the files.
void
aws_certs_destroy(aws_certs* c);

#endif // DISCOVERY_FAKE_SERVERS
