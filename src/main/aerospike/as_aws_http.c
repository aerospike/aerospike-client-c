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

#include <aerospike/as_log_macros.h>
#include <aerospike/as_lookup.h>
#include <aerospike/as_socket.h>
#include <aerospike/as_tls.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_clock.h>

#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>

#include <errno.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <aerospike/as_poll.h>

#if !defined(_MSC_VER)
#include <fcntl.h>
#endif

#include "as_aws_private.h"

//---------------------------------
// Macros
//---------------------------------

#define HTTP_BUFFER_SIZE 8192
#define HTTP_LINE_MAX 8192
#define HTTP_MAX_HEADERS 100
#define HTTP_REASON_SIZE AS_ERROR_MESSAGE_MAX_SIZE

#if !defined(_MSC_VER)
#define HTTP_EINTR EINTR
#if defined(MSG_NOSIGNAL)
#define HTTP_SEND_FLAGS MSG_NOSIGNAL
#else
#define HTTP_SEND_FLAGS 0
#endif
#else
#define HTTP_EINTR WSAEINTR
#endif

//---------------------------------
// Types
//---------------------------------

typedef enum {
	HTTP_IO_OK,
	HTTP_IO_EOF,

	// A TLS connection ended without close_notify.
	HTTP_IO_TRUNCATED,

	HTTP_IO_TIMEOUT,
	HTTP_IO_ERROR
} http_io;

typedef struct http_conn_s {
	as_socket_fd fd;
	bool open;
	SSL* ssl;

	// SSL_shutdown() must not follow a fatal TLS error.
	bool ssl_fatal;

	bool default_trust;
	const char* trust_bundle;
	uint64_t deadline;
	size_t pos;
	size_t end;

	// Every byte read from the connection.
	size_t total;

	char reason[HTTP_REASON_SIZE];
	uint8_t buf[HTTP_BUFFER_SIZE];
} http_conn;

//---------------------------------
// Globals
//---------------------------------

#if !defined(_WIN32)
// System CA bundles, tried in order when the OpenSSL default certificate file is missing.
static const char* const http_ca_bundles[] = {
	"/etc/ssl/certs/ca-certificates.crt",                // Debian, Ubuntu, Arch, Gentoo
	"/etc/pki/tls/certs/ca-bundle.crt",                  // Fedora, RHEL
	"/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem", // RHEL 7 and later
	"/etc/ssl/ca-bundle.pem",                            // openSUSE
	"/etc/ssl/cert.pem",                                 // Alpine, macOS, FreeBSD
	"/usr/local/etc/ssl/cert.pem"                        // FreeBSD ports
};
#endif

//---------------------------------
// Static Functions: text
//---------------------------------

static char
http_lower(char c)
{
	return (c >= 'A' && c <= 'Z') ? (char)(c + ('a' - 'A')) : c;
}

static bool
http_iequals(const char* a, size_t a_len, const char* b)
{
	if (a_len != strlen(b)) {
		return false;
	}

	for (size_t i = 0; i < a_len; i++) {
		if (http_lower(a[i]) != http_lower(b[i])) {
			return false;
		}
	}
	return true;
}

// Trim as Java String.trim(): every character up to and including the space.
static char*
http_trim(char* s)
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

static bool
http_visible(const char* s, size_t len)
{
	for (size_t i = 0; i < len; i++) {
		unsigned char c = (unsigned char)s[i];

		if (c <= ' ' || c >= 0x7f) {
			return false;
		}
	}
	return true;
}

static bool
http_valid_name(const char* name)
{
	if (!name || !name[0]) {
		return false;
	}

	for (const char* p = name; *p; p++) {
		if (!((*p >= 'a' && *p <= 'z') || (*p >= '0' && *p <= '9') || *p == '-')) {
			return false;
		}
	}
	return true;
}

static bool
http_valid_value(const char* value)
{
	if (!value) {
		return false;
	}

	for (const unsigned char* p = (const unsigned char*)value; *p; p++) {
		if (*p < 0x20 || *p == 0x7f) {
			return false;
		}
	}
	return true;
}

static bool
http_is_ip(const char* host)
{
	uint8_t addr[16];
	return inet_pton(AF_INET, host, addr) == 1 || inet_pton(AF_INET6, host, addr) == 1;
}

static void
http_error_text(int e, char* out, size_t size)
{
#if !defined(_MSC_VER)
	snprintf(out, size, "%s", strerror(e));
#else
	snprintf(out, size, "error %d", e);
#endif
}

// Describe and clear the OpenSSL error queue.
static void
http_openssl_error(char* out, size_t size)
{
	unsigned long e = ERR_get_error();

	if (e) {
		ERR_error_string_n(e, out, size);
	}
	else {
		snprintf(out, size, "unknown error");
	}
	ERR_clear_error();
}

static char*
http_dup_range(const char* s, size_t len)
{
	char* out = cf_malloc(len + 1);

	if (out) {
		memcpy(out, s, len);
		out[len] = 0;
	}
	return out;
}

//---------------------------------
// Static Functions: sockets
//---------------------------------

static inline bool
http_would_block(int e)
{
#if !defined(_MSC_VER)
	return e == EAGAIN || e == EWOULDBLOCK || e == EINPROGRESS;
#else
	return e == WSAEWOULDBLOCK || e == WSAEINPROGRESS;
#endif
}

static bool
http_socket(int family, as_socket_fd* out)
{
	as_socket_fd fd = socket(family, SOCK_STREAM, 0);

#if !defined(_MSC_VER)
	if (fd < 0) {
		return false;
	}

	int flags = fcntl(fd, F_GETFL, 0);

	if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
		as_close(fd);
		return false;
	}

#if defined(__APPLE__)
	int f = 1;

	if (setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &f, sizeof(f)) < 0) {
		as_close(fd);
		return false;
	}
#endif
#else
	if (fd == INVALID_SOCKET) {
		return false;
	}

	unsigned long mode = 1;

	if (ioctlsocket(fd, FIONBIO, &mode) != 0) {
		as_close(fd);
		return false;
	}
#endif
	*out = fd;
	return true;
}

// Wait until fd is readable (or writable) or the deadline passes. Returns 1 when ready, 0 at the
// deadline and -1 on error.
static int
http_wait(as_socket_fd fd, bool read, uint64_t deadline)
{
	as_poll poll;
	as_poll_init(&poll, fd);
	int rv;

	while (true) {
		uint64_t now = cf_getns();

		if (now >= deadline) {
			rv = 0;
			break;
		}

		// as_poll_socket() has millisecond resolution and waits forever for a zero timeout, so
		// round to the nearest millisecond and treat less than half a millisecond as a timeout.
		uint64_t ms = (deadline - now + 500000) / 1000000;

		if (ms == 0) {
			rv = 0;
			break;
		}
		rv = as_poll_socket(&poll, fd, ms > INT32_MAX ? INT32_MAX : (uint32_t)ms, read);

		if (rv > 0) {
			rv = 1;
			break;
		}

		if (rv == 0 || (rv == -1 && as_last_error() == HTTP_EINTR)) {
			continue;
		}
		rv = -1;
		break;
	}
	as_poll_destroy(&poll);
	return rv;
}

static void
http_addr_text(const struct sockaddr* addr, char* out, size_t size)
{
	char ip[INET6_ADDRSTRLEN] = "?";
	uint16_t port;

	if (addr->sa_family == AF_INET) {
		const struct sockaddr_in* in = (const struct sockaddr_in*)addr;
		inet_ntop(AF_INET, (void*)&in->sin_addr, ip, sizeof(ip));
		port = ntohs(in->sin_port);
		snprintf(out, size, "%s:%u", ip, port);
	}
	else {
		const struct sockaddr_in6* in = (const struct sockaddr_in6*)addr;
		inet_ntop(AF_INET6, (void*)&in->sin6_addr, ip, sizeof(ip));
		port = ntohs(in->sin6_port);
		snprintf(out, size, "[%s]:%u", ip, port);
	}
}

static http_io
http_connect(http_conn* c, const as_aws_url* url)
{
	as_address_iterator iter;
	as_error lookup_err;

	if (as_lookup_host(&iter, &lookup_err, url->host, url->port) != AEROSPIKE_OK) {
		snprintf(c->reason, sizeof(c->reason), "%s", lookup_err.message);
		return HTTP_IO_ERROR;
	}

	snprintf(c->reason, sizeof(c->reason), "no address for %s", url->host);

	http_io rv = HTTP_IO_ERROR;
	struct sockaddr* addr;

	while (as_lookup_next(&iter, &addr)) {
		char text[INET6_ADDRSTRLEN + 10];
		char error[128];
		http_addr_text(addr, text, sizeof(text));

		if (cf_getns() >= c->deadline) {
			rv = HTTP_IO_TIMEOUT;
			break;
		}

		as_socket_fd fd;

		if (!http_socket(addr->sa_family, &fd)) {
			http_error_text(as_last_error(), error, sizeof(error));
			snprintf(c->reason, sizeof(c->reason), "socket for %s failed: %s", text, error);
			continue;
		}

		socklen_t len = addr->sa_family == AF_INET ? sizeof(struct sockaddr_in) :
			sizeof(struct sockaddr_in6);

		if (connect(fd, addr, len) != 0 && !http_would_block(as_last_error())) {
			http_error_text(as_last_error(), error, sizeof(error));
			snprintf(c->reason, sizeof(c->reason), "connect to %s failed: %s", text, error);
			as_close(fd);
			continue;
		}

		int w = http_wait(fd, false, c->deadline);

		if (w == 0) {
			as_close(fd);
			rv = HTTP_IO_TIMEOUT;
			break;
		}

		int so_error = 0;
		socklen_t so_len = sizeof(so_error);

		if (w < 0 || getsockopt(fd, SOL_SOCKET, SO_ERROR, (char*)&so_error, &so_len) != 0 ||
			so_error != 0) {
			http_error_text(so_error ? so_error : as_last_error(), error, sizeof(error));
			snprintf(c->reason, sizeof(c->reason), "connect to %s failed: %s", text, error);
			as_close(fd);
			continue;
		}

		c->fd = fd;
		c->open = true;
		rv = HTTP_IO_OK;
		break;
	}
	as_lookup_end(&iter);
	return rv;
}

static http_io
http_ssl_wait(http_conn* c, int e)
{
	int w = http_wait(c->fd, e == SSL_ERROR_WANT_READ, c->deadline);

	if (w == 0) {
		return HTTP_IO_TIMEOUT;
	}

	if (w < 0) {
		char error[128];
		http_error_text(as_last_error(), error, sizeof(error));
		snprintf(c->reason, sizeof(c->reason), "wait failed: %s", error);
		return HTTP_IO_ERROR;
	}
	return HTTP_IO_OK;
}

static bool
http_is_file(const char* path)
{
	struct stat st;
	return path && path[0] && stat(path, &st) == 0 && (st.st_mode & S_IFMT) == S_IFREG;
}

static void
http_verify_failed(http_conn* c, long verify)
{
	const char* error = X509_verify_cert_error_string(verify);

	if (!c->default_trust) {
		snprintf(c->reason, sizeof(c->reason), "certificate verification failed: %s", error);
	}
	else if (c->trust_bundle) {
		snprintf(c->reason, sizeof(c->reason), "certificate verification failed: %s (trust comes "
			"from %s and the OpenSSL default verify paths; set ca_file or ca_path)", error,
			c->trust_bundle);
	}
	else {
		snprintf(c->reason, sizeof(c->reason), "certificate verification failed: %s (trust comes "
			"from the OpenSSL default verify paths; set ca_file or ca_path)", error);
	}
}

static http_io
http_handshake(http_conn* c, const as_aws_tls* tls, const char* host)
{
	ERR_clear_error();
	c->ssl = SSL_new(tls->ctx);

	if (!c->ssl) {
		char ossl[256];
		http_openssl_error(ossl, sizeof(ossl));
		snprintf(c->reason, sizeof(c->reason), "TLS setup failed: %s", ossl);
		return HTTP_IO_ERROR;
	}

	if (SSL_set_fd(c->ssl, (int)c->fd) != 1 ||
		(!http_is_ip(host) && SSL_set_tlsext_host_name(c->ssl, host) != 1)) {
		char ossl[256];
		http_openssl_error(ossl, sizeof(ossl));
		snprintf(c->reason, sizeof(c->reason), "TLS setup failed: %s", ossl);
		c->ssl_fatal = true;
		return HTTP_IO_ERROR;
	}

	while (true) {
		ERR_clear_error();
		int rv = SSL_connect(c->ssl);

		if (rv == 1) {
			break;
		}

		int e = SSL_get_error(c->ssl, rv);

		if (e == SSL_ERROR_WANT_READ || e == SSL_ERROR_WANT_WRITE) {
			http_io io = http_ssl_wait(c, e);

			if (io != HTTP_IO_OK) {
				return io;
			}
			continue;
		}

		c->ssl_fatal = true;
		long verify = SSL_get_verify_result(c->ssl);

		if (verify != X509_V_OK) {
			ERR_clear_error();
			http_verify_failed(c, verify);
			return HTTP_IO_ERROR;
		}

		if (e == SSL_ERROR_SYSCALL && ERR_peek_error() == 0) {
			snprintf(c->reason, sizeof(c->reason), "TLS handshake failed: connection closed");
			return HTTP_IO_ERROR;
		}

		char ossl[256];
		http_openssl_error(ossl, sizeof(ossl));
		snprintf(c->reason, sizeof(c->reason), "TLS handshake failed: %s", ossl);
		return HTTP_IO_ERROR;
	}

	long verify = SSL_get_verify_result(c->ssl);

	if (verify != X509_V_OK) {
		http_verify_failed(c, verify);
		return HTTP_IO_ERROR;
	}

#if OPENSSL_VERSION_NUMBER >= 0x30000000L
	X509* cert = SSL_get1_peer_certificate(c->ssl);
#else
	X509* cert = SSL_get_peer_certificate(c->ssl);
#endif

	if (!cert) {
		snprintf(c->reason, sizeof(c->reason), "certificate verification failed: no peer certificate");
		return HTTP_IO_ERROR;
	}

	bool match = as_tls_match_identity_strict(cert, host);
	X509_free(cert);

	if (!match) {
		snprintf(c->reason, sizeof(c->reason),
			"certificate verification failed: certificate does not match host %s", host);
		return HTTP_IO_ERROR;
	}
	return HTTP_IO_OK;
}

static http_io
http_write(http_conn* c, const char* data, size_t size)
{
	size_t pos = 0;

	while (pos < size) {
		if (c->ssl) {
			ERR_clear_error();
			int n = SSL_write(c->ssl, data + pos, (int)(size - pos));

			if (n > 0) {
				pos += (size_t)n;
				continue;
			}

			int e = SSL_get_error(c->ssl, n);

			if (e == SSL_ERROR_WANT_READ || e == SSL_ERROR_WANT_WRITE) {
				http_io io = http_ssl_wait(c, e);

				if (io != HTTP_IO_OK) {
					return io;
				}
				continue;
			}

			c->ssl_fatal = true;
			char ossl[256];
			http_openssl_error(ossl, sizeof(ossl));
			snprintf(c->reason, sizeof(c->reason), "write failed: %s", ossl);
			return HTTP_IO_ERROR;
		}

		int w = http_wait(c->fd, false, c->deadline);

		if (w == 0) {
			return HTTP_IO_TIMEOUT;
		}

		if (w > 0) {
#if !defined(_MSC_VER)
			int n = (int)send(c->fd, data + pos, size - pos, HTTP_SEND_FLAGS);
#else
			int n = send(c->fd, data + pos, (int)(size - pos), 0);
#endif

			if (n > 0) {
				pos += (size_t)n;
				continue;
			}

			int e = as_last_error();

			if (n < 0 && (e == HTTP_EINTR || http_would_block(e))) {
				continue;
			}
		}

		char error[128];
		http_error_text(as_last_error(), error, sizeof(error));
		snprintf(c->reason, sizeof(c->reason), "write failed: %s", error);
		return HTTP_IO_ERROR;
	}
	return HTTP_IO_OK;
}

static http_io
http_count(http_conn* c, size_t n)
{
	c->total += n;

	if (c->total > AS_AWS_HTTP_MAX_RESPONSE) {
		snprintf(c->reason, sizeof(c->reason), "HTTP response exceeds %d bytes",
			AS_AWS_HTTP_MAX_RESPONSE);
		return HTTP_IO_ERROR;
	}
	c->pos = 0;
	c->end = n;
	return HTTP_IO_OK;
}

// Read more bytes into the empty buffer.
static http_io
http_fill(http_conn* c)
{
	while (true) {
		if (c->ssl) {
			ERR_clear_error();
			errno = 0;
			int n = SSL_read(c->ssl, c->buf, sizeof(c->buf));

			if (n > 0) {
				return http_count(c, (size_t)n);
			}

			int last = as_last_error();
			int e = SSL_get_error(c->ssl, n);

			switch (e) {
			case SSL_ERROR_ZERO_RETURN:
				return HTTP_IO_EOF;

			case SSL_ERROR_WANT_READ:
			case SSL_ERROR_WANT_WRITE: {
				http_io io = http_ssl_wait(c, e);

				if (io != HTTP_IO_OK) {
					return io;
				}
				continue;
			}

			case SSL_ERROR_SYSCALL:
				c->ssl_fatal = true;

				if (ERR_peek_error() == 0 && last == 0) {
					return HTTP_IO_TRUNCATED;
				}
				break;

			default:
				c->ssl_fatal = true;
#if defined(SSL_R_UNEXPECTED_EOF_WHILE_READING)
				if (ERR_GET_REASON(ERR_peek_error()) == SSL_R_UNEXPECTED_EOF_WHILE_READING) {
					ERR_clear_error();
					return HTTP_IO_TRUNCATED;
				}
#endif
				break;
			}

			char text[256];

			if (ERR_peek_error() != 0) {
				http_openssl_error(text, sizeof(text));
			}
			else {
				http_error_text(last, text, sizeof(text));
			}
			snprintf(c->reason, sizeof(c->reason), "read failed: %s", text);
			return HTTP_IO_ERROR;
		}

		int w = http_wait(c->fd, true, c->deadline);

		if (w == 0) {
			return HTTP_IO_TIMEOUT;
		}

		if (w > 0) {
#if !defined(_MSC_VER)
			int n = (int)recv(c->fd, c->buf, sizeof(c->buf), 0);
#else
			int n = recv(c->fd, (char*)c->buf, (int)sizeof(c->buf), 0);
#endif

			if (n > 0) {
				return http_count(c, (size_t)n);
			}

			if (n == 0) {
				return HTTP_IO_EOF;
			}

			int e = as_last_error();

			if (e == HTTP_EINTR || http_would_block(e)) {
				continue;
			}
		}

		char error[128];
		http_error_text(as_last_error(), error, sizeof(error));
		snprintf(c->reason, sizeof(c->reason), "read failed: %s", error);
		return HTTP_IO_ERROR;
	}
}

static http_io
http_truncated(http_conn* c)
{
	snprintf(c->reason, sizeof(c->reason), "truncated response");
	return HTTP_IO_ERROR;
}

// Read one line without its CR and LF characters into line (HTTP_LINE_MAX + 1 bytes).
static http_io
http_line(http_conn* c, char* line, size_t* len)
{
	size_t n = 0;

	while (true) {
		if (c->pos == c->end) {
			http_io io = http_fill(c);

			if (io == HTTP_IO_EOF || io == HTTP_IO_TRUNCATED) {
				return http_truncated(c);
			}

			if (io != HTTP_IO_OK) {
				return io;
			}
		}

		uint8_t b = c->buf[c->pos++];

		if (b == '\n') {
			break;
		}

		if (b == '\r') {
			continue;
		}

		if (b == 0) {
			snprintf(c->reason, sizeof(c->reason), "invalid HTTP header line");
			return HTTP_IO_ERROR;
		}

		if (n >= HTTP_LINE_MAX) {
			snprintf(c->reason, sizeof(c->reason), "HTTP header line exceeds %d bytes",
				HTTP_LINE_MAX);
			return HTTP_IO_ERROR;
		}
		line[n++] = (char)b;
	}
	line[n] = 0;
	*len = n;
	return HTTP_IO_OK;
}

// Read size bytes into body, or every byte until the end of the stream when to_eof is set.
static http_io
http_read(http_conn* c, as_aws_buf* body, size_t size, bool to_eof)
{
	while (to_eof || size > 0) {
		if (c->pos == c->end) {
			http_io io = http_fill(c);

			if (io == HTTP_IO_EOF) {
				return to_eof ? HTTP_IO_OK : http_truncated(c);
			}

			if (io == HTTP_IO_TRUNCATED) {
				return http_truncated(c);
			}

			if (io != HTTP_IO_OK) {
				return io;
			}
		}

		size_t n = c->end - c->pos;

		if (!to_eof && n > size) {
			n = size;
		}

		if (!as_aws_buf_append(body, c->buf + c->pos, n)) {
			snprintf(c->reason, sizeof(c->reason), "out of memory");
			return HTTP_IO_ERROR;
		}
		c->pos += n;

		if (!to_eof) {
			size -= n;
		}
	}
	return HTTP_IO_OK;
}

static http_io
http_trailers(http_conn* c, char* line)
{
	for (uint32_t count = 0; ; count++) {
		size_t len;
		http_io io = http_line(c, line, &len);

		if (io != HTTP_IO_OK) {
			return io;
		}

		if (len == 0) {
			return HTTP_IO_OK;
		}

		if (count >= HTTP_MAX_HEADERS) {
			snprintf(c->reason, sizeof(c->reason), "HTTP response has more than %d trailers",
				HTTP_MAX_HEADERS);
			return HTTP_IO_ERROR;
		}
	}
}

static http_io
http_chunked(http_conn* c, as_aws_buf* body, char* line)
{
	while (true) {
		size_t len;
		http_io io = http_line(c, line, &len);

		if (io != HTTP_IO_OK) {
			return io;
		}

		char* semi = strchr(line, ';');

		if (semi) {
			*semi = 0;
		}

		char* hex = http_trim(line);
		size_t digits = strlen(hex);
		size_t size = 0;

		if (digits == 0 || digits > 15) {
			snprintf(c->reason, sizeof(c->reason), "invalid HTTP chunk size");
			return HTTP_IO_ERROR;
		}

		for (size_t i = 0; i < digits; i++) {
			char d = http_lower(hex[i]);
			size_t v;

			if (d >= '0' && d <= '9') {
				v = (size_t)(d - '0');
			}
			else if (d >= 'a' && d <= 'f') {
				v = (size_t)(d - 'a' + 10);
			}
			else {
				snprintf(c->reason, sizeof(c->reason), "invalid HTTP chunk size");
				return HTTP_IO_ERROR;
			}
			size = size * 16 + v;
		}

		if (size == 0) {
			return http_trailers(c, line);
		}

		if (size > AS_AWS_HTTP_MAX_RESPONSE) {
			snprintf(c->reason, sizeof(c->reason), "HTTP response exceeds %d bytes",
				AS_AWS_HTTP_MAX_RESPONSE);
			return HTTP_IO_ERROR;
		}

		io = http_read(c, body, size, false);

		if (io != HTTP_IO_OK) {
			return io;
		}

		// The line break after the chunk data.
		io = http_line(c, line, &len);

		if (io != HTTP_IO_OK) {
			return io;
		}

		if (len != 0) {
			snprintf(c->reason, sizeof(c->reason), "HTTP chunk data exceeds its size");
			return HTTP_IO_ERROR;
		}
	}
}

static bool
http_parse_status(const char* line, int* status)
{
	if (strncmp(line, "HTTP/", 5) != 0) {
		return false;
	}

	const char* s = strchr(line, ' ');

	if (!s) {
		return false;
	}
	s++;

	const char* e = s;
	int value = 0;

	while (*e >= '0' && *e <= '9' && e - s < 9) {
		value = value * 10 + (*e - '0');
		e++;
	}

	if (e == s || (*e != 0 && *e != ' ')) {
		return false;
	}
	*status = value;
	return true;
}

static bool
http_parse_length(const char* text, size_t* out)
{
	size_t len = strlen(text);
	size_t value = 0;

	if (len == 0 || len > 15) {
		return false;
	}

	for (size_t i = 0; i < len; i++) {
		if (text[i] < '0' || text[i] > '9') {
			return false;
		}
		value = value * 10 + (size_t)(text[i] - '0');
	}
	*out = value;
	return true;
}

static http_io
http_response(http_conn* c, char* line, as_aws_http_response* response)
{
	size_t len;
	http_io io = http_line(c, line, &len);

	if (io != HTTP_IO_OK) {
		if (io == HTTP_IO_ERROR && c->total == 0 && strcmp(c->reason, "truncated response") == 0) {
			snprintf(c->reason, sizeof(c->reason), "connection closed without a response");
		}
		return io;
	}

	int status;

	if (!http_parse_status(line, &status)) {
		snprintf(c->reason, sizeof(c->reason), "invalid HTTP status line");
		return HTTP_IO_ERROR;
	}

	bool chunked = false;
	bool has_length = false;
	bool length_conflict = false;
	char length[32] = "";

	for (uint32_t count = 0; ; count++) {
		io = http_line(c, line, &len);

		if (io != HTTP_IO_OK) {
			return io;
		}

		if (len == 0) {
			break;
		}

		if (count >= HTTP_MAX_HEADERS) {
			snprintf(c->reason, sizeof(c->reason), "HTTP response has more than %d headers",
				HTTP_MAX_HEADERS);
			return HTTP_IO_ERROR;
		}

		char* colon = strchr(line, ':');

		if (!colon || colon == line) {
			continue;
		}

		*colon = 0;
		char* name = http_trim(line);
		char* value = http_trim(colon + 1);
		size_t name_len = strlen(name);

		if (http_iequals(name, name_len, "content-length")) {
			if (!has_length) {
				has_length = true;

				if (strlen(value) >= sizeof(length)) {
					length_conflict = true;
				}
				snprintf(length, sizeof(length), "%s", value);
			}
			else if (strcmp(length, value) != 0) {
				length_conflict = true;
			}
		}
		else if (http_iequals(name, name_len, "transfer-encoding")) {
			chunked = http_iequals(value, strlen(value), "chunked");
		}
		else if (http_iequals(name, name_len, "x-amzn-requestid")) {
			snprintf(response->request_id, sizeof(response->request_id), "%s", value);
		}
	}

	as_aws_buf body;
	as_aws_buf_init(&body);

	if (chunked) {
		io = http_chunked(c, &body, line);
	}
	else if (has_length) {
		size_t size;

		if (length_conflict || !http_parse_length(length, &size)) {
			snprintf(c->reason, sizeof(c->reason), length_conflict ?
				"conflicting HTTP content-length headers" : "invalid HTTP content-length");
			io = HTTP_IO_ERROR;
		}
		else if (size > AS_AWS_HTTP_MAX_RESPONSE) {
			snprintf(c->reason, sizeof(c->reason), "HTTP response exceeds %d bytes",
				AS_AWS_HTTP_MAX_RESPONSE);
			io = HTTP_IO_ERROR;
		}
		else {
			io = http_read(c, &body, size, false);
		}
	}
	else if (status >= 200 && status != 204 && status != 304) {
		io = http_read(c, &body, 0, true);
	}

	if (io != HTTP_IO_OK) {
		as_aws_buf_destroy(&body);
		return io;
	}

	response->status = status;
	response->body_size = body.size;

	if (body.size > 0) {
		response->body = (uint8_t*)as_aws_buf_detach(&body);
	}
	else {
		as_aws_buf_destroy(&body);
	}
	return HTTP_IO_OK;
}

static void
http_close(http_conn* c)
{
	if (c->ssl) {
		if (!c->ssl_fatal && SSL_is_init_finished(c->ssl)) {
			ERR_clear_error();
			SSL_shutdown(c->ssl);
		}
		SSL_free(c->ssl);
		c->ssl = NULL;
	}
	ERR_clear_error();

	if (c->open) {
		as_close(c->fd);
		c->open = false;
	}
	OPENSSL_cleanse(c->buf, sizeof(c->buf));
}

static bool
http_valid_host(const char* host, size_t len)
{
	if (len == 0 || len > 253) {
		return false;
	}

	for (size_t i = 0; i < len; i++) {
		char c = host[i];

		if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
			c == '-' || c == '.')) {
			return false;
		}
	}
	return true;
}

//---------------------------------
// Functions
//---------------------------------

bool
as_aws_url_parse(const char* text, as_aws_url* url)
{
	memset(url, 0, sizeof(as_aws_url));

	const char* sep = text ? strstr(text, "://") : NULL;

	if (!sep || strchr(text, '#')) {
		return false;
	}

	size_t scheme_len = (size_t)(sep - text);

	if (http_iequals(text, scheme_len, "https")) {
		url->tls = true;
	}
	else if (!http_iequals(text, scheme_len, "http")) {
		return false;
	}

	const char* auth = sep + 3;
	size_t auth_len = strcspn(auth, "/?");
	const char* auth_end = auth + auth_len;

	if (memchr(auth, '@', auth_len)) {
		return false;
	}

	const char* host;
	size_t host_len;
	const char* port = NULL;
	bool ipv6 = false;

	if (auth_len > 0 && auth[0] == '[') {
		const char* close = memchr(auth, ']', auth_len);

		if (!close) {
			return false;
		}

		host = auth + 1;
		host_len = (size_t)(close - host);

		const char* after = close + 1;

		if (after < auth_end) {
			if (*after != ':') {
				return false;
			}
			port = after + 1;
		}

		char ip[INET6_ADDRSTRLEN];
		uint8_t addr[16];

		if (host_len == 0 || host_len >= sizeof(ip)) {
			return false;
		}

		memcpy(ip, host, host_len);
		ip[host_len] = 0;

		if (inet_pton(AF_INET6, ip, addr) != 1) {
			return false;
		}
		ipv6 = true;
	}
	else {
		const char* colon = memchr(auth, ':', auth_len);
		host = auth;

		if (colon) {
			// A second colon is an unbracketed IPv6 address.
			if (memchr(colon + 1, ':', (size_t)(auth_end - colon - 1))) {
				return false;
			}
			host_len = (size_t)(colon - auth);
			port = colon + 1;
		}
		else {
			host_len = auth_len;
		}

		if (!http_valid_host(host, host_len)) {
			return false;
		}
	}

	if (port) {
		size_t port_len = (size_t)(auth_end - port);
		uint32_t value = 0;

		if (port_len == 0 || port_len > 5) {
			return false;
		}

		for (size_t i = 0; i < port_len; i++) {
			if (port[i] < '0' || port[i] > '9') {
				return false;
			}
			value = value * 10 + (uint32_t)(port[i] - '0');
		}

		if (value == 0 || value > 65535) {
			return false;
		}
		url->port = (uint16_t)value;
	}
	else {
		url->port = url->tls ? 443 : 80;
	}

	const char* path = auth_end;
	size_t path_len = strcspn(path, "?");
	const char* query = path[path_len] == '?' ? path + path_len + 1 : NULL;

	if (!http_visible(path, path_len) || (query && !http_visible(query, strlen(query)))) {
		return false;
	}

	url->host = http_dup_range(host, host_len);
	url->path = path_len ? http_dup_range(path, path_len) : cf_strdup("/");
	url->query = query ? cf_strdup(query) : NULL;

	as_aws_buf header;
	as_aws_buf_init(&header);
	as_aws_buf_printf(&header, ipv6 ? "[%.*s]" : "%.*s", (int)host_len, host);

	if (port) {
		as_aws_buf_printf(&header, ":%u", url->port);
	}
	url->host_header = as_aws_buf_detach(&header);

	if (!url->host || !url->path || (query && !url->query) || !url->host_header) {
		as_aws_url_destroy(url);
		return false;
	}
	return true;
}

void
as_aws_url_destroy(as_aws_url* url)
{
	cf_free(url->host);
	cf_free(url->host_header);
	cf_free(url->path);
	cf_free(url->query);
	memset(url, 0, sizeof(as_aws_url));
}

as_status
as_aws_tls_init(as_error* err, const char* ca_file, const char* ca_path, as_aws_tls* tls)
{
	memset(tls, 0, sizeof(as_aws_tls));
	as_tls_check_init();
	ERR_clear_error();

	SSL_CTX* ctx = SSL_CTX_new(TLS_client_method());
	char ossl[256];

	if (!ctx || SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION) != 1 ||
		(!ca_file && !ca_path && SSL_CTX_set_default_verify_paths(ctx) != 1)) {
		http_openssl_error(ossl, sizeof(ossl));
		SSL_CTX_free(ctx);
		return as_aws_errorf(err, AEROSPIKE_ERR_TLS_ERROR, NULL, 0,
			"AWS EC2 seed provider: failed to create TLS context: %s", ossl);
	}

	if (ca_file && SSL_CTX_load_verify_locations(ctx, ca_file, NULL) != 1) {
		http_openssl_error(ossl, sizeof(ossl));
		SSL_CTX_free(ctx);
		return as_aws_errorf(err, AEROSPIKE_ERR_TLS_ERROR, NULL, 0,
			"AWS EC2 seed provider: failed to load ca_file %s: %s", ca_file, ossl);
	}

	if (ca_path) {
		struct stat st;

		if (stat(ca_path, &st) != 0) {
			http_error_text(errno, ossl, sizeof(ossl));
			SSL_CTX_free(ctx);
			return as_aws_errorf(err, AEROSPIKE_ERR_TLS_ERROR, NULL, 0,
				"AWS EC2 seed provider: failed to load ca_path %s: %s", ca_path, ossl);
		}

		if ((st.st_mode & S_IFMT) != S_IFDIR) {
			SSL_CTX_free(ctx);
			return as_aws_errorf(err, AEROSPIKE_ERR_TLS_ERROR, NULL, 0,
				"AWS EC2 seed provider: failed to load ca_path %s: not a directory", ca_path);
		}

		if (SSL_CTX_load_verify_locations(ctx, NULL, ca_path) != 1) {
			http_openssl_error(ossl, sizeof(ossl));
			SSL_CTX_free(ctx);
			return as_aws_errorf(err, AEROSPIKE_ERR_TLS_ERROR, NULL, 0,
				"AWS EC2 seed provider: failed to load ca_path %s: %s", ca_path, ossl);
		}
	}

	const char* bundle = NULL;

#if !defined(_WIN32)
	if (!ca_file && !ca_path) {
		bundle = as_aws_trust_bundle(getenv(X509_get_default_cert_file_env()),
			getenv(X509_get_default_cert_dir_env()), X509_get_default_cert_file(),
			http_ca_bundles, sizeof(http_ca_bundles) / sizeof(http_ca_bundles[0]));

		if (bundle && SSL_CTX_load_verify_locations(ctx, bundle, NULL) != 1) {
			ERR_clear_error();
			bundle = NULL;
		}

		if (bundle) {
			as_log_debug("AWS EC2 seed provider trusts the system CA bundle %s: the OpenSSL "
				"default certificate file %s does not exist", bundle, X509_get_default_cert_file());
		}
	}
#endif

	SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, NULL);
	as_tls_manage_sigpipe();
	tls->ctx = ctx;
	tls->default_trust = !ca_file && !ca_path;
	tls->trust_bundle = bundle;
	return AEROSPIKE_OK;
}

const char*
as_aws_trust_bundle(const char* env_file, const char* env_dir, const char* default_file,
	const char* const* candidates, uint32_t candidates_size)
{
	if ((env_file && env_file[0]) || (env_dir && env_dir[0]) || http_is_file(default_file)) {
		return NULL;
	}

	for (uint32_t i = 0; i < candidates_size; i++) {
		if (http_is_file(candidates[i])) {
			return candidates[i];
		}
	}
	return NULL;
}

void
as_aws_tls_destroy(as_aws_tls* tls)
{
	if (tls->ctx) {
		SSL_CTX_free(tls->ctx);
	}
	memset(tls, 0, sizeof(as_aws_tls));
}

as_aws_http_rc
as_aws_http_request(as_error* err, const as_aws_tls* tls, const char* action, const char* method,
	const as_aws_url* url, const as_aws_header* headers, uint32_t headers_size,
	const uint8_t* body, size_t body_size, uint64_t deadline_ns, const char* const* secrets,
	uint32_t secrets_size, as_aws_http_response* response)
{
	memset(response, 0, sizeof(as_aws_http_response));

	bool has_host = false;

	for (uint32_t i = 0; i < headers_size; i++) {
		const as_aws_header* h = &headers[i];

		if (!http_valid_name(h->name)) {
			as_aws_errorf(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED, secrets, secrets_size,
				"AWS EC2 seed provider: %s failed: invalid header name", action);
			return AS_AWS_HTTP_ERROR;
		}

		if (!http_valid_value(h->value)) {
			as_aws_errorf(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED, secrets, secrets_size,
				"AWS EC2 seed provider: %s failed: invalid %s header value", action, h->name);
			return AS_AWS_HTTP_ERROR;
		}

		if (strcmp(h->name, "host") == 0) {
			has_host = true;
		}
	}

	// The request carries credentials, so it is cleansed.
	as_aws_buf req;
	as_aws_buf_init(&req);
	as_aws_buf_printf(&req, "%s %s%s%s HTTP/1.1\r\n", method, url->path, url->query ? "?" : "",
		url->query ? url->query : "");

	if (!has_host) {
		as_aws_buf_printf(&req, "host: %s\r\n", url->host_header);
	}

	for (uint32_t i = 0; i < headers_size; i++) {
		as_aws_buf_printf(&req, "%s: %s\r\n", headers[i].name, headers[i].value);
	}

	if (body) {
		as_aws_buf_printf(&req, "content-length: %zu\r\n", body_size);
	}
	as_aws_buf_append_str(&req, "connection: close\r\n\r\n");

	if (body) {
		as_aws_buf_append(&req, body, body_size);
	}

	if (!as_aws_buf_ok(&req)) {
		as_aws_buf_destroy(&req);
		as_error_set_message(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
			"AWS EC2 seed provider: failed to allocate a request");
		return AS_AWS_HTTP_ERROR;
	}

	http_conn* c = cf_malloc(sizeof(http_conn));
	char* line = cf_malloc(HTTP_LINE_MAX + 1);

	if (!c || !line) {
		cf_free(c);
		cf_free(line);
		as_aws_buf_destroy(&req);
		as_error_set_message(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
			"AWS EC2 seed provider: failed to allocate a request");
		return AS_AWS_HTTP_ERROR;
	}

	memset(c, 0, offsetof(http_conn, buf));
	c->deadline = deadline_ns;
	c->default_trust = tls->default_trust;
	c->trust_bundle = tls->trust_bundle;

	http_io io = http_connect(c, url);

	if (io == HTTP_IO_OK && url->tls) {
		io = http_handshake(c, tls, url->host);
	}

	if (io == HTTP_IO_OK) {
		io = http_write(c, req.data, req.size);
	}
	as_aws_buf_destroy(&req);

	if (io == HTTP_IO_OK) {
		io = http_response(c, line, response);
	}
	http_close(c);
	OPENSSL_cleanse(line, HTTP_LINE_MAX + 1);
	cf_free(line);

	as_aws_http_rc rc;

	if (io == HTTP_IO_OK) {
		rc = AS_AWS_HTTP_OK;
	}
	else if (io == HTTP_IO_TIMEOUT) {
		as_aws_errorf(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED, secrets, secrets_size,
			"AWS EC2 seed provider: %s timed out", action);
		rc = AS_AWS_HTTP_TIMEOUT;
	}
	else {
		as_aws_errorf(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED, secrets, secrets_size,
			"AWS EC2 seed provider: %s failed: %s", action, c->reason);
		rc = AS_AWS_HTTP_ERROR;
	}

	if (rc != AS_AWS_HTTP_OK) {
		as_aws_http_response_destroy(response, true);
	}
	cf_free(c);
	return rc;
}

void
as_aws_http_response_destroy(as_aws_http_response* response, bool sensitive)
{
	if (response->body) {
		if (sensitive) {
			OPENSSL_cleanse(response->body, response->body_size);
		}
		cf_free(response->body);
	}
	memset(response, 0, sizeof(as_aws_http_response));
}
