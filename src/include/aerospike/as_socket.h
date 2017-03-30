/*
 * Copyright 2008-2017 Aerospike, Inc.
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

#include <aerospike/as_error.h>
#include <citrusleaf/cf_clock.h>
#include <stddef.h>
#include <stdint.h>

#include <openssl/ssl.h>

#include <aerospike/as_config.h>

#if defined(__linux__) || defined(__APPLE__)
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

// Windows send() and recv() parameter types are different.
#define as_socket_data_t void
#define as_socket_size_t size_t
#define as_close(fd) (close(fd))
#endif

#if defined(__APPLE__)
#define SOL_TCP IPPROTO_TCP
#define MSG_NOSIGNAL 0
#endif

#if defined(CF_WINDOWS)
#include <WinSock2.h>
#include <Ws2tcpip.h>

#define as_socket_data_t char
#define as_socket_size_t int
#define as_close(fd) (closesocket(fd))

#define MSG_DONTWAIT	0
#define MSG_NOSIGNAL	0

#define SHUT_RDWR		SD_BOTH
#endif // CF_WINDOWS

#define AS_IP_ADDRESS_SIZE 64

#ifdef __cplusplus
extern "C" {
#endif

/**
 *	This structure holds TLS context which can be shared (read-only)
 *	by all the connections to a specific cluster.
 */
typedef struct as_tls_context_s {
	SSL_CTX* ssl_ctx;
	void* cert_blacklist;
	uint64_t max_socket_idle;
	bool log_session_info;
} as_tls_context;

struct as_queue_lock_s;
struct as_node_s;

/**
 *	Socket fields for both regular and TLS sockets.
 */
typedef struct as_socket_s {
	int fd;
	int family;
	union {
		struct as_queue_lock_s* queue; // Used when sync socket is active.
		uint64_t last_used;            // Used when socket in pool.
	};
	as_tls_context* ctx;
	const char* tls_name;
	SSL* ssl;
} as_socket;

/**
 * @private
 * Initialize an as_socket structure.
 */
void
as_socket_init(as_socket* sock);
	
/**
 *	@private
 *	Create non-blocking socket.  Family should be AF_INET or AF_INET6.
 *	If socket create fails, return -errno.
 */
int
as_socket_create_fd(int family);

/**
 *	@private
 *	Create non-blocking socket.
 *	Family should be AF_INET or AF_INET6.
 */
int
as_socket_create(as_socket* sock, int family, as_tls_context* ctx, const char* tls_name);

/**
 *	@private
 *	Wrap existing fd in a socket.
 *	Family should be AF_INET or AF_INET6.
 */
bool
as_socket_wrap(as_socket* sock, int family, int fd, as_tls_context* ctx, const char* tls_name);
	
/**
 *	@private
 *	Connect to non-blocking socket.
 */
bool
as_socket_start_connect(as_socket* sock, struct sockaddr* addr);

/**
 *	@private
 *	Create non-blocking socket and connect.
 */
as_status
as_socket_create_and_connect(as_socket* sock, as_error* err, struct sockaddr* addr, as_tls_context* ctx, const char* tls_name);

/**
 * @private
 * Close and release resources associated with a as_socket.
 */
void
as_socket_close(as_socket* sock);

/**
 * @private
 * Create error message for socket error.
 */
as_status
as_socket_error(int fd, struct as_node_s* node, as_error* err, as_status status, const char* msg, int code);

/**
 * @private
 * Append address to error message.
 */
void
as_socket_error_append(as_error* err, struct sockaddr* addr);

/**
 *	@private
 *	Peek for socket connection status using underlying fd.
 *  Needed to support libuv.
 *
 *	@return   0 : socket is connected, but no data available.
 *			> 0 : byte size of data available.
 *			< 0 : socket is invalid.
 */
int
as_socket_validate_fd(int fd);

/**
 *	@private
 *	Peek for socket connection status.
 *
 *	@return   0 : socket is connected, but no data available.
 *			> 0 : byte size of data available.
 *			< 0 : socket is invalid.
 */
int
as_socket_validate(as_socket* sock);

#if defined(__linux__) || defined(__APPLE__)

/**
 *	@private
 *	Calculate future deadline given timeout.
 */
static inline uint64_t
as_socket_deadline(uint32_t timeout_ms)
{
	return (timeout_ms && timeout_ms <= INT32_MAX)? cf_getms() + timeout_ms : 0;
}

/**
 *	@private
 *	Write socket data without timeouts.
 */
as_status
as_socket_write_forever(as_error* err, as_socket* sock, struct as_node_s* node, uint8_t *buf, size_t buf_len);

/**
 *	@private
 *	Write socket data with future deadline in milliseconds.
 *	Do not adjust for zero deadline.
 */
as_status
as_socket_write_limit(as_error* err, as_socket* sock, struct as_node_s* node, uint8_t *buf, size_t buf_len, uint64_t deadline);

/**
 *	@private
 *	Write socket data with future deadline in milliseconds.
 *	If deadline is zero, do not set deadline.
 */
static inline as_status
as_socket_write_deadline(as_error* err, as_socket* sock, struct as_node_s* node, uint8_t *buf, size_t buf_len, uint64_t deadline)
{
	if (deadline) {
		return as_socket_write_limit(err, sock, node, buf, buf_len, deadline);
	}
	else {
		return as_socket_write_forever(err, sock, node, buf, buf_len);
	}
}

/**
 *	@private
 *	Write socket data with timeout in milliseconds.
 *	If timeout is zero or > MAXINT, do not set timeout.
 */
static inline as_status
as_socket_write_timeout(as_error* err, as_socket* sock, struct as_node_s* node, uint8_t *buf, size_t buf_len, uint32_t timeout_ms)
{
	if (timeout_ms && timeout_ms <= INT32_MAX) {
		return as_socket_write_limit(err, sock, node, buf, buf_len, cf_getms() + timeout_ms);
	}
	else {
		return as_socket_write_forever(err, sock, node, buf, buf_len);
	}
}

/**
 *	@private
 *	Read socket data without timeouts.
 */
as_status
as_socket_read_forever(as_error* err, as_socket* sock, struct as_node_s* node, uint8_t *buf, size_t buf_len);

/**
 *	@private
 *	Read socket data with future deadline in milliseconds.
 *	Do not adjust for zero deadline.
 */
as_status
as_socket_read_limit(as_error* err, as_socket* sock, struct as_node_s* node, uint8_t *buf, size_t buf_len, uint64_t deadline);

/**
 *	@private
 *	Read socket data with future deadline in milliseconds.
 *	If deadline is zero, do not set deadline.
 */
static inline as_status
as_socket_read_deadline(as_error* err, as_socket* sock, struct as_node_s* node, uint8_t *buf, size_t buf_len, uint64_t deadline)
{
	if (deadline) {
		return as_socket_read_limit(err, sock, node, buf, buf_len, deadline);
	}
	else {
		return as_socket_read_forever(err, sock, node, buf, buf_len);
	}
}

/**
 *	@private
 *	Read socket data with timeout in milliseconds.
 *	If timeout is zero or > MAXINT, do not set timeout.
 */
static inline as_status
as_socket_read_timeout(as_error* err, as_socket* sock, struct as_node_s* node, uint8_t *buf, size_t buf_len, uint32_t timeout_ms)
{
	if (timeout_ms && timeout_ms <= INT32_MAX) {
		return as_socket_read_limit(err, sock, node, buf, buf_len, cf_getms() + timeout_ms);
	}
	else {
		return as_socket_read_forever(err, sock, node, buf, buf_len);
	}
}

#endif

#ifdef __cplusplus
} // end extern "C"
#endif
