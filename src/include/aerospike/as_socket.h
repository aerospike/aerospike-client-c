/*
 * Copyright 2008-2016 Aerospike, Inc.
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

#ifdef __cplusplus
extern "C" {
#endif

/**
 *	@private
 *	Create non-blocking socket.
 */
int
as_socket_create_nb();

/**
 *	@private
 *	Connect to non-blocking socket.
 */
as_status
as_socket_start_connect_nb(as_error* err, int fd, struct sockaddr_in *sa);

/**
 *	@private
 *	Create non-blocking socket and connect.
 */
as_status
as_socket_create_and_connect_nb(as_error* err, struct sockaddr_in *sa, int* fd);

/**
 *	@private
 *	Peek for socket connection status.
 *
 *	@return   0 : socket is connected, but no data available.
 *			> 0 : byte size of data available.
 *			< 0 : socket is invalid.
 */
int
as_socket_validate(int fd);

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
as_socket_write_forever(as_error* err, int fd, uint8_t *buf, size_t buf_len);

/**
 *	@private
 *	Write socket data with future deadline in milliseconds.
 *	Do not adjust for zero deadline.
 */
as_status
as_socket_write_limit(as_error* err, int fd, uint8_t *buf, size_t buf_len, uint64_t deadline);

/**
 *	@private
 *	Write socket data with future deadline in milliseconds.
 *	If deadline is zero, do not set deadline.
 */
static inline as_status
as_socket_write_deadline(as_error* err, int fd, uint8_t *buf, size_t buf_len, uint64_t deadline)
{
	if (deadline) {
		return as_socket_write_limit(err, fd, buf, buf_len, deadline);
	}
	else {
		return as_socket_write_forever(err, fd, buf, buf_len);
	}
}

/**
 *	@private
 *	Write socket data with timeout in milliseconds.
 *	If timeout is zero or > MAXINT, do not set timeout.
 */
static inline as_status
as_socket_write_timeout(as_error* err, int fd, uint8_t *buf, size_t buf_len, uint32_t timeout_ms)
{
	if (timeout_ms && timeout_ms <= INT32_MAX) {
		return as_socket_write_limit(err, fd, buf, buf_len, cf_getms() + timeout_ms);
	}
	else {
		return as_socket_write_forever(err, fd, buf, buf_len);
	}
}

/**
 *	@private
 *	Read socket data without timeouts.
 */
as_status
as_socket_read_forever(as_error* err, int fd, uint8_t *buf, size_t buf_len);

/**
 *	@private
 *	Read socket data with future deadline in milliseconds.
 *	Do not adjust for zero deadline.
 */
as_status
as_socket_read_limit(as_error* err, int fd, uint8_t *buf, size_t buf_len, uint64_t deadline);

/**
 *	@private
 *	Read socket data with future deadline in milliseconds.
 *	If deadline is zero, do not set deadline.
 */
static inline as_status
as_socket_read_deadline(as_error* err, int fd, uint8_t *buf, size_t buf_len, uint64_t deadline)
{
	if (deadline) {
		return as_socket_read_limit(err, fd, buf, buf_len, deadline);
	}
	else {
		return as_socket_read_forever(err, fd, buf, buf_len);
	}
}

/**
 *	@private
 *	Read socket data with timeout in milliseconds.
 *	If timeout is zero or > MAXINT, do not set timeout.
 */
static inline as_status
as_socket_read_timeout(as_error* err, int fd, uint8_t *buf, size_t buf_len, uint32_t timeout_ms)
{
	if (timeout_ms && timeout_ms <= INT32_MAX) {
		return as_socket_read_limit(err, fd, buf, buf_len, cf_getms() + timeout_ms);
	}
	else {
		return as_socket_read_forever(err, fd, buf, buf_len);
	}
}

/**
 *	@private
 *	Convert socket address to a string.
 */
static inline void
as_socket_address_name(struct sockaddr_in* address, char* name)
{
	inet_ntop(AF_INET, &(address->sin_addr), name, INET_ADDRSTRLEN);
}

#endif

#ifdef __cplusplus
} // end extern "C"
#endif
