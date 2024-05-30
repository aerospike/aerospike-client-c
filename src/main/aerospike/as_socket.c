/*
 * Copyright 2008-2024 Aerospike, Inc.
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
#include <aerospike/as_socket.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_node.h>
#include <aerospike/as_poll.h>
#include <aerospike/as_tls.h>
#include <citrusleaf/alloc.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

#if !defined(_MSC_VER)
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>

#define AS_EINTR EINTR

static inline bool
as_socket_is_error(int e)
{
	return !(e == ETIMEDOUT || e == EWOULDBLOCK || e == EINPROGRESS || e == EAGAIN);
}

#else // _MSC_VER
#define AS_EINTR WSAEINTR

static inline bool
as_socket_is_error(int e)
{
	return !(e == WSAETIMEDOUT || e == WSAEWOULDBLOCK || e == WSAEINPROGRESS);
}
#endif

#if defined(__linux__)
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#endif // __linux__

// May want to specify preference for permanent public addresses sometime in the future.
// #ifndef IPV6_PREFER_SRC_PUBLIC
// #define IPV6_PREFER_SRC_PUBLIC 2
// #endif

#define IPV6_ADDR_PREFERENCES 72

bool as_socket_stop_on_interrupt = false;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

void
as_socket_init(as_socket* sock)
{
	memset(sock, 0, sizeof(as_socket));
#if !defined(_MSC_VER)
	sock->fd = -1;
#else
	sock->fd = INVALID_SOCKET;
#endif
}

int
as_socket_create_fd(int family, as_socket_fd* fdp)
{
	// Create the socket.
	as_socket_fd fd = socket(family, SOCK_STREAM, 0);

#if !defined(_MSC_VER)
	if (fd < 0) {
		return -1;
	}

	// Make the socket nonblocking.
	int flags = fcntl(fd, F_GETFL, 0);

	if (flags < 0) {
		as_close(fd);
		return -2;
	}

	if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
		as_close(fd);
		return -2;
	}
#else
	if (fd == INVALID_SOCKET) {
		return -1;
	}

	unsigned long mode = 1;
	if (ioctlsocket(fd, FIONBIO, &mode) != 0) {
		as_close(fd);
		return -2;
	}
#endif

	// Enable TCP no delay.
	int f = 1;
	if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (const char*)&f, sizeof(f)) < 0) {
		as_close(fd);
		return -3;
	}

#ifdef __APPLE__
	if (setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &f, sizeof(f)) < 0) {
		as_close(fd);
		return -4;
	}
#endif

	// May want to specify preference for permanent public addresses sometime in the future.
	// int p = IPV6_PREFER_SRC_PUBLIC;
	// setsockopt(fd, IPPROTO_IPV6, IPV6_ADDR_PREFERENCES, &p, sizeof(p));
	
	*fdp = fd;
	return 0;
}

int
as_socket_create(as_socket* sock, int family, as_tls_context* ctx, const char* tls_name)
{
	as_socket_fd fd;
	int rv = as_socket_create_fd(family, &fd);
	
	if (rv != 0) {
		return rv;
	}
	
	if (! as_socket_wrap(sock, family, fd, ctx, tls_name)) {
		return -5;
	}
	return 0;
}

bool
as_socket_wrap(as_socket* sock, int family, as_socket_fd fd, as_tls_context* ctx, const char* tls_name)
{
	sock->fd = fd;
#if !defined(_MSC_VER)
	sock->family = family;
#endif
	sock->last_used = 0;

	if (ctx) {
		if (as_tls_wrap(ctx, sock, tls_name) < 0) {
			as_close(sock->fd);
			sock->fd = -1;
			return false;
		}
	}
	else {
		sock->ctx = NULL;
		sock->tls_name = NULL;
		sock->ssl = NULL;
	}
	return true;
}

bool
as_socket_start_connect(as_socket* sock, struct sockaddr* addr, uint64_t deadline_ms)
{
	socklen_t size = as_address_size(addr);

	if (!as_socket_connect_fd(sock->fd, addr, size)) {
		return false;
	}

	if (sock->ctx) {
		if (as_tls_connect(sock, deadline_ms)) {
			return false;
		}
	}
	return true;
}

as_status
as_socket_create_and_connect(as_socket* sock, as_error* err, struct sockaddr* addr, as_tls_context* ctx, const char* tls_name, uint64_t deadline_ms)
{
	// Create the socket.
	int rv = as_socket_create(sock, addr->sa_family, ctx, tls_name);
	
	if (rv < 0) {
		char name[AS_IP_ADDRESS_SIZE];
		as_address_name(addr, name, sizeof(name));
		return as_error_update(err, AEROSPIKE_ERR_CONNECTION, "Socket create failed: %d, %s", rv, name);
	}
	
	// Initiate non-blocking connect.
	if (! as_socket_start_connect(sock, addr, deadline_ms)) {
		as_socket_close(sock);
		char name[AS_IP_ADDRESS_SIZE];
		as_address_name(addr, name, sizeof(name));
		return as_error_update(err, AEROSPIKE_ERR_CONNECTION, "Socket connect failed: %s", name);
	}
	return AEROSPIKE_OK;
}

void
as_socket_close(as_socket* sock)
{
	if (sock->ctx) {
		SSL_shutdown(sock->ssl);
		shutdown(sock->fd, SHUT_RDWR);
		SSL_free(sock->ssl);
	}
	else {
		shutdown(sock->fd, SHUT_RDWR);
	}
	as_close(sock->fd);
	sock->fd = -1;
}

as_status
as_socket_error(as_socket_fd fd, as_node* node, as_error* err, as_status status, const char* msg, int code)
{
	if (node) {
		// Print code, address and local port when node present.
		struct sockaddr_storage sa;
		socklen_t len = sizeof(sa);
		uint16_t local_port;

		if (getsockname(fd, (struct sockaddr*)&sa, &len) == 0) {
			local_port = as_address_port((struct sockaddr*)&sa);
		}
		else {
			local_port = 0;
		}
		return as_error_update(err, status, "%s: %d, %s, %d", msg, code, as_node_get_address_string(node), local_port);
	}
	else {
		// Print code only when node not present.  Address will be appended by caller.
		return as_error_update(err, status, "%s: %d", msg, code);
	}
}

void
as_socket_error_append(as_error* err, struct sockaddr* addr)
{
	char name[AS_IP_ADDRESS_SIZE];
	as_address_name(addr, name, sizeof(name));

	int alen = (int)strlen(name);
	int elen = (int)strlen(err->message);

	if (alen + 2 < sizeof(err->message) - elen) {
		char* p = err->message + elen;
		*p++ = ',';
		*p++ = ' ';
		memcpy(p, name, alen);
		p += alen;
		*p = 0;
	}
}

int
as_socket_validate_fd(as_socket_fd fd)
{
#if !defined(_MSC_VER)
	uint8_t buf[8];
#if defined(__linux__) 
	ssize_t rv = recv(fd, (void*)buf, sizeof(buf), MSG_PEEK | MSG_DONTWAIT | MSG_NOSIGNAL);
#else
	ssize_t rv = recv(fd, (void*)buf, sizeof(buf), MSG_PEEK | MSG_DONTWAIT);
#endif

	if (rv < 0) {
		// Return zero if valid and no data available.
		return (errno == EWOULDBLOCK || errno == EAGAIN) ? 0 : -1;
	}

	// Return size of data available if peek succeeded.
	return (rv > 0) ? (int)rv : -1;
#else
	unsigned long bytes;
	int rv = ioctlsocket(fd, FIONREAD, &bytes);
	return (rv == 0) ? bytes : -1;
#endif
}

as_status
as_socket_write_deadline(
	as_error* err, as_socket* sock, struct as_node_s* node, uint8_t *buf, size_t buf_len,
	uint32_t socket_timeout, uint64_t deadline
	)
{
	if (sock->ctx) {
		as_status status = AEROSPIKE_OK;
		int rv = as_tls_write(sock, buf, buf_len, socket_timeout, deadline);

		if (rv < 0) {
			status = as_socket_error(sock->fd, node, err, AEROSPIKE_ERR_CONNECTION, "TLS write error", rv);
		}
		else if (rv == 1) {
			// Do not set error string to avoid affecting performance.
			// Calling functions usually retry, so the error string is
			// not used anyway.
			status = err->code = AEROSPIKE_ERR_TIMEOUT;
			err->message[0] = 0;
		}
		return status;
	}

	as_poll poll;
	as_poll_init(&poll, sock->fd);

	size_t pos = 0;
	as_status status = AEROSPIKE_OK;
	uint32_t timeout;
	//int try = 0;

	do {
		if (deadline > 0) {
			uint64_t now = cf_getms();

			if (now >= deadline) {
				// Timeout.  Do not set error string to avoid affecting performance.
				// Calling functions usually retry, so the error string is not used anyway.
				status = err->code = AEROSPIKE_ERR_TIMEOUT;
				err->message[0] = 0;
				break;
			}

			timeout = (uint32_t)(deadline - now);

			if (socket_timeout > 0 && socket_timeout < timeout) {
				timeout = socket_timeout;
			}
		}
		else {
			timeout = socket_timeout;
		}

		int rv = as_poll_socket(&poll, sock->fd, timeout, false);

		if (rv > 0) {
#if defined(__linux__) 
			int w_bytes = (int)send(sock->fd, buf + pos, buf_len - pos, MSG_NOSIGNAL);
#elif defined(_MSC_VER)
			int w_bytes = send(sock->fd, buf + pos, (int)(buf_len - pos), 0);
#else
			int w_bytes = (int)write(sock->fd, buf + pos, buf_len - pos);
#endif
		
			if (w_bytes > 0) {
				pos += w_bytes;
			}
			else if (w_bytes == 0) {
				// We shouldn't see 0 returned unless we try to write 0 bytes, which we don't.
				status = as_error_set_message(err, AEROSPIKE_ERR_CONNECTION, "Bad file descriptor");
				break;
			}
			else {
				int e = as_last_error();
				if (as_socket_is_error(e)) {
					status = as_socket_error(sock->fd, node, err, AEROSPIKE_ERR_CONNECTION, "Socket write error", e);
					break;
				}
			}
		}
		else if (rv == 0) {
			// Timeout.  Do not set error string to avoid affecting performance.
			// Calling functions usually retry, so the error string is not used anyway.
			status = err->code = AEROSPIKE_ERR_TIMEOUT;
			err->message[0] = 0;
			break;
		}
		else if (rv == -1) {
			int e = as_last_error();
			if (e != AS_EINTR || as_socket_stop_on_interrupt) {
				status = as_socket_error(sock->fd, node, err, AEROSPIKE_ERR_CONNECTION, "Socket write error", e);
				break;
			}
		}
	
		//try++;
	
	} while (pos < buf_len);

	as_poll_destroy(&poll);
	return status;
}

as_status
as_socket_read_deadline(
	as_error* err, as_socket* sock, as_node* node, uint8_t *buf, size_t buf_len,
	uint32_t socket_timeout, uint64_t deadline
	)
{
	if (sock->ctx) {
		as_status status = AEROSPIKE_OK;
		int rv = as_tls_read(sock, buf, buf_len, socket_timeout, deadline);

		if (rv < 0) {
			status = as_socket_error(sock->fd, node, err, AEROSPIKE_ERR_CONNECTION, "TLS read error", rv);
		}
		else if (rv == 1) {
			// Do not set error string to avoid affecting performance.
			// Calling functions usually retry, so the error string is
			// not used anyway.
			status = err->code = AEROSPIKE_ERR_TIMEOUT;
			err->message[0] = 0;
		}
		return status;
	}

	as_poll poll;
	as_poll_init(&poll, sock->fd);

	size_t pos = 0;
	as_status status = AEROSPIKE_OK;
	uint32_t timeout;
	//int try = 0;

	do {
		if (deadline > 0) {
			uint64_t now = cf_getms();

			if (now >= deadline) {
				// Timeout.  Do not set error string to avoid affecting performance.
				// Calling functions usually retry, so the error string is not used anyway.
				status = err->code = AEROSPIKE_ERR_TIMEOUT;
				err->message[0] = 0;
				break;
			}

			timeout = (uint32_t)(deadline - now);

			if (socket_timeout > 0 && socket_timeout < timeout) {
				timeout = socket_timeout;
			}
		}
		else {
			timeout = socket_timeout;
		}

		int rv = as_poll_socket(&poll, sock->fd, timeout, true);

		if (rv > 0) {
#if !defined(_MSC_VER)
			int r_bytes = (int)read(sock->fd, buf + pos, buf_len - pos);
#else
			int r_bytes = (int)recv(sock->fd, buf + pos, (int)(buf_len - pos), 0);
#endif

			if (r_bytes > 0) {
				pos += r_bytes;
			}
			else if (r_bytes == 0) {
				// We believe this means that the server has closed this socket.
				status = as_error_set_message(err, AEROSPIKE_ERR_CONNECTION, "Bad file descriptor");
				break;
			}
			else {
				int e = as_last_error();
				if (as_socket_is_error(e)) {
					status = as_socket_error(sock->fd, node, err, AEROSPIKE_ERR_CONNECTION, "Socket read error", e);
					break;
				}
			}
		}
		else if (rv == 0) {
			// Timeout.  Do not set error string to avoid affecting performance.
			// Calling functions usually retry, so the error string is not used anyway.
			status = err->code = AEROSPIKE_ERR_TIMEOUT;
			err->message[0] = 0;
			break;
		}
		else if (rv == -1) {
			int e = as_last_error();
			if (e != AS_EINTR || as_socket_stop_on_interrupt) {
				status = as_socket_error(sock->fd, node, err, AEROSPIKE_ERR_CONNECTION, "Socket read error", e);
				break;
			}
		}
	
		//try++;
	
	} while (pos < buf_len);

	as_poll_destroy(&poll);
	return status;
}
