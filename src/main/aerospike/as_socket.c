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
#include <aerospike/as_socket.h>
#include <aerospike/as_address.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_node.h>
#include <aerospike/as_tls.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_byte_order.h>
#include <citrusleaf/cf_clock.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

bool as_socket_stop_on_interrupt = false;

#include <fcntl.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/time.h>
#include <errno.h>

// May want to specify preference for permanent public addresses sometime in the future.
// #ifndef IPV6_PREFER_SRC_PUBLIC
// #define IPV6_PREFER_SRC_PUBLIC 2
// #endif

#define IPV6_ADDR_PREFERENCES 72
#define IS_CONNECTING() (errno == EINPROGRESS)

#if defined(__linux__)
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#endif // __linux__

#define STACK_LIMIT (16 * 1024)

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

//
// There is a conflict even among various versions of
// linux, because it's common to compile kernels - or set ulimits -
// where the FD_SETSIZE is much greater than a compiled version.
// Thus, we can compute the required size of the fdset and use a reasonable size
// the other option is using epoll, which is a little scary for this kind of
// "I just want a timeout" usage.
//
// The reality is 8 bits per byte, but this calculation is a little more general
//
static inline size_t
as_fdset_size(int fd)
{
	// Roundup fd in increments of FD_SETSIZE and convert to bytes.
	return ((fd / FD_SETSIZE) + 1) * FD_SETSIZE / 8;
}

// From glibc-2.15 (ubuntu 12.0.4+), the FD_* functions has a check on the
// number of fds passed. According to the man page of FD_SET, the behavior
// is undefined for fd > 1024. But this is enforced from glibc-2.15
// https://sourceware.org/bugzilla/show_bug.cgi?id=10352
//
// So, manipulate the base and the offset of the fd in it
//
static inline void
as_fd_set(int fd, fd_set *fdset)
{
	FD_SET(fd%FD_SETSIZE, &fdset[fd/FD_SETSIZE]);
}

static inline int
as_fd_isset(int fd, fd_set *fdset)
{
	return FD_ISSET(fd%FD_SETSIZE, &fdset[fd/FD_SETSIZE]);
}

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

void
as_socket_init(as_socket* sock)
{
	sock->fd = -1;
	sock->family = 0;
	sock->idle_check.max_socket_idle = sock->idle_check.last_used = 0;
	sock->ctx = NULL;
	sock->tls_name = NULL;
	sock->ssl = NULL;
}

int
as_socket_create_fd(int family)
{
	// Create the socket.
	int fd = socket(family, SOCK_STREAM, 0);
	
	if (fd < 0) {
		return -errno;
	}
	
    // Make the socket nonblocking.
	int flags = fcntl(fd, F_GETFL, 0);
	
    if (flags < 0) {
		int err = -errno;
		close(fd);
		return err;
	}
	
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
		int err = -errno;
		close(fd);
		return err;
	}
	
	int f = 1;
	if (setsockopt(fd, SOL_TCP, TCP_NODELAY, &f, sizeof(f)) < 0) {
		int err = -errno;
		close(fd);
		return err;
	}

#ifdef __APPLE__
	if (setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &f, sizeof(f)) < 0) {
		int err = -errno;
		close(fd);
		return err;
	}
#endif

	// May want to specify preference for permanent public addresses sometime in the future.
	// int p = IPV6_PREFER_SRC_PUBLIC;
	// setsockopt(fd, IPPROTO_IPV6, IPV6_ADDR_PREFERENCES, &p, sizeof(p));
	
	return fd;
}

int
as_socket_create(as_socket* sock, int family, as_tls_context* ctx, const char* tls_name)
{
	int fd = as_socket_create_fd(family);
	
	if (fd < 0) {
		return -1;
	}
	
	if (! as_socket_wrap(sock, family, fd, ctx, tls_name)) {
		return -4;
	}
	return 0;
}

bool
as_socket_wrap(as_socket* sock, int family, int fd, as_tls_context* ctx, const char* tls_name)
{
	sock->fd = fd;
	sock->family = family;
	sock->idle_check.max_socket_idle = sock->idle_check.last_used = 0;

	if (ctx->ssl_ctx) {
		if (as_tls_wrap(ctx, sock, tls_name) < 0) {
			close(sock->fd);
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

	if (connect(sock->fd, addr, size)) {
        if (! IS_CONNECTING()) {
			return false;
		}
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
	close(sock->fd);
	sock->fd = -1;
}

as_status
as_socket_error(int fd, as_node* node, as_error* err, as_status status, const char* msg, int code)
{
	if (node) {
		// Print code, address and local port when node present.
		struct sockaddr_storage sa;
		socklen_t len = sizeof(sa);
		in_port_t local_port;

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
		char* p = stpcpy(err->message + elen, ", ");
		memcpy(p, name, alen);
		p += alen;
		*p = 0;
	}
}

int
as_socket_validate_fd(int fd)
{
	uint8_t buf[8];
	ssize_t rv = recv(fd, (void*)buf, sizeof(buf), MSG_PEEK | MSG_DONTWAIT | MSG_NOSIGNAL);
	
	if (rv < 0) {
		// Return zero if valid and no data available.
		return (errno == EWOULDBLOCK || errno == EAGAIN) ? 0 : -1;
	}
	
	// Return size of data available if peek succeeded.
	return (rv > 0) ? (int)rv : -1;
}

int
as_socket_validate(as_socket* sock)
{
	if (sock->idle_check.max_socket_idle > 0) {
		uint32_t idle = (uint32_t)cf_get_seconds() - sock->idle_check.last_used;

		if (idle > sock->idle_check.max_socket_idle) {
			return -1;
		}
	}

	return as_socket_validate_fd(sock->fd);
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

	struct timeval tv;
	struct timeval* tvp = NULL;
	size_t pos = 0;

	// Setup fdset. This looks weird, but there's no guarantee that compiled
	// FD_SETSIZE has much to do with the machine we're running on.
	size_t wset_size = as_fdset_size(sock->fd);
	fd_set* wset = (fd_set*)(wset_size > STACK_LIMIT ? cf_malloc(wset_size) : alloca(wset_size));

	if (!wset) {
		return as_socket_error(sock->fd, node, err, AEROSPIKE_ERR_CONNECTION, "Socket fdset allocation error", (int)wset_size);
	}

	as_status status = AEROSPIKE_OK;
	int try = 0;

	do {
		if (deadline > 0) {
			uint64_t now = cf_getms();

			if (now > deadline) {
				// Timeout.  Do not set error string to avoid affecting performance.
				// Calling functions usually retry, so the error string is not used anyway.
				status = err->code = AEROSPIKE_ERR_TIMEOUT;
				err->message[0] = 0;
				break;
			}

			uint64_t ms_left = deadline - now;

			if (socket_timeout > 0 && socket_timeout < ms_left) {
				ms_left = socket_timeout;
			}
			tv.tv_sec = ms_left / 1000;
			tv.tv_usec = (ms_left % 1000) * 1000;
			tvp = &tv;
		}
		else {
			if (socket_timeout > 0) {
				tv.tv_sec = socket_timeout / 1000;
				tv.tv_usec = (socket_timeout % 1000) * 1000;
				tvp = &tv;
			}
		}

		memset((void*)wset, 0, wset_size);
		as_fd_set(sock->fd, wset);
	
		int rv = select(sock->fd+1, 0 /*readfd*/, wset /*writefd*/, 0/*oobfd*/, tvp);
	
		// we only have one fd, so we know it's ours, but select seems confused sometimes - do the safest thing
		if ((rv > 0) && as_fd_isset(sock->fd, wset)) {
		
#if defined(__linux__)
			int w_bytes = (int)send(sock->fd, buf + pos, buf_len - pos, MSG_NOSIGNAL);
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
			else if (errno != ETIMEDOUT
					 && errno != EWOULDBLOCK && errno != EINPROGRESS && errno != EAGAIN) {
				status = as_socket_error(sock->fd, node, err, AEROSPIKE_ERR_CONNECTION, "Socket write error", errno);
				break;
			}
		}
		else if (rv == 0) {
			// Timeout.  Do not set error string to avoid affecting performance.
			// Calling functions usually retry, so the error string is not used anyway.
			status = err->code = AEROSPIKE_ERR_TIMEOUT;
			err->message[0] = 0;
			break;
		}
		else {
			if (rv == -1 && (errno != EINTR || as_socket_stop_on_interrupt)) {
				status = as_socket_error(sock->fd, node, err, AEROSPIKE_ERR_CONNECTION, "Socket write error", errno);
				break;
			}
		}
	
		try++;
	
	} while (pos < buf_len);

	if (wset_size > STACK_LIMIT) {
		cf_free(wset);
	}
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

	struct timeval tv;
	struct timeval* tvp = NULL;
	size_t pos = 0;

	// Setup fdset. This looks weird, but there's no guarantee that compiled
	// FD_SETSIZE has much to do with the machine we're running on.
	size_t rset_size = as_fdset_size(sock->fd);
	fd_set* rset = (fd_set*)(rset_size > STACK_LIMIT ? cf_malloc(rset_size) : alloca(rset_size));

	if (!rset) {
		return as_socket_error(sock->fd, node, err, AEROSPIKE_ERR_CONNECTION, "Socket fdset allocation error", (int)rset_size);
	}

	as_status status = AEROSPIKE_OK;
	int try = 0;

	do {
		if (deadline > 0) {
			uint64_t now = cf_getms();

			if (now > deadline) {
				// Timeout.  Do not set error string to avoid affecting performance.
				// Calling functions usually retry, so the error string is not used anyway.
				status = err->code = AEROSPIKE_ERR_TIMEOUT;
				err->message[0] = 0;
				break;
			}

			uint64_t ms_left = deadline - now;

			if (socket_timeout > 0 && socket_timeout < ms_left) {
				ms_left = socket_timeout;
			}
			tv.tv_sec = ms_left / 1000;
			tv.tv_usec = (ms_left % 1000) * 1000;
			tvp = &tv;
		}
		else {
			if (socket_timeout > 0) {
				tv.tv_sec = socket_timeout / 1000;
				tv.tv_usec = (socket_timeout % 1000) * 1000;
				tvp = &tv;
			}
		}

		memset((void*)rset, 0, rset_size);
		as_fd_set(sock->fd, rset);
		int rv = select(sock->fd+1, rset /*readfd*/, 0 /*writefd*/, 0 /*oobfd*/, tvp);
	
		// we only have one fd, so we know it's ours?
		if ((rv > 0) && as_fd_isset(sock->fd, rset)) {
		
			int r_bytes = (int)read(sock->fd, buf + pos, buf_len - pos);
		
			if (r_bytes > 0) {
				pos += r_bytes;
			}
			else if (r_bytes == 0) {
				// We believe this means that the server has closed this socket.
				status = as_error_set_message(err, AEROSPIKE_ERR_CONNECTION, "Bad file descriptor");
				break;
			}
			else if (errno != ETIMEDOUT
					 // It's apparently possible that select() returns successfully yet
					 // the socket is not ready for reading.
					 && errno != EWOULDBLOCK && errno != EINPROGRESS && errno != EAGAIN) {
				status = as_socket_error(sock->fd, node, err, AEROSPIKE_ERR_CONNECTION, "Socket read error", errno);
				break;
			}
		}
		else if (rv == 0) {
			// Timeout.  Do not set error string to avoid affecting performance.
			// Calling functions usually retry, so the error string is not used anyway.
			status = err->code = AEROSPIKE_ERR_TIMEOUT;
			err->message[0] = 0;
			break;
		}
		else {
			if (rv == -1 && (errno != EINTR || as_socket_stop_on_interrupt)) {
				status = as_socket_error(sock->fd, node, err, AEROSPIKE_ERR_CONNECTION, "Socket read error", errno);
				break;
			}
		}
	
		try++;
	
	} while (pos < buf_len);

	if (rset_size > STACK_LIMIT) {
		cf_free(rset);
	}
	return status;
}
