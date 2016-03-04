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
#include <aerospike/as_socket.h>
#include <aerospike/as_log_macros.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_byte_order.h>
#include <citrusleaf/cf_clock.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

bool as_socket_stop_on_interrupt = false;

#if defined(__linux__) || defined(__APPLE__)
#include <fcntl.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/time.h>
#include <errno.h>
#define IS_CONNECTING() (errno == EINPROGRESS)
#endif // __linux__ __APPLE__

#if defined(__linux__)
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#endif // __linux__

#if defined(CF_WINDOWS)
#include <WinSock2.h>

#undef errno
#undef EAGAIN
#undef EINPROGRESS
#undef EWOULDBLOCK

// If we ever use errno for other than socket operations, we may have to
// introduce new and different definitions for errno.
#define errno (WSAGetLastError())
#define EAGAIN			WSAEWOULDBLOCK
#define EINPROGRESS		WSAEINPROGRESS
#define EWOULDBLOCK		WSAEWOULDBLOCK
#define IS_CONNECTING() (errno == EWOULDBLOCK)
#endif // CF_WINDOWS

#if defined(__linux__) || defined(__APPLE__)

#define STACK_LIMIT (16 * 1024)

/******************************************************************************
 * DEBUG FUNCTIONS
 *****************************************************************************/

// #define DEBUG_TIME

#ifdef DEBUG_TIME
#include <pthread.h>

static void
debug_time_printf(const char* desc, int try, int busy, uint64_t start, uint64_t end, uint64_t deadline)
{
	as_log_info("%s|%zu|%d|%d|%lu|%lu|%lu",
		desc, (uint64_t)pthread_self(),
		try,
		busy,
		start,
		end,
		deadline
	);
}
#endif

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

int
as_socket_create_nb()
{
	// Create the socket.
	int fd = socket(AF_INET, SOCK_STREAM, 0);

	if (fd == -1) {
		return -1;
	}

    // Make the socket nonblocking.
	int flags = fcntl(fd, F_GETFL, 0);

    if (flags < 0) {
		close(fd);
		return -2;
	}

    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
		close(fd);
		return -3;
	}

	int f = 1;
	setsockopt(fd, SOL_TCP, TCP_NODELAY, &f, sizeof(f));
#ifdef __APPLE__
	setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &f, sizeof(f));
#endif
	return fd;
}

as_status
as_socket_start_connect_nb(as_error* err, int fd, struct sockaddr_in* sa)
{
	if (0 != connect(fd, (struct sockaddr*)sa, sizeof(*sa))) {
        if (! IS_CONNECTING()) {
			return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Socket connect failed, errno %d", errno);
		}
	}
	return AEROSPIKE_OK;
}

as_status
as_socket_create_and_connect_nb(as_error* err, struct sockaddr_in *sa, int* fd_out)
{
	// Create the socket.
	int fd = as_socket_create_nb();
	
	if (fd < 0) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Socket create failed");
	}
	
	// Initiate non-blocking connect.
	as_status status = as_socket_start_connect_nb(err, fd, sa);
	
	if (status) {
		as_close(fd);
		return status;
	}
	*fd_out = fd;
	return AEROSPIKE_OK;
}

int
as_socket_validate(int fd)
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

as_status
as_socket_write_forever(as_error* err, int fd, uint8_t *buf, size_t buf_len)
{
	// MacOS will return "socket not connected" errors even when connection is
	// blocking.  Therefore, select() is required before writing.  Since write
	// timeout function also calls select(), use write timeout function with
	// 1 minute timeout.
	return as_socket_write_timeout(err, fd, buf, buf_len, 60000);
}

as_status
as_socket_write_limit(as_error* err, int fd, uint8_t *buf, size_t buf_len, uint64_t deadline)
{
#ifdef DEBUG_TIME
	uint64_t start = cf_getms();
#endif
	struct timeval tv;
	size_t pos = 0;
    
	int flags;
	if (-1 == (flags = fcntl(fd, F_GETFL, 0)))
		flags = 0;
	if (! (flags & O_NONBLOCK)) {
		if (-1 == fcntl(fd, F_SETFL, flags | O_NONBLOCK)) {
			return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Socket nonblocking set failed.");
		}
	}
	
	// Setup fdset. This looks weird, but there's no guarantee that compiled
	// FD_SETSIZE has much to do with the machine we're running on.
	size_t wset_size = as_fdset_size(fd);
	fd_set* wset = (fd_set*)(wset_size > STACK_LIMIT ? cf_malloc(wset_size) : alloca(wset_size));
	
	if (!wset) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Socket fdset allocation error: %d", wset_size);
	}
	
	as_status status = AEROSPIKE_OK;
	int try = 0;
#ifdef DEBUG_TIME
	int select_busy = 0;
#endif
    
	do {
		uint64_t now = cf_getms();
		if (now > deadline) {
#ifdef DEBUG_TIME
			debug_time_printf("socket writeselect timeout", try, select_busy, start, now, deadline);
#endif
			// Do not set error string to avoid affecting performance.
			// Calling functions usually retry, so the error string is not used anyway.
			status = err->code = AEROSPIKE_ERR_TIMEOUT;
			err->message[0] = 0;
			goto Out;
        }
        
		uint64_t ms_left = deadline - now;
		tv.tv_sec = ms_left / 1000;
		tv.tv_usec = (ms_left % 1000) * 1000;
        
		memset((void*)wset, 0, wset_size);
		as_fd_set(fd, wset);
        
		int rv = select(fd +1, 0 /*readfd*/, wset /*writefd*/, 0/*oobfd*/, &tv);
        
		// we only have one fd, so we know it's ours, but select seems confused sometimes - do the safest thing
		if ((rv > 0) && as_fd_isset(fd, wset)) {
            
#if defined(__linux__)
			int r_bytes = (int)send(fd, buf + pos, buf_len - pos, MSG_NOSIGNAL);
#else
			int r_bytes = (int)write(fd, buf + pos, buf_len - pos);
#endif
            
			if (r_bytes > 0) {
				pos += r_bytes;
				if (pos >= buf_len)	{
					// done happily
					goto Out;
				}
            }
			else if (r_bytes == 0) {
				// We shouldn't see 0 returned unless we try to write 0 bytes, which we don't.
				status = as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Bad file descriptor");
				goto Out;
			}
            else if (errno != ETIMEDOUT
                     && errno != EWOULDBLOCK && errno != EINPROGRESS && errno != EAGAIN) {
#ifdef DEBUG_TIME
    			debug_time_printf("socket write timeout", try, select_busy, start, now, deadline);
#endif
				status = as_error_update(err, AEROSPIKE_ERR_CLIENT, "Socket write error: %d", errno);
                goto Out;
			}
		}
        else if (rv == 0) {
#ifdef DEBUG_TIME
        	select_busy++;
#endif
    	}
        else {
			if (rv == -1 && (errno != EINTR || as_socket_stop_on_interrupt)) {
				status = as_error_update(err, AEROSPIKE_ERR_CLIENT, "Socket write error: %d", errno);
				goto Out;
			}
        }
        
		try++;
        
	} while( pos < buf_len );
    
Out:
	if (wset_size > STACK_LIMIT) {
		cf_free(wset);
	}
	return status;
}

//
// These FOREVER calls are only called in the 'getmany' case, which is used
// for application level highly variable queries
//
as_status
as_socket_read_forever(as_error* err, int fd, uint8_t *buf, size_t buf_len)
{
	// one way is to make sure the fd is blocking, and block
	int flags;
	if (-1 == (flags = fcntl(fd, F_GETFL, 0)))
		flags = 0;
	if (flags & O_NONBLOCK) {
		if (-1 == fcntl(fd, F_SETFL, flags & ~O_NONBLOCK)) {
			return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Socket blocking set failed.");
		}
	}
    
	size_t pos = 0;
	do {
		int r_bytes = (int)read(fd, buf + pos, buf_len - pos );
		if (r_bytes < 0) { // don't combine these into one line! think about it!
			if (errno != ETIMEDOUT) {
				return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Socket read forever failed, errno %d", errno);
			}
		}
		else if (r_bytes == 0) {
			// blocking read returns 0 bytes socket timedout at server side
			// is closed
			return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Bad file descriptor");
		}
		else {
			pos += r_bytes;
		}
	} while (pos < buf_len);
    
	return AEROSPIKE_OK;
}

as_status
as_socket_read_limit(as_error* err, int fd, uint8_t *buf, size_t buf_len, uint64_t deadline)
{
#ifdef DEBUG_TIME
	uint64_t start = cf_getms();
#endif
	struct timeval tv;
	size_t pos = 0;
    
	int flags;
	if (-1 == (flags = fcntl(fd, F_GETFL, 0)))
		flags = 0;
	if (! (flags & O_NONBLOCK)) {
		if (-1 == fcntl(fd, F_SETFL, flags | O_NONBLOCK)) {
			return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Socket nonblocking set failed.");
		}
	}
    
	// Setup fdset. This looks weird, but there's no guarantee that compiled
	// FD_SETSIZE has much to do with the machine we're running on.
	size_t rset_size = as_fdset_size(fd);
	fd_set* rset = (fd_set*)(rset_size > STACK_LIMIT ? cf_malloc(rset_size) : alloca(rset_size));
	
	if (!rset) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Socket fdset allocation error: %d", rset_size);
	}

	int status = AEROSPIKE_OK;
	int try = 0;
#ifdef DEBUG_TIME
	int select_busy = 0;
#endif
    
	do {
		uint64_t now = cf_getms();
		if (now > deadline) {
#ifdef DEBUG_TIME
			debug_time_printf("socket readselect timeout", try, select_busy, start, now, deadline);
#endif
			// Do not set error string to avoid affecting performance.
			// Calling functions usually retry, so the error string is not used anyway.
			status = err->code = AEROSPIKE_ERR_TIMEOUT;
			err->message[0] = 0;
			goto Out;
        }
        
		uint64_t ms_left = deadline - now;
		tv.tv_sec = ms_left / 1000;
		tv.tv_usec = (ms_left % 1000) * 1000;
        
		memset((void*)rset, 0, rset_size);
		as_fd_set(fd, rset);
		int rv = select(fd +1, rset /*readfd*/, 0 /*writefd*/, 0 /*oobfd*/, &tv);
        
		// we only have one fd, so we know it's ours?
		if ((rv > 0) && as_fd_isset(fd, rset)) {
            
			int r_bytes = (int)read(fd, buf + pos, buf_len - pos);
            
			if (r_bytes > 0) {
				pos += r_bytes;
            }
			else if (r_bytes == 0) {
				// We believe this means that the server has closed this socket.
				status = as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Bad file descriptor");
				goto Out;
			}
            else if (errno != ETIMEDOUT
                     // It's apparently possible that select() returns successfully yet
                     // the socket is not ready for reading.
                     && errno != EWOULDBLOCK && errno != EINPROGRESS && errno != EAGAIN) {
#ifdef DEBUG_TIME
    			debug_time_printf("socket read timeout", try, select_busy, start, now, deadline);
#endif
				status = as_error_update(err, AEROSPIKE_ERR_CLIENT, "Socket read error: %d", errno);
				goto Out;
            }
		}
		else if (rv == 0) {
#ifdef DEBUG_TIME
			select_busy++;
#endif
        }
        else {
			if (rv == -1 && (errno != EINTR || as_socket_stop_on_interrupt)) {
				status = as_error_update(err, AEROSPIKE_ERR_CLIENT, "Socket read error: %d", errno);
				goto Out;
			}
        }
        
        try++;
        
	} while (pos < buf_len);
    
Out:
	if (rset_size > STACK_LIMIT) {
		cf_free(rset);
	}
	return status;
}

#else // CF_WINDOWS
//====================================================================
// Windows
//

#include <WinSock2.h>
#include <Ws2tcpip.h>

int
as_socket_create_nb()
{
	// Create the socket.
	SOCKET sock = socket(AF_INET, SOCK_STREAM, 0);

	if (sock == -1) {
		return sock;
	}

    // Make the socket nonblocking.
	u_long iMode = 1;

	if (NO_ERROR != ioctlsocket(sock, FIONBIO, &iMode)) {
		// close sock here?
		return -1;
	}

	int f = 1;
	setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char*)&f, sizeof(f));

	return (int)sock;
}

#endif // CF_WINDOWS
