/*
 * Copyright 2008-2018 Aerospike, Inc.
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

#if !defined(_MSC_VER)
#include <sys/select.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * TYPES
 *****************************************************************************/

typedef struct as_poll_s {
#if !defined(_MSC_VER)
	size_t size;
	fd_set* set;
#else
	fd_set set;
#endif
} as_poll;

/******************************************************************************
 * INLINE FUNCTIONS
 *****************************************************************************/

#if !defined(_MSC_VER)

#define AS_STACK_LIMIT (16 * 1024)

// There is a conflict even among various versions of linux,
// because it's common to compile kernels - or set ulimits -
// where the FD_SETSIZE is much greater than a compiled version.
// Thus, we can compute the required size of the fdset and use a reasonable size
// the other option is using epoll, which is a little scary for this kind of
// "I just want a timeout" usage.
//
// The reality is 8 bits per byte, but this calculation is a little more general.
// Roundup fd in increments of FD_SETSIZE and convert to bytes.
#define as_poll_init(_poll, _fd)\
	(_poll)->size = (((_fd) / FD_SETSIZE) + 1) * FD_SETSIZE / 8;\
	(_poll)->set = ((_poll)->size > AS_STACK_LIMIT)? cf_malloc((_poll)->size) : alloca((_poll)->size);

static inline int
as_poll_socket(as_poll* poll, as_socket_fd fd, uint32_t timeout, bool read)
{
	// From glibc-2.15 (ubuntu 12.0.4+), the FD_* functions has a check on the
	// number of fds passed. According to the man page of FD_SET, the behavior
	// is undefined for fd > 1024. But this is enforced from glibc-2.15
	// https://sourceware.org/bugzilla/show_bug.cgi?id=10352
	//
	// So, manipulate the base and the offset of the fd in it.
	memset(poll->set, 0, poll->size);
	FD_SET(fd % FD_SETSIZE, &poll->set[fd / FD_SETSIZE]);

	struct timeval tv;
	struct timeval* tvp;
	int rv;

	if (timeout > 0) {
		tv.tv_sec = timeout / 1000;
		tv.tv_usec = (timeout % 1000) * 1000;
		tvp = &tv;
	}
	else {
		tvp = NULL;
	}

	if (read) {
		rv = select(fd + 1, poll->set /*readfd*/, 0 /*writefd*/, 0/*oobfd*/, tvp);
	}
	else {
		rv = select(fd + 1, 0 /*readfd*/, poll->set /*writefd*/, 0/*oobfd*/, tvp);
	}

	if (rv <= 0) {
		return rv;
	}

	if (! FD_ISSET(fd % FD_SETSIZE, &poll->set[fd / FD_SETSIZE])) {
		return -2;
	}
	return rv;
}

static inline void
as_poll_destroy(as_poll* poll)
{
	if (poll->size > AS_STACK_LIMIT) {
		cf_free(poll->set);
	}
}

#else  // _MSC_VER

#define as_poll_init(_poll, _fd)

static inline int
as_poll_socket(as_poll* poll, as_socket_fd fd, uint32_t timeout, bool read)
{
	FD_ZERO(&poll->set);
	FD_SET(fd, &poll->set);

	struct timeval tv;
	struct timeval* tvp;
	int rv;

	if (timeout > 0) {
		tv.tv_sec = timeout / 1000;
		tv.tv_usec = (timeout % 1000) * 1000;
		tvp = &tv;
	}
	else {
		tvp = NULL;
	}

	if (read) {
		rv = select(0, &poll->set /*readfd*/, 0 /*writefd*/, 0/*oobfd*/, tvp);
	}
	else {
		rv = select(0, 0 /*readfd*/, &poll->set /*writefd*/, 0/*oobfd*/, tvp);
	}

	if (rv <= 0) {
		return rv;
	}

	if (! FD_ISSET(fd, &poll->set)) {
		return -2;
	}
	return rv;
}

#define as_poll_destroy(_poll)

#endif

#ifdef __cplusplus
} // end extern "C"
#endif
