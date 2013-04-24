/*
 * Copyright 2013 Aerospike. All rights reserved.
 */

#pragma once

#ifndef CF_WINDOWS
//====================================================================
// Linux
//

#include <errno.h>

#define IS_CONNECTING() (errno == EINPROGRESS)


#else // CF_WINDOWS
//====================================================================
// Windows
//

#include <WinSock2.h>

#undef errno

#undef EAGAIN
#undef EBADF
#undef ECONNREFUSED
#undef EINPROGRESS
#undef EWOULDBLOCK

// If we ever use errno for other than socket operations, we may have to
// introduce new and different definitions for errno.
#define errno (WSAGetLastError())

#define EAGAIN			WSAEWOULDBLOCK
#define EBADF			WSAEBADF
#define ECONNREFUSED	WSAECONNREFUSED
#define EINPROGRESS		WSAEINPROGRESS
#define EWOULDBLOCK		WSAEWOULDBLOCK

#define IS_CONNECTING() (errno == EWOULDBLOCK)


#endif // CF_WINDOWS
