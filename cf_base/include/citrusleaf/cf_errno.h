/*
 * Copyright 2013 Aerospike. All rights reserved.
 */

#pragma once

#ifndef CF_WINDOWS
//====================================================================
// Linux
//

#include <errno.h>


#else // CF_WINDOWS
//====================================================================
// Windows
//

#define errno (WSAGetLastError())

#define EAGAIN			WSAEWOULDBLOCK
#define EBADF			WSAEBADF
#define ECONNREFUSED	WSAECONNREFUSED
#define EINPROGRESS		WSAEINPROGRESS
#define EWOULDBLOCK		WSAEWOULDBLOCK


#endif // CF_WINDOWS
