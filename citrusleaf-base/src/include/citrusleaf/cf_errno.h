/******************************************************************************
 * Copyright 2008-2013 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to 
 * deal in the Software without restriction, including without limitation the 
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 * sell copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *****************************************************************************/
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
