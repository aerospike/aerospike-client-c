/*
 * Copyright 2013 Aerospike. All rights reserved.
 */

#pragma once

#ifndef CF_WINDOWS
#ifdef OSX
//====================================================================
// Mac OS
//

#include <libkern/OSByteOrder.h>

#define cf_byteswap64p(_p) (OSSwapBigToHostInt64(*(_p)))

#else
//====================================================================
// Linux
//

// A real pity that Linux requires this for bool, true & false:
#include <stdbool.h>

#include <alloca.h>

#include <netinet/in.h>
#include <asm/byteorder.h>

#define htonll(_n) (__cpu_to_be64(_n))
#define ntohll(_n) (__be64_to_cpu(_n))

#define cf_byteswap64p(_p) (__swab64p((const __u64*)(_p)))


#endif
#else // CF_WINDOWS
//====================================================================
// Windows
//

// For alloca():
#include <malloc.h>

#include <WinSock2.h>
#include <stdlib.h>

#define cf_byteswap64p(_p) (_byteswap_uint64(*(uint64_t*)(_p)))


#endif // CF_WINDOWS
