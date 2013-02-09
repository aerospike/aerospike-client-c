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

#include <stdint.h>
#include <stdlib.h>
#include <WinSock2.h>

#define cf_byteswap64p(_p) (_byteswap_uint64(*(uint64_t*)(_p)))


#endif // CF_WINDOWS
