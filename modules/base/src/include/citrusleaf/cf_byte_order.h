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
