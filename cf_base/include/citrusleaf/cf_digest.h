/*
 *  Citrusleaf Foundation
 *  include/digest.h - message digests
 *
 *  Copyright 2008 by Citrusleaf.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */
#pragma once
#include <stdint.h>
#include <openssl/ripemd.h>
#include "cf_log.h"

/* SYNOPSIS
 * Cryptographic message digests
 * The intent is to provide an algorithm-neutral API for the computation of
 * cryptographic digests of arbitrary bytes.  Consequently, we define the
 * cf_digest type to be an array of bytes of the appropriate length.
 * The actual computation is done in one shot by calling cf_digest_compute().
 */

#ifdef __cplusplus
extern "C" {
#endif

/* cf_digest
 * Storage for a message digest */
#define CF_DIGEST_KEY_SZ RIPEMD160_DIGEST_LENGTH
typedef struct { uint8_t digest[CF_DIGEST_KEY_SZ]; } cf_digest;

static inline void
cf_digest_dump(cf_digest *digest)
{
	uint8_t *d = (uint8_t *) digest;
	cf_debug("%02x %02x %02x %02x : %02x %02x %02x %02x : %02x %02x",
		d[0],d[1],d[2],d[3],d[4],d[5],d[6],d[7],d[8],d[9]);
	cf_debug("%02x %02x %02x %02x : %02x %02x %02x %02x : %02x %02x",
		d[10],d[11],d[12],d[13],d[14],d[15],d[16],d[17],d[18],d[19]);
}


/* cf_digest_compute
 * Compute the digest of an input */ 
static inline void
cf_digest_compute(void *data, size_t len, cf_digest *d)
{
	RIPEMD160((const unsigned char *) data, len, (unsigned char *) d->digest);
}


// Compute a digest
// the first value is the key data, the second is the set
// DO NOT USE THIS FUNCTION EXTERNALLY
// the externally visible function is 'citrusleaf_calculate_digest', which has
// the proper typing and swizzling

static inline void
cf_digest_compute2(void *data1, size_t len1, void *data2, size_t len2, cf_digest *d)
{
	if (len1 == 0) {
		RIPEMD160((const unsigned char *) data2, len2, (unsigned char *) d->digest);
	}
	else {
		RIPEMD160_CTX c;
		RIPEMD160_Init(&c);
		RIPEMD160_Update(&c, data1, len1);
		RIPEMD160_Update(&c, data2, len2);
		RIPEMD160_Final( (unsigned char *)(d->digest), &c);
	}
}

/* as_partition_getid
 * A brief utility function to derive the partition ID from a digest */

typedef uint16_t cl_partition_id;
static inline cl_partition_id
cl_partition_getid(uint32_t n_partitions, cf_digest *d)
{
	uint16_t *d_int = (uint16_t *)&d->digest[0];
	cl_partition_id r = *d_int & (n_partitions - 1);
    return(r);
}

#ifdef __cplusplus
} // end extern "C"
#endif


