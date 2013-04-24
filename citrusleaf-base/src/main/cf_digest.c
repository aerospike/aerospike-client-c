/*
 * Copyright 2012 Aerospike. All rights reserved.
 */

#include <stdint.h>
#include <stdio.h>

#include "citrusleaf/cf_log_internal.h"

#include "citrusleaf/cf_digest.h"

void
cf_digest_string(cf_digest *digest, char* output)
{
	uint8_t *d = (uint8_t *) digest;
	char* p = output;

	*p++ = '0';
	*p++ = 'x';

	for (int i = 0; i < CF_DIGEST_KEY_SZ; i++) {
		sprintf(p, "%02x", d[i]);
		p += 2;
	}
}

void
cf_digest_dump(cf_digest *digest)
{
	uint8_t *d = (uint8_t *) digest;
	cf_debug("%02x %02x %02x %02x : %02x %02x %02x %02x : %02x %02x",
		d[0],d[1],d[2],d[3],d[4],d[5],d[6],d[7],d[8],d[9]);
	cf_debug("%02x %02x %02x %02x : %02x %02x %02x %02x : %02x %02x",
		d[10],d[11],d[12],d[13],d[14],d[15],d[16],d[17],d[18],d[19]);
}
