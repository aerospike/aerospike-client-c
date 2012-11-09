/*
 * Copyright 2012 Aerospike. All rights reserved.
 */
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_log_internal.h"

void
cf_digest_dump(cf_digest *digest)
{
	uint8_t *d = (uint8_t *) digest;
	cf_debug("%02x %02x %02x %02x : %02x %02x %02x %02x : %02x %02x",
		d[0],d[1],d[2],d[3],d[4],d[5],d[6],d[7],d[8],d[9]);
	cf_debug("%02x %02x %02x %02x : %02x %02x %02x %02x : %02x %02x",
		d[10],d[11],d[12],d[13],d[14],d[15],d[16],d[17],d[18],d[19]);
}
