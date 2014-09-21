/*
 * Copyright 2008-2014 Aerospike, Inc.
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
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_types.h>
#include <citrusleaf/cf_client_rc.h>


/* cf_client_rc_count
 * Get the reservation count for a memory region */
cf_atomic_int_t
cf_client_rc_count(void *addr)
{
	cf_client_rc_counter *rc;

	rc = (cf_client_rc_counter *) (((uint8_t *)addr) - sizeof(cf_client_rc_counter));

	return(*rc);
}


/* cf_client_rc_reserve
 * Get a reservation on a memory region */
int
cf_client_rc_reserve(void *addr)
{
	cf_client_rc_counter *rc;

	/* Extract the address of the reference counter, then increment it */
	rc = (cf_client_rc_counter *) (((uint8_t *)addr) - sizeof(cf_client_rc_counter));

	int i = (int) cf_atomic32_add(rc, 1);
	smb_mb();
	return(i);

}


/* cf_client_rc_release
 * Release a reservation on a memory region */
cf_atomic_int_t
cf_client_rc_release_x(void *addr, bool autofree)
{
	cf_client_rc_counter *rc;
	uint64_t c;
	
	/* Release the reservation; if this reduced the reference count to zero,
	 * then free the block if autofree is set, and return 1.  Otherwise,
	 * return 0 */
	rc = (cf_client_rc_counter *) (((uint8_t *)addr) - sizeof(cf_client_rc_counter));
	smb_mb();
	if (0 == (c = cf_atomic32_decr(rc)))
		if (autofree)
			free((void *)rc);

	return (cf_atomic_int_t)c;
}


/* cf_client_rc_alloc
 * Allocate a reference-counted memory region.  This region will be filled
 * with uint8_ts of value zero */
void *
cf_client_rc_alloc(size_t sz)
{
	uint8_t *addr;
	size_t asz = sizeof(cf_client_rc_counter) + sz;

	addr = (uint8_t*)malloc(asz);
	if (NULL == addr)
		return(NULL);

	cf_atomic_int_set((cf_client_rc_counter *)addr, 1);
	uint8_t *base = addr + sizeof(cf_client_rc_counter);

	return(base);
}


/* cf_client_rc_free
 * Deallocate a reference-counted memory region */
void
cf_client_rc_free(void *addr)
{
	cf_client_rc_counter *rc;

	rc = (cf_client_rc_counter *) (((uint8_t *)addr) - sizeof(cf_client_rc_counter));

	free((void *)rc);

	return;
}
