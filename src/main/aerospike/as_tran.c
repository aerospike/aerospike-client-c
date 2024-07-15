/*
 * Copyright 2008-2024 Aerospike, Inc.
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
#include <aerospike/as_tran.h>
#include <citrusleaf/cf_random.h>

//---------------------------------
// Functions
//---------------------------------

void
as_tran_init(as_tran* tran)
{
	// An id of zero is considered invalid. Create random numbers
	// in a loop until non-zero is returned.
	uint64_t id = cf_get_rand64();

	while (id == 0) {
		id = cf_get_rand64();
	}
	
	as_tran_init_uint64(tran, id);
}

void
as_tran_init_uint64(as_tran* tran, uint64_t id)
{
	tran->id = id;
	tran->ns[0] = 0;
	tran->deadline = 0;
	
	// TODO: Initialize reads and writes hashmaps.
}

bool
as_tran_set_ns(as_tran* tran, const char* ns)
{
	if (tran->ns[0] == 0) {
		as_strncpy(tran->ns, ns, sizeof(tran->ns));
		return true;
	}
	
	return strncmp(tran->ns, ns, sizeof(tran->ns)) == 0;
}

bool
as_tran_on_read(as_tran* tran, as_key* key, uint64_t version)
{
	// TODO: Do not call as_tran_on_read() when version not returned from server. 
	// TODO: Can we just make zero an invalid version?
	if (! as_tran_set_ns(tran, key->ns)) {
		return false;
	}
	
	// TODO: Put key/version in reads hashmap.
	return true;
}

uint64_t
as_tran_get_read_version(as_tran* tran, as_key* key)
{
	// TODO:
	//return reads.get(key);
	return 0;
}

bool
as_tran_on_write(as_tran* tran, as_key* key, uint64_t version, int rc)
{
	// TOOD: Can we just make zero an invalid version?
	// TODO: Should key.namespace be verified here?
	if (version != 0) {
		// TODO:
		//reads.put(key, version);
	}
	else {
		if (rc == AEROSPIKE_OK) {
			// TODO:
			//reads.remove(key);
			//writes.add(key);
		}
	}
	return true;
}

void
as_tran_close(as_tran* tran)
{
	tran->ns[0] = 0;
	tran->deadline = 0;
	
	// TODO:
	//reads.clear();
	//writes.clear();
}
