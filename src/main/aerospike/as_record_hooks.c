/*
 * Copyright 2008-2016 Aerospike, Inc.
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
#include <aerospike/as_bin.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_key.h>
#include <aerospike/as_list.h>
#include <aerospike/as_map.h>
#include <aerospike/as_nil.h>
#include <aerospike/as_record.h>
#include <aerospike/as_string.h>

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "_bin.h"

/******************************************************************************
 *	EXTERN FUNCTIONS
 *****************************************************************************/

extern void as_record_release(as_record * rec);

/******************************************************************************
 *	HOOK FUNCTIONS
 *****************************************************************************/

static bool as_record_rec_destroy(as_rec * r) 
{
	as_record_release((as_record *) r);
	return true;
}

static uint32_t as_record_rec_hashcode(const as_rec * r) 
{
	as_record * rec = (as_record *) r;

	uint32_t hash = 0;

	for(int i = 0; i < rec->bins.size; i++) {
		int c;
		char * str = rec->bins.entries[i].name;
		while ( (c = *str++) ) {
			hash += c + (hash << 6) + (hash << 16) - hash;
		}
		if ( rec->bins.entries[i].valuep != NULL ) {
			hash += as_val_hashcode(rec->bins.entries[i].valuep);
		}
	}

	return hash;
}


static as_val * as_record_rec_get(const as_rec * r, const char * name) 
{
	return r && name ? (as_val *) as_record_get((as_record *) r, name) : NULL;
}

static int as_record_rec_set(const as_rec * r, const char * name, const as_val * value) 
{
	return r && name ? as_record_set((as_record *) r, name, (as_bin_value *)value) : 1;
}

static int as_record_rec_remove(const as_rec * r, const char * name) 
{
	return r && name ? as_record_set_nil((as_record *) r, name) : 1;
}

static uint32_t as_record_rec_ttl(const as_rec * r) 
{
	return r ? ((as_record *) r)->ttl : 0;
}

static uint16_t  as_record_rec_gen(const as_rec * r) 
{
	return r ? ((as_record *) r)->gen : 0;
}

static as_bytes * as_record_rec_digest(const as_rec * r) 
{
	return r ? as_bytes_new_wrap(((as_record *) r)->key.digest.value, AS_DIGEST_VALUE_SIZE, false) : NULL;
}

static uint16_t as_record_rec_numbins(const as_rec * r) 
{
	return r ? as_record_numbins((as_record *) r) : 0;
}

static bool as_record_rec_foreach(const as_rec * r, as_rec_foreach_callback callback, void * udata) 
{
	return r ? as_record_foreach((as_record *) r, callback, udata) : 0;
}

/******************************************************************************
 *	HOOKS
 ******************************************************************************/

const as_rec_hooks as_record_rec_hooks = {

	/***************************************************************************
	 *	instance hooks
	 **************************************************************************/

	.destroy	= as_record_rec_destroy,

	/***************************************************************************
	 *	info hooks
	 **************************************************************************/

	.hashcode	= as_record_rec_hashcode,

	/***************************************************************************
	 *	accessor and modifier hooks
	 **************************************************************************/

	.get		= as_record_rec_get,
	.set		= as_record_rec_set,
	.remove		= as_record_rec_remove,
	.ttl		= as_record_rec_ttl,
	.gen		= as_record_rec_gen,
	.numbins	= as_record_rec_numbins,
	.digest		= as_record_rec_digest,
	.foreach 	= as_record_rec_foreach
};
