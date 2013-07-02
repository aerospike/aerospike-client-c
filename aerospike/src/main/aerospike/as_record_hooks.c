/******************************************************************************
 *	Copyright 2008-2013 by Aerospike.
 *	* Permission is hereby granted, free of charge, to any person obtaining a copy 
 *	of this software and associated documentation files (the "Software"), to 
 *	deal in the Software without restriction, including without limitation the 
 *	rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 *	sell copies of the Software, and to permit persons to whom the Software is 
 *	furnished to do so, subject to the following conditions:
 *	* The above copyright notice and this permission notice shall be included in 
 *	all copies or substantial portions of the Software.
 *	* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 *	FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 *	IN THE SOFTWARE.
 *****************************************************************************/

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
	return r && name ? as_record_get((as_record *) r, name) : NULL;
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
	.digest		= as_record_rec_digest
};
