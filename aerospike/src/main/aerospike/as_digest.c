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

#include <aerospike/as_digest.h>
#include <citrusleaf/cf_digest.h>

#include <stdlib.h>
#include <string.h>

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

static as_digest * as_digest_defaults(as_digest * digest, bool free, const char * set, as_key * key) 
{
	digest->_free = free;
	digest->set = set ? strdup(set) : NULL;
	digest->key = key;
	as_digest_compute(digest);
	return digest;
}

/**
 * Initializes a digest.
 */
as_digest * as_digest_init(as_digest * digest, const char * set, as_key * key) 
{
	if ( !digest ) return digest;
	return as_digest_defaults(digest, false, set, key);
}

/**
 * Creates a new digest on the heap.
 */
as_digest * as_digest_new(const char * set,  as_key * key) 
{
	as_digest * digest = (as_digest *) malloc(sizeof(as_digest));
	if ( !digest ) return digest;
	return as_digest_defaults(digest, false, set, key);
}

/**
 * Destory the digest, releasing resources.
 */
void as_digest_destroy(as_digest * digest) 
{
	if ( digest ) {
		if ( digest->set ) {
			free(digest->set);
			digest->set = NULL;
		}
		if ( digest->key != NULL ) {
			as_val_destroy((as_val *) digest->key);
			digest->key = NULL;
		}
		if ( digest->_free ) {
			free(digest);
		}
	}
}

/**
 * Compute the digest value
 */
void as_digest_compute(as_digest * digest)
{
	as_val * val = (as_val *) digest->key;
	if ( val ) {
		switch ( val->type ) {
			case AS_INTEGER: {
				as_integer * v = (as_integer *) val;
				cf_digest_compute2(digest->set, digest->set ? strlen(digest->set) : 0, (uint8_t *) &v->value, sizeof(int64_t), (cf_digest *) digest->value);
				break;
			}
			case AS_STRING: {
				as_string * v = (as_string *) val;
				cf_digest_compute2(digest->set, digest->set ? strlen(digest->set) : 0, v->value, as_string_len(v), (cf_digest *) digest->value);
				break;
			}
			case AS_BYTES: {
				as_bytes * v = (as_bytes *) val;
				cf_digest_compute2(digest->set, digest->set ? strlen(digest->set) : 0, v->value, v->len, (cf_digest *) digest->value);
				break;
			}
			default:
				cf_digest_compute2(digest->set, digest->set ? strlen(digest->set) : 0, NULL, 0, (cf_digest *) digest->value);
				break;
		}
	}
	else {
		cf_digest_compute2(digest->set, digest->set ? strlen(digest->set) : 0, NULL, 0, (cf_digest *) digest->value);
	}
}

