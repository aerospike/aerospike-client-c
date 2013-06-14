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
	memcpy(&digest->key, key, sizeof(as_key));
	as_digest_compute(digest);
	return digest;
}

/**
 * Initializes a digest.
 */
as_digest * as_digest_init(as_digest * digest, const char * set, const char * key) 
{
	if ( !digest ) return digest;
	as_key k = {
		.type = key ? AS_TYPE_STR : AS_TYPE_NULL,
		.value.str = key ? strdup(key) : NULL
	};
	return as_digest_defaults(digest, false, set, &k);
}

/**
 * Initializes a digest.
 */
as_digest * as_digest_init2(as_digest * digest, const char * set, int64_t key) 
{
	if ( !digest ) return digest;
	as_key k = {
		.type = AS_TYPE_INT,
		.value.int64 = key
	};
	return as_digest_defaults(digest, false, set, &k);
}

/**
 * Creates a new digest on the heap.
 */
as_digest * as_digest_new(const char * set, const char * key) 
{
	as_digest * digest = (as_digest *) malloc(sizeof(as_digest));
	if ( !digest ) return digest;
	as_key k = {
		.type = key ? AS_TYPE_STR : AS_TYPE_NULL,
		.value.str = key ? strdup(key) : NULL
	};
	return as_digest_defaults(digest, false, set, &k);
}

/**
 * Creates a new digest on the heap.
 */
as_digest * as_digest_new2(const char * set, int64_t key) 
{
	as_digest * digest = (as_digest *) malloc(sizeof(as_digest));
	if ( !digest ) return digest;
	as_key k = {
		.type = AS_TYPE_INT,
		.value.int64 = key
	};
	return as_digest_defaults(digest, false, set, &k);
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
		if ( digest->key.type == AS_TYPE_STR ) {
			free(digest->key.value.str);
			digest->key.value.str = NULL;
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
	switch ( digest->key.type ) {
		case AS_TYPE_INT:
			cf_digest_compute2(digest->set, digest->set ? strlen(digest->set) : 0, &digest->key.value.int64, sizeof(int64_t), (cf_digest *) digest->value);
			break;
		case AS_TYPE_STR:
			cf_digest_compute2(digest->set, digest->set ? strlen(digest->set) : 0, digest->key.value.str, strlen(digest->key.value.str), (cf_digest *) digest->value);
			break;
		default:
			cf_digest_compute2(digest->set, digest->set ? strlen(digest->set) : 0, NULL, 0, (cf_digest *) digest->value);
			break;
	}

}

