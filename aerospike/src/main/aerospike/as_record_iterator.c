/******************************************************************************
 *	Copyright 2008-2013 by Aerospike.
 *
 *	Permission is hereby granted, free of charge, to any person obtaining a copy 
 *	of this software and associated documentation files (the "Software"), to 
 *	deal in the Software without restriction, including without limitation the 
 *	rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 *	sell copies of the Software, and to permit persons to whom the Software is 
 *	furnished to do so, subject to the following conditions:
 *	
 *	The above copyright notice and this permission notice shall be included in 
 *	all copies or substantial portions of the Software.
 *	
 *	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
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
#include <aerospike/as_rec.h>
#include <aerospike/as_record.h>
#include <aerospike/as_record_iterator.h>
#include <aerospike/as_string.h>
#include <aerospike/as_util.h>
#include <aerospike/as_val.h>

#include <stdint.h>

/******************************************************************************
 *	STATIC FUNCTIONS
 *****************************************************************************/

static inline as_record_iterator * as_record_iterator_cons(as_record_iterator * iterator, as_record * record)
{
	if ( !iterator && !record ) return NULL;
	iterator->record = record;
	iterator->pos = 0;
	return iterator;
}

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/**
 *	Create and initialize a heap allocated as_record_iterator for the 
 *	specified record.
 *
 *	~~~~~~~~~~{.c}
 *	as_record_iterator * it = as_record_iterator_new(rec);
 *
 *	while ( as_record_iterator_has_next(&it) ) {
 *		as_bin * bin = as_record_iterator_next(&it);
 *	}
 *
 *	as_record_iterator_destroy(&it);
 *	~~~~~~~~~~
 *	
 *	@param record 	The record to iterate over.
 *
 *	@return On success, a new as_record_iterator. Otherwsie an error occurred.
 *
 *	@relates as_record_iterator
 */
as_record_iterator * as_record_iterator_new(as_record * record)
{
	as_record_iterator * iterator = (as_record_iterator *) malloc(sizeof(as_record_iterator));
	return as_record_iterator_cons(iterator, record);
}

/**
 *	Initializes a stack allocated as_record_iterator for the specified record.
 *
 *	~~~~~~~~~~{.c}
 *	as_record_iterator it;
 *	as_record_iterator_init(&it, rec);
 *	
 *	while ( as_record_iterator_has_next(&it) ) {
 *		as_bin * bin = as_record_iterator_next(&it);
 *	}
 *
 *	as_record_iterator_destroy(&it);
 *	~~~~~~~~~~
 *
 *	When you are finished using the `as_record` instance, you should release the 
 *	resources allocated to it by calling `as_record_destroy()`.
 *
 *	@param iterator		The iterator to initialize.
 *	@param record		The record to iterate over.
 *
 *	@return On success, a new as_record_iterator. Otherwsie an error occurred.
 *
 *	@relates as_record_iterator
 */
as_record_iterator * as_record_iterator_init(as_record_iterator * iterator, as_record * record)
{
	return as_record_iterator_cons(iterator, record);
}

/**
 *	Destroy the as_record_iterator and associated resources.
 *
 *	@param iterator The iterator to destroy.
 *
 *	@relates as_record_iterator
 */
void as_record_iterator_destroy(as_record_iterator * iterator)
{
	iterator->record = NULL;
	iterator->pos = 0;
}

/**
 *	Test if there are more bins in the iterator.
 *
 *	@param iterator The iterator to test.
 *
 *	@return the number of bins in the record.
 *
 *	@relates as_record_iterator
 */
bool as_record_iterator_has_next(const as_record_iterator * iterator)
{
	return iterator && iterator->record && iterator->record->bins.size > iterator->pos ? true : false;
}

/**
 *	Read the next bin from the iterator. 
 *
 *	@param iterator		The iterator to read from.
 *
 *	@return On success, the next bin from the iterator. Otherwise, NULL if no more bins are available.
 *
 *	@relates as_record_iterator
 */
as_bin * as_record_iterator_next(as_record_iterator * iterator)
{
	return iterator && 
		iterator->record && 
		iterator->record->bins.size > iterator->pos ? 
		&iterator->record->bins.entries[iterator->pos++] : NULL;
}


