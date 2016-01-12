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
#include <aerospike/as_rec.h>
#include <aerospike/as_record.h>
#include <aerospike/as_record_iterator.h>
#include <aerospike/as_string.h>
#include <aerospike/as_util.h>
#include <aerospike/as_val.h>

#include <citrusleaf/alloc.h>

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/******************************************************************************
 *	STATIC FUNCTIONS
 *****************************************************************************/

static inline as_record_iterator * as_record_iterator_cons(as_record_iterator * iterator, const as_record * record, bool free)
{
	if ( ! ( iterator && record ) ) return NULL;
	iterator->_free = free;
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
as_record_iterator * as_record_iterator_new(const as_record * record)
{
	as_record_iterator * iterator = (as_record_iterator *) cf_malloc(sizeof(as_record_iterator));
	return as_record_iterator_cons(iterator, record, true);
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
as_record_iterator * as_record_iterator_init(as_record_iterator * iterator, const as_record * record)
{
	return as_record_iterator_cons(iterator, record, false);
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

	if ( iterator->_free ) {
		cf_free(iterator);
	}
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


