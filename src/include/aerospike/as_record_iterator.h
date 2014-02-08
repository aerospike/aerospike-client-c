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

#pragma once 

#include <aerospike/as_bin.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_key.h>
#include <aerospike/as_list.h>
#include <aerospike/as_map.h>
#include <aerospike/as_rec.h>
#include <aerospike/as_record.h>
#include <aerospike/as_string.h>
#include <aerospike/as_util.h>
#include <aerospike/as_val.h>

#include <stdbool.h>
#include <stdint.h>

/******************************************************************************
 *	TYPES
 *****************************************************************************/

/**
 *	Iterator over bins of a record.
 *
 *	## Initialization
 *
 *	The as_record_iterator can be initialized via:
 *
 *	- as_record_iterator_init() — initializes a stack allocated 
 *		as_record_iterator.
 *	- as_record_iterator_new() — allocated and initializes an 
 *		as_record_iterator on the heap.
 *
 *	Both of the function require the record on which it will iterate.
 *
 *	To initialize an as_record_iterator on the stack:
 *
 *	~~~~~~~~~~{.c}
 *	as_record_iterator it;
 *	as_record_iterator_init(&it, record);
 *	~~~~~~~~~~
 *
 *	To initialize an as_record_iterator on the heap:
 *
 *	~~~~~~~~~~{.c}
 *	as_record_iterator * it as_record_iterator_new(record);
 *	~~~~~~~~~~
 *
 *	## Destruction 
 *
 *	When you no longer require the iterator, you should release it and 
 *	associated resource via as_record_iterator_destroy():
 *
 *	~~~~~~~~~~{.c}
 *	as_record_iterator_destroy(it);
 *	~~~~~~~~~~
 *
 *	## Usage
 *
 *	With an initialized as_record_iterator, you can traverse the bins of
 *	a record. 
 *
 *	Traversal is usually performed by first checking to see if 
 *	the there are any bins available to traverse to via 
 *	as_record_iterator_has_next(), which returns true if there are more bins,
 *	or false if there are no more bins. 
 *
 *	~~~~~~~~~~{.c}
 *	as_record_iterator_has_next(&it);
 *	~~~~~~~~~~
 *
 *	When you are sure there are more bins, then you will use 
 *	as_record_iterator_next() to read the next bin. If there are no bins 
 *	available, then NULL is returned.
 *
 *	~~~~~~~~~~{.c}
 *	as_bin * bin = as_record_iterator_next(&it);
 *	~~~~~~~~~~
 *
 *	If  as_record_iterator_next() returns a bin, then you can use the following
 *	functions to get information about the bin:
 *
 *	- as_bin_get_name() — Get the bin's name.
 *	- as_bin_get_value() — Get the bin's value.
 *	- as_bin_get_type() — Get the bin's values' types.
 *
 *	Most often, a traversal is performed in a while loop. The following is a 
 *	simple example:
 *
 *	~~~~~~~~~~{.c}
 *	while ( as_record_iterator_has_next(&it) ) {
 *		as_bin * bin = as_record_iterator_next(&it);
 *		char *   name = as_bin_get_name(bin);
 *		as_val * value = (as_val *) as_bin_get_value(bin);
 *	}
 *	~~~~~~~~~~
 *
 *	@ingroup as_record_object
 */
typedef struct as_record_iterator_s {

	/**
	 *	@private
	 *	If true, then as_record_iterator_destroy() will free this object.
	 */
	bool _free;

	/**
	 *	The record being iterated over.
	 */
	const as_record * record;

	/**
	 *	Current position of the iterator
	 */
	uint32_t pos;

} as_record_iterator;

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
 *	@return On success, a new as_record_iterator. Otherwise an error occurred.
 *
 *	@relates as_record_iterator
 *	@ingroup as_record_object
 */
as_record_iterator * as_record_iterator_new(const as_record * record);

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
 *	@param record		The record to iterate over
 *
 *	@return On success, a new as_record_iterator. Otherwise an error occurred.
 *
 *	@relates as_record_iterator
 *	@ingroup as_record_object
 */
as_record_iterator * as_record_iterator_init(as_record_iterator * iterator, const as_record * record);

/**
 *	Destroy the as_record_iterator and associated resources.
 *
 *	@param iterator The iterator to destroy.
 *
 *	@relates as_record_iterator
 *	@ingroup as_record_object
 */
void as_record_iterator_destroy(as_record_iterator * iterator);

/**
 *	Test if there are more bins in the iterator.
 *
 *	@param iterator The iterator to test.
 *
 *	@return the number of bins in the record.
 *
 *	@relates as_record_iterator
 *	@ingroup as_record_object
 */
bool as_record_iterator_has_next(const as_record_iterator * iterator);

/**
 *	Read the next bin from the iterator. 
 *
 *	@param iterator		The iterator to read from.
 *
 *	@return The next bin from the iterator.
 *
 *	@relates as_record_iterator
 *	@ingroup as_record_object
 */
as_bin * as_record_iterator_next(as_record_iterator * iterator);


