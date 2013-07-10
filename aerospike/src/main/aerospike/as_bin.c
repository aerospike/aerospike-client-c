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
#include <aerospike/as_integer.h>
#include <aerospike/as_string.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_list.h>
#include <aerospike/as_map.h>
#include <aerospike/as_val.h>

/******************************************************************************
 *	INLINE FUNCTIONS
 *****************************************************************************/

/**
 *	Get the name of the bin.
 *
 *	~~~~~~~~~~{.c}
 *	char * name = as_bin_get_name(bin);
 *	~~~~~~~~~~
 *
 *
 *	@parameter bin 	The bin to get the name of.
 *
 *	@return The name of the bin.
 *
 *	@relates as_bin
 *	@ingroup as_record_t
 */
extern inline char * as_bin_get_name(as_bin * bin);

/**
 *	Get the value of the bin.
 *
 *	~~~~~~~~~~{.c}
 *	as_bin_value * val = as_bin_get_value(bin);
 *	~~~~~~~~~~
 *
 *
 *	@parameter bin 	The bin to get the value of.
 *
 *	@return The value of the bin. If NULL is returned, then the bin did not contain a value.
 *
 *	@relates as_bin
 *	@ingroup as_record_t
 */
extern inline as_bin_value * as_bin_get_value(as_bin * bin);

/**
 *	Get the type for the value of the bin.
 *
 *	~~~~~~~~~~{.c}
 *	as_val_t type = as_bin_get_type(bin);
 *	~~~~~~~~~~
 *
 *
 *	@parameter bin 	The bin inquire.
 *
 *	@return The type of the bin's value. If AS_VAL_UNDEF is returned, then the bin did not contain a value.
 *
 *	@relates as_bin
 *	@ingroup as_record_t
 */
extern inline as_val_t as_bin_get_type(as_bin * bin);

