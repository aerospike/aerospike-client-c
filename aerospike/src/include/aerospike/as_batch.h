/******************************************************************************
 *      Copyright 2008-2013 by Aerospike.
 *
 *      Permission is hereby granted, free of charge, to any person obtaining a copy 
 *      of this software and associated documentation files (the "Software"), to 
 *      deal in the Software without restriction, including without limitation the 
 *      rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 *      sell copies of the Software, and to permit persons to whom the Software is 
 *      furnished to do so, subject to the following conditions:
 *      
 *      The above copyright notice and this permission notice shall be included in 
 *      all copies or substantial portions of the Software.
 *      
 *      THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *      IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *      FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *      AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *      LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 *      FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 *      IN THE SOFTWARE.
 *****************************************************************************/
#pragma once 

#include <aerospike/as_bin.h>
#include <aerospike/as_key.h>

/*****************************************************************************
 * STRUCTURES
 *****************************************************************************/

typedef struct as_batch_bins_s {
	
	bool _free;
	
	uint16_t capacity;
		
	uint16_t size;
	
	as_bin_name * entries;

} as_batch_bins;


typedef struct as_batch_s {

	bool _free;
	
	as_namespace ns;
	
	as_set set;

	as_batch_bins select;

}as_batch;

/*********************************************************************************
 * 		INSTANCE FUNCTIONS
 *********************************************************************************/

as_batch * as_batch_init(as_batch * batch, const as_namespace ns, const as_set set);

as_batch * as_batch_new(const as_namespace ns, const as_set set);

void as_batch_destroy(as_batch * batch);


