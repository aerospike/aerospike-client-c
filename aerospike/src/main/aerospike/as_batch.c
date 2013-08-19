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

#include <aerospike/as_batch.h>
#include <citrusleaf/cl_batch.h>
#include <citrusleaf/cf_random.h>

static as_batch * as_batch_defaults(as_batch * batch, bool free, const as_namespace ns, const as_set set)
{
	if(batch == NULL) return batch;
	
	batch->_free = free;
	
	 if ( strlen(ns) < AS_NAMESPACE_MAX_SIZE ) {
                strcpy(batch->ns, ns);
        }
        else {
                batch->ns[0] = '\0';
        }

    //check set==NULL and set name length
	if ( set && strlen(set) < AS_SET_MAX_SIZE ) {
                strcpy(batch->set, set);
        }
        else {
                batch->set[0] = '\0';
        }

        batch->select._free = false;
        batch->select.capacity = 0;
        batch->select.size = 0;
        batch->select.entries = NULL;

        return batch;
}

as_batch * as_batch_new(const as_namespace ns, const as_set set)
{
	as_batch * batch = (as_batch*) malloc(sizeof(as_batch));
	if(!batch) return NULL;
	return as_batch_defaults(batch, true, ns, set);
}

as_batch * as_batch_init(as_batch * batch, const as_namespace ns, as_set set)
{
	if(!batch) return batch;
	return as_batch_defaults(batch, false, ns, set);
}
	

