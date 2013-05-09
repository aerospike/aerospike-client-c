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
#pragma once

#include <citrusleaf/cl_types.h>
#include <citrusleaf/cl_cluster.h>

/******************************************************************************
 * TYPES
 ******************************************************************************/



/******************************************************************************
 * INLINE FUNCTIONS
 ******************************************************************************/




/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/




/**
 * Get many digest
 * This version of the call acts exactly as the get digest call, but takes an array
 * of digests. Those digests will be retrieved with the same API conventions as the
 * previous ones.
 * To come: an array of keys, but it'll just be a wrapper on this.
 */
cl_rv citrusleaf_get_many_digest(
    cl_cluster *asc, char *ns, const cf_digest *digests, int n_digests, 
    cl_bin *bins, int n_bins, bool get_key /*if true, retrieve key instead of simply digest*/, 
    citrusleaf_get_many_cb cb, void *udata
    );

/**
 * Get many digest without a callback
 * This version of the batch-get call does not need the callback function. It will return an array of records. 
 * The results are returned in an array. No ordering is guaranteed between the input digest array and 
 * the returned rows. If the corresponding records for the digests are not found in the cluster, there wont 
 * be any corresponding entry in the result array indicating that the records are missing. The caller must 
 * call the citrusleaf_free_batchresult() to free the memory allocated during this operation.
 */
cl_rv citrusleaf_get_many_digest_direct(cl_cluster *asc, char *ns, const cf_digest *digests, int n_digests, cl_batchresult **br);

/**
 * Utility function to free the memory allocated by the citrusleaf_get_many_digest_direct() function
 */
void citrusleaf_free_batchresult(cl_batchresult *br);

/**
 * Key exists many digest
 * This version of the call acts exactly as the exists digest call, but takes an array
 * of digests. Those digests will be retrieved with the same API conventions as the
 * previous ones.
 * To come: an array of keys, but it'll just be a wrapper on this.
 */
cl_rv citrusleaf_exists_many_digest(
    cl_cluster *asc, char *ns, const cf_digest *digests, int n_digests, 
    cl_bin *bins, int n_bins, bool get_key /*if true, retrieve key instead of simply digest*/, 
    citrusleaf_get_many_cb cb, void *udata
    );
