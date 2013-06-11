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

/******************************************************************************
 * TYPES
 *****************************************************************************/

enum as_status_e {
	AEROSPIKE_OK						= 0x000,    // SUCCESS
	AEROSPIKE_ERR						= 0x100,    // GENERIC ERROR
	AEROSPIKE_ERR_CLIENT				= 0x200,    // GENERIC CLIENT ERROR
	AEROSPIKE_ERR_CLUSTER				= 0x300,    // GENERIC CLUSTER ERROR
	AEROSPIKE_ERR_WRITE					= 0x400,    // GENERIC WRITE ERROR
	AEROSPIKE_ERR_READ					= 0x400,    // GENERIC READ ERROR
	AEROSPIKE_ERR_SCAN					= 0x500,    // GENERIC SCAN ERROR
	AEROSPIKE_ERR_QUERY					= 0x600,    // GENERIC QUERY ERROR
    AEROSPIKE_ERR_QUERY_ABORTED 		= 0x601,
    AEROSPIKE_ERR_QUERY_QUEUEFULL 		= 0x602,
	AEROSPIKE_ERR_INDEX     			= 0x700,    // GENERIC INDEXING ERROR
    AEROSPIKE_ERR_INDEX_KEY_NOTFOUND 	= 0x701,
    AEROSPIKE_ERR_INDEX_TYPE_MISMATCH 	= 0x702,
    AEROSPIKE_ERR_INDEX_NOTFOUND 		= 0x702,
    AEROSPIKE_ERR_INDEX_OOM 			= 0x702,
    AEROSPIKE_ERR_INDEX_GENERIC 		= 0x702,
    AEROSPIKE_ERR_INDEX_EXISTS 			= 0x702,
    AEROSPIKE_ERR_INDEX_SINGLEBIN_NS 	= 0x702,
    AEROSPIKE_ERR_INDEX_UNKNOWN_TYPE 	= 0x702,
    AEROSPIKE_ERR_INDEX_FOUND 			= 0x702,
    AEROSPIKE_ERR_INDEX_NOTREADABLE 	= 0x702,
	AEROSPIKE_ERR_UDF       			= 0x800,    // GENERIC UDF ERROR

};

typedef enum as_status_e as_status;


