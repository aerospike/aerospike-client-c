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

#ifdef __cplusplus
extern "C" {
#endif


#include <openssl/sha.h>

#include <aerospike/as_cluster.h>
#include <citrusleaf/cf_crypto.h>

#include <aerospike/as_result.h>
#include <aerospike/as_types.h>

/******************************************************************************
 *	TYPES
 ******************************************************************************/

typedef void * (* cl_pair_parser_callback)(char * key, char * value, void * context);

typedef void * (* cl_seq_parser_callback)(char * value, void * context);

typedef struct {
	char delim;
	void * context;
	cl_seq_parser_callback callback;
} cl_seq_parser;

typedef struct {
	char delim;
	void * context;
	cl_pair_parser_callback callback;
} cl_pair_parser;

typedef struct {
	char pair_delim;
	char seq_delim;
	void * context;
	cl_pair_parser_callback callback;
} cl_pairseq_parser;

typedef struct {
	char delim;
	void * context;
	cl_pair_parser_callback callback;
} cl_parameters_parser;

/******************************************************************************
 *	FUNCTIONS
 ******************************************************************************/

int cl_seq_parse(char * seq, cl_seq_parser * parser);

int cl_pair_parse(char * pair, cl_pair_parser * parser);

int cl_pairseq_parse(char * pairseq, cl_pairseq_parser * parser);

int cl_parameters_parse(char * parameters, cl_parameters_parser * parser);

#ifdef __cplusplus
} // end extern "C"
#endif
