/*
 * Copyright 2008-2014 Aerospike, Inc.
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
