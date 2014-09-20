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
#include <citrusleaf/cl_parsers.h>

/******************************************************************************
 * MACROS
 ******************************************************************************/

#define LOG(msg, ...) \
	// { printf("%s@%s:%d - ", __func__, __FILE__, __LINE__); printf(msg, ##__VA_ARGS__ ); printf("\n"); }

/******************************************************************************
 *	FUNCTIONS
 ******************************************************************************/

/**
 *	Apply the function to each (key,value) pair. 
 *	The (key,value) pair is delimited by "key=value;"
 */
int cl_pair_parse(char * pair, cl_pair_parser * parser)
{
	if ( !pair || !(*pair) ) return 0;
	LOG("pair = %s", pair);

	char *  ks	= NULL;
	int     ke	= 0;
	char *	k	= NULL;
	char *  vs	= NULL;
	int     ve	= 0;
	char *  v	= NULL;
	char *	p	= NULL;
	
	ks = (char *) pair;
	for ( ke = 0; ks[ke] != parser->delim && ks[ke] != '\0'; ke++);

	if ( ks[ke] == '\0' ) {
		return 1;
	}

	vs = ks + ke + 1;
	ve = (int)strlen(vs);

	k = ks;
	k[ke] = '\0';

	v = vs;
	v[ve] = '\0';

	p = vs + ve + 1;

	LOG("key = %s", k);
	LOG("value = %s", v);

	parser->context = parser->callback(k, v, parser->context);
	return 0;
}

/**
 * Apply the function to each value in a sequence, delimited `delim`.
 */
int cl_seq_parse(char * seq, cl_seq_parser * parser)
{
	if ( !seq || !(*seq) ) return 0;
	LOG("seq = %s", seq);
	LOG("delim = %c", parser->delim);

	char *	vs	= NULL;
	int		ve	= 0;
	char *	v	= NULL;
	char *	p	= NULL;    
	
	vs = (char *) seq;
	for ( ve = 0; vs[ve] != parser->delim && vs[ve] != '\0'; ve++);

	// Move p only if this isn't the last string
	if ( vs[ve] != '\0' ) {
		p = vs + ve + 1;
	}
	
	v = vs;
	v[ve] = '\0';
	
	if ( v[0] != '\0' ) {
		LOG("value = %s", v);
		LOG("p = %s", p);
		LOG("callback %p %p",parser->callback, parser->context);
		parser->context = parser->callback(v, parser->context);
		return cl_seq_parse(p, parser);
	}

	return 0;
}

static void * cl_pairseq_pair_parse(char * pair, void * context)
{
	// LOG("pair = %s", pair);
	cl_pair_parse(pair, (cl_pair_parser *) context);
	return context;
}

int cl_pairseq_parse(char * pairseq, cl_pairseq_parser * parser)
{
	// LOG("pairseq = %s", pairseq);

	cl_pair_parser pair_parser = {
		.delim = parser->pair_delim,
		.context = parser->context,
		.callback = parser->callback
	};

	cl_seq_parser seq_parser = {
		.delim = parser->seq_delim,
		.context = &pair_parser,
		.callback = cl_pairseq_pair_parse
	};

	return cl_seq_parse(pairseq, &seq_parser);
}

int cl_parameters_parse(char * parameters, cl_parameters_parser * parser)
{
	// LOG("parameters = %s", parameters);
	
	cl_pairseq_parser pairseq_parser = {
		.pair_delim = '=',
		.seq_delim = parser->delim,
		.context = parser->context,
		.callback = parser->callback,
	};
	return cl_pairseq_parse(parameters, &pairseq_parser);
}
