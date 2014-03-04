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
