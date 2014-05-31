/******************************************************************************
 * Copyright 2008-2014 by Aerospike.
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
#include <aerospike/as_info.h>
#include <citrusleaf/cf_types.h>

void
as_info_parse_multi_response(char* buf, as_vector* /* <as_name_value> */ values)
{
	// Info buffer format: name1\tvalue1\nname2\tvalue2\n...
	char* p = buf;
	char* begin = p;
	
	as_name_value nv;

	while (*p) {
		if (*p == '\t') {
			// Found end of name. Null terminate it.
			*p = 0;
			nv.name = begin;
			begin = ++p;
			
			// Parse value.
			while (*p) {
				if (*p == '\n') {
					*p = 0;
					break;
				}
				p++;
			}
			nv.value = begin;
			as_vector_append(values, &nv);
			begin = ++p;
		}
		else if (*p == '\n') {
			// Found new line before tab.
			*p = 0;
			
			if (p > begin) {
				// Name returned without value.
				nv.name = begin;
				nv.value = p;
				as_vector_append(values, &nv);
			}
			begin = ++p;
		}
		else {
			p++;
		}
	}
	
	if (p > begin) {
		// Name returned without value.
		nv.name = begin;
		nv.value = p;
		as_vector_append(values, &nv);
	}
}
