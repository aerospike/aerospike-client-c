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
