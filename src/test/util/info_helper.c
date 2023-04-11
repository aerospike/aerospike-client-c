/*
 * Copyright 2008-2022 Aerospike, Inc.
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
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_info.h>
#include <aerospike/as_info.h>
#include "../test.h"

extern aerospike * as;

void get_info_field(const char *input, const char *field, char *output, uint32_t out_len)
{
	as_error err;
	as_error_reset(&err);

	char* response = NULL;
	as_status status = aerospike_info_any(as, &err, NULL, input, &response);

	if (status != AEROSPIKE_OK) {
        error("aerospike_info_any() error: (%d) %s @ %s[%s:%d]",
			err.code, err.message, err.func, err.file, err.line);
        return;
	}

	if (!response) {
        error("no response returned");
        return;
	}

	char* begin = NULL;
	status = as_info_parse_single_response(response, &begin);

	if (status != AEROSPIKE_OK) {
        error("as_info_parse_single_response() error: %d", err.code);
        return;
	}

	// Response format: name1=value1;name2=value2;...
	char* p = begin;

	as_name_value nv;

	while (*p) {
		if (*p == '=') {
			// Found end of name. Null terminate it.
			*p = 0;
			nv.name = begin;
			begin = ++p;

			// Parse value.
			while (*p) {
				if (*p == ';') {
					*p = 0;
					break;
				}
				p++;
			}
			nv.value = begin;
			begin = ++p;

			// found my field
			if ( strcmp(nv.name,field)==0 ) {
				strncpy(output,nv.value,out_len);
				goto Done;
			}
		}
		else if (*p == ';') {
			// Found new nv pair.
			*p = 0;

			if (p > begin) {
				// Name returned without value.
				nv.name = begin;
				nv.value = p;
			}
			begin = ++p;
		}
		else {
			p++;
		}
	}

	if (p > begin) {
		// Name returned without value.
		*output=0;
	}

Done:
	if (response) {
		free(response);
	}
}
