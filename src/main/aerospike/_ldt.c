/*
 * Copyright 2008-2016 Aerospike, Inc.
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
#include <stdlib.h>
#include "_ldt.h"

as_status ldt_parse_error(as_error *error)
{
	int delim_pos = 4;
	if ( error->message[0] && error->message[delim_pos]==':' ) {
		long code = strtol(error->message, NULL, 10);
		if (code > 0) {
			error->code = (as_status)code;
			memmove(error->message, &error->message[delim_pos], strlen(error->message)+1);
		}
	}
	return error->code;
}
