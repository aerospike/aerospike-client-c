/*
 * Copyright 2008-2018 Aerospike, Inc.
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
#include <aerospike/as_host.h>
#include <stdlib.h>

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

static bool
as_host_parse(char** pp, as_host* host)
{
	// Format: address1:port1
	// Destructive parse. String is modified.
	// IPV6 addresses can start with bracket.
	char* p = *pp;

	if (*p == '[') {
		host->name = ++p;

		while (*p) {
			if (*p == ']') {
				*p++ = 0;

				if (*p == ':') {
					p++;
					host->port = (uint16_t)strtol(p, &p, 10);
					*pp = p;
					return true;
				}
				else {
					break;
				}
			}
			p++;
		}
	}
	else {
		host->name = p;

		while (*p) {
			if (*p == ':') {
				*p++ = 0;
				host->port = (uint16_t)strtol(p, &p, 10);
				*pp = p;
				return true;
			}
			p++;
		}
	}
	host->port = 0;
	return false;
}

bool
as_host_parse_addresses(char* p, as_vector* hosts)
{
	// Format: address1:port1,...
	// Destructive parse. String is modified.
	if (*p == 0) {
		// At least one host is required.
		return false;
	}

	as_host host;

	while (as_host_parse(&p, &host)) {
		as_vector_append(hosts, &host);

		if (*p == 0) {
			return true;
		}

		if (*p != ',') {
			return false;
		}
		p++;
	}
	return false;
}
