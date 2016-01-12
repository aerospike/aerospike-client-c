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
#include <aerospike/as_proto.h>
#include <citrusleaf/cf_byte_order.h>
#include <stdint.h>
#include <string.h>

// Byte swap proto from current machine byte order to network byte order (big endian).
void
as_proto_swap_to_be(as_proto *p)
{
	uint8_t	 version = p->version;
	uint8_t  type = p->type;
	p->version = p->type = 0;
	p->sz = cf_swap_to_be64(*(uint64_t*)p);
	p->version = version;
	p->type = type;
}

// Byte swap proto from network byte order (big endian) to current machine byte order.
void
as_proto_swap_from_be(as_proto *p)
{
	uint8_t	 version = p->version;
	uint8_t  type = p->type;
	p->version = p->type = 0;
	p->sz = cf_swap_from_be64(*(uint64_t*)p);
	p->version = version;
	p->type = type;
}

// Byte swap header from network byte order (big endian) to current machine byte order.
void
as_msg_swap_header_from_be(as_msg *m)
{
	m->generation = cf_swap_from_be32(m->generation);
	m->record_ttl =  cf_swap_from_be32(m->record_ttl);
	m->transaction_ttl = cf_swap_from_be32(m->transaction_ttl);
	m->n_fields = cf_swap_from_be16(m->n_fields);
	m->n_ops= cf_swap_from_be16(m->n_ops);
}
