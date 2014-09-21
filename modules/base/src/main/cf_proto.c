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
#include <stdint.h>
#include <string.h>

#include <citrusleaf/cf_byte_order.h>
#include <citrusleaf/cf_proto.h>

// Byte swap proto from current machine byte order to network byte order (big endian).
void
cl_proto_swap_to_be(cl_proto *p)
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
cl_proto_swap_from_be(cl_proto *p)
{
	uint8_t	 version = p->version;
	uint8_t  type = p->type;
	p->version = p->type = 0;
	p->sz = cf_swap_from_be64(*(uint64_t*)p);
	p->version = version;
	p->type = type;
}

// Byte swap header from current machine byte order to network byte order (big endian).
void
cl_msg_swap_header_to_be(cl_msg *m)
{
	m->generation = cf_swap_to_be32(m->generation);
	m->record_ttl =  cf_swap_to_be32(m->record_ttl);
	m->transaction_ttl = cf_swap_to_be32(m->transaction_ttl);
	m->n_fields = cf_swap_to_be16(m->n_fields);
	m->n_ops= cf_swap_to_be16(m->n_ops);
}

// Byte swap header from network byte order (big endian) to current machine byte order.
void
cl_msg_swap_header_from_be(cl_msg *m)
{
	m->generation = cf_swap_from_be32(m->generation);
	m->record_ttl =  cf_swap_from_be32(m->record_ttl);
	m->transaction_ttl = cf_swap_from_be32(m->transaction_ttl);
	m->n_fields = cf_swap_from_be16(m->n_fields);
	m->n_ops= cf_swap_from_be16(m->n_ops);
}

// Byte swap operation from current machine byte order to network byte order (big endian).
void
cl_msg_swap_op_to_be(cl_msg_op *op)
{
	op->op_sz = cf_swap_to_be32(op->op_sz);
}

// Byte swap operation from network byte order (big endian) to current machine byte order.
void
cl_msg_swap_op_from_be(cl_msg_op *op)
{
	op->op_sz = cf_swap_from_be32(op->op_sz);
}

// Byte swap field from current machine byte order to network byte order (big endian).
void
cl_msg_swap_field_to_be(cl_msg_field *mf)
{
	mf->field_sz = cf_swap_to_be32(mf->field_sz);
}

// Byte swap field from network byte order (big endian) to current machine byte order.
void
cl_msg_swap_field_from_be(cl_msg_field *mf)
{
	mf->field_sz = cf_swap_from_be32(mf->field_sz);
}
