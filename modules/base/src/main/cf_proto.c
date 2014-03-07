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
