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

#include <citrusleaf/cf_byte_order.h>
#include <citrusleaf/cf_proto.h>


void
cl_proto_swap(cl_proto *p)
{
	uint8_t	 version = p->version;
	uint8_t  type = p->type;
	p->version = p->type = 0;
	p->sz = cf_byteswap64p(p);
	p->version = version;
	p->type = type;
}

#if 0 // if you don't have that nice linux swap
void
cl_proto_swap_header(cl_proto *p)
{

	uint8_t *buf = (uint8_t *)p;
	uint8_t _t;
	_t = buf[2]; buf[2] = buf[7]; buf[7] = _t;
	_t = buf[3]; buf[3] = buf[6]; buf[6] = _t;
	_t = buf[4]; buf[4] = buf[5]; buf[5] = _t;
}
#endif


void
cl_msg_swap_header(cl_msg *m)
{
	m->generation = ntohl(m->generation);
	m->record_ttl =  ntohl(m->record_ttl);
	m->transaction_ttl = ntohl(m->transaction_ttl);
	m->n_fields = ntohs(m->n_fields);
	m->n_ops= ntohs(m->n_ops);
}


void
cl_msg_swap_op(cl_msg_op *op)
{
	op->op_sz = ntohl(op->op_sz);
}

// fields better be swapped before you call this

void
cl_msg_swap_ops(cl_msg *m)
{
	cl_msg_op *op = 0;
	int *n = 0; // actually not necessary

	while ((op = cl_msg_op_iterate(m,op,n))) {
		cl_msg_swap_op(op);
	}
}

void
cl_msg_swap_field(cl_msg_field *mf)
{
	mf->field_sz = ntohl(mf->field_sz);
}

// swaps all the fields but nothing else

void
cl_msg_swap_fields(cl_msg *m)
{
	cl_msg_field *mf = (cl_msg_field *) m->data;

	for (int i=0;i<m->n_fields;i++) {
		cl_msg_swap_field(mf);
		mf = cl_msg_field_get_next(mf);
	}
}

void
cl_msg_swap_fields_and_ops(cl_msg *m)
{
	cl_msg_field *mf = (cl_msg_field *) m->data;

	for (int i=0;i<m->n_fields;i++) {
		cl_msg_swap_field(mf);
		mf = cl_msg_field_get_next(mf);
	}

	cl_msg_op *op = (cl_msg_op *)mf;
	for (int i=0;i<m->n_ops;i++) {
		cl_msg_swap_op(op);
		op = cl_msg_op_get_next(op);
	}
}
