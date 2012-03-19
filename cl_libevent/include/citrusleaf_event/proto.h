/*
 *  Citrusleaf Aerospike
 *  include/proto.h - wire protocol definition
 *
 *  Copyright 2008-2009 by Citrusleaf.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */
#pragma once
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif


#define CL_PROTO_RESULT_OK 0
#define CL_PROTO_RESULT_FAIL 1 // unknown failure
#define CL_PROTO_RESULT_NOTFOUND 2
#define CL_PROTO_RESULT_FAIL_GENERATION 3
#define CL_PROTO_RESULT_FAIL_PARAMETER 4
#define CL_PROTO_RESULT_FAIL_KEY_EXISTS 5 // if 'WRITE_ADD', could fail because already exists
#define CL_PROTO_RESULT_FAIL_BIN_EXISTS 6


// Forward definitions
struct cl_bin_s;


// NOTE! This is the only place where datamodel.h is exported in the external
// proto.h. Maybe I should make a different (annotated) proto.h?

/* cl_particle_type
 * Particles are typed, which reflects their contents:
 *    NULL: no associated content (not sure I really need this internally?)
 *    INTEGER: a signed, 64-bit integer
 *    BIGNUM: a big number
 *    STRING: a null-terminated UTF-8 string
 *    BLOB: arbitrary-length binary data
 *    TIMESTAMP: milliseconds since 1 January 1970, 00:00:00 GMT
 *    DIGEST: an internal Aerospike key digest */
typedef enum {
    CL_PARTICLE_TYPE_NULL = 0,
    CL_PARTICLE_TYPE_INTEGER = 1,
    CL_PARTICLE_TYPE_FLOAT = 2,
    CL_PARTICLE_TYPE_STRING = 3,
    CL_PARTICLE_TYPE_BLOB = 4,
    CL_PARTICLE_TYPE_TIMESTAMP = 5,
    CL_PARTICLE_TYPE_DIGEST = 6,
    CL_PARTICLE_TYPE_JAVA_BLOB = 7,
	CL_PARTICLE_TYPE_CSHARP_BLOB = 8,
	CL_PARTICLE_TYPE_PYTHON_BLOB = 9,
	CL_PARTICLE_TYPE_RUBY_BLOB = 10,
	CL_PARTICLE_TYPE_MAX = 11
} cl_particle_type;


/* SYNOPSIS
 * Aerospike wire protocol
 *
 * Version 2
 *
 * Aerospike uses a message-oriented wire protocol to transfer information.                           
 * Each message consists of a header, which determines the type and the length
 * to follow. This is called the 'proto_msg'.
 *
 * these messages are vectored out to the correct handler. Over TCP, they can be
 * pipelined (but not out of order). If we wish to support out of order responses,
 * we should upgrade the protocol.
 *
 * the most common type of message is the cl_msg, a message which reads or writes
 * a single row to the data store.
 *
 */

 
#define PROTO_VERSION 2
#define PROTO_TYPE_INFO 1       // ascii-format message for determining server info
#define PROTO_TYPE_CL_MSG 3

typedef struct cl_proto_s {
	uint8_t		version;
	uint8_t		type;
	uint64_t	size:48;
	uint8_t		data[];
} __attribute__ ((__packed__)) cl_proto;


 /* cl_msg_field
 * Aerospike message field */
typedef struct cl_msg_field_s {
#define CL_MSG_FIELD_TYPE_NAMESPACE 0 // UTF8 string
#define CL_MSG_FIELD_TYPE_SET 1
#define CL_MSG_FIELD_TYPE_KEY 2 // contains a key type
#define CL_MSG_FIELD_TYPE_BIN 3    // used for secondary key access - contains a bin, thus a name and value
#define CL_MSG_FIELD_TYPE_DIGEST_RIPE 4    // used to send the digest just computed to the server so it doesn't have to
#define CL_MSG_FIELD_TYPE_GU_TID 5    // used to send the digest just computed to the server so it doesn't have to
	uint32_t field_sz; // get the data size through the accessor function, don't worry, it's a small macro
	uint8_t type;
	uint8_t data[];
} __attribute__((__packed__)) cl_msg_field;


 
#define CL_MSG_OP_READ 1			// read the value in question
#define CL_MSG_OP_WRITE 2			// write the value in question
#define CL_MSG_OP_WRITE_UNIQUE 3  // write a namespace-wide unique value
#define CL_MSG_OP_WRITE_NOW 4     // write the server-current time
#define CL_MSG_OP_ADD 5
 
typedef struct cl_msg_op_s {
	uint32_t op_size;
	uint8_t  op;
	uint8_t  particle_type;
	uint8_t  version;
	uint8_t  name_size;
	uint8_t	 name[]; // UTF-8
	// there's also a value here but you can't have two variable size arrays
} __attribute__((__packed__)) cl_msg_op;
 
static inline uint8_t * cl_msg_op_get_value_p(cl_msg_op *op)
{
	return ( ((uint8_t *)op) + sizeof(cl_msg_op) + op->name_size);
}

static inline uint32_t cl_msg_op_get_value_size(cl_msg_op *op)
{
	return( op->op_size - (4 + op->name_size) );
}

static inline uint32_t cl_msg_field_get_value_size(cl_msg_field *f)
{
	return( f->field_sz - 1 );
}


typedef struct cl_msg_key_s {
	cl_msg_field	f;
	uint8_t  key[];
} __attribute__ ((__packed__)) cl_msg_key;

typedef struct cl_msg_number_s {
	cl_msg_field	f;
	uint32_t number;
} __attribute__ ((__packed__)) cl_msg_number;


#define CITRUSLEAF_RESULT_OK	0
#define CITRUSLEAF_RESULT_FAIL 1
#define CITRUSLEAF_RESULT_NOTFOUND 2

/* cl_ms
 * Aerospike message
 * size: size of the payload, not including the header */
typedef struct cl_msg_s {
/*00*/	uint8_t 	header_sz;    // number of uint8_ts in this header
/*01*/	uint8_t 	info1; 		  // bitfield about this request
/*02*/  uint8_t     info2;
/*03*/  uint8_t     info3;
/*04*/  uint8_t     unused;
/*05*/	uint8_t 	result_code;
/*06*/	uint32_t 	generation;
/*10*/  uint32_t	record_ttl;
/*14*/  uint32_t    transaction_ttl;
/*18*/	uint16_t 	n_fields; // size in uint8_ts
/*20*/	uint16_t 	n_ops;     // number of operations
/*22*/	uint8_t data[]; // data contains first the fields, then the ops
} __attribute__((__packed__)) cl_msg;


/* cl_ms
 * Aerospike message
 * sz: size of the payload, not including the header */
typedef struct as_msg_s {
	    cl_proto  	proto;
		cl_msg		m;
} __attribute__((__packed__)) as_msg;

#define CL_MSG_INFO1_READ				(1 << 0)		// contains a read operation
#define CL_MSG_INFO1_GET_ALL			(1 << 1) 		// get all bins, period
#define CL_MSG_INFO1_GET_ALL_NODATA 	(1 << 2) 		// get all bins WITHOUT data (currently unimplemented)
#define CL_MSG_INFO1_VERIFY     		(1 << 3) 		// verify is a GET transaction that includes data, and assert if the data aint right

#define CL_MSG_INFO2_WRITE				(1 << 0)		// contains a write semantic
#define CL_MSG_INFO2_DELETE 			(1 << 1)  		// fling a record into the belly of Moloch
#define CL_MSG_INFO2_GENERATION			(1 << 2) 		// pay attention to the generation
#define CL_MSG_INFO2_GENERATION_GT		(1 << 3) 		// apply write if new generation >= old, good for restore
#define CL_MSG_INFO2_GENERATION_DUP  	(1 << 4)		// if a generation collision, create a duplicate
#define CL_MSG_INFO2_WRITE_UNIQUE		(1 << 5) 		// write only if it doesn't exist
#define CL_MSG_INFO2_WRITE_BINUNIQUE	(1 << 6)

#define CL_MSG_INFO3_LAST      			(1 << 0)     	// this is the last of a multi-part message
#define CL_MSG_INFO3_TRACE				(1 << 1)		// apply server trace logging for this transaction
#define CL_MSG_INFO3_TOMBSTONE			(1 << 2)		// if set on response, a version was a delete tombstone


static inline cl_msg_field *
cl_msg_field_get_next(cl_msg_field *mf)
{
	return ( (cl_msg_field *) (((uint8_t *)mf) + sizeof(mf->field_sz) + mf->field_sz) );
}

/* cl_msg_field_get
 * Retrieve a specific field from a message */
static inline cl_msg_field *
cl_msg_field_get(cl_msg *msg, uint8_t type)
{
	uint16_t n;
	cl_msg_field *fp = NULL;

	fp = (cl_msg_field *)msg->data;
	for (n = 0; n < msg->n_fields; n++) {

		if (fp->type == type)
			break;

		fp = cl_msg_field_get_next(fp);
	}
	if (n == msg->n_fields)
		return(NULL);
	else
		return(fp);
}

/* cl_msg_field_getnext
 * iterator for all fields of a particular type
 * First time through: pass 0 as current, you'll get a field
 * next time: pass the current as current
 * you'll get null when there are no more
 */
static inline cl_msg_op *
cl_msg_op_get_next(cl_msg_op *op)
{
	return ( (cl_msg_op *) (((uint8_t *) op) + sizeof(op->op_size) + op->op_size ) );
}

 
static inline cl_msg_op *
cl_msg_op_iterate(cl_msg *msg, cl_msg_op *current, int *n)
{
	// skip over the fields the first time
	if (!current) {
		if (msg->n_ops == 0) return(0); // short cut
		cl_msg_field *mf = (cl_msg_field *) msg->data;
		for (uint i=0;i<msg->n_fields;i++)
			mf = cl_msg_field_get_next(mf);
		current = (cl_msg_op *) mf;
		*n = 0;
		return(current);
	}
	(*n)++;
	if (*n >= msg->n_ops)	return(0);
	return ( cl_msg_op_get_next( current ) );
	
}	


/* cl_msg_size_get
 * Get the size of a message */
static inline size_t
cl_proto_size_get(cl_proto *proto)
{
	return( sizeof(cl_proto) + proto->size);
}


/* Function declarations */
extern void cl_proto_swap(cl_proto *m);
extern void cl_msg_swap_header(cl_msg *m);
extern void cl_msg_swap_field(cl_msg_field *mf);
extern void cl_msg_swap_fields(cl_msg *m);
extern void cl_msg_swap_ops(cl_msg *m);
extern void cl_msg_swap_op(cl_msg_op *op);
extern void cl_msg_swap_fields_and_ops(cl_msg *m);

#ifdef __cplusplus
} // end extern "C"
#endif

