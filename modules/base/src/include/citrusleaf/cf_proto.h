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
#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// See aerospike-client-c/src/include/citrusleaf/cl_types.h for server error codes.

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

 
#define CL_PROTO_VERSION 2
#define CL_PROTO_TYPE_INFO 1       // ascii-format message for determining server info
#define CL_PROTO_TYPE_CL_MSG 3
#define CL_PROTO_TYPE_CL_MSG_COMPRESSED 4

#define CL_RESULT_OK	0
#define CL_RESULT_FAIL 1
#define CL_RESULT_NOTFOUND 2

#if defined(__APPLE__) || defined(CF_WINDOWS)

#pragma pack(push, 1) // packing is now 1
typedef struct cl_proto_s {
	uint64_t	version	:8;
	uint64_t	type	:8;
	uint64_t	sz		:48;
} cl_proto;
#pragma pack(pop) // packing is back to what it was

#pragma pack(push, 1) // packing is now 1
/*
 * zlib decompression API needs original size of the compressed data.
 * So we need to transfer it to another end.
 * This structure packs together - 
 * header + original size of data + compressed data
 */
typedef struct cl_comp_proto_s {
	cl_proto  	proto;     // Protocol header
	uint64_t 	org_sz;    // Original size of compressed data hold in 'data'
	uint8_t data[];        // Compressed data
}  cl_comp_proto;
#pragma pack(pop) // packing is back to what it was


/* cl_msg_field
 * Aerospike message field */

#pragma pack(push, 1)
typedef struct cl_msg_field_s {
	uint32_t field_sz; // get the data size through the accessor function, don't worry, it's a small macro
	uint8_t type;
	uint8_t data[];
} cl_msg_field;
#pragma pack(pop)

#pragma pack(push, 1) // packing is now 1
typedef struct cl_msg_op_s {
	uint32_t op_sz;
	uint8_t  op;
	uint8_t  particle_type;
	uint8_t  version;
	uint8_t  name_sz;
	uint8_t	 name[]; // UTF-8
	// there's also a value here but you can't have two variable size arrays
} cl_msg_op;
#pragma pack(pop) // packing is back to what it was

/* cl_msg_key_s
*/
// Not using it anywhere in libevent2 client 
// Please be aware when using this for any other client
/*
#pragma pack(push, 1) // packing is now 1
typedef struct cl_msg_key_s {
	cl_msg_field	f;
	uint8_t  key[];
} cl_msg_key;
#pragma pack(pop) // packing is back to what it was
*/
/* cl_msg_number_s
*/

#pragma pack(push, 1) // packing is now 1
typedef struct cl_msg_number_s {
	uint32_t number;
	cl_msg_field	f;
} cl_msg_number;
#pragma pack(pop) // packing is back to what it was


/* cl_msg
 * Aerospike message
 * size: size of the payload, not including the header */

#pragma pack(push, 1) // packing is now 1
typedef struct cl_msg_s {
/*00*/	uint8_t		header_sz;			// number of uint8_ts in this header
/*01*/	uint8_t		info1;				// bitfield about this request
/*02*/	uint8_t		info2;
/*03*/	uint8_t		info3;
/*04*/	uint8_t		unused;
/*05*/	uint8_t		result_code;
/*06*/	uint32_t	generation;
/*10*/	uint32_t	record_ttl;
/*14*/	uint32_t	transaction_ttl;
/*18*/	uint16_t	n_fields;			// size in uint8_ts
/*20*/	uint16_t	n_ops;				// number of operations
/*22*/	uint8_t		data[0];			// data contains first the fields, then the ops
}  cl_msg;
#pragma pack(pop) // packing is back to what it was


/* cl_ms
 * Aerospike message
 * sz: size of the payload, not including the header */

#pragma pack(push, 1) // packing is now 1
typedef struct as_msg_s {
		cl_proto  	proto;
		cl_msg		m;
} as_msg;
#pragma pack(pop) // packing is back to what it was


#else

typedef struct cl_proto_s {
	uint8_t		version;
	uint8_t		type;
	uint64_t	sz:48;
	uint8_t		data[];
} __attribute__ ((__packed__)) cl_proto;

/*
 * zlib decompression API needs original size of the compressed data.
 * So we need to transfer it to another end.
 * This structure packs together - 
 * header + original size of data + compressed data
 */
typedef struct cl_comp_proto_s {
	cl_proto  	proto;     // Protocol header
	uint64_t 	org_sz;    // Original size of compressed data hold in 'data'
	uint8_t data[];        // Compressed data
}  cl_comp_proto;

 /* cl_msg_field
 * Aerospike message field */
typedef struct cl_msg_field_s {
	uint32_t field_sz; // get the data size through the accessor function, don't worry, it's a small macro
	uint8_t type;
	uint8_t data[];
} __attribute__((__packed__)) cl_msg_field;

 
typedef struct cl_msg_op_s {
	uint32_t op_sz;
	uint8_t  op;
	uint8_t  particle_type;
	uint8_t  version;
	uint8_t  name_sz;
	uint8_t	 name[]; // UTF-8
	// there's also a value here but you can't have two variable size arrays
} __attribute__((__packed__)) cl_msg_op;
 

typedef struct cl_msg_key_s {
	cl_msg_field	f;
	uint8_t  key[];
} __attribute__ ((__packed__)) cl_msg_key;

typedef struct cl_msg_number_s {
	cl_msg_field	f;
	uint32_t number;
} __attribute__ ((__packed__)) cl_msg_number;



/* cl_msg
 * Aerospike message
 * size: size of the payload, not including the header */
typedef struct cl_msg_s {
/*00*/	uint8_t		header_sz;			// number of uint8_ts in this header
/*01*/	uint8_t		info1;				// bitfield about this request
/*02*/	uint8_t		info2;
/*03*/	uint8_t		info3;
/*04*/	uint8_t		unused;
/*05*/	uint8_t		result_code;
/*06*/	uint32_t	generation;
/*10*/	uint32_t	record_ttl;
/*14*/	uint32_t	transaction_ttl;
/*18*/	uint16_t	n_fields;			// size in uint8_ts
/*20*/	uint16_t	n_ops;				// number of operations
/*22*/	uint8_t		data[];				// data contains first the fields, then the ops
} __attribute__((__packed__)) cl_msg;

/* cl_ms
 * Aerospike message
 * sz: size of the payload, not including the header */
typedef struct as_msg_s {
	    cl_proto  	proto;
		cl_msg		m;
} __attribute__((__packed__)) as_msg;

#endif

// 0-19 STANDARD
#define CL_MSG_FIELD_TYPE_NAMESPACE 0 // UTF8 string
#define CL_MSG_FIELD_TYPE_SET 1
#define CL_MSG_FIELD_TYPE_KEY 2  // contains a key type
#define CL_MSG_FIELD_TYPE_BIN 3  // used for secondary key access - contains a bin, thus a name and value
#define CL_MSG_FIELD_TYPE_DIGEST_RIPE 4  // used to send the digest just computed to the server so it doesn't have to
#define CL_MSG_FIELD_TYPE_GU_TID 5
#define CL_MSG_FIELD_TYPE_DIGEST_RIPE_ARRAY 6
#define CL_MSG_FIELD_TYPE_TRID 7
// We are going to overload the OPTIONS field -- this will hold either SCAN
// options or QUERY options, depending on the type of call.  THis is done with
// the expectation that the call will have only one or the other (Nov 20, 2013 tjl)
#define CL_MSG_FIELD_TYPE_SCAN_OPTIONS 8
#define CL_MSG_FIELD_TYPE_QUERY_OPTIONS 8
	
// 20-29 RESERVED FOR SECONDARY INDEX
#define CL_MSG_FIELD_TYPE_INDEX_NAME			21
#define CL_MSG_FIELD_TYPE_INDEX_RANGE			22
#define CL_MSG_FIELD_TYPE_INDEX_FILTER			23
#define CL_MSG_FIELD_TYPE_INDEX_LIMIT			24
#define CL_MSG_FIELD_TYPE_INDEX_ORDER_BY		25
	
// 30-39 RESEVED FOR UDF
#define CL_MSG_FIELD_TYPE_UDF_FILENAME          30
#define CL_MSG_FIELD_TYPE_UDF_FUNCTION          31
#define CL_MSG_FIELD_TYPE_UDF_ARGLIST           32
#define CL_MSG_FIELD_TYPE_UDF_OP                33
// NOTE: UDF_OP really holds "Stream" or "Record" UDF type.  And, going forward
// from this point (Nov 21, 2013), we're going to have two fields that all will
// treat the same.  Udf type will be (None, Record, Stream), and the Transaction
// Call type will be Query/Scan and the Transaction ResultType will be
// FOREGROUND or BACKGROUND  (as specified in the query/scan options).
// Historical note:  Somehow QUERY and SCAN took different paths and started
// using this field differently.
// QUERY CLIENT had: None, Record, Stream
// QUERY SERVER had: UDF, Aggregate, MR
// SCAN  SERVER had: None, UDF, Background
// On the wire, we put: 0=Record and 1=Stream into field 33 (above)
#define CL_UDF_MSG_VAL_RECORD 0
#define CL_UDF_MSG_VAL_STREAM 1
	
// 40-49 RESERVED FOR QUERY
#define CL_MSG_FIELD_TYPE_QUERY_BINLIST			40
	
	
#define CL_MSG_OP_READ 1			// read the value in question
#define CL_MSG_OP_WRITE 2			// write the value in question
#define CL_MSG_OP_WRITE_UNIQUE 3	// write a namespace-wide unique value
#define CL_MSG_OP_WRITE_NOW 4		// write the server-current time
#define CL_MSG_OP_INCR 5
#define CL_MSG_OP_APPEND_SEGMENT 6          // Append segment to a particle
#define CL_MSG_OP_APPEND_SEGMENT_EXT 7      // Extended append - with parameters
#define CL_MSG_OP_APPEND_SEGMENT_QUERY 8    // Query to return subset of segments
#define CL_MSG_OP_APPEND 9                  // Add to an existing particle
#define CL_MSG_OP_PREPEND 10                // Add to the beginning of an existing particle
#define CL_MSG_OP_TOUCH   11                // Touch 

#define CL_MSG_OP_MC_INCR    129        // Memcache-compatible version of the increment command
#define CL_MSG_OP_MC_APPEND  130        // Memcache compatible append. Allow appending to ints.
#define CL_MSG_OP_MC_PREPEND 131        // Memcache compatile prepend. Allow prepending to ints.
#define CL_MSG_OP_MC_TOUCH   132        // Memcache compatible touch - does not change generation count

#define CL_MSG_INFO1_READ				(1 << 0) // contains a read operation
#define CL_MSG_INFO1_GET_ALL			(1 << 1) // get all bins, period
#define CL_MSG_INFO1_GET_ALL_NODATA		(1 << 2) // get all bins WITHOUT data (currently unimplemented)
// (Note:  Bit 3 is unused.)
#define CL_MSG_INFO1_XDR				(1 << 4) // operation is being performed by XDR
#define CL_MSG_INFO1_GET_NOBINDATA		(1 << 5) // do not get information about bins and its data
#define CL_MSG_INFO1_CONSISTENCY_LEVEL_B0	(1 << 6) // read consistency level - bit 0
#define CL_MSG_INFO1_CONSISTENCY_LEVEL_B1	(1 << 7) // read consistency level - bit 1

#define CL_MSG_INFO2_WRITE				(1 << 0) // contains a write semantic
#define CL_MSG_INFO2_DELETE				(1 << 1) // delete record
#define CL_MSG_INFO2_GENERATION			(1 << 2) // pay attention to the generation
#define CL_MSG_INFO2_GENERATION_GT		(1 << 3) // apply write if new generation >= old, good for restore
#define CL_MSG_INFO2_GENERATION_DUP		(1 << 4) // if a generation collision, create a duplicate
#define CL_MSG_INFO2_CREATE_ONLY		(1 << 5) // write record only if it doesn't exist
#define CL_MSG_INFO2_BIN_CREATE_ONLY	(1 << 6) // write bin only if it doesn't exist
#define CL_MSG_INFO2_WRITE_MERGE		(1 << 7) // merge this with current

#define CL_MSG_INFO3_LAST				(1 << 0) // this is the last of a multi-part message
#define CL_MSG_INFO3_COMMIT_LEVEL_B0  	(1 << 1) // write commit level - bit 0
#define CL_MSG_INFO3_COMMIT_LEVEL_B1  	(1 << 2) // write commit level - bit 1
#define CL_MSG_INFO3_UPDATE_ONLY		(1 << 3) // update existing record only, do not create new record
#define CL_MSG_INFO3_CREATE_OR_REPLACE	(1 << 4) // completely replace existing record, or create new record
#define CL_MSG_INFO3_REPLACE_ONLY		(1 << 5) // completely replace existing record, do not create new record
#define CL_MSG_INFO3_BIN_REPLACE_ONLY	(1 << 6) // replace existing bin, do not create new bin
// (Note:  Bit 7 is unused.)

static inline uint8_t * cl_msg_op_get_value_p(cl_msg_op *op)
{
	return ( ((uint8_t *)op) + sizeof(cl_msg_op) + op->name_sz);
}

static inline uint32_t cl_msg_op_get_value_sz(cl_msg_op *op)
{
	return( op->op_sz - (4 + op->name_sz) );
}

static inline uint32_t cl_msg_field_get_value_sz(cl_msg_field *f)
{
	return( f->field_sz - 1 );
}

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
	return ( (cl_msg_op *) (((uint8_t *) op) + sizeof(op->op_sz) + op->op_sz ) );
}

 
static inline cl_msg_op *
cl_msg_op_iterate(cl_msg *msg, cl_msg_op *current, int *n)
{
	// skip over the fields the first time
	if (!current) {
		if (msg->n_ops == 0) return(0); // short cut
		cl_msg_field *mf = (cl_msg_field *) msg->data;
		for (uint32_t i = 0; i < msg->n_fields; i++)
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
	return( sizeof(cl_proto) + proto->sz);
}


/* Function declarations */
extern void cl_proto_swap_to_be(cl_proto *m);
extern void cl_proto_swap_from_be(cl_proto *m);
extern void cl_msg_swap_header_to_be(cl_msg *m);
extern void cl_msg_swap_header_from_be(cl_msg *m);
extern void cl_msg_swap_field_to_be(cl_msg_field *mf);
extern void cl_msg_swap_field_from_be(cl_msg_field *mf);
extern void cl_msg_swap_op_to_be(cl_msg_op *op);
extern void cl_msg_swap_op_from_be(cl_msg_op *op);

#ifdef __cplusplus
} // end extern "C"
#endif
