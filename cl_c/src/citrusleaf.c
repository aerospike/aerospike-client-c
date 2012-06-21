/*
 * A good, basic C client for the Aerospike protocol
 * Creates a library which is linkable into a variety of systems
 *
 * First attempt is a very simple non-threaded blocking interface
 * currently coded to C99 - in our tree, GCC 4.2 and 4.3 are used
 *
 * Brian Bulkowski, 2009, 2010, 2011
 * All rights reserved
 */

#include <sys/types.h>
#include <sys/socket.h> // socket calls
#include <stdio.h>
#include <errno.h> //errno
#include <stdlib.h> //fprintf
#include <unistd.h> // close
#include <string.h>
#include <pthread.h>
#include <fcntl.h>
#include <arpa/inet.h>  // ntonl
#include <inttypes.h> // PRIu64

#ifdef OSX
#include <libkern/OSByteOrder.h> // for the 64-bit swap macro.
#else //Linux
#include <asm/byteorder.h> // 64-bit swap macro
#endif

#include "citrusleaf/citrusleaf.h"
#include "citrusleaf/cl_cluster.h"
#include "citrusleaf/citrusleaf-internal.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/proto.h"
#include "citrusleaf/cf_hist.h"
#include "citrusleaf/cf_socket.h"


#include <signal.h>

// This is a per-transaction deadline kind of thing
#define DEFAULT_TIMEOUT 200

// 

static bool g_initialized = false;
int g_cl_turn_debug_on = false;


// #define DEBUG_HISTOGRAM 1 // histogram printed in citrusleaf_print_stats()
// #define DEBUG 1
// #define DEBUG_VERBOSE 1
// #define DEBUG_TIME 1 // debugs involving timing

#ifdef DEBUG_HISTOGRAM
    static cf_histogram *cf_hist;
#endif

#ifdef DEBUG_TIME
static void debug_printf(long before_write_time, long after_write_time, long before_read_header_time, long after_read_header_time, 
		long before_read_body_time, long after_read_body_time, long deadline_ms, int progress_timeout_ms)
{
	fprintf(stderr, "tid %zu - Before Write - deadline %"PRIu64" progress_timeout %d now is %"PRIu64"\n", (uint64_t)pthread_self(), deadline_ms, progress_timeout_ms, before_write_time);
	fprintf(stderr, "tid %zu - After Write - now is %"PRIu64"\n", (uint64_t)pthread_self(), after_write_time);
	fprintf(stderr, "tid %zu - Before Read header - deadline %"PRIu64" progress_timeout %d now is %"PRIu64"\n", (uint64_t)pthread_self(), deadline_ms, progress_timeout_ms, before_read_header_time);        
	fprintf(stderr, "tid %zu - After Read header - now is %"PRIu64"\n", (uint64_t)pthread_self(), after_read_header_time);
	fprintf(stderr, "tid %zu - Before Read body - deadline %"PRIu64" progress_timeout %d now is %"PRIu64"\n", (uint64_t)pthread_self(), deadline_ms, progress_timeout_ms, before_read_body_time);
	fprintf(stderr, "tid %zu - After Read body - now is %"PRIu64"\n", (uint64_t)pthread_self(), after_read_body_time);
}
#endif

//
// Citrusleaf Object calls
//

void
citrusleaf_object_init(cl_object *o)
{
	o->type = CL_NULL;
	o->sz = 0;
	o->free = 0;
}

void citrusleaf_object_init_str(cl_object *o, const char *str)
{
	o->type = CL_STR;
	o->sz = strlen(str);
	o->u.str = (char *) str;
	o->free = 0;
}

void citrusleaf_object_init_str2(cl_object *o, const char *str, size_t len)
{
	o->type = CL_STR;
	o->sz = len;
	o->u.str = (char *) str;
	o->free = 0;
}

void citrusleaf_object_init_int(cl_object *o, int64_t i)
{
	o->type = CL_INT;
	o->sz = sizeof(o->u.i64);
	o->u.i64 = i;
	o->free = 0;
}

void citrusleaf_object_init_blob(cl_object *o, const void *blob, size_t len)
{
	o->type = CL_BLOB;
	o->sz = len;
	o->u.blob = (void *) blob;
	o->free = 0;
}

void citrusleaf_object_init_blob2(cl_object *o, const void *blob, size_t len, cl_type t)
{
	o->type = t;
	o->sz = len;
	o->u.blob = (void *) blob;
	o->free = 0;
}

void citrusleaf_object_init_blob_type(cl_object *o, int blob_type, void *blob, size_t len)
{
	o->type = blob_type;
	o->sz = len;
	o->u.blob = blob;
	o->free = 0;
}

void citrusleaf_object_init_null(cl_object *o)
{
	o->type = CL_NULL;
	o->sz = 0;
	o->free = 0;
}


void citrusleaf_object_free(cl_object *o) {
	if (o->free){	
		free(o->free);
		o->free = NULL;
	}
}

void citrusleaf_bins_free(cl_bin *bins, int n_bins) {
	for (int i=0;i<n_bins;i++) {
		if (bins[i].object.free) free(bins[i].object.free);
	}
}

//
// Debug calls for printing the buffers. Very useful for debugging....
#ifdef DEBUG_VERBOSE
static void
dump_buf(char *info, uint8_t *buf, size_t buf_len)
{
	fprintf(stderr, "dump_buf: %s\n",info);
	uint i;
	for (i=0;i<buf_len;i++) {
		if (i % 16 == 8)
			fprintf(stderr, " :");
		if (i && (i % 16 == 0))
			fprintf(stderr, "\n");
		fprintf(stderr, "%02x ",buf[i]);
	}
	fprintf(stderr, "\n");
}
#endif
	
// forward ref
static int value_to_op_int(int64_t value, uint8_t *data);

static void
dump_values(cl_bin *bins, cl_operation *operations, int n_bins)
{
	fprintf(stderr, " n bins: %d\n",n_bins);
	for (int i=0;i<n_bins;i++) {
		cl_object *object = (bins ? &bins[i].object : &operations[i].bin.object);
		char *name        = (bins ? bins[i].bin_name : operations[i].bin.bin_name);
		fprintf(stderr, "%d %s:  (sz %zd)\n",i, name,object->sz);
		switch (object->type) {
			case CL_NULL:
				fprintf(stderr, "NULL \n");
				break;
			case CL_INT:
				fprintf(stderr, "int   %"PRIu64"\n",object->u.i64);
				break;
			case CL_STR:
				fprintf(stderr, "str   %s\n",object->u.str);
				break;
			default:
			    fprintf(stderr, "unk type %d\n",object->type);
				break;
		}
	}
}

static void
dump_key(char *msg, cl_object const *key)
{
	switch (key->type) {
		case CL_NULL:
			fprintf(stderr, "%s: key NULL \n",msg);
			break;
		case CL_INT:
			fprintf(stderr, "%s: key int   %"PRIu64"\n",msg,key->u.i64);
			break;
		case CL_STR:
			fprintf(stderr, "%s: key str   %s\n",msg,key->u.str);
			break;
		default:
			fprintf(stderr, "%s: key unk type %d\n",msg,key->type);
			break;
	}
}


//
// Buffer formatting calls
//

uint8_t *
cl_write_header(uint8_t *buf, size_t msg_sz, uint info1, uint info2, uint info3, uint32_t generation, uint32_t record_ttl, uint32_t transaction_ttl, uint32_t n_fields, uint32_t n_ops )
{
	as_msg *msg = (as_msg *) buf;
	
	msg->proto.version = CL_PROTO_VERSION;
	msg->proto.type = CL_PROTO_TYPE_CL_MSG;
	msg->proto.sz = msg_sz - sizeof(cl_proto);
	cl_proto_swap(&msg->proto);
	msg->m.header_sz = sizeof(cl_msg);
	msg->m.info1 = info1;
	msg->m.info2 = info2;
	msg->m.info3 = info3;
	msg->m.unused = 0;
	msg->m.result_code = 0;
	msg->m.generation = generation;
	msg->m.record_ttl = record_ttl;
	msg->m.transaction_ttl = transaction_ttl;
	msg->m.n_fields = n_fields;
	msg->m.n_ops = n_ops;
	cl_msg_swap_header(&msg->m);
	return (buf + sizeof(as_msg));
}


//
// lay out a request into a buffer
// Caller is encouraged to allocate some stack space for something like this
// buf if the space isn't big enough we'll malloc
//
// FIELDS WILL BE SWAPED INTO NETWORK ORDER
//
// The digest will be  

static uint8_t *
write_fields(uint8_t *buf, const char *ns, int ns_len, const char *set, int set_len, const cl_object *key, const cf_digest *d, cf_digest *d_ret, 
	uint64_t trid, cl_scan_param_field *scan_param_field)
{
	
	// lay out the fields
	cl_msg_field *mf = (cl_msg_field *) buf;
	cl_msg_field *mf_tmp = mf;
	
	if (ns) {
		mf->type = CL_MSG_FIELD_TYPE_NAMESPACE;
		mf->field_sz = ns_len + 1;
		memcpy(mf->data, ns, ns_len);
		mf_tmp = cl_msg_field_get_next(mf);
		cl_msg_swap_field(mf);
		mf = mf_tmp;
	}

	if (set) {	
		mf->type = CL_MSG_FIELD_TYPE_SET;
		mf->field_sz = set_len + 1;
		memcpy(mf->data, set, set_len);
		mf_tmp = cl_msg_field_get_next(mf);
		cl_msg_swap_field(mf);
		mf = mf_tmp;
	}

	if (trid) {
		mf->type = CL_MSG_FIELD_TYPE_TRID;
		//Convert the transaction-id to network byte order (big-endian)
		uint64_t trid_nbo = __cpu_to_be64(trid); //swaps in place
		mf->field_sz = sizeof(trid_nbo) + 1;
		memcpy(mf->data, &trid_nbo, sizeof(trid_nbo));
		mf_tmp = cl_msg_field_get_next(mf);
		cl_msg_swap_field(mf);
		mf = mf_tmp;
	}

	if (scan_param_field) {
		mf->type = CL_MSG_FIELD_TYPE_SCAN_OPTIONS;
		mf->field_sz = sizeof(cl_scan_param_field) + 1;
		memcpy(mf->data, scan_param_field, sizeof(cl_scan_param_field));
		mf_tmp = cl_msg_field_get_next(mf);
		cl_msg_swap_field(mf);
		mf = mf_tmp;
	}	

	if (key) {
		mf->type = CL_MSG_FIELD_TYPE_KEY;
		// make a function call here, similar to our prototype code in the server
		uint8_t *fd = (uint8_t *) &mf->data;
		switch (key->type) {
			case CL_STR:
				fd[0] = key->type;
				mf->field_sz = key->sz + 2;
				memcpy(&fd[1], key->u.str, key->sz);
				break;
			case CL_INT:
				fd[0] = key->type;
				mf->field_sz = value_to_op_int(key->u.i64, &fd[1]) + 2; 
                uint64_t swapped = __swab64(key->u.i64);
                memcpy(&fd[1], &swapped, sizeof(swapped));
				break;
			case CL_BLOB:
			case CL_JAVA_BLOB:
			case CL_CSHARP_BLOB:
			case CL_PYTHON_BLOB:
			case CL_RUBY_BLOB:
			case CL_PHP_BLOB:
				fd[0] = key->type;
				mf->field_sz = key->sz + 2;
				memcpy(&fd[1], key->u.blob, key->sz);
				break;
			default:
//	#ifdef DEBUG		
				fprintf(stderr, " transmit key: unknown citrusleaf type %d",key->type);
//	#endif		
				return(0);
		}
		mf_tmp = cl_msg_field_get_next(mf);
		cl_msg_swap_field(mf);
		
		// calculate digest
		if (d_ret) {

			cf_digest_compute2( (char *)set, set_len, mf->data, key->sz + 1, d_ret);

//			fprintf(stderr, "digest compute: %d : %02x %02x %02x %02x\n",
//				key->sz+1, mf->data[0],mf->data[1],mf->data[2],mf->data[3]);
			
//			fprintf(stderr, "digest compute: %02x %02x %02x %02x\n",
//				d_ret->digest[0],d_ret->digest[1],d_ret->digest[2],d_ret->digest[3]);
			
		}
		
		mf = mf_tmp;
	}
	
	if (d) {
		mf->type = CL_MSG_FIELD_TYPE_DIGEST_RIPE;
		mf->field_sz = sizeof(cf_digest) + 1;
		memcpy(mf->data, d, sizeof(cf_digest));
		mf_tmp = cl_msg_field_get_next(mf);
		cl_msg_swap_field(mf);
		if (d_ret)
			memcpy(d_ret, d, sizeof(cf_digest));

		mf = mf_tmp;
		
	}
	return ( (uint8_t *) mf_tmp );
}

static uint8_t *
write_fields_digests(uint8_t *buf, const char *ns, int ns_len, const cf_digest *digests, int n_digests)
{
	
	// lay out the fields
	cl_msg_field *mf = (cl_msg_field *) buf;
	cl_msg_field *mf_tmp = mf;
	
	if (ns) {
		mf->type = CL_MSG_FIELD_TYPE_NAMESPACE;
		mf->field_sz = ns_len + 1;
		memcpy(mf->data, ns, ns_len);
		mf_tmp = cl_msg_field_get_next(mf);
		cl_msg_swap_field(mf);
		mf = mf_tmp;
	}

	if (digests) {
		mf->type = CL_MSG_FIELD_TYPE_DIGEST_RIPE_ARRAY;
		int digest_sz = sizeof(cf_digest) * n_digests;
		mf->field_sz = digest_sz + 1;
		memcpy(mf->data, digests, digest_sz);
		mf_tmp = cl_msg_field_get_next(mf);
		cl_msg_swap_field(mf);		
		mf = mf_tmp;
	}

	return ( (uint8_t *) mf_tmp );
}


// Convert the int value to the wire protocol

static int
value_to_op_int(int64_t value, uint8_t *data)
{
#ifdef OSX
	uint64_t swapped = OSSwapHostToBigInt64(value);
#else // Linux
	uint64_t swapped = __cpu_to_be64((__u64) value);  // swap in place
#endif
	memcpy(data, &swapped, sizeof(uint64_t));
	return(8);
}

// Get the size of the wire protocol value
// Must match previous function EXACTLY 

static int
value_to_op_int_sz(int64_t i)
{
	return(8);
}

// In the MC_INCR operation, two uint64s are packed into a 
// blob. ntoh them both and put into the output.
static int
value_to_op_two_ints(void *value, uint8_t *data)
{
	int64_t value1  = *(int64_t *)value;
	int64_t value2  = *(int64_t *)((uint8_t *)value + sizeof(int64_t));

#ifdef OSX
	uint64_t swapped1 = OSSwapHostToBigInt64(value1);
	uint64_t swapped2 = OSSwapHostToBigInt64(value2);
#else // Linux
	uint64_t swapped1 = __cpu_to_be64((__u64) value1);  // swap in place
	uint64_t swapped2 = __cpu_to_be64((__u64) value2);  // swap in place
#endif
	memcpy(data, &swapped1, sizeof(uint64_t));
	memcpy(data + sizeof(uint64_t), &swapped2, sizeof(uint64_t));

	return (2*sizeof(uint64_t));
}


// convert a wire protocol integer value to a local int64
static int
op_to_value_int(uint8_t	*buf, int sz, int64_t *value)
{
//	fprintf(stderr, "op to value int: sz %d\n",sz);
	if (sz > 8)	return(-1);
	if (sz == 8) {
		// no need to worry about sign extension - blast it
#ifdef OSX
		*value = OSSwapHostToBigInt64(*(uint64_t *) buf);
#else //Linux
		*value = __cpu_to_be64(*(__u64 *) buf);
#endif	
		return(0);
	}
	if (sz == 0) {
		*value = 0;
		return(0);
	}
	if (sz == 1 && *buf < 0x7f) {
		*value = *buf;
		return(0);
	}
	
	// negative numbers must be sign extended; yuck
	if (*buf & 0x80) {
		uint8_t	lg_buf[8];
		int i;
		for (i=0;i<8-sz;i++)	lg_buf[i]=0xff;
		memcpy(&lg_buf[i],buf,sz);
#ifdef OSX
		*value = OSSwapHostToBigInt64(*(uint64_t *) buf);
#else //Linux
		*value = __cpu_to_be64((__u64) *buf);
#endif
		return(0);
	}
	// positive numbers don't
	else {
		int64_t	v = 0;
		for (int i=0;i<sz;i++,buf++) {
			v <<= 8;
			v |= *buf;
		}
		*value = v;
		return(0);
	}
	
	
	return(0);
}

int
cl_value_to_op_get_size(cl_bin *v, size_t *sz)
{
	switch(v->object.type) {
		case CL_NULL:
			break;
		case CL_INT:
			*sz += value_to_op_int_sz(v->object.u.i64);
			break;
		case CL_STR:
			*sz += v->object.sz;
			break;
		case CL_PYTHON_BLOB:
		case CL_RUBY_BLOB:
		case CL_JAVA_BLOB:
		case CL_CSHARP_BLOB:
		case CL_BLOB:
		case CL_PHP_BLOB:
			*sz += v->object.sz;
			break;
		default:
//#ifdef DEBUG			
			fprintf(stderr, "internal error value_to_op get size has unknown value type %d\n",v->object.type);
//#endif			
			return(-1);
	}
	return(0);
}

// Lay an C structure bin into network order operation

int
cl_value_to_op(cl_bin *v, cl_operator operator, cl_operation *operation, cl_msg_op *op)
{
	cl_bin *bin = v?v:&operation->bin;
	int	bin_len = strlen(bin->bin_name);
	op->op_sz = sizeof(cl_msg_op) + bin_len - sizeof(uint32_t);
	op->name_sz = bin_len;
	op->version = 0;
	memcpy(op->name, bin->bin_name, bin_len);

	cl_operator tmpOp = 0;
	cl_bin      *tmpValue = 0;
	
	if( v ){
		tmpOp = operator;
		tmpValue = v;
	}else if( operation ){
		tmpOp = operation->op;
		tmpValue = &(operation->bin);
	}

	switch(tmpOp) {
		case CL_OP_WRITE:
			op->op = CL_MSG_OP_WRITE;
			break;
		case CL_OP_READ:
			op->op = CL_MSG_OP_READ;
			break;
		case CL_OP_INCR:
			op->op = CL_MSG_OP_INCR;
			break;
		case CL_OP_MC_INCR:
			op->op = CL_MSG_OP_MC_INCR;
			break;
		case CL_OP_APPEND:
			op->op = CL_MSG_OP_APPEND;
			break;
		case CL_OP_PREPEND:
			op->op = CL_MSG_OP_PREPEND;
			break;
		case CL_OP_MC_APPEND:
			op->op = CL_MSG_OP_MC_APPEND;
			break;
		case CL_OP_MC_PREPEND:
			op->op = CL_MSG_OP_MC_PREPEND;
			break;
		case CL_OP_TOUCH:
			op->op = CL_MSG_OP_TOUCH;
			break;
		case CL_OP_MC_TOUCH:
			op->op = CL_MSG_OP_MC_TOUCH;
			break;
		default:
			fprintf(stderr, "API user requested unknown operation type %d, fail",(int)tmpOp);
			return(-1);
	}

	uint8_t *data = cl_msg_op_get_value_p(op);
	op->particle_type = tmpValue->object.type;
	switch(tmpValue->object.type) {
		case CL_NULL:
			break;
		case CL_INT:
			op->op_sz += value_to_op_int(tmpValue->object.u.i64, data);
			break;
		case CL_STR:
			op->op_sz += tmpValue->object.sz;
			memcpy(data, tmpValue->object.u.str, tmpValue->object.sz);
			break;
		case CL_BLOB:
		case CL_JAVA_BLOB:
		case CL_CSHARP_BLOB:
		case CL_PYTHON_BLOB:
		case CL_RUBY_BLOB:
		case CL_PHP_BLOB:
			if (op->op == CL_MSG_OP_MC_INCR) {
				op->op_sz += value_to_op_two_ints(tmpValue->object.u.blob, data);
			} else {
				op->op_sz += tmpValue->object.sz;
				memcpy(data, tmpValue->object.u.blob, tmpValue->object.sz);
			}
			break;
		default:
#ifdef DEBUG				
			fprintf(stderr, "internal error value_to_op has unknown value type %d\n",tmpValue->object.type);
#endif				
			return(-1);
	}
	return(0);
}

//
// n_values can be passed in 0, and then values is undefined / probably 0.
//
// The DIGEST is filled *in* by this function - should be passed in uninitialized


int
cl_compile(uint info1, uint info2, uint info3, const char *ns, const char *set, const cl_object *key, const cf_digest *digest,
	cl_bin *values, cl_operator operator, cl_operation *operations, int n_values,  
	uint8_t **buf_r, size_t *buf_sz_r, const cl_write_parameters *cl_w_p, cf_digest *d_ret, uint64_t trid, cl_scan_param_field *scan_param_field)
{
	// I hate strlen
	int		ns_len = ns ? strlen(ns) : 0;
	int		set_len = set ? strlen(set) : 0;
	int		i;

	// determine the size
	size_t	msg_sz = sizeof(as_msg); // header
	// fields
	if (ns)     msg_sz += sizeof(cl_msg_field) + ns_len;
	if (set)    msg_sz += sizeof(cl_msg_field) + set_len;
	if (key)    msg_sz += sizeof(cl_msg_field) + 1 + key->sz;
	if (digest) msg_sz += sizeof(cl_msg_field) + 1 + sizeof(cf_digest);
	if (trid)   msg_sz += sizeof(cl_msg_field) + sizeof(trid);
	if (scan_param_field)	msg_sz += sizeof(cl_msg_field) + 1 + sizeof(cl_scan_param_field);

	// ops
	for (i=0;i<n_values;i++) {
		cl_bin *tmpValue = 0;
		if( values ){
			tmpValue = &values[i];
		}else if( operations ){
			tmpValue = &operations[i].bin;
		}
		
		msg_sz += sizeof(cl_msg_op) + strlen(tmpValue->bin_name);

        if (0 != cl_value_to_op_get_size(tmpValue, &msg_sz)) {
			fprintf(stderr,"illegal parameter: bad type %d write op %d\n",tmpValue->object.type,i);
			return(-1);
		}
	}
	
	// size too small? malloc!
	uint8_t	*buf;
	uint8_t *mbuf = 0;
	if ((*buf_r) && (msg_sz > *buf_sz_r)) {
		mbuf = buf = malloc(msg_sz);
		if (!buf) 			return(-1);
		*buf_r = buf;
	}
	else
		buf = *buf_r;
	
	*buf_sz_r = msg_sz;
	
	// debug - shouldn't be required
	memset(buf, 0, msg_sz);
	
	// lay in some parameters
	uint32_t generation = 0;
	if (cl_w_p) {
		if (cl_w_p->unique) {
			info2 |= CL_MSG_INFO2_WRITE_UNIQUE;
		} else if (cl_w_p->unique_bin) {
			info2 |= CL_MSG_INFO2_WRITE_BINUNIQUE;
		} else if (cl_w_p->use_generation) {
			info2 |= CL_MSG_INFO2_GENERATION;
			generation = cl_w_p->generation;
		} else if (cl_w_p->use_generation_gt) {
			info2 |= CL_MSG_INFO2_GENERATION_GT;
			generation = cl_w_p->generation;
		} else if (cl_w_p->use_generation_dup) {
			info2 |= CL_MSG_INFO2_GENERATION_DUP;
			generation = cl_w_p->generation;
		}
	}
	
	uint32_t record_ttl = cl_w_p ? cl_w_p->record_ttl : 0;
	uint32_t transaction_ttl = cl_w_p ? cl_w_p->timeout_ms : 0;

	// lay out the header
	int n_fields = ( ns ? 1 : 0 ) + (set ? 1 : 0) + (key ? 1 : 0) + (digest ? 1 : 0) + (trid ? 1 : 0) + (scan_param_field ? 1 : 0);
	buf = cl_write_header(buf, msg_sz, info1, info2, info3, generation, record_ttl, transaction_ttl, n_fields, n_values);
		
	// now the fields
	buf = write_fields(buf, ns, ns_len, set, set_len, key, digest, d_ret, trid,scan_param_field);
	if (!buf) {
		if (mbuf)	free(mbuf);
		return(-1);
	}

	// lay out the ops
	if (n_values) {
		cl_msg_op *op = (cl_msg_op *) buf;
		cl_msg_op *op_tmp;
		for (i = 0; i< n_values;i++) {
			if( values ) {
				cl_value_to_op( &values[i], operator, NULL, op);
			}else if (operations) {
				cl_value_to_op( NULL, 0, &operations[i], op);
			}
	
			op_tmp = cl_msg_op_get_next(op);
			cl_msg_swap_op(op);
			op = op_tmp;
		}
	}
	return(0);	
}

// A special version that compiles for a list of multiple digests instead of a single
// 

static int
compile_digests(uint info1, uint info2, uint info3, const char *ns, const cf_digest *digests, int n_digests, cl_bin *values, cl_operator operator,
	cl_operation *operations, int n_values, uint8_t **buf_r, size_t *buf_sz_r, const cl_write_parameters *cl_w_p)
{
	// I hate strlen
	int		ns_len = ns ? strlen(ns) : 0;
	int		i;
	
	// determine the size
	size_t	msg_sz = sizeof(as_msg); // header
	// fields
	if (ns) msg_sz += ns_len + sizeof(cl_msg_field);
	msg_sz += sizeof(cl_msg_field) + 1 + (sizeof(cf_digest) * n_digests);
	// ops
	
	for (i=0;i<n_values;i++) {
		cl_bin *tmpValue = values?&values[i]:&(operations[i].bin);
		msg_sz += sizeof(cl_msg_op) + strlen(tmpValue->bin_name);

        if (0 != cl_value_to_op_get_size(tmpValue, &msg_sz)) {
            fprintf(stderr,"illegal parameter: bad type %d write op %d\n",tmpValue->object.type,i);
            return(-1);
        }
	}
	
	
	// size too small? malloc!
	uint8_t	*buf;
	uint8_t *mbuf = 0;
	if ((*buf_r) && (msg_sz > *buf_sz_r)) {
		mbuf = buf = malloc(msg_sz);
		if (!buf) 			return(-1);
		*buf_r = buf;
	}else{
		buf = *buf_r;
	}

	*buf_sz_r = msg_sz;
	
	// debug - shouldn't be required
	memset(buf, 0, msg_sz);
	
	// lay in some parameters
	uint32_t generation = 0;
	if (cl_w_p) {
		if (cl_w_p->unique) {
			info2 |= CL_MSG_INFO2_WRITE_UNIQUE;
		} else if (cl_w_p->unique_bin) {
			info2 |= CL_MSG_INFO2_WRITE_BINUNIQUE;
		} else if (cl_w_p->use_generation) {
			info2 |= CL_MSG_INFO2_GENERATION;
			generation = cl_w_p->generation;
		} else if (cl_w_p->use_generation_gt) {
			info2 |= CL_MSG_INFO2_GENERATION_GT;
			generation = cl_w_p->generation;
		} else if (cl_w_p->use_generation_dup) {
			info2 |= CL_MSG_INFO2_GENERATION_DUP;
			generation = cl_w_p->generation;
		}
	}
	
	uint32_t record_ttl = cl_w_p ? cl_w_p->record_ttl : 0;
	uint32_t transaction_ttl = cl_w_p ? cl_w_p->timeout_ms : 0;

	// lay out the header - currently always 2, the digest array and the ns
	int n_fields = 2;
	buf = cl_write_header(buf, msg_sz, info1, info2, info3, generation, record_ttl, transaction_ttl, n_fields, 0/*n_values*/);
		
	// now the fields
	buf = write_fields_digests(buf, ns, ns_len, digests, n_digests);
	if (!buf) {
		if (mbuf)	free(mbuf);
		return(-1);
	}

	// lay out the ops
	
	if (n_values) {

		cl_msg_op *op = (cl_msg_op *) buf;
		cl_msg_op *op_tmp;
		for (i = 0; i< n_values;i++) {
			if( values ){	
				cl_value_to_op( &values[i], operator, NULL, op);
			}else{
				cl_value_to_op(NULL, 0, &operations[i], op);
			}
	
			op_tmp = cl_msg_op_get_next(op);
			cl_msg_swap_op(op);
			op = op_tmp;
		}
	}
	
	return(0);	
}


// 0 if OK, -1 if fail

static int
set_object(cl_msg_op *op, cl_object *obj)
{
	int rv = 0;
	
	obj->type = op->particle_type;
	
	switch(op->particle_type) {
		case CL_NULL:
			//fprintf(stderr, "parse: received null object\n");
			obj->sz = 0;
			obj->free = 0;
			break;
		case CL_INT:
			obj->sz = 0; // unused in integer case
			obj->free = 0;
			rv = op_to_value_int(cl_msg_op_get_value_p(op), cl_msg_op_get_value_sz(op),&(obj->u.i64));
			break;
		case CL_STR:
			obj->sz = cl_msg_op_get_value_sz(op);
			obj->free = obj->u.str = malloc(obj->sz+1);
			if (obj->free == NULL)	return(-1);
			memcpy(obj->u.str, cl_msg_op_get_value_p(op), obj->sz);
			obj->u.str[obj->sz] = 0;
			break;
		case CL_BLOB:
		case CL_JAVA_BLOB:
		case CL_CSHARP_BLOB:
		case CL_PYTHON_BLOB:
		case CL_RUBY_BLOB:
		case CL_PHP_BLOB:
			obj->sz = cl_msg_op_get_value_sz(op);
			obj->free = obj->u.blob = malloc(obj->sz);
			if (obj->free == 0)	return(-1);
			memcpy(obj->u.blob, cl_msg_op_get_value_p(op), obj->sz);
			break;
		default:
// #ifdef DEBUG	
			fprintf(stderr, "parse: received unknown object type %d",op->particle_type);
// #endif	
			return(-1);
	}
	return(rv);
}	

//
// Search through the value list and set the pre-existing correct one
// Leads ot n-squared in this section of code
// See other comment....
static int
set_value_search(cl_msg_op *op, cl_bin *values, cl_operation *operations, int n_values)
{
	// currently have to loop through the values to find the right one
	// how that sucks! it's easy to fix eventuallythough
	int i;
	cl_bin *value = 0;
	for (i=0;i<n_values;i++)
	{	
		value = values ? &(values[i]) : &(operations[i].bin);
		if (memcmp(value->bin_name, op->name, op->name_sz) == 0)
			break;
	}
	if (i == n_values) {
#ifdef DEBUG		
		fprintf(stderr, "set value: but value wasn't there to begin with. Don't understand.\n");
#endif		
		return(-1);
	}
	
	// copy
	set_object(op, &value->object);
	return(0);
}
	

//
// Copy this particular operation to that particular value
void
cl_set_value_particular(cl_msg_op *op, cl_bin *value)
{
	if (op->name_sz > sizeof(value->bin_name)) {
#ifdef DEBUG		
		fprintf(stderr, "Set Value Particular: bad response from server");
#endif	
		return;
	}
	
	memcpy(value->bin_name, op->name, op->name_sz);
	value->bin_name[op->name_sz] = 0;
	set_object(op, &value->object);
}
	



//
// parse the incoming response buffer, copy the incoming ops into the values array passed in
// It might be that the values_r is different. If so, it was malloced for you and must be freed.
//
// The caller is allows to pass values_r and n_values_r as NULL if it doesn't want those bits
// parsed out.
//

int
cl_parse(cl_msg *msg, uint8_t *buf, size_t buf_len, cl_bin **values_r, cl_operation **operations_r, int *n_values_r, uint64_t *trid)
{
	uint8_t *buf_lim = buf + buf_len;
	
	int i;
	if (msg->n_fields) {
		cl_msg_field *mf = (cl_msg_field *)buf;
		
		for (i=0;i<msg->n_fields;i++) {
			
			if (buf_lim < buf + sizeof(cl_msg_field)) {
#ifdef DEBUG
				fprintf(stderr, "parse: too short message: said there was a field, but too short\n");
#endif
				return(-1);
			}

			cl_msg_swap_field(mf);
			if (mf->type == CL_MSG_FIELD_TYPE_TRID) {
				uint64_t trid_nbo;
				//We get the transaction-id in network byte order (big-endian)
				//We should convert to host byte order
				memcpy(&trid_nbo, mf->data, sizeof(trid_nbo));
				*trid = __be64_to_cpu(trid_nbo);
				//fprintf(stderr, "Field is trid with value %"PRIu64"\n", *trid);
			}

			mf = cl_msg_field_get_next(mf);
		}
		buf = (uint8_t *) mf;
	}

	cl_msg_op *op = (cl_msg_op *)buf;
	
	// if we weren't passed in a buffer to complete, we need to make a new one
	// XXX - You've got a likely memory leak here.  If we need *more* bins than the 
	// caller has allocated to us, we allocate a larger block. But we do so by 
	// nuking the reference to the block that the caller has given us, replacing
	// it with our own (malloc'd) memory, and not telling the caller.  - CSW
	if (n_values_r && (values_r || operations_r) ) {
		if (msg->n_ops > *n_values_r) {
			// straight bin path..
			if( values_r ){								   
				cl_bin *values = malloc( sizeof(cl_bin) * msg->n_ops );
				if (values == 0) 		return(-1);
				*n_values_r = msg->n_ops;
				*values_r = values;
			}else{ // operations path 
				cl_operation *operations = (cl_operation *)malloc( sizeof(cl_operation) *msg->n_ops );
				if( operations == 0 )		return(-1);
				*n_values_r = msg->n_ops;
				*operations_r = operations;
			}
			

			// if we already have our filled-out value structure, just copy in
			for (i=0;i<msg->n_ops;i++) {
				cl_bin *value = values_r ? &((*values_r)[i]) : &((*operations_r)[i].bin);
				
				if (buf_lim < buf + sizeof(cl_msg_op)) {
#ifdef DEBUG
					fprintf(stderr, "parse: too short message: said there was ops, iteration %d, but too short\n",i);
#endif
					return(-1);
				}

				cl_msg_swap_op(op);
				
				cl_set_value_particular(op, value);
				
				op = cl_msg_op_get_next(op);
				
			}
			
		}
		else {
			// if we already have our filled-out value structure, just copy in
			for (i=0;i<msg->n_ops;i++) {

				if (buf_lim < buf + sizeof(cl_msg_op)) {
#ifdef DEBUG
					fprintf(stderr, "parse: too short message: said there was ops, iteration %d, but too short\n",i);
#endif
					return(-1);
				}

				cl_msg_swap_op(op);
				
				// This is a little peculiar. We could get a response that wasn't in the result
				// set, would be nice to throw an error
				set_value_search(op, values_r?*values_r:NULL, operations_r?*operations_r:NULL, *n_values_r);
				
				op = cl_msg_op_get_next(op);
			}
		}
	}
	
	
	return(0);
}

//
// Omnibus (!beep!! !beep!!) internal function that the externals can map to
// If you don't want any values back, pass the values and n_values pointers as null
//
// WARNING - this parsing system relied on the length of cl_msg, which is
// clumsy and against the spirit of the protocol. The length of cl_msg is specified
// in the protocol, and the length of the message is defined - it should all be used.
//
// EITHER set + key must be set, or digest must be set! not both!
//
// Similarly, either values or operations must be set, but not both.

static int
do_the_full_monte(cl_cluster *asc, int info1, int info2, int info3, const char *ns, const char *set, const cl_object *key,
	const cf_digest *digest, cl_bin **values, cl_operator operator, cl_operation **operations, int *n_values, 
	uint32_t *cl_gen, const cl_write_parameters *cl_w_p, uint64_t *trid)
{
	int rv = -1;
#ifdef DEBUG_HISTOGRAM	
    uint64_t start_time = cf_getms();
#endif	
	

	uint8_t		rd_stack_buf[STACK_BUF_SZ];	
	uint8_t		*rd_buf = rd_stack_buf;
	size_t		rd_buf_sz = 0;
    
	uint8_t		wr_stack_buf[STACK_BUF_SZ];
	uint8_t		*wr_buf = wr_stack_buf;
	size_t		wr_buf_sz = sizeof(wr_stack_buf);

	as_msg 		msg;
    
    uint        progress_timeout_ms;
	uint64_t deadline_ms;
	cl_cluster_node *node = 0;

	int fd = -1;

//	if( *values ){
//		dump_values(*values, null, *n_values);
//	}else if( *operations ){
//		dump_values(null, *operations, *n_values);
//	}	
	// 
	cf_digest d_ret;	
	if (n_values && ( values || operations) ){
		if (cl_compile(info1, info2, info3, ns, set, key, digest, values?*values:NULL, operator, operations?*operations:NULL,
				*n_values , &wr_buf, &wr_buf_sz, cl_w_p, &d_ret, *trid, NULL)) {
			return(rv);
		}
	}else{
		if (cl_compile(info1, info2, info3, ns, set, key, digest, 0, 0, 0, 0, &wr_buf, &wr_buf_sz, cl_w_p, &d_ret, *trid, NULL)) {
			return(rv);
		}
	}	

#ifdef DEBUG_VERBOSE
	dump_buf("sending request to cluster:", wr_buf, wr_buf_sz);
#endif	

	int try = 0;

#ifdef DEBUG_TIME
	uint64_t before_write_time = 0;
	uint64_t after_write_time = 0;
	uint64_t before_read_header_time = 0;
	uint64_t after_read_header_time = 0;
	uint64_t before_read_body_time = 0;
    uint64_t after_read_body_time = 0;	
#endif

    deadline_ms = 0;
    progress_timeout_ms = 0;
    if (cl_w_p && cl_w_p->timeout_ms) {
    	// policy: if asking for a long timeout, give enough time to try two servers
    	if (cl_w_p->timeout_ms > 700) {
			deadline_ms = cf_getms() + cl_w_p->timeout_ms;
			progress_timeout_ms = cl_w_p->timeout_ms / 2;
		}
		else {
			deadline_ms = cf_getms() + cl_w_p->timeout_ms;
			progress_timeout_ms = cl_w_p->timeout_ms;
		}
#ifdef DEBUG_VERBOSE        
        fprintf(stderr, "transaction has deadline: in %d deadlinems %"PRIu64" progress %d \n",
        	(int)cl_w_p->timeout_ms,deadline_ms,progress_timeout_ms);
#endif        
    }
    else {
        progress_timeout_ms = DEFAULT_PROGRESS_TIMEOUT;
    }
	
	// retry request based on the write_policy
	do {

#ifdef DEBUG_TIME
		before_write_time = 0;
		after_write_time = 0;
		before_read_header_time = 0;
		after_read_header_time = 0;
		before_read_body_time = 0;
		after_read_body_time = 0;
#endif
        
#ifdef DEBUG		
		if (try > 0)
			fprintf(stderr,"request retrying try %d tid %zu\n",try,(uint64_t)pthread_self());
#endif        
		try++;
		
		// Get an FD from a cluster
		node = cl_cluster_node_get(asc, ns, &d_ret, info2 & CL_MSG_INFO2_WRITE ? true : false);
		if (!node) {
#ifdef DEBUG
			fprintf(stderr, "warning: no healthy nodes in cluster, retrying\n");
#endif
			usleep(10000);
			goto Retry;
		}
		fd = cl_cluster_node_fd_get(node, false, asc->nbconnect);
		if (fd == -1) {
#ifdef DEBUG			
			fprintf(stderr, "warning: node %s has no file descriptors, retrying transaction (tid %zu)\n",node->name,(uint64_t)pthread_self() );
#endif			
			usleep(1000);
			goto Retry;
		}

#ifdef DEBUG_VERBOSE
		fprintf(stderr, "node %s fd %d\n",node->name, fd);
#endif		
		
		// Hate special cases, but we have to clear the verify bit on delete verify
		if ( (info2 & CL_MSG_INFO2_DELETE) && (info1 & CL_MSG_INFO1_VERIFY))
		{
			as_msg *msgp = (as_msg *)wr_buf;
			msgp->m.info1 &= ~CL_MSG_INFO1_VERIFY;
		}
		
		// send it to the cluster - non blocking socket, but we're blocking
//		fprintf(stderr, "+w1 %d\n",fd);

#ifdef DEBUG_TIME
        before_write_time = cf_getms();
#endif
		rv = cf_socket_write_timeout(fd, wr_buf, wr_buf_sz, deadline_ms, progress_timeout_ms);
#ifdef DEBUG_TIME
        after_write_time = cf_getms();
#endif
//		fprintf(stderr, "-w1 %d\n",fd);

		if (rv != 0) {
#ifdef DEBUG			
			fprintf(stderr, "Citrusleaf: write timeout or error when writing header to server - %d fd %d errno %d (tid %zu)\n",rv,fd,errno,(uint64_t)pthread_self());
#endif
#ifdef DEBUG_TIME
            debug_printf(before_write_time, after_write_time, before_read_header_time, after_read_header_time, before_read_body_time, after_read_body_time,
                         deadline_ms, progress_timeout_ms);           	
#endif

			cl_cluster_node_dun(node, rv == ETIMEDOUT ? NODE_DUN_TIMEOUT : NODE_DUN_NET_ERR);
			goto Retry;
		}

#ifdef DEBUG		
		memset(&msg, 0, sizeof(as_msg));
#endif
#ifdef DEBUG_TIME
        before_read_header_time = cf_getms();
#endif		
		
		// Now turn around and read into this fine cl_msg, which is the short header
		rv = cf_socket_read_timeout(fd, (uint8_t *) &msg, sizeof(as_msg), deadline_ms, progress_timeout_ms);
#ifdef DEBUG_TIME
        after_read_header_time = cf_getms();
#endif

		if (rv) {

#ifdef DEBUG            
			fprintf(stderr, "Citrusleaf: error when reading header from server - rv %d fd %d\n",rv,fd);
#endif
#ifdef DEBUG_TIME
            debug_printf(before_write_time, after_write_time, before_read_header_time, after_read_header_time, before_read_body_time, after_read_body_time,
                         deadline_ms, progress_timeout_ms);           	
#endif            

			cl_cluster_node_dun(node, rv == ETIMEDOUT ? NODE_DUN_TIMEOUT : NODE_DUN_NET_ERR);
			goto Retry;
	
		}
#ifdef DEBUG_VERBOSE
		dump_buf("read header from cluster", (uint8_t *) &msg, sizeof(cl_msg));
#endif	
		cl_proto_swap(&msg.proto);
		cl_msg_swap_header(&msg.m);

		if (/*(info1 & CL_MSG_INFO1_READ) &&*/ cl_gen) {
			*cl_gen = msg.m.generation;
		}
	
		
		// second read for the remainder of the message - expect this to cover everything requested
		// if there's no error
		rd_buf_sz =  msg.proto.sz  - msg.m.header_sz;
		if (rd_buf_sz > 0) {
			if (rd_buf_sz > sizeof(rd_stack_buf)) {
				rd_buf = malloc(rd_buf_sz);
				if (!rd_buf) {
                    fprintf(stderr, "malloc fail: trying %zu\n",rd_buf_sz);
                    rv = -1; 
                    goto Error; 
                }
			}
#ifdef DEBUG_TIME
        before_read_body_time = cf_getms();
#endif            
			rv = cf_socket_read_timeout(fd, rd_buf, rd_buf_sz, deadline_ms, progress_timeout_ms);
#ifdef DEBUG_TIME
        after_read_body_time = cf_getms();
#endif
			if (rv) {
				if (rd_buf != rd_stack_buf) { free(rd_buf); }
                rd_buf = 0;
                
#ifdef DEBUG            
                fprintf(stderr, "Citrusleaf: error when reading from server - rv %d fd %d\n",rv,fd);
#endif
#ifdef DEBUG_TIME
				debug_printf(before_write_time, after_write_time, before_read_header_time, after_read_header_time, before_read_body_time, after_read_body_time, 
                             deadline_ms, progress_timeout_ms);           	
#endif

				cl_cluster_node_dun(node, rv == ETIMEDOUT ? NODE_DUN_TIMEOUT : NODE_DUN_NET_ERR);
				goto Retry;
			}

#ifdef DEBUG_VERBOSE
			dump_buf("read body from cluster", rd_buf, rd_buf_sz);
#endif	
			
			// todo: check error!!?!?!
		}

        goto Ok;
		
Retry:		

		if (fd != -1) {
			close(fd);
			fd = -1;
		}

		if (node) {
            cl_cluster_node_put(node); 
            node = 0; 
        }

        if (deadline_ms && (deadline_ms < cf_getms() ) ) {
#ifdef DEBUG            
            fprintf(stderr, "out of luck out of time : deadline %"PRIu64" now %"PRIu64"\n",
                deadline_ms, cf_getms());
#endif            
            rv = CITRUSLEAF_FAIL_TIMEOUT;
            goto Error;
        }
		
	} while ( (cl_w_p == 0) || (cl_w_p->w_pol == CL_WRITE_RETRY) );
	
Error:	
	
#ifdef DEBUG	
	fprintf(stderr, "exiting with failure: wpol %d timeleft %d rv %d\n",
		(int)(cl_w_p ? cl_w_p->w_pol : 0),
		(int)(deadline_ms - cf_getms() ), rv );
#endif	

    if (fd != -1)   close(fd);

	if (wr_buf != wr_stack_buf)		free(wr_buf);
	if (rd_buf && (rd_buf != rd_stack_buf))		free(rd_buf);
	
	return(rv);
    
Ok:    

	cl_cluster_node_ok(node);
    cl_cluster_node_fd_put(node, fd, false);
	cl_cluster_node_put(node);
   
	if (wr_buf != wr_stack_buf)		free(wr_buf);

	if (rd_buf) {
		if (0 != cl_parse(&msg.m, rd_buf, rd_buf_sz, values, operations, n_values, trid)) {
			rv = CITRUSLEAF_FAIL_UNKNOWN;
		}
		else {
			rv = msg.m.result_code;
			// special case: if there was a retry, and we're doing a delete, force 'not found'
			// errors to 'ok' because the first delete might have succeeded
			if ((try > 1) && (rv == 2) && (info2 & CL_MSG_INFO2_DELETE)) {
//				dump_key("delete: stomping error code on retransmitted delete", key);
				rv = 0;
			}
		}
    }
    else {
        rv = CITRUSLEAF_FAIL_UNKNOWN;
    }    
	if (rd_buf && (rd_buf != rd_stack_buf))		free(rd_buf);
	
	if (rv == 0 && (values || operations) && n_values) {
		for (int i=0;i<*n_values;i++) {
			cl_bin *bin = values? &(*values)[i] : &((*operations)[i].bin);
//			if ( bin->object.type == CL_NULL ) {
//				fprintf(stderr, " return object with null type: name %s digest %"PRIx64"\n",bin->bin_name, *(uint64_t *) &d_ret);
//				// raise(SIGINT);
//			}
		}
	}
#ifdef DEBUG_HISTOGRAM	
    cf_histogram_insert_data_point(cf_hist, start_time);
#endif	
#ifdef DEBUG_VERBOSE
	if (rv != 0) {
		fprintf(stderr, "exiting OK clause with failure: wpol %d timeleft %d rv %d\n",
			(int)(cl_w_p ? cl_w_p->w_pol : 0),
			(int)(deadline_ms - cf_getms() ), rv );
	}
#endif	

	return(rv);

}


//
// head functions
//



extern cl_rv
citrusleaf_get(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, cl_bin *values, int n_values, int timeout_ms, uint32_t *cl_gen)
{
    if (!g_initialized) return(-1);

    	uint64_t trid=0;
	cl_write_parameters cl_w_p;
	cl_write_parameters_set_default(&cl_w_p);
	cl_w_p.timeout_ms = timeout_ms;

	return( do_the_full_monte( asc, CL_MSG_INFO1_READ, 0, 0, ns, set, key, 0, &values, CL_OP_READ, 0, &n_values, cl_gen, &cl_w_p, &trid) );
}

extern cl_rv
citrusleaf_get_digest(cl_cluster *asc, const char *ns, const cf_digest *digest, cl_bin *values, int n_values, int timeout_ms, uint32_t *cl_gen)
{
	
    if (!g_initialized) return(-1);
    
    	uint64_t trid=0;
	cl_write_parameters cl_w_p;
	cl_write_parameters_set_default(&cl_w_p);
	cl_w_p.timeout_ms = timeout_ms;

	return( do_the_full_monte( asc, CL_MSG_INFO1_READ, 0, 0, ns, 0,0, digest, &values, CL_OP_READ, 0, &n_values, cl_gen, &cl_w_p, &trid) );
}


extern cl_rv
citrusleaf_put(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, const cl_bin *values, int n_values, const cl_write_parameters *cl_w_p)
{
    if (!g_initialized) return(-1);

    	uint64_t trid=0;
	return( do_the_full_monte( asc, 0, CL_MSG_INFO2_WRITE, 0, ns, set, key, 0, (cl_bin **) &values, CL_OP_WRITE, 0, &n_values, NULL, cl_w_p, &trid) );
}

extern cl_rv
citrusleaf_put_digest(cl_cluster *asc, const char *ns, const cf_digest *digest, const cl_bin *values, int n_values, const cl_write_parameters *cl_w_p)
{
    if (!g_initialized) return(-1);

    
    	uint64_t trid=0;
	return( do_the_full_monte( asc, 0, CL_MSG_INFO2_WRITE, 0, ns, 0, 0, digest, (cl_bin **) &values, CL_OP_WRITE, 0, &n_values, NULL, cl_w_p, &trid) );
}

cl_rv
citrusleaf_put_replace(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, const cl_bin *values, int n_values, const cl_write_parameters *cl_w_p)
{
    if (!g_initialized) return(-1);

    uint64_t trid=0;
	return( do_the_full_monte( asc, 0, CL_MSG_INFO2_WRITE, CL_MSG_INFO3_REPLACE, ns, set, key, 0, (cl_bin **) &values, CL_OP_WRITE, 0, &n_values, NULL, cl_w_p, &trid) );
}

extern cl_rv
citrusleaf_restore(cl_cluster *asc, const char *ns, const cf_digest *digest, const char *set, const cl_bin *values, int n_values, const cl_write_parameters *cl_w_p)
{
    if (!g_initialized) return(-1);

    uint64_t trid=0;
	return( do_the_full_monte( asc, 0, CL_MSG_INFO2_WRITE, 0, ns, set, 0, digest, (cl_bin **) &values, CL_OP_WRITE, 0, &n_values, NULL, cl_w_p, &trid) );
}

extern cl_rv
citrusleaf_async_put(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, const cl_bin *values, 
			int n_values, const cl_write_parameters *cl_w_p, uint64_t trid, void *udata)
{
	if (!g_initialized) return(-1);

	//Hardcoding to say that the client is XDS(in info1 bitmap). 
	//If this is used by some other clients in the future, we should parameterize it.
	return( cl_do_async_monte( asc, CL_MSG_INFO1_XDS, CL_MSG_INFO2_WRITE, ns, set, key, 0, (cl_bin **) &values,
					CL_OP_WRITE, 0, &n_values, NULL, cl_w_p, &trid, udata) ); 
}

extern cl_rv
citrusleaf_async_put_digest(cl_cluster *asc, const char *ns, const cf_digest *digest, const cl_bin *values, 
			int n_values, const cl_write_parameters *cl_w_p, uint64_t trid, void *udata)
{
	if (!g_initialized) return(-1);

	//Hardcoding to say that the client is XDS(in info1 bitmap). 
	//If this is used by some other clients in the future, we should parameterize it.
	return( cl_do_async_monte( asc, CL_MSG_INFO1_XDS, CL_MSG_INFO2_WRITE, ns, 0, 0, digest, (cl_bin **) &values,
					CL_OP_WRITE, 0, &n_values, NULL, cl_w_p, &trid, udata) ); 
}


extern cl_rv
citrusleaf_delete(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, const cl_write_parameters *cl_w_p)
{
    if (!g_initialized) return(-1);
    
    	uint64_t trid=0;
	return( do_the_full_monte( asc, 0, CL_MSG_INFO2_DELETE | CL_MSG_INFO2_WRITE, 0, ns, set, key, 0, 0, 0, 0, 0, NULL, cl_w_p, &trid) );
}

extern cl_rv
citrusleaf_delete_digest(cl_cluster *asc, const char *ns, const cf_digest *digest, const cl_write_parameters *cl_w_p)
{
    if (!g_initialized) return(-1);
    
    	uint64_t trid=0;
	return( do_the_full_monte( asc, 0, CL_MSG_INFO2_DELETE | CL_MSG_INFO2_WRITE, 0, ns, 0, 0, digest, 0, 0, 0, 0, NULL, cl_w_p, &trid) );
}


//
// Efficiently determine if the key exists.
//  (Note:  The bins are currently ignored but may be testable in the future.)
//

extern cl_rv
citrusleaf_exists_key(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, cl_bin *values, int n_values, int timeout_ms, uint32_t *cl_gen)
{
    if (!g_initialized) return(-1);

    	uint64_t trid=0;
	cl_write_parameters cl_w_p;
	cl_write_parameters_set_default(&cl_w_p);
	cl_w_p.timeout_ms = timeout_ms;

	return( do_the_full_monte( asc, CL_MSG_INFO1_READ | CL_MSG_INFO1_NOBINDATA, 0, 0, ns, set, key, 0, &values, CL_OP_READ, 0, &n_values, cl_gen, &cl_w_p, &trid) );
}

extern cl_rv
citrusleaf_exists_digest(cl_cluster *asc, const char *ns, const cf_digest *digest, cl_bin *values, int n_values, int timeout_ms, uint32_t *cl_gen)
{
	
    if (!g_initialized) return(-1);
    
    	uint64_t trid=0;
	cl_write_parameters cl_w_p;
	cl_write_parameters_set_default(&cl_w_p);
	cl_w_p.timeout_ms = timeout_ms;

	return( do_the_full_monte( asc, CL_MSG_INFO1_READ | CL_MSG_INFO1_NOBINDATA, 0, 0, ns, 0,0, digest, &values, CL_OP_READ, 0, &n_values, cl_gen, &cl_w_p, &trid) );
}


extern cl_rv
citrusleaf_get_all(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, cl_bin **values, int *n_values, int timeout_ms, uint32_t *cl_gen)
{
	if ((values == 0) || (n_values == 0)) {
		fprintf(stderr, "citrusleaf_get_all: illegal parameters passed\n");
		return(-1);
	}
    if (!g_initialized) return(-1);

	*values = 0;
	*n_values = 0;

    	uint64_t trid=0;
	cl_write_parameters cl_w_p;
	cl_write_parameters_set_default(&cl_w_p);
	cl_w_p.timeout_ms = timeout_ms;
	
	return( do_the_full_monte( asc, CL_MSG_INFO1_READ | CL_MSG_INFO1_GET_ALL, 0, 0, ns, set, key, 0/*dig*/, values, CL_OP_READ, 0, n_values, cl_gen, &cl_w_p, &trid) );
}

extern cl_rv
citrusleaf_get_all_digest(cl_cluster *asc, const char *ns, const cf_digest *digest, cl_bin **values, int *n_values, int timeout_ms, uint32_t *cl_gen)
{
	if ((values == 0) || (n_values == 0)) {
		fprintf(stderr, "citrusleaf_get_all: illegal parameters passed\n");
		return(-1);
	}
    if (!g_initialized) return(-1);

	*values = 0;
	*n_values = 0;

    	uint64_t trid=0;
	cl_write_parameters cl_w_p;
	cl_write_parameters_set_default(&cl_w_p);
	cl_w_p.timeout_ms = timeout_ms;
	
	return( do_the_full_monte( asc, CL_MSG_INFO1_READ | CL_MSG_INFO1_GET_ALL, 0, 0, ns, 0, 0, digest, values, CL_OP_READ, 0, n_values, cl_gen, &cl_w_p, &trid) );
}


extern cl_rv
citrusleaf_verify(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, const cl_bin *values, int n_values, int timeout_ms, uint32_t *cl_gen)
{
    if (!g_initialized) return(-1);
    

    	uint64_t trid=0;
	cl_write_parameters cl_w_p;
	cl_write_parameters_set_default(&cl_w_p);
	cl_w_p.timeout_ms = timeout_ms;
	
	return( do_the_full_monte( asc, CL_MSG_INFO1_READ | CL_MSG_INFO1_VERIFY, 0, 0, ns, set, key, 0, (cl_bin **) &values, CL_OP_READ, 0, &n_values, cl_gen, &cl_w_p, &trid) );
}

extern cl_rv
citrusleaf_delete_verify(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, const cl_write_parameters *cl_w_p)
{
    if (!g_initialized) return(-1);
    
    	uint64_t trid=0;
	return( do_the_full_monte( asc, CL_MSG_INFO1_VERIFY, CL_MSG_INFO2_DELETE | CL_MSG_INFO2_WRITE, 0, ns, set, key, 0, 0, 0, 0, 0, NULL, cl_w_p, &trid) );
}

extern int
citrusleaf_calculate_digest(const char *set, const cl_object *key, cf_digest *digest)
{
	int set_len = set ? strlen(set) : 0;
	
	// make the key as it's laid out for digesting
	// THIS IS A STRIPPED DOWN VERSION OF THE CODE IN write_fields ABOVE
	// MUST STAY IN SYNC!!!
	uint8_t k[key->sz + 1];
	switch (key->type) {
		case CL_STR:
			k[0] = key->type;
			memcpy(&k[1], key->u.str, key->sz);
			break;
		case CL_INT:
			k[0] = key->type;
			value_to_op_int(key->u.i64,&k[1]);
			break;
		case CL_BLOB:
		case CL_JAVA_BLOB:
		case CL_CSHARP_BLOB:
		case CL_PYTHON_BLOB:
		case CL_RUBY_BLOB:
		case CL_PHP_BLOB:
			k[0] = key->type;
			memcpy(&k[1], key->u.blob, key->sz);
			break;
		default:
//	#ifdef DEBUG		
			fprintf(stderr, " transmit key: unknown citrusleaf type %d",key->type);
//	#endif		
			return(-1);
	}

	cf_digest_compute2((char *)set, set_len, k, key->sz + 1, digest);
	
	return(0);
}


//
// operate allows the caller to specify any set of operations on any record.
// any bin. It can't be used to operate and 'get many' in the response, though.
//

extern cl_rv
citrusleaf_operate(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, cl_operation *operations, int n_operations, const cl_write_parameters *cl_w_p,  int replace, uint32_t *generation)
{
    if (!g_initialized) return(-1);
    
	// see if there are any read or write bits ---
	//   (this is slightly obscure c usage....)
	int info1 = 0, info2 = 0, info3 = 0;
	uint64_t trid=0;

	for (int i=0;i<n_operations;i++) {
		switch (operations[i].op) {
		case CL_OP_WRITE:
		case CL_OP_MC_INCR:
		case CL_OP_INCR:
		case CL_OP_APPEND:
		case CL_OP_PREPEND:
		case CL_OP_MC_APPEND:
		case CL_OP_MC_PREPEND:
		case CL_OP_MC_TOUCH:
		case CL_OP_TOUCH:
			info2 = CL_MSG_INFO2_WRITE;
			break;
		case CL_OP_READ:
			info1 = CL_MSG_INFO1_READ;
			break;
		default:
			break;
		}
		
		if (info1 && info2) break;
	}
	

	if (replace)
		info3 = CL_MSG_INFO3_REPLACE;

	return( do_the_full_monte( asc, info1, info2, info3, ns, set, key, 0, 0, 0, &operations, &n_operations, generation, cl_w_p, &trid) );
}

extern int citrusleaf_cluster_init();

void citrusleaf_set_debug(bool debug_flag) 
{
   g_cl_turn_debug_on = debug_flag;
}

int citrusleaf_init() 
{

 	citrusleaf_batch_init();

	citrusleaf_cluster_init();

#ifdef DEBUG_HISTOGRAM	
    if (NULL == (cf_hist = cf_histogram_create("transaction times")))
        fprintf(stderr, "couldn't create histogram for client");
#endif	

    g_initialized = true;
    
	return(0);
}

void citrusleaf_shutdown(void) {

	if (g_initialized == false)	return;

	citrusleaf_cluster_shutdown();
	// citrusleaf_batch_shutdown();
	// citrusleaf_info_shutdown();

	g_initialized = false;

}

extern void citrusleaf_print_stats();

void citrusleaf_print_stats(void)
{
#ifdef DEBUG_HISTOGRAM
        cf_histogram_dump(cf_hist);
#endif
}
