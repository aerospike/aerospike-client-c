/*******************************************************************************
 * Copyright 2008-2026 by Aerospike.
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
 ******************************************************************************/

//==========================================================
// Vector K-Nearest-Neighbor (KNN) search example.
//
// Demonstrates vector search end to end, built entirely from primitives that
// already exist in the client:
//
//   KNN  ==  ORDER BY <distance-expression> LIMIT k
//
// Each record stores a native VECTOR bin ("vec"). A foreground query attaches
// a read-expression op that, per record, computes the distance between that
// record's vector and a fixed query vector and projects it into a scalar bin
// ("val"). The query then orders by that projected bin and keeps only the best
// k - so the server returns the k nearest neighbors, fully ranked, without the
// caller sorting anything.
//
// The distance is a plain VectorExp operator, so KNN reuses the exact same
// Top-K machinery that orders by any scalar bin. Two metrics are shown:
//
//   euclideanDistance(vectorBin, queryVec)  -> smaller is nearer -> ORDER BY ASC
//   dotProduct(vectorBin, queryVec)         -> larger is better  -> ORDER BY DESC
//
// The expression is packed inline as msgpack (no extra library surface), so
// this example is self-contained.
//
// NOTE: requires a server release that supports the VectorExp distance
// operators, the EXP_RTYPE_VECTOR bin type, and the ORDER_BY/TOP_K query
// fields with a read-expression ops projection.
//

//==========================================================
// Includes
//

#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_query.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_error.h>
#include <aerospike/as_exp_operations.h>
#include <aerospike/as_key.h>
#include <aerospike/as_msgpack.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_query.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>

#include "example_utils.h"

//==========================================================
// Constants
//

#define N_RECORDS 20 // records (each with a "vec" bin)
#define DIM 4         // vector dimension
#define TOP_K 5       // neighbors to return

// VECTOR wire format (vector_flat): an 8-byte little-endian header followed by
// the element array. Matches the server's on-wire layout.
#define VEC_FORMAT_VERSION 1
#define VEC_ELEMENT_FLOAT 3 // element type: 32-bit float

// Expression wire opcodes / result types (packed inline). These are the server
// exp_op_code / exp_rtype constants the VectorExp feature added.
#define EXP_CODE_BIN 81           // reference a bin
#define EXP_TYPE_VECTOR 10        // read a bin as a VECTOR (EXP_RTYPE_VECTOR)
#define EXP_VECTOR_EUCLIDEAN 52   // euclideanDistance(a, b)  (squared L2)
#define EXP_VECTOR_DOT_PRODUCT 53 // dotProduct(a, b)
#define EXP_VECTOR_COSINE_SIM 54  // cosineSimilarity(a, b)

#define VEC_BIN "vec" // the stored vector bin
#define VAL_BIN "val" // the projected per-record distance/score
#define ID_BIN "id"   // a human-friendly record identifier

//==========================================================
// Forward Declarations
//

static void cleanup(aerospike* p_as);
static bool insert_records(aerospike* p_as);
static as_status query_knn(aerospike* p_as, as_error* err, int op_code,
		as_order direction, const float* query_vec, const char* label);
static bool print_neighbor_cb(const as_val* p_val, void* udata);

//==========================================================
// Vector helpers
//

// Serialize a FLOAT-element vector into buf (vector_flat layout); returns its
// byte length. buf must hold 8 + dims * 4 bytes.
static uint32_t
build_vector(uint8_t* buf, const float* vals, uint32_t dims)
{
	buf[0] = VEC_FORMAT_VERSION;
	buf[1] = VEC_ELEMENT_FLOAT;
	memcpy(buf + 2, &dims, sizeof(dims)); // dimensions (LE uint32)
	buf[6] = 0;                           // reserved
	buf[7] = 0;
	memcpy(buf + 8, vals, dims * sizeof(float));

	return (uint32_t)(8 + dims * sizeof(float));
}

// Build the read-expression: <op>(vectorBin("vec"), val(query_vec)). Packed
// inline as msgpack: [op, [BIN, VECTOR, "vec"], <blob query vector>]. The query
// vector is a BLOB literal carrying the raw vector_flat bytes (as_pack_bytes
// frames it with the AS_BYTES_BLOB tag the server strips). Returns a heap
// as_exp the caller must free.
static as_exp*
build_distance_exp(int op_code, const uint8_t* query_vec, uint32_t query_vec_sz)
{
	uint8_t buf[512];
	as_packer pk = { .buffer = buf, .capacity = (uint32_t)sizeof(buf) };

	as_pack_list_header(&pk, 3); // op + 2 operands
	as_pack_int64(&pk, op_code);

	// operand 0: vectorBin("vec") -> [BIN, EXP_RTYPE_VECTOR, name]
	as_pack_list_header(&pk, 3);
	as_pack_int64(&pk, EXP_CODE_BIN);
	as_pack_int64(&pk, EXP_TYPE_VECTOR);
	as_pack_str(&pk, (const uint8_t*)VEC_BIN, (uint32_t)strlen(VEC_BIN));

	// operand 1: val(query_vec) -> BLOB literal (vector_flat bytes)
	as_pack_bytes(&pk, query_vec, query_vec_sz);

	as_exp* exp = (as_exp*)malloc(sizeof(as_exp) + pk.offset);
	exp->packed_sz = pk.offset;
	memcpy(exp->packed, buf, pk.offset);
	return exp;
}

//==========================================================
// KNN Example
//

int
main(int argc, char* argv[])
{
	// Parse command line arguments.
	if (! example_get_opts(argc, argv, EXAMPLE_MULTI_KEY_OPTS)) {
		exit(-1);
	}

	// Connect to the aerospike database cluster.
	aerospike as;
	example_connect_to_aerospike(&as);

	// Start clean.
	example_remove_test_records(&as);

	if (! insert_records(&as)) {
		cleanup(&as);
		exit(-1);
	}

	as_error err;

	// Nearest by Euclidean distance to (7.3, 7.3, 7.3, 7.3): smaller is nearer,
	// so order ascending. Expected ids by |i - 7.3|: 7, 8, 6, 9, 5.
	float q_near[DIM] = { 7.3f, 7.3f, 7.3f, 7.3f };

	if (query_knn(&as, &err, EXP_VECTOR_EUCLIDEAN, AS_ORDER_ASCENDING, q_near,
			"euclideanDistance ASC (nearest to 7.3)") != AEROSPIKE_OK) {
		LOG("euclidean KNN failed: %d - %s", err.code, err.message);
		cleanup(&as);
		exit(-1);
	}

	// Best by dot product with (1,1,1,1): dot = 4*i, larger is better, so order
	// descending. Expected ids: 19, 18, 17, 16, 15.
	float q_dot[DIM] = { 1.f, 1.f, 1.f, 1.f };

	if (query_knn(&as, &err, EXP_VECTOR_DOT_PRODUCT, AS_ORDER_DESCENDING, q_dot,
			"dotProduct DESC (largest projection)") != AEROSPIKE_OK) {
		LOG("dot-product KNN failed: %d - %s", err.code, err.message);
		cleanup(&as);
		exit(-1);
	}

	// Cleanup and disconnect from the database cluster.
	cleanup(&as);

	LOG("vector KNN example successfully completed");
	return 0;
}

//==========================================================
// Helpers
//

static void
cleanup(aerospike* p_as)
{
	example_remove_test_records(p_as);
	example_cleanup(p_as);
}

// Insert N_RECORDS records, each with an "id" bin and a native VECTOR bin
// "vec". Record i holds the vector (i, i, i, i), so nearest-neighbor results
// are easy to verify by inspection.
static bool
insert_records(aerospike* p_as)
{
	for (uint32_t i = 0; i < N_RECORDS; i++) {
		float vals[DIM];

		for (uint32_t d = 0; d < DIM; d++) {
			vals[d] = (float)i;
		}

		uint8_t vec[8 + DIM * sizeof(float)];
		uint32_t vec_sz = build_vector(vec, vals, DIM);

		as_record rec;
		as_record_inita(&rec, 2);
		as_record_set_int64(&rec, ID_BIN, (int64_t)i);

		// Write "vec" as a native VECTOR bin (particle type AS_BYTES_VECTOR).
		as_record_set_raw_typep(&rec, VEC_BIN, vec, vec_sz, AS_BYTES_VECTOR,
				false);

		as_key key;
		as_key_init_int64(&key, g_namespace, g_set, (int64_t)i);

		as_error err;

		if (aerospike_key_put(p_as, &err, NULL, &key, &rec) != AEROSPIKE_OK) {
			LOG("aerospike_key_put() returned %d - %s", err.code, err.message);
			as_record_destroy(&rec);
			return false;
		}

		as_record_destroy(&rec);
	}

	LOG("inserted %d records, each with a %d-dim VECTOR bin", N_RECORDS, DIM);
	return true;
}

// Run one KNN query: project <op>(vectorBin, query_vec) into "val" via a
// read-expression op, ORDER BY "val" in the given direction, LIMIT TOP_K.
static as_status
query_knn(aerospike* p_as, as_error* err, int op_code, as_order direction,
		const float* query_vec, const char* label)
{
	uint8_t qbuf[8 + DIM * sizeof(float)];
	uint32_t qsz = build_vector(qbuf, query_vec, DIM);

	as_exp* exp = build_distance_exp(op_code, qbuf, qsz);

	// ops projection: compute the distance into "val", and read "id" back so we
	// can identify each neighbor. Ownership passes to the query.
	as_operations* ops = as_operations_new(2);
	as_operations_exp_read(ops, VAL_BIN, exp, AS_EXP_READ_DEFAULT);
	as_operations_add_read(ops, ID_BIN);

	free(exp); // as_operations_exp_read copied the packed expression.

	as_query query;
	as_query_init(&query, g_namespace, g_set);
	query.ops = ops; // destroyed by as_query_destroy().

	as_query_order_by(&query, VAL_BIN, AS_QUERY_ORDER_BY_DOUBLE, direction,
			AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
	as_query_top_k(&query, TOP_K);

	LOG("KNN: %s, top %d", label, TOP_K);

	int rank = 0;
	as_status status = aerospike_query_foreach(p_as, err, NULL, &query,
			print_neighbor_cb, &rank);

	as_query_destroy(&query);
	return status;
}

static bool
print_neighbor_cb(const as_val* p_val, void* udata)
{
	int* rank = (int*)udata;

	if (! p_val) {
		return true; // query complete
	}

	as_record* rec = as_record_fromval(p_val);

	if (! rec) {
		return true;
	}

	int64_t id = as_record_get_int64(rec, ID_BIN, -1);
	double val = as_record_get_double(rec, VAL_BIN, 0.0);

	LOG("  #%d: id=%" PRId64 " %s=%.4f", ++(*rank), id, VAL_BIN, val);
	return true;
}
