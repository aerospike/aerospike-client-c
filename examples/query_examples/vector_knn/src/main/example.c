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
// NOTE: requires a server release that supports the VectorExp distance
// operators, the EXP_RTYPE_VECTOR bin type, and the ORDER_BY/TOP_K query
// fields with a read-expression ops projection.
//

//==========================================================
// Includes
//

#include <inttypes.h>
#include <stdint.h>
#include <stdlib.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_query.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_error.h>
#include <aerospike/as_exp_operations.h>
#include <aerospike/as_key.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_query.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>
#include <aerospike/as_vector_value.h>

#include "example_utils.h"

//==========================================================
// Constants
//

#define N_RECORDS 20 // records (each with a "vec" bin)
#define DIM 4         // vector dimension
#define TOP_K 5       // neighbors to return

#define VEC_BIN "vec" // the stored vector bin
#define VAL_BIN "val" // the projected per-record distance/score
#define ID_BIN "id"   // a human-friendly record identifier

//==========================================================
// Forward Declarations
//

static void cleanup(aerospike* p_as);
static bool insert_records(aerospike* p_as);
static as_status query_knn(aerospike* p_as, as_error* err, as_vector_distance_metric metric,
		as_order direction, const float* query_vec, const char* label);
static bool print_neighbor_cb(const as_val* p_val, void* udata);

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

	if (query_knn(&as, &err, AS_VECTOR_DISTANCE_EUCLIDEAN_SQUARED, AS_ORDER_ASCENDING, q_near,
			"euclideanDistance ASC (nearest to 7.3)") != AEROSPIKE_OK) {
		LOG("euclidean KNN failed: %d - %s", err.code, err.message);
		cleanup(&as);
		exit(-1);
	}

	// Best by dot product with (1,1,1,1): dot = 4*i, larger is better, so order
	// descending. Expected ids: 19, 18, 17, 16, 15.
	float q_dot[DIM] = { 1.f, 1.f, 1.f, 1.f };

	if (query_knn(&as, &err, AS_VECTOR_DISTANCE_DOT_PRODUCT, AS_ORDER_DESCENDING, q_dot,
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

		as_vector_value* vec = as_vector_value_new_float32(vals, DIM);

		as_record rec;
		as_record_inita(&rec, 2);
		as_record_set_int64(&rec, ID_BIN, (int64_t)i);

		if (! vec || ! as_record_set_vector(&rec, VEC_BIN, vec)) {
			LOG("failed to create vector for record %u", i);
			as_vector_value_destroy(vec);
			as_record_destroy(&rec);
			return false;
		}
		as_vector_value_destroy(vec);

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
query_knn(aerospike* p_as, as_error* err, as_vector_distance_metric metric, as_order direction,
		const float* query_vec, const char* label)
{
	as_vector_value* qvec = as_vector_value_new_float32(query_vec, DIM);
	as_bytes qbytes;

	if (! qvec || ! as_vector_value_to_bytes(qvec, &qbytes)) {
		as_vector_value_destroy(qvec);
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT,
				"Failed to serialize query vector");
	}

	as_exp_build(exp,
		as_exp_vector_dist(metric, as_bytes_get(&qbytes), as_bytes_size(&qbytes),
			as_exp_bin_vector(VEC_BIN)));

	as_bytes_destroy(&qbytes);
	as_vector_value_destroy(qvec);

	// ops projection: compute the distance into "val", and read "id" back so we
	// can identify each neighbor. Ownership passes to the query.
	as_operations* ops = as_operations_new(2);
	as_operations_exp_read(ops, VAL_BIN, exp, AS_EXP_READ_DEFAULT);
	as_operations_add_read(ops, ID_BIN);

	as_exp_destroy(exp); // as_operations_exp_read copied the packed expression.

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
