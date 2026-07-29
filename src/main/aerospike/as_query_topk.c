/*
 * Copyright 2008-2026 Aerospike, Inc.
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
#include <aerospike/as_query_topk.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_double.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_msgpack.h>
#include <aerospike/as_record.h>
#include <aerospike/as_string.h>
#include <aerospike/as_val.h>
#include <aerospike/as_vector.h>
#include <citrusleaf/alloc.h>
#include <ctype.h>
#include <pthread.h>
#include <string.h>

//---------------------------------
// Types
//---------------------------------

typedef enum {
	AS_QUERY_TOPK_SYNC,
	AS_QUERY_TOPK_ASYNC
} as_query_topk_kind;

struct as_query_topk_collector_s {
	as_query_order_by_field order_by;
	uint32_t k;
	as_query_topk_kind kind;

	union {
		struct {
			aerospike_query_foreach_callback callback;
			void* udata;
		} sync;

		struct {
			as_async_query_record_listener listener;
			void* udata;
		} async;
	} real;

	// Protects entries. Multiple node threads (sync: thread pool workers,
	// async: event loops) may append concurrently while the underlying
	// query is in flight.
	pthread_mutex_t lock;

	// as_record* - one heap-owned, independently-copied record per
	// collected candidate. Bounded by (n_nodes * k) since each node's batch
	// is already trimmed to <= k server-side.
	as_vector entries;
};

typedef struct {
	bool is_nil;
	union {
		int64_t i;
		double d;
		struct { const char* p; size_t len; } str;
		struct { const uint8_t* p; uint32_t len; } bytes;
	} v;
} as_query_topk_key;

//---------------------------------
// Deep copy: detach a record from the caller's (about to be destroyed)
// stack frame by round-tripping every bin/key value through msgpack. This
// mirrors how this client already handles arbitrary as_val payloads
// elsewhere (query UDF arglist serialization, as_query_from_bytes()) and,
// unlike a manual field-by-field union copy, works correctly for every
// as_val subtype a bin can hold (scalars, strings, bytes, lists, maps)
// without needing type-specific ownership-transfer logic.
//---------------------------------

static as_status
as_query_topk_copy_val(const as_val* src, as_val** dst, as_error* err)
{
	if (! src || src->type == AS_NIL) {
		*dst = (as_val*)&as_nil;
		return AEROSPIKE_OK;
	}

	as_serializer ser;
	as_msgpack_init(&ser);

	as_buffer buf;
	as_buffer_init(&buf);

	as_serializer_serialize(&ser, (as_val*)src, &buf);

	as_val* val = NULL;
	int rv = as_serializer_deserialize(&ser, &buf, &val);

	as_buffer_destroy(&buf);
	as_serializer_destroy(&ser);

	if (rv != 0 || ! val) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT,
			"Failed to copy Top-K candidate record value");
	}

	*dst = val;
	return AEROSPIKE_OK;
}

static as_record*
as_query_topk_copy_record(const as_record* src, as_error* err)
{
	as_record* dst = as_record_new(src->bins.size);

	if (! dst) {
		as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to allocate Top-K candidate record");
		return NULL;
	}

	dst->gen = src->gen;
	dst->ttl = src->ttl;
	as_strncpy(dst->key.ns, src->key.ns, sizeof(dst->key.ns));
	as_strncpy(dst->key.set, src->key.set, sizeof(dst->key.set));
	dst->key.digest = src->key.digest;

	if (src->key.valuep) {
		as_val* keyval = NULL;

		if (as_query_topk_copy_val((as_val*)src->key.valuep, &keyval, err) != AEROSPIKE_OK) {
			as_record_destroy(dst);
			return NULL;
		}
		dst->key.valuep = (as_key_value*)keyval;
	}

	for (uint16_t i = 0; i < src->bins.size; i++) {
		as_bin* sb = &src->bins.entries[i];
		as_bin* db = &dst->bins.entries[i];

		as_strncpy(db->name, sb->name, sizeof(db->name));

		as_val* val = NULL;

		if (as_query_topk_copy_val((as_val*)sb->valuep, &val, err) != AEROSPIKE_OK) {
			// Only bins [0, i) were populated; bump size so destroy cleans up exactly those.
			dst->bins.size = i;
			as_record_destroy(dst);
			return NULL;
		}
		db->valuep = (as_bin_value*)val;
		dst->bins.size = i + 1;
	}

	return dst;
}

//---------------------------------
// Comparator - matches the server's per-type ordering semantics
// NIL-for-missing/type-mismatched bin always sorts worst regardless of direction,
// digest-ascending tie-break, case-folding for STRING + CASE_INSENSITIVE.
//---------------------------------

static void
as_query_topk_extract_key(const as_record* rec, const as_query_order_by_field* order_by, as_query_topk_key* key)
{
	key->is_nil = true;

	as_bin_value* bv = as_record_get(rec, order_by->bin);

	if (! bv) {
		return;
	}

	as_val* val = (as_val*)bv;

	switch (order_by->type) {
		case AS_QUERY_ORDER_BY_INTEGER:
			if (val->type == AS_INTEGER) {
				key->v.i = as_integer_get((as_integer*)val);
				key->is_nil = false;
			}
			break;

		case AS_QUERY_ORDER_BY_DOUBLE:
			if (val->type == AS_DOUBLE) {
				key->v.d = as_double_get((as_double*)val);
				key->is_nil = false;
			}
			break;

		case AS_QUERY_ORDER_BY_STRING:
			if (val->type == AS_STRING) {
				key->v.str.p = as_string_get((as_string*)val);
				key->v.str.len = as_string_len((as_string*)val);
				key->is_nil = false;
			}
			break;

		case AS_QUERY_ORDER_BY_BYTES:
			if (val->type == AS_BYTES) {
				key->v.bytes.p = as_bytes_get((as_bytes*)val);
				key->v.bytes.len = as_bytes_size((as_bytes*)val);
				key->is_nil = false;
			}
			break;
	}
}

// ASCII-only case fold. The server's own case-fold (as_particle_string_case_fold)
// may not treat non-ASCII bytes identically; this client does not attempt to
// replicate that exactly. Case-insensitive Top-K ordering with non-ASCII
// order-by values may therefore not perfectly match the server's per-node
// pre-sort in rare cases. Plain ASCII inputs (the common case) are unaffected.
static int
as_query_topk_strcasecmp(const char* a, size_t alen, const char* b, size_t blen)
{
	size_t min_len = alen < blen ? alen : blen;

	for (size_t i = 0; i < min_len; i++) {
		int ca = tolower((unsigned char)a[i]);
		int cb = tolower((unsigned char)b[i]);

		if (ca != cb) {
			return ca - cb;
		}
	}
	return (alen > blen) - (alen < blen);
}

static int
as_query_topk_compare_typed(
	const as_query_topk_key* a, const as_query_topk_key* b, const as_query_order_by_field* order_by
	)
{
	switch (order_by->type) {
		case AS_QUERY_ORDER_BY_INTEGER:
			return (a->v.i > b->v.i) - (a->v.i < b->v.i);

		case AS_QUERY_ORDER_BY_DOUBLE:
			return (a->v.d > b->v.d) - (a->v.d < b->v.d);

		case AS_QUERY_ORDER_BY_STRING:
			if (order_by->flags & AS_QUERY_ORDER_BY_CASE_INSENSITIVE) {
				return as_query_topk_strcasecmp(a->v.str.p, a->v.str.len, b->v.str.p, b->v.str.len);
			}
			else {
				size_t min_len = a->v.str.len < b->v.str.len ? a->v.str.len : b->v.str.len;
				int cmp = memcmp(a->v.str.p, b->v.str.p, min_len);
				return cmp != 0 ? cmp : (a->v.str.len > b->v.str.len) - (a->v.str.len < b->v.str.len);
			}

		case AS_QUERY_ORDER_BY_BYTES: {
			uint32_t min_len = a->v.bytes.len < b->v.bytes.len ? a->v.bytes.len : b->v.bytes.len;
			int cmp = memcmp(a->v.bytes.p, b->v.bytes.p, min_len);
			return cmp != 0 ? cmp : (a->v.bytes.len > b->v.bytes.len) - (a->v.bytes.len < b->v.bytes.len);
		}

		default:
			return 0;
	}
}

static inline int
as_query_topk_cmp_digest(const as_record* a, const as_record* b)
{
	return memcmp(a->key.digest.value, b->key.digest.value, AS_DIGEST_VALUE_SIZE);
}

// Total order over final rank position: negative means a ranks ahead of (is better than) b.
static int
as_query_topk_cmp_rank(const as_record* a, const as_record* b, const as_query_order_by_field* order_by)
{
	as_query_topk_key ka, kb;
	as_query_topk_extract_key(a, order_by, &ka);
	as_query_topk_extract_key(b, order_by, &kb);

	if (ka.is_nil || kb.is_nil) {
		if (ka.is_nil && kb.is_nil) {
			return as_query_topk_cmp_digest(a, b);
		}
		// NIL (missing bin, or bin present with the wrong type) always sorts worst,
		// regardless of direction - matches the Confluence design doc's "Not errors" note.
		return ka.is_nil ? 1 : -1;
	}

	int cmp = as_query_topk_compare_typed(&ka, &kb, order_by);

	// Typed compare above is a plain ascending compare; DESC means "larger value ranks
	// first", so flip the sign to turn it into rank order.
	if (order_by->direction == AS_ORDER_DESCENDING) {
		cmp = -cmp;
	}

	return cmp != 0 ? cmp : as_query_topk_cmp_digest(a, b);
}

//---------------------------------
// Merge sort over an array of as_record* - avoids qsort_r/qsort_s, whose
// signatures diverge across the platforms (Linux/macOS/Windows) this client
// supports, while still being O(n log n) for the (n <= n_nodes * k) input.
//---------------------------------

typedef int (*as_query_topk_cmp_fn)(const as_record* a, const as_record* b, const void* ctx);

static void
as_query_topk_merge(
	as_record** arr, as_record** tmp, uint32_t lo, uint32_t mid, uint32_t hi,
	as_query_topk_cmp_fn cmp, const void* ctx
	)
{
	uint32_t i = lo, j = mid, k = lo;

	while (i < mid && j < hi) {
		tmp[k++] = (cmp(arr[i], arr[j], ctx) <= 0) ? arr[i++] : arr[j++];
	}
	while (i < mid) {
		tmp[k++] = arr[i++];
	}
	while (j < hi) {
		tmp[k++] = arr[j++];
	}
	memcpy(&arr[lo], &tmp[lo], (size_t)(hi - lo) * sizeof(as_record*));
}

static void
as_query_topk_sort_range(
	as_record** arr, as_record** tmp, uint32_t lo, uint32_t hi, as_query_topk_cmp_fn cmp,
	const void* ctx
	)
{
	if (hi - lo <= 1) {
		return;
	}
	uint32_t mid = lo + (hi - lo) / 2;
	as_query_topk_sort_range(arr, tmp, lo, mid, cmp, ctx);
	as_query_topk_sort_range(arr, tmp, mid, hi, cmp, ctx);
	as_query_topk_merge(arr, tmp, lo, mid, hi, cmp, ctx);
}

static void
as_query_topk_sort(as_record** arr, uint32_t n, as_query_topk_cmp_fn cmp, const void* ctx)
{
	if (n <= 1) {
		return;
	}
	as_record** tmp = cf_malloc(sizeof(as_record*) * n);
	as_query_topk_sort_range(arr, tmp, 0, n, cmp, ctx);
	cf_free(tmp);
}

static int
as_query_topk_cmp_digest_fn(const as_record* a, const as_record* b, const void* ctx)
{
	(void)ctx;
	return as_query_topk_cmp_digest(a, b);
}

static int
as_query_topk_cmp_rank_fn(const as_record* a, const as_record* b, const void* ctx)
{
	return as_query_topk_cmp_rank(a, b, (const as_query_order_by_field*)ctx);
}

//---------------------------------
// Finalize: dedup by digest (rare, migration-induced), sort into final rank
// order, truncate to k. Destroys every buffered record that does not make the cut.
// Returns a freshly allocated array (caller must cf_free()) of at most k
// as_record*, each owned by the caller from this point on.
//---------------------------------

static as_record**
as_query_topk_finalize(as_query_topk_collector* collector, uint32_t* out_count)
{
	uint32_t n = collector->entries.size;

	if (n == 0) {
		*out_count = 0;
		return NULL;
	}

	as_record** arr = cf_malloc(sizeof(as_record*) * n);

	for (uint32_t i = 0; i < n; i++) {
		arr[i] = *(as_record**)as_vector_get(&collector->entries, i);
	}

	// Every record now referenced only by arr (about to be consumed - either delivered
	// and destroyed, or destroyed here as a duplicate/truncated-away candidate). Clear
	// the vector so a later as_query_topk_collector_destroy() call does not try to
	// destroy these same records a second time.
	as_vector_clear(&collector->entries);

	// Dedup by digest first: duplicates are not guaranteed to be adjacent once sorted
	// by rank (the same digest could carry a different observed order-by value across
	// nodes/rounds if a migration raced with the scan), so group by digest explicitly.
	as_query_topk_sort(arr, n, as_query_topk_cmp_digest_fn, NULL);

	uint32_t unique_n = 0;

	for (uint32_t i = 0; i < n; i++) {
		if (unique_n > 0 && as_query_topk_cmp_digest(arr[unique_n - 1], arr[i]) == 0) {
			as_record_destroy(arr[i]);
			continue;
		}
		arr[unique_n++] = arr[i];
	}

	as_query_topk_sort(arr, unique_n, as_query_topk_cmp_rank_fn, &collector->order_by);

	uint32_t final_n = unique_n < collector->k ? unique_n : collector->k;

	for (uint32_t i = final_n; i < unique_n; i++) {
		as_record_destroy(arr[i]);
	}

	*out_count = final_n;
	return arr;
}

//---------------------------------
// Public API
//---------------------------------

static as_query_topk_collector*
as_query_topk_collector_create(const as_query_order_by_field* order_by, uint32_t k, as_query_topk_kind kind)
{
	as_query_topk_collector* collector = cf_malloc(sizeof(as_query_topk_collector));

	collector->order_by = *order_by;
	collector->k = k;
	collector->kind = kind;
	pthread_mutex_init(&collector->lock, NULL);
	as_vector_init(&collector->entries, sizeof(as_record*), k > 64 ? 64 : k);
	return collector;
}

as_query_topk_collector*
as_query_topk_collector_create_sync(
	const as_query_order_by_field* order_by, uint32_t k, aerospike_query_foreach_callback callback,
	void* udata
	)
{
	as_query_topk_collector* collector = as_query_topk_collector_create(order_by, k, AS_QUERY_TOPK_SYNC);
	collector->real.sync.callback = callback;
	collector->real.sync.udata = udata;
	return collector;
}

as_query_topk_collector*
as_query_topk_collector_create_async(
	const as_query_order_by_field* order_by, uint32_t k, as_async_query_record_listener listener,
	void* udata
	)
{
	as_query_topk_collector* collector = as_query_topk_collector_create(order_by, k, AS_QUERY_TOPK_ASYNC);
	collector->real.async.listener = listener;
	collector->real.async.udata = udata;
	return collector;
}

static void
as_query_topk_collector_append(as_query_topk_collector* collector, const as_record* src)
{
	as_error err;
	as_error_init(&err);

	as_record* copy = as_query_topk_copy_record(src, &err);

	if (! copy) {
		// Allocation/serialization failure copying a single candidate. Extremely rare
		// (out of memory, or a bin value this client's serializer can't round-trip).
		// Dropping this one candidate is preferable to aborting the whole query.
		return;
	}

	pthread_mutex_lock(&collector->lock);
	as_vector_append(&collector->entries, &copy);
	pthread_mutex_unlock(&collector->lock);
}

void
as_query_topk_collector_destroy(as_query_topk_collector* collector)
{
	for (uint32_t i = 0; i < collector->entries.size; i++) {
		as_record_destroy(*(as_record**)as_vector_get(&collector->entries, i));
	}
	as_vector_destroy(&collector->entries);
	pthread_mutex_destroy(&collector->lock);
	cf_free(collector);
}

static void
as_query_topk_flush_sync(as_query_topk_collector* collector)
{
	uint32_t count;
	as_record** results = as_query_topk_finalize(collector, &count);
	bool notify = true;

	for (uint32_t i = 0; i < count; i++) {
		if (notify && ! collector->real.sync.callback((as_val*)results[i], collector->real.sync.udata)) {
			notify = false;
		}
		as_record_destroy(results[i]);
	}

	if (results) {
		cf_free(results);
	}

	// Signal completion, matching aerospike_query_foreach_callback's contract of one
	// final NULL callback (as_query_execute()/as_query_partitions() do the same).
	collector->real.sync.callback(NULL, collector->real.sync.udata);
}

bool
as_query_topk_collect(const as_val* val, void* udata)
{
	as_query_topk_collector* collector = (as_query_topk_collector*)udata;

	if (! val) {
		as_query_topk_flush_sync(collector);
		return true;
	}

	as_query_topk_collector_append(collector, as_record_fromval(val));
	return true;
}

static void
as_query_topk_flush_async(as_query_topk_collector* collector, as_event_loop* event_loop)
{
	uint32_t count;
	as_record** results = as_query_topk_finalize(collector, &count);
	bool notify = true;

	for (uint32_t i = 0; i < count; i++) {
		if (notify &&
			! collector->real.async.listener(NULL, results[i], collector->real.async.udata, event_loop)) {
			notify = false;
		}
		as_record_destroy(results[i]);
	}

	if (results) {
		cf_free(results);
	}

	// Signal completion, matching as_async_query_record_listener's contract of one
	// final NULL-record callback.
	collector->real.async.listener(NULL, NULL, collector->real.async.udata, event_loop);
}

bool
as_query_topk_collect_async(as_error* err, as_record* record, void* udata, as_event_loop* event_loop)
{
	as_query_topk_collector* collector = (as_query_topk_collector*)udata;

	if (err) {
		// Fatal error. Buffered records may be incomplete - do not flush them as if they
		// were a valid Top-K result. Forward the error once, matching the contract.
		bool rv = collector->real.async.listener(err, NULL, collector->real.async.udata, NULL);
		as_query_topk_collector_destroy(collector);
		return rv;
	}

	if (! record) {
		// Underlying query completed successfully across every targeted node.
		as_query_topk_flush_async(collector, event_loop);
		as_query_topk_collector_destroy(collector);
		return true;
	}

	as_query_topk_collector_append(collector, record);
	return true;
}
