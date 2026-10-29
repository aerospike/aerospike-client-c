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
#include <aerospike/as_boolean.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_double.h>
#include <aerospike/as_error.h>
#include <aerospike/as_geojson.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_nil.h>
#include <aerospike/as_record.h>
#include <aerospike/as_string.h>
#include <aerospike/as_val.h>
#include <citrusleaf/alloc.h>
#include <math.h>
#include <pthread.h>
#include <string.h>

//---------------------------------
// Types
//---------------------------------

typedef struct {
	as_record* rec;

	// Milliseconds since the Citrusleaf epoch, or 0 if the server did not send it.
	uint64_t last_update_time;
} as_query_topk_entry;

struct as_query_topk_collector_s {
	as_query_order_by_field order_by;
	as_query_order_by_field then_order_by;
	uint32_t k;

	// Exactly one of callback (synchronous query) and listener (asynchronous
	// query) is set.
	aerospike_query_foreach_callback callback;
	as_async_query_record_listener listener;
	void* udata;

	// Protects heap, heap_n and index. Multiple thread pool workers (one per
	// node) may append concurrently while a synchronous query is in flight. An
	// asynchronous query runs every node command on one event loop.
	pthread_mutex_t lock;

	// The best k candidates seen so far, each a heap-owned copy, in a binary heap
	// with the worst-ranked candidate at the root. Keeping only k candidates, rather
	// than every node's k, bounds the memory to about k records whatever the
	// cluster size.
	as_query_topk_entry* heap;
	uint32_t heap_n;

	// Open-addressing set of the digests in heap (linear probing, NULL = empty
	// slot), so a duplicate from another node is found without scanning the heap.
	as_record** index;
	uint32_t index_mask;

	// A candidate could not be buffered, so the merged result would be wrong.
	// Protected by lock. See as_query_topk_collector_status().
	bool failed;
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
// Deep copy: detach a record from the query parser, which builds it on its
// stack (as_record_inita()), stores scalar, string, GeoJSON and bytes values
// inline in its bins and key, and destroys it when the callback returns.
//---------------------------------

static char*
as_query_topk_copy_str(const char* src, size_t len)
{
	char* dst = cf_malloc(len + 1);

	if (! dst) {
		return NULL;
	}
	memcpy(dst, src, len);
	dst[len] = 0;
	return dst;
}

static as_val*
as_query_topk_copy_val(const as_val* src)
{
	if (! src) {
		return (as_val*)&as_nil;
	}

	switch (src->type) {
		case AS_NIL:
			return (as_val*)&as_nil;

		case AS_BOOLEAN: {
			bool b = as_boolean_get((const as_boolean*)src);
			return (as_val*)as_boolean_new(b);
		}

		case AS_INTEGER: {
			int64_t i = as_integer_get((const as_integer*)src);
			return (as_val*)as_integer_new(i);
		}

		case AS_DOUBLE: {
			double d = as_double_get((const as_double*)src);
			return (as_val*)as_double_new(d);
		}

		// Copy the full length: strings may contain NUL bytes.
		case AS_STRING: {
			as_string* s = (as_string*)src;
			size_t len = as_string_len(s);
			char* value = as_string_get(s);
			char* copy = as_query_topk_copy_str(value, len);

			if (! copy) {
				return NULL;
			}
			return (as_val*)as_string_new_wlen(copy, len, true);
		}

		case AS_GEOJSON: {
			as_geojson* g = (as_geojson*)src;
			size_t len = as_geojson_len(g);
			char* value = as_geojson_get(g);
			char* copy = as_query_topk_copy_str(value, len);

			if (! copy) {
				return NULL;
			}
			return (as_val*)as_geojson_new_wlen(copy, len, true);
		}

		case AS_BYTES: {
			const as_bytes* b = (const as_bytes*)src;
			uint32_t size = as_bytes_size(b);
			uint8_t* copy = cf_malloc(size);

			if (! copy) {
				return NULL;
			}
			memcpy(copy, b->value, size);

			as_bytes* dst = as_bytes_new_wrap(copy, size, true);

			if (! dst) {
				cf_free(copy);
				return NULL;
			}
			dst->type = b->type;
			return (as_val*)dst;
		}

		// The parser deserializes lists and maps into reference counted heap
		// values, so the copy shares them instead of duplicating them.
		case AS_LIST:
		case AS_MAP:
			return as_val_reserve(src);

		default:
			return NULL;
	}
}

static as_record*
as_query_topk_copy_record(const as_record* src)
{
	as_record* dst = as_record_new(src->bins.size);

	if (! dst) {
		return NULL;
	}

	dst->gen = src->gen;
	dst->ttl = src->ttl;
	as_strncpy(dst->key.ns, src->key.ns, sizeof(dst->key.ns));
	as_strncpy(dst->key.set, src->key.set, sizeof(dst->key.set));
	dst->key.digest = src->key.digest;

	if (src->key.valuep) {
		as_val* keyval = as_query_topk_copy_val((as_val*)src->key.valuep);

		if (! keyval) {
			as_record_destroy(dst);
			return NULL;
		}
		dst->key.valuep = (as_key_value*)keyval;
	}

	for (uint16_t i = 0; i < src->bins.size; i++) {
		as_bin* sb = &src->bins.entries[i];
		as_bin* db = &dst->bins.entries[i];

		as_strncpy(db->name, sb->name, sizeof(db->name));

		as_val* val = as_query_topk_copy_val((as_val*)sb->valuep);

		if (! val) {
			// dst->bins.size counts only the bins copied so far, so destroy frees exactly those.
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
// NIL-for-missing/type-mismatched bin always sorts worst regardless of direction
// and leaves the next order key to decide, NaN sorts above every other double,
// -0.0 equals 0.0, digest-ascending tie-break, ASCII case-folding for
// STRING + CASE_INSENSITIVE.
//---------------------------------

static void
as_query_topk_extract_key(const as_record* rec, const as_query_order_by_field* order_by, as_query_topk_key* key)
{
	*key = (as_query_topk_key){.is_nil = true};

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

// The server folds only ASCII letters, so tolower() is not used: its result
// depends on the process locale.
static inline int
as_query_topk_ascii_lower(unsigned char c)
{
	if (c >= 'A' && c <= 'Z') {
		return c + ('a' - 'A');
	}
	return c;
}

static int
as_query_topk_strcasecmp(const char* a, size_t alen, const char* b, size_t blen)
{
	size_t min_len = alen < blen ? alen : blen;

	for (size_t i = 0; i < min_len; i++) {
		int ca = as_query_topk_ascii_lower((unsigned char)a[i]);
		int cb = as_query_topk_ascii_lower((unsigned char)b[i]);

		if (ca != cb) {
			return ca - cb;
		}
	}
	return (alen > blen) - (alen < blen);
}

static int
as_query_topk_cmp_double(double a, double b)
{
	int a_nan = isnan(a) ? 1 : 0;
	int b_nan = isnan(b) ? 1 : 0;

	if (a_nan || b_nan) {
		return a_nan - b_nan;
	}
	return (a > b) - (a < b);
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
			return as_query_topk_cmp_double(a->v.d, b->v.d);

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

// Rank order on one order key: negative means a ranks ahead of (is better than) b.
// Returns 0 when both are NIL, so the next order key decides.
static int
as_query_topk_cmp_key(const as_record* a, const as_record* b, const as_query_order_by_field* order_by)
{
	as_query_topk_key ka;
	as_query_topk_extract_key(a, order_by, &ka);

	as_query_topk_key kb;
	as_query_topk_extract_key(b, order_by, &kb);

	if (ka.is_nil || kb.is_nil) {
		// NIL (missing bin, or bin present with the wrong type) always sorts worst,
		// regardless of direction - matches the Confluence design doc's "Not errors" note.
		return (int)ka.is_nil - (int)kb.is_nil;
	}

	int cmp = as_query_topk_compare_typed(&ka, &kb, order_by);

	// Typed compare above is a plain ascending compare; DESC means "larger value ranks
	// first", so flip the sign to turn it into rank order.
	if (order_by->direction == AS_ORDER_DESCENDING) {
		cmp = -cmp;
	}
	return cmp;
}

// Total order over final rank position: negative means a ranks ahead of (is better than) b.
static int
as_query_topk_cmp_rank(const as_record* a, const as_record* b, const as_query_topk_collector* collector)
{
	int cmp = as_query_topk_cmp_key(a, b, &collector->order_by);

	if (cmp != 0) {
		return cmp;
	}

	if (collector->then_order_by.defined) {
		cmp = as_query_topk_cmp_key(a, b, &collector->then_order_by);

		if (cmp != 0) {
			return cmp;
		}
	}
	return as_query_topk_cmp_digest(a, b);
}

//---------------------------------
// Merge sort over an array of as_record* - avoids qsort_r/qsort_s, whose
// signatures diverge across the platforms (Linux/macOS/Windows) this client
// supports, while still being O(n log n) for the (n <= k) input.
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
as_query_topk_cmp_rank_fn(const as_record* a, const as_record* b, const void* ctx)
{
	return as_query_topk_cmp_rank(a, b, (const as_query_topk_collector*)ctx);
}

//---------------------------------
// Bounded heap of the best k candidates, worst at the root, and its digest index.
//---------------------------------

static inline uint32_t
as_query_topk_index_slot(const as_query_topk_collector* collector, const uint8_t* digest)
{
	// A digest is a hash, so any 4 of its bytes spread the slots evenly.
	uint32_t h;
	memcpy(&h, digest, sizeof(h));
	return h & collector->index_mask;
}

// Return the slot that holds digest, or the empty slot where it would go.
static uint32_t
as_query_topk_index_find(const as_query_topk_collector* collector, const uint8_t* digest)
{
	uint32_t i = as_query_topk_index_slot(collector, digest);

	while (collector->index[i] &&
		memcmp(collector->index[i]->key.digest.value, digest, AS_DIGEST_VALUE_SIZE) != 0) {
		i = (i + 1) & collector->index_mask;
	}
	return i;
}

static void
as_query_topk_index_remove(as_query_topk_collector* collector, const as_record* rec)
{
	uint32_t i = as_query_topk_index_find(collector, rec->key.digest.value);
	collector->index[i] = NULL;

	// Shift back every later record in the probe run that can no longer be reached
	// from its home slot once slot i is empty.
	uint32_t j = i;

	while (true) {
		j = (j + 1) & collector->index_mask;

		as_record* moved = collector->index[j];

		if (! moved) {
			return;
		}

		uint32_t home = as_query_topk_index_slot(collector, moved->key.digest.value);
		bool reachable = (i <= j) ? (i < home && home <= j) : (i < home || home <= j);

		if (! reachable) {
			collector->index[i] = moved;
			collector->index[j] = NULL;
			i = j;
		}
	}
}

static inline void
as_query_topk_heap_swap(as_query_topk_entry* heap, uint32_t a, uint32_t b)
{
	as_query_topk_entry tmp = heap[a];
	heap[a] = heap[b];
	heap[b] = tmp;
}

static void
as_query_topk_heap_sift_up(as_query_topk_collector* collector, uint32_t i)
{
	as_query_topk_entry* heap = collector->heap;

	while (i > 0) {
		uint32_t parent = (i - 1) / 2;

		if (as_query_topk_cmp_rank(heap[i].rec, heap[parent].rec, collector) <= 0) {
			return;
		}
		as_query_topk_heap_swap(heap, i, parent);
		i = parent;
	}
}

static void
as_query_topk_heap_sift_down(as_query_topk_collector* collector, uint32_t i)
{
	as_query_topk_entry* heap = collector->heap;
	uint32_t n = collector->heap_n;

	while (true) {
		uint32_t worst = i;
		uint32_t left = 2 * i + 1;
		uint32_t right = left + 1;

		if (left < n && as_query_topk_cmp_rank(heap[left].rec, heap[worst].rec, collector) > 0) {
			worst = left;
		}

		if (right < n && as_query_topk_cmp_rank(heap[right].rec, heap[worst].rec, collector) > 0) {
			worst = right;
		}

		if (worst == i) {
			return;
		}
		as_query_topk_heap_swap(heap, i, worst);
		i = worst;
	}
}

// Return the heap position of a kept record. Only a duplicate needs it, and
// duplicates are rare, so a scan is simpler than tracking positions in the index.
static uint32_t
as_query_topk_heap_find(const as_query_topk_collector* collector, const as_record* kept)
{
	uint32_t i = 0;

	while (collector->heap[i].rec != kept) {
		i++;
	}
	return i;
}

typedef enum {
	AS_QUERY_TOPK_DROP,
	AS_QUERY_TOPK_ADD,
	AS_QUERY_TOPK_REPLACE
} as_query_topk_action;

// What to do with a candidate. A record can reach the client from two nodes while its
// partition migrates, and the copy with the later last-update time replaces a kept
// copy. On a tie, including a server that sends no last-update time, the kept copy
// stays. Any other candidate is added if it ranks ahead of the worst kept candidate
// or there is room.
static as_query_topk_action
as_query_topk_heap_action(
	const as_query_topk_collector* collector, const as_record* rec, uint64_t last_update_time
	)
{
	uint32_t slot = as_query_topk_index_find(collector, rec->key.digest.value);
	as_record* kept = collector->index[slot];

	if (kept) {
		uint32_t pos = as_query_topk_heap_find(collector, kept);

		if (last_update_time > collector->heap[pos].last_update_time) {
			return AS_QUERY_TOPK_REPLACE;
		}
		return AS_QUERY_TOPK_DROP;
	}

	if (collector->heap_n < collector->k ||
		as_query_topk_cmp_rank(rec, collector->heap[0].rec, collector) < 0) {
		return AS_QUERY_TOPK_ADD;
	}
	return AS_QUERY_TOPK_DROP;
}

// Add copy, for which as_query_topk_heap_action() returned AS_QUERY_TOPK_ADD under the
// same lock hold.
static void
as_query_topk_heap_add(as_query_topk_collector* collector, as_record* copy, uint64_t last_update_time)
{
	as_query_topk_entry entry = {.rec = copy, .last_update_time = last_update_time};

	if (collector->heap_n == collector->k) {
		as_record* worst = collector->heap[0].rec;

		as_query_topk_index_remove(collector, worst);
		as_record_destroy(worst);
		collector->heap[0] = entry;
		as_query_topk_heap_sift_down(collector, 0);
	}
	else {
		collector->heap[collector->heap_n] = entry;
		collector->heap_n++;
		as_query_topk_heap_sift_up(collector, collector->heap_n - 1);
	}

	uint32_t slot = as_query_topk_index_find(collector, copy->key.digest.value);
	collector->index[slot] = copy;
}

// Replace the kept copy of the same record with copy, for which
// as_query_topk_heap_action() returned AS_QUERY_TOPK_REPLACE under the same lock hold.
// The newer copy is kept wherever it ranks.
static void
as_query_topk_heap_replace(
	as_query_topk_collector* collector, as_record* copy, uint64_t last_update_time
	)
{
	uint32_t slot = as_query_topk_index_find(collector, copy->key.digest.value);
	as_record* kept = collector->index[slot];
	uint32_t pos = as_query_topk_heap_find(collector, kept);

	as_record_destroy(kept);
	collector->index[slot] = copy;
	collector->heap[pos] = (as_query_topk_entry){.rec = copy, .last_update_time = last_update_time};
	as_query_topk_heap_sift_up(collector, pos);
	as_query_topk_heap_sift_down(collector, pos);
}

//---------------------------------
// Finalize: sort the kept candidates into final rank order. Returns the heap array
// (caller must cf_free()) of the kept as_record*, each owned by the caller from this
// point on.
//---------------------------------

static as_record**
as_query_topk_finalize(as_query_topk_collector* collector, uint32_t* out_count)
{
	uint32_t n = collector->heap_n;
	as_record** results = cf_malloc(sizeof(as_record*) * (n > 0 ? n : 1));

	for (uint32_t i = 0; i < n; i++) {
		results[i] = collector->heap[i].rec;
	}

	// The records are now referenced only by results, so a later
	// as_query_topk_collector_destroy() call must not destroy them a second time.
	collector->heap_n = 0;

	as_query_topk_sort(results, n, as_query_topk_cmp_rank_fn, collector);

	*out_count = n;
	return results;
}

//---------------------------------
// Public API
//---------------------------------

static as_query_topk_collector*
as_query_topk_collector_new(const as_query* query, void* udata)
{
	as_query_topk_collector* collector = cf_malloc(sizeof(as_query_topk_collector));
	uint32_t k = query->top_k;

	collector->order_by = query->order_by;
	collector->then_order_by = query->then_order_by;
	collector->k = k;
	collector->callback = NULL;
	collector->listener = NULL;
	collector->udata = udata;
	collector->failed = false;
	pthread_mutex_init(&collector->lock, NULL);
	collector->heap = cf_malloc(sizeof(as_query_topk_entry) * k);
	collector->heap_n = 0;

	// At least twice k slots keep the probe runs short.
	uint32_t index_size = 1;

	while (index_size < 2 * k) {
		index_size <<= 1;
	}
	collector->index = cf_calloc(index_size, sizeof(as_record*));
	collector->index_mask = index_size - 1;
	return collector;
}

as_query_topk_collector*
as_query_topk_collector_create(
	const as_query* query, aerospike_query_foreach_callback callback, void* udata
	)
{
	as_query_topk_collector* collector = as_query_topk_collector_new(query, udata);
	collector->callback = callback;
	return collector;
}

as_query_topk_collector*
as_query_topk_collector_create_async(
	const as_query* query, as_async_query_record_listener listener, void* udata
	)
{
	as_query_topk_collector* collector = as_query_topk_collector_new(query, udata);
	collector->listener = listener;
	return collector;
}

static void
as_query_topk_set_buffer_error(as_error* err)
{
	as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to buffer a Top-K candidate record");
}

// Return false when this or an earlier candidate could not be buffered.
static bool
as_query_topk_collector_append(
	as_query_topk_collector* collector, const as_record* src, uint64_t last_update_time
	)
{
	// Skip copying a candidate that can't make the top k, or any candidate once
	// the query has failed.
	pthread_mutex_lock(&collector->lock);
	bool failed = collector->failed;
	as_query_topk_action action = AS_QUERY_TOPK_DROP;

	if (! failed) {
		action = as_query_topk_heap_action(collector, src, last_update_time);
	}
	pthread_mutex_unlock(&collector->lock);

	if (failed) {
		return false;
	}

	if (action == AS_QUERY_TOPK_DROP) {
		return true;
	}

	// Copy outside the lock so node threads do not wait on each other. Another
	// node thread can fail or add candidates meanwhile, so both are checked
	// again below.
	as_record* copy = as_query_topk_copy_record(src);

	pthread_mutex_lock(&collector->lock);

	if (! copy) {
		// Out of memory, or a bin value type the query parser never produces.
		// A missing candidate could belong in the top k, so the query fails.
		collector->failed = true;
	}
	else if (collector->failed) {
		as_record_destroy(copy);
	}
	else {
		action = as_query_topk_heap_action(collector, copy, last_update_time);

		if (action == AS_QUERY_TOPK_ADD) {
			as_query_topk_heap_add(collector, copy, last_update_time);
		}
		else if (action == AS_QUERY_TOPK_REPLACE) {
			as_query_topk_heap_replace(collector, copy, last_update_time);
		}
		else {
			as_record_destroy(copy);
		}
	}

	bool ok = ! collector->failed;
	pthread_mutex_unlock(&collector->lock);
	return ok;
}

as_status
as_query_topk_collector_status(const as_query_topk_collector* collector, as_error* err)
{
	if (collector->failed) {
		as_query_topk_set_buffer_error(err);
		return err->code;
	}
	return AEROSPIKE_OK;
}

void
as_query_topk_collector_destroy(as_query_topk_collector* collector)
{
	for (uint32_t i = 0; i < collector->heap_n; i++) {
		as_record_destroy(collector->heap[i].rec);
	}
	cf_free(collector->heap);
	cf_free(collector->index);
	pthread_mutex_destroy(&collector->lock);
	cf_free(collector);
}

static void
as_query_topk_flush(as_query_topk_collector* collector)
{
	// A candidate could not be buffered, so as_query_topk_collect() returned
	// false. as_query_partitions() treats that like a user abort: it returns
	// AEROSPIKE_OK and still sends the val == NULL completion that calls this
	// function. Skip delivery, including the final NULL, so the application
	// does not get an incomplete top k. as_query_topk_partitions() then
	// returns the error from as_query_topk_collector_status().
	if (collector->failed) {
		return;
	}

	uint32_t count;
	as_record** results = as_query_topk_finalize(collector, &count);
	bool notify = true;

	for (uint32_t i = 0; i < count; i++) {
		if (notify && ! collector->callback((as_val*)results[i], collector->udata)) {
			notify = false;
		}
		as_record_destroy(results[i]);
	}

	if (results) {
		cf_free(results);
	}

	// Signal completion, matching aerospike_query_foreach_callback's contract of one
	// final NULL callback (as_query_partitions() does the same).
	collector->callback(NULL, collector->udata);
}

bool
as_query_topk_collect(const as_val* val, void* udata)
{
	as_query_topk_collector* collector = (as_query_topk_collector*)udata;

	if (! val) {
		as_query_topk_flush(collector);
		return true;
	}

	as_record* rec = as_record_fromval(val);
	return as_query_topk_collector_append(collector, rec, 0);
}

bool
as_query_topk_collect_record(
	as_query_topk_collector* collector, const as_record* record, uint64_t last_update_time
	)
{
	return as_query_topk_collector_append(collector, record, last_update_time);
}

static void
as_query_topk_flush_async(as_query_topk_collector* collector, as_event_loop* event_loop)
{
	uint32_t count;
	as_record** results = as_query_topk_finalize(collector, &count);
	bool notify = true;

	for (uint32_t i = 0; i < count; i++) {
		if (notify && ! collector->listener(NULL, results[i], collector->udata, event_loop)) {
			notify = false;
		}
		as_record_destroy(results[i]);
	}

	if (results) {
		cf_free(results);
	}

	// Like the async query executor, do not notify a listener that returned false again.
	if (notify) {
		collector->listener(NULL, NULL, collector->udata, event_loop);
	}
}

static bool
as_query_topk_append_async(
	as_query_topk_collector* collector, const as_record* record, uint64_t last_update_time,
	as_event_loop* event_loop
	)
{
	if (as_query_topk_collector_append(collector, record, last_update_time)) {
		return true;
	}

	// Returning false stops the query without notifying the listener again.
	as_error buffer_err;
	as_error_init(&buffer_err);
	as_query_topk_set_buffer_error(&buffer_err);
	collector->listener(&buffer_err, NULL, collector->udata, event_loop);
	as_query_topk_collector_destroy(collector);
	return false;
}

bool
as_query_topk_collect_async(as_error* err, as_record* record, void* udata, as_event_loop* event_loop)
{
	as_query_topk_collector* collector = (as_query_topk_collector*)udata;

	if (err) {
		// The buffered records may be missing a node's candidates, so they are not a
		// valid Top-K result.
		collector->listener(err, NULL, collector->udata, event_loop);
		as_query_topk_collector_destroy(collector);
		return false;
	}

	if (! record) {
		as_query_topk_flush_async(collector, event_loop);
		as_query_topk_collector_destroy(collector);
		return false;
	}

	return as_query_topk_append_async(collector, record, 0, event_loop);
}

bool
as_query_topk_collect_record_async(
	as_query_topk_collector* collector, const as_record* record, uint64_t last_update_time,
	as_event_loop* event_loop
	)
{
	return as_query_topk_append_async(collector, record, last_update_time, event_loop);
}
