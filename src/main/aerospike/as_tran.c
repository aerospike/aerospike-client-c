/*
 * Copyright 2008-2024 Aerospike, Inc.
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
#include <aerospike/as_tran.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_random.h>

//---------------------------------
// Types
//---------------------------------

typedef void (*khash_reduce_fn)(const uint8_t* keyd, const char* set, uint64_t version, void* udata);

//---------------------------------
// Static Functions
//---------------------------------

static inline as_khash_row*
get_row(const as_khash* h, const uint8_t* keyd)
{
	return &h->table[*(uint32_t*)keyd % h->n_rows];
}

static inline void
fill_ele(as_khash_ele* e, const uint8_t* keyd, const char* set, uint64_t version)
{
	memcpy(e->keyd, keyd, sizeof(e->keyd));
	strcpy(e->set, set);
	e->version = version;
	e->next = NULL;
}

static void
khash_init(as_khash* h, uint32_t n_rows)
{
	pthread_mutex_init(&h->lock, NULL);
	h->n_eles = 0;
	h->n_rows = n_rows;
	h->table = (as_khash_row*)cf_malloc(n_rows * sizeof(as_khash_row));

	for (uint32_t i = 0; i < h->n_rows; i++) {
		h->table[i].used = false;
	}
}

static void
khash_destroy(as_khash* h)
{
	as_khash_row* row = h->table;

	for (uint32_t i = 0; i < h->n_rows; i++) {
		if (row->used) {
			as_khash_ele* e = row->head.next;

			while (e != NULL) {
				as_khash_ele* t = e->next;
				cf_free(e);
				e = t;
			}
		}
		row++;
	}

	pthread_mutex_destroy(&h->lock);
	cf_free(h->table);
}

static bool
khash_is_empty(const as_khash* h)
{
	return h->n_eles == 0;
}

static void
khash_put(as_khash* h, const uint8_t* keyd, const char* set, uint64_t version)
{
	as_khash_row* row = get_row(h, keyd);
	as_khash_ele* e = &row->head;

	pthread_mutex_lock(&h->lock);

	// Most common case should be insert into empty row.
	if (! row->used) {
		fill_ele(e, keyd, set, version);
		h->n_eles++;
		row->used = true;
		pthread_mutex_unlock(&h->lock);
		return;
	}

	do {
		if (memcmp(keyd, e->keyd, AS_DIGEST_VALUE_SIZE) == 0) {
			e->version = version;
			pthread_mutex_unlock(&h->lock);
			return;
		}

		e = e->next;
	} while (e != NULL);

	e = (as_khash_ele*)cf_malloc(sizeof(as_khash_ele));
	fill_ele(e, keyd, set, version);
	h->n_eles++;

	// Insert just after head.
	e->next = row->head.next;
	row->head.next = e;

	pthread_mutex_unlock(&h->lock);
}

static void
khash_remove(as_khash* h, const uint8_t* keyd)
{
	as_khash_row* row = get_row(h, keyd);
	as_khash_ele* e = &row->head;
	as_khash_ele* e_prev = NULL;
	as_khash_ele* free_e = NULL;

	pthread_mutex_lock(&h->lock);

	if (! row->used) {
		pthread_mutex_unlock(&h->lock);
		return;
	}

	do {
		if (memcmp(keyd, e->keyd, AS_DIGEST_VALUE_SIZE) == 0) {
			if (e_prev != NULL) {
				e_prev->next = e->next;
				free_e = e;
			}
			else if (e->next == NULL) {
				row->used = false;
			}
			else {
				free_e = e->next;
				*e = *e->next;
			}

			h->n_eles--;
			break;
		}
		
		e_prev = e;
		e = e->next;
	} while (e != NULL);

	pthread_mutex_unlock(&h->lock);

	if (free_e != NULL) {
		cf_free(free_e);
	}
}

static uint64_t
khash_get_version(as_khash* h, const uint8_t* keyd)
{
	as_khash_row* row = get_row(h, keyd);

	if (! row->used) {
		return 0;
	}

	as_khash_ele* e = &row->head;

	pthread_mutex_lock(&h->lock);

	if (! row->used) {
		pthread_mutex_unlock(&h->lock);
		return 0;
	}

	do {
		if (memcmp(keyd, e->keyd, AS_DIGEST_VALUE_SIZE) == 0) {
			uint64_t version = e->version;
			pthread_mutex_unlock(&h->lock);
			return version;
		}

		e = e->next;
	} while (e != NULL);

	pthread_mutex_unlock(&h->lock);
	return 0;
}

// Only called if not empty, and can't contend with anything else.
static void
khash_reduce(as_khash* h, khash_reduce_fn cb, void* udata)
{
	as_khash_row* row = h->table;

	for (uint32_t i = 0; i < h->n_rows; i++) {
		if (row->used) {
			as_khash_ele* e = &row->head;

			do {
				cb(e->keyd, e->set, e->version, udata);
				e = e->next;
			} while (e != NULL);
		}
		row++;
	}
}

static uint64_t
as_tran_create_id(void)
{
	// An id of zero is considered invalid. Create random numbers
	// in a loop until non-zero is returned.
	uint64_t id = cf_get_rand64();

	while (id == 0) {
		id = cf_get_rand64();
	}
	return id;
}

//---------------------------------
// Functions
//---------------------------------

void
as_tran_init(as_tran* tran)
{
	uint64_t id = as_tran_create_id();
	as_tran_init_all(tran, id, 256, 256);
}

void
as_tran_init_buckets(as_tran* tran, uint32_t read_capacity, uint32_t write_capacity)
{
	uint64_t id = as_tran_create_id();
	as_tran_init_all(tran, id, read_capacity, write_capacity);
}

void
as_tran_init_all(as_tran* tran, uint64_t id, uint32_t read_buckets, uint32_t write_buckets)
{
	tran->id = id;
	tran->ns[0] = 0;
	tran->deadline = 0;
	khash_init(&tran->reads, read_buckets);
	khash_init(&tran->writes, write_buckets);
}

bool
as_tran_set_ns(as_tran* tran, const char* ns)
{
	if (tran->ns[0] == 0) {
		as_strncpy(tran->ns, ns, sizeof(tran->ns));
		return true;
	}
	
	return strncmp(tran->ns, ns, sizeof(tran->ns)) == 0;
}

bool
as_tran_on_read(as_tran* tran, as_key* key, uint64_t version)
{
	// TODO: Do not call as_tran_on_read() when version not returned from server. 
	// TODO: Can we just make zero an invalid version?
	if (! as_tran_set_ns(tran, key->ns)) {
		return false;
	}
	
	// TODO: Put key/version in reads hashmap.
	return true;
}

uint64_t
as_tran_get_read_version(as_tran* tran, as_key* key)
{
	// TODO:
	//return reads.get(key);
	return 0;
}

bool
as_tran_on_write(as_tran* tran, as_key* key, uint64_t version, int rc)
{
	// TOOD: Can we just make zero an invalid version?
	// TODO: Should key.namespace be verified here?
	if (version != 0) {
		// TODO:
		//reads.put(key, version);
	}
	else {
		if (rc == AEROSPIKE_OK) {
			// TODO:
			//reads.remove(key);
			//writes.add(key);
		}
	}
	return true;
}

void
as_tran_close(as_tran* tran)
{
	tran->ns[0] = 0;
	tran->deadline = 0;
	
	// TODO:
	//reads.clear();
	//writes.clear();
}
