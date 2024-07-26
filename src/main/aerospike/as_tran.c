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
#include <aerospike/as_batch.h>
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
	as_strncpy(e->set, set, sizeof(e->set));
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
khash_clear(as_khash* h)
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
			row->used = false;
		}
		row++;
	}
	
	h->n_eles = 0;
}

static void
khash_destroy(as_khash* h)
{
	if (h->n_eles > 0) {
		khash_clear(h);
	}
	pthread_mutex_destroy(&h->lock);
	cf_free(h->table);
}

static bool
khash_is_empty(const as_khash* h)
{
	// TODO: Should this be done under lock?
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

static bool
khash_contains(as_khash* h, const uint8_t* keyd)
{
	as_khash_row* row = get_row(h, keyd);

	if (! row->used) {
		return false;
	}

	as_khash_ele* e = &row->head;

	pthread_mutex_lock(&h->lock);

	if (! row->used) {
		pthread_mutex_unlock(&h->lock);
		return false;
	}

	do {
		if (memcmp(keyd, e->keyd, AS_DIGEST_VALUE_SIZE) == 0) {
			pthread_mutex_unlock(&h->lock);
			return true;
		}

		e = e->next;
	} while (e != NULL);

	pthread_mutex_unlock(&h->lock);
	return false;
}

// This function is only called on as_tran_commit() or as_tran_abort() so it's not possible
// to contend with other parallel khash function calls. Therfore, there are no locks.
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

static void
as_tran_init_all(as_tran* tran, uint32_t read_buckets, uint32_t write_buckets)
{
	// An id of zero is considered invalid. Create random numbers
	// in a loop until non-zero is returned.
	uint64_t id = cf_get_rand64();

	while (id == 0) {
		id = cf_get_rand64();
	}
	
	tran->id = id;
	tran->ns[0] = 0;
	tran->deadline = 0;
	tran->roll_attempted = false;
	khash_init(&tran->reads, read_buckets);
	khash_init(&tran->writes, write_buckets);
}

//---------------------------------
// Functions
//---------------------------------

void
as_tran_init(as_tran* tran)
{
	as_tran_init_all(tran, 128, 128);
	tran->free = false;
}

void
as_tran_init_capacity(as_tran* tran, uint32_t reads_capacity, uint32_t writes_capacity)
{
	if (reads_capacity < 16) {
		reads_capacity = 16;
	}

	if (writes_capacity < 16) {
		writes_capacity = 16;
	}

	// Double record capacity to allocate enough buckets to alleviate collisions.
	as_tran_init_all(tran, reads_capacity * 2, writes_capacity * 2);
	tran->free = false;
}

as_tran*
as_tran_create(void)
{
	as_tran* tran = cf_malloc(sizeof(as_tran));
	as_tran_init(tran);
	tran->free = true;
	return tran;
}

as_tran*
as_tran_create_capacity(uint32_t reads_capacity, uint32_t writes_capacity)
{
	as_tran* tran = cf_malloc(sizeof(as_tran));
	as_tran_init_capacity(tran, reads_capacity, writes_capacity);
	tran->free = true;
	return tran;
}

void
as_tran_destroy(as_tran* tran)
{
	khash_destroy(&tran->reads);
	khash_destroy(&tran->writes);
	
	if (tran->free) {
		cf_free(tran);
	}
}

void
as_tran_on_read(as_tran* tran, const uint8_t* digest, const char* set, uint64_t version)
{
	if (version != 0) {
		khash_put(&tran->reads, digest, set, version);
	}
}

uint64_t
as_tran_get_read_version(as_tran* tran, const as_key* key)
{
	return khash_get_version(&tran->reads, key->digest.value);
}

void
as_tran_on_write(as_tran* tran, const uint8_t* digest, const char* set, uint64_t version, int rc)
{
	if (version != 0) {
		khash_put(&tran->reads, digest, set, version);
	}
	else {
		if (rc == AEROSPIKE_OK) {
			khash_remove(&tran->reads, digest);
			khash_put(&tran->writes, digest, set, 0);
		}
	}
}

bool
as_tran_writes_contain(as_tran* tran, const as_key* key)
{
	return khash_contains(&tran->writes, key->digest.value);
}

as_status
as_tran_set_ns(as_tran* tran, const char* ns, as_error* err)
{
	if (tran->ns[0] == 0) {
		as_strncpy(tran->ns, ns, sizeof(tran->ns));
		return AEROSPIKE_OK;
	}
	
	if (strncmp(tran->ns, ns, sizeof(tran->ns)) != 0) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
			"Namespace must be the same for all commands in the MRT. orig: %s new: %s",
			tran->ns, ns);
	}
	return AEROSPIKE_OK;
}

as_status
as_tran_set_ns_batch(as_tran* tran, const as_batch* batch, as_error* err)
{
	uint32_t n_keys = batch->keys.size;

	for (uint32_t i = 0; i < n_keys; i++) {
		as_key* key = &batch->keys.entries[i];
		as_status status = as_tran_set_ns(tran, key->ns, err);

		if (status != AEROSPIKE_OK) {
			return status;
		}
	}
	return AEROSPIKE_OK;
}

bool
as_tran_set_roll_attempted(as_tran* tran)
{
	if (tran->roll_attempted) {
		return false;
	}
	
	tran->roll_attempted = true;
	return true;
}

void
as_tran_clear(as_tran* tran)
{
	tran->ns[0] = 0;
	tran->deadline = 0;
	khash_clear(&tran->reads);
	khash_clear(&tran->writes);
}
