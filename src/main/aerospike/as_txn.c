/*
 * Copyright 2008-2025 Aerospike, Inc.
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
#include <aerospike/as_txn.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_random.h>

//---------------------------------
// Types
//---------------------------------

typedef void (*khash_reduce_fn)(const uint8_t* keyd, const char* set, uint64_t version, void* udata);

//---------------------------------
// Static Functions
//---------------------------------

static inline as_txn_hash_row*
get_row(const as_txn_hash* h, const uint8_t* keyd)
{
	return &h->table[*(uint32_t*)keyd % h->n_rows];
}

static inline void
fill_ele(as_txn_key* e, const uint8_t* keyd, const char* set, uint64_t version)
{
	memcpy(e->digest, keyd, sizeof(e->digest));
	as_strncpy(e->set, set, sizeof(e->set));
	e->version = version;
	e->next = NULL;
}

static void
as_txn_hash_init(as_txn_hash* h, uint32_t n_rows)
{
	pthread_mutex_init(&h->lock, NULL);
	h->n_eles = 0;
	h->n_rows = n_rows;
	h->table = (as_txn_hash_row*)cf_malloc(n_rows * sizeof(as_txn_hash_row));

	for (uint32_t i = 0; i < h->n_rows; i++) {
		h->table[i].used = false;
	}
}

static void
as_txn_hash_clear(as_txn_hash* h)
{
	as_txn_hash_row* row = h->table;

	for (uint32_t i = 0; i < h->n_rows; i++) {
		if (row->used) {
			as_txn_key* e = row->head.next;

			while (e != NULL) {
				as_txn_key* t = e->next;
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
as_txn_hash_destroy(as_txn_hash* h)
{
	if (h->n_eles > 0) {
		as_txn_hash_clear(h);
	}
	pthread_mutex_destroy(&h->lock);
	cf_free(h->table);
}

static void
as_txn_hash_put(as_txn_hash* h, const uint8_t* keyd, const char* set, uint64_t version)
{
	as_txn_hash_row* row = get_row(h, keyd);
	as_txn_key* e = &row->head;

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
		if (memcmp(keyd, e->digest, AS_DIGEST_VALUE_SIZE) == 0) {
			e->version = version;
			pthread_mutex_unlock(&h->lock);
			return;
		}

		e = e->next;
	} while (e != NULL);

	e = (as_txn_key*)cf_malloc(sizeof(as_txn_key));
	fill_ele(e, keyd, set, version);
	h->n_eles++;

	// Insert just after head.
	e->next = row->head.next;
	row->head.next = e;

	pthread_mutex_unlock(&h->lock);
}

static void
as_txn_hash_remove(as_txn_hash* h, const uint8_t* keyd)
{
	as_txn_hash_row* row = get_row(h, keyd);
	as_txn_key* e = &row->head;
	as_txn_key* e_prev = NULL;
	as_txn_key* free_e = NULL;

	pthread_mutex_lock(&h->lock);

	if (! row->used) {
		pthread_mutex_unlock(&h->lock);
		return;
	}

	do {
		if (memcmp(keyd, e->digest, AS_DIGEST_VALUE_SIZE) == 0) {
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
as_txn_hash_get_version(as_txn_hash* h, const uint8_t* keyd)
{
	as_txn_hash_row* row = get_row(h, keyd);

	if (! row->used) {
		return 0;
	}

	as_txn_key* e = &row->head;

	pthread_mutex_lock(&h->lock);

	if (! row->used) {
		pthread_mutex_unlock(&h->lock);
		return 0;
	}

	do {
		if (memcmp(keyd, e->digest, AS_DIGEST_VALUE_SIZE) == 0) {
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
as_txn_hash_contains(as_txn_hash* h, const uint8_t* keyd)
{
	as_txn_hash_row* row = get_row(h, keyd);

	if (! row->used) {
		return false;
	}

	as_txn_key* e = &row->head;

	pthread_mutex_lock(&h->lock);

	if (! row->used) {
		pthread_mutex_unlock(&h->lock);
		return false;
	}

	do {
		if (memcmp(keyd, e->digest, AS_DIGEST_VALUE_SIZE) == 0) {
			pthread_mutex_unlock(&h->lock);
			return true;
		}

		e = e->next;
	} while (e != NULL);

	pthread_mutex_unlock(&h->lock);
	return false;
}

static void
as_txn_init_all(as_txn* txn, uint32_t read_buckets, uint32_t write_buckets)
{
	// An id of zero is considered invalid. Create random numbers
	// in a loop until non-zero is returned.
	uint64_t id = cf_get_rand64();

	while (id == 0) {
		id = cf_get_rand64();
	}
	
	txn->id = id;
	txn->ns[0] = 0;
	txn->timeout = 0; // Zero means use server configuration mrt-duration.
	txn->deadline = 0;
	txn->state = AS_TXN_STATE_OPEN;
	txn->write_in_doubt = false;
	txn->in_doubt = false;
	as_txn_hash_init(&txn->reads, read_buckets);
	as_txn_hash_init(&txn->writes, write_buckets);
}

//---------------------------------
// Functions
//---------------------------------

void
as_txn_init(as_txn* txn)
{
	as_txn_init_all(txn, AS_TXN_READ_CAPACITY_DEFAULT, AS_TXN_WRITE_CAPACITY_DEFAULT);
	txn->free = false;
}

void
as_txn_init_capacity(as_txn* txn, uint32_t reads_capacity, uint32_t writes_capacity)
{
	if (reads_capacity < 16) {
		reads_capacity = 16;
	}

	if (writes_capacity < 16) {
		writes_capacity = 16;
	}

	// Double record capacity to allocate enough buckets to alleviate collisions.
	as_txn_init_all(txn, reads_capacity * 2, writes_capacity * 2);
	txn->free = false;
}

as_txn*
as_txn_create(void)
{
	as_txn* txn = cf_malloc(sizeof(as_txn));
	as_txn_init(txn);
	txn->free = true;
	return txn;
}

as_txn*
as_txn_create_capacity(uint32_t reads_capacity, uint32_t writes_capacity)
{
	as_txn* txn = cf_malloc(sizeof(as_txn));
	as_txn_init_capacity(txn, reads_capacity, writes_capacity);
	txn->free = true;
	return txn;
}

void
as_txn_destroy(as_txn* txn)
{
	as_txn_hash_destroy(&txn->reads);
	as_txn_hash_destroy(&txn->writes);
	
	if (txn->free) {
		cf_free(txn);
	}
}

void
as_txn_on_read(as_txn* txn, const uint8_t* digest, const char* set, uint64_t version)
{
	if (version != 0) {
		as_txn_hash_put(&txn->reads, digest, set, version);
	}
}

uint64_t
as_txn_get_read_version(as_txn* txn, const uint8_t* digest)
{
	return as_txn_hash_get_version(&txn->reads, digest);
}

void
as_txn_on_write(as_txn* txn, const uint8_t* digest, const char* set, uint64_t version, int rc)
{
	if (version != 0) {
		as_txn_hash_put(&txn->reads, digest, set, version);
	}
	else {
		if (rc == AEROSPIKE_OK) {
			as_txn_hash_remove(&txn->reads, digest);
			as_txn_hash_put(&txn->writes, digest, set, 0);
		}
	}
}

void
as_txn_on_write_in_doubt(as_txn* txn, const uint8_t* digest, const char* set)
{
	txn->write_in_doubt = true;
	as_txn_hash_remove(&txn->reads, digest);
	as_txn_hash_put(&txn->writes, digest, set, 0);
}

bool
as_txn_writes_contain(as_txn* txn, const as_key* key)
{
	return as_txn_hash_contains(&txn->writes, key->digest.value);
}

as_status
as_txn_verify_command(as_txn* txn, as_error* err)
{
	if (txn->state != AS_TXN_STATE_OPEN) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
			"Command not allowed in current transaction state: %d", txn->state);
	}
	return AEROSPIKE_OK;
}

as_status
as_txn_set_ns(as_txn* txn, const char* ns, as_error* err)
{
	if (txn->ns[0] == 0) {
		as_strncpy(txn->ns, ns, sizeof(txn->ns));
		return AEROSPIKE_OK;
	}
	
	if (strncmp(txn->ns, ns, sizeof(txn->ns)) != 0) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
			"Namespace must be the same for all commands in the transaction. orig: %s new: %s",
			txn->ns, ns);
	}
	return AEROSPIKE_OK;
}

void
as_txn_clear(as_txn* txn)
{
	txn->ns[0] = 0;
	txn->deadline = 0;
	as_txn_hash_clear(&txn->reads);
	as_txn_hash_clear(&txn->writes);
}

as_txn_key*
as_txn_iter_next(as_txn_iter* iter)
{
	if (iter->ele) {
		as_txn_key* tmp = iter->ele;
		iter->ele = tmp->next;
		return tmp;
	}

	while (iter->idx < iter->khash->n_rows) {
		if (iter->row->used) {
			as_txn_key* tmp = &iter->row->head;
			iter->ele = tmp->next;
			iter->row++;
			iter->idx++;
			return tmp;
		}
		iter->row++;
		iter->idx++;
	}
	return NULL;
}
