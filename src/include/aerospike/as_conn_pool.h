/*
 * Copyright 2008-2020 Aerospike, Inc.
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

#include <aerospike/as_queue.h>
#include <aerospike/as_socket.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * @private
 * Sync connection pool.
 */
typedef struct as_conn_pool_s {
	/**
	 * Mutex lock.
	 */
	pthread_mutex_t lock;

	/**
	 * Queue.
	 */
	as_queue queue;

	/**
	 * Minimum number of connections.
	 */
	uint32_t min_size;
} as_conn_pool;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * @private
 * Initialize a connection pool.
 */
static inline void
as_conn_pool_init(as_conn_pool* pool, uint32_t item_size, uint32_t min_size, uint32_t max_size)
{
	pthread_mutex_init(&pool->lock, NULL);
	as_queue_init(&pool->queue, item_size, max_size);
	pool->min_size = min_size;
}

/**
 * @private
 * Pop connection from head of pool.
 */
static inline bool
as_conn_pool_pop_head(as_conn_pool* pool, as_socket* sock)
{
	pthread_mutex_lock(&pool->lock);
	bool status = as_queue_pop(&pool->queue, sock);
	pthread_mutex_unlock(&pool->lock);
	return status;
}

/**
 * @private
 * Pop connection from tail of pool.
 */
static inline bool
as_conn_pool_pop_tail(as_conn_pool* pool, as_socket* sock)
{
	pthread_mutex_lock(&pool->lock);
	bool status = as_queue_pop_tail(&pool->queue, sock);
	pthread_mutex_unlock(&pool->lock);
	return status;
}

/**
 * @private
 * Push connection to head of pool if size < capacity.
 */
static inline bool
as_conn_pool_push_head(as_conn_pool* pool, as_socket* sock)
{
	pthread_mutex_lock(&pool->lock);
	bool status = as_queue_push_head_limit(&pool->queue, sock);
	pthread_mutex_unlock(&pool->lock);
	return status;
}

/**
 * @private
 * Push connection to tail of pool if size < capacity.
 */
static inline bool
as_conn_pool_push_tail(as_conn_pool* pool, as_socket* sock)
{
	pthread_mutex_lock(&pool->lock);
	bool status = as_queue_push_limit(&pool->queue, sock);
	pthread_mutex_unlock(&pool->lock);
	return status;
}

/**
 * @private
 * Increment connection total.
 * Return true if connection total is within limits.
 */
static inline bool
as_conn_pool_incr(as_conn_pool* pool)
{
	return as_faa_uint32(&pool->queue.total, 1) < pool->queue.capacity;
}

/**
 * @private
 * Decrement connection total.
 */
static inline void
as_conn_pool_decr(as_conn_pool* pool)
{
	as_decr_uint32(&pool->queue.total);
}

/**
 * @private
 * Return number of connections that might be closed.
 */
static inline int
as_conn_pool_excess(as_conn_pool* pool)
{
	return as_load_uint32(&pool->queue.total) - pool->min_size;
}

/**
 * @private
 * Destroy a connection pool.
 */
static inline void
as_conn_pool_destroy(as_conn_pool* pool)
{
	as_socket sock;

	pthread_mutex_lock(&pool->lock);

	while (as_queue_pop(&pool->queue, &sock)) {
		as_socket_close(&sock);
	}

	as_queue_destroy(&pool->queue);
	pthread_mutex_unlock(&pool->lock);
	pthread_mutex_destroy(&pool->lock);
}

#ifdef __cplusplus
} // end extern "C"
#endif
