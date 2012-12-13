/*
 * Aerospike Client - C Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
#include <sys/types.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>

#include "citrusleaf/citrusleaf.h"
#include "citrusleaf/citrusleaf-internal.h"

extern int g_init_pid;

#define MAX_PUT_QUEUE_THREADS 6

typedef struct {
	cl_cluster *asc;
	char ns[32];
	cf_digest digest;
	cl_bin* values;
	int n_values;
	cl_write_parameters wp;
} put_item;

static cf_queue* g_put_queue = 0;
static pthread_t g_put_queue_threads[MAX_PUT_QUEUE_THREADS];
static int g_put_queue_thread_count = 0;
static int g_size_limit = 0;
static cf_atomic32 g_put_queue_init = 0;

static void
fastncpy(char* trg, const char* src, int size)
{
	size--;

	for (int i = 0; i < size && *src; i++) {
		*trg++ = *src++;
	}
	*trg = 0;
}

static cl_rv
put_forget_item(const char *ns, const cl_bin *values, int n_values, const cl_write_parameters *wp, put_item* item)
{
	// Use lazy instantiation for put queue threads. Default to 1 worker thread.
	// The user can override the number of threads by calling citrusleaf_async_put_queue_init()
	// directly before making put forget calls.
	if (cf_atomic32_get(g_put_queue_init) == 0) {
		citrusleaf_put_queue_init(100000, 1);
	}

	// Discard items when queue is full.
	if (cf_queue_sz(g_put_queue) >= g_size_limit) {
		return CITRUSLEAF_FAIL_ASYNCQ_FULL;
	}

	fastncpy(item->ns, ns, sizeof(item->ns));
	citrusleaf_copy_bins(&item->values, values, n_values);
	item->n_values = n_values;
	memcpy(&item->wp, wp, sizeof(cl_write_parameters));

	cf_queue_push(g_put_queue, item);
	return CITRUSLEAF_OK;
}

//
// Add put request to put queue and return immediately. Background thread(s) will process the
// queue and send the put request to the appropriate server node.  The returned write status will
// not be available.
//
cl_rv
citrusleaf_put_forget(cl_cluster *asc, const char *ns, const char *set, const cl_object *key,
	const cl_bin *values, int n_values, const cl_write_parameters *wp)
{
	put_item item;
	item.asc = asc;
	citrusleaf_calculate_digest(set, key, &item.digest);
	return put_forget_item(ns, values, n_values, wp, &item);
}

//
// Add put request with digest key to put queue and return immediately.  Background thread(s)
// will process the queue and send the put request to the appropriate server node.  The returned
// write status will not be available.
//
cl_rv
citrusleaf_put_forget_digest(cl_cluster *asc, const char *ns, const cf_digest *digest,
	const cl_bin *values, int n_values, const cl_write_parameters *wp)
{
	put_item item;
	item.asc = asc;
	memcpy(&item.digest, digest, sizeof(cf_digest));
	return put_forget_item(ns, values, n_values, wp, &item);
}

//
// Process one queue's put requests.
//
static void*
put_queue_process(void* q_to_wait_on)
{
	cf_queue* worker_queue = (cf_queue*)q_to_wait_on;
	put_item item;

	while (1) {
		if (cf_queue_pop(worker_queue, &item, CF_QUEUE_FOREVER) != 0) {
			cf_error("Failed to pop from put queue.");
			break;
		}

		/* See citrusleaf_put_queue_shutdown() for more details */
		if(item.asc == 0) {
			pthread_exit(NULL);
		}

		citrusleaf_put_digest(item.asc, item.ns, &item.digest, item.values, item.n_values, &item.wp);
		citrusleaf_bins_free(item.values, item.n_values);
		free(item.values);
	}
	return 0;
}

//
// Initialize put queue and worker threads.
// size_limit: Maximum number of items allowed in queue. Puts are rejected when the maximum is reached.
// n_threads: Number of worker threads to create (Maximum is 6).
//
cl_rv
citrusleaf_put_queue_init(int size_limit, int n_threads)
{
	if (cf_atomic32_incr(&g_put_queue_init) != 1) {
		return CITRUSLEAF_OK;
	}

	g_size_limit = size_limit;
	g_put_queue = cf_queue_create(sizeof(put_item), true);

	if (n_threads <= 0) {
		n_threads = 1;
	}
	else if (n_threads > MAX_PUT_QUEUE_THREADS) {
		n_threads = MAX_PUT_QUEUE_THREADS;
		cf_warn("Put queue threads are limited to %d", MAX_PUT_QUEUE_THREADS);
	}

	g_put_queue_thread_count = n_threads;

	for (int i = 0; i < n_threads; i++) {
		pthread_create(&g_put_queue_threads[i], 0, put_queue_process, (void*)g_put_queue);
	}
	return CITRUSLEAF_OK;
}

//
// Close put queue threads gracefully.
//
void
citrusleaf_put_queue_shutdown()
{
	if (g_put_queue_thread_count <= 0)
		return;

	/* 
	 * If a process is forked, the threads in it do not get spawned in the child process.
	 * In citrusleaf_init(), we are remembering the process id(g_init_pid) of the process who spawned the
	 * background threads. If the current process is not the process who spawned the background threads
	 * then it cannot call pthread_join() on the threads which does not exist in this process.
	 */
	if(g_init_pid == getpid()) {
		put_item item;

		memset(&item, 0, sizeof(put_item));

		int i;

		for (i = 0; i < g_put_queue_thread_count; i++) {
			cf_queue_push(g_put_queue, &item);
		}

		for (i = 0; i < g_put_queue_thread_count; i++) {
			pthread_join(g_put_queue_threads[i], NULL);
		}

		cf_queue_destroy(g_put_queue);
		g_put_queue = 0;
	}
}
