/*******************************************************************************
 * Copyright 2008-2017 by Aerospike.
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
#include "benchmark.h"
#include <aerospike/as_monitor.h>
#include <citrusleaf/cf_clock.h>
#include <pthread.h>

extern as_monitor monitor;

static void*
ticker_worker(void* udata)
{
	clientdata* data = (clientdata*)udata;
	latency* write_latency = &data->write_latency;
	bool latency = data->latency;
	char latency_header[512];
	char latency_detail[512];
	
	uint64_t prev_time = cf_getms();
	data->period_begin = prev_time;
	
	if (latency) {
		latency_set_header(write_latency, latency_header);
	}
	sleep(1);

	uint64_t total_count = 0;
	bool complete = false;
	
	while (total_count < data->n_keys) {
		uint64_t time = cf_getms();
		int64_t elapsed = time - prev_time;
		prev_time = time;

		uint32_t write_current = ck_pr_fas_32(&data->write_count, 0);
		uint32_t write_timeout_current = ck_pr_fas_32(&data->write_timeout_count, 0);
		uint32_t write_error_current = ck_pr_fas_32(&data->write_error_count, 0);
		uint32_t write_tps = (uint32_t)((double)write_current * 1000 / elapsed + 0.5);
		total_count += write_current;

		data->period_begin = time;

		blog_info("write(tps=%u timeouts=%u errors=%u total=%" PRIu64 ")",
			write_tps, write_timeout_current, write_error_current, total_count);
		
		if (latency) {
			blog_line("%s", latency_header);
			latency_print_results(write_latency, "write", latency_detail);
			blog_line("%s", latency_detail);
		}

		if (complete) {
			break;
		}
		
		sleep(1);

		if (! data->valid) {
			// Go through one more iteration to print last line.
			complete = true;
		}
	}
	return 0;
}

static void*
linear_write_worker(void* udata)
{
	threaddata* tdata = (threaddata*)udata;
	clientdata* cdata = tdata->cdata;
	uint64_t key_start = tdata->key_start;
	uint64_t n_keys = tdata->n_keys;

	for (uint64_t i = 0; i < n_keys && cdata->valid; i++) {
		if (! write_record_sync(cdata, tdata, key_start + i)) {
			// An error occurred.
			// Keys must be linear, so repeat last key.
			i--;
		}
		throttle(cdata);
	}
	destroy_threaddata(tdata);
	return 0;
}

static void
linear_write_worker_async(clientdata* cdata)
{
	// Generate max command writes to seed the event loops.
	// Then start a new command in each command callback.
	// This effectively throttles new command generation, by only allowing
	// asyncMaxCommands at any point in time.
	as_monitor_begin(&monitor);

	if (cdata->async_max_commands > cdata->n_keys) {
		cdata->async_max_commands = (int)cdata->n_keys;
	}

	int max = cdata->async_max_commands;
	uint64_t keys_per_command = cdata->n_keys / max;
	uint64_t rem = cdata->n_keys - (keys_per_command * max);
	uint64_t start = cdata->key_start;

	for (int i = 1; i <= max; i++) {
		// Allocate separate buffers for each seed command and reuse them in callbacks.
		uint64_t key_count = (i < rem)? keys_per_command + 1 : keys_per_command;
		threaddata* tdata = create_threaddata(cdata, start, key_count);
		start += key_count;

		// Start seed commands on random event loops.
		linear_write_async(cdata, tdata, NULL);
	}
	as_monitor_wait(&monitor);
}

int
linear_write(clientdata* cdata)
{
	blog_info("Initialize %u records", cdata->n_keys);
	
	pthread_t ticker;
	if (pthread_create(&ticker, 0, ticker_worker, cdata) != 0) {
		cdata->valid = false;
		blog_error("Failed to create thread.");
		return -1;
	}
	
	if (cdata->async) {
		// Asynchronous mode.
		linear_write_worker_async(cdata);
	}
	else {
		// Synchronous mode.
		// Start threads with each thread performing writes in a loop.
		int max = cdata->threads;
		blog_info("Start %d generator threads", max);
		pthread_t threads[max];
		uint64_t keys_per_thread = cdata->n_keys / max;
		uint64_t rem = cdata->n_keys - (keys_per_thread * max);
		uint64_t start = cdata->key_start;

		for (int i = 0; i < max; i++) {
			uint64_t key_count = (i < rem)? keys_per_thread + 1 : keys_per_thread;
			threaddata* tdata = create_threaddata(cdata, start, key_count);
			start += key_count;

			if (pthread_create(&threads[i], 0, linear_write_worker, tdata) != 0) {
				destroy_threaddata(tdata);
				cdata->valid = false;
				blog_error("Failed to create thread.");
				return -1;
			}
		}

		for (int i = 0; i < max; i++) {
			pthread_join(threads[i], 0);
		}
	}
	cdata->valid = false;
	pthread_join(ticker, 0);
	return 0;
}
