/*******************************************************************************
 * Copyright 2008-2016 by Aerospike.
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
#include <aerospike/as_random.h>
#include <citrusleaf/cf_clock.h>
#include <pthread.h>
#include <unistd.h>

extern as_monitor monitor;

static void*
ticker_worker(void* udata)
{
	clientdata* data = (clientdata*)udata;
	latency* write_latency = &data->write_latency;
	latency* read_latency = &data->read_latency;
	bool latency = data->latency;
	char latency_header[512];
	char latency_detail[512];
	
	uint64_t prev_time = cf_getms();
	data->period_begin = prev_time;
	
	if (latency) {
		latency_set_header(write_latency, latency_header);
	}
	sleep(1);
	
	while (data->valid) {
		uint64_t time = cf_getms();
		int64_t elapsed = time - prev_time;
		prev_time = time;
		
		uint32_t write_current = ck_pr_fas_32(&data->write_count, 0);
		uint32_t write_timeout_current = ck_pr_fas_32(&data->write_timeout_count, 0);
		uint32_t write_error_current = ck_pr_fas_32(&data->write_error_count, 0);
		uint32_t read_current = ck_pr_fas_32(&data->read_count, 0);
		uint32_t read_timeout_current = ck_pr_fas_32(&data->read_timeout_count, 0);
		uint32_t read_error_current = ck_pr_fas_32(&data->read_error_count, 0);
		uint32_t transactions_current = ck_pr_load_32(&data->transactions_count);

		data->period_begin = time;
	
		uint32_t write_tps = (uint32_t)((double)write_current * 1000 / elapsed + 0.5);
		uint32_t read_tps = (uint32_t)((double)read_current * 1000 / elapsed + 0.5);
		
		blog_info("write(tps=%d timeouts=%d errors=%d) read(tps=%d timeouts=%d errors=%d) total(tps=%d timeouts=%d errors=%d)",
			write_tps, write_timeout_current, write_error_current,
			read_tps, read_timeout_current, read_error_current,
			write_tps + read_tps, write_timeout_current + read_timeout_current, write_error_current + read_error_current);
		
		if (latency) {
			blog_line("%s", latency_header);
			latency_print_results(write_latency, "write", latency_detail);
			blog_line("%s", latency_detail);
			latency_print_results(read_latency, "read", latency_detail);
			blog_line("%s", latency_detail);
		}

		if ((data->transactions_limit > 0) && (transactions_current > data->transactions_limit)) {
			blog_line("Performed %d (> %d) transactions. Shutting down...", transactions_current, data->transactions_limit);
			data->valid = false;
			continue;
		}

		if (write_timeout_current + write_error_current > 10) {
			if (is_stop_writes(&data->client, data->host, data->port, data->namespace)) {
				if (data->valid) {
					blog_error("Server is currently in readonly mode. Shutting down...");
					data->valid = false;
					continue;
				}
			}
		}

		sleep(1);
	}
	return 0;
}

static void*
random_worker(void* udata)
{
	clientdata* cdata = (clientdata*)udata;
	threaddata* tdata = create_threaddata(cdata, 0);
	uint32_t key_max = cdata->key_max;
	int throughput = cdata->throughput;
	int read_pct = cdata->read_pct;
	int key;
	int die;
	
	while (cdata->valid) {
		// Choose key at random.
		key = as_random_next_uint32(tdata->random) % key_max + 1;
		
		// Roll a percentage die.
		die = as_random_next_uint32(tdata->random) % 100;
		
		if (die < read_pct) {
			read_record_sync(key, cdata);
		}
		else {
			write_record_sync(cdata, tdata, key);
		}
		ck_pr_inc_32(&cdata->transactions_count);

		if (throughput > 0) {
			int transactions = cdata->write_count + cdata->read_count;
			
			if (transactions > throughput) {
				int64_t millis = (int64_t)cdata->period_begin + 1000L - (int64_t)cf_getms();
				
				if (millis > 0) {
					usleep((uint32_t)millis * 1000);
				}
			}
		}
	}
	destroy_threaddata(tdata);
	return 0;
}

static void
random_worker_async(clientdata* cdata)
{
	// Generate max command writes to seed the event loops.
	// Then start a new command in each command callback.
	// This effectively throttles new command generation, by only allowing
	// asyncMaxCommands at any point in time.
	as_monitor_begin(&monitor);
	
	int max = cdata->async_max_commands;
	
	for (int i = 1; i <= max; i++) {
		// Allocate separate buffers for each seed command and reuse them in callbacks.
		threaddata* tdata = create_threaddata(cdata, 0);
		
		// Start seed commands on random event loops.
		random_read_write_async(cdata, tdata, 0);
	}
	as_monitor_wait(&monitor);
}

int
random_read_write(clientdata* data)
{
	blog_info("Read/write using %d records", data->key_max);
	
	pthread_t ticker;
	if (pthread_create(&ticker, 0, ticker_worker, data) != 0) {
		data->valid = false;
		blog_error("Failed to create thread.");
		return -1;
	}
	
	if (data->async) {
		// Asynchronous mode.
		random_worker_async(data);
	}
	else {
		// Synchronous mode.
		int max = data->threads;
		blog_info("Start %d generator threads", max);
		pthread_t threads[max];
		
		for (int i = 0; i < max; i++) {
			if (pthread_create(&threads[i], 0, random_worker, data) != 0) {
				data->valid = false;
				blog_error("Failed to create thread.");
				return -1;
			}
		}
		
		for (int i = 0; i < max; i++) {
			pthread_join(threads[i], 0);
		}
	}
	data->valid = false;
	pthread_join(ticker, 0);
	return 0;
}
