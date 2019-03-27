/*******************************************************************************
 * Copyright 2008-2019 by Aerospike.
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
#include "aerospike/aerospike_info.h"
#include "aerospike/as_config.h"
#include "aerospike/as_event.h"
#include "aerospike/as_log.h"
#include "aerospike/as_monitor.h"
#include "aerospike/as_random.h"
#include <stdlib.h>
#include <time.h>

as_monitor monitor;

void
blog_line(const char* fmt, ...)
{
	char fmtbuf[1024];
	size_t len = strlen(fmt);
	memcpy(fmtbuf, fmt, len);
	char* p = fmtbuf + len;
	*p++ = '\n';
	*p = 0;
	
	va_list ap;
	va_start(ap, fmt);
	vprintf(fmtbuf, ap);
	va_end(ap);
}

void
blog_detailv(as_log_level level, const char* fmt, va_list ap)
{
	// Write message all at once so messages generated from multiple threads have less of a chance
	// of getting garbled.
	char fmtbuf[1024];
	time_t now = time(NULL);
	struct tm* t = localtime(&now);
	int len = sprintf(fmtbuf, "%d-%02d-%02d %02d:%02d:%02d %s ",
		t->tm_year+1900, t->tm_mon+1, t->tm_mday, t->tm_hour, t->tm_min, t->tm_sec, as_log_level_tostring(level));
	size_t len2 = strlen(fmt);
	char* p = fmtbuf + len;
	memcpy(p, fmt, len2);
	p += len2;
	*p++ = '\n';
	*p = 0;
	
	vprintf(fmtbuf, ap);
}

void
blog_detail(as_log_level level, const char* fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	blog_detailv(level, fmt, ap);
	va_end(ap);
}

static bool
as_client_log_callback(as_log_level level, const char * func, const char * file, uint32_t line, const char * fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	blog_detailv(level, fmt, ap);
	va_end(ap);
	return true;
}

static int
connect_to_server(arguments* args, aerospike* client)
{
	if (args->async) {
		as_monitor_init(&monitor);

#if AS_EVENT_LIB_DEFINED
		if (! as_event_create_loops(args->event_loop_capacity)) {
			blog_error("Failed to create asynchronous event loops");
			return 2;
		}
#else
		blog_error("Must 'make EVENT_LIB=<libname>' to use asynchronous functions.");
		return 2;
#endif
	}
	
	as_config cfg;
	as_config_init(&cfg);
	
	if (! as_config_add_hosts(&cfg, args->hosts, args->port)) {
		blog_error("Invalid host(s) %s", args->hosts);
		return 3;
	}

	as_config_set_user(&cfg, args->user, args->password);
	cfg.use_shm = args->use_shm;
	cfg.conn_timeout_ms = 10000;
	cfg.login_timeout_ms = 10000;

	// Disable batch/scan/query thread pool because these commands are not used in benchmarks.
	cfg.thread_pool_size = 0;
	cfg.conn_pools_per_node = args->conn_pools_per_node;

	if (cfg.async_max_conns_per_node < (uint32_t)args->async_max_commands) {
		cfg.async_max_conns_per_node = args->async_max_commands;
	}

	as_policies* p = &cfg.policies;

	p->read.base.total_timeout = args->read_timeout;
	p->read.base.max_retries = args->max_retries;
	p->read.replica = args->replica;
	p->read.read_mode_ap = args->read_mode_ap;
	p->read.read_mode_sc = args->read_mode_sc;

	p->write.base.total_timeout = args->write_timeout;
	p->write.base.max_retries = args->max_retries;
	p->write.replica = args->replica;
	p->write.commit_level = args->write_commit_level;
	p->write.durable_delete = args->durable_deletes;

	p->operate.base.total_timeout = args->write_timeout;
	p->operate.base.max_retries = args->max_retries;
	p->operate.replica = args->replica;
	p->operate.commit_level = args->write_commit_level;
	p->operate.durable_delete = args->durable_deletes;
	p->operate.read_mode_ap = args->read_mode_ap;
	p->operate.read_mode_sc = args->read_mode_sc;

	p->remove.base.total_timeout = args->write_timeout;
	p->remove.base.max_retries = args->max_retries;
	p->remove.replica = args->replica;
	p->remove.commit_level = args->write_commit_level;
	p->remove.durable_delete = args->durable_deletes;

	p->info.timeout = 10000;

	// Transfer ownership of all heap allocated TLS fields via shallow copy.
	memcpy(&cfg.tls, &args->tls, sizeof(as_config_tls));
	cfg.auth_mode = args->auth_mode;

	aerospike_init(client, &cfg);
	
	as_error err;
	
	if (aerospike_connect(client, &err) != AEROSPIKE_OK) {
		blog_error("%s", err.message);
		aerospike_destroy(client);
		return 1;
	}
	return 0;
}

static bool
is_single_bin(aerospike* client, const char* namespace)
{
	char filter[256];
	sprintf(filter, "namespace/%s", namespace);
	
	char* res = 0;
	as_error err;
	as_status rc = aerospike_info_any(client, &err, 0, filter, &res);
	bool single_bin = false;
	
	if (rc == AEROSPIKE_OK) {
		static const char* search = "single-bin=";
		char* p = strstr(res, search);
		
		if (p) {
			// The compiler (with -O3 flag) will know search is a literal and optimize strlen accordingly.
			p += strlen(search);
			char* q = strchr(p, ';');
			
			if (q) {
				*q = 0;
				
				if (strcmp(p, "true") == 0) {
					single_bin = true;
				}
			}
		}
		free(res);
	}
	else {
		blog_error("Info request failed: %d - %s", err.code, err.message);
	}
	return single_bin;
}

bool
is_stop_writes(aerospike* client, const char* namespace)
{
	char filter[256];
	sprintf(filter, "namespace/%s", namespace);
	
	char* res = 0;
	as_error err;
	as_status rc = aerospike_info_any(client, &err, NULL, filter, &res);
	bool stop_writes = false;
	
	if (rc == AEROSPIKE_OK) {
		static const char* search = "stop-writes=";
		char* p = strstr(res, search);
		
		if (p) {
			// The compiler (with -O3 flag) will know search is a literal and optimize strlen accordingly.
			p += strlen(search);
			char* q = strchr(p, ';');
			
			if (q) {
				*q = 0;
				
				if (strcmp(p, "true") == 0) {
					stop_writes = true;
				}
			}
		}
	}
	else {
		blog_error("Info request failed: %d - %s", err.code, err.message);
	}
	free(res);
	return stop_writes;
}

int
run_benchmark(arguments* args)
{
	clientdata data;
	memset(&data, 0, sizeof(clientdata));
	data.namespace = args->namespace;
	data.set = args->set;
	data.threads = args->threads;
	data.throughput = args->throughput;
	data.read_pct = args->read_pct;
	data.del_bin = args->del_bin;
	data.bintype = args->bintype;
	data.binlen = args->binlen;
	data.binlen_type = args->binlen_type;
	data.numbins = args->numbins;
	data.random = args->random;
	data.transactions_limit = args->transactions_limit;
	data.transactions_count = 0;
	data.latency = args->latency;
	data.debug = args->debug;
	data.valid = 1;
	data.async = args->async;
	data.async_max_commands = args->async_max_commands;
	data.fixed_value = NULL;

	if (args->debug) {
		as_log_set_level(AS_LOG_LEVEL_DEBUG);
	}
	else {
		as_log_set_level(AS_LOG_LEVEL_INFO);
	}

	as_log_set_callback(as_client_log_callback);

	int ret = connect_to_server(args, &data.client);
	
	if (ret != 0) {
		return ret;
	}
	
	bool single_bin = is_single_bin(&data.client, args->namespace);
	
	if (single_bin) {
		data.bin_name = "";
	}
	else {
		data.bin_name = "testbin";
	}

	if (! args->random) {
		gen_value(args, &data.fixed_value);
	}
	
	if (args->latency) {
		latency_init(&data.write_latency, args->latency_columns, args->latency_shift);
		
		if (! args->init) {
			latency_init(&data.read_latency, args->latency_columns, args->latency_shift);
		}
	}

	data.key_start = args->start_key;
	data.key_count = 0;

	if (args->init) {
		data.n_keys = (uint64_t)((double)args->keys / 100.0 * args->init_pct + 0.5);
		ret = linear_write(&data);
	}
	else {
		data.n_keys = args->keys;
		ret = random_read_write(&data);
	}
	
	if (! args->random) {
		as_val_destroy(data.fixed_value);
	}

	if (args->latency) {
		latency_free(&data.write_latency);
		
		if (! args->init) {
			latency_free(&data.read_latency);
		}
	}

	as_error err;
	aerospike_close(&data.client, &err);
	aerospike_destroy(&data.client);
	
	if (args->async) {
		as_event_close_loops();
		as_monitor_destroy(&monitor);
	}
	
	return ret;
}
