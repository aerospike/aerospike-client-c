/*******************************************************************************
 * Copyright 2008-2013 by Aerospike.
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
#pragma once

#include "aerospike/aerospike.h"
#include "aerospike/as_record.h"
#include "latency.h"

typedef struct arguments_t {
	char* host_string;
	char** hosts;
	int host_count;
	int port;
	const char* namespace;
	const char* set;
	int keys;
	char bintype;
	int binlen;
	bool random;
	bool init;
	int init_pct;
	int read_pct;
	int threads;
	int throughput;
	int read_timeout;
	int write_timeout;
	int max_retries;
	bool debug;
	bool latency;
	int latency_columns;
	int latency_shift;
} arguments;

typedef struct clientdata_t {
	const char* host;
	const char* namespace;
	const char* set;
	const char* bin_name;
	
	cf_atomic64 period_begin;
	
	aerospike client;
	as_bin_value fixed_value;
	
	latency write_latency;
	cf_atomic32 write_count;
	cf_atomic32 write_timeout_count;
	cf_atomic32 write_error_count;
	
	latency read_latency;
	cf_atomic32 read_count;
	cf_atomic32 read_timeout_count;
	cf_atomic32 read_error_count;
	
	cf_atomic32 current_key;
	cf_atomic32 valid;
	int32_t records;
	
	int port;
	int threads;
	int throughput;
	int read_pct;
	int binlen;
	char bintype;
	
	bool random;
	bool latency;
	bool debug;
} clientdata;

int run_benchmark(arguments* args);
int linear_write(clientdata* data);
int random_read_write(clientdata* data);
int write_record(int key, clientdata* data);
int read_record(int key, clientdata* data);
int gen_value(arguments* args, as_bin_value* val);
bool is_stop_writes(aerospike* client, const char* host, int port, const char* namespace);

void blog_line(const char* fmt, ...);
void blog_detail(as_log_level level, const char* fmt, ...);
void blog_detailv(as_log_level level, const char* fmt, va_list ap);

#define blog(_fmt, _args...) { printf(_fmt, ## _args); }
#define blog_info(_fmt, _args...) { blog_detail(AS_LOG_LEVEL_INFO, _fmt, ## _args); }
#define blog_error(_fmt, _args...) { blog_detail(AS_LOG_LEVEL_ERROR, _fmt, ## _args); }
