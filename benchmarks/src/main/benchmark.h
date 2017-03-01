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
#pragma once

#include "aerospike/aerospike.h"
#include "aerospike/as_event.h"
#include "aerospike/as_password.h"
#include "aerospike/as_random.h"
#include "aerospike/as_record.h"
#include "latency.h"

typedef enum {
	LEN_TYPE_COUNT,
	LEN_TYPE_BYTES,
	LEN_TYPE_KBYTES
} len_type;

typedef struct arguments_t {
	char* hosts;
	int port;
	const char* user;
	char password[AS_PASSWORD_HASH_SIZE];
	const char* namespace;
	const char* set;
	uint64_t start_key;
	uint64_t keys;
	char bintype;
	int binlen;
	int numbins;
	len_type binlen_type;
	bool random;
	bool init;
	int init_pct;
	int read_pct;
	bool del_bin;
	uint64_t transactions_limit;
	int threads;
	int throughput;
	int read_timeout;
	int write_timeout;
	int max_retries;
	bool debug;
	bool latency;
	int latency_columns;
	int latency_shift;
	bool use_shm;
	as_policy_replica read_replica;
	as_policy_consistency_level read_consistency_level;
	as_policy_commit_level write_commit_level;
	bool durable_deletes;
	bool async;
	int async_max_commands;
	int event_loop_capacity;
	as_config_tls tls;
} arguments;

typedef struct clientdata_t {
	const char* namespace;
	const char* set;
	const char* bin_name;
	
	uint64_t transactions_limit;
	uint64_t transactions_count;
	uint64_t key_min;
	uint64_t key_max;
	uint64_t key_count;
	uint64_t period_begin;
	
	aerospike client;
	as_val *fixed_value;
	
	latency write_latency;
	uint32_t write_count;
	uint32_t write_timeout_count;
	uint32_t write_error_count;
	
	uint32_t read_count;
	uint32_t read_timeout_count;
	uint32_t read_error_count;
	latency read_latency;

	uint32_t valid;
	
	int async_max_commands;
	int threads;
	int throughput;
	int read_pct;
	int binlen;
	int numbins;
	len_type binlen_type;
	
	char bintype;
	bool del_bin;
	bool random;
	bool latency;
	bool debug;
	bool async;
} clientdata;

typedef struct threaddata_t {
	clientdata* cdata;
	as_random* random;
	uint8_t* buffer;
	uint64_t begin;
	as_key key;
	as_record rec;
} threaddata;

int run_benchmark(arguments* args);
int linear_write(clientdata* data);
int random_read_write(clientdata* data);

threaddata* create_threaddata(clientdata* cdata, uint64_t key);
void destroy_threaddata(threaddata* tdata);

void write_record_sync(clientdata* cdata, threaddata* tdata, uint64_t key);
int read_record_sync(uint64_t key, clientdata* data);
void throttle(clientdata* cdata);

void linear_write_async(clientdata* cdata, threaddata* tdata, as_event_loop* event_loop);
void random_read_write_async(clientdata* cdata, threaddata* tdata, as_event_loop* event_loop);

int gen_value(arguments* args, as_val** val);
bool is_stop_writes(aerospike* client, const char* namespace);

void blog_line(const char* fmt, ...);
void blog_detail(as_log_level level, const char* fmt, ...);
void blog_detailv(as_log_level level, const char* fmt, va_list ap);

#define blog(_fmt, _args...) { printf(_fmt, ## _args); }
#define blog_info(_fmt, _args...) { blog_detail(AS_LOG_LEVEL_INFO, _fmt, ## _args); }
#define blog_error(_fmt, _args...) { blog_detail(AS_LOG_LEVEL_ERROR, _fmt, ## _args); }
