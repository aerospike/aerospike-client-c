/*
 * Copyright 2008-2014 Aerospike, Inc.
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
#include <aerospike/aerospike_query.h>
#include <aerospike/as_aerospike.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_command.h>
#include <aerospike/as_error.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_module.h>
#include <aerospike/as_msgpack.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_query.h>
#include <aerospike/as_serializer.h>
#include <aerospike/as_socket.h>
#include <aerospike/as_status.h>
#include <aerospike/as_stream.h>
#include <aerospike/as_udf_context.h>
#include <aerospike/mod_lua.h>
#include <citrusleaf/cf_random.h>
#include <stdint.h>

/******************************************************************************
 * TYPES
 *****************************************************************************/

typedef struct as_query_task_s {
	as_node* node;
	
	as_cluster* cluster;
	const as_policy_query* policy;
	const as_query* query;
	aerospike_query_foreach_callback callback;
	void* udata;
	as_error* err;
	cf_queue* stream_q;
	cf_queue* complete_q;
	uint32_t* error_mutex;
	uint64_t task_id;
	
	uint8_t* cmd;
	size_t cmd_size;
} as_query_task;

typedef struct as_query_complete_task_s {
	as_node* node;
	uint64_t task_id;
	as_status result;
} as_query_complete_task;

typedef struct as_query_stream_callback_s {
    void* udata;
    aerospike_query_foreach_callback callback;
} as_query_stream_callback;

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static int
as_query_aerospike_log(const as_aerospike * as, const char * file, const int line, const int level, const char * msg)
{
    switch(level) {
        case 1:
			as_log_warn("%s:%d - %s", file, line, msg);
            break;
        case 2:
			as_log_info("%s:%d - %s", file, line, msg);
            break;
        case 3:
 			as_log_debug("%s:%d - %s", file, line, msg);
            break;
        default:
			as_log_trace("%s:%d - %s", file, line, msg);
            break;
    }
    return 0;
}

static const as_aerospike_hooks query_aerospike_hooks = {
    .destroy = NULL,
    .rec_create = NULL,
    .rec_update = NULL,
    .rec_remove = NULL,
    .rec_exists = NULL,
    .log = as_query_aerospike_log,
};

// This is a no-op. the queue and its contents are destroyed in cl_query_destroy().
static int
as_queue_stream_destroy(as_stream *s)
{
    return 0;
}

static as_val*
as_queue_stream_read(const as_stream* s)
{
    as_val* val = NULL;
	
    if (cf_queue_pop(as_stream_source(s), &val, CF_QUEUE_NOWAIT) == CF_QUEUE_EMPTY) {
        return NULL;
    }
    // Push it back so it can be destroyed
    cf_queue_push(as_stream_source(s), &val);
    return val;
}

static as_stream_status
as_queue_stream_write(const as_stream* s, as_val* val)
{
    if (cf_queue_push(as_stream_source(s), &val) != CF_QUEUE_OK) {
        as_log_error("Write to client side stream failed.");
        as_val_destroy(val);
        return AS_STREAM_ERR;
    }
    return AS_STREAM_OK;
}

static const as_stream_hooks queue_stream_hooks = {
    .destroy  = as_queue_stream_destroy,
    .read     = as_queue_stream_read,
    .write    = as_queue_stream_write
};

static int
as_callback_stream_destroy(as_stream* s)
{
    return 0;
}

static as_stream_status
as_callback_stream_write(const as_stream* s, as_val* val)
{
	as_query_stream_callback* source = (as_query_stream_callback*)as_stream_source(s);
	bool rv = source->callback(val, source->udata);
	as_val_destroy(val);
	return rv? AS_STREAM_OK : AS_STREAM_ERR;
}

static const as_stream_hooks callback_stream_hooks = {
    .destroy  = as_callback_stream_destroy,
    .read     = NULL,
    .write    = as_callback_stream_write
};

// This callback will populate an intermediate stream, to be used for the aggregation.
static bool
as_query_aggregate_callback(const as_val* v, void* udata)
{
	as_stream* queue_stream = (as_stream*)udata;
	as_stream_status status = as_stream_write(queue_stream, (as_val*)v);
    return status? false : true;
}

static as_status
as_query_parse_record(uint8_t** pp, as_msg* msg, as_query_task* task, as_error* err)
{
	bool rv = true;
	
	if (task->stream_q) {
		// Parse aggregate return values.
		as_val* val = 0;
		as_status status = as_command_parse_success_failure_bins(pp, err, msg, &val);
		
		if (status != AEROSPIKE_OK) {
			return status;
		}
		
		if (task->callback) {
			rv = task->callback(val, task->udata);
		}
		else {
			as_val_destroy(val);
		}
	}
	else {
		// Parse normal record values.
		as_record rec;
		as_record_inita(&rec, msg->n_ops);
		
		rec.gen = msg->generation;
		rec.ttl = cf_server_void_time_to_ttl(msg->record_ttl);
		
		uint8_t* p = *pp;
		p = as_command_parse_key(p, msg->n_fields, &rec.key);
		p = as_command_parse_bins(&rec, p, msg->n_ops, true);
		*pp = p;
		
		if (task->callback) {
			rv = task->callback((as_val*)&rec, task->udata);
		}
		as_record_destroy(&rec);
	}
	return rv ? AEROSPIKE_OK : AEROSPIKE_ERR_CLIENT_ABORT;
}

static as_status
as_query_parse_records(uint8_t* buf, size_t size, as_query_task* task, as_error* err)
{
	uint8_t* p = buf;
	uint8_t* end = buf + size;
	as_status status;
	
	while (p < end) {
		as_msg* msg = (as_msg*)p;
		as_msg_swap_header_from_be(msg);
		
		if (msg->result_code) {
			return as_error_set_message(err, msg->result_code, as_error_string(msg->result_code));
		}
		p += sizeof(as_msg);
		
		if (msg->info3 & AS_MSG_INFO3_LAST) {
			return AEROSPIKE_NO_MORE_RECORDS;
		}
		
		status = as_query_parse_record(&p, msg, task, err);
		
		if (status != AEROSPIKE_OK) {
			return status;
		}
		
		if (ck_pr_load_32(task->error_mutex)) {
			err->code = AEROSPIKE_ERR_QUERY_ABORTED;
			return err->code;
		}
	}
	return AEROSPIKE_OK;
}

static as_status
as_query_parse(as_error* err, int fd, uint64_t deadline_ms, void* udata)
{
	as_query_task* task = udata;
	as_status status = AEROSPIKE_OK;
	uint8_t* buf = 0;
	size_t capacity = 0;
	
	while (true) {
		// Read header
		as_proto proto;
		status = as_socket_read_deadline(err, fd, (uint8_t*)&proto, sizeof(as_proto), deadline_ms);
		
		if (status) {
			break;
		}
		as_proto_swap_from_be(&proto);
		size_t size = proto.sz;
		
		if (size > 0) {
			// Prepare buffer
			if (size > capacity) {
				as_command_free(buf, capacity);
				capacity = size;
				buf = as_command_init(capacity);
			}
			
			// Read remaining message bytes in group
			status = as_socket_read_deadline(err, fd, buf, size, deadline_ms);
			
			if (status) {
				break;
			}
			
			status = as_query_parse_records(buf, size, task, err);
			
			if (status != AEROSPIKE_OK) {
				if (status == AEROSPIKE_NO_MORE_RECORDS) {
					status = AEROSPIKE_OK;
				}
				break;
			}
		}
	}
	as_command_free(buf, capacity);
	return status;
}

static as_status
as_query_command_execute(as_query_task* task)
{
	as_command_node cn;
	cn.node = task->node;
	
	as_error err;
	as_error_init(&err);
	as_status status = as_command_execute(&err, &cn, task->cmd, task->cmd_size, task->policy->timeout, AS_POLICY_RETRY_NONE, as_query_parse, task);
		
	if (status) {
		// Set main error only once.
		if (ck_pr_fas_32(task->error_mutex, 1) == 0) {
			// Don't set error when user aborts query,
			if (status != AEROSPIKE_ERR_CLIENT_ABORT) {
				as_error_copy(task->err, &err);
			}
		}
	}
	return status;
}

static void*
as_query_worker(void* data)
{
	as_cluster* cluster = (as_cluster*)data;
	as_query_task task;
	
	while (cf_queue_pop(cluster->query_q, &task, CF_QUEUE_FOREVER) == CF_QUEUE_OK) {
		// This is how batch shutdown signals we're done.
		if (! task.cluster) {
			break;
		}
		
		as_query_complete_task complete_task;
		complete_task.node = task.node;
		complete_task.task_id = task.task_id;
		complete_task.result = as_query_command_execute(&task);
		
		cf_queue_push(task.complete_q, &complete_task);
	}
	return 0;
}

static void
as_query_threads_init(as_cluster* cluster)
{
	// We do this lazily, during the first query request, so make sure it's only
	// done once.
	if (ck_pr_fas_32(&cluster->query_initialized, 1) == 1 || cluster->query_q) {
		return;
	}
	
	// Create dispatch queue.
	cluster->query_q = cf_queue_create(sizeof(as_query_task), true);
	
	// Create thread pool.
	for (int i = 0; i < AS_NUM_QUERY_THREADS; i++) {
		pthread_create(&cluster->query_threads[i], 0, as_query_worker, cluster);
	}
}

void
as_query_threads_shutdown(as_cluster* cluster)
{
	// Check whether we ever (lazily) initialized query machinery.
	if (ck_pr_load_32(&cluster->query_initialized) == 0 && ! cluster->query_q) {
		return;
	}
	
	// This tells the worker threads to stop. We do this (instead of using a
	// "running" flag) to allow the workers to "wait forever" on processing the
	// work dispatch queue, which has minimum impact when the queue is empty.
	// This also means all queued requests get processed when shutting down.
	for (int i = 0; i < AS_NUM_QUERY_THREADS; i++) {
		as_query_task task;
		task.cluster = NULL;
		cf_queue_push(cluster->query_q, &task);
	}
	
	for (int i = 0; i < AS_NUM_QUERY_THREADS; i++) {
		pthread_join(cluster->query_threads[i], NULL);
	}
	
	cf_queue_destroy(cluster->query_q);
	cluster->query_q = NULL;
	ck_pr_store_32(&cluster->query_initialized, 0);
}

static uint8_t*
as_query_write_range_string(uint8_t* p, char* begin, char* end)
{
	// Write particle type.
	*p++ = AS_BYTES_STRING;
	
	// Write begin value.
	char* q = begin;
	uint32_t* len_ptr = (uint32_t*)p;
	p += 4;
	while (*q) {
		*p++ = *q++;
	}
	*len_ptr = cf_swap_to_be32((uint32_t)(q - begin));
	
	// Write end value.
	q = end;
	len_ptr = (uint32_t*)p;
	p += 4;
	while (*q) {
		*p++ = *q++;
	}
	*len_ptr = cf_swap_to_be32((uint32_t)(q - end));
	
	return p;
}

static uint8_t*
as_query_write_range_integer(uint8_t* p, int64_t begin, int64_t end)
{
	// Write particle type.
	*p++ = AS_BYTES_INTEGER;

	// Write begin value.
	*(uint32_t*)p = cf_swap_to_be32(sizeof(int64_t));
	p += 4;
	*(int64_t*)p = cf_swap_to_be64(begin);
	p += sizeof(int64_t);

	// Write end value.
	*(uint32_t*)p = cf_swap_to_be32(sizeof(int64_t));
	p += 4;
	*(int64_t*)p = cf_swap_to_be64(end);
	p += sizeof(int64_t);
	
	return p;
}

static as_status
as_query_execute(as_query_task* task, const as_query * query, as_nodes* nodes, uint32_t n_nodes)
{
	// Build Command.  It's okay to share command across threads because query does not have retries.
	// If retries were allowed, the timeout field in the command would change on retry which
	// would conflict with other threads.
	size_t size = AS_HEADER_SIZE;
	uint32_t filter_size = 0;
	uint32_t bin_name_size = 0;
	uint16_t n_fields = 0;
	
	// Estimate namespace size.
	if (query->ns) {
		size += as_command_string_field_size(query->ns);
		n_fields++;
	}
	
	// Estimate set size.
	if (query->set) {
		size += as_command_string_field_size(query->set);
		n_fields++;
	}
	
	// Estimate indextype size.
	// For single where clause queries
	if (query->where.size == 1) {
		size += as_command_field_size(1);
		n_fields++;
	}

	// Estimate taskId size.
	size += as_command_field_size(8);
	n_fields++;
	
	// Estimate size of query filters.
	if (query->where.size > 0) {
		size += AS_FIELD_HEADER_SIZE;
		filter_size++;  // Add byte for num filters.
		
		for (uint16_t i = 0; i < query->where.size; i++ ) {
			as_predicate* pred = &query->where.entries[i];
			
			// bin name size(1) + particle type size(1) + begin particle size(4) + end particle size(4) = 10
			filter_size += strlen(pred->bin) + 10;
			
			switch(pred->type) {
				case AS_PREDICATE_EQUAL:
					if (pred->dtype == AS_INDEX_STRING) {
						filter_size += strlen(pred->value.string) * 2;
					}
					else if (pred->dtype == AS_INDEX_NUMERIC) {
						filter_size += sizeof(int64_t) * 2;
					}
					break;
				case AS_PREDICATE_RANGE:
					filter_size += sizeof(int64_t) * 2;
					break;
			}
		}
		size += filter_size;
		n_fields++;

		// Query bin names are specified as a field (Scan bin names are specified later as operations)
		// Estimate size for selected bin names.
		if (query->select.size > 0) {
			size += AS_FIELD_HEADER_SIZE;
			bin_name_size++;  // Add byte for num bin names.

			for (uint16_t i = 0; i < query->select.size; i++) {
				bin_name_size += strlen(query->select.entries[i]) + 1;
			}
			size += bin_name_size;
			n_fields++;
		}
	}
	else {
		// Estimate scan options size.
		size += as_command_field_size(2);
		n_fields++;
	}
	
	// Estimate background function size.
	as_buffer argbuffer;
	as_buffer_init(&argbuffer);
	
	if (query->apply.function[0]) {
		size += as_command_field_size(1);
		size += as_command_string_field_size(query->apply.module);
		size += as_command_string_field_size(query->apply.function);
		
		if (query->apply.arglist) {
			// If the query has a udf w/ arglist, then serialize it.
			as_serializer ser;
			as_msgpack_init(&ser);
            as_serializer_serialize(&ser, (as_val*)query->apply.arglist, &argbuffer);
			as_serializer_destroy(&ser);
		}
		size += as_command_field_size(argbuffer.size);
		n_fields += 4;
	}
	
	// Estimate size for selected bin names on scan (query bin names already handled).
	if (query->where.size == 0) {
		if (query->select.size > 0) {
			for (uint16_t i = 0; i < query->select.size; i++) {
				size += as_command_string_operation_size(query->select.entries[i]);
			}
		}
	}
	
	// Write command buffer.
	uint8_t* cmd = as_command_init(size);
	uint16_t n_ops = (query->where.size == 0)? query->select.size : 0;
	uint8_t* p = as_command_write_header_read(cmd, AS_MSG_INFO1_READ, AS_POLICY_CONSISTENCY_LEVEL_ONE, task->policy->timeout, n_fields, n_ops);
	
	// Write namespace.
	if (query->ns) {
		p = as_command_write_field_string(p, AS_FIELD_NAMESPACE, query->ns);
	}
	
	// Write set.
	if (query->set) {
		p = as_command_write_field_string(p, AS_FIELD_SETNAME, query->set);
	}
	
	// Write indextype.
	if (query->where.size == 1) {
		as_predicate* pred = &query->where.entries[0];
		p = as_command_write_field_header(p, AS_FIELD_INDEX_TYPE, 1);
		*p++ = pred->itype;
	}

	// Write taskId field
	p = as_command_write_field_uint64(p, AS_FIELD_TASK_ID, task->task_id);

	// Write query filters.
	if (query->where.size > 0) {
		p = as_command_write_field_header(p, AS_FIELD_INDEX_RANGE, filter_size);
		*p++ = query->where.size;
				
		for (uint16_t i = 0; i < query->where.size; i++ ) {
			as_predicate* pred = &query->where.entries[i];
			
			// Write bin name, but do not transfer null byte.
			uint8_t* len_ptr = p++;
			uint8_t* s = (uint8_t*)pred->bin;
			while (*s) {
				*p++ = *s++;
			}
			*len_ptr = (uint8_t)(s - (uint8_t*)pred->bin);
			
			// Write particle type and range values.
			switch(pred->type) {
				case AS_PREDICATE_EQUAL:
					if (pred->dtype == AS_INDEX_STRING) {
						p = as_query_write_range_string(p, pred->value.string, pred->value.string);
					}
					else if (pred->dtype == AS_INDEX_NUMERIC) {
						p = as_query_write_range_integer(p, pred->value.integer, pred->value.integer);
					}
					break;
				case AS_PREDICATE_RANGE:
					p = as_query_write_range_integer(p, pred->value.integer_range.min, pred->value.integer_range.max);
					break;
			}
		}
		
		// Query bin names are specified as a field (Scan bin names are specified later as operations)
		// Write selected bin names.
		if (query->select.size > 0) {
			p = as_command_write_field_header(p, AS_FIELD_QUERY_BINS, bin_name_size);
			*p++ = query->select.size;
			
			for (uint16_t i = 0; i < query->select.size; i++) {
				// Write bin name, but do not transfer null byte.
				uint8_t* len_ptr = p++;
				uint8_t* name = (uint8_t*)query->select.entries[i];
				uint8_t* n = (uint8_t*)name;
				while (*n) {
					*p++ = *n++;
				}
				*len_ptr = (uint8_t)(n - name);
			}
		}
	}
	else {
		// Write scan options
		p = as_command_write_field_header(p, AS_FIELD_SCAN_OPTIONS, 2);
		*p++ = 0;
		*p++ = 100;
		// Priority and scan percent not available in query policy.  This should be added.
		//uint8_t priority = query->priority << 4;
		//
		//if (policy->fail_on_cluster_change) {
		//	priority |= 0x08;
		//}
		//*p++ = priority;
		//*p++ = query->percent;
	}
	
	// Write aggregation function
	if (query->apply.function[0]) {
		p = as_command_write_field_header(p, AS_FIELD_UDF_OP, 1);
		*p++ = 1;
		p = as_command_write_field_string(p, AS_FIELD_UDF_PACKAGE_NAME, query->apply.module);
		p = as_command_write_field_string(p, AS_FIELD_UDF_FUNCTION, query->apply.function);
		p = as_command_write_field_buffer(p, AS_FIELD_UDF_ARGLIST, &argbuffer);
	}
    as_buffer_destroy(&argbuffer);
	
	// Estimate size for selected bin names on scan (query bin names already handled).
	if (query->where.size == 0) {
		if (query->select.size > 0) {
			for (uint16_t i = 0; i < query->select.size; i++) {
				p = as_command_write_bin_name(p, query->select.entries[i]);
			}
		}
	}
	
	size = as_command_write_end(cmd, p);
	task->cmd = cmd;
	task->cmd_size = size;
	task->complete_q = cf_queue_create(sizeof(as_query_complete_task), true);

	// Run tasks in parallel.
	for (uint32_t i = 0; i < n_nodes; i++) {
		task->node = nodes->array[i];
		cf_queue_push(task->cluster->query_q, task);
	}

	// Wait for tasks to complete.
	as_status status = AEROSPIKE_OK;
	for (uint32_t i = 0; i < n_nodes; i++) {
		as_query_complete_task complete;
		cf_queue_pop(task->complete_q, &complete, CF_QUEUE_FOREVER);
		
		if (complete.result != AEROSPIKE_OK && status == AEROSPIKE_OK) {
			status = complete.result;
		}
	}
	
	// If user aborts query, command is considered successful.
	if (status == AEROSPIKE_ERR_CLIENT_ABORT) {
		status = AEROSPIKE_OK;
	}
	
    // If completely successful, make the callback that signals completion.
    if (status == AEROSPIKE_OK) {
    	task->callback(NULL, task->udata);
    }
	
	// Release temporary queue.
	cf_queue_destroy(task->complete_q);
	
	// Free command memory.
	as_command_free(cmd, size);
	
	return status;
}

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 *	Execute a query and call the callback function for each result item.
 *
 *	~~~~~~~~~~{.c}
 *	as_query query;
 *	as_query_init(&query, "test", "demo");
 *	as_query_select(&query, "bin1");
 *	as_query_where(&query, "bin2", as_integer_equals(100));
 *
 *	if ( aerospike_query_foreach(&as, &err, NULL, &query, callback, NULL) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *
 *	as_query_destroy(&query);
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param query		The query to execute against the cluster.
 *	@param callback		The callback function to call for each result value.
 *	@param udata		User-data to be passed to the callback.
 *
 *	@return AEROSPIKE_OK on success, otherwise an error.
 *
 *	@ingroup query_operations
 */
as_status aerospike_query_foreach(
	aerospike * as, as_error * err, const as_policy_query * policy, const as_query * query,
	aerospike_query_foreach_callback callback, void * udata) 
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.query;
	}
	
	as_cluster* cluster = as->cluster;
	as_nodes* nodes = as_nodes_reserve(cluster);
	uint32_t n_nodes = nodes->size;
	
	if (n_nodes == 0) {
		as_nodes_release(nodes);
		return as_error_set_message(err, AEROSPIKE_ERR_SERVER, "Query command failed because cluster is empty.");
	}

	// Reserve each node in cluster.
	for (uint32_t i = 0; i < n_nodes; i++) {
		as_node_reserve(nodes->array[i]);
	}

	// Intialize query thread pool.
	as_query_threads_init(cluster);

	as_status status = AEROSPIKE_OK;
	uint32_t error_mutex = 0;
	
	// Initialize task.
	as_query_task task;
	task.cluster = cluster;
	task.policy = policy;
	task.query = query;
	task.err = err;
	task.error_mutex = &error_mutex;
	task.task_id = cf_get_rand64() / 2;
	
	if (query->apply.function[0]) {
		// Query with aggregation.
        // Setup as_aerospike, so we can get log() function.
        as_aerospike as;
        as_aerospike_init(&as, NULL, &query_aerospike_hooks);
		
		task.stream_q = cf_queue_create(sizeof(void*), true);
		
        // Stream for results from each node
        as_stream queue_stream;
        as_stream_init(&queue_stream, task.stream_q, &queue_stream_hooks);
		
		task.callback = as_query_aggregate_callback;
		task.udata = &queue_stream;

		as_query_stream_callback source;
		source.udata = udata;
		source.callback = callback;
		
        // The callback stream provides the ability to write to a callback function
        // when as_stream_write is called.
        as_stream ostream;
		as_stream_init(&ostream, &source, &callback_stream_hooks);
		
		status = as_query_execute(&task, query, nodes, n_nodes);
		
		if (status == AEROSPIKE_OK) {
        	as_udf_context ctx = {
        		.as = &as,
        		.timer = NULL,
        		.memtracker = NULL
        	};
			
            // Apply the UDF to the result stream
            as_result res;
            as_result_init(&res);
			
            status = as_module_apply_stream(&mod_lua, &ctx, query->apply.module, query->apply.function, &queue_stream, query->apply.arglist, &ostream, &res);
			
			if (status) {
                char* rs = as_module_err_string(status);
				
                if (res.value) {
                    switch (as_val_type(res.value)) {
                        case AS_STRING: {
                            as_string* lua_s = as_string_fromval(res.value);
                            char* lua_err  = (char*)as_string_tostring(lua_s);
							status = as_error_update(err, AEROSPIKE_ERR_UDF, "%s : %s", rs, lua_err);
                            break;
						}
							
                        default:
							status = as_error_update(err, AEROSPIKE_ERR_UDF, "%s : Unknown stack as_val type", rs);
                            break;
                    }
				}
				else {
					status = as_error_set_message(err, AEROSPIKE_ERR_UDF, rs);
				}
				cf_free(rs);
			}
			as_result_destroy(&res);
		}
		
		// Empty stream queue.
        as_val* val = NULL;
        while (cf_queue_pop(task.stream_q, &val, CF_QUEUE_NOWAIT) == CF_QUEUE_OK) {
            as_val_destroy(val);
        }
        cf_queue_destroy(task.stream_q);
	}
	else {
		// Normal query without aggregation.
		task.callback = callback;
		task.udata = udata;
		task.stream_q = 0;
		status = as_query_execute(&task, query, nodes, n_nodes);
	}
	
	// Release each node in cluster.
	for (uint32_t i = 0; i < n_nodes; i++) {
		as_node_release(nodes->array[i]);
	}

	// Release nodes array.
	as_nodes_release(nodes);
	return status;
}
