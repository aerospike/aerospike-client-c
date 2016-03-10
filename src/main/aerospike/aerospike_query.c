/*
 * Copyright 2008-2016 Aerospike, Inc.
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
#include <aerospike/as_async.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_command.h>
#include <aerospike/as_error.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_module.h>
#include <aerospike/as_msgpack.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_query.h>
#include <aerospike/as_random.h>
#include <aerospike/as_serializer.h>
#include <aerospike/as_socket.h>
#include <aerospike/as_status.h>
#include <aerospike/as_stream.h>
#include <aerospike/as_thread_pool.h>
#include <aerospike/as_udf_context.h>
#include <aerospike/mod_lua.h>
#include <stdint.h>

#include "as_stap.h"

/******************************************************************************
 * TYPES
 *****************************************************************************/

#define QUERY_FOREGROUND 1
#define QUERY_BACKGROUND 2

typedef struct as_query_user_callback_s {
	aerospike_query_foreach_callback callback;
	void* udata;
} as_query_user_callback;

typedef struct as_query_task_s {
	as_node* node;
	
	as_cluster* cluster;
	const as_policy_write* write_policy;
	const as_query* query;
	aerospike_query_foreach_callback callback;
	void* udata;
	uint32_t* error_mutex;
	as_error* err;
	cf_queue* input_queue;
	cf_queue* complete_q;
	uint64_t task_id;
	
	uint8_t* cmd;
	size_t cmd_size;
	
	uint32_t timeout;
	bool deserialize;
} as_query_task;

typedef struct as_query_task_aggr_s {
	const as_query* query;
	as_stream* input_stream;
	as_query_user_callback* callback_data;
	uint32_t* error_mutex;
	as_error* err;
	cf_queue* complete_q;
} as_query_task_aggr;

typedef struct as_query_complete_task_s {
	as_node* node;
	uint64_t task_id;
	as_status result;
} as_query_complete_task;

typedef struct as_async_query_executor {
	as_event_executor executor;
	as_async_query_record_listener listener;
} as_async_query_executor;

typedef struct as_async_query_command {
	as_event_command command;
	uint8_t space[];
} as_async_query_command;

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

// This is a no-op. the queue and its contents are destroyed at end of aerospike_query_foreach().
static int
as_input_stream_destroy(as_stream *s)
{
	return 0;
}

static as_val*
as_input_stream_read(const as_stream* s)
{
	as_val* val = NULL;
	cf_queue_pop(as_stream_source(s), &val, CF_QUEUE_FOREVER);
	return val;
}

static as_stream_status
as_input_stream_write(const as_stream* s, as_val* val)
{
    if (cf_queue_push(as_stream_source(s), &val) != CF_QUEUE_OK) {
        as_log_error("Write to client side stream failed.");
        as_val_destroy(val);
        return AS_STREAM_ERR;
    }
    return AS_STREAM_OK;
}

static const as_stream_hooks input_stream_hooks = {
    .destroy  = as_input_stream_destroy,
    .read     = as_input_stream_read,
    .write    = as_input_stream_write
};

// This callback will populate an intermediate stream, to be used for the aggregation.
static bool
as_query_aggregate_callback(const as_val* v, void* udata)
{
	as_stream* input_stream = (as_stream*)udata;
	as_stream_status status = as_stream_write(input_stream, (as_val*)v);
    return status? false : true;
}

static int
as_output_stream_destroy(as_stream* s)
{
    return 0;
}

static as_stream_status
as_output_stream_write(const as_stream* s, as_val* val)
{
	as_query_user_callback* source = (as_query_user_callback*)as_stream_source(s);
	bool rv = source->callback(val, source->udata);
	as_val_destroy(val);
	return rv? AS_STREAM_OK : AS_STREAM_ERR;
}

static const as_stream_hooks output_stream_hooks = {
    .destroy  = as_output_stream_destroy,
    .read     = NULL,
    .write    = as_output_stream_write
};

static void
as_query_complete_async(as_event_executor* executor, as_error* err)
{
	((as_async_query_executor*)executor)->listener(err, 0, executor->udata, executor->event_loop);
}

static bool
as_query_parse_record_async(as_event_command* cmd, uint8_t** pp, as_msg* msg)
{
	as_record rec;
	as_record_inita(&rec, msg->n_ops);
	
	rec.gen = msg->generation;
	rec.ttl = cf_server_void_time_to_ttl(msg->record_ttl);
	
	uint8_t* p = *pp;
	p = as_command_parse_key(p, msg->n_fields, &rec.key);
	p = as_command_parse_bins(&rec, p, msg->n_ops, cmd->deserialize);
	*pp = p;
	
	as_event_executor* executor = cmd->udata;  // udata is overloaded to contain executor.
	bool rv = ((as_async_query_executor*)executor)->listener(0, &rec, executor->udata, executor->event_loop);
	as_record_destroy(&rec);
	return rv;
}

static bool
as_query_parse_records_async(as_event_command* cmd)
{
	as_event_executor* executor = cmd->udata;  // udata is overloaded to contain executor.
	uint8_t* p = cmd->buf;
	uint8_t* end = p + cmd->len;
	
	while (p < end) {
		as_msg* msg = (as_msg*)p;
		as_msg_swap_header_from_be(msg);
		
		if (msg->result_code) {
			as_error err;
			as_error_set_message(&err, msg->result_code, as_error_string(msg->result_code));
			as_event_response_error(cmd, &err);
			return true;
		}
		p += sizeof(as_msg);
		
		if (msg->info3 & AS_MSG_INFO3_LAST) {
			as_event_executor_complete(cmd);
			return true;
		}
		
		if (! executor->valid) {
			as_error err;
			as_error_set_message(&err, AEROSPIKE_ERR_CLIENT_ABORT, "");
			as_event_response_error(cmd, &err);
			return true;
		}

		if (! as_query_parse_record_async(cmd, &p, msg)) {
			executor->valid = false;
			as_error err;
			as_error_set_message(&err, AEROSPIKE_ERR_CLIENT_ABORT, "");
			as_event_response_error(cmd, &err);
			return true;
		}
	}
	return false;
}

static as_status
as_query_parse_record(uint8_t** pp, as_msg* msg, as_query_task* task, as_error* err)
{
	bool rv = true;
	
	if (task->input_queue) {
		AEROSPIKE_QUERY_AGGPARSE_STARTING(task->task_id, task->node->name);

		// Parse aggregate return values.
		as_val* val = 0;
		as_status status = as_command_parse_success_failure_bins(pp, err, msg, &val);
		
		AEROSPIKE_QUERY_AGGPARSE_FINISHED(task->task_id, task->node->name);

		if (status != AEROSPIKE_OK) {
			return status;
		}
		
		if (task->callback) {
			AEROSPIKE_QUERY_AGGCB_STARTING(task->task_id, task->node->name);
			rv = task->callback(val, task->udata);
			AEROSPIKE_QUERY_AGGCB_FINISHED(task->task_id, task->node->name);
		}
		else {
			as_val_destroy(val);
		}
	}
	else {
		AEROSPIKE_QUERY_RECPARSE_STARTING(task->task_id, task->node->name);

		// Parse normal record values.
		as_record rec;
		as_record_inita(&rec, msg->n_ops);
		
		rec.gen = msg->generation;
		rec.ttl = cf_server_void_time_to_ttl(msg->record_ttl);
		
		uint8_t* p = *pp;
		p = as_command_parse_key(p, msg->n_fields, &rec.key);

		AEROSPIKE_QUERY_RECPARSE_BINS(task->task_id, task->node->name);

		p = as_command_parse_bins(&rec, p, msg->n_ops, task->deserialize);
		*pp = p;
		
		AEROSPIKE_QUERY_RECPARSE_FINISHED(task->task_id, task->node->name);

		if (task->callback) {
			AEROSPIKE_QUERY_RECCB_STARTING(task->task_id, task->node->name);
			rv = task->callback((as_val*)&rec, task->udata);
			AEROSPIKE_QUERY_RECCB_FINISHED(task->task_id, task->node->name);
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
#if defined(USE_SYSTEMTAP)
    size_t nrecs = 0;
#endif

	AEROSPIKE_QUERY_PARSE_RECORDS_STARTING(task->task_id, task->node->name, size);

	while (p < end) {
		as_msg* msg = (as_msg*)p;
		as_msg_swap_header_from_be(msg);
		
		if (msg->result_code) {
			status = as_error_set_message(err, msg->result_code, as_error_string(msg->result_code));
			AEROSPIKE_QUERY_PARSE_RECORDS_FINISHED(task->task_id, task->node->name, nrecs, status);
			return status;
		}
		p += sizeof(as_msg);
		
		if (msg->info3 & AS_MSG_INFO3_LAST) {
			AEROSPIKE_QUERY_PARSE_RECORDS_FINISHED(task->task_id, task->node->name, nrecs, AEROSPIKE_NO_MORE_RECORDS);
			return AEROSPIKE_NO_MORE_RECORDS;
		}
		
		status = as_query_parse_record(&p, msg, task, err);
		
		if (status != AEROSPIKE_OK) {
			AEROSPIKE_QUERY_PARSE_RECORDS_FINISHED(task->task_id, task->node->name, nrecs, status);
			return status;
		}
		

#if defined(USE_SYSTEMTAP)
		++nrecs;
#endif
		if (ck_pr_load_32(task->error_mutex)) {
			err->code = AEROSPIKE_ERR_QUERY_ABORTED;
			AEROSPIKE_QUERY_PARSE_RECORDS_FINISHED(task->task_id, task->node->name, nrecs, err->code);
			return err->code;
		}
	}

	AEROSPIKE_QUERY_PARSE_RECORDS_FINISHED(task->task_id, task->node->name, nrecs, AEROSPIKE_OK);

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
	
	AEROSPIKE_QUERY_COMMAND_EXECUTE(task->task_id, task->node->name);

	as_error err;
	as_error_init(&err);
	as_status status = as_command_execute(task->cluster, &err, &cn, task->cmd, task->cmd_size, task->timeout, 0, as_query_parse, task);
		
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

static void
as_query_worker(void* data)
{
	as_query_task* task = (as_query_task*)data;
		
	as_query_complete_task complete_task;
	complete_task.node = task->node;
	complete_task.task_id = task->task_id;
	complete_task.result = as_query_command_execute(task);
		
	cf_queue_push(task->complete_q, &complete_task);
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
as_query_write_range_geojson(uint8_t* p, char* begin, char* end)
{
	// Write particle type.
	*p++ = AS_BYTES_GEOJSON;
	
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

static size_t
as_query_command_size(const as_query* query, uint16_t* fields, as_buffer* argbuffer,
	uint32_t* filter_sz, uint32_t* bin_name_sz
	)
{
	size_t size = AS_HEADER_SIZE;
	uint32_t filter_size = 0;
	uint32_t bin_name_size = 0;
	uint16_t n_fields = 0;
	
	// Estimate namespace size.
	if (query->ns) {
		size += as_command_string_field_size(query->ns);
		n_fields++;
	}
	
	// Estimate set size.  Do not send empty sets.
	if (query->set && *query->set) {
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
					if (pred->dtype == AS_INDEX_NUMERIC) {
                        filter_size += sizeof(int64_t) * 2;
                    }
					else if (pred->dtype == AS_INDEX_GEO2DSPHERE) {
						filter_size += strlen(pred->value.string) * 2;
					}
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
	as_buffer_init(argbuffer);
	
	if (query->apply.function[0]) {
		size += as_command_field_size(1);
		size += as_command_string_field_size(query->apply.module);
		size += as_command_string_field_size(query->apply.function);
		
		if (query->apply.arglist) {
			// If the query has a udf w/ arglist, then serialize it.
			as_serializer ser;
			as_msgpack_init(&ser);
            as_serializer_serialize(&ser, (as_val*)query->apply.arglist, argbuffer);
			as_serializer_destroy(&ser);
		}
		size += as_command_field_size(argbuffer->size);
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
	*fields = n_fields;
	*filter_sz = filter_size;
	*bin_name_sz = bin_name_size;
	return size;
}

static size_t
as_query_command_init(
	uint8_t* cmd, const as_query* query, uint8_t query_type, const as_policy_write* wp,
	uint64_t task_id, uint32_t timeout, uint16_t n_fields, uint32_t filter_size,
	uint32_t bin_name_size, as_buffer* argbuffer
	)
{
	// Write command buffer.
	uint16_t n_ops = (query->where.size == 0)? query->select.size : 0;
	uint8_t* p;
	
	if (wp) {
		p = as_command_write_header(cmd, AS_MSG_INFO1_READ, AS_MSG_INFO2_WRITE, wp->commit_level, 0, wp->exists, AS_POLICY_GEN_IGNORE, 0, 0, timeout, n_fields, n_ops);
	}
	else {
		p = as_command_write_header_read(cmd, AS_MSG_INFO1_READ, AS_POLICY_CONSISTENCY_LEVEL_ONE, timeout, n_fields, n_ops);
	}
	
	// Write namespace.
	if (query->ns) {
		p = as_command_write_field_string(p, AS_FIELD_NAMESPACE, query->ns);
	}
	
	// Write set.
	if (query->set && *query->set) {
		p = as_command_write_field_string(p, AS_FIELD_SETNAME, query->set);
	}
	
	// Write indextype.
	if (query->where.size == 1) {
		as_predicate* pred = &query->where.entries[0];
		p = as_command_write_field_header(p, AS_FIELD_INDEX_TYPE, 1);
		*p++ = pred->itype;
	}
	
	// Write taskId field
	p = as_command_write_field_uint64(p, AS_FIELD_TASK_ID, task_id);
	
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
					if (pred->dtype == AS_INDEX_NUMERIC) {
                        p = as_query_write_range_integer(p, pred->value.integer_range.min, pred->value.integer_range.max);
                    }
					else if (pred->dtype == AS_INDEX_GEO2DSPHERE) {
						p = as_query_write_range_geojson(p, pred->value.string, pred->value.string);
                    }
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
		*p++ = query_type;
		p = as_command_write_field_string(p, AS_FIELD_UDF_PACKAGE_NAME, query->apply.module);
		p = as_command_write_field_string(p, AS_FIELD_UDF_FUNCTION, query->apply.function);
		p = as_command_write_field_buffer(p, AS_FIELD_UDF_ARGLIST, argbuffer);
	}
    as_buffer_destroy(argbuffer);
	
	// Estimate size for selected bin names on scan (query bin names already handled).
	if (query->where.size == 0) {
		if (query->select.size > 0) {
			for (uint16_t i = 0; i < query->select.size; i++) {
				p = as_command_write_bin_name(p, query->select.entries[i]);
			}
		}
	}
	
	return as_command_write_end(cmd, p);
}

static as_status
as_query_execute(as_query_task* task, const as_query* query, as_nodes* nodes, uint32_t n_nodes, uint8_t query_type)
{
	// Build Command.  It's okay to share command across threads because query does not have retries.
	// If retries were allowed, the timeout field in the command would change on retry which
	// would conflict with other threads.
	as_buffer argbuffer;
	uint32_t filter_size = 0;
	uint32_t bin_name_size = 0;
	uint16_t n_fields = 0;
	
	size_t size = as_query_command_size(query, &n_fields, &argbuffer, &filter_size, &bin_name_size);
	uint8_t* cmd = as_command_init(size);
	size = as_query_command_init(cmd, query, query_type, task->write_policy, task->task_id,
								 task->timeout, n_fields, filter_size, bin_name_size, &argbuffer);
	
	task->cmd = cmd;
	task->cmd_size = size;
	task->complete_q = cf_queue_create(sizeof(as_query_complete_task), true);

	as_status status = AEROSPIKE_OK;
	uint32_t n_wait_nodes = n_nodes;

	// Run tasks in parallel.
	for (uint32_t i = 0; i < n_nodes; i++) {
		// Stack allocate task for each node.  It should be fine since the task
		// only needs to be valid within this function.
		as_query_task* task_node = alloca(sizeof(as_query_task));
		memcpy(task_node, task, sizeof(as_query_task));
		task_node->node = nodes->array[i];
		
		int rc = as_thread_pool_queue_task(&task->cluster->thread_pool, as_query_worker, task_node);
		
		if (rc) {
			// Thread could not be added. Abort entire query.
			if (ck_pr_fas_32(task->error_mutex, 1) == 0) {
				status = as_error_update(task->err, AEROSPIKE_ERR_CLIENT, "Failed to add query thread: %d", rc);
 			}
			
			// Reset node count to threads that were run.
			n_wait_nodes = i;
			break;
		}

		AEROSPIKE_QUERY_ENQUEUE_TASK(task->task_id, task_node->node->name);
	}

	// Wait for tasks to complete.
	for (uint32_t i = 0; i < n_wait_nodes; i++) {
		as_query_complete_task complete;
		cf_queue_pop(task->complete_q, &complete, CF_QUEUE_FOREVER);
		
		AEROSPIKE_QUERY_COMMAND_COMPLETE(task->task_id, complete.node->name);

		if (complete.result != AEROSPIKE_OK && status == AEROSPIKE_OK) {
			status = complete.result;
		}
	}
	
	// If user aborts query, command is considered successful.
	if (status == AEROSPIKE_ERR_CLIENT_ABORT) {
		status = AEROSPIKE_OK;
	}
	
	// Make the callback that signals completion.
	if (task->callback) {
		task->callback(NULL, task->udata);
	}
	
	// Release temporary queue.
	cf_queue_destroy(task->complete_q);
	
	// Free command memory.
	as_command_free(cmd, size);
	
	return status;
}

static void
as_query_aggregate(void* data)
{
	as_query_task_aggr* task = (as_query_task_aggr*)data;
	const as_query* query = task->query;
	
	// Setup as_aerospike, so we can get log() function.
	as_aerospike as;
	as_aerospike_init(&as, NULL, &query_aerospike_hooks);
	
	as_udf_context ctx = {
		.as = &as,
		.timer = NULL,
		.memtracker = NULL
	};

	// The callback stream provides the ability to write to a user callback function
	// when as_stream_write is called.
	as_stream output_stream;
	as_stream_init(&output_stream, task->callback_data, &output_stream_hooks);
	
	// Apply the UDF to the result stream
	as_result res;
	as_result_init(&res);
	
	as_status status = as_module_apply_stream(&mod_lua, &ctx, query->apply.module, query->apply.function, task->input_stream, query->apply.arglist, &output_stream, &res);
	
	if (status) {
		// Aggregation failed. Abort entire query.
		if (ck_pr_fas_32(task->error_mutex, 1) == 0) {
			char* rs = as_module_err_string(status);
			
			if (res.value) {
				switch (as_val_type(res.value)) {
					case AS_STRING: {
						as_string* lua_s = as_string_fromval(res.value);
						char* lua_err  = (char*)as_string_tostring(lua_s);
						status = as_error_update(task->err, AEROSPIKE_ERR_UDF, "%s : %s", rs, lua_err);
						break;
					}
						
					default:
						status = as_error_update(task->err, AEROSPIKE_ERR_UDF, "%s : Unknown stack as_val type", rs);
						break;
				}
			}
			else {
				status = as_error_set_message(task->err, AEROSPIKE_ERR_UDF, rs);
			}
			cf_free(rs);
		}
	}
	as_result_destroy(&res);
	cf_queue_push(task->complete_q, &status);
}

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_status
aerospike_query_foreach(
	aerospike* as, as_error* err, const as_policy_query* policy, const as_query* query,
	aerospike_query_foreach_callback callback, void* udata)
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
		return as_error_set_message(err, AEROSPIKE_ERR_SERVER, "Command failed because cluster is empty.");
	}

	// Reserve each node in cluster.
	for (uint32_t i = 0; i < n_nodes; i++) {
		as_node_reserve(nodes->array[i]);
	}

	as_status status = AEROSPIKE_OK;
	uint32_t error_mutex = 0;
	
	// Initialize task.
	as_query_task task = {
		.node = 0,
		.cluster = cluster,
		.write_policy = 0,
		.query = query,
		.callback = 0,
		.udata = 0,
		.error_mutex = &error_mutex,
		.err = err,
		.input_queue = 0,
		.complete_q = 0,
		.task_id = as_random_get_uint64(),
		.cmd = 0,
		.cmd_size = 0,
		.timeout = policy->timeout,
		.deserialize = policy->deserialize
	};
	
	AEROSPIKE_QUERY_FOREACH_STARTING(task.task_id);

	if (query->apply.function[0]) {
		// Query with aggregation.
		task.input_queue = cf_queue_create(sizeof(void*), true);
		
        // Stream for results from each node
        as_stream input_stream;
        as_stream_init(&input_stream, task.input_queue, &input_stream_hooks);
		
		task.callback = as_query_aggregate_callback;
		task.udata = &input_stream;
		
		as_query_user_callback callback_data;
		callback_data.callback = callback;
		callback_data.udata = udata;

		as_query_task_aggr task_aggr;
		task_aggr.query = query;
		task_aggr.input_stream = &input_stream;
		task_aggr.callback_data = &callback_data;
		task_aggr.error_mutex = &error_mutex;
		task_aggr.err = err;
		task_aggr.complete_q = cf_queue_create(sizeof(as_status), true);
		
		// Run lua aggregation in separate thread.
		int rc = as_thread_pool_queue_task(&cluster->thread_pool, as_query_aggregate, &task_aggr);
		
		if (rc == 0) {
			status = as_query_execute(&task, query, nodes, n_nodes, QUERY_FOREGROUND);
			
			// Wait for aggregation thread to finish.
			as_status complete_status = AEROSPIKE_OK;
			cf_queue_pop(task_aggr.complete_q, &complete_status, CF_QUEUE_FOREVER);
			
			if (complete_status != AEROSPIKE_OK && status == AEROSPIKE_OK) {
				status = complete_status;
			}
		}
		else {
			status = as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to add aggregate thread: %d", rc);
		}
		
		cf_queue_destroy(task_aggr.complete_q);
		
		// Empty input queue.
        as_val* val = NULL;
        while (cf_queue_pop(task.input_queue, &val, CF_QUEUE_NOWAIT) == CF_QUEUE_OK) {
            as_val_destroy(val);
        }
        cf_queue_destroy(task.input_queue);
	}
	else {
		// Normal query without aggregation.
		task.callback = callback;
		task.udata = udata;
		task.input_queue = 0;
		status = as_query_execute(&task, query, nodes, n_nodes, QUERY_FOREGROUND);
	}
	
	// Release each node in cluster.
	for (uint32_t i = 0; i < n_nodes; i++) {
		as_node_release(nodes->array[i]);
	}

	// Release nodes array.
	as_nodes_release(nodes);

	AEROSPIKE_QUERY_FOREACH_FINISHED(task.task_id);

	return status;
}

as_status
aerospike_query_async(
	aerospike* as, as_error* err, const as_policy_query* policy, const as_query* query,
	as_async_query_record_listener listener, void* udata, as_event_loop* event_loop)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.query;
	}
	
	if (query->apply.function[0]) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Async aggregate queries are not supported.");
	}
	
	uint64_t task_id = as_random_get_uint64();
	
	as_nodes* nodes = as_nodes_reserve(as->cluster);
	uint32_t n_nodes = nodes->size;
	
	if (n_nodes == 0) {
		as_nodes_release(nodes);
		return as_error_set_message(err, AEROSPIKE_ERR_SERVER, "Command failed because cluster is empty.");
	}
	
	// Reserve each node in cluster.
	for (uint32_t i = 0; i < n_nodes; i++) {
		as_node_reserve(nodes->array[i]);
	}
	
	// Query will be split up into a command for each node.
	// Allocate query data shared by each command.
	as_async_query_executor* executor = cf_malloc(sizeof(as_async_query_executor));
	as_event_executor* exec = &executor->executor;
	pthread_mutex_init(&exec->lock, NULL);
	exec->event_loop = as_event_assign(event_loop);
	exec->complete_fn = as_query_complete_async;
	exec->udata = udata;
	exec->max = n_nodes;
	exec->max_concurrent = n_nodes;
	exec->count = 0;
	exec->commands = 0;
	exec->valid = true;
	executor->listener = listener;

	as_buffer argbuffer;
	uint32_t filter_size = 0;
	uint32_t bin_name_size = 0;
	uint16_t n_fields = 0;
	
	size_t size = as_query_command_size(query, &n_fields, &argbuffer, &filter_size, &bin_name_size);
	uint8_t* cmd_buf = as_command_init(size);
	size = as_query_command_init(cmd_buf, query, QUERY_FOREGROUND, NULL, task_id, policy->timeout,
								 n_fields, filter_size, bin_name_size, &argbuffer);
	
	// Allocate enough memory to cover, then, round up memory size in 8KB increments to allow socket
	// read to reuse buffer.
	size_t s = (sizeof(as_async_query_command) + size + AS_AUTHENTICATION_MAX_SIZE + 8191) & ~8191;
	
	as_status status = AEROSPIKE_OK;

	// Create all query commands.
	for (uint32_t i = 0; i < n_nodes; i++) {
		as_event_command* cmd = cf_malloc(s);
		cmd->event_loop = exec->event_loop;
		cmd->conn = 0;
		cmd->cluster = as->cluster;
		cmd->node = nodes->array[i];
		cmd->udata = executor;  // Overload udata to be the executor.
		cmd->parse_results = as_query_parse_records_async;
		cmd->buf = ((as_async_query_command*)cmd)->space;
		cmd->capacity = (uint32_t)(s - sizeof(as_async_query_command));
		cmd->len = (uint32_t)size;
		cmd->pos = 0;
		cmd->auth_len = 0;
		cmd->timeout_ms = policy->timeout;
		cmd->type = AS_ASYNC_TYPE_QUERY;
		cmd->state = AS_ASYNC_STATE_UNREGISTERED;
		cmd->pipe_listener = NULL;
		cmd->deserialize = policy->deserialize;
		cmd->free_buf = false;
		memcpy(cmd->buf, cmd_buf, size);
		
		status = as_event_command_execute(cmd, err);
		
		if (status != AEROSPIKE_OK) {
			as_event_executor_cancel(exec, i);
			break;
		}
	}
	
	// Free command buffer.
	as_command_free(cmd_buf, size);
	
	as_nodes_release(nodes);
	return status;
}

as_status
aerospike_query_background(
	aerospike* as, as_error* err, const as_policy_write* policy,
	const as_query* query, uint64_t* query_id)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.write;
	}
	
	if (! query->apply.function[0]) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "Function is required.");
	}

	as_cluster* cluster = as->cluster;
	as_nodes* nodes = as_nodes_reserve(cluster);
	uint32_t n_nodes = nodes->size;
	
	if (n_nodes == 0) {
		as_nodes_release(nodes);
		return as_error_set_message(err, AEROSPIKE_ERR_SERVER, "Command failed because cluster is empty.");
	}
	
	// Reserve each node in cluster.
	for (uint32_t i = 0; i < n_nodes; i++) {
		as_node_reserve(nodes->array[i]);
	}
	
	// Set task id
	uint64_t task_id;
	if (query_id) {
		if (*query_id == 0) {
			*query_id = as_random_get_uint64();
		}
		task_id = *query_id;
	}
	else {
		task_id = as_random_get_uint64();
	}

	uint32_t error_mutex = 0;

	// Initialize task.
	as_query_task task = {
		.node = 0,
		.cluster = cluster,
		.write_policy = policy,
		.query = query,
		.callback = 0,
		.udata = 0,
		.error_mutex = &error_mutex,
		.err = err,
		.input_queue = 0,
		.complete_q = 0,
		.task_id = task_id,
		.cmd = 0,
		.cmd_size = 0,
		.timeout = policy->timeout,
		.deserialize = false
	};
	
	as_status status = as_query_execute(&task, query, nodes, n_nodes, QUERY_BACKGROUND);

	// Release each node in cluster.
	for (uint32_t i = 0; i < n_nodes; i++) {
		as_node_release(nodes->array[i]);
	}
	
	// Release nodes array.
	as_nodes_release(nodes);
	return status;
}
