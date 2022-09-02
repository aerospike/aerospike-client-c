/*
 * Copyright 2008-2022 Aerospike, Inc.
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
#include <aerospike/aerospike_scan.h>
#include <aerospike/as_aerospike.h>
#include <aerospike/as_async.h>
#include <aerospike/as_cdt_internal.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_command.h>
#include <aerospike/as_error.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_module.h>
#include <aerospike/as_msgpack.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_partition_tracker.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_query.h>
#include <aerospike/as_query_validate.h>
#include <aerospike/as_random.h>
#include <aerospike/as_serializer.h>
#include <aerospike/as_sleep.h>
#include <aerospike/as_socket.h>
#include <aerospike/as_status.h>
#include <aerospike/as_stream.h>
#include <aerospike/as_thread_pool.h>
#include <aerospike/as_udf_context.h>
#include <aerospike/mod_lua.h>

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
	as_node_partitions* np;

	as_partition_tracker* pt;
	as_cluster* cluster;
	const as_policy_query* query_policy;
	const as_policy_write* write_policy;
	const as_query* query;
	aerospike_query_foreach_callback callback;
	void* udata;
	as_error* err;
	uint32_t* error_mutex;
	cf_queue* input_queue;
	cf_queue* complete_q;
	uint64_t task_id;
	uint64_t cluster_key;

	uint8_t* cmd;
	size_t cmd_size;
	uint8_t query_type;
	bool first;
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
	as_cluster* cluster;
	as_partition_tracker* pt;
	uint8_t* cmd_buf;
	uint32_t cmd_size;
	uint32_t cmd_size_pre;
	uint32_t cmd_size_post;
	uint32_t task_id_offset;
	uint32_t info_timeout;
	uint16_t n_fields;
	bool deserialize;
	bool has_where;
} as_async_query_executor;

typedef struct as_async_query_command {
	as_event_command command;
	as_node_partitions* np;
	uint8_t space[];
} as_async_query_command;

typedef struct as_query_builder {
	as_partition_tracker* pt;
	as_node_partitions* np;
	as_buffer argbuffer;
	as_queue* opsbuffers;
	uint64_t max_records;
	size_t size;
	uint32_t filter_size;
	uint32_t task_id_offset;
	uint32_t parts_full_size;
	uint32_t parts_partial_digest_size;
	uint32_t parts_partial_bval_size;
	uint32_t bin_name_size;
	uint32_t cmd_size_pre;
	uint32_t cmd_size_post;
	uint16_t n_fields;
	uint16_t n_ops;
	bool is_new;
} as_query_builder;

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static int
as_query_aerospike_log(const as_aerospike* as, const char * file, const int line, const int level, const char * msg)
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
as_query_complete_async(as_event_executor* executor)
{
	// If query callback already returned false, do not re-notify user.
	if (executor->notify) {
		((as_async_query_executor*)executor)->listener(executor->err, 0, executor->udata,
			executor->event_loop);
	}
}

static as_status
as_query_partition_retry_async(as_async_query_executor* qe, as_error* err);

static inline void
as_query_partition_executor_destroy(as_async_query_executor* qe)
{
	as_partition_tracker_destroy(qe->pt);
	cf_free(qe->pt);
	cf_free(qe->cmd_buf);
}

static void
as_query_partition_notify(as_async_query_executor* qe, as_error* err)
{
	if (err) {
		as_partition_error(qe->pt->parts_all);
	}

	as_query_partition_executor_destroy(qe);

	// If query callback already returned false, do not re-notify user.
	if (qe->executor.notify) {
		qe->listener(err, NULL, qe->executor.udata, qe->executor.event_loop);
	}
}

static void
as_query_partition_complete_async(as_event_executor* ee)
{
	as_async_query_executor* qe = (as_async_query_executor*)ee;

	// Handle error.
	if (ee->err) {
		as_query_partition_notify(qe, ee->err);
		return;
	}

	// Check if all partitions received.
	as_error err;
	as_status status = as_partition_tracker_is_complete(qe->pt, qe->cluster, &err);

	if (status == AEROSPIKE_OK) {
		// Scan complete.
		as_query_partition_notify(qe, NULL);
		return;
	}

	// Stop on all errors except AEROSPIKE_ERR_CLIENT.
	if (status != AEROSPIKE_ERR_CLIENT) {
		as_query_partition_notify(qe, &err);
		return;
	}

	// Reassign incomplete partitions to nodes.
	status = as_partition_tracker_assign(qe->pt, qe->cluster, ee->ns, &err);

	if (status != AEROSPIKE_OK) {
		as_query_partition_notify(qe, &err);
		return;
	}

	// Retry.
	as_query_partition_retry_async(qe, &err);
}

static as_status
as_query_parse_record_async(
	as_async_query_executor* qe, as_async_query_command* qc, uint8_t** pp, as_msg* msg,
	as_error* err
	)
{
	as_record rec;
	as_record_inita(&rec, msg->n_ops);
	
	rec.gen = msg->generation;
	rec.ttl = cf_server_void_time_to_ttl(msg->record_ttl);

	uint64_t bval = 0;
	*pp = as_command_parse_key(*pp, msg->n_fields, &rec.key, &bval);

	as_status status = as_command_parse_bins(pp, err, &rec, msg->n_ops,
		qc->command.flags2 & AS_ASYNC_FLAGS2_DESERIALIZE);

	if (status != AEROSPIKE_OK) {
		as_record_destroy(&rec);
		return status;
	}

	bool rv = qe->listener(NULL, &rec, qe->executor.udata, qe->executor.event_loop);

	if (! rv) {
		as_record_destroy(&rec);
		qe->executor.notify = false;
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT_ABORT, "");
	}

	if (qc->np) {
		as_partition_tracker_set_last(qe->pt, qc->np, &rec.key.digest, bval,
			qc->command.cluster->n_partitions);
	}

	as_record_destroy(&rec);
	return AEROSPIKE_OK;
}

static bool
as_query_parse_records_async(as_event_command* cmd)
{
	as_error err;
	as_async_query_command* qc = (as_async_query_command*)cmd;
	as_async_query_executor* qe = cmd->udata;  // udata is overloaded to contain executor.
	uint8_t* p = cmd->buf + cmd->pos;
	uint8_t* end = cmd->buf + cmd->len;

	while (p < end) {
		as_msg* msg = (as_msg*)p;
		as_msg_swap_header_from_be(msg);
		p += sizeof(as_msg);

		if (msg->info3 & AS_MSG_INFO3_LAST) {
			if (msg->result_code != AEROSPIKE_OK) {
				// The server returned a fatal error.
				as_error_set_message(&err, msg->result_code, as_error_string(msg->result_code));
				as_event_response_error(cmd, &err);
				return true;
			}
			as_event_query_complete(cmd);
			return true;
		}

		if (qc->np) {
			if (msg->info3 & AS_MSG_INFO3_PARTITION_DONE) {
				// When an error code is received, mark partition as unavailable
				// for the current round. Unavailable partitions will be retried
				// in the next round. Generation is overloaded as partition id.
				if (msg->result_code != AEROSPIKE_OK) {
					as_partition_tracker_part_unavailable(qe->pt, qc->np, msg->generation);
				}
				continue;
			}
		}

		if (msg->result_code != AEROSPIKE_OK) {
			// Background scans return AEROSPIKE_ERR_RECORD_NOT_FOUND
			// when the set does not exist on the target node.
			if (msg->result_code == AEROSPIKE_ERR_RECORD_NOT_FOUND) {
				// Non-fatal error.
				as_event_query_complete(cmd);
				return true;
			}
			as_error_set_message(&err, msg->result_code, as_error_string(msg->result_code));
			as_event_response_error(cmd, &err);
			return true;
		}

		if (! qe->executor.valid) {
			as_error_set_message(&err, AEROSPIKE_ERR_CLIENT_ABORT, "");
			as_event_response_error(cmd, &err);
			return true;
		}

		if (as_query_parse_record_async(qe, qc, &p, msg, &err) != AEROSPIKE_OK) {
			as_event_response_error(cmd, &err);
			return true;
		}
	}
	return false;
}

static as_status
as_query_parse_record(uint8_t** pp, as_msg* msg, as_query_task* task, as_error* err)
{
	if (task->input_queue) {
		// Parse aggregate return values.
		as_val* val = 0;
		as_status status = as_command_parse_success_failure_bins(pp, err, msg, &val);
		
		if (status != AEROSPIKE_OK) {
			return status;
		}
		
		if (task->callback) {
			bool rv = task->callback(val, task->udata);

			if (! rv) {
				return AEROSPIKE_ERR_CLIENT_ABORT;
			}
		}
		else {
			as_val_destroy(val);
		}
	}
	else {
		// If a background query with operate command is sent to a server that does not support it
		// (server versions < 4.7), that server will return query results to the client instead of
		// running it in the background.  Return error when this happens.
		if (! task->query_policy) {
			return as_error_set_message(err, AEROSPIKE_ERR_CLIENT,
										"Server does not support background query with operations");
		}

		// Parse normal record values.
		as_record rec;
		as_record_inita(&rec, msg->n_ops);
		
		rec.gen = msg->generation;
		rec.ttl = cf_server_void_time_to_ttl(msg->record_ttl);

		uint64_t bval = 0;
		*pp = as_command_parse_key(*pp, msg->n_fields, &rec.key, &bval);

		as_status status = as_command_parse_bins(pp, err, &rec, msg->n_ops, task->query_policy->deserialize);

		if (status != AEROSPIKE_OK) {
			as_record_destroy(&rec);
			return status;
		}

		if (task->callback) {
			bool rv = task->callback((as_val*)&rec, task->udata);

			if (! rv) {
				as_record_destroy(&rec);
				return AEROSPIKE_ERR_CLIENT_ABORT;
			}
		}

		if (task->pt) {
			as_partition_tracker_set_last(task->pt, task->np, &rec.key.digest, bval,
				task->cluster->n_partitions);
		}
		as_record_destroy(&rec);
	}
	return AEROSPIKE_OK;
}

static as_status
as_query_parse_records(as_error* err, as_command* cmd, as_node* node, uint8_t* buf, size_t size)
{
	as_query_task* task = cmd->udata;
	uint8_t* p = buf;
	uint8_t* end = buf + size;
	as_status status;

	while (p < end) {
		as_msg* msg = (as_msg*)p;
		as_msg_swap_header_from_be(msg);
		p += sizeof(as_msg);

		if (msg->info3 & AS_MSG_INFO3_LAST) {
			if (msg->result_code != AEROSPIKE_OK) {
				// The server returned a fatal error.
				return as_error_set_message(err, msg->result_code, as_error_string(msg->result_code));
			}
			return AEROSPIKE_NO_MORE_RECORDS;
		}

		if (task->pt) {
			if (msg->info3 & AS_MSG_INFO3_PARTITION_DONE) {
				// When an error code is received, mark partition as unavailable
				// for the current round. Unavailable partitions will be retried
				// in the next round. Generation is overloaded as partition id.
				if (msg->result_code != AEROSPIKE_OK) {
					as_partition_tracker_part_unavailable(task->pt, task->np, msg->generation);
				}
				continue;
			}
		}

		if (msg->result_code != AEROSPIKE_OK) {
			// Background scans return AEROSPIKE_ERR_RECORD_NOT_FOUND
			// when the set does not exist on the target node.
			if (msg->result_code == AEROSPIKE_ERR_RECORD_NOT_FOUND) {
				// Non-fatal error.
				return AEROSPIKE_NO_MORE_RECORDS;
			}
			return as_error_set_message(err, msg->result_code, as_error_string(msg->result_code));
		}

		status = as_query_parse_record(&p, msg, task, err);
		
		if (status != AEROSPIKE_OK) {
			return status;
		}

		if (as_load_uint32(task->error_mutex)) {
			err->code = AEROSPIKE_ERR_QUERY_ABORTED;
			return err->code;
		}
	}
	return AEROSPIKE_OK;
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

static as_status
as_query_command_size(
	const as_policy_base* base_policy, const as_query* query, as_query_builder* qb, as_error* err
	)
{
	qb->size = AS_HEADER_SIZE;
	uint32_t filter_size = 0;
	uint16_t n_fields = 0;

	if (qb->np) {
		qb->parts_full_size = qb->np->parts_full.size * 2;
		qb->parts_partial_digest_size = qb->np->parts_partial.size * AS_DIGEST_VALUE_SIZE;
		qb->parts_partial_bval_size = (query->where.size == 0)?
			0 : qb->np->parts_partial.size * sizeof(uint64_t);
	}
	else {
		qb->parts_full_size = 0;
		qb->parts_partial_digest_size = 0;
		qb->parts_partial_bval_size = 0;
	}

	if (query->ns[0]) {
		qb->size += as_command_string_field_size(query->ns);
		n_fields++;
	}
	
	if (query->set[0]) {
		qb->size += as_command_string_field_size(query->set);
		n_fields++;
	}

	// RPS field used in new servers and not used (but harmless to add) in old servers.
	if (query->records_per_second > 0) {
		qb->size += as_command_field_size(sizeof(uint32_t));
		n_fields++;
	}

	// Socket timeout field used in new servers and not used (but harmless to add) in old servers.
	qb->size += as_command_field_size(sizeof(uint32_t));
	n_fields++;

	// Estimate taskId size.
	qb->size += as_command_field_size(8);
	n_fields++;

	// Estimate size of query filter.
	if (query->where.size > 0) {
		// Only one filter is allowed by the server.
		as_predicate* pred = &query->where.entries[0];

		// Estimate AS_FIELD_INDEX_TYPE
		if (pred->itype != AS_INDEX_TYPE_DEFAULT) {
			qb->size += as_command_field_size(1);
			n_fields++;
		}

		// Estimate AS_FIELD_INDEX_RANGE
		qb->size += AS_FIELD_HEADER_SIZE;
		filter_size++;  // Add byte for num filters.

		// bin name size(1) + particle type size(1) + begin particle size(4) + end particle size(4) = 10
		filter_size += (uint32_t)strlen(pred->bin) + 10;
		
		switch(pred->type) {
			case AS_PREDICATE_EQUAL:
				if (pred->dtype == AS_INDEX_STRING) {
					filter_size += (uint32_t)strlen(pred->value.string) * 2;
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
					filter_size += (uint32_t)strlen(pred->value.string) * 2;
				}
				break;
		}
		qb->size += filter_size;
		n_fields++;
		qb->filter_size = filter_size;

		if (! qb->is_new) {
			// Query bin names are specified as a field (Scan bin names are specified later as
			// operations) in old servers. Estimate size for selected bin names.
			qb->bin_name_size = 0;

			if (query->select.size > 0) {
				qb->size += AS_FIELD_HEADER_SIZE;
				qb->bin_name_size++;  // Add byte for num bin names.
				
				for (uint16_t i = 0; i < query->select.size; i++) {
					qb->bin_name_size += (uint32_t)strlen(query->select.entries[i]) + 1;
				}
				qb->size += qb->bin_name_size;
				n_fields++;
			}
		}

		if (pred->ctx) {
			qb->size += AS_FIELD_HEADER_SIZE + pred->ctx_size;
			n_fields++;
		}
	}

	// Estimate aggregation/background function size.
	as_buffer_init(&qb->argbuffer);

	if (query->apply.function[0]) {
		qb->size += as_command_field_size(1);
		qb->size += as_command_string_field_size(query->apply.module);
		qb->size += as_command_string_field_size(query->apply.function);

		if (query->apply.arglist) {
			// If the query has a udf w/ arglist, then serialize it.
			as_serializer ser;
			as_msgpack_init(&ser);
			as_serializer_serialize(&ser, (as_val*)query->apply.arglist, &qb->argbuffer);
			as_serializer_destroy(&ser);
		}
		qb->size += as_command_field_size(qb->argbuffer.size);
		n_fields += 4;
	}

	// Estimate filter expression size.
	if (base_policy->filter_exp) {
		qb->size += AS_FIELD_HEADER_SIZE + base_policy->filter_exp->packed_sz;
		n_fields++;
	}

	if (qb->parts_full_size > 0) {
		qb->size += as_command_field_size(qb->parts_full_size);
		n_fields++;
	}

	if (qb->parts_partial_digest_size > 0) {
		qb->size += as_command_field_size(qb->parts_partial_digest_size);
		n_fields++;
	}

	if (qb->parts_partial_bval_size > 0) {
		qb->size += as_command_field_size(qb->parts_partial_bval_size);
		n_fields++;
	}

	// Max records field used in new servers and not used (but harmless to add) in old servers.
	if (qb->max_records > 0) {
		qb->size += as_command_field_size(8);
		n_fields++;
	}

	qb->n_fields = n_fields;
	qb->n_ops = 0;

	// Operations (used in background query) and bin names (used in foreground query)
	// are mutually exclusive.
	if (query->ops) {
		// Estimate size for background operations.
		as_operations* ops = query->ops;

		for (uint16_t i = 0; i < ops->binops.size; i++) {
			as_binop* op = &ops->binops.entries[i];
			as_status status = as_command_bin_size(&op->bin, qb->opsbuffers, &qb->size, err);

			if (status != AEROSPIKE_OK) {
				return status;
			}
		}
		qb->n_ops = ops->binops.size;
	}
	else if (qb->is_new || query->where.size == 0) {
		// Estimate size for selected bin names (query bin names already handled for old servers).
		for (uint16_t i = 0; i < query->select.size; i++) {
			qb->size += as_command_string_operation_size(query->select.entries[i]);
		}
		qb->n_ops = query->select.size;
	}
	return AEROSPIKE_OK;
}

static size_t
as_query_command_init(
	uint8_t* cmd, const as_policy_base* base_policy, const as_policy_query* query_policy,
	const as_policy_write* write_policy, const as_query* query, uint8_t query_type,
	uint64_t task_id, as_query_builder* qb
	)
{
	// Write command buffer.
	uint8_t* p;
	
	if (query_policy) {
		// Foreground query.
		uint8_t read_attr = AS_MSG_INFO1_READ;

		if (query->no_bins) {
			read_attr |= AS_MSG_INFO1_GET_NOBINDATA;
		}

		if (query_policy->short_query) {
			read_attr |= AS_MSG_INFO1_SHORT_QUERY;
		}

		uint8_t info_attr = qb->is_new ? AS_MSG_INFO3_PARTITION_DONE : 0;

		p = as_command_write_header_read(cmd, base_policy, AS_POLICY_READ_MODE_AP_ONE,
			AS_POLICY_READ_MODE_SC_SESSION, base_policy->total_timeout, qb->n_fields, qb->n_ops,
			read_attr, info_attr);
	}
	else if (query->ops) {
		// Background query with operations.
		uint32_t ttl = (query->ttl)? query->ttl : query->ops->ttl;
		p = as_command_write_header_write(cmd, base_policy, write_policy->commit_level,
			write_policy->exists, AS_POLICY_GEN_IGNORE, 0, ttl, qb->n_fields, qb->n_ops,
			write_policy->durable_delete, 0, AS_MSG_INFO2_WRITE, 0);
	}
	else {
		// Background query with UDF.
		p = as_command_write_header_write(cmd, base_policy, write_policy->commit_level,
			write_policy->exists, AS_POLICY_GEN_IGNORE, 0, query->ttl, qb->n_fields, qb->n_ops,
			write_policy->durable_delete, 0, AS_MSG_INFO2_WRITE, 0);
	}

	// Write namespace.
	if (query->ns[0]) {
		p = as_command_write_field_string(p, AS_FIELD_NAMESPACE, query->ns);
	}
	
	// Write set.
	if (query->set[0]) {
		p = as_command_write_field_string(p, AS_FIELD_SETNAME, query->set);
	}
	
	if (query->records_per_second > 0) {
		p = as_command_write_field_uint32(p, AS_FIELD_RPS, query->records_per_second);
	}

	// Write socket timeout.
	p = as_command_write_field_uint32(p, AS_FIELD_SOCKET_TIMEOUT, base_policy->socket_timeout);

	// Write taskId field
	p = as_command_write_field_uint64(p, AS_FIELD_TASK_ID, task_id);

	qb->task_id_offset = ((uint32_t)(p - cmd)) - sizeof(uint64_t);

	// Write query filters.
	if (query->where.size > 0) {
		// Only one filter is allowed by the server.
		as_predicate* pred = &query->where.entries[0];

		// Write indextype.
		if (pred->itype != AS_INDEX_TYPE_DEFAULT) {
			p = as_command_write_field_header(p, AS_FIELD_INDEX_TYPE, 1);
			*p++ = pred->itype;
		}

		p = as_command_write_field_header(p, AS_FIELD_INDEX_RANGE, qb->filter_size);
		*p++ = (uint8_t)1;  // Only one filter is allowed by the server.

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

		if (! qb->is_new) {
			// Query bin names are specified as a field (Scan bin names are specified later as
			// operations) for old servers. Write selected bin names.
			if (query->select.size > 0) {
				p = as_command_write_field_header(p, AS_FIELD_QUERY_BINS, qb->bin_name_size);
				*p++ = (uint8_t)query->select.size;
				
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

		if (pred->ctx) {
			p = as_command_write_field_header(p, AS_FIELD_INDEX_CONTEXT, pred->ctx_size);

			as_packer pk = {.buffer = p, .capacity = pred->ctx_size};

			p += as_cdt_ctx_pack(pred->ctx, &pk);
		}
	}

	// Write aggregation/background function.
	if (query->apply.function[0]) {
		p = as_command_write_field_header(p, AS_FIELD_UDF_OP, 1);
		*p++ = query_type;
		p = as_command_write_field_string(p, AS_FIELD_UDF_PACKAGE_NAME, query->apply.module);
		p = as_command_write_field_string(p, AS_FIELD_UDF_FUNCTION, query->apply.function);
		p = as_command_write_field_buffer(p, AS_FIELD_UDF_ARGLIST, &qb->argbuffer);
	}
	as_buffer_destroy(&qb->argbuffer);

	// Write filter expression.
	if (base_policy->filter_exp) {
		p = as_exp_write(base_policy->filter_exp, p);
	}

	qb->cmd_size_pre = (uint32_t)(p - cmd);

	if (qb->parts_full_size > 0) {
		p = as_command_write_field_header(p, AS_FIELD_PID_ARRAY, qb->parts_full_size);

		as_vector* list = &qb->np->parts_full;

		for (uint32_t i = 0; i < list->size; i++) {
			uint16_t part_id = as_partition_tracker_get_id(list, i);
			*(uint16_t*)p = cf_swap_to_le16(part_id);
			p += sizeof(uint16_t);
		}
	}

	if (qb->parts_partial_digest_size > 0) {
		p = as_command_write_field_header(p, AS_FIELD_DIGEST_ARRAY, qb->parts_partial_digest_size);

		as_partition_tracker* pt = qb->pt;
		as_vector* list = &qb->np->parts_partial;

		for (uint32_t i = 0; i < list->size; i++) {
			as_partition_status* ps = as_partition_tracker_get_status(pt, list, i);
			memcpy(p, ps->digest.value, AS_DIGEST_VALUE_SIZE);
			p += AS_DIGEST_VALUE_SIZE;
		}
	}

	if (qb->parts_partial_bval_size > 0) {
		p = as_command_write_field_header(p, AS_FIELD_BVAL_ARRAY, qb->parts_partial_bval_size);

		as_partition_tracker* pt = qb->pt;
		as_vector* list = &qb->np->parts_partial;

		for (uint32_t i = 0; i < list->size; i++) {
			as_partition_status* ps = as_partition_tracker_get_status(pt, list, i);
			*(uint64_t*)p = cf_swap_to_le64(ps->bval);
			p += sizeof(uint64_t);
		}
	}

	if (qb->max_records > 0) {
		p = as_command_write_field_uint64(p, AS_FIELD_MAX_RECORDS, qb->max_records);
	}

	if (query->ops) {
		as_operations* ops = query->ops;

		for (uint16_t i = 0; i < ops->binops.size; i++) {
			as_binop* op = &ops->binops.entries[i];
			p = as_command_write_bin(p, op->op, &op->bin, qb->opsbuffers);
		}
		as_buffers_destroy(qb->opsbuffers);
	}
	else if (qb->is_new || query->where.size == 0) {
		for (uint16_t i = 0; i < query->select.size; i++) {
			p = as_command_write_bin_name(p, query->select.entries[i]);
		}
	}

	qb->cmd_size_post = ((uint32_t)(p - cmd)) - qb->cmd_size_pre;
	return as_command_write_end(cmd, p);
}

static as_status
as_query_command_execute_old(as_query_task* task)
{
	as_error err;
	as_error_init(&err);

	as_status status;

	if (task->cluster_key && ! task->first) {
		uint32_t timeout = task->query_policy? task->query_policy->info_timeout : 10000;
		status = as_query_validate(&err, task->node, task->query->ns, timeout, task->cluster_key);

		if (status) {
			// Set main error only once.
			if (as_fas_uint32(task->error_mutex, 1) == 0) {
				as_error_copy(task->err, &err);
			}
			return status;
		}
	}

	const as_policy_base* policy;
	uint8_t flags;

	if (task->query_policy) {
		policy = &task->query_policy->base;
		flags = AS_COMMAND_FLAGS_READ;
	}
	else {
		policy = &task->write_policy->base;
		flags = 0;
	}

	as_command cmd;
	cmd.cluster = task->cluster;
	cmd.policy = policy;
	cmd.node = task->node;
	cmd.ns = NULL;        // Not referenced when node set.
	cmd.partition = NULL; // Not referenced when node set.
	cmd.parse_results_fn = as_query_parse_records;
	cmd.udata = task;
	cmd.buf = task->cmd;
	cmd.buf_size = task->cmd_size;
	cmd.partition_id = 0; // Not referenced when node set.
	cmd.replica = AS_POLICY_REPLICA_MASTER;
	cmd.flags = flags;
	cmd.master = true;

	as_command_start_timer(&cmd);

	// Individual query node commands must not retry.
	cmd.max_retries = 0;

	status = as_command_execute(&cmd, &err);

	if (status) {
		// Set main error only once.
		if (as_fas_uint32(task->error_mutex, 1) == 0) {
			// Don't set error when user aborts query,
			if (status != AEROSPIKE_ERR_CLIENT_ABORT) {
				as_error_copy(task->err, &err);
			}
		}
		return status;
	}

	if (task->cluster_key) {
		uint32_t timeout = task->query_policy? task->query_policy->info_timeout : 10000;
		status = as_query_validate(&err, task->node, task->query->ns, timeout, task->cluster_key);

		if (status) {
			// Set main error only once.
			if (as_fas_uint32(task->error_mutex, 1) == 0) {
				as_error_copy(task->err, &err);
			}
			return status;
		}
	}
	return status;
}

static inline void
as_query_builder_init(
	as_query_builder* qb, as_cluster* cluster, as_queue* opsbuffers, as_partition_tracker* pt,
	as_node_partitions* np
	)
{
	qb->pt = pt;
	qb->np = np;
	qb->opsbuffers = opsbuffers;
	qb->max_records = (np)? np->record_max : 0;
	qb->is_new = cluster->has_partition_query;
}

static as_status
as_query_command_execute_new(as_query_task* task)
{
	as_error err;
	as_error_init(&err);

	as_queue opsbuffers;

	if (task->query->ops) {
		as_queue_inita(&opsbuffers, sizeof(as_buffer), task->query->ops->binops.size);
	}

	as_query_builder qb;
	as_query_builder_init(&qb, task->cluster, &opsbuffers, task->pt, task->np);

	const as_policy_base* base_policy = (task->query_policy)? &task->query_policy->base :
															  &task->write_policy->base;

	as_status status = as_query_command_size(base_policy, task->query, &qb, &err);

	if (status != AEROSPIKE_OK) {
		if (task->query->ops) {
			as_buffers_destroy(&opsbuffers);
		}

		// Set main error only once.
		if (as_fas_uint32(task->error_mutex, 1) == 0) {
			as_error_copy(task->err, &err);
		}
		return status;
	}

	uint8_t* buf = as_command_buffer_init(qb.size);
	size_t size = as_query_command_init(buf, base_policy, task->query_policy, task->write_policy,
		task->query, task->query_type, task->task_id, &qb);

	const as_policy_base* policy = &task->query_policy->base;
	uint8_t flags = AS_COMMAND_FLAGS_READ;

	as_command cmd;
	cmd.cluster = task->cluster;
	cmd.policy = policy;
	cmd.node = task->node;
	cmd.ns = NULL;        // Not referenced when node set.
	cmd.partition = NULL; // Not referenced when node set.
	cmd.parse_results_fn = as_query_parse_records;
	cmd.udata = task;
	cmd.buf = buf;
	cmd.buf_size = size;
	cmd.partition_id = 0; // Not referenced when node set.
	cmd.replica = AS_POLICY_REPLICA_MASTER;
	cmd.flags = flags;
	cmd.master = true;

	as_command_start_timer(&cmd);

	// Individual query node commands must not retry.
	cmd.max_retries = 0;

	status = as_command_execute(&cmd, &err);

	// Free command memory.
	as_command_buffer_free(buf, qb.size);

	if (status != AEROSPIKE_OK) {
		if (task->pt && as_partition_tracker_should_retry(task->pt, task->np, status)) {
			return AEROSPIKE_OK;
		}

		// Set main error only once.
		if (as_fas_uint32(task->error_mutex, 1) == 0) {
			// Don't set error when user aborts query,
			if (status != AEROSPIKE_ERR_CLIENT_ABORT) {
				as_error_copy(task->err, &err);
			}
		}
	}
	return status;
}

static void
as_query_worker_old(void* data)
{
	as_query_task* task = (as_query_task*)data;
		
	as_query_complete_task complete_task;
	complete_task.node = task->node;
	complete_task.task_id = task->task_id;

	if (as_load_uint32(task->error_mutex) == 0) {
		complete_task.result = as_query_command_execute_old(task);
	}
	else {
		complete_task.result = AEROSPIKE_ERR_QUERY_ABORTED;
	}

	cf_queue_push(task->complete_q, &complete_task);
}

static void
as_query_worker_new(void* data)
{
	as_query_task* task = (as_query_task*)data;
		
	as_query_complete_task complete_task;
	complete_task.node = task->node;
	complete_task.task_id = task->task_id;

	if (as_load_uint32(task->error_mutex) == 0) {
		complete_task.result = as_query_command_execute_new(task);
	}
	else {
		complete_task.result = AEROSPIKE_ERR_QUERY_ABORTED;
	}

	cf_queue_push(task->complete_q, &complete_task);
}

static as_status
as_query_execute(as_query_task* task, const as_query* query, as_nodes* nodes)
{
	as_status status = AEROSPIKE_OK;

	if (task->query_policy && task->query_policy->fail_on_cluster_change) {
		status = as_query_validate_begin(task->err, nodes->array[0], query->ns,
										 task->query_policy->info_timeout, &task->cluster_key);

		if (status) {
			return status;
		}
	}

	as_queue opsbuffers;

	if (query->ops) {
		as_queue_inita(&opsbuffers, sizeof(as_buffer), query->ops->binops.size);
	}

	as_query_builder qb;
	as_query_builder_init(&qb, task->cluster, &opsbuffers, NULL, NULL);

	const as_policy_base* base_policy = (task->query_policy)? &task->query_policy->base :
															  &task->write_policy->base;

	// Build Command. It's okay to share command across threads because old query protocol does
	// not have retries. If retries were allowed, the timeout field in the command would change on
	// retry which would conflict with other threads.
	status = as_query_command_size(base_policy, task->query, &qb, task->err);

	if (status != AEROSPIKE_OK) {
		if (query->ops) {
			as_buffers_destroy(&opsbuffers);
		}
		return status;
	}

	uint8_t* cmd = as_command_buffer_init(qb.size);
	size_t size = as_query_command_init(cmd, base_policy, task->query_policy, task->write_policy,
		task->query, task->query_type, task->task_id, &qb);

	task->cmd = cmd;
	task->cmd_size = size;
	task->complete_q = cf_queue_create(sizeof(as_query_complete_task), true);

	uint32_t n_wait_nodes = nodes->size;
	uint32_t thread_pool_size = task->cluster->thread_pool.thread_size;

	// Run tasks in parallel.
	for (uint32_t i = 0; i < nodes->size; i++) {
		// Stack allocate task for each node.  It should be fine since the task
		// only needs to be valid within this function.
		as_query_task* task_node = alloca(sizeof(as_query_task));
		memcpy(task_node, task, sizeof(as_query_task));
		task_node->node = nodes->array[i];
		
		// If the thread pool size is > 0 farm out the tasks to the pool, otherwise run in current thread.
		if (thread_pool_size > 0) {
			int rc = as_thread_pool_queue_task(&task->cluster->thread_pool, as_query_worker_old, task_node);
			
			if (rc) {
				// Thread could not be added. Abort entire query.
				if (as_fas_uint32(task->error_mutex, 1) == 0) {
					status = as_error_update(task->err, AEROSPIKE_ERR_CLIENT, "Failed to add query thread: %d", rc);
	 			}
				
				// Reset node count to threads that were run.
				n_wait_nodes = i;
				break;
			}
		} else {
			if ((status = as_query_command_execute_old(task_node)) != AEROSPIKE_OK) {
				break;
			}
		}
		task->first = false;
	}

	// Wait for tasks to complete.
	if (thread_pool_size > 0) {
		for (uint32_t i = 0; i < n_wait_nodes; i++) {
			as_query_complete_task complete;
			cf_queue_pop(task->complete_q, &complete, CF_QUEUE_FOREVER);
			
			if (complete.result != AEROSPIKE_OK && status == AEROSPIKE_OK) {
				status = complete.result;
			}
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
	as_command_buffer_free(cmd, qb.size);
	
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
		if (as_fas_uint32(task->error_mutex, 1) == 0) {
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

static as_status
as_query_partitions(
	as_cluster* cluster, as_error* err, const as_policy_query* policy, const as_query* query,
	as_partition_tracker* pt, aerospike_query_foreach_callback callback, void* udata)
{
	as_status status;

	while (true) {
		uint64_t task_id = as_random_get_uint64();
		status = as_partition_tracker_assign(pt, cluster, query->ns, err);

		if (status != AEROSPIKE_OK) {
			return status;
		}

		uint32_t n_nodes = pt->node_parts.size;

		// Initialize task.
		uint32_t error_mutex = 0;

		as_query_task task = {
			.node = NULL,
			.np = NULL,
			.pt = pt,
			.cluster = cluster,
			.query_policy = policy,
			.write_policy = NULL,
			.query = query,
			.callback = callback,
			.udata = udata,
			.err = err,
			.error_mutex = &error_mutex,
			.input_queue = NULL,
			.complete_q = NULL,
			.task_id = task_id,
			.cluster_key = 0,
			.cmd = NULL,
			.cmd_size = 0,
			.query_type = QUERY_FOREGROUND,
			.first = true
		};

		if (n_nodes > 1) {
			uint32_t n_wait_nodes = n_nodes;
			task.complete_q = cf_queue_create(sizeof(as_query_complete_task), true);

			// Run node queries in parallel.
			for (uint32_t i = 0; i < n_nodes; i++) {
				// Stack allocate task for each node.  It should be fine since the task
				// only needs to be valid within this function.
				as_query_task* task_node = alloca(sizeof(as_query_task));
				memcpy(task_node, &task, sizeof(as_query_task));

				task_node->np = as_vector_get(&pt->node_parts, i);
				task_node->node = task_node->np->node;

				int rc = as_thread_pool_queue_task(&cluster->thread_pool, as_query_worker_new, task_node);
				
				if (rc) {
					// Thread could not be added. Abort entire query.
					if (as_fas_uint32(task.error_mutex, 1) == 0) {
						status = as_error_update(task.err, AEROSPIKE_ERR_CLIENT, "Failed to add query thread: %d", rc);
					}
					
					// Reset node count to threads that were run.
					n_wait_nodes = i;
					break;
				}
			}

			// Wait for tasks to complete.
			for (uint32_t i = 0; i < n_wait_nodes; i++) {
				as_query_complete_task complete;
				cf_queue_pop(task.complete_q, &complete, CF_QUEUE_FOREVER);
				
				if (complete.result != AEROSPIKE_OK && status == AEROSPIKE_OK) {
					status = complete.result;
				}
			}
			
			// Release temporary queue.
			cf_queue_destroy(task.complete_q);
		}
		else {
			task.complete_q = 0;
			
			// Run node queries in series.
			for (uint32_t i = 0; i < n_nodes && status == AEROSPIKE_OK; i++) {
				task.np = as_vector_get(&pt->node_parts, i);
				task.node = task.np->node;
				status = as_query_command_execute_new(&task);
			}
		}

		// If user aborts query, command is considered successful.
		if (status == AEROSPIKE_ERR_CLIENT_ABORT) {
			status = AEROSPIKE_OK;
			break;
		}

		if (status != AEROSPIKE_OK) {
			return status;
		}

		status = as_partition_tracker_is_complete(pt, cluster, err);

		// Stop on ok and all errors except AEROSPIKE_ERR_CLIENT.
		if (status != AEROSPIKE_ERR_CLIENT) {
			break;
		}

		if (pt->sleep_between_retries > 0) {
			// Sleep before trying again.
			as_sleep(pt->sleep_between_retries);
		}
	}

	if (status == AEROSPIKE_OK) {
		callback(NULL, udata);
	}
	return status;
}

static as_status
as_query_partition_execute_async(
	as_async_query_executor* qe, as_partition_tracker* pt, as_error* err
	)
{
	as_event_executor* ee = &qe->executor;
	uint32_t n_nodes = pt->node_parts.size;

	for (uint32_t i = 0; i < n_nodes; i++) {
		as_node_partitions* np = as_vector_get(&pt->node_parts, i);

		uint32_t parts_full_size = np->parts_full.size * 2;
		uint32_t parts_partial_digest_size = np->parts_partial.size * AS_DIGEST_VALUE_SIZE;
		uint32_t parts_partial_bval_size = qe->has_where ?
			np->parts_partial.size * sizeof(uint64_t) : 0;

		size_t size = qe->cmd_size;
		uint16_t n_fields = qe->n_fields;

		if (parts_full_size > 0) {
			size += parts_full_size + AS_FIELD_HEADER_SIZE;
			n_fields++;
		}

		if (parts_partial_digest_size > 0) {
			size += parts_partial_digest_size + AS_FIELD_HEADER_SIZE;
			n_fields++;
		}

		if (parts_partial_bval_size > 0) {
			size += parts_partial_bval_size + AS_FIELD_HEADER_SIZE;
			n_fields++;
		}

		if (np->record_max > 0) {
			size += as_command_field_size(8);
			n_fields++;
		}

		// Allocate enough memory to cover, then, round up memory size in 8KB increments to reduce
		// fragmentation and to allow socket read to reuse buffer.
		size_t s = (sizeof(as_async_query_command) + size + AS_AUTHENTICATION_MAX_SIZE + 8191) & ~8191;
		as_async_query_command* qcmd = cf_malloc(s);
		qcmd->np = np;

		as_event_command* cmd = (as_event_command*)qcmd;
		cmd->buf = qcmd->space;

		uint8_t* p = cmd->buf;

		// Copy first part of generic command.
		memcpy(p, qe->cmd_buf, qe->cmd_size_pre);

		// Update n_fields in header.
		*(uint16_t*)&p[26] = cf_swap_to_be16(n_fields);
		p += qe->cmd_size_pre;

		// Write node specific partitions.
		if (parts_full_size > 0) {
			p = as_command_write_field_header(p, AS_FIELD_PID_ARRAY, parts_full_size);

			as_vector* list = &np->parts_full;

			for (uint32_t i = 0; i < list->size; i++) {
				uint16_t part_id = as_partition_tracker_get_id(list, i);
				*(uint16_t*)p = cf_swap_to_le16(part_id);
				p += sizeof(uint16_t);
			}
		}

		// Write node specific digests.
		if (parts_partial_digest_size > 0) {
			p = as_command_write_field_header(p, AS_FIELD_DIGEST_ARRAY, parts_partial_digest_size);

			as_vector* list = &np->parts_partial;

			for (uint32_t i = 0; i < list->size; i++) {
				as_partition_status* ps = as_partition_tracker_get_status(pt, list, i);
				memcpy(p, ps->digest.value, AS_DIGEST_VALUE_SIZE);
				p += AS_DIGEST_VALUE_SIZE;
			}
		}

		// Write node specific bvals.
		if (parts_partial_bval_size > 0) {
			p = as_command_write_field_header(p, AS_FIELD_BVAL_ARRAY, parts_partial_bval_size);

			as_vector* list = &np->parts_partial;

			for (uint32_t i = 0; i < list->size; i++) {
				as_partition_status* ps = as_partition_tracker_get_status(pt, list, i);
				*(uint64_t*)p = cf_swap_to_le64(ps->bval);
				p += sizeof(uint64_t);
			}
		}

		// Write record limit.
		if (np->record_max > 0) {
			p = as_command_write_field_uint64(p, AS_FIELD_MAX_RECORDS, np->record_max);
		}

		memcpy(p, qe->cmd_buf + qe->cmd_size_pre, qe->cmd_size_post);
		p += qe->cmd_size_post;
		size = as_command_write_end(cmd->buf, p);

		cmd->total_deadline = pt->total_timeout;
		cmd->socket_timeout = pt->socket_timeout;
		cmd->max_retries = 0;
		cmd->iteration = 0;
		cmd->replica = AS_POLICY_REPLICA_MASTER;
		cmd->event_loop = ee->event_loop;
		cmd->cluster = qe->cluster;
		cmd->node = np->node;
		// Reserve node because as_event_command_free() will release node
		// on command completion.
		as_node_reserve(cmd->node);
		cmd->ns = NULL;
		cmd->partition = NULL;
		cmd->udata = qe;  // Overload udata to be the executor.
		cmd->parse_results = as_query_parse_records_async;
		cmd->pipe_listener = NULL;
		cmd->write_len = (uint32_t)size;
		cmd->read_capacity = (uint32_t)(s - size - sizeof(as_async_query_command));
		cmd->type = AS_ASYNC_TYPE_QUERY_PARTITION;
		cmd->proto_type = AS_MESSAGE_TYPE;
		cmd->state = AS_ASYNC_STATE_UNREGISTERED;
		cmd->flags = AS_ASYNC_FLAGS_MASTER;
		cmd->flags2 = qe->deserialize ? AS_ASYNC_FLAGS2_DESERIALIZE : 0;
		ee->commands[i] = cmd;
	}

	// Run commands.
	uint32_t max = ee->max_concurrent;

	for (uint32_t i = 0; i < max; i++) {
		ee->queued++;

		as_event_command* cmd = ee->commands[i];
		as_status status = as_event_command_execute(cmd, err);

		if (status != AEROSPIKE_OK) {
			// as_event_executor_destroy() will release nodes that were not queued.
			// as_event_executor_cancel() or as_event_executor_error() will eventually
			// call as_event_executor_destroy().
			if (pt->iteration == 1) {
				// On first iteration, cleanup and do not call listener.
				as_query_partition_executor_destroy(qe);
				as_event_executor_cancel(ee, i);
			}
			else {
				// On retry, caller will cleanup and call listener.
				as_event_executor_error(ee, err, n_nodes - i);
			}
			return status;
		}
	}
	return AEROSPIKE_OK;
}

static as_status
as_query_partition_async(
	as_cluster* cluster, as_error* err, const as_policy_query* policy, const as_query* query,
	as_partition_tracker* pt, as_async_scan_listener listener, void* udata,
	as_event_loop* event_loop
	)
{
	pt->sleep_between_retries = 0;
	as_status status = as_partition_tracker_assign(pt, cluster, query->ns, err);

	if (status != AEROSPIKE_OK) {
		as_partition_tracker_destroy(pt);
		cf_free(pt);
		return status;
	}

	as_queue opsbuffers;

	if (query->ops) {
		as_queue_inita(&opsbuffers, sizeof(as_buffer), query->ops->binops.size);
	}

	uint64_t task_id = as_random_get_uint64();

	// Create command builder without partition fields.
	// The partition fields will be added later.
	as_query_builder qb;
	as_query_builder_init(&qb, cluster, &opsbuffers, NULL, NULL);

	status = as_query_command_size(&policy->base, query, &qb, err);

	if (status != AEROSPIKE_OK) {
		if (query->ops) {
			as_buffers_destroy(&opsbuffers);
		}
		as_partition_tracker_destroy(pt);
		cf_free(pt);
		return status;
	}

	uint8_t* cmd_buf = cf_malloc(qb.size);
	size_t cmd_size = as_query_command_init(cmd_buf, &policy->base, policy, NULL, query,
		QUERY_FOREGROUND, task_id, &qb);

	as_async_query_executor* qe = cf_malloc(sizeof(as_async_query_executor));
	qe->listener = listener;
	qe->cluster = cluster;
	qe->pt = pt;
	qe->cmd_buf = cmd_buf;
	qe->cmd_size = (uint32_t)cmd_size;
	qe->cmd_size_pre = qb.cmd_size_pre;
	qe->cmd_size_post = qb.cmd_size_post;
	qe->task_id_offset = qb.task_id_offset;
	qe->info_timeout = policy->info_timeout;
	qe->n_fields = qb.n_fields;
	qe->deserialize = policy->deserialize;
	qe->has_where = query->where.size > 0;

	uint32_t n_nodes = pt->node_parts.size;

	as_event_executor* ee = &qe->executor;
	pthread_mutex_init(&ee->lock, NULL);
	ee->max = n_nodes;
	ee->max_concurrent = n_nodes;
	ee->commands = cf_malloc(sizeof(as_event_command*) * n_nodes);
	ee->event_loop = as_event_assign(event_loop);
	ee->complete_fn = as_query_partition_complete_async;
	ee->udata = udata;
	ee->err = NULL;
	ee->ns = cf_strdup(query->ns);
	ee->cluster_key = 0;
	ee->count = 0;
	ee->queued = 0;
	ee->notify = true;
	ee->valid = true;

	return as_query_partition_execute_async(qe, pt, err);
}

static as_status
as_query_partition_retry_async(as_async_query_executor* qe_old, as_error* err)
{
	as_async_query_executor* qe = cf_malloc(sizeof(as_async_query_executor));
	qe->listener = qe_old->listener;
	qe->cluster = qe_old->cluster;
	qe->pt = qe_old->pt;
	qe->cmd_buf = qe_old->cmd_buf;
	qe->cmd_size = qe_old->cmd_size;
	qe->cmd_size_pre = qe_old->cmd_size_pre;
	qe->cmd_size_post = qe_old->cmd_size_post;
	qe->task_id_offset = qe_old->task_id_offset;
	qe->info_timeout = qe_old->info_timeout;
	qe->n_fields = qe_old->n_fields;
	qe->deserialize = qe_old->deserialize;
	qe->has_where = qe_old->has_where;

	// Must change task_id each round. Otherwise, server rejects command.
	uint64_t task_id = as_random_get_uint64();
	*(uint64_t*)(qe->cmd_buf + qe->task_id_offset) = task_id;

	uint32_t n_nodes = qe->pt->node_parts.size;

	as_event_executor* ee_old = &qe_old->executor;
	as_event_executor* ee = &qe->executor;
	pthread_mutex_init(&ee->lock, NULL);
	ee->max = n_nodes;
	ee->max_concurrent = n_nodes;
	ee->commands = cf_malloc(sizeof(as_event_command*) * n_nodes);
	ee->event_loop = ee_old->event_loop;
	ee->complete_fn = ee_old->complete_fn;
	ee->udata = ee_old->udata;
	ee->err = NULL;
	ee->ns = ee_old->ns;
	ee_old->ns = NULL;
	ee->cluster_key = 0;
	ee->count = 0;
	ee->queued = 0;
	ee->notify = true;
	ee->valid = true;

	return as_query_partition_execute_async(qe, qe->pt, err);
}

static void
convert_query_to_scan(
	const as_policy_query* query_policy, const as_query* query, as_policy_scan* scan_policy,
	as_scan* scan
	)
{
	as_policy_scan_init(scan_policy);
	memcpy(&scan_policy->base, &query_policy->base, sizeof(as_policy_base));
	scan_policy->max_records = query->max_records;
	scan_policy->records_per_second = query->records_per_second;

	as_scan_init(scan, query->ns, query->set);
	scan->select.entries = query->select.entries;
	scan->select.capacity = query->select.capacity;
	scan->select.size = query->select.size;
	scan->select._free = query->select._free;

	strcpy(scan->apply_each.module, query->apply.module);
	strcpy(scan->apply_each.function, query->apply.function);
	scan->apply_each.arglist = query->apply.arglist;
	scan->apply_each._free = query->apply._free;

	scan->ops = query->ops;
	scan->paginate = query->paginate;
	scan->no_bins = query->no_bins;
	scan->concurrent = true;
	scan->deserialize_list_map = query_policy->deserialize;
	scan->_free = query->_free;
}

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

bool
as_async_query_should_retry(as_event_command* cmd, as_status status)
{
	as_async_query_command* qc = (as_async_query_command*)cmd;
	as_async_query_executor* qe = cmd->udata;
	return as_partition_tracker_should_retry(qe->pt, qc->np, status);
}

as_status
aerospike_query_foreach(
	aerospike* as, as_error* err, const as_policy_query* policy, as_query* query,
	aerospike_query_foreach_callback callback, void* udata)
{
	if (query->ops) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
			"Use aerospike_query_background() for background queries");
	}

	as_error_reset(err);

	if (! policy) {
		policy = &as->config.policies.query;
	}

	as_cluster* cluster = as->cluster;
	as_status status;

	if (cluster->has_partition_query && ! query->apply.function[0]) {
		// Partition query.
		uint32_t n_nodes;
		status = as_cluster_validate_size(cluster, err, &n_nodes);

		if (status != AEROSPIKE_OK) {
			return status;
		}

		as_partition_tracker pt;
		as_partition_tracker_init_nodes(&pt, cluster, &policy->base, query->max_records,
			&query->parts_all, query->paginate, n_nodes);

		status = as_query_partitions(cluster, err, policy, query, &pt, callback, udata);

		if (status != AEROSPIKE_OK) {
			as_partition_error(query->parts_all);
		}
		as_partition_tracker_destroy(&pt);
		return status;
	}

	// Aggregation query and old foreground query.
	// Convert to a scan when filter doesn't exist and not aggregation query.
	if (query->where.size == 0 && ! query->apply.function[0]) {
		as_policy_scan scan_policy;
		as_scan scan;
		convert_query_to_scan(policy, query, &scan_policy, &scan);

		return aerospike_scan_foreach(as, err, &scan_policy, &scan, callback, udata);
	}

	as_nodes* nodes;
	status = as_cluster_reserve_all_nodes(cluster, err, &nodes);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	uint32_t error_mutex = 0;
	uint64_t task_id = as_random_get_uint64();
		
	// Initialize task.
	as_query_task task = {
		.node = NULL,
		.np = NULL,
		.pt = NULL,
		.cluster = cluster,
		.query_policy = policy,
		.write_policy = NULL,
		.query = query,
		.callback = NULL,
		.udata = NULL,
		.err = err,
		.error_mutex = &error_mutex,
		.input_queue = NULL,
		.complete_q = NULL,
		.task_id = task_id,
		.cluster_key = 0,
		.cmd = NULL,
		.cmd_size = 0,
		.query_type = QUERY_FOREGROUND,
		.first = true
	};
		
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
			status = as_query_execute(&task, query, nodes);
			
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
		status = as_query_execute(&task, query, nodes);
	}

	as_cluster_release_all_nodes(nodes);
	return status;
}

as_status
aerospike_query_partitions(
	aerospike* as, as_error* err, const as_policy_query* policy, as_query* query,
	as_partition_filter* pf, aerospike_query_foreach_callback callback, void* udata
	)
{
	if (query->apply.function[0] || query->ops) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
			"Aggregation or background queries cannot query by partition");
	}

	as_cluster* cluster = as->cluster;

	if (! cluster->has_partition_query) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
			"Partition query not supported by connected server");
	}

	as_error_reset(err);

	if (! policy) {
		policy = &as->config.policies.query;
	}

	uint32_t n_nodes;
	as_status status = as_cluster_validate_size(cluster, err, &n_nodes);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	if (pf->parts_all && ! query->parts_all) {
		as_query_set_partitions(query, pf->parts_all);
	}

	as_partition_tracker pt;
	status = as_partition_tracker_init_filter(&pt, cluster, &policy->base, query->max_records,
		&query->parts_all, query->paginate, n_nodes, pf, err);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = as_query_partitions(cluster, err, policy, query, &pt, callback, udata);

	if (status != AEROSPIKE_OK) {
		as_partition_error(query->parts_all);
	}
	as_partition_tracker_destroy(&pt);
	return status;
}

as_status
aerospike_query_async(
	aerospike* as, as_error* err, const as_policy_query* policy, as_query* query,
	as_async_query_record_listener listener, void* udata, as_event_loop* event_loop)
{
	if (query->apply.function[0] || query->ops) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT,
			"Async aggregation or background queries are not supported");
	}

	as_error_reset(err);

	if (! policy) {
		policy = &as->config.policies.query;
	}
	
	as_cluster* cluster = as->cluster;
	as_status status;

	if (cluster->has_partition_query) {
		// Partition query.
		uint32_t n_nodes;
		status = as_cluster_validate_size(cluster, err, &n_nodes);

		if (status != AEROSPIKE_OK) {
			return status;
		}

		as_partition_tracker* pt = cf_malloc(sizeof(as_partition_tracker));
		as_partition_tracker_init_nodes(pt, cluster, &policy->base, query->max_records,
			&query->parts_all, query->paginate, n_nodes);
		return as_query_partition_async(cluster, err, policy, query, pt, listener, udata, event_loop);
	}

	// Old query. Convert to a scan when filter doesn't exist.
	if (query->where.size == 0) {
		as_policy_scan scan_policy;
		as_scan scan;
		convert_query_to_scan(policy, query, &scan_policy, &scan);

		return aerospike_scan_async(as, err, &scan_policy, &scan, NULL, listener, udata, event_loop);
	}

	uint64_t task_id = as_random_get_uint64();
	as_queue opsbuffers;

	if (query->ops) {
		as_queue_inita(&opsbuffers, sizeof(as_buffer), query->ops->binops.size);
	}

	as_query_builder qb;
	as_query_builder_init(&qb, cluster, &opsbuffers, NULL, NULL);

	status = as_query_command_size(&policy->base, query, &qb, err);

	if (status != AEROSPIKE_OK) {
		if (query->ops) {
			as_buffers_destroy(&opsbuffers);
		}
		return status;
	}

	uint8_t* cmd_buf = as_command_buffer_init(qb.size);
	size_t size = as_query_command_init(cmd_buf, &policy->base, policy, NULL, query,
		QUERY_FOREGROUND, task_id, &qb);

	as_nodes* nodes;
	status = as_cluster_reserve_all_nodes(cluster, err, &nodes);

	if (status != AEROSPIKE_OK) {
		if (query->ops) {
			as_buffers_destroy(&opsbuffers);
		}
		as_command_buffer_free(cmd_buf, qb.size);
		return status;
	}

	// Query will be split up into a command for each node.
	// Allocate query data shared by each command.
	as_async_query_executor* executor = cf_malloc(sizeof(as_async_query_executor));
	as_event_executor* exec = &executor->executor;
	pthread_mutex_init(&exec->lock, NULL);
	exec->commands = cf_malloc(sizeof(as_event_command*) * nodes->size);
	exec->event_loop = as_event_assign(event_loop);
	exec->complete_fn = as_query_complete_async;
	exec->udata = udata;
	exec->err = NULL;
	exec->ns = NULL;
	exec->cluster_key = 0;
	exec->max_concurrent = nodes->size;
	exec->max = nodes->size;
	exec->count = 0;
	exec->queued = 0;
	exec->notify = true;
	exec->valid = true;
	executor->listener = listener;
	executor->info_timeout = policy->info_timeout;

	// Allocate enough memory to cover, then, round up memory size in 8KB increments to allow socket
	// read to reuse buffer.
	size_t s = (sizeof(as_async_query_command) + size + AS_AUTHENTICATION_MAX_SIZE + 8191) & ~8191;

	// Create all query commands.
	for (uint32_t i = 0; i < nodes->size; i++) {
		as_async_query_command* qcmd = cf_malloc(s);
		qcmd->np = NULL;

		as_event_command* cmd = &qcmd->command;
		cmd->total_deadline = policy->base.total_timeout;
		cmd->socket_timeout = policy->base.socket_timeout;
		cmd->max_retries = 0;
		cmd->iteration = 0;
		cmd->replica = AS_POLICY_REPLICA_MASTER;
		cmd->event_loop = exec->event_loop;
		cmd->cluster = cluster;
		cmd->node = nodes->array[i];
		cmd->ns = NULL;
		cmd->partition = NULL;
		cmd->udata = executor;  // Overload udata to be the executor.
		cmd->parse_results = as_query_parse_records_async;
		cmd->pipe_listener = NULL;
		cmd->buf = ((as_async_query_command*)cmd)->space;
		cmd->write_len = (uint32_t)size;
		cmd->read_capacity = (uint32_t)(s - size - sizeof(as_async_query_command));
		cmd->type = AS_ASYNC_TYPE_QUERY;
		cmd->proto_type = AS_MESSAGE_TYPE;
		cmd->state = AS_ASYNC_STATE_UNREGISTERED;
		cmd->flags = AS_ASYNC_FLAGS_MASTER;
		cmd->flags2 = policy->deserialize ? AS_ASYNC_FLAGS2_DESERIALIZE : 0;
		memcpy(cmd->buf, cmd_buf, size);
		exec->commands[i] = cmd;
	}
	
	// Free command buffer.
	as_command_buffer_free(cmd_buf, qb.size);
	
	if (policy->fail_on_cluster_change) {
		// Verify migrations are not in progress.
		status = as_query_validate_begin_async(exec, query->ns, err);
	}
	else {
		// Run query commands.
		uint32_t max = exec->max_concurrent;

		for (uint32_t i = 0; i < max; i++) {
			exec->queued++;
			as_event_command* cmd = exec->commands[i];
			status = as_event_command_execute(cmd, err);

			if (status != AEROSPIKE_OK) {
				as_event_executor_cancel(exec, i);
				break;
			}
		}
	}

	// Do not call as_cluster_release_all_nodes() because individual nodes
	// are released on each async command destroy.
	as_nodes_release(nodes);
	return status;
}

as_status
aerospike_query_partitions_async(
	aerospike* as, as_error* err, const as_policy_query* policy, as_query* query,
	as_partition_filter* pf, as_async_query_record_listener listener, void* udata,
	as_event_loop* event_loop
	)
{
	if (query->apply.function[0] || query->ops) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
			"Aggregation or background queries cannot query by partition");
	}

	as_cluster* cluster = as->cluster;

	if (! cluster->has_partition_query) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
			"Partition query not supported by connected server");
	}

	as_error_reset(err);

	if (! policy) {
		policy = &as->config.policies.query;
	}

	uint32_t n_nodes;
	as_status status = as_cluster_validate_size(cluster, err, &n_nodes);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	if (pf->parts_all && ! query->parts_all) {
		as_query_set_partitions(query, pf->parts_all);
	}

	as_partition_tracker* pt = cf_malloc(sizeof(as_partition_tracker));
	status = as_partition_tracker_init_filter(pt, cluster, &policy->base, query->max_records,
		&query->parts_all, query->paginate, n_nodes, pf, err);

	if (status != AEROSPIKE_OK) {
		cf_free(pt);
		return status;
	}
	return as_query_partition_async(cluster, err, policy, query, pt, listener, udata, event_loop);
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
	
	if (! (query->apply.function[0] || query->ops)) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"Background function or ops is required");
	}

	as_cluster* cluster = as->cluster;

	as_nodes* nodes;
	as_status status = as_cluster_reserve_all_nodes(as->cluster, err, &nodes);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	// Set task id
	uint64_t task_id = as_task_id_resolve(query_id);
	uint32_t error_mutex = 0;

	// Initialize task.
	as_query_task task = {
		.node = NULL,
		.np = NULL,
		.pt = NULL,
		.cluster = cluster,
		.query_policy = NULL,
		.write_policy = policy,
		.query = query,
		.callback = NULL,
		.udata = NULL,
		.err = err,
		.error_mutex = &error_mutex,
		.input_queue = NULL,
		.complete_q = NULL,
		.task_id = task_id,
		.cluster_key = 0,
		.cmd = NULL,
		.cmd_size = 0,
		.query_type = QUERY_BACKGROUND,
		.first = true
	};

	status = as_query_execute(&task, query, nodes);

	as_cluster_release_all_nodes(nodes);
	return status;
}

uint32_t
as_query_get_info_timeout(as_event_executor* executor)
{
	return ((as_async_query_executor*)executor)->info_timeout;
}
