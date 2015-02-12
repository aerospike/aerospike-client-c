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
#include <aerospike/aerospike_scan.h>
#include <aerospike/aerospike_info.h>
#include <aerospike/as_command.h>
#include <aerospike/as_key.h>
#include <aerospike/as_log.h>
#include <aerospike/as_msgpack.h>
#include <aerospike/as_serializer.h>
#include <aerospike/as_socket.h>

#include <citrusleaf/cf_clock.h>
#include <citrusleaf/cf_queue.h>
#include <citrusleaf/cf_random.h>

/******************************************************************************
 * TYPES
 *****************************************************************************/

typedef struct as_scan_task_s {
	as_node* node;
	
	as_cluster* cluster;
	const as_policy_scan* policy;
	const as_scan* scan;
	aerospike_scan_foreach_callback callback;
	void* udata;
	as_error* err;
	cf_queue* complete_q;
	uint32_t* error_mutex;
	uint64_t task_id;
	
	uint8_t* cmd;
	size_t cmd_size;
} as_scan_task;

typedef struct as_scan_complete_task_s {
	as_node* node;
	uint64_t task_id;
	as_status result;
} as_scan_complete_task;

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static uint8_t*
as_scan_parse_record(uint8_t* p, as_msg* msg, as_scan_task* task)
{
	as_record rec;
	as_record_inita(&rec, msg->n_ops);
	
	rec.gen = msg->generation;
	rec.ttl = cf_server_void_time_to_ttl(msg->record_ttl);
	
	p = as_command_parse_key(p, msg->n_fields, &rec.key);
	p = as_command_parse_bins(&rec, p, msg->n_ops, task->scan->deserialize_list_map);
	
	if (task->callback) {
		bool rv = task->callback((as_val*)&rec, task->udata);
		as_record_destroy(&rec);
		return rv ? p : 0;
	}
	as_record_destroy(&rec);
	return p;
}

static as_status
as_scan_parse_records(uint8_t* buf, size_t size, as_scan_task* task)
{
	uint8_t* p = buf;
	uint8_t* end = buf + size;
	
	while (p < end) {
		as_msg* msg = (as_msg*)p;
		as_msg_swap_header_from_be(msg);
		
		if (msg->result_code) {
		   // Special case - if we scan a set name that doesn't exist on a
		   // node, it will return "not found" - we unify this with the
		   // case where OK is returned and no callbacks were made. [AKG]
		   // We are sending "no more records back" to the caller which will
		   // send OK to the main worker thread.
			if (msg->result_code == AEROSPIKE_ERR_RECORD_NOT_FOUND) {
				return AEROSPIKE_NO_MORE_RECORDS;
			}
			return msg->result_code;
		}
		p += sizeof(as_msg);
		
		if (msg->info3 & AS_MSG_INFO3_LAST) {
			return AEROSPIKE_NO_MORE_RECORDS;
		}
		
		p = as_scan_parse_record(p, msg, task);
		
		if (!p) {
			return AEROSPIKE_NO_MORE_RECORDS;
		}
		
		if (ck_pr_load_32(task->error_mutex)) {
			return AEROSPIKE_NO_MORE_RECORDS;
		}
	}
	return AEROSPIKE_OK;
}

static as_status
as_scan_parse(as_error* err, int fd, uint64_t deadline_ms, void* udata)
{
	as_scan_task* task = udata;
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
			
			status = as_scan_parse_records(buf, size, task);
			
			if (status != AEROSPIKE_OK) {
				if (status == AEROSPIKE_NO_MORE_RECORDS) {
					status = AEROSPIKE_OK;
				}
				else {
					as_error_set_message(err, status, as_error_string(status));
				}
				break;
			}
		}
		else {
			status = as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Received zero sized data packet from server.");
			break;
		}
	}
	as_command_free(buf, capacity);
	return status;
}

static as_status
as_scan_command_execute(as_scan_task* task)
{
	as_command_node cn;
	cn.node = task->node;
	
	as_error err;
	as_error_init(&err);
	as_status status = as_command_execute(&err, &cn, task->cmd, task->cmd_size, task->policy->timeout, AS_POLICY_RETRY_NONE, as_scan_parse, task);
	
	if (status) {
		// Copy error to main error only once.
		if (ck_pr_fas_32(task->error_mutex, 1) == 0) {
			memcpy(task->err, &err, sizeof(as_error));
		}
	}
	return status;
}

static void*
as_scan_worker(void* data)
{
	as_cluster* cluster = (as_cluster*)data;
	as_scan_task task;
	
	while (cf_queue_pop(cluster->scan_q, &task, CF_QUEUE_FOREVER) == CF_QUEUE_OK) {
		// This is how batch shutdown signals we're done.
		if (! task.cluster) {
			break;
		}
		
		as_scan_complete_task complete_task;
		complete_task.node = task.node;
		complete_task.task_id = task.task_id;
		complete_task.result = as_scan_command_execute(&task);
		
		cf_queue_push(task.complete_q, &complete_task);
	}
	return 0;
}

static void
as_scan_threads_init(as_cluster* cluster)
{
	// We do this lazily, during the first scan request, so make sure it's only
	// done once.
	if (ck_pr_fas_32(&cluster->scan_initialized, 1) == 1 || cluster->scan_q) {
		return;
	}
		
	// Create dispatch queue.
	cluster->scan_q = cf_queue_create(sizeof(as_scan_task), true);
	
	// Create thread pool.
	for (int i = 0; i < AS_NUM_SCAN_THREADS; i++) {
		pthread_create(&cluster->scan_threads[i], 0, as_scan_worker, cluster);
	}
}

void
as_scan_threads_shutdown(as_cluster* cluster)
{
	// Check whether we ever (lazily) initialized scan machinery.
	if (ck_pr_load_32(&cluster->scan_initialized) == 0 && ! cluster->scan_q) {
		return;
	}
	
	// This tells the worker threads to stop. We do this (instead of using a
	// "running" flag) to allow the workers to "wait forever" on processing the
	// work dispatch queue, which has minimum impact when the queue is empty.
	// This also means all queued requests get processed when shutting down.
	for (int i = 0; i < AS_NUM_SCAN_THREADS; i++) {
		as_scan_task task;
		task.cluster = NULL;
		cf_queue_push(cluster->scan_q, &task);
	}
	
	for (int i = 0; i < AS_NUM_SCAN_THREADS; i++) {
		pthread_join(cluster->scan_threads[i], NULL);
	}
	
	cf_queue_destroy(cluster->scan_q);
	cluster->scan_q = NULL;
	ck_pr_store_32(&cluster->scan_initialized, 0);
}

static size_t
as_scan_command_size(const as_scan* scan, uint16_t* fields, as_buffer* argbuffer)
{
	// Build Command.  It's okay to share command across threads because scan does not have retries.
	// If retries were allowed, the timeout field in the command would change on retry which
	// would conflict with other threads.
	size_t size = AS_HEADER_SIZE;
	uint16_t n_fields = 0;
	
	if (scan->ns) {
		size += as_command_string_field_size(scan->ns);
		n_fields++;
	}
	
	if (scan->set) {
		size += as_command_string_field_size(scan->set);
		n_fields++;
	}
	
	// Estimate scan options size.
	size += as_command_field_size(2);
	n_fields++;
	
	// Estimate taskId size.
	size += as_command_field_size(8);
	n_fields++;
	
	// Estimate background function size.
	as_buffer_init(argbuffer);
	
	if (scan->apply_each.function[0]) {
		size += as_command_field_size(1);
		size += as_command_string_field_size(scan->apply_each.module);
		size += as_command_string_field_size(scan->apply_each.function);
		
		if (scan->apply_each.arglist) {
			// If the query has a udf w/ arglist, then serialize it.
			as_serializer ser;
			as_msgpack_init(&ser);
            as_serializer_serialize(&ser, (as_val*)scan->apply_each.arglist, argbuffer);
			as_serializer_destroy(&ser);
		}
		size += as_command_field_size(argbuffer->size);
		n_fields += 4;
	}
	
	// Estimate size for selected bin names.
	if (scan->select.size > 0) {
		for (uint16_t i = 0; i < scan->select.size; i++) {
			size += as_command_string_operation_size(scan->select.entries[i]);
		}
	}
	*fields = n_fields;
	return size;
}

static size_t
as_scan_command_init(uint8_t* cmd, const as_policy_scan* policy, const as_scan* scan,
	uint64_t task_id, uint16_t n_fields, as_buffer* argbuffer)
{
	uint8_t* p;
	
	if (scan->apply_each.function[0]) {
		p = as_command_write_header(cmd, AS_MSG_INFO1_READ, AS_MSG_INFO2_WRITE,
			AS_POLICY_COMMIT_LEVEL_ALL, AS_POLICY_CONSISTENCY_LEVEL_ONE, AS_POLICY_EXISTS_IGNORE,
			AS_POLICY_GEN_IGNORE, 0, 0, policy->timeout, n_fields, 0);
	}
	else {
		uint8_t read_attr = (scan->no_bins)? AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_NOBINDATA : AS_MSG_INFO1_READ;
		p = as_command_write_header_read(cmd, read_attr, AS_POLICY_CONSISTENCY_LEVEL_ONE, policy->timeout, n_fields, scan->select.size);
	}
	
	if (scan->ns) {
		p = as_command_write_field_string(p, AS_FIELD_NAMESPACE, scan->ns);
	}
	
	if (scan->set) {
		p = as_command_write_field_string(p, AS_FIELD_SETNAME, scan->set);
	}
	
	// Write scan options
	p = as_command_write_field_header(p, AS_FIELD_SCAN_OPTIONS, 2);
	uint8_t priority = scan->priority << 4;
	
	if (policy->fail_on_cluster_change) {
		priority |= 0x08;
	}
	*p++ = priority;
	*p++ = scan->percent;
	
	// Write taskId field
	p = as_command_write_field_uint64(p, AS_FIELD_TASK_ID, task_id);
	
	// Write background function
	if (scan->apply_each.function[0]) {
		p = as_command_write_field_header(p, AS_FIELD_UDF_OP, 1);
		*p++ = 2;
		p = as_command_write_field_string(p, AS_FIELD_UDF_PACKAGE_NAME, scan->apply_each.module);
		p = as_command_write_field_string(p, AS_FIELD_UDF_FUNCTION, scan->apply_each.function);
		p = as_command_write_field_buffer(p, AS_FIELD_UDF_ARGLIST, argbuffer);
	}
    as_buffer_destroy(argbuffer);
	
	if (scan->select.size > 0) {
		for (uint16_t i = 0; i < scan->select.size; i++) {
			p = as_command_write_bin_name(p, scan->select.entries[i]);
		}
	}
	return as_command_write_end(cmd, p);
}

static as_status
as_scan_generic(
	aerospike* as, as_error* err, const as_policy_scan* policy, const as_scan* scan,
	aerospike_scan_foreach_callback callback, void* udata, uint64_t* task_id_ptr)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.scan;
	}
	
	as_cluster* cluster = as->cluster;
	as_nodes* nodes = as_nodes_reserve(cluster);
	uint32_t n_nodes = nodes->size;
	
	if (n_nodes == 0) {
		as_nodes_release(nodes);
		return as_error_set_message(err, AEROSPIKE_ERR_SERVER, "Scan command failed because cluster is empty.");
	}
	
	// Reserve each node in cluster.
	for (uint32_t i = 0; i < n_nodes; i++) {
		as_node_reserve(nodes->array[i]);
	}
	
	uint64_t task_id;
	if (task_id_ptr) {
		if (*task_id_ptr == 0) {
			*task_id_ptr = cf_get_rand64() / 2;
		}
		task_id = *task_id_ptr;
	}
	else {
		task_id = cf_get_rand64() / 2;
	}

	// Create scan command
	as_buffer argbuffer;
	uint16_t n_fields = 0;
	size_t size = as_scan_command_size(scan, &n_fields, &argbuffer);
	uint8_t* cmd = as_command_init(size);
	size = as_scan_command_init(cmd, policy, scan, task_id, n_fields, &argbuffer);
	
	// Initialize task.
	uint32_t error_mutex = 0;
	as_scan_task task;
	task.cluster = as->cluster;
	task.policy = policy;
	task.scan = scan;
	task.callback = callback;
	task.udata = udata;
	task.err = err;
	task.error_mutex = &error_mutex;
	task.task_id = task_id;
	task.cmd = cmd;
	task.cmd_size = size;
	
	as_status status = AEROSPIKE_OK;
	
	if (scan->concurrent) {
		// Run node scans in parallel.
		as_scan_threads_init(cluster);
		
		task.complete_q = cf_queue_create(sizeof(as_scan_complete_task), true);

		for (uint32_t i = 0; i < n_nodes; i++) {
			task.node = nodes->array[i];
			cf_queue_push(cluster->scan_q, &task);
		}

		// Wait for tasks to complete.
		for (uint32_t i = 0; i < n_nodes; i++) {
			as_scan_complete_task complete;
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
		
		// Run node scans in series.
		for (uint32_t i = 0; i < n_nodes && status == AEROSPIKE_OK; i++) {
			task.node = nodes->array[i];
			status = as_scan_command_execute(&task);
		}
	}
	
	// Release each node in cluster.
	for (uint32_t i = 0; i < n_nodes; i++) {
		as_node_release(nodes->array[i]);
	}
	
	// Release nodes array.
	as_nodes_release(nodes);

	// Free command memory.
	as_command_free(cmd, size);

	// If completely successful, make the callback that signals completion.
	if (callback && status == AEROSPIKE_OK) {
		callback(NULL, udata);
	}
	return status;
}

// Wrapper for background scan info.
typedef struct bg_scan_info_s {
	char job_id[32];
	int job_id_len;
	as_scan_info * info;
} bg_scan_info;

const char JOB_STATUS_TAG[] = "job_status=";
const int JOB_STATUS_TAG_LEN = sizeof(JOB_STATUS_TAG) - 1;

const char JOB_PROGRESS_TAG[] = "job_progress(%)=";
const int JOB_PROGRESS_TAG_LEN = sizeof(JOB_PROGRESS_TAG) - 1;

const char SCANNED_RECORDS_TAG[] = "scanned_records=";
const int SCANNED_RECORDS_TAG_LEN = sizeof(SCANNED_RECORDS_TAG) - 1;

/**
 * The info callback made for each node when doing aerospike_scan_info().
 */
static bool
scan_info_cb(const as_error * err, const as_node * node, const char * req, char * res, void * udata)
{
	bg_scan_info* p_bsi = (bg_scan_info*)udata;

	// For now, fast and dirty parsing for exactly what we're looking for...
	// If we can't find the expected tag on this node, something's wrong, but
	// try the others. (OK? Or should we ever return false and wipe the info?)

	char* p_read = strstr(res, p_bsi->job_id);
	if (! p_read) {
		return true;
	}
	p_read += p_bsi->job_id_len;

	// If any node is aborted, we're aborted overall, don't bother parse status.
	if (p_bsi->info->status != AS_SCAN_STATUS_ABORTED) {
		p_read = strstr(p_read, JOB_STATUS_TAG);
		if (! p_read) {
			return true;
		}
		p_read += JOB_STATUS_TAG_LEN;

		if (strncmp(p_read, "ABORTED", 7) == 0) {
			p_bsi->info->status = AS_SCAN_STATUS_ABORTED;
		}
		else if (strncmp(p_read, "IN PROGRESS", 11) == 0) {
			// Otherwise if any node is in progress, we're in progress overall.
			p_bsi->info->status = AS_SCAN_STATUS_INPROGRESS;
		}
		else if (p_bsi->info->status == AS_SCAN_STATUS_UNDEF &&
				strncmp(p_read, "DONE", 4) == 0) {
			// Only if we haven't modified the status - if a prior node was in
			// progress, overall we're in progress.
			p_bsi->info->status = AS_SCAN_STATUS_COMPLETED;
		}
	}

	p_read = strstr(p_read, JOB_PROGRESS_TAG);
	if (! p_read) {
		return true;
	}
	p_read += JOB_PROGRESS_TAG_LEN;

	// Be pessimistic - use the slowest node's progress.
	uint32_t pct = atoi(p_read);
	if (p_bsi->info->progress_pct == 0 || pct < p_bsi->info->progress_pct) {
		p_bsi->info->progress_pct = pct;
	}

	p_read = strstr(p_read, SCANNED_RECORDS_TAG);
	if (! p_read) {
		return true;
	}
	p_read += SCANNED_RECORDS_TAG_LEN;

	// Accumulate total.
	p_bsi->info->records_scanned += atoi(p_read);

	return true;
}


/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 *	Scan the records in the specified namespace and set in the cluster.
 *
 *	Scan will be run in the background by a thread on client side.
 *	No callback will be called in this case.
 *	
 *	~~~~~~~~~~{.c}
 *	as_scan scan;
 *	as_scan_init(&scan, "test", "demo");
 *	as_scan_apply_each(&scan, "udf_module", "udf_function", NULL);
 *	
 *	uint64_t scanid = 0;
 *	
 *	if ( aerospike_scan_background(&as, &err, NULL, &scan, &scanid) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	else {
 *		printf("Running background scan job: %ll", scanid);
 *	}
 *
 *	as_scan_destroy(&scan);
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param scan 		The scan to execute against the cluster.
 *	@param scan_id		The id for the scan job, which can be used for querying the status of the scan.
 *
 *	@return AEROSPIKE_OK on success. Otherwise an error occurred.
 */
as_status aerospike_scan_background(
	aerospike * as, as_error * err, const as_policy_scan * policy, 
	const as_scan * scan, uint64_t * scan_id
	)
{
	return as_scan_generic(as, err, policy, scan, 0, 0, scan_id);
}

/**
 *	Wait for a background scan to be completed by servers.
 *
 *	~~~~~~~~~~{.c}
 *	uint64_t scan_id = 1234;
 *	aerospike_scan_wait(&as, &err, NULL, scan_id, 0);
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param scan_id		The id for the scan job.
 *	@param interval_ms	The polling interval in milliseconds. If zero, 1000 ms is used.
 *
 *	@return AEROSPIKE_OK on success. Otherwise an error occurred.
 */
as_status aerospike_scan_wait(
	aerospike * as, as_error * err, const as_policy_info * policy,
	uint64_t scan_id, uint32_t interval_ms
	)
{
	uint32_t interval_micros = (interval_ms <= 0)? 1000 * 1000 : interval_ms * 1000;
	as_scan_info info;
	as_status status;
	
	// Poll to see when scan is done.
	do {
		usleep(interval_micros);
		status = aerospike_scan_info(as, err, policy, scan_id, &info);
	} while (status == AEROSPIKE_OK && info.status == AS_SCAN_STATUS_INPROGRESS);
	
	return status;
}

/**
 *	Check on a background scan running on the server.
 *
 *	~~~~~~~~~~{.c}
 *	uint64_t scan_id = 1234;
 *	as_scan_info scan_info;
 *
 *	if ( aerospike_scan_info(&as, &err, NULL, &scan, scan_id, &scan_info) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	else {
 *		printf("Scan id=%ll, status=%s", scan_id, scan_info.status);
 *	}
 *	~~~~~~~~~~
 *
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param scan_id		The id for the scan job to check the status of.
 *	@param info			Information about this scan, to be populated by this operation.
 *
 *	@return AEROSPIKE_OK on success. Otherwise an error occurred.
 */
as_status aerospike_scan_info(
	aerospike * as, as_error * err, const as_policy_info * policy,
	uint64_t scan_id, as_scan_info * info
	)
{
	// Initialize the info...
	info->status = AS_SCAN_STATUS_UNDEF;
	info->progress_pct = 0;
	info->records_scanned = 0;

	bg_scan_info bsi;
	bsi.job_id_len = sprintf(bsi.job_id, "job_id=%" PRIu64 ":", scan_id);
	bsi.info = info;

	return aerospike_info_foreach(as, err, policy, "scan-list\n", scan_info_cb, (void *) &bsi);
}

/**
 *	Scan the records in the specified namespace and set in the cluster.
 *
 *	Call the callback function for each record scanned. When all records have 
 *	been scanned, then callback will be called with a NULL value for the record.
 *
 *	~~~~~~~~~~{.c}
 *	as_scan scan;
 *	as_scan_init(&scan, "test", "demo");
 *	
 *	if ( aerospike_scan_foreach(&as, &err, NULL, &scan, callback, NULL) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *
 *	as_scan_destroy(&scan);
 *	~~~~~~~~~~
 *	
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param scan			The scan to execute against the cluster.
 *	@param callback		The function to be called for each record scanned.
 *	@param udata		User-data to be passed to the callback.
 *
 *	@return AEROSPIKE_OK on success. Otherwise an error occurred.
 */
as_status aerospike_scan_foreach(
	aerospike * as, as_error * err, const as_policy_scan * policy, 
	const as_scan * scan, 
	aerospike_scan_foreach_callback callback, void * udata) 
{
	return as_scan_generic(as, err, policy, scan, callback, udata, 0);
}

/**
 *	Scan the records in the specified namespace and set for a single node.
 *
 *	The callback function will be called for each record scanned. When all records have
 *	been scanned, then callback will be called with a NULL value for the record.
 *
 *	~~~~~~~~~~{.c}
 *	char* node_names = NULL;
 *	int n_nodes = 0;
 *	as_cluster_get_node_names(as->cluster, &n_nodes, &node_names);
 *
 *	if (n_nodes <= 0)
 *		return <error>;
 *
 *	as_scan scan;
 *	as_scan_init(&scan, "test", "demo");
 *
 *	if (aerospike_scan_node(&as, &err, NULL, &scan, node_names[0], callback, NULL) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *
 *	free(node_names);
 *	as_scan_destroy(&scan);
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param scan			The scan to execute against the cluster.
 *	@param node_name	The node name to scan.
 *	@param callback		The function to be called for each record scanned.
 *	@param udata		User-data to be passed to the callback.
 *
 *	@return AEROSPIKE_OK on success. Otherwise an error occurred.
 */
as_status aerospike_scan_node(
	aerospike * as, as_error * err, const as_policy_scan * policy,
	const as_scan * scan,  const char* node_name,
	aerospike_scan_foreach_callback callback, void * udata)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.scan;
	}

	// Retrieve node.
	as_node* node = as_node_get_by_name(as->cluster, node_name);
	
	if (! node) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid node name: %s", node_name);
	}

	// Create scan command
	uint64_t task_id = cf_get_rand64() / 2;
	as_buffer argbuffer;
	uint16_t n_fields = 0;
	size_t size = as_scan_command_size(scan, &n_fields, &argbuffer);
	uint8_t* cmd = as_command_init(size);
	size = as_scan_command_init(cmd, policy, scan, task_id, n_fields, &argbuffer);
	
	// Initialize task.
	uint32_t error_mutex = 0;
	as_scan_task task;
	task.node = node;
	task.cluster = as->cluster;
	task.policy = policy;
	task.scan = scan;
	task.callback = callback;
	task.udata = udata;
	task.err = err;
	task.complete_q = 0;
	task.error_mutex = &error_mutex;
	task.task_id = task_id;
	task.cmd = cmd;
	task.cmd_size = size;
	
	// Run scan.
	as_status status = as_scan_command_execute(&task);
		
	// Free command memory.
	as_command_free(cmd, size);
	
	// Release node.
	as_node_release(node);
	
	// If completely successful, make the callback that signals completion.
	if (callback && status == AEROSPIKE_OK) {
		callback(NULL, udata);
	}
	return status;
}
