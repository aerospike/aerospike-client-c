/*
 * Copyright 2008-2023 Aerospike, Inc.
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
#include <aerospike/as_job.h>
#include <aerospike/as_info.h>
#include <aerospike/as_sleep.h>
#include <aerospike/as_socket.h>
#include <citrusleaf/alloc.h>
#include <stdlib.h>

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static char*
as_find_next(char* p)
{
	while (*p) {
		if (*p == ':') {
			return p + 1;
		}
		p++;
	}
	return p;
}

static char*
as_mark_end(char* p)
{
	while (*p) {
		if (*p == ':') {
			*p = 0;
			return p + 1;
		}
		p++;
	}
	return p;
}

static char*
as_set_records_read(char* p, as_job_info* info) {
	char* begin = p;
	p = as_mark_end(p);

	long count = atol(begin);
	info->records_read += count;
	return p;
}

static void
as_job_process(char* response, as_job_info* info)
{
	char* p = response;
	char* begin;
	bool found_recs_read = false;
	
	while (*p) {
		if (strncmp(p, "status=", 7) == 0) {
			p += 7;
			
			// Newer servers use "active(ok)" while older servers use "IN_PROGRESS"
			if (strncmp(p, "active", 6) == 0 || strncmp(p, "IN_PROGRESS", 11) == 0) {
				info->status = AS_JOB_STATUS_INPROGRESS;
			}
			// Newer servers use "done" while older servers use "DONE"
			else if (strncasecmp(p, "done", 4) == 0) {
				if (info->status == AS_JOB_STATUS_UNDEF) {
					info->status = AS_JOB_STATUS_COMPLETED;
				}
			}
			p = as_find_next(p);
		}
		else if (strncmp(p, "job-progress=", 13) == 0) {
			p += 13;
			begin = p;
			p = as_mark_end(p);
			
			uint32_t pct = atoi(begin);
			
			// Be pessimistic - use the slowest node's progress.
			if (info->progress_pct == 0 || pct < info->progress_pct) {
				info->progress_pct = pct;
			}
		}
		// Recent servers use recs-succeeded.
		else if (!found_recs_read && strncmp(p, "recs-succeeded=", 15) == 0) {
			p += 15;
			p = as_set_records_read(p, info);
			found_recs_read = true;
		}
		// Some servers used dash while much older servers use underscore.
		else if (!found_recs_read && (strncmp(p, "recs-read=", 10) == 0 ||
			strncmp(p, "recs_read=", 10) == 0)) {
			p += 10;
			p = as_set_records_read(p, info);
			found_recs_read = true;
		}
		else {
			p = as_find_next(p);
		}
	}
}

/******************************************************************************
 * PUBLIC FUNCTIONS
 *****************************************************************************/

as_status
aerospike_job_wait(
   aerospike* as, as_error* err, const as_policy_info* policy, const char* module, uint64_t job_id,
   uint32_t interval_ms)
{
	if (!interval_ms) {
		interval_ms = 1000;
	}

	as_job_info info;
	as_status status;
	
	// Poll to see when job is done.
	do {
		as_sleep(interval_ms);
		status = aerospike_job_info(as, err, policy, module, job_id, true, &info);
	} while (status == AEROSPIKE_OK && info.status == AS_JOB_STATUS_INPROGRESS);
	
	return status;
}

as_status
aerospike_job_info(
	aerospike* as, as_error* err, const as_policy_info* policy, const char* module, uint64_t job_id,
	bool stop_if_in_progress, as_job_info* info)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.info;
	}

	char cmd1[128];
	char cmd2[128];
	char cmd3[128];
	sprintf(cmd1, "query-show:trid=%" PRIu64 "\n", job_id);
	sprintf(cmd2, "%s-show:trid=%" PRIu64 "\n", module, job_id);
	sprintf(cmd3, "jobs:module=%s;cmd=get-job;trid=%" PRIu64 "\n", module, job_id);

	info->status = AS_JOB_STATUS_UNDEF;
	info->progress_pct = 0;
	info->records_read = 0;
		
	as_status status = AEROSPIKE_ERR_CLUSTER;
	uint64_t deadline = as_socket_deadline(policy->timeout);
	as_cluster* cluster = as->cluster;
	as_nodes* nodes = as_nodes_reserve(cluster);
	
	for (uint32_t i = 0; i < nodes->size; i++) {
		as_node* node = nodes->array[i];
		char* command;

		if (node->features & AS_FEATURES_PARTITION_QUERY) {
			// query-show works for both scan and query.
			command = cmd1;
		}
		else if (node->features & AS_FEATURES_QUERY_SHOW) {
			// scan-show and query-show are separate.
			command = cmd2;
		}
		else {
			// old job monitor syntax.
			command = cmd3;
		}

		char* response = 0;
		
		status = as_info_command_node(err, node, command, true, deadline, &response);
		
		if (status == AEROSPIKE_OK) {
			as_job_process(response, info);
			cf_free(response);
			
			if (stop_if_in_progress && info->status == AS_JOB_STATUS_INPROGRESS) {
				break;
			}
		}
		else if (status == AEROSPIKE_ERR_RECORD_NOT_FOUND) {
			if (info->status == AS_JOB_STATUS_UNDEF) {
				info->status = AS_JOB_STATUS_COMPLETED;
			}
			as_error_reset(err);
			status = AEROSPIKE_OK;
		}
		else {
			break;
		}
	}
	as_nodes_release(nodes);
	return status;
}
