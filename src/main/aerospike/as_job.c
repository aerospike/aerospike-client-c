/*
 * Copyright 2008-2015 Aerospike, Inc.
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
#include <aerospike/as_socket.h>

/******************************************************************************
 * STATIC DATA
 *****************************************************************************/

const char JOB_STATUS_TAG[] = "status=";
const int JOB_STATUS_TAG_LEN = sizeof(JOB_STATUS_TAG) - 1;

const char JOB_PROGRESS_TAG[] = "job-progress=";
const int JOB_PROGRESS_TAG_LEN = sizeof(JOB_PROGRESS_TAG) - 1;

const char RECORDS_READ_TAG[] = "recs-read=";
const int RECORDS_READ_TAG_LEN = sizeof(RECORDS_READ_TAG) - 1;

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

static void
as_job_process(char* response, as_job_info* info)
{
	char* p = response;
	char* begin;
	
	while (*p) {
		if (strncmp(p, JOB_STATUS_TAG, JOB_STATUS_TAG_LEN) == 0) {
			p += JOB_STATUS_TAG_LEN;
			
			if (strncmp(p, "active", 6) == 0) {
				info->status = AS_JOB_STATUS_INPROGRESS;
			}
			else if (strncmp(p, "done", 4) == 0) {
				if (info->status == AS_JOB_STATUS_UNDEF) {
					info->status = AS_JOB_STATUS_COMPLETED;
				}
			}
			p = as_find_next(p);
		}
		else if (strncmp(p, JOB_PROGRESS_TAG, JOB_PROGRESS_TAG_LEN) == 0) {
			p += JOB_PROGRESS_TAG_LEN;
			begin = p;
			p = as_mark_end(p);
			
			uint32_t pct = atoi(begin);
			
			// Be pessimistic - use the slowest node's progress.
			if (info->progress_pct == 0 || pct < info->progress_pct) {
				info->progress_pct = pct;
			}
		}
		else if (strncmp(p, RECORDS_READ_TAG, RECORDS_READ_TAG_LEN) == 0) {
			p += RECORDS_READ_TAG_LEN;
			begin = p;
			p = as_mark_end(p);
			
			uint64_t count = atol(begin);
			info->records_read += count;
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
	uint32_t interval_micros = (interval_ms <= 0)? 1000 * 1000 : interval_ms * 1000;
	as_job_info info;
	as_status status;
	
	// Poll to see when job is done.
	do {
		usleep(interval_micros);
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

	char command[128];
	sprintf(command, "jobs:module=%s;cmd=get-job;trid=%" PRIu64 "\n", module, job_id);

	info->status = AS_JOB_STATUS_UNDEF;
	info->progress_pct = 0;
	info->records_read = 0;
		
	as_status status = AEROSPIKE_ERR_CLUSTER;
	uint64_t deadline = as_socket_deadline(policy->timeout);
	as_cluster* cluster = as->cluster;
	as_nodes* nodes = as_nodes_reserve(cluster);
	
	for (uint32_t i = 0; i < nodes->size; i++) {
		as_node* node = nodes->array[i];
		struct sockaddr_in* sa_in = as_node_get_address(node);
		char* response = 0;
		
		status = as_info_command_host(cluster, err, sa_in, command, true, deadline, &response);
		
		if (status == AEROSPIKE_OK) {
			as_job_process(response, info);
			free(response);
			
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
			if (status != AEROSPIKE_ERR_CLUSTER) {
				break;
			}
		}
	}
	as_nodes_release(nodes);
	return status;
}
