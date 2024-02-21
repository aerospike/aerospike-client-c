/*
 * Copyright 2008-2024 Aerospike, Inc.
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
#include <aerospike/as_metrics.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_event.h>
#include <aerospike/as_node.h>
#include <aerospike/as_string_builder.h>

//---------------------------------
// Functions
//---------------------------------

as_status
aerospike_enable_metrics(aerospike* as, as_error* err, as_metrics_policy* policy)
{
	return as_cluster_enable_metrics(err, as->cluster, policy);
}

as_status
aerospike_disable_metrics(aerospike* as, as_error* err)
{
	return as_cluster_disable_metrics(err, as->cluster);
}

void
as_metrics_policy_init(as_metrics_policy* policy)
{
	policy->report_size_limit = 0;
	as_strncpy(policy->report_dir, ".", sizeof(policy->report_dir));
	policy->interval = 30;
	policy->latency_columns = 7;
	policy->latency_shift = 1;
	policy->metrics_listeners.enable_listener = NULL;
	policy->metrics_listeners.snapshot_listener = NULL;
	policy->metrics_listeners.node_close_listener = NULL;
	policy->metrics_listeners.disable_listener = NULL;
	policy->metrics_listeners.udata = NULL;
}
