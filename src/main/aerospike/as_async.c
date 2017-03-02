/*
 * Copyright 2008-2017 Aerospike, Inc.
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
#include <aerospike/as_async.h>

/******************************************************************************
 * GLOBALS
 *****************************************************************************/

extern uint32_t as_cluster_count;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

uint32_t
as_async_get_cluster_count()
{
	return ck_pr_load_32(&as_cluster_count);
}

uint32_t
as_async_get_pending(as_cluster* cluster)
{
	// Subtract one to account for extra as_cluster_create() pending reference count.
	// Maxint could be returned if pending is really zero, but that can only occur after
	// as_cluster_destroy() was called, so the result is garbage anyway.
	return ck_pr_load_32(&cluster->async_pending) - 1;
}

void
as_async_get_connections(as_cluster* cluster, uint32_t* async_conn, uint32_t* async_conn_pool)
{
	*async_conn = ck_pr_load_32(&cluster->async_conn_count);
	*async_conn_pool = ck_pr_load_32(&cluster->async_conn_pool);
}
