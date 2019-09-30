/*
 * Copyright 2008-2019 Aerospike, Inc.
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
#include <aerospike/as_event.h>
#include <aerospike/as_event_internal.h>

/******************************************************************************
 * EVENT_LIB NOT DEFINED FUNCTIONS
 *****************************************************************************/

#if ! AS_EVENT_LIB_DEFINED

bool
as_event_create_loop(as_event_loop* event_loop)
{
	return false;
}

void
as_event_register_external_loop(as_event_loop* event_loop)
{
}

bool
as_event_execute(as_event_loop* event_loop, as_event_executable executable, void* udata)
{
	return false;
}

void
as_event_command_write_start(as_event_command* cmd)
{
}

void
as_event_connect(as_event_command* cmd, as_async_conn_pool* pool)
{
}

void
as_event_node_destroy(as_node* node)
{
}

#endif
