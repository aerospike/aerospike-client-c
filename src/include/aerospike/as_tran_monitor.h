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
#pragma once 

#include <aerospike/aerospike.h>
#include <aerospike/as_key.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Types
//---------------------------------

struct as_batch_s;
struct as_batch_records_s;
struct as_event_command;

//---------------------------------
// Sync Functions
//---------------------------------

as_status
as_tran_monitor_add_key(aerospike* as, const as_policy_base* cmd_policy, const as_key* cmd_key, as_error* err);

as_status
as_tran_monitor_add_keys_batch(aerospike* as, const as_policy_base* cmd_policy, const struct as_batch_s* batch, as_error* err);

as_status
as_tran_monitor_add_keys_records(aerospike* as, const as_policy_base* cmd_policy, struct as_batch_records_s* records, as_error* err);

//---------------------------------
// Async Functions
//---------------------------------

as_status
as_async_execute(aerospike* as, as_error* err, const as_policy_base* cmd_policy, const as_key* cmd_key, struct as_event_command* cmd);

#ifdef __cplusplus
} // end extern "C"
#endif
