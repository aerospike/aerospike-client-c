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
#pragma once 

#include <aerospike/as_policy.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 *	TYPES
 *****************************************************************************/

/**
 *	@private
 *	Generic command policy.
 */
typedef struct as_command_policy_s {
	uint32_t socket_timeout;
	uint32_t total_timeout;
	uint32_t max_retries;
	uint32_t sleep_between_retries;
	bool retry_on_timeout;
} as_command_policy;

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

/**
 *	@private
 *	Convert write policy to generic policy.
 */
static inline void
as_command_policy_write(as_command_policy* trg, const as_policy_write* src)
{
	trg->socket_timeout = 0;
	trg->total_timeout = src->timeout;
	trg->max_retries = src->retry;
	trg->sleep_between_retries = src->sleep_between_retries;
	trg->retry_on_timeout = src->retry_on_timeout;
}

/**
 *	@private
 *	Convert read policy to generic policy.
 */
static inline void
as_command_policy_read(as_command_policy* trg, const as_policy_read* src)
{
	trg->socket_timeout = 0;
	trg->total_timeout = src->timeout;
	trg->max_retries = src->retry;
	trg->sleep_between_retries = src->sleep_between_retries;
	trg->retry_on_timeout = src->retry_on_timeout;
}

/**
 *	@private
 *	Convert operate policy to generic policy.
 */
static inline void
as_command_policy_operate(as_command_policy* trg, const as_policy_operate* src)
{
	trg->socket_timeout = 0;
	trg->total_timeout = src->timeout;
	trg->max_retries = src->retry;
	trg->sleep_between_retries = src->sleep_between_retries;
	trg->retry_on_timeout = src->retry_on_timeout;
}

/**
 *	@private
 *	Convert apply policy to generic policy.
 */
static inline void
as_command_policy_apply(as_command_policy* trg, const as_policy_apply* src)
{
	trg->socket_timeout = 0;
	trg->total_timeout = src->timeout;
	trg->max_retries = src->retry;
	trg->sleep_between_retries = src->sleep_between_retries;
	trg->retry_on_timeout = src->retry_on_timeout;
}

/**
 *	@private
 *	Convert remove policy to generic policy.
 */
static inline void
as_command_policy_remove(as_command_policy* trg, const as_policy_remove* src)
{
	trg->socket_timeout = 0;
	trg->total_timeout = src->timeout;
	trg->max_retries = src->retry;
	trg->sleep_between_retries = src->sleep_between_retries;
	trg->retry_on_timeout = src->retry_on_timeout;
}

/**
 *	@private
 *	Convert batch policy to generic policy.
 */
static inline void
as_command_policy_batch(as_command_policy* trg, const as_policy_batch* src)
{
	trg->socket_timeout = 0;
	trg->total_timeout = src->timeout;
	trg->max_retries = src->retry;
	trg->sleep_between_retries = src->sleep_between_retries;
	trg->retry_on_timeout = src->retry_on_timeout;
}

/**
 *	@private
 *	Convert scan policy to generic policy.
 */
static inline void
as_command_policy_scan(as_command_policy* trg, const as_policy_scan* src)
{
	trg->socket_timeout = src->socket_timeout;
	trg->total_timeout = src->timeout;
	trg->max_retries = 0;
	trg->sleep_between_retries = 0;
	trg->retry_on_timeout = false;
}

/**
 *	@private
 *	Convert query policy to generic policy.
 */
static inline void
as_command_policy_query(as_command_policy* trg, const as_policy_query* src)
{
	trg->socket_timeout = src->socket_timeout;
	trg->total_timeout = src->timeout;
	trg->max_retries = 0;
	trg->sleep_between_retries = 0;
	trg->retry_on_timeout = false;
}

/**
 *	@private
 *	Convert query policy to generic policy.
 */
static inline void
as_command_policy_query_background(as_command_policy* trg, uint32_t timeout)
{
	trg->socket_timeout = 0;
	trg->total_timeout = timeout;
	trg->max_retries = 0;
	trg->sleep_between_retries = 0;
	trg->retry_on_timeout = false;
}

#ifdef __cplusplus
} // end extern "C"
#endif
