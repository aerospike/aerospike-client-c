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
#pragma once 

#include <aerospike/as_policy.h>

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/** 
 *	Resolve policy values from global and local policy.
 *
 *	@param p 		The policy to populate with resolved values
 *	@param global	An `as_policies` providing default (global) values.
 *	@param local	A policy providing local overrides of globals.
 *
 *	@return The resolved policy (p).
 */
as_policy_read * as_policy_read_resolve(as_policy_read * p, const as_policies * global, const as_policy_read * local);

/** 
 *	Resolve policy values from global and local policy.
 *
 *	@param p 		The policy to populate with resolved values
 *	@param global	An `as_policies` providing default (global) values.
 *	@param local	A policy providing local overrides of globals.
 *
 *	@return The resolved policy (p).
 */
as_policy_apply * as_policy_apply_resolve(as_policy_apply * p, const as_policies * global, const as_policy_apply * local);

/** 
 *	Resolve policy values from global and local policy.
 *
 *	@param p 		The policy to populate with resolved values
 *	@param global	An `as_policies` providing default (global) values.
 *	@param local	A policy providing local overrides of globals.
 *
 *	@return The resolved policy (p).
 */
as_policy_write * as_policy_write_resolve(as_policy_write * p, const as_policies * global, const as_policy_write * local);

/** 
 *	Resolve policy values from global and local policy.
 *
 *	@param p 		The policy to populate with resolved values
 *	@param global	An `as_policies` providing default (global) values.
 *	@param local	A policy providing local overrides of globals.
 *
 *	@return The resolved policy (p).
 */
as_policy_operate * as_policy_operate_resolve(as_policy_operate * p, const as_policies * global, const as_policy_operate * local);

/** 
 *	Resolve policy values from global and local policy.
 *
 *	@param p 		The policy to populate with resolved values
 *	@param global	An `as_policies` providing default (global) values.
 *	@param local	A policy providing local overrides of globals.
 *
 *	@return The resolved policy (p).
 */
as_policy_remove * as_policy_remove_resolve(as_policy_remove * p, const as_policies * global, const as_policy_remove * local);

/** 
 *	Resolve policy values from global and local policy.
 *
 *	@param p 		The policy to populate with resolved values
 *	@param global	An `as_policies` providing default (global) values.
 *	@param local	A policy providing local overrides of globals.
 *
 *	@return The resolved policy (p).
 */
as_policy_scan * as_policy_scan_resolve(as_policy_scan * p, const as_policies * global, const as_policy_scan * local);

/** 
 *	Resolve policy values from global and local policy.
 *
 *	@param p 		The policy to populate with resolved values
 *	@param global	An `as_policies` providing default (global) values.
 *	@param local	A policy providing local overrides of globals.
 *
 *	@return The resolved policy (p).
 */
as_policy_query * as_policy_query_resolve(as_policy_query * p, const as_policies * global, const as_policy_query * local);

/** 
 *	Resolve policy values from global and local policy.
 *
 *	@param p 		The policy to populate with resolved values
 *	@param global	An `as_policies` providing default (global) values.
 *	@param local	A policy providing local overrides of globals.
 *
 *	@return The resolved policy (p).
 */
as_policy_info * as_policy_info_resolve(as_policy_info * p, const as_policies * global, const as_policy_info * local);

