/******************************************************************************
 *	Copyright 2008-2013 by Aerospike.
 *
 *	Permission is hereby granted, free of charge, to any person obtaining a copy 
 *	of this software and associated documentation files (the "Software"), to 
 *	deal in the Software without restriction, including without limitation the 
 *	rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 *	sell copies of the Software, and to permit persons to whom the Software is 
 *	furnished to do so, subject to the following conditions:
 *	
 *	The above copyright notice and this permission notice shall be included in 
 *	all copies or substantial portions of the Software.
 *	
 *	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 *	FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 *	IN THE SOFTWARE.
 *****************************************************************************/

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

