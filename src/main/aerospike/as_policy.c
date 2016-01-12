/*
 * Copyright 2008-2016 Aerospike, Inc.
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
#include <aerospike/as_policy.h>
#include <stdbool.h>
#include <stdint.h>

/******************************************************************************
 *	MACROS
 *****************************************************************************/

#define as_policy_resolve(__local, __global) \
	if ((int)__local == -1) {\
		__local = __global;\
	}

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

as_policies*
as_policies_init(as_policies* p)
{
	// Global defaults
	p->timeout = AS_POLICY_TIMEOUT_DEFAULT;
	p->retry = AS_POLICY_RETRY_DEFAULT;
	p->key = AS_POLICY_KEY_DEFAULT;
	p->gen = AS_POLICY_GEN_DEFAULT;
	p->exists = AS_POLICY_EXISTS_DEFAULT;
	p->replica = AS_POLICY_REPLICA_DEFAULT;
	p->consistency_level = AS_POLICY_CONSISTENCY_LEVEL_DEFAULT;
	p->commit_level = AS_POLICY_COMMIT_LEVEL_DEFAULT;
	
	// Set local defaults to undefined.
	// Undefined variables will be set to global defaults in as_policies_resolve().
	p->read.timeout = -1;
	p->read.retry = -1;
	p->read.key = -1;
	p->read.replica = -1;
	p->read.consistency_level = -1;
	p->read.deserialize = true;

	p->write.timeout = -1;
	p->write.retry = -1;
	p->write.compression_threshold = AS_POLICY_COMPRESSION_THRESHOLD_DEFAULT;
	p->write.key = -1;
	p->write.gen = -1;
	p->write.exists = -1;
	p->write.commit_level = -1;

	p->operate.timeout = -1;
	p->operate.retry = -1;
	p->operate.key = -1;
	p->operate.gen = -1;
	p->operate.replica = -1;
	p->operate.consistency_level = -1;
	p->operate.commit_level = -1;
	p->operate.deserialize = true;

	p->remove.timeout = -1;
	p->remove.retry = -1;
	p->remove.key = -1;
	p->remove.gen = -1;
	p->remove.generation = 0;
	p->remove.commit_level = -1;

	p->apply.timeout = -1;
	p->apply.key = -1;
	p->apply.commit_level = -1;
	p->apply.ttl = 0;	// Set to AS_RECORD_DEFAULT_TTL. TTL does not go through as_policies_resolve().

	as_policy_info_init(&p->info);
	p->info.timeout = -1;

	as_policy_batch_init(&p->batch);
	p->batch.timeout = -1;

	p->admin.timeout = -1;

	// Scan timeout should not be tied to global timeout.
	as_policy_scan_init(&p->scan);

	// Query timeout should not be tied to global timeout.
	as_policy_query_init(&p->query);

	return p;
}

void
as_policies_resolve(as_policies* p)
{
	as_policy_resolve(p->read.timeout, p->timeout);
	as_policy_resolve(p->read.retry, p->retry);
	as_policy_resolve(p->read.key, p->key);
	as_policy_resolve(p->read.replica, p->replica);
	as_policy_resolve(p->read.consistency_level, p->consistency_level);

	as_policy_resolve(p->write.timeout, p->timeout);
	as_policy_resolve(p->write.retry, p->retry);
	as_policy_resolve(p->write.key, p->key);
	as_policy_resolve(p->write.gen, p->gen);
	as_policy_resolve(p->write.exists, p->exists);
	as_policy_resolve(p->write.commit_level, p->commit_level);

	as_policy_resolve(p->operate.timeout, p->timeout);
	as_policy_resolve(p->operate.retry, p->retry);
	as_policy_resolve(p->operate.key, p->key);
	as_policy_resolve(p->operate.gen, p->gen);
	as_policy_resolve(p->operate.consistency_level, p->consistency_level);
	as_policy_resolve(p->operate.commit_level, p->commit_level);

	as_policy_resolve(p->remove.timeout, p->timeout);
	as_policy_resolve(p->remove.retry, p->retry);
	as_policy_resolve(p->remove.key, p->key);
	as_policy_resolve(p->remove.gen, p->gen);
	as_policy_resolve(p->remove.commit_level, p->commit_level);

	as_policy_resolve(p->apply.timeout, p->timeout);
	as_policy_resolve(p->apply.key, p->key);
	as_policy_resolve(p->apply.commit_level, p->commit_level);

	as_policy_resolve(p->info.timeout, p->timeout);

	as_policy_resolve(p->batch.timeout, p->timeout);

	as_policy_resolve(p->admin.timeout, p->timeout);
}
