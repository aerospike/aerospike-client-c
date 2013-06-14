
#include <aerospike/as_policy.h>

#include <stdbool.h>
#include <stdint.h>

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * Initialize as_policy_write to default values.
 */
as_policy_write * as_policy_write_init(as_policy_write * p) 
{
	p->timeout = 1000;
	p->digest = AS_POLICY_DIGEST_DEFAULT;
	p->mode = AS_POLICY_WRITEMODE_ASYNC;
	p->gen = AS_POLICY_GEN_DEFAULT;
	return p;
}

/**
 * Initialize as_policy_read to default values.
 */
as_policy_read * as_policy_read_init(as_policy_read * p) {
	p->timeout = 1000;
	return p;
}

/**
 * Initialize as_policy_remote to default values.
 */
as_policy_remove * as_policy_remove_init(as_policy_remove * p)
{
	p->timeout = 1000;
	p->mode = AS_POLICY_WRITEMODE_ASYNC;
	p->gen = AS_POLICY_GEN_DEFAULT;
	return p;
}

/**
 * Initialize as_policy_scan to default values.
 */
as_policy_scan * as_policy_scan_init(as_policy_scan * p) 
{
	p->timeout = 1000;
	p->fail_on_cluster_change = true;
	return p;
}

/**
 * Initialize as_policy_query to default values.
 */
as_policy_query * as_policy_query_init(as_policy_query * p)
{
	p->timeout = 1000;
	return p;
}

/**
 * Initialize as_policy_info to default values.
 */
as_policy_info * as_policy_info_init(as_policy_info * p)
{
	p->timeout = 1000;
	p->send_as_is = true;
	p->check_bounds = true;
	return p;
}

/**
 * Initialize as_policies to default values.
 */
as_policies * as_policies_init(as_policies * p)
{
	p->timeout = 1000;
	as_policy_write_init(&p->write);
	as_policy_read_init(&p->read);
	as_policy_remove_init(&p->remove);
	as_policy_scan_init(&p->scan);
	as_policy_query_init(&p->query);
	as_policy_info_init(&p->info);
	return p;
}

