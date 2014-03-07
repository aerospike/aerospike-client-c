
#include <aerospike/as_policy.h>

#include <stdbool.h>
#include <stdint.h>

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/**
 *	Initialize as_policy_read to undefined values.
 */
as_policy_read * as_policy_read_init(as_policy_read * p) {
	p->timeout	= 0;
	p->key		= AS_POLICY_KEY_UNDEF;
	return p;
}

/**
 *	Initialize as_policy_apply to undefined values.
 */
as_policy_apply * as_policy_apply_init(as_policy_apply * p) {
	p->timeout	= 0;
	p->key		= AS_POLICY_KEY_UNDEF;
	return p;
}

/**
 *	Initialize as_policy_write to undefined values.
 */
as_policy_write * as_policy_write_init(as_policy_write * p) 
{
	p->timeout	= 0;
	p->retry	= AS_POLICY_RETRY_UNDEF;
	p->key		= AS_POLICY_KEY_UNDEF;
	p->gen		= AS_POLICY_GEN_UNDEF;
	p->exists	= AS_POLICY_EXISTS_UNDEF;
	return p;
}

/**
 *	Initialize as_policy_operate to undefined values.
 */
as_policy_operate * as_policy_operate_init(as_policy_operate * p)
{
	p->timeout		= 0;
	p->retry		= AS_POLICY_RETRY_UNDEF;
	p->key			= AS_POLICY_KEY_UNDEF;
	p->gen			= AS_POLICY_GEN_UNDEF;
	return p;
}

/**
 *	Initialize as_policy_remove to undefined values.
 */
as_policy_remove * as_policy_remove_init(as_policy_remove * p)
{
	p->timeout		= 0;
	p->generation	= 0;
	p->retry		= AS_POLICY_RETRY_UNDEF;
	p->key			= AS_POLICY_KEY_UNDEF;
	p->gen			= AS_POLICY_GEN_UNDEF;
	return p;
}

/**
 *	Initialize as_policy_scan to undefined values.
 */
as_policy_scan * as_policy_scan_init(as_policy_scan * p) 
{
	p->timeout					= 0;
	p->fail_on_cluster_change	= AS_POLICY_BOOL_UNDEF;
	return p;
}

/**
 *	Initialize as_policy_query to undefined values.
 */
as_policy_query * as_policy_query_init(as_policy_query * p)
{
	p->timeout = 0;
	return p;
}

/**
 *	Initialize as_policy_info to undefined values.
 */
as_policy_info * as_policy_info_init(as_policy_info * p)
{
	p->timeout		= 0;
	p->send_as_is	= AS_POLICY_BOOL_UNDEF;
	p->check_bounds	= AS_POLICY_BOOL_UNDEF;
	return p;
}

/**
 *	Initialize as_policy_batch to undefined values.
 */
as_policy_batch * as_policy_batch_init(as_policy_batch * p)
{
	p->timeout		= 0;
	return p;
}

/**
 *	Initialize as_policies to default values.
 */
as_policies * as_policies_init(as_policies * p)
{
	// defaults
	p->timeout	= AS_POLICY_TIMEOUT_DEFAULT;
	p->retry	= AS_POLICY_RETRY_DEFAULT;
	p->key		= AS_POLICY_KEY_DEFAULT;
	p->gen		= AS_POLICY_GEN_DEFAULT;
	p->exists	= AS_POLICY_EXISTS_DEFAULT;
	
	as_policy_write_init(&p->write);
	as_policy_read_init(&p->read);
	as_policy_operate_init(&p->operate);
	as_policy_remove_init(&p->remove);
	as_policy_apply_init(&p->apply);
	as_policy_scan_init(&p->scan);
	as_policy_query_init(&p->query);
	as_policy_info_init(&p->info);
	as_policy_batch_init(&p->batch);
	return p;
}

