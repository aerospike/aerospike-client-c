/*
 * Copyright 2008-2020 Aerospike, Inc.
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
#include <aerospike/as_exp.h>
#include <aerospike/as_predexp.h>

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_policies*
as_policies_init(as_policies* p)
{
	as_policy_read_init(&p->read);
	as_policy_write_init(&p->write);
	as_policy_operate_init(&p->operate);
	as_policy_remove_init(&p->remove);
	as_policy_apply_init(&p->apply);
	as_policy_batch_init(&p->batch);
	as_policy_scan_init(&p->scan);
	as_policy_query_init(&p->query);
	as_policy_info_init(&p->info);
	as_policy_admin_init(&p->admin);
	as_policy_event_init(&p->event);
	return p;
}

void
as_policies_destroy(as_policies* p)
{
	as_exp_destroy(p->read.base.filter_exp);
	as_exp_destroy(p->write.base.filter_exp);
	as_exp_destroy(p->operate.base.filter_exp);
	as_exp_destroy(p->remove.base.filter_exp);
	as_exp_destroy(p->apply.base.filter_exp);
	as_exp_destroy(p->batch.base.filter_exp);
	as_exp_destroy(p->scan.base.filter_exp);
	as_exp_destroy(p->query.base.filter_exp);

	as_predexp_list_destroy(p->read.base.predexp);
	as_predexp_list_destroy(p->write.base.predexp);
	as_predexp_list_destroy(p->operate.base.predexp);
	as_predexp_list_destroy(p->remove.base.predexp);
	as_predexp_list_destroy(p->apply.base.predexp);
	as_predexp_list_destroy(p->batch.base.predexp);
	as_predexp_list_destroy(p->scan.base.predexp);
	as_predexp_list_destroy(p->query.base.predexp);
}
