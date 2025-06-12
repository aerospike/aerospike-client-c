/*
 * Copyright 2008-2025 Aerospike, Inc.
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
#include <aerospike/as_metrics.h>
#include <aerospike/aerospike.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_config_file.h>
#include <aerospike/as_event.h>
#include <aerospike/as_node.h>
#include <aerospike/as_string_builder.h>

//---------------------------------
// Static Functions
//---------------------------------

static const as_metrics_policy*
as_metrics_policy_merge(aerospike* as, const as_metrics_policy* src, as_metrics_policy* mrg)
{
	if (!src) {
		as_config* config = aerospike_load_config(as);
		return &config->policies.metrics;
	}
	else if (as->config_bitmap) {
		uint8_t* bitmap = as->config_bitmap;
		as_config* config = aerospike_load_config(as);
		as_metrics_policy* cfg = &config->policies.metrics;

		mrg->labels = as_field_is_set(bitmap, AS_METRICS_LABELS)?
			cfg->labels : src->labels;
		mrg->latency_columns = as_field_is_set(bitmap, AS_METRICS_LATENCY_COLUMNS)?
			cfg->latency_columns : src->latency_columns;
		mrg->latency_shift = as_field_is_set(bitmap, AS_METRICS_LATENCY_SHIFT)?
			cfg->latency_shift : src->latency_shift;
		mrg->enable = as_field_is_set(bitmap, AS_METRICS_ENABLE)?
			cfg->enable : src->enable;

		mrg->metrics_listeners = src->metrics_listeners;
		as_strncpy(mrg->report_dir, src->report_dir, sizeof(mrg->report_dir));
		mrg->report_size_limit = src->report_size_limit;
		mrg->interval = src->interval;
		return mrg;
	}
	else {
		return src;
	}
}

//---------------------------------
// Functions
//---------------------------------

as_status
aerospike_enable_metrics(aerospike* as, as_error* err, const as_metrics_policy* policy)
{
	as_cluster* cluster = as->cluster;

	pthread_mutex_lock(&cluster->metrics_lock);

	if (as->config_bitmap &&
		as_field_is_set(as->config_bitmap, AS_METRICS_ENABLE) &&
		!as->config.policies.metrics.enable) {
		pthread_mutex_unlock(&cluster->metrics_lock);
		return as_error_set_message(err, AEROSPIKE_METRICS_CONFLICT,
			"Metrics can not be enabled by this function when metrics is disabled by dynamic configuration");
	}

	as_metrics_policy merged;
	policy = as_metrics_policy_merge(as, policy, &merged);

	as_status status = as_cluster_enable_metrics(err, cluster, policy);

	pthread_mutex_unlock(&cluster->metrics_lock);
	return status;
}

as_status
aerospike_disable_metrics(aerospike* as, as_error* err)
{
	as_cluster* cluster = as->cluster;

	pthread_mutex_lock(&cluster->metrics_lock);

	if (cluster->metrics_enabled && as->config_bitmap &&
		as_field_is_set(as->config_bitmap, AS_METRICS_ENABLE) &&
		as->config.policies.metrics.enable) {
		pthread_mutex_unlock(&cluster->metrics_lock);
		return as_error_set_message(err, AEROSPIKE_METRICS_CONFLICT,
			"Metrics can not be disabled by this function when metrics is enabled by dynamic configuration");
	}

	as_status status = as_cluster_disable_metrics(err, cluster);
	pthread_mutex_unlock(&cluster->metrics_lock);
	return status;
}

void
as_metrics_policy_init(as_metrics_policy* policy)
{
	policy->labels = NULL;
	policy->report_size_limit = 0;
	as_strncpy(policy->report_dir, ".", sizeof(policy->report_dir));
	policy->interval = 30;
	policy->latency_columns = 7;
	policy->latency_shift = 1;
	policy->metrics_listeners.enable_listener = NULL;
	policy->metrics_listeners.snapshot_listener = NULL;
	policy->metrics_listeners.node_close_listener = NULL;
	policy->metrics_listeners.disable_listener = NULL;
	policy->metrics_listeners.udata = NULL;
	policy->enable = false;
}

void
as_metrics_policy_destroy(as_metrics_policy* policy)
{
	as_metrics_policy_destroy_labels(policy);
}

void
as_metrics_labels_destroy(as_vector* labels)
{
	if (labels) {
		for (uint32_t i = 0; i < labels->size; i++) {
			as_metrics_label* label = as_vector_get(labels, i);
			cf_free(label->name);
			cf_free(label->value);
		}
		as_vector_destroy(labels);
	}
}

void
as_metrics_policy_destroy_labels(as_metrics_policy* policy)
{
	as_metrics_labels_destroy(policy->labels);
	policy->labels = NULL;
}

void
as_metrics_policy_set_labels(as_metrics_policy* policy, as_vector* labels)
{
	as_vector* old = policy->labels;
	policy->labels = labels;
	as_metrics_labels_destroy(old);
}

as_vector*
as_metrics_labels_copy(as_vector* labels)
{
	as_vector* list = NULL;

	if (labels) {
		list = as_vector_create(sizeof(as_metrics_label), labels->size);

		for (uint32_t i = 0; i < labels->size; i++) {
			as_metrics_label* label = as_vector_get(labels, i);

			as_metrics_label tmp;
			tmp.name = cf_strdup(label->name);
			tmp.value = cf_strdup(label->value);

			as_vector_append(list, &tmp);
		}
	}
	return list;
}

bool
as_metrics_labels_equal(as_vector* labels1, as_vector* labels2)
{
	if (labels1 == NULL) {
		return labels2 == NULL;
	}
	else if (labels2 == NULL) {
		return false;
	}

	if (labels1->size != labels2->size) {
		return false;
	}

	for (uint32_t i = 0; i < labels1->size; i++) {
		as_metrics_label* label1 = as_vector_get(labels1, i);
		as_metrics_label* label2 = as_vector_get(labels2, i);

		if (strcmp(label1->name, label2->name) != 0) {
			return false;
		}

		if (strcmp(label1->value, label2->value) != 0) {
			return false;
		}
	}
	return true;
}

void
as_metrics_policy_copy_labels(as_metrics_policy* policy, as_vector* labels)
{
	as_vector* list = as_metrics_labels_copy(labels);
	as_metrics_policy_set_labels(policy, list);
}

void
as_metrics_policy_add_label(as_metrics_policy* policy, const char* name, const char* value)
{
	if (!policy->labels) {
		policy->labels = as_vector_create(sizeof(as_metrics_label), 8);
	}

	as_metrics_label label;
	label.name = cf_strdup(name);
	label.value = cf_strdup(value);

	as_vector_append(policy->labels, &label);
}
