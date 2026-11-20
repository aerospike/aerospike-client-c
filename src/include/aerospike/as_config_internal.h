/*
 * Copyright 2008-2026 Aerospike, Inc.
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

// Private to the client library and its tests. Not installed.

#include <aerospike/as_config.h>
#include <aerospike/as_discovery.h>
#include <aerospike/as_error.h>
#include <aerospike/as_map.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Macros
//---------------------------------

/**
 * @private
 * Number of built-in seed provider names and of built-in address translator names.
 */
#define AS_CONFIG_BUILTIN_PROVIDERS 3
#define AS_CONFIG_BUILTIN_TRANSLATORS 3

//---------------------------------
// Types
//---------------------------------

/**
 * @private
 * How a registration is built again at reconnect. build fills out, an as_seed_provider or an
 * as_address_translator zeroed by the caller, with an instance whose name the caller copies.
 * registry is the config's factory registry of that kind. destroy_state may be NULL.
 */
typedef struct as_plugin_recipe_s {
	as_status (*build)(as_error* err, const struct as_plugin_registry_s* registry, void* state,
		void* out);
	void (*destroy_state)(void* state);
	void* state;
	bool binding;
} as_plugin_recipe;

//---------------------------------
// Globals
//---------------------------------

/**
 * @private
 * Built-in names, in the order valid values are listed: static, dns, aws-tags.
 */
extern const char* const as_config_builtin_providers[AS_CONFIG_BUILTIN_PROVIDERS];

/**
 * @private
 * Built-in names, in the order valid values are listed: static-map, port-map, pass-through.
 */
extern const char* const as_config_builtin_translators[AS_CONFIG_BUILTIN_TRANSLATORS];

//---------------------------------
// Functions
//---------------------------------

/**
 * @private
 * Release the registered instances. A released registration without a recipe is marked
 * provider_released. With keep_recipes (sync aerospike_close()) the recipes and the factory
 * registries stay, so aerospike_connect() builds the instances again. Without it
 * (aerospike_close() of an unconnected async client, whose aerospike_destroy() never reaches
 * as_config_destroy()) the recipes and the registries are released too.
 */
AS_EXTERN void
as_config_release_plugins(as_config* config, bool keep_recipes);

/**
 * @private
 * Build the provider and the translator from their recipes when they were released. Called by
 * aerospike_connect() before the cluster is created.
 */
AS_EXTERN as_status
as_config_rebuild_plugins(as_config* config, as_error* err);

/**
 * @private
 * Register a seed provider with its recipe, or NULL for an instance that is not rebuilt. Takes
 * both; owned->name is already copied. A NULL owned resets to the default (recipe must then be
 * NULL). Cannot fail; the caller checks registration_locked.
 */
void
as_config_install_seed_provider(as_config* config, const as_seed_provider* owned,
	as_plugin_recipe* recipe);

/**
 * @private
 * Register an address translator with its recipe; same contract as
 * as_config_install_seed_provider().
 */
void
as_config_install_translator(as_config* config, const as_address_translator* owned,
	as_plugin_recipe* recipe);

/**
 * @private
 * Allocate a recipe. Takes state: on failure destroy_state(state) runs and NULL is returned.
 */
as_plugin_recipe*
as_plugin_recipe_create(as_status (*build)(as_error*, const struct as_plugin_registry_s*, void*,
	void*), void (*destroy_state)(void*), void* state);

/**
 * @private
 * Release a recipe and its state. NULL is ignored.
 */
void
as_plugin_recipe_destroy(as_plugin_recipe* recipe);

/**
 * @private
 * Recipe of a factory registration: the registry index and a reference to options, which must
 * be client-owned. NULL on allocation failure.
 */
as_plugin_recipe*
as_plugin_recipe_factory(uint32_t index, const as_map* options, bool translator);

/**
 * @private
 * Recipe of the built-in DNS seed provider with a deep copy of a valid dns. NULL on allocation
 * failure.
 */
as_plugin_recipe*
as_plugin_recipe_dns(const as_dns_seed_provider_config* dns);

/**
 * @private
 * Recipe of the built-in AWS EC2 seed provider with a deep copy of a valid aws. When aws has a
 * credentials hook, the recipe takes it over in a reference-counted holder: each instance the
 * recipe builds, and the instance passed to as_aws_recipe_adopt(), holds a reference, and the
 * last release runs the hook destroy. NULL on allocation failure, with the hook left to the
 * caller.
 */
as_plugin_recipe*
as_plugin_recipe_aws(const as_aws_seed_provider_config* aws);

/**
 * @private
 * Make provider, built from the same configuration by as_discovery_aws_seed_provider(), share
 * the hook holder of recipe instead of owning the hook.
 */
void
as_aws_recipe_adopt(as_plugin_recipe* recipe, as_seed_provider* provider);

/**
 * @private
 * Recipe of the built-in port-map translator with a copy of valid entries. NULL on allocation
 * failure.
 */
as_plugin_recipe*
as_plugin_recipe_port_map(const as_addr_map* entries, uint32_t size);

/**
 * @private
 * Recipe of the built-in pass-through translator. NULL on allocation failure.
 */
as_plugin_recipe*
as_plugin_recipe_pass_through(void);

/**
 * @private
 * Number of registered factories. registry may be NULL.
 */
uint32_t
as_plugin_registry_size(const struct as_plugin_registry_s* registry);

/**
 * @private
 * Name of the factory at index, in registration order.
 */
const char*
as_plugin_registry_name(const struct as_plugin_registry_s* registry, uint32_t index);

/**
 * @private
 * Index of the factory registered under name (case-sensitive), or -1. registry may be NULL.
 */
int32_t
as_plugin_registry_find(const struct as_plugin_registry_s* registry, const char* name);

/**
 * @private
 * Build a seed provider with the factory at index. On success out is a validated instance whose
 * name is copied. On failure nothing is left to release and err holds the reason only:
 * "factory failed: <message>", "factory returned an invalid provider" or an allocation failure.
 */
as_status
as_plugin_build_seed_provider(as_error* err, const struct as_plugin_registry_s* registry,
	uint32_t index, const as_map* options, as_seed_provider* out);

/**
 * @private
 * Build an address translator with the factory at index; same contract as
 * as_plugin_build_seed_provider().
 */
as_status
as_plugin_build_translator(as_error* err, const struct as_plugin_registry_s* registry,
	uint32_t index, const as_map* options, as_address_translator* out);

#ifdef __cplusplus
} // end extern "C"
#endif
