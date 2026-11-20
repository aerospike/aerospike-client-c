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
#include <aerospike/as_config.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_config_internal.h>
#include <aerospike/as_discovery.h>
#include <aerospike/as_msgpack_serializer.h>
#include <aerospike/as_orderedmap.h>
#include <aerospike/as_password.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_serializer.h>
#include <aerospike/as_string.h>
#include <aerospike/mod_lua_config.h>
#include <ctype.h>
#include <stdlib.h>
#include <string.h>

//---------------------------------
// Types
//---------------------------------

typedef struct as_plugin_entry_s {
	char* name;
	union {
		as_seed_provider_factory provider;
		as_address_translator_factory translator;
	} factory;
	void (*destroy)(void* udata);
	void* udata;
} as_plugin_entry;

// Entries are never removed, so an index stays valid while the vector reallocates.
struct as_plugin_registry_s {
	as_vector entries; // as_plugin_entry
};

//---------------------------------
// Globals
//---------------------------------

const char* const as_config_builtin_providers[AS_CONFIG_BUILTIN_PROVIDERS] = {
	"static", "dns", "aws-tags"
};

const char* const as_config_builtin_translators[AS_CONFIG_BUILTIN_TRANSLATORS] = {
	"static-map", "port-map", "pass-through"
};

//---------------------------------
// Static Functions
//---------------------------------

static bool
provider_is_valid(const as_seed_provider* provider)
{
	return provider && provider->size >= sizeof(as_seed_provider) &&
		provider->api_version == AS_DISCOVERY_API_VERSION && provider->name &&
		provider->name[0] && provider->refresh && provider->needs_periodic_refresh;
}

static bool
translator_is_valid(const as_address_translator* translator)
{
	return translator && translator->size >= sizeof(as_address_translator) &&
		translator->api_version == AS_DISCOVERY_API_VERSION && translator->name &&
		translator->name[0] && translator->translate;
}

static void
destroy_provider(as_discovery_config* discovery)
{
	if (discovery->provider_registered) {
		as_seed_provider* p = &discovery->provider;

		// A refresh still running on a discovery thread destroys udata when it returns.
		if (p->destroy &&
			!as_seed_inflight_defer_destroy(discovery->inflight, p->destroy, p->udata)) {
			p->destroy(p->udata);
		}
		cf_free((void*)p->name);
	}
	memset(&discovery->provider, 0, sizeof(as_seed_provider));
	discovery->provider_registered = false;
	discovery->binding_provider = false;
}

static void
destroy_translator(as_translation_config* translation)
{
	if (translation->provider_registered) {
		if (translation->provider.destroy) {
			translation->provider.destroy(translation->provider.udata);
		}
		cf_free((void*)translation->provider.name);
	}
	memset(&translation->provider, 0, sizeof(as_address_translator));
	translation->provider_registered = false;
}

static void
replace_provider(as_discovery_config* discovery, const as_seed_provider* copy)
{
	if (discovery->provider_registered && discovery->provider.udata &&
		discovery->provider.udata == copy->udata) {
		// Same state now owned by the new registration.
		cf_free((void*)discovery->provider.name);
		discovery->binding_provider = false;
	}
	else {
		destroy_provider(discovery);
	}
	discovery->provider = *copy;
	discovery->provider_registered = true;
	discovery->provider_released = false;
}

static void
replace_translator(as_translation_config* translation, const as_address_translator* copy)
{
	if (translation->provider_registered && translation->provider.udata &&
		translation->provider.udata == copy->udata) {
		// Same state now owned by the new registration.
		cf_free((void*)translation->provider.name);
	}
	else {
		destroy_translator(translation);
	}
	translation->provider = *copy;
	translation->provider_registered = true;
	translation->provider_released = false;
}

static bool
binding_periodic(void* udata)
{
	(void)udata;
	return true;
}

static bool
binding_not_periodic(void* udata)
{
	(void)udata;
	return false;
}

static void
registry_destroy(struct as_plugin_registry_s** registry)
{
	struct as_plugin_registry_s* r = *registry;

	if (!r) {
		return;
	}

	for (uint32_t i = 0; i < r->entries.size; i++) {
		as_plugin_entry* entry = as_vector_get(&r->entries, i);

		if (entry->destroy) {
			entry->destroy(entry->udata);
		}
		cf_free(entry->name);
	}
	as_vector_destroy(&r->entries);
	cf_free(r);
	*registry = NULL;
}

static bool
is_builtin(const char* name, const char* const* names, uint32_t size)
{
	for (uint32_t i = 0; i < size; i++) {
		if (strcmp(name, names[i]) == 0) {
			return true;
		}
	}
	return false;
}

static as_status
registry_add(as_config* config, struct as_plugin_registry_s** registry, const char* type,
	const char* name, bool has_factory, const char* const* builtins, uint32_t builtins_size,
	const as_plugin_entry* entry, as_error* err)
{
	if (!config) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid %s registration", type);
	}

	if (!name || !name[0]) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid %s name: %s", type,
			name ? name : "(null)");
	}

	if (is_builtin(name, builtins, builtins_size)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid %s name '%s': built-in name",
			type, name);
	}

	if (as_plugin_registry_find(*registry, name) >= 0) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid %s name '%s': already registered",
			type, name);
	}

	if (!has_factory) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid %s factory for '%s': null",
			type, name);
	}

	if (config->discovery.registration_locked) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
			"Invalid %s name '%s': cannot register while the client is connected", type, name);
	}

	as_plugin_entry copy = *entry;
	copy.name = cf_strdup(name);

	if (!copy.name) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to copy %s name", type);
	}

	if (!*registry) {
		struct as_plugin_registry_s* r = cf_malloc(sizeof(struct as_plugin_registry_s));

		if (!r) {
			cf_free(copy.name);
			return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to allocate %s registry", type);
		}
		as_vector_init(&r->entries, sizeof(as_plugin_entry), 4);
		*registry = r;
	}
	as_vector_append(&(*registry)->entries, &copy);
	return AEROSPIKE_OK;
}

// Validate a selection from code and return the registry index of name.
static as_status
select_check(as_config* config, const struct as_plugin_registry_s* registry, const char* type,
	const char* name, const char* const* builtins, uint32_t builtins_size, const char* locked,
	int32_t* index, as_error* err)
{
	if (!config) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid %s", type);
	}

	if (!name || !name[0]) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid %s name: %s", type,
			name ? name : "(null)");
	}

	if (config->discovery.registration_locked) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, locked);
	}

	if (is_builtin(name, builtins, builtins_size)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
			"Invalid %s name '%s': built-in name, use its own setter", type, name);
	}

	*index = as_plugin_registry_find(registry, name);

	if (*index < 0) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid %s name '%s': not registered",
			type, name);
	}
	return AEROSPIKE_OK;
}

// Deep copy options into a client-owned map through a msgpack round trip.
static as_status
copy_options(const as_map* options, as_map** out, as_error* err)
{
	if (!options) {
		*out = (as_map*)as_orderedmap_new(1);

		if (!*out) {
			return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to allocate options");
		}
		return AEROSPIKE_OK;
	}

	as_serializer ser;
	as_msgpack_init(&ser);

	as_buffer buffer;
	as_buffer_init(&buffer);

	as_val* copy = NULL;
	int rc = as_serializer_serialize(&ser, (as_val*)options, &buffer);

	if (rc == 0) {
		rc = as_serializer_deserialize(&ser, &buffer, &copy);
	}
	as_buffer_destroy(&buffer);
	as_serializer_destroy(&ser);

	if (rc != 0 || as_val_type(copy) != AS_MAP) {
		as_val_destroy(copy);
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "Invalid options: cannot copy");
	}
	*out = (as_map*)copy;
	return AEROSPIKE_OK;
}

static as_status
factory_failed(as_error* err, as_status status, const as_error* local)
{
	return as_error_update(err, status, "factory failed: %s",
		local->message[0] ? local->message : as_error_string(status));
}

// Copy the name of a built instance. On failure the instance is destroyed.
static bool
own_provider_name(as_seed_provider* provider)
{
	provider->name = cf_strdup(provider->name);

	if (!provider->name) {
		if (provider->destroy) {
			provider->destroy(provider->udata);
		}
		return false;
	}
	return true;
}

static bool
own_translator_name(as_address_translator* translator)
{
	translator->name = cf_strdup(translator->name);

	if (!translator->name) {
		if (translator->destroy) {
			translator->destroy(translator->udata);
		}
		return false;
	}
	return true;
}

// Call the factory at index and validate its result, whose name is not copied.
static as_status
call_provider_factory(as_error* err, const struct as_plugin_registry_s* registry, uint32_t index,
	const as_map* options, as_seed_provider* out)
{
	const as_plugin_entry* entry = as_vector_get((as_vector*)&registry->entries, index);
	memset(out, 0, sizeof(as_seed_provider));

	as_error local;
	as_error_init(&local);
	as_status status = entry->factory.provider(&local, options, out, entry->udata);

	if (status != AEROSPIKE_OK) {
		return factory_failed(err, status, &local);
	}

	if (!provider_is_valid(out)) {
		if (out->destroy) {
			out->destroy(out->udata);
		}
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"factory returned an invalid provider");
	}
	return AEROSPIKE_OK;
}

static as_status
call_translator_factory(as_error* err, const struct as_plugin_registry_s* registry,
	uint32_t index, const as_map* options, as_address_translator* out)
{
	const as_plugin_entry* entry = as_vector_get((as_vector*)&registry->entries, index);
	memset(out, 0, sizeof(as_address_translator));

	as_error local;
	as_error_init(&local);
	as_status status = entry->factory.translator(&local, options, out, entry->udata);

	if (status != AEROSPIKE_OK) {
		return factory_failed(err, status, &local);
	}

	if (!translator_is_valid(out)) {
		if (out->destroy) {
			out->destroy(out->udata);
		}
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"factory returned an invalid translator");
	}
	return AEROSPIKE_OK;
}

static void
clear_recipe(struct as_plugin_recipe_s** recipe)
{
	as_plugin_recipe_destroy(*recipe);
	*recipe = NULL;
}

//---------------------------------
// Static Functions: recipes
//---------------------------------

// A factory recipe stores the registry index, which stays valid while the registry vector
// reallocates, and a reference to client-owned options.
typedef struct factory_state_s {
	uint32_t index;
	as_map* options;
} factory_state;

static void
factory_state_destroy(void* state)
{
	factory_state* s = state;
	as_map_destroy(s->options);
	cf_free(s);
}

static as_status
factory_build_provider(as_error* err, const struct as_plugin_registry_s* registry, void* state,
	void* out)
{
	factory_state* s = state;
	return call_provider_factory(err, registry, s->index, s->options, out);
}

static as_status
factory_build_translator(as_error* err, const struct as_plugin_registry_s* registry,
	void* state, void* out)
{
	factory_state* s = state;
	return call_translator_factory(err, registry, s->index, s->options, out);
}

static void
free_strings(const char* const* strings, uint32_t size)
{
	if (!strings) {
		return;
	}

	for (uint32_t i = 0; i < size; i++) {
		cf_free((void*)strings[i]);
	}
	cf_free((void*)strings);
}

// Copy size strings into *out, NULL when there are none. On failure *out holds what was copied.
static bool
copy_strings(const char* const* src, uint32_t size, const char* const** out)
{
	*out = NULL;

	if (!src || size == 0) {
		return true;
	}

	const char** copy = cf_calloc(size, sizeof(char*));

	if (!copy) {
		return false;
	}
	*out = copy;

	for (uint32_t i = 0; i < size; i++) {
		if (!(copy[i] = cf_strdup(src[i]))) {
			return false;
		}
	}
	return true;
}

static void
dns_config_destroy(void* state)
{
	as_dns_seed_provider_config* c = state;
	free_strings(c->names, c->names_size);
	free_strings(c->servers, c->servers_size);
	cf_free((void*)c->tls_name);
	cf_free(c);
}

static as_dns_seed_provider_config*
dns_config_copy(const as_dns_seed_provider_config* src)
{
	as_dns_seed_provider_config* c = cf_malloc(sizeof(as_dns_seed_provider_config));

	if (!c) {
		return NULL;
	}
	*c = *src;
	c->names = NULL;
	c->servers = NULL;
	c->tls_name = NULL;

	bool ok = copy_strings(src->names, src->names_size, &c->names) &&
		copy_strings(src->servers, src->servers_size, &c->servers) &&
		(!src->tls_name || (c->tls_name = cf_strdup(src->tls_name)));

	if (!ok) {
		dns_config_destroy(c);
		return NULL;
	}
	return c;
}

static as_status
dns_build(as_error* err, const struct as_plugin_registry_s* registry, void* state, void* out)
{
	(void)registry;
	return as_discovery_dns_seed_provider(err, state, out);
}

typedef struct port_map_state_s {
	as_addr_map* entries;
	uint32_t size;
} port_map_state;

static void
port_map_state_destroy(void* state)
{
	port_map_state* s = state;

	if (s->entries) {
		for (uint32_t i = 0; i < s->size; i++) {
			cf_free(s->entries[i].orig);
			cf_free(s->entries[i].alt);
		}
		cf_free(s->entries);
	}
	cf_free(s);
}

static as_status
port_map_build(as_error* err, const struct as_plugin_registry_s* registry, void* state, void* out)
{
	(void)registry;
	port_map_state* s = state;
	return as_discovery_port_map_translator(err, s->entries, s->size, out);
}

static as_status
pass_through_build(as_error* err, const struct as_plugin_registry_s* registry, void* state,
	void* out)
{
	(void)err;
	(void)registry;
	(void)state;
	as_discovery_pass_through_translator(out);
	return AEROSPIKE_OK;
}

//---------------------------------
// Functions
//---------------------------------

as_config*
as_config_init(as_config* c)
{
	c->hosts = NULL;
	memset(c->user, 0, sizeof(c->user));
	memset(c->password, 0, sizeof(c->password));
	c->cluster_name = NULL;
	c->app_id = NULL;
	c->event_callback = NULL;
	c->event_callback_udata = NULL;
	c->ip_map = NULL;
	c->ip_map_size = 0;
	memset(&c->discovery, 0, sizeof(as_discovery_config));
	c->discovery.merge_policy = AS_SEED_MERGE;
	c->discovery.execution_mode = AS_DISCOVERY_EXECUTION_THREAD;
	c->discovery.refresh_interval_ms = 30000;
	c->discovery.refresh_timeout_ms = 10000;
	c->discovery.tend_refresh_deadline_ms = 100;
	memset(&c->translation, 0, sizeof(as_translation_config));
	c->translation.on_miss = AS_TRANSLATION_PASS_THROUGH;
	c->min_conns_per_node = 0;
	c->max_conns_per_node = 100;
	c->async_min_conns_per_node = 0;
	c->async_max_conns_per_node = 100;
	c->pipe_max_conns_per_node = 64;
	c->conn_pools_per_node = 1;
	c->conn_timeout_ms = 1000;
	c->login_timeout_ms = 5000;
	c->max_socket_idle = 0;
	c->max_error_rate = 100;
	c->error_rate_window = 1;
	c->tender_interval = 1000;
	c->thread_pool_size = 16;
	c->tend_thread_cpu = -1;
	as_policies_init(&c->policies);
	c->config_provider.path = NULL;
	c->config_provider.interval = AS_CONFIG_PROVIDER_INTERVAL_DEFAULT;
	as_config_lua_init(&c->lua);
	memset(&c->tls, 0, sizeof(as_config_tls));
	c->auth_mode = AS_AUTH_INTERNAL;
	c->fail_if_not_connected = true;
	c->use_services_alternate = false;
	c->force_single_node = false;
	c->rack_aware = false;
	c->rack_id = 0;
	c->rack_ids = NULL;
	c->use_shm = false;
	c->shm_key = 0xAC000000;
	c->shm_max_nodes = 16;
	c->shm_max_namespaces = 8;
	c->shm_takeover_threshold_sec = 30;
	return c;
}

void
as_config_destroy(as_config* config) {
	// Private: For internal use only.
	// Never call from applications because ownership of config fields is transferred
	// to aerospike in aerospike_init() or aerospike_new().
	// Only exception is ip_map which is left alone for legacy reasons.
	as_vector* hosts = config->hosts;
	
	if (hosts) {
		for (uint32_t i = 0; i < hosts->size; i++) {
			as_host* host = as_vector_get(hosts, i);
			as_host_destroy(host);
		}
		as_vector_destroy(hosts);
	}

	if (config->rack_ids) {
		as_vector_destroy(config->rack_ids);
	}

	if (config->cluster_name) {
		cf_free(config->cluster_name);
	}

	if (config->app_id) {
		cf_free(config->app_id);
	}

	if (config->config_provider.path) {
		cf_free(config->config_provider.path);
	}

	// Instances first, then the recipes that build them, then the factories the recipes call.
	destroy_provider(&config->discovery);
	as_seed_inflight_release(config->discovery.inflight);
	config->discovery.inflight = NULL;
	destroy_translator(&config->translation);
	clear_recipe(&config->discovery.recipe);
	clear_recipe(&config->translation.recipe);
	registry_destroy(&config->discovery.registry);
	registry_destroy(&config->translation.registry);

	as_translation_config* translation = &config->translation;

	if (translation->file_ip_map) {
		for (uint32_t i = 0; i < translation->file_ip_map_size; i++) {
			cf_free(translation->file_ip_map[i].orig);
			cf_free(translation->file_ip_map[i].alt);
		}

		if (config->ip_map == translation->file_ip_map) {
			config->ip_map = NULL;
			config->ip_map_size = 0;
		}
		cf_free(translation->file_ip_map);
		translation->file_ip_map = NULL;
		translation->file_ip_map_size = 0;
	}

	as_policies_destroy(&config->policies);

	as_config_tls* tls = &config->tls;

	if (tls->cafile) {
		cf_free(tls->cafile);
	}

	if (tls->castring) {
		cf_free(tls->castring);
	}

	if (tls->capath) {
		cf_free(tls->capath);
	}

	if (tls->protocols) {
		cf_free(tls->protocols);
	}

	if (tls->cipher_suite) {
		cf_free(tls->cipher_suite);
	}

	if (tls->cert_blacklist) {
		cf_free(tls->cert_blacklist);
	}

	if (tls->keyfile) {
		cf_free(tls->keyfile);
	}

	if (tls->keyfile_pw) {
		cf_free(tls->keyfile_pw);
	}

	if (tls->keystring) {
		cf_free(tls->keystring);
	}

	if (tls->certfile) {
		cf_free(tls->certfile);
	}

	if (tls->certstring) {
		cf_free(tls->certstring);
	}
}

void
as_config_release_plugins(as_config* config, bool keep_recipes)
{
	as_discovery_config* d = &config->discovery;
	as_translation_config* t = &config->translation;
	bool provider = d->provider_registered || d->recipe;
	bool translator = t->provider_registered || t->recipe;

	// The provider destroy is deferred while a refresh of the previous connection runs.
	destroy_provider(d);
	destroy_translator(t);

	if (!keep_recipes) {
		clear_recipe(&d->recipe);
		clear_recipe(&t->recipe);
		registry_destroy(&d->registry);
		registry_destroy(&t->registry);
	}

	if (provider && !d->recipe) {
		d->provider_released = true;
	}

	if (translator && !t->recipe) {
		t->provider_released = true;
	}
}

uint32_t
as_plugin_registry_size(const struct as_plugin_registry_s* registry)
{
	return registry ? registry->entries.size : 0;
}

const char*
as_plugin_registry_name(const struct as_plugin_registry_s* registry, uint32_t index)
{
	const as_plugin_entry* entry = as_vector_get((as_vector*)&registry->entries, index);
	return entry->name;
}

int32_t
as_plugin_registry_find(const struct as_plugin_registry_s* registry, const char* name)
{
	if (!registry) {
		return -1;
	}

	for (uint32_t i = 0; i < registry->entries.size; i++) {
		const as_plugin_entry* entry = as_vector_get((as_vector*)&registry->entries, i);

		if (strcmp(entry->name, name) == 0) {
			return (int32_t)i;
		}
	}
	return -1;
}

as_status
as_plugin_build_seed_provider(as_error* err, const struct as_plugin_registry_s* registry,
	uint32_t index, const as_map* options, as_seed_provider* out)
{
	as_status status = call_provider_factory(err, registry, index, options, out);

	if (status == AEROSPIKE_OK && !own_provider_name(out)) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to copy seed provider name");
	}
	return status;
}

as_status
as_plugin_build_translator(as_error* err, const struct as_plugin_registry_s* registry,
	uint32_t index, const as_map* options, as_address_translator* out)
{
	as_status status = call_translator_factory(err, registry, index, options, out);

	if (status == AEROSPIKE_OK && !own_translator_name(out)) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT,
			"Failed to copy address translator name");
	}
	return status;
}

as_plugin_recipe*
as_plugin_recipe_create(as_status (*build)(as_error*, const struct as_plugin_registry_s*, void*,
	void*), void (*destroy_state)(void*), void* state)
{
	as_plugin_recipe* recipe = cf_malloc(sizeof(as_plugin_recipe));

	if (!recipe) {
		if (destroy_state) {
			destroy_state(state);
		}
		return NULL;
	}
	recipe->build = build;
	recipe->destroy_state = destroy_state;
	recipe->state = state;
	return recipe;
}

void
as_plugin_recipe_destroy(as_plugin_recipe* recipe)
{
	if (!recipe) {
		return;
	}

	if (recipe->destroy_state) {
		recipe->destroy_state(recipe->state);
	}
	cf_free(recipe);
}

as_plugin_recipe*
as_plugin_recipe_factory(uint32_t index, const as_map* options, bool translator)
{
	factory_state* state = cf_malloc(sizeof(factory_state));

	if (!state) {
		return NULL;
	}
	state->index = index;
	state->options = (as_map*)as_val_reserve((as_val*)options);
	return as_plugin_recipe_create(translator ? factory_build_translator : factory_build_provider,
		factory_state_destroy, state);
}

as_plugin_recipe*
as_plugin_recipe_dns(const as_dns_seed_provider_config* dns)
{
	as_dns_seed_provider_config* copy = dns_config_copy(dns);

	if (!copy) {
		return NULL;
	}
	return as_plugin_recipe_create(dns_build, dns_config_destroy, copy);
}

as_plugin_recipe*
as_plugin_recipe_port_map(const as_addr_map* entries, uint32_t size)
{
	port_map_state* state = cf_calloc(1, sizeof(port_map_state));

	if (!state) {
		return NULL;
	}

	if (size > 0) {
		state->entries = cf_calloc(size, sizeof(as_addr_map));

		if (!state->entries) {
			cf_free(state);
			return NULL;
		}
		state->size = size;

		for (uint32_t i = 0; i < size; i++) {
			state->entries[i].orig = cf_strdup(entries[i].orig);
			state->entries[i].alt = cf_strdup(entries[i].alt);

			if (!state->entries[i].orig || !state->entries[i].alt) {
				port_map_state_destroy(state);
				return NULL;
			}
		}
	}
	return as_plugin_recipe_create(port_map_build, port_map_state_destroy, state);
}

as_plugin_recipe*
as_plugin_recipe_pass_through(void)
{
	return as_plugin_recipe_create(pass_through_build, NULL, NULL);
}

void
as_config_install_seed_provider(as_config* config, const as_seed_provider* owned,
	as_plugin_recipe* recipe)
{
	if (owned) {
		replace_provider(&config->discovery, owned);
	}
	else {
		destroy_provider(&config->discovery);
	}
	clear_recipe(&config->discovery.recipe);
	config->discovery.recipe = recipe;
}

void
as_config_install_translator(as_config* config, const as_address_translator* owned,
	as_plugin_recipe* recipe)
{
	if (owned) {
		replace_translator(&config->translation, owned);
	}
	else {
		destroy_translator(&config->translation);
	}
	clear_recipe(&config->translation.recipe);
	config->translation.recipe = recipe;
}

as_status
as_config_rebuild_plugins(as_config* config, as_error* err)
{
	as_discovery_config* d = &config->discovery;
	as_translation_config* t = &config->translation;
	as_error local;
	as_error_init(&local);

	if (!d->provider_registered && d->recipe) {
		as_seed_provider provider;
		memset(&provider, 0, sizeof(provider));
		as_status status = d->recipe->build(&local, d->registry, d->recipe->state, &provider);

		if (status != AEROSPIKE_OK) {
			return as_error_update(err, status, "Failed to rebuild the seed provider: %s",
				local.message);
		}

		if (!own_provider_name(&provider)) {
			return as_error_set_message(err, AEROSPIKE_ERR_CLIENT,
				"Failed to copy seed provider name");
		}
		replace_provider(d, &provider);
	}

	if (!t->provider_registered && t->recipe) {
		as_address_translator translator;
		memset(&translator, 0, sizeof(translator));
		as_status status = t->recipe->build(&local, t->registry, t->recipe->state, &translator);

		if (status != AEROSPIKE_OK) {
			return as_error_update(err, status, "Failed to rebuild the address translator: %s",
				local.message);
		}

		if (!own_translator_name(&translator)) {
			return as_error_set_message(err, AEROSPIKE_ERR_CLIENT,
				"Failed to copy address translator name");
		}
		replace_translator(t, &translator);
	}
	return AEROSPIKE_OK;
}

as_status
as_config_set_seed_provider(as_config* config, const as_seed_provider* provider, as_error* err)
{
	if (!config || !provider_is_valid(provider)) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "Invalid seed provider");
	}

	if (config->discovery.registration_locked) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"Seed provider cannot be replaced while the client is connected");
	}

	as_seed_provider copy = *provider;
	copy.name = cf_strdup(provider->name);

	if (!copy.name) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to copy seed provider name");
	}

	as_config_install_seed_provider(config, &copy, NULL);
	return AEROSPIKE_OK;
}

as_status
as_config_set_address_translator(as_config* config, const as_address_translator* translator,
	as_error* err)
{
	if (!config || !translator_is_valid(translator)) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "Invalid address translator");
	}

	if (config->discovery.registration_locked) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"Address translator cannot be replaced while the client is connected");
	}

	as_address_translator copy = *translator;
	copy.name = cf_strdup(translator->name);

	if (!copy.name) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT,
			"Failed to copy address translator name");
	}

	as_config_install_translator(config, &copy, NULL);
	return AEROSPIKE_OK;
}

// Install a built translator whose name is not copied yet, with the recipe that rebuilds it.
// On failure the translator is destroyed and the old registration is kept.
static as_status
install_built_translator(as_config* config, as_address_translator* translator,
	as_plugin_recipe* recipe, as_error* err)
{
	if (!recipe) {
		if (translator->destroy) {
			translator->destroy(translator->udata);
		}
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT,
			"Failed to allocate address translator recipe");
	}

	if (!own_translator_name(translator)) {
		as_plugin_recipe_destroy(recipe);
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT,
			"Failed to copy address translator name");
	}
	as_config_install_translator(config, translator, recipe);
	return AEROSPIKE_OK;
}

static as_status
translator_unlocked(as_config* config, as_error* err)
{
	if (!config) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "Invalid address translator");
	}

	if (config->discovery.registration_locked) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"Address translator cannot be replaced while the client is connected");
	}
	return AEROSPIKE_OK;
}

as_status
as_config_set_port_map_translator(as_config* config, const as_addr_map* entries, uint32_t size,
	as_error* err)
{
	as_status status = translator_unlocked(config, err);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_address_translator translator;
	status = as_discovery_port_map_translator(err, entries, size, &translator);

	if (status != AEROSPIKE_OK) {
		return status;
	}
	return install_built_translator(config, &translator, as_plugin_recipe_port_map(entries, size),
		err);
}

as_status
as_config_set_pass_through_translator(as_config* config, as_error* err)
{
	as_status status = translator_unlocked(config, err);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_address_translator translator;
	as_discovery_pass_through_translator(&translator);
	return install_built_translator(config, &translator, as_plugin_recipe_pass_through(), err);
}

as_status
as_config_set_dns_seed_provider(as_config* config, const as_dns_seed_provider_config* dns,
	as_error* err)
{
	if (!config) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "Invalid seed provider");
	}

	if (config->discovery.registration_locked) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"Seed provider cannot be replaced while the client is connected");
	}

	as_seed_provider provider;
	as_status status = as_discovery_dns_seed_provider(err, dns, &provider);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_plugin_recipe* recipe = as_plugin_recipe_dns(dns);

	if (!recipe) {
		provider.destroy(provider.udata);
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT,
			"Failed to allocate seed provider recipe");
	}

	if (!own_provider_name(&provider)) {
		as_plugin_recipe_destroy(recipe);
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to copy seed provider name");
	}
	as_config_install_seed_provider(config, &provider, recipe);
	return AEROSPIKE_OK;
}

as_status
as_config_set_binding_seed_provider(as_config* config, const char* name,
	as_status (*refresh)(as_error* err, const as_seed_candidate_context* context,
		as_seed_candidates* candidates, void* udata),
	bool periodic, void (*destroy)(void* udata), void* udata, as_error* err)
{
	if (!config) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "Invalid seed provider");
	}

	if (config->discovery.execution_mode != AS_DISCOVERY_EXECUTION_THREAD) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"Binding seed provider requires thread execution mode");
	}

	as_seed_provider provider = {
		.size = sizeof(as_seed_provider),
		.api_version = AS_DISCOVERY_API_VERSION,
		.name = name,
		.refresh = refresh,
		.needs_periodic_refresh = periodic ? binding_periodic : binding_not_periodic,
		.destroy = destroy,
		.udata = udata
	};
	as_status status = as_config_set_seed_provider(config, &provider, err);

	if (status == AEROSPIKE_OK) {
		config->discovery.binding_provider = true;
	}
	return status;
}

as_status
as_config_register_seed_provider(as_config* config, const char* name,
	as_seed_provider_factory factory, void (*destroy)(void* udata), void* udata, as_error* err)
{
	as_plugin_entry entry = {
		.factory.provider = factory,
		.destroy = destroy,
		.udata = udata
	};
	return registry_add(config, config ? &config->discovery.registry : NULL, "seed provider",
		name, factory != NULL, as_config_builtin_providers, AS_CONFIG_BUILTIN_PROVIDERS, &entry,
		err);
}

as_status
as_config_register_address_translator(as_config* config, const char* name,
	as_address_translator_factory factory, void (*destroy)(void* udata), void* udata,
	as_error* err)
{
	as_plugin_entry entry = {
		.factory.translator = factory,
		.destroy = destroy,
		.udata = udata
	};
	return registry_add(config, config ? &config->translation.registry : NULL,
		"address translator", name, factory != NULL, as_config_builtin_translators,
		AS_CONFIG_BUILTIN_TRANSLATORS, &entry, err);
}

as_status
as_config_select_seed_provider(as_config* config, const char* name, const as_map* options,
	as_error* err)
{
	int32_t index = -1;
	as_status status = select_check(config, config ? config->discovery.registry : NULL,
		"seed provider", name, as_config_builtin_providers, AS_CONFIG_BUILTIN_PROVIDERS,
		"Seed provider cannot be replaced while the client is connected", &index, err);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_map* copy;
	as_error local;
	as_error_init(&local);
	status = copy_options(options, &copy, &local);

	if (status == AEROSPIKE_OK) {
		as_seed_provider provider;
		status = as_plugin_build_seed_provider(&local, config->discovery.registry,
			(uint32_t)index, copy, &provider);

		if (status == AEROSPIKE_OK) {
			as_plugin_recipe* recipe = as_plugin_recipe_factory((uint32_t)index, copy, false);

			if (recipe) {
				as_config_install_seed_provider(config, &provider, recipe);
			}
			else {
				if (provider.destroy) {
					provider.destroy(provider.udata);
				}
				cf_free((void*)provider.name);
				status = as_error_set_message(&local, AEROSPIKE_ERR_CLIENT,
					"Failed to allocate seed provider recipe");
			}
		}
		as_map_destroy(copy);
	}

	if (status != AEROSPIKE_OK) {
		return as_error_update(err, status, "Invalid seed provider '%s': %s", name,
			local.message);
	}
	return AEROSPIKE_OK;
}

as_status
as_config_select_address_translator(as_config* config, const char* name,
	const as_map* options, as_error* err)
{
	int32_t index = -1;
	as_status status = select_check(config, config ? config->translation.registry : NULL,
		"address translator", name, as_config_builtin_translators, AS_CONFIG_BUILTIN_TRANSLATORS,
		"Address translator cannot be replaced while the client is connected", &index, err);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_map* copy;
	as_error local;
	as_error_init(&local);
	status = copy_options(options, &copy, &local);

	if (status == AEROSPIKE_OK) {
		as_address_translator translator;
		status = as_plugin_build_translator(&local, config->translation.registry,
			(uint32_t)index, copy, &translator);

		if (status == AEROSPIKE_OK) {
			as_plugin_recipe* recipe = as_plugin_recipe_factory((uint32_t)index, copy, true);

			if (recipe) {
				as_config_install_translator(config, &translator, recipe);
			}
			else {
				if (translator.destroy) {
					translator.destroy(translator.udata);
				}
				cf_free((void*)translator.name);
				status = as_error_set_message(&local, AEROSPIKE_ERR_CLIENT,
					"Failed to allocate address translator recipe");
			}
		}
		as_map_destroy(copy);
	}

	if (status != AEROSPIKE_OK) {
		return as_error_update(err, status, "Invalid address translator '%s': %s", name,
			local.message);
	}
	return AEROSPIKE_OK;
}

bool
as_config_add_hosts(as_config* config, const char* string, uint16_t default_port)
{
	// String format: hostname1[:tlsname1][:port1],...
	// Hostname may also be an IP address in the following formats.
	// IPv4: xxx.xxx.xxx.xxx
	// IPv6: [xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx]
	// IPv6: [xxxx::xxxx]
	const char* p = string;
	
	if (! config->hosts) {
		// Initialize list.  Determine list capacity in first pass.
		uint32_t capacity = 1;
		
		while (*p) {
			if (*p == ',') {
				capacity++;
			}
			p++;
		}
		config->hosts = as_vector_create(sizeof(as_host), capacity);
		p = string;
	}

	// Parse hosts
	as_host host;
	const char* begin;
	uint32_t length;
	bool more;
	
	while (*p) {
		host.name = 0;
		host.tls_name = 0;
		host.port = default_port;
		length = 0;
		more = false;
		
		// Parse hostname or IP address.
		if (*p == '[') {
			// IPv6 addresses are enclosed by brackets.
			p++;
			begin = p;
			
			while (*p) {
				if (*p == ']') {
					p++;
					
					if (*p == ':') {
						p++;
						more = true;
					}
					else if (*p == ',') {
						p++;
					}
					break;
				}
				length++;
				p++;
			}
		}
		else {
			begin = p;
			
			while (*p) {
				if (*p == ',') {
					p++;
					break;
				}
				else if (*p == ':') {
					p++;
					more = true;
					break;
				}
				length++;
				p++;
			}
		}
		host.name = cf_malloc(length + 1);
		memcpy(host.name, begin, length);
		host.name[length] = 0;
		
		if (more) {
			// Parse tls name or port.
			begin = p;
			length = 0;
			more = false;
			
			while (*p) {
				if (*p == ',') {
					p++;
					break;
				}
				else if (*p == ':') {
					// Must be tls name.
					host.tls_name = cf_malloc(length + 1);
					memcpy(host.tls_name, begin, length);
					host.tls_name[length] = 0;
					p++;
					more = true;
					break;
				}
				length++;
				p++;
			}
			
			if (more) {
				if (! isdigit(*p)) {
					cf_free(host.name);
					cf_free(host.tls_name);
					return false;
				}
				host.port = (uint16_t)strtol(p, (char**)&p, 10);

				if (*p) {
					if (*p == ',') {
						p++;
					}
					else {
						cf_free(host.name);
						cf_free(host.tls_name);
						return false;
					}
				}
			}
			else {
				if (isdigit(*begin)) {
					char* q = NULL;
					host.port = (uint16_t)strtol(begin, &q, 10);
					
					if (*q && *q != ',') {
						cf_free(host.name);
						return false;
					}
				}
				else {
					host.tls_name = cf_malloc(length + 1);
					memcpy(host.tls_name, begin, length);
					host.tls_name[length] = 0;
				}
			}
		}
		as_vector_append(config->hosts, &host);
	}
	return true;
}

static void
add_host(as_config* config, char* addr, char* tls_name, uint16_t port)
{
	if (! config->hosts) {
		config->hosts = as_vector_create(sizeof(as_host), 16);
	}

	as_host* host = as_vector_reserve(config->hosts);
	host->name = addr;
	host->tls_name = tls_name;
	host->port = port;
}

void
as_config_add_host(as_config* config, const char* addr, uint16_t port)
{
	add_host(config, cf_strdup(addr), NULL, port);
}

void
as_config_tls_add_host(as_config* config, const char* addr, const char* tls_name, uint16_t port)
{
	add_host(config, cf_strdup(addr), cf_strdup(tls_name), port);
}

void
as_config_clear_hosts(as_config* config)
{
	as_vector* hosts = config->hosts;

	if (hosts) {
		for (uint32_t i = 0; i < hosts->size; i++) {
			as_host* host = as_vector_get(hosts, i);
			as_host_destroy(host);
		}
		as_vector_clear(hosts);
	}
}

bool
as_config_set_user(as_config* config, const char* user, const char* password)
{
	if (! (user && *user)) {
		return false;
	}

	if (! (password && *password)) {
		return false;
	}

	if (strlen(user) >= sizeof(config->user)) {
		return false;
	}

	if (strlen(password) >= sizeof(config->password)) {
		return false;
	}

	// Neither field is touched unless both are valid and fit, so a false
	// return above leaves config->user and config->password completely unmodified.
	as_strncpy(config->user, user, sizeof(config->user));
	as_strncpy(config->password, password, sizeof(config->password));
	return true;
}

void
as_config_add_rack_id(as_config* config, int rack_id)
{
	if (! config->rack_ids) {
		config->rack_ids = as_vector_create(sizeof(int), 8);
	}
	as_vector_append(config->rack_ids, &rack_id);
}

void
as_config_set_string(char** str, const char* value)
{
	if (*str) {
		cf_free(*str);
	}

	*str = value ? cf_strdup(value) : NULL;
}

bool
as_auth_mode_from_string(as_auth_mode* auth, const char* str)
{
	if (strcasecmp(str, "INTERNAL") == 0) {
		*auth = AS_AUTH_INTERNAL;
		return true;
	}

	if (strcasecmp(str, "EXTERNAL") == 0) {
		*auth = AS_AUTH_EXTERNAL;
		return true;
	}

	if (strcasecmp(str, "EXTERNAL_INSECURE") == 0) {
		*auth = AS_AUTH_EXTERNAL_INSECURE;
		return true;
	}

	if (strcasecmp(str, "PKI") == 0) {
		*auth = AS_AUTH_PKI;
		return true;
	}

	return false;
}

void
as_config_massage_error_rate(as_config* config)
{
	double ratio = (config->error_rate_window == 0)? 0.0 :
		((double)config->max_error_rate / (double)config->error_rate_window);

	if (! (ratio >= 1.0 - 0.000001 && ratio <= 100.0 + 0.000001)) {
		uint32_t mer = config->max_error_rate;
		uint32_t erw = config->error_rate_window;

		config->max_error_rate = 100;
		config->error_rate_window = 1;

		as_log_warn(
			"Invalid circuit breaker configuration: max_error_rate: %u, error_rate_window: %u, ratio: %.2f. The ratio (max_error_rate/error_rate_window) must be between 1 and 100. Resetting to defaults - max_error_rate: %u and error_rate_window: %u",
			mer, erw, ratio, config->max_error_rate, config->error_rate_window);
	}
}
