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

/**
 * @defgroup discovery_api Pluggable Discovery
 *
 * Seed discovery and address translation plugins.
 *
 * Plugins run in-process with no isolation. A crash, hang or memory error in
 * plugin code is a crash, hang or memory error in the client. Plugins must not
 * create threads, tasks, timers or any other scheduling resource of their own;
 * the client owns all scheduling.
 *
 * The client owns each registered provider and translator from registration
 * until the registration ends (see as_seed_provider.destroy). Results belong to
 * the client once returned.
 */

#include <aerospike/as_error.h>
#include <aerospike/as_host.h>
#include <aerospike/as_vector.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Macros
//---------------------------------

/**
 * Function table version understood by this client.
 */
#define AS_DISCOVERY_API_VERSION 1

/**
 * Default port of hosts resolved by the DNS seed provider from A and AAAA records.
 */
#define AS_DNS_DEFAULT_PORT 3000

/**
 * Default total time in milliseconds of one DNS seed provider refresh.
 */
#define AS_DNS_DEFAULT_TIMEOUT_MS 80

/**
 * Default port of a DNS resolver.
 */
#define AS_DNS_DEFAULT_SERVER_PORT 53

/**
 * Default port of hosts returned by the AWS EC2 seed provider.
 */
#define AS_AWS_DEFAULT_PORT 3000

/**
 * Default total time in milliseconds of one AWS EC2 seed provider refresh.
 */
#define AS_AWS_DEFAULT_TIMEOUT_MS 5000

//---------------------------------
// Types
//---------------------------------

struct as_addr_map_s;

/**
 * Source list from which a server advertised an endpoint.
 *
 * @ingroup discovery_api
 */
typedef enum as_endpoint_source_e {
	AS_ENDPOINT_SOURCE_STANDARD,
	AS_ENDPOINT_SOURCE_ALTERNATE
} as_endpoint_source;

/**
 * How configured (static) seeds and discovered seeds are combined. Duplicate
 * hosts (same name and port) are removed, keeping the first occurrence.
 *
 * Default: AS_SEED_MERGE
 *
 * @ingroup discovery_api
 */
typedef enum as_seed_merge_policy_e {
	/**
	 * Static seeds first, then discovered seeds. Use for migration, resilience
	 * and to keep a known-good endpoint while discovery catches up.
	 */
	AS_SEED_MERGE,

	/**
	 * At initialization behave as AS_SEED_MERGE. After initialization the
	 * discovered seeds of each successful refresh replace the seed list. An
	 * empty discovery result after initialization keeps the previous list.
	 * Use for greenfield deployments where the static seeds are legacy.
	 */
	AS_SEED_REPLACE,

	/**
	 * Discovered seeds only. Static seeds are used when discovery returns an
	 * empty result, and at initialization also when it fails. A failed refresh
	 * after initialization keeps the previous seeds. Use when the static seeds
	 * are an emergency fallback.
	 */
	AS_SEED_DISCOVERY_ONLY
} as_seed_merge_policy;

/**
 * Behavior when a translator has no mapping for an advertised endpoint.
 *
 * Default: AS_TRANSLATION_PASS_THROUGH
 *
 * @ingroup discovery_api
 */
typedef enum as_translation_miss_policy_e {
	/**
	 * Use the advertised endpoint unchanged.
	 */
	AS_TRANSLATION_PASS_THROUGH,

	/**
	 * Drop the endpoint; the node can still be reached through its other
	 * endpoints. A peer that cannot be added is not created: the miss is logged
	 * at warning level as an AEROSPIKE_ERR_TRANSLATION_MISS message naming the
	 * translator, the node, the advertised host:port and the source list, and
	 * the peer is retried on every tend. A miss alone does not fail
	 * aerospike_connect(). When initialization fails for another reason, such
	 * as no reachable seed, a recorded translation error is returned instead of
	 * the generic connection error.
	 */
	AS_TRANSLATION_STRICT
} as_translation_miss_policy;

/**
 * Context in which a seed provider is periodically refreshed.
 *
 * Default: AS_DISCOVERY_EXECUTION_THREAD
 *
 * @ingroup discovery_api
 */
typedef enum as_discovery_execution_mode_e {
	/**
	 * One client-owned discovery thread, started when the provider needs
	 * periodic refresh or is a binding provider. Refresh never runs on the tend
	 * thread.
	 */
	AS_DISCOVERY_EXECUTION_THREAD,

	/**
	 * Refresh runs on the tend thread every refresh_interval_ms / tender_interval
	 * tends, after that tend's reseed attempt. Only providers whose
	 * supports_deadline returns true are eligible. The deadline is measured, not
	 * enforced: a refresh that takes longer than tend_refresh_deadline_ms, counted
	 * in whole milliseconds, delays that tend and its result is discarded. After
	 * three consecutive overruns the provider is no longer refreshed and the
	 * initialization seeds are restored until the client is recreated. An on-time
	 * refresh resets the count.
	 */
	AS_DISCOVERY_EXECUTION_TEND
} as_discovery_execution_mode;

/**
 * Built-in translator kinds understood by as_discovery_conformance_translate().
 *
 * @ingroup discovery_api
 */
typedef enum as_translator_kind_e {
	AS_TRANSLATOR_STATIC_MAP,
	AS_TRANSLATOR_PORT_MAP,
	AS_TRANSLATOR_PASS_THROUGH
} as_translator_kind;

/**
 * DNS record type queried by the DNS seed provider for each name.
 *
 * Default: AS_DNS_RECORD_A
 *
 * @ingroup discovery_api
 */
typedef enum as_dns_record_type_e {
	/**
	 * A and AAAA records: one host per address with as_dns_seed_provider_config.port.
	 * All A addresses come first, then all AAAA addresses, each family in sorted order.
	 * At most the first 256 addresses of each family are used; a warning is logged
	 * when an answer has more.
	 */
	AS_DNS_RECORD_A,

	/**
	 * SRV records: one host per record with the record's port, ordered by priority
	 * (lowest first), then weight (highest first), then target and port. At most
	 * the first 256 records in that order are used; a warning is logged when an
	 * answer has more. Each target is resolved within the same refresh to one
	 * address: the first sorted A record of the target in the additional section
	 * of the SRV answer, otherwise an A query, then an AAAA query when the target
	 * has no A record. AAAA records in the additional section are not used. A
	 * target that does not resolve is dropped and logged at warning level; targets
	 * left without time for a query are logged together. When every resolver is
	 * silent, each target lookup uses up to 3/4 of the time left, so only the
	 * first few such targets are queried. Records with port 0 are dropped; the
	 * first is logged, and the rest are counted in one line.
	 */
	AS_DNS_RECORD_SRV
} as_dns_record_type;

/**
 * Configuration of the built-in DNS seed provider. Initialize it with
 * as_dns_seed_provider_config_init(), then set names and any other field.
 * Every string and array is copied when the provider is created; the caller
 * keeps ownership of them and may change or release them afterwards.
 *
 * @ingroup discovery_api
 */
typedef struct as_dns_seed_provider_config_s {
	/**
	 * DNS names to resolve, queried in order. Required: one or more names, no
	 * duplicates (compared case-insensitively). A name is queried as an absolute
	 * name; a trailing dot is optional. No search domain is appended.
	 */
	const char* const* names;

	/**
	 * Number of names.
	 */
	uint32_t names_size;

	/**
	 * Default: AS_DNS_RECORD_A
	 */
	as_dns_record_type record_type;

	/**
	 * Port of hosts resolved from A and AAAA records. Ignored for
	 * AS_DNS_RECORD_SRV, which uses each record's port. Must be > 0.
	 * Default: AS_DNS_DEFAULT_PORT
	 */
	uint16_t port;

	/**
	 * TLS name set on every returned host. NULL leaves the TLS name to be
	 * defaulted the same way as for configured seeds. Must not be empty.
	 * Default: NULL
	 */
	const char* tls_name;

	/**
	 * Total time in milliseconds of one refresh, across every name, record type,
	 * SRV target, resolver and UDP or TCP exchange. Must be > 0. It must not
	 * exceed discovery.tend_refresh_deadline_ms with AS_DISCOVERY_EXECUTION_TEND,
	 * or discovery.refresh_timeout_ms with AS_DISCOVERY_EXECUTION_THREAD;
	 * otherwise aerospike_connect() fails with AEROSPIKE_ERR_PARAM. With the
	 * default tend deadline of 100 ms only a local resolver answers reliably in
	 * time.
	 * Default: AS_DNS_DEFAULT_TIMEOUT_MS
	 */
	uint32_t timeout_ms;

	/**
	 * Resolver addresses. Each query tries them in order, starting with the one
	 * that last answered in the same refresh; see as_discovery_dns_seed_provider().
	 * Each is a numeric IP address with an optional port: "ip[:port]", with an
	 * IPv6 address in brackets when a port is given ("[2001:db8::1]:53"). The port
	 * defaults to AS_DNS_DEFAULT_SERVER_PORT.
	 * When none is given, the system resolvers are read once when the provider is
	 * created: the nameserver entries of /etc/resolv.conf, or the DNS servers of
	 * the active network adapters on Windows.
	 * Default: NULL (system resolvers)
	 */
	const char* const* servers;

	/**
	 * Number of servers.
	 */
	uint32_t servers_size;
} as_dns_seed_provider_config;

/**
 * EC2 instance field returned as the seed address by the AWS EC2 seed provider.
 *
 * Default: AS_AWS_ADDRESS_PRIVATE_IP
 *
 * @ingroup discovery_api
 */
typedef enum as_aws_address_field_e {
	/**
	 * Primary private IPv4 address (privateIpAddress).
	 */
	AS_AWS_ADDRESS_PRIVATE_IP,

	/**
	 * Public IPv4 address (ipAddress).
	 */
	AS_AWS_ADDRESS_PUBLIC_IP,

	/**
	 * Private DNS name (privateDnsName). The client resolves it when it seeds.
	 */
	AS_AWS_ADDRESS_PRIVATE_DNS,

	/**
	 * Public DNS name (dnsName). The client resolves it when it seeds.
	 */
	AS_AWS_ADDRESS_PUBLIC_DNS
} as_aws_address_field;

/**
 * One EC2 DescribeInstances filter. An instance matches when it matches one of the values of
 * every filter.
 *
 * @ingroup discovery_api
 */
typedef struct as_aws_filter_s {
	/**
	 * Filter name, passed to EC2 as given: "tag:<key>" or any other EC2 filter name. Must not be
	 * empty, must not be "instance-state-name", which the provider sets to running, and must not
	 * repeat an earlier filter name (compared case-sensitively).
	 */
	const char* name;

	/**
	 * Filter values. Required: one or more non-empty values.
	 */
	const char* const* values;

	/**
	 * Number of values.
	 */
	uint32_t values_size;
} as_aws_filter;

/**
 * Configuration of the built-in AWS EC2 seed provider. Initialize it with
 * as_aws_seed_provider_config_init(), then set region, filters, the credentials and any other
 * field. Every string and array is copied when the provider is created; the caller keeps
 * ownership of them and may change or release them afterwards.
 *
 * @ingroup discovery_api
 */
typedef struct as_aws_seed_provider_config_s {
	/**
	 * AWS region of the instances and of the default endpoint, such as "us-east-1". Required;
	 * must match [a-z0-9]+(-[a-z0-9]+)+.
	 */
	const char* region;

	/**
	 * DescribeInstances filters, sent in order. Required: one or more filters.
	 */
	const as_aws_filter* filters;

	/**
	 * Number of filters.
	 */
	uint32_t filters_size;

	/**
	 * Instance field returned as the seed address.
	 * Default: AS_AWS_ADDRESS_PRIVATE_IP
	 */
	as_aws_address_field address_field;

	/**
	 * Port set on every returned host. Must be > 0.
	 * Default: AS_AWS_DEFAULT_PORT
	 */
	uint16_t port;

	/**
	 * TLS name set on every returned host. NULL leaves the TLS name to be defaulted the same way
	 * as for configured seeds. Must not be empty.
	 * Default: NULL
	 */
	const char* tls_name;

	/**
	 * Total time in milliseconds of one refresh, across every connect, TLS handshake, write, read
	 * and page. Must be > 0, and must not exceed discovery.refresh_timeout_ms, otherwise
	 * aerospike_connect() fails with AEROSPIKE_ERR_PARAM.
	 * Default: AS_AWS_DEFAULT_TIMEOUT_MS
	 */
	uint32_t timeout_ms;

	/**
	 * EC2 endpoint URL, such as a VPC interface endpoint: "http" or "https", a host (an IPv6
	 * address in brackets), an optional port and path, and no query, fragment or userinfo.
	 * NULL uses the regional endpoint https://ec2.<region>.amazonaws.com, or
	 * https://ec2.<region>.amazonaws.com.cn for cn-* regions.
	 * Default: NULL
	 */
	const char* endpoint;

	/**
	 * PEM file of the certificate authorities trusted for HTTPS endpoints. When ca_file and
	 * ca_path are both NULL, the OpenSSL default verify paths are trusted, which honour the
	 * SSL_CERT_FILE and SSL_CERT_DIR environment variables. When neither variable is set and
	 * the default certificate file of the OpenSSL build does not exist, as happens with an
	 * OpenSSL bundled into a prebuilt package, the first existing system bundle among
	 * /etc/ssl/certs/ca-certificates.crt, /etc/pki/tls/certs/ca-bundle.crt,
	 * /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem, /etc/ssl/ca-bundle.pem,
	 * /etc/ssl/cert.pem and /usr/local/etc/ssl/cert.pem is also trusted. On Windows the OpenSSL
	 * defaults are not the system certificate store, so set ca_file (or SSL_CERT_FILE) there.
	 * The file is loaded for any endpoint scheme. Must not be empty. A C client extension.
	 * Default: NULL
	 */
	const char* ca_file;

	/**
	 * Directory of hashed certificate authority files (see openssl rehash) trusted for HTTPS
	 * endpoints, with or instead of ca_file. Must not be empty. A C client extension.
	 * Default: NULL
	 */
	const char* ca_path;

	/**
	 * AWS access key id used to sign requests. Required; letters and digits only.
	 */
	const char* access_key_id;

	/**
	 * AWS secret access key. Required; visible ASCII characters only. Never logged or put in a
	 * message.
	 */
	const char* secret_access_key;

	/**
	 * Session token of temporary credentials, sent as x-amz-security-token. NULL or empty for
	 * none; otherwise visible ASCII characters only. Never logged or put in a message.
	 * Default: NULL
	 */
	const char* session_token;
} as_aws_seed_provider_config;

/**
 * An endpoint advertised by a server. All strings are read-only, owned by the
 * client and valid only for the duration of the call that receives them.
 *
 * @ingroup discovery_api
 */
typedef struct as_endpoint_s {
	/**
	 * Server node name.
	 */
	const char* node_name;

	/**
	 * TLS identity of the node. NULL when absent. Translation never changes it.
	 */
	const char* tls_name;

	/**
	 * Advertised host name or IP address.
	 */
	const char* host;

	/**
	 * Advertised port.
	 */
	uint16_t port;

	/**
	 * List from which the endpoint was advertised.
	 */
	as_endpoint_source source;
} as_endpoint;

/**
 * Read-only context supplied to a seed provider refresh callback. All fields are
 * owned by the client and valid only for the duration of the call.
 *
 * @ingroup discovery_api
 */
typedef struct as_seed_candidate_context_s {
	/**
	 * Configured cluster name. NULL when not configured.
	 */
	const char* cluster_name;

	/**
	 * Configured seeds (as_host elements).
	 */
	const as_vector* configured_seeds;

	/**
	 * Monotonic tend iteration. Zero at client initialization and incremented
	 * once per tend after it, whether or not the tend succeeded.
	 */
	uint64_t tend_iteration;
} as_seed_candidate_context;

/**
 * Client-owned output container for seed candidates. The client allocates it;
 * the provider adds hosts with as_seed_candidates_append(), which copies every
 * string. Hosts are in preference order: the first is tried first.
 *
 * @ingroup discovery_api
 */
typedef struct as_seed_candidates_s {
	/**
	 * Copied candidates (as_host elements).
	 */
	as_vector* hosts;
} as_seed_candidates;

/**
 * Versioned seed provider function table.
 *
 * Required: refresh, needs_periodic_refresh.
 * Optional (NULL): supports_deadline defaults to false, destroy to a no-op.
 *
 * The table is copied at registration and the client takes ownership of udata.
 * name is copied. A failed registration leaves udata owned by the caller.
 *
 * @ingroup discovery_api
 */
typedef struct as_seed_provider_s {
	/**
	 * Set to sizeof(as_seed_provider).
	 */
	uint32_t size;

	/**
	 * Set to AS_DISCOVERY_API_VERSION.
	 */
	uint32_t api_version;

	/**
	 * Stable, non-empty plugin name used in log and error messages.
	 */
	const char* name;

	/**
	 * Fill candidates with copied hosts using as_seed_candidates_append().
	 * Return AEROSPIKE_OK (an empty result is a valid answer) or an error set
	 * on err; on error the previous seeds are kept. An error at
	 * initialization falls back to the configured seeds;
	 * aerospike_connect() fails with AEROSPIKE_ERR_SEED_DISCOVERY_FAILED only
	 * when none are configured. Later errors are logged and never stop tend.
	 *
	 * Context: a non-binding provider whose needs_periodic_refresh returns false
	 * runs once, at initialization, on the application thread that connects.
	 * In AS_DISCOVERY_EXECUTION_THREAD with a periodic provider, and for every
	 * binding provider, every call (including the initial one) runs on the
	 * client-owned discovery thread, and aerospike_connect() waits at most
	 * refresh_timeout_ms for the initial result. In AS_DISCOVERY_EXECUTION_TEND
	 * the initial call runs on the connecting thread and later calls on the
	 * tend thread, where it must return within tend_refresh_deadline_ms.
	 *
	 * Within one aerospike object no two refresh calls ever overlap, including
	 * across close, re-registration (same or different udata) and reconnect. A
	 * udata registered on several configs or aerospike objects is called by
	 * each client independently, so those calls can overlap.
	 *
	 * Close: aerospike_close() waits at most refresh_timeout_ms for an
	 * in-progress thread-mode refresh. A failed aerospike_connect() shares one
	 * budget with its initial wait: connect waits at most refresh_timeout_ms in
	 * total for provider calls, measured from the start of the initial call.
	 * After that they return, and the call finishes on the discovery thread.
	 *
	 * Reconnect: when a refresh left running by a previous connection of the
	 * same aerospike object has not returned, aerospike_connect() waits at most
	 * refresh_timeout_ms for it. This applies whichever provider is registered
	 * now, including one registered while the old call was still running. If
	 * the old call is still running, connect fails with
	 * AEROSPIKE_ERR_SEED_DISCOVERY_FAILED, names the provider in the message and
	 * starts nothing.
	 */
	as_status (*refresh)(as_error* err, const as_seed_candidate_context* context,
		as_seed_candidates* candidates, void* udata);

	/**
	 * Return true if candidates can change and must be refreshed periodically.
	 * When false, no periodic work is scheduled, and no discovery thread is
	 * started unless the provider is a binding provider (which runs its single
	 * call there). Called synchronously once per connect on the application
	 * thread that connects the client, with no time limit, so it must return
	 * without blocking or waiting for another thread. A binding provider has no
	 * such callback; it states periodic at registration.
	 */
	bool (*needs_periodic_refresh)(void* udata);

	/**
	 * Return true if refresh does no I/O or bounds all I/O so that it returns
	 * within a caller-supplied deadline. Only such providers may use
	 * AS_DISCOVERY_EXECUTION_TEND. Called synchronously once per connect on the
	 * application thread that connects the client, with no time limit, so it
	 * must return without blocking or waiting for another thread. Optional.
	 */
	bool (*supports_deadline)(void* udata);

	/**
	 * Release udata. Called exactly once, after the last call has returned,
	 * when the registration ends:
	 * - at aerospike_destroy() or as_config_destroy();
	 * - on async aerospike_close() of a connected client;
	 * - on async aerospike_close() of an unconnected client (registrations
	 *   only);
	 * - on replacement whenever registration is unlocked: before the first
	 *   connect, after a failed connect, or after a sync aerospike_close().
	 *   Replacing with the same non-NULL udata destroys nothing.
	 *
	 * A sync-only aerospike_close() does not destroy the registration, so the
	 * client can reconnect with it.
	 *
	 * When a refresh is still running at that point, destroy runs on the
	 * discovery thread once it returns. So destroy may run on any client
	 * thread, and may run after aerospike_close(), aerospike_destroy() or
	 * as_config_destroy() has returned; none of them waits for it. A binding's
	 * destroy must therefore stay safe while its runtime shuts down.
	 *
	 * destroy is otherwise called synchronously by the thread that ends the
	 * registration (aerospike_destroy(), as_config_destroy(), a replacing
	 * setter, or aerospike_close()), so it must never block or wait for another
	 * thread, because that thread may be the caller. For Node.js this means
	 * releasing a thread-safe function or queueing work, never calling into JS
	 * and waiting. Optional.
	 */
	void (*destroy)(void* udata);

	/**
	 * Plugin state passed to every callback.
	 */
	void* udata;
} as_seed_provider;

/**
 * Versioned address translator function table.
 *
 * Required: translate.
 * Optional (NULL): on_cluster_view_changed and destroy default to no-ops.
 *
 * The table is copied at registration and the client takes ownership of udata.
 * name is copied. A failed registration leaves udata owned by the caller.
 *
 * @ingroup discovery_api
 */
typedef struct as_address_translator_s {
	/**
	 * Set to sizeof(as_address_translator).
	 */
	uint32_t size;

	/**
	 * Set to AS_DISCOVERY_API_VERSION.
	 */
	uint32_t api_version;

	/**
	 * Stable, non-empty plugin name used in log and error messages.
	 */
	const char* name;

	/**
	 * Translate an advertised endpoint. On a mapping, fill out with a copied
	 * transport host and port (as_host_copy_fields() with a NULL tls_name) and
	 * return true; the client owns and destroys out. Return false for a miss,
	 * leaving out untouched; the miss is handled by as_translation_config.on_miss.
	 * The TLS identity always remains endpoint->tls_name; any out->tls_name is
	 * ignored.
	 *
	 * Context: the tend thread or the application thread during cluster
	 * initialization. Must be synchronous, in-memory, perform no I/O and not
	 * block (target p99 below 1 ms).
	 */
	bool (*translate)(const as_endpoint* endpoint, as_host* out, void* udata);

	/**
	 * Notification that the cluster view changed. generation is a client-wide
	 * counter. Same context and restrictions as translate. Optional.
	 */
	void (*on_cluster_view_changed)(uint32_t generation, void* udata);

	/**
	 * Release udata. Same timing and rules as as_seed_provider.destroy: called
	 * exactly once, after the last translate or on_cluster_view_changed call has
	 * returned, and it must never block or wait for another thread. Optional.
	 */
	void (*destroy)(void* udata);

	/**
	 * Plugin state passed to every callback.
	 */
	void* udata;
} as_address_translator;

struct as_seed_inflight_s;

/**
 * Seed discovery configuration owned by as_config. Register a provider with
 * as_config_set_seed_provider() or as_config_set_binding_seed_provider(); when
 * none is registered the static configured-seed provider is used.
 *
 * Adding this structure changes the size of the public by-value as_config and
 * aerospike structures, so applications must be recompiled.
 *
 * @ingroup discovery_api
 */
typedef struct as_discovery_config_s {
	/**
	 * Registered provider. Do not set directly.
	 */
	as_seed_provider provider;

	/**
	 * Default: AS_SEED_MERGE
	 */
	as_seed_merge_policy merge_policy;

	/**
	 * Default: AS_DISCOVERY_EXECUTION_THREAD
	 */
	as_discovery_execution_mode execution_mode;

	/**
	 * Periodic refresh interval in milliseconds. When the provider needs
	 * periodic refresh it must be at least as_config.tender_interval, otherwise
	 * aerospike_connect() fails with AEROSPIKE_ERR_PARAM. In
	 * AS_DISCOVERY_EXECUTION_TEND the provider is refreshed every
	 * refresh_interval_ms / tender_interval tends.
	 * Default: 30000
	 */
	uint32_t refresh_interval_ms;

	/**
	 * Bound on one refresh in AS_DISCOVERY_EXECUTION_THREAD, in milliseconds:
	 * the longest aerospike_connect() waits for the initial refresh before it
	 * continues with the configured seeds. A refresh that takes longer is
	 * logged. Must be > 0 when a discovery thread runs: for a provider that
	 * needs periodic refresh in AS_DISCOVERY_EXECUTION_THREAD, and for every
	 * binding provider. With the built-in DNS or AWS EC2 seed provider it must be
	 * >= that provider's timeout_ms.
	 * Default: 10000
	 */
	uint32_t refresh_timeout_ms;

	/**
	 * Deadline for one refresh in AS_DISCOVERY_EXECUTION_TEND, in milliseconds.
	 * Must be > 0 and <= as_config.tender_interval / 2 for a provider that needs
	 * periodic refresh.
	 * Default: 100
	 */
	uint32_t tend_refresh_deadline_ms;

	/**
	 * True when a provider is registered. Do not set directly.
	 */
	bool provider_registered;

	/**
	 * True after aerospike_close() released the registration of an unconnected
	 * async client. aerospike_connect() fails until a provider is registered
	 * again. Do not set directly.
	 */
	bool provider_released;

	/**
	 * True when the registered provider is a binding seed callback, which is
	 * restricted to AS_DISCOVERY_EXECUTION_THREAD. Do not set directly.
	 */
	bool binding_provider;

	/**
	 * True while a cluster created from this config is active. Provider and
	 * translator registration is rejected while set. Do not set directly.
	 */
	bool registration_locked;

	/**
	 * Tracks a discovery-thread refresh that can outlive the connection that
	 * started it. Created by the first connect that starts a discovery thread
	 * and released by as_config_destroy(). Do not set directly.
	 */
	struct as_seed_inflight_s* inflight;
} as_discovery_config;

/**
 * Address translation configuration owned by as_config. Register a translator
 * with as_config_set_address_translator(); when none is registered the static
 * map over as_config.ip_map is used; while one is registered ip_map is ignored.
 *
 * @ingroup discovery_api
 */
typedef struct as_translation_config_s {
	/**
	 * Registered translator. Do not set directly.
	 */
	as_address_translator provider;

	/**
	 * Default: AS_TRANSLATION_PASS_THROUGH
	 */
	as_translation_miss_policy on_miss;

	/**
	 * True when a translator is registered. Do not set directly.
	 */
	bool provider_registered;

	/**
	 * True after aerospike_close() released the registration of an unconnected
	 * async client. aerospike_connect() fails until a translator is registered
	 * again. Do not set directly.
	 */
	bool provider_released;
} as_translation_config;

/**
 * Input for one conformance translate row.
 *
 * @ingroup discovery_api
 */
typedef struct as_conformance_translation_s {
	/**
	 * Built-in translator to evaluate.
	 */
	as_translator_kind kind;

	/**
	 * Miss policy applied when the translator has no mapping.
	 */
	as_translation_miss_policy on_miss;

	/**
	 * Translator map entries. For AS_TRANSLATOR_STATIC_MAP: host to host,
	 * which is also the default translator built over as_config.ip_map.
	 * For AS_TRANSLATOR_PORT_MAP: orig is an advertised host:port or a node
	 * name and alt is the host:port to connect to (see
	 * as_discovery_port_map_translator()). Ignored for
	 * AS_TRANSLATOR_PASS_THROUGH.
	 */
	const struct as_addr_map_s* map;

	/**
	 * Number of map entries.
	 */
	uint32_t map_size;
} as_conformance_translation;

//---------------------------------
// Functions
//---------------------------------

/**
 * Initialize a client-owned candidate container.
 *
 * @ingroup discovery_api
 */
AS_EXTERN bool
as_seed_candidates_init(as_seed_candidates* candidates, uint32_t capacity);

/**
 * Release a candidate container and every copied host in it.
 *
 * @ingroup discovery_api
 */
AS_EXTERN void
as_seed_candidates_destroy(as_seed_candidates* candidates);

/**
 * Append a copy of name and, if not NULL, tls_name. An empty tls_name is the
 * same as NULL. The caller keeps ownership of its strings. Returns false on
 * invalid arguments or allocation failure.
 *
 * @ingroup discovery_api
 */
AS_EXTERN bool
as_seed_candidates_append(as_seed_candidates* candidates, const char* name,
	const char* tls_name, uint16_t port);

/**
 * Fill provider with the default static seed provider: refresh copies the
 * configured seeds in order, needs_periodic_refresh is false, supports_deadline
 * is true, and it has no state and no destroy callback.
 *
 * @ingroup discovery_api
 */
AS_EXTERN void
as_discovery_static_seed_provider(as_seed_provider* provider);

/**
 * Copy the configured seeds from context into candidates, in order.
 *
 * @ingroup discovery_api
 */
AS_EXTERN as_status
as_discovery_static_seed_refresh(as_error* err, const as_seed_candidate_context* context,
	as_seed_candidates* candidates);

/**
 * Translate through an exact, case-sensitive, host-only static map; this is the
 * legacy as_config.ip_map rule. On a mapping, out receives a copied host, the
 * endpoint port and a NULL tls_name, and true is returned. A false result is a
 * miss and leaves out untouched.
 *
 * @ingroup discovery_api
 */
AS_EXTERN bool
as_discovery_static_map_translate(const struct as_addr_map_s* entries, uint32_t size,
	const as_endpoint* endpoint, as_host* out);

/**
 * Fill translator with the built-in port-map translator named "port-map".
 *
 * Each entry maps orig to alt. orig is an advertised host:port, or, when it
 * contains no ':', a node name. alt is the host:port to connect to. IPv6 hosts
 * must be written in brackets in both ([2001:db8::1]:3000). Ports are decimal
 * and in the range 1 to 65535. A lookup tries the advertised host:port first
 * (exact, case-sensitive host) and the advertised node name second. The mapped
 * endpoint keeps the advertised TLS name.
 *
 * The entries are validated and copied once; later changes to them have no
 * effect. Null or empty keys or values, empty hosts, a missing or invalid port,
 * an unbracketed IPv6 host and duplicate keys return AEROSPIKE_ERR_PARAM with a
 * message naming the entry; translator is then left untouched.
 *
 * On success the caller owns translator->udata until it is registered with
 * as_config_set_address_translator(), which then takes ownership; release an
 * unregistered table with translator->destroy(translator->udata). Most callers
 * should use as_config_set_port_map_translator().
 * @ingroup discovery_api
 */
AS_EXTERN as_status
as_discovery_port_map_translator(as_error* err, const struct as_addr_map_s* entries,
	uint32_t size, as_address_translator* translator);

/**
 * Fill translator with the built-in pass-through translator named
 * "pass-through". It returns the advertised host and port as a mapping, so it
 * succeeds even when as_translation_config.on_miss is AS_TRANSLATION_STRICT.
 * It has no state and no destroy callback.
 * @ingroup discovery_api
 */
AS_EXTERN void
as_discovery_pass_through_translator(as_address_translator* translator);

/**
 * Initialize a DNS seed provider configuration with its defaults: no names,
 * AS_DNS_RECORD_A, AS_DNS_DEFAULT_PORT, no TLS name, AS_DNS_DEFAULT_TIMEOUT_MS
 * and the system resolvers.
 *
 * @ingroup discovery_api
 */
AS_EXTERN void
as_dns_seed_provider_config_init(as_dns_seed_provider_config* config);

/**
 * Fill provider with the built-in DNS seed provider named "dns", which resolves
 * the configured names on every refresh.
 *
 * Every returned host is a numeric IPv4 or IPv6 address, never a host name, so
 * the client does no further name resolution when it seeds from the result.
 * Queries use classic DNS over UDP, retried over TCP when an answer is
 * truncated; the system resolver library and getaddrinfo() are not used.
 * Each query asks every resolver at most once, in order, except that within one
 * refresh the resolver that last answered (with records, an empty answer or
 * NXDOMAIN) is asked first; every refresh starts with the first resolver. As in
 * the Java client, each exchange gets 1/(2 * resolvers) of the time left when
 * the query starts, at least 1 ms, and a TCP retry of a truncated answer gets
 * the same share again, so a silent resolver delays a query by one share. No
 * query starts with less than 1 ms left. When every resolver is silent, one A
 * and AAAA lookup uses up to 3/4 of the time left, so names and SRV targets
 * after the first few get no query and fail with a timeout. All I/O of one
 * refresh is bounded by timeout_ms. An answer with more than 32 CNAME records
 * is treated as malformed. Hosts are deduplicated by address and port, keeping
 * the first occurrence, and returned in the same order for the same DNS answers.
 *
 * needs_periodic_refresh and supports_deadline return true. The provider
 * creates no thread; it runs in the context selected by
 * discovery.execution_mode. timeout_ms must not exceed
 * discovery.tend_refresh_deadline_ms in AS_DISCOVERY_EXECUTION_TEND, or
 * discovery.refresh_timeout_ms in AS_DISCOVERY_EXECUTION_THREAD.
 *
 * When some names resolve, their hosts are returned and each failed name is
 * logged at warning level. When no name yields a host (NXDOMAIN, SERVFAIL,
 * REFUSED, timeout, an empty or malformed answer, or no resolvable SRV target),
 * refresh returns AEROSPIKE_ERR_SEED_DISCOVERY_FAILED naming each name and its
 * failure, and the client keeps its previous seeds.
 *
 * The configuration is validated and copied, and the system resolvers are read,
 * once; later changes to the configuration or to the system resolvers have no
 * effect. An invalid value returns AEROSPIKE_ERR_PARAM with a message naming
 * it, as does finding no usable system resolver when servers is empty;
 * provider is then left untouched.
 *
 * On success the caller owns provider->udata until it is registered with
 * as_config_set_seed_provider(), which then takes ownership; release an
 * unregistered table with provider->destroy(provider->udata). Most callers
 * should use as_config_set_dns_seed_provider().
 *
 * @ingroup discovery_api
 */
AS_EXTERN as_status
as_discovery_dns_seed_provider(as_error* err, const as_dns_seed_provider_config* config,
	as_seed_provider* provider);

/**
 * Initialize an AWS EC2 seed provider configuration with its defaults: no region, filters or
 * credentials, AS_AWS_ADDRESS_PRIVATE_IP, AS_AWS_DEFAULT_PORT, no TLS name,
 * AS_AWS_DEFAULT_TIMEOUT_MS, the regional endpoint and the OpenSSL default trust.
 *
 * @ingroup discovery_api
 */
AS_EXTERN void
as_aws_seed_provider_config_init(as_aws_seed_provider_config* config);

/**
 * Fill provider with the built-in AWS EC2 seed provider named "aws-tags", which lists the
 * running EC2 instances matching the configured filters, usually a tag, and returns one seed
 * host per instance. Use it for a self-managed Aerospike cluster on EC2 whose nodes carry a
 * known tag. It sees only the instances of the credentials' own account and region, so it is
 * for self-managed clusters on EC2, not Aerospike Cloud.
 *
 * Each refresh sends one SigV4-signed EC2 Query API DescribeInstances request (API version
 * 2016-11-15) with the filters in order plus instance-state-name=running, and follows NextToken
 * pagination. It returns one host per instance with a non-empty address_field, with port and
 * tls_name, sorted by address and deduplicated. Instances without that field, or whose value is
 * longer than 255 bytes or contains whitespace or control characters, are skipped and counted
 * at debug level. An empty result is a success. Any failed page, HTTP error, timeout, response
 * whose root element is not DescribeInstancesResponse or allocation failure fails the refresh
 * with AEROSPIKE_ERR_SEED_DISCOVERY_FAILED and a message that starts with
 * "AWS EC2 seed provider: " and includes the AWS error code and request id when EC2 returns
 * them; the client then keeps its previous seeds. Seeds returned by a seed provider are used as
 * returned, unless one equals a configured seed by name and port, so rewriting provider seeds
 * needs an address translator. The secret access key, the session token and request
 * signatures are never logged or put in a message.
 *
 * Minimum IAM policy of the credentials:
 * @code
 * {
 *   "Version": "2012-10-17",
 *   "Statement": [
 *     {"Effect": "Allow", "Action": ["ec2:DescribeInstances"], "Resource": "*"}
 *   ]
 * }
 * @endcode
 *
 * Credentials: this release of the API signs with access_key_id, secret_access_key and the
 * optional session_token of the configuration, which are required.
 *
 * Threads and timeouts: a cold EC2 call takes several hundred milliseconds, more than a tend
 * can give, so the provider is thread-only: needs_periodic_refresh returns true and
 * supports_deadline false. With AS_DISCOVERY_EXECUTION_TEND,
 * as_config_set_aws_seed_provider() and aerospike_connect() fail with AEROSPIKE_ERR_PARAM, the
 * latter before any request. timeout_ms bounds every connect, TLS handshake, write, read and
 * page of one refresh and must not exceed discovery.refresh_timeout_ms; aerospike_connect()
 * fails with AEROSPIKE_ERR_PARAM otherwise. The endpoint host name is resolved with the system
 * resolver (getaddrinfo()), which timeout_ms cannot bound; every resolved address is tried in
 * order within the same timeout_ms, so an address that never answers uses up the time left for
 * the next ones. The provider creates no thread, timer or pooled connection: each request uses
 * a new connection that is closed when the response is read. No HTTP proxy is used and
 * redirects are not followed.
 *
 * HTTPS: TLS 1.2 or later with SNI, the peer certificate verified against ca_file and ca_path
 * when either is set, otherwise against the OpenSSL default verify paths (and SSL_CERT_FILE or
 * SSL_CERT_DIR) plus, when those find no certificate file, the first existing system CA bundle
 * (see as_aws_seed_config.ca_file), and the endpoint host matched against its subject
 * alternative names. On Windows the OpenSSL defaults are not the system certificate store; set
 * ca_file there. The TLS context is created, ca_file and ca_path loaded, and SIGPIPE set to
 * SIG_IGN process-wide when it still has the default handler (as cluster TLS does, because
 * OpenSSL can raise it while writing) for any endpoint scheme, including http.
 *
 * The provider keeps state across refreshes, so a provider value belongs to exactly one
 * registration: register it with one config only.
 *
 * The configuration is validated and copied, the endpoint parsed and the TLS context created
 * once; later changes to the configuration have no effect. An invalid value returns
 * AEROSPIKE_ERR_PARAM with a message naming it, an unreadable ca_file or ca_path
 * AEROSPIKE_ERR_TLS_ERROR, and an allocation failure AEROSPIKE_ERR_CLIENT; provider is then
 * left untouched.
 *
 * On success the caller owns provider->udata until it is registered with
 * as_config_set_seed_provider(), which then takes ownership; release an unregistered table with
 * provider->destroy(provider->udata). Most callers should use
 * as_config_set_aws_seed_provider().
 *
 * @ingroup discovery_api
 */
AS_EXTERN as_status
as_discovery_aws_seed_provider(as_error* err, const as_aws_seed_provider_config* config,
	as_seed_provider* provider);

/**
 * Complete one translation: apply the miss policy, validate the mapping and
 * produce the effective endpoint. This is the single implementation shared by
 * the cluster and the conformance entry point.
 *
 * If mapped is true, mapped_host holds the translator output; it is always
 * consumed (destroyed) by this call, and any tls_name in it is ignored. If
 * mapped is false it is a miss: AS_TRANSLATION_PASS_THROUGH uses the advertised
 * host and port, AS_TRANSLATION_STRICT returns AEROSPIKE_ERR_TRANSLATION_MISS
 * naming translator_name, the node, the advertised host:port and the source
 * list.
 *
 * On AEROSPIKE_OK, out receives a copy of the effective host and port with a
 * copy of endpoint->tls_name; the caller destroys it with as_host_destroy().
 * An invalid mapped host or port (empty, over-long, port zero) returns
 * AEROSPIKE_ERR_CLIENT.
 * @ingroup discovery_api
 */
AS_EXTERN as_status
as_discovery_complete_translation(as_error* err, const char* translator_name,
	as_translation_miss_policy on_miss, const as_endpoint* endpoint, bool mapped,
	as_host* mapped_host, as_host* out);

/**
 * Merge static and discovered hosts per policy into output, which must be
 * initialized by the caller and receives copies. init is true for the result
 * obtained at client initialization and false for later refreshes. Duplicates
 * (same name and port) are removed, keeping the first occurrence. An empty
 * output after initialization means the previous seeds are kept.
 *
 * @ingroup discovery_api
 */
AS_EXTERN bool
as_discovery_merge_hosts(as_seed_merge_policy policy, bool init, const as_vector* static_hosts,
	const as_vector* discovery_hosts, as_seed_candidates* output);

/**
 * Conformance entry point for one translate row, evaluated against the C
 * built-ins without a cluster. On AEROSPIKE_OK, out receives the copied
 * effective endpoint the client would connect to: the translated or (on a
 * pass-through miss) advertised host and port, with the endpoint tls_name. The
 * caller destroys out with as_host_destroy().
 *
 * Returns AEROSPIKE_ERR_TRANSLATION_MISS for a strict miss and
 * AEROSPIKE_ERR_PARAM for invalid input, including an invalid port-map entry.
 * AEROSPIKE_ERR_UNSUPPORTED_FEATURE is reserved for translator kinds the build
 * does not implement.
 *
 * @ingroup discovery_api
 */
AS_EXTERN as_status
as_discovery_conformance_translate(as_error* err, const as_conformance_translation* translation,
	const as_endpoint* endpoint, as_host* out);

/**
 * Conformance entry point for one merge row. results[0] is the discovery result
 * at initialization and each later entry is a periodic refresh; a NULL entry is
 * a failed refresh. snapshots must have results_size elements; on AEROSPIKE_OK
 * each is initialized with the seed snapshot published after the corresponding
 * result, and the caller destroys each with as_seed_candidates_destroy(). A
 * failed refresh, or an empty merge after initialization, keeps the previous
 * snapshot. On error no snapshot is left allocated.
 *
 * @ingroup discovery_api
 */
AS_EXTERN as_status
as_discovery_conformance_merge(as_error* err, as_seed_merge_policy policy,
	const as_vector* static_hosts, const as_vector* const* results, uint32_t results_size,
	as_seed_candidates* snapshots);

#ifdef __cplusplus
} // end extern "C"
#endif
