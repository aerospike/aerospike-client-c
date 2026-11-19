# Pluggable discovery: plugin guide

This guide covers the C client's pluggable seed discovery and address translation: the built-in
plugins, how to write and register your own, which thread calls each callback, how seeds are
merged, and how TLS, AWS and load balancers interact with them. The YAML keys that select the
built-ins are listed in [YAML configuration: discovery and translation](yaml-config-discovery.md).
The API reference is in `as_discovery.h` and `as_config.h` (the "Pluggable Discovery" group of the
API docs).

## Model

- A **seed provider** returns the seed hosts the client tries when it initializes the cluster and
  when it reseeds because no node is active. Cluster membership still comes from the servers: the
  provider only chooses where the client starts.
- An **address translator** rewrites each endpoint a server advertises (in `peers-*` and
  `service-*` replies) into the host and port the client connects to.
- **Configured seeds** (static seeds) are the hosts added with `as_config_add_host()`,
  `as_config_add_hosts()` or `as_config_tls_add_host()`. **Discovered seeds** are the hosts a
  provider returns. The merge policy combines them into the seed list.
- Each refresh result is published as an immutable **seed snapshot**; a reseed reads the latest
  snapshot and never calls the provider.

Plugins run in-process with no isolation: a crash, hang or memory error in plugin code is a crash,
hang or memory error in the client. Plugins must not create threads, tasks, timers or any other
scheduling resource of their own; the client owns all scheduling.

## Built-ins

| Plugin | Kind | Register in code | YAML |
| --- | --- | --- | --- |
| `static` | provider: copies the configured seeds, in order, once | default (nothing to register) | `discovery.provider: static` |
| `dns` | provider: resolves DNS names (A/AAAA or SRV) on every refresh | `as_config_set_dns_seed_provider()` | `discovery.provider: dns` |
| `aws-tags` | provider: lists running EC2 instances matching filters (usually a tag) | `as_config_set_aws_seed_provider()` | `discovery.provider: aws-tags` |
| static map | translator: exact, host-only map over `as_config.ip_map`; the port is kept | default (set `ip_map`) | `ip_map`, or `translation.provider: static-map` with `static_map` |
| `port-map` | translator: maps an advertised `host:port`, or a node name, to a `host:port` | `as_config_set_port_map_translator()` | `translation.provider: port-map` |
| `pass-through` | translator: returns every advertised endpoint unchanged as a mapping | `as_config_set_pass_through_translator()` | `translation.provider: pass-through` |

The static map is the default translator, and the only one that also translates seeds: configured
seeds and `as_cluster_add_seed()` hosts, at initialization and on every reseed, as `ip_map` always
has. A seed returned by a provider is translated only when it equals a configured seed by name and
port; other provider seeds are used as returned.

While any other translator is registered, `ip_map` is ignored, configured seeds included, and
`aerospike_connect()` logs a warning. No translator then applies to seeds: configured and
discovered seeds are connected to as they are, and only the endpoints the servers advertise are
translated. Every seed must therefore be reachable from the client as configured or as returned.

## Registration and ownership

Register a plugin on the `as_config` before `aerospike_init()` or `aerospike_new()`, or on
`as->config` before `aerospike_connect()`:

| Function | Registers |
| --- | --- |
| `as_config_set_seed_provider()` | a custom `as_seed_provider` table |
| `as_config_set_address_translator()` | a custom `as_address_translator` table |
| `as_config_set_dns_seed_provider()`, `as_config_set_aws_seed_provider()` | a built-in provider from its configuration |
| `as_config_set_port_map_translator()`, `as_config_set_pass_through_translator()` | a built-in translator |
| `as_config_set_binding_seed_provider()` | a provider implemented by a language binding; see [Language bindings](#language-bindings) |
| `as_config_register_seed_provider()`, `as_config_register_address_translator()` | a factory under a name, which the config file (`provider: <name>`) or a select call picks; registering builds nothing; see [Registered providers and translators](yaml-config-discovery.md#registered-providers-and-translators) |
| `as_config_select_seed_provider()`, `as_config_select_address_translator()` | the instance a registered factory builds from the options passed, as the config file would |

Rules:

- The function table is validated (`size`, `api_version`, a non-empty `name` and the required
  callbacks) and copied, `name` included. On success the config owns `udata` and calls `destroy`
  exactly once; on failure `udata` stays with the caller.
- A new registration replaces the previous one and destroys it, unless it has the same non-NULL
  `udata`, which destroys nothing.
- Registration fails with `AEROSPIKE_ERR_PARAM` while a client created from the config is
  connected.
- A YAML provider or translator replaces, and destroys, the code registration, and logs a
  warning naming both; see [YAML configuration](yaml-config-discovery.md#staticclientdiscovery).

Lifecycle, which differs from the Java client:

- **Sync client** (no async event loops, or libevent single-thread mode): `aerospike_close()`
  destroys every provider and translator instance. `aerospike_connect()` on the same `aerospike`
  object builds again the ones the client knows how to build: the built-ins set with their
  setters, and the plugins selected by the config file or by `as_config_select_*()`, whose
  factory is called again with the same options. A rebuild or factory error fails that connect.
  A table registered directly (`as_config_set_seed_provider()`,
  `as_config_set_address_translator()`, `as_config_set_binding_seed_provider()`) cannot be
  rebuilt: connect fails with `Seed provider was released by aerospike_close(). Register it
  again` (or the translator equivalent) until it is registered again. Registered factories stay
  until `aerospike_destroy()`.
- **Async client**: `aerospike_close()` releases every registration, factories and selections
  included, whether the client was connected or not. A later `aerospike_connect()` on the same
  object fails with the same message until a plugin is registered again.

When a refresh is still running as the registration ends, `destroy` runs later on the discovery
thread, after the refresh returns; see [Thread context](#thread-context).

## Execution modes

`as_config.discovery.execution_mode` (YAML `discovery.execution_mode`) selects where a periodic
provider is refreshed:

- `AS_DISCOVERY_EXECUTION_THREAD` (`thread`, default): one client-owned discovery thread, started
  only when the provider needs periodic refresh or is a binding provider. `aerospike_connect()`
  waits at most `refresh_timeout_ms` (default 10000) for the initial result.
- `AS_DISCOVERY_EXECUTION_TEND` (`tend`): the initial refresh runs on the connecting thread and later
  refreshes on the tend thread, every `refresh_interval_ms / tender_interval` tends, within
  `tend_refresh_deadline_ms` (default 100). The deadline is measured, not enforced: a late refresh
  delays that tend and its result is discarded, and after three consecutive overruns the provider is
  no longer refreshed and the initialization seeds are restored until the client is recreated.

Nothing preempts a `tend` refresh: the tend thread waits for it to return. So a custom provider
that declares `supports_deadline` and then overruns blocks the tend thread for the whole call, up
to three consecutive times before it is switched off, and a call that never returns blocks it for
good. While tend is blocked, the client does not refresh nodes, peers or partition maps, reseed,
balance connections or reload the config file, so commands keep using the last cluster view, even
after a node fails or partitions move. Most commands do not wait for tend, but a command whose new
connection fails authentication (for example after the session expired) waits for the current tend
to end, which on an event loop holds up that loop, and `aerospike_close()` waits for it too.
Return true from `supports_deadline` only for a refresh that cannot block, such as one that reads
memory.

The mode matters only for a periodic provider. A non-periodic provider other than a binding is
called once, on the connecting thread, in either mode, and no thread starts.

| Provider | Periodic | Supports a deadline | Modes |
| --- | --- | --- | --- |
| `static` | no | yes | Either; no thread and no periodic work. |
| `dns` | yes | yes | `thread` (default) or `tend`. In `tend`, `timeout_ms` must not exceed `tend_refresh_deadline_ms`, and at the default 100 ms only a local resolver answers reliably. In `thread`, `timeout_ms` must not exceed `refresh_timeout_ms`. |
| `aws-tags` | yes | no | `thread` only. `as_config_set_aws_seed_provider()` and `aerospike_connect()` fail in `tend`. |
| binding | as registered | no | `thread` only, periodic or not; a non-periodic binding runs its single call on the discovery thread. Registration fails in `tend`, and switching to `tend` afterwards fails `aerospike_connect()`. |
| custom | `needs_periodic_refresh` | `supports_deadline` | `thread` (default) or `tend`; `tend` requires `supports_deadline` to return true. |

`aws-tags` is thread-only because of latency: a cold EC2 call takes several hundred milliseconds,
and on real AWS about 10% of warm calls took longer than 80 ms and about 3% longer than the 100 ms
tend deadline. In `tend` mode such a provider would keep overrunning and be switched off.

`aerospike_connect()` checks the refresh settings of a periodic provider and fails with
`AEROSPIKE_ERR_PARAM` when `refresh_interval_ms` is below the tend interval, when
`tend_refresh_deadline_ms` is 0 or above half the tend interval in `tend`, or when
`refresh_timeout_ms` is 0 in `thread`. The messages are listed in the
[YAML reference](yaml-config-discovery.md#staticclientdiscovery).

## Thread context

`as_seed_provider`:

| Callback | Thread | Rules |
| --- | --- | --- |
| `refresh` | A non-periodic, non-binding provider: once, on the application thread that connects. `thread` mode with a periodic provider, and every binding provider: every call, the initial one included, on the discovery thread. `tend` mode: the initial call on the connecting thread, later calls on the tend thread. | Within one `aerospike` object no two calls overlap, across close, re-registration and reconnect included. In `tend`, return within `tend_refresh_deadline_ms`. |
| `needs_periodic_refresh` | Once per connect, on the connecting thread. | Must return without blocking or waiting for another thread. |
| `supports_deadline` | Once per connect, on the connecting thread. Optional. | Must return without blocking or waiting for another thread. |
| `destroy` | The thread that ends the registration or the instance (`aerospike_destroy()`, `as_config_destroy()`, a replacing setter, or `aerospike_close()`, sync or async), or the discovery thread when a refresh is still running, once it returns. Optional. | Must never block or wait for another thread, because that thread may be the caller. It may run after `aerospike_close()` or `aerospike_destroy()` has returned. |

`as_address_translator`:

| Callback | Thread | Rules |
| --- | --- | --- |
| `translate` | The tend thread, or the application thread during cluster initialization in `aerospike_connect()`. | Synchronous and in memory: no I/O, DNS included, and no blocking. Target p99 below 1 ms. |
| `on_cluster_view_changed` | As `translate`. Optional. | As `translate`. |
| `destroy` | As the provider's `destroy`. Optional. | As the provider's `destroy`. |

`translate` running on the application thread during initialization departs from the spec, which
names the tend thread only. The Java client does the same while it seeds the cluster.

The `aws-tags` credentials hook runs its `get` on the discovery thread with the time left of the
refresh's `timeout_ms`, and its `destroy` follows the provider `destroy` rules.

Close and reconnect bounds for `thread` mode: `aerospike_close()` waits at most
`refresh_timeout_ms` for a running refresh, and `aerospike_connect()` on the same object waits at
most `refresh_timeout_ms` for a refresh left running by the previous connection; see
[Status codes](#status-codes).

## Merge policies

`as_config.discovery.merge_policy` (YAML `discovery.merge_policy`) combines the configured seeds
and the discovered seeds. Duplicate hosts (same name and port) are removed, keeping the first
occurrence.

| Policy | At initialization | On a later refresh | Use when |
| --- | --- | --- | --- |
| `AS_SEED_MERGE` (`merge`, default) | Configured seeds, then discovered seeds. | The same: configured seeds, then the new discovered seeds. | Migrating to discovery, resilience, and keeping a known-good endpoint while discovery catches up. |
| `AS_SEED_REPLACE` (`replace`) | As `merge`. | The discovered seeds replace the list. An empty result is published as an empty list. | Greenfield deployments where the configured seeds are legacy or bootstrap only. |
| `AS_SEED_DISCOVERY_ONLY` (`discovery_only`) | Discovered seeds only; the configured seeds when discovery returns an empty result or fails. | Discovered seeds only; the configured seeds when the result is empty. A failed refresh keeps the previous seeds. | The configured seeds are an emergency fallback only. |

For every policy:

- A failed refresh after initialization keeps the previous seeds. A successful refresh is published
  even when its merged list is empty. While the list is empty and the cluster has no node, every
  tend fails with `No seed host to connect to`, and the client reconnects only after a refresh
  returns a seed: at most `refresh_interval_ms` (default 30 s) after the provider starts returning
  hosts again, plus any delay of the provider's own, such as EC2's eventual consistency.
- The built-in providers reach an empty list differently. `aws-tags` returns an empty result when
  no running instance matches its filters, so `replace`, and `merge` or `discovery_only` without
  configured seeds, publish an empty list. `dns` reports a failure when no name resolves, so the
  previous seeds are kept. To keep a fallback endpoint while discovery returns nothing, use `merge`
  or `discovery_only` with configured seeds.
- At initialization a failed refresh counts as an empty result. When the merged list is then empty,
  `aerospike_connect()` fails with `AEROSPIKE_ERR_SEED_DISCOVERY_FAILED`. A successful empty result
  is not a discovery failure: with no configured seeds, `aerospike_connect()` fails with
  `AEROSPIKE_ERR_CLIENT` and `No seed host to connect to`.
- **Late initial refresh** (`thread` mode): when the initial refresh does not return within
  `refresh_timeout_ms`, `aerospike_connect()` continues with the configured seeds alone (every policy
  gives them, because there is no discovered host yet) and fails with
  `AEROSPIKE_ERR_SEED_DISCOVERY_FAILED` only when there are none. When the late result arrives, it is
  published merged as at initialization, so with `replace` the configured seeds are still included
  until the next refresh. A late failure keeps the configured seeds.
- `as_discovery_merge_hosts()` is the merge function the client uses, and
  `as_discovery_conformance_merge()` runs a whole sequence of results; see
  [Conformance entry points](#conformance-entry-points).

## Translation misses

`as_config.translation.on_miss` (YAML `translation.on_miss`) applies when a translator returns
false (no mapping) for an advertised endpoint:

- `AS_TRANSLATION_PASS_THROUGH` (`pass_through`, default): the advertised endpoint is used
  unchanged.
- `AS_TRANSLATION_STRICT` (`strict`): the endpoint is dropped. A peer left without an endpoint is
  not added; the miss is logged at warning level as an `AEROSPIKE_ERR_TRANSLATION_MISS` message naming
  the translator, the node, the advertised `host:port` and the source list, and the peer is retried
  on every tend. A miss alone does not fail `aerospike_connect()`.

Any other `on_miss` value fails `aerospike_connect()` with `AEROSPIKE_ERR_PARAM`.

A translator returns false for a miss and leaves the policy to the client; it does not implement
either behaviour itself. The `pass-through` built-in always maps, so it succeeds even with
`strict`.

Two peers translated to the same `host:port` collide: each colliding peer is logged with an
`AEROSPIKE_ERR_TRANSLATION_COLLISION` message and is not added, and an existing live node is kept.

## TLS identity

Translation changes only the transport host and port. TLS always authenticates the node as its
`tls_name` from the server's `peers-*` reply (`as_endpoint.tls_name`); any `tls_name` a translator
sets in its output is ignored. So a certificate is matched against the node's TLS name, never
against the host the translator returned. Discovered seeds without a TLS name get the same
default as configured seeds when TLS is enabled: the cluster name when `as_config.cluster_name` is
set, otherwise the host name.

The client matches the TLS name against the server certificate per RFC 9525:

- A TLS name that is an IPv4 or IPv6 literal matches only an iPAddress subject alternative name
  (SAN) entry with the same address. A name shaped like an IP address that is not a valid one (it
  contains a colon, or only digits and dots, such as `010.0.0.1` or `1.2.3`) matches no entry.
- Any other TLS name matches a dNSName SAN entry, compared case-insensitively (ASCII). The only
  wildcard is a whole left-most label of `*`, matching exactly one non-empty label:
  `*.example.com` matches `a.example.com`, but not `example.com` or `a.b.example.com`.
- The subject CN is not an identifier.

**Deprecated fallback.** When the RFC 9525 match fails, the client still applies its previous
matching, unchanged, and accepts the certificate when that matches:

- the TLS name against the certificate's subject CN, even when the certificate has SAN entries;
- a TLS name that is an IP address, or shaped like one without being valid (see above), compared
  as text, exactly or through a `*.` wildcard, against dNSName SAN entries or the subject CN, where
  the RFC 9525 rule compares IP addresses with iPAddress entries only.

A fallback match logs one warning per TLS name per process:

```
TLS certificate matched 'node1' only through the legacy name check, which is not RFC 9525 compliant (for example CN matching or non-conforming wildcards). This fallback is deprecated and will be rejected in the next major release. Issue a certificate with a matching SAN dNSName or iPAddress entry
```

The fallback is removed in the next major release. Issue certificates with a SAN dNSName entry for
a host-name TLS name, or an iPAddress entry for an IP TLS name.

## aws-tags IAM policy and credentials

The `aws-tags` provider sends one SigV4-signed EC2 `DescribeInstances` request per refresh (plus
pagination) and returns one host per running instance that matches the filters. It sees only the
instances of the credentials' own account and region, so it is for self-managed Aerospike clusters
on EC2, not Aerospike Cloud.

Minimum IAM policy of the credentials:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Action": ["ec2:DescribeInstances"], "Resource": "*"}
  ]
}
```

Credentials: a credentials hook (`as_aws_seed_provider_config.credentials_hook`), when set,
replaces the chain. Otherwise the first source found, in this order, is used:

1. `access_key_id`, `secret_access_key` and `session_token` of the configuration (code only);
2. the environment variables `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`;
3. the static keys of profile `AWS_PROFILE` (default `default`) in the shared credentials file
   `AWS_SHARED_CREDENTIALS_FILE` or `~/.aws/credentials`;
4. EKS web identity (`AWS_WEB_IDENTITY_TOKEN_FILE` and `AWS_ROLE_ARN`), through STS
   `AssumeRoleWithWebIdentity`;
5. ECS container credentials (`AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` or
   `AWS_CONTAINER_CREDENTIALS_FULL_URI`);
6. the EC2 instance profile through IMDSv2, unless `AWS_EC2_METADATA_DISABLED` is `true`.

SSO, `credential_process`, `role_arn` profiles and cross-account roles are not in the chain; use
the hook, which may wrap the application's own AWS SDK credentials provider. The environment and
the home directory are read once, when the provider is built. The source is logged once at info
level as `AWS EC2 seed provider credentials from <label>`; no key, token or role name is logged.
The full contract, including HTTPS trust (`ca_file`, `ca_path`) and the hook rules, is documented on
`as_discovery_aws_seed_provider()` in `as_discovery.h`.

## Language bindings

`as_config_set_binding_seed_provider()` registers a provider implemented in a binding's runtime
(Python, Node.js) as a C callback:

- It runs in `thread` mode only: registration fails while `execution_mode` is `tend`, and
  `aerospike_connect()` fails if the mode is switched to `tend` afterwards.
- Every `refresh` call, the initial one included, runs on the discovery thread, periodic or not,
  never on the connecting or tend thread. `periodic` is passed at registration instead of a
  `needs_periodic_refresh` callback, so the client never calls into the runtime to decide it.
- Blocking budget: `aerospike_connect()` waits at most `refresh_timeout_ms` for the initial result,
  and `aerospike_close()` at most `refresh_timeout_ms` for a running refresh. `refresh` may wait for
  the runtime (for example acquire the Python GIL); `destroy` must not.
- Error contract: return hosts copied with `as_seed_candidates_append()` and `AEROSPIKE_OK`, or a
  non-OK status with `err` set; the client then keeps the previous seeds, and at initialization
  falls back to the configured seeds. A result with an empty host name, a host name over 255
  bytes or port 0 fails the same way, except for a configured seed returned as given.
- The thread that calls `aerospike_connect()`, `aerospike_close()` or `aerospike_destroy()` must not
  hold anything the callback needs. Node.js calls `aerospike_connect()` from a worker thread; close
  and destroy may run on the JS main thread while its loop is the registered event loop, because
  close then waits for a running refresh on a client thread, which also completes the destroy.
  Python releases the GIL around all three.
- A binding translator is not supported.

## Conformance entry points

Three functions run rows of the shared conformance fixtures against the C built-ins without a
cluster, so a binding built on the C client can run the translate, merge and collision rows
directly:

- `as_discovery_conformance_translate()`: one translate row. Given a built-in translator kind
  (`AS_TRANSLATOR_STATIC_MAP`, `AS_TRANSLATOR_PORT_MAP`, `AS_TRANSLATOR_PASS_THROUGH`), its map, an
  `on_miss` policy and an advertised endpoint, it returns the effective host and port the client
  would connect to, with the endpoint's TLS name, or `AEROSPIKE_ERR_TRANSLATION_MISS` for a strict
  miss.
- `as_discovery_conformance_merge()`: one merge row. Given a merge policy, the configured seeds and a
  sequence of discovery results (the first at initialization, a NULL entry for a failed refresh), it
  returns the seed snapshot published after each result.
- `as_discovery_conformance_collision()`: one collision row. Given a translator and the peers of one
  peers reply, it returns the node names that become nodes and one
  `AEROSPIKE_ERR_TRANSLATION_COLLISION` message per colliding peer, through the same collision code
  the cluster uses.

They share their implementation with the cluster (`as_discovery_complete_translation()` and
`as_discovery_merge_hosts()`). The client's own runner in `src/test/conformance` runs the fixture
rows through them.

## Status codes

| Code | Name | Returned by |
| --- | --- | --- |
| -25 | `AEROSPIKE_ERR_TRANSLATION_MISS` | A strict miss: logged for a peer that cannot be added, returned by the conformance entry points, and returned by `aerospike_connect()` instead of the generic connection error when initialization fails for another reason after a miss was recorded. |
| -26 | `AEROSPIKE_ERR_TRANSLATION_COLLISION` | Two peers translated to one `host:port`: logged for each colliding peer, and returned in the same way as -25. |
| -27 | `AEROSPIKE_ERR_SEED_DISCOVERY_FAILED` | `aerospike_connect()`, when the initial refresh fails or times out and no configured seed is left to fall back on. |

The `dns` and `aws-tags` providers fail a refresh with `AEROSPIKE_ERR_CLIENT` (-1). After
initialization the failure only reaches the log, and the previous seeds are kept. A reconnect
while a refresh left running by the previous connection of the same `aerospike` object has not
returned within `refresh_timeout_ms` also fails with `AEROSPIKE_ERR_CLIENT`; the message names
the provider, and nothing is started.

Invalid discovery and translation settings, at registration or in `aerospike_connect()`, fail
with `AEROSPIKE_ERR_PARAM` (-2).

## Differences from the Java client

The Java client is the reference for the plugin interfaces and the configuration. The C client
differs in these points:

| Topic | C client | Java client |
| --- | --- | --- |
| Validation errors | `AEROSPIKE_ERR_PARAM` (-2). | `CLIENT_ERROR` (-1). |
| Plugins across close | A sync `aerospike_close()` destroys every instance, and reconnect rebuilds the built-in, file-selected and code-selected ones; a table registered directly must be registered again. An async client releases every registration at close; see [Registration and ownership](#registration-and-ownership). | The provider is closed when the client closes; a translator has no close method. |
| Selecting a registered name from code | `as_config_select_seed_provider()` and `as_config_select_address_translator()` call the factory at once, and again on every reconnect after a sync close. | Only the config file selects a registered name; the factory runs once, when the client is constructed. |
| YAML factory options | Typed as SnakeYAML types them, with a few differences; see [Option values](yaml-config-discovery.md#option-values). | SnakeYAML 2.6. |
| YAML aliases and merge keys | An alias to a collection works only in factory options and skipped content. Merge keys work only in factory options and directly under the blocks; anywhere else the client parses, they make the file invalid. See [Anchors, aliases and merge keys](yaml-config-discovery.md#anchors-aliases-and-merge-keys). | Applied everywhere. |
| `ip_map` | Also rewrites configured seeds, and is ignored with a warning while another translator is registered; see [Built-ins](#built-ins). | Applies to the addresses servers advertise; no warning. |
| Static `use_services_alternate` on reload | Not re-applied; only `dynamic.client.use_service_alternative` changes a running client. | Re-applied. |
| aws-tags timeouts | One `timeout_ms`, default 5000; `init_timeout_ms` is accepted and ignored. | `timeout_ms` 2000, and `init_timeout_ms` 10000 for the first refresh. |

## Connection idle behind load balancers

AWS Network Load Balancer TCP listeners and PrivateLink endpoints drop a flow that has been idle for
350 seconds by default. An idle pooled connection that outlives the flow fails on its next use, so
the client must close idle connections before that.

With the defaults (`max_socket_idle` 0, `min_conns_per_node` 0, `async_min_conns_per_node` 0,
`tender_interval` 1000), the client already does:

- Checkout does not check idle time.
- Every `AS_CLUSTER_BALANCE_TENDS` (30) tends, the tend thread closes pooled connections idle longer
  than `AS_CLUSTER_IDLE_TRIM_DEFAULT_SEC` (55 seconds), from the least recently used end, down to the
  minimum count. In libevent single-thread mode, async connections are trimmed every 30 seconds by a
  timer in each event loop where the application called `as_event_loop_register_aerospike()`.
- So an idle connection is closed within about 55 s + 30 × `tender_interval`: 85 seconds at the
  default 1000 ms, plus the time the tends themselves take. That is well under 350 seconds.

Set `max_socket_idle` (YAML `dynamic.client.max_socket_idle`) below the load balancer's idle timeout,
for example 340, when any of these holds:

- `min_conns_per_node` or `async_min_conns_per_node` is above 0: the reserved connections are never
  trimmed below the minimum, however long they are idle.
- Pipelining is used: pipeline connections are never trimmed.
- The NLB idle timeout is lowered below about 85 seconds (it can be set as low as 60).
- `tender_interval` is above about 9.8 seconds, where 55 + 30 × `tender_interval` exceeds 350
  seconds. The tend thread keeps the interval it started with: YAML `dynamic.client.tend_interval`
  counts when the client is created, and a value changed by a reload does not change the tend
  period.
- libevent single-thread mode runs without `as_event_loop_register_aerospike()` in every event loop,
  where async connections are never trimmed.

With `max_socket_idle` set, checkout discards any connection idle longer than it (sync, async and
pipeline), and the tend trim uses it too, so any value below the load balancer's timeout is safe,
reserved connections included. This overrides the advice on `min_conns_per_node` and
`async_min_conns_per_node` to keep `max_socket_idle` at 0 for clients behind a load balancer.

## Deployment limits (AWS)

This limit comes from the load balancer, not from the client: no translator limits the number of
nodes.

A Network Load Balancer has at most 50 listeners, and each listener serves a single port. A layout
that gives every node its own port on one endpoint therefore reaches at most 50 nodes per endpoint,
whether clients reach the load balancer directly or through an AWS PrivateLink endpoint service.
This applies to:

- `pass-through` with `alternate-access-port`, where every node advertises the endpoint's name and
  its own port;
- `port-map`, where every node maps to the endpoint's name and its own port.

Above 50 nodes, spread the nodes over several endpoint services, each with its own load balancer and
endpoint name, or use VPC Lattice resource endpoints. This limit and the idle teardown above have not
yet been exercised against AWS by the client's test suite.

## Example

A provider that returns a host list the application updates, for example from a service-registry
callback it already runs, and a translator over a fixed NAT table. `refresh` only reads memory, so
the provider supports a deadline and can run in either mode.

```c
typedef struct {
	pthread_mutex_t lock;
	as_vector hosts; // as_host, updated by the application
} host_list;

static as_status
host_list_refresh(as_error* err, const as_seed_candidate_context* context,
	as_seed_candidates* candidates, void* udata)
{
	host_list* list = udata;
	as_status status = AEROSPIKE_OK;

	pthread_mutex_lock(&list->lock);

	for (uint32_t i = 0; i < list->hosts.size; i++) {
		as_host* host = as_vector_get(&list->hosts, i);

		if (!as_seed_candidates_append(candidates, host->name, host->tls_name, host->port)) {
			status = as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to append host");
			break;
		}
	}
	pthread_mutex_unlock(&list->lock);
	return status;
}

static bool
host_list_true(void* udata)
{
	return true;
}

static void
host_list_destroy(void* udata)
{
	// Release the list. Never block here.
}

typedef struct {
	const char* advertised; // "host:port"
	const char* host;
	uint16_t port;
} nat_entry;

static bool
nat_translate(const as_endpoint* endpoint, as_host* out, void* udata)
{
	const nat_entry* entry = udata;
	char key[300];
	snprintf(key, sizeof(key), "%s:%u", endpoint->host, endpoint->port);

	for (; entry->advertised; entry++) {
		if (strcmp(entry->advertised, key) == 0) {
			as_host_copy_fields(out, entry->host, NULL, entry->port);
			return true;
		}
	}
	return false; // A miss: translation.on_miss applies.
}

as_status
register_plugins(as_config* config, host_list* list, const nat_entry* table, as_error* err)
{
	as_seed_provider provider = {
		.size = sizeof(as_seed_provider),
		.api_version = AS_DISCOVERY_API_VERSION,
		.name = "host-list",
		.refresh = host_list_refresh,
		.needs_periodic_refresh = host_list_true,
		.supports_deadline = host_list_true,
		.destroy = host_list_destroy,
		.udata = list
	};

	as_address_translator translator = {
		.size = sizeof(as_address_translator),
		.api_version = AS_DISCOVERY_API_VERSION,
		.name = "nat-table",
		.translate = nat_translate,
		.udata = (void*)table
	};

	if (as_config_set_seed_provider(config, &provider, err) != AEROSPIKE_OK) {
		return err->code; // The config did not take list: the caller still owns it.
	}

	if (as_config_set_address_translator(config, &translator, err) != AEROSPIKE_OK) {
		return err->code;
	}
	config->translation.on_miss = AS_TRANSLATION_STRICT;
	return AEROSPIKE_OK;
}
```
