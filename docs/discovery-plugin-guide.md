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
| `as_config_set_seed_provider_factory()`, `as_config_set_address_translator_factory()` | a custom plugin built by a factory, which a sync close and reconnect call again, so the plugin need not be registered again; see [Factories without a name](#factories-without-a-name) |
| `as_config_set_dns_seed_provider()`, `as_config_set_aws_seed_provider()` | a built-in provider from its configuration |
| `as_config_set_port_map_translator()`, `as_config_set_pass_through_translator()` | a built-in translator |
| `as_config_set_binding_seed_provider()` | a provider implemented by a language binding, built by a factory that is called again after a sync close; see [Language bindings](#language-bindings) |
| `as_config_register_seed_provider()`, `as_config_register_address_translator()` | a factory under a name, which the config file (`provider: <name>`) or a select call picks; registering builds nothing; see [Registered providers and translators](yaml-config-discovery.md#registered-providers-and-translators) |
| `as_config_register_binding_seed_provider()` | a language-binding provider factory under a name, picked the same way; the instance it builds is a binding provider; see [Language bindings](#language-bindings) |
| `as_config_select_seed_provider()`, `as_config_select_address_translator()` | the instance a registered factory builds from the options passed, as the config file would |

Rules:

- The function table is validated (`size`, `api_version`, a non-empty `name` and the required
  callbacks) and copied, `name` included. On success the config owns `udata` and calls `destroy`
  exactly once; on failure `udata` stays with the caller.
- A new registration replaces the previous one and destroys it, unless it is a table registered
  directly with the same non-NULL `udata`, which destroys nothing. An instance built by a factory
  is always destroyed when it is replaced, even if the new one has the same `udata` pointer.
- Registration fails with `AEROSPIKE_ERR_PARAM` while a client created from the config is
  connected.
- A YAML provider or translator replaces, and destroys, the code registration, and logs a
  warning naming both; see [YAML configuration](yaml-config-discovery.md#staticclientdiscovery).

Lifecycle, which differs from the Java client:

- **Sync client** (no async event loops, or libevent single-thread mode): `aerospike_close()`
  destroys every provider and translator instance. `aerospike_connect()` on the same `aerospike`
  object builds again the ones the client knows how to build: the built-ins set with their
  setters, the plugins selected by the config file or by `as_config_select_*()`, whose
  factory is called again with the same options, and the plugins set with
  `as_config_set_*_factory()` or `as_config_set_binding_seed_provider()`, whose factory is
  called again. A rebuild or factory error fails that connect, and the next connect tries again.
  A table registered directly (`as_config_set_seed_provider()`,
  `as_config_set_address_translator()`) cannot be rebuilt: connect fails with `Seed provider was
  released by aerospike_close(). Register it again` (or the translator equivalent) until it is
  registered again. Registered factories stay until `aerospike_destroy()`.
- **Async client**: `aerospike_close()` releases every registration, factories and selections
  included, whether the client was connected or not. A later `aerospike_connect()` on the same
  object fails with the same message until a plugin is registered again.

`destroy` in a table is C's counterpart of `close()` in the Java client's plugin interface: the
client never frees plugin memory itself, and the callback releases the plugin's own `udata`. For a
rebuilt instance it ends only the instance of that connection.

When a refresh is still running as the registration ends, `destroy` runs later on the discovery
thread, after the refresh returns; see [Thread context](#thread-context).

### Factories without a name

`as_config_set_seed_provider_factory(config, factory, destroy, udata, err)` and
`as_config_set_address_translator_factory()` take an `as_seed_provider_factory` or
`as_address_translator_factory` instead of a table. Use them for a custom plugin that should
survive close and reconnect:

- The factory is called at once, with an empty options map, and again by `aerospike_connect()`
  after each sync `aerospike_close()`. A factory error at registration returns its status with
  `factory failed: ...` and keeps the previous registration; at reconnect it fails that connect
  with `Failed to rebuild the seed provider: factory failed: ...` (or the translator equivalent).
- On success the registration owns `udata` and calls `destroy(udata)` once, when the registration
  ends: it is replaced, `aerospike_destroy()` or `as_config_destroy()` runs, or an async client is
  closed. It runs once per successful registration, also for the same `udata` registered again.
  On failure the caller keeps `udata`.
- Each instance holds its own reference to the state it shares with `udata`, because the instance
  `destroy` ends only that connection's instance and may run after `destroy(udata)`.

This is the same as `as_config_register_*()` followed by `as_config_select_*()`, without a name
that the config file could also select.

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
| `dns` | yes | yes | `thread` (default) or `tend`. All I/O of one refresh is bounded by `timeout_ms`, default 2000 ms in `thread` mode and 80 ms in `tend` mode. In `tend`, `timeout_ms` must not exceed `tend_refresh_deadline_ms`, and at the default 100 ms only a local resolver answers reliably. In `thread`, `timeout_ms` must not exceed `refresh_timeout_ms`. When `timeout_ms` is left at 0 and the default is too large, the error marks it, for example `DNS seed provider timeout_ms 2000 (thread-mode default) exceeds discovery refresh_timeout_ms 1999. Set timeout_ms or raise refresh_timeout_ms`, or in `tend` `... timeout_ms 80 (tend-mode default) exceeds discovery tend_refresh_deadline_ms 79. Set timeout_ms or use execution_mode thread`. The default is chosen again at each `aerospike_connect()` whose discovery settings pass these checks, even if that connect then fails, for example because no seed answers. A connect rejected by the checks leaves the provider unchanged. |
| `aws-tags` | yes | no | `thread` only. `as_config_set_aws_seed_provider()` and `aerospike_connect()` fail in `tend`. |
| binding | as registered | no | `thread` only, periodic or not; a non-periodic binding runs its single call on the discovery thread. Registration (or the select of a name registered as a binding) fails in `tend`, and switching to `tend` afterwards fails `aerospike_connect()`. |
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

No address is used for more than one node, for every translator. Two addresses are the same when
their host and port match after translation, with hostnames compared case-insensitively and
nothing resolved. A peer address that another node of the reply, the refreshing node, a live node
or a node added earlier in the tend also has is shared, and the peer skips it and uses its other
addresses. The skip is logged naming both nodes: at info level when the shared address is the
server's own advertised address, unchanged, and as a warning naming the translator when the
translator produced it. An address whose strings differ but that answers, once connected, as a
node the client already knows (the refreshing node, a live node, a node added earlier in the tend
or another peer of the reply), such as a server-side alias or a mapping typo, is skipped in the
same way: the peer tries its next address, the known node is untouched, and the skip is logged
at the same levels. A peer whose every address is shared or skipped is not added: it is logged
once with an `AEROSPIKE_ERR_TRANSLATION_COLLISION` message, its peers generation is kept so it is
retried on every tend, and an existing live node is kept. When one of its hosts also failed for
another reason, such as a connect timeout, or an address answered as an unknown node, the peer
gets the usual failure warning instead. A host dropped by a strict translation miss is reported
with its own `AEROSPIKE_ERR_TRANSLATION_MISS` message as well.

A peer's addresses are tried in the order the server advertises them, and a shared address is
skipped, and logged, when the peer reaches it. A peer address that validates at the socket of
another healthy node, the refreshing node, a live node or a node added earlier in the tend,
collides at once, whatever name the socket answers as, as behind a load balancer: the peer stops
there, its remaining addresses are not tried, and the `AEROSPIKE_ERR_TRANSLATION_COLLISION`
message lists them as not tried. The node already at the socket is kept; no node is ever removed
because another peer collided with it. Addresses after the one a peer validates or collides at
are never reached, so they are not logged.

Each IP address of a peer hostname is an address of that host. The first one whose connect
completes is validated: the address cached from an earlier tend, then the resolver's order. When
that IP is another node's socket, the whole peer stops, as in the Java client, even if a later IP
of the same hostname is the peer's own.

A socket collision is found against the nodes validated before the peer, so its outcome depends
on order:

- The resolver's order of a hostname's IP addresses: the peer above can join on a tend where the
  resolver lists its own IP first.
- The order of the server's reply: a peer whose first address is another node's socket stops when
  the reply lists that node before the peer. When the reply lists that node after it, the address
  answers as an unknown node, and the peer can join through its own address. Once that node is in
  the cluster, the peer stops on every tend.
- A shared address string does not depend on order: every peer's shared addresses are found
  before any peer is validated. A round-robin name that every peer advertises is caught this way,
  before any connect.

A stopped peer is retried on every tend and logged at warning level each time. The fix is to give
each node its own address or mapping.

### Reading a collision message

- "no address of its own" means that no usable address was found before the peer stopped.
- The addresses after "Not tried:" were never checked and may be the peer's own.
- An address that failed to connect is not listed.
- The translator is `static-map` when none is registered, even when the shared address came from
  the server, because the rule applies to the default translator too.
- After a socket collision, the message is reported even if an earlier address of the peer failed
  to connect. When no address validated or reached another node's socket, and at least one failed
  to connect, the peer gets the usual failure warning instead of this message.
- A translator name over 128 bytes is cut with `...` in this and every other translation message,
  so the node name and the advice are always shown.

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

**Rejected certificates.** A certificate that matches the TLS name in neither way is rejected. The
client logs the warning `Invalid TLS name: <name>`, and the connect fails with an error that
contains the same text: `Invalid TLS name: <name> (<address>)` for a seed, peer or command
connection, and `Invalid TLS name: <name>` for an async connection. The status code does not
change. A handshake that fails for another reason keeps its own message.

When no seed connects, `aerospike_connect()` fails with an error that lists each seed with the
error it failed with, as the Java client does:

```
Failed to connect to [2] host(s): 10.0.0.1 4333 Invalid TLS name: cluster.example.com (10.0.0.1:4333); 10.0.0.2 4333 Socket write error: 111, 10.0.0.2:4333
```

A seed skipped because it already failed in the same attempt is listed without an error. When the
list does not fit in the error message, it ends with `; and N more`. A missing alternate address,
a strict translation miss or a collision is returned instead of the list.

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

`as_config_set_binding_seed_provider(config, factory, destroy, udata, err)` registers a provider
implemented in a binding's runtime (Python, Node.js) through a factory, so that close followed by
connect works on the same `aerospike` object:

- Factory, ownership, rebuild and error rules are those of
  `as_config_set_seed_provider_factory()`; see [Factories without a name](#factories-without-a-name).
- The factory fills the whole table (`name`, `refresh`, `needs_periodic_refresh`, `destroy` and
  `udata`). It may enter the runtime the way `refresh` does (Python: connect runs with the GIL
  released). `needs_periodic_refresh` is pure C and must not enter the runtime. Node.js runs with
  async event loops, where `aerospike_close()` ends the registration, so its factory is only
  called at registration.
- It runs in `thread` mode only: registration fails while `execution_mode` is `tend`, and
  `aerospike_connect()` fails if the mode is switched to `tend` afterwards.
- Every `refresh` call, the initial one included, runs on the discovery thread, periodic or not,
  never on the connecting or tend thread.
- Blocking budget: `aerospike_connect()` waits at most `refresh_timeout_ms` for the initial result,
  and `aerospike_close()` at most `refresh_timeout_ms` for a running refresh. `refresh` may wait for
  the runtime (for example acquire the Python GIL), and so may the factory; the instance `destroy`
  must not.
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

`as_config_register_binding_seed_provider(config, name, factory, destroy, udata, err)` registers
the same kind of factory under a name, so that the config file (`provider: <name>`) or
`as_config_select_seed_provider()` can pick it. Name, ownership and error rules are those of
`as_config_register_seed_provider()`, and registering builds nothing. The instance it builds is a
binding provider, with every rule above, also after a rebuild by reconnect: even a periodic
instance whose `supports_deadline` returns true never runs on the tend thread. The factory is
never called in `tend`. Registration does not check the mode, because the config file sets
`execution_mode` afterwards. In `tend`:

- Selecting the name from code fails with
  `Invalid seed provider '<name>': Binding seed provider requires thread execution mode`.
- The file keeps the selection without building it, and its other keys apply.
  `aerospike_connect()` fails with `Binding seed provider requires thread execution mode` until
  the mode is `thread`. Then connect calls the factory, so a factory error shows there and not
  at init.
- Switching to `tend` after a selection, also after a sync close, fails `aerospike_connect()`
  the same way.

The factory is called on the thread that selects the name, on the thread that calls
`aerospike_init()` or `aerospike_new()` when the file selects it in `thread`, and on the thread
that connects after each sync close or after the file selected it in `tend`. Python takes the
GIL in the factory. A Node.js factory called from `aerospike_new()` or a select on the JS main
thread must not wait for that thread; one called by connect runs on the connecting worker
thread.

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
- `as_discovery_conformance_collision()`: one collision row. Given a translator and the endpoints of
  one peers reply, where consecutive endpoints of one node name are the addresses of one peer, it
  returns the node names that become nodes and one `AEROSPIKE_ERR_TRANSLATION_COLLISION` message
  per peer left without an address of its own, through the same collision code the cluster uses.

They share their implementation with the cluster (`as_discovery_complete_translation()` and
`as_discovery_merge_hosts()`). The client's own runner in `src/test/conformance` runs the fixture
rows through them.

## Status codes

| Code | Name | Returned by |
| --- | --- | --- |
| -25 | `AEROSPIKE_ERR_TRANSLATION_MISS` | A strict miss: logged for a peer that cannot be added, returned by the conformance entry points, and returned by `aerospike_connect()` instead of the generic connection error when initialization fails for another reason after a miss was recorded. |
| -26 | `AEROSPIKE_ERR_TRANSLATION_COLLISION` | A peer with no address of its own, or a peer that stopped at another node's socket: logged for each such peer, and returned in the same way as -25. The message names the translator, `static-map` when none is registered, the peer, each address it skipped with the node that has it, in address order up to and including the one it stopped at (the first three, then how many more), and, after a socket collision, every address not tried, for example `Address translator static-map left node B with no address of its own: 10.0.0.1:3000 is used by A, lb.example.com:3000 is used by C. Not tried: 10.0.0.9:3000. Give each node its own reachable address or mapping`. A message longer than the error buffer cuts its lists with `...` and keeps the advice. See [Reading a collision message](#reading-a-collision-message). |
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
| Plugins across close | A sync `aerospike_close()` destroys every instance, and reconnect rebuilds the built-in, file-selected, code-selected, factory-set and binding ones; a table registered directly must be registered again. An async client releases every registration at close; see [Registration and ownership](#registration-and-ownership). | The provider is closed when the client closes; a translator has no close method. |
| Selecting a registered name from code | `as_config_select_seed_provider()` and `as_config_select_address_translator()` call the factory at once, and again on every reconnect after a sync close. | Only the config file selects a registered name; the factory runs once, when the client is constructed. |
| YAML factory options | Typed as SnakeYAML types them, with a few differences; see [Option values](yaml-config-discovery.md#option-values). | SnakeYAML 2.6. |
| YAML aliases and merge keys | An alias to a collection works only in factory options and skipped content. Merge keys work only in factory options and directly under the blocks; anywhere else the client parses, they make the file invalid. See [Anchors, aliases and merge keys](yaml-config-discovery.md#anchors-aliases-and-merge-keys). | Applied everywhere. |
| `ip_map` | Also rewrites configured seeds, and is ignored with a warning while another translator is registered; see [Built-ins](#built-ins). | Applies to the addresses servers advertise; no warning. |
| Static `use_services_alternate` on reload | Not re-applied; only `dynamic.client.use_service_alternative` changes a running client. | Re-applied. |
| aws-tags timeouts | One `timeout_ms`, default 5000; `init_timeout_ms` is accepted and ignored. | `timeout_ms` 2000, and `init_timeout_ms` 10000 for the first refresh. |
| Hostname address move | Only a node whose address is one of its hostname's addresses moves; the new address becomes the hostname's preferred address, and a collision has its own warning; see [Nodes behind a hostname with several addresses](#nodes-behind-a-hostname-with-several-addresses). | Also moves a node matched to a hostname only through a loopback address, records no preferred address, and logs a collision as a translation collision. |

## Nodes behind a hostname with several addresses

A node reached through a hostname, such as a load balancer name with one address per zone, moves
to another address of that hostname when its own address stops answering. This needs no
discovery or translation setting.

- The node moves only when its refresh failed in 3 tends in a row, and only when its address is
  one of the addresses its hostname resolved to: a seed whose address load balancer detection
  replaced, or a node matched to a hostname only through a loopback address, never moves. A
  hostname with a single address never moves its node either.
- Each tend tries one address, the one after the address tried last, skipping the node's own.
  Connect, login and info requests end together within half the tend interval, or
  `conn_timeout_ms` when that is set and smaller.
- The candidate must answer with the node's name. A candidate that is another node is skipped. A
  candidate that is also a healthy node, or a node moved in the same tend, is skipped with the
  warning `Address <host:port> of <hostname> is also node <other>, so node <name> does not move to it`.
  The other node keeps the address, including a node moved earlier in the same tend.
- The moved node replaces the old one in one swap, so the name is never missing or held twice,
  and it takes over the partitions in the same tend. The move is logged at info level as
  `Move node <name> <hostname> from <host:port> to <host:port>`, and the new address becomes the
  preferred address of the hostname and port for later connects.

The hostname lookup has no time bound and runs on the tend thread, one failing node after
another, so each failing node can delay the tend by one bound plus its lookup.

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
