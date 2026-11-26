# YAML configuration: discovery and translation

These keys sit under `static.client` in the client's YAML config file. They select and
configure the seed provider and the address translator, and they replace the values set in
code on `as_config`. To write a custom seed provider or address translator, or for the
behaviour behind each key (threads, merge policies, TLS), see the
[plugin guide](discovery-plugin-guide.md).

```yaml
version: 1.2.0
static:
  client:
    use_services_alternate: true
    discovery:
      provider: static
      merge_policy: merge
      execution_mode: thread
      refresh_interval_ms: 30000
      refresh_timeout_ms: 10000
      tend_refresh_deadline_ms: 100
    translation:
      provider: port-map
      on_miss: strict
      port_map:
        "10.0.0.1:3000": "lb.example.com:30001"
        "BB9020011AC4202": "lb.example.com:30002"
```

The keys do not depend on the file's `version`. The expected versions are `1.0.0`, `1.1.0` and
`1.2.0`; any other version logs a warning, and the file is still read in full.

## How the file applies

- **Path.** The file is named by the environment variable `AEROSPIKE_CLIENT_CONFIG_URL`, or by
  `as_config.config_provider.path` (`as_config_provider_set_path()`). The environment variable
  takes precedence, and a leading `file://` is stripped from it.
- **When.** The file is read inside `aerospike_init()` and `aerospike_new()`. Its values replace
  what the `as_config` passed there holds. A field set, or a plugin registered, on `as->config`
  after that call replaces the file's value in turn, as for every other key.
- **Absent keys** leave the code value: the value set in code, otherwise the `as_config_init()`
  default.
- **Logging.** With logging at info level, every discovery and translation key present in the
  file logs `Set <path> = <value>`, for example `Set static.client.discovery.merge_policy = replace`.
  These lines are logged while the file is parsed, before it is validated, so a rejected file
  still logs `Set` lines; the error that follows means nothing was applied (see
  [Invalid settings](#invalid-settings)). The existing keys of the file log
  `Set <section>.<name> = <value>` only when the value changes.
- **Unknown keys** are logged at info and skipped everywhere, including inside `discovery`,
  `translation`, `dns` and `aws`; the rest of the file applies. Inside those blocks the log names
  the full path (`Unexpected field: static.client.discovery.consul`); directly under
  `static.client` it reads `Unexpected field: client.<name>`, or `Unexpected section` or
  `Unexpected sequence` for a map or list value. Under a registered provider or translator name,
  the other keys of `discovery` and `translation` are not logged: they become the factory's
  options (see [Registered providers and translators](#registered-providers-and-translators)).

## `static.client`

| Key | Type | Values | Default | `as_config` field |
| --- | --- | --- | --- | --- |
| `ip_map` | map of string to string | advertised host to the host to connect to; the port is kept | none (no translation) | `ip_map`, `ip_map_size` |
| `use_services_alternate` | bool | `true`, `false` | `false` | `use_services_alternate` |

`ip_map` replaces the code `ip_map` as a whole; the two are not merged, and an empty map
(`ip_map: {}`) clears it. It cannot be combined with `translation.static_map`. While a
translator other than the static map is registered (`port-map`, `pass-through` or a code
translator), `ip_map` is ignored, configured seeds included, and `aerospike_connect()` logs:

```
ip_map is ignored while address translator port-map is registered, configured seeds included. Move the mappings into the translator (for example port-map entries) and configure seeds with reachable addresses
```

`dynamic.client.use_service_alternative` sets the same field and wins when both are set. The
static value is the fallback: when a reload finds no dynamic key, the client returns to the
static value, not to the code value.

## `static.client.discovery`

| Key | Type | Values | Default | `as_config` field |
| --- | --- | --- | --- | --- |
| `provider` | string | `static`, `dns`, `aws-tags`, or a registered name | `static` | `discovery.provider` |
| `dns` | map | see [`static.client.discovery.dns`](#staticclientdiscoverydns) | none | `discovery.provider` (with `provider: dns`) |
| `aws` | map | see [`static.client.discovery.aws`](#staticclientdiscoveryaws) | none | `discovery.provider` (with `provider: aws-tags`) |
| `merge_policy` | string | `merge`, `replace`, `discovery_only` | `merge` | `discovery.merge_policy` |
| `execution_mode` | string | `thread`, `tend` | `thread` | `discovery.execution_mode` |
| `refresh_interval_ms` | int | milliseconds | `30000` | `discovery.refresh_interval_ms` |
| `refresh_timeout_ms` | int | milliseconds | `10000` | `discovery.refresh_timeout_ms` |
| `tend_refresh_deadline_ms` | int | milliseconds | `100` | `discovery.tend_refresh_deadline_ms` |

`provider` registers:

| Value | Registration |
| --- | --- |
| `static` | None: the code registration is destroyed and the default static provider over the configured seeds is used, the same as registering nothing. |
| `dns` | The built-in DNS provider built from the `dns` block, as `as_config_set_dns_seed_provider()` would. |
| `aws-tags` | The built-in AWS EC2 provider built from the `aws` block, as `as_config_set_aws_seed_provider()` would. |
| a registered name | The provider returned by the factory registered under that name with `as_config_register_seed_provider()` or `as_config_register_binding_seed_provider()` (a binding provider), called with the block's other keys as options; see [Registered providers and translators](#registered-providers-and-translators). |

A provider from the file replaces, and destroys, a provider registered in code before
`aerospike_init()` or `aerospike_new()`, and logs at warning level:

```
Seed provider my-provider registered in code is replaced by static.client.discovery.provider static in /etc/aerospike/client.yaml
```

With `provider` absent, the code registration is kept.
For what each merge policy and execution mode does, see
[Merge policies](discovery-plugin-guide.md#merge-policies) and
[Execution modes](discovery-plugin-guide.md#execution-modes) in the plugin guide.

The integers are not range checked in the file. They are checked by `aerospike_connect()`,
whether they came from the file or from code, and only when the provider needs periodic
refresh (`dns`, `aws-tags`, or a code provider whose `needs_periodic_refresh` returns true).
A binding provider (`as_config_set_binding_seed_provider()`, or a name registered with
`as_config_register_binding_seed_provider()`) needs `execution_mode: thread` and a
`refresh_timeout_ms` above 0 even when it is not periodic. `aerospike_connect()` then fails with
`AEROSPIKE_ERR_PARAM` and one of these messages:

| Rule | Message |
| --- | --- |
| A binding provider needs `execution_mode: thread` | `Binding seed provider requires thread execution mode` |
| `execution_mode: tend` needs a provider that supports a deadline; `aws-tags` never does, nor does a code provider whose `supports_deadline` is absent or returns false | `Seed provider aws-tags does not support a deadline and cannot use discovery execution_mode tend. Use execution_mode thread` |
| `refresh_interval_ms` must be at least the tend interval (`dynamic.client.tend_interval`, `as_config.tender_interval`) | `Discovery refresh_interval_ms 500 must be greater or equal to the tend interval 1000` |
| In `tend`, `tend_refresh_deadline_ms` must be above 0 and at most half the tend interval | `Invalid discovery tend_refresh_deadline_ms 600: must be > 0 and <= tend interval / 2 (500)` |
| In `thread`, `refresh_timeout_ms` must be above 0 (also for a non-periodic binding provider) | `Invalid discovery refresh_timeout_ms 0: must be > 0` |
| In `tend`, the `dns` `timeout_ms` must not exceed `tend_refresh_deadline_ms` | `DNS seed provider timeout_ms 150 exceeds discovery tend_refresh_deadline_ms 100. Lower timeout_ms or use execution_mode thread` |
| In `thread`, the `dns` or `aws` `timeout_ms` must not exceed `refresh_timeout_ms` | `AWS EC2 seed provider timeout_ms 15000 exceeds discovery refresh_timeout_ms 10000. Lower timeout_ms or raise refresh_timeout_ms` |
| The same rules apply to the default `timeout_ms` of 0, which is 2000 ms in `thread` and 80 ms in `tend`; the message then marks the default and advises setting it | `DNS seed provider timeout_ms 80 (tend-mode default) exceeds discovery tend_refresh_deadline_ms 79. Set timeout_ms or use execution_mode thread` |

These rules also apply when the provider comes from code and only `execution_mode` comes from
the file: an `aws-tags` provider registered in code with `execution_mode: tend` in the file is
accepted at init and fails `aerospike_connect()`.

## `static.client.discovery.dns`

`provider: dns` builds the built-in DNS provider from the `dns` block, which is required with
`provider: dns` and invalid with any other provider. On every refresh the provider resolves each
name in `names` and returns the addresses as seed hosts. The keys map onto
`as_dns_seed_provider_config`, starting from `as_dns_seed_provider_config_init()`.

| Key | Type | Values | Default |
| --- | --- | --- | --- |
| `names` | list of string | one or more DNS names, no duplicates (compared case-insensitively, trailing dot optional), queried as absolute names | required |
| `record_type` | string | `a` (A and AAAA records), `srv` (SRV records) | `a` |
| `port` | int | 1 to 65535: port set on hosts resolved from A and AAAA records; ignored for `srv`, which uses each record's port | `3000` |
| `tls_name` | string | TLS name set on every returned host | none (the configured-seed default) |
| `timeout_ms` | int | at least 1: total milliseconds of one refresh, across every name, record type, SRV target and resolver | `2000` in thread mode, `80` in tend mode |
| `servers` | list of string | resolver addresses, `ip[:port]`, with an IPv6 address in brackets when a port is given (`"[2001:db8::1]:53"`), tried in order | the system resolvers: the `nameserver` entries of `/etc/resolv.conf`, or the DNS servers of the active network adapters on Windows, read once when the provider is built |

`timeout_ms` must not exceed `tend_refresh_deadline_ms` in `tend` mode, or `refresh_timeout_ms`
in `thread` mode (checked at connect, see above). Without the key the mode default applies, so in
`thread` mode a `refresh_timeout_ms` below 2000 fails `aerospike_connect()` unless `timeout_ms`
is set. With the default tend deadline of 100 ms only a local resolver answers reliably in time;
in `thread` mode, the default, the 10000 ms `refresh_timeout_ms` leaves room for any resolver.
In `thread` mode with the default, an unresponsive resolver makes the first refresh of
`aerospike_connect()` take up to about 2000 ms before the client falls back to the configured
seeds, and `aerospike_close()` can wait as long for a refresh in flight.

Every returned host is a numeric IP address. When some names resolve, their hosts are returned
and each failed name is logged at warning level. When no name yields a host, the refresh fails
with `AEROSPIKE_ERR_CLIENT`, the failure is logged and the previous seeds are kept; at
initialization the client continues with the configured seeds, and `aerospike_connect()` fails
with `AEROSPIKE_ERR_SEED_DISCOVERY_FAILED` only when there are none. The full behaviour is documented
on `as_discovery_dns_seed_provider()` in `as_discovery.h`.

## `static.client.discovery.aws`

`provider: aws-tags` builds the built-in AWS EC2 provider from the `aws` block, which is required
with `provider: aws-tags` and invalid with any other provider. On every refresh the provider lists
the running EC2 instances that match `filters`, usually a tag, and returns one seed host per
instance. Use it for a self-managed Aerospike cluster on EC2 in the client's own account and
region; it does not discover Aerospike Cloud clusters. The keys map onto
`as_aws_seed_provider_config`, starting from `as_aws_seed_provider_config_init()`.

| Key | Type | Values | Default |
| --- | --- | --- | --- |
| `region` | string | AWS region of the instances and of the default endpoint, such as `us-east-1` | required |
| `filters` | list of maps | one or more `name` (string) and `values` (non-empty list of strings) pairs, passed to `DescribeInstances` as given: `tag:<key>` or any other EC2 filter name except `instance-state-name` | required |
| `address_field` | string | `private_ip`, `public_ip`, `private_dns`, `public_dns` | `private_ip` |
| `port` | int | 1 to 65535: port set on every host | `3000` |
| `tls_name` | string | TLS name set on every host | none (the configured-seed default) |
| `timeout_ms` | int | at least 1: total milliseconds of one refresh, credentials and every page included | `5000` |
| `init_timeout_ms` | int | at least 1. Validated, logged at info and ignored: the first refresh is bounded by `refresh_timeout_ms` | ignored |
| `endpoint` | string | EC2 endpoint URL, `http` or `https`, such as a VPC interface endpoint | `https://ec2.<region>.amazonaws.com` (`.com.cn` for `cn-*` regions) |

`aws-tags` runs in `thread` mode only. With `execution_mode: tend`, `aerospike_connect()` fails
before any request is sent (see above). `timeout_ms` must not exceed `refresh_timeout_ms`.

Code only: static access keys (`access_key_id`, `secret_access_key`, `session_token`), the
credentials hook, `ca_file` and `ca_path`. Without them the provider uses its credential chain,
starting with the `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`
environment variables, which are read once, when the provider is built at `aerospike_init()` or
`aerospike_new()`. For the chain and the minimum IAM policy, see
[aws-tags IAM policy and credentials](discovery-plugin-guide.md#aws-tags-iam-policy-and-credentials).

## `static.client.translation`

| Key | Type | Values | Default | `as_config` field |
| --- | --- | --- | --- | --- |
| `provider` | string | `static-map`, `port-map`, `pass-through`, or a registered name | `static-map` | `translation.provider` |
| `on_miss` | string | `pass_through`, `strict` | `pass_through` | `translation.on_miss` |
| `static_map` | map of string to string | advertised host to the host to connect to; the port is kept | none | `ip_map`, `ip_map_size` (with `provider: static-map`) |
| `port_map` | map of string to string | key: an advertised `host:port` (IPv6 host in brackets) or a node name; value: the `host:port` to connect to | none | `translation.provider` (with `provider: port-map`) |

`provider` registers:

| Value | Registration |
| --- | --- |
| `static-map` | None: the code translator is destroyed and the default static map over `ip_map` is used. With `static_map`, that map replaces `ip_map`. |
| `port-map` | The built-in port-map translator over `port_map`, which is required, as `as_config_set_port_map_translator()` would. |
| `pass-through` | The built-in pass-through translator, as `as_config_set_pass_through_translator()` would. Use it with `use_services_alternate: true` when each node advertises a client-reachable alternate address and port. |
| a registered name | The translator returned by the factory registered under that name with `as_config_register_address_translator()`, called with the block's other keys as options; see [Registered providers and translators](#registered-providers-and-translators). |

A translator from the file replaces, and destroys, a translator registered in code, and logs
`Address translator <name> registered in code is replaced by static.client.translation.provider
<value> in <path>` at warning level. With `provider` absent, the code registration is kept,
unless `static_map` is set, which implies `static-map`. `static_map` is valid only with `static-map` and `port_map` only with `port-map`.
`static_map` replaces the code `ip_map` like `static.client.ip_map`, and an empty map clears it.
On AWS, the one-port-per-node layouts of `port-map` and `pass-through` have a node limit per load
balancer; see [Deployment limits](discovery-plugin-guide.md#deployment-limits-aws).

`on_miss` applies when the translator has no mapping for an advertised endpoint:

| Value | Behaviour |
| --- | --- |
| `pass_through` (default) | Connect to the advertised endpoint unchanged. |
| `strict` | Drop the endpoint. A peer left without an endpoint is not added: the miss is logged at warning level as an `AEROSPIKE_ERR_TRANSLATION_MISS` (-25) message and the peer is retried on every tend. |

`port_map` keys and values: an IPv6 host is written in brackets, and a bracketed key must be
quoted, because `[` starts a YAML flow sequence: `"[2001:db8::1]:3000": "lb.example.com:30001"`.
Quoting every key and value keeps the file readable by other clients too, where an unquoted
`host:port` can be read as a number.

## Registered providers and translators

A seed provider or address translator written for the application is registered in code under a
name, and the file selects it with `provider: <name>`:

```c
as_config config;
as_config_init(&config);
as_config_provider_set_path(&config, "/etc/aerospike/client.yml");

as_error err;
as_config_register_seed_provider(&config, "consul", consul_factory, consul_destroy, consul_state,
	&err);

aerospike as;
aerospike_init(&as, &config);
```

```yaml
version: 1.2.0
static:
  client:
    discovery:
      provider: consul
      merge_policy: merge
      service: aerospike
      endpoints:
        - 127.0.0.1:8500
```

- **Register first.** The file is read inside `aerospike_init()` and `aerospike_new()`, so the
  name must be registered on the `as_config` passed there. A name that is not registered makes
  the file invalid, and the error lists the built-in and registered names:
  `Invalid static.client.discovery.provider: consul. valid values: static, dns, aws-tags`.
  Translators are registered with `as_config_register_address_translator()`; the two sets of
  names are separate. The factory contract is in the [plugin guide](discovery-plugin-guide.md).
- **Options.** The factory receives every key of the block except `provider` and the built-in
  sections (`dns`, `aws`, `static_map`, `port_map`, which stay invalid with a registered name),
  as an `as_orderedmap` keyed by the key as written. The client settings of the block
  (`merge_policy`, `execution_mode`, the discovery integers, `on_miss`) are applied to the client
  as usual and passed in the options too.
- **When the factory runs.** Once, while the file is loaded, on the thread that calls
  `aerospike_init()` or `aerospike_new()` (a binding factory in `tend` mode is the exception,
  below). A sync `aerospike_close()` destroys the instance and
  `aerospike_connect()` calls the factory again with the same options. The `static` section is
  not reloaded, so the factory is not called again when the file changes; changed options need a
  new client.
- **Binding providers.** A provider implemented by a language binding (Python, Node.js) is
  registered with `as_config_register_binding_seed_provider()` under a name from the same set.
  The file selects it as any registered name, and the instance follows the binding rules of the
  [plugin guide](discovery-plugin-guide.md#language-bindings): every call runs on the discovery
  thread, bounded by `refresh_timeout_ms`. With `execution_mode: tend` the file is still accepted
  and its other keys apply, but the factory is not called: `aerospike_connect()` fails with
  `Binding seed provider requires thread execution mode`. After the mode is set to `thread` in
  code, connect calls the factory, so a factory error shows there and not at init.
- **Option errors.** A value the client cannot type (below) makes the file invalid only under a
  registered name, and the error names its full path:
  `Invalid static.client.discovery.x.a[1].b: 'z' is not a valid !!int`. With a built-in provider
  the same keys are only logged as unexpected fields.

### Option values

Values are typed the way SnakeYAML 2.6, the parser the Java client reads the file with, types
them, so a file shared with Java gives a factory the same values.

| YAML value | `as_val` |
| --- | --- |
| Plain `~`, `null`, `Null`, `NULL`, or nothing | `as_nil` |
| Plain `yes`, `no`, `true`, `false`, `on`, `off`, lowercase, capitalized or uppercase | `as_boolean` |
| Plain integer: decimal, `0b` binary, `0x` hex, `0` octal, base 60 (`1:30` is 90), `_` separators | `as_integer` |
| Plain float: `1.5`, `1e5`, `.5`, `.inf`, `-.Inf`, `.nan`, base 60 (`1:30.5`), `_` separators | `as_double` |
| Any other scalar, any quoted scalar, a timestamp such as `2001-12-14` | `as_string` |
| Sequence | `as_arraylist` |
| Mapping, with string or integer keys | `as_orderedmap` |

- **Tags** run SnakeYAML's constructor for the type, which accepts more than the plain forms:
  `!!float 1` is 1.0, `!!float .5f` is 0.5 and `!!float 0x1.8p1` is 3.0 (Java's hex form);
  `!!int _1` is 1; `!!bool` takes any case (`!!bool tRuE`) and gives nil for another value;
  `!!null` always gives nil; `!!str` gives the string; `!!timestamp` checks the format and gives
  the string. `!!map` and `!!seq` are accepted on collections. The non-specific tag `!` types a
  scalar as if it were plain, whatever its quotes: `! '5'` is 5. Any other tag (`!!binary`,
  `!!set`, `!!omap`, `!!pairs`, `!local`) is an option error.
- **Base 60** follows Java's arithmetic, which wraps at 32 bits: `100000:00:00:00` is 125163520.
- **Floats** do not depend on the process locale: `1.5` is 1.5 under a locale whose decimal
  point is a comma.
- **Differences from Java:** a timestamp arrives as its string, not a date; an integer outside
  the int64 range arrives as its string, not a BigInteger; a map key inside an option must be a
  string or an integer.

### Anchors, aliases and merge keys

- **Aliases to scalars** work everywhere in the file, client settings and built-in sections
  included, and keep the anchor's tag and quotes: `merge_policy: *policy`. An anchor defined
  again replaces the earlier one.
- **Aliases to collections** (a map or a list) work in factory options and in content the client
  skips. In options the alias reuses the collection, which must be defined in the factory options
  of either block; an alias to a collection defined elsewhere (outside the blocks, or in `dns`,
  `aws`, `static_map` or `port_map`) is an option error, as is a recursive alias (`&r [*r]`),
  which Java accepts. Where the client parses the value itself, a client setting or a built-in
  section, the file is invalid:
  `Invalid static.client.ip_map: YAML aliases to collections are not supported for client settings`.
- **Merge keys** (`<<`, or a key tagged `!!merge`) in an option map take a map, a list of maps,
  or an alias to either. Keys written in the map win over merged ones, the last of a repeated
  key wins, and among merged maps the earlier one wins. A source reached by an alias keeps its
  own merges. Inside a source written inline, a repeated key keeps its last value and the
  source's own keys win over its merges, where SnakeYAML keeps the first:
  `{<<: {a: 1, a: 2}}` gives `a: 2` here and `a: 1` in Java.
- **Merge keys directly under `discovery` or `translation`** add options the same way, after the
  block's own keys, whatever the provider; with a built-in provider the merged keys are logged as
  unexpected fields. A merged `provider`, client setting or built-in section that the block does
  not also set makes the file invalid, because Java applies it and this client does not:
  `Invalid static.client.discovery.merge_policy: client settings from YAML merge keys (<<) are not supported`.
  Any problem with such a merge value, for example a value that is not a map or a list of maps,
  or an alias without a value to reuse, fails the file for every provider.
- **Merge keys anywhere else the client parses the map itself** (the top level, `static`,
  `static.client`, `dns`, `aws`, `ip_map`, `static_map`, `port_map` and the `dynamic` sections)
  make the file invalid, because Java applies them there and this client does not:
  `Invalid static.client.<<: YAML merge keys (<<) are only supported in factory options`.
  Content the client skips may use them.

## Invalid settings

Any of the following makes the whole file invalid. The client logs one error,
`Failed to parse: <path>` followed on the next line by the detail, applies nothing from the file
(not even the keys outside `discovery` and `translation`), and starts on the config and the
registrations set in code. Code registrations are not destroyed.

- A YAML syntax error anywhere in the file, including inside a section the client does not read.
  The detail is the libyaml problem and its offset.
- SnakeYAML's file-wide limits, anywhere in the file, which Java enforces the same way:
  - a value nested in more than 50 collections, counting every collection open around it from
    the top of the file: `YAML nesting deeper than 50 levels: line 7, column 57`;
  - an alias whose anchor is not defined before it: `Undefined YAML alias *zz: line 7, column 10`;
  - more than 50 aliases to collections:
    `More than 50 YAML aliases to collections: line 8, column 211`.
- A key that is not a scalar, at any level the client parses:
  `Invalid static.client: expected scalar keys, not a sequence`, or
  `Invalid top-level key: expected a scalar, not a sequence`.
- An alias to a collection where the client parses the value itself, a merge key directly
  under `discovery` or `translation` that brings in `provider`, a client setting or a built-in
  section, and a merge key in any other map the client parses itself; see
  [Anchors, aliases and merge keys](#anchors-aliases-and-merge-keys).
- An unknown enum value, for example
  `Invalid static.client.discovery.execution_mode: background. valid values: thread, tend`. The
  same applies to `provider`, `merge_policy`, `dns.record_type`, `aws.address_field`,
  `translation.provider` and `translation.on_miss`.
- A known key with a value of the wrong kind, for example `discovery: static`, `ip_map: 10.0.0.1`
  or `dns.port: [1]`: `Invalid static.client.discovery: expected a map, not a scalar`. A list
  element or map value that is not a scalar, and a filter that is not a map, are reported the
  same way with the element named, for example `static.client.discovery.dns.names[0]`.
- A filter without `name` or `values`: `Invalid static.client.discovery.aws.filters[0]: name is required`.
- An integer that is not a plain decimal or is out of range:
  `Invalid uint static.client.discovery.refresh_interval_ms: 5s`, or
  `Invalid uint static.client.discovery.dns.port: 0. valid range: 1 - 65535`. Trailing characters
  (`5s`, `1.5`, `50abc`) are rejected in the discovery and translation keys only; the existing
  keys of the file keep their lenient parsing, where `50abc` is read as 50.
- A bool other than `true` or `false`: `Invalid bool static.client.use_services_alternate: yes`.
- A block or map that does not match the provider:
  - `static.client.discovery.dns is required with provider dns`, and
    `static.client.discovery.dns is only valid with provider dns`;
  - `static.client.discovery.aws is required with provider aws-tags`, and
    `static.client.discovery.aws is only valid with provider aws-tags`;
  - `static.client.translation.static_map is only valid with provider static-map, not 'port-map'`;
  - `static.client.translation.port_map is only valid with provider port-map`, with
    `, not '<provider>'` when another provider is named;
  - `static.client.translation.port_map is required with provider port-map`;
  - `static.client.translation.static_map and static.client.ip_map are both set`.
- A value the built-in factory rejects, such as a duplicate DNS name, a `servers` entry that is
  not an IP address, a malformed `region` or `endpoint`, a filter named `instance-state-name`, or
  a `port_map` entry without a valid port: `Invalid static.client.discovery.dns: <reason>`, and
  the same for `static.client.discovery.aws` and `static.client.translation.port_map`.
- With a registered name: a factory that fails
  (`Invalid static.client.discovery.provider: consul. factory failed: <message>`) or returns an
  invalid table (`... factory returned an invalid provider`, or `translator`), and an option
  value the client cannot type (see [Option values](#option-values)). A binding factory is not
  called in `tend` mode, so its errors show at `aerospike_connect()` instead.

Example error:

```
Failed to parse: /etc/aerospike/client.yaml
Invalid static.client.translation.on_miss: drop. valid values: pass_through, strict
```

Not checked in the file: the rules on the discovery integers and the execution mode, which
`aerospike_connect()` checks (see [`static.client.discovery`](#staticclientdiscovery)).

## Reload

The client checks the file's modification time every `static.client.config_interval`
milliseconds (`as_config.config_provider.interval`, default 5000), on the tend thread, and
reloads it when the time is newer than the last one seen.

- **Static keys are not reloaded.** The whole `static` section, including every discovery and
  translation key and `static.client.use_services_alternate`, is read once, at `aerospike_init()`
  or `aerospike_new()`. A change needs a new client. Only the dynamic
  `dynamic.client.use_service_alternative` changes a running client's alternate setting. Unlike
  the Java client, a changed static `use_services_alternate` is not re-applied on reload.
- **Invalid static values are not checked on reload.** The `static` section is skipped, not
  validated, so a reload can succeed with a file whose static values would be rejected. The same
  file is then rejected as a whole at the next start, and that client runs on the code config
  only, without the file's dynamic values either. Check static changes by starting a client, for
  example in a test environment, before rolling them out.
- **A syntax error is still fatal on reload**, even inside the skipped `static` section, because
  the whole file must parse.
- **A failed reload** logs `Dynamic configuration error: <detail>` at warning and keeps the running
  config. The new modification time is stored before the file is read, so a failed reload is not
  retried until the file's modification time changes again.
- **Replace the file atomically.** Write the new content to a temporary file in the same directory,
  then rename it over the config file. Editing the file in place can trigger a reload of a
  half-written file, which is rejected. On Windows, where the modification time has 1 second
  resolution, the completed write in the same second may then never be read.

## Examples

Each example shows the YAML file and the equivalent C code. A client built from the code alone
behaves like one that reads the file.

### DNS split network: SRV seeds and pass-through

The seed name resolves only on the client's network, to addresses the client can reach, while
the servers advertise client-reachable alternate addresses (`alternate-access-address` and
`alternate-access-port`). No seed is configured in code, so discovery is the only seed source.

```yaml
version: 1.2.0
static:
  client:
    use_services_alternate: true
    discovery:
      provider: dns
      merge_policy: discovery_only
      dns:
        names:
          - _aerospike._tcp.client-net.example.com
        record_type: srv
        timeout_ms: 200
        servers:
          - 10.20.0.2
    translation:
      provider: pass-through
```

```c
as_config config;
as_config_init(&config);
config.use_services_alternate = true;
config.discovery.merge_policy = AS_SEED_DISCOVERY_ONLY;

const char* names[] = {"_aerospike._tcp.client-net.example.com"};
const char* servers[] = {"10.20.0.2"};
as_dns_seed_provider_config dns;
as_dns_seed_provider_config_init(&dns);
dns.names = names;
dns.names_size = 1;
dns.record_type = AS_DNS_RECORD_SRV;
dns.timeout_ms = 200;
dns.servers = servers;
dns.servers_size = 1;

as_error err;
as_config_set_dns_seed_provider(&config, &dns, &err);
as_config_set_pass_through_translator(&config, &err);

aerospike as;
aerospike_init(&as, &config);
```

### aws-tags on self-managed EC2

The nodes carry the tag `aerospike-cluster=prod-a` and are reached on their private IPs, port
3000. A seed host configured in code is kept as a fallback (`merge`). Credentials come from the
chain, for example the instance profile.

```yaml
version: 1.2.0
static:
  client:
    discovery:
      provider: aws-tags
      merge_policy: merge
      refresh_interval_ms: 30000
      aws:
        region: us-east-1
        filters:
          - name: tag:aerospike-cluster
            values:
              - prod-a
        address_field: private_ip
        port: 3000
```

```c
as_config config;
as_config_init(&config);
as_config_add_host(&config, "10.0.1.10", 3000);

const char* values[] = {"prod-a"};
as_aws_filter filter = {"tag:aerospike-cluster", values, 1};
as_aws_seed_provider_config aws;
as_aws_seed_provider_config_init(&aws);
aws.region = "us-east-1";
aws.filters = &filter;
aws.filters_size = 1;
aws.address_field = AS_AWS_ADDRESS_PRIVATE_IP;
aws.port = 3000;

as_error err;
as_config_set_aws_seed_provider(&config, &aws, &err);
```

### Port-map with strict misses

Each node is reached through its own port on one load balancer name. One entry is keyed by the
advertised `host:port`, the other by the node name. With `on_miss: strict`, a node that has no
entry is not connected.

```yaml
version: 1.2.0
static:
  client:
    translation:
      provider: port-map
      on_miss: strict
      port_map:
        "10.0.0.1:3000": "lb.example.com:30001"
        "BB9020011AC4202": "lb.example.com:30002"
```

```c
as_config config;
as_config_init(&config);
as_config_add_host(&config, "lb.example.com", 30001);
config.translation.on_miss = AS_TRANSLATION_STRICT;

as_addr_map entries[] = {
	{"10.0.0.1:3000", "lb.example.com:30001"},
	{"BB9020011AC4202", "lb.example.com:30002"}
};

as_error err;
as_config_set_port_map_translator(&config, entries, 2, &err);
```

The seed in this example (`lb.example.com:30001`) is configured in code, because a registered
translator is never applied to configured seeds.

### Pass-through with alternate addresses

Each node advertises a client-reachable alternate address and port, and the client connects to
them unchanged.

```yaml
version: 1.2.0
static:
  client:
    use_services_alternate: true
    translation:
      provider: pass-through
```

```c
as_config config;
as_config_init(&config);
as_config_add_host(&config, "aerospike.example.com", 3000);
config.use_services_alternate = true;

as_error err;
as_config_set_pass_through_translator(&config, &err);
```
