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

// Private to the client library. Not installed.

#include <aerospike/as_discovery.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Return true when provider is the built-in DNS seed provider. Then set timeout_ms to the refresh
 * timeout that a client in the given mode would use: the configured value, or the mode default
 * when the configured value is 0, in which case mode_default is set to true. Nothing is changed.
 * Identification uses the function table and state, never the provider name, so a custom
 * provider named "dns" is not matched.
 */
bool
as_dns_seed_provider_timeout(
	const as_seed_provider* provider, bool tend, uint32_t* timeout_ms, bool* mode_default
	);

/**
 * When provider is the built-in DNS seed provider configured with timeout_ms 0, set its
 * effective refresh timeout to the mode default: AS_DNS_DEFAULT_TEND_TIMEOUT_MS when tend is
 * true, otherwise AS_DNS_DEFAULT_THREAD_TIMEOUT_MS. The value is recomputed from the configured
 * one on every call, so an earlier connect leaves no trace. Called only from seed manager
 * validation, before any refresh of the provider can run.
 */
void
as_dns_seed_provider_apply_mode_default(const as_seed_provider* provider, bool tend);

#ifdef __cplusplus
} // end extern "C"
#endif
