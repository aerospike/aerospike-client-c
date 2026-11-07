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
 * Return true and set timeout_ms to the configured refresh timeout when provider is the
 * built-in DNS seed provider. Identification uses the function table and state, never the
 * provider name, so a custom provider named "dns" is not matched.
 */
bool
as_dns_seed_provider_timeout(const as_seed_provider* provider, uint32_t* timeout_ms);

#ifdef __cplusplus
} // end extern "C"
#endif
