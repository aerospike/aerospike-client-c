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

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Longest translator name, in bytes, that messages show whole.
 */
#define AS_DISCOVERY_TRANSLATOR_SHOWN 128

/**
 * Size of a buffer for as_discovery_translator_label(): the shown bytes, "..." and the terminator.
 */
#define AS_DISCOVERY_TRANSLATOR_LABEL_SIZE (AS_DISCOVERY_TRANSLATOR_SHOWN + 4)

/**
 * Copy translator name into label for a message. A translator name has no length limit, so a name
 * longer than AS_DISCOVERY_TRANSLATOR_SHOWN bytes is cut, without splitting a UTF-8 sequence, and
 * ends with "...". The rest of the message, such as the node name, then always fits. Returns label.
 */
const char*
as_discovery_translator_label(char label[AS_DISCOVERY_TRANSLATOR_LABEL_SIZE], const char* name);

#ifdef __cplusplus
} // end extern "C"
#endif
