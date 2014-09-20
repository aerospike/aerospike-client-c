/*
 * Copyright 2008-2014 Aerospike, Inc.
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

#include <citrusleaf/cf_log.h>

//====================================================================
// Internal API - for use by Aerospike client only
//

#ifndef CF_WINDOWS
//====================================================================
// Linux
//

#define cf_error(__fmt, __args...) \
	if (CF_ERROR <= G_LOG_LEVEL) {(*G_LOG_CB)(CF_ERROR, __fmt, ## __args);}

#define cf_warn(__fmt, __args...) \
	if (CF_WARN <= G_LOG_LEVEL) {(*G_LOG_CB)(CF_WARN, __fmt, ## __args);}

#define cf_info(__fmt, __args...) \
	if (CF_INFO <= G_LOG_LEVEL) {(*G_LOG_CB)(CF_INFO, __fmt, ## __args);}

#define cf_debug(__fmt, __args...) \
	if (CF_DEBUG <= G_LOG_LEVEL) {(*G_LOG_CB)(CF_DEBUG, __fmt, ## __args);}


#else // CF_WINDOWS
//====================================================================
// Windows
//

#define cf_error(__fmt, ...) \
	if (CF_ERROR <= G_LOG_LEVEL) {(*G_LOG_CB)(CF_ERROR, __fmt, ## __VA_ARGS__);}

#define cf_warn(__fmt, ...) \
	if (CF_WARN <= G_LOG_LEVEL) {(*G_LOG_CB)(CF_WARN, __fmt, ## __VA_ARGS__);}

#define cf_info(__fmt, ...) \
	if (CF_INFO <= G_LOG_LEVEL) {(*G_LOG_CB)(CF_INFO, __fmt, ## __VA_ARGS__);}

#define cf_debug(__fmt, ...) \
	if (CF_DEBUG <= G_LOG_LEVEL) {(*G_LOG_CB)(CF_DEBUG, __fmt, ## __VA_ARGS__);}


#endif // CF_WINDOWS
