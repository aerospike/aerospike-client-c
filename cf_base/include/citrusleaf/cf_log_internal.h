/*
 * Copyright 2012 Aerospike. All rights reserved.
 */
#pragma once

#include "cf_log.h"

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
