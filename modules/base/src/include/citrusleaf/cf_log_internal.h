/******************************************************************************
 * Copyright 2008-2013 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to 
 * deal in the Software without restriction, including without limitation the 
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 * sell copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *****************************************************************************/
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
