#pragma once

#include <citrusleaf/cf_log_internal.h>

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define _log_debug(__fmt, ... ) \
	cf_debug("@%s[%s:%d] - "__fmt, __func__, __FILE__, __LINE__, ##__VA_ARGS__);

#define _log_info(__fmt, ... ) \
	cf_debug("@%s[%s:%d] - "__fmt, __func__, __FILE__, __LINE__, ##__VA_ARGS__);

#define _log_warn(__fmt, ... ) \
	cf_debug("@%s[%s:%d] - "__fmt, __func__, __FILE__, __LINE__, ##__VA_ARGS__);

#define _log_error(__fmt, ... ) \
	cf_debug("@%s[%s:%d] - "__fmt, __func__, __FILE__, __LINE__, ##__VA_ARGS__);
