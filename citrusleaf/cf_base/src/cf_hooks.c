/*
 * cf_hooks.c
 * 
 * Allows callers to define their own mutex and (and soon, allocation)
 * functions if for some reason they don't like the standard ones.
 *
 * Copyright 2012, Citrusleaf inc.
 */

#include <stddef.h>

#include "citrusleaf/cf_hooks.h"

cf_mutex_hooks* g_mutex_hooks = NULL;

/*
void 
cf_hook_alloc(cf_alloc_hooks* hooks)
{
	g_alloc_hooks = hooks;
}
*/


