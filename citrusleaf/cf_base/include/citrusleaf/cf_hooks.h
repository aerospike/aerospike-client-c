/*
 *  Citrusleaf Foundation
 *  include/cf_hooks.h - hooks for application defined allocation and locking
 *
 *  Copyright 2012 by Citrusleaf.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */
 

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

typedef struct cf_mutex_hooks_s {
	// Allocate and initialize new lock.
	void *(*alloc)(void);
	// Release all storage held in 'lock'.
	void (*free)(void *lock);
	// Acquire an already-allocated lock at 'lock'.
	int (*lock)(void *lock);
	// Release a lock at 'lock'.
	int (*unlock)(void *lock);
} cf_mutex_hooks;

extern cf_mutex_hooks* g_mutex_hooks;

static inline void
cf_hook_mutex(cf_mutex_hooks *hooks)
{
	g_mutex_hooks = hooks;
}

static inline void* 
cf_hooked_mutex_alloc()
{
	return g_mutex_hooks ? g_mutex_hooks->alloc() : 0;
}

static inline void
cf_hooked_mutex_free(void *lock)
{
	if (lock) {
		g_mutex_hooks->free(lock);
	}
}

static inline int
cf_hooked_mutex_lock(void *lock)
{
	return lock ? g_mutex_hooks->lock(lock) : 0;
}

static inline int
cf_hooked_mutex_unlock(void *lock)
{
	return lock ? g_mutex_hooks->unlock(lock) : 0;
}

#ifdef __cplusplus
}
#endif

