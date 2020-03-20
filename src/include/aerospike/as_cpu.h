/*
 * Copyright 2008-2018 Aerospike, Inc.
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

#include <pthread.h>

#if defined(__APPLE__)
#include <mach/thread_policy.h>
#endif

#if defined(__FreeBSD__)
#include <sys/_cpuset.h>
#include <sys/cpuset.h>
#include <pthread_np.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * Assign a thread attribute to a specific cpu core.
 */
static inline int
as_cpu_assign_thread_attr(pthread_attr_t* attr, int cpu_id)
{
#if defined(__APPLE__) || defined(AS_ALPINE)
	// CPU affinity will be set later.
	return 0;
#elif defined(__FreeBSD__)
	cpuset_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(cpu_id, &cpuset);
	return pthread_attr_setaffinity_np(attr, sizeof(cpuset_t), &cpuset);
#else
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(cpu_id, &cpuset);
	return pthread_attr_setaffinity_np(attr, sizeof(cpu_set_t), &cpuset);
#endif
}

/**
 * Assign a running thread to a specific cpu core.
 */
static inline int
as_cpu_assign_thread(pthread_t thread, int cpu_id)
{
#if defined(__APPLE__)
	thread_affinity_policy_data_t policy = {cpu_id};
	thread_port_t mach_thread = pthread_mach_thread_np(thread);
	return thread_policy_set(mach_thread, THREAD_AFFINITY_POLICY, (thread_policy_t)&policy, 1);
#elif defined(AS_ALPINE)
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(cpu_id, &cpuset);
	return pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
#else
	// CPU affinity already set.
	return 0;
#endif
}

#ifdef __cplusplus
} // end extern "C"
#endif
