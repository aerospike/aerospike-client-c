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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/shm.h>

#include <citrusleaf/citrusleaf.h>


#define NUM_NODES 128
#define NUM_NAMESPACES 10

/*Shared memory return values*/
#define CL_SHM_ERROR -1
#define CL_SHM_OK 0

//The shared memory is divided into nodes, each node has a socket address structure and 
//the data associated with that structure. 
#define SZ_NODE_IP				32
#define SZ_NAMESPACE 			32
#define SZ_PARTITION_ID 		4
#define MAX_NEIGHBORS 			(NUM_NODES-1)
#define NUM_PARTITIONS 			4096
#define MAX_ADDRESSES_PER_NODE 	4

#define SZ_FIELD_NEIGHBORS 		(MAX_NEIGHBORS * SZ_NODE_IP + 1)
/*
 *  Example:
 *	node BB958DE9B776038
 *	partition-generation 29218
 *	services 192.168.3.102:3000;192.168.3.103:3000
 */

// For 3 field names with line separators, and partition-generation value:
#define SZ_OVERHEAD				((3 * 32) + 20)
// Size of each base-64 encoded bitmap:
#define SZ_BITMAP				((NUM_PARTITIONS + 7) / 8)
#define SZ_ENCODED_BITMAP		(((SZ_BITMAP + 2) / 3) * 4)
// With namespace name and per-namespace separators:
#define SZ_NS_ENCODED_BITMAP	(64 + SZ_ENCODED_BITMAP + 1)
// Finally:
#define SZ_REPLICAS_TEXT 		(SZ_OVERHEAD + (NUM_NAMESPACES * 2 * SZ_NS_ENCODED_BITMAP))
/*
 *  Example:
 *  partition-generation 292219
 * 	replicas-master      foo:Ab2T60...;bar:ry4Jfs...; ...
 * 	replicas-prole       foo:8xd4K2...;bar:4hTe5q...; ...
 */

#define SHMMAX_SYS_FILE 		"/proc/sys/kernel/shmmax"
#define DEFAULT_NUM_NODES_FOR_SHM 	64
#define DEFAULT_SHM_KEY 		229857887

/* The shm structure has some metadata (updater_id, node_count, global lock)
 * and then start the actual node information. Each node's information is further
 * represented by a structure cl_shm_ninfo which has a socket address, node level lock
 * and the fields */
typedef struct {
	struct sockaddr_in address_array[MAX_ADDRESSES_PER_NODE];
	pthread_mutex_t ninfo_lock;
	int address_count;
	uint32_t partition_generation;
	char node_name[NODE_NAME_SIZE];
	char services[SZ_FIELD_NEIGHBORS];
	char replicas[SZ_REPLICAS_TEXT];
} cl_shm_ninfo;

typedef struct {
	size_t updater_id;
	int node_count;
	int partition_count;
	pthread_mutex_t shm_lock;

	/* Change this approach to calculating address 
 	 * of all the structures in the shared memory upfront*/
	cl_shm_ninfo node_info[];
} cl_shm;

/* This is a global structure which has shared memory information like size,
 * its nodes size, and id, the update thread period and the condition on which it will end itself*/
typedef struct {
	int id;
	size_t shm_sz;
	size_t node_sz;
	/*Condition on which the updater thread will exit*/
	bool update_thread_end_cond;
	int update_period;
} cl_shm_info;

/*Switch to move between shared memory and back*/
extern bool g_shared_memory;

/*Shared memory functions*/
int citrusleaf_use_shm(int num_nodes, key_t key);
int citrusleaf_shm_free();

int cl_shm_get_partition_count();

cl_shm_ninfo* cl_shm_find_node_from_name(const char* node_name);
cl_shm_ninfo* cl_shm_find_node_from_address(struct sockaddr_in* sa_in);

int cl_shm_node_lock(cl_shm_ninfo* shared_node);
void cl_shm_node_unlock(cl_shm_ninfo* shared_node);
