#include "citrusleaf.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/shm.h>

#define NUM_NODES 128
#define NUM_NAMESPACES 10

/*Shared memory return values*/
#define SHM_ERROR -1
#define SHM_OK 0

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

#define SZ_FIELD_PARTITIONS 	(NUM_NAMESPACES * NUM_PARTITIONS * (SZ_NAMESPACE + SZ_PARTITION_ID + 2) + 1)
/*
 *  Example:
 * 	replicas-read   bar:0;bar:2;bar:3;.....
 *	replicas-write  bar:0;bar:4;bar:5;.....
 */

#define SHMMAX_SYS_FILE 		"/proc/sys/kernel/shmmax"
#define DEFAULT_NUM_NODES_FOR_SHM 	64
#define DEFAULT_SHM_KEY 		229857887

/* The shm structure has some metadata (updater_id, node_count, global lock)
 * and then start the actual node information. Each node's information is further
 * represented by a structure shm_ninfo which has a socket address, node level lock
 * and the fields */
typedef struct {
	struct sockaddr_in address_array[MAX_ADDRESSES_PER_NODE];
	pthread_mutex_t ninfo_lock;
	int address_count;
	uint32_t partition_generation;
	char node_name[NODE_NAME_SIZE];
	char services[SZ_FIELD_NEIGHBORS];
	char write_replicas[SZ_FIELD_PARTITIONS];
	char read_replicas[SZ_FIELD_PARTITIONS];
	bool dun;
} shm_ninfo;

typedef struct {
	size_t updater_id;
	int node_count;
	int partition_count;
	pthread_mutex_t shm_lock;

	/* Change this approach to calculating address 
 	 * of all the structures in the shared memory upfront*/
	shm_ninfo node_info[];
} shm;

/* This is a global structure which has shared memory information like size,
 * its nodes size, and id, the update thread period and the condition on which it will end itself*/
typedef struct {
	int id;
	size_t shm_sz;
	size_t node_sz;
	/*Condition on which the updater thread will exit*/
	bool update_thread_end_cond;
	int update_period;
} shm_info;

/*Switch to move between shared memory and back*/
extern bool g_shared_memory;

/*Shared memory functions*/
int citrusleaf_use_shm(int num_nodes, key_t key);
int citrusleaf_shm_free();

int cl_shm_get_partition_count();

shm_ninfo* cl_shm_find_node_from_name(const char* node_name);
shm_ninfo* cl_shm_find_node_from_address(struct sockaddr_in* sa_in);

int cl_shm_node_lock(shm_ninfo* shared_node);
void cl_shm_node_unlock(shm_ninfo* shared_node);
