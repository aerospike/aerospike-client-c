#include "citrusleaf.h"
//#include "citrusleaf/citrusleaf-internal.h"
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<pthread.h>
#define NUM_NODES 128
#define NUM_NAMESPACES 10
#define SZ_SHM (sizeof(size_t) + 2*sizeof(pthread_mutex_t) + 2*sizeof(int) + (NUM_NODES * SZ_NODE))
#include<sys/shm.h>
/*Shared memory return values*/
#define SHM_ERROR -1
#define SHM_OK 0

//The shared memory is divided into nodes, each node has a socket address structure and 
//the data associated with that structure. 
#define SZ_SOCK sizeof(struct sockaddr_in)
#define SHM_FIELD_COUNT 4
#define SZ_FIELD_NAME 32
#define SZ_NODE_NAME 32
#define SZ_NAMESPACE 32
#define SZ_PARTITION_ID sizeof(size_t)
#define MAX_NEIGHBORS (NUM_NODES-1)
#define NUM_PARTITIONS 4096
#define SZ_PARTITION_GEN sizeof(size_t)

#define SZ_FIELD_NODE_NAME SZ_FIELD_NAME + SZ_NODE_NAME
#define SZ_FIELD_NEIGHBORS SZ_FIELD_NAME*3 + SZ_NODE_NAME + SZ_PARTITION_GEN + MAX_NEIGHBORS*SZ_NODE_NAME
#define SZ_FIELD_PARTITIONS 2*(SZ_FIELD_NAME + (SZ_NAMESPACE + 2 + SZ_PARTITION_ID)*NUM_PARTITIONS*NUM_NAMESPACES)
#define SZ_FIELD_NUM_PARTITIONS SZ_FIELD_NAME + sizeof(size_t)
typedef struct {
	struct sockaddr_in sa_in;
	pthread_mutex_t ninfo_lock;

	/*Field data*/	
	char node_name[SZ_FIELD_NODE_NAME];
	char neighbors[SZ_FIELD_NEIGHBORS];
	char partitions[SZ_FIELD_PARTITIONS];
	char num_partitions[SZ_FIELD_NUM_PARTITIONS];
	
} shm_ninfo;

typedef struct{
	size_t updater_id;
	int node_count;
	pthread_mutex_t shm_lock;
	shm_ninfo node_info[NUM_NODES];
} shm;


typedef struct {
	int id;
	size_t shm_sz;
	size_t node_sz;
	/*Condition on which the updater thread will exit*/
	bool update_thread_end_cond;
	int update_speed;


}shm_info;	

typedef struct shm_header_info_s {
	char name[64];
	size_t offset;
	size_t size;
}shm_header_info;

/*Switch to move between shared memory and back*/
extern bool SHARED_MEMORY;
extern shm_header_info g_shm_header_info[SHM_FIELD_COUNT];
/*Shared memory functions*/
int cl_shm_init(void);
extern void * g_shm_base;
int cl_shm_free();
extern int g_shmid;

void * cl_shm_updater_fn(void *);
extern pthread_t shm_update_thr;
int cl_shm_info_host(struct sockaddr_in * sa_in, char * names, char ** values, int timeout_ms, bool send_asis);
int cl_shm_read(struct sockaddr_in * sa_in, int field_type, char **values, int timeout, bool send_asis);
int cl_shm_get_size(char * name);
