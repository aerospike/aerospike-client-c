#include <sys/types.h>
#include <sys/socket.h> // socket calls
#include <stdio.h>
#include <errno.h> //errno
#include <stdlib.h> //fprintf
#include <unistd.h> // close
#include <string.h>
#include <pthread.h>
#include <fcntl.h>
#include <arpa/inet.h> // inet_ntop
#include <signal.h>

#include <netdb.h> //gethostbyname_r

#include "citrusleaf/citrusleaf.h"
#include "citrusleaf/citrusleaf-internal.h"
#include "citrusleaf/cl_cluster.h"
#include "citrusleaf/proto.h"
#include "citrusleaf/cl_shm.h"
#include "citrusleaf/cf_socket.h"

#define INFO_TIMEOUT_MS 300
#define DEBUG 1

/*Do we want shared memory?*/
bool SHARED_MEMORY=true;

/* Shared memory global variables */
shm_info g_shm_info;

/* Shared memory pointer */
shm * g_shm_pt;

/* Shared memory updater thread */
pthread_t shm_update_thr;

/* Initialize shared memory segment */
int cl_shm_init() {
	/* Get key */
	key_t key;
	
	/* Use ftok to generate a key associated with a file. Every process will get the same key
 	 * back if the caller calls the same parameters */
	key = ftok("/home/sunanda/shar_client/client/client/c_clients/cl_c/shm_key",42);
	#ifdef DEBUG
		fprintf(stderr,"Shared memory key is %d\n",key);
	#endif
	if(key == -1) {
		#ifdef DEBUG
			fprintf(stderr,"Ftok failed with error = %s\n",strerror(errno));
		#endif
		return SHM_ERROR;
	}
	void * shm_pt = (void*)0;
	
	/* The size of the shared memory is the size of the shm structure*/
	g_shm_info.shm_sz = sizeof(shm);
	
	/* Fix the update thread end condition to false and update speed to 1*/
	g_shm_info.update_thread_end_cond = false;
	g_shm_info.update_speed = 1;
	
	/*First try to exclusively create a shared memory, for this only one process will succeed.
 	 * others will fail giving an errno of EEXIST*/
	 if((g_shm_info.id = shmget(key,g_shm_info.shm_sz,IPC_CREAT | IPC_EXCL | 0666))<0) {
		/*if there are any other errors apart from EEXIST, we can return gracefully */
		if(errno != EEXIST) {
			#ifdef DEBUG
				fprintf(stderr,"Error in getting shared memory: %s\n",strerror(errno));
			#endif
			return SHM_ERROR;	
		}
		else {
		/*For all the processes that failed to create with EEXIST, we try to get the shared memory again so that we
 		* have a valid shmid */
			if((g_shm_info.id = shmget(key,g_shm_info.shm_sz,IPC_CREAT | 0666))<0) {
				#ifdef DEBUG
					fprintf(stderr,"Error in getting shared memory: %s\n",strerror(errno));
				#endif
				return SHM_ERROR;
			}
	
			/* Attach to the shared memory */	
			if((shm_pt = shmat(g_shm_info.id,NULL,0))==(void*)-1) {
				#ifdef DEBUG
					fprintf(stderr,"Error in attaching to shared memory: %s pid: %d\n",strerror(errno),getpid());
				#endif	
				return SHM_ERROR;
			}
		
			/*The shared memory base pointer*/	
			g_shm_pt = (shm*)shm_pt;
			g_shm_pt->node_count = 0;
		}
	}
	else {
	/* The process who got the shared memory in the exclusive case */
		
		#ifdef DEBUG
			fprintf(stderr,"Succeeded in creating shm : pid %d shmid %d\n",getpid(),g_shm_info.id);
		#endif
		
		/* Attach to the shared memory */	
		if((shm_pt = shmat(g_shm_info.id,NULL,0))==(void*)-1) {
			#ifdef DEBUG
				fprintf(stderr,"Error in attaching to shared memory: %s pid: %d\n",strerror(errno),getpid());
			#endif	
			return SHM_ERROR;
		}

		/*The shared memory base pointer*/	
		g_shm_pt = (shm*)shm_pt;
		memset(g_shm_pt,0,g_shm_info.shm_sz);	

		/* If you are the one who created the shared memory, only you can initialize the mutexes. Seems fair!
 	 	* because if we let everyone initialize the mutexes, we are in deep deep trouble */	
		pthread_mutexattr_t attr;
		pthread_mutexattr_init (&attr);
		pthread_mutexattr_setpshared (&attr, PTHREAD_PROCESS_SHARED);
		pthread_mutexattr_setrobust_np (&attr, PTHREAD_MUTEX_ROBUST_NP);
		if(pthread_mutex_init (&(g_shm_pt->shm_lock), &attr)!=0) {
			#ifdef DEBUG
				fprintf(stderr,"Mutex init failed pid %d\n",getpid());
			#endif
			pthread_mutexattr_destroy(&attr);
			return SHM_ERROR;
		}
		pthread_mutexattr_destroy(&attr);
		return SHM_OK;	
	}

	/* all well? return aok!*/
	return SHM_OK;
}

/* Detach and remove shared memory */
int cl_shm_free() {
	/* signal the update thread to exit and wait till it exits */
	g_shm_info.update_thread_end_cond = true;
	pthread_join(shm_update_thr,NULL);

	/*Destroy all the mutexes - that includes the global lock as well as all the node level locks*/
	pthread_mutex_destroy(&(g_shm_pt->shm_lock));
	for(int i=0; i < g_shm_pt->node_count; i++) {
		pthread_mutex_destroy(&(g_shm_pt->node_info[i].ninfo_lock));	
	}
	
	/*Detach shared memory*/
	if(shmdt(g_shm_pt) < 0 ) {
		#ifdef DEBUG
			fprintf(stderr,"Error in detaching from shared memory: %s pid: %d\n",strerror(errno),getpid());
		#endif
		return SHM_ERROR;
	}
	
	/*Try removing the shared memory - it will fail if any other process is still attached*/
	if(shmctl(g_shm_info.id,IPC_RMID,0) < 0 ) return SHM_ERROR;
}

/*Just for testing purposes, we want to see who is updating the shared memory*/
void cl_shm_set_updater_id(size_t pid) {
	g_shm_pt->updater_id = pid;
	return;
}

/* Given a field id and the position of a node in the shared memory, this function will return
 * the position of that particular field in that node*/
char * get_field_address(shm_ninfo * node_info , int field_id) {
	switch(field_id) {
		case 0:
			return &(node_info->node_name);
		case 1:
			return &(node_info->neighbors);
		case 2:
			return &(node_info->partitions);
		case 3:
			return &(node_info->num_partitions);
	}
	return NULL;
}

/* Allocate from the shared memory*/
int  cl_shm_alloc(struct sockaddr_in * sa_in, int field_id, char ** values, int * pos) {
	int rv;
	int found = 0;
	char * field_addr;

	//Search for the current sockaddr
	for(int i=0; i<g_shm_pt->node_count; i++) {
		if(memcmp(&(g_shm_pt->node_info[i].sa_in),sa_in,sizeof(struct sockaddr_in))==0) {
			found = 1;
			/*Fills the position of the node in the shared memory*/
			*pos = i;
			break;
		}
	}
	//Found the socket address! Yay! Just need to place the data
	if(found==1) {
		field_addr = get_field_address(&(g_shm_pt->node_info[*pos]),field_id);
		if(field_addr==NULL) {
			#ifdef DEBUG
				fprintf(stderr,"Failed to get a field address, returning\n");
			#endif
			return SHM_ERROR;
		}
		/* Fill the returning values with the address of the field returned from the above function*/
		*values = field_addr;
	}
	else {
		/*In the case the node is not found, place the node at the last offset*/
		memcpy(&(g_shm_pt->node_info[g_shm_pt->node_count].sa_in),sa_in,sizeof(struct sockaddr_in));
		
		/*Initialise the lock - only the updater can initialize the node level mutex*/
		size_t selfpid = getpid();
		if(selfpid == g_shm_pt->updater_id) {
			pthread_mutexattr_t attr;
			pthread_mutexattr_init (&attr);
			pthread_mutexattr_setpshared (&attr, PTHREAD_PROCESS_SHARED);
			pthread_mutexattr_setrobust_np (&attr, PTHREAD_MUTEX_ROBUST_NP);
			if(pthread_mutex_init (&(g_shm_pt->node_info[g_shm_pt->node_count].ninfo_lock), &attr)!=0) {
				#ifdef DEBUG
					fprintf(stderr,"node level mutex init failed pid %d\n",getpid());
				#endif
				pthread_mutexattr_destroy(&attr);
				return SHM_ERROR;
			}
			pthread_mutexattr_destroy(&attr);
		}	
		field_addr = get_field_address(&(g_shm_pt->node_info[g_shm_pt->node_count]),field_id);
		
		if(field_addr==NULL) {
			#ifdef DEBUG
				fprintf(stderr,"Failed to get the field address\n");
			#endif
			return SHM_ERROR;
		}

		/*Return the position of the data of the particular string name. Move by sizeof a socket address, a lock
 		 * and the offset of string name in the process*/
		*values = field_addr;
		
		/* A new node is allocated, increase the count*/
		g_shm_pt->node_count++;
	}
	return SHM_OK;	
}
/* Get data from the host and update the shared memory*/
int cl_shm_info_host(struct sockaddr_in * sa_in, char * names, char ** values, int timeout_ms, bool send_asis) {
	int rv = -1;
    	int io_rv;
	*values = 0;
	
	// Deal with the incoming 'names' parameter
	// Translate interior ';'  in the passed-in names to \n
	uint32_t	slen = 0;
	if (names) {
		if (send_asis) {
			slen = strlen(names);
		} else {
			char *_t = names;
			while (*_t) { 
				slen++; 
				if ((*_t == ';') || (*_t == ':') || (*_t == ',')) *_t = '\n'; 
				_t++; 
			}
		}
	}
	
	// Sometimes people forget/cant add the trailing '\n'. Be nice and add it for them.
	// using a stack allocated variable so we dn't have to clean up, Pain in the ass syntactically
	// but a nice little thing
	if (names) {
		if (names[slen-1] == '\n') {
			slen = 0;
		} else { 
			slen++; if (slen > 1024) { return(-1); } 
		}
	}
	char names_with_term[slen+1];
	if (slen) { 
		strcpy(names_with_term, names);
		names_with_term[slen-1] = '\n';
		names_with_term[slen] = 0;
		names = names_with_term;
	}
		
	// Actually doing a non-blocking connect
	int fd = cf_create_nb_socket(sa_in, timeout_ms);
	if (fd == -1) {
		return (-1);
	}

	cl_proto 	*req;
	uint8_t		buf[1024];
	uint		buf_sz;
	
	if (names) {
		uint sz = strlen(names);
		buf_sz = sz + sizeof(cl_proto);
		if (buf_sz < 1024)
			req = (cl_proto *) buf;
		else
			req = (cl_proto *) malloc(buf_sz);
		if (req == NULL)	goto Done;

		req->sz = sz;
		memcpy(req->data,names,sz);
	}
	else {
		req = (cl_proto *) buf;
		req->sz = 0;
		buf_sz = sizeof(cl_proto);
	}
		
	req->version = CL_PROTO_VERSION;
	req->type = CL_PROTO_TYPE_INFO;
	cl_proto_swap(req);
	
    if (timeout_ms)
        io_rv = cf_socket_write_timeout(fd, (uint8_t *) req, buf_sz, 0, timeout_ms);
    else
        io_rv = cf_socket_write_forever(fd, (uint8_t *) req, buf_sz);
    
	if ((uint8_t *)req != buf){
		free(req->data);
		free(req);
	}
	if (io_rv != 0) {
#ifdef DEBUG        
		fprintf(stderr, "info returned error, rv %d errno %d bufsz %d\n",io_rv, errno, buf_sz);
#endif        
		goto Done;
	}
	
	cl_proto	*rsp = (cl_proto *)buf;
    if (timeout_ms) 
        io_rv = cf_socket_read_timeout(fd, buf, 8, 0, timeout_ms);
    else
        io_rv = cf_socket_read_forever(fd, buf, 8);
    
    if (0 != io_rv) {
#ifdef DEBUG        
		fprintf(stderr, "info read not 8 bytes, fail, rv %d errno %d\n",rv, errno);
#endif        
		goto Done;
	}
	cl_proto_swap(rsp);
	
	if (rsp->sz) {
		/* Allocate a buffer in the local memory which you can send to the server to get the values*/
		uint8_t *v_buf = malloc(rsp->sz + 1);
		if (!v_buf) goto Done;
        
        if (timeout_ms)
            io_rv = cf_socket_read_timeout(fd, v_buf, rsp->sz, 0, timeout_ms);
        else
            io_rv = cf_socket_read_forever(fd, v_buf, rsp->sz);
        
        if (io_rv != 0) {
            free(v_buf);
            goto Done;
		}
			
		v_buf[rsp->sz] = 0;
		int pos;
		int field_id=-1;
		if(strcmp(names,"node\n")==0) {
			field_id = 0;
		}
		else if(strcmp(names,"node\npartition-generation\nservices\n")==0) {
			field_id = 1;
		}
		else if(strcmp(names,"replicas-read\nreplicas-write\n")==0) {
			field_id = 2;
		}
		else if(strcmp(names,"partitions\n")==0) {
			field_id = 3;
		}
		/*This buffer is then copied to *values and sent back to the program that called for it*/
		rv = cl_shm_alloc(sa_in,field_id,values,&pos);
		if(*values==NULL ||  rv!=0) {
			goto Done;
		}
		/* Take the lock while updating the shared memory so that no one else can read it */
		pthread_mutex_lock(&(g_shm_pt->node_info[pos].ninfo_lock));
		memcpy(*values,v_buf,rsp->sz + 1);
		pthread_mutex_unlock(&(g_shm_pt->node_info[pos].ninfo_lock));
		
	}                                                                                               
	else {
		*values = 0;
	}
	rv = 0;

Done:	
	shutdown(fd, SHUT_RDWR);
	close(fd);
	
	return(rv);
	
}

/* Read from the shared memory and put into the pointer passed (values) */
int cl_shm_read(struct sockaddr_in * sa_in, int field_id, char **values, int timeout, bool send_as_is){
	//Search in the shared memory with sa_in. 
	for(int i=0; i<g_shm_pt->node_count; i++){
		/*Search for this socket address in the shared memory*/
		if(memcmp(&(g_shm_pt->node_info[i].sa_in),sa_in,sizeof(struct sockaddr_in))==0){
			/* Found it! */
			char * field_addr = get_field_address(&(g_shm_pt->node_info[i]),field_id);
			if(field_addr==NULL) {
				#ifdef DEBUG
					fprintf(stderr,"Failed to return a field address, returning\n");
				#endif
				return SHM_ERROR;
			}
			/* Take the lock to read the data into *values from the shared memory so that the updater
 			 * can not update in between */
			pthread_mutex_lock(&(g_shm_pt->node_info[i].ninfo_lock));
			memcpy(*values, field_addr, strlen((char*)field_addr));
			pthread_mutex_unlock(&(g_shm_pt->node_info[i].ninfo_lock));
			/*Everything went well, return  aok */
			return SHM_OK;
		}
		
	}
	return SHM_ERROR;	
}

void cl_shm_update(cl_cluster * asc) {
	/* Check if the thread is set to exit or not. If it is, gracefully exit*/	
	if(g_shm_info.update_thread_end_cond) {
		pthread_exit(NULL);
	}
	//Take Lock - only one process can update
	int rv = pthread_mutex_trylock(&(g_shm_pt->shm_lock));
	if(rv==0) {
		//Update the updater id in the shared memory
		size_t self_pid = getpid();
		cl_shm_set_updater_id(self_pid);

		//Update shared memory
		uint n_hosts = cf_vector_size(&asc->host_str_v);
		cf_vector_define(sockaddr_in_v, sizeof( struct sockaddr_in ), 0);
		for (uint i=0;i<n_hosts;i++) {
			//For debug
			char *host = cf_vector_pointer_get(&asc->host_str_v, i);	
	        	int port = cf_vector_integer_get(&asc->host_port_v, i);
			
			//Resolve hosts and store them in sockaddr_in_v
			cl_lookup(asc, cf_vector_pointer_get(&asc->host_str_v, i), 
					cf_vector_integer_get(&asc->host_port_v, i),
					&sockaddr_in_v);
		}
		for (uint i=0;i<cf_vector_size(&sockaddr_in_v);i++) {
			struct sockaddr_in *sa_in = cf_vector_getp(&sockaddr_in_v,i);
			char * values;
			cl_shm_info_host(sa_in,"node",&values,INFO_TIMEOUT_MS, false);
			cl_shm_info_host(sa_in,"node\npartition-generation\nservices",&values,INFO_TIMEOUT_MS, false);
			cl_shm_info_host(sa_in,"replicas-read\nreplicas-write",&values,INFO_TIMEOUT_MS,false);
			cl_shm_info_host(sa_in,"partitions",&values,INFO_TIMEOUT_MS,false);
		}
		
	}
	else {
		return;
	}
}
void * cl_shm_updater_fn(void * gcc_is_ass) {
	uint64_t cnt = 1;
	do {
		sleep(1);
		cf_ll_element *e = cf_ll_get_head(&cluster_ll);
		while(e) {
			if((cnt % g_shm_info.update_speed) == 0) {
				cl_shm_update((cl_cluster*)e);
			}
		}
		cnt++;
	} while(1);
	return (0);
}
