/*
 * Copyright 2012 Aerospike. All rights reserved.
 */
#pragma once

#include <stdbool.h>
#include "citrusleaf.h"

typedef struct {
	char* values;
	char node_name[NODE_NAME_SIZE];
	char* services;
	uint32_t partition_generation;
	bool dun;
} cl_node_info;

typedef struct {
	char* values;
	char* write_replicas;
	char* read_replicas;
} cl_replicas;

int cl_get_node_info(const char* node_name, struct sockaddr_in* sa_in, cl_node_info* node_info);
int cl_request_node_info(struct sockaddr_in* sa_in, cl_node_info* node_info);
void cl_node_info_free(cl_node_info* node_info);

int cl_get_replicas(const char* node_name, struct sockaddr_in* sa_in, cl_replicas* replicas);
int cl_request_replicas(struct sockaddr_in* sa_in, cl_replicas* replicas);
void cl_replicas_free(cl_replicas* replicas);

int cl_get_node_name(struct sockaddr_in* sa_in, char* node_name);
int cl_request_node_name(struct sockaddr_in* sa_in, char* node_name);

int cl_get_n_partitions(struct sockaddr_in* sa_in, int* n_partitions);
int cl_request_n_partitions(struct sockaddr_in* sa_in, int* n_partitions);

bool cl_strncpy(char* trg, const char* src, int len);
