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

#include <stdbool.h>
#include <citrusleaf/citrusleaf.h>

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
