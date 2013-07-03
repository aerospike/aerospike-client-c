/******************************************************************************
 *	Copyright 2008-2013 by Aerospike.
 *
 *	Permission is hereby granted, free of charge, to any person obtaining a copy 
 *	of this software and associated documentation files (the "Software"), to 
 *	deal in the Software without restriction, including without limitation the 
 *	rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 *	sell copies of the Software, and to permit persons to whom the Software is 
 *	furnished to do so, subject to the following conditions:
 *	
 *	The above copyright notice and this permission notice shall be included in 
 *	all copies or substantial portions of the Software.
 *	
 *	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 *	FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 *	IN THE SOFTWARE.
 *****************************************************************************/

#pragma once 

#include <netinet/in.h>
#include <sys/socket.h>

/******************************************************************************
 *	MACROS
 *****************************************************************************/

/**
 *	The size of as_node.name
 */
#define AS_NODE_NAME_MAX_SIZE 20

/**
 *	The maximum string length of as_node.name
 */
#define AS_NODE_NAME_MAX_LEN AS_NODE_NAME_MAX_SIZE - 1

/******************************************************************************
 *	TYPES
 *****************************************************************************/

/**
 *	Represents a node in the cluster.
 */
typedef struct as_node_s {

	/**
	 *	The name of the node.
	 */
	char name[AS_NODE_NAME_MAX_SIZE];
	
} as_node;
