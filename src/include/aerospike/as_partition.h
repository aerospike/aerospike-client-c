/******************************************************************************
 * Copyright 2008-2014 by Aerospike.
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

#include <aerospike/as_node.h>
#include <citrusleaf/cf_digest.h>

/******************************************************************************
 *	MACROS
 *****************************************************************************/

/**
 *	Maximum namespace size including null byte.  Effective maximum length is 31.
 */
#define AS_MAX_NAMESPACE_SIZE 32

/******************************************************************************
 *	TYPES
 *****************************************************************************/

/**
 *	@private
 *  Map of namespace data partitions to nodes.
 */
typedef struct as_partition_s {
	/**
	 *	@private
	 *  Master node for this partition.
	 */
	as_node* master;
	
	/**
	 *	@private
	 *  Prole node for this partition.
	 *  TODO - not ideal for replication factor > 2.
	 */
	as_node* prole;
} as_partition;

/**
 *	@private
 *  Map of namespace to data partitions.
 */
typedef struct as_partition_table_s {
	/**
	 *	@private
	 *	Namespace
	 */
	char ns[AS_MAX_NAMESPACE_SIZE];
	
	/**
	 *	@private
	 *  Fixed length of partition array.
	 */
	uint32_t size;

	/**
	 *	@private
	 *	Array of partitions for a given namespace.
	 */
	as_partition partitions[];
} as_partition_table;

/**
 *	@private
 *  Reference counted array of partition table pointers.
 */
typedef struct as_partition_tables_s {
	/**
	 *	@private
	 *  Reference count of partition table array.
	 */
	uint32_t ref_count;
	
	/**
	 *	@private
	 *  Length of partition table array.
	 */
	uint32_t size;

	/**
	 *	@private
	 *  Partition table array.
	 */
	as_partition_table* array[];
} as_partition_tables;

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

/**
 *	@private
 *	Create reference counted structure containing partition tables.
 */
as_partition_tables*
as_partition_tables_create(uint32_t capacity);

/**
 *	@private
 *	Destroy and release memory for partition table.
 */
void
as_partition_table_destroy(as_partition_table* table);

/**
 *	@private
 *	Get partition table given namespace.
 */
as_partition_table*
as_partition_tables_get(as_partition_tables* tables, const char* ns);

/**
 *	@private
 *	Is node referenced in any partition table.
 */
bool
as_partition_tables_find_node(as_partition_tables* tables, as_node* node);
