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
#include <stdint.h>

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * Replication Policy
 */
typedef enum as_policy_writemode_e {
	AS_POLICY_WRITEMODE_ASYNC, 
	AS_POLICY_WRITEMODE_ONESHOT, 
	AS_POLICY_WRITEMODE_RETRY
} as_policy_writemode;

/**
 * Generation Policy
 */
typedef enum as_policy_gen_e {

	/**
	 * Write a record, regardless of generation
	 */
	AS_POLICY_GEN_DEFAULT,

	/**
	 * Write a record, iff generations are equal
	 */
	AS_POLICY_GEN_EQ,

	/**
	 * Write a record, iff local generation is greater-than remote generation
	 */
	AS_POLICY_GEN_GT,

	/**
	 * Write a record creating a duplicate, iff the generation collides (?)
	 */
	AS_POLICY_GEN_DUP

} as_policy_gen;

/**
 * Digest (Key) Policy
 */
typedef enum as_policy_digest_e {

	/**
	 * Create or update a record, regarldess of 
	 * its existence.
	 */
	AS_POLICY_DIGEST_DEFAULT,

	/**
	 * Create a record, if it doesn't exist.
	 */
	AS_POLICY_DIGEST_CREATE,

	/**
	 * Update a record, if it exists.
	 */
	AS_POLICY_DIGEST_UPDATE

} as_policy_digest;

/**
 * Write Policy
 */
typedef struct as_policy_write_s {

	/**
	 * Maximum time in milliseconds to wait for 
	 * the operation to complete.
	 */
	uint32_t            timeout;

	/**
	 * The write mode defines the behavior 
	 * for writing data to the cluster.
	 */
	as_policy_writemode mode;

	/**
	 * Specifies the behavior for the digest value
	 */
	as_policy_digest    digest;

	/**
	 * Specifies the behavior for the generation
	 * value.
	 */
	as_policy_gen       gen;

} as_policy_write;


/**
 * Removal Policy
 */
typedef struct as_policy_remove_s {

	/**
	 * Maximum time in milliseconds to wait for 
	 * the operation to complete.
	 */
	uint32_t timeout;

	/**
	 * The generation of the record.
	 */
	uint16_t generation;

	/**
	 * The write mode defines the behavior 
	 * for writing data to the cluster.
	 */
	as_policy_writemode mode;

	/**
	 * Specifies the behavior for the generation
	 * value.
	 */
	as_policy_gen gen;

} as_policy_remove;

/**
 * Read Policy
 */
typedef struct as_policy_read_s {

	/**
	 * Maximum time in milliseconds to wait for 
	 * the operation to complete.
	 */
	uint32_t timeout;

} as_policy_read;

/**
 * Query Policy
 */
typedef struct as_policy_query_s {

	/**
	 * Maximum time in milliseconds to wait for 
	 * the operation to complete.
	 */
	uint32_t timeout;

} as_policy_query;

/**
 * Scan Policy
 */
typedef struct as_policy_scan_s {

	/**
	 * Maximum time in milliseconds to wait for 
	 * the operation to complete.
	 */
	uint32_t timeout;

	/**
	 * Abort the scan if the cluster is not in a 
	 * stable state.
	 */
	bool fail_on_cluster_change;

} as_policy_scan;

/**
 * Info Policy
 */
typedef struct as_policy_info_s {

	/**
	 * Maximum time in milliseconds to wait for 
	 * the operation to complete.
	 */
	uint32_t timeout;

	/**
	 * @todo Provide a description
	 */
	bool send_as_is;

	/**
	 * @todo Provide a description
	 */
	bool check_bounds;

} as_policy_info;

/**
 * struct of all the policies.
 * This is utilizes for defining defaults within an aerospike 
 * client or configuration.
 */
typedef struct as_policies_s {

	/**
	 * Default timeout in milliseconds.
	 * Will be used if specific policies have a timeout of 0 (zero).
	 */
	uint32_t timeout;

	/**
	 * The default write policy.
	 */
	as_policy_write write;

	/**
	 * The default read policy.
	 */
	as_policy_read read;

	/**
	 * The default remove policy.
	 */
	as_policy_remove remove;

	/**
	 * The default query policy.
	 */
	as_policy_query query;

	/**
	 * The default scan policy.
	 */
	as_policy_scan scan;

	/**
	 * The default info policy.
	 */
	as_policy_info info;

} as_policies;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * Initialize as_policy_write to default values.
 */
as_policy_write * as_policy_write_init(as_policy_write * p) ;

/**
 * Initialize as_policy_read to default values.
 */
as_policy_read * as_policy_read_init(as_policy_read * p);

/**
 * Initialize as_policy_remote to default values.
 */
as_policy_remove * as_policy_remove_init(as_policy_remove * p);

/**
 * Initialize as_policy_scan to default values.
 */
as_policy_scan * as_policy_scan_init(as_policy_scan * p);

/**
 * Initialize as_policy_query to default values.
 */
as_policy_query * as_policy_query_init(as_policy_query * p);

/**
 * Initialize as_policy_info to default values.
 */
as_policy_info * as_policy_info_init(as_policy_info * p);

/**
 * Initialize as_policies to default values.
 */
as_policies * as_policies_init(as_policies * p);

