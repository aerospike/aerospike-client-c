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
typedef enum as_policy_repl_e {
	AS_POLICY_REPL_ASYNC, 
	AS_POLICY_REPL_ONESHOT, 
	AS_POLICY_REPL_RETRY
} as_policy_repl;

/**
 * Generation Policy
 */
typedef enum as_policy_gen_e {
	AS_POLICY_GEN_DEFAULT,      // write a record, regardless of generation
	AS_POLICY_GEN_EQ,           // write a record, iff generations are equal
	AS_POLICY_GEN_GT,           // write a record, iff local generation is greater-than remote generation
	AS_POLICY_GEN_DUP           // write a record creating a duplicate, iff the generation collides (?)
} as_policy_gen;

/**
 * Digest (Key) Policy
 */
typedef enum as_policy_digest_e {
	AS_POLICY_DIGEST_DEFAULT,   // write a record - regardless if digest exists
	AS_POLICY_DIGEST_CREATE,    // create a record - digest SHOULD NOT exist
	AS_POLICY_DIGEST_UPDATE     // update a record - digest SHOULD exist
} as_policy_digest;

/**
 * Write Policy
 */
typedef struct as_policy_write_s {
	uint32_t            timeout;
	bool                unique;
	uint32_t            generation;
	as_policy_digest    digest;
	as_policy_repl      repl;
	as_policy_gen       gen;
} as_policy_write;


/**
 * Removal Policy
 */
typedef struct as_policy_remove_s {
	uint32_t        timeout;
	uint32_t        generation;
	as_policy_repl  repl;
	as_policy_gen   gen;
} as_policy_remove;

/**
 * Read Policy
 */
typedef struct as_policy_read_s {
	uint32_t timeout;
} as_policy_read;

/**
 * Query Policy
 */
typedef struct as_policy_query_s {
	uint32_t timeout;
} as_policy_query;

/**
 * Scan Policy
 */
typedef struct as_policy_scan_s {
	uint32_t    timeout;
	bool        fail_on_cluster_change;
} as_policy_scan;

/**
 * Info Policy
 */
typedef struct as_policy_info_s {
	uint32_t timeout;       // timeout(ms)
	bool send_as_is;        // send ... as is ... ?
	bool check_bounds;      // check bounds of ... ?
} as_policy_info;

/**
 * struct of all the policies.
 * This is utilizes for defining defaults within an aerospike 
 * client or configuration.
 */
typedef struct as_policies_s {
	uint32_t            timeout;        // default timeout(ms), used when policy timeout is 0 (zero)
	as_policy_write     write;
	as_policy_read      read;
	as_policy_remove    remove;
	as_policy_query     query;
	as_policy_scan      scan;
	as_policy_info      info;
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

