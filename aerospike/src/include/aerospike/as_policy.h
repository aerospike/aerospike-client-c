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

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * Replication Policy
 */
enum as_policy_repl_e {
	AS_POLICY_REPL_ASYNC, 
	AS_POLICY_REPL_ONESHOT, 
	AS_POLICY_REPL_RETRY
};

typedef enum as_policy_repl_e as_policy_repl;

/**
 * Generation Policy
 */
enum as_policy_gen_e {
	AS_POLICY_GEN_DEFAULT,      // write a record, regardless of generation
	AS_POLICY_GEN_EQ,           // write a record, iff generations are equal
	AS_POLICY_GEN_GT,           // write a record, iff local generation is greater-than remote generation
	AS_POLICY_GEN_DUP           // write a record creating a duplicate, iff the generation collides (?)
};

typedef enum as_policy_gen_e as_policy_gen;

/**
 * Digest (Key) Policy
 */
enum as_policy_digest_e {
	AS_POLICY_DIGEST_DEFAULT,   // write a record - regardless if digest exists
	AS_POLICY_DIGEST_CREATE,    // create a record - digest SHOULD NOT exist
	AS_POLICY_DIGEST_UPDATE     // update a record - digest SHOULD exist
};

typedef enum as_policy_digest_e as_policy_digest;

/**
 * Write Policy
 */
struct as_policy_write_s {
	uint32_t            timeout;
	bool                unique;
	uint32_t            generation;
	as_policy_digest    digest;
	as_policy_repl      repl;
	as_policy_gen       gen;
};

typedef struct as_policy_write_s as_policy_write;


/**
 * Removal Policy
 */
struct as_policy_remove_s {
	uint32_t        timeout;
	uint32_t        generation;
	as_policy_repl  repl;
	as_policy_gen   gen;
};

typedef struct as_policy_remove_s as_policy_remove;

/**
 * Read Policy
 */
struct as_policy_read_s {
	uint32_t timeout;
};

typedef struct as_policy_read_s as_policy_read;

/**
 * Query Policy
 */
struct as_policy_query_s {
	uint32_t timeout;
};

typedef struct as_policy_query_s as_policy_query;

/**
 * Priority levels for a scan operation.
 */
enum as_scan_priority_e { 
	AS_SCAN_PRIORITY_AUTO, 
	AS_SCAN_PRIORITY_LOW, 
	AS_SCAN_PRIORITY_MEDIUM, 
	AS_SCAN_PRIORITY_HIGH
};

typedef enum as_scan_priority_e as_scan_priority;

/**
 * Scan Policy
 */
struct as_policy_scan_s {
	uint32_t    timeout;
	bool        fail_on_cluster_change;
};

typedef struct as_policy_scan_s as_policy_scan;

/**
 * Info Policy
 */
struct as_policy_info_s {
	uint32_t timeout;       // timeout(ms)
	bool send_as_is;        // send ... as is ... ?
	bool check_bounds;      // check bounds of ... ?
};

typedef struct as_policy_info_s as_policy_info;

/**
 * LDT Policy
 */
struct as_policy_ldt_s {
	uint32_t timeout;       // timeout(ms)
};

typedef struct as_policy_ldt_s as_policy_ldt;

/**
 * struct of all the policies.
 * This is utilizes for defining defaults within an aerospike 
 * client or configuration.
 */
struct as_policies_s {
	uint32_t            timeout;        // default timeout(ms), used when policy timeout is 0 (zero)
	as_policy_write     write;
	as_policy_read      read;
	as_policy_remove    remove;
	as_policy_query     query;
	as_policy_scan      scan;
	as_policy_info      info;
	as_policy_ldt       ldt;
};

typedef struct as_policies_s as_policies;
