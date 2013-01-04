/*
 *      arglist.h
 *
 * Copyright 2008-2013 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

 #pragma once

/******************************************************************************
 * CONSTANTS
 ******************************************************************************/

typedef enum cl_write_policy_e cl_write_policy;
typedef struct cl_write_parameters_s cl_write_parameters;


enum cl_write_policy_e { 
    CL_WRITE_ASYNC, 
    CL_WRITE_ONESHOT, 
    CL_WRITE_RETRY, 
    CL_WRITE_ASSURED
};

/**
 * write info structure
 * There's a lot of info that can go into a write ---
 */
struct cl_write_parameters_s {
    bool            unique;                 // write unique - means success if didn't exist before
    bool            unique_bin;             // write unique bin - means success if the bin didn't exist before
    bool            use_generation;         // generation must be exact for write to succeed
    bool            use_generation_gt;      // generation must be less - good for backup & restore
    bool            use_generation_dup;     // on generation collision, create a duplicat
    uint32_t        generation;
    int             timeout_ms;
    uint32_t        record_ttl;             // seconds, from now, when the record would be auto-removed from the DBcd 
    cl_write_policy w_pol;
};

/******************************************************************************
 * INLINE FUNCTIONS
 ******************************************************************************/

static inline void cl_write_parameters_set_default(cl_write_parameters *cl_w_p) {
    cl_w_p->unique = false;
    cl_w_p->unique_bin = false;
    cl_w_p->use_generation = false;
    cl_w_p->use_generation_gt = false;
    cl_w_p->use_generation_dup = false;
    cl_w_p->timeout_ms = 0;
    cl_w_p->record_ttl = 0;
    cl_w_p->w_pol = CL_WRITE_RETRY;
}

static inline void cl_write_parameters_set_generation( cl_write_parameters *cl_w_p, uint32_t generation) {
    cl_w_p->generation = generation;
    cl_w_p->use_generation = true;
}

static inline void cl_write_parameters_set_generation_gt( cl_write_parameters *cl_w_p, uint32_t generation) {
    cl_w_p->generation = generation;
    cl_w_p->use_generation_gt = true;
}

static inline void cl_write_parameters_set_generation_dup( cl_write_parameters *cl_w_p, uint32_t generation) {
    cl_w_p->generation = generation;
    cl_w_p->use_generation_dup = true;
}
