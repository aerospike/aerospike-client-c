/*
 *      cl_object.h
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

#include <stdlib.h>

/******************************************************************************
 * TYPES
 ******************************************************************************/

typedef enum cl_type_e cl_type;
typedef struct cl_object_s cl_object;

enum cl_type_e { 
    CL_NULL         = 0,
    CL_INT          = 1,
    CL_FLOAT        = 2,
    CL_STR          = 3,
    CL_BLOB         = 4,
    CL_TIMESTAMP    = 5,
    CL_DIGEST       = 6,
    CL_JAVA_BLOB    = 7,
    CL_CSHARP_BLOB  = 8,
    CL_PYTHON_BLOB  = 9,
    CL_RUBY_BLOB    = 10,
    CL_PHP_BLOB     = 11,
    CL_ERLANG_BLOB  = 12,
    CL_APPEND       = 13,
    CL_RTA_LIST     = 14,
    CL_RTA_DICT     = 15,
    CL_RTA_APPEND_DICT = 16,
    CL_RTA_APPEND_LIST = 17,
    CL_LUA_BLOB     = 18,
    CL_MAP          = 19,
    CL_LIST         = 20,
    CL_UNKNOWN      = 666666
};

/**
 * An object is the value in a bin, or it is used as a key
 * The object is typed according to the citrusleaf typing system
 * These are often stack allocated, and are assigned using the 'wrap' calls
 */
struct cl_object_s {
    cl_type         type;
    size_t          sz; 
    union {
        char *      str;    // note for str: sz is strlen (not strlen+1 
        void *      blob;
        int64_t     i64;    // easiest to have one large int type
    } u;
    void *          free;   // if this is set, this must be freed on destructuion   
};

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

void citrusleaf_object_init(cl_object * o);
void citrusleaf_object_init_str(cl_object * o, char const * str);
void citrusleaf_object_init_str2(cl_object * o, char const * str, size_t str_len);
void citrusleaf_object_init_blob(cl_object * o, void const * buf, size_t buf_len);
void citrusleaf_object_init_blob2(cl_object * o, void const * buf, size_t buf_len, cl_type type);
void citrusleaf_object_init_int(cl_object * o, int64_t i);
void citrusleaf_object_init_null(cl_object * o);
void citrusleaf_object_free(cl_object * o);
int citrusleaf_copy_object(cl_object * destobj, cl_object * srcobj);
