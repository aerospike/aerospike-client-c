/*******************************************************************************
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
 ******************************************************************************/

#pragma once


//==========================================================
// Includes
//

#include <stdbool.h>
#include <stdio.h>

#include <aerospike/aerospike.h>
#include <aerospike/as_key.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_record.h>


//==========================================================
// Example Logging Macros
//

#define LOG(_fmt, _args...) { printf(_fmt "\n", ## _args); fflush(stdout); }

#ifdef SHOW_DETAIL
#define DETAIL(_fmt, _args...) { printf(_fmt "\n", ## _args); fflush(stdout); }
#else
#define DETAIL(_fmt, _args...)
#endif


//==========================================================
// Example Test Key
//

extern as_key g_key;


//==========================================================
// Example Test Key Count
//

extern uint32_t g_n_keys;


//==========================================================
// Example Command Line Options
//

#define EXAMPLE_BASIC_OPTS "h:p:n:s:k:"
#define EXAMPLE_MULTI_KEY_OPTS "h:p:n:s:K:"

// Must be called first!
bool example_get_opts(int argc, char* argv[], const char* which_opts);


//==========================================================
// Example Command Line Options
//

void example_connect_to_aerospike(aerospike* p_as);
void example_cleanup(aerospike* p_as);
bool example_read_test_record(aerospike* p_as);
void example_remove_test_record(aerospike* p_as);
void example_dump_record(as_record* p_rec);
void example_dump_operations(as_operations* p_ops);
