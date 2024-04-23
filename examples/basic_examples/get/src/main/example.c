/*******************************************************************************
 * Copyright 2008-2018 by Aerospike.
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


//==========================================================
// Includes
//

#include <stddef.h>
#include <stdlib.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_sleep.h>
#include <aerospike/as_status.h>
#include <aerospike/aerospike_batch.h>
#include <aerospike/as_arraylist.h>

#include "example_utils.h"


//==========================================================
// Forward Declarations
//

// bool write_record(aerospike* p_as);


//==========================================================
// GET Example
//

int
main(int argc, char* argv[])
{
	as_config config;
	as_config_init(&config);
	as_config_add_host(&config, "127.0.0.1", 3000);

	config.policies.batch_apply.key = AS_POLICY_KEY_SEND;

	aerospike as;
	aerospike_init(&as, &config);
	as_error err;

	if (aerospike_connect(&as, &err) != AEROSPIKE_OK) {
	  fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
	}

	as_batch_records recs;
	as_batch_records_init(&recs, 1);

	as_batch_apply_record *r = as_batch_apply_reserve(&recs);
	as_key_init_int64(&r->key, "test", "demo", 0);
	r->module = "sample";
	r->function = "list_append";
	as_arraylist *args = as_arraylist_new(2, 0);
	as_arraylist_set_str(args, 0, "ilist_bin");
	as_arraylist_set_int64(args, 1, 200);
	r->arglist = (as_list*)args;

	aerospike_batch_write(&as, &err, NULL, &recs);

	as_arraylist_destroy(args);
	as_batch_records_destroy(&recs);

	aerospike_close(&as, &err);
	return 0;
}
