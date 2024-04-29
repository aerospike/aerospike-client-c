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
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_sleep.h>
#include <aerospike/as_status.h>
#include <aerospike/aerospike_info.h>

#include "example_utils.h"


//==========================================================
// Forward Declarations
//

bool callback(const as_error *err, const as_node *node, const char *req, char *res, void *udata)
{
	printf("Request: %s\n", req);
	if (res) {
		printf("Response: %s\n", res);
	} else {
		printf("Response is NULL\n");
	}

	return true;
}

//==========================================================
// GET Example
//

int
main(int argc, char* argv[])
{
	as_config config;
	as_config_init(&config);
	as_config_add_host(&config, "127.0.0.1", 3000);

	aerospike as;
	aerospike_init(&as, &config);

	as_error err;
	as_error_init(&err);

	if (aerospike_connect(&as, &err) != AEROSPIKE_OK) {
		goto CLEANUP1;
	}

	as_status status = aerospike_info_foreach(&as, &err, NULL, "fake_request_string_not_real", callback, NULL);
	printf("Status: %d\n", status);

	aerospike_close(&as, &err);

CLEANUP1:
	if (err.code != AEROSPIKE_OK) {
		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
	}
	return err.code;
}


//==========================================================
// Helpers
//
