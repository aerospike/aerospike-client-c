/*******************************************************************************
 * Copyright 2008-2016 by Aerospike.
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

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>

#include "example_utils.h"


//==========================================================
// Constants
//

const char TEST_BIN[] = "test-bin";


//==========================================================
// Forward Declarations
//

bool read_generation(aerospike* p_as, uint16_t* p_gen);


//==========================================================
// GENERATION Example
//

int
main(int argc, char* argv[])
{
	// Parse command line arguments.
	if (! example_get_opts(argc, argv, EXAMPLE_BASIC_OPTS)) {
		exit(-1);
	}

	// Connect to the aerospike database cluster.
	aerospike as;
	example_connect_to_aerospike(&as);

	// Start clean.
	example_remove_test_record(&as);

	as_error err;

	// Create an as_record object with one (integer value) bin. By using
	// as_record_inita(), we won't need to destroy the record if we only set
	// bins using as_record_set_int64().
	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, TEST_BIN, 1001);

	// Log its contents.
	LOG("as_record object to write to database:");
	example_dump_record(&rec);

	// Write the record to the database. If the record isn't already in the
	// database, it will be created with generation = 1.
	if (aerospike_key_put(&as, &err, NULL, &g_key, &rec) != AEROSPIKE_OK) {
		LOG("aerospike_key_put() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	LOG("write succeeded");

	uint16_t gen;

	// Read the record back, and get its generation.
	if (! read_generation(&as, &gen)) {
		example_cleanup(&as);
		exit(-1);
	}

	// Update the as_record object with a different bin value. In general it's
	// ok to do this - all as_record_set_... calls destroy any previous value.
	as_record_set_int64(&rec, TEST_BIN, 1002);

	// Set its generation equal to that of the record in the database.
	rec.gen = gen;

	// Require that the next write will only succeed if generations match.
	as_policy_write wpol;
	as_policy_write_init(&wpol);
	wpol.gen = AS_POLICY_GEN_EQ;

	// Log its contents.
	LOG("as_record object to write to database:");
	example_dump_record(&rec);

	// Re-write the record in the database. The write should succeed, and
	// increment the generation.
	if (aerospike_key_put(&as, &err, &wpol, &g_key, &rec) != AEROSPIKE_OK) {
		LOG("aerospike_key_put() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	LOG("re-write requiring generation = %u succeeded", rec.gen);

	// Read the record back, and get its generation.
	if (! read_generation(&as, &gen)) {
		example_cleanup(&as);
		exit(-1);
	}

	// Update the record object with a different bin value.
	as_record_set_int64(&rec, TEST_BIN, 1003);

	// Set its generation way past that of the record in the database.
	rec.gen = gen + 10;

	// Log its contents.
	LOG("as_record object to write to database:");
	example_dump_record(&rec);

	// Try to re-write the record in the database. Use the same write policy,
	// requiring generations to match. This write should fail.
	if (aerospike_key_put(&as, &err, &wpol, &g_key, &rec) !=
			AEROSPIKE_ERR_RECORD_GENERATION) {
		LOG("aerospike_key_put() returned %d - %s, expected "
				"AEROSPIKE_ERR_RECORD_GENERATION", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	LOG("re-write requiring generation = %u failed as expected", rec.gen);

	// Now require that the next write will only succeed if the specified
	// generation is greater than that of the record in the database.
	wpol.gen = AS_POLICY_GEN_GT;

	// Log its contents.
	LOG("as_record object to write to database:");
	example_dump_record(&rec);

	// Try again. This write should succeed, and increment the generation. (Note
	// that it does not write the record with the local generation!)
	if (aerospike_key_put(&as, &err, &wpol, &g_key, &rec) != AEROSPIKE_OK) {
		LOG("aerospike_key_put() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	LOG("re-write requiring generation < %u succeeded", rec.gen);

	// Read the record back, and get its generation.
	if (! read_generation(&as, &gen)) {
		example_cleanup(&as);
		exit(-1);
	}

	// Cleanup and disconnect from the database cluster.
	example_cleanup(&as);

	LOG("generation example successfully completed");

	return 0;
}


//==========================================================
// Helpers
//

bool
read_generation(aerospike* p_as, uint16_t* p_gen)
{
	as_error err;
	as_record* p_rec = NULL;

	// Read the test record from the database.
	if (aerospike_key_get(p_as, &err, NULL, &g_key, &p_rec) != AEROSPIKE_OK) {
		LOG("aerospike_key_get() returned %d - %s", err.code, err.message);
		return false;
	}

	// If we didn't get an as_record object back, something's wrong.
	if (! p_rec) {
		LOG("aerospike_key_get() retrieved null as_record object");
		return false;
	}

	// Log the result.
	LOG("record was successfully read from database:");
	example_dump_record(p_rec);

	// Return the generation.
	*p_gen = p_rec->gen;

	// Destroy the as_record object.
	as_record_destroy(p_rec);

	return true;
}
