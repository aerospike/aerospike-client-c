#pragma once

#include <citrusleaf/as_types.h>

/**
 * Upload a UDF file to the server.
 * @param the path to the file to upload
 */
int udf_put(const char *);

/**
 * Remove a UDF file from the server.
 * @param the name of file to remove
 */
int udf_remove(const char *);

/**
 * Test if the UDF file exists on the server.
 * @param the name of the file to test
 */
int udf_exists(const char *);

/**
 * Apply a UDF to a record
 */
int udf_apply_record(const char *, const char *, const char *, const char *, const char *, as_list *, as_result *);

void print_result(uint32_t rc, as_result * r);