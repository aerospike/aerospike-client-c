#pragma once

#include <stdbool.h>

/**
 * Upload a UDF file to the server.
 * @param the path to the file to upload
 */
bool udf_put(const char *);

/**
 * Remove a UDF file from the server.
 * @param the name of file to remove
 */
bool udf_remove(const char *);

/**
 * Test if the UDF file exists on the server.
 * @param the name of the file to test
 */
bool udf_exists(const char *);
