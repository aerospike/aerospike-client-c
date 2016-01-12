/*
 * Copyright 2008-2016 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
#pragma once

#include <aerospike/as_bytes.h>

#define WAIT_MS(__ms) nanosleep((struct timespec[]){{0, __ms##000000}}, NULL)

/**
 * Read a file's content into the as_bytes.
 */
bool udf_readfile(const char * filename, as_bytes * content);

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
