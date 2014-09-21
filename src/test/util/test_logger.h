/*
 * Copyright 2008-2014 Aerospike, Inc.
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

/**
 * An as_logger for tests.
 */

#include <aerospike/as_logger.h>

/*****************************************************************************
 * FUNCTIONS
 *****************************************************************************/

typedef struct test_logger_context_s {
    as_logger_level level;
} test_logger_context;

/*****************************************************************************
 * VARIABLES
 *****************************************************************************/

extern test_logger_context test_logger;

/*****************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_logger * test_logger_new();
as_logger * test_logger_init(as_logger *);
