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
#include "../test.h"
#include "test_aerospike.h"

/*****************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static int test_aerospike_log(const as_aerospike * as, const char * file, const int line, const int level, const char * msg);

/*****************************************************************************
 * CONSTANTS
 *****************************************************************************/

static const as_aerospike_hooks test_aerospike_hooks = {
    .destroy = NULL,
    .rec_create = NULL,
    .rec_update = NULL,
    .rec_remove = NULL,
    .rec_exists = NULL,
    .log = test_aerospike_log,
};

/*****************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

as_aerospike * test_aerospike_new() {
    return as_aerospike_new(NULL, &test_aerospike_hooks);
}

as_aerospike * test_aerospike_init(as_aerospike * a) {
    return as_aerospike_init(a, NULL, &test_aerospike_hooks);
}

static int test_aerospike_log(const as_aerospike * as, const char * file, const int line, const int level, const char * msg) {
    char l[10] = {'\0'};
    switch(level) {
        case 1:
            strncpy(l,"WARN",10);
            break;
        case 2:
            strncpy(l,"INFO",10);
            break;
        case 3:
            strncpy(l,"DEBUG",10);
            break;
        default:
            strncpy(l,"TRACE",10);
            break;
    }
    atf_log_line(stderr, l, ATF_LOG_PREFIX, file, line, msg);
    return 0;
}


