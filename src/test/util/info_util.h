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

#include <aerospike/as_cluster.h>
#include <citrusleaf/citrusleaf.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_info.h>

/**
 * Given a particular query string and a key, return the value of the key
 * from all nodes in the cluster. It is the caller's responsibility to free
 * the resulting value array
 */
char **get_stats(char * query, char * key, as_cluster * asc);
