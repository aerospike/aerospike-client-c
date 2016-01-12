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
#include <aerospike/as_integer.h>
#include <aerospike/as_string.h>
#include <aerospike/as_rec.h>
#include <aerospike/as_map.h>
#include <aerospike/as_hashmap.h>
#include "map_rec.h"

/*****************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static bool         map_rec_destroy(as_rec *);
static uint32_t     map_rec_hashcode(const as_rec *);

static as_val *     map_rec_get(const as_rec *, const char *);
static int          map_rec_set(const as_rec *, const char *, const as_val *);
static int          map_rec_remove(const as_rec *, const char *);
static uint32_t     map_rec_ttl(const as_rec *);
static uint16_t     map_rec_gen(const as_rec *);

/*****************************************************************************
 * CONSTANTS
 *****************************************************************************/

const as_rec_hooks map_rec_hooks = {
    .destroy    = map_rec_destroy,
    .hashcode   = map_rec_hashcode,
    .get        = map_rec_get,
    .set        = map_rec_set,
    .remove     = map_rec_remove,
    .ttl        = map_rec_ttl,
    .gen        = map_rec_gen
};

/*****************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_rec * map_rec_new() {
    as_map * m = (as_map *) as_hashmap_new(32);
    return as_rec_new(m, &map_rec_hooks);
}

as_rec * map_rec_init(as_rec * r) {
    as_map * m = (as_map *) as_hashmap_new(32);
    return as_rec_init(r, m, &map_rec_hooks);
}

static bool map_rec_destroy(as_rec * r) {
    as_map * m = (as_map *) r->data;
    as_map_destroy(m);
    r->data = NULL;
    return 0;
}

static as_val * map_rec_get(const as_rec * r, const char * name) {
    as_map * m = (as_map *) r->data;
    as_string s;
    as_string_init(&s, (char *) name, false);
    as_val * v = as_map_get(m, (as_val *) &s);
    as_string_destroy(&s);
    return v;
}

static int map_rec_set(const as_rec * r, const char * name, const as_val * value) {
    as_map * m = (as_map *) as_rec_source(r);
    return as_map_set(m, (as_val *) as_string_new(strdup(name),true), (as_val *) value);
}

static int map_rec_remove(const as_rec * r, const char * name) {
    return 0;
}

static uint32_t map_rec_ttl(const as_rec * r) {
    return 0;
}

static uint16_t map_rec_gen(const as_rec * r) {
    return 0;
}

static uint32_t map_rec_hashcode(const as_rec * r) {
    return 0;
}
