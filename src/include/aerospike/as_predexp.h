/*
 * Copyright 2016-2017 Aerospike, Inc.
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

#ifdef __cplusplus
extern "C" {
#endif

struct as_predexp_base_s;

typedef void (*as_predexp_base_dtor_fn) (struct as_predexp_base_s *);
typedef size_t (*as_predexp_base_size_fn) (struct as_predexp_base_s *);
typedef uint8_t * (*as_predexp_base_write_fn) (struct as_predexp_base_s *, uint8_t *p);
	
/**
 *	Defines a predicate expression base.
 */
typedef struct as_predexp_base_s {

	/**
	 * Destructor for this object.
	 */
	as_predexp_base_dtor_fn dtor_fn;

	/**
	 * Returns serialization size of this object.
	 */
	as_predexp_base_size_fn size_fn;

	/**
	 * Serialize this object into a command.
	 */
	as_predexp_base_write_fn write_fn;

} as_predexp_base;

/**
 *	Create a logical AND predicate expression.
 *
 *  The AND predicate expression returns true if all of it's children
 *  are true.
 *
 *  For example, the following sequence of predicate expressions
 *  selects records where the value of bin "c" is between 11 and 20
 *  inclusive:
 *
 *	~~~~~~~~~~{.c}
 *	as_query_predexp_inita(&q, 7);
 *	as_query_predexp_add(&q, as_predexp_integer_value(11));
 *	as_query_predexp_add(&q, as_predexp_integer_bin("c"));
 *  as_query_predexp_add(&q, as_predexp_integer_greatereq());
 *  as_query_predexp_add(&q, as_predexp_integer_value(20));
 *	as_query_predexp_add(&q, as_predexp_integer_bin("c"));
 *	as_query_predexp_add(&q, as_predexp_integer_lesseq());
 *	as_query_predexp_add(&q, as_predexp_and(2));
 *	~~~~~~~~~~
 *
 *  @param nexpr	The number of child expressions to AND.
 *
 *  @returns a predicate expression suitable for adding to a query or
 *  scan.
 */
as_predexp_base*
as_predexp_and(uint16_t nexpr);
	
/**
 *	Create a logical OR predicate expression.
 *
 *  The OR predicate expression returns true if any of it's children
 *  are true.
 *
 *  For example, the following sequence of predicate expressions
 *  selects records where the value of bin "pet" is "cat" or "dog".
 *
 *	~~~~~~~~~~{.c}
 *	as_query_predexp_inita(&q, 7);
 *	as_query_predexp_add(&q, as_predexp_string_value("cat"));
 *	as_query_predexp_add(&q, as_predexp_string_bin("pet"));
 *  as_query_predexp_add(&q, as_predexp_string_equal());
 *	as_query_predexp_add(&q, as_predexp_string_value("dog"));
 *	as_query_predexp_add(&q, as_predexp_string_bin("pet"));
 *  as_query_predexp_add(&q, as_predexp_string_equal());
 *	as_query_predexp_add(&q, as_predexp_or(2));
 *	~~~~~~~~~~
 *
 *  @param nexpr	The number of child expressions to OR.
 *
 *  @returns a predicate expression suitable for adding to a query or
 *  scan.
 */
as_predexp_base*
as_predexp_or(uint16_t nexpr);
	
/**
 *	Create a logical NOT predicate expression.
 *
 *  The NOT predicate expression returns true if it's child is false.
 *
 *  For example, the following sequence of predicate expressions
 *  selects records where the value of bin "pet" is not "dog".
 *
 *	~~~~~~~~~~{.c}
 *	as_query_predexp_inita(&q, 4);
 *	as_query_predexp_add(&q, as_predexp_string_value("dog"));
 *	as_query_predexp_add(&q, as_predexp_string_bin("pet"));
 *  as_query_predexp_add(&q, as_predexp_string_equal());
 *	as_query_predexp_add(&q, as_predexp_not());
 *	~~~~~~~~~~
 *
 *  @returns a predicate expression suitable for adding to a query or
 *  scan.
 */
as_predexp_base*
as_predexp_not();

/**
 *	Create a constant integer value predicate expression.
 *
 *  For example, the following sequence of predicate expressions
 *  selects records where the value of bin "c" is between 11 and 20
 *  inclusive:
 *
 *	~~~~~~~~~~{.c}
 *	as_query_predexp_inita(&q, 7);
 *	as_query_predexp_add(&q, as_predexp_integer_value(11));
 *	as_query_predexp_add(&q, as_predexp_integer_bin("c"));
 *  as_query_predexp_add(&q, as_predexp_integer_greatereq());
 *  as_query_predexp_add(&q, as_predexp_integer_value(20));
 *	as_query_predexp_add(&q, as_predexp_integer_bin("c"));
 *	as_query_predexp_add(&q, as_predexp_integer_lesseq());
 *	as_query_predexp_add(&q, as_predexp_and(2));
 *	~~~~~~~~~~
 *
 *  @param value	The integer value.
 *
 *  @returns a predicate expression suitable for adding to a query or
 *  scan.
 */
as_predexp_base*
as_predexp_integer_value(int64_t value);

/**
 *	Create a constant string value predicate expression.
 *
 *  For example, the following sequence of predicate expressions
 *  selects records where the value of bin "pet" is "cat" or "dog":
 *
 *	~~~~~~~~~~{.c}
 *	as_query_predexp_inita(&q, 7);
 *	as_query_predexp_add(&q, as_predexp_string_value("cat"));
 *	as_query_predexp_add(&q, as_predexp_string_bin("pet"));
 *  as_query_predexp_add(&q, as_predexp_string_equal());
 *	as_query_predexp_add(&q, as_predexp_string_value("dog"));
 *	as_query_predexp_add(&q, as_predexp_string_bin("pet"));
 *  as_query_predexp_add(&q, as_predexp_string_equal());
 *	as_query_predexp_add(&q, as_predexp_or(2));
 *	~~~~~~~~~~
 *
 *  @param value	The string value.
 *
 *  @returns a predicate expression suitable for adding to a query or
 *  scan.
 */
as_predexp_base*
as_predexp_string_value(char const * value);

/**
 *	Create a constant GeoJSON value predicate expression.
 *
 *  For example, the following sequence of predicate expressions
 *  selects records where a point in bin "loc" is inside the
 *  specified polygon:
 *
 *	~~~~~~~~~~{.c}
 *	char const * region =
 *		"{ "
 *		"    \"type\": \"Polygon\", "
 *		"    \"coordinates\": [ "
 *		"        [[-122.500000, 37.000000],[-121.000000, 37.000000], "
 *		"         [-121.000000, 38.080000],[-122.500000, 38.080000], "
 *		"         [-122.500000, 37.000000]] "
 *		"    ] "
 *		" } ";
 *
 *	as_query_predexp_inita(&query, 3);
 *	as_query_predexp_add(&query, as_predexp_geojson_value(region));
 *	as_query_predexp_add(&query, as_predexp_geojson_bin("loc"));
 *	as_query_predexp_add(&query, as_predexp_geojson_within());
 *	~~~~~~~~~~
 *
 *  @param value	The GeoJSON string value.
 *
 *  @returns a predicate expression suitable for adding to a query or
 *  scan.
 */
as_predexp_base*
as_predexp_geojson_value(char const * value);

/**
 *	Create an integer bin predicate expression.
 *
 *  For example, the following sequence of predicate expressions
 *  selects records where the value of bin "c" is between 11 and 20
 *  inclusive:
 *
 *	~~~~~~~~~~{.c}
 *	as_query_predexp_inita(&q, 7);
 *	as_query_predexp_add(&q, as_predexp_integer_value(11));
 *	as_query_predexp_add(&q, as_predexp_integer_bin("c"));
 *  as_query_predexp_add(&q, as_predexp_integer_greatereq());
 *  as_query_predexp_add(&q, as_predexp_integer_value(20));
 *	as_query_predexp_add(&q, as_predexp_integer_bin("c"));
 *	as_query_predexp_add(&q, as_predexp_integer_lesseq());
 *	as_query_predexp_add(&q, as_predexp_and(2));
 *	~~~~~~~~~~
 *
 *  @param binname	The name of the bin.
 *
 *  @returns a predicate expression suitable for adding to a query or
 *  scan.
 */
as_predexp_base*
as_predexp_integer_bin(char const * binname);

/**
 *	Create an string bin predicate expression.
 *
 *  For example, the following sequence of predicate expressions
 *  selects records where the value of bin "pet" is "cat" or "dog":
 *
 *	~~~~~~~~~~{.c}
 *	as_query_predexp_inita(&q, 7);
 *	as_query_predexp_add(&q, as_predexp_string_value("cat"));
 *	as_query_predexp_add(&q, as_predexp_string_bin("pet"));
 *  as_query_predexp_add(&q, as_predexp_string_equal());
 *	as_query_predexp_add(&q, as_predexp_string_value("dog"));
 *	as_query_predexp_add(&q, as_predexp_string_bin("pet"));
 *  as_query_predexp_add(&q, as_predexp_string_equal());
 *	as_query_predexp_add(&q, as_predexp_or(2));
 *	~~~~~~~~~~
 *
 *  @param binname	The name of the bin.
 *
 *  @returns a predicate expression suitable for adding to a query or
 *  scan.
 */
as_predexp_base*
as_predexp_string_bin(char const * binname);

/**
 *	Create an GeoJSON bin predicate expression.
 *
 *  For example, the following sequence of predicate expressions
 *  selects records where a point in bin "loc" is inside the
 *  specified polygon:
 *
 *	~~~~~~~~~~{.c}
 *	char const * region =
 *		"{ "
 *		"    \"type\": \"Polygon\", "
 *		"    \"coordinates\": [ "
 *		"        [[-122.500000, 37.000000],[-121.000000, 37.000000], "
 *		"         [-121.000000, 38.080000],[-122.500000, 38.080000], "
 *		"         [-122.500000, 37.000000]] "
 *		"    ] "
 *		" } ";
 *
 *	as_query_predexp_inita(&query, 3);
 *	as_query_predexp_add(&query, as_predexp_geojson_value(region));
 *	as_query_predexp_add(&query, as_predexp_geojson_bin("loc"));
 *	as_query_predexp_add(&query, as_predexp_geojson_within());
 *	~~~~~~~~~~
 *
 *  @param binname	The name of the bin.
 *
 *  @returns a predicate expression suitable for adding to a query or
 *  scan.
 */
as_predexp_base*
as_predexp_geojson_bin(char const * binname);

/**
 *	Create a list bin predicate expression.
 *
 *  For example, the following sequence of predicate expressions
 *  selects records where one of the list items is "cat":
 *
 *	~~~~~~~~~~{.c}
 *	as_query_predexp_inita(&q, 5);
 *	as_query_predexp_add(&q, as_predexp_string_value("cat"));
 *	as_query_predexp_add(&q, as_predexp_string_var("item"));
 *	as_query_predexp_add(&q, as_predexp_string_equal());
 *	as_query_predexp_add(&q, as_predexp_list_bin("pets"));
 *	as_query_predexp_add(&q, as_predexp_list_iterate_or("item"));
 *	~~~~~~~~~~
 *
 *  @param binname	The name of the bin.
 *
 *  @returns a predicate expression suitable for adding to a query or
 *  scan.
 */
as_predexp_base*
as_predexp_list_bin(char const * binname);

/**
 *	Create a map bin predicate expression.
 *
 *  For example, the following sequence of predicate expressions
 *  selects records where the map contains a key of "cat":
 *
 *	~~~~~~~~~~{.c}
 *	as_query_predexp_inita(&q, 5);
 *	as_query_predexp_add(&q, as_predexp_string_value("cat"));
 *	as_query_predexp_add(&q, as_predexp_string_var("key"));
 *	as_query_predexp_add(&q, as_predexp_string_equal());
 *	as_query_predexp_add(&q, as_predexp_map_bin("petcount"));
 *	as_query_predexp_add(&q, as_predexp_mapkey_iterate_or("key"));
 *	~~~~~~~~~~
 *
 *  @param binname	The name of the bin.
 *
 *  @returns a predicate expression suitable for adding to a query or
 *  scan.
 */
as_predexp_base*
as_predexp_map_bin(char const * binname);

/**
 *	Create an integer iteration variable predicate expression.
 *
 *  For example, the following sequence of predicate expressions
 *  selects records where the list contains a value of 42:
 *
 *	~~~~~~~~~~{.c}
 *	as_query_predexp_inita(&q, 5);
 *	as_query_predexp_add(&q, as_predexp_integer_value(42));
 *	as_query_predexp_add(&q, as_predexp_integer_var("item"));
 *	as_query_predexp_add(&q, as_predexp_integer_equal());
 *	as_query_predexp_add(&q, as_predexp_list_bin("numbers"));
 *	as_query_predexp_add(&q, as_predexp_list_iterate_or("item"));
 *	~~~~~~~~~~
 *
 *  @param varname	The name of the iteration variable.
 *
 *  @returns a predicate expression suitable for adding to a query or
 *  scan.
 */
as_predexp_base*
as_predexp_integer_var(char const * varname);

/**
 *	Create an string iteration variable predicate expression.
 *
 *  For example, the following sequence of predicate expressions
 *  selects records where one of the list items is "cat":
 *
 *	~~~~~~~~~~{.c}
 *	as_query_predexp_inita(&q, 5);
 *	as_query_predexp_add(&q, as_predexp_string_value("cat"));
 *	as_query_predexp_add(&q, as_predexp_string_var("item"));
 *	as_query_predexp_add(&q, as_predexp_string_equal());
 *	as_query_predexp_add(&q, as_predexp_list_bin("pets"));
 *	as_query_predexp_add(&q, as_predexp_list_iterate_or("item"));
 *	~~~~~~~~~~
 *
 *  @param varname	The name of the iteration variable.
 *
 *  @returns a predicate expression suitable for adding to a query or
 *  scan.
 */
as_predexp_base*
as_predexp_string_var(char const * varname);

/**
 *	Create an GeoJSON iteration variable predicate expression.
 *
 *  @param varname	The name of the iteration variable.
 *
 *  @returns a predicate expression suitable for adding to a query or
 *  scan.
 */
as_predexp_base*
as_predexp_geojson_var(char const * varname);

/**
 *	Create a record size metadata predicate expression.
 *
 *  For example, the following sequence of predicate expressions
 *  selects records that are larger than 65K:
 *  inclusive:
 *
 *	~~~~~~~~~~{.c}
 *	as_query_predexp_inita(&q, 3);
 *	as_query_predexp_add(&q, as_predexp_integer_value(65 * 1024));
 *	as_query_predexp_add(&q, as_predexp_recsize());
 *	as_query_predexp_add(&q, as_predexp_integer_greatereq());
 *	~~~~~~~~~~
 *
 *  @returns a predicate expression suitable for adding to a query or
 *  scan.
 */
as_predexp_base*
as_predexp_recsize();

/**
 *	Create a last update record metadata predicate expression.
 *
 *  For example, the following sequence of predicate expressions
 *  selects records that have been updated after an timestamp:
 *
 *	~~~~~~~~~~{.c}
 *	as_query_predexp_inita(&q, 3);
 *	as_query_predexp_add(&q, as_predexp_integer_value(g_tstampns));
 *	as_query_predexp_add(&q, as_predexp_last_update());
 *	as_query_predexp_add(&q, as_predexp_integer_greater());
 *	~~~~~~~~~~
 *
 *  @returns a predicate expression suitable for adding to a query or
 *  scan.
 */
as_predexp_base*
as_predexp_last_update();

/**
 *	Create a void time record metadata predicate expression.
 *
 *  For example, the following sequence of predicate expressions
 *  selects records that have void time set to 0 (no expiration):
 *
 *	~~~~~~~~~~{.c}
 *  as_query_predexp_inita(&q, 3);
 *	as_query_predexp_add(&q, as_predexp_integer_value(0));
 *	as_query_predexp_add(&q, as_predexp_void_time());
 *	as_query_predexp_add(&q, as_predexp_integer_equal());
 *	~~~~~~~~~~
 *
 *  @returns a predicate expression suitable for adding to a query or
 *  scan.
 */
as_predexp_base*
as_predexp_void_time();

/**
 *	Create an integer comparison predicate expression.
 *
 *  For example, the following sequence of predicate expressions
 *  selects records that have bin "foo" equal to 42:
 *
 *	~~~~~~~~~~{.c}
 *  as_query_predexp_inita(&q, 3);
 *	as_query_predexp_add(&q, as_predexp_integer_value(42));
 *	as_query_predexp_add(&q, as_predexp_integer_bin("foo"));
 *	as_query_predexp_add(&q, as_predexp_integer_equal());
 *	~~~~~~~~~~
 *
 *  @returns a predicate expression suitable for adding to a query or
 *  scan.
 */
as_predexp_base*
as_predexp_integer_equal();

as_predexp_base*
as_predexp_integer_unequal();

as_predexp_base*
as_predexp_integer_greater();

as_predexp_base*
as_predexp_integer_greatereq();

as_predexp_base*
as_predexp_integer_less();

as_predexp_base*
as_predexp_integer_lesseq();

/**
 *	Create an string comparison predicate expression.
 *
 *  For example, the following sequence of predicate expressions
 *  selects records that have bin "pet" equal to "cat":
 *
 *	~~~~~~~~~~{.c}
 *  as_query_predexp_inita(&q, 3);
 *	as_query_predexp_add(&q, as_predexp_string_value("cat"));
 *	as_query_predexp_add(&q, as_predexp_string_bin("pet"));
 *	as_query_predexp_add(&q, as_predexp_string_equal());
 *	~~~~~~~~~~
 *
 *  @returns a predicate expression suitable for adding to a query or
 *  scan.
 */
as_predexp_base*
as_predexp_string_equal();

as_predexp_base*
as_predexp_string_unequal();

/**
 *	Create an string regular expression predicate expression.
 *
 *  For example, the following sequence of predicate expressions
 *  selects records that have bin "hex" value ending in '1' or '2':
 *
 *	~~~~~~~~~~{.c}
 *	as_query_predexp_inita(&q, 3);
 *	as_query_predexp_add(&q, as_predexp_string_value("0x00.[12]"));
 *	as_query_predexp_add(&q, as_predexp_string_bin("hex"));
 *	as_query_predexp_add(&q, as_predexp_string_regex(0));
 *	~~~~~~~~~~
 *
 *  @param opts	POSIX regex cflags value.
 *
 *  @returns a predicate expression suitable for adding to a query or
 *  scan.
 */
as_predexp_base*
as_predexp_string_regex(uint32_t opts);

/**
 *	Create an GeoJSON Points-in-Region predicate expression.
 *
 *  For example, the following sequence of predicate expressions
 *  selects records where a point in bin "loc" is inside the
 *  specified polygon:
 *
 *	~~~~~~~~~~{.c}
 *	char const * region =
 *		"{ "
 *		"    \"type\": \"Polygon\", "
 *		"    \"coordinates\": [ "
 *		"        [[-122.500000, 37.000000],[-121.000000, 37.000000], "
 *		"         [-121.000000, 38.080000],[-122.500000, 38.080000], "
 *		"         [-122.500000, 37.000000]] "
 *		"    ] "
 *		" } ";
 *
 *	as_query_predexp_inita(&query, 3);
 *	as_query_predexp_add(&query, as_predexp_geojson_value(region));
 *	as_query_predexp_add(&query, as_predexp_geojson_bin("loc"));
 *	as_query_predexp_add(&query, as_predexp_geojson_within());
 *	~~~~~~~~~~
 *
 *  @returns a predicate expression suitable for adding to a query or
 *  scan.
 */
as_predexp_base*
as_predexp_geojson_within();

/**
 *	Create an GeoJSON Regions-Containing-Point predicate expression.
 *
 *  For example, the following sequence of predicate expressions
 *  selects records where a region in bin "loc" is contains the
 *  specified query point:
 *
 *	~~~~~~~~~~{.c}
 *	char const * point =
 *		"{ "
 *		"    \"type\": \"Point\", "
 *		"    \"coordinates\": [ -122.0986857, 37.4214209 ] "
 *		"} ";
 *
 *	as_query_predexp_inita(&query, 3);
 *	as_query_predexp_add(&query, as_predexp_geojson_value(point));
 *	as_query_predexp_add(&query, as_predexp_geojson_bin("loc"));
 *	as_query_predexp_add(&query, as_predexp_geojson_contains());
 *	~~~~~~~~~~
 *
 *  @returns a predicate expression suitable for adding to a query or
 *  scan.
 */
as_predexp_base*
as_predexp_geojson_contains();

/**
 *	Create an list iteration OR predicate expression.
 *
 *  For example, the following sequence of predicate expressions
 *  selects records where one of the list items is "cat":
 *
 *	~~~~~~~~~~{.c}
 *	as_query_predexp_inita(&q, 5);
 *	as_query_predexp_add(&q, as_predexp_string_value("cat"));
 *	as_query_predexp_add(&q, as_predexp_string_var("item"));
 *	as_query_predexp_add(&q, as_predexp_string_equal());
 *	as_query_predexp_add(&q, as_predexp_list_bin("pets"));
 *	as_query_predexp_add(&q, as_predexp_list_iterate_or("item"));
 *	~~~~~~~~~~
 *
 *  @param varname	The name of the list item iteration variable.
 *
 *  @returns a predicate expression suitable for adding to a query or
 *  scan.
 */
as_predexp_base*
as_predexp_list_iterate_or(char const * varname);

/**
 *	Create an list iteration AND predicate expression.
 *
 *  For example, the following sequence of predicate expressions
 *  selects records where none of the list items is "cat":
 *
 *	~~~~~~~~~~{.c}
 *	as_query_predexp_inita(&q, 6);
 *	as_query_predexp_add(&q, as_predexp_string_value("cat"));
 *	as_query_predexp_add(&q, as_predexp_string_var("item"));
 *	as_query_predexp_add(&q, as_predexp_string_equal());
 *	as_query_predexp_add(&q, as_predexp_not());
 *	as_query_predexp_add(&q, as_predexp_list_bin("pets"));
 *	as_query_predexp_add(&q, as_predexp_list_iterate_and("item"));
 *	~~~~~~~~~~
 *
 *  @param varname	The name of the list item iteration variable.
 *
 *  @returns a predicate expression suitable for adding to a query or
 *  scan.
 */
as_predexp_base*
as_predexp_list_iterate_and(char const * varname);

/**
 *	Create an map key iteration OR predicate expression.
 *
 *  For example, the following sequence of predicate expressions
 *  selects records where one of the map keys is "cat":
 *
 *	~~~~~~~~~~{.c}
 *	as_query_predexp_inita(&q, 5);
 *	as_query_predexp_add(&q, as_predexp_string_value("cat"));
 *	as_query_predexp_add(&q, as_predexp_string_var("item"));
 *	as_query_predexp_add(&q, as_predexp_string_equal());
 *	as_query_predexp_add(&q, as_predexp_map_bin("petcount"));
 *	as_query_predexp_add(&q, as_predexp_mapkey_iterate_or("item"));
 *	~~~~~~~~~~
 *
 *  @param varname	The name of the map key iteration variable.
 *
 *  @returns a predicate expression suitable for adding to a query or
 *  scan.
 */
as_predexp_base*
as_predexp_mapkey_iterate_or(char const * varname);

/**
 *	Create an map key iteration AND predicate expression.
 *
 *  For example, the following sequence of predicate expressions
 *  selects records where none of the map keys is "cat":
 *
 *	~~~~~~~~~~{.c}
 *	as_query_predexp_inita(&q, 6);
 *	as_query_predexp_add(&q, as_predexp_string_value("cat"));
 *	as_query_predexp_add(&q, as_predexp_string_var("item"));
 *	as_query_predexp_add(&q, as_predexp_string_equal());
 *	as_query_predexp_add(&q, as_predexp_not());
 *	as_query_predexp_add(&q, as_predexp_map_bin("petcount"));
 *	as_query_predexp_add(&q, as_predexp_mapkey_iterate_or("item"));
 *	~~~~~~~~~~
 *
 *  @param varname	The name of the map key iteration variable.
 *
 *  @returns a predicate expression suitable for adding to a query or
 *  scan.
 */
as_predexp_base*
as_predexp_mapkey_iterate_and(char const * varname);

/**
 *	Create an map value iteration OR predicate expression.
 *
 *  For example, the following sequence of predicate expressions
 *  selects records where one of the map values is 0:
 *
 *	~~~~~~~~~~{.c}
 *	as_query_predexp_inita(&q, 5);
 *	as_query_predexp_add(&q, as_predexp_integer_value(0));
 *	as_query_predexp_add(&q, as_predexp_integer_var("count"));
 *	as_query_predexp_add(&q, as_predexp_integer_equal());
 *	as_query_predexp_add(&q, as_predexp_map_bin("petcount"));
 *	as_query_predexp_add(&q, as_predexp_mapval_iterate_or("count"));
 *	~~~~~~~~~~
 *
 *  @param varname	The name of the map value iteration variable.
 *
 *  @returns a predicate expression suitable for adding to a query or
 *  scan.
 */
as_predexp_base*
as_predexp_mapval_iterate_or(char const * varname);

/**
 *	Create an map value iteration AND predicate expression.
 *
 *  For example, the following sequence of predicate expressions
 *  selects records where none of the map values is 0:
 *
 *	~~~~~~~~~~{.c}
 *	as_query_predexp_inita(&q, 6);
 *	as_query_predexp_add(&q, as_predexp_integer_value(0));
 *	as_query_predexp_add(&q, as_predexp_integer_var("count"));
 *	as_query_predexp_add(&q, as_predexp_integer_equal());
 *	as_query_predexp_add(&q, as_predexp_not());
 *	as_query_predexp_add(&q, as_predexp_map_bin("petcount"));
 *	as_query_predexp_add(&q, as_predexp_mapval_iterate_and("count"));
 *	~~~~~~~~~~
 *
 *  @param varname	The name of the map value iteration variable.
 *
 *  @returns a predicate expression suitable for adding to a query or
 *  scan.
 */
as_predexp_base*
as_predexp_mapval_iterate_and(char const * varname);

#ifdef __cplusplus
} // end extern "C"
#endif
