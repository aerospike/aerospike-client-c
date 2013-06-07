/******************************************************************************
 * Copyright 2008-2013 by Aerospike.
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
 *****************************************************************************/

#pragma once 

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * String Equality
 */ 
struct as_predicate_string_eq_s {
	char * value;
};

typedef struct as_predicate_string_eq_s as_predicate_string_eq;

/**
 * Integer Equality
 */
struct as_predicate_integer_eq {
	int64_t value;
};

typedef struct as_predicate_integer_eq_s as_predicate_integer_eq;

/**
 * Integer Range
 */
struct as_predicate_integer_in {
	int64_t min;
	int64_t max;
};

typedef struct as_predicate_integer_in_s as_predicate_integer_in;

/**
 * Union of supported predicates
 */
union as_predicate_data_u {
	as_predicate_string_eq  string_eq;
	as_predicate_integer_eq integer_eq;
	as_predicate_integer_in integer_in;
};

typedef union as_predicate_data_u as_predicate_data;

/**
 * Predicate Identifiers
 */
enum as_predicate_type_e {
	AS_PREDICATE_UNDEF,
	AS_PREDICATE_STRING_EQ,
	AS_PREDICATE_INTEGER_EQ,
	AS_PREDICATE_INTEGER_IN
}

typedef enum as_predicate_type_e as_predicate_type;

/**
 * Predicate
 */
struct as_predicate_s {

	/**
	 * Bin to apply the predicate to
	 */
	char * bin;

	/**
	 * The predicate type, dictates which value to use from the union
	 */
	as_predicate_type type;

	/**
	 * The data for the predicate.
	 */
	as_predicate_data data;
};

typedef struct as_predicate_s as_predicate;
