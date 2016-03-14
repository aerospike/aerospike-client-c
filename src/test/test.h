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
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdarg.h>
#include <math.h>

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define ATF_PLAN_SUITE_MAX 128
#define ATF_SUITE_TEST_MAX 128

/******************************************************************************
 * atf_test
 *****************************************************************************/

typedef struct atf_test_s atf_test;
typedef struct atf_test_result_s atf_test_result;

struct atf_test_s {
    const char *    name;
    const char *    desc;
    void            (* run)(atf_test *, atf_test_result *);
};

struct atf_test_result_s {
    atf_test *      test;
    bool            success;
    char            message[1024];
};

atf_test_result * atf_test_run(atf_test * test);

atf_test_result * atf_test_result_new(atf_test * test);
void atf_test_result_destroy(atf_test_result * test_result);


#define TEST(__test_name, __test_desc) \
    static void test_spec__##__test_name(atf_test *, atf_test_result *); \
    static atf_test test__##__test_name = { \
        .name = #__test_name, \
        .desc = __test_desc, \
        .run = test_spec__##__test_name \
    }; \
    atf_test * __test_name = & test__##__test_name; \
    static void test_spec__##__test_name(atf_test * self, atf_test_result * __result__)

/******************************************************************************
 * atf_suite
 *****************************************************************************/

typedef struct atf_suite_s atf_suite;
typedef struct atf_suite_result_s atf_suite_result;

struct atf_suite_s {
    const char *    name;
    const char *    desc;
    atf_test *      tests[ATF_SUITE_TEST_MAX];
    uint32_t        size;
    void            (* init)(atf_suite *);
    bool            (* before)(atf_suite *);
    bool            (* after)(atf_suite *);
};

struct atf_suite_result_s {
    atf_suite *         suite;
    atf_test_result *   tests[ATF_SUITE_TEST_MAX];
    uint32_t            size;
    uint32_t            success;
};

atf_suite * atf_suite_add(atf_suite * suite, atf_test * test);
uint32_t atf_suite_size(atf_suite * suite);
atf_suite_result * atf_suite_run(atf_suite * suite);

atf_suite * atf_suite_after(atf_suite * suite, bool (* after)(atf_suite * suite));
atf_suite * atf_suite_before(atf_suite * suite, bool (* before)(atf_suite * suite));

atf_suite_result * atf_suite_result_new(atf_suite * suite);
void atf_suite_result_destroy(atf_suite_result * result);

atf_suite_result * atf_suite_result_add(atf_suite_result * suite_result, atf_test_result * test_result);
void atf_suite_result_print(atf_suite_result * suite_result);


#define SUITE(__suite_name, __suite_desc) \
    static void suite_spec__##__suite_name(atf_suite *); \
    static atf_suite suite__##__suite_name = { \
        .name = #__suite_name, \
        .desc = __suite_desc, \
        .tests = {NULL}, \
        .size = 0, \
        .init = suite_spec__##__suite_name, \
        .before = NULL, \
        .after = NULL \
    }; \
    atf_suite * __suite_name = & suite__##__suite_name; \
    static void suite_spec__##__suite_name(atf_suite * self)

extern char const * g_test_filter;

// Exclude all but the specified test.
void atf_test_filter(char const * test);

#define suite_add(__test)										\
    extern atf_test * __test;									\
	if (!g_test_filter || strcmp(#__test, g_test_filter) == 0)	\
		atf_suite_add(self, __test)

#define suite_before(__func) \
    atf_suite_before(self, __func)

#define suite_after(__func) \
    atf_suite_after(self, __func)


/******************************************************************************
 * atf_plan
 *****************************************************************************/

typedef struct atf_plan_s atf_plan;
typedef struct atf_plan_result_s atf_plan_result;

struct atf_plan_s {
    const char *    name;
    atf_suite *     suites[ATF_PLAN_SUITE_MAX];
    uint32_t        size;
    bool            (* before)(atf_plan *);
    bool            (* after)(atf_plan *);
};

struct atf_plan_result_s {
    atf_plan *          plan;
    atf_suite_result *  suites[ATF_PLAN_SUITE_MAX];
    uint32_t            size;
};

atf_plan * atf_plan_add(atf_plan * self, atf_suite * suite);
int atf_plan_run(atf_plan * self, atf_plan_result * result);

atf_plan * atf_plan_after(atf_plan * plan, bool (* after)(atf_plan * plan));
atf_plan * atf_plan_before(atf_plan * plan, bool (* before)(atf_plan * plan));

atf_plan_result * atf_plan_result_add(atf_plan_result * plan_result, atf_suite_result * suite_result);

atf_plan_result * atf_plan_result_new(atf_plan * plan);
void atf_plan_result_destroy(atf_plan_result * result);


#define PLAN(__plan_name)\
    static void plan_spec__##__plan_name(atf_plan * self); \
    static atf_plan plan__##__plan_name = { \
        .name = #__plan_name, \
        .suites = {NULL}, \
        .size = 0, \
        .before = NULL, \
        .after = NULL \
    }; \
    atf_plan * __plan_name = & plan__##__plan_name; \
    int main(int argc, char ** args) { \
    	g_argc = argc; \
		g_argv = args; \
        atf_plan_result * result = atf_plan_result_new(__plan_name); \
        plan_spec__##__plan_name(__plan_name); \
        int rc = atf_plan_run(__plan_name, result); \
        return rc; \
    }\
    static void plan_spec__##__plan_name(atf_plan * self) \

extern char const * g_suite_filter;

// Exclude all but the specified suite.
void atf_suite_filter(char const * suite);

#define plan_add(__suite)											\
    extern atf_suite * __suite;										\
	if (!g_suite_filter || strcmp(#__suite, g_suite_filter) == 0)	\
		atf_plan_add(self, __suite)

#define plan_before(__func) \
    atf_plan_before(self, __func)

#define plan_after(__func) \
    atf_plan_after(self, __func)

/******************************************************************************
 * atf_assert
 *****************************************************************************/

void atf_assert(atf_test_result * test_result, const char * exp, const char * file, int line);

void atf_assert_true(atf_test_result * test_result, const char * exp, const char * file, int line);
void atf_assert_false(atf_test_result * test_result, const char * exp, const char * file, int line);

void atf_assert_null(atf_test_result * test_result, const char * exp, const char * file, int line);
void atf_assert_not_null(atf_test_result * test_result, const char * exp, const char * file, int line);

void atf_assert_int_eq(atf_test_result * result, const char * actual_exp, int64_t actual, int64_t expected, const char * file, int line);
void atf_assert_int_ne(atf_test_result * result, const char * actual_exp, int64_t actual, int64_t expected, const char * file, int line);

void atf_assert_double_eq(atf_test_result * result, const char * actual_exp, double actual, double expected, const char * file, int line);

void atf_assert_string_eq(atf_test_result * result, const char * actual_exp, const char * actual, const char * expected, const char * file, int line);

void atf_assert_log(atf_test_result * result, const char * exp, const char * file, int line, const char * fmt, ...);


#define assert(EXP) \
    if ( (EXP) != true ) return atf_assert(__result__, #EXP, __FILE__, __LINE__);

#define assert_true(EXP) \
    if ( (EXP) != true ) return atf_assert_true(__result__, #EXP, __FILE__, __LINE__);

#define assert_false(EXP) \
    if ( (EXP) == true ) return atf_assert_false(__result__, #EXP, __FILE__, __LINE__);

#define assert_null(EXP) \
    if ( (EXP) != NULL ) return atf_assert_null(__result__, #EXP, __FILE__, __LINE__);

#define assert_not_null(EXP) \
    if ( (EXP) == NULL ) return atf_assert_not_null(__result__, #EXP, __FILE__, __LINE__);


#define assert_int_eq(ACTUAL, EXPECTED) \
    if ( (ACTUAL) != (EXPECTED) ) return atf_assert_int_eq(__result__, #ACTUAL, ACTUAL, EXPECTED, __FILE__, __LINE__);

#define assert_int_ne(ACTUAL, EXPECTED) \
    if ( (ACTUAL) == (EXPECTED) ) return atf_assert_int_ne(__result__, #ACTUAL, ACTUAL, EXPECTED, __FILE__, __LINE__);


#define assert_double_eq(ACTUAL, EXPECTED) \
	if ( fabs((ACTUAL) - (EXPECTED)) > 0.0000000001) return atf_assert_double_eq(__result__, #ACTUAL, ACTUAL, EXPECTED, __FILE__, __LINE__);


#define assert_string_eq(ACTUAL, EXPECTED) \
    if ( strcmp(ACTUAL, EXPECTED) != 0 ) return atf_assert_string_eq(__result__, #ACTUAL, ACTUAL, EXPECTED, __FILE__, __LINE__);


#define assert_log(EXP, fmt, args ... ) \
    if ( (EXP) == true ) return atf_assert_log(__result__, #EXP, __FILE__, __LINE__, fmt, ##args );

#define fail_async(_mon_, fmt, args ... ) \
	atf_assert_log(__result__, "", __FILE__, __LINE__, fmt, ##args);\
	as_monitor_notify(_mon_);

#define assert_success_async(_mon_, _err_, _udata_) \
	atf_test_result* __result__ = _udata_;\
	if (_err_) {\
		fail_async(_mon_, "Error %d: %s", _err_->code, _err_->message);\
		return;\
	}

#define assert_status_async(_mon_, _status_, _err_) \
	if (_status_) {\
		fail_async(_mon_, "Error %d: %s", _err_.code, _err_.message);\
		return;\
	}

#define assert_async(_mon_, EXP) \
	if (!(EXP)) {\
		atf_assert(__result__, #EXP, __FILE__, __LINE__);\
		as_monitor_notify(_mon_);\
		return;\
	}

#define assert_int_eq_async(_mon_, ACTUAL, EXPECTED) \
	if ((ACTUAL) != (EXPECTED)) {\
		atf_assert_int_eq(__result__, #ACTUAL, ACTUAL, EXPECTED, __FILE__, __LINE__);\
		as_monitor_notify(_mon_);\
		return;\
	}

#define assert_string_eq_async(_mon_, ACTUAL, EXPECTED) \
	if (strcmp(ACTUAL, EXPECTED) != 0) {\
		atf_assert_string_eq(__result__, #ACTUAL, ACTUAL, EXPECTED, __FILE__, __LINE__);\
		as_monitor_notify(_mon_);\
		return;\
	}

/******************************************************************************
 * atf_log
 *****************************************************************************/

#define ATF_LOG_PREFIX "        "

#define debug(fmt, args...) \
    atf_log_line(stderr, "DEBUG", ATF_LOG_PREFIX, __FILE__, __LINE__, fmt, ## args);

#define info(fmt, args...) \
    atf_log(stderr, "INFO", ATF_LOG_PREFIX, __FILE__, __LINE__, fmt, ## args);

#define warn(fmt, args...) \
    atf_log(stderr, "WARN", ATF_LOG_PREFIX, __FILE__, __LINE__, fmt, ## args);

#define error(fmt, args...) \
    atf_log(stderr, "ERROR", ATF_LOG_PREFIX, __FILE__, __LINE__, fmt, ## args);

void atf_log(FILE * f, const char * level, const char * prefix, const char * file, int line, const char * fmt, ...);

void atf_logv(FILE * f, const char * level, const char * prefix, const char * file, int line, const char * fmt, va_list ap);

void atf_log_line(FILE * f, const char * level, const char * prefix, const char * file, int line, const char * fmt, ...);

void atf_log_line(FILE * f, const char * level, const char * prefix, const char * file, int line, const char * fmt, ...);
