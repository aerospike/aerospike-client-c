#include "test.h"

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define out stdout
#define LOG_MESSAGE_MAX 1024


/******************************************************************************
 * atf_test
 *****************************************************************************/

atf_test_result * atf_test_run(atf_test * test) {
    atf_test_result * result = atf_test_result_new(test);
    test->run(test, result);
    return result;
}

atf_test_result * atf_test_result_new(atf_test * test) {
    atf_test_result * res = (atf_test_result *) malloc(sizeof(atf_test_result));
    res->test = test;
    res->success = true;
    res->message[0] = '\0';
    return res;
}

void atf_test_result_free(atf_test_result * test_result) {
    free(test_result);
}

/******************************************************************************
 * atf_suite
 *****************************************************************************/

atf_suite * atf_suite_add(atf_suite * suite, atf_test * test) {
    suite->tests[suite->size] = test;
    suite->size++;
    return suite;
}

atf_suite * atf_suite_before(atf_suite * suite, bool (* before)(atf_suite * suite)) {
    suite->before = before;
    return suite;
}

atf_suite * atf_suite_after(atf_suite * suite, bool (* after)(atf_suite * suite)) {
    suite->after = after;
    return suite;
}

uint32_t atf_suite_size(atf_suite * suite) {
    return suite->size;
}

atf_suite_result * atf_suite_run(atf_suite * suite) {

    if ( suite->init ) suite->init(suite);

    printf("[+] %s: %d tests: %s\n", suite->name, suite->size, suite->desc);

    atf_suite_result * suite_result = atf_suite_result_new(suite);

    if ( suite->before ) {
        if ( suite->before(suite) == false ) {
            return suite_result;
        }
    }

    for ( int i = 0; i < suite->size; i++ ) {
        atf_test * test = suite->tests[i];
        printf("    [+] %s: (%d/%d) %s\n", suite->name, i+1, suite->size, test->desc);
        atf_test_result * test_result = atf_test_run(test);
        atf_suite_result_add(suite_result, test_result);
        if ( ! test_result->success ) {
            printf("        [✘] %s\n", test_result->message);
        }

    }

    if ( suite->after ) {
        if ( suite->after(suite) == false ) {
            return suite_result;
        }
    }

    return suite_result;
}

void atf_suite_result_print(atf_suite_result * suite_result) {
    if ( suite_result->success < suite_result->size ) {
        printf("[✘] %s: %d/%d tests passed.\n", suite_result->suite->name, suite_result->success, suite_result->size);
        for ( int i = 0; i < suite_result->size; i++ ) {
            atf_test_result * test_result = suite_result->tests[i];
            if ( ! test_result->success ) {
                printf("    [✘] %s\n", test_result->test->desc);
                printf("        %s\n", test_result->message);
            }

        }
    }
    else {
        printf("[✔] %s: %d/%d tests passed.\n", suite_result->suite->name, suite_result->success, suite_result->size);
    }
}

atf_suite_result * atf_suite_result_new(atf_suite * suite) {
    atf_suite_result * res = (atf_suite_result *) malloc(sizeof(atf_suite_result));
    res->suite = suite;
    res->size = 0;
    res->success = 0;
    return res;
}

void atf_suite_result_free(atf_suite_result * suite_result) {
    free(suite_result);
}

atf_suite_result * atf_suite_result_add(atf_suite_result * suite_result, atf_test_result * test_result) {
    suite_result->tests[suite_result->size++] = test_result;
    if ( test_result->success ) suite_result->success++;
    return suite_result;
}

/******************************************************************************
 * atf_plan
 *****************************************************************************/

atf_plan * atf_plan_add(atf_plan * plan, atf_suite * suite) {
    plan->suites[plan->size] = suite;
    plan->size++;
    return plan;
}

atf_plan_result * atf_plan_result_add(atf_plan_result * plan_result, atf_suite_result * suite_result) {
    plan_result->suites[plan_result->size++] = suite_result;
    return plan_result;
}

atf_plan * atf_plan_before(atf_plan * plan, bool (* before)(atf_plan * plan)) {
    plan->before = before;
    return plan;
}

atf_plan * atf_plan_after(atf_plan * plan, bool (* after)(atf_plan * plan)) {
    plan->after = after;
    return plan;
}


int atf_plan_run(atf_plan * plan, atf_plan_result * result) {

    printf("\n");
    printf("===============================================================================\n");
    printf("\n");

    if ( plan->before ) {
        if ( plan->before(plan) == false ) {
            return -1;
        }
    }

    for( int i = 0; i < plan->size; i++ ) {
        atf_plan_result_add( result, atf_suite_run(plan->suites[i]) );
    }

    if ( plan->after ) {
        if ( plan->after(plan) == false ) {
            return -2;
        }
    }

    printf("\n");
    printf("===============================================================================\n");
    printf("\n");

    printf("SUMMARY\n");
    printf("\n");
    
    uint32_t total = 0;
    uint32_t passed = 0;

    for( int i = 0; i < result->size; i++ ) {
        atf_suite_result_print(result->suites[i]);
        total += result->suites[i]->size;
        passed += result->suites[i]->success;
    }

    printf("\n");

    printf("%d tests: %d passed, %d failed\n", total, passed, total-passed);

    return total-passed;
}

/******************************************************************************
 * atf_assert
 *****************************************************************************/


void atf_assert(atf_test_result * result, const char * exp, const char * file, int line) {
    snprintf(result->message, LOG_MESSAGE_MAX, "assertion failed: %s [at %s:%d]", exp, file, line);
    result->success = false;
}

void atf_assert_true(atf_test_result * result, const char * exp, const char * file, int line) {
    snprintf(result->message, LOG_MESSAGE_MAX, "assertion failed: %s is not true. [at %s:%d]", exp, file, line);
    result->success = false;
}

void atf_assert_false(atf_test_result * result, const char * exp, const char * file, int line) {
    snprintf(result->message, LOG_MESSAGE_MAX, "assertion failed: %s is not false. [at %s:%d]", exp, file, line);
    result->success = false;
}

void atf_assert_null(atf_test_result * result, const char * exp, const char * file, int line) {
    snprintf(result->message, LOG_MESSAGE_MAX, "assertion failed: %s is not NULL. [at %s:%d]", exp, file, line);
    result->success = false;
}

void atf_assert_not_null(atf_test_result * result, const char * exp, const char * file, int line) {
    snprintf(result->message, LOG_MESSAGE_MAX, "assertion failed: %s is NULL. [at %s:%d]", exp, file, line);
    result->success = false;
}

void atf_assert_int_eq(atf_test_result * result, const char * actual_exp, int64_t actual, int64_t expected, const char * file, int line) {
    snprintf(result->message, LOG_MESSAGE_MAX, "assertion failed: %s == %ld, when %ld was expected. [at %s:%d]", actual_exp, actual, expected, file, line);
    result->success = false;
}

void atf_assert_int_ne(atf_test_result * result, const char * actual_exp, int64_t actual, int64_t expected, const char * file, int line) {
    snprintf(result->message, LOG_MESSAGE_MAX, "assertion failed: %s == %ld, when it shouldn't be. [at %s:%d]", actual_exp, actual, file, line);
    result->success = false;
}

void atf_assert_string_eq(atf_test_result * result, const char * actual_exp, const char * actual, const char * expected, const char * file, int line) {
    snprintf(result->message, LOG_MESSAGE_MAX, "assertion failed: %s == \"%s\", when \"%s\" was expected. [at %s:%d]", actual_exp, actual, expected, file, line);
    result->success = false;
}

void atf_assert_log(atf_test_result * result, const char * exp, const char * file, int line, const char * fmt, ...) {

    char msg[LOG_MESSAGE_MAX] = {0};
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, LOG_MESSAGE_MAX, fmt, ap);
    va_end(ap);

    snprintf(result->message, LOG_MESSAGE_MAX, "assertion failed: %s. %s [at %s:%d]", exp, msg, file, line);
    result->success = false;
}

/******************************************************************************
 * atf_log
 *****************************************************************************/

void atf_log(FILE * f, const char * level, const char * prefix, const char * file, int line, const char * fmt, ...) {
    char msg[LOG_MESSAGE_MAX] = {0};
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, LOG_MESSAGE_MAX, fmt, ap);
    va_end(ap);
    fprintf(f, "%s%s\n", prefix, msg);
}

void atf_log_line(FILE * f, const char * level, const char * prefix, const char * file, int line, const char * fmt, ...) {
    char msg[LOG_MESSAGE_MAX] = {0};
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, LOG_MESSAGE_MAX, fmt, ap);
    va_end(ap);
    fprintf(f, "%s[%s:%d] %s - %s\n", prefix, file, line, level, msg);
}
