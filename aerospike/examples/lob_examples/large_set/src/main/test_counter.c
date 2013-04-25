/*  Citrusleaf Performance Test: Counter
 *  counter.c: manage the atomic counters for testing
 *
 *  Copyright 2013 by Citrusleaf, Aerospike Inc.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */

#include "test.h"


atomic_int  *
atomic_int_create(uint64_t val) {
    atomic_int *ai = malloc(sizeof(atomic_int));
    ai->val = val;
    pthread_mutex_init(&ai->lock,0);
    return(ai);
}

void
atomic_int_destroy(atomic_int *ai) {
    pthread_mutex_destroy(&ai->lock);
    free(ai);
}


uint64_t
atomic_int_add(atomic_int *ai, int val) {
    uint32_t    rv;
    pthread_mutex_lock(&ai->lock);
    ai->val += val;
    rv = ai->val;
    pthread_mutex_unlock(&ai->lock);
    return(rv);
}

uint64_t
atomic_int_get(atomic_int *ai) {
    uint64_t    val;
    pthread_mutex_lock(&ai->lock);
    val = ai->val;
    pthread_mutex_unlock(&ai->lock);
    return(val);
}

void *
counter_fn(void *arg) {
    test_counter_thread_control *ctc = (test_counter_thread_control *) arg;

    while (ctc->death == 0) {
        sleep(1);
        fprintf(stderr, "TEST(FN): Total Keys(%"PRIu64") \n",
        cf_atomic_int_get(ctc->keys->val) );

        fprintf(stderr, ">> Read Ops(%"PRIu64") Read Vals(%"PRIu64") \n",
        cf_atomic_int_get(ctc->read_ops->val),
            cf_atomic_int_get(ctc->read_vals->val) );

        fprintf(stderr, ">> Write Ops(%"PRIu64") Write Vals(%"PRIu64") \n",
        cf_atomic_int_get(ctc->write_ops->val),
            cf_atomic_int_get(ctc->write_vals->val) );

        fprintf(stderr, ">> Delete Ops(%"PRIu64") Delete Vals(%"PRIu64") \n",
        cf_atomic_int_get(ctc->delete_ops->val),
            cf_atomic_int_get(ctc->delete_vals->val) );

        citrusleaf_print_stats();
    } // end while
    return(0);
} // end counter_fn()



// Return a pointer to the counter thread control object
void *
start_test_counter_thread(
        atomic_int *read_ops, atomic_int *read_vals,
        atomic_int *write_ops, atomic_int *write_vals,
        atomic_int *delete_ops, atomic_int *delete_vals,
        atomic_int *keys)
{
    test_counter_thread_control *ctc =
    (test_counter_thread_control *) malloc(sizeof(test_counter_thread_control));

    ctc->read_ops = read_ops;
    ctc->read_vals = read_vals;
    ctc->write_ops = write_ops;
    ctc->write_vals = write_vals;
    ctc->delete_ops = delete_ops;
    ctc->delete_vals = delete_vals;
    ctc->keys = keys;
    ctc->death = 0;
    pthread_create(&ctc->th, 0, counter_fn, ctc);
    return(ctc);
}


void
stop_test_counter_thread(void *control) {
    test_counter_thread_control *ctc = (test_counter_thread_control *)control;
    ctc->death = 1;
    pthread_join(ctc->th, 0);
    free(ctc);
}

