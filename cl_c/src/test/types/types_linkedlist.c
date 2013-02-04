
#include "../test.h"
#include <citrusleaf/as_linkedlist.h>
#include <citrusleaf/as_integer.h>

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( types_linkedlist_empty, "as_linkedlist is empty" ) {
    as_linkedlist ll;
    as_linkedlist_init(&ll,NULL,NULL);
    assert( ll.head == NULL );
    assert( ll.tail == NULL );
}

TEST( types_linkedlist_list, "as_linkedlist w/ list ops" ) {

    int rc = 0;

    as_linkedlist ll;
    as_linkedlist_init(&ll,NULL,NULL);

    assert_null( ll.head );
    assert_null( ll.tail );

    as_list l;
    as_list_init(&l, &ll, &as_linkedlist_list);

    assert_not_null( l.source );

    assert_int_eq( as_list_size(&l), 0 );

    for ( int i = 1; i < 6; i++) {
        rc = as_list_append(&l, (as_val *) as_integer_new(i));
        assert_int_eq( rc, 0 );
        assert_int_eq( as_list_size(&l), i );
    }

    for ( int i = 6; i < 11; i++) {
        rc = as_list_prepend(&l, (as_val *) as_integer_new(i));
        assert_int_eq( rc, 0 );
        assert_int_eq( as_list_size(&l), i );
    }

    as_list * t = as_list_take(&l, 5);
    assert_int_eq( as_list_size(t), 5 );

    as_integer * t_head = (as_integer *) as_list_head(t);
    as_integer * l_head = (as_integer *) as_list_head(&l);

    assert( t_head->value == l_head->value );

    as_list * d = as_list_drop(&l, 5);
    assert_int_eq( as_list_size(d), 5 );

    as_integer * d_0 = (as_integer *) as_list_get(d, 0);
    as_integer * l_5 = (as_integer *) as_list_get(&l, 5);

    assert( d_0->value == l_5->value );

    as_list_destroy(&l);

    assert_null( l.source );

    as_linkedlist_destroy(&ll);
    assert_null( ll.head );
    assert_null( ll.tail );
}

TEST( types_linkedlist_iterator, "as_linkedlist w/ iterator ops" ) {
    
    as_linkedlist ll;
    as_linkedlist_init(&ll,NULL,NULL);
    
    as_list l;
    as_list_init(&l, &ll, &as_linkedlist_list);

    for ( int i = 1; i < 6; i++) {
        as_list_append(&l, (as_val *) as_integer_new(i));
    }

    assert_int_eq( as_list_size(&l), 5 );

    as_iterator * i = NULL;
    as_integer * v = NULL;


    i  = as_list_iterator(&l);

    assert_true( as_iterator_has_next(i) );

    v = (as_integer *) as_iterator_next(i);
    assert_int_eq( as_integer_toint(v), 1 );

    v = (as_integer *) as_iterator_next(i);
    assert_int_eq( as_integer_toint(v), 2 );

    v = (as_integer *) as_iterator_next(i);
    assert_int_eq( as_integer_toint(v), 3 );

    v = (as_integer *) as_iterator_next(i);
    assert_int_eq( as_integer_toint(v), 4 );

    v = (as_integer *) as_iterator_next(i);
    assert_int_eq( as_integer_toint(v), 5 );

    assert_false( as_iterator_has_next(i) );

    as_iterator_free(i);
}

TEST( types_linkedlist_stack, "as_linkedlist via stack allocation" ) {

    as_linkedlist * tail = NULL;

    as_linkedlist a;
    tail = as_linkedlist_init(&a, (as_val *) as_integer_new(1), tail);

    as_linkedlist b;
    tail = as_linkedlist_init(&b, (as_val *) as_integer_new(2), tail);

    as_linkedlist c;
    tail = as_linkedlist_init(&c, (as_val *) as_integer_new(3), tail);

    as_list l;
    as_list_init(&l, &c, &as_linkedlist_list);

    assert_int_eq( as_list_size(&l), 3 );

    as_iterator * i = NULL;
    as_integer * v = NULL;

    i  = as_list_iterator(&l);
    
    assert_true( as_iterator_has_next(i) );

    v = (as_integer *) as_iterator_next(i);
    assert_int_eq( as_integer_toint(v), 3 );

    v = (as_integer *) as_iterator_next(i);
    assert_int_eq( as_integer_toint(v), 2 );

    v = (as_integer *) as_iterator_next(i);
    assert_int_eq( as_integer_toint(v), 1 );

    assert_false( as_iterator_has_next(i) );
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( types_linkedlist, "as_linkedlist" ) {
    suite_add( types_linkedlist_empty );
    suite_add( types_linkedlist_list );
    suite_add( types_linkedlist_iterator );
    suite_add( types_linkedlist_stack );
}