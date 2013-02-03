
#include "../test.h"
#include <citrusleaf/as_arraylist.h>
#include <citrusleaf/as_integer.h>

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( types_arraylist_empty, "as_arraylist is empty" ) {
    as_arraylist l;
    as_arraylist_init(&l,0,0);
    assert( l.size == 0 );
}

TEST( types_arraylist_cap10_blk0, "as_arraylist w/ capacity 10, block_size 0" ) {

    int rc = 0;

    as_arraylist a;
    as_arraylist_init(&a,10,0);

    assert_int_eq( a.capacity, 10 );
    assert_int_eq( a.block_size, 0 );
    assert_int_eq( a.size, 0 );

    as_list l;
    as_list_init(&l, &a, &as_arraylist_list);

    assert( a.size == as_list_size(&l) );
    assert_int_eq( a.size, 0 );

    for ( int i = 1; i < 6; i++) {
        rc = as_list_append(&l, (as_val *) as_integer_new(i));
        assert_int_eq( rc, AS_ARRAYLIST_OK );
        assert( a.size == as_list_size(&l) );
        assert_int_eq( a.size, i );
        assert_int_eq( a.capacity, 10);
    }

    for ( int i = 6; i < 11; i++) {
        rc = as_list_prepend(&l, (as_val *) as_integer_new(i));
        assert_int_eq( rc, AS_ARRAYLIST_OK );
        assert( a.size == as_list_size(&l) );
        assert_int_eq( a.size, i );
        assert_int_eq( a.capacity, 10);
    }

    rc = as_list_append(&l, (as_val *) as_integer_new(11));
    assert_int_ne( rc, AS_ARRAYLIST_OK );
    assert( a.size == as_list_size(&l) );
    assert_int_eq( a.size, 10);
    assert_int_eq( a.capacity, 10);

    rc = as_list_prepend(&l, (as_val *) as_integer_new(12));
    assert_int_ne( rc, AS_ARRAYLIST_OK );
    assert( a.size == as_list_size(&l));
    assert_int_eq( a.size, 10);
    assert_int_eq( a.capacity, 10);

}

TEST( types_arraylist_cap10_blk10, "as_arraylist w/ capacity 10, block_size 10" ) {

    int rc = 0;

    as_arraylist a;
    as_arraylist_init(&a,10,10);

    assert_int_eq( a.capacity, 10 );
    assert_int_eq( a.block_size, 10 );
    assert_int_eq( a.size, 0 );

    as_list l;
    as_list_init(&l, &a, &as_arraylist_list);

    assert( a.size == as_list_size(&l) );
    assert_int_eq( a.size, 0 );

    for ( int i = 1; i < 6; i++) {
        rc = as_list_append(&l, (as_val *) as_integer_new(i));
        assert_int_eq( rc, AS_ARRAYLIST_OK );
        assert( a.size == as_list_size(&l) );
        assert_int_eq( a.size, i );
        assert_int_eq( a.capacity, 10);
    }

    for ( int i = 6; i < 11; i++) {
        rc = as_list_prepend(&l, (as_val *) as_integer_new(i));
        assert_int_eq( rc, AS_ARRAYLIST_OK );
        assert( a.size == as_list_size(&l) );
        assert_int_eq( a.size, i );
        assert_int_eq( a.capacity, 10);
    }

    rc = as_list_append(&l, (as_val *) as_integer_new(11));
    assert_int_eq( rc, AS_ARRAYLIST_OK );
    assert( a.size == as_list_size(&l) );
    assert_int_eq( a.size, 11);
    assert_int_eq( a.capacity, 20);

    rc = as_list_prepend(&l, (as_val *) as_integer_new(12));
    assert_int_eq( rc, AS_ARRAYLIST_OK );
    assert( a.size == as_list_size(&l));
    assert_int_eq( a.size, 12);
    assert_int_eq( a.capacity, 20);

}

TEST( types_arraylist_list, "as_arraylist w/ list ops" ) {

    int rc = 0;

    as_arraylist a;
    as_arraylist_init(&a,10,10);

    assert_int_eq( a.capacity, 10 );
    assert_int_eq( a.block_size, 10 );
    assert_int_eq( a.size, 0 );

    as_list l;
    as_list_init(&l, &a, &as_arraylist_list);

    assert( a.size == as_list_size(&l) );
    assert_int_eq( a.size, 0 );

    for ( int i = 1; i < 6; i++) {
        rc = as_list_append(&l, (as_val *) as_integer_new(i));
        assert_int_eq( rc, AS_ARRAYLIST_OK );
        assert( a.size == as_list_size(&l) );
        assert_int_eq( a.size, i );
        assert_int_eq( a.capacity, 10);
    }

    for ( int i = 6; i < 11; i++) {
        rc = as_list_prepend(&l, (as_val *) as_integer_new(i));
        assert_int_eq( rc, AS_ARRAYLIST_OK );
        assert( a.size == as_list_size(&l) );
        assert_int_eq( a.size, i );
        assert_int_eq( a.capacity, 10);
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

}

TEST( types_arraylist_iterator, "as_linkedlist w/ iterator ops" ) {

    as_arraylist al;
    as_arraylist_init(&al,10,10);
    
    as_list l;
    as_list_init(&l, &al, &as_arraylist_list);

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
/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( types_arraylist, "as_arraylist" ) {
    suite_add( types_arraylist_empty );
    suite_add( types_arraylist_cap10_blk0 );
    suite_add( types_arraylist_cap10_blk10 );
    suite_add( types_arraylist_list );
    suite_add( types_arraylist_iterator );
}