*******************
Arguments & Results
*******************

Records
=======

The first argument to every user-defined function is a record that the function is to be applied to. 
This record may or may not exist in the cluster. The UDF will always be invoked on the database node that should contain 
the record, because the UDF call will be sent to the dataabase node that is supposed to store the record based on the 
record's digest. 

A user-defined function can choose to perform actions on a record such as check the existence of the record, create 
a (new) record, update the (existing) record or remove the (existing) record. Also, it can choose to ignore the record
argument.

When ever a UDF function is defined in Lua, the first argument will alway be sent to the function, even if it isn't used. 
So the following function::

    function do_something()
        return 1
    end

Is actually called as::

    do_something(record)

eventhough ``record`` was not specified for the function.

In addition to the record argument, a UDF may specify additional arguments which are of the supported types: Integer,
String, List and Map. Any additional arguments should be provided after the record argument, so in this case the record argument is
required::

    function echo(r, value)
        return value
    end

The return values from UDFs must also be of one of the supported types. In Lua, you cannot return a Lua Table, but 
you can return either a :type:`List` or a :type:`Map`. 

In the C API, the record is specified as part of the C API call, while the additional arguments are provided in an 
:type:`as_list`. The return value of a UDF is wrapped in an :type:`as_result`, which indicates whether the call was 
successful. 

A common C pattern is::

    as_result result;
    as_result_init(&result);
    ...

    as_list * arglist = as_arglist_new(2);
    ...

    int rc = citrusleaf_udf_record_apply(cluster, ns, set, &key, file, func, arglist, 1000, &result);

    if ( result->is_success ) {
        ...
        printf("%s", as_val_tostring(result->value));
    }
    else {
        ...
        printf("error: %s", as_val_tostring(result->value));
    }


Integers
========

Add the following function to a ``my_functions.lua``::

    function add(r, a, b)
        return a + b
    end

In addition to the record argument, the function accepts two arguments and returns the sum of the two arguments. The addition (+) operation is only valid for numeric types
in Lua, and so the return value is the sum of two integers.

Upload the file::
    
    $ udf-put my_functions.lua

Then call the function via command-line::

    $ udf-record-apply test test 1 my_functions add 2 3
    5

In C, you can add integers to the arglist::
    
    as_list * arglist = as_arglist(2);
    as_list_add_integer(arglist, 2);
    as_list_add_integer(arglist, 3);

To get an integer result::

    if ( result->is_success ) {
        as_integer * s = as_integer_fromval(result->value);
    }

Strings
=======

Add the following function to a ``my_functions.lua``::

    function concat(r, a, b)
        return a .. " " .. b
    end

The ``..`` is a Lua operation for concatenating strings. In the function, we concatenate the strings with a space delimiter.

Upload the file::
    
    $ udf-put my_functions.lua

Then call the function via command-line::

    $ udf-record-apply test test 1 my_functions concat "hello" "world"
    hello world

In C, you can add strings to the arglist::

    as_list * arglist = as_arglist_new(2);
    as_list_add_string(arglist, "hello");
    as_list_add_string(arglist, "world");

Then you can get a string result value::
    
    if ( result->is_success ) {
        as_string * s = as_string_fromval(result->value);
    }

Lists
=====

Add the following function to a ``my_functions.lua``::

    function lappend(r, l, a)
        list.append(l, a)
        return l
    end

In the function, ``list`` is a library functions for the :type:`List` type in Lua. This function append a value ``a`` to the :type:`List` ``l``.

Upload the file::
    
    $ udf-put my_functions.lua

Then call the function via command-line::

    $ udf-record-apply test test 1 my_functions lappend "[1,2,3]" 4
    [ 1, 2, 3, 4 ]

For the command-line utility, we use a JSON Array to encode lists.

In C, you can add lists to the arglist::

    as_list * l = as_arraylist_new(3);
    as_list_add_integer(l, 1);
    as_list_add_integer(l, 2);
    as_list_add_integer(l, 3);

    as_list * arglist = as_arglist_new(2);
    as_list_add_list(arglist, l);
    as_list_add_integer(arglist, 4);

Then you can get a List result value::

    if ( result->is_success ) {
        as_list * s = as_list_fromval(result->value);
    }


Maps
====

Add the following function to a ``my_functions.lua``::

    function mput(r, l, k, v)
        map.put(m, k, v)
        return m
    end

In the function, ``map`` is a library of functions for the :type:`Map` type in Lua. This function take a :type:`Map` ``m`` and sets a key ``k`` with the value ``v``.

Upload the file::
    
    $ udf-put my_functions.lua

Then call the function via command-line::

    $ udf-record-apply test test 1 my_functions mput '["a":"A", "b":"B", "c":"C"]' "d" "D"
    { "a": "A", "b": "B", "c": "C", "d": "D" }

For the command-line utility, we use a JSON Object to encode a Map.

In C, you can add lists to the arglist::

    as_map * m = as_hashmap_new(3);
    as_map_set(m, as_string_new("a"), as_string_new("A"));
    as_map_set(m, as_string_new("b"), as_string_new("B"));
    as_map_set(m, as_string_new("c"), as_string_new("C"));

    as_list * arglist = as_arglist_new(3);
    as_list_add_map(arglist, m);
    as_list_add_string(arglist, "d");
    as_list_add_string(arglist, "D");

Then you can get a Map result value::

    if ( result->is_success ) {
        as_map * s = as_map_fromval(result->value);
    }

