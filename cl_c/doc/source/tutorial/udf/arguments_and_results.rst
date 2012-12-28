*******************
Arguments & Results
*******************

Arguments for and results from a UDF must be one of the supported types: Integer, String, List and/or Map. When a UDF is called, its first argument is a Record, followed by additional arguments.

**In C**, the record is specified as part of the function call via the arguments for ``namespace``, ``set`` and ``key``, while the additional arguments are provided in an :type:`as_list`. The return value of a UDF is wrapped in an :type:`as_result`, which indicates if the result the UDF call was a success of failure.

A common C pattern is::

    as_result result;
    as_result_init(&result);

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

Constraints
===========

The return values of a UDF must be of one of the supported types: ``Integer``, ``String``, :type:`List` and :type:`Map`. 

A UDF in Lua, cannot return a Lua Table, nor can it have multiple return values. Instead, you will need to use either a :type:`List` or a :type:`Map`.

Valid Lua, but not allowed for a UDF::

    function multiple(r, a, b, c)
        return a, b, c
    end

    function seq(r, a, b, c)
        return {a, b, c}
    end

    function dict(r, a, b, c)
        return {a=a, b=b, c=c}
    end

Alternative::

    function multiple(r, a, b, c)
        return list {a, b, c}
    end

    function seq(r, a, b, c)
        return list {a, b, c}
    end

    function dict(r, a, b, c)
        return map {a=a, b=b, c=c}
    end

You will notice that the "alternative" above, uses Lua Tables to initialize a :type:`List` and a :type:`Map`. We do not support returning a Lua Table, is because a Table can be both a dictionary type and an index type at the same time, which makes it difficult to map to other environments that have explicit structures for the two types.

Lua Numbers represent both a Float and Integer value, but Aerospike does not support Float values, so all Lua Numbers will be converted to Integers whenever possible. 

Records
=======

The first argument to a UDF is the :type:`Record` that the function is to be applied to. 

The :type:`Record` may or may not exist at the time the UDF is called. A UDF is called on the database node, where the :type:`Record` should reside based on the digest of the record. If the :type:`Record` doesn't exist, then the UDF can choose to create the record.

A UDF function may opt to not define arguments::

    function one()
        return 1
    end

However, when a UDF requires additional arguments, the arguments should always follow a record argument::

    function echo(r, something)
        return something
    end

The additional arguments must be one of the supported types: Integer, String, :type:`List` and :type:`Map`. 

**NOTE:** A record cannot be returned from a UDF. Instead, you will need to return a :type:`Map` containing the specific keys and values your want returned from the record::

    function specific_fields(r)
        local m = map()
        m["a"] = r["a"]
        m["b"] = r["b"]
        m["c"] = r["c"]
        return m
    end


Integers
========

Integers are represented as an :type:`as_integer` in C, and as a ``Number`` in Lua. An Integer can be passed as an argument to a UDF and can be returned from a UDF.

As an example, in a file named ``my_functions.lua`` will define a function that will return the sum of two integers::

    function sum(r, a, b)
        return a + b
    end

We will upload this file then call the function::

    $ aerospike udf-put my_functions.lua
    $ aerospike udf-record-apply test test 1 my_functions sum 2 3
    5

**In C**, you can add integers to the arglist for a function::

    as_list * arglist = as_arglist_new(2);
    as_list_add_integer(arglist, 2);
    as_list_add_integer(arglist, 3);

You can also get an :type:`as_integer` from a result value::
    
    if ( result->is_success ) {
        as_integer * i = as_integer_fromval(result->value);
    }


Strings
=======

Strings are represented as an :type:`as_string` in C and as (Lua) ``String`` in Lua. A String can be passed as an argument to a UDF and can be returned from a UDF.

As an example, in a file named ``my_functions.lua`` we will define a function that will return the concatenation of two string arguments, delimited by a space::

    function concat(r, a, b)
        return a .. " " .. b
    end

We will upload this file then call the function::

    $ aerospike udf-put my_functions.lua
    $ aerospike udf-record-apply test test 1 my_functions concat "hello" "world"
    hello world

**In C**, you can add strings to the arglist for a function::

    as_list * arglist = as_arglist_new(2);
    as_list_add_string(arglist, "hello");
    as_list_add_string(arglist, "world");

You can also get an :type:`as_string` from a result value::
    
    if ( result->is_success ) {
        as_string * s = as_string_fromval(result->value);
    }

Lists
=====

Lists are represented as an :type:`as_list` in C and as a :type:`List` in Lua. A List can be passed as an argument to a UDF and can be returned from a UDF.

As an example, in a file named ``my_functions.lua`` we will define a function that will append a value to a list, then return the updated list::

    function lappend(r, l, a)
        list.append(l, a)
        return l
    end

We will upload this file then call the function::

    $ aerospike udf-put my_functions.lua
    $ aerospike udf-record-apply test test 1 my_functions lappend "[1,2,3]" 4
    [ 1, 2, 3, 4 ]

For the command-line utility, we use a JSON Array to encode lists.

**In C**, you can add :type:`as_list` to the arglist::

    as_list * l = as_arraylist_new(3);
    as_list_add_integer(l, 1);
    as_list_add_integer(l, 2);
    as_list_add_integer(l, 3);

    as_list * arglist = as_arglist_new(2);
    as_list_add_list(arglist, l);
    as_list_add_integer(arglist, 4);

You can also get an :type:`as_list` from a result value::

    if ( result->is_success ) {
        as_list * l = as_list_fromval(result->value);
    }


Maps
====

Lists are represented as an :type:`as_list` in C and as a :type:`List` in Lua. A List can be passed as an argument to a UDF and can be returned from a UDF.

As an example, in a file named ``my_functions.lua`` we will define a function that will set a new value for a given key in a map, then return the updated map::

    function mput(r, m, k, v)
        map.put(m, k, v)
        return m
    end

We will upload this file then call the function::
    
    $ aerospike udf-put my_functions.lua
    $ aerospike udf-record-apply test test 1 my_functions mput '["a":"A", "b":"B", "c":"C"]' "d" "D"
    { "a": "A", "b": "B", "c": "C", "d": "D" }

For the command-line utility, we use a JSON Object to encode a Map.

**In C**, you can add lists to the arglist::

    as_map * m = as_hashmap_new(3);
    as_map_set(m, as_string_new("a"), as_string_new("A"));
    as_map_set(m, as_string_new("b"), as_string_new("B"));
    as_map_set(m, as_string_new("c"), as_string_new("C"));

    as_list * arglist = as_arglist_new(3);
    as_list_add_map(arglist, m);
    as_list_add_string(arglist, "d");
    as_list_add_string(arglist, "D");

You can also get an :type:`as_map` as a result value::

    if ( result->is_success ) {
        as_map * s = as_map_fromval(result->value);
    }

