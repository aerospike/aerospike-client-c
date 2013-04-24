************************************
Using ascli Key-Value Store Commands
************************************

``ascli`` is a command-line utility that provides the ability to execute commands against an Aerospike cluster. The 
``ascli`` utility can be found at ``/opt/citrusleaf/bin/``.  If you find yourself using ``ascli`` often, then you 
may want to add it to your ``PATH`` environment variable::

    $ export PATH=$PATH:/opt/citrusleaf/bin

Running ``acli`` with the ``--help`` option will provide you will additional usage information, including the available commands.

Put a Record
------------

To put a record in the database, use the ``put`` command::

    $ ascli put <namespace> <set> <key> <record>

The ``<object>`` argument is the record encoded as a JSON Object, whose keys are the field names and the values are field values. 

The values can be either an ``Integer``, ``String``, :type:`List`, or :type:`Map`. The :type:`List` is encoded as a JSON Array. The :type:`Map` is encoded as a JSON Object.

For example, we will create a new record with each of the types::

    $ ascli put test test test '{"a": "A", "b": 1, "c": [1,2,3], "d": {"x": 4}}'

    $ if [ $? == 0 ]; then echo "success"; else echo "failure"; fi;
    success

Get a Record
------------

To get a record from the database, use the ``get`` comamand::

    $ ascli get <namespace> <set> <key>

This will output be :type:`Record` encoded as a JSON Object, whose keys are the field names and the values are field values. 

The values will be either an ``Integer``, ``String``, :type:`List`, or :type:`Map`. The :type:`List` is encoded as a JSON Array. The :type:`Map` is encoded as a JSON Object.

For example, to get the record we created earlier::

    $ ascli get test test test
    {"a": "A", "b": 1, "c": [1,2,3], "d": {"x": 4}}

    $ if [ $? == 0 ]; then echo "success"; else echo "failure"; fi;
    success

Existence of a Record
---------------------

To test the existence of a record in the database, use the ``exists`` command::

    $ ascli exists <namespace> <set> <key>

For example, to test the record we created earlier::

    $ ascli exists test test test
    
    $ if [ $? == 0 ]; then echo "success"; else echo "failure"; fi;
    success

Remove a Record
---------------

To remove a record from the database, use the ``remove`` command::

    $ ascli remove <namespace> <set> <key>

For example, to remove the record we created earlier::

    $ ascli remove test test test
    $ if [ $? == 0 ]; then echo "success"; else echo "failure"; fi;
    success

    $ ascli exists test test test
    $ if [ $? == 0 ]; then echo "success"; else echo "failure"; fi;
    success

