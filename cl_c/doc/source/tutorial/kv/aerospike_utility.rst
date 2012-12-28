***************************
Using the aerospike Utility
***************************

Aerospike provides a command-line utility to assist in the managing records in the database::

    $ /opt/citrusleaf/bin/aerospike

If you run ``aerospike`` without any arguments, it will display usage information.

We recommend you add the path to ``aerospike`` to your ``PATH`` environment variable::

    $ export PATH=$PATH:/opt/citrusleaf/bin


Put a Record
------------

To put a record in the database, use the ``put`` command::

    $ aerospike put <namespace> <set> <key> <record>

The ``<object>`` argument is the record encoded as a JSON Object, whose keys are the field names and the values are field values. 

The values can be either an ``Integer``, ``String``, :type:`List`, or :type:`Map`. The :type:`List` is encoded as a JSON Array. The :type:`Map` is encoded as a JSON Object.

For example, we will create a new record with each of the types::

    $ aerospike put test test test '{"a": "A", "b": 1, "c": [1,2,3], "d": {"x": 4}}'

    $ if [ $? == 0 ]; then echo "success"; else echo "failure"; fi;
    success

Get a Record
------------

To get a record from the database, use the ``get`` comamand::

    $ aerospike get <namespace> <set> <key>

This will output be :type:`Record` encoded as a JSON Object, whose keys are the field names and the values are field values. 

The values will be either an ``Integer``, ``String``, :type:`List`, or :type:`Map`. The :type:`List` is encoded as a JSON Array. The :type:`Map` is encoded as a JSON Object.

For example, to get the record we created earlier::

    $ aerospike get test test test
    {"a": "A", "b": 1, "c": [1,2,3], "d": {"x": 4}}

    $ if [ $? == 0 ]; then echo "success"; else echo "failure"; fi;
    success

Existence of a Record
---------------------

To test the existence of a record in the database, use the ``exists`` command::

    $ aerospike exists <namespace> <set> <key>

For example, to test the record we created earlier::

    $ aerospike exists test test test
    
    $ if [ $? == 0 ]; then echo "success"; else echo "failure"; fi;
    success

Remove a Record
---------------

To remove a record from the database, use the ``remove`` command::

    $ aerospike remove <namespace> <set> <key>

For example, to remove the record we created earlier::

    $ aerospike remove test test test
    $ if [ $? == 0 ]; then echo "success"; else echo "failure"; fi;
    success

    $ aerospike exists test test test
    $ if [ $? == 0 ]; then echo "success"; else echo "failure"; fi;
    success

