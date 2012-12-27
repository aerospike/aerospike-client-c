********************************
Using the Command-Line Utilities
********************************

Aerospike provides command-line utilities to assist in the managing of UDFs and to call UDFs from command-line.

Before we begin, you should create a text file named ``my_functions.lua``, containing::

    function hello()
        return "Hello World!"
    end

Upload a UDF file
-----------------

To upload a UDF file, use the ``udf-put`` command::

    $ udf-put <file>

You can specify the path to the file. The file itself will be uploaded, but the directories will not be created.

For example, to upload ``my_functions.lua``::

    $ udf-put my_functions.lua

List UDF files
--------------

To list UDF files, use the ``udf-list`` command::

    $ udf-list
    my_functions.lua

Get a UDF File
--------------

To get the contents of a UDF file, use the ``udf-get`` comamand::

    $ udf-get <filename>

This will output the contents of the file.

For example, to get ``my_functions.lua``::
    
    $ udf-get my_functions.lua
    function hello()
        return "Hello World!"
    end

Call a UDF
----------

To call a function in a UDF file, use the ``udf-record-apply`` command::

    $ udf-record-apply <namespace> <set> <key> <filename> <function> [args ...]

For example, to call the ``hello()`` function in ``my_functions.lua``::

    $ udf-record-apply test test 1 my_functions hello
    "Hello World!"

You will notice that ``udf-record-apply`` does not require a file extension for the ``<filename>`` argument. The reason is we support
calling UDF defined in `.lua` files or `.so` files that are developed as Lua modules, so we will perform a filepath resolution by looking 
for a ``.lua`` file first then a ``.so`` file.


Remove a UDF File
-------------------

To remove a UDF file, use the ``udf-remove`` command::

    $ udf-remove <filename>

For example, to remove ``my_functions.lua``::

    $ udf-remove my_functions.lua
