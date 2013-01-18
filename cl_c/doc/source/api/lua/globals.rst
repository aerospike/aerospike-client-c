*************
Globals
*************

Logging Functions
-----------------

All the logging functions accept a format string as the first parameter and a variable argument list.::

    trace("three %d", 3)

..  lua:function:: trace(String msg, ...)

    Log a trace message.

..  lua:function:: debug(String msg, ...)

    Log a debug message.

..  lua:function:: info(String msg, ...)

    Log an info message.

..  lua:function:: warn(String msg, ...)

    Log a warning message.
