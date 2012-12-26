# Aerospike C Client Documentation

The documentation for Aerospike C Client can be generated for HTML, ePub, LaTeX and more.


## Prerequisites

The documentation is written in [reStructuredText](http://docutils.sourceforge.net/rst.html), and uses [Sphinx](http://sphinx-doc.org/) to compile the documentation. You must have [Python](http://www.python.org/) and [Sphinx](http://sphinx-doc.org/) installed in order to build the documentation.

Please visit [Sphinx](http://sphinx-doc.org/) for details on installing and using Sphinx.


## Build

Simply run `make` to see the build targets. It will generate the documentation in a `build` directory, with the a subdirectory for the target.

    $ make <target>

### HTML Documentation

To compile HTML documentation, run

    $ make html

To view compiled HTML documentation, open `build/html/index.html`

