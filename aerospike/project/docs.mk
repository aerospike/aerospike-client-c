
.PHONY: html
html: docs

.PHONY: docs
docs:
	doxygen project/doxyfile
