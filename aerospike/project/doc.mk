
.PHONY: html
html: doc

.PHONY: doc
doc:
	doxygen project/doxyfile
