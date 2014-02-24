
.PHONY: html
html: docs

.PHONY: docs
docs:
	doxygen project/doxyfile

.PHONY: docs-clean
docs-clean:
	rm -rf target/apidocs