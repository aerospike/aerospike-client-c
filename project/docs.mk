
.PHONY: html
html: docs

.PHONY: docs
docs:
	doxygen project/doxyfile TARGET_INCL=$TARGET_INCL

.PHONY: docs-clean
docs-clean:
	rm -rf target/apidocs