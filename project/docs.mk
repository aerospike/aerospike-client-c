
.PHONY: html
html: docs

.PHONY: docs
docs:
	TARGET_INCL=$(TARGET_INCL) doxygen project/doxyfile

.PHONY: docs-clean
docs-clean:
	rm -rf target/apidocs
