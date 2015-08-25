
.PHONY: html
html: docs

.PHONY: docs
docs:
	TARGET_INCL=$(TARGET_INCL) doxygen project/doxyfile
	@ln -sf apidocs/html target/docs

.PHONY: docs-clean
docs-clean:
	rm -rf target/apidocs target/docs
