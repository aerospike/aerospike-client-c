# Build Aerospike C Client source distribution.

DEPTH = ../..

REPO=$(realpath $(DEPTH))
PKG = $(REPO)/pkg
DIST = $(PKG)/dist
SOURCE = $(DIST)/SOURCE
SOURCE_CLIENT_C = $(SOURCE)/aerospike-client-c

REV = $(shell $(DEPTH)/build/version)

# Name of the source package:
ARCHIVE = $(DEPTH)/pkg/packages/aerospike-client-c-$(REV).src.tar.bz2

all: dist package clean

.PHONY:dist
dist:
	bash git-cp-files.sh $(REPO) $(SOURCE_CLIENT_C)

.PHONY: package
package: dist
	tar cvfj $(ARCHIVE) -C $(SOURCE) aerospike-client-c

clean:
	rm -rf $(SOURCE)
