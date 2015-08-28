# Build Aerospike C Client RPM Distribution.

DEPTH = ../..

PKG = $(realpath $(DEPTH)/pkg)
SOURCE_ROOT = $(PKG)/dist
BUILD_ROOT = $(SOURCE_ROOT)/BUILD
OPT_AS = $(BUILD_ROOT)/opt/aerospike

REV = $(shell $(DEPTH)/build/version | sed 's/-/_/g')
OS = $(shell $(DEPTH)/build/os_version)
ARCH = `uname -m`
TARGET = target/`uname`-$(ARCH)
USR_PREFIX = /usr

LIB_PKG = aerospike-client-c-$(REV)-1.$(OS).$(ARCH).rpm
DEVEL_PKG = aerospike-client-c-devel-$(REV)-1.$(OS).$(ARCH).rpm

BASE_NAME = aerospike-client-c-$(REV).$(OS).$(ARCH)
BASE_DIR = $(DEPTH)/target/$(BASE_NAME)
TGZ = $(PKG)/packages/$(BASE_NAME).tgz

all:	tgz

tgz:	lib devel
	rm -rf $(BASE_DIR)
	mkdir $(BASE_DIR)
	cd $(BASE_DIR) ; \
		ln -s $(DEPTH)/README.md ; \
		ln -s $(DEPTH)/LICENSE ; \
		ln -s $(PKG)/packages/$(LIB_PKG) ; \
		ln -s $(PKG)/packages/$(DEVEL_PKG) ; \
		ln -s $(DEPTH)/target/docs ; \
		ln -s $(DEPTH)/target/benchmarks ; \
		ln -s $(DEPTH)/target/examples
	tar cvfhz $(TGZ) -C $(DEPTH)/target $(BASE_NAME)

lib:	dist-lib1 spec-lib package1 clean1

devel:	dist-lib2 dist-devel spec-devel package2 clean2

include $(PKG)/Makefile.in

spec-lib:
	sed 's/@VERSION@/'$(REV)'/g' < $(PKG)/rpm/client-spec-base > $(PKG)/rpm/client.spec

spec-devel:
	sed 's/@VERSION@/'$(REV)'/g' < $(PKG)/rpm/client_devel-spec-base > $(PKG)/rpm/client.spec

package%:
	install -d $(SOURCE_ROOT)/RPMS/$(ARCH)
	cd $(DEPTH); rpmbuild -bb -vv --buildroot $(BUILD_ROOT) $(PKG)/rpm/client.spec
	find $(SOURCE_ROOT)/RPMS -type f -exec mv {} $(PKG)/packages \;

clean%:
	rm -rf $(PKG)/rpm/client.spec
	rm -rf $(SOURCE_ROOT)/*
