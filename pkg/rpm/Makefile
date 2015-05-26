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

LIB_PKG = aerospike-client-c-$(REV)-1.$(OS).$(ARCH).rpm
DEVEL_PKG = aerospike-client-c-devel-$(REV)-1.$(OS).$(ARCH).rpm

TGZ = $(PKG)/packages/aerospike-client-c-$(REV).$(OS).$(ARCH).tgz

all:	tgz

tgz:	lib devel
	tar cvfhz $(TGZ) -C $(DEPTH) README.md LICENSE -C $(PKG)/packages $(LIB_PKG) $(DEVEL_PKG) -C $(DEPTH)/target docs benchmarks examples

lib:	dist-lib1 spec-lib package1 clean1

devel:	dist-lib2 dist-devel spec-devel package2 clean2

dist-lib%:
	install -d $(BUILD_ROOT)/usr/lib
	install -pm 644 $(DEPTH)/$(TARGET)/lib/* $(BUILD_ROOT)/usr/lib

	install -d $(OPT_AS)/client
	install -d $(OPT_AS)/client/usr/udf/lua
	install -d $(OPT_AS)/client/sys/udf/lua
	install -pm 644 $(DEPTH)/modules/lua-core/src/*.lua $(OPT_AS)/client/sys/udf/lua

dist-devel:
	install -d $(BUILD_ROOT)/usr/include/aerospike
	install -d $(BUILD_ROOT)/usr/include/citrusleaf
	install -d $(BUILD_ROOT)/usr/include/ck
	install -d $(BUILD_ROOT)/usr/include/ck/gcc
	install -d $(BUILD_ROOT)/usr/include/ck/spinlock

	install -pm 644 $(DEPTH)/$(TARGET)/include/aerospike/* $(BUILD_ROOT)/usr/include/aerospike/
	install -pm 644 $(DEPTH)/$(TARGET)/include/citrusleaf/* $(BUILD_ROOT)/usr/include/citrusleaf/
	cp -pr $(DEPTH)/$(TARGET)/include/ck $(BUILD_ROOT)/usr/include/

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
