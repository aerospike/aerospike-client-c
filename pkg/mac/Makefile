# Build Aerospike C Client Mac OS X "Darwin" Distribution.

DEPTH = ../..

PKG = $(realpath $(DEPTH)/pkg)
SOURCE_ROOT = $(PKG)/dist
BUILD_ROOT = $(SOURCE_ROOT)/BUILD
OPT_AS = $(BUILD_ROOT)/opt/aerospike

REV = $(shell $(DEPTH)/build/version | sed 's/-/_/g')
OS = $(shell $(DEPTH)/build/os_version)
ARCH = `uname -m`
TARGET = target/`uname`-$(ARCH)

LIB_PKG = aerospike-client-c-$(REV).pkg
DEVEL_PKG = aerospike-client-c-devel-$(REV).pkg

TGZ = $(PKG)/packages/aerospike-client-c-$(REV).$(OS).$(ARCH).tgz

all:	tgz

tgz:	lib devel
	tar cvfhz $(TGZ) -C $(DEPTH) README.md LICENSE -C $(PKG)/packages $(LIB_PKG) $(DEVEL_PKG) -C $(DEPTH)/target docs benchmarks examples

lib:	dist-lib1 package-lib clean1

devel:	dist-lib2 dist-devel package-devel clean2

dist-lib%:
	install -d $(BUILD_ROOT)/usr/local/lib
	install -pm 644 $(DEPTH)/$(TARGET)/lib/* $(BUILD_ROOT)/usr/local/lib

	install -d $(OPT_AS)/client
	install -d $(OPT_AS)/client/usr/udf/lua
	install -d $(OPT_AS)/client/sys/udf/lua
	install -pm 644 $(DEPTH)/modules/lua-core/src/*.lua $(OPT_AS)/client/sys/udf/lua

dist-devel:
	install -d $(BUILD_ROOT)/usr/local/include/aerospike
	install -d $(BUILD_ROOT)/usr/local/include/citrusleaf
	install -d $(BUILD_ROOT)/usr/local/include/ck
	install -d $(BUILD_ROOT)/usr/local/include/ck/gcc
	install -d $(BUILD_ROOT)/usr/local/include/ck/spinlock

	install -pm 644 $(DEPTH)/$(TARGET)/include/aerospike/* $(BUILD_ROOT)/usr/local/include/aerospike/
	install -pm 644 $(DEPTH)/$(TARGET)/include/citrusleaf/* $(BUILD_ROOT)/usr/local/include/citrusleaf/
	cp -pr $(DEPTH)/$(TARGET)/include/ck $(BUILD_ROOT)/usr/local/include/

package-lib:
	pkgbuild --identifier com.aerospike.client.c.$(REV) --root $(BUILD_ROOT) $(PKG)/packages/$(LIB_PKG)


package-devel:
	pkgbuild --identifier com.aerospike.client.c.$(REV) --root $(BUILD_ROOT) $(PKG)/packages/$(DEVEL_PKG)

clean%:
	rm -rf $(SOURCE_ROOT)/*
