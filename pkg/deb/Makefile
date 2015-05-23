# Build Aerospike C Client ".deb" Distribution.

DEPTH = ../..

PKG = $(realpath $(DEPTH)/pkg)
SOURCE_ROOT = $(PKG)/dist
BUILD_ROOT = $(SOURCE_ROOT)/BUILD
OPT_AS = $(BUILD_ROOT)/opt/aerospike

REV = $(shell $(DEPTH)/build/version)
OS = $(shell $(DEPTH)/build/os_version)
ARCH = `uname -m`
TARGET = target/`uname`-$(ARCH)

LIB_PKG = aerospike-client-c-$(REV).$(OS).$(ARCH).deb
DEVEL_PKG = aerospike-client-c-devel-$(REV).$(OS).$(ARCH).deb

TGZ = $(PKG)/packages/aerospike-client-c-$(REV).$(OS).$(ARCH).tgz

all:	tgz

tgz:	lib devel
	tar cvfhz $(TGZ) -C $(DEPTH) README.md license.txt -C $(PKG)/packages $(LIB_PKG) $(DEVEL_PKG) -C $(DEPTH)/target docs benchmarks examples

lib:	dist-lib1 spec-lib package-lib clean1

devel:	dist-lib2 dist-devel spec-devel package-devel clean2

dist-lib%:
	install -d $(BUILD_ROOT)/DEBIAN
	install -pm 755 $(PKG)/deb/postinst.client $(BUILD_ROOT)/DEBIAN/postinst

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
	sed 's/@VERSION@/'$(REV)'/g' < $(PKG)/deb/client.spec > $(BUILD_ROOT)/DEBIAN/control

spec-devel:
	sed 's/@VERSION@/'$(REV)'/g' < $(PKG)/deb/client_devel.spec > $(BUILD_ROOT)/DEBIAN/control

package-lib:
	fakeroot dpkg-deb --build $(BUILD_ROOT) $(PKG)/packages/$(LIB_PKG)

package-devel:
	fakeroot dpkg-deb --build $(BUILD_ROOT) $(PKG)/packages/$(DEVEL_PKG)

clean%:
	rm -rf $(SOURCE_ROOT)/*
