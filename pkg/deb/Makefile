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
USR_PREFIX = /usr

LIB_PKG = aerospike-client-c-$(REV).$(OS).$(ARCH).deb
DEVEL_PKG = aerospike-client-c-devel-$(REV).$(OS).$(ARCH).deb

TGZ = $(PKG)/packages/aerospike-client-c-$(REV).$(OS).$(ARCH).tgz

all:	tgz

tgz:	lib devel
	tar cvfhz $(TGZ) -C $(DEPTH) README.md LICENSE -C $(PKG)/packages $(LIB_PKG) $(DEVEL_PKG) -C $(DEPTH)/target docs benchmarks examples

lib:	dist-deb1 dist-lib1 spec-lib package-lib clean1

devel:	dist-deb2 dist-lib2 dist-devel spec-devel package-devel clean2

include $(PKG)/Makefile.in

dist-deb%:
	install -d $(BUILD_ROOT)/DEBIAN
	install -pm 755 $(PKG)/deb/postinst.client $(BUILD_ROOT)/DEBIAN/postinst

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
