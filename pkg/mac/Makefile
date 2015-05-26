# Build Aerospike C Client Mac OS X "Darwin" Distribution.

DEPTH = ../..

PKG = $(realpath $(DEPTH)/pkg)
SOURCE_ROOT = $(PKG)/dist
BUILD_ROOT = $(SOURCE_ROOT)/BUILD
OPT_AS = $(BUILD_ROOT)/$(USR_PREFIX)/aerospike

REV = $(shell $(DEPTH)/build/version | sed 's/-/_/g')
OS = $(shell $(DEPTH)/build/os_version)
ARCH = `uname -m`
TARGET = target/`uname`-$(ARCH)
USR_PREFIX = /usr/local

LIB_PKG = aerospike-client-c-$(REV).pkg
DEVEL_PKG = aerospike-client-c-devel-$(REV).pkg

TGZ = $(PKG)/packages/aerospike-client-c-$(REV).$(OS).$(ARCH).tgz

all:	tgz

tgz:	lib devel
	tar cvfhz $(TGZ) -C $(DEPTH) README.md LICENSE -C $(PKG)/packages $(LIB_PKG) $(DEVEL_PKG) -C $(DEPTH)/target docs benchmarks examples

lib:	dist-lib1 package-lib clean1

devel:	dist-lib2 dist-devel package-devel clean2

include $(PKG)/Makefile.in

package-lib:
	pkgbuild --identifier com.aerospike.client.c.$(REV) --root $(BUILD_ROOT) $(PKG)/packages/$(LIB_PKG)


package-devel:
	pkgbuild --identifier com.aerospike.client.c.$(REV) --root $(BUILD_ROOT) $(PKG)/packages/$(DEVEL_PKG)

clean%:
	rm -rf $(SOURCE_ROOT)/*
