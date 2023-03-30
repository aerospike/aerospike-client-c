###############################################################################
##  SETTINGS                                                                 ##
###############################################################################
include project/settings.mk

# Modules
COMMON := modules/common
LUAMOD := modules/lua
LUAJIT := modules/luajit
MOD_LUA	:= modules/mod-lua
MODULES	:= COMMON MOD_LUA

# Use the Lua submodule?  [By default, yes.]
USE_LUAMOD = 1

# Use LuaJIT instead of Lua?  [By default, no.]
USE_LUAJIT = 0

# Permit easy overriding of the default.
ifeq ($(USE_LUAJIT),1)
  USE_LUAMOD = 0
endif

ifeq ($(and $(USE_LUAMOD:0=),$(USE_LUAJIT:0=)),1)
  $(error Only at most one of USE_LUAMOD or USE_LUAJIT may be enabled (i.e., set to 1.))
else
  ifeq ($(USE_LUAMOD),1)
    MODULES += LUAMOD
  else
    ifeq ($(USE_LUAJIT),1)
      MODULES += LUAJIT
    endif
  endif
endif

# Override optimizations via: make O=n
O = 3

# Make-local Compiler Flags
CC_FLAGS = -std=gnu99 -g -Wall -fPIC -O$(O)
CC_FLAGS += -fno-common -fno-strict-aliasing 
CC_FLAGS += -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_GNU_SOURCE $(EXT_CFLAGS)

ifeq ($(ARCH),x86_64)
  CC_FLAGS += -march=nocona
endif

ifeq ($(EVENT_LIB),libev)
  CC_FLAGS += -DAS_USE_LIBEV
endif

ifeq ($(EVENT_LIB),libuv)
  CC_FLAGS += -DAS_USE_LIBUV
endif

ifeq ($(EVENT_LIB),libevent)
  CC_FLAGS += -DAS_USE_LIBEVENT
endif

ifeq ($(OS),Darwin)
  CC_FLAGS += -D_DARWIN_UNLIMITED_SELECT -I/usr/local/include

  ifneq ($(wildcard /opt/homebrew/include),)
    # Mac new homebrew external include path
    CC_FLAGS += -I/opt/homebrew/include
  else ifneq ($(wildcard /usr/local/opt/libevent/include),)
  	# Mac old homebrew libevent include path
    CC_FLAGS += -I/usr/local/opt/libevent/include
  endif

  ifneq ($(wildcard /opt/homebrew/opt/openssl/include),)
    # Mac new homebrew openssl include path
    CC_FLAGS += -I/opt/homebrew/opt/openssl/include
  else ifneq ($(wildcard /usr/local/opt/openssl/include),)
    # Mac old homebrew openssl include path
    CC_FLAGS += -I/usr/local/opt/openssl/include
  else ifneq ($(wildcard /opt/local/include/openssl),)
    # macports openssl include path
    CC_FLAGS += -I/opt/local/include
  endif

  LUA_PLATFORM = macosx
else ifeq ($(OS),FreeBSD)
  CC_FLAGS += -finline-functions -I/usr/local/include
  LUA_PLATFORM = freebsd
else
  CC_FLAGS += -finline-functions -rdynamic
  LUA_PLATFORM = linux

  ifneq ($(wildcard /etc/alpine-release),)
    CC_FLAGS += -DAS_ALPINE
  endif
endif

# Linker flags
LD_FLAGS = $(LDFLAGS) 

ifeq ($(OS),Darwin)
  LD_FLAGS += -undefined dynamic_lookup
endif

# DEBUG Settings
ifdef DEBUG
  O = 0
  CC_FLAGS += -pg -fprofile-arcs -ftest-coverage -g2
  LD_FLAGS += -pg -fprofile-arcs -lgcov
endif

# Include Paths
INC_PATH += $(COMMON)/$(TARGET_INCL)
INC_PATH += $(MOD_LUA)/$(TARGET_INCL)

# Library Paths
# LIB_PATH +=

ifeq ($(USE_LUAMOD),1)
  INC_PATH += $(LUAMOD)/src
else
  ifeq ($(USE_LUAJIT),1)
    INC_PATH += $(LUAJIT)/src
  else
    # Find where the Lua development package is installed in the build environment.
    INC_PATH += $(or \
      $(wildcard /usr/include/lua-5.1), \
      $(wildcard /usr/include/lua5.1))
    INCLUDE_LUA_5_1 = /usr/include/lua5.1
    ifneq ($(wildcard $(INCLUDE_LUA_5_1)),)
      LUA_SUFFIX=5.1
    endif
    ifeq ($(OS),Darwin)
      ifneq ($(wildcard /usr/local/include),)
        INC_PATH += /usr/local/include
      endif
      ifneq ($(wildcard /usr/local/lib),)
        LIB_LUA = -L/usr/local/lib
      endif
    endif
    LIB_LUA += -llua$(LUA_SUFFIX)
  endif
endif

###############################################################################
##  OBJECTS                                                                  ##
###############################################################################

AEROSPIKE = 
AEROSPIKE += _bin.o
AEROSPIKE += aerospike.o
AEROSPIKE += aerospike_batch.o
AEROSPIKE += aerospike_index.o
AEROSPIKE += aerospike_info.o
AEROSPIKE += aerospike_key.o
AEROSPIKE += aerospike_query.o
AEROSPIKE += aerospike_scan.o
AEROSPIKE += aerospike_stats.o
AEROSPIKE += aerospike_udf.o
AEROSPIKE += as_address.o
AEROSPIKE += as_admin.o
AEROSPIKE += as_async.o
AEROSPIKE += as_batch.o
AEROSPIKE += as_bit_operations.o
AEROSPIKE += as_cdt_ctx.o
AEROSPIKE += as_cdt_internal.o
AEROSPIKE += as_command.o
AEROSPIKE += as_config.o
AEROSPIKE += as_cluster.o
AEROSPIKE += as_error.o
AEROSPIKE += as_event.o
AEROSPIKE += as_event_ev.o
AEROSPIKE += as_event_uv.o
AEROSPIKE += as_event_event.o
AEROSPIKE += as_event_none.o
AEROSPIKE += as_exp_operations.o
AEROSPIKE += as_exp.o
AEROSPIKE += as_hll_operations.o
AEROSPIKE += as_host.o
AEROSPIKE += as_info.o
AEROSPIKE += as_job.o
AEROSPIKE += as_key.o
AEROSPIKE += as_list_operations.o
AEROSPIKE += as_lookup.o
AEROSPIKE += as_map_operations.o
AEROSPIKE += as_node.o
AEROSPIKE += as_operations.o
AEROSPIKE += as_partition.o
AEROSPIKE += as_partition_tracker.o
AEROSPIKE += as_peers.o
AEROSPIKE += as_pipe.o
AEROSPIKE += as_policy.o
AEROSPIKE += as_proto.o
AEROSPIKE += as_query.o
AEROSPIKE += as_query_validate.o
AEROSPIKE += as_record.o
AEROSPIKE += as_record_hooks.o
AEROSPIKE += as_record_iterator.o
AEROSPIKE += as_scan.o
AEROSPIKE += as_shm_cluster.o
AEROSPIKE += as_socket.o
AEROSPIKE += as_tls.o
AEROSPIKE += as_udf.o
AEROSPIKE += version.o

OBJECTS := 
OBJECTS += $(AEROSPIKE:%=$(TARGET_OBJ)/aerospike/%)

DEPS :=
DEPS += $(COMMON)/$(TARGET_OBJ)/common/aerospike/*.o
DEPS += $(COMMON)/$(TARGET_OBJ)/common/citrusleaf/*.o
DEPS += $(MOD_LUA)/$(TARGET_OBJ)/*.o

ifeq ($(USE_LUAMOD),1)
  LUA_DYNAMIC_OBJ = $(filter-out $(LUAMOD)/src/lua.o $(LUAMOD)/src/luac.o, $(shell ls $(LUAMOD)/src/*.o))
  LUA_STATIC_OBJ  = $(LUA_DYNAMIC_OBJ)
else
  ifeq ($(USE_LUAJIT),1)
    LUA_DYNAMIC_OBJ = $(shell ls $(LUAJIT)/src/*_dyn.o)
    LUA_STATIC_OBJ  = $(filter-out $(LUA_DYNAMIC_OBJ) $(LUAJIT)/src/luajit.o, $(shell ls $(LUAJIT)/src/*.o))
  endif
endif

###############################################################################
##  HEADERS                                                                  ##
###############################################################################

COMMON-HEADERS :=
COMMON-HEADERS += $(wildcard $(COMMON)/$(SOURCE_INCL)/aerospike/*.h)
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/citrusleaf/alloc.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/citrusleaf/cf_b64.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/citrusleaf/cf_byte_order.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/citrusleaf/cf_clock.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/citrusleaf/cf_ll.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/citrusleaf/cf_queue.h

EXCLUDE-HEADERS = 

HEADERS := 
HEADERS += $(filter-out $(EXCLUDE-HEADERS), $(wildcard $(SOURCE_INCL)/aerospike/*.h))
HEADERS += $(COMMON-HEADERS)

###############################################################################
##  MAIN TARGETS                                                             ##
###############################################################################

.PHONY: all
all: modules build prepare

.PHONY: clean
clean: modules-clean docs-clean package-clean
	@rm -rf $(TARGET)

.PHONY: version
version:
	pkg/set_version $(shell pkg/version)

.PHONY: build
build:  libaerospike

.PHONY: prepare
prepare: modules-prepare $(subst $(SOURCE_INCL),$(TARGET_INCL),$(HEADERS))
	$(noop)

.PHONY: prepare-clean
prepare-clean: 
	@rm -rf $(TARGET_INCL)

.PHONY: libaerospike
libaerospike: libaerospike.a libaerospike.$(DYNAMIC_SUFFIX)

.PHONY: libaerospike.a libaerospike.$(DYNAMIC_SUFFIX)
libaerospike.a: $(TARGET_LIB)/libaerospike.a
libaerospike.$(DYNAMIC_SUFFIX): $(TARGET_LIB)/libaerospike.$(DYNAMIC_SUFFIX)

.PHONY: docs
docs:
	TARGET_INCL=$(TARGET_INCL) doxygen project/doxyfile

.PHONY: docs-clean
docs-clean:
	rm -rf $(TARGET)/docs

.PHONY: install
install: all
	pkg/install $(TARGET_BASE)

.PHONY: package
package:
	pkg/package

.PHONY: package-clean
package-clean:
	rm -rf $(TARGET)/packages/*

.PHONY: tags etags
tags etags:
	etags `find examples modules src -name "*.[ch]" | egrep -v '(target/Linux|m4)'` `find /usr/include -name "*.h"`

###############################################################################
##  BUILD TARGETS                                                            ##
###############################################################################

$(TARGET_OBJ)/%.o: $(COMMON)/$(TARGET_LIB)/libaerospike-common.a $(MOD_LUA)/$(TARGET_LIB)/libmod_lua.a $(SOURCE_MAIN)/%.c $(SOURCE_INCL)/citrusleaf/*.h | modules
	$(object)

$(TARGET_OBJ)/aerospike/%.o: $(COMMON)/$(TARGET_LIB)/libaerospike-common.a $(MOD_LUA)/$(TARGET_LIB)/libmod_lua.a $(SOURCE_MAIN)/aerospike/%.c $(SOURCE_INCL)/citrusleaf/*.h $(SOURCE_INCL)/aerospike/*.h | modules
	$(object)

$(TARGET_LIB)/libaerospike.$(DYNAMIC_SUFFIX): $(OBJECTS) | modules
	$(library) $(DEPS) $(LUA_DYNAMIC_OBJ)

$(TARGET_LIB)/libaerospike.a: $(OBJECTS) | modules
	$(archive) $(DEPS) $(LUA_STATIC_OBJ)

$(TARGET_INCL)/aerospike: | $(TARGET_INCL)
	mkdir $@

$(TARGET_INCL)/aerospike/%.h: $(SOURCE_INCL)/aerospike/%.h | $(TARGET_INCL)/aerospike
	cp -p $^ $@

###############################################################################
include project/modules.mk project/test.mk project/rules.mk

