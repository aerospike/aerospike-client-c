###############################################################################
##  SETTINGS                                                                 ##
###############################################################################
include project/settings.mk

# Modules
COMMON := $(abspath modules/common)
LUAMOD := $(abspath modules/lua)
MOD_LUA	:= $(abspath modules/mod-lua)
MODULES	:= COMMON
MODULES += MOD_LUA

# Override optimizations via: make O=n
O = 3

# Make-local Compiler Flags
EXT_CFLAGS =
CC_FLAGS = -std=gnu99 -g -Wall -fPIC -O$(O)
CC_FLAGS += -fno-common -fno-strict-aliasing 
CC_FLAGS += -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_GNU_SOURCE $(EXT_CFLAGS)

ifeq ($(ARCH),x86_64)
  REAL_ARCH = -march=nocona
endif

ifeq ($(ARCH),aarch64)
  REAL_ARCH = -mcpu=neoverse-n1
endif

CC_CFLAGS += $(REAL_ARCH)
EVENT_LIB =

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
  LUA_PLATFORM = LUA_USE_MACOSX

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
else ifeq ($(OS),FreeBSD)
  CC_FLAGS += -finline-functions -I/usr/local/include
  LUA_PLATFORM = LUA_USE_LINUX # nothing BSD specific in luaconf.h
else
  CC_FLAGS += -finline-functions -rdynamic
  LUA_PLATFORM = LUA_USE_LINUX

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
INC_PATH += $(COMMON)/$(SOURCE_INCL)
INC_PATH += $(MOD_LUA)/$(SOURCE_INCL)
INC_PATH += $(LUAMOD)

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
AEROSPIKE += aerospike_txn.o
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
AEROSPIKE += as_latency.o
AEROSPIKE += as_list_operations.o
AEROSPIKE += as_lookup.o
AEROSPIKE += as_map_operations.o
AEROSPIKE += as_metrics.o
AEROSPIKE += as_metrics_writer.o
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
AEROSPIKE += as_txn.o
AEROSPIKE += as_txn_monitor.o
AEROSPIKE += as_udf.o
AEROSPIKE += version.o

OBJECTS := 
OBJECTS += $(AEROSPIKE:%=$(TARGET_OBJ)/aerospike/%)

DEPS =
DEPS += $(COMMON)/$(TARGET_OBJ)/common/aerospike/*.o
DEPS += $(COMMON)/$(TARGET_OBJ)/common/citrusleaf/*.o
DEPS += $(MOD_LUA)/$(TARGET_OBJ)/*.o

EXP_DEPS := $(foreach DD, $(DEPS), $(wildcard $(DEP)))

LUA_OBJECTS = $(filter-out $(LUAMOD)/lua.o, $(shell ls $(LUAMOD)/*.o))

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

AEROSPIKE-HEADERS := $(filter-out $(EXCLUDE-HEADERS), $(wildcard $(SOURCE_INCL)/aerospike/*.h))

HEADERS := $(AEROSPIKE-HEADERS)
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
prepare: modules-prepare $(subst $(SOURCE_INCL),$(TARGET_INCL),$(AEROSPIKE-HEADERS))

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

$(TARGET_OBJ)/aerospike/%.o: $(SOURCE_MAIN)/aerospike/%.c
	$(object)

$(TARGET_LIB)/libaerospike.$(DYNAMIC_SUFFIX): $(OBJECTS) $(EXP_DEPS) | modules
	$(library) $(DEPS) $(LUA_OBJECTS)

$(TARGET_LIB)/libaerospike.a: $(OBJECTS) $(EXP_DEPS) | modules
	$(archive) $(DEPS) $(LUA_OBJECTS)

$(TARGET_INCL)/aerospike/%.h: $(SOURCE_INCL)/aerospike/%.h
	@mkdir -p $(@D)
	cp -p $< $@

###############################################################################
include project/modules.mk project/test.mk project/rules.mk

