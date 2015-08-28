include project/settings.mk
###############################################################################
##  SETTINGS                                                                 ##
###############################################################################

# Modules
COMMON		:= $(realpath modules/common)
LUA_CORE	:= $(realpath modules/lua-core)
LUAMOD		:= $(realpath modules/lua)
LUAJIT		:= $(realpath modules/luajit)
MOD_LUA		:= $(realpath modules/mod-lua)
MODULES		:= COMMON MOD_LUA

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

# Use SystemTap?  [By default, no.]
USE_SYSTEMTAP = 0

# Make-local Compiler Flags
CC_FLAGS = -std=gnu99 -g -Wall -fPIC -O$(O)
CC_FLAGS += -fno-common -fno-strict-aliasing -finline-functions
CC_FLAGS += -march=nocona -DMARCH_$(ARCH)
CC_FLAGS += -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_GNU_SOURCE $(EXT_CFLAGS)

ifeq ($(OS),Darwin)
  CC_FLAGS += -D_DARWIN_UNLIMITED_SELECT
  LUA_PLATFORM = macosx
else
  CC_FLAGS += -rdynamic
  LUA_PLATFORM = linux
endif

ifneq ($(CF),)
  CC_FLAGS += -I$(CF)/include
endif

ifeq ($(USE_SYSTEMTAP),1)
CC_FLAGS +=	-DUSE_SYSTEMTAP
endif

# Linker flags
LD_FLAGS = $(LDFLAGS) -lm -fPIC

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

ifeq ($(USE_SYSTEMTAP),1)
## SYSTEMTAP_PROBES_H = $(COMMON)/$(SOURCE_INCL)/aerospike/probes.h
SYSTEMTAP_PROBES_H = $(SOURCE_MAIN)/aerospike/probes.h
SYSTEMTAP_PROBES_D = $(SOURCE_MAIN)/aerospike/probes.d
SYSTEMTAP_PROBES_O = $(TARGET_OBJ)/probes.o
## SYSTEMTAP_PROBES_O = $(COMMON)/$(TARGET_OBJ)/common/aerospike/probes.o
endif

###############################################################################
##  OBJECTS                                                                  ##
###############################################################################

AEROSPIKE = 
AEROSPIKE += _bin.o
AEROSPIKE += _ldt.o
AEROSPIKE += aerospike.o
AEROSPIKE += aerospike_batch.o
AEROSPIKE += aerospike_index.o
AEROSPIKE += aerospike_info.o
AEROSPIKE += aerospike_llist.o
AEROSPIKE += aerospike_lmap.o
AEROSPIKE += aerospike_lset.o
AEROSPIKE += aerospike_lstack.o
AEROSPIKE += aerospike_key.o
AEROSPIKE += aerospike_query.o
AEROSPIKE += aerospike_scan.o
AEROSPIKE += aerospike_udf.o
AEROSPIKE += as_admin.o
AEROSPIKE += as_batch.o
AEROSPIKE += as_command.o
AEROSPIKE += as_config.o
AEROSPIKE += as_cluster.o
AEROSPIKE += as_error.o
AEROSPIKE += as_info.o
AEROSPIKE += as_job.o
AEROSPIKE += as_key.o
AEROSPIKE += as_lookup.o
AEROSPIKE += as_node.o
AEROSPIKE += as_operations.o
AEROSPIKE += as_partition.o
AEROSPIKE += as_policy.o
AEROSPIKE += as_proto.o
AEROSPIKE += as_query.o
AEROSPIKE += as_record.o
AEROSPIKE += as_record_hooks.o
AEROSPIKE += as_record_iterator.o
AEROSPIKE += as_scan.o
AEROSPIKE += as_shm_cluster.o
AEROSPIKE += as_socket.o
AEROSPIKE += as_udf.o
AEROSPIKE += as_ldt.o

OBJECTS := 
OBJECTS += $(AEROSPIKE:%=$(TARGET_OBJ)/aerospike/%)

DEPS :=
DEPS += $(COMMON)/$(TARGET_OBJ)/common/aerospike/*.o
DEPS += $(COMMON)/$(TARGET_OBJ)/common/citrusleaf/*.o
DEPS += $(MOD_LUA)/$(TARGET_OBJ)/*.o

ifeq ($(USE_LUAMOD),1)
  LUA_DYNAMIC_OBJ = $(filter-out  $(LUAMOD)/src/lua.o $(LUAMOD)/src/luac.o, $(wildcard $(LUAMOD)/src/*.o))
  LUA_STATIC_OBJ  = $(LUA_DYNAMIC_OBJ)
else
  ifeq ($(USE_LUAJIT),1)
    LUA_DYNAMIC_OBJ = $(wildcard $(LUAJIT)/src/*_dyn.o)
    LUA_STATIC_OBJ  = $(filter-out $(LUA_DYNAMIC_OBJ) $(LUAJIT)/src/luajit.o, $(wildcard $(LUAJIT)/src/*.o))
  endif
endif

###############################################################################
##  HEADERS                                                                  ##
###############################################################################

COMMON-HEADERS :=
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_arraylist.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_arraylist_iterator.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_boolean.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_bytes.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_double.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_hashmap.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_hashmap_iterator.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_integer.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_iterator.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_list.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_list_iterator.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_log.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_map.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_map_iterator.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_nil.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_pair.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_password.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_rec.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_stream.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_string.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_stringmap.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_util.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_val.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_vector.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/citrusleaf/alloc.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/citrusleaf/cf_arch.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/citrusleaf/cf_atomic.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/citrusleaf/cf_b64.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/citrusleaf/cf_clock.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/citrusleaf/cf_queue.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/citrusleaf/cf_types.h
COMMON-HEADERS += $(shell find $(COMMON)/$(SOURCE_INCL)/aerospike/ck -type f)

EXCLUDE-HEADERS = 

HEADERS := 
HEADERS += $(filter-out $(EXCLUDE-HEADERS), $(wildcard $(SOURCE_INCL)/aerospike/*.h))
HEADERS += $(COMMON-HEADERS)

###############################################################################
##  MAIN TARGETS                                                             ##
###############################################################################

all: modules build prepare

.PHONY: prepare
prepare: modules-prepare $(subst $(SOURCE_INCL),$(TARGET_INCL),$(HEADERS))
	$(noop)

.PHONY: prepare-clean
prepare-clean: 
	@rm -rf $(TARGET_INCL)

.PHONY: build
build:  libaerospike

.PHONY: cleanall
cleanall: build-clean

.PHONY: build-clean
build-clean:
	@rm -rf $(TARGET) $(SYSTEMTAP_PROBES_H)

.PHONY: libaerospike
libaerospike: $(SYSTEMTAP_PROBES_H) libaerospike.a libaerospike.$(DYNAMIC_SUFFIX)

.PHONY: libaerospike.a libaerospike.$(DYNAMIC_SUFFIX)
libaerospike.a: $(TARGET_LIB)/libaerospike.a
libaerospike.$(DYNAMIC_SUFFIX): $(TARGET_LIB)/libaerospike.$(DYNAMIC_SUFFIX)

.PHONY: examples
examples:
	@rm -rf $(TARGET_EXAMPLES)
	@mkdir $(TARGET_EXAMPLES)
	./build/prep_examples $(TARGET_EXAMPLES)

.PHONY: rpm deb mac src
rpm deb mac src:
	$(MAKE) -C pkg/$@

.PHONY: package
package: all docs examples
	rm -rf pkg/packages/*
	$(MAKE) $(PKG)

.PHONY: source
source: src

install:
	cp -p $(TARGET_LIB)/libaerospike.* /usr/local/lib/

tags etags:
	etags `find benchmarks demos examples modules src -name "*.[ch]" | egrep -v '(target/Linux|m4)'` `find /usr/include -name "*.h"`

###############################################################################
##  BUILD TARGETS                                                            ##
###############################################################################

$(TARGET_OBJ)/%.o: $(COMMON)/$(TARGET_LIB)/libaerospike-common.a $(MOD_LUA)/$(TARGET_LIB)/libmod_lua.a $(SOURCE_MAIN)/%.c $(SOURCE_INCL)/citrusleaf/*.h | modules
	$(object)

$(TARGET_OBJ)/aerospike/%.o: $(COMMON)/$(TARGET_LIB)/libaerospike-common.a $(MOD_LUA)/$(TARGET_LIB)/libmod_lua.a $(SOURCE_MAIN)/aerospike/%.c $(SOURCE_INCL)/citrusleaf/*.h $(SOURCE_INCL)/aerospike/*.h | modules
	$(object)

$(TARGET_LIB)/libaerospike.$(DYNAMIC_SUFFIX): $(OBJECTS) $(TARGET_OBJ)/version.o $(SYSTEMTAP_PROBES_O) | modules
	$(library) $(wildcard $(DEPS)) $(LUA_DYNAMIC_OBJ)

$(TARGET_LIB)/libaerospike.a: $(OBJECTS) $(TARGET_OBJ)/version.o $(SYSTEMTAP_PROBES_O) | modules
	$(archive) $(wildcard $(DEPS)) $(LUA_STATIC_OBJ)

ifeq ($(USE_SYSTEMTAP),1)
$(SYSTEMTAP_PROBES_H):	$(SYSTEMTAP_PROBES_D)
	dtrace -h -s $< -o $@

$(SYSTEMTAP_PROBES_O):	$(SYSTEMTAP_PROBES_D)
	dtrace -G -s $< -o $@
endif

###############################################################################
include project/modules.mk project/test.mk project/docs.mk project/rules.mk
