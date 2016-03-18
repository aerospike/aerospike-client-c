include project/settings.mk
###############################################################################
##  SETTINGS                                                                 ##
###############################################################################

# Modules
BASE 		:= $(realpath modules/base)
COMMON 		:= $(realpath modules/common)
LUA_CORE 	:= $(realpath modules/lua-core)
MOD_LUA 	:= $(realpath modules/mod-lua)
# If concurrency kit repo path not defined, use default ck module in this repo.
# Currency kit headers are used only.
ifndef CK
	CK := $(realpath modules/ck)
endif
MODULES 	:= BASE COMMON MOD_LUA CK

# Override optimizations via: make O=n
O = 3

# Make-local Compiler Flags
CC_FLAGS = -std=gnu99 -g -Wall -fPIC -O$(O)
CC_FLAGS += -fno-common -fno-strict-aliasing -finline-functions
ifeq ($(ARCH),$(filter $(ARCH),ppc64 ppc64le))
  CC_FLAGS += -DMARCH_$(ARCH)
else
  CC_FLAGS += -march=nocona -DMARCH_$(ARCH)
endif
CC_FLAGS += -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_GNU_SOURCE $(EXT_CFLAGS)

ifeq ($(OS),Darwin)
CC_FLAGS += -D_DARWIN_UNLIMITED_SELECT
else
CC_FLAGS += -rdynamic
endif

ifneq ($(CF), )
CC_FLAGS += -I$(CF)/include
endif

# Linker flags
LD_FLAGS = $(LDFLAGS) -lm -fPIC

ifeq ($(OS),Darwin)
LD_FLAGS += -undefined dynamic_lookup
endif

# DEBUG Settings
ifdef DEBUG
O=0
CC_FLAGS += -pg -fprofile-arcs -ftest-coverage -g2
LD_FLAGS += -pg -fprofile-arcs -lgcov
endif

# Include Paths
INC_PATH += $(BASE)/$(TARGET_INCL)
INC_PATH += $(COMMON)/$(TARGET_INCL)
INC_PATH += $(MOD_LUA)/$(TARGET_INCL)
INC_PATH += $(CK)/include
INC_PATH += /usr/local/include

INC_PATH += $(or \
    $(wildcard /usr/include/lua-5.1), \
    $(wildcard /usr/include/lua5.1) \
    )

# Library Paths
# LIB_PATH +=

###############################################################################
##  OBJECTS                                                                  ##
###############################################################################

CITRUSLEAF = 
CITRUSLEAF += citrusleaf.o
CITRUSLEAF += cl_batch.o
CITRUSLEAF += cl_info.o
CITRUSLEAF += cl_parsers.o
CITRUSLEAF += cl_query.o
CITRUSLEAF += cl_sindex.o
CITRUSLEAF += cl_scan.o
CITRUSLEAF += cl_scan2.o
CITRUSLEAF += cl_udf.o

AEROSPIKE = 
AEROSPIKE += _bin.o
AEROSPIKE += _logger.o
AEROSPIKE += _ldt.o
AEROSPIKE += _policy.o
AEROSPIKE += _shim.o
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
AEROSPIKE += as_bin.o
AEROSPIKE += as_config.o
AEROSPIKE += as_cluster.o
AEROSPIKE += as_error.o
AEROSPIKE += as_info.o
AEROSPIKE += as_key.o
AEROSPIKE += as_log.o
AEROSPIKE += as_lookup.o
AEROSPIKE += as_node.o
AEROSPIKE += as_operations.o
AEROSPIKE += as_partition.o
AEROSPIKE += as_policy.o
AEROSPIKE += as_query.o
AEROSPIKE += as_record.o
AEROSPIKE += as_record_hooks.o
AEROSPIKE += as_record_iterator.o
AEROSPIKE += as_scan.o
AEROSPIKE += as_shm_cluster.o
AEROSPIKE += as_udf.o
AEROSPIKE += as_ldt.o

OBJECTS := 
OBJECTS += $(CITRUSLEAF:%=$(TARGET_OBJ)/citrusleaf/%) 
OBJECTS += $(AEROSPIKE:%=$(TARGET_OBJ)/aerospike/%)

DEPS :=
DEPS += $(COMMON)/$(TARGET_OBJ)/common/aerospike/*.o
DEPS += $(COMMON)/$(TARGET_OBJ)/common/citrusleaf/*.o
DEPS += $(BASE)/$(TARGET_OBJ)/base/*.o
DEPS += $(MOD_LUA)/$(TARGET_OBJ)/*.o

###############################################################################
##  HEADERS                                                                  ##
###############################################################################

COMMON-HEADERS :=
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_arraylist.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_arraylist_iterator.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_boolean.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_bytes.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_hashmap.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_hashmap_iterator.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_integer.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_iterator.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_list.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_list_iterator.h
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

BASE-HEADERS := $(BASE)/$(SOURCE_INCL)/citrusleaf/cf_log.h

EXCLUDE-HEADERS = 

HEADERS := 
HEADERS += $(filter-out $(EXCLUDE-HEADERS), $(wildcard $(SOURCE_INCL)/aerospike/*.h))
HEADERS += $(COMMON-HEADERS)
HEADERS += $(BASE-HEADERS)

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
build: libaerospike

.PHONY: build-clean
build-clean:
	@rm -rf $(TARGET)

.PHONY: libaerospike
libaerospike: libaerospike.a libaerospike.$(DYNAMIC_SUFFIX)

.PHONY: libaerospike.a libaerospike.$(DYNAMIC_SUFFIX)
libaerospike.a: $(TARGET_LIB)/libaerospike.a
libaerospike.$(DYNAMIC_SUFFIX): $(TARGET_LIB)/libaerospike.$(DYNAMIC_SUFFIX)

install:
	cp $(TARGET_LIB)/libaerospike.* /usr/local/lib/

###############################################################################
##  BUILD TARGETS                                                            ##
###############################################################################

$(TARGET_OBJ)/%.o: $(BASE)/$(TARGET_LIB)/libaerospike-base.a $(COMMON)/$(TARGET_LIB)/libaerospike-common.a $(MOD_LUA)/$(TARGET_LIB)/libmod_lua.a $(SOURCE_MAIN)/%.c $(SOURCE_INCL)/citrusleaf/*.h | modules
	$(object)

$(TARGET_OBJ)/aerospike/%.o: $(BASE)/$(TARGET_LIB)/libaerospike-base.a $(COMMON)/$(TARGET_LIB)/libaerospike-common.a $(MOD_LUA)/$(TARGET_LIB)/libmod_lua.a $(SOURCE_MAIN)/aerospike/%.c $(SOURCE_INCL)/citrusleaf/*.h $(SOURCE_INCL)/aerospike/*.h | modules
	$(object)

$(TARGET_LIB)/libaerospike.$(DYNAMIC_SUFFIX): $(OBJECTS) $(TARGET_OBJ)/version.o | modules
	$(library) $(wildcard $(DEPS))

$(TARGET_LIB)/libaerospike.a: $(OBJECTS) $(TARGET_OBJ)/version.o | modules
	$(archive) $(wildcard $(DEPS))

$(TARGET_INCL)/aerospike: | $(TARGET_INCL)
	mkdir $@

$(TARGET_INCL)/citrusleaf: | $(TARGET_INCL)
	mkdir $@

$(TARGET_INCL)/aerospike/%.h:: $(SOURCE_INCL)/aerospike/%.h | $(TARGET_INCL)/aerospike
	cp $^ $@

$(TARGET)/old-include/citrusleaf/%.h:: $(SOURCE_INCL)/citrusleaf/%.h | $(TARGET_INCL)/citrusleaf
	cp $^ $@

###############################################################################
include project/modules.mk project/test.mk project/docs.mk project/rules.mk
