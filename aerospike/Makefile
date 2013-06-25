ROOT = ..
include $(ROOT)/project/settings.mk
###############################################################################
##  SETTINGS                                                                 ##
###############################################################################

# Modules
BASE 		:= $(realpath ../citrusleaf-base)
COMMON 		:= $(realpath ../aerospike-common)
MOD_LUA 	:= $(realpath ../aerospike-mod-lua)
MSGPACK 	:= $(realpath ../msgpack)
MODULES 	:= BASE COMMON MOD_LUA MSGPACK

# Overrride optimizations via: make O=n
O=3

# Enable memcount 
MEM_COUNT=1

# Make-local Compiler Flags
CC_FLAGS = -std=gnu99 -g -rdynamic -Wall 
CC_FLAGS += -fno-common -fno-strict-aliasing -fPIC 
CC_FLAGS += -DMARCH_$(ARCH) -D_FILE_OFFSET_BITS=64 
CC_FLAGS += -D_REENTRANT -D_GNU_SOURCE -DMEM_COUNT

# Make-local Linker Flags
LD_FLAGS = -lm

# DEBUG Settings
ifdef DEBUG
O=0
CC_FLAGS += -pg -fprofile-arcs -ftest-coverage -g2
LD_FLAGS += -pg -fprofile-arcs -lgcov
endif

# Make-tree Compler Flags
CFLAGS = -O$(O) -DMEM_COUNT=$(MEM_COUNT)

# Make-tree Linker Flags
# LDFLAGS = 

# Include Paths
INC_PATH += $(BASE)/$(TARGET_INCL)
INC_PATH += $(COMMON)/$(TARGET_INCL)
INC_PATH += $(MOD_LUA)/$(TARGET_INCL)
INC_PATH += $(MSGPACK)/src

# Library Paths
# LIB_PATH +=

###############################################################################
##  OBJECTS                                                                  ##
###############################################################################

CITRUSLEAF = 
CITRUSLEAF += citrusleaf.o
CITRUSLEAF += cl_async.o
CITRUSLEAF += cl_batch.o
CITRUSLEAF += cl_cluster.o
CITRUSLEAF += cl_info.o
CITRUSLEAF += cl_lookup.o
CITRUSLEAF += cl_partition.o
CITRUSLEAF += cl_query.o
CITRUSLEAF += cl_sindex.o
CITRUSLEAF += cl_scan.o
CITRUSLEAF += cl_scan2.o
CITRUSLEAF += cl_udf.o
CITRUSLEAF += aerospike_lstack.o
CITRUSLEAF += aerospike_lset.o

AEROSPIKE = 
AEROSPIKE += _bin.o
AEROSPIKE += _policy.o
AEROSPIKE += _shim.o
AEROSPIKE += aerospike.o
AEROSPIKE += aerospike_info.o
AEROSPIKE += aerospike_key.o
AEROSPIKE += aerospike_query.o
AEROSPIKE += aerospike_scan.o
AEROSPIKE += aerospike_index.o
AEROSPIKE += aerospike_udf.o
AEROSPIKE += as_config.o
AEROSPIKE += as_error.o
AEROSPIKE += as_key.o
AEROSPIKE += as_log.o
AEROSPIKE += as_operations.o
AEROSPIKE += as_policy.o
AEROSPIKE += as_query.o
AEROSPIKE += as_record.o
AEROSPIKE += as_scan.o
AEROSPIKE += as_udf.o

OBJECTS := 
OBJECTS += $(CITRUSLEAF:%=$(TARGET_OBJ)/citrusleaf/%) 
OBJECTS += $(AEROSPIKE:%=$(TARGET_OBJ)/aerospike/%)

DEPS :=
DEPS += $(COMMON)/$(TARGET_OBJ)/common/aerospike/*.o
DEPS += $(COMMON)/$(TARGET_OBJ)/common/citrusleaf/*.o
DEPS += $(BASE)/$(TARGET_OBJ)/base/*.o
DEPS += $(MOD_LUA)/$(TARGET_OBJ)/*.o
DEPS += $(addprefix $(MSGPACK)/src/.libs/, unpack.o objectc.o version.o vrefbuffer.o zone.o)

###############################################################################
##  HEADERS                                                                  ##
###############################################################################

COMMON-HEADERS :=
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_arraylist.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_arraylist_iterator.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_bytes.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_hashmap.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_hashmap_iterator.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_integer.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_iterator.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_linkedlist.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_linkedlist_iterator.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_list.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_map.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_nil.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_pair.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_rec.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_stream.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_string.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_stringmap.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_util.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/aerospike/as_val.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/citrusleaf/cf_arch.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/citrusleaf/cf_atomic.h
# COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/citrusleaf/cf_shash.h
COMMON-HEADERS += $(COMMON)/$(SOURCE_INCL)/citrusleaf/cf_types.h

HEADERS := 
HEADERS += $(wildcard $(SOURCE_INCL)/aerospike/*.h)

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
libaerospike: libaerospike.a libaerospike.so

.PHONY: libaerospike.a libaerospike.so
libaerospike.a: $(TARGET_LIB)/libaerospike.a
libaerospike.so: $(TARGET_LIB)/libaerospike.so

###############################################################################
##  BUILD TARGETS                                                            ##
###############################################################################

$(TARGET_OBJ)/%.o: $(BASE)/$(TARGET_LIB)/libaerospike-base.a $(COMMON)/$(TARGET_LIB)/libaerospike-common.a $(MOD_LUA)/$(TARGET_LIB)/libmod_lua.a $(SOURCE_MAIN)/%.c $(SOURCE_INCL)/citrusleaf/*.h | modules
	$(object)

$(TARGET_OBJ)/aerospike/%.o: $(BASE)/$(TARGET_LIB)/libaerospike-base.a $(COMMON)/$(TARGET_LIB)/libaerospike-common.a $(MOD_LUA)/$(TARGET_LIB)/libmod_lua.a $(SOURCE_MAIN)/aerospike/%.c $(SOURCE_INCL)/citrusleaf/*.h $(SOURCE_INCL)/aerospike/*.h | modules
	$(object)

$(TARGET_LIB)/libaerospike.so: $(OBJECTS) $(TARGET_OBJ)/version.o | modules
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
include project/modules.mk project/test.mk project/docs.mk $(ROOT)/project/rules.mk
