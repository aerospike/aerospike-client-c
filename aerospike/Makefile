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
CITRUSLEAF += as_scan.o
CITRUSLEAF += cl_sindex.o
CITRUSLEAF += cl_scan.o
CITRUSLEAF += cl_udf.o
CITRUSLEAF += aerospike_lstack.o
CITRUSLEAF += aerospike_lset.o

OBJECTS :=
OBJECTS += $(COMMON)/$(TARGET_OBJ)/common/aerospike/*.o
OBJECTS += $(COMMON)/$(TARGET_OBJ)/common/citrusleaf/*.o
OBJECTS += $(BASE)/$(TARGET_OBJ)/base/*.o
OBJECTS += $(MOD_LUA)/$(TARGET_OBJ)/*.o
OBJECTS += $(addprefix $(MSGPACK)/src/.libs/, unpack.o objectc.o version.o vrefbuffer.o zone.o)

HEADERS := $(wildcard $(SOURCE_INCL)/citrusleaf/*.h)

###############################################################################
##  MAIN TARGETS                                                             ##
###############################################################################

all: modules build prepare

.PHONY: prepare
prepare: modules-prepare $(subst $(SOURCE_INCL),$(TARGET_INCL),$(HEADERS)) 
	$(noop)

.PHONY: build
build: libcitrusleaf

.PHONY: build-clean
build-clean:
	@rm -rf $(TARGET_BIN)
	@rm -rf $(TARGET_LIB)

.PHONY: libcitrusleaf
libcitrusleaf: libcitrusleaf.a libcitrusleaf.so

.PHONY: libcitrusleaf.a libcitrusleaf.so
libcitrusleaf.a: $(TARGET_LIB)/libcitrusleaf.a
libcitrusleaf.so: $(TARGET_LIB)/libcitrusleaf.so

###############################################################################
##  BUILD TARGETS                                                            ##
###############################################################################

$(TARGET_OBJ)/%.o: $(BASE)/$(TARGET_LIB)/libaerospike-base.a $(COMMON)/$(TARGET_LIB)/libaerospike-common.a $(MOD_LUA)/$(TARGET_LIB)/libmod_lua.a $(SOURCE_MAIN)/%.c $(SOURCE_INCL)/citrusleaf/*.h
	$(object)

$(TARGET_LIB)/libcitrusleaf.so: $(CITRUSLEAF:%=$(TARGET_OBJ)/%) $(TARGET_OBJ)/version.o | modules
	$(library) $(wildcard $(OBJECTS))

$(TARGET_LIB)/libcitrusleaf.a: $(CITRUSLEAF:%=$(TARGET_OBJ)/%) $(TARGET_OBJ)/version.o | modules
	$(archive) $(wildcard $(OBJECTS))

$(TARGET_INCL)/aerospike: | $(TARGET_INCL)
	mkdir $@

$(TARGET_INCL)/citrusleaf: | $(TARGET_INCL)
	mkdir $@

$(TARGET_INCL)/citrusleaf/%.h:: $(SOURCE_INCL)/citrusleaf/%.h | $(TARGET_INCL)/citrusleaf
	cp $^ $@

###############################################################################
include project/modules.mk project/test.mk $(ROOT)/project/rules.mk