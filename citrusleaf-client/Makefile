ROOT = ..
include $(ROOT)/project/settings.mk
###############################################################################
##  SETTINGS                                                                 ##
###############################################################################

# Modules
BASE 		:= $(realpath ../citrusleaf-base)
COMMON 		:= $(realpath ../aerospike-common)
MODULES 	:= BASE COMMON

# Overrride optimizations via: make O=n
O=3

# Make-local Compiler Flags
CC_FLAGS = -std=gnu99 -g -rdynamic -Wextra
CC_FLAGS += -fno-common -fno-strict-aliasing -fPIC 
CC_FLAGS += -DMARCH_$(ARCH) -D_FILE_OFFSET_BITS=64 
CC_FLAGS += -D_REENTRANT -D_GNU_SOURCE 
CC_FLAGS += -march=nocona

# Make-local Linker Flags
# LD_FLAGS =

# DEBUG Settings
ifdef DEBUG
O=0
CC_FLAGS += -pg -fprofile-arcs -ftest-coverage -g2
LD_FLAGS += -pg -fprofile-arcs -lgcov
endif

# Make-tree Compler Flags
CFLAGS = -O$(O)

# Make-tree Linker Flags
# LDFLAGS = 

# Include Paths
INC_PATH += $(BASE)/$(TARGET_INCL)
INC_PATH += $(COMMON)/$(TARGET_INCL)

# Library Paths
# LIB_PATH +=

###############################################################################
##  OBJECTS                                                                  ##
###############################################################################

CITRUSLEAF = 
CITRUSLEAF += citrusleaf.o
CITRUSLEAF += cl_info.o
CITRUSLEAF += cl_cluster.o
CITRUSLEAF += cl_partition.o
CITRUSLEAF += cl_lookup.o
CITRUSLEAF += cl_batch.o
CITRUSLEAF += cl_scan.o
CITRUSLEAF += cl_async.o
CITRUSLEAF += cl_shm.o
CITRUSLEAF += cl_request.o

# BASE_OBJECTS =
# BASE_OBJECTS += cf_alloc.o
# BASE_OBJECTS += cf_average.o
# BASE_OBJECTS += cf_digest.o
# BASE_OBJECTS += cf_hist.o
# BASE_OBJECTS += cf_hooks.o
# BASE_OBJECTS += cf_ll.o
# BASE_OBJECTS += cf_log.o
# BASE_OBJECTS += cf_proto.o
# BASE_OBJECTS += cf_queue.o
# BASE_OBJECTS += cf_service.o
# BASE_OBJECTS += cf_shash.o
# BASE_OBJECTS += cf_socket.o
# BASE_OBJECTS += cf_vector.o

OBJECTS = 
OBJECTS += $(wildcard $(COMMON)/$(TARGET_OBJ)/citrusleaf/*.o)
OBJECTS += $(wildcard $(BASE)/$(TARGET_OBJ)/client/*.o)

###############################################################################
##  MAIN TARGETS                                                             ##
###############################################################################

all: build prepare

.PHONY: prepare
prepare: modules-prepare $(TARGET_INCL)/citrusleaf/*.h
	$(noop)

.PHONY: build
build: modules libcitrusleaf

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

$(TARGET_OBJ)/%.o: $(CITRUSLEAF_BASE)/$(TARGET_LIB)/libcf-client.a $(COMMON)/$(TARGET_LIB)/libcf-common.a $(SOURCE_MAIN)/%.c $(SOURCE_INCL)/citrusleaf/*.h
	$(object)

$(TARGET_LIB)/libcitrusleaf.so: $(CITRUSLEAF:%=$(TARGET_OBJ)/%) $(TARGET_OBJ)/version.o
	$(library) $(OBJECTS)

$(TARGET_LIB)/libcitrusleaf.a: $(CITRUSLEAF:%=$(TARGET_OBJ)/%) $(TARGET_OBJ)/version.o
	$(archive) $(OBJECTS)

$(TARGET_INCL)/citrusleaf: | $(TARGET_INCL)
	mkdir $@

$(TARGET_INCL)/citrusleaf/%.h:: $(SOURCE_INCL)/citrusleaf/%.h | $(TARGET_INCL)/citrusleaf
	cp -p $(filter %.h, $^) $(TARGET_INCL)/citrusleaf

###############################################################################
include project/modules.mk $(ROOT)/project/rules.mk