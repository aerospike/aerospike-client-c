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
CC_FLAGS += -DMARCH_$(ARCH) 
CC_FLAGS += -D_FILE_OFFSET_BITS=64 
CC_FLAGS += -D_REENTRANT
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
CITRUSLEAF += ev2citrusleaf.o
CITRUSLEAF += cl_info.o
CITRUSLEAF += cl_cluster.o
CITRUSLEAF += cl_lookup.o
CITRUSLEAF += cl_partition.o
CITRUSLEAF += cl_batch.o

OBJECTS = 
OBJECTS += $(COMMON)/$(TARGET_OBJ)/common-hooked/citrusleaf/*.o
OBJECTS += $(BASE)/$(TARGET_OBJ)/base-hooked/*.o

###############################################################################
##  MAIN TARGETS                                                             ##
###############################################################################

all: build prepare

.PHONY: prepare
prepare: modules-prepare $(TARGET_INCL)/citrusleaf_event2/*.h

.PHONY: build
build: modules libev2citrusleaf

.PHONY: build-clean
build-clean:
	@rm -rf $(TARGET_BIN)
	@rm -rf $(TARGET_LIB)

.PHONY: libev2citrusleaf
libev2citrusleaf: libev2citrusleaf.a libev2citrusleaf.so

.PHONY: libev2citrusleaf.a libev2citrusleaf.so
libev2citrusleaf.a: $(TARGET_LIB)/libev2citrusleaf.a
libev2citrusleaf.so: $(TARGET_LIB)/libev2citrusleaf.so

###############################################################################
##  BUILD TARGETS                                                            ##
###############################################################################

$(TARGET_OBJ)/%.o: $(BASE)/$(TARGET_LIB)/libaerospike-base-hooked.a $(COMMON)/$(TARGET_LIB)/libaerospike-common-hooked.a $(SOURCE_MAIN)/%.c $(SOURCE_INCL)/citrusleaf_event2/*.h
	$(object)

$(TARGET_LIB)/libev2citrusleaf.so: $(CITRUSLEAF:%=$(TARGET_OBJ)/%) $(TARGET_OBJ)/version.o | modules
	$(library) $(wildcard $(OBJECTS))

$(TARGET_LIB)/libev2citrusleaf.a: $(CITRUSLEAF:%=$(TARGET_OBJ)/%) $(TARGET_OBJ)/version.o | modules
	$(archive) $(wildcard $(OBJECTS))

$(TARGET_INCL)/citrusleaf: | $(TARGET_INCL)
	mkdir $@

$(TARGET_INCL)/citrusleaf_event2: | $(TARGET_INCL)
	mkdir $@

$(TARGET_INCL)/citrusleaf_event2/%.h:: $(SOURCE_INCL)/citrusleaf_event2/%.h | $(TARGET_INCL)/citrusleaf_event2
	cp -p $^ $(TARGET_INCL)/citrusleaf_event2

###############################################################################
include project/modules.mk $(ROOT)/project/rules.mk