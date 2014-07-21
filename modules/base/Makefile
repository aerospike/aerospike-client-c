ROOT = ../..
include $(ROOT)/project/settings.mk
###############################################################################
##  SETTINGS                                                                 ##
###############################################################################

# Modules
COMMON 		:= $(realpath ../aerospike-common)
MODULES 	:= COMMON

# Overrride optimizations via: make O=n
O = 3

# Make-local Compiler Flags
ifeq ($(OS),Darwin)
CC_FLAGS = -std=gnu99 -g -Wextra
else
CC_FLAGS = -std=gnu99 -g -rdynamic -Wextra
endif
CC_FLAGS += -fno-common -fno-strict-aliasing -fPIC -O$(O)
CC_FLAGS += -D_FILE_OFFSET_BITS=64 
CC_FLAGS += -D_REENTRANT 
CC_FLAGS += -D_GNU_SOURCE

# Make-local Linker Flags
LD_FLAGS = -Wall -Winline -rdynamic

# DEBUG Settings
ifdef DEBUG
O=0
CC_FLAGS += -pg -fprofile-arcs -ftest-coverage -g2
LD_FLAGS += -pg -fprofile-arcs -lgcov
endif

# Include Paths
INC_PATH += $(COMMON)/$(TARGET_INCL)

# Library Paths
# LIB_PATH +=

###############################################################################
##  OBJECTS                                                                  ##
###############################################################################

BASE =
BASE += cf_average.o
BASE += cf_client_rc.o
# BASE += cf_digest.o
# BASE += cf_hooks.o
# BASE += cf_ll.o
BASE += cf_log.o
BASE += cf_proto.o
BASE += cf_service.o
# BASE += cf_shash.o
BASE += cf_socket.o
# BASE += cf_vector.o
BASE += cf_packet_compression.o

HEADERS := $(wildcard $(SOURCE_INCL)/citrusleaf/*.h)

###############################################################################
##  MAIN TARGETS                                                             ##
###############################################################################

all: build prepare

.PHONY: prepare
prepare: modules-prepare $(subst $(SOURCE_INCL),$(TARGET_INCL),$(HEADERS)) 

.PHONY: build
build: libaerospike-base libaerospike-base-hooked

.PHONY: build-clean
build-clean:
	@rm -rf $(TARGET_BIN)
	@rm -rf $(TARGET_LIB)

.PHONY: libaerospike-base libaerospike-base.a libaerospike-base.so
libaerospike-base: libaerospike-base.a libaerospike-base.so
libaerospike-base.a: $(TARGET_LIB)/libaerospike-base.a
libaerospike-base.so: $(TARGET_LIB)/libaerospike-base.so

.PHONY: libaerospike-base-hooked libaerospike-base-hooked.a libaerospike-base-hooked.so
libaerospike-base-hooked: libaerospike-base-hooked.a libaerospike-base-hooked.so
libaerospike-base-hooked.a: $(TARGET_LIB)/libaerospike-base-hooked.a
libaerospike-base-hooked.so: $(TARGET_LIB)/libaerospike-base-hooked.so

###############################################################################
##  BUILD TARGETS                                                            ##
###############################################################################

# BASE

$(TARGET_OBJ)/base/%.o: $(SOURCE_MAIN)/%.c $(SOURCE_INCL)/citrusleaf/*.h | modules-prepare 
	$(object)

$(TARGET_LIB)/libaerospike-base.so: modules-prepare $(BASE:%=$(TARGET_OBJ)/base/%) | $(COMMON)/$(TARGET_INCL)/aerospike/*.h
	$(library)

$(TARGET_LIB)/libaerospike-base.a: modules-prepare $(BASE:%=$(TARGET_OBJ)/base/%) | $(COMMON)/$(TARGET_INCL)/aerospike/*.h
	$(archive)

# HOOKED BASE

$(TARGET_OBJ)/base-hooked/%.o: CC_FLAGS += -DEXTERNAL_LOCKS
$(TARGET_OBJ)/base-hooked/%.o: $(SOURCE_MAIN)/%.c $(SOURCE_INCL)/citrusleaf/*.h
	$(object)

$(TARGET_LIB)/libaerospike-base-hooked.so: modules-prepare $(BASE:%=$(TARGET_OBJ)/base-hooked/%) | $(COMMON)/$(TARGET_INCL)/aerospike/*.h
	$(library)

$(TARGET_LIB)/libaerospike-base-hooked.a: modules-prepare $(BASE:%=$(TARGET_OBJ)/base-hooked/%) | $(COMMON)/$(TARGET_INCL)/aerospike/*.h
	$(archive)

# BASE HEADERS

$(TARGET_INCL)/citrusleaf: | $(TARGET_INCL)
	mkdir $@

$(TARGET_INCL)/citrusleaf/%.h:: $(SOURCE_INCL)/citrusleaf/%.h | $(TARGET_INCL)/citrusleaf
	cp $^ $@

###############################################################################
include project/modules.mk $(ROOT)/project/rules.mk
