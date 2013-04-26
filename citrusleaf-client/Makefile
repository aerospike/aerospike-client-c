###############################################################################
include ../project/settings.makefile
###############################################################################

CITRUSLEAF_BASE = $(REPO)/citrusleaf-base

###############################################################################
##  FLAGS                                                                    ##
###############################################################################

# To Override Optimization Level, run make with the "O" variable set.
# 	make O=0

O=3

CFLAGS = -O$(O)

CC_FLAGS = -std=gnu99 -g -rdynamic -Wextra
CC_FLAGS += -fno-common -fno-strict-aliasing -fPIC 
CC_FLAGS += -DMARCH_$(ARCH) 
CC_FLAGS += -D_FILE_OFFSET_BITS=64 
CC_FLAGS += -D_REENTRANT
CC_FLAGS += -march=nocona

INC_PATH += $(CITRUSLEAF_BASE)/$(TARGET_INCL)


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

BASE =
BASE += cf_alloc.o
BASE += cf_average.o
BASE += cf_digest.o
BASE += cf_hist.o
BASE += cf_hooks.o
BASE += cf_ll.o
BASE += cf_log.o
BASE += cf_proto.o
BASE += cf_queue.o
BASE += cf_service.o
BASE += cf_shash.o
BASE += cf_socket.o
BASE += cf_vector.o

###############################################################################
##  MAIN TARGETS                                                             ##
###############################################################################

all: build prepare

.PHONY: prepare
prepare: $(CITRUSLEAF_BASE)/$(TARGET_INCL) $(TARGET_INCL)/citrusleaf/*.h

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

$(TARGET_SRC)/version.c: | $(TARGET_SRC)
	bash ./src/shell/version.sh > $@

$(TARGET_OBJ)/%.o: $(TARGET_SRC)/%.c
	$(object)

$(TARGET_OBJ)/%.o: $(CITRUSLEAF_BASE)/$(TARGET_LIB)/libcf-client.a $(SOURCE_MAIN)/%.c $(SOURCE_INCL)/citrusleaf/*.h
	$(object)

$(TARGET_LIB)/libcitrusleaf.so: $(CITRUSLEAF:%=$(TARGET_OBJ)/%) $(TARGET_OBJ)/version.o
	$(library) $(BASE:%=$(CITRUSLEAF_BASE)/$(TARGET_OBJ)/client/%)

$(TARGET_LIB)/libcitrusleaf.a: $(CITRUSLEAF:%=$(TARGET_OBJ)/%) $(TARGET_OBJ)/version.o
	$(archive) $(BASE:%=$(CITRUSLEAF_BASE)/$(TARGET_OBJ)/client/%)

$(TARGET_INCL)/citrusleaf: | $(TARGET_INCL)
	mkdir $@

$(TARGET_INCL)/citrusleaf/%.h:: $(SOURCE_INCL)/citrusleaf/%.h $(CITRUSLEAF_BASE)/$(TARGET_INCL)/citrusleaf/%.h | $(TARGET_INCL)/citrusleaf
	cp -p $(filter %.h, $^) $(TARGET_INCL)/citrusleaf

$(TARGET_INCL)/citrusleaf/%.h::  $(CITRUSLEAF_BASE)/$(TARGET_INCL)/citrusleaf/%.h | $(TARGET_INCL)/citrusleaf $(CITRUSLEAF_BASE)/$(TARGET_INCL)
	cp -p $(filter %.h, $^) $(TARGET_INCL)/citrusleaf

###############################################################################
##  BUILD DEPENDENCIES                                                       ##
###############################################################################

$(CITRUSLEAF_BASE)/$(TARGET_LIB)/libcf-client.a: $(CITRUSLEAF_BASE)/$(SOURCE_INCL)/citrusleaf/*.h $(CITRUSLEAF_BASE)/$(SOURCE_MAIN)/*.c
	$(MAKE) -C $(CITRUSLEAF_BASE) libcf-client.a prepare

.PHONY: $(CITRUSLEAF_BASE)/$(TARGET_INCL)
$(CITRUSLEAF_BASE)/$(TARGET_INCL): $(CITRUSLEAF_BASE)/$(TARGET_INCL)/citrusleaf/*.h | $(TARGET_INCL)/citrusleaf

.PHONY: $(CITRUSLEAF_BASE)/$(TARGET_INCL)/citrusleaf
$(CITRUSLEAF_BASE)/$(TARGET_INCL)/citrusleaf: $(CITRUSLEAF_BASE)/$(SOURCE_INCL)/citrusleaf/*.h
	$(MAKE) -C $(CITRUSLEAF_BASE) prepare

$(CITRUSLEAF_BASE)/$(TARGET_INCL)/citrusleaf/%.h:  $(CITRUSLEAF_BASE)/$(TARGET_INCL)/citrusleaf | $(TARGET_INCL)/citrusleaf
	$(noop)

###############################################################################
include $(RULES)
###############################################################################