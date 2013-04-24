###############################################################################
include ../project/settings.makefile
###############################################################################

###############################################################################
##  FLAGS                                                                    ##
###############################################################################

CITRUSLEAF_BASE = ../citrusleaf-base

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
CITRUSLEAF += ev2citrusleaf.o
CITRUSLEAF += cl_info.o
CITRUSLEAF += cl_cluster.o
CITRUSLEAF += cl_lookup.o
CITRUSLEAF += cl_partition.o
CITRUSLEAF += cl_batch.o

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
BASE += cf_shash.o
BASE += cf_socket.o
BASE += cf_vector.o

###############################################################################
##  MAIN TARGETS                                                             ##
###############################################################################

all: build prepare

.PHONY: prepare
prepare: $(TARGET_INCL)

.PHONY: build
build: libev2citrusleaf

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

$(TARGET_LIB)/libev2citrusleaf.so: $(CITRUSLEAF:%=$(TARGET_OBJ)/%) | $(CITRUSLEAF_BASE)/$(TARGET_LIB)/libcf-hooked.a
	$(library) $(BASE:%=$(CITRUSLEAF_BASE)/$(TARGET_OBJ)/hooked/%)

$(TARGET_LIB)/libev2citrusleaf.a: $(CITRUSLEAF:%=$(TARGET_OBJ)/%) | $(CITRUSLEAF_BASE)/$(TARGET_LIB)/libcf-hooked.a
	$(archive) $(BASE:%=$(CITRUSLEAF_BASE)/$(TARGET_OBJ)/hooked/%)

$(TARGET_INCL): $(wildcard $(SOURCE_INCL)/citrusleaf_event2/*.h) $(CITRUSLEAF_BASE)/$(TARGET_INCL)/citrusleaf/*.h
	@if [ ! -e "$(TARGET_INCL)" ]; then mkdir -p $(TARGET_INCL); fi
	@if [ ! -e "$(TARGET_INCL)/citrusleaf_event2" ]; then mkdir -p $(TARGET_INCL)/citrusleaf_event2; fi
	@if [ ! -e "$(TARGET_INCL)/citrusleaf" ]; then mkdir -p $(TARGET_INCL)/citrusleaf; fi
	cp -pR $(SOURCE_INCL)/citrusleaf_event2/*.h $(TARGET_INCL)/citrusleaf_event2/.
	cp -pR $(CITRUSLEAF_BASE)/$(TARGET_INCL)/citrusleaf/*.h $(TARGET_INCL)/citrusleaf/.

###############################################################################
##  DEPENDENCIES                                                             ##
###############################################################################

$(CITRUSLEAF_BASE)/$(TARGET_LIB)/libcf-hooked.a:
	$(MAKE) -c $(CITRUSLEAF_BASE) libcf-hooked.a

###############################################################################
include ../project/rules.makefile
###############################################################################