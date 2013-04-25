###############################################################################
include ../project/settings.makefile
###############################################################################

###############################################################################
##  FLAGS                                                                    ##
###############################################################################

# To Override Optimization Level, run make with the "O" variable set.
# 	make O=0

O=3

CFLAGS = -O$(O)

CC_FLAGS = -std=gnu99 -g -rdynamic -Wextra
CC_FLAGS += -fno-common -fno-strict-aliasing -fPIC 
CC_FLAGS += -D_FILE_OFFSET_BITS=64 
CC_FLAGS += -D_REENTRANT 
CC_FLAGS += -D_GNU_SOURCE

###############################################################################
##  OBJECTS                                                                  ##
###############################################################################

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
prepare: $(TARGET_INCL)/citrusleaf/*.h

.PHONY: build
build: libcf-client libcf-hooked

.PHONY: build-clean
build-clean:
	@rm -rf $(TARGET_BIN)
	@rm -rf $(TARGET_LIB)

.PHONY: libcf-client libcf-client.a libcf-client.so
libcf-client: libcf-client.a libcf-client.so
libcf-client.a: $(TARGET_LIB)/libcf-client.a
libcf-client.so: $(TARGET_LIB)/libcf-client.so

.PHONY: libcf-hooked libcf-hooked.a libcf-hooked.so
libcf-hooked: libcf-hooked.a libcf-hooked.so
libcf-hooked.a: $(TARGET_LIB)/libcf-hooked.a
libcf-hooked.so: $(TARGET_LIB)/libcf-hooked.so

###############################################################################
##  BUILD TARGETS                                                            ##
###############################################################################

# CLIENT (STANDARD) BASE

$(TARGET_OBJ)/client/%.o: $(SOURCE_MAIN)/%.c $(SOURCE_INCL)/citrusleaf/*.h
	$(object)

$(TARGET_LIB)/libcf-client.so: $(BASE:%=$(TARGET_OBJ)/client/%)

$(TARGET_LIB)/libcf-client.a: $(BASE:%=$(TARGET_OBJ)/client/%)

# HOOKED BASE

$(TARGET_OBJ)/hooked/%.o: CC_FLAGS += -DEXTERNAL_LOCKS
$(TARGET_OBJ)/hooked/%.o: $(SOURCE_MAIN)/%.c $(SOURCE_INCL)/citrusleaf/*.h
	$(object)

$(TARGET_LIB)/libcf-hooked.so: $(BASE:%=$(TARGET_OBJ)/hooked/%)

$(TARGET_LIB)/libcf-hooked.a: $(BASE:%=$(TARGET_OBJ)/hooked/%)

# BASE HEADERS

$(TARGET_INCL)/citrusleaf: | $(TARGET_INCL)
	mkdir -p $@

$(TARGET_INCL)/citrusleaf/%.h:: $(SOURCE_INCL)/citrusleaf/%.h | $(TARGET_INCL)/citrusleaf
	cp -p $^ $(TARGET_INCL)/citrusleaf

###############################################################################
include $(RULES)
###############################################################################