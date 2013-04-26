###############################################################################
include $(shell git rev-parse --show-toplevel)/project/settings.makefile
###############################################################################

CITRUSLEAF = $(REPO)/citrusleaf-client

###############################################################################
##  FLAGS                                                                    ##
###############################################################################

CC_FLAGS = -std=gnu99  -Wall
CC_FLAGS += -g -fno-common -fomit-frame-pointer
CC_FLAGS += -fno-strict-aliasing -rdynamic
CC_FLAGS += -D_REENTRANT -D MARCH_$(ARCH)

LD_FLAGS = 
LD_FLAGS += -lssl -lcrypto -lpthread -lrt
LD_FLAGS += -llua
LD_FLAGS += -lm

INC_PATH = $(CITRUSLEAF)/$(TARGET_INCL)

###############################################################################
##  OBJECTS                                                                  ##
###############################################################################

OBJECTS = example.o

###############################################################################
##  MAIN TARGETS                                                             ##
###############################################################################

all: build

.PHONY: build
build: example

.PHONY: build-clean
build-clean:
	@rm -rf $(TARGET_BIN)
	@rm -rf $(TARGET_LIB)

.PHONY: example
example: $(TARGET_BIN)/example

###############################################################################
##  BUILD TARGETS                                                            ##
###############################################################################

$(TARGET_OBJ)/%.o: %.c $(CITRUSLEAF)/$(TARGET_LIB)/libcitrusleaf.a 
	$(object)

$(TARGET_BIN)/example: $(OBJECTS:%=$(TARGET_OBJ)/%) 
	$(executable) $(CITRUSLEAF)/$(TARGET_LIB)/libcitrusleaf.a

###############################################################################
##  BUILD DEPENDENCIES                                                       ##
###############################################################################

$(CITRUSLEAF)/$(TARGET_LIB)/libcitrusleaf.a: $(CITRUSLEAF)/$(SOURCE_INCL)/citrusleaf/*.h  $(CITRUSLEAF)/$(SOURCE_MAIN)/*.c
	$(MAKE) -C $(CITRUSLEAF) libcitrusleaf.a prepare

###############################################################################
include $(RULES)
###############################################################################