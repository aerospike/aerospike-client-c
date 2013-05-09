ROOT = ../../..
include $(ROOT)/project/settings.mk
###############################################################################
##  SETTINGS                                                                 ##
###############################################################################

CITRUSLEAF = $(ROOT)/citrusleaf-libevent

CC_FLAGS = -std=gnu99  -Wall
CC_FLAGS += -g -fno-common -fomit-frame-pointer
CC_FLAGS += -fno-strict-aliasing -rdynamic
CC_FLAGS += -D_REENTRANT -D MARCH_$(ARCH)

# LD_FLAGS = -levent
# LD_FLAGS += -lssl -lcrypto -lpthread -lrt
# LD_FLAGS += -lm

LIB_PATH = $(CITRUSLEAF)/$(TARGET_LIB)
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

.PHONY: run
run: $(TARGET_BIN)/example
	$(TARGET_BIN)/example

.PHONY: example
example: $(TARGET_BIN)/example

###############################################################################
##  BUILD TARGETS                                                            ##
###############################################################################

$(TARGET_OBJ)/%.o: %.c $(CITRUSLEAF)/$(TARGET_LIB)/libev2citrusleaf.a 
	$(object)

$(TARGET_BIN)/example: $(OBJECTS:%=$(TARGET_OBJ)/%) 
	$(executable) $(CITRUSLEAF)/$(TARGET_LIB)/libev2citrusleaf.a -levent -lssl -lcrypto -lpthread -lrt -lm

###############################################################################
##  BUILD DEPENDENCIES                                                       ##
###############################################################################

$(CITRUSLEAF)/$(TARGET_LIB)/libev2citrusleaf.a: $(CITRUSLEAF)/$(SOURCE_INCL)/citrusleaf_event2/*.h $(CITRUSLEAF)/$(SOURCE_MAIN)/*.c
	$(MAKE) -C $(CITRUSLEAF) libcitrusleaf.a prepare

###############################################################################
include $(ROOT)/project/rules.mk