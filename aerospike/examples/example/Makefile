ROOT = ../../..
###############################################################################
include $(ROOT)/project/settings.makefile
###############################################################################

AEROSPIKE = $(ROOT)/aerospike

###############################################################################
##  FLAGS                                                                    ##
###############################################################################

CC_FLAGS = -std=gnu99  -Wall

LD_FLAGS = 
LD_FLAGS += -lssl -lcrypto -lpthread -lrt
LD_FLAGS += -llua
LD_FLAGS += -lm

INC_PATH = $(AEROSPIKE)/$(TARGET_INCL)

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

$(TARGET_OBJ)/%.o: %.c $(AEROSPIKE)/$(TARGET_LIB)/libcitrusleaf.a 
	$(object)

$(TARGET_BIN)/example: $(OBJECTS:%=$(TARGET_OBJ)/%) 
	$(executable) $(AEROSPIKE)/$(TARGET_LIB)/libcitrusleaf.a

###############################################################################
##  BUILD DEPENDENCIES                                                       ##
###############################################################################

$(AEROSPIKE)/$(TARGET_LIB)/libcitrusleaf.a: $(AEROSPIKE)/$(SOURCE_INCL)/*.h  $(AEROSPIKE)/$(SOURCE_MAIN)/*.c
	$(MAKE) -C $(AEROSPIKE) libcitrusleaf.a prepare

###############################################################################
include $(RULES)
###############################################################################