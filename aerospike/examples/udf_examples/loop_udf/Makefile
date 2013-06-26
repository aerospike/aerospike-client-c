ROOT = ../../../..
include $(ROOT)/project/settings.mk
###############################################################################
##  SETTINGS                                                                 ##
###############################################################################

AEROSPIKE = $(ROOT)/aerospike

CC_FLAGS = -std=gnu99  -Wall

LD_FLAGS = 
LD_FLAGS += -lssl -lcrypto -lpthread -lrt
LD_FLAGS += -llua
LD_FLAGS += -lm

#INC_PATH = $(AEROSPIKE)/$(TARGET_INCL)
INC_PATH = $(AEROSPIKE)/$(SOURCE_INCL)
INC_PATH += $(ROOT)/aerospike-common/$(TARGET_INCL)/
INC_PATH += $(ROOT)/citrusleaf-base/$(TARGET_INCL)/


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

$(TARGET_OBJ)/%.o: %.c $(AEROSPIKE)/$(TARGET_LIB)/libaerospike.a 
	$(object)

$(TARGET_BIN)/example: $(OBJECTS:%=$(TARGET_OBJ)/%) 
	$(executable) $(AEROSPIKE)/$(TARGET_LIB)/libaerospike.a

###############################################################################
##  BUILD DEPENDENCIES                                                       ##
###############################################################################

$(AEROSPIKE)/$(TARGET_LIB)/libaerospike.a: $(AEROSPIKE)/$(SOURCE_INCL)/citrusleaf/*.h  $(AEROSPIKE)/$(SOURCE_MAIN)/citrusleaf/*.c
	$(MAKE) -C $(AEROSPIKE) libaerospike.a prepare

###############################################################################
include $(ROOT)/project/rules.mk
