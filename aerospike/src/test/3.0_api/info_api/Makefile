ROOT = ../../../../../
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

INC_PATH = $(AEROSPIKE)/$(TARGET_INCL)

###############################################################################
##  OBJECTS                                                                  ##
###############################################################################

OBJECTS = info.o

###############################################################################
##  MAIN TARGETS                                                             ##
###############################################################################

all: build

.PHONY: build
build: info

.PHONY: build-clean
build-clean:
	@rm -rf $(TARGET_BIN)
	@rm -rf $(TARGET_LIB)

.PHONY: run
run: $(TARGET_BIN)/info
	$(TARGET_BIN)/info

.PHONY: info
info: $(TARGET_BIN)/info

###############################################################################
##  BUILD TARGETS                                                            ##
###############################################################################

$(TARGET_OBJ)/%.o: %.c $(AEROSPIKE)/$(TARGET_LIB)/libcitrusleaf.a 
	$(object)

$(TARGET_BIN)/info: $(OBJECTS:%=$(TARGET_OBJ)/%) 
	$(executable) $(AEROSPIKE)/$(TARGET_LIB)/libcitrusleaf.a

###############################################################################
##  BUILD DEPENDENCIES                                                       ##
###############################################################################

$(AEROSPIKE)/$(TARGET_LIB)/libcitrusleaf.a: $(AEROSPIKE)/$(SOURCE_INCL)/citrusleaf/*.h  $(AEROSPIKE)/$(SOURCE_MAIN)/*.c
	$(MAKE) -C $(AEROSPIKE) libcitrusleaf.a prepare

###############################################################################
include $(ROOT)/project/rules.mk
