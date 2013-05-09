ROOT = ../../../..
include $(ROOT)/project/settings.mk
###############################################################################
##  SETTINGS                                                                 ##
###############################################################################

AEROSPIKE = $(ROOT)/aerospike

CC_FLAGS = -std=gnu99 -g -rdynamic -Wall 
CC_FLAGS += -fno-common -fno-strict-aliasing -fPIC 
CC_FLAGS += -DMARCH_$(ARCH) -D_FILE_OFFSET_BITS=64 
CC_FLAGS += -D_REENTRANT -D_GNU_SOURCE -DMEM_COUNT

LD_FLAGS = 
LD_FLAGS += -lssl -lcrypto -lpthread -lrt
LD_FLAGS += -llua
LD_FLAGS += -lm

INC_PATH += $(AEROSPIKE)/$(TARGET_INCL)

###############################################################################
##  OBJECTS                                                                  ##
###############################################################################

MANAGE_UDF 	= manage_udf.o
REC_UDF 	= rec_udf.o utils.o

###############################################################################
##  MAIN TARGETS                                                             ##
###############################################################################

all: build

.PHONY: build
build: manage_udf rec_udf

.PHONY: build-clean
build-clean:
	@rm -rf $(TARGET_BIN)
	@rm -rf $(TARGET_LIB)

.PHONY: manage_udf rec_udf
manage_udf: $(TARGET_BIN)/manage_udf
rec_udf: 	$(TARGET_BIN)/rec_udf

.PHONY: run
run: 
	./$(TARGET_BIN)/rec_udf
	./$(TARGET_BIN)/manage_udf

###############################################################################
##  BUILD TARGETS                                                            ##
###############################################################################

$(TARGET_OBJ)/%.o: %.c $(AEROSPIKE)/$(TARGET_LIB)/libcitrusleaf.a 
	$(object)

$(TARGET_BIN)/manage_udf: $(MANAGE_UDF:%=$(TARGET_OBJ)/%)
	$(executable) $(AEROSPIKE)/$(TARGET_LIB)/libcitrusleaf.a

$(TARGET_BIN)/rec_udf: $(REC_UDF:%=$(TARGET_OBJ)/%)
	$(executable) $(AEROSPIKE)/$(TARGET_LIB)/libcitrusleaf.a

###############################################################################
##  BUILD DEPENDENCIES                                                       ##
###############################################################################

$(AEROSPIKE)/$(TARGET_LIB)/libcitrusleaf.a: $(AEROSPIKE)/$(SOURCE_INCL)/citrusleaf/*.h  $(AEROSPIKE)/$(SOURCE_MAIN)/*.c
	$(MAKE) -C $(AEROSPIKE) libcitrusleaf.a prepare

###############################################################################
include $(ROOT)/project/rules.mk