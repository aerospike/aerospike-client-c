REPO = $(shell git rev-parse --show-toplevel)

###############################################################################
include $(REPO)/project/settings.makefile
###############################################################################

###############################################################################
##  FLAGS                                                                    ##
###############################################################################

CLIENT = $(REPO)/aerospike/
CLIENT_INC = $(CLIENT)/$(TARGET_INCL)
CLIENT_LIB = $(CLIENT)/$(TARGET_LIB)/libcitrusleaf.a

# To Override Optimization Level, run make with the "O" variable set.
# 	make O=0

O=4

CFLAGS = -O$(O)

CC_FLAGS = -std=gnu99 -g -rdynamic -Wall 
CC_FLAGS += -fno-common -fno-strict-aliasing -fPIC 
CC_FLAGS += -DMARCH_$(ARCH) -D_FILE_OFFSET_BITS=64 
CC_FLAGS += -D_REENTRANT -D_GNU_SOURCE -DMEM_COUNT

LD_FLAGS = -llua -lm -lssl -lcrypto -lpthread -lrt

INC_PATH += $(CLIENT_INC)


###############################################################################
##  OBJECTS                                                                  ##
###############################################################################

MANAGE_UDF 	= manage_udf.o
REC_UDF 	= rec_udf.o utils.o

###############################################################################
##  MAIN TARGETS                                                             ##
###############################################################################

all: build run

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
	# ./$(TARGET_BIN)/rec_udf
	./$(TARGET_BIN)/manage_udf

###############################################################################
##  BUILD TARGETS                                                            ##
###############################################################################

$(TARGET_BIN)/manage_udf: $(MANAGE_UDF:%=$(TARGET_OBJ)/%) $(CLIENT_LIB)
	$(executable) 

$(TARGET_BIN)/rec_udf: $(REC_UDF:%=$(TARGET_OBJ)/%) $(CLIENT_LIB)
	$(executable) 

$(CLIENT_LIB):
	$(MAKE) -C $(REPO)/aerospike

###############################################################################
include $(REPO)/project/rules.makefile
###############################################################################