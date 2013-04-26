# Citrusleaf Tools
# Makefile:  Stolen from rec_udf for use with Large SET Objects (LSET)

# DEPTH = ../../../../../..
# include $(DEPTH)/make_in/Makefile.in

CC_FLAGS = -std=gnu99 -g -rdynamic -Wall
CC_FLAGS += -fno-common -fno-strict-aliasing -fPIC
CC_FLAGS += -DMARCH_$(ARCH) -D_FILE_OFFSET_BITS=64
CC_FLAGS += -D_REENTRANT -D_GNU_SOURCE -DMEM_COUNT

LD_FLAGS =
LD_FLAGS += -lssl -lcrypto -lpthread -lrt
LD_FLAGS += -llua
LD_FLAGS += -lm

DIR_OBJECT = ../obj
# DIR_TARGET = ../bin
DIR_TARGET = ../
DIR_LIB = ../../../../target/Linux-x86_64/lib

SOURCES       = test_log.c test_config.c test_counter.c test_main.c
SOURCES      += lset.c run_tests.c
SOURCE_HEADERS = test_log.h test_config.h test_counter.h test.h
TARGET        = lset

INCLUDES = -I../include/ -I../../../../target/Linux-x86_64/include/ 
INCLUDES +=  -I../../../../target/Linux-x86_64/include/citrusleaf/

LIBRARIES = $(DIR_LIB)/libcitrusleaf.a  -lssl -lcrypto -lpthread -lrt
LIBRARIES += -llua
LIBRARIES += -lm

# LDFLAGS += -L$(DIR_LIB)
# LDFLAGS = $(LD_FLAGS)
CFLAGS = $(CC_FLAGS)

OBJECTS = $(SOURCES:%.c=$(DIR_OBJECT)/%.o)
DEPENDENCIES = $(OBJECTS:%.o=%.d)

.PHONY: all
all: $(TARGET)

.PHONY: clean
clean:
	/bin/rm -f $(OBJECTS) $(DIR_TARGET)/$(TARGET)

.PHONY: depclean
depclean: clean
	/bin/rm -f $(DEPENDENCIES) 

.PHONY: $(TARGET)
$(TARGET): $(OBJECTS)
	$(CC) $(LDFLAGS) -o $(DIR_TARGET)/$(TARGET) $(OBJECTS) $(LIBRARIES)

# -include $(DEPENDENCIES) 

$(DIR_OBJECT)/%.o: %.c
	$(CC) $(CFLAGS) -MMD -o $@ -c $(INCLUDES) $<
