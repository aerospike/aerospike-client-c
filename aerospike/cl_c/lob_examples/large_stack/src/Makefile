# Citrusleaf Tools
# Makefile:  Stolen from rec_udf for use with Large Stack Objects (LSTACK)

DEPTH = ../../../../../..
include $(DEPTH)/make_in/Makefile.in

DIR_OBJECT = ../obj
# DIR_TARGET = ../bin
DIR_TARGET = ../
DIR_LIB = ../../../target/Linux-x86_64/lib

SOURCES       = test_log.c test_config.c test_counter.c test_main.c
SOURCES      += lstack.c run_tests.c
SOURCE_HEADERS = test_log.h test_config.h test_counter.h test.h
TARGET        = lstack

INCLUDES = -I../include/ -I../../../target/Linux-x86_64/include/ 
INCLUDES +=  -I../../../target/Linux-x86_64/include/citrusleaf/

LIBRARIES = $(DIR_LIB)/libcitrusleaf.a  -lssl -lcrypto -lpthread -lrt
ifeq (${MR_LANG},LUA)
LIBRARIES += -llua
endif
LIBRARIES += -lm

LDFLAGS += -L$(DEPTH)/lib/ -L$(DEPTH)/shared/lib/ 

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

-include $(DEPENDENCIES) 

$(DIR_OBJECT)/%.o: %.c
	$(CC) $(CFLAGS_NATIVE) -MMD -o $@ -c $(INCLUDES) $<
