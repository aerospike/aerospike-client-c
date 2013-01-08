# Citrusleaf Foundation
# Makefile for an example program

# interesting directories
DIR_INCLUDE = ../../../target/Linux-x86_64/include
DIR_LIB = ../../../target/Linux-x86_64/lib
DIR_OBJECT = ../obj
DIR_TARGET = ..

# common variables. Note that march=native first supported in GCC 4.2; 
# users of older version should pick a more appropriate value
CC = gcc
ARCH_NATIVE = $(shell uname -m)
CFLAGS_NATIVE = -std=gnu99 -g -O3 -fno-common -fno-strict-aliasing -rdynamic -fPIC -Wall -DMARCH_$(ARCH) -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_GNU_SOURCE
LDFLAGS = 

LD = gcc
LDFLAGS = $(CFLAGS_NATIVE) -L$(DIR_LIB) 
LIBRARIES = $(DIR_LIB)/libcitrusleaf.a -lssl -lpthread -lrt -lcrypto -lz

HEADERS = loop_udf.h 
SOURCES = loop_udf.c 
TARGET = loop_udf

OBJECTS = $(SOURCES:%.c=$(DIR_OBJECT)/%.o)
DEPENDENCIES = $(OBJECTS:%.o=%.d)

.PHONY: all
all: $(TARGET)

.PHONY: clean
clean:
	/bin/rm -f $(DIR_TARGET)/$(TARGET) $(DIR_OBJECT)/*.d $(DIR_OBJECT)/*.o

.PHONY: depclean
depclean: clean
	/bin/rm -f $(DEPENDENCIES)

.PHONY: $(TARGET)
$(TARGET): $(OBJECTS)
	$(LD) $(LDFLAGS) -o $(DIR_TARGET)/$(TARGET) $(OBJECTS) $(LIBRARIES)
	chmod +x $(DIR_TARGET)/$(TARGET)

-include $(DEPENDENCIES)

$(DIR_OBJECT)/%.o: %.c | prepare
	$(CC) $(CFLAGS_NATIVE) -MMD -o $@ -c -I$(DIR_INCLUDE) $<

prepare: 
	mkdir -p $(DIR_OBJECT)
