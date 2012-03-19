# Citrusleaf Foundation
# Makefile for an example program

# interesting directories
DIR_INCLUDE = ../../include
DIR_LIB = ../../lib
DIR_OBJECT = ../obj
DIR_TARGET = ../bin

# common variables. Note that march=native first supported in GCC 4.2; 
# users of older version should pick a more appropriate value
CC = gcc
ARCH_NATIVE = $(shell uname -m)
#CFLAGS_NATIVE = -g -march=native -fno-common -fomit-frame-pointer 
CFLAGS_NATIVE = -g -fno-common -fomit-frame-pointer 
CFLAGS_NATIVE += -fno-strict-aliasing -rdynamic -std=gnu99 -Wall 
CFLAGS_NATIVE += -D_REENTRANT -D MARCH_$(ARCH_NATIVE)
# CFLAGS_NATIVE += -O3

LD = gcc
LDFLAGS = $(CFLAGS_NATIVE) -L$(DIR_LIB)
LIBRARIES = -lcitrusleaf -lssl -lpthread -lrt

HEADERS = 
SOURCES = exists_example.c
TARGET = exists_example

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

$(DIR_OBJECT)/%.o: %.c
	$(CC) $(CFLAGS_NATIVE) -MMD -o $@ -c -I$(DIR_INCLUDE) $<
