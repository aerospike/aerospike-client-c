# Citrusleaf Foundation
# Makefile for an example program

# interesting directories
DIR_INCLUDE = ../include
DIR_LIB = ../lib
DIR_OBJECT = obj
DIR_TARGET = .

# common variables. Note that march=native first supported in GCC 4.2; 
# users of older version should pick a more appropriate value
CC = gcc
ARCH_NATIVE = $(shell uname -m)
CFLAGS_NATIVE = -g -fno-common  
CFLAGS_NATIVE += -fno-strict-aliasing -rdynamic -std=gnu99 -Wall 
CFLAGS_NATIVE += -D_REENTRANT -D MARCH_$(ARCH_NATIVE)
# CFLAGS_NATIVE += -O3 -fomit-frame-pointer

# this code wants libevent-1.4, which I have installed locally. Change this to suit.
LD = gcc
LDFLAGS = $(CFLAGS_NATIVE) -L$(DIR_LIB)
LIBRARIES = -lev2citrusleaf /usr/local/lib/libevent-2.0.so.5 -lssl -lpthread -lrt

HEADERS = 
SOURCES = main.c
TARGET = example6

OBJECTS = $(SOURCES:%.c=$(DIR_OBJECT)/%.o)
DEPENDENCIES = $(OBJECTS:%.o=%.d)

.PHONY: all
all: example6

.PHONY: clean
clean:
	/bin/rm -f $(DIR_OBJECT)/* $(DIR_TARGET)/$(TARGET)

.PHONY: depclean
depclean: clean
	/bin/rm -f $(DEPENDENCIES)

.PHONY: example6
example6: $(OBJECTS)
	$(LD) $(LDFLAGS) -o $(DIR_TARGET)/$(TARGET) $(OBJECTS) $(LIBRARIES)
	chmod +x example6

-include $(DEPENDENCIES)

$(DIR_OBJECT)/%.o: %.c
	$(CC) $(CFLAGS_NATIVE) -MMD -o $@ -c -I$(DIR_INCLUDE) $<
