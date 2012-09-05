# Citrusleaf Foundation
# Makefile for an example program

# interesting directories
DIR_INCLUDE = ../include
DIR_CF_INCLUDE = ../../cf_base/include
DIR_LIB = ../lib
DIR_CF_LIB = ../../cf_base/lib
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
LDFLAGS = $(CFLAGS_NATIVE) -L$(DIR_LIB) -L$(DIR_CF_LIB)
#LIBRARIES = -lev2citrusleaf  -levent -levent_openssl -lssl -lpthread -lrt
#LIBRARIES = -lev2citrusleaf /usr/local/lib/libevent-2.0.so.5.1.3 -lssl -lpthread -lrt
LIBRARIES = -lev2citrusleaf /usr/local/lib/libevent-2.0.so.5 -lssl -lcrypto -lpthread -lrt

HEADERS = 
SOURCES = main.c
TARGET = example

OBJECTS = $(SOURCES:%.c=$(DIR_OBJECT)/%.o)
DEPENDENCIES = $(OBJECTS:%.o=%.d)

.PHONY: all
all: example

.PHONY: clean
clean:
	/bin/rm -f $(DIR_OBJECT)/* $(DIR_TARGET)/$(TARGET)

.PHONY: depclean
depclean: clean
	/bin/rm -f $(DEPENDENCIES)

.PHONY: example
example: $(OBJECTS)
	$(LD) $(LDFLAGS) -o $(DIR_TARGET)/$(TARGET) $(OBJECTS) $(LIBRARIES)
	chmod +x example

-include $(DEPENDENCIES)

$(DIR_OBJECT)/%.o: %.c
	$(CC) $(CFLAGS_NATIVE) -MMD -o $@ -c -I$(DIR_INCLUDE) -I$(DIR_CF_INCLUDE) $<
