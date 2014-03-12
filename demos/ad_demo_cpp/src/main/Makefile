CC = g++

OS = $(shell uname)
ARCH = $(shell arch)
PLATFORM = $(OS)-$(ARCH)

PREFIX = ../../../../target/$(PLATFORM)

LIBRARY_PATH = $(PREFIX)/lib
INCLUDE_PATH = $(PREFIX)/include

CFLAGS = -I$(INCLUDE_PATH)
CFLAGS += -std=c++0x -g -rdynamic -Wall 
CFLAGS += -fno-common -fno-strict-aliasing -fPIC 
CFLAGS += -DMARCH_$(ARCH) -D_FILE_OFFSET_BITS=64 
CFLAGS += -D_REENTRANT -D_GNU_SOURCE
CFLAGS += -I$(MSGPACK_PATH)/src

ifeq ($(OS),Darwin)
LDFLAGS = $(LIBRARY_PATH)/libaerospike.a -lssl -lcrypto -lpthread -L/usr/local/lib -llua -lm -lz
else
LDFLAGS = $(LIBRARY_PATH)/libaerospike.a -lssl -lcrypto -lpthread -lrt -llua -lm -lz
endif

../../bin/ad_udf: ad_udf.cpp
ifeq ($(wildcard $(LIBRARY_PATH)/libaerospike.a),)
	$(warning Error: )
	$(warning Error: 'libaerospike.a' not found in '$(LIBRARY_PATH)'.)
	$(warning Error: )
	$(warning Error: PREFIX = $(PREFIX))
	$(warning Error: LIBRARY_PATH = $(LIBRARY_PATH))
	$(warning Error: )
	$(warning Error: 'PREFIX' must be set to a valid path.)
	$(warning Error: 'PREFIX' may be definined during make:)
	$(warning Error:     $ make PREFIX=<PATH>)
	$(warning Error: )
	$(error Not able to find required libraries.)
endif
ifeq ($(wildcard $(INCLUDE_PATH)/aerospike/aerospike.h),)
	$(warning Error: )
	$(warning Error: 'aerospike/aerospike.h' not found in '$(INCLUDE_PATH)'.)
	$(warning Error: )
	$(warning Error: PREFIX = $(PREFIX))
	$(warning Error: INCLUDE_PATH = $(INCLUDE_PATH))
	$(warning Error: )
	$(warning Error: 'PREFIX' must be set to a valid path.)
	$(warning Error: 'PREFIX' may be definined during make:)
	$(warning Error:     $ make PREFIX=<PATH>)
	$(warning Error: )
	$(error Not able to find required header files.)
endif
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)
