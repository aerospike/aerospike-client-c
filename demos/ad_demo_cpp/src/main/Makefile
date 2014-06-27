OS = $(shell uname)
ARCH = $(shell uname -m)
PLATFORM = $(OS)-$(ARCH)

PREFIX = ../../../../target/$(PLATFORM)

LIBRARY_PATH = $(PREFIX)/lib
INCLUDE_PATH = $(PREFIX)/include

CFLAGS = -I$(INCLUDE_PATH)
CFLAGS += -std=c++0x -g -rdynamic -Wall 
CFLAGS += -fno-common -fno-strict-aliasing -fPIC 
CFLAGS += -DMARCH_$(ARCH) -D_FILE_OFFSET_BITS=64 
CFLAGS += -D_REENTRANT -D_GNU_SOURCE

ifeq ($(OS),Darwin)
CC = clang++
LDFLAGS = $(LIBRARY_PATH)/libaerospike.a -lssl -lcrypto -lpthread -L/usr/local/lib -lm -lz
else
CC = g++
LDFLAGS = $(LIBRARY_PATH)/libaerospike.a -lssl -lcrypto -lpthread -lrt -lm -lz
endif

LUA_CPATH += $(or \
    $(wildcard /usr/include/lua-5.1), \
    $(wildcard /usr/include/lua5.1))

ifeq ($(OS),Darwin)
LUA_LIBPATH += $(or \
    $(wildcard /usr/local/lib/liblua.5.1.dylib), \
	$(error Cannot find liblua 5.1) \
    )
LUA_LIBDIR = $(dir LUA_LIBPATH)
LUA_LIB = $(patsubst lib%.dylib,%,$(notdir $(LUA_LIBPATH)))
else
# Linux
LUA_LIBPATH += $(or \
    $(wildcard /usr/lib/liblua5.1.so), \
    $(wildcard /usr/lib/x86_64-linux-gnu/liblua5.1.so), \
    $(wildcard /usr/lib64/liblua-5.1.so), \
    $(wildcard /usr/lib/liblua.so), \
    $(wildcard /usr/lib/liblua.so), \
	$(error Cannot find liblua 5.1) \
    )
LUA_LIBDIR = $(dir LUA_LIBPATH)
LUA_LIB = $(patsubst lib%.so,%,$(notdir $(LUA_LIBPATH)))
endif

CFLAGS += $(LUA_CPATH:%:-I%)
LDFLAGS += -L$(LUA_LIBDIR) -l$(LUA_LIB)

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
