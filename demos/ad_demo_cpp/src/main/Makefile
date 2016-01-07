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

ifeq ($(EVENT_LIB),libev)
  CFLAGS += -DAS_USE_LIBEV -I/usr/local/include
endif

ifeq ($(EVENT_LIB),libuv)
  CFLAGS += -DAS_USE_LIBUV -I/usr/local/include
endif

LDFLAGS = $(LIBRARY_PATH)/libaerospike.a

ifeq ($(EVENT_LIB),libev)
  LDFLAGS += -L/usr/local/lib -lev
endif

ifeq ($(EVENT_LIB),libuv)
  LDFLAGS += -L/usr/local/lib -luv
endif

ifeq ($(OS),Darwin)
CC = clang++
LDFLAGS += -lssl -lcrypto -lpthread -L/usr/local/lib -lm -lz
else
CC = g++
LDFLAGS += -lssl -lcrypto -lpthread -lrt -lm -lz
endif

LUA_CPATH += $(or \
    $(wildcard /usr/include/lua-5.1), \
    $(wildcard /usr/include/lua5.1))

ifeq ($(OS),Darwin)
LUA_LIBPATH += $(or \
    $(wildcard /usr/local/lib/liblua.5.1.dylib), \
    $(wildcard /usr/local/lib/liblua.5.1.a), \
    $(wildcard /usr/local/lib/liblua.dylib), \
    $(wildcard /usr/local/lib/liblua.a), \
	$(error Cannot find liblua 5.1) \
    )
LUA_LIBDIR = $(dir LUA_LIBPATH)
LUA_LIB = $(patsubst lib%,%,$(basename $(notdir $(LUA_LIBPATH))))
else
# Linux
LUA_LIBPATH += $(or \
    $(wildcard /usr/lib/liblua5.1.so), \
    $(wildcard /usr/lib/liblua5.1.a), \
    $(wildcard /usr/lib/x86_64-linux-gnu/liblua5.1.so), \
    $(wildcard /usr/lib/x86_64-linux-gnu/liblua5.1.a), \
    $(wildcard /usr/lib64/liblua-5.1.so), \
    $(wildcard /usr/lib64/liblua-5.1.a), \
    $(wildcard /usr/lib/liblua.so), \
    $(wildcard /usr/lib/liblua.a), \
	$(error Cannot find liblua 5.1) \
    )
LUA_LIBDIR = $(dir LUA_LIBPATH)
LUA_LIB = $(patsubst lib%,%, $(basename $(notdir $(LUA_LIBPATH))))
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
