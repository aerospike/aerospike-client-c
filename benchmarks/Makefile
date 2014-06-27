###############################################################################
##  SETTINGS                                                                 ##
###############################################################################

AEROSPIKE := ..
AS_HOST := 127.0.0.1
AS_PORT := 3000

OS = $(shell uname)
ARCH = $(shell uname -m)
PLATFORM = $(OS)-$(ARCH)

CFLAGS = -std=gnu99 -g -Wall -fPIC -O3
CFLAGS += -fno-common -fno-strict-aliasing
CFLAGS += -march=nocona -DMARCH_$(ARCH)
CFLAGS += -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_GNU_SOURCE

ifeq ($(OS),Darwin)
CFLAGS += -D_DARWIN_UNLIMITED_SELECT
else
CFLAGS += -rdynamic
endif

CFLAGS += -I$(AEROSPIKE)/target/$(PLATFORM)/include -I$(AEROSPIKE)/target/$(PLATFORM)/include/ck

LDFLAGS = 
LDFLAGS += -lssl -lcrypto -lpthread

ifeq ($(OS),Darwin)
LDFLAGS += -L/usr/local/lib
else
LDFLAGS += -lrt
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

LDFLAGS += -lm

ifeq ($(OS),Darwin)
LDFLAGS += -lz
endif

ifeq ($(OS),Darwin)
CC = clang
else
CC = gcc
endif

###############################################################################
##  OBJECTS                                                                  ##
###############################################################################

OBJECTS = benchmark.o latency.o linear.o main.o random.o record.o

###############################################################################
##  MAIN TARGETS                                                             ##
###############################################################################

all: build

.PHONY: build
build: target/benchmarks

.PHONY: clean
clean:
	@rm -rf target

target:
	mkdir $@

target/obj: | target
	mkdir $@

target/obj/%.o: src/main/%.c | target/obj
	$(CC) $(CFLAGS) -o $@ -c $^

target/benchmarks: $(addprefix target/obj/,$(OBJECTS)) | target
	$(CC) -o $@ $^ $(AEROSPIKE)/target/$(PLATFORM)/lib/libaerospike.a $(LDFLAGS)

.PHONY: run
run: build
	./target/benchmarks -h $(AS_HOST) -p $(AS_PORT)

.PHONY: valgrind
valgrind: build
	valgrind --tool=memcheck --leak-check=yes --show-reachable=yes --num-callers=20 --track-fds=yes -v ./target/benchmarks
