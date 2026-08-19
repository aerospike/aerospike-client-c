# make AEROSPIKE=<PATH>
AEROSPIKE := ../../..

OS = $(shell uname)
ARCH = $(shell uname -m)
PLATFORM = $(OS)-$(ARCH)

CC_FLAGS = -std=gnu99 -g -Wall -fPIC -O3
CC_FLAGS += -fno-common -fno-strict-aliasing
CC_FLAGS += -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_GNU_SOURCE $(EXT_CFLAGS)

ifeq ($(ARCH),x86_64)
  CC_FLAGS += -march=nocona
endif

ifeq ($(OS),Darwin)
  CC_FLAGS += -D_DARWIN_UNLIMITED_SELECT

  ifneq ($(wildcard /opt/homebrew/include),)
    CC_FLAGS += -I/opt/homebrew/include
  else ifneq ($(wildcard /usr/local/opt/libevent/include),)
    CC_FLAGS += -I/usr/local/opt/libevent/include
  endif

  ifneq ($(wildcard /opt/homebrew/opt/openssl/include),)
    CC_FLAGS += -I/opt/homebrew/opt/openssl/include
  else ifneq ($(wildcard /usr/local/opt/openssl/include),)
    CC_FLAGS += -I/usr/local/opt/openssl/include
  else ifneq ($(wildcard /opt/local/include/openssl),)
    CC_FLAGS += -I/opt/local/include
  endif
else ifeq ($(OS),Linux)
  CC_FLAGS += -I/usr/local/include -rdynamic
else
  CC_FLAGS += -I/usr/local/include
endif

CC_FLAGS += -I$(AEROSPIKE)/src/include
CC_FLAGS += -I$(AEROSPIKE)/modules/common/src/include

ifeq ($(EVENT_LIB),libev)
  CC_FLAGS += -DAS_USE_LIBEV
endif

ifeq ($(EVENT_LIB),libuv)
  CC_FLAGS += -DAS_USE_LIBUV
endif

ifeq ($(EVENT_LIB),libevent)
  CC_FLAGS += -DAS_USE_LIBEVENT
endif

LD_FLAGS = $(EXT_LDFLAGS)

ifeq ($(OS),Darwin)
  ifneq ($(wildcard /opt/homebrew/lib),)
    LD_FLAGS += -L/opt/homebrew/lib
  else
    LD_FLAGS += -L/usr/local/lib
  endif

  ifneq ($(wildcard /opt/homebrew/opt/openssl/lib),)
    LD_FLAGS += -L/opt/homebrew/opt/openssl/lib
  else
    LD_FLAGS += -L/usr/local/opt/openssl/lib
  endif

  LINK_SUFFIX =
else ifeq ($(OS),FreeBSD)
  LD_FLAGS += -L/usr/local/lib
  LINK_SUFFIX = -lrt
else
  LD_FLAGS += -L/usr/local/lib
  LINK_SUFFIX = -lrt -ldl
endif

ifeq ($(EVENT_LIB),libev)
  LD_FLAGS += -lev
endif

ifeq ($(EVENT_LIB),libuv)
  LD_FLAGS += -luv
endif

ifeq ($(EVENT_LIB),libevent)
  LD_FLAGS += -levent_core -levent_pthreads
endif

LD_FLAGS += -lssl -lcrypto -lpthread -lyaml -lm -lz $(LINK_SUFFIX)

CC = cc

ROOT_BUILD_ARGS =

ifneq ($(EVENT_LIB),)
  ROOT_BUILD_ARGS += EVENT_LIB=$(EVENT_LIB)
endif

TARGET = target/probe_server_facts
OBJECTS = target/obj/probe_server_facts.o
CLIENT_LIB = $(AEROSPIKE)/target/$(PLATFORM)/lib/libaerospike.a

all: build

.PHONY: build
build: $(TARGET)

.PHONY: clean
clean:
	@rm -rf target

target:
	mkdir $@

target/obj: | target
	mkdir $@

$(CLIENT_LIB):
	$(MAKE) -C $(AEROSPIKE) $(ROOT_BUILD_ARGS) build prepare

target/obj/probe_server_facts.o: src/main/probe_server_facts.c | target/obj
	$(CC) $(CC_FLAGS) -o $@ -c $<

$(TARGET): $(OBJECTS) $(CLIENT_LIB) | target
	$(CC) -o $@ $^ $(LD_FLAGS)
