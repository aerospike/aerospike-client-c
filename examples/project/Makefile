###############################################################################
##  SETTINGS                                                                 ##
###############################################################################

AS_HOST := 127.0.0.1
AS_PORT := 3000
AS_ARGS := -h $(AS_HOST) -p $(AS_PORT)

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
    # Mac new homebrew external include path
    CC_FLAGS += -I/opt/homebrew/include
  else ifneq ($(wildcard /usr/local/opt/libevent/include),)
  	# Mac old homebrew libevent include path
    CC_FLAGS += -I/usr/local/opt/libevent/include
  endif

  ifneq ($(wildcard /opt/homebrew/opt/openssl/include),)
    # Mac new homebrew openssl include path
    CC_FLAGS += -I/opt/homebrew/opt/openssl/include
  else ifneq ($(wildcard /usr/local/opt/openssl/include),)
    # Mac old homebrew openssl include path
    CC_FLAGS += -I/usr/local/opt/openssl/include
  else ifneq ($(wildcard /opt/local/include/openssl),)
    # macports openssl include path
    CC_FLAGS += -I/opt/local/include
  endif
else ifeq ($(OS),Linux)
  CC_FLAGS += -I/usr/local/include -rdynamic
else
  CC_FLAGS += -I/usr/local/include
endif

CC_FLAGS += -I$(AEROSPIKE)/target/$(PLATFORM)/include
CC_FLAGS += -I../../utils/src/include

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
    # Mac new homebrew external lib path
    LD_FLAGS += -L/opt/homebrew/lib
  else
    # Mac old homebrew external lib path
    LD_FLAGS += -L/usr/local/lib
  
    ifeq ($(EVENT_LIB),libevent)
      LD_FLAGS += -L/usr/local/opt/libevent/lib
    endif
  endif

  ifneq ($(wildcard /opt/homebrew/opt/openssl/lib),)
    # Mac new homebrew openssl lib path
    LD_FLAGS += -L/opt/homebrew/opt/openssl/lib
  else
    # Mac old homebrew openssl lib path
    LD_FLAGS += -L/usr/local/opt/openssl/lib
  endif
  
  ifeq ($(USE_LUAJIT),1)
    LD_FLAGS += -pagezero_size 10000 -image_base 100000000
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

LD_FLAGS += -lssl -lcrypto $(LIB_LUA) -lpthread -lm -lz $(LINK_SUFFIX)

ifeq ($(OS),Linux)
  LD_FLAGS += -lrt -ldl
else ifeq ($(OS),FreeBSD)
  LD_FLAGS += -lrt
endif

# Use the Lua submodule?  [By default, yes.]
USE_LUAMOD = 1

# Use LuaJIT instead of Lua?  [By default, no.]
USE_LUAJIT = 0

# Permit easy overriding of the default.
ifeq ($(USE_LUAJIT),1)
  USE_LUAMOD = 0
endif

ifeq ($(and $(USE_LUAMOD:0=),$(USE_LUAJIT:0=)),1)
  $(error Only at most one of USE_LUAMOD or USE_LUAJIT may be enabled (i.e., set to 1.))
endif

ifeq ($(USE_LUAJIT),1)
  ifeq ($(OS),Darwin)
    LD_FLAGS += -pagezero_size 10000 -image_base 100000000
  endif
else
  ifeq ($(USE_LUAMOD),0)
    # Find where the Lua development package is installed in the build environment.
    ifeq ($(OS),Darwin)
      LUA_LIBPATH = $(or \
	$(wildcard /usr/local/lib/liblua.5.1.dylib), \
	$(wildcard /usr/local/lib/liblua.5.1.a), \
	$(wildcard /usr/local/lib/liblua.dylib), \
	$(wildcard /usr/local/lib/liblua.a), \
	  $(error Cannot find liblua 5.1))
      LUA_LIBDIR = $(dir $(LUA_LIBPATH))
      LUA_LIB = $(patsubst lib%,%,$(basename $(notdir $(LUA_LIBPATH))))
    else
      # Linux
      LUA_LIBPATH = $(or \
	$(wildcard /usr/lib/liblua5.1.so), \
	$(wildcard /usr/lib/liblua5.1.a), \
	$(wildcard /usr/lib/x86_64-linux-gnu/liblua5.1.so), \
	$(wildcard /usr/lib/x86_64-linux-gnu/liblua5.1.a), \
	$(wildcard /usr/lib64/liblua-5.1.so), \
	$(wildcard /usr/lib64/liblua-5.1.a), \
	$(wildcard /usr/lib/liblua.so), \
	$(wildcard /usr/lib/liblua.a), \
	  $(error Cannot find liblua 5.1))
      LUA_LIBDIR = $(dir $(LUA_LIBPATH))
      LUA_LIB = $(patsubst lib%,%,$(basename $(notdir $(LUA_LIBPATH))))
    endif
    LD_FLAGS += -L$(LUA_LIBDIR) -l$(LUA_LIB)
  endif
endif

LD_FLAGS += -lm -lz
CC = cc

###############################################################################
##  OBJECTS                                                                  ##
###############################################################################

OBJECTS = example.o example_utils.o

###############################################################################
##  MAIN TARGETS                                                             ##
###############################################################################

all: build

.PHONY: build
build: target/example

.PHONY: clean
clean:
	@rm -rf target

target:
	mkdir $@

target/obj: | target
	mkdir $@

target/obj/example_utils.o: ../../utils/src/main/example_utils.c | target/obj
	$(CC) $(CC_FLAGS) -o $@ -c $^

target/obj/%.o: src/main/%.c | target/obj
	$(CC) $(CC_FLAGS) -o $@ -c $^

target/example: $(addprefix target/obj/,$(OBJECTS)) $(AEROSPIKE)/target/$(PLATFORM)/lib/libaerospike.a | target
	$(CC) -o $@ $^ $(LD_FLAGS)

.PHONY: run
run: build
	./target/example $(AS_ARGS)

.PHONY: valgrind
valgrind: build
	valgrind --tool=memcheck --leak-check=yes --show-reachable=yes --num-callers=20 --track-fds=yes -v ./target/example

