###############################################################################
##  SETTINGS                                                                 ##
###############################################################################

AS_HOST := 127.0.0.1
AS_PORT := 3000

OS = $(shell uname)
ARCH = $(shell arch)
PLATFORM = $(OS)-$(ARCH)

CFLAGS = -std=gnu99 -g -Wall -fPIC
CFLAGS += -fno-common -fno-strict-aliasing
CFLAGS += -march=nocona -DMARCH_$(ARCH)
CFLAGS += -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_GNU_SOURCE

ifeq ($(OS),Darwin)
CFLAGS += -D_DARWIN_UNLIMITED_SELECT
else
CFLAGS += -rdynamic
endif

CFLAGS += -I$(AEROSPIKE)/target/$(PLATFORM)/include
CFLAGS += -I../../utils/src/include

LDFLAGS = 
LDFLAGS += -lssl -lcrypto -lpthread

ifeq ($(OS),Darwin)
LDFLAGS += -L/usr/local/lib
else
LDFLAGS += -lrt
endif

LDFLAGS += -llua
LDFLAGS += -lm

ifeq ($(OS),Darwin)
LDFLAGS += -lz
endif

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
	gcc $(CFLAGS) -o $@ -c $^

target/obj/%.o: src/main/%.c | target/obj
	gcc $(CFLAGS) -o $@ -c $^

target/example: $(addprefix target/obj/,$(OBJECTS)) | target
	gcc -o $@ $^ $(AEROSPIKE)/target/$(PLATFORM)/lib/libaerospike.a $(LDFLAGS)

.PHONY: run
run: build
	./target/example -h $(AS_HOST) -p $(AS_PORT)

.PHONY: valgrind
valgrind: build
	valgrind --tool=memcheck --leak-check=yes --show-reachable=yes --num-callers=20 --track-fds=yes -v ./target/example

