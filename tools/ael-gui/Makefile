###############################################################################
## ael-gui - local browser playground for AEL expressions + error details.
##
## Builds against this repo's static lib - build it at the repo root with
## `make` first (this branch includes the error-details client support the
## tool depends on).
##
##     make                # build target/ael-gui
##     make run            # build and run against 127.0.0.1:3000 test
##     make asan           # ASan/LSan build (target/ael-gui-asan)
##
## Override the cluster on the run line, e.g.:
##     make run AS_HOST=172.17.0.2 AS_NS=test
##
## Building from a checkout elsewhere, point AEROSPIKE at the client repo:
##     make AEROSPIKE=/path/to/aerospike-client-c
###############################################################################

AEROSPIKE ?= ../..

AS_HOST ?= 127.0.0.1
AS_PORT ?= 3000
AS_NS   ?= test
AS_SET  ?= ael-gui
HTTP_PORT ?= 8280

OS   = $(shell uname)
ARCH = $(shell uname -m)
PLATFORM = $(OS)-$(ARCH)

CC := cc

CC_FLAGS = -std=gnu99 -g -Wall -Wextra -fno-common -fno-strict-aliasing -O2
CC_FLAGS += -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_GNU_SOURCE
CC_FLAGS += -I$(AEROSPIKE)/target/$(PLATFORM)/include

LD_FLAGS = -lssl -lcrypto -lpthread -lyaml -lm -lz -lrt -ldl

LIBAEROSPIKE = $(AEROSPIKE)/target/$(PLATFORM)/lib/libaerospike.a

.PHONY: all build clean run asan

all: build
build: target/ael-gui

clean:
	@rm -rf target

target:
	mkdir $@

target/ael-gui: ael_gui.c $(LIBAEROSPIKE) | target
	$(CC) $(CC_FLAGS) -o $@ ael_gui.c $(LIBAEROSPIKE) $(LD_FLAGS)

# Sanitizer build of the tool itself (client lib stays stock).
target/ael-gui-asan: ael_gui.c $(LIBAEROSPIKE) | target
	$(CC) $(CC_FLAGS) -fsanitize=address -fno-omit-frame-pointer -o $@ \
		ael_gui.c $(LIBAEROSPIKE) $(LD_FLAGS)

asan: target/ael-gui-asan

run: build
	./target/ael-gui -h $(AS_HOST) -p $(AS_PORT) -n $(AS_NS) -s $(AS_SET) \
		-l $(HTTP_PORT)
