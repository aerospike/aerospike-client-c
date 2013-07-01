ROOT = ../../../..
AEROSPIKE = $(ROOT)/aerospike
###############################################################################
##  SETTINGS                                                                 ##
###############################################################################

CFLAGS = -std=gnu99 -g -rdynamic -Wall 
CFLAGS += -fno-common -fno-strict-aliasing -fPIC 
CFLAGS += -DMARCH_$(ARCH) -D_FILE_OFFSET_BITS=64 
CFLAGS += -D_REENTRANT -D_GNU_SOURCE -DMEM_COUNT
CFLAGS += -I$(AEROSPIKE)/src/include
CFLAGS += -I$(ROOT)/aerospike-common/src/include
CFLAGS += -I../../utils/src/include
CFLAGS += -I$(ROOT)/citrusleaf-base/src/include

LDFLAGS = 
LDFLAGS += -lssl -lcrypto -lpthread -lrt
LDFLAGS += -llua
LDFLAGS += -lm

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

target/bin: | target
	mkdir $@

target/obj: | target
	mkdir $@

target/obj/example_utils.o: ../../utils/src/main/example_utils.c | target/obj
	gcc $(CFLAGS) -o $@ -c $^

target/obj/%.o: src/main/%.c | target/obj
	gcc $(CFLAGS) -o $@ -c $^

target/example: $(addprefix target/obj/,$(OBJECTS)) | target/bin
	gcc $(LDFLAGS) -o $@ $^ $(AEROSPIKE)/target/Linux-x86_64/lib/libaerospike.a

.PHONY: run
run: build
	./target/example
all: build

.PHONY: build
build: target/example

.PHONY: build-clean
build-clean:
	@rm -rf $(TARGET_BIN)
	@rm -rf $(TARGET_LIB)

.PHONY: run
run: $(TARGET_BIN)/example

.PHONY: example
example: $(TARGET_BIN)/example

