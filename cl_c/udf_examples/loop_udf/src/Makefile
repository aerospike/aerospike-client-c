# Citrusleaf Tools
# Makefile

DEPTH = ../../../..
include $(DEPTH)/make_in/Makefile.in

DIR_INCLUDE = ../include 
DIR_INCLUDE += $(DEPTH)/client/c_clients/cl_c/target/Linux-x86_64/include 

DIR_OBJECT = ../obj
DIR_TARGET = ../bin

HEADERS = loop_sproc.h 
SOURCES =  loop_sproc.c 
TARGET = loop_sproc 

CFLAGS_NATIVE += -DMARCH_x86_64

INCLUDES = $(DIR_INCLUDE:%=-I%) 
LDFLAGS += -L$(DEPTH)/client/c_clients/cl_c/target/Linux-x86_64/lib
LDFLAGS += -L$(DEPTH)/client/c_clients/cl_c/target/Linux-x86_64/deps
LIBRARIES += $(DEPTH)/client/c_clients/cl_c/target/Linux-x86_64/lib/libcitrusleaf.a
LIBRARIES += $(DEPTH)/client/c_clients/cl_c/modules/msgpack/src/.libs/libmsgpackc.a
LIBRARIES +=  -llua -lcrypto -lssl -lpthread -lrt 

OBJECTS = $(SOURCES:%.c=$(DIR_OBJECT)/%.o)
DEPENDENCIES = $(OBJECTS:%.o=%.d)

.PHONY: all
all: loop_sproc 

.PHONY: clean
clean:
	/bin/rm -f $(OBJECTS) $(DIR_TARGET)/$(TARGET) 

.PHONY: depclean
depclean: clean
	/bin/rm -f $(DEPENDENCIES) 

.PHONY: $(TARGET)
$(TARGET): $(OBJECTS)
	$(CC) $(LDFLAGS) -o $(DIR_TARGET)/$(TARGET) $(OBJECTS) $(LIBRARIES) -Wl,-rpath,/usr/local/lib

-include $(DEPENDENCIES) 

$(DIR_OBJECT)/%.o: %.c
	$(CC) $(CFLAGS_NATIVE) -MMD -o $@ -c $(INCLUDES) $<

