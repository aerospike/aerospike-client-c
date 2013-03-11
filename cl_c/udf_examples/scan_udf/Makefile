# Citrusleaf Tools
# Makefile

DIR_OBJECT = ./obj
DIR_TARGET = .

HEADERS     = scan_udf.h
SOURCES     = scan_udf.c
TARGET      = scan_udf

INCLUDES = -I../include/ -I../../target/Linux-x86_64/include/
INCLUDES +=  -I../../target/Linux-x86_64/include/citrusleaf/

LIBRARIES = -lcitrusleaf  -lssl -lcrypto -lpthread -lrt

AS_CFLAGS = -D_FILE_OFFSET_BITS=64 -std=gnu99 -D_REENTRANT -D_GNU_SOURCE
CFLAGS_NATIVE = -g  -fno-common -fno-strict-aliasing -rdynamic  -Wall -Wredundant-decls $(AS_CFLAGS)

ifeq (${MR_LANG},LUA)
LIBRARIES += -llua
endif
LIBRARIES += -lm

LDFLAGS += -L$(DEPTH)/lib/ -L$(DEPTH)/shared/lib/ 

OBJECTS = $(SOURCES:%.c=$(DIR_OBJECT)/%.o)
DEPENDENCIES = $(OBJECTS:%.o=%.d)

.PHONY: all
all: scan_udf

.PHONY: clean
clean:
	/bin/rm -f $(OBJECTS) $(DIR_TARGET)/$(TARGET)

.PHONY: depclean
depclean: clean
	/bin/rm -f $(DEPENDENCIES) 

.PHONY: $(TARGET)
$(TARGET): $(OBJECTS)
	$(CC) $(LDFLAGS) -o $(DIR_TARGET)/$(TARGET) $(OBJECTS) $(LIBRARIES)

-include $(DEPENDENCIES) 

$(DIR_OBJECT)/%.o: %.c
	$(CC) $(CFLAGS_NATIVE) -MMD -o $@ -c $(INCLUDES) $<

