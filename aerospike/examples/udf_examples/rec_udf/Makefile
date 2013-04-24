# Citrusleaf Tools
# Makefile

CLIENT_PATH = ../../../target/Linux-x86_64

DIR_OBJECT = ./obj
DIR_TARGET = .
DIR_LIB = $(CLIENT_PATH)/lib

SOURCES       = rec_udf.c utils.c 
TARGET        = rec_udf

MANAGE_SOURCES = manage_udf.c
MANAGE_TARGET  = manage_udf

INCLUDES = -I$(CLIENT_PATH)/include/
LIBRARIES = $(DIR_LIB)/libcitrusleaf.a  -lssl -lcrypto -lpthread -lrt

AS_CFLAGS = -D_FILE_OFFSET_BITS=64 -std=gnu99 -D_REENTRANT -D_GNU_SOURCE
CFLAGS_NATIVE = -g  -fno-common -fno-strict-aliasing -rdynamic  -Wall -Wredundant-decls $(AS_CFLAGS)

ifeq (${MR_LANG},LUA)
LIBRARIES += -llua
endif
LIBRARIES += -lm

LDFLAGS += -L$(DEPTH)/lib/ -L$(DEPTH)/shared/lib/ 

OBJECTS = $(SOURCES:%.c=$(DIR_OBJECT)/%.o)
DEPENDENCIES = $(OBJECTS:%.o=%.d)
DOC_OBJECTS = $(DOC_SOURCES:%.c=$(DIR_OBJECT)/%.o)
DOC_DEPENDENCIES = $(DOC_OBJECTS:%.o=%.d)

RLIST_OBJECTS = $(RLIST_SOURCES:%.c=$(DIR_OBJECT)/%.o)
RLIST_DEPENDENCIES = $(RLIST_OBJECTS:%.o=%.d)
MANAGE_OBJECTS = $(MANAGE_SOURCES:%.c=$(DIR_OBJECT)/%.o)
MANAGE_DEPENDENCIES = $(MANAGE_OBJECTS:%.o=%.d)

SCAN_OBJECTS = $(SCAN_SOURCES:%.c=$(DIR_OBJECT)/%.o)
SCAN_DEPENDENCIES = $(SCAN_OBJECTS:%.o=%.d)


.PHONY: all
all:  rec_udf manage_udf

.PHONY: clean
clean:
	/bin/rm -f $(OBJECTS) $(DIR_TARGET)/$(TARGET) $(DIR_TARGET)/$(MANAGE_TARGET) 

.PHONY: depclean
depclean: clean
	/bin/rm -f $(DEPENDENCIES) 

.PHONY: $(TARGET)
$(TARGET): $(OBJECTS)
	$(CC) $(LDFLAGS) -o $(DIR_TARGET)/$(TARGET) $(OBJECTS) $(LIBRARIES)
$(MANAGE_TARGET): $(MANAGE_OBJECTS)
	$(CC) $(LDFLAGS) -o $(DIR_TARGET)/$(MANAGE_TARGET) $(MANAGE_OBJECTS) $(LIBRARIES)
-include $(DEPENDENCIES) 

$(DIR_OBJECT)/%.o: %.c | prepare
	$(CC) $(CFLAGS_NATIVE) -MMD -o $@ -c $(INCLUDES) $<

prepare:
	mkdir -p $(DIR_OBJECT)
