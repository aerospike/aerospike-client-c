# Citrusleaf Tools
# Makefile

DEPTH = ../../../../../..
include $(DEPTH)/make_in/Makefile.in

DIR_OBJECT = ../obj
DIR_TARGET = ../bin
DIR_LIB = ../../../target/Linux-x86_64/lib

SOURCES       = rec_udf.c utils.c 
TARGET        = rec_udf
DOC_HEADERS   = doc_udf.h utils.h 
DOC_SOURCES   = doc_udf.c utils.c 
DOC_TARGET    = doc_udf
RLIST_HEADERS = redis_list_sproc.h utils.h 
RLIST_SOURCES = redis_list_sproc.c utils.c 
RLIST_TARGET  = redis_list_sproc

MANAGE_SOURCES = manage_udf.c
MANAGE_TARGET  = manage_udf

INCLUDES = -I../include/ -I../../../target/Linux-x86_64/include/
LIBRARIES = $(DIR_LIB)/libcitrusleaf.a  -lssl -lcrypto -lpthread -lrt
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



.PHONY: all
all: rec_udf doc_udf manage_udf
#all: rec_sproc doc_sproc manage_sproc redis_list_sproc

.PHONY: clean
clean:
	/bin/rm -f $(OBJECTS) $(DIR_TARGET)/$(TARGET) $(DIR_TARGET)/$(DOC_TARGET) $(DIR_TARGET)/$(MANAGE_TARGET) $(DIR_TARGET)/$(RLIST_TARGET) 

.PHONY: depclean
depclean: clean
	/bin/rm -f $(DEPENDENCIES) 

.PHONY: $(TARGET)
$(TARGET): $(OBJECTS)
	$(CC) $(LDFLAGS) -o $(DIR_TARGET)/$(TARGET) $(OBJECTS) $(LIBRARIES)
$(DOC_TARGET): $(DOC_OBJECTS)
	$(CC) $(LDFLAGS) -o $(DIR_TARGET)/$(DOC_TARGET) $(DOC_OBJECTS) $(LIBRARIES)
$(RLIST_TARGET): $(RLIST_OBJECTS)
	$(CC) $(LDFLAGS) -o $(DIR_TARGET)/$(RLIST_TARGET) $(RLIST_OBJECTS) $(LIBRARIES)
$(MANAGE_TARGET): $(MANAGE_OBJECTS)
	$(CC) $(LDFLAGS) -o $(DIR_TARGET)/$(MANAGE_TARGET) $(MANAGE_OBJECTS) $(LIBRARIES)

-include $(DEPENDENCIES) 

$(DIR_OBJECT)/%.o: %.c
	$(CC) $(CFLAGS_NATIVE) -MMD -o $@ -c $(INCLUDES) $<
