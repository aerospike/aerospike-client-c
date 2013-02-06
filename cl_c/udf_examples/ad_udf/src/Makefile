# Citrusleaf Tools
# Makefile

DEPTH = ../../../../../..
include $(DEPTH)/make_in/Makefile.in

DIR_OBJECT = ../obj
DIR_TARGET = ../bin
DIR_LIB = ../../../target/Linux-x86_64/lib

HEADERS = ad_udf.h utils.h 
SOURCES =  ad_udf.c utils.c 
TARGET = ad_udf

INCLUDES = -I../include/ -I../../../target/Linux-x86_64/include
LIBRARIES = $(DIR_LIB)/libcitrusleaf.a  -lssl -lpthread -lrt  -lcrypto
ifeq (${MR_LANG},LUA)
LIBRARIES += -llua
endif
LIBRARIES += -lm

LDFLAGS += -L$(DEPTH)/lib/ -L$(DEPTH)/shared/lib/ 

OBJECTS = $(SOURCES:%.c=$(DIR_OBJECT)/%.o)
DEPENDENCIES = $(OBJECTS:%.o=%.d)

.PHONY: all
all: ad_udf 

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

