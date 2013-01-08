# Citrusleaf Tools
# Makefile

DEPTH = ../../../..
include $(DEPTH)/make_in/Makefile.in

DIR_INCLUDE = ../include $(DEPTH)/shared/include/ $(DEPTH)/include/
DIR_OBJECT = ../obj
DIR_TARGET = ../bin

HEADERS = ad_sproc.h utils.h 
SOURCES =  ad_sproc.c utils.c 
TARGET = ad_sproc

INCLUDES = $(DIR_INCLUDE:%=-I%) 
LIBRARIES = -lcitrusleaf -lcf-client  -lssl -lpthread -lrt  -lcrypto
ifeq (${MR_LANG},LUA)
LIBRARIES += -llua
endif
LIBRARIES += -lm

LDFLAGS += -L$(DEPTH)/lib/ -L$(DEPTH)/shared/lib/ 

OBJECTS = $(SOURCES:%.c=$(DIR_OBJECT)/%.o)
DEPENDENCIES = $(OBJECTS:%.o=%.d)

.PHONY: all
all: ad_sproc 

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

