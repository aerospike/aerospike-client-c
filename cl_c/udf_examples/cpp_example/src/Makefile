# Citrusleaf Tools
# Makefile

DEPTH = ../../../../../..
include $(DEPTH)/make_in/Makefile.in

DIR_OBJECT = ../obj
DIR_TARGET = ../bin
DIR_LIB = ../../../target/Linux-x86_64/lib

SOURCES       = test_udf.cpp 
TARGET        = test_udf

INCLUDES = -I../include/ -I../../../target/Linux-x86_64/include/ -I../../../target/Linux-x86_64/include/citrusleaf/
LIBRARIES = $(DIR_LIB)/libcitrusleaf.a  -lssl -lcrypto -lpthread -lrt
ifeq (${MR_LANG},LUA)
LIBRARIES += -llua
endif
LIBRARIES += -lm

LDFLAGS += -L$(DEPTH)/lib/ -L$(DEPTH)/shared/lib/ 

OBJECTS = $(SOURCES:%.cpp=$(DIR_OBJECT)/%.o)
DEPENDENCIES = $(OBJECTS:%.o=%.d)

.PHONY: all
all: test_udf

.PHONY: clean
clean:
	/bin/rm -f $(OBJECTS) $(DIR_TARGET)/$(TARGET)

.PHONY: depclean
depclean: clean
	/bin/rm -f $(DEPENDENCIES) 

.PHONY: $(TARGET)
$(TARGET): $(OBJECTS)
	g++ $(LDFLAGS) -o $(DIR_TARGET)/$(TARGET) $(OBJECTS) $(LIBRARIES)

-include $(DEPENDENCIES) 

$(DIR_OBJECT)/%.o: %.cpp | prepare
	g++ -MMD -o $@ -c $(INCLUDES) $<

prepare:
	mkdir -p $(DIR_TARGET)
	mkdir -p $(DIR_OBJECT)
