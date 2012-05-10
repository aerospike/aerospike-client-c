C_DEPTH = ..
DIR_INCLUDE_C =  $(C_DEPTH)/include/
DIR_INCLUDE_PY =  /usr/include/python2.6/
DIR_OBJECT = ./obj

AS_CFLAGS = -D_FILE_OFFSET_BITS=64 -std=gnu99 -D_REENTRANT
MARCH_NATIVE = $(shell uname -m)
CFLAGS_NATIVE = -g -O3 -fPIC -fno-common -fno-strict-aliasing -rdynamic -Wextra $(AS_CFLAGS) -D MARCH_$(MARCH_NATIVE)
# Linux auto-sets to 64, MacOS doesn't
ifeq ($(SYSTEM),Darwin)
	CFLAGS_NATIVE += -v -D OSX
endif
# if our version is 4.1, can't use the native flag
ifeq (${CC_V},4.1)
    CFLAGS_NATIVE += -march=nocona
else
    CFLAGS_NATIVE += -march=native -msse4
endif

LDFLAGS = -fPIC -shared 
CC = gcc
C_LIB = $(C_DEPTH)/lib/libcitrusleaf.a
TARGET_S = _citrusleaf.so
SOURCE = citrusleaf.i

all: $(TARGET_S) putobj 
$(TARGET_S): citrusleaf_wrap.o $(C_LIB)
	ld $(LDFLAGS) citrusleaf_wrap.o -lrt -lz -lssl $(C_LIB) -o $(LANG)/$(TARGET_S)

citrusleaf_wrap.o: citrusleaf_wrap.c $(DIR_INCLUDE_C)
	mkdir -p $(DIR_OBJECT)
	$(CC) $(CFLAGS_NATIVE) -lmaple -c -I$(DIR_INCLUDE_C) -I$(DIR_INCLUDE_PY) $< 

citrusleaf_wrap.c: $(SOURCE)
	mkdir -p $(LANG)
	swig -outdir $(LANG) -$(LANG) $(SOURCE)

putobj: 
	mv *.o $(DIR_OBJECT)

#Run conditional clean. 
ifeq ($(LANG),)
clean: 
	rm -rf $(DIR_OBJECT)/*.o
	rm -rf citrusleaf_wrap.c
	rm -rf $(LANG)/*.so
	rm -rf $(LANG)/citrusleaf*	
else
clean:
	rm -rf $(DIR_OBJECT)/*.o
	rm -rf citrusleaf_wrap.c
	rm -rf python/citrusleaf*
	rm -rf python/*.so
#Add here for more languages
endif	
