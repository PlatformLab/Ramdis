LIBNAME=libramdis

OBJ=ramdis.o

RAMDIS_MAJOR=0
RAMDIS_MINOR=0

DYLIBSUFFIX=so
STLIBSUFFIX=a
DYLIB_MINOR_NAME=$(LIBNAME).$(DYLIBSUFFIX).$(RAMDIS_MAJOR).$(RAMDIS_MINOR)
DYLIB_MAJOR_NAME=$(LIBNAME).$(DYLIBSUFFIX).$(RAMDIS_MAJOR)
DYLIBNAME=$(LIBNAME).$(DYLIBSUFFIX)
DYLIB_MAKE_CMD=$(CC) -shared -Wl,-soname,$(DYLIB_MINOR_NAME) -o $(DYLIBNAME) $(LDFLAGS)
STLIBNAME=$(LIBNAME).$(STLIBSUFFIX)
STLIB_MAKE_CMD=ar rcs $(STLIBNAME)

RAMCLOUD_SRC := $(HOME)/RAMCloud/src
RAMCLOUD_LIB := $(HOME)/RAMCloud/obj.master

CC := g++
LDFLAGS := -L$(RAMCLOUD_LIB) -lramcloud -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lpthread -lssl -lcrypto
CFLAGS := -Idocopt.cpp -I$(RAMCLOUD_SRC) -I$(RAMCLOUD_LIB) -fPIC

all: $(DYLIBNAME) $(STLIBNAME)

$(DYLIBNAME): $(OBJ)
	$(DYLIB_MAKE_CMD) $(OBJ)
	ln -f -s $(DYLIBNAME) $(DYLIB_MINOR_NAME)

$(STLIBNAME): $(OBJ)
	$(STLIB_MAKE_CMD) $(OBJ)

%.o: %.cc
	$(CC) -std=c++11 -c $(CFLAGS) $<

clean:
	rm -rf $(DYLIBNAME) $(STLIBNAME) $(DYLIB_MINOR_NAME) *.o
