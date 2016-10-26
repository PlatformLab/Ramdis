CC := g++

# Convenience variables for making Ramdis (hiredis-based) library
LIBRAMDIS_NAME := libhiredis
LIBRAMDIS_MAJOR := 0
LIBRAMDIS_MINOR := 11
LIBRAMDIS_SONAME := $(LIBRAMDIS_NAME).so.$(LIBRAMDIS_MAJOR).$(LIBRAMDIS_MINOR)

# Includes and library dependencies of RAMCloud
RAMCLOUD_SRC := $(HOME)/RAMCloud/src
RAMCLOUD_LIB := $(HOME)/RAMCloud/obj.master
RC_CLIENT_INCLUDES := -I$(RAMCLOUD_SRC) -I$(RAMCLOUD_LIB)
RC_CLIENT_LIBDEPS := -L$(RAMCLOUD_LIB) -lramcloud -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lpthread -lssl -lcrypto

# Includes and library dependencies of docopt
DOCOPT_DIR := ../docopt.cpp
DOCOPT_INCLUDES := -I$(DOCOPT_DIR)
DOCOPT_LIBDEPS := -L$(DOCOPT_DIR)

CFLAGS := $(DOCOPT_INCLUDES) $(RC_CLIENT_INCLUDES)
LDFLAGS := $(DOCOPT_LIBDEPS) $(RC_CLIENT_LIBDEPS)

all: $(LIBRAMDIS_NAME).so $(LIBRAMDIS_NAME).a

$(LIBRAMDIS_NAME).so: $(LIBRAMDIS_NAME:lib%=%.o) net.o sds.o
	$(CC) -shared -Wl,-soname,$(LIBRAMDIS_SONAME) -o $@ $(LDFLAGS) $^
	ln -f -s $@ $(LIBRAMDIS_SONAME)

$(LIBRAMDIS_NAME).a: $(LIBRAMDIS_NAME:lib%=%.o) net.o sds.o
	ar rcs $@ $^

$(LIBRAMDIS_NAME:lib%=%.o): $(LIBRAMDIS_NAME:lib%=%.cc) $(LIBRAMDIS_NAME:lib%=%.h)
	$(CC) -std=c++11 -c $< $(CFLAGS) -fPIC

net.o: net.c net.h
	$(CC) -std=c++11 -c $< $(CFLAGS) -g -o $@ -fPIC
	
sds.o: sds.c sds.h
	$(CC) -std=c++11 -c $< $(CFLAGS) -g -o $@ -fPIC

clean:
	rm -rf $(LIBRAMDIS_NAME).so $(LIBRAMDIS_NAME).a $(LIBRAMDIS_SONAME) *.o
