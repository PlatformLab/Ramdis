CC := g++

# Convenience variables for making Ramdis (hiredis-based) library
LIBRAMDIS_NAME := libhiredis
LIBRAMDIS_MAJOR := 0
LIBRAMDIS_MINOR := 0
LIBRAMDIS_SONAME := $(LIBRAMDIS_NAME).so.$(LIBRAMDIS_MAJOR).$(LIBRAMDIS_MINOR)

# Includes and library dependencies of RAMCloud
RAMCLOUD_SRC := $(HOME)/RAMCloud/src
RAMCLOUD_LIB := $(HOME)/RAMCloud/obj.master
RC_CLIENT_INCLUDES := -I$(RAMCLOUD_SRC) -I$(RAMCLOUD_LIB)
RC_CLIENT_LIBDEPS := -L$(RAMCLOUD_LIB) -lramcloud -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lpthread -lssl -lcrypto

# Includes and library dependencies of docopt
DOCOPT_DIR := ./docopt.cpp
DOCOPT_INCLUDES := -I$(DOCOPT_DIR)
DOCOPT_LIBDEPS := -L$(DOCOPT_DIR)

CFLAGS := $(DOCOPT_INCLUDES) $(RC_CLIENT_INCLUDES)
LDFLAGS := $(DOCOPT_LIBDEPS) $(RC_CLIENT_LIBDEPS)

TARGETS := ramdis-server

all: $(LIBRAMDIS_NAME).so $(LIBRAMDIS_NAME).a $(TARGETS)

$(LIBRAMDIS_NAME).so: $(LIBRAMDIS_NAME:lib%=%.o)
	$(CC) -shared -Wl,-soname,$(LIBRAMDIS_SONAME) -o $@ $(LDFLAGS) $^
	ln -f -s $@ $(LIBRAMDIS_SONAME)

$(LIBRAMDIS_NAME).a: $(LIBRAMDIS_NAME:lib%=%.o)
	ar rcs $@ $^

$(LIBRAMDIS_NAME:lib%=%.o): $(LIBRAMDIS_NAME:lib%=%.cc) $(LIBRAMDIS_NAME:lib%=%.h)
	$(CC) -std=c++11 -c $< $(CFLAGS) -fPIC

$(TARGETS): $(DOCOPT_DIR)/docopt.o $(TARGETS:=.o)
	$(CC) -o $@ $^ $(LDFLAGS) 

%.o: %.cc %.h
	$(CC) -std=c++11 -c $< $(CFLAGS) -g -o $@

%.o: %.cc
	$(CC) -std=c++11 -c $< $(CFLAGS) -g -o $@

%.o: %.cpp %.h
	$(CC) -std=c++11 -c $< $(CFLAGS) -g -o $@

%.o: %.cpp
	$(CC) -std=c++11 -c $< $(CFLAGS) -g -o $@


clean:
	rm -rf $(LIBRAMDIS_NAME).so $(LIBRAMDIS_NAME).a $(LIBRAMDIS_SONAME) $(TARGETS) *.o
