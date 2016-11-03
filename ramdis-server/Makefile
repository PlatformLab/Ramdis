CC := g++

# Change this to change the default verbosity level
# Defaults to LL_INFO
VERBOSITY ?= 3 # LL_INFO

# Includes and library dependencies of RAMCloud
RAMCLOUD_SRC := $(HOME)/RAMCloud/src
RAMCLOUD_LIB := $(HOME)/RAMCloud/obj.master
RC_CLIENT_INCLUDES := -I$(RAMCLOUD_SRC) -I$(RAMCLOUD_LIB)
RC_CLIENT_LIBDEPS := -L$(RAMCLOUD_LIB) -lramcloud -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lpthread -lssl -lcrypto

# Includes and library dependencies of docopt
DOCOPT_DIR := ../docopt.cpp
DOCOPT_INCLUDES := -I$(DOCOPT_DIR)
DOCOPT_LIBDEPS := -L$(DOCOPT_DIR)

CFLAGS := $(DOCOPT_INCLUDES) $(RC_CLIENT_INCLUDES) -DVERBOSITY=$(VERBOSITY)
LDFLAGS := $(DOCOPT_LIBDEPS) $(RC_CLIENT_LIBDEPS)

TARGETS := ramdis-server

all: $(TARGETS)

$(TARGETS): $(DOCOPT_DIR)/docopt.o $(TARGETS:=.o) sds.o zmalloc.o commands.o
	$(CC) -o $@ $^ $(LDFLAGS) 

%.o: %.cc %.h
	$(CC) -std=c++11 -c $< $(CFLAGS) -g -o $@

%.o: %.cc
	$(CC) -std=c++11 -c $< $(CFLAGS) -g -o $@

%.o: %.cpp %.h
	$(CC) -std=c++11 -c $< $(CFLAGS) -g -o $@

%.o: %.cpp
	$(CC) -std=c++11 -c $< $(CFLAGS) -g -o $@

%.o: %.c %.h
	$(CC) -std=c++11 -c $< $(CFLAGS) -g -o $@

%.o: %.c
	$(CC) -std=c++11 -c $< $(CFLAGS) -g -o $@

clean:
	rm -rf $(TARGETS) *.o
