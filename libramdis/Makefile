CC := g++

# Change this to change the default verbosity level
# Defaults to LL_INFO
#LL_FATAL 0
#LL_ERROR 1
#LL_WARN 2
#LL_INFO 3
#LL_DEBUG 4
#LL_TRACE 5
VERBOSITY ?= 3 # LL_INFO

# Includes and library dependencies of RAMCloud
RAMCLOUD_SRC := $(HOME)/RAMCloud/src
RAMCLOUD_LIB := $(HOME)/RAMCloud/obj.master
RC_CLIENT_INCLUDES := -I$(RAMCLOUD_SRC) -I$(RAMCLOUD_LIB)
RC_CLIENT_LIBDEPS := -L$(RAMCLOUD_LIB) -L../PerfUtils -lramcloud -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lpthread -lssl -lcrypto -lPerfUtils

# Includes and library dependencies of docopt
CFLAGS := $(RC_CLIENT_INCLUDES) -DVERBOSITY=$(VERBOSITY)
LDFLAGS := $(RC_CLIENT_LIBDEPS)

all: libramdis.so libramdis.a

libramdis.so: ramdis.o
	$(CC) -shared -Wl,-soname,libramdis.so.0.0 -o $@ $(LDFLAGS) $^
	ln -f -s $@ libramdis.so.0.0

libramdis.a: ramdis.o
	ar rcs $@ $^

%.o: %.cc %.h
	$(CC) -std=c++11 -c $< $(CFLAGS) -g -o $@ -fPIC

clean:
	rm -rf *.o *.a *.so *.so.*
