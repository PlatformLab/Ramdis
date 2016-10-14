# Point to location of RAMCloud sources
RAMCLOUD_DIR := $(HOME)/RAMCloud

CC := g++
LDFLAGS := -L$(RAMCLOUD_DIR)/obj.master -lramcloud -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lpthread -lssl -lcrypto
SOURCES := $(wildcard *.cc)
OBJECTS := $(SOURCES:.cc=.o)
INCLUDES := -Idocopt.cpp -I$(RAMCLOUD_DIR)/src -I$(RAMCLOUD_DIR)/obj.master
CCFLAGS := --std=c++11 $(INCLUDES) -g
TARGETS := ramdis-cli

all: $(TARGETS)

#%: src/main/cpp/%.cc
#	g++ -o $@ $^ $(RAMCLOUD_OBJ_DIR)/OptionParser.o -g -std=c++0x -I$(RAMCLOUD_DIR)/src -I$(RAMCLOUD_OBJ_DIR) -L$(RAMCLOUD_OBJ_DIR) -lramcloud -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lpthread -lssl -lcrypto 

$(TARGETS): docopt.o $(OBJECTS)
	        $(CC) -o $@ $^ $(LDFLAGS) 

docopt.o: docopt.cpp/docopt.cpp
	        $(CC) $(CCFLAGS) -c $<

%.o: %.cc %.h
	        $(CC) $(CCFLAGS) -c $<

%.o: %.cc
	        $(CC) $(CCFLAGS) -c $<

clean:
	        rm -f *.o $(TARGETS)
