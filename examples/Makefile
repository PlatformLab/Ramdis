# Point to location of RAMCloud sources
RAMCLOUD_DIR := $(HOME)/RAMCloud

CXX := g++
CXXLDFLAGS := -L$(RAMCLOUD_DIR)/obj.master -lramcloud -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lpthread -lssl -lcrypto
CXXSOURCES := $(wildcard *.cc)
CXXOBJECTS := $(CXXSOURCES:.cc=.o)
CXXINCLUDES := -I../docopt.cpp -I$(RAMCLOUD_DIR)/src -I$(RAMCLOUD_DIR)/obj.master
CXXFLAGS := --std=c++11 $(CXXINCLUDES) -g
CXXTARGETS := ramdis-cli

CC := gcc
CCSOURCES := $(wildcard *.c)
CCOBJECTS := $(CCSOURCES:.c=.o)
CCINCLUDES := -I..
CCLDFLAGS := -L.. -lramdis
CCFLAGS := $(CCINCLUDES)
CCTARGETS := 

all: $(CXXTARGETS) $(CCTARGETS)

#%: src/main/cpp/%.cc
#	g++ -o $@ $^ $(RAMCLOUD_OBJ_DIR)/OptionParser.o -g -std=c++0x -I$(RAMCLOUD_DIR)/src -I$(RAMCLOUD_OBJ_DIR) -L$(RAMCLOUD_OBJ_DIR) -lramcloud -lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lpthread -lssl -lcrypto 

$(CXXTARGETS): ../docopt.cpp/docopt.o $(CXXOBJECTS)
	        $(CXX) -o $@ $^ $(CXXLDFLAGS) 

$(CCTARGETS): $(CCOBJECTS)
					$(CC) -o $@ $^ $(CCLDFLAGS)

%.o: %.cc %.h
	        $(CXX) $(CXXFLAGS) -c $<

%.o: %.cc
	        $(CXX) $(CXXFLAGS) -c $<

%.o: %.c %.h
	        $(CC) $(CCFLAGS) -c $<

%.o: %.c
	        $(CC) $(CCFLAGS) -c $<

clean:
	        rm -f *.o $(CXXTARGETS) $(CCTARGETS)
