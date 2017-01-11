# Point to location of RAMCloud sources
RAMCLOUD_DIR := $(HOME)/RAMCloud

CC := gcc
CCSOURCES := $(wildcard *.c)
CCOBJECTS := $(CCSOURCES:.c=.o)
CCINCLUDES := -I../libramdis
CCLDFLAGS := -L../libramdis -lramdis -lpthread
CCFLAGS := $(CCINCLUDES)
CCTARGETS := ramdis-benchmark

all: $(CCTARGETS)

$(CCTARGETS): $(CCOBJECTS)
					$(CC) -o $@ $^ $(CCLDFLAGS)

%.o: %.c %.h
	        $(CC) $(CCFLAGS) -g -c $<

%.o: %.c
	        $(CC) $(CCFLAGS) -g -c $<

clean:
	        rm -f *.o $(CXXTARGETS) $(CCTARGETS)
