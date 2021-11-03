# Compiler
CC = g++

# Compiling options
CFLAGS = -std=c++11 -mavx2 -mavx -mfma
CFLAGS += -O2 -s -DNDEBUG

# Compiling header and libs
HEADER = -I./../../include 
LIBB = -L./../../bin -lpthread -lstreamlec

# Compiling rules
all: source processor sink

source: source.cpp
	$(CC) source.cpp $(CFLAGS) $(HEADER) $(LIBB) -o source

sink: sink.cpp
	$(CC) sink.cpp $(CFLAGS) $(HEADER) $(LIBB) -o sink 

processor: processor.cpp
	$(CC) processor.cpp $(CFLAGS) $(HEADER) $(LIBB) -o processor

clean:
	rm -rf source sink processor
