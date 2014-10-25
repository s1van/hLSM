include ../Makefile.config

EXECUTABLE = mapkeeper_leveldb

all :
	g++ -pthread -Wall -o $(EXECUTABLE) *cpp -I $(THRIFT_DIR)/include/thrift -I $(THRIFT_DIR)/include \
	-I ../../leveldb-1.15.0/include -I ../../leveldb-1.15.0 \
        -lboost_thread -lboost_filesystem -lboost_program_options -lthrift -I ../thrift/gen-cpp -lboost_system \
	-L $(THRIFT_DIR)/lib \
        -L ../thrift/gen-cpp -lmapkeeper \
	-L ../../leveldb-1.15.0/db -lleveldb \
           -Wl,-rpath,\$$ORIGIN/../thrift/gen-cpp			\
           -Wl,-rpath,$(THRIFT_DIR)/lib  \
	   -DLEVELDB_PLATFORM_POSIX


run:
	./$(EXECUTABLE) --sync

clean :
	- rm -rf $(THRIFT_SRC) $(EXECUTABLE) *.o 

wipe:
	- rm -rf data/*
