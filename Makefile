#--------------------------
# Macro definitions
	CXXFLAGS = -O1 -g -Wall -I/usr/local/include/thrift -std=c++11
	OBJ = main.o mongoose.o graph.o GH.o graphrpc_constants.o graphrpc_types.o
	TARGET = cs426_graph_server
#--------------------------
# Rules
all:$(TARGET)
$(TARGET):$(OBJ)
	$(CXX) -o $@ $(OBJ) -L/usr/local/lib -lthrift -lboost_system -lboost_thread
clean:
	rm -f $(OBJ) $(TARGET)
#---------------------------
#Dependencies
main.o: main.cpp mongoose.h graph.h GH.h
GH.o: GH.cpp GH.h
graphrpc_constants.o: graphrpc_constants.cpp graphrpc_constants.h
graphrpc_types.o: graphrpc_types.cpp graphrpc_types.h
mongoose.o: mongoose.c mongoose.h
graph.o: graph.cpp graph.h
