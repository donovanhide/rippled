TEST_DIR = tests
TOOLS_DIR = tools
BUILD := debug

# Paths to the fused gtest files.
GTEST_H = $(TEST_DIR)/gtest/gtest.h
GTEST_ALL_C = $(TEST_DIR)/gtest/gtest-all.cc
GTEST_MAIN_CC = $(TEST_DIR)/gtest/gtest_main.cc

cxxflags.debug := -g -O0
cxxflags.release := -g -O3 -DNDEBUG

CPPFLAGS += -I$(TEST_DIR) -I. -isystem $(TEST_DIR)/gtest
CXXFLAGS += ${cxxflags.${BUILD}} -Wall -Wextra -Wpedantic -std=c++1y -DGTEST_LANG_CXX11=1
LDFLAGS += -lpthread

all : keyvadb_unittests kvd dump

valgrind : all
	valgrind --dsymutil=yes --track-origins=yes ./keyvadb_unittests

check : all
	./keyvadb_unittests

clean :
	rm -rf keyvadb_unittests *.o

gtest-all.o : $(GTEST_H) $(GTEST_ALL_C)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(TEST_DIR)/gtest/gtest-all.cc

unittests.o : $(TEST_DIR)/unittests.cc db/*.h tests/*.h $(GTEST_H)
	$(CXX) $(CPPFLAGS) $(TESTFLAGS) $(CXXFLAGS) -c $(TEST_DIR)/unittests.cc

keyvadb_unittests : unittests.o gtest-all.o 
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

kvd.o : $(TOOLS_DIR)/kvd.cc db/*.h
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(TOOLS_DIR)/kvd.cc

kvd : kvd.o
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

dump.o : $(TOOLS_DIR)/dump.cc 
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(TOOLS_DIR)/dump.cc

dump : dump.o
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)