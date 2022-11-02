VELOX_ROOT ?= ${HOME}/velox
VELOX_BIN = ${VELOX_ROOT}/_build/release
VELOX_SRC = ${VELOX_ROOT}
VELOX_GEN = ${VELOX_BIN}
FOLLY_SRC = ${VELOX_BIN}/_deps/folly-src
XSIMD_SRC = ${VELOX_ROOT}/third_party/xsimd/include
GTEST_SRC = ${VELOX_ROOT}/third_party/googletest/googletest/include
PROTOBUF_SRC = ${VELOX_BIN}/_deps/protobuf-src/src
XXHASH_SRC = ${VELOX_ROOT}/velox/external/xxhash

INCLUDES = -I${VELOX_SRC} -I${FOLLY_SRC} -I${FOLLY_SRC}/../folly-build -I${XSIMD_SRC} -I${GTEST_SRC} -I${PROTOBUF_SRC} -I${VELOX_GEN} -I${XXHASH_SRC}
FLAGS = -O3 -std=gnu++17 -mavx -pthread

VELOX_LIB = $(shell find ${VELOX_BIN} -name "libvelox*.a")
FOLLY_LIB = ${FOLLY_SRC}/../folly-build/libfolly.a
PROTO_LIB = ${VELOX_BIN}/_deps/protobuf-build/libprotobuf.a ${VELOX_BIN}/_deps/protobuf-build/libprotoc.a

VELOX_ARCHIVES = ${VELOX_LIB} ${FOLLY_LIB} ${PROTO_LIB}
SHARED_LIBRARIES = -lglog -lgflags -lpthread -lfmt -ldl -levent -ldouble-conversion -lre2 -lboost_atomic -lboost_program_options -lboost_context -lboost_filesystem -lboost_regex -lboost_thread -lboost_system -lboost_date_time -L/usr/lib64 -lsnappy -llz4 -lzstd -lz

.DEFAULT_GOAL = test

from_substrait.o: from_substrait.cpp
	c++ from_substrait.cpp ${INCLUDES} ${FLAGS} -c -o from_substrait.o

test: from_substrait.o
	c++ ${FLAGS} from_substrait.o -Wl,--start-group ${VELOX_ARCHIVES} ${SHARED_LIBRARIES} -Wl,--end-group -o from_substrait	