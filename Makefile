VELOX_ROOT ?= ${HOME}/velox
VELOX_BIN = ${VELOX_ROOT}/_build/release
VELOX_SRC = ${VELOX_ROOT}
VELOX_GEN = ${VELOX_BIN}
FOLLY_SRC = ${VELOX_BIN}/_deps/folly-src
XSIMD_SRC = ${VELOX_ROOT}/third_party/xsimd/include
GTEST_SRC = ${VELOX_ROOT}/third_party/googletest/googletest/include
PROTOBUF_SRC = ${VELOX_BIN}/_deps/protobuf-src/src
XXHASH_SRC = ${VELOX_ROOT}/velox/external/xxhash
ARROW_SRC = ${VELOX_BIN}/third_party/arrow_ep/install/include

INCLUDES = -I${VELOX_SRC} -I${FOLLY_SRC} -I${FOLLY_SRC}/../folly-build -I${XSIMD_SRC} -I${GTEST_SRC} -I${PROTOBUF_SRC} -I${VELOX_GEN} -I${XXHASH_SRC} -I${ARROW_SRC}
FLAGS = -std=gnu++17 -mavx -pthread -ggdb

VELOX_LIB = $(shell find ${VELOX_BIN} -name "libvelox*.a")
FOLLY_LIB = $(shell find ${VELOX_BIN} -name "libfolly.a")
PROTO_LIB = $(shell find ${VELOX_BIN} -name "libprotobuf.a")
MD5_LIB = $(shell find ${VELOX_BIN} -name "libmd5.a")	
DUCKDB_LIB = $(shell find ${VELOX_BIN} -name "libduckdb.a") $(shell find ${VELOX_BIN} -name "libtpch_extension.a") $(shell find ${VELOX_BIN} -name "libdbgen.a")
ARROW_LIB = ${VELOX_BIN}/third_party/arrow_ep/install/lib64/*.a

VELOX_ARCHIVES = ${VELOX_LIB} ${FOLLY_LIB} ${PROTO_LIB} ${MD5_LIB} ${DUCKDB_LIB} ${ARROW_LIB}
SHARED_LIBRARIES = -lglog -lgflags -lpthread -lfmt -ldl -levent -ldouble-conversion -lre2 -lboost_atomic -lboost_program_options -lboost_context -lboost_filesystem -lboost_regex -lboost_thread -lboost_system -lboost_date_time -L/usr/lib64 -lsnappy -llz4 -lzstd -lz -levent_openssl -lcrypto

PYTHON_VERSION = $(shell python -c "import sys; print('%d.%d' % sys.version_info[:2])")
PYTHON_INCLUDE = /usr/include/python${PYTHON_VERSION}

.PHONY: all python cli clean
.DEFAULT_GOAL = all

from_substrait.o: from_substrait.cpp
	c++ from_substrait.cpp -o from_substrait.o ${FLAGS} ${INCLUDES} -c

from_substrait: from_substrait.o
	c++ ${FLAGS} from_substrait.o -o from_substrait -Wl,--start-group ${VELOX_ARCHIVES} ${SHARED_LIBRARIES} -Wl,--end-group

veloxmodule.o: veloxmodule.cpp
	c++ veloxmodule.cpp -o veloxmodule.o ${FLAGS} -I${PYTHON_INCLUDE} ${INCLUDES} ${FLAGS} -fpic -c

velox.so: veloxmodule.o
	c++ ${FLAGS} veloxmodule.o -o velox.so -Wl,--start-group ${VELOX_ARCHIVES} ${SHARED_LIBRARIES} -Wl,--end-group -shared

cli: from_substrait

python: velox.so

all: from_substrait velox.so

clean:
	rm velox.so veloxmodule.o from_substrait.o from_substrait
