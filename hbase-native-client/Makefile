# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#use "gcc" to compile source files
CC:=g++
LD:=g++
 
DEBUG_PATH = build/debug
RELEASE_PATH = build/release
PROTO_SRC_DIR = build/if
MODULES = connection core serde test-util utils security
SRC_DIR = $(MODULES)
DEBUG_BUILD_DIR = $(addprefix $(DEBUG_PATH)/,$(MODULES))
RELEASE_BUILD_DIR = $(addprefix $(RELEASE_PATH)/,$(MODULES))
INCLUDE_DIR = . build/

#flags to pass to the CPP compiler & linker
CPPFLAGS_DEBUG = -D_GLIBCXX_USE_CXX11_ABI=0 -g -Wall -std=c++14 -pedantic -fPIC
CPPFLAGS_RELEASE = -D_GLIBCXX_USE_CXX11_ABI=0 -DNDEBUG -O2 -Wall -std=c++14 -pedantic -fPIC
LDFLAGS = -lprotobuf -lzookeeper_mt -lsasl2 -lfolly -lwangle
LINKFLAG = -shared

#define list of source files and object files
SRC = $(foreach sdir,$(SRC_DIR),$(wildcard $(sdir)/*.cc))
PROTOSRC = $(patsubst %.proto, $(addprefix build/,%.pb.cc),$(wildcard if/*.proto))
DEPS =  $(foreach sdir,$(SRC_DIR),$(wildcard $(sdir)/*.h))
PROTODEPS = $(patsubst %.proto, $(addprefix build/,%.pb.h),$(wildcard if/*.proto))
DEBUG_OBJ = $(patsubst %.cc,$(DEBUG_PATH)/%.o,$(SRC))
DEBUG_OBJ += $(patsubst %.cc,$(DEBUG_PATH)/%.o,$(PROTOSRC))
RELEASE_OBJ = $(patsubst %.cc,$(RELEASE_PATH)/%.o,$(SRC))
INCLUDES = $(addprefix -I,$(INCLUDE_DIR))
	
LIB_DIR = /usr/local
LIB_LIBDIR = $(LIB_DIR)/lib
LIB_INCDIR = $(LIB_DIR)/include
LIB_RELEASE=$(RELEASE_PATH)/libHbaseClient.so
ARC_RELEASE=$(RELEASE_PATH)/libHbaseClient.a
LIB_DEBUG=$(DEBUG_PATH)/libHbaseClient_d.so
ARC_DEBUG=$(DEBUG_PATH)/libHbaseClient_d.a

vpath %.cc $(SRC_DIR)

$(LIB_DEBUG): $(DEBUG_BUILD_DIR)
define make-goal-dbg
$1/%.o: %.cc $(DEPS) $(PROTODEPS) $(PROTOSRC)
	$(CC) -c $$< -o $$@ $(CPPFLAGS_DEBUG) $(INCLUDES)
endef

$(LIB_RELEASE): $(RELEASE_BUILD_DIR)
define make-goal-rel
$1/%.o: %.cc $(DEPS) $(PROTODEPS) $(PROTOSRC)
	$(CC) -c $$< -o $$@ $(CPPFLAGS_RELEASE) $(INCLUDES) 
endef

.PHONY: all clean install 

build: checkdirs protos $(LIB_DEBUG) $(LIB_RELEASE) $(ARC_DEBUG) $(ARC_RELEASE) 

checkdirs: $(DEBUG_BUILD_DIR) $(RELEASE_BUILD_DIR) $(PROTO_SRC_DIR)

protos: createprotosrc
	@make all -f Makefile.protos

createprotosrc:	$(PROTO_SRC_DIR)
	@protoc --proto_path=if --cpp_out=$(PROTO_SRC_DIR) if/*.proto

install:
	cp $(LIB_RELEASE) $(LIB_LIBDIR)/libHbaseClient.so
	cp $(ARC_RELEASE) $(LIB_LIBDIR)/libHbaseClient.a
	cp $(LIB_DEBUG) $(LIB_LIBDIR)/libHbaseClient_d.so
	cp $(ARC_DEBUG) $(LIB_LIBDIR)/libHbaseClient_d.a
	
$(PROTO_SRC_DIR):
	@mkdir -p $@

$(DEBUG_BUILD_DIR):
	@mkdir -p $@

$(RELEASE_BUILD_DIR):
	@mkdir -p $@

$(ARC_DEBUG):  $(DEBUG_OBJ)
	ar rcs $@ $^

$(ARC_RELEASE):  $(RELEASE_OBJ)
	ar rcs $@ $^

$(LIB_RELEASE):	$(RELEASE_OBJ)
	$(LD) $(LINKFLAG) -o $@ $(LDFLAGS) $(RELEASE_OBJ)

$(LIB_DEBUG): $(DEBUG_OBJ)
	$(LD) $(LINKFLAG) -o $@ $(LDFLAGS) $(DEBUG_OBJ)

#to clean re-compilable files
clean:
	@rm -rf $(DEBUG_BUILD_DIR) $(RELEASE_BUILD_DIR) $(LIB_RELEASE) $(LIB_DEBUG) $(ARC_RELEASE) $(ARC_DEBUG) docs buck-out build

$(foreach bdir,$(DEBUG_BUILD_DIR), $(eval $(call make-goal-dbg,$(bdir))))

$(foreach bdir,$(RELEASE_BUILD_DIR),$(eval $(call make-goal-rel,$(bdir))))

check:
	$(shell buck test --all --no-results-cache)

lint:
	bin/cpplint.sh

doc:
	$(shell doxygen hbase.doxygen > /dev/null)

help:
	@echo "Available targets:"
	@echo ""
	@echo " build    : will build everything."
	@echo " clean    : will remove the docs folder, object files and local libraries"
	@echo " install  : will copy the libs to $(LIB_LIBDIR). super user priviliege would be required."
	@echo " check    : will test everything."
	@echo " protos   : will build the corresponding sources for protobufs present in if/ directory."

all: build doc check

