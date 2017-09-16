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

#use "g++" to compile source files
CC := g++
LD := g++

SRC_HBASE := src/hbase
INCLUDE_HBASE := include/
BUILD_PATH := build
DEBUG_PATH := $(BUILD_PATH)/debug
RELEASE_PATH := $(BUILD_PATH)/release
PROTO_SRC_DIR := $(SRC_HBASE)/if
PROTO_CXX_DIR := $(BUILD_PATH)/$(PROTO_SRC_DIR)
MODULES := connection client exceptions security serde utils
TEST_MODULES := test-util # These modules contain test code, not included in the build for the lib
SRC_DIR := $(addprefix $(SRC_HBASE)/,$(MODULES))
DEBUG_BUILD_DIR := $(addprefix $(DEBUG_PATH)/hbase/,$(MODULES))
RELEASE_BUILD_DIR := $(addprefix $(RELEASE_PATH)/hbase/,$(MODULES))

INCLUDE_DIR := . src $(BUILD_PATH)/src $(INCLUDE_HBASE)
TEST_BUILD_INCLUDE_DIR := $(INLCUDE_DIR) $(JAVA_HOME)/include/ $(JAVA_HOME)/include/linux

#flags to pass to the CPP compiler & linker
CPPFLAGS_DEBUG := -D_GLIBCXX_USE_CXX11_ABI=0 -g -Wall -std=c++14 -pedantic -fPIC -MMD -MP
CPPFLAGS_RELEASE := -D_GLIBCXX_USE_CXX11_ABI=0 -DNDEBUG -O2 -Wall -std=c++14 -pedantic -fPIC -MMD -MP
LDFLAGS := -lprotobuf -lzookeeper_mt -lsasl2 -lfolly -lwangle
TEST_BUILD_LDFLAGS := $(LDFLAGS) -L $(JAVA_HOME)/jre/lib/amd64/server -ljvm
LINKFLAG := -shared

#define list of source files and object files
ALLSRC := $(foreach sdir,$(SRC_DIR),$(wildcard $(sdir)/*.cc))
EXCLUDE_SRC := $(foreach sdir,$(SRC_DIR),$(wildcard $(sdir)/*-test.cc)) \
	$(SRC_HBASE)/client/simple-client.cc $(SRC_HBASE)/client/load-client.cc
SRC := $(filter-out $(EXCLUDE_SRC), $(ALLSRC))
PROTOSRC := $(patsubst $(PROTO_SRC_DIR)/%.proto, $(addprefix $(PROTO_CXX_DIR)/,%.pb.cc),$(wildcard $(PROTO_SRC_DIR)/*.proto))
PROTOHDR := $(patsubst $(PROTO_SRC_DIR)/%.proto, $(addprefix $(PROTO_CXX_DIR)/,%.pb.h),$(wildcard $(PROTO_SRC_DIR)/*.proto))
DEBUG_OBJ := $(patsubst $(SRC_HBASE)/%.cc,$(DEBUG_PATH)/hbase/%.o,$(SRC))
DEBUG_OBJ += $(patsubst $(PROTO_CXX_DIR)/%.cc,$(DEBUG_PATH)/hbase/if/%.o,$(PROTOSRC))
RELEASE_OBJ := $(patsubst $(SRC_HBASE)/%.cc,$(RELEASE_PATH)/hbase/%.o,$(SRC))
RELEASE_OBJ += $(patsubst $(PROTO_CXX_DIR)/%.cc,$(RELEASE_PATH)/hbase/if/%.o,$(PROTOSRC))
INCLUDES := $(addprefix -I,$(INCLUDE_DIR))

LIB_DIR := /usr/local
LIB_LIBDIR := $(LIB_DIR)/lib
LIB_INCDIR := $(LIB_DIR)/include
LIB_RELEASE := $(RELEASE_PATH)/libHBaseClient.so
ARC_RELEASE := $(RELEASE_PATH)/libHBaseClient.a
LIB_DEBUG := $(DEBUG_PATH)/libHBaseClient_d.so
ARC_DEBUG := $(DEBUG_PATH)/libHBaseClient_d.a
LOCAL_INCLUDE_DIR := /usr/local/include/

build: checkdirs protos copyfiles $(LIB_DEBUG) $(LIB_RELEASE) $(ARC_DEBUG) $(ARC_RELEASE)

vpath %.cc $(SRC_DIR)

$(LIB_DEBUG):
define make-goal-dbg
DEPS := $(DEBUG_OBJ:.o=.d)
-include $(DEPS)
$1/%.o: %.cc
	$(CC) -c $$< -o $$@ -MF$$(@:%.o=%.d) -MT$$@ $(CPPFLAGS_DEBUG) $(INCLUDES)
endef

$(LIB_RELEASE):
define make-goal-rel
DEPS := $(RELEASE_OBJ:.o=.d)
-include $(DEPS)
$1/%.o: %.cc
	$(CC) -c $$< -o $$@ -MF$$(@:%.o=%.d) -MT$$@ $(CPPFLAGS_RELEASE) $(INCLUDES)
endef

.PHONY: all clean install copyfiles

checkdirs: $(DEBUG_BUILD_DIR) $(RELEASE_BUILD_DIR) $(PROTO_CXX_DIR)

copyfiles:
	@bin/copy-protobuf.sh
	@bin/copy-version.sh

# .proto files are in src/hbase/if. These are compiled into C++ code by the 
# protoc compiler, and turned into .cc and .h files under build/src/hbase/if
$(PROTO_CXX_DIR)/%.pb.cc $(PROTO_CXX_DIR)/%.pb.h: $(PROTO_SRC_DIR)/%.proto
	@protoc --proto_path=$(PROTO_SRC_DIR) --cpp_out=$(PROTO_CXX_DIR) $<

# protos target compiles the .cc and .h files into .o files for the protobuf
# generated source files
protos: $(PROTO_CXX_DIR) $(PROTOSRC) $(PROTOHDR)
	@make -j8 all -f Makefile.protos

install_headers:
	cp -r $(INCLUDE_HBASE)/hbase $(LOCAL_INCLUDE_DIR)
	mkdir -p $(LOCAL_INCLUDE_DIR)/hbase/if
	cp -r $(PROTO_CXX_DIR)/*.h $(LOCAL_INCLUDE_DIR)/hbase/if

uninstall_headers:
	rm -rf $(LOCAL_INCLUDE_DIR)/hbase

install: install_headers
	cp $(LIB_RELEASE) $(LIB_LIBDIR)/libHBaseClient.so
	cp $(ARC_RELEASE) $(LIB_LIBDIR)/libHBaseClient.a
	cp $(LIB_DEBUG) $(LIB_LIBDIR)/libHBaseClient_d.so
	cp $(ARC_DEBUG) $(LIB_LIBDIR)/libHBaseClient_d.a
	ldconfig

uninstall: uninstall_headers
	rm -f $(LIB_LIBDIR)/libHBaseClient.so $(LIB_LIBDIR)/libHBaseClient.a $(LIB_LIBDIR)/libHBaseClient_d.so $(LIB_LIBDIR)/libHBaseClient_d.a
	ldconfig

$(PROTO_CXX_DIR):
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

clean:
	@rm -rf docs buck-out $(BUILD_PATH)

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
	@echo " all          : builds everything, creates doc and runs tests."
	@echo " build        : will build/rebuild everything."
	@echo " check        : will test everything."
	@echo " clean        : removes docs folder, object files and local libraries from build/ directory."
	@echo " copyfiles    : copies native version.h from mvn build and proto locally to hbase-native-client."
	@echo " doc          : generates documentation."
	@echo " install      : will copy the libs to $(LIB_LIBDIR). super user priviliege would be required."
	@echo " protos       : will create PB CPP sec and headers from if/*.proto and build them."
	@echo " uninstall    : removes the libs from $(LIB_LIBDIR)."
	@echo " lint         : will ensure that code conforms to Google coding style."
	@echo "If no target is specified 'build' will be executed"

all: copyfiles build doc check
