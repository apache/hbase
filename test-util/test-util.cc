/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include "test-util/test-util.h"

#include <folly/Format.h>

using hbase::TestUtil;
using folly::Random;

const static int STR_LEN = 32;

std::string TestUtil::RandString() {
  auto s = std::string(STR_LEN, 'z');

  for (int i = 0; i < STR_LEN; i++) {
    auto r = Random::rand32('a', 'z');
    s[i] = static_cast<char>(r);
  }
  return s;
}

TestUtil::TestUtil() : temp_dir_(TestUtil::RandString()) {
  auto p = temp_dir_.path().string();
  auto cmd = std::string{"bin/start-local-hbase.sh " + p};
  auto res_code = std::system(cmd.c_str());
  CHECK(res_code == 0);
}
TestUtil::~TestUtil() {
  auto res_code = std::system("bin/stop-local-hbase.sh");
  CHECK(res_code == 0);
}

void TestUtil::RunShellCmd(const std::string& command) {
  auto cmd_string = folly::sformat("echo \"{}\" | ../bin/hbase shell", command);
  auto res_code = std::system(cmd_string.c_str());
  CHECK(res_code == 0);
}
