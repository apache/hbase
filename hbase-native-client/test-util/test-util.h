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
#pragma once

#include <folly/Random.h>
#include <folly/experimental/TestUtil.h>

#include <cstdlib>
#include <string>

namespace hbase {
/**
 * @brief Class to deal with a local instance cluster for testing.
 */
class TestUtil {
 public:
  /**
   * Creating a TestUtil will spin up a cluster.
   */
  TestUtil();

  /**
   * Destroying a TestUtil will spin down a cluster and remove the test dir.
   */
  ~TestUtil();

  /**
   * Run a command in the hbase shell. Command should not include any double
   * quotes.
   *
   * This should only be used until there is a good Admin support from the
   * native client
   */
  void RunShellCmd(const std::string &command);

  /**
   * Create a random string. This random string is all letters, as such it is
   * very good for use as a directory name.
   */
  static std::string RandString(int len = 32);

 private:
  folly::test::TemporaryDirectory temp_dir_;
};
}  // namespace hbase
