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

#include <folly/experimental/TestUtil.h>
#include <folly/Random.h>

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
   * Destroying a TestUtil will spin down a cluster.
   */
  ~TestUtil();
  /**
   * Run a command in the hbase shell.
   */
  void RunShellCmd(const std::string& command);
  static std::string RandString();

private:
  folly::test::TemporaryDirectory temp_dir_;
};
} // namespace hbase
