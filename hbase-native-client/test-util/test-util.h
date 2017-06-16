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
#include <memory>
#include <string>
#include <vector>
#include "core/configuration.h"
#include "test-util/mini-cluster.h"

namespace hbase {
/**
 * @brief Class to deal with a local instance cluster for testing.
 */
class TestUtil {
 public:
  TestUtil();

  /**
   * Destroying a TestUtil will spin down a cluster and remove the test dir.
   */
  ~TestUtil();

  /**
   * Create a random string. This random string is all letters, as such it is
   * very good for use as a directory name.
   */
  static std::string RandString(int len = 32);

  /**
   * Returns the configuration to talk to the local cluster
   */
  std::shared_ptr<Configuration> conf() const { return conf_; }

  /**
   * Starts mini hbase cluster with specified number of region servers
   */
  void StartMiniCluster(int32_t num_region_servers);

  void StopMiniCluster();
  void CreateTable(const std::string &table, const std::string &family);
  void CreateTable(const std::string &table, const std::vector<std::string> &families);
  void CreateTable(const std::string &table, const std::string &family,
                   const std::vector<std::string> &keys);
  void CreateTable(const std::string &table, const std::vector<std::string> &families,
                   const std::vector<std::string> &keys);

  void StartStandAloneInstance();
  void StopStandAloneInstance();
  void RunShellCmd(const std::string &);
  void MoveRegion(const std::string &region, const std::string &server);

 private:
  std::unique_ptr<MiniCluster> mini_;
  folly::test::TemporaryDirectory temp_dir_;
  std::shared_ptr<Configuration> conf_ = std::make_shared<Configuration>();
};
}  // namespace hbase
