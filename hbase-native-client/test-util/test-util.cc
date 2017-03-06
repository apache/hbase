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
#include <string.h>

#include <folly/Format.h>

using hbase::TestUtil;
using folly::Random;

std::string TestUtil::RandString(int len) {
  // Create the whole string.
  // Filling everything with z's
  auto s = std::string(len, 'z');

  // Now pick a bunch of random numbers
  for (int i = 0; i < len; i++) {
    // use Folly's random to get the numbers
    // as I don't want to have to learn
    // all the cpp rand invocation magic.
    auto r = Random::rand32('a', 'z');
    // Cast that to ascii.
    s[i] = static_cast<char>(r);
  }
  return s;
}

TestUtil::TestUtil() : TestUtil::TestUtil(2, "") {}

TestUtil::TestUtil(int servers, const std::string& confPath)
    : temp_dir_(TestUtil::RandString()), numRegionServers(servers), conf_path(confPath) {
  auto p = temp_dir_.path().string();
  StartMiniCluster(2);
  std::string quorum("localhost:");
  const std::string port = mini->GetConfValue("hbase.zookeeper.property.clientPort");
  conf()->Set("hbase.zookeeper.quorum", quorum + port);
}

TestUtil::~TestUtil() { StopMiniCluster(); }

void TestUtil::StartMiniCluster(int num_region_servers) {
  mini = std::make_unique<MiniCluster>();
  mini->StartCluster(num_region_servers, conf_path);
}
void TestUtil::StopMiniCluster() { mini->StopCluster(); }

void TestUtil::CreateTable(std::string tblNam, std::string familyName) {
  mini->CreateTable(tblNam, familyName);
}
void TestUtil::CreateTable(std::string tblNam, std::string familyName, std::string key1,
        std::string k2) {
  mini->CreateTable(tblNam, familyName, key1, k2);
}
void TestUtil::TablePut(std::string table, std::string row, std::string fam, std::string col,
        std::string value) {
  mini->TablePut(table, row, fam, col, value);
}
