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

#include <gtest/gtest.h>

#include "core/cell.h"
#include "core/client.h"
#include "core/configuration.h"
#include "core/get.h"
#include "core/hbase-configuration-loader.h"
#include "core/put.h"
#include "core/result.h"
#include "core/table.h"
#include "serde/table-name.h"
#include "test-util/test-util.h"

using hbase::Cell;
using hbase::Configuration;
using hbase::Get;
using hbase::Put;
using hbase::Table;
using hbase::TestUtil;

class ClientTest : public ::testing::Test {
 public:
  static const constexpr char *kDefHBaseConfPath = "./build/test-data/client-test/conf/";
  static const constexpr char *kHBaseDefaultXml = "hbase-default.xml";
  static const constexpr char *kHBaseSiteXml = "hbase-site.xml";
  static const constexpr char *kHBaseXmlData =
      "<?xml version=\"1.0\"?>\n<?xml-stylesheet type=\"text/xsl\" "
      "href=\"configuration.xsl\"?>\n<!--\n/**\n *\n * Licensed to the Apache "
      "Software Foundation (ASF) under one\n * or more contributor license "
      "agreements.  See the NOTICE file\n * distributed with this work for "
      "additional information\n * regarding copyright ownership.  The ASF "
      "licenses this file\n * to you under the Apache License, Version 2.0 "
      "(the\n * \"License\"); you may not use this file except in compliance\n * "
      "with the License.  You may obtain a copy of the License at\n *\n *     "
      "http://www.apache.org/licenses/LICENSE-2.0\n *\n * Unless required by "
      "applicable law or agreed to in writing, software\n * distributed under "
      "the License is distributed on an \"AS IS\" BASIS,\n * WITHOUT WARRANTIES "
      "OR CONDITIONS OF ANY KIND, either express or implied.\n * See the License "
      "for the specific language governing permissions and\n * limitations under "
      "the License.\n "
      "*/\n-->\n<configuration>\n\n</configuration>";

  static void WriteDataToFile(const std::string &file, const std::string &xml_data) {
    std::ofstream hbase_conf;
    hbase_conf.open(file.c_str());
    hbase_conf << xml_data;
    hbase_conf.close();
  }

  static void CreateHBaseConf(const std::string &dir, const std::string &file,
                              const std::string xml_data) {
    // Remove temp file always
    boost::filesystem::remove((dir + file).c_str());
    boost::filesystem::create_directories(dir.c_str());
    WriteDataToFile((dir + file), xml_data);
  }

  static void CreateHBaseConfWithEnv() {
    // Creating Empty Config Files so that we dont get a Configuration exception @Client
    CreateHBaseConf(kDefHBaseConfPath, kHBaseDefaultXml, kHBaseXmlData);
    // the hbase-site.xml would be persisted by MiniCluster
    setenv("HBASE_CONF", kDefHBaseConfPath, 1);
  }
  static std::unique_ptr<hbase::TestUtil> test_util;

  static void SetUpTestCase() {
    google::InstallFailureSignalHandler();
    test_util = std::make_unique<hbase::TestUtil>();
    test_util->StartMiniCluster(2);
  }
};
std::unique_ptr<hbase::TestUtil> ClientTest::test_util = nullptr;

TEST_F(ClientTest, EmptyConfigurationPassedToClient) { ASSERT_ANY_THROW(hbase::Client client); }

TEST_F(ClientTest, ConfigurationPassedToClient) {
  // Remove already configured env if present.
  unsetenv("HBASE_CONF");
  ClientTest::CreateHBaseConfWithEnv();

  // Create Configuration
  hbase::HBaseConfigurationLoader loader;
  auto conf = loader.LoadDefaultResources();
  // Create a client
  hbase::Client client(conf.value());
  client.Close();
}

TEST_F(ClientTest, DefaultConfiguration) {
  // Remove already configured env if present.
  unsetenv("HBASE_CONF");
  ClientTest::CreateHBaseConfWithEnv();

  // Create Configuration
  hbase::Client client;
  client.Close();
}

TEST_F(ClientTest, PutGet) {
  // Using TestUtil to populate test data
  ClientTest::test_util->CreateTable("t", "d");

  // Create TableName and Row to be fetched from HBase
  auto tn = folly::to<hbase::pb::TableName>("t");
  auto row = "test1";

  // Create a client
  hbase::Client client(*ClientTest::test_util->conf());

  // Get connection to HBase Table
  auto table = client.Table(tn);
  ASSERT_TRUE(table) << "Unable to get connection to Table.";

  // Perform Puts
  table->Put(Put{"test1"}.AddColumn("d", "1", "value1"));
  table->Put(Put{"test1"}.AddColumn("d", "extra", "value for extra"));

  // Perform the Get
  hbase::Get get(row);
  auto result = table->Get(get);

  // Test the values, should be same as in put executed on hbase shell
  ASSERT_TRUE(!result->IsEmpty()) << "Result shouldn't be empty.";
  EXPECT_EQ("test1", result->Row());
  EXPECT_EQ("value1", *(result->Value("d", "1")));
  EXPECT_EQ("value for extra", *(result->Value("d", "extra")));

  table->Close();
  client.Close();
}

TEST_F(ClientTest, GetForNonExistentTable) {
  // Create TableName and Row to be fetched from HBase
  auto tn = folly::to<hbase::pb::TableName>("t_not_exists");
  auto row = "test1";

  // Get to be performed on above HBase Table
  hbase::Get get(row);

  // Create a client
  hbase::Client client(*ClientTest::test_util->conf());

  // Get connection to HBase Table
  auto table = client.Table(tn);
  ASSERT_TRUE(table) << "Unable to get connection to Table.";

  // Perform the Get
  ASSERT_ANY_THROW(table->Get(get)) << "Table does not exist. We should get an exception";

  table->Close();
  client.Close();
}

TEST_F(ClientTest, GetForNonExistentRow) {
  // Using TestUtil to populate test data
  ClientTest::test_util->CreateTable("t_exists", "d");

  // Create TableName and Row to be fetched from HBase
  auto tn = folly::to<hbase::pb::TableName>("t_exists");
  auto row = "row_not_exists";

  // Get to be performed on above HBase Table
  hbase::Get get(row);

  // Create a client
  hbase::Client client(*ClientTest::test_util->conf());

  // Get connection to HBase Table
  auto table = client.Table(tn);
  ASSERT_TRUE(table) << "Unable to get connection to Table.";

  // Perform the Get
  auto result = table->Get(get);
  ASSERT_TRUE(result->IsEmpty()) << "Result should  be empty.";

  table->Close();
  client.Close();
}

TEST_F(ClientTest, PutsWithTimestamp) {
  // Using TestUtil to populate test data
  ClientTest::test_util->CreateTable("t_puts_with_timestamp", "d");

  // Create TableName and Row to be fetched from HBase
  auto tn = folly::to<hbase::pb::TableName>("t_puts_with_timestamp");
  auto row = "test1";

  // Create a client
  hbase::Client client(*ClientTest::test_util->conf());

  // Get connection to HBase Table
  auto table = client.Table(tn);
  ASSERT_TRUE(table) << "Unable to get connection to Table.";

  int64_t ts = 42;
  // Perform Puts
  table->Put(Put{"test1"}.AddColumn("d", "1", ts, "value1"));
  auto cell =
      std::make_unique<Cell>("test1", "d", "extra", ts, "value for extra", hbase::CellType::PUT);
  table->Put(Put{"test1"}.Add(std::move(cell)));

  // Perform the Get
  hbase::Get get(row);
  auto result = table->Get(get);

  // Test the values, should be same as in put executed on hbase shell
  ASSERT_TRUE(!result->IsEmpty()) << "Result shouldn't be empty.";
  EXPECT_EQ("test1", result->Row());
  EXPECT_EQ("value1", *(result->Value("d", "1")));
  EXPECT_EQ("value for extra", *(result->Value("d", "extra")));
  EXPECT_EQ(ts, result->ColumnLatestCell("d", "1")->Timestamp());
  EXPECT_EQ(ts, result->ColumnLatestCell("d", "extra")->Timestamp());

  table->Close();
  client.Close();
}
