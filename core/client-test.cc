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

#include "core/client.h"
#include <gtest/gtest.h>
#include "core/configuration.h"
#include "core/get.h"
#include "core/hbase_configuration_loader.h"
#include "core/result.h"
#include "core/table.h"
#include "serde/table-name.h"
#include "test-util/test-util.h"

class ClientTest {
 public:
  const static std::string kDefHBaseConfPath;

  const static std::string kHBaseDefaultXml;
  const static std::string kHBaseSiteXml;

  const static std::string kHBaseXmlData;

  static void WriteDataToFile(const std::string &file, const std::string &xml_data) {
    std::ofstream hbase_conf;
    hbase_conf.open(file.c_str());
    hbase_conf << xml_data;
    hbase_conf.close();
  }

  static void CreateHBaseConf(const std::string &dir, const std::string &file,
                              const std::string xml_data) {
    // Directory will be created if not present
    if (!boost::filesystem::exists(dir)) {
      boost::filesystem::create_directories(dir);
    }
    // Remove temp file always
    boost::filesystem::remove((dir + file).c_str());
    WriteDataToFile((dir + file), xml_data);
  }

  static void CreateHBaseConfWithEnv() {
    // Creating Empty Config Files so that we dont get a Configuration exception @Client
    CreateHBaseConf(kDefHBaseConfPath, kHBaseDefaultXml, kHBaseXmlData);
    CreateHBaseConf(kDefHBaseConfPath, kHBaseSiteXml, kHBaseXmlData);
    setenv("HBASE_CONF", kDefHBaseConfPath.c_str(), 1);
  }
};

const std::string ClientTest::kDefHBaseConfPath("./build/test-data/client-test/conf/");

const std::string ClientTest::kHBaseDefaultXml("hbase-default.xml");
const std::string ClientTest::kHBaseSiteXml("hbase-site.xml");

const std::string ClientTest::kHBaseXmlData(
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
    "*/\n-->\n<configuration>\n\n</configuration>");

TEST(Client, EmptyConfigurationPassedToClient) { ASSERT_ANY_THROW(hbase::Client client); }

TEST(Client, ConfigurationPassedToClient) {
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

TEST(Client, DefaultConfiguration) {
  // Remove already configured env if present.
  unsetenv("HBASE_CONF");
  ClientTest::CreateHBaseConfWithEnv();

  // Create Configuration
  hbase::Client client;
  client.Close();
}

TEST(Client, Get) {
  // Remove already configured env if present.
  unsetenv("HBASE_CONF");
  ClientTest::CreateHBaseConfWithEnv();

  // Using TestUtil to populate test data
  hbase::TestUtil *test_util = new hbase::TestUtil();
  test_util->RunShellCmd("create 't', 'd'");
  test_util->RunShellCmd("put 't', 'test2', 'd:2', 'value2'");
  test_util->RunShellCmd("put 't', 'test2', 'd:extra', 'value for extra'");

  // Create TableName and Row to be fetched from HBase
  auto tn = folly::to<hbase::pb::TableName>("t");
  auto row = "test2";

  // Get to be performed on above HBase Table
  hbase::Get get(row);

  // Create Configuration
  hbase::HBaseConfigurationLoader loader;
  auto conf = loader.LoadDefaultResources();

  // Create a client
  hbase::Client client(conf.value());

  // Get connection to HBase Table
  auto table = client.Table(tn);
  ASSERT_TRUE(table) << "Unable to get connection to Table.";

  // Perform the Get
  auto result = table->Get(get);

  // Stopping the connection as we are getting segfault due to some folly issue
  // The connection stays open and we don't want that.
  // So we are stopping the connection.
  // We can remove this once we have fixed the folly part
  delete test_util;

  // Test the values, should be same as in put executed on hbase shell
  ASSERT_TRUE(!result->IsEmpty()) << "Result shouldn't be empty.";
  EXPECT_EQ("test2", result->Row());
  EXPECT_EQ("value2", *(result->Value("d", "2")));
  EXPECT_EQ("value for extra", *(result->Value("d", "extra")));

  table->Close();
  client.Close();
}

TEST(Client, GetForNonExistentTable) {
  // Remove already configured env if present.
  unsetenv("HBASE_CONF");
  ClientTest::CreateHBaseConfWithEnv();

  // Using TestUtil to populate test data
  hbase::TestUtil *test_util = new hbase::TestUtil();

  // Create TableName and Row to be fetched from HBase
  auto tn = folly::to<hbase::pb::TableName>("t_not_exists");
  auto row = "test2";

  // Get to be performed on above HBase Table
  hbase::Get get(row);

  // Create Configuration
  hbase::HBaseConfigurationLoader loader;
  auto conf = loader.LoadDefaultResources();

  // Create a client
  hbase::Client client(conf.value());

  // Get connection to HBase Table
  auto table = client.Table(tn);
  ASSERT_TRUE(table) << "Unable to get connection to Table.";

  // Perform the Get
  ASSERT_ANY_THROW(table->Get(get)) << "Table does not exist. We should get an exception";

  // Stopping the connection as we are getting segfault due to some folly issue
  // The connection stays open and we don't want that.
  // So we are stopping the connection.
  // We can remove this once we have fixed the folly part
  delete test_util;

  table->Close();
  client.Close();
}

TEST(Client, GetForNonExistentRow) {
  // Remove already configured env if present.
  unsetenv("HBASE_CONF");
  ClientTest::CreateHBaseConfWithEnv();

  // Using TestUtil to populate test data
  hbase::TestUtil *test_util = new hbase::TestUtil();
  test_util->RunShellCmd("create 't_exists', 'd'");

  // Create TableName and Row to be fetched from HBase
  auto tn = folly::to<hbase::pb::TableName>("t_exists");
  auto row = "row_not_exists";

  // Get to be performed on above HBase Table
  hbase::Get get(row);

  // Create Configuration
  hbase::HBaseConfigurationLoader loader;
  auto conf = loader.LoadDefaultResources();

  // Create a client
  hbase::Client client(conf.value());

  // Get connection to HBase Table
  auto table = client.Table(tn);
  ASSERT_TRUE(table) << "Unable to get connection to Table.";

  // Perform the Get
  auto result = table->Get(get);
  ASSERT_TRUE(result->IsEmpty()) << "Result should  be empty.";

  // Stopping the connection as we are getting segfault due to some folly issue
  // The connection stays open and we don't want that.
  // So we are stopping the connection.
  // We can remove this once we have fixed the folly part
  delete test_util;

  table->Close();
  client.Close();
}
