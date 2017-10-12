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

#include <fstream>
#include "hbase/client/append.h"
#include "hbase/client/cell.h"
#include "hbase/client/client.h"
#include "hbase/client/configuration.h"
#include "hbase/client/delete.h"
#include "hbase/client/get.h"
#include "hbase/client/hbase-configuration-loader.h"
#include "hbase/client/increment.h"
#include "hbase/client/put.h"
#include "hbase/client/result.h"
#include "hbase/client/table.h"
#include "hbase/exceptions/exception.h"
#include "hbase/serde/table-name.h"
#include "hbase/test-util/test-util.h"
#include "hbase/utils/bytes-util.h"
#include "hbase/utils/optional.h"

using hbase::Cell;
using hbase::Configuration;
using hbase::Get;
using hbase::RetriesExhaustedException;
using hbase::none;
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

TEST_F(ClientTest, Append) {
  // Using TestUtil to populate test data
  ClientTest::test_util->CreateTable("t", "d");

  // Create TableName and Row to be fetched from HBase
  auto tn = folly::to<hbase::pb::TableName>("t");
  auto row = "test1";

  // Create a client
  hbase::Client client(*ClientTest::test_util->conf());

  // Get connection to HBase Table
  auto table = client.Table(tn);
  ASSERT_TRUE(table != nullptr) << "Unable to get connection to Table.";
  std::string val1 = "a";
  auto result = table->Append(hbase::Append{row}.Add("d", "1", val1));

  ASSERT_TRUE(!result->IsEmpty()) << "Result shouldn't be empty.";
  EXPECT_EQ(row, result->Row());
  EXPECT_EQ(val1, *(result->Value("d", "1")));

  std::string val2 = "b";
  result = table->Append(hbase::Append{row}.Add("d", "1", val2));

  ASSERT_TRUE(!result->IsEmpty()) << "Result shouldn't be empty.";
  EXPECT_EQ(row, result->Row());
  EXPECT_EQ("ab", *(result->Value("d", "1")));
}

TEST_F(ClientTest, PutGetDelete) {
  // Using TestUtil to populate test data
  std::string tableName = "t1";
  ClientTest::test_util->CreateTable(tableName, "d");

  // Create TableName and Row to be fetched from HBase
  auto tn = folly::to<hbase::pb::TableName>(tableName);
  auto row = "test1";

  // Create a client
  hbase::Client client(*ClientTest::test_util->conf());

  // Get connection to HBase Table
  auto table = client.Table(tn);
  ASSERT_TRUE(table != nullptr) << "Unable to get connection to Table.";

  // Perform Puts
  std::string valExtra = "value for extra";
  std::string valExt = "value for ext";
  table->Put(Put{row}.AddColumn("d", "1", "value1"));
  // Put two values for column "extra"
  table->Put(Put{row}.AddColumn("d", "extra", "1st val extra"));
  usleep(1000);
  table->Put(Put{row}.AddColumn("d", "extra", valExtra));
  table->Put(Put{row}.AddColumn("d", "ext", valExt));

  // Perform the Get
  hbase::Get get(row);
  auto result = table->Get(get);

  // Test the values, should be same as in put executed on hbase shell
  ASSERT_TRUE(!result->IsEmpty()) << "Result shouldn't be empty.";
  EXPECT_EQ("test1", result->Row());
  EXPECT_EQ("value1", *(result->Value("d", "1")));
  EXPECT_EQ(valExtra, *(result->Value("d", "extra")));
  auto cell = *(result->ColumnCells("d", "extra"))[0];
  auto tsExtra = cell.Timestamp();
  auto tsExt = (*(result->ColumnCells("d", "ext"))[0]).Timestamp();

  // delete column "1"
  table->Delete(hbase::Delete{row}.AddColumn("d", "1"));
  result = table->Get(get);
  ASSERT_TRUE(!result->IsEmpty()) << "Result shouldn't be empty.";
  ASSERT_FALSE(result->Value("d", "1")) << "Column 1 should be gone";
  EXPECT_EQ(valExtra, *(result->Value("d", "extra")));

  // delete cell from column "extra" with timestamp tsExtra
  table->Delete(hbase::Delete{row}.AddColumn("d", "extra", tsExtra));
  result = table->Get(get);
  ASSERT_TRUE(!result->IsEmpty()) << "Result shouldn't be empty.";
  ASSERT_FALSE(result->Value("d", "1")) << "Column 1 should be gone";
  ASSERT_TRUE(result->Value("d", "extra") != none) << "Column extra should have value";
  EXPECT_EQ(valExt, *(result->Value("d", "ext"))) << "Column ext should have value";

  // delete all cells from "extra" column
  table->Delete(hbase::Delete{row}.AddColumns("d", "extra"));
  result = table->Get(get);
  ASSERT_TRUE(!result->IsEmpty()) << "Result shouldn't be empty.";
  ASSERT_FALSE(result->Value("d", "1")) << "Column 1 should be gone";
  ASSERT_FALSE(result->Value("d", "extra")) << "Column extra should be gone";
  EXPECT_EQ(valExt, *(result->Value("d", "ext"))) << "Column ext should have value";

  // Delete the row and verify that subsequent Get returns nothing
  table->Delete(hbase::Delete{row}.AddFamily("d"));
  result = table->Get(get);
  ASSERT_TRUE(result->IsEmpty()) << "Result should be empty.";

  table->Close();
  client.Close();
}

TEST_F(ClientTest, Increment) {
  // Using TestUtil to populate test data
  ClientTest::test_util->CreateTable("t1", "d");

  // Create TableName and Row to be fetched from HBase
  auto tn = folly::to<hbase::pb::TableName>("t1");
  auto row = "test1";

  // Create a client
  hbase::Client client(*ClientTest::test_util->conf());

  // Get connection to HBase Table
  auto table = client.Table(tn);
  ASSERT_TRUE(table != nullptr) << "Unable to get connection to Table.";
  int64_t incr1 = 1235;
  auto result = table->Increment(hbase::Increment{row}.AddColumn("d", "1", incr1));
  EXPECT_EQ(row, result->Row());

  long l = hbase::BytesUtil::ToInt64(*(result->Value("d", "1")));
  EXPECT_EQ(incr1, l);

  int64_t incr2 = -2;
  result = table->Increment(hbase::Increment{row}.AddColumn("d", "1", incr2));

  EXPECT_EQ(row, result->Row());
  EXPECT_EQ(incr1 + incr2, hbase::BytesUtil::ToInt64(*(result->Value("d", "1"))));
}

TEST_F(ClientTest, CheckAndPut) {
  // Using TestUtil to populate test data
  ClientTest::test_util->CreateTable("check", "d");

  // Create TableName and Row to be fetched from HBase
  auto tn = folly::to<hbase::pb::TableName>("check");
  auto row = "test1";

  // Create a client
  hbase::Client client(*ClientTest::test_util->conf());

  // Get connection to HBase Table
  auto table = client.Table(tn);
  ASSERT_TRUE(table != nullptr) << "Unable to get connection to Table.";

  // Perform Puts
  table->Put(Put{row}.AddColumn("d", "1", "value1"));
  auto result = table->CheckAndPut(row, "d", "1", "value1", Put{row}.AddColumn("d", "1", "value2"));
  ASSERT_TRUE(result) << "CheckAndPut didn't replace value";

  result = table->CheckAndPut(row, "d", "1", "value1", Put{row}.AddColumn("d", "1", "value3"));

  // Perform the Get
  hbase::Get get(row);
  auto result1 = table->Get(get);
  EXPECT_EQ("value2", *(result1->Value("d", "1")));
  ASSERT_FALSE(result) << "CheckAndPut shouldn't replace value";
}

TEST_F(ClientTest, CheckAndDelete) {
  // Using TestUtil to populate test data
  ClientTest::test_util->CreateTable("checkDel", "d");

  // Create TableName and Row to be fetched from HBase
  auto tn = folly::to<hbase::pb::TableName>("checkDel");
  auto row = "test1";

  // Create a client
  hbase::Client client(*ClientTest::test_util->conf());

  // Get connection to HBase Table
  auto table = client.Table(tn);
  ASSERT_TRUE(table != nullptr) << "Unable to get connection to Table.";

  auto val1 = "value1";

  // Perform Puts
  table->Put(Put{row}.AddColumn("d", "1", val1));
  table->Put(Put{row}.AddColumn("d", "2", "value2"));
  auto result = table->CheckAndDelete(row, "d", "1", val1, hbase::Delete{row}.AddColumn("d", "2"));
  ASSERT_TRUE(result) << "CheckAndDelete didn't replace value";

  // Perform the Get
  hbase::Get get(row);
  auto result1 = table->Get(get);
  EXPECT_EQ(val1, *(result1->Value("d", "1")));
  ASSERT_FALSE(result1->Value("d", "2")) << "Column 2 should be gone";
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
  ASSERT_TRUE(table != nullptr) << "Unable to get connection to Table.";

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

  ClientTest::test_util->conf()->SetInt("hbase.client.retries.number", 5);
  // Create a client
  hbase::Client client(*ClientTest::test_util->conf());

  // Get connection to HBase Table
  auto table = client.Table(tn);
  ASSERT_TRUE(table != nullptr) << "Unable to get connection to Table.";

  // Perform the Get
  try {
    table->Get(get);
    FAIL() << "Should have thrown RetriesExhaustedException";
  } catch (const RetriesExhaustedException &ex) {
    ASSERT_EQ(0, ex.num_retries());
  } catch (...) {
    FAIL() << "Should have thrown RetriesExhaustedException";
  }

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
  ASSERT_TRUE(table != nullptr) << "Unable to get connection to Table.";

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
  ASSERT_TRUE(table != nullptr) << "Unable to get connection to Table.";

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

void SetClientParams() {
  ClientTest::test_util->conf()->SetInt("hbase.client.cpu.thread.pool.size", 6);
  ClientTest::test_util->conf()->SetInt("hbase.client.operation.timeout", 600000);
  ClientTest::test_util->conf()->SetInt("hbase.client.retries.number", 7);
  ClientTest::test_util->conf()->SetInt("hbase.client.start.log.errors.counter", 1);
}

void PerformPuts(uint64_t num_rows, std::shared_ptr<hbase::Client> client,
                 const std::string &table_name) {
  auto tn = folly::to<hbase::pb::TableName>(table_name);
  auto table = client->Table(tn);
  ASSERT_TRUE(table != nullptr) << "Unable to get connection to Table.";
  // Perform Puts
  for (uint64_t i = 0; i < num_rows; i++) {
    table->Put(Put{"test" + std::to_string(i)}.AddColumn("d", std::to_string(i),
                                                         "value" + std::to_string(i)));
  }
}

void MakeGets(uint64_t num_rows, const std::string &row_prefix, std::vector<hbase::Get> &gets) {
  // Perform the Gets
  for (uint64_t i = 0; i < num_rows; ++i) {
    auto row = "test" + std::to_string(i);
    hbase::Get get(row);
    gets.push_back(get);
  }
  gets.push_back(hbase::Get("test2"));
  gets.push_back(hbase::Get("testextra"));
}

void TestMultiResults(uint64_t num_rows, const std::vector<std::shared_ptr<hbase::Result>> &results,
                      const std::vector<hbase::Get> &gets) {
  // Test the values, should be same as in put executed on hbase shell
  ASSERT_TRUE(!results.empty()) << "Result vector shouldn't be empty.";

  uint32_t i = 0;
  for (; i < num_rows; ++i) {
    ASSERT_TRUE(!results[i]->IsEmpty()) << "Result for Get " << gets[i].row()
                                        << " must not be empty";
    EXPECT_EQ("test" + std::to_string(i), results[i]->Row());
    EXPECT_EQ("value" + std::to_string(i), results[i]->Value("d", std::to_string(i)).value());
  }
  // We are inserting test2 twice so the below test should pass
  ASSERT_TRUE(!results[i]->IsEmpty()) << "Result for Get " << gets[i].row() << " must not be empty";

  ++i;
  ASSERT_TRUE(results[i]->IsEmpty()) << "Result for Get " << gets[i].row() << " must be empty";
}

TEST_F(ClientTest, MultiGets) {
  std::string table_name = "t";
  // Using TestUtil to populate test data
  ClientTest::test_util->CreateTable(table_name, "d");

  // Create TableName and Row to be fetched from HBase
  auto tn = folly::to<hbase::pb::TableName>(table_name);

  SetClientParams();
  // Create a client
  hbase::Client client(*ClientTest::test_util->conf());

  uint64_t num_rows = 50000;
  PerformPuts(num_rows, std::make_shared<hbase::Client>(client), table_name);

  // Get connection to HBase Table
  auto table = client.Table(tn);
  ASSERT_TRUE(table != nullptr) << "Unable to get connection to Table.";

  std::vector<hbase::Get> gets;
  MakeGets(num_rows, "test", gets);

  auto results = table->Get(gets);

  TestMultiResults(num_rows, results, gets);

  table->Close();
  client.Close();
}

TEST_F(ClientTest, MultiGetsWithRegionSplits) {
  // Using TestUtil to populate test data
  std::vector<std::string> keys{"test0",   "test100", "test200", "test300", "test400",
                                "test500", "test600", "test700", "test800", "test900"};
  std::string table_name = "t";
  ClientTest::test_util->CreateTable(table_name, "d", keys);

  // Create TableName and Row to be fetched from HBase
  auto tn = folly::to<hbase::pb::TableName>(table_name);

  SetClientParams();

  // Create a client
  hbase::Client client(*ClientTest::test_util->conf());

  uint64_t num_rows = 50000;
  PerformPuts(num_rows, std::make_shared<hbase::Client>(client), table_name);

  // Get connection to HBase Table
  auto table = client.Table(tn);
  ASSERT_TRUE(table != nullptr) << "Unable to get connection to Table.";

  std::vector<hbase::Get> gets;
  MakeGets(num_rows, "test", gets);

  auto results = table->Get(gets);

  TestMultiResults(num_rows, results, gets);

  table->Close();
  client.Close();
}

void PerformMultiPuts(uint64_t num_rows, std::shared_ptr<hbase::Client> client,
                      const std::string &table_name) {
  auto tn = folly::to<hbase::pb::TableName>(table_name);
  auto table = client->Table(tn);
  ASSERT_TRUE(table) << "Unable to get connection to Table.";
  std::vector<hbase::Put> puts;
  // Perform Puts
  for (uint64_t i = 0; i < num_rows; i++) {
    puts.push_back(Put{"test" + std::to_string(i)}.AddColumn("d", std::to_string(i),
                                                             "value" + std::to_string(i)));
  }
  table->Put(puts);
}

void PerformMultiPuts(std::vector<hbase::Put> &puts, std::shared_ptr<Table> table) {
  table->Put(puts);
}

TEST_F(ClientTest, MultiGetsWithMultiPuts) {
  std::string table_name = "t";
  // Using TestUtil to populate test data
  ClientTest::test_util->CreateTable(table_name, "d");

  // Create TableName and Row to be fetched from HBase
  auto tn = folly::to<hbase::pb::TableName>(table_name);

  SetClientParams();
  // Create a client
  hbase::Client client(*ClientTest::test_util->conf());

  uint64_t num_rows = 50000;
  PerformMultiPuts(num_rows, std::make_shared<hbase::Client>(client), table_name);

  // Get connection to HBase Table
  auto table = client.Table(tn);
  ASSERT_TRUE(table) << "Unable to get connection to Table.";

  std::vector<hbase::Get> gets;
  MakeGets(num_rows, "test", gets);

  auto results = table->Get(gets);

  TestMultiResults(num_rows, results, gets);

  table->Close();
  client.Close();
}

TEST_F(ClientTest, MultiGetsWithMultiPutsAndSplitRegions) {
  // Using TestUtil to populate test data
  std::vector<std::string> keys{"test0",   "test100", "test200", "test300", "test400",
                                "test500", "test600", "test700", "test800", "test900"};
  std::string table_name = "t";
  ClientTest::test_util->CreateTable(table_name, "d", keys);

  // Create TableName and Row to be fetched from HBase
  auto tn = folly::to<hbase::pb::TableName>(table_name);

  SetClientParams();

  // Create a client
  hbase::Client client(*ClientTest::test_util->conf());

  uint64_t num_rows = 50000;
  PerformMultiPuts(num_rows, std::make_shared<hbase::Client>(client), table_name);

  // Get connection to HBase Table
  auto table = client.Table(tn);
  ASSERT_TRUE(table) << "Unable to get connection to Table.";

  std::vector<hbase::Get> gets;
  MakeGets(num_rows, "test", gets);

  auto results = table->Get(gets);

  TestMultiResults(num_rows, results, gets);

  table->Close();
  client.Close();
}

TEST_F(ClientTest, MultiPuts) {
  std::string table_name = "t";
  // Using TestUtil to populate test data
  ClientTest::test_util->CreateTable(table_name, "d");

  // Create TableName and Row to be fetched from HBase
  auto tn = folly::to<hbase::pb::TableName>(table_name);

  SetClientParams();
  // Create a client
  hbase::Client client(*ClientTest::test_util->conf());
  std::shared_ptr<Table> table = client.Table(tn);
  ASSERT_TRUE(table) << "Unable to get connection to Table.";

  uint64_t num_rows = 80000;
  uint64_t batch_num_rows = 10000;
  std::vector<hbase::Put> puts;
  for (uint64_t i = 0; i < num_rows;) {
    puts.clear();
    // accumulate batch_num_rows at a time
    for (uint64_t j = 0; j < batch_num_rows && i < num_rows; ++j) {
      hbase::Put put("test" + std::to_string(i));
      put.AddColumn("d", std::to_string(i), "value" + std::to_string(i));
      puts.push_back(put);
      i++;
    }
    PerformMultiPuts(puts, table);
  }
  table->Close();
  client.Close();
}

TEST_F(ClientTest, MultiPutsWithRegionSplits) {
  // Using TestUtil to populate test data
  std::vector<std::string> keys{"test0",   "test100", "test200", "test300", "test400",
                                "test500", "test600", "test700", "test800", "test900"};
  std::string table_name = "t";
  ClientTest::test_util->CreateTable(table_name, "d", keys);

  // Create TableName and Row to be fetched from HBase
  auto tn = folly::to<hbase::pb::TableName>(table_name);

  SetClientParams();

  // Create a client
  hbase::Client client(*ClientTest::test_util->conf());
  std::shared_ptr<Table> table = client.Table(tn);
  ASSERT_TRUE(table) << "Unable to get connection to Table.";

  uint64_t num_rows = 80000;
  uint64_t batch_num_rows = 10000;
  std::vector<hbase::Put> puts;
  for (uint64_t i = 0; i < num_rows;) {
    puts.clear();
    // accumulate batch_num_rows at a time
    for (uint64_t j = 0; j < batch_num_rows && i < num_rows; ++j) {
      hbase::Put put("test" + std::to_string(i));
      put.AddColumn("d", std::to_string(i), "value" + std::to_string(i));
      puts.push_back(put);
      i++;
    }
    PerformMultiPuts(puts, table);
  }
  table->Close();
  client.Close();
}
