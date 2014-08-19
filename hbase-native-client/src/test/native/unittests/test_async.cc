/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#line 19 "test_async.cc" // ensures short filename in logs.

#include <fstream>
#include <stdio.h>
#include <string.h>

#include <hbase/hbase.h>
#include <gtest/gtest.h>

#include "common_utils.h"
#include "libhbaseutil.h"

namespace hbase {
namespace test {

static const char *TABLE_CREATE_FILE  = "src/test/resources/table_create.dat";
static const char *PUT_ROWS_FILE      = "src/test/resources/table_put.dat";

static std::string zk_quorum = "";
static std::string zk_root_node = "";
static std::string log_file = "";

uint64_t expectedNumberOfCellCount = 0;
uint64_t expectedNumberOfRowCount = 0;
uint64_t maxNumberOfRows = 0;

class HBaseCAPI: public ::testing::Test {
  // shared by all tests.
protected:
  // Per-test-case set-up.
  // Called before the first test in this test case.
  static void SetUpTestCase() {
    fprintf(stdout, "\n **** HBaseCAPI::SetUpTestCase **** \n");
    log_file = GetConfigParameter("LIBHBASE_LOG_FILE");
    InitLogger(log_file);
    zk_quorum = GetConfigParameter("LIBHBASE_ZK_QUORUM");
    zk_root_node = GetConfigParameter("LIBHBASE_ZK_ROOT_NODE");

    HBASE_LOG_INFO("Using Zookeeper quorum: '%s'", zk_quorum.c_str());
    HBASE_LOG_INFO("zk_root_node: '%s'", zk_root_node.c_str());
    if (zk_root_node.empty()) {
      ASSERT_EQ(connectionCreate((const char*)zk_quorum.c_str(), NULL), 0);
    } else {
      ASSERT_EQ(connectionCreate((const char*)zk_quorum.c_str(),
          (const char*) zk_root_node.c_str()), 0);
    }
    ASSERT_EQ(clientCreate(), 0);
    ASSERT_EQ(adminCreate(), 0);
  }

  // Per-test-case tear-down.
  // Called after the last test in this test case.
  static void TearDownTestCase() {
    ASSERT_EQ(adminDestroy(), 0);
    ASSERT_EQ(clientDestroy(), 0);
    ASSERT_EQ(connectionDestroy(), 0);
    ASSERT_EQ(CloseLogger(), 0);
    fprintf(stdout, "\n **** HBaseCAPI::TearDownTestCase **** \n");
  }

  virtual void SetUp() { }

  virtual void TearDown() { }
};

TEST_F(HBaseCAPI, table_create) {
  std::string line;
  std::vector<std::string> words;
  std::string table_name = "";

  HBASE_LOG_INFO("*** hbase table create API test ***");
  std::ifstream myfile(TABLE_CREATE_FILE);
  if (myfile.is_open()) {
    while (getline(myfile, line)) {
      std::vector<std::string> columnFamilies;
      words = SplitString(line, ',');
      for (int t = 0; t < (int) words.size(); ++t) {
        HBASE_LOG_DEBUG("splitted word:%s", words.at(t).c_str());
        if (t == 0) {
          table_name = words.at(t);
        } else {
          columnFamilies.push_back(words.at(t));
        }
      }
      ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
      ASSERT_EQ(createTable((char*)table_name.c_str(), columnFamilies), 0);
    }
    myfile.close();
  } else {
    HBASE_LOG_FATAL("failed to open file:%s", TABLE_CREATE_FILE);
    ASSERT_FALSE(myfile == NULL);
  }
}

TEST_F(HBaseCAPI, table_delete_non_exsting) {
  std::string line;
  std::vector<std::string> words;
  std::string table_name = "non_existing_table";

  HBASE_LOG_INFO("*** table delete test with non existing table ***");
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  ASSERT_EQ(deleteTable((char*)table_name.c_str()), 2);
}

TEST_F(HBaseCAPI, table_create_existing) {
  std::string line;
  std::vector<std::string> words;
  std::string table_name = "test_table";
  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };

  HBASE_LOG_INFO("*** table create test with already existing table name***");
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  ASSERT_EQ(createTable((char*)table_name.c_str(), columnFamilies), 0);
  ASSERT_EQ(createTable((char*)table_name.c_str(), columnFamilies), 17);
}

TEST_F(HBaseCAPI, disable_enableTable) {
  std::string table_name = "test_table";
  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };

  HBASE_LOG_INFO(" **** table disable enable API test ****");
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  ASSERT_EQ(createTable((char*)table_name.c_str(), columnFamilies), 0);
  ASSERT_EQ(disableTable((char*)table_name.c_str()), 0);
  ASSERT_EQ(enableTable((char*)table_name.c_str()), 0);
}

TEST_F(HBaseCAPI, disable_enableTable_multiple_times) {
  std::string table_name = "test_table";
  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };

  HBASE_LOG_INFO("**** table disable enable table multiple times ****");
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  ASSERT_EQ(createTable((char*)table_name.c_str(), columnFamilies), 0);
  EXPECT_EQ(disableTable((char*)table_name.c_str()), 0);
  EXPECT_EQ(disableTable((char*)table_name.c_str()), 0);
  EXPECT_EQ(enableTable((char*)table_name.c_str()), 0);
  EXPECT_EQ(enableTable((char*)table_name.c_str()), 0);
}

TEST_F(HBaseCAPI, disable_enable_non_existing_table) {
  std::string table_name = "test_table";
  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };

  HBASE_LOG_INFO("**** table disable enable with non existing table ****");
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  ASSERT_EQ(enableTable((char*)table_name.c_str()), 2);
}

TEST_F(HBaseCAPI, enabled_verification) {
  std::string table_name = "test_table";
  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };

  HBASE_LOG_INFO("******* table is enabled API test *****");
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  ASSERT_EQ(isTableEnabled((char*)table_name.c_str()), 2);  // non existing table test
  ASSERT_EQ(createTable((char*)table_name.c_str(), columnFamilies), 0);
  ASSERT_EQ(isTableEnabled((char*)table_name.c_str()), 0);
  ASSERT_EQ(disableTable((char*)table_name.c_str()), 0);
  ASSERT_EQ(isTableEnabled((char*)table_name.c_str()), HBASE_TABLE_DISABLED);
}

TEST_F(HBaseCAPI, put_test) {
  std::string line;
  std::vector<std::string> words;
  bool table_created = false;

  HBASE_LOG_INFO("***  table put API test ***");
  HBASE_LOG_INFO("table put API test with input file:%s", PUT_ROWS_FILE);

  std::ifstream myfile(PUT_ROWS_FILE);
  if (myfile.is_open()) {
    std::string table_name = "";
    while (getline(myfile, line)) {
      if (line.at(0) == '#') {
        HBASE_LOG_DEBUG("this line is comment in file skipping ..");
        continue;
      }
      std::vector<std::string> columnFamilies;
      std::vector<std::string> rowColumns;
      HBASE_LOG_DEBUG("current line:%s", line.c_str());
      words = SplitString(line, ',');
      std::string row_key = "";
      for (int t = 0; t < (int) words.size(); ++t) {
        HBASE_LOG_DEBUG("splitted word:%s ", words.at(t).c_str());
        if (!table_created) {
          if (t == 0) {
            table_name = words.at(t);
          } else {
            columnFamilies.push_back(words.at(t));
          }
        } else {
          if (t == 0) {
            row_key = words.at(t);
          } else {
            rowColumns.push_back(words.at(t));
          }
        }
      }
      if (!table_created) {
        HBASE_LOG_INFO("creating table:%s ", words.at(0).c_str());
        ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
        ASSERT_EQ(createTable((char*)table_name.c_str(), columnFamilies), 0);
        table_created = true;
      } else {
        HBASE_LOG_INFO("inserting row to table: %s with key: %s",
            table_name.c_str(), row_key.c_str());
        ASSERT_EQ(putRow(table_name, row_key, rowColumns), 0);
      }
    }
    HBASE_LOG_INFO("wait for puts..");
    waitForPutsToComplete();
    HBASE_LOG_INFO("wait for puts completed");

    myfile.close();
  } else {
    HBASE_LOG_FATAL("faild to open file:%s", PUT_ROWS_FILE);
    ASSERT_FALSE(myfile == NULL);
  }
}

TEST_F (HBaseCAPI, put_non_existing_cf_async) {
  std::string table_name = "put_table";
  std::string table_not_existing = "non_existing_table";

  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };
  std::vector<std::string> row1_data {
    "testcf1:co1:4", "testcf1:col5:3", "testcf2:col1:6" };
  std::vector<std::string> row2_data {
    "testcf1:co1:4", "testcf1:col2:3", "testcf2:col1:6" };

  HBASE_LOG_INFO("***  table put API test with non existing table and non existing CF ***");
  HBASE_LOG_INFO("creating table for put request:%s ", table_name.c_str());
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  ASSERT_EQ(createTable((char*)table_name.c_str(), columnFamilies), 0);

  std::vector<std::string> row1_cf_not_exist { "none:col6:3" };
  EXPECT_EQ(putRowAndWait(table_not_existing, "row1", row1_data), 2);
  EXPECT_EQ(putRowAndWait(table_name, "row2", row1_cf_not_exist), 2);
}

TEST_F(HBaseCAPI, get_test_async) {
  std::string line;
  std::vector<std::string> words;
  std::string table_name = "test_table1";

  HBASE_LOG_INFO("*** hbase table get API test ***");
  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };
  std::vector<std::string> row1_data {
    "testcf1:co1:4", "testcf1:col5:3", "testcf2:col1:6" };
  std::vector<std::string> row2_data {
    "testcf1:co1:4", "testcf1:col2:3", "testcf2:col1:6" };

  HBASE_LOG_INFO("creating table for get requrest:%s", table_name.c_str());
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  ASSERT_EQ(createTable((char*)table_name.c_str(), columnFamilies), 0);
  ASSERT_EQ(putRow(table_name, "row1", row1_data), 0);
  ASSERT_EQ(putRow(table_name, "row2", row2_data), 0);

  HBASE_LOG_INFO("wait for puts..");
  ASSERT_TRUE(waitForPutsToComplete());
  HBASE_LOG_INFO("wait for puts completed");

  std::vector<std::string> row1_data1 { "testcf1:col6:3" };
  EXPECT_EQ(getVerifyRow(table_name, "row1", row1_data1), CELL_NOT_FOUND_ERR);
  EXPECT_EQ(getVerifyRow(table_name, "row2", row2_data), 0);
}

TEST_F (HBaseCAPI, get_entire_row_async) {
  std::string table_name = "get_table_row";

  HBASE_LOG_INFO("*** hbase table get entire row test ***");
  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };
  std::vector<std::string> row_data {
    "testcf1:co1:4", "testcf1:col5:3", "testcf2:col1:6", "testcf1:col:6" };

  HBASE_LOG_INFO("creating table for get request:%s ", table_name.c_str());
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  ASSERT_EQ(createTable((char*)table_name.c_str(), columnFamilies), 0);
  EXPECT_EQ(0, putRow(table_name, "row1", row_data));
  HBASE_LOG_INFO("wait for puts..");
  ASSERT_TRUE(waitForPutsToComplete());

  HBASE_LOG_INFO("wait for puts completed");
  EXPECT_EQ(0, getVerifyRow(table_name, "row1", row_data, true, false));
}

TEST_F (HBaseCAPI, verify_cf_ttl_async) {
  std::string table_name = "test_ttl";

  HBASE_LOG_INFO("*** hbase table put and get with ttl ***");
  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };
  std::vector<std::string> row_data { "testcf1:co1:4", "testcf1:col5:3",
    "testcf2:col1:6", "testcf1:col:6" };

  HBASE_LOG_INFO("creating table for get request:%s ", table_name.c_str());
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  ASSERT_EQ(
      createTable((char*)table_name.c_str(), columnFamilies, 3, 0/*min versions*/,
          1/*set ttl to 1*/), 0);
  EXPECT_EQ(0, putRow(table_name, "row1", row_data));
  HBASE_LOG_INFO("wait for puts..");
  ASSERT_TRUE(waitForPutsToComplete());
  HBASE_LOG_INFO("wait for puts completed");

  // sleep for 2 seconds so that cell will be deleted
  sleep(2);
  std::vector<std::string> row1_valid_single_cell { "testcf1:co1:4" };
  EXPECT_EQ(getVerifyRow(table_name, "row1", row1_valid_single_cell), CELL_NOT_FOUND_ERR);
}

TEST_F (HBaseCAPI, verify_cf_ttl_with_min_versions_async) {
  std::string table_name = "test_ttl";

  HBASE_LOG_INFO("*** hbase table put and get with ttl and min versions ***");
  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };
  std::vector<std::string> row_data {
    "testcf1:co1:4", "testcf1:col5:3", "testcf2:col1:6", "testcf1:col:6" };

  HBASE_LOG_INFO("creating table for get request:%s ", table_name.c_str());
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  ASSERT_EQ(
      createTable((char*)table_name.c_str(), columnFamilies, 3, 1/*min versions*/,
          1/*set ttl to 1*/), 0);
  EXPECT_EQ(0, putRow(table_name, "row1", row_data));
  HBASE_LOG_INFO("wait for puts..");
  ASSERT_TRUE(waitForPutsToComplete());
  HBASE_LOG_INFO("wait for puts completed");

  // sleep for 2 seconds so that ttl will be expired
  // but cell should not remove as min versions is 1
  sleep(2);
  std::vector<std::string> row1_valid_single_cell { "testcf1:co1:4" };
  EXPECT_EQ(getVerifyRow(table_name, "row1", row1_valid_single_cell), 0);
}

TEST_F (HBaseCAPI, set_get_log_level) {
  HBASE_LOG_INFO("setting and getting different log levels trade level");
  HBaseLogLevel level = HBASE_LOG_LEVEL_INFO;
  setLogLevel(level);
  EXPECT_EQ(getLogLevel(), HBASE_LOG_LEVEL_INFO);

  level = HBASE_LOG_LEVEL_TRACE;
  setLogLevel(level);
  EXPECT_EQ(getLogLevel(), HBASE_LOG_LEVEL_TRACE);

  level = (HBaseLogLevel) 9999; //set to invalid loglevel and it should not change log level

  setLogLevel(level);
  EXPECT_EQ(getLogLevel(), HBASE_LOG_LEVEL_TRACE);
}

TEST_F (HBaseCAPI, put_row_with_durability_async) {
  std::string table_name = "test_table";
  HBASE_LOG_INFO("*** hbase table put row with durablity test ***");
  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };
  std::vector<std::string> row_data { "testcf1:co1:4", "testcf1:col5:3",
    "testcf2:col1:6", "testcf1:col:6" };

  HBASE_LOG_INFO("creating table for get request:%s", table_name.c_str());
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  ASSERT_EQ(createTable((char*)table_name.c_str(), columnFamilies), 0);
  EXPECT_EQ(0, putRow(table_name, "row1", row_data, HBASE_LATEST_TIMESTAMP, DURABILITY_ASYNC_WAL));
  EXPECT_EQ(0, putRow(table_name, "row2", row_data, HBASE_LATEST_TIMESTAMP, DURABILITY_SYNC_WAL));
  EXPECT_EQ(0, putRow(table_name, "row3", row_data, HBASE_LATEST_TIMESTAMP, DURABILITY_SKIP_WAL));
  EXPECT_EQ(0, putRow(table_name, "row4", row_data, HBASE_LATEST_TIMESTAMP, DURABILITY_FSYNC_WAL));
  HBASE_LOG_INFO("wait for puts..");
  ASSERT_TRUE(waitForPutsToComplete());
  HBASE_LOG_INFO("wait for puts completed");

  EXPECT_EQ(0, getVerifyRow(table_name, "row1", row_data, true, false));
  EXPECT_EQ(0, getVerifyRow(table_name, "row2", row_data, true, false));
  EXPECT_EQ(0, getVerifyRow(table_name, "row3", row_data, true, false));
  EXPECT_EQ(0, getVerifyRow(table_name, "row4", row_data, true, false));
}

TEST_F (HBaseCAPI, get_entire_row_disabled_table_async) {
  std::string table_name = "test_table";

  HBASE_LOG_INFO("*** hbase table get entire row from a disabled table***");
  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };
  std::vector<std::string> row_data { "testcf1:co1:4", "testcf1:col5:3", "testcf2:col1:6",
      "testcf1:col:6" };

  HBASE_LOG_INFO("creating table for get request:%s ", table_name.c_str());
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  ASSERT_EQ(createTable((char*)table_name.c_str(), columnFamilies), 0);
  EXPECT_EQ(0, putRow(table_name, "row1", row_data));
  HBASE_LOG_INFO("wait for puts..");
  ASSERT_TRUE(waitForPutsToComplete());
  HBASE_LOG_INFO("wait for puts completed");

  // disable table now
  ASSERT_EQ(disableTable((char*)table_name.c_str()), 0);
  // FIXME: this one currently return HBASE_INTERNAL_ERR
  // instead of HBASE_TABLE_DISABLED, require fix in region server
  fprintf(stdout, "**** Attempting to fetch a row from a disabled table. This is going to take a while ****\n");
  fflush(stdout);
  EXPECT_EQ(HBASE_INTERNAL_ERR, getVerifyRow(table_name, "row1", row_data, true, false));
}

TEST_F (HBaseCAPI, client_flush) {
  std::string table_name = "flush_table";

  HBASE_LOG_INFO("*** hbase table get entire row test ***");
  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };
  std::vector<std::string> row_data { "testcf1:co1:4", "testcf1:col5:3", "testcf2:col1:6",
      "testcf1:col:6" };

  HBASE_LOG_INFO("creating table for get request:%s ", table_name.c_str());
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  ASSERT_EQ(createTable((char*)table_name.c_str(), columnFamilies), 0);
  EXPECT_EQ(0, putRow(table_name, "row1", row_data));
  EXPECT_EQ(0, putRow(table_name, "row2", row_data));
  EXPECT_EQ(0, putRow(table_name, "row3", row_data));
  HBASE_LOG_INFO("doing clientFlush");
  EXPECT_EQ(0, clientFlush());
  HBASE_LOG_INFO("flush Completed");
  HBASE_LOG_INFO("wait for puts..");
  ASSERT_TRUE(waitForPutsToComplete());
  HBASE_LOG_INFO("wait for puts completed");

  EXPECT_EQ(0, getVerifyRow(table_name, "row1", row_data, true, false));
}

TEST_F (HBaseCAPI, get_entire_cf_async) {
  std::string table_name = "test_table";

  HBASE_LOG_INFO("*** hbase table get entire column family test ***");
  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };
  std::vector<std::string> row_data { "testcf1:co1:4", "testcf1:col5:3", "testcf2:col1:6",
      "testcf1:col:6" };

  HBASE_LOG_INFO("creating table for get request:%s ", table_name.c_str());
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  ASSERT_EQ(createTable((char*)table_name.c_str(), columnFamilies), 0);
  EXPECT_EQ(0, putRow(table_name, "row1", row_data));

  HBASE_LOG_INFO("wait for puts..");
  ASSERT_TRUE(waitForPutsToComplete());
  HBASE_LOG_INFO("wait for puts completed");

  std::vector<std::string> expected_data { "testcf1:co1:4", "testcf1:col5:3", "testcf1:col:6" };
  EXPECT_EQ(0, getVerifyRow(table_name, "row1", expected_data, false, true));
}

TEST_F (HBaseCAPI, get_test_with_invalid_cf_table_async) {
  std::string table_name = "test_table";
  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };
  std::vector<std::string> row1_data {
    "testcf1:co1:4", "testcf1:col5:3", "testcf2:col1:6", "testcf1:col:6" };
  std::vector<std::string> row2_data { "testcf1:co1:4", "testcf1:col2:3", "testcf2:col1:6" };

  HBASE_LOG_INFO("*** hbase table get test with invalid cf and with invalid table name ***");
  HBASE_LOG_INFO("creating table for get request:%s ", table_name.c_str());
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  ASSERT_EQ(createTable((char*)table_name.c_str(), columnFamilies), 0);
  EXPECT_EQ(0, putRow(table_name, "row1", row1_data));
  EXPECT_EQ(0, putRow(table_name, "row2", row2_data));
  HBASE_LOG_INFO("wait for puts..");
  ASSERT_TRUE(waitForPutsToComplete());
  HBASE_LOG_INFO("wait for puts completed");

  std::vector<std::string> invalid_CF { "none" };
  std::vector<std::string> valid_CF { "testcf1" };
  std::string table_name_invalid = "no_table_with_this_name";
  EXPECT_EQ(2, getVerifyRow(table_name, "row2", invalid_CF, false, true));
  EXPECT_EQ(2, getVerifyRow(table_name_invalid, "row1", valid_CF, true));
}

TEST_F(HBaseCAPI, get_test_with_versions) {
  std::string table_name = "test_table";
  HBASE_LOG_INFO("*** table get test with specified number of versions ***");
  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };
  std::vector<std::string> row1_data {
    "testcf1:co1:4", "testcf1:col5:3", "testcf2:col1:6", "testcf1:col:6" };

  HBASE_LOG_INFO("creating table for get request:%s ", table_name.c_str());
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  // create table with versions as 4
  ASSERT_EQ(createTable((char*)table_name.c_str(), columnFamilies, 4), 0);

  // perform putRow 4 times so that it creates the versions
  EXPECT_EQ(0, putRowAndWait(table_name, "row1", row1_data));
  EXPECT_EQ(0, putRowAndWait(table_name, "row1", row1_data));
  EXPECT_EQ(0, putRowAndWait(table_name, "row1", row1_data));
  EXPECT_EQ(0, putRowAndWait(table_name, "row1", row1_data));
  expectedNumberOfCellCount = 16; // 4 versions for 4 columns so expected cell count :16
  EXPECT_EQ(0, getVerifyRow(table_name, "row1", row1_data, false, false, 4));
  expectedNumberOfCellCount = 0;
}

TEST_F(HBaseCAPI, delete_singlecell_test_async) {
  std::string line;
  std::vector<std::string> words;
  std::string table_name = "test_table";

  HBASE_LOG_INFO("*** hbase table delete single cell test ***");
  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };
  std::vector<std::string> row1_data { "testcf1:col1:4", "testcf1:col5:3", "testcf2:col1:6" };
  std::vector<std::string> row2_data { "testcf1:col1:4", "testcf1:col2:3", "testcf2:col1:6" };
  std::vector<std::string> row3_data { "testcf1:col1:8", "testcf1:col2:9", "testcf1:col6:19",
      "testcf2:col1:10", "testcf2:col2:9" };
  HBASE_LOG_INFO("creating table:%s", table_name.c_str());
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  ASSERT_EQ(createTable((char*)table_name.c_str(), columnFamilies), 0);
  ASSERT_EQ(putRow(table_name, "row1", row1_data), 0);
  ASSERT_EQ(putRow(table_name, "row2", row2_data), 0);
  ASSERT_EQ(putRow(table_name, "row3", row3_data), 0);
  HBASE_LOG_INFO("wait for puts..");
  ASSERT_TRUE(waitForPutsToComplete());
  HBASE_LOG_INFO("wait for puts completed");

  std::vector<std::string> row1_valid_single_cell { "testcf1:col1" };
  std::vector<std::string> row2_valid_single_cell { "testcf1:col2" };

  EXPECT_EQ(getVerifyRow(table_name, "row1", row1_valid_single_cell), 0);
  EXPECT_EQ(deleteRow(table_name, "row1", row1_valid_single_cell), 0);
  EXPECT_EQ(getVerifyRow(table_name, "row1", row1_valid_single_cell), CELL_NOT_FOUND_ERR);

  EXPECT_EQ(getVerifyRow(table_name, "row2", row2_valid_single_cell), 0);
  EXPECT_EQ(deleteRow(table_name, "row2", row2_valid_single_cell), 0);
  EXPECT_EQ(getVerifyRow(table_name, "row2", row2_valid_single_cell), CELL_NOT_FOUND_ERR);
}

TEST_F(HBaseCAPI, delete_multicell_test_async) {
  std::string line;
  std::vector<std::string> words;
  std::string table_name = "test_table";

  HBASE_LOG_INFO("*** hbase table delete multiple cells test ***");
  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };
  std::vector<std::string> row1_data { "testcf1:col1:4", "testcf1:col5:3", "testcf2:col1:6" };
  std::vector<std::string> row2_data { "testcf1:col1:4", "testcf1:col2:3", "testcf2:col1:6" };
  std::vector<std::string> row3_data { "testcf1:col1:8", "testcf1:col2:9", "testcf1:col6:19",
      "testcf2:col1:10", "testcf2:col2:9" };
  HBASE_LOG_INFO("creating table:%s", table_name.c_str());
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  ASSERT_EQ(createTable((char*)table_name.c_str(), columnFamilies), 0);
  ASSERT_EQ(putRow(table_name, "row1", row1_data), 0);
  ASSERT_EQ(putRow(table_name, "row2", row2_data), 0);
  ASSERT_EQ(putRow(table_name, "row3", row3_data), 0);
  HBASE_LOG_INFO("wait for puts..");
  ASSERT_TRUE(waitForPutsToComplete());
  HBASE_LOG_INFO("wait for puts completed");

  std::vector<std::string> row1_valid_multi_cell { "testcf1:col1", "testcf2:col1" };
  std::vector<std::string> row1_valid_multi_cell_1 { "testcf1:col1" };
  std::vector<std::string> row1_valid_multi_cell_2 { "testcf2:col1" };

  EXPECT_EQ(getVerifyRow(table_name, "row1", row1_valid_multi_cell), 0);
  EXPECT_EQ(deleteRow(table_name, "row1", row1_valid_multi_cell), 0);
  EXPECT_EQ(getVerifyRow(table_name, "row1", row1_valid_multi_cell), CELL_NOT_FOUND_ERR);
  EXPECT_EQ(getVerifyRow(table_name, "row1", row1_valid_multi_cell_1), CELL_NOT_FOUND_ERR);
  EXPECT_EQ(getVerifyRow(table_name, "row1", row1_valid_multi_cell_2), CELL_NOT_FOUND_ERR);
}

TEST_F(HBaseCAPI, delete_entire_cf_test_async) {
  std::string table_name = "test_table";

  HBASE_LOG_INFO("*** hbase table delete entire cf test ***");
  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };
  std::vector<std::string> row1_data { "testcf1:col1:4", "testcf1:col5:3", "testcf2:col1:6" };
  std::vector<std::string> row2_data { "testcf1:col1:4", "testcf1:col2:3", "testcf2:col1:6" };
  std::vector<std::string> row3_data { "testcf1:col1:8", "testcf1:col2:9", "testcf1:col6:19",
      "testcf2:col1:10", "testcf2:col2:9" };

  HBASE_LOG_INFO("creating table:%s", table_name.c_str());
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  ASSERT_EQ(createTable((char*)table_name.c_str(), columnFamilies), 0);
  ASSERT_EQ(putRow(table_name, "row1", row1_data), 0);
  ASSERT_EQ(putRow(table_name, "row2", row2_data), 0);
  ASSERT_EQ(putRow(table_name, "row3", row3_data), 0);
  HBASE_LOG_INFO("wait for puts..");
  ASSERT_TRUE(waitForPutsToComplete());
  HBASE_LOG_INFO("wait for puts completed");

  std::vector<std::string> row1_valid_CF { "testcf1" };
  std::vector<std::string> row1_valid_CF_cells { "testcf1:col1:4", "testcf1:col5:3" };
  std::vector<std::string> row1_valid_CF_cells_1 { "testcf1:col1:4" };
  std::vector<std::string> row1_valid_CF_cells_2 { "testcf1:col5:3" };

  EXPECT_EQ(getVerifyRow(table_name, "row1", row1_valid_CF_cells), 0);
  EXPECT_EQ(deleteRow(table_name, "row1", row1_valid_CF), 0);

  // now verify the cells of the row of one column family which is deleted in previous operation
  EXPECT_EQ(getVerifyRow(table_name, "row1", row1_valid_CF_cells), CELL_NOT_FOUND_ERR);
}

TEST_F(HBaseCAPI, delete_entire_row_test_async) {
  std::string table_name = "test_table";

  HBASE_LOG_INFO("*** hbase table delete entire row  test ***");
  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };
  std::vector<std::string> row1_data { "testcf1:col1:4", "testcf1:col5:3", "testcf2:col1:6" };
  std::vector<std::string> row2_data { "testcf1:col1:4", "testcf1:col2:3", "testcf2:col1:6" };
  std::vector<std::string> row3_data { "testcf1:col1:8", "testcf1:col2:9", "testcf1:col6:19",
      "testcf2:col1:10", "testcf2:col2:9" };
  HBASE_LOG_INFO("creating table:%s", table_name.c_str());
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  ASSERT_EQ(createTable((char*)table_name.c_str(), columnFamilies), 0);
  ASSERT_EQ(putRow(table_name, "row1", row1_data), 0);
  ASSERT_EQ(putRow(table_name, "row2", row2_data), 0);
  ASSERT_EQ(putRow(table_name, "row3", row3_data), 0);
  HBASE_LOG_INFO("wait for puts..");
  ASSERT_TRUE(waitForPutsToComplete());
  HBASE_LOG_INFO("wait for puts completed");

  std::vector<std::string> delete_entire_row { "" };
  std::vector<std::string> row1_valid_cells_1 { "testcf1:col1:4" };
  std::vector<std::string> row1_valid_cells_2 { "testcf1:col5:3" };
  std::vector<std::string> row1_valid_cells_3 { "testcf2:col1" };

  // before deleting
  EXPECT_EQ(getVerifyRow(table_name, "row1", row1_data), 0);
  // delete entire row
  EXPECT_EQ(deleteRow(table_name, "row1", delete_entire_row), 0);
  // verifying all the columns of entire row
  EXPECT_EQ(getVerifyRow(table_name, "row1", row1_valid_cells_1), CELL_NOT_FOUND_ERR);
  EXPECT_EQ(getVerifyRow(table_name, "row1", row1_valid_cells_2), CELL_NOT_FOUND_ERR);
  EXPECT_EQ(getVerifyRow(table_name, "row1", row1_valid_cells_3), CELL_NOT_FOUND_ERR);
}

TEST_F(HBaseCAPI, delete_nonexisting_row_test_async) {
  std::string table_name = "del_nerow_t1";

  HBASE_LOG_INFO("*** hbase table delete non existing row test ***");
  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };
  std::vector<std::string> row1_data { "testcf1:col1:4", "testcf1:col5:3", "testcf2:col1:6" };
  std::vector<std::string> row2_data { "testcf1:col1:4", "testcf1:col2:3", "testcf2:col1:6" };
  std::vector<std::string> row3_data { "testcf1:col1:8", "testcf1:col2:9", "testcf1:col6:19",
      "testcf2:col1:10", "testcf2:col2:9" };
  HBASE_LOG_INFO("creating table:%s", table_name.c_str());
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  ASSERT_EQ(createTable((char*)table_name.c_str(), columnFamilies), 0);
  ASSERT_EQ(putRow(table_name, "row1", row1_data), 0);
  ASSERT_EQ(putRow(table_name, "row2", row2_data), 0);
  ASSERT_EQ(putRow(table_name, "row3", row3_data), 0);
  HBASE_LOG_INFO("wait for puts..");
  ASSERT_TRUE(waitForPutsToComplete());
  HBASE_LOG_INFO("wait for puts completed");

  // make sure row not exists
  EXPECT_EQ(getVerifyRow(table_name, "nonexistingrow", row1_data), CELL_NOT_FOUND_ERR);

  // try deleting non existing row - hbase returns zero for non existing row
  EXPECT_EQ(deleteRow(table_name, "nonexistingrow", row1_data), 0);
}

TEST_F(HBaseCAPI, delete_get_non_existing_cf_test_async) {
  std::string table_name = "test_table";

  HBASE_LOG_INFO("*** hbase table delete and get for non existing cf test ***");
  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };
  std::vector<std::string> row1_data { "testcf1:col1:4", "testcf1:col5:3", "testcf2:col1:6" };
  std::vector<std::string> row2_data { "testcf1:col1:4", "testcf1:col2:3", "testcf2:col1:6" };
  std::vector<std::string> row3_data { "testcf1:col1:8", "testcf1:col2:9", "testcf1:col6:19",
      "testcf2:col1:10", "testcf2:col2:9" };
  HBASE_LOG_INFO("creating table:%s", table_name.c_str());
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  ASSERT_EQ(createTable((char*)table_name.c_str(), columnFamilies), 0);
  // putting the data
  ASSERT_EQ(putRow(table_name, "row1", row1_data), 0);
  ASSERT_EQ(putRow(table_name, "row2", row2_data), 0);
  ASSERT_EQ(putRow(table_name, "row3", row3_data), 0);
  HBASE_LOG_INFO("wait for puts..");
  ASSERT_TRUE(waitForPutsToComplete());
  HBASE_LOG_INFO("wait for puts completed");

  std::vector<std::string> row1_non_existing_cf { "none:col1" };

  // non existing cf should give 2 error for both delete and get

  EXPECT_EQ(getVerifyRow(table_name, "row1", row1_non_existing_cf), 2);
  EXPECT_EQ(deleteRow(table_name, "row1", row1_non_existing_cf), 2);
}

TEST_F(HBaseCAPI, delete_test_ts_async) {
  std::string table_name = "test_table";

  HBASE_LOG_INFO("*** hbase table delete with time stamp test ***");
  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };
  std::vector<std::string> row2_data_ts { "testcf1:col1:4000",
    "testcf1:col2:3000", "testcf2:col1:6000" };
  std::vector<std::string> row2_data { "testcf1:col1:4", "testcf1:col2:3", "testcf2:col1:6" };

  HBASE_LOG_INFO("creating table:%s", table_name.c_str());
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  ASSERT_EQ(createTable((char*)table_name.c_str(), columnFamilies), 0);

  // putting the row with ts ->1000
  ASSERT_EQ(putRowAndWait(table_name, "row2", row2_data_ts, 1000), 0);

  // putting the row with latest time stamp for same cells but different values
  ASSERT_EQ(putRowAndWait(table_name, "row2", row2_data), 0);

  std::vector<std::string> row1_data_cell1 { "testcf1:col6:3" };
  std::vector<std::string> row1_data_cell2 { "testcf1:col1:3" };
  std::vector<std::string> row2_data_cell3 { "testcf1:col1:4" };
  std::vector<std::string> cf1 { "testcf1" };
  std::vector<std::string> empty { "" };

  // total number of cells 3 with two diffeent ts -> expected cell count 6
  expectedNumberOfCellCount = 6;
  EXPECT_EQ(getVerifyRow(table_name, "row2", row2_data), 0);

  // delete the row with ts->2000 so that ts with 1000 will be deleted
  EXPECT_EQ(deleteRow(table_name, "row2", row2_data, 2000), 0);
  // MAPR-13104
  // still it should keep the values with latest time stamp,
  expectedNumberOfCellCount = 3;
  EXPECT_EQ(getVerifyRow(table_name, "row2", row2_data), 0);
  expectedNumberOfCellCount = 0;
}

TEST_F(HBaseCAPI, scan_table_async) {
  std::string table_name = "scan_table";

  HBASE_LOG_INFO("*** table scan ***");
  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };
  std::vector<std::string> row_data { "testcf1:col1:4", "testcf1:col2:3", "testcf2:col1:6" };

  HBASE_LOG_INFO("creating table:%s", table_name.c_str());
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  ASSERT_EQ(createTable((char*)table_name.c_str(), columnFamilies), 0);
  // put 1000 records
  for (int i = 0; i < 1000; i++) {
    char row_key[20] = "";
    sprintf((char*) row_key, "%s_%d", "user", i);
    std::string rowKey(row_key);
    ASSERT_EQ(putRow(table_name, rowKey, row_data), 0);
  }

  HBASE_LOG_INFO("wait for puts..");
  waitForPutsToComplete();
  HBASE_LOG_INFO("wait for puts completed");

  expectedNumberOfRowCount = 1000;
  expectedNumberOfCellCount = 3000;
  HBASE_LOG_INFO("scan table expected no of rows:%lu expected no of cells:%lu",
      expectedNumberOfRowCount, expectedNumberOfCellCount);
  EXPECT_EQ(scanTable(table_name), 0);
  expectedNumberOfRowCount = 0;  //reset
  EXPECT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
}

TEST_F(HBaseCAPI, scan_table_with_non_existing_table) {
  std::string table_name = "scan_table";

  HBASE_LOG_INFO("*** table scan with non existing table***");
  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };

  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  expectedNumberOfRowCount = 0;
  expectedNumberOfCellCount = 0;
  HBASE_LOG_INFO("scan table for non existing table with expected no of rows:",
      expectedNumberOfRowCount, " expected no of cells", expectedNumberOfCellCount);
  EXPECT_EQ(scanTable(table_name), 2);
}

TEST_F(HBaseCAPI, scan_empty_table_async) {
  std::string table_name = "scan_table";

  HBASE_LOG_INFO("*** table scan for empty table ***");
  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };

  HBASE_LOG_INFO("creating table:%s", table_name.c_str());
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  ASSERT_EQ(createTable((char*)table_name.c_str(), columnFamilies), 0);

  expectedNumberOfRowCount = 0;
  expectedNumberOfCellCount = 0;
  HBASE_LOG_INFO("scan table for empty table with expected no of rows:%ld",
      expectedNumberOfRowCount);
  EXPECT_EQ(scanTable(table_name), 0);
}

TEST_F(HBaseCAPI, scan_table_with_versions) {
  std::string table_name = "scan_table_with_verions";

  HBASE_LOG_INFO("*** table scan with versions **");
  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };
  std::vector<std::string> row_data { "testcf1:col1:4", "testcf1:col2:3", "testcf2:col1:6" };

  HBASE_LOG_INFO("creating table:%s", table_name.c_str());
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  ASSERT_EQ(createTable((char*)table_name.c_str(), columnFamilies), 0);
  // put 1000 records 3 times so that 3 versions will be created
  for (int k = 0; k < 3; k++) {
    for (int i = 0; i < 1000; i++) {
      char row_key[20] = "";
      sprintf((char*) row_key, "%s_%d", "user", i);
      std::string rowKey(row_key);
      ASSERT_EQ(putRow(table_name, rowKey, row_data), 0);
    }
  }
  HBASE_LOG_INFO("wait for puts..");
  waitForPutsToComplete();
  HBASE_LOG_INFO("wait for puts completed");

  expectedNumberOfRowCount = 1000;
  expectedNumberOfCellCount = 9000;
  HBASE_LOG_INFO("scan table with verions:3 with expected no of rows:%lu"
      "expected Number of Cells:%lu", expectedNumberOfRowCount, expectedNumberOfCellCount);
  EXPECT_EQ(scanTable(table_name, 3/*versions*/), 0);

  expectedNumberOfRowCount = 1000;
  expectedNumberOfCellCount = 6000;
  HBASE_LOG_INFO("scan table with verions:2 with expected no of rows:%lu"
      "expected Number of Cells:%lu", expectedNumberOfRowCount, expectedNumberOfCellCount);

  EXPECT_EQ(scanTable(table_name, 2/*versions*/), 0);

  expectedNumberOfRowCount = 1000;
  expectedNumberOfCellCount = 3000;
  HBASE_LOG_INFO("scan table with verions:1 with expected no of rows:%lu"
      " expected Number of Cells:%lu", expectedNumberOfRowCount, expectedNumberOfCellCount);

  EXPECT_EQ(scanTable(table_name, 1/*versions*/), 0);
  expectedNumberOfRowCount = 0;  //reset
  expectedNumberOfCellCount = 0;
}

TEST_F(HBaseCAPI, scan_table_with_max_num_rows) {
  std::string table_name = "scan_table_num_rows";

  HBASE_LOG_INFO("*** table scan  max number of rows **");
  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };
  std::vector<std::string> row_data { "testcf1:col1:4", "testcf1:col2:3", "testcf2:col1:6" };

  HBASE_LOG_INFO("creating table:%s", table_name.c_str());
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  ASSERT_EQ(createTable((char*)table_name.c_str(), columnFamilies), 0);
  for (int i = 0; i < 1000; i++) {
    char row_key[20] = "";
    sprintf((char*) row_key, "%s_%d", "user", i);
    std::string rowKey(row_key);
    ASSERT_EQ(putRow(table_name, rowKey, row_data), 0);
  }
  HBASE_LOG_INFO("wait for puts..");
  waitForPutsToComplete();
  HBASE_LOG_INFO("wait for puts completed");

  expectedNumberOfRowCount = 1000;
  maxNumberOfRows = 1000;
  HBASE_LOG_INFO("scan table with verions:3 maxnorows:%lu with expected no of row:%lu",
      maxNumberOfRows, expectedNumberOfRowCount);
  EXPECT_EQ(scanTable(table_name, 1/*versions*/, maxNumberOfRows), 0);

  expectedNumberOfRowCount = 1000;
  maxNumberOfRows = 50;
  HBASE_LOG_INFO("scan table with verions:3 maxnorows:%lu with expected no of row:%lu",
      maxNumberOfRows, expectedNumberOfRowCount);
  EXPECT_EQ(scanTable(table_name, 3/*versions*/, maxNumberOfRows), 0);

  expectedNumberOfRowCount = 1000;
  maxNumberOfRows = 10000;
  HBASE_LOG_INFO("scan table with verions:3 maxnorows:%lu with expected no of row:%lu",
      maxNumberOfRows, expectedNumberOfRowCount);
  EXPECT_EQ(scanTable(table_name, 3/*versions*/, maxNumberOfRows), 0);

  maxNumberOfRows = 0;  //reset
}

TEST_F(HBaseCAPI, scan_table_with_row_start_key) {
  std::string table_name = "scan_table";
  std::string start_key = "";

  HBASE_LOG_INFO("*** table scan with specified start row key ***");
  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };

  std::vector<std::string> row_data { "testcf1:col1:4", "testcf1:col2:3", "testcf2:col1:6" };

  HBASE_LOG_INFO("creating table:%s", table_name.c_str());
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  ASSERT_EQ(createTable((char*)table_name.c_str(), columnFamilies), 0);
  // put 1000 records
  for (int i = 0; i < 1000; i++) {
    char row_key[20] = "";
    sprintf((char*) row_key, "%s_%d", "user", i);
    std::string rowKey(row_key);
    ASSERT_EQ(putRow(table_name, rowKey, row_data), 0);
  }
  HBASE_LOG_INFO("wait for puts..");
  waitForPutsToComplete();
  HBASE_LOG_INFO("wait for puts completed");

  start_key = "user_499";
  expectedNumberOfRowCount = 556;
  HBASE_LOG_INFO(" scan table with start key:%s expected rows:%lu", start_key.c_str(),
      expectedNumberOfRowCount);
  EXPECT_EQ(scanTable(table_name, 1, 10000, start_key), 0);

  start_key = "user_0";
  expectedNumberOfRowCount = 1000;
  HBASE_LOG_INFO(" scan table with start key:%s expected rows:%lu", start_key.c_str(),
      expectedNumberOfRowCount);
  EXPECT_EQ(scanTable(table_name, 1, 10000, start_key), 0);

  start_key = "user_999";
  expectedNumberOfRowCount = 1;
  HBASE_LOG_INFO(" scan table with start key:%s expected rows:%lu", start_key.c_str(),
      expectedNumberOfRowCount);
  EXPECT_EQ(scanTable(table_name, 1, 10000, start_key), 0);

  EXPECT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
}

TEST_F(HBaseCAPI, scan_table_with_invalid_start_key) {
  std::string table_name = "scan_table";
  std::string start_key = "";

  HBASE_LOG_INFO("*** table scan with invalid start row key ***");
  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };
  std::vector<std::string> row_data {
    "testcf1:col1:4", "testcf1:col2:3", "testcf2:col1:6" };

  HBASE_LOG_INFO("creating table:%s", table_name.c_str());
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  ASSERT_EQ(createTable((char*)table_name.c_str(), columnFamilies), 0);
  // put 1000 records
  for (int i = 0; i < 1000; i++) {
    char row_key[20] = "";
    sprintf((char*) row_key, "%s_%d", "user", i);
    std::string rowKey(row_key);
    ASSERT_EQ(putRow(table_name, rowKey, row_data), 0);
  }
  HBASE_LOG_INFO("wait for puts..");
  waitForPutsToComplete();
  HBASE_LOG_INFO("wait for puts completed");

  start_key = "user_99999";
  expectedNumberOfRowCount = 0;
  HBASE_LOG_INFO(" scan table with start key:%s expected rows:%lu",
      start_key.c_str(), expectedNumberOfRowCount);
  EXPECT_EQ(scanTable(table_name, 1, 10000, start_key), 0);
}

TEST_F(HBaseCAPI, scan_table_with_start_end_key) {
  std::string table_name = "scan_table";
  std::string start_key = "";
  std::string end_key = "";

  HBASE_LOG_INFO("*** table scan with start and end row key ***");
  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };
  std::vector<std::string> row_data {
    "testcf1:col1:4", "testcf1:col2:3", "testcf2:col1:6" };

  HBASE_LOG_INFO("creating table:", table_name.c_str());
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  ASSERT_EQ(createTable((char*)table_name.c_str(), columnFamilies), 0);
  // put 1000 records
  for (int i = 0; i < 1000; i++) {
    char row_key[20] = "";
    sprintf((char*) row_key, "%s_%d", "user", i);
    std::string rowKey(row_key);
    ASSERT_EQ(putRow(table_name, rowKey, row_data), 0);
  }
  HBASE_LOG_INFO("wait for puts..");
  waitForPutsToComplete();
  HBASE_LOG_INFO("wait for puts completed");

  expectedNumberOfRowCount = 999;
  start_key = "user_0";
  end_key = "user_999"; //exclusive
  HBASE_LOG_INFO(" scan table with start key:%s endkey:%s expected rows:%lu",
      start_key.c_str(), end_key.c_str(), expectedNumberOfRowCount);
  EXPECT_EQ(scanTable(table_name, 1, 10000, start_key, end_key), 0);
  expectedNumberOfCellCount = 0; //reset

  expectedNumberOfRowCount = 552;
  start_key = "user_500";
  end_key = "user_999";
  HBASE_LOG_INFO(" scan table with start key:%s endkey:%s expected rows:%lu",
      start_key.c_str(), end_key.c_str(), expectedNumberOfRowCount);
  EXPECT_EQ(scanTable(table_name, 1, 10000, start_key, end_key), 0);

  expectedNumberOfRowCount = 0; //reset
  EXPECT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
}

TEST_F(HBaseCAPI, scan_table_with_invalid_start_end_key) {
  std::string table_name = "scan_table";
  std::string start_key = "";
  std::string end_key = "";

  HBASE_LOG_INFO("*** table scan with start and end row key ***");
  std::vector<std::string> columnFamilies { "testcf1", "testcf2" };
  std::vector<std::string> row_data {
    "testcf1:col1:4", "testcf1:col2:3", "testcf2:col1:6" };

  HBASE_LOG_INFO("creating table:", table_name.c_str());
  ASSERT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
  ASSERT_EQ(createTable((char*)table_name.c_str(), columnFamilies), 0);
  // put 1000 records
  for (int i = 0; i < 1000; i++) {
    char row_key[20] = "";
    sprintf((char*) row_key, "%s_%d", "user", i);
    std::string rowKey(row_key);
    ASSERT_EQ(putRow(table_name, rowKey, row_data), 0);
  }
  HBASE_LOG_INFO("wait for puts..");
  waitForPutsToComplete();
  HBASE_LOG_INFO("wait for puts completed");

  // end key is less than start key
  expectedNumberOfRowCount = 0;

  start_key = "user_995";
  end_key = "user_0";
  HBASE_LOG_INFO(" scan table with start key:%s endkey:%s expected rows:%lu",
      start_key.c_str(), end_key.c_str(), expectedNumberOfRowCount);
  EXPECT_EQ(scanTable(table_name, 1, 10000, start_key, end_key), 0);
  expectedNumberOfCellCount = 0; //reset

  // valid start key invalid end key
  expectedNumberOfRowCount = 0;
  start_key = "user_500";
  end_key = "user_1999";
  HBASE_LOG_INFO(" scan table with start key:%s endkey:%s expected rows:%lu",
      start_key.c_str(), end_key.c_str(), expectedNumberOfRowCount);
  EXPECT_EQ(scanTable(table_name, 1, 10000, start_key, end_key), 0);

  // with invalid start key valid end key
  expectedNumberOfRowCount = 0;
  start_key = "user_aaa";
  end_key = "user_999";
  HBASE_LOG_INFO(" scan table with start key:%s endkey:%s expected rows:%lu",
      start_key.c_str(), end_key.c_str(), expectedNumberOfRowCount);
  EXPECT_EQ(scanTable(table_name, 1, 10000, start_key, end_key), 0);

  EXPECT_EQ(deleteTableIfExists((char*)table_name.c_str()), 0);
}

/**
 * Test entry point.
 */
extern "C" int
main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

} /* namespace test */
} /* namespace hbase */
