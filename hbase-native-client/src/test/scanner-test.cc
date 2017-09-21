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

#include <folly/Conv.h>
#include <gtest/gtest.h>

#include <chrono>
#include <thread>
#include <vector>

#include "hbase/client/async-client-scanner.h"
#include "hbase/client/async-table-result-scanner.h"
#include "hbase/client/cell.h"
#include "hbase/client/client.h"
#include "hbase/client/configuration.h"
#include "hbase/client/filter.h"
#include "hbase/client/get.h"
#include "hbase/client/hbase-configuration-loader.h"
#include "hbase/client/put.h"
#include "hbase/client/result.h"
#include "hbase/client/row.h"
#include "hbase/client/table.h"
#include "hbase/if/Comparator.pb.h"
#include "hbase/if/Filter.pb.h"
#include "hbase/serde/table-name.h"
#include "hbase/test-util/test-util.h"
#include "hbase/utils/time-util.h"

using hbase::Cell;
using hbase::ComparatorFactory;
using hbase::Comparator;
using hbase::Configuration;
using hbase::Get;
using hbase::Put;
using hbase::Result;
using hbase::Scan;
using hbase::Table;
using hbase::TestUtil;
using hbase::TimeUtil;
using hbase::AsyncClientScanner;
using hbase::AsyncTableResultScanner;
using hbase::FilterFactory;
using hbase::pb::CompareType;

class ScannerTest : public ::testing::Test {
 public:
  static std::unique_ptr<hbase::TestUtil> test_util;
  static const uint32_t num_rows;

  static void SetUpTestCase() {
    google::InstallFailureSignalHandler();
    test_util = std::make_unique<hbase::TestUtil>();
    test_util->StartMiniCluster(2);
  }
};
std::unique_ptr<hbase::TestUtil> ScannerTest::test_util = nullptr;
const uint32_t ScannerTest::num_rows = 1000;

std::string Family(uint32_t i) { return "f" + folly::to<std::string>(i); }

std::string Row(uint32_t i, int width) {
  std::ostringstream s;
  s.fill('0');
  s.width(width);
  s << i;
  return "row" + s.str();
}

std::string Row(uint32_t i) { return Row(i, 3); }

std::unique_ptr<Put> MakePut(const std::string &row, uint32_t num_families) {
  auto put = std::make_unique<Put>(row);

  for (uint32_t i = 0; i < num_families; i++) {
    put->AddColumn(Family(i), "q1", row);
    put->AddColumn(Family(i), "q2", row + "-" + row);
  }

  return std::move(put);
}

void CheckResult(const Result &r, std::string expected_row, uint32_t num_families) {
  VLOG(1) << r.DebugString();
  auto row = r.Row();
  ASSERT_EQ(row, expected_row);
  ASSERT_EQ(r.Cells().size(), num_families * 2);
  for (uint32_t i = 0; i < num_families; i++) {
    ASSERT_EQ(*r.Value(Family(i), "q1"), row);
    ASSERT_EQ(*r.Value(Family(i), "q2"), row + "-" + row);
  }
}

void CreateTable(std::string table_name, uint32_t num_families, uint32_t num_rows,
                 int32_t num_regions) {
  LOG(INFO) << "Creating the table " << table_name
            << " with num_regions:" << folly::to<std::string>(num_regions);
  std::vector<std::string> families;
  for (uint32_t i = 0; i < num_families; i++) {
    families.push_back(Family(i));
  }
  if (num_regions <= 1) {
    ScannerTest::test_util->CreateTable(table_name, families);
  } else {
    std::vector<std::string> keys;
    for (int32_t i = 0; i < num_regions - 1; i++) {
      keys.push_back(Row(i * (num_rows / (num_regions - 1))));
      LOG(INFO) << "Split key:" << keys[keys.size() - 1];
    }
    ScannerTest::test_util->CreateTable(table_name, families, keys);
  }
}

std::unique_ptr<hbase::Client> CreateTableAndWriteData(std::string table_name,
                                                       uint32_t num_families, uint32_t num_rows,
                                                       int32_t num_regions) {
  CreateTable(table_name, num_families, num_rows, num_regions);
  auto tn = folly::to<hbase::pb::TableName>(table_name);
  auto client = std::make_unique<hbase::Client>(*ScannerTest::test_util->conf());
  auto table = client->Table(tn);

  LOG(INFO) << "Writing data to the table, num_rows:" << num_rows;
  // Perform Puts
  for (uint32_t i = 0; i < num_rows; i++) {
    table->Put(*MakePut(Row(i), num_families));
  }
  return std::move(client);
}

void TestScan(const Scan &scan, uint32_t num_families, int32_t start, int32_t num_rows,
              Table *table) {
  LOG(INFO) << "Starting scan for the test with start:" << scan.StartRow()
            << ", stop:" << scan.StopRow() << " expected_num_rows:" << num_rows;
  auto scanner = table->Scan(scan);

  uint32_t i = start;
  auto r = scanner->Next();
  while (r != nullptr) {
    CheckResult(*r, Row(i++), num_families);
    r = scanner->Next();
  }
  ASSERT_EQ(i - start, num_rows);
}

void TestScan(const Scan &scan, int32_t start, int32_t num_rows, Table *table) {
  TestScan(scan, 1, start, num_rows, table);
}

void TestScan(uint32_t num_families, int32_t start, int32_t stop, int32_t num_rows, Table *table) {
  Scan scan{};
  if (start >= 0) {
    scan.SetStartRow(Row(start));
  } else {
    start = 0;  // neded for below logic
  }
  if (stop >= 0) {
    scan.SetStopRow(Row(stop));
  }

  TestScan(scan, num_families, start, num_rows, table);
}

void TestScan(int32_t start, int32_t stop, int32_t num_rows, Table *table) {
  TestScan(1, start, stop, num_rows, table);
}

void TestScan(uint32_t num_families, std::string start, std::string stop, int32_t num_rows,
              Table *table) {
  Scan scan{};

  scan.SetStartRow(start);
  scan.SetStopRow(stop);

  LOG(INFO) << "Starting scan for the test with start:" << start << ", stop:" << stop
            << " expected_num_rows:" << num_rows;
  auto scanner = table->Scan(scan);

  uint32_t i = 0;
  auto r = scanner->Next();
  while (r != nullptr) {
    VLOG(1) << r->DebugString();
    i++;
    ASSERT_EQ(r->Map().size(), num_families);
    r = scanner->Next();
  }
  ASSERT_EQ(i, num_rows);
}

void TestScan(std::string start, std::string stop, int32_t num_rows, Table *table) {
  TestScan(1, start, stop, num_rows, table);
}

void TestScanCombinations(Table *table, uint32_t num_families) {
  // full table
  TestScan(num_families, -1, -1, 1000, table);
  TestScan(num_families, -1, 999, 999, table);
  TestScan(num_families, 0, -1, 1000, table);
  TestScan(num_families, 0, 999, 999, table);
  TestScan(num_families, 10, 990, 980, table);
  TestScan(num_families, 1, 998, 997, table);

  TestScan(num_families, 123, 345, 222, table);
  TestScan(num_families, 234, 456, 222, table);
  TestScan(num_families, 345, 567, 222, table);
  TestScan(num_families, 456, 678, 222, table);

  // single results
  TestScan(num_families, 111, 111, 1, table);  // split keys are like 111, 222, 333, etc
  TestScan(num_families, 111, 112, 1, table);
  TestScan(num_families, 332, 332, 1, table);
  TestScan(num_families, 332, 333, 1, table);
  TestScan(num_families, 333, 333, 1, table);
  TestScan(num_families, 333, 334, 1, table);
  TestScan(num_families, 42, 42, 1, table);
  TestScan(num_families, 921, 921, 1, table);
  TestScan(num_families, 0, 0, 1, table);
  TestScan(num_families, 0, 1, 1, table);
  TestScan(num_families, 999, 999, 1, table);

  // few results
  TestScan(num_families, 0, 0, 1, table);
  TestScan(num_families, 0, 2, 2, table);
  TestScan(num_families, 0, 5, 5, table);
  TestScan(num_families, 10, 15, 5, table);
  TestScan(num_families, 105, 115, 10, table);
  TestScan(num_families, 111, 221, 110, table);
  TestScan(num_families, 111, 222, 111, table);  // crossing region boundary 111-222
  TestScan(num_families, 111, 223, 112, table);
  TestScan(num_families, 111, 224, 113, table);
  TestScan(num_families, 990, 999, 9, table);
  TestScan(num_families, 900, 998, 98, table);

  // empty results
  TestScan(num_families, "a", "a", 0, table);
  TestScan(num_families, "a", "r", 0, table);
  TestScan(num_families, "", "r", 0, table);
  TestScan(num_families, "s", "", 0, table);
  TestScan(num_families, "s", "z", 0, table);
  TestScan(num_families, Row(110) + "a", Row(111), 0, table);
  TestScan(num_families, Row(111) + "a", Row(112), 0, table);
  TestScan(num_families, Row(123) + "a", Row(124), 0, table);

  // custom
  TestScan(num_families, Row(111, 3), Row(1111, 4), 1, table);
  TestScan(num_families, Row(0, 3), Row(0, 4), 1, table);
  TestScan(num_families, Row(999, 3), Row(9999, 4), 1, table);
  TestScan(num_families, Row(111, 3), Row(1111, 4), 1, table);
  TestScan(num_families, Row(0, 3), Row(9999, 4), 1000, table);
  TestScan(num_families, "a", "z", 1000, table);
}

// some of these tests are from TestAsyncTableScan* and some from TestFromClientSide* and
// TestScannersFromClientSide*

TEST_F(ScannerTest, SingleRegionScan) {
  auto client = CreateTableAndWriteData("t_single_region_scan", 1, num_rows, 1);
  auto table = client->Table(folly::to<hbase::pb::TableName>("t_single_region_scan"));

  TestScanCombinations(table.get(), 1);
}

TEST_F(ScannerTest, MultiRegionScan) {
  auto client = CreateTableAndWriteData("t_multi_region_scan", 1, num_rows, 10);
  auto table = client->Table(folly::to<hbase::pb::TableName>("t_multi_region_scan"));

  TestScanCombinations(table.get(), 1);
}

TEST_F(ScannerTest, ScanWithPauses) {
  auto max_result_size =
      ScannerTest::test_util->conf()->GetInt("hbase.client.scanner.max.result.size", 2097152);
  ScannerTest::test_util->conf()->SetInt("hbase.client.scanner.max.result.size", 100);
  auto client = CreateTableAndWriteData("t_multi_region_scan", 1, num_rows, 5);
  auto table = client->Table(folly::to<hbase::pb::TableName>("t_multi_region_scan"));

  VLOG(1) << "Starting scan for the test";
  Scan scan{};
  scan.SetCaching(100);
  auto scanner = table->Scan(scan);

  uint32_t i = 0;
  auto r = scanner->Next();
  while (r != nullptr) {
    CheckResult(*r, Row(i++), 1);
    r = scanner->Next();
    std::this_thread::sleep_for(TimeUtil::MillisToNanos(10));
  }

  auto s = static_cast<AsyncTableResultScanner *>(scanner.get());
  ASSERT_GT(s->num_prefetch_stopped(), 0);

  ASSERT_EQ(i, num_rows);
  ScannerTest::test_util->conf()->SetInt("hbase.client.scanner.max.result.size", max_result_size);
}

TEST_F(ScannerTest, ScanWithFilters) {
  auto client = CreateTableAndWriteData("t_scan_with_filters", 1, num_rows, 1);
  auto table = client->Table(folly::to<hbase::pb::TableName>("t_scan_with_filters"));

  Scan scan{};
  scan.SetFilter(FilterFactory::ValueFilter(CompareType::GREATER_OR_EQUAL,
                                            *ComparatorFactory::BinaryComparator(Row(800))));

  TestScan(scan, 800, 200, table.get());
}

TEST_F(ScannerTest, ScanMultiFamily) {
  auto client = CreateTableAndWriteData("t_scan_multi_family", 3, num_rows, 1);
  auto table = client->Table(folly::to<hbase::pb::TableName>("t_scan_multi_family"));

  TestScanCombinations(table.get(), 3);
}

TEST_F(ScannerTest, ScanNullQualifier) {
  std::string table_name{"t_scan_null_qualifier"};
  std::string row{"row"};
  CreateTable(table_name, 1, 1, 1);

  auto tn = folly::to<hbase::pb::TableName>(table_name);
  auto client = std::make_unique<hbase::Client>(*ScannerTest::test_util->conf());
  auto table = client->Table(tn);

  // Perform Puts
  Put put{row};
  put.AddColumn(Family(0), "q1", row);
  put.AddColumn(Family(0), "", row);
  table->Put(put);

  Scan scan1{};
  scan1.AddColumn(Family(0), "");
  auto scanner1 = table->Scan(scan1);
  auto r1 = scanner1->Next();
  ASSERT_EQ(r1->Cells().size(), 1);
  ASSERT_EQ(scanner1->Next(), nullptr);

  Scan scan2{};
  scan2.AddFamily(Family(0));
  auto scanner2 = table->Scan(scan2);
  auto r2 = scanner2->Next();
  ASSERT_EQ(r2->Cells().size(), 2);
  ASSERT_EQ(scanner2->Next(), nullptr);
}

TEST_F(ScannerTest, ScanNoResults) {
  std::string table_name{"t_scan_no_results"};
  auto client = CreateTableAndWriteData(table_name, 1, num_rows, 3);
  auto table = client->Table(folly::to<hbase::pb::TableName>(table_name));

  Scan scan{};
  scan.AddColumn(Family(0), "non_existing_qualifier");

  TestScan(scan, 0, 0, table.get());
}
