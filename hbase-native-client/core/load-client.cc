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

#include <folly/Logging.h>
#include <folly/Random.h>
#include <gflags/gflags.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>

#include "core/client.h"
#include "core/get.h"
#include "core/put.h"
#include "core/table.h"
#include "serde/table-name.h"
#include "utils/time-util.h"

using hbase::Client;
using hbase::Configuration;
using hbase::Get;
using hbase::Put;
using hbase::Table;
using hbase::pb::TableName;
using hbase::TimeUtil;
using folly::Random;

DEFINE_string(table, "load_test_table", "What table to do the reads and writes with");
DEFINE_string(families, "f", "comma separated list of column family names");
DEFINE_string(conf, "", "Conf directory to read the config from (optional)");
DEFINE_string(zookeeper, "localhost:2181", "What zk quorum to talk to");
DEFINE_string(znode, "/hbase", "parent znode");
DEFINE_uint64(num_rows, 1'000'000, "How many rows to write and read");
DEFINE_uint64(num_cols, 1000, "How many columns there are in a row");
DEFINE_int32(threads, 10, "How many client threads");
DEFINE_int32(batch_num_rows, 100, "number of rows in one multi-get / multi-put");
DEFINE_uint64(report_num_rows, 5000, "How frequent we should report the progress");
DEFINE_bool(gets, true, "perform gets");
DEFINE_bool(scans, true, "perform scans");
DEFINE_bool(puts, true, "perform put's");
DEFINE_bool(appends, true, "perform append's");

static constexpr const char *kNumColumn = "num";
static constexpr const char *incrPrefix = "i";
static constexpr const char *appendPrefix = "a";

std::string PrefixZero(int total_width, int num) {
  std::string str = std::to_string(num);
  auto prefix_len = total_width - str.length();
  if (prefix_len > 0) return std::string(total_width - str.length(), '0') + str;
  return str;
}

bool Verify(std::shared_ptr<hbase::Result> result, std::string family, int m) {
  auto col = std::to_string(m);
  auto int_val = hbase::BytesUtil::ToInt64(*(result->Value(family, incrPrefix + col)));
  if (int_val != m) {
    LOG(ERROR) << "value is not " << col << " for " << result->Row();
    return false;
  }
  auto l = *(result->Value(family, col));
  if (l != col) {
    LOG(ERROR) << "value " << *(result->Value(family, "1")) << " is not " << col;
    return false;
  }
  l = *(result->Value(family, appendPrefix + col));
  if (l != col) {
    LOG(ERROR) << "value " << *(result->Value(family, "1")) << " is not " << col;
    return false;
  }
  return true;
}

bool DoScan(int iteration, uint64_t max_row, uint64_t rows, std::unique_ptr<Table> table,
            const std::vector<std::string> &families) {
  hbase::Scan scan{};
  auto start = iteration * rows;
  auto end = start + rows;
  auto width = std::to_string(max_row).length();
  scan.SetStartRow(PrefixZero(width, start));
  scan.SetStopRow(PrefixZero(width, end));
  auto scanner = table->Scan(scan);

  auto cnt = start;
  auto r = scanner->Next();
  while (r != nullptr) {
    auto row = PrefixZero(width, cnt);
    if (r->Row().compare(row) != 0) {
      LOG(ERROR) << "row " << r->Row() << " is not the expected: " << row;
      return false;
    }
    for (auto family : families) {
      auto cols = std::stoi(*(r->Value(family, kNumColumn)));
      VLOG(3) << "scan gets " << std::to_string(cols) << " columns";
      for (int m = 1; m <= cols; m++) {
        if (!Verify(r, family, m)) return false;
      }
    }
    cnt++;
    r = scanner->Next();
  }
  LOG(INFO) << "Iteration " << iteration << " scanned " << std::to_string(cnt - start) << " rows";
  return true;
}

bool DoGet(int iteration, uint64_t max_row, uint64_t rows, std::unique_ptr<Table> table,
           const std::vector<std::string> &families, int batch_num_rows) {
  auto width = std::to_string(max_row).length();
  for (uint64_t k = iteration; k <= max_row;) {
    std::vector<hbase::Get> gets;
    for (uint64_t i = 0; i < batch_num_rows && k <= max_row; ++i, k += FLAGS_threads) {
      std::string row = PrefixZero(width, k);
      hbase::Get get(row);
      gets.push_back(get);
    }
    VLOG(3) << "getting for " << batch_num_rows << " rows";
    auto results = table->Get(gets);
    for (uint64_t i = 0; i < batch_num_rows && i < results.size(); ++i) {
      auto result = results[i];
      if (result == nullptr) {
        LOG(ERROR) << "didn't get result";
        return false;
      }
      // Test the values
      for (auto family : families) {
        auto cols = std::stoi(*(result->Value(family, kNumColumn)));
        VLOG(3) << "gets " << std::to_string(cols) << " columns";
        for (int m = 1; m <= cols; m++) {
          if (!Verify(result, family, m)) return false;
        }
      }
    }
    k += batch_num_rows;
  }
  LOG(INFO) << "Sent " << rows << " gets in iteration " << iteration;
  return true;
}

void DoPut(int iteration, uint64_t max_row, uint64_t rows, int cols, std::unique_ptr<Table> table,
           const std::vector<std::string> &families) {
  auto width = std::to_string(max_row).length();
  for (uint64_t j = 0; j < rows; j++) {
    std::string row = PrefixZero(width, iteration * rows + j);
    auto put = Put{row};
    for (auto family : families) {
      auto n_cols = Random::rand32(1, cols);
      put.AddColumn(family, kNumColumn, std::to_string(n_cols));
      for (unsigned int k = 1; k <= n_cols; k++) {
        put.AddColumn(family, std::to_string(k), std::to_string(k));
      }
    }
    table->Put(put);
    if ((j + 1) % FLAGS_report_num_rows == 0)
      LOG(INFO) << "Written " << std::to_string(j + 1) << " rows";
  }
  LOG(INFO) << "written " << std::to_string(rows) << " rows in " << std::to_string(iteration)
            << " iteration";
}

bool DoAppendIncrement(int iteration, uint64_t max_row, uint64_t rows, int cols,
                       std::unique_ptr<Table> table, const std::vector<std::string> &families) {
  auto width = std::to_string(max_row).length();
  for (uint64_t j = 0; j < rows; j++) {
    std::string row = PrefixZero(width, iteration * rows + j);
    hbase::Get get(row);
    auto result = table->Get(get);
    for (auto family : families) {
      auto n_cols = std::stoi(*(result->Value(family, kNumColumn)));
      for (unsigned int k = 1; k <= n_cols; k++) {
        table->Increment(
            hbase::Increment{row}.AddColumn(family, incrPrefix + std::to_string(k), k));
        if (!table->Append(hbase::Append{row}.Add(family, appendPrefix + std::to_string(k),
                                                  std::to_string(k)))) {
          LOG(ERROR) << "append for " << row << " family: " << family << " failed";
          return false;
        }
      }
    }
    if ((j + 1) % FLAGS_report_num_rows == 0)
      LOG(INFO) << "Written " << std::to_string(j + 1) << " increments";
  }
  return true;
}

int main(int argc, char *argv[]) {
  google::SetUsageMessage("Load client to manipulate multiple rows from HBase on the comamnd line");
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
  FLAGS_logtostderr = 1;
  FLAGS_stderrthreshold = 1;

  if (FLAGS_batch_num_rows < 1) {
    LOG(ERROR) << "size of multi get should be positive";
    return -1;
  }
  if (!FLAGS_gets && !FLAGS_scans && !FLAGS_puts) {
    LOG(ERROR) << "Must perform at least Get or Put operations";
    return -1;
  }
  std::shared_ptr<Configuration> conf = nullptr;
  if (FLAGS_conf == "") {
    // Configuration
    conf = std::make_shared<Configuration>();
    conf->Set("hbase.zookeeper.quorum", FLAGS_zookeeper);
    conf->Set("zookeeper.znode.parent", FLAGS_znode);
  } else {
    setenv("HBASE_CONF", FLAGS_conf.c_str(), 1);
    hbase::HBaseConfigurationLoader loader;
    conf = std::make_shared<Configuration>(loader.LoadDefaultResources().value());
  }
  auto tn = std::make_shared<TableName>(folly::to<TableName>(FLAGS_table));
  auto num_puts = FLAGS_num_rows;

  auto client = std::make_unique<Client>(*conf);

  // Do the Put requests

  std::vector<std::string> families;
  std::size_t pos = 0, found;
  while ((found = FLAGS_families.find_first_of(',', pos)) != std::string::npos) {
    families.push_back(FLAGS_families.substr(pos, found - pos));
    pos = found + 1;
  }
  families.push_back(FLAGS_families.substr(pos));

  int rows = FLAGS_num_rows / FLAGS_threads;
  if (FLAGS_num_rows % FLAGS_threads != 0) rows++;
  int cols = FLAGS_num_cols;
  bool succeeded = true;
  if (FLAGS_puts) {
    auto start_ns = TimeUtil::GetNowNanos();
    std::vector<std::thread> writer_threads;
    for (int i = 0; i < FLAGS_threads; i++) {
      writer_threads.push_back(std::thread([=, &client, &tn, &families, &succeeded] {
        // Get connection to HBase Table
        auto table = client->Table(*tn);

        DoPut(i, FLAGS_num_rows - 1, rows, cols, std::move(table), families);
        table->Close();
      }));
    }
    for (std::vector<std::thread>::iterator it = writer_threads.begin(); it != writer_threads.end();
         it++) {
      std::thread thread = std::move(*it);
      thread.join();
    }
    LOG(INFO) << "Successfully sent  " << num_puts << " Put requests in "
              << TimeUtil::ElapsedMillis(start_ns) << " ms.";
  }
  if (FLAGS_appends) {
    auto start_ns = TimeUtil::GetNowNanos();
    std::vector<std::thread> writer_threads;
    for (int i = 0; i < FLAGS_threads; i++) {
      writer_threads.push_back(std::thread([=, &client, &tn, &families, &succeeded] {
        // Get connection to HBase Table
        auto table = client->Table(*tn);

        succeeded &=
            DoAppendIncrement(i, FLAGS_num_rows - 1, rows, cols, std::move(table), families);
        table->Close();
      }));
    }
    for (std::vector<std::thread>::iterator it = writer_threads.begin(); it != writer_threads.end();
         it++) {
      std::thread thread = std::move(*it);
      thread.join();
    }
    LOG(INFO) << "Successfully sent  " << num_puts << " append requests in "
              << TimeUtil::ElapsedMillis(start_ns) << " ms.";
  }

  if (FLAGS_scans) {
    auto start_ns = TimeUtil::GetNowNanos();
    std::vector<std::thread> reader_threads;
    for (int i = 0; i < FLAGS_threads; i++) {
      reader_threads.push_back(std::thread([=, &client, &tn, &families, &succeeded] {
        // Get connection to HBase Table
        auto table1 = client->Table(*tn);

        succeeded &= DoScan(i, FLAGS_num_rows - 1, rows, std::move(table1), families);
        table1->Close();
      }));
    }
    for (std::vector<std::thread>::iterator it = reader_threads.begin(); it != reader_threads.end();
         it++) {
      std::thread thread = std::move(*it);
      thread.join();
    }

    LOG(INFO) << (succeeded ? "Successful. " : "Failed. ") << "Spent "
              << TimeUtil::ElapsedMillis(start_ns) << " ms.";
  }

  if (FLAGS_gets) {
    auto start_ns = TimeUtil::GetNowNanos();
    std::vector<std::thread> reader_threads;
    for (int i = 0; i < FLAGS_threads; i++) {
      reader_threads.push_back(std::thread([=, &client, &tn, &families, &succeeded] {
        // Get connection to HBase Table
        auto table1 = client->Table(*tn);

        succeeded &=
            DoGet(i, FLAGS_num_rows - 1, rows, std::move(table1), families, FLAGS_batch_num_rows);
        table1->Close();
      }));
    }
    for (std::vector<std::thread>::iterator it = reader_threads.begin(); it != reader_threads.end();
         it++) {
      std::thread thread = std::move(*it);
      thread.join();
    }

    LOG(INFO) << (succeeded ? "Successful. " : "Failed. ") << "Spent "
              << TimeUtil::ElapsedMillis(start_ns) << " ms.";
  }
  client->Close();

  return succeeded ? 0 : -1;
}
