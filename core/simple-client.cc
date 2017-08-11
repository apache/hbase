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

#include "connection/rpc-client.h"
#include "core/client.h"
#include "core/get.h"
#include "core/hbase-configuration-loader.h"
#include "core/put.h"
#include "core/scan.h"
#include "core/table.h"
#include "serde/server-name.h"
#include "serde/table-name.h"
#include "utils/time-util.h"

using hbase::Client;
using hbase::Configuration;
using hbase::Get;
using hbase::HBaseConfigurationLoader;
using hbase::Scan;
using hbase::Put;
using hbase::Result;
using hbase::Table;
using hbase::pb::TableName;
using hbase::pb::ServerName;
using hbase::TimeUtil;

DEFINE_string(table, "test_table", "What table to do the reads or writes");
DEFINE_string(row, "row_", "row prefix");
DEFINE_string(zookeeper, "localhost:2181", "What zk quorum to talk to");
DEFINE_string(conf, "", "Conf directory to read the config from (optional)");
DEFINE_uint64(num_rows, 10000, "How many rows to write and read");
DEFINE_uint64(batch_num_rows, 10000, "How many rows batch for multi-gets and multi-puts");
DEFINE_uint64(report_num_rows, 10000, "How frequent we should report the progress");
DEFINE_bool(puts, true, "Whether to perform puts");
DEFINE_bool(gets, true, "Whether to perform gets");
DEFINE_bool(multigets, true, "Whether to perform multi-gets");
DEFINE_bool(scans, true, "Whether to perform scans");
DEFINE_bool(display_results, false, "Whether to display the Results from Gets");
DEFINE_int32(threads, 6, "How many cpu threads");

std::unique_ptr<Put> MakePut(const std::string &row) {
  auto put = std::make_unique<Put>(row);
  put->AddColumn("f", "q", row);
  return std::move(put);
}

std::string Row(const std::string &prefix, uint64_t i) {
  auto suf = folly::to<std::string>(i);
  return prefix + suf;
}

void ValidateResult(const Result &result, const std::string &row) {
  CHECK(!result.IsEmpty());
  CHECK_EQ(result.Row(), row);
  CHECK_EQ(result.Size(), 1);
  CHECK_EQ(result.Value("f", "q").value(), row);
}

int main(int argc, char *argv[]) {
  gflags::SetUsageMessage("Simple client to get a single row from HBase on the comamnd line");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
  FLAGS_logtostderr = 1;
  FLAGS_stderrthreshold = 1;

  std::shared_ptr<Configuration> conf = nullptr;
  if (FLAGS_conf == "") {
    // Configuration
    conf = std::make_shared<Configuration>();
    conf->Set("hbase.zookeeper.quorum", FLAGS_zookeeper);
    conf->SetInt("hbase.client.cpu.thread.pool.size", FLAGS_threads);
  } else {
    setenv("HBASE_CONF", FLAGS_conf.c_str(), 1);
    hbase::HBaseConfigurationLoader loader;
    conf = std::make_shared<Configuration>(loader.LoadDefaultResources().value());
  }

  auto row = FLAGS_row;

  auto tn = std::make_shared<TableName>(folly::to<TableName>(FLAGS_table));
  auto num_puts = FLAGS_num_rows;

  auto client = std::make_unique<Client>(*conf);
  auto table = client->Table(*tn);

  auto start_ns = TimeUtil::GetNowNanos();

  // Do the Put requests
  if (FLAGS_puts) {
    LOG(INFO) << "Sending put requests";
    for (uint64_t i = 0; i < num_puts; i++) {
      table->Put(*MakePut(Row(FLAGS_row, i)));
      if (i != 0 && i % FLAGS_report_num_rows == 0) {
        LOG(INFO) << "Sent  " << i << " Put requests in " << TimeUtil::ElapsedMillis(start_ns)
                  << " ms.";
      }
    }

    LOG(INFO) << "Successfully sent  " << num_puts << " Put requests in "
              << TimeUtil::ElapsedMillis(start_ns) << " ms.";
  }

  // Do the Get requests
  if (FLAGS_gets) {
    LOG(INFO) << "Sending get requests";
    start_ns = TimeUtil::GetNowNanos();
    for (uint64_t i = 0; i < num_puts; i++) {
      auto row = Row(FLAGS_row, i);
      auto result = table->Get(Get{row});
      if (FLAGS_display_results) {
        LOG(INFO) << result->DebugString();
      } else if (i != 0 && i % FLAGS_report_num_rows == 0) {
        LOG(INFO) << "Sent  " << i << " Get requests in " << TimeUtil::ElapsedMillis(start_ns)
                  << " ms.";
      }
      ValidateResult(*result, row);
    }

    LOG(INFO) << "Successfully sent  " << num_puts << " Get requests in "
              << TimeUtil::ElapsedMillis(start_ns) << " ms.";
  }

  // Do the Multi-Gets
  if (FLAGS_multigets) {
    LOG(INFO) << "Sending multi-get requests";
    start_ns = TimeUtil::GetNowNanos();
    std::vector<hbase::Get> gets;

    for (uint64_t i = 0; i < num_puts;) {
      gets.clear();
      // accumulate batch_num_rows at a time
      for (uint64_t j = 0; j < FLAGS_batch_num_rows && i < num_puts; ++j) {
        hbase::Get get(Row(FLAGS_row, i));
        gets.push_back(get);
        i++;
      }
      auto results = table->Get(gets);

      if (FLAGS_display_results) {
        for (const auto &result : results) LOG(INFO) << result->DebugString();
      } else if (i != 0 && i % FLAGS_report_num_rows == 0) {
        LOG(INFO) << "Sent  " << i << " Multi-Get requests in " << TimeUtil::ElapsedMillis(start_ns)
                  << " ms.";
      }
    }

    LOG(INFO) << "Successfully sent  " << num_puts << " Multi-Get requests in "
              << TimeUtil::ElapsedMillis(start_ns) << " ms.";
  }

  // Do the Scan
  if (FLAGS_scans) {
    LOG(INFO) << "Starting scanner";
    start_ns = TimeUtil::GetNowNanos();
    Scan scan{};
    auto scanner = table->Scan(scan);

    uint64_t i = 0;
    auto r = scanner->Next();
    while (r != nullptr) {
      if (FLAGS_display_results) {
        LOG(INFO) << r->DebugString();
      }
      r = scanner->Next();
      i++;
      if (!FLAGS_display_results && i != 0 && i % FLAGS_report_num_rows == 0) {
        LOG(INFO) << "Scan iterated over " << i << " results " << TimeUtil::ElapsedMillis(start_ns)
                  << " ms.";
      }
    }

    LOG(INFO) << "Successfully iterated over  " << i << " Scan results in "
              << TimeUtil::ElapsedMillis(start_ns) << " ms.";
    scanner->Close();
  }

  table->Close();
  client->Close();

  return 0;
}
