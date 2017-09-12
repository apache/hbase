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
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <vector>

#include "hbase/client/cell.h"
#include "hbase/client/result.h"
#include "hbase/client/scan-result-cache.h"

using hbase::ScanResultCache;
using hbase::Result;
using hbase::Cell;
using hbase::CellType;

using ResultVector = std::vector<std::shared_ptr<Result>>;

std::shared_ptr<Cell> CreateCell(const int32_t &key, const std::string &family,
                                 const std::string &column) {
  auto row = folly::to<std::string>(key);
  return std::make_shared<Cell>(row, family, column, std::numeric_limits<int64_t>::max(), row,
                                CellType::PUT);
}

std::shared_ptr<Result> CreateResult(std::shared_ptr<Cell> cell, bool partial) {
  return std::make_shared<Result>(std::vector<std::shared_ptr<Cell>>{cell}, false, false, partial);
}

TEST(ScanResultCacheTest, NoPartial) {
  ScanResultCache cache;
  ASSERT_EQ(ResultVector{}, cache.AddAndGet(ResultVector{}, false));
  ASSERT_EQ(ResultVector{}, cache.AddAndGet(ResultVector{}, true));
  int32_t count = 10;
  ResultVector results{};
  for (int32_t i = 0; i < count; i++) {
    results.push_back(CreateResult(CreateCell(i, "cf", "cq1"), false));
  }
  ASSERT_EQ(results, cache.AddAndGet(results, false));
}

TEST(ScanResultCacheTest, Combine1) {
  ScanResultCache cache;
  auto prev_result = CreateResult(CreateCell(0, "cf", "cq1"), true);
  auto result1 = CreateResult(CreateCell(1, "cf", "cq1"), true);
  auto result2 = CreateResult(CreateCell(1, "cf", "cq2"), true);
  auto result3 = CreateResult(CreateCell(1, "cf", "cq3"), true);
  auto results = cache.AddAndGet(ResultVector{prev_result, result1}, false);
  ASSERT_EQ(1L, results.size());
  ASSERT_EQ(prev_result, results[0]);

  ASSERT_EQ(0, cache.AddAndGet(ResultVector{result2}, false).size());
  ASSERT_EQ(0, cache.AddAndGet(ResultVector{result3}, false).size());
  ASSERT_EQ(0, cache.AddAndGet(ResultVector{}, true).size());

  results = cache.AddAndGet(ResultVector{}, false);
  ASSERT_EQ(1, results.size());
  ASSERT_EQ(1, folly::to<int32_t>(results[0]->Row()));
  ASSERT_EQ(3, results[0]->Cells().size());
  ASSERT_EQ(1, folly::to<int32_t>(*results[0]->Value("cf", "cq1")));
  ASSERT_EQ(1, folly::to<int32_t>(*results[0]->Value("cf", "cq2")));
  ASSERT_EQ(1, folly::to<int32_t>(*results[0]->Value("cf", "cq3")));
}

TEST(ScanResultCacheTest, Combine2) {
  ScanResultCache cache;
  auto result1 = CreateResult(CreateCell(1, "cf", "cq1"), true);
  auto result2 = CreateResult(CreateCell(1, "cf", "cq2"), true);
  auto result3 = CreateResult(CreateCell(1, "cf", "cq3"), true);

  auto next_result1 = CreateResult(CreateCell(2, "cf", "cq1"), true);
  auto next_to_next_result1 = CreateResult(CreateCell(3, "cf", "cq2"), false);

  ASSERT_EQ(0, cache.AddAndGet(ResultVector{result1}, false).size());
  ASSERT_EQ(0, cache.AddAndGet(ResultVector{result2}, false).size());
  ASSERT_EQ(0, cache.AddAndGet(ResultVector{result3}, false).size());

  auto results = cache.AddAndGet(ResultVector{next_result1}, false);
  ASSERT_EQ(1, results.size());
  ASSERT_EQ(1, folly::to<int32_t>(results[0]->Row()));
  ASSERT_EQ(3, results[0]->Cells().size());
  ASSERT_EQ(1, folly::to<int32_t>(*results[0]->Value("cf", "cq1")));
  ASSERT_EQ(1, folly::to<int32_t>(*results[0]->Value("cf", "cq2")));
  ASSERT_EQ(1, folly::to<int32_t>(*results[0]->Value("cf", "cq3")));

  results = cache.AddAndGet(ResultVector{next_to_next_result1}, false);
  ASSERT_EQ(2, results.size());
  ASSERT_EQ(2, folly::to<int32_t>(results[0]->Row()));
  ASSERT_EQ(1, results[0]->Cells().size());
  ASSERT_EQ(2, folly::to<int32_t>(*results[0]->Value("cf", "cq1")));
  ASSERT_EQ(3, folly::to<int32_t>(results[1]->Row()));
  ASSERT_EQ(1, results[1]->Cells().size());
  ASSERT_EQ(3, folly::to<int32_t>(*results[1]->Value("cf", "cq2")));
}

TEST(ScanResultCacheTest, Combine3) {
  ScanResultCache cache;
  auto result1 = CreateResult(CreateCell(1, "cf", "cq1"), true);
  auto result2 = CreateResult(CreateCell(1, "cf", "cq2"), true);
  auto next_result1 = CreateResult(CreateCell(2, "cf", "cq1"), false);
  auto next_to_next_result1 = CreateResult(CreateCell(3, "cf", "cq1"), true);

  ASSERT_EQ(0, cache.AddAndGet(ResultVector{result1}, false).size());
  ASSERT_EQ(0, cache.AddAndGet(ResultVector{result2}, false).size());

  auto results = cache.AddAndGet(ResultVector{next_result1, next_to_next_result1}, false);

  ASSERT_EQ(2, results.size());
  ASSERT_EQ(1, folly::to<int32_t>(results[0]->Row()));
  ASSERT_EQ(2, results[0]->Cells().size());
  ASSERT_EQ(1, folly::to<int32_t>(*results[0]->Value("cf", "cq1")));
  ASSERT_EQ(1, folly::to<int32_t>(*results[0]->Value("cf", "cq2")));
  ASSERT_EQ(2, folly::to<int32_t>(results[1]->Row()));
  ASSERT_EQ(1, results[1]->Cells().size());
  ASSERT_EQ(2, folly::to<int32_t>(*results[1]->Value("cf", "cq1")));

  results = cache.AddAndGet(ResultVector{}, false);

  ASSERT_EQ(1, results.size());
  ASSERT_EQ(3, folly::to<int32_t>(results[0]->Row()));
  ASSERT_EQ(1, results[0]->Cells().size());
  ASSERT_EQ(3, folly::to<int32_t>(*results[0]->Value("cf", "cq1")));
}

TEST(ScanResultCacheTest, Combine4) {
  ScanResultCache cache;
  auto result1 = CreateResult(CreateCell(1, "cf", "cq1"), true);
  auto result2 = CreateResult(CreateCell(1, "cf", "cq2"), false);
  auto next_result1 = CreateResult(CreateCell(2, "cf", "cq1"), true);
  auto next_result2 = CreateResult(CreateCell(2, "cf", "cq2"), false);

  ASSERT_EQ(0, cache.AddAndGet(ResultVector{result1}, false).size());

  auto results = cache.AddAndGet(ResultVector{result2, next_result1}, false);

  ASSERT_EQ(1, results.size());
  ASSERT_EQ(1, folly::to<int32_t>(results[0]->Row()));
  ASSERT_EQ(2, results[0]->Cells().size());
  ASSERT_EQ(1, folly::to<int32_t>(*results[0]->Value("cf", "cq1")));
  ASSERT_EQ(1, folly::to<int32_t>(*results[0]->Value("cf", "cq2")));

  results = cache.AddAndGet(ResultVector{next_result2}, false);

  ASSERT_EQ(1, results.size());
  ASSERT_EQ(2, folly::to<int32_t>(results[0]->Row()));
  ASSERT_EQ(2, results[0]->Cells().size());
  ASSERT_EQ(2, folly::to<int32_t>(*results[0]->Value("cf", "cq1")));
  ASSERT_EQ(2, folly::to<int32_t>(*results[0]->Value("cf", "cq2")));
}

TEST(ScanResultCacheTest, SizeOf) {
  std::string e{""};
  std::string f{"f"};
  std::string foo{"foo"};

  LOG(INFO) << sizeof(e) << " " << e.capacity();
  LOG(INFO) << sizeof(f) << " " << f.capacity();
  LOG(INFO) << sizeof(foo) << " " << foo.capacity();
}
