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

#include <memory>
#include <vector>
#include "connection/request.h"
#include "connection/response.h"
#include "core/multi-response.h"
#include "core/result.h"
#include "if/Client.pb.h"
#include "serde/cell-scanner.h"

namespace hbase {

/**
 * ResponseConverter class
 * This class converts a PB Response to corresponding Result or other objects.
 */
class ResponseConverter {
 public:
  ~ResponseConverter();

  static std::shared_ptr<Result> ToResult(const hbase::pb::Result& result,
                                          const std::shared_ptr<CellScanner> cell_scanner);

  /**
   * @brief Returns a Result object created by PB Message in passed Response object.
   * @param resp - Response object having the PB message.
   */
  static std::shared_ptr<hbase::Result> FromGetResponse(const Response& resp);

  static std::shared_ptr<hbase::Result> FromMutateResponse(const Response& resp);

  static std::vector<std::shared_ptr<Result>> FromScanResponse(const Response& resp);

  static std::vector<std::shared_ptr<Result>> FromScanResponse(
      const std::shared_ptr<pb::ScanResponse> resp, std::shared_ptr<CellScanner> cell_scanner);

  static std::unique_ptr<hbase::MultiResponse> GetResults(std::shared_ptr<Request> req,
                                                          const Response& resp);

 private:
  // Constructor not required. We have all static methods to extract response from PB messages.
  ResponseConverter();
};

} /* namespace hbase */
