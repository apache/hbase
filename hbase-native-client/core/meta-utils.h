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

#include <string>

#include "connection/Request.h"
#include "if/HBase.pb.h"
#include "serde/table-name.h"

namespace hbase {

/**
 * @brief Utility for meta operations.
 */
class MetaUtil {
public:
  /**
   * Given a table and a row give the row key from which to start a scan to find
   * region locations.
   */
  std::string RegionLookupRowkey(const hbase::pb::TableName &tn,
                                 const std::string &row) const;

  /**
   * Given a row we're trying to access create a request to look up the
   * location.
   */
  std::unique_ptr<Request> MetaRequest(const hbase::pb::TableName tn,
                                       const std::string &row) const;
};
} // namespace hbase
