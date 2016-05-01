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

#include "connection/service.h"
#include "if/HBase.pb.h"


namespace hbase {

class RegionLocation {
public:
  RegionLocation(hbase::pb::RegionInfo ri, hbase::pb::ServerName sn,
                 std::shared_ptr<HBaseService> service)
      : ri_(ri), sn_(sn), service_(service) {}

  const hbase::pb::RegionInfo& region_info() { return ri_; }
  const hbase::pb::ServerName& server_name() { return sn_; }
  std::shared_ptr<HBaseService> service() { return service_; }

private:
  hbase::pb::RegionInfo ri_;
  hbase::pb::ServerName sn_;
  std::shared_ptr<HBaseService> service_;
};

} // namespace hbase
