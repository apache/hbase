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

/**
 * @brief class to hold where a region is located.
 *
 * This class holds where a region is located, the information about it, the
 * region name, and a connection to the service used for connecting to it.
 */
class RegionLocation {
 public:
  /**
   * Constructor.
   * @param region_name The region name of this region.
   * @param ri The decoded RegionInfo of this region.
   * @param sn The server name of the HBase regionserver thought to be hosting
   * this region.
   * @param service the connected service to the regionserver.
   */
  RegionLocation(std::string region_name, hbase::pb::RegionInfo ri,
                 hbase::pb::ServerName sn,
                 std::shared_ptr<HBaseService> service)
      : region_name_(region_name), ri_(ri), sn_(sn), service_(service) {}

  /**
   * Get a reference to the regio info
   */
  const hbase::pb::RegionInfo &region_info() { return ri_; }

  /**
   * Get a reference to the server name
   */
  const hbase::pb::ServerName &server_name() { return sn_; }

  /**
   * Get a reference to the region name.
   */
  const std::string &region_name() { return region_name_; }

  /**
   * Get a service. This could be closed or null. It's the caller's
   * responsibility to check.
   */
  std::shared_ptr<HBaseService> service() { return service_; }

  /**
   * Set the service.
   * This should be used if the region moved or if the connection is thought to
   * be bad and a new tcp connection needs to be made.
   */
  void set_service(std::shared_ptr<HBaseService> s) { service_ = s; }

  /**
   * Set the servername if the region has moved.
   */
  void set_server_name(hbase::pb::ServerName sn) { sn_ = sn; }

 private:
  std::string region_name_;
  hbase::pb::RegionInfo ri_;
  hbase::pb::ServerName sn_;
  std::shared_ptr<HBaseService> service_;
};

}  // namespace hbase
