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

#include "connection/request.h"

#include "if/Client.pb.h"

using namespace hbase;

Request::Request(std::shared_ptr<google::protobuf::Message> req,
                 std::shared_ptr<google::protobuf::Message> resp,
                 std::string method)
    : req_msg_(req), resp_msg_(resp), method_(method), call_id_(0) {}

std::unique_ptr<Request> Request::get() {
  return std::make_unique<Request>(std::make_shared<hbase::pb::GetRequest>(),
                                  std::make_shared<hbase::pb::GetResponse>(),
                                  "Get");
}
std::unique_ptr<Request> Request::mutate() {
  return std::make_unique<Request>(std::make_shared<hbase::pb::MutateRequest>(),
                                  std::make_shared<hbase::pb::MutateResponse>(),
                                  "Mutate");
}
std::unique_ptr<Request> Request::scan() {
  return std::make_unique<Request>(std::make_shared<hbase::pb::ScanRequest>(),
                                  std::make_shared<hbase::pb::ScanResponse>(),
                                  "Scan");
}
