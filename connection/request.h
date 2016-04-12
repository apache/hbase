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

#include <google/protobuf/message.h>

#include <cstdint>
#include <string>

namespace hbase {
class Request {
public:
  Request() : call_id_(0) {}
  uint32_t call_id() { return call_id_; }
  void set_call_id(uint32_t call_id) { call_id_ = call_id; }
  google::protobuf::Message *msg() { return msg_.get(); }
  void set_msg(std::shared_ptr<google::protobuf::Message> msg) { msg_ = msg; }
  std::string method() { return method_; }
  void set_method(std::string method) { method_ = method; }

private:
  uint32_t call_id_;
  std::shared_ptr<google::protobuf::Message> msg_ = nullptr;
  std::string method_ = "Get";
};
} // namespace hbase
