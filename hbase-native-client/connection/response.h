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

#include <cstdint>

// Forward
namespace google {
namespace protobuf {
class Message;
}
}

namespace hbase {

class Response {
public:
  Response() : call_id_(0), response_(nullptr) {}
  uint32_t call_id() { return call_id_; }
  void set_call_id(uint32_t call_id) { call_id_ = call_id; }
  std::shared_ptr<google::protobuf::Message> response() const {
    return response_;
  }
  void set_response(std::shared_ptr<google::protobuf::Message> response) {
    response_ = std::move(response);
  }

private:
  uint32_t call_id_;
  std::shared_ptr<google::protobuf::Message> response_;
};
} // namespace hbase
