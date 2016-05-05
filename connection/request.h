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
#include <memory>
#include <string>

namespace hbase {

/**
 * Main request class.
 * This holds the request object and the un-filled in approriatley typed
 * response object.
 */
class Request {
public:
  /** Create a request object for a get */
  static std::unique_ptr<Request> get();
  /** Create a request object for a mutate */
  static std::unique_ptr<Request> mutate();
  /** Create a request object for a scan */
  static std::unique_ptr<Request> scan();

  /**
   * This should be private. Do not use this.
   *
   *
   * Constructor that's public for make_unique. This sets all the messages and
   * method name.
   */
  Request(std::shared_ptr<google::protobuf::Message> req,
          std::shared_ptr<google::protobuf::Message> resp, std::string method);

  /** Get the call id. */
  uint32_t call_id() { return call_id_; }
  /** Set the call id. This should only be set once. */
  void set_call_id(uint32_t call_id) { call_id_ = call_id; }
  /** Get the backing request protobuf message. */
  std::shared_ptr<google::protobuf::Message> req_msg() { return req_msg_; }
  /** Get the backing response protobuf message. */
  std::shared_ptr<google::protobuf::Message> resp_msg() { return resp_msg_; }
  /** Get the method name. This is used to the the receiving rpc server what
   * method type to decode. */
  std::string method() { return method_; }

private:
  uint32_t call_id_;
  std::shared_ptr<google::protobuf::Message> req_msg_ = nullptr;
  std::shared_ptr<google::protobuf::Message> resp_msg_ = nullptr;
  std::string method_ = "Get";
};
} // namespace hbase
