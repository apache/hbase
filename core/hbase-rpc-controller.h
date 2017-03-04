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

#include <google/protobuf/service.h>
#include <chrono>
#include <string>

using google::protobuf::RpcController;
using google::protobuf::Closure;

using std::chrono::milliseconds;

namespace hbase {

class HBaseRpcController : public RpcController {
 public:
  HBaseRpcController() {}
  virtual ~HBaseRpcController() = default;

  void set_call_timeout(const milliseconds& call_timeout) {
    // TODO:
  }

  void Reset() override {}

  bool Failed() const override { return false; }

  std::string ErrorText() const override { return ""; }

  void StartCancel() override {}

  void SetFailed(const std::string& reason) override {}

  bool IsCanceled() const override { return false; }

  void NotifyOnCancel(Closure* callback) override {}
};

} /* namespace hbase */
