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
#include "connection/client-dispatcher.h"

using namespace folly;
using namespace hbase;
using namespace wangle;

ClientDispatcher::ClientDispatcher() : requests_(), current_call_id_(9) {}

void ClientDispatcher::read(Context *ctx, Response in) {
  auto call_id = in.call_id();
  auto search = requests_.find(call_id);
  CHECK(search != requests_.end());
  auto p = std::move(search->second);

  requests_.erase(call_id);

  // TODO(eclark): check if the response
  // is an exception. If it is then set that.
  p.setValue(in);
}

Future<Response> ClientDispatcher::operator()(std::unique_ptr<Request> arg) {
  auto call_id = ++current_call_id_;

  arg->set_call_id(call_id);
  auto &p = requests_[call_id];
  auto f = p.getFuture();
  p.setInterruptHandler([call_id, this](const folly::exception_wrapper &e) {
    this->requests_.erase(call_id);
  });
  this->pipeline_->write(std::move(arg));

  return f;
}

Future<Unit> ClientDispatcher::close() { return ClientDispatcherBase::close(); }

Future<Unit> ClientDispatcher::close(Context *ctx) {
  return ClientDispatcherBase::close(ctx);
}
