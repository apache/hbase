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
#include <folly/ExceptionWrapper.h>
#include <folly/Format.h>
#include <folly/io/async/AsyncSocketException.h>
#include <utility>
#include "exceptions/exception.h"

using std::unique_ptr;

namespace hbase {

ClientDispatcher::ClientDispatcher() : current_call_id_(9), requests_(5000) {}

void ClientDispatcher::read(Context *ctx, unique_ptr<Response> in) {
  auto call_id = in->call_id();
  auto p = requests_.find_and_erase(call_id);

  VLOG(3) << folly::sformat("Read hbase::Response, call_id: {}, hasException: {}, what: {}",
                            in->call_id(), bool(in->exception()), in->exception().what());

  if (in->exception()) {
    p.setException(in->exception());
  } else {
    p.setValue(std::move(in));
  }
}

folly::Future<unique_ptr<Response>> ClientDispatcher::operator()(unique_ptr<Request> arg) {
  auto call_id = current_call_id_++;
  arg->set_call_id(call_id);

  // TODO: if the map is full (or we have more than hbase.client.perserver.requests.threshold)
  // then throw ServerTooBusyException so that upper layers will retry.
  auto &p = requests_[call_id];

  auto f = p.getFuture();
  p.setInterruptHandler([call_id, this](const folly::exception_wrapper &e) {
    LOG(ERROR) << "e = " << call_id;
    this->requests_.erase(call_id);
  });

  try {
    this->pipeline_->write(std::move(arg));
  } catch (const folly::AsyncSocketException &e) {
    p.setException(folly::exception_wrapper{ConnectionException{folly::exception_wrapper{e}}});
    /* clear folly::Promise to avoid overflow. */
    requests_.erase(call_id);
  }

  return f;
}

folly::Future<folly::Unit> ClientDispatcher::close() { return ClientDispatcherBase::close(); }

folly::Future<folly::Unit> ClientDispatcher::close(Context *ctx) {
  return ClientDispatcherBase::close(ctx);
}
}  // namespace hbase
