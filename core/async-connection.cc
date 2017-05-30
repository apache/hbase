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

#include "core/async-connection.h"
#include "core/async-rpc-retrying-caller-factory.h"

namespace hbase {

void AsyncConnectionImpl::Init() {
  connection_conf_ = std::make_shared<hbase::ConnectionConfiguration>(*conf_);
  // start thread pools
  auto io_threads = conf_->GetInt(kClientIoThreadPoolSize, sysconf(_SC_NPROCESSORS_ONLN));
  auto cpu_threads = conf_->GetInt(kClientCpuThreadPoolSize, 2 * sysconf(_SC_NPROCESSORS_ONLN));
  cpu_executor_ = std::make_shared<wangle::CPUThreadPoolExecutor>(cpu_threads);
  io_executor_ = std::make_shared<wangle::IOThreadPoolExecutor>(io_threads);
  /*
   * We need a retry_executor for a thread pool of size 1 due to a possible bug in wangle/folly.
   * Otherwise, Assertion 'isInEventBaseThread()' always fails. See the comments
   * in async-rpc-retrying-caller.cc.
   */
  retry_executor_ = std::make_shared<wangle::IOThreadPoolExecutor>(1);
  retry_timer_ = folly::HHWheelTimer::newTimer(retry_executor_->getEventBase());

  std::shared_ptr<Codec> codec = nullptr;
  if (conf_->Get(kRpcCodec, hbase::KeyValueCodec::kJavaClassName) ==
      std::string(KeyValueCodec::kJavaClassName)) {
    codec = std::make_shared<hbase::KeyValueCodec>();
  } else {
    LOG(WARNING) << "Not using RPC Cell Codec";
  }
  rpc_client_ = std::make_shared<hbase::RpcClient>(io_executor_, codec, conf_,
                                                   connection_conf_->connect_timeout());
  location_cache_ =
      std::make_shared<hbase::LocationCache>(conf_, cpu_executor_, rpc_client_->connection_pool());
  caller_factory_ =
      std::make_shared<AsyncRpcRetryingCallerFactory>(shared_from_this(), retry_timer_);
}

// We can't have the threads continue running after everything is done
// that leads to an error.
AsyncConnectionImpl::~AsyncConnectionImpl() { Close(); }

void AsyncConnectionImpl::Close() {
  if (is_closed_) return;

  cpu_executor_->stop();
  io_executor_->stop();
  retry_executor_->stop();
  retry_timer_->destroy();
  if (rpc_client_.get()) rpc_client_->Close();
  is_closed_ = true;
}

}  // namespace hbase
