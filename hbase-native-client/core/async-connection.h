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

#include <folly/futures/Future.h>
#include <folly/io/IOBuf.h>
#include <wangle/concurrent/CPUThreadPoolExecutor.h>
#include <wangle/concurrent/IOThreadPoolExecutor.h>

#include <memory>
#include <string>
#include <utility>

#include "connection/rpc-client.h"
#include "core/async-region-locator.h"
#include "core/configuration.h"
#include "core/connection-configuration.h"
#include "core/hbase-configuration-loader.h"
#include "core/hbase-rpc-controller.h"
#include "core/keyvalue-codec.h"
#include "core/location-cache.h"
#include "if/Cell.pb.h"
#include "serde/table-name.h"

namespace hbase {

class AsyncRpcRetryingCallerFactory;

class AsyncConnection {
 public:
  AsyncConnection() {}
  virtual ~AsyncConnection() {}
  virtual std::shared_ptr<Configuration> conf() = 0;
  virtual std::shared_ptr<ConnectionConfiguration> connection_conf() = 0;
  virtual std::shared_ptr<AsyncRpcRetryingCallerFactory> caller_factory() = 0;
  virtual std::shared_ptr<RpcClient> rpc_client() = 0;
  virtual std::shared_ptr<AsyncRegionLocator> region_locator() = 0;
  virtual std::shared_ptr<HBaseRpcController> CreateRpcController() = 0;
  virtual std::shared_ptr<wangle::CPUThreadPoolExecutor> cpu_executor() = 0;
  virtual std::shared_ptr<wangle::IOThreadPoolExecutor> io_executor() = 0;
  virtual std::shared_ptr<wangle::IOThreadPoolExecutor> retry_executor() = 0;
  virtual void Close() = 0;
};

class AsyncConnectionImpl : public AsyncConnection,
                            public std::enable_shared_from_this<AsyncConnectionImpl> {
 public:
  virtual ~AsyncConnectionImpl();

  // See https://mortoray.com/2013/08/02/safely-using-enable_shared_from_this/
  template <typename... T>
  static std::shared_ptr<AsyncConnectionImpl> Create(T&&... all) {
    auto conn =
        std::shared_ptr<AsyncConnectionImpl>(new AsyncConnectionImpl(std::forward<T>(all)...));
    conn->Init();
    return conn;
  }

  std::shared_ptr<Configuration> conf() override { return conf_; }
  std::shared_ptr<ConnectionConfiguration> connection_conf() override { return connection_conf_; }
  std::shared_ptr<AsyncRpcRetryingCallerFactory> caller_factory() override {
    return caller_factory_;
  }
  std::shared_ptr<RpcClient> rpc_client() override { return rpc_client_; }
  std::shared_ptr<LocationCache> location_cache() { return location_cache_; }
  std::shared_ptr<AsyncRegionLocator> region_locator() override { return location_cache_; }
  std::shared_ptr<HBaseRpcController> CreateRpcController() override {
    return std::make_shared<HBaseRpcController>();
  }
  std::shared_ptr<wangle::CPUThreadPoolExecutor> cpu_executor() override { return cpu_executor_; }
  std::shared_ptr<wangle::IOThreadPoolExecutor> io_executor() override { return io_executor_; }
  std::shared_ptr<wangle::IOThreadPoolExecutor> retry_executor() override {
    return retry_executor_;
  }

  void Close() override;

 protected:
  AsyncConnectionImpl() {}

 private:
  /** Parameter name for HBase client IO thread pool size. Defaults to num cpus */
  static constexpr const char* kClientIoThreadPoolSize = "hbase.client.io.thread.pool.size";
  /** Parameter name for HBase client CPU thread pool size. Defaults to (2 * num cpus) */
  static constexpr const char* kClientCpuThreadPoolSize = "hbase.client.cpu.thread.pool.size";
  /** The RPC codec to encode cells. For now it is KeyValueCodec */
  static constexpr const char* kRpcCodec = "hbase.client.rpc.codec";

  std::shared_ptr<Configuration> conf_;
  std::shared_ptr<ConnectionConfiguration> connection_conf_;
  std::shared_ptr<AsyncRpcRetryingCallerFactory> caller_factory_;
  std::shared_ptr<folly::HHWheelTimer> retry_timer_;
  std::shared_ptr<wangle::CPUThreadPoolExecutor> cpu_executor_;
  std::shared_ptr<wangle::IOThreadPoolExecutor> io_executor_;
  std::shared_ptr<wangle::IOThreadPoolExecutor> retry_executor_;
  std::shared_ptr<LocationCache> location_cache_;
  std::shared_ptr<RpcClient> rpc_client_;
  bool is_closed_ = false;

 private:
  explicit AsyncConnectionImpl(std::shared_ptr<Configuration> conf) : conf_(conf) {}
  void Init();
};
}  // namespace hbase
