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
#include "hbase/connection/pipeline.h"

#include <folly/Logging.h>
#include <wangle/channel/AsyncSocketHandler.h>
#include <wangle/channel/EventBaseHandler.h>
#include <wangle/channel/OutputBufferingHandler.h>
#include <wangle/codec/LengthFieldBasedFrameDecoder.h>

#include "hbase/connection/client-handler.h"
#include "hbase/connection/sasl-handler.h"

namespace hbase {

RpcPipelineFactory::RpcPipelineFactory(std::shared_ptr<Codec> codec,
                                       std::shared_ptr<Configuration> conf)
    : user_util_(), codec_(codec), conf_(conf) {}
SerializePipeline::Ptr RpcPipelineFactory::newPipeline(
    std::shared_ptr<folly::AsyncTransportWrapper> sock) {
  folly::SocketAddress addr;  // for logging
  sock->getPeerAddress(&addr);

  auto pipeline = SerializePipeline::create();
  pipeline->addBack(wangle::AsyncSocketHandler{sock});
  pipeline->addBack(wangle::EventBaseHandler{});
  bool secure = false;
  /* for RPC test, there's no need to setup Sasl */
  if (!conf_->GetBool(RpcSerde::HBASE_CLIENT_RPC_TEST_MODE,
                      RpcSerde::DEFAULT_HBASE_CLIENT_RPC_TEST_MODE)) {
    secure = security::User::IsSecurityEnabled(*conf_);
    pipeline->addBack(SaslHandler{user_util_.user_name(secure), conf_});
  }
  pipeline->addBack(wangle::LengthFieldBasedFrameDecoder{});
  pipeline->addBack(ClientHandler{user_util_.user_name(secure), codec_, conf_, addr.describe()});
  pipeline->finalize();
  return pipeline;
}
}  // namespace hbase
