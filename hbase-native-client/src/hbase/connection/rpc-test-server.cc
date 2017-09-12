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
#include <wangle/channel/AsyncSocketHandler.h>
#include <wangle/channel/EventBaseHandler.h>
#include <wangle/codec/LengthFieldBasedFrameDecoder.h>
#include <wangle/codec/LengthFieldPrepender.h>
#include <wangle/service/ServerDispatcher.h>

#include "hbase/connection/rpc-test-server-handler.h"
#include "hbase/connection/rpc-test-server.h"
#include "hbase/if/test.pb.h"

namespace hbase {

RpcTestServerSerializePipeline::Ptr RpcTestServerPipelineFactory::newPipeline(
    std::shared_ptr<AsyncTransportWrapper> sock) {
  if (service_ == nullptr) {
    initService(sock);
  }
  CHECK(service_ != nullptr);

  auto pipeline = RpcTestServerSerializePipeline::create();
  pipeline->addBack(AsyncSocketHandler(sock));
  // ensure we can write from any thread
  pipeline->addBack(EventBaseHandler());
  pipeline->addBack(LengthFieldBasedFrameDecoder());
  pipeline->addBack(RpcTestServerSerializeHandler());
  pipeline->addBack(MultiplexServerDispatcher<std::unique_ptr<Request>, std::unique_ptr<Response>>(
      service_.get()));
  pipeline->finalize();

  return pipeline;
}

void RpcTestServerPipelineFactory::initService(std::shared_ptr<AsyncTransportWrapper> sock) {
  /* get server address */
  SocketAddress localAddress;
  sock->getLocalAddress(&localAddress);

  /* init service with server address */
  service_ = std::make_shared<ExecutorFilter<std::unique_ptr<Request>, std::unique_ptr<Response>>>(
      std::make_shared<CPUThreadPoolExecutor>(1),
      std::make_shared<RpcTestService>(std::make_shared<SocketAddress>(localAddress)));
}

Future<std::unique_ptr<Response>> RpcTestService::operator()(std::unique_ptr<Request> request) {
  /* build Response */
  auto response = std::make_unique<Response>();
  response->set_call_id(request->call_id());
  std::string method_name = request->method();

  if (method_name == "ping") {
    auto pb_resp_msg = std::make_shared<EmptyResponseProto>();
    response->set_resp_msg(pb_resp_msg);
    VLOG(1) << "RPC server:"
            << " ping called.";

  } else if (method_name == "echo") {
    auto pb_resp_msg = std::make_shared<EchoResponseProto>();
    /* get msg from client */
    auto pb_req_msg = std::static_pointer_cast<EchoRequestProto>(request->req_msg());
    pb_resp_msg->set_message(pb_req_msg->message());
    response->set_resp_msg(pb_resp_msg);
    VLOG(1) << "RPC server:"
            << " echo called, " << pb_req_msg->message();

  } else if (method_name == "error") {
    auto pb_resp_msg = std::make_shared<EmptyResponseProto>();
    response->set_resp_msg(pb_resp_msg);
    VLOG(1) << "RPC server:"
            << " error called.";
    response->set_exception(RpcTestException("server error!"));

  } else if (method_name == "pause") {
    auto pb_resp_msg = std::make_shared<EmptyResponseProto>();
    /* sleeping */
    auto pb_req_msg = std::static_pointer_cast<PauseRequestProto>(request->req_msg());
    std::this_thread::sleep_for(std::chrono::milliseconds(pb_req_msg->ms()));
    response->set_resp_msg(pb_resp_msg);
    VLOG(1) << "RPC server:"
            << " pause called, " << pb_req_msg->ms() << " ms";

  } else if (method_name == "addr") {
    // TODO:
  } else if (method_name == "socketNotOpen") {
    auto pb_resp_msg = std::make_shared<EmptyResponseProto>();
    response->set_resp_msg(pb_resp_msg);
  }

  return folly::makeFuture<std::unique_ptr<Response>>(std::move(response));
}
}  // namespace hbase
