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

#include "hbase/connection/rpc-test-server-handler.h"
#include "hbase/if/RPC.pb.h"
#include "hbase/if/test.pb.h"

namespace hbase {

void RpcTestServerSerializeHandler::read(Context* ctx, std::unique_ptr<folly::IOBuf> buf) {
  buf->coalesce();
  pb::RequestHeader header;

  int used_bytes = serde_.ParseDelimited(buf.get(), &header);
  VLOG(3) << "Read RPC RequestHeader size=" << used_bytes << " call_id=" << header.call_id();

  auto received = CreateReceivedRequest(header.method_name());

  buf->trimStart(used_bytes);
  if (header.has_request_param() && received != nullptr) {
    used_bytes = serde_.ParseDelimited(buf.get(), received->req_msg().get());
    VLOG(3) << "Read RPCRequest, buf length:" << buf->length()
            << ", header PB length:" << used_bytes;
    received->set_call_id(header.call_id());
  }

  if (received != nullptr) {
    ctx->fireRead(std::move(received));
  }
}

folly::Future<folly::Unit> RpcTestServerSerializeHandler::write(Context* ctx,
                                                                std::unique_ptr<Response> resp) {
  VLOG(3) << "Writing RPC Request";
  // Send the data down the pipeline.
  return ctx->fireWrite(
      serde_.Response(resp->call_id(), resp->resp_msg().get(), resp->exception()));
}

std::unique_ptr<Request> RpcTestServerSerializeHandler::CreateReceivedRequest(
    const std::string& method_name) {
  std::unique_ptr<Request> result = nullptr;

  if (method_name == "ping") {
    result = std::make_unique<Request>(std::make_shared<EmptyRequestProto>(),
                                       std::make_shared<EmptyResponseProto>(), method_name);
  } else if (method_name == "echo") {
    result = std::make_unique<Request>(std::make_shared<EchoRequestProto>(),
                                       std::make_shared<EchoResponseProto>(), method_name);
  } else if (method_name == "error") {
    result = std::make_unique<Request>(std::make_shared<EmptyRequestProto>(),
                                       std::make_shared<EmptyResponseProto>(), method_name);
  } else if (method_name == "pause") {
    result = std::make_unique<Request>(std::make_shared<PauseRequestProto>(),
                                       std::make_shared<EmptyResponseProto>(), method_name);
  } else if (method_name == "addr") {
    result = std::make_unique<Request>(std::make_shared<EmptyRequestProto>(),
                                       std::make_shared<AddrResponseProto>(), method_name);
  } else if (method_name == "socketNotOpen") {
    result = std::make_unique<Request>(std::make_shared<EmptyRequestProto>(),
                                       std::make_shared<EmptyResponseProto>(), method_name);
  }
  return result;
}
}  // end of namespace hbase
