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

#include "connection/client-handler.h"

#include <folly/Likely.h>
#include <glog/logging.h>

#include <string>

#include "connection/request.h"
#include "connection/response.h"
#include "if/Client.pb.h"
#include "if/RPC.pb.h"

using namespace hbase;
using namespace folly;
using namespace wangle;
using hbase::pb::ResponseHeader;
using hbase::pb::GetResponse;
using google::protobuf::Message;

ClientHandler::ClientHandler(std::string user_name)
    : user_name_(user_name), serde_(), once_flag_(std::make_unique<std::once_flag>()),
      resp_msgs_(
          make_unique<folly::AtomicHashMap<
              uint32_t, std::shared_ptr<google::protobuf::Message>>>(5000)) {}

void ClientHandler::read(Context *ctx, std::unique_ptr<IOBuf> buf) {
  if (LIKELY(buf != nullptr)) {
    buf->coalesce();
    Response received;
    ResponseHeader header;

    int used_bytes = serde_.ParseDelimited(buf.get(), &header);
    LOG(INFO) << "Read ResponseHeader size=" << used_bytes
              << " call_id=" << header.call_id()
              << " has_exception=" << header.has_exception();

    // Get the response protobuf from the map
    auto search = resp_msgs_->find(header.call_id());
    // It's an error if it's not there.
    CHECK(search != resp_msgs_->end());
    auto resp_msg = search->second;
    CHECK(resp_msg != nullptr);

    // Make sure we don't leak the protobuf
    resp_msgs_->erase(header.call_id());

    // set the call_id.
    // This will be used to by the dispatcher to match up
    // the promise with the response.
    received.set_call_id(header.call_id());

    // If there was an exception then there's no
    // data left on the wire.
    if (header.has_exception() == false) {
      buf->trimStart(used_bytes);
      used_bytes = serde_.ParseDelimited(buf.get(), resp_msg.get());
      // Make sure that bytes were parsed.
      CHECK(used_bytes == buf->length());
      received.set_resp_msg(resp_msg);
    }
    ctx->fireRead(std::move(received));
  }
}

Future<Unit> ClientHandler::write(Context *ctx, std::unique_ptr<Request> r) {
  // We need to send the header once.
  // So use call_once to make sure that only one thread wins this.
  std::call_once((*once_flag_), [ctx, this]() {
    auto pre = serde_.Preamble();
    auto header = serde_.Header(user_name_);
    pre->appendChain(std::move(header));
    ctx->fireWrite(std::move(pre));
  });

  // Now store the call id to response.
  resp_msgs_->insert(r->call_id(), r->resp_msg());
  // Send the data down the pipeline.
  return ctx->fireWrite(
      serde_.Request(r->call_id(), r->method(), r->req_msg().get()));
}
