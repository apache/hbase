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
    : user_name_(user_name), need_send_header_(true), ser_(), deser_(),
      resp_msgs_() {}

void ClientHandler::read(Context *ctx, std::unique_ptr<IOBuf> buf) {
  if (LIKELY(buf != nullptr)) {
    buf->coalesce();
    Response received;
    ResponseHeader header;

    int used_bytes = deser_.parse_delimited(buf.get(), &header);
    LOG(INFO) << "Read ResponseHeader size=" << used_bytes
              << " call_id=" << header.call_id()
              << " has_exception=" << header.has_exception();

    // Get the response protobuf from the map
    auto search = resp_msgs_.find(header.call_id());
    // It's an error if it's not there.
    CHECK(search != resp_msgs_.end());
    auto resp_msg = search->second;
    CHECK(resp_msg != nullptr);

    // Make sure we don't leak the protobuf
    resp_msgs_.erase(search);

    // set the call_id.
    // This will be used to by the dispatcher to match up
    // the promise with the response.
    received.set_call_id(header.call_id());

    // If there was an exception then there's no
    // data left on the wire.
    if (header.has_exception() == false) {
      buf->trimStart(used_bytes);
      used_bytes = deser_.parse_delimited(buf.get(), resp_msg.get());
      // Make sure that bytes were parsed.
      CHECK(used_bytes == buf->length());
      received.set_response(resp_msg);
    }
    ctx->fireRead(std::move(received));
  }
}

// TODO(eclark): Figure out how to handle the
// network errors that are going to come.
Future<Unit> ClientHandler::write(Context *ctx, std::unique_ptr<Request> r) {
  // Keep track of if we have sent the header.
  if (UNLIKELY(need_send_header_)) {
    need_send_header_ = false;

    // Should we be sending just one fireWrite?
    // Right now we're sending one for the header
    // and one for the request.
    //
    // That doesn't seem like too bad, but who knows.
    auto pre = ser_.preamble();
    auto header = ser_.header(user_name_);
    pre->appendChain(std::move(header));
    ctx->fireWrite(std::move(pre));
  }

  resp_msgs_[r->call_id()] = r->resp_msg();
  return ctx->fireWrite(
      ser_.request(r->call_id(), r->method(), r->req_msg().get()));
}
