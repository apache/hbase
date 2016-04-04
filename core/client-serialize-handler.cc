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

#include "core/client-serialize-handler.h"

#include <string>

using namespace hbase;
using namespace folly;
using namespace wangle;

static const std::string PREAMBLE = "HBas";
static const std::string INTERFACE = "ClientService";
static const uint8_t RPC_VERSION = 0;
static const uint8_t AUTH_TYPE = 80;

// TODO(eclark): Make this actually do ANYTHING.
void ClientSerializeHandler::read(Context *ctx, std::unique_ptr<IOBuf> msg) {
  Response received;
  ctx->fireRead(received);
}

Future<Unit> ClientSerializeHandler::write(Context *ctx, Request r) {
  // Keep track of if we have sent the header.
  if (need_send_header_) {
    need_send_header_ = false;

    // Should this be replacing the IOBuf rather than
    // sending several different calls?
    write_preamble(ctx);
    write_header(ctx);
  }

  // Send out the actual request and not just a test string.
  std::string out{"test"};
  return ctx->fireWrite(prepend_length(IOBuf::copyBuffer(out)));
}

Future<Unit> ClientSerializeHandler::write_preamble(Context *ctx) {
  auto magic = IOBuf::copyBuffer(PREAMBLE);
  auto buf = IOBuf::create(2);
  buf->append(2);
  folly::io::RWPrivateCursor c(buf.get());

  // Version
  c.write(RPC_VERSION);
  // Standard security aka Please don't lie to me.
  c.write(AUTH_TYPE);
  magic->appendChain(std::move(buf));
  return ctx->fireWrite(std::move(magic));
}

Future<Unit> ClientSerializeHandler::write_header(Context *ctx) {
  pb::ConnectionHeader h;

  // TODO(eclark): Make this not a total lie.
  h.mutable_user_info()->set_effective_user("elliott");
  // The service name that we want to talk to.
  //
  // Right now we're completely ignoring the service interface.
  // That may or may not be the correct thing to do.
  // It worked for a while with the java client; until it
  // didn't.
  h.set_service_name(INTERFACE);
  // TODO(eclark): Make this 1 copy.
  auto msg = IOBuf::copyBuffer(h.SerializeAsString());
  return ctx->fireWrite(prepend_length(std::move(msg)));
}

// Our own simple version of LengthFieldPrepender
std::unique_ptr<IOBuf>
ClientSerializeHandler::prepend_length(std::unique_ptr<IOBuf> msg) {
  // Java ints are 4 long. So create a buffer that large
  auto len_buf = IOBuf::create(4);
  // Then make those bytes visible.
  len_buf->append(4);

  io::RWPrivateCursor c(len_buf.get());
  // Get the size of the data to be pushed out the network.
  auto size = msg->computeChainDataLength();

  // Write the length to this IOBuf.
  c.writeBE(static_cast<uint32_t>(size));

  // Then attach the origional to the back of len_buf
  len_buf->appendChain(std::move(msg));
  return len_buf;
}
