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

#include "serde/rpc.h"

#include <folly/Logging.h>
#include <folly/io/Cursor.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/message.h>

#include <utility>

#include "if/HBase.pb.h"
#include "if/RPC.pb.h"

using namespace hbase;

using folly::IOBuf;
using folly::io::RWPrivateCursor;
using google::protobuf::Message;
using google::protobuf::Message;
using google::protobuf::io::ArrayInputStream;
using google::protobuf::io::ArrayOutputStream;
using google::protobuf::io::CodedInputStream;
using google::protobuf::io::CodedOutputStream;
using google::protobuf::io::ZeroCopyOutputStream;
using std::string;
using std::unique_ptr;

static const std::string PREAMBLE = "HBas";
static const std::string INTERFACE = "ClientService";
static const uint8_t RPC_VERSION = 0;
static const uint8_t DEFAULT_AUTH_TYPE = 80;

int RpcSerde::ParseDelimited(const IOBuf *buf, Message *msg) {
  if (buf == nullptr || msg == nullptr) {
    return -2;
  }

  DCHECK(!buf->isChained());

  ArrayInputStream ais{buf->data(), static_cast<int>(buf->length())};
  CodedInputStream coded_stream{&ais};

  uint32_t msg_size;

  // Try and read the varint.
  if (coded_stream.ReadVarint32(&msg_size) == false) {
    FB_LOG_EVERY_MS(ERROR, 1000) << "Unable to read a var uint32_t";
    return -3;
  }

  coded_stream.PushLimit(msg_size);
  // Parse the message.
  if (msg->MergeFromCodedStream(&coded_stream) == false) {
    FB_LOG_EVERY_MS(ERROR, 1000) << "Unable to read a protobuf message from data.";
    return -4;
  }

  // Make sure all the data was consumed.
  if (coded_stream.ConsumedEntireMessage() == false) {
    FB_LOG_EVERY_MS(ERROR, 1000) << "Orphaned data left after reading protobuf message";
    return -5;
  }

  return coded_stream.CurrentPosition();
}

RpcSerde::RpcSerde() : auth_type_(DEFAULT_AUTH_TYPE) {}

unique_ptr<IOBuf> RpcSerde::Preamble() {
  auto magic = IOBuf::copyBuffer(PREAMBLE, 0, 2);
  magic->append(2);
  RWPrivateCursor c(magic.get());
  c.skip(4);
  // Version
  c.write(RPC_VERSION);
  // Standard security aka Please don't lie to me.
  c.write(auth_type_);
  return magic;
}

unique_ptr<IOBuf> RpcSerde::Header(const string &user) {
  pb::ConnectionHeader h;

  // TODO(eclark): Make this not a total lie.
  h.mutable_user_info()->set_effective_user(user);
  // The service name that we want to talk to.
  //
  // Right now we're completely ignoring the service interface.
  // That may or may not be the correct thing to do.
  // It worked for a while with the java client; until it
  // didn't.
  // TODO: send the service name and user from the RpcClient
  h.set_service_name(INTERFACE);
  return PrependLength(SerializeMessage(h));
}

unique_ptr<IOBuf> RpcSerde::Request(const uint32_t call_id, const string &method,
                                    const Message *msg) {
  pb::RequestHeader rq;
  rq.set_method_name(method);
  rq.set_call_id(call_id);
  rq.set_request_param(msg != nullptr);
  auto ser_header = SerializeDelimited(rq);
  if (msg != nullptr) {
    auto ser_req = SerializeDelimited(*msg);
    ser_header->appendChain(std::move(ser_req));
  }

  return PrependLength(std::move(ser_header));
}

unique_ptr<IOBuf> RpcSerde::PrependLength(unique_ptr<IOBuf> msg) {
  // Java ints are 4 long. So create a buffer that large
  auto len_buf = IOBuf::create(4);
  // Then make those bytes visible.
  len_buf->append(4);

  RWPrivateCursor c(len_buf.get());
  // Get the size of the data to be pushed out the network.
  auto size = msg->computeChainDataLength();

  // Write the length to this IOBuf.
  c.writeBE(static_cast<uint32_t>(size));

  // Then attach the origional to the back of len_buf
  len_buf->appendChain(std::move(msg));
  return len_buf;
}

unique_ptr<IOBuf> RpcSerde::SerializeDelimited(const Message &msg) {
  // Get the buffer size needed for just the message.
  int msg_size = msg.ByteSize();
  int buf_size = CodedOutputStream::VarintSize32(msg_size) + msg_size;

  // Create a buffer big enough to hold the varint and the object.
  auto buf = IOBuf::create(buf_size);
  buf->append(buf_size);

  // Create the array output stream.
  ArrayOutputStream aos{buf->writableData(), static_cast<int>(buf->length())};
  // Wrap the ArrayOuputStream in the coded output stream to allow writing
  // Varint32
  CodedOutputStream cos{&aos};

  // Write out the size.
  cos.WriteVarint32(msg_size);

  // Now write the rest out.
  // We're using the protobuf output streams here to keep track
  // of where in the output array we are rather than IOBuf.
  msg.SerializeWithCachedSizesToArray(cos.GetDirectBufferForNBytesAndAdvance(msg_size));

  // Return the buffer.
  return buf;
}
// TODO(eclark): Make this 1 copy.
unique_ptr<IOBuf> RpcSerde::SerializeMessage(const Message &msg) {
  auto buf = IOBuf::copyBuffer(msg.SerializeAsString());
  return buf;
}
