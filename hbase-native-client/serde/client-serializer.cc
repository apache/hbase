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
#include "serde/client-serializer.h"

#include <folly/io/Cursor.h>
#include <folly/Logging.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include "if/HBase.pb.h"
#include "if/RPC.pb.h"

using namespace hbase;

using folly::IOBuf;
using folly::io::RWPrivateCursor;
using google::protobuf::Message;
using google::protobuf::io::ArrayOutputStream;
using google::protobuf::io::CodedOutputStream;
using google::protobuf::io::ZeroCopyOutputStream;
using std::string;
using std::unique_ptr;

static const std::string PREAMBLE = "HBas";
static const std::string INTERFACE = "ClientService";
static const uint8_t RPC_VERSION = 0;
static const uint8_t DEFAULT_AUTH_TYPE = 80;

ClientSerializer::ClientSerializer() : auth_type_(DEFAULT_AUTH_TYPE) {}

unique_ptr<IOBuf> ClientSerializer::preamble() {
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

unique_ptr<IOBuf> ClientSerializer::header(const string &user) {
  pb::ConnectionHeader h;

  // TODO(eclark): Make this not a total lie.
  h.mutable_user_info()->set_effective_user(user);
  // The service name that we want to talk to.
  //
  // Right now we're completely ignoring the service interface.
  // That may or may not be the correct thing to do.
  // It worked for a while with the java client; until it
  // didn't.
  h.set_service_name(INTERFACE);
  return prepend_length(serialize_message(h));
}

unique_ptr<IOBuf> ClientSerializer::request(const uint32_t call_id,
                                            const string &method,
                                            const Message *msg) {
  pb::RequestHeader rq;
  rq.set_method_name(method);
  rq.set_call_id(call_id);
  rq.set_request_param(msg != nullptr);
  auto ser_header = serialize_delimited(rq);
  if (msg != nullptr) {
    auto ser_req = serialize_delimited(*msg);
    ser_header->appendChain(std::move(ser_req));
  }

  return prepend_length(std::move(ser_header));
}

unique_ptr<IOBuf> ClientSerializer::prepend_length(unique_ptr<IOBuf> msg) {
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

unique_ptr<IOBuf> ClientSerializer::serialize_delimited(const Message &msg) {
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
  msg.SerializeWithCachedSizesToArray(
      cos.GetDirectBufferForNBytesAndAdvance(msg_size));

  // Return the buffer.
  return buf;
}
// TODO(eclark): Make this 1 copy.
unique_ptr<IOBuf> ClientSerializer::serialize_message(const Message &msg) {
  auto buf = IOBuf::copyBuffer(msg.SerializeAsString());
  return buf;
}
