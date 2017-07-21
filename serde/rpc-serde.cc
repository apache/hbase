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

#include <folly/Conv.h>
#include <folly/Logging.h>
#include <folly/io/Cursor.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/message.h>
#include <boost/algorithm/string.hpp>

#include <utility>

#include "if/RPC.pb.h"
#include "rpc-serde.h"
#include "utils/version.h"

using folly::IOBuf;
using folly::io::RWPrivateCursor;
using google::protobuf::Message;
using google::protobuf::io::ArrayInputStream;
using google::protobuf::io::ArrayOutputStream;
using google::protobuf::io::CodedInputStream;
using google::protobuf::io::CodedOutputStream;
using google::protobuf::io::ZeroCopyOutputStream;

namespace hbase {

static const std::string PREAMBLE = "HBas";
static const std::string INTERFACE = "ClientService";
static const uint8_t RPC_VERSION = 0;
static const uint8_t DEFAULT_AUTH_TYPE = 80;
static const uint8_t KERBEROS_AUTH_TYPE = 81;

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

RpcSerde::RpcSerde() {}

RpcSerde::RpcSerde(std::shared_ptr<Codec> codec) : codec_(codec) {}

std::unique_ptr<IOBuf> RpcSerde::Preamble(bool secure) {
  auto magic = IOBuf::copyBuffer(PREAMBLE, 0, 2);
  magic->append(2);
  RWPrivateCursor c(magic.get());
  c.skip(4);
  // Version
  c.write(RPC_VERSION);
  if (secure) {
    // for now support only KERBEROS (DIGEST is not supported)
    c.write(KERBEROS_AUTH_TYPE);
  } else {
    c.write(DEFAULT_AUTH_TYPE);
  }
  return magic;
}

std::unique_ptr<IOBuf> RpcSerde::Header(const std::string &user) {
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

  std::unique_ptr<pb::VersionInfo> version_info = CreateVersionInfo();

  h.set_allocated_version_info(version_info.release());

  if (codec_ != nullptr) {
    h.set_cell_block_codec_class(codec_->java_class_name());
  }
  return PrependLength(SerializeMessage(h));
}

std::unique_ptr<pb::VersionInfo> RpcSerde::CreateVersionInfo() {
  std::unique_ptr<pb::VersionInfo> version_info = std::make_unique<pb::VersionInfo>();
  version_info->set_user(Version::user);
  version_info->set_revision(Version::revision);
  version_info->set_url(Version::url);
  version_info->set_date(Version::date);
  version_info->set_src_checksum(Version::src_checksum);
  version_info->set_version(Version::version);

  std::string version{Version::version};
  std::vector<std::string> version_parts;
  boost::split(version_parts, version, boost::is_any_of("."), boost::token_compress_on);
  uint32_t major_version = 0, minor_version = 0;
  if (version_parts.size() >= 2) {
    version_info->set_version_major(folly::to<uint32_t>(version_parts[0]));
    version_info->set_version_minor(folly::to<uint32_t>(version_parts[1]));
  }

  VLOG(1) << "Client VersionInfo:" << version_info->ShortDebugString();
  return version_info;
}

std::unique_ptr<IOBuf> RpcSerde::Request(const uint32_t call_id, const std::string &method,
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

std::unique_ptr<folly::IOBuf> RpcSerde::Response(const uint32_t call_id,
                                                 const google::protobuf::Message *msg) {
  pb::ResponseHeader rh;
  rh.set_call_id(call_id);
  auto ser_header = SerializeDelimited(rh);
  auto ser_resp = SerializeDelimited(*msg);
  ser_header->appendChain(std::move(ser_resp));

  return PrependLength(std::move(ser_header));
}

std::unique_ptr<CellScanner> RpcSerde::CreateCellScanner(std::unique_ptr<folly::IOBuf> buf,
                                                         uint32_t offset, uint32_t length) {
  if (codec_ == nullptr) {
    return nullptr;
  }
  return codec_->CreateDecoder(std::move(buf), offset, length);
}

std::unique_ptr<IOBuf> RpcSerde::PrependLength(std::unique_ptr<IOBuf> msg) {
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

std::unique_ptr<IOBuf> RpcSerde::SerializeDelimited(const Message &msg) {
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
std::unique_ptr<IOBuf> RpcSerde::SerializeMessage(const Message &msg) {
  auto buf = IOBuf::copyBuffer(msg.SerializeAsString());
  return buf;
}
}  // namespace hbase
