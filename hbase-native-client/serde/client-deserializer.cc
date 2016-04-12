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

#include "serde/client-deserializer.h"

#include <google/protobuf/message.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <folly/Logging.h>

using namespace hbase;

using folly::IOBuf;
using google::protobuf::Message;
using google::protobuf::io::ArrayInputStream;
using google::protobuf::io::CodedInputStream;

int ClientDeserializer::parse_delimited(const IOBuf *buf, Message *msg) {
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
    FB_LOG_EVERY_MS(ERROR, 1000)
        << "Unable to read a protobuf message from data.";
    return -4;
  }

  // Make sure all the data was consumed.
  if (coded_stream.ConsumedEntireMessage() == false) {
    FB_LOG_EVERY_MS(ERROR, 1000)
        << "Orphaned data left after reading protobuf message";
    return -5;
  }

  return coded_stream.CurrentPosition();
}
