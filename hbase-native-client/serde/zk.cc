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

#include "serde/zk.h"

#include <folly/io/Cursor.h>
#include <folly/io/IOBuf.h>
#include <google/protobuf/message.h>

using hbase::ZkDeserializer;
using std::runtime_error;
using folly::IOBuf;
using folly::io::Cursor;
using google::protobuf::Message;

static const std::string MAGIC_STRING = "PBUF";

bool ZkDeserializer::Parse(IOBuf *buf, Message *out) {

  // The format is like this
  // 1 byte of magic number. 255
  // 4 bytes of id length.
  // id_length number of bytes for the id of who put up the znode
  // 4 bytes of a magic string PBUF
  // Then the protobuf serialized without a varint header.

  Cursor c{buf};

  // There should be a magic number for recoverable zk
  uint8_t magic_num = c.read<uint8_t>();
  if (magic_num != 255) {
    LOG(ERROR) << "Magic number not in ZK znode data expected 255 got ="
               << unsigned(magic_num);
    throw runtime_error("Magic number not in znode data");
  }
  // How long is the id?
  uint32_t id_len = c.readBE<uint32_t>();

  if (id_len >= c.length()) {
    LOG(ERROR) << "After skiping the if from zookeeper data there's not enough "
                  "left to read anything else";
    throw runtime_error("Not enough bytes to decode from zookeeper");
  }

  // Skip the id
  c.skip(id_len);

  // Make sure that the magic string is there.
  if (MAGIC_STRING != c.readFixedString(4)) {
    LOG(ERROR) << "There was no PBUF magic string.";
    throw runtime_error("No PBUF magic string in the zookpeeper data.");
  }

  // Try to decode the protobuf.
  // If there's an error bail out.
  if (out->ParseFromArray(c.data(), c.length()) == false) {
    LOG(ERROR) << "Error parsing Protobuf Message";
    throw runtime_error("Error parsing protobuf");
  }

  return true;
}
