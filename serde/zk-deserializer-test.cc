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

#include "serde/zk-deserializer.h"

#include <folly/Logging.h>
#include <folly/io/Cursor.h>
#include <folly/io/IOBuf.h>
#include <gtest/gtest.h>

#include "if/ZooKeeper.pb.h"

using namespace hbase;
using namespace hbase::pb;
using namespace folly;
using namespace std;
using namespace folly::io;

// Test that would test if there's nothing there.
TEST(TestZkDesializer, TestThrowNoMagicNum) {
  ZkDeserializer deser;
  MetaRegionServer mrs;

  auto buf = IOBuf::create(100);
  buf->append(100);
  RWPrivateCursor c{buf.get()};
  c.write<uint8_t>(99);
  ASSERT_THROW(deser.parse(buf.get(), &mrs), runtime_error);
}

// Test if the protobuf is in a format that we can't decode
TEST(TestZkDesializer, TestBadProtoThrow) {
  ZkDeserializer deser;
  MetaRegionServer mrs;
  string magic{"PBUF"};

  // Set ServerName
  mrs.mutable_server()->set_host_name("test");
  mrs.mutable_server()->set_port(567);
  mrs.mutable_server()->set_start_code(9567);

  // One byte magic number
  // four bytes for id length
  // four bytes for id
  // four bytes for PBUF
  uint32_t start_len = 1 + 4 + 4 + 4;
  // How large the protobuf will be
  uint32_t pbuf_size = mrs.ByteSize();

  auto buf = IOBuf::create(start_len + pbuf_size);
  buf->append(start_len + pbuf_size);
  RWPrivateCursor c{buf.get()};

  // Write the magic number
  c.write<uint8_t>(255);
  // Write the id len
  c.writeBE<uint32_t>(4);
  // Write the id
  c.write<uint32_t>(13);
  // Write the PBUF string
  c.push(reinterpret_cast<const uint8_t *>(magic.c_str()), 4);

  // Create the protobuf
  MetaRegionServer out;
  ASSERT_THROW(deser.parse(buf.get(), &out), runtime_error);
}

// Test to make sure the whole thing works.
TEST(TestZkDesializer, TestNoThrow) {
  ZkDeserializer deser;
  MetaRegionServer mrs;
  string magic{"PBUF"};

  // Set ServerName
  mrs.mutable_server()->set_host_name("test");
  mrs.mutable_server()->set_port(567);
  mrs.mutable_server()->set_start_code(9567);

  // One byte magic number
  // four bytes for id length
  // four bytes for id
  // four bytes for PBUF
  uint32_t start_len = 1 + 4 + 4 + 4;
  // How large the protobuf will be
  uint32_t pbuf_size = mrs.ByteSize();

  auto buf = IOBuf::create(start_len + pbuf_size);
  buf->append(start_len + pbuf_size);
  RWPrivateCursor c{buf.get()};

  // Write the magic number
  c.write<uint8_t>(255);
  // Write the id len
  c.writeBE<uint32_t>(4);
  // Write the id
  c.write<uint32_t>(13);
  // Write the PBUF string
  c.push(reinterpret_cast<const uint8_t *>(magic.c_str()), 4);

  // Now write the serialized protobuf
  mrs.SerializeWithCachedSizesToArray(buf->writableData() + start_len);

  // Create the protobuf
  MetaRegionServer out;
  ASSERT_TRUE(deser.parse(buf.get(), &out));
  ASSERT_EQ(mrs.server().host_name(), out.server().host_name());
}
