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
#include <gtest/gtest.h>

#include <folly/io/Cursor.h>

#include <string>

#include "serde/client-serializer.h"
#include "if/HBase.pb.h"
#include "if/RPC.pb.h"

using namespace hbase;
using namespace hbase::pb;
using namespace folly;
using namespace folly::io;

TEST(ClientSerializerTest, PreambleIncludesHBas) {
  ClientSerializer ser;
  auto buf = ser.preamble();
  const char *p = reinterpret_cast<const char *>(buf->data());
  // Take the first for chars and make sure they are the
  // magic string
  EXPECT_EQ("HBas", std::string(p, 4));

  EXPECT_EQ(6, buf->computeChainDataLength());
}

TEST(ClientSerializerTest, PreambleIncludesVersion) {
  ClientSerializer ser;
  auto buf = ser.preamble();
  EXPECT_EQ(0, static_cast<const uint8_t *>(buf->data())[4]);
  EXPECT_EQ(80, static_cast<const uint8_t *>(buf->data())[5]);
}

TEST(ClientSerializerTest, TestHeaderLengthPrefixed) {
  ClientSerializer ser;
  auto header = ser.header("elliott");

  // The header should be prefixed by 4 bytes of length.
  EXPECT_EQ(4, header->length());
  EXPECT_TRUE(header->length() < header->computeChainDataLength());
  EXPECT_TRUE(header->isChained());

  // Now make sure the length is correct.
  Cursor cursor(header.get());
  auto prefixed_len = cursor.readBE<uint32_t>();
  EXPECT_EQ(prefixed_len, header->next()->length());
}

TEST(ClientSerializerTest, TestHeaderDecode) {
  ClientSerializer ser;
  auto buf = ser.header("elliott");
  auto header_buf = buf->next();
  ConnectionHeader h;

  EXPECT_TRUE(h.ParseFromArray(header_buf->data(), header_buf->length()));
  EXPECT_EQ("elliott", h.user_info().effective_user());
}
