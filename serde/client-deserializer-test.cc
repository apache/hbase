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

#include <folly/io/IOBuf.h>
#include <gtest/gtest.h>

#include "if/Client.pb.h"

using namespace hbase;
using folly::IOBuf;
using hbase::pb::GetRequest;
using hbase::pb::RegionSpecifier;
using hbase::pb::RegionSpecifier_RegionSpecifierType;

TEST(TestRpcSerde, TestReturnFalseOnNullPtr) {
  RpcSerde deser;
  ASSERT_LT(deser.ParseDelimited(nullptr, nullptr), 0);
}

TEST(TestRpcSerde, TestReturnFalseOnBadInput) {
  RpcSerde deser;
  auto buf = IOBuf::copyBuffer("test");
  GetRequest gr;

  ASSERT_LT(deser.ParseDelimited(buf.get(), &gr), 0);
}

TEST(TestRpcSerde, TestGoodGetRequestFullRoundTrip) {
  GetRequest in;
  RpcSerde ser;
  RpcSerde deser;

  // fill up the GetRequest.
  in.mutable_region()->set_value("test_region_id");
  in.mutable_region()->set_type(
      RegionSpecifier_RegionSpecifierType::
          RegionSpecifier_RegionSpecifierType_ENCODED_REGION_NAME);
  in.mutable_get()->set_row("test_row");

  // Create the buffer
  auto buf = ser.SerializeDelimited(in);

  GetRequest out;

  int used_bytes = deser.ParseDelimited(buf.get(), &out);

  ASSERT_GT(used_bytes, 0);
  ASSERT_EQ(used_bytes, buf->length());
}
