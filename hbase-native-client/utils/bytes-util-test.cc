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

#include <folly/Logging.h>
#include <gtest/gtest.h>
#include <string>

#include "utils/bytes-util.h"

using hbase::BytesUtil;

TEST(TestBytesUtil, TestToStringBinary) {
  std::string empty{""};
  EXPECT_EQ(empty, BytesUtil::ToStringBinary(empty));

  std::string foo_bar{"foo bar"};
  EXPECT_EQ(foo_bar, BytesUtil::ToStringBinary(foo_bar));

  std::string foo_bar2{"foo bar_/!@#$%^&*(){}[]|1234567890"};
  EXPECT_EQ(foo_bar2, BytesUtil::ToStringBinary(foo_bar2));

  char zero = 0;
  EXPECT_EQ("\\x00", BytesUtil::ToStringBinary(std::string{zero}));

  char max = 255;
  EXPECT_EQ("\\xFF", BytesUtil::ToStringBinary(std::string{max}));

  EXPECT_EQ("\\x00\\xFF", BytesUtil::ToStringBinary(std::string{zero} + std::string{max}));

  EXPECT_EQ("foo_\\x00\\xFF_bar",
            BytesUtil::ToStringBinary("foo_" + std::string{zero} + std::string{max} + "_bar"));
}

TEST(TestBytesUtil, TestToStringToInt64) {
  int64_t num = 761235;
  EXPECT_EQ(num, BytesUtil::ToInt64(BytesUtil::ToString(num)));

  num = -56125;
  EXPECT_EQ(num, BytesUtil::ToInt64(BytesUtil::ToString(num)));

  num = 0;
  EXPECT_EQ(num, BytesUtil::ToInt64(BytesUtil::ToString(num)));
}

TEST(TestBytesUtil, TestCreateClosestRowAfter) {
  std::string empty{""};
  EXPECT_EQ(BytesUtil::CreateClosestRowAfter(empty), std::string{'\0'});

  std::string foo{"foo"};
  EXPECT_EQ(BytesUtil::CreateClosestRowAfter(foo), std::string{"foo"} + '\0');

  EXPECT_EQ("f\\x00", BytesUtil::ToStringBinary(BytesUtil::CreateClosestRowAfter("f")));
}
