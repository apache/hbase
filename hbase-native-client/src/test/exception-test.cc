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

#include "hbase/exceptions/exception.h"

#include "folly/ExceptionWrapper.h"

using hbase::ExceptionUtil;
using hbase::IOException;
using hbase::RemoteException;

TEST(ExceptionUtilTest, IOExceptionShouldRetry) {
  IOException ex{};
  EXPECT_TRUE(ExceptionUtil::ShouldRetry(ex));

  ex.set_do_not_retry(true);
  EXPECT_FALSE(ExceptionUtil::ShouldRetry(ex));

  ex.set_do_not_retry(false);
  EXPECT_TRUE(ExceptionUtil::ShouldRetry(ex));

  IOException ex2{"description", true};
  EXPECT_FALSE(ExceptionUtil::ShouldRetry(ex2));

  IOException ex3{"description", std::runtime_error("ex"), true};
  EXPECT_FALSE(ExceptionUtil::ShouldRetry(ex3));
}

TEST(ExceptionUtilTest, RemoteExceptionShouldRetry) {
  RemoteException ex{};
  EXPECT_TRUE(ExceptionUtil::ShouldRetry(ex));

  ex.set_do_not_retry(true);
  EXPECT_FALSE(ExceptionUtil::ShouldRetry(ex));

  ex.set_do_not_retry(false);
  EXPECT_TRUE(ExceptionUtil::ShouldRetry(ex));

  ex.set_exception_class_name("org.apache.hadoop.hbase.FooException");
  EXPECT_TRUE(ExceptionUtil::ShouldRetry(ex));

  ex.set_exception_class_name("org.apache.hadoop.hbase.NotServingRegionException");
  EXPECT_TRUE(ExceptionUtil::ShouldRetry(ex));

  ex.set_exception_class_name("org.apache.hadoop.hbase.UnknownRegionException");
  EXPECT_FALSE(ExceptionUtil::ShouldRetry(ex));
}
