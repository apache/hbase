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

#include "core/configuration.h"
#include <gtest/gtest.h>

using hbase::Configuration;

TEST(Configuration, SetGetBool) {
  Configuration conf;

  /* test true/false */
  conf.SetBool("bool_key1", true);
  EXPECT_EQ(true, conf.GetBool("bool_key1", false));
  conf.SetBool("bool_key2", false);
  EXPECT_EQ(false, conf.GetBool("bool_key2", true));

  /* test 1/0 */
  conf.SetBool("bool_key3", 1);
  EXPECT_EQ(true, conf.GetBool("bool_key3", false));
  conf.SetBool("bool_key4", 0);
  EXPECT_EQ(false, conf.GetBool("bool_key4", true));

  /* test non zero integer */
  conf.SetBool("bool_key5", 5);
  EXPECT_EQ(true, conf.GetBool("bool_key5", false));
  conf.SetBool("bool_key6", -1);
  EXPECT_EQ(true, conf.GetBool("bool_key5", false));

  /* test non zero float */
  conf.SetBool("bool_key7", 5.1);
  EXPECT_EQ(true, conf.GetBool("bool_key7", false));
  conf.SetBool("bool_key8", -1.2);
  EXPECT_EQ(true, conf.GetBool("bool_key8", false));
}

TEST(Configuration, SetGetForBool) {
  Configuration conf;

  /* test true/false */
  conf.Set("bool_key1", "true");
  EXPECT_EQ(true, conf.GetBool("bool_key1", false));
  conf.Set("bool_key2", "false");
  EXPECT_EQ(false, conf.GetBool("bool_key2", true));

  /* test 1/0 */
  conf.Set("bool_key3", "1");
  EXPECT_EQ(true, conf.GetBool("bool_key3", false));
  conf.Set("bool_key4", "0");
  EXPECT_EQ(false, conf.GetBool("bool_key4", true));

  /* test non zero integer */
  conf.Set("bool_key5", "5");
  EXPECT_THROW(conf.GetBool("bool_key5", false), std::runtime_error);
  conf.Set("bool_key6", "-1");
  EXPECT_THROW(conf.GetBool("bool_key6", false), std::runtime_error);

  /* test non zero float */
  conf.Set("bool_key7", "5.1");
  EXPECT_THROW(conf.GetBool("bool_key7", false), std::runtime_error);
  conf.Set("bool_key8", "-1.2");
  EXPECT_THROW(conf.GetBool("bool_key8", false), std::runtime_error);
}

TEST(Configuration, SetGet) {
  Configuration conf;

  EXPECT_EQ(conf.Get("foo", "default"), "default");
  conf.Set("foo", "bar");
  EXPECT_EQ(conf.Get("foo", "default"), "bar");
}

TEST(Configuration, SetGetInt) {
  Configuration conf;

  EXPECT_EQ(conf.GetInt("foo", 0), 0);
  conf.SetInt("foo", 42);
  EXPECT_EQ(conf.GetInt("foo", 0), 42);
}

TEST(Configuration, SetGetLong) {
  Configuration conf;

  EXPECT_EQ(conf.GetLong("foo", 0), 0);
  conf.SetLong("foo", 42);
  EXPECT_EQ(conf.GetLong("foo", 0), 42);
}

TEST(Configuration, SetGetDouble) {
  Configuration conf;

  EXPECT_EQ(conf.GetDouble("foo", 0), 0);
  conf.SetDouble("foo", 42.0);
  EXPECT_EQ(conf.GetDouble("foo", 0), 42.0);
}

TEST(Configuration, SetGetBoolBasic) {
  Configuration conf;

  EXPECT_EQ(conf.GetBool("foo", false), false);
  conf.SetInt("foo", true);
  EXPECT_EQ(conf.GetInt("foo", false), true);
}
