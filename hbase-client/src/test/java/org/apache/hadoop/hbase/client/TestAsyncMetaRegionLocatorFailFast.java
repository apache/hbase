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
 */
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ClientTests.class, SmallTests.class })
public class TestAsyncMetaRegionLocatorFailFast {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncMetaRegionLocatorFailFast.class);

  private static Configuration CONF = HBaseConfiguration.create();

  private static AsyncMetaRegionLocator LOCATOR;

  private static final class FaultyConnectionRegistry extends DoNothingConnectionRegistry {

    public FaultyConnectionRegistry(Configuration conf) {
      super(conf);
    }

    @Override
    public CompletableFuture<RegionLocations> getMetaRegionLocations() {
      return FutureUtils.failedFuture(new DoNotRetryRegionException("inject error"));
    }
  }

  @BeforeClass
  public static void setUp() {
    LOCATOR = new AsyncMetaRegionLocator(new FaultyConnectionRegistry(CONF));
  }

  @Test(expected = DoNotRetryIOException.class)
  public void test() throws IOException {
    FutureUtils.get(LOCATOR.getRegionLocations(RegionInfo.DEFAULT_REPLICA_ID, false));
  }
}
