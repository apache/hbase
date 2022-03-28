/**
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ClientTests.class, SmallTests.class })
public class TestConnectionRegistryLeak {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestConnectionRegistryLeak.class);

  public static final class ConnectionRegistryForTest extends DoNothingConnectionRegistry {

    private boolean closed = false;

    public ConnectionRegistryForTest(Configuration conf) {
      super(conf);
      CREATED.add(this);
    }

    @Override
    public CompletableFuture<String> getClusterId() {
      return FutureUtils.failedFuture(new IOException("inject error"));
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  private static final List<ConnectionRegistryForTest> CREATED = new ArrayList<>();

  private static Configuration CONF = HBaseConfiguration.create();

  @BeforeClass
  public static void setUp() {
    CONF.setClass(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY,
        ConnectionRegistryForTest.class, ConnectionRegistry.class);
  }

  @Test
  public void test() throws InterruptedException {
    for (int i = 0; i < 10; i++) {
      try {
        ConnectionFactory.createAsyncConnection(CONF).get();
        fail();
      } catch (ExecutionException e) {
        // expected
      }
    }
    assertEquals(10, CREATED.size());
    CREATED.forEach(r -> assertTrue(r.closed));
  }
}
