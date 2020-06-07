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

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * See HBASE-24513.
 */
@Category({ ClientTests.class, SmallTests.class })
public class TestAsyncConnectionConfiguration {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncConnectionConfiguration.class);

  @Test
  public void testDefaultReadWriteRpcTimeout() {
    Configuration conf = HBaseConfiguration.create();
    long timeoutMs = 1000;
    conf.setLong(HConstants.HBASE_RPC_TIMEOUT_KEY, timeoutMs);
    AsyncConnectionConfiguration config = new AsyncConnectionConfiguration(conf);
    long expected = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    assertEquals(expected, config.getRpcTimeoutNs());
    assertEquals(expected, config.getReadRpcTimeoutNs());
    assertEquals(expected, config.getWriteRpcTimeoutNs());
  }
}
