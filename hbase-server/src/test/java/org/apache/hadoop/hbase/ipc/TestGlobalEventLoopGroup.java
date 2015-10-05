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
package org.apache.hadoop.hbase.ipc;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ SmallTests.class })
public class TestGlobalEventLoopGroup {

  @Test
  public void test() {
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean(AsyncRpcClient.USE_GLOBAL_EVENT_LOOP_GROUP, true);
    AsyncRpcClient client = new AsyncRpcClient(conf);
    assertNotNull(AsyncRpcClient.GLOBAL_EVENT_LOOP_GROUP);
    AsyncRpcClient client1 = new AsyncRpcClient(conf);
    assertSame(client.bootstrap.group(), client1.bootstrap.group());
    client1.close();
    assertFalse(client.bootstrap.group().isShuttingDown());

    conf.setBoolean(AsyncRpcClient.USE_GLOBAL_EVENT_LOOP_GROUP, false);
    AsyncRpcClient client2 = new AsyncRpcClient(conf);
    assertNotSame(client.bootstrap.group(), client2.bootstrap.group());
    client2.close();

    client.close();
  }
}
