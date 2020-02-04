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
package org.apache.hadoop.hbase.ipc;

import java.net.InetSocketAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RPCTests.class, SmallTests.class})
public class TestHBaseClient {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHBaseClient.class);

  @Test
  public void testFailedServer(){
    ManualEnvironmentEdge ee = new ManualEnvironmentEdge();
    EnvironmentEdgeManager.injectEdge(ee);
    FailedServers fs = new FailedServers(new Configuration());
    Throwable testThrowable = new Throwable();//throwable already tested in TestFailedServers.java

    InetSocketAddress ia = InetSocketAddress.createUnresolved("bad", 12);
     // same server as ia
    InetSocketAddress ia2 = InetSocketAddress.createUnresolved("bad", 12);
    InetSocketAddress ia3 = InetSocketAddress.createUnresolved("badtoo", 12);
    InetSocketAddress ia4 = InetSocketAddress.createUnresolved("badtoo", 13);


    Assert.assertFalse(fs.isFailedServer(ia));

    fs.addToFailedServers(ia,testThrowable);
    Assert.assertTrue(fs.isFailedServer(ia));
    Assert.assertTrue(fs.isFailedServer(ia2));

    ee.incValue(1);
    Assert.assertTrue(fs.isFailedServer(ia));
    Assert.assertTrue(fs.isFailedServer(ia2));

    ee.incValue(RpcClient.FAILED_SERVER_EXPIRY_DEFAULT + 1);
    Assert.assertFalse(fs.isFailedServer(ia));
    Assert.assertFalse(fs.isFailedServer(ia2));

    fs.addToFailedServers(ia,testThrowable);
    fs.addToFailedServers(ia3,testThrowable);
    fs.addToFailedServers(ia4,testThrowable);

    Assert.assertTrue(fs.isFailedServer(ia));
    Assert.assertTrue(fs.isFailedServer(ia2));
    Assert.assertTrue(fs.isFailedServer(ia3));
    Assert.assertTrue(fs.isFailedServer(ia4));

    ee.incValue(RpcClient.FAILED_SERVER_EXPIRY_DEFAULT + 1);
    Assert.assertFalse(fs.isFailedServer(ia));
    Assert.assertFalse(fs.isFailedServer(ia2));
    Assert.assertFalse(fs.isFailedServer(ia3));
    Assert.assertFalse(fs.isFailedServer(ia4));


    fs.addToFailedServers(ia3,testThrowable);
    Assert.assertFalse(fs.isFailedServer(ia4));
  }
}
