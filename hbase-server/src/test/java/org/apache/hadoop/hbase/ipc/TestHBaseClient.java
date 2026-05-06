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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(RPCTests.TAG)
@Tag(SmallTests.TAG)
public class TestHBaseClient {
  @Test
  public void testFailedServer() {
    ManualEnvironmentEdge ee = new ManualEnvironmentEdge();
    EnvironmentEdgeManager.injectEdge(ee);
    FailedServers fs = new FailedServers(new Configuration());
    Throwable testThrowable = new Throwable();// throwable already tested in TestFailedServers.java

    Address ia = Address.fromParts("bad", 12);
    // same server as ia
    Address ia2 = Address.fromParts("bad", 12);
    Address ia3 = Address.fromParts("badtoo", 12);
    Address ia4 = Address.fromParts("badtoo", 13);

    assertFalse(fs.isFailedServer(ia));

    fs.addToFailedServers(ia, testThrowable);
    assertTrue(fs.isFailedServer(ia));
    assertTrue(fs.isFailedServer(ia2));

    ee.incValue(1);
    assertTrue(fs.isFailedServer(ia));
    assertTrue(fs.isFailedServer(ia2));

    ee.incValue(RpcClient.FAILED_SERVER_EXPIRY_DEFAULT + 1);
    assertFalse(fs.isFailedServer(ia));
    assertFalse(fs.isFailedServer(ia2));

    fs.addToFailedServers(ia, testThrowable);
    fs.addToFailedServers(ia3, testThrowable);
    fs.addToFailedServers(ia4, testThrowable);

    assertTrue(fs.isFailedServer(ia));
    assertTrue(fs.isFailedServer(ia2));
    assertTrue(fs.isFailedServer(ia3));
    assertTrue(fs.isFailedServer(ia4));

    ee.incValue(RpcClient.FAILED_SERVER_EXPIRY_DEFAULT + 1);
    assertFalse(fs.isFailedServer(ia));
    assertFalse(fs.isFailedServer(ia2));
    assertFalse(fs.isFailedServer(ia3));
    assertFalse(fs.isFailedServer(ia4));

    fs.addToFailedServers(ia3, testThrowable);
    assertFalse(fs.isFailedServer(ia4));
  }
}
