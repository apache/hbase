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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({SmallTests.class, ClientTests.class})
public class TestBufferedMutator {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestBufferedMutator.class);

  @Rule
  public TestName name = new TestName();

  /**
   * My BufferedMutator.
   * Just to prove that I can insert a BM other than default.
   */
  public static class MyBufferedMutator extends BufferedMutatorImpl {
    MyBufferedMutator(ClusterConnection conn, RpcRetryingCallerFactory rpcCallerFactory,
        RpcControllerFactory rpcFactory, BufferedMutatorParams params) {
      super(conn, rpcCallerFactory, rpcFactory, params);
    }
  }

  @Test
  public void testAlternateBufferedMutatorImpl() throws IOException {
    BufferedMutatorParams params =  new BufferedMutatorParams(TableName.valueOf(name.getMethodName()));
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY,
        DoNothingConnectionRegistry.class.getName());
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      BufferedMutator bm = connection.getBufferedMutator(params);
      // Assert we get default BM if nothing specified.
      assertTrue(bm instanceof BufferedMutatorImpl);
      // Now try and set my own BM implementation.
      params.implementationClassName(MyBufferedMutator.class.getName());
      bm = connection.getBufferedMutator(params);
      assertTrue(bm instanceof MyBufferedMutator);
    }
    // Now try creating a Connection after setting an alterate BufferedMutator into
    // the configuration and confirm we get what was expected.
    conf.set(BufferedMutator.CLASSNAME_KEY, MyBufferedMutator.class.getName());
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      BufferedMutator bm = connection.getBufferedMutator(params);
      assertTrue(bm instanceof MyBufferedMutator);
    }
  }
}
