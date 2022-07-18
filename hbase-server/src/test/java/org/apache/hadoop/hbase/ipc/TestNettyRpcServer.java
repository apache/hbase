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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNameTestRule;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.LoadTestKVGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@Category({ RPCTests.class, MediumTests.class })
@RunWith(Parameterized.class)
public class TestNettyRpcServer {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestNettyRpcServer.class);

  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  private static final int NUM_ROWS = 100;
  private static final int MIN_LEN = 1000;
  private static final int MAX_LEN = 1000000;
  protected static final LoadTestKVGenerator GENERATOR = new LoadTestKVGenerator(MIN_LEN, MAX_LEN);
  protected static HBaseTestingUtil TEST_UTIL;

  @Rule
  public TableNameTestRule name = new TableNameTestRule();

  @Parameterized.Parameter
  public String allocatorType;

  @Parameters
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[][] { { NettyRpcServer.POOLED_ALLOCATOR_TYPE },
      { NettyRpcServer.UNPOOLED_ALLOCATOR_TYPE }, { NettyRpcServer.HEAP_ALLOCATOR_TYPE },
      { SimpleByteBufAllocator.class.getName() } });
  }

  @Before
  public void setup() throws Exception {
    // A subclass may have already created TEST_UTIL and is now upcalling to us
    if (TEST_UTIL == null) {
      TEST_UTIL = new HBaseTestingUtil();
    }
    TEST_UTIL.getConfiguration().set(RpcServerFactory.CUSTOM_RPC_SERVER_IMPL_CONF_KEY,
      NettyRpcServer.class.getName());
    TEST_UTIL.getConfiguration().set(NettyRpcServer.HBASE_NETTY_ALLOCATOR_KEY, allocatorType);
    TEST_UTIL.startMiniCluster();
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testNettyRpcServer() throws Exception {
    doTest(name.getTableName());
  }

  protected void doTest(TableName tableName) throws Exception {
    // Splitting just complicates the test scenario, disable it
    final TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
      .setRegionSplitPolicyClassName(DisabledRegionSplitPolicy.class.getName()).build();
    try (Table table =
      TEST_UTIL.createTable(desc, new byte[][] { FAMILY }, TEST_UTIL.getConfiguration())) {
      // put some test data
      for (int i = 0; i < NUM_ROWS; i++) {
        final byte[] rowKey = Bytes.toBytes(LoadTestKVGenerator.md5PrefixedKey(i));
        final byte[] v = GENERATOR.generateRandomSizeValue(rowKey, QUALIFIER);
        table.put(new Put(rowKey).addColumn(FAMILY, QUALIFIER, v));
      }
      // read to verify it.
      for (int i = 0; i < NUM_ROWS; i++) {
        final byte[] rowKey = Bytes.toBytes(LoadTestKVGenerator.md5PrefixedKey(i));
        final Result r = table.get(new Get(rowKey).addColumn(FAMILY, QUALIFIER));
        assertNotNull("Result was empty", r);
        final byte[] v = r.getValue(FAMILY, QUALIFIER);
        assertNotNull("Result did not contain expected value", v);
        assertTrue("Value was not verified", LoadTestKVGenerator.verify(v, rowKey, QUALIFIER));
      }
    }
  }

}
