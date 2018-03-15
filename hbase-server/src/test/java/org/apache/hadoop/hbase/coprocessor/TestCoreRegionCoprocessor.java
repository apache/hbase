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
package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MockRegionServerServices;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Test CoreCoprocessor Annotation works giving access to facility not usually available.
 * Test RegionCoprocessor.
 */
@Category({CoprocessorTests.class, SmallTests.class})
public class TestCoreRegionCoprocessor {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCoreRegionCoprocessor.class);

  @Rule public TestName name = new TestName();
  HBaseTestingUtility HTU = HBaseTestingUtility.createLocalHTU();
  private HRegion region = null;
  private RegionServerServices rss;

  @Before
  public void before() throws IOException {
    String methodName = this.name.getMethodName();
    TableName tn = TableName.valueOf(methodName);
    ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(methodName)).build();
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tn).setColumnFamily(cfd).build();
    RegionInfo ri = RegionInfoBuilder.newBuilder(tn).build();
    this.rss = new MockRegionServerServices(HTU.getConfiguration());
    this.region = HRegion.openHRegion(ri, td, null, HTU.getConfiguration(), this.rss, null);
  }

  @After
  public void after() throws IOException {
    this.region.close();
  }

  /**
   * No annotation with CoreCoprocessor. This should make it so I can NOT get at instance of a
   * RegionServerServices instance after some gymnastics.
   */
  public static class NotCoreRegionCoprocessor implements RegionCoprocessor {
    public NotCoreRegionCoprocessor() {}
  }

  /**
   * Annotate with CoreCoprocessor. This should make it so I can get at instance of a
   * RegionServerServices instance after some gymnastics.
   */
  @org.apache.hadoop.hbase.coprocessor.CoreCoprocessor
  public static class CoreRegionCoprocessor implements RegionCoprocessor {
    public CoreRegionCoprocessor() {}
  }

  /**
   * Assert that when a Coprocessor is annotated with CoreCoprocessor, then it is possible to
   * access a RegionServerServices instance. Assert the opposite too.
   * Do it to RegionCoprocessors.
   * @throws IOException
   */
  @Test
  public void testCoreRegionCoprocessor() throws IOException {
    RegionCoprocessorHost rch = region.getCoprocessorHost();
    RegionCoprocessorEnvironment env =
        rch.load(null, NotCoreRegionCoprocessor.class.getName(), 0, HTU.getConfiguration());
    assertFalse(env instanceof HasRegionServerServices);
    env = rch.load(null, CoreRegionCoprocessor.class.getName(), 1, HTU.getConfiguration());
    assertTrue(env instanceof HasRegionServerServices);
    assertEquals(this.rss, ((HasRegionServerServices)env).getRegionServerServices());
  }
}
