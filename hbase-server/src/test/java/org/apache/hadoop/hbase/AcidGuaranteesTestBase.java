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
package org.apache.hadoop.hbase;

import static org.apache.hadoop.hbase.AcidGuaranteesTestTool.FAMILIES;
import static org.apache.hadoop.hbase.AcidGuaranteesTestTool.TABLE_NAME;

import java.util.List;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.CompactingMemStore;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.MemStoreLAB;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Test case that uses multiple threads to read and write multifamily rows into a table, verifying
 * that reads never see partially-complete writes. This can run as a junit test, or with a main()
 * function which runs against a real cluster (eg for testing with failures, region movement, etc)
 */
public abstract class AcidGuaranteesTestBase {

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private AcidGuaranteesTestTool tool = new AcidGuaranteesTestTool();

  protected abstract MemoryCompactionPolicy getMemoryCompactionPolicy();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Set small flush size for minicluster so we exercise reseeking scanners
    Configuration conf = UTIL.getConfiguration();
    conf.set(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, String.valueOf(128 * 1024));
    // prevent aggressive region split
    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
      ConstantSizeRegionSplitPolicy.class.getName());
    conf.setInt("hfile.format.version", 3); // for mob tests
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    MemoryCompactionPolicy policy = getMemoryCompactionPolicy();
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TABLE_NAME)
        .setValue(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY, policy.name());
    if (policy == MemoryCompactionPolicy.EAGER) {
      builder.setValue(MemStoreLAB.USEMSLAB_KEY, "false");
      builder.setValue(CompactingMemStore.IN_MEMORY_FLUSH_THRESHOLD_FACTOR_KEY, "0.9");
    }
    Stream.of(FAMILIES).map(ColumnFamilyDescriptorBuilder::of)
        .forEachOrdered(builder::setColumnFamily);
    UTIL.getAdmin().createTable(builder.build());
    tool.setConf(UTIL.getConfiguration());
  }

  @After
  public void tearDown() throws Exception {
    UTIL.deleteTable(TABLE_NAME);
  }

  private void runTestAtomicity(long millisToRun, int numWriters, int numGetters, int numScanners,
      int numUniqueRows) throws Exception {
    runTestAtomicity(millisToRun, numWriters, numGetters, numScanners, numUniqueRows, false);
  }

  private void runTestAtomicity(long millisToRun, int numWriters, int numGetters, int numScanners,
      int numUniqueRows, boolean useMob) throws Exception {
    List<String> args = Lists.newArrayList("-millis", String.valueOf(millisToRun), "-numWriters",
      String.valueOf(numWriters), "-numGetters", String.valueOf(numGetters), "-numScanners",
      String.valueOf(numScanners), "-numUniqueRows", String.valueOf(numUniqueRows), "-crazyFlush");
    if (useMob) {
      args.add("-useMob");
    }
    tool.run(args.toArray(new String[0]));
  }

  @Test
  public void testGetAtomicity() throws Exception {
    runTestAtomicity(20000, 5, 5, 0, 3);
  }

  @Test
  public void testScanAtomicity() throws Exception {
    runTestAtomicity(20000, 5, 0, 5, 3);
  }

  @Test
  public void testMixedAtomicity() throws Exception {
    runTestAtomicity(20000, 5, 2, 2, 3);
  }

  @Test
  public void testMobGetAtomicity() throws Exception {
    runTestAtomicity(20000, 5, 5, 0, 3, true);
  }

  @Test
  public void testMobScanAtomicity() throws Exception {
    runTestAtomicity(20000, 5, 0, 5, 3, true);
  }

  @Test
  public void testMobMixedAtomicity() throws Exception {
    runTestAtomicity(20000, 5, 2, 2, 3, true);
  }
}
