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

import static org.apache.hadoop.hbase.AcidGuaranteesTestTool.FAMILY_A;
import static org.apache.hadoop.hbase.AcidGuaranteesTestTool.FAMILY_B;
import static org.apache.hadoop.hbase.AcidGuaranteesTestTool.FAMILY_C;
import static org.apache.hadoop.hbase.AcidGuaranteesTestTool.TABLE_NAME;

import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

/**
 * This Integration Test verifies acid guarantees across column families by frequently writing
 * values to rows with multiple column families and concurrently reading entire rows that expect all
 * column families.
 * <p>
 * Sample usage:
 *
 * <pre>
 * hbase org.apache.hadoop.hbase.IntegrationTestAcidGuarantees -Dmillis=10000 -DnumWriters=50
 * -DnumGetters=2 -DnumScanners=2 -DnumUniqueRows=5
 * </pre>
 */
@Category(IntegrationTests.class)
public class IntegrationTestAcidGuarantees extends IntegrationTestBase {
  private static final int SERVER_COUNT = 1; // number of slaves for the smallest cluster

  // The unit test version.
  AcidGuaranteesTestTool tool;

  @Override
  public int runTestFromCommandLine() throws Exception {
    return tool.run(new String[0]);
  }

  @Override
  public void setUpCluster() throws Exception {
    // Set small flush size for minicluster so we exercise reseeking scanners
    util = getTestingUtil(getConf());
    util.initializeCluster(SERVER_COUNT);
    conf = getConf();
    conf.set(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, String.valueOf(128 * 1024));
    // prevent aggressive region split
    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
      ConstantSizeRegionSplitPolicy.class.getName());

    tool = new AcidGuaranteesTestTool();
    tool.setConf(getConf());
  }

  @Override
  public TableName getTablename() {
    return TABLE_NAME;
  }

  @Override
  protected Set<String> getColumnFamilies() {
    return Sets.newHashSet(Bytes.toString(FAMILY_A), Bytes.toString(FAMILY_B),
      Bytes.toString(FAMILY_C));
  }

  private void runTestAtomicity(long millisToRun, int numWriters, int numGetters, int numScanners,
      int numUniqueRows) throws Exception {
    tool.run(new String[] { "-millis", String.valueOf(millisToRun), "-numWriters",
        String.valueOf(numWriters), "-numGetters", String.valueOf(numGetters), "-numScanners",
        String.valueOf(numScanners), "-numUniqueRows", String.valueOf(numUniqueRows) });
  }

  // ***** Actual integration tests
  @Test
  public void testGetAtomicity() throws Exception {
    runTestAtomicity(20000, 4, 4, 0, 3);
  }

  @Test
  public void testScanAtomicity() throws Exception {
    runTestAtomicity(20000, 3, 0, 2, 3);
  }

  @Test
  public void testMixedAtomicity() throws Exception {
    runTestAtomicity(20000, 4, 2, 2, 3);
  }

  // **** Command line hook
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int ret = ToolRunner.run(conf, new IntegrationTestAcidGuarantees(), args);
    System.exit(ret);
  }
}
