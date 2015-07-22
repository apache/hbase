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

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Set;

/**
 * This Integration Test verifies acid guarantees across column families by frequently writing
 * values to rows with multiple column families and concurrently reading entire rows that expect all
 * column families.
 *
 * <p>
 * Sample usage:
 * <pre>
 * hbase org.apache.hadoop.hbase.IntegrationTestAcidGuarantees -Dmillis=10000 -DnumWriters=50
 * -DnumGetters=2 -DnumScanners=2 -DnumUniqueRows=5
 * </pre>
 */
@Category(IntegrationTests.class)
public class IntegrationTestAcidGuarantees extends IntegrationTestBase {
  private static final int SERVER_COUNT = 1; // number of slaves for the smallest cluster

  // The unit test version.
  TestAcidGuarantees tag;

  @Override
  public int runTestFromCommandLine() throws Exception {
    Configuration c = getConf();
    int millis = c.getInt("millis", 5000);
    int numWriters = c.getInt("numWriters", 50);
    int numGetters = c.getInt("numGetters", 2);
    int numScanners = c.getInt("numScanners", 2);
    int numUniqueRows = c.getInt("numUniqueRows", 3);
    boolean useMob = c.getBoolean("useMob",false);
    tag.runTestAtomicity(millis, numWriters, numGetters, numScanners, numUniqueRows, true, useMob);
    return 0;
  }

  @Override
  public void setUpCluster() throws Exception {
    // Set small flush size for minicluster so we exercise reseeking scanners
    util = getTestingUtil(getConf());
    util.initializeCluster(SERVER_COUNT);
    conf = getConf();
    conf.set(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, String.valueOf(128*1024));
    // prevent aggressive region split
    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
            ConstantSizeRegionSplitPolicy.class.getName());
    this.setConf(util.getConfiguration());

    // replace the HBaseTestingUtility in the unit test with the integration test's
    // IntegrationTestingUtility
    tag = new TestAcidGuarantees();
    tag.setHBaseTestingUtil(util);
  }

  @Override
  public TableName getTablename() {
    return TestAcidGuarantees.TABLE_NAME;
  }

  @Override
  protected Set<String> getColumnFamilies() {
    return Sets.newHashSet(Bytes.toString(TestAcidGuarantees.FAMILY_A),
            Bytes.toString(TestAcidGuarantees.FAMILY_B),
            Bytes.toString(TestAcidGuarantees.FAMILY_C));
  }

  // ***** Actual integration tests

  @Test
  public void testGetAtomicity() throws Exception {
    tag.runTestAtomicity(20000, 5, 5, 0, 3);
  }

  @Test
  public void testScanAtomicity() throws Exception {
    tag.runTestAtomicity(20000, 5, 0, 5, 3);
  }

  @Test
  public void testMixedAtomicity() throws Exception {
    tag.runTestAtomicity(20000, 5, 2, 2, 3);
  }


  // **** Command line hook

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int ret = ToolRunner.run(conf, new IntegrationTestAcidGuarantees(), args);
    System.exit(ret);
  }
}


