package org.apache.hadoop.hbase.quotas;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Category(MediumTests.class)
public class TestSpaceQuotasWithRegionReplicas {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSpaceQuotasWithRegionReplicas.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestSpaceQuotasWithRegionReplicas.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final int NUM_RETRIES = 10;

  @Rule
  public TestName testName = new TestName();
  private SpaceQuotaHelperForTests helper;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    SpaceQuotaHelperForTests.updateConfigForQuotas(conf);
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void removeAllQuotas() throws Exception {
    helper = new SpaceQuotaHelperForTests(TEST_UTIL, testName, new AtomicLong(0));
    helper.removeAllQuotas();
  }

  @Test
  public void testSetQuotaWithRegionReplicaSingleRegion() throws Exception {
    setQuotaAndVerifyForRegionReplication(1, 2, SpaceViolationPolicy.NO_INSERTS);
    setQuotaAndVerifyForRegionReplication(1, 2, SpaceViolationPolicy.NO_WRITES);
    setQuotaAndVerifyForRegionReplication(1, 2, SpaceViolationPolicy.NO_WRITES_COMPACTIONS);
    setQuotaAndVerifyForRegionReplication(1, 2, SpaceViolationPolicy.DISABLE);
  }

  @Test
  public void testSetQuotaWithRegionReplicaMultipleRegion() throws Exception {
    setQuotaAndVerifyForRegionReplication(5, 3, SpaceViolationPolicy.NO_INSERTS);
    setQuotaAndVerifyForRegionReplication(6, 3, SpaceViolationPolicy.NO_WRITES);
    setQuotaAndVerifyForRegionReplication(6, 3, SpaceViolationPolicy.NO_WRITES_COMPACTIONS);
    setQuotaAndVerifyForRegionReplication(6, 3, SpaceViolationPolicy.DISABLE);
  }

  @Test
  public void testSetQuotaWithSingleRegionZeroRegionReplica() throws Exception {
    setQuotaAndVerifyForRegionReplication(1, 0, SpaceViolationPolicy.NO_INSERTS);
    setQuotaAndVerifyForRegionReplication(1, 0, SpaceViolationPolicy.NO_WRITES);
    setQuotaAndVerifyForRegionReplication(1, 0, SpaceViolationPolicy.NO_WRITES_COMPACTIONS);
    setQuotaAndVerifyForRegionReplication(1, 0, SpaceViolationPolicy.DISABLE);
  }

  @Test
  public void testSetQuotaWithMultipleRegionZeroRegionReplicas() throws Exception {
    setQuotaAndVerifyForRegionReplication(5, 0, SpaceViolationPolicy.NO_INSERTS);
    setQuotaAndVerifyForRegionReplication(6, 0, SpaceViolationPolicy.NO_WRITES);
    setQuotaAndVerifyForRegionReplication(6, 0, SpaceViolationPolicy.NO_WRITES_COMPACTIONS);
    setQuotaAndVerifyForRegionReplication(6, 0, SpaceViolationPolicy.DISABLE);
  }

  private void setQuotaAndVerifyForRegionReplication(int region, int replicatedRegion,
      SpaceViolationPolicy policy) throws Exception {
    TableName tn = helper.createTableWithRegions(TEST_UTIL.getAdmin(),
        NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR, region, replicatedRegion);
    helper.setQuotaLimit(tn, policy, 5L);
    helper.writeData(tn, 5L * SpaceQuotaHelperForTests.ONE_MEGABYTE);
    Put p = new Put(Bytes.toBytes("to_reject"));
    p.addColumn(Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("to"),
        Bytes.toBytes("reject"));
    // Adding a sleep for 5 sec, so all the chores run and to void flakiness of the test.
    Thread.sleep(5000);
    helper.verifyViolation(policy, tn, p);
  }
}
