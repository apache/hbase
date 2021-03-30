package org.apache.hadoop.hbase.rsgroup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.After;
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
public class TestUpdateRSGroupConfiguration extends TestRSGroupsBase {
  protected static final Logger LOG = LoggerFactory.getLogger(TestUpdateRSGroupConfiguration.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestUpdateRSGroupConfiguration.class);
  @Rule
  public TestName name = new TestName();
  private static final String TEST_GROUP = "test";
  private static final String TEST2_GROUP = "test2";

  @BeforeClass
  public static void setUp() throws Exception {
    setUpConfigurationFiles(TEST_UTIL);
    setUpTestBeforeClass();
    addResourceToRegionServerConfiguration(TEST_UTIL);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    tearDownAfterClass();
  }

  @Before
  public void beforeMethod() throws Exception {
    setUpBeforeMethod();
  }

  @After
  public void afterMethod() throws Exception {
    tearDownAfterMethod();
  }

  @Test
  public void testOnlineConfigChangeInRSGroup() throws Exception {
    addGroup(TEST_GROUP, 1);
    rsGroupAdmin.updateConfiguration(TEST_GROUP);
  }

  @Test
  public void testNonexistentRSGroup() throws Exception {
    try {
      rsGroupAdmin.updateConfiguration(TEST2_GROUP);
      fail("Group does not exist: test2");
    } catch (IllegalArgumentException iae) {
      // expected
    }
  }

  @Test
  public void testCustomOnlineConfigChangeInRSGroup() throws Exception {
    // Check the default configuration of the RegionServers
    TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads().forEach(thread -> {
      Configuration conf = thread.getRegionServer().getConfiguration();
      assertEquals(0, conf.getInt("hbase.custom.config", 0));
    });

    replaceHBaseSiteXML();
    RSGroupInfo testRSGroup = addGroup(TEST_GROUP, 1);
    RSGroupInfo test2RSGroup = addGroup(TEST2_GROUP, 1);
    rsGroupAdmin.updateConfiguration(TEST_GROUP);

    // Check the configuration of the RegionServer in test rsgroup, should be update
    Configuration regionServerConfiguration =
      TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().stream()
        .map(JVMClusterUtil.RegionServerThread::getRegionServer)
        .filter(regionServer ->
          (regionServer.getServerName().getAddress().equals(testRSGroup.getServers().first())))
        .collect(Collectors.toList()).get(0).getConfiguration();
    int custom = regionServerConfiguration.getInt("hbase.custom.config", 0);
    assertEquals(1000, custom);

    // Check the configuration of the RegionServer in test2 rsgroup, should not be update
    regionServerConfiguration =
      TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().stream()
        .map(JVMClusterUtil.RegionServerThread::getRegionServer)
        .filter(regionServer ->
          (regionServer.getServerName().getAddress().equals(test2RSGroup.getServers().first())))
        .collect(Collectors.toList()).get(0).getConfiguration();
    custom = regionServerConfiguration.getInt("hbase.custom.config", 0);
    assertEquals(0, custom);

    restoreHBaseSiteXML();
  }
}
