/**
 *
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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({MediumTests.class})
public class TestUpdateConfiguration extends AbstractTestUpdateConfiguration {
  private static final Log LOG = LogFactory.getLog(TestUpdateConfiguration.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setup() throws Exception {
    setUpConfigurationFiles(TEST_UTIL);
    TEST_UTIL.startMiniCluster(2, 1);
    addResourceToRegionServerConfiguration(TEST_UTIL);
  }

  @Rule
  public TestName name = new TestName();

  @Test
  public void testOnlineConfigChange() throws IOException {
    LOG.debug("Starting the test " + name.getMethodName());
    Admin admin = TEST_UTIL.getHBaseAdmin();
    ServerName server = TEST_UTIL.getHBaseCluster().getRegionServer(0).getServerName();
    admin.updateConfiguration(server);
  }

  @Test
  public void testMasterOnlineConfigChange() throws IOException {
    LOG.debug("Starting the test " + name.getMethodName());
    replaceHBaseSiteXML();
    Admin admin = TEST_UTIL.getHBaseAdmin();

    ServerName server = TEST_UTIL.getHBaseCluster().getMaster().getServerName();
    admin.updateConfiguration(server);
    Configuration conf = TEST_UTIL.getMiniHBaseCluster().getMaster().getConfiguration();
    int custom = conf.getInt("hbase.custom.config", 0);
    assertEquals(1000, custom);
    restoreHBaseSiteXML();
  }

  @Test
  public void testAllOnlineConfigChange() throws IOException {
    LOG.debug("Starting the test " + name.getMethodName());
    Admin admin = TEST_UTIL.getHBaseAdmin();
    admin.updateConfiguration();
  }

  @Test
  public void testAllCustomOnlineConfigChange() throws IOException {
    LOG.debug("Starting the test " + name.getMethodName());
    replaceHBaseSiteXML();

    Admin admin = TEST_UTIL.getHBaseAdmin();
    admin.updateConfiguration();

    // Check the configuration of the Masters
    Configuration masterConfiguration = TEST_UTIL.getMiniHBaseCluster().getMaster(0).getConfiguration();
    int custom = masterConfiguration.getInt("hbase.custom.config", 0);
    assertEquals(custom, 1000);
    Configuration backupMasterConfiguration = TEST_UTIL.getMiniHBaseCluster().getMaster(1).getConfiguration();
    custom = backupMasterConfiguration.getInt("hbase.custom.config", 0);
    assertEquals(custom, 1000);

    // Check the configuration of the RegionServer
    Configuration regionServerConfiguration = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0).getConfiguration();
    custom = regionServerConfiguration.getInt("hbase.custom.config", 0);
    assertEquals(custom, 1000);

    restoreHBaseSiteXML();
  }
}
