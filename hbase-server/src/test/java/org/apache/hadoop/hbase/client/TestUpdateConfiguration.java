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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MediumTests.class})
public class TestUpdateConfiguration {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestUpdateConfiguration.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestUpdateConfiguration.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setup() throws Exception {
    // Set master number and use default values for other options.
    StartMiniClusterOption option = StartMiniClusterOption.builder().numMasters(2).build();
    TEST_UTIL.startMiniCluster(option);
  }

  @Test
  public void testOnlineConfigChange() throws IOException {
    LOG.debug("Starting the test");
    Admin admin = TEST_UTIL.getAdmin();
    ServerName server = TEST_UTIL.getHBaseCluster().getRegionServer(0).getServerName();
    admin.updateConfiguration(server);
  }

  @Test
  public void testMasterOnlineConfigChange() throws IOException {
    LOG.debug("Starting the test");
    Path cnfPath = FileSystems.getDefault().getPath("target/test-classes/hbase-site.xml");
    Path cnf2Path = FileSystems.getDefault().getPath("target/test-classes/hbase-site2.xml");
    Path cnf3Path = FileSystems.getDefault().getPath("target/test-classes/hbase-site3.xml");
    // make a backup of hbase-site.xml
    Files.copy(cnfPath, cnf3Path, StandardCopyOption.REPLACE_EXISTING);
    // update hbase-site.xml by overwriting it
    Files.copy(cnf2Path, cnfPath, StandardCopyOption.REPLACE_EXISTING);

    Admin admin = TEST_UTIL.getAdmin();
    ServerName server = TEST_UTIL.getHBaseCluster().getMaster().getServerName();
    admin.updateConfiguration(server);
    Configuration conf = TEST_UTIL.getMiniHBaseCluster().getMaster().getConfiguration();
    int custom = conf.getInt("hbase.custom.config", 0);
    assertEquals(1000, custom);
    // restore hbase-site.xml
    Files.copy(cnf3Path, cnfPath, StandardCopyOption.REPLACE_EXISTING);
  }

  @Test
  public void testAllOnlineConfigChange() throws IOException {
    LOG.debug("Starting the test");
    Admin admin = TEST_UTIL.getAdmin();
    admin.updateConfiguration();
  }

  @Test
  public void testAllCustomOnlineConfigChange() throws IOException {
    LOG.debug("Starting the test");
    Path cnfPath = FileSystems.getDefault().getPath("target/test-classes/hbase-site.xml");
    Path cnf2Path = FileSystems.getDefault().getPath("target/test-classes/hbase-site2.xml");
    Path cnf3Path = FileSystems.getDefault().getPath("target/test-classes/hbase-site3.xml");
    // make a backup of hbase-site.xml
    Files.copy(cnfPath, cnf3Path, StandardCopyOption.REPLACE_EXISTING);
    // update hbase-site.xml by overwriting it
    Files.copy(cnf2Path, cnfPath, StandardCopyOption.REPLACE_EXISTING);

    Admin admin = TEST_UTIL.getAdmin();
    admin.updateConfiguration();

    // Check the configuration of the Masters
    Configuration masterConfiguration =
        TEST_UTIL.getMiniHBaseCluster().getMaster(0).getConfiguration();
    int custom = masterConfiguration.getInt("hbase.custom.config", 0);
    assertEquals(1000, custom);
    Configuration backupMasterConfiguration =
        TEST_UTIL.getMiniHBaseCluster().getMaster(1).getConfiguration();
    custom = backupMasterConfiguration.getInt("hbase.custom.config", 0);
    assertEquals(1000, custom);

    // Check the configuration of the RegionServer
    Configuration regionServerConfiguration =
        TEST_UTIL.getMiniHBaseCluster().getRegionServer(0).getConfiguration();
    custom = regionServerConfiguration.getInt("hbase.custom.config", 0);
    assertEquals(1000, custom);

    // restore hbase-site.xml
    Files.copy(cnf3Path, cnfPath, StandardCopyOption.REPLACE_EXISTING);
  }
}
