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
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MediumTests.class})
public class TestUpdateConfiguration {
  private static final Log LOG = LogFactory.getLog(TestUpdateConfiguration.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  
  @BeforeClass
  public static void setup() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  @Test
  public void testOnlineConfigChange() throws IOException {
    LOG.debug("Starting the test");
    Admin admin = TEST_UTIL.getHBaseAdmin();
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

    Admin admin = TEST_UTIL.getHBaseAdmin();
    ServerName server = TEST_UTIL.getHBaseCluster().getMaster().getServerName();
    admin.updateConfiguration(server);
    Configuration conf = TEST_UTIL.getMiniHBaseCluster().getMaster().getConfiguration();
    int custom = conf.getInt("hbase.custom.config", 0);
    assertEquals(custom, 1000);
    // restore hbase-site.xml
    Files.copy(cnf3Path, cnfPath, StandardCopyOption.REPLACE_EXISTING);
  }
}
