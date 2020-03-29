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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;

/**
 * Base class to test Configuration Update logic. It wraps up things needed to
 * test configuration change and provides utility methods for test cluster setup,
 * updating/restoring configuration file.
 */
public abstract class AbstractTestUpdateConfiguration {
  private static final String SERVER_CONFIG = "hbase-site.xml";
  private static final String OVERRIDE_SERVER_CONFIG  = "override-hbase-site.xml";
  private static final String BACKUP_SERVER_CONFIG  = "backup-hbase-site.xml";

  private static Path configFileUnderTestDataDir;
  private static Path overrideConfigFileUnderTestDataDir;
  private static Path backupConfigFileUnderTestDataDir;

  protected static void setUpConfigurationFiles(final HBaseTestingUtility testUtil)
    throws Exception {
    // Before this change, the test will update hbase-site.xml under target/test-classes and
    // trigger a config reload. Since target/test-classes/hbase-site.xml is being used by
    // other testing cases at the same time, this update will break other testing cases so it will
    // be flakey in nature.
    // To avoid this, the change is to make target/test-classes/hbase-site.xml immutable. A new
    // hbase-site.xml will be created under its test data directory, i.e,
    // hbase-server/target/test-data/UUID, this new file will be added as a resource for the
    // config, new update will be applied to this new file and only visible to this specific test
    // case. The target/test-classes/hbase-site.xml will not be changed during the test.

    String absoluteDataPath = testUtil.getDataTestDir().toString();

    // Create test-data directories.
    Files.createDirectories(Paths.get(absoluteDataPath));

    // Copy hbase-site.xml from target/test-class to target/test-data/UUID directory.
    Path configFile = Paths.get("target", "test-classes", SERVER_CONFIG);
    configFileUnderTestDataDir = Paths.get(absoluteDataPath, SERVER_CONFIG);
    Files.copy(configFile, configFileUnderTestDataDir);

    // Copy override config file overrider-hbase-site.xml from target/test-class to
    // target/test-data/UUID directory.
    Path overrideConfigFile = Paths.get("target", "test-classes",
      OVERRIDE_SERVER_CONFIG);
    overrideConfigFileUnderTestDataDir = Paths.get(absoluteDataPath, OVERRIDE_SERVER_CONFIG);
    Files.copy(overrideConfigFile, overrideConfigFileUnderTestDataDir);

    backupConfigFileUnderTestDataDir = Paths.get(absoluteDataPath, BACKUP_SERVER_CONFIG);

    // Add the new custom config file to Configuration
    testUtil.getConfiguration().addResource(testUtil.getDataTestDir(SERVER_CONFIG));
  }

  protected static void addResourceToRegionServerConfiguration(final HBaseTestingUtility testUtil) {
    // When RegionServer is created in MiniHBaseCluster, it uses HBaseConfiguration.create(conf) of
    // the master Configuration. The create() just copies config params over, it does not do
    // a clone for a historic reason. Properties such as resources are lost during this process.
    // Exposing a new method in HBaseConfiguration causes confusion. Instead, the new hbase-site.xml
    // under test-data directory is added to RegionServer's configuration as a workaround.
    for (RegionServerThread rsThread : testUtil.getMiniHBaseCluster().getRegionServerThreads()) {
      rsThread.getRegionServer().getConfiguration().addResource(
        testUtil.getDataTestDir(SERVER_CONFIG));
    }
  }

  /**
   * Replace the hbase-site.xml file under this test's data directory with the content of the
   * override-hbase-site.xml file. Stashes the current existing file so that it can be restored
   * using {@link #restoreHBaseSiteXML()}.
   *
   * @throws IOException if an I/O error occurs
   */
  protected void replaceHBaseSiteXML() throws IOException {
    // make a backup of hbase-site.xml
    Files.copy(configFileUnderTestDataDir,
      backupConfigFileUnderTestDataDir, StandardCopyOption.REPLACE_EXISTING);
    // update hbase-site.xml by overwriting it
    Files.copy(overrideConfigFileUnderTestDataDir,
      configFileUnderTestDataDir, StandardCopyOption.REPLACE_EXISTING);
  }

  /**
   * Restores the hbase-site.xml file that was stashed by a previous call to
   * {@link #replaceHBaseSiteXML()}.
   *
   * @throws IOException if an I/O error occurs
   */
  protected void restoreHBaseSiteXML() throws IOException {
    // restore hbase-site.xml
    Files.copy(backupConfigFileUnderTestDataDir,
      configFileUnderTestDataDir, StandardCopyOption.REPLACE_EXISTING);
  }
}
