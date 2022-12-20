/*
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
package org.apache.hadoop.hbase.backup;

import java.io.IOException;
import java.security.PrivilegedAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.master.region.MasterRegionFactory;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(SmallTests.class)
public class TestBackupUtils {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBackupUtils.class);
  private static final Logger LOG = LoggerFactory.getLogger(TestBackupUtils.class);

  protected static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  protected static Configuration conf = TEST_UTIL.getConfiguration();

  @Test
  public void testGetBulkOutputDir() {
    // Create a user who is not the current user
    String fooUserName = "foo1234";
    String fooGroupName = "group1";
    UserGroupInformation ugi =
      UserGroupInformation.createUserForTesting(fooUserName, new String[] { fooGroupName });
    // Get user's home directory
    Path fooHomeDirectory = ugi.doAs(new PrivilegedAction<Path>() {
      @Override
      public Path run() {
        try (FileSystem fs = FileSystem.get(conf)) {
          return fs.getHomeDirectory();
        } catch (IOException ioe) {
          LOG.error("Failed to get foo's home directory", ioe);
        }
        return null;
      }
    });

    Path bulkOutputDir = ugi.doAs(new PrivilegedAction<Path>() {
      @Override
      public Path run() {
        try {
          return BackupUtils.getBulkOutputDir("test", conf, false);
        } catch (IOException ioe) {
          LOG.error("Failed to get bulk output dir path", ioe);
        }
        return null;
      }
    });
    // Make sure the directory is in foo1234's home directory
    Assert.assertTrue(bulkOutputDir.toString().startsWith(fooHomeDirectory.toString()));
  }

  @Test
  public void testFilesystemWalHostNameParsing() throws IOException {
    String host = "localhost";
    int port = 60030;
    ServerName serverName = ServerName.valueOf(host, port, 1234);
    Path walRootDir = CommonFSUtils.getWALRootDir(conf);
    Path oldLogDir = new Path(walRootDir, HConstants.HREGION_OLDLOGDIR_NAME);

    Path testWalPath = new Path(oldLogDir,
      serverName.toString() + BackupUtils.LOGNAME_SEPARATOR + EnvironmentEdgeManager.currentTime());
    Path testMasterWalPath =
      new Path(oldLogDir, testWalPath.getName() + MasterRegionFactory.ARCHIVED_WAL_SUFFIX);

    String parsedHost = BackupUtils.parseHostFromOldLog(testMasterWalPath);
    Assert.assertNull(parsedHost);

    parsedHost = BackupUtils.parseHostFromOldLog(testWalPath);
    Assert.assertEquals(parsedHost, host + Addressing.HOSTNAME_PORT_SEPARATOR + port);
  }
}
