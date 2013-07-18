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
package org.apache.hadoop.hbase.master.snapshot;

import static org.junit.Assert.assertFalse;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test that the snapshot log cleaner finds logs referenced in a snapshot
 */
@Category(SmallTests.class)
public class TestSnapshotLogCleaner {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @AfterClass
  public static void cleanup() throws IOException {
    Configuration conf = TEST_UTIL.getConfiguration();
    Path rootDir = FSUtils.getRootDir(conf);
    FileSystem fs = FileSystem.get(conf);
    // cleanup
    fs.delete(rootDir, true);
  }

  @Test
  public void testFindsSnapshotFilesWhenCleaning() throws IOException {
    Configuration conf = TEST_UTIL.getConfiguration();
    FSUtils.setRootDir(conf, TEST_UTIL.getDataTestDir());
    Path rootDir = FSUtils.getRootDir(conf);
    FileSystem fs = FileSystem.get(conf);
    SnapshotLogCleaner cleaner = new SnapshotLogCleaner();
    cleaner.setConf(conf);

    // write an hfile to the snapshot directory
    String snapshotName = "snapshot";
    byte[] snapshot = Bytes.toBytes(snapshotName);
    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
    Path snapshotLogDir = new Path(snapshotDir, HConstants.HREGION_LOGDIR_NAME);
    String timestamp = "1339643343027";
    String hostFromMaster = "localhost%2C59648%2C1339643336601";

    Path hostSnapshotLogDir = new Path(snapshotLogDir, hostFromMaster);
    String snapshotlogfile = hostFromMaster + "." + timestamp + ".hbase";

    // add the reference to log in the snapshot
    fs.create(new Path(hostSnapshotLogDir, snapshotlogfile));

    // now check to see if that log file would get deleted.
    Path oldlogDir = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    Path logFile = new Path(oldlogDir, snapshotlogfile);
    fs.create(logFile);

    // make sure that the file isn't deletable
    assertFalse(cleaner.isFileDeletable(fs.getFileStatus(logFile)));
  }
}
