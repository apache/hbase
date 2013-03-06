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
package org.apache.hadoop.hbase.snapshot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.snapshot.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.HSnapshotDescription;
import org.apache.hadoop.hbase.snapshot.TakeSnapshotUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.Assert;

/**
 * Utilities class for snapshots
 */
public class SnapshotTestingUtils {

  private static final Log LOG = LogFactory.getLog(SnapshotTestingUtils.class);

  /**
   * Assert that we don't have any snapshots lists
   * @throws IOException if the admin operation fails
   */
  public static void assertNoSnapshots(HBaseAdmin admin) throws IOException {
    assertEquals("Have some previous snapshots", 0, admin.listSnapshots().size());
  }

  /**
   * Make sure that there is only one snapshot returned from the master and its name and table match
   * the passed in parameters.
   */
  public static void assertOneSnapshotThatMatches(HBaseAdmin admin, HSnapshotDescription snapshot)
      throws IOException {
    assertOneSnapshotThatMatches(admin, snapshot.getName(), snapshot.getTable());
  }

  /**
   * Make sure that there is only one snapshot returned from the master and its name and table match
   * the passed in parameters.
   */
  public static void assertOneSnapshotThatMatches(HBaseAdmin admin, SnapshotDescription snapshot)
      throws IOException {
    assertOneSnapshotThatMatches(admin, snapshot.getName(), snapshot.getTable());
  }

  /**
   * Make sure that there is only one snapshot returned from the master and its name and table match
   * the passed in parameters.
   */
  public static List<SnapshotDescription> assertOneSnapshotThatMatches(HBaseAdmin admin,
      String snapshotName, String tableName) throws IOException {
    // list the snapshot
    List<SnapshotDescription> snapshots = admin.listSnapshots();

    assertEquals("Should only have 1 snapshot", 1, snapshots.size());
    assertEquals(snapshotName, snapshots.get(0).getName());
    assertEquals(tableName, snapshots.get(0).getTable());

    return snapshots;
  }

  /**
   * Make sure that there is only one snapshot returned from the master and its name and table match
   * the passed in parameters.
   */
  public static List<SnapshotDescription> assertOneSnapshotThatMatches(HBaseAdmin admin,
      byte[] snapshot, byte[] tableName) throws IOException {
    return assertOneSnapshotThatMatches(admin, Bytes.toString(snapshot), Bytes.toString(tableName));
  }

  /**
   * Confirm that the snapshot contains references to all the files that should be in the snapshot
   */
  public static void confirmSnapshotValid(SnapshotDescription snapshotDescriptor,
      byte[] tableName, byte[] testFamily, Path rootDir, HBaseAdmin admin, FileSystem fs,
      boolean requireLogs, Path logsDir, Set<String> snapshotServers) throws IOException {
    Path snapshotDir = SnapshotDescriptionUtils
        .getCompletedSnapshotDir(snapshotDescriptor, rootDir);
    assertTrue(fs.exists(snapshotDir));
    Path snapshotinfo = new Path(snapshotDir, SnapshotDescriptionUtils.SNAPSHOTINFO_FILE);
    assertTrue(fs.exists(snapshotinfo));
    // check the logs dir
    if (requireLogs) {
      TakeSnapshotUtils.verifyAllLogsGotReferenced(fs, logsDir, snapshotServers,
        snapshotDescriptor, new Path(snapshotDir, HConstants.HREGION_LOGDIR_NAME));
    }
    // check the table info
    HTableDescriptor desc = FSTableDescriptors.getTableDescriptor(fs, rootDir, tableName);
    HTableDescriptor snapshotDesc = FSTableDescriptors.getTableDescriptor(fs, snapshotDir);
    assertEquals(desc, snapshotDesc);

    // check the region snapshot for all the regions
    List<HRegionInfo> regions = admin.getTableRegions(tableName);
    for (HRegionInfo info : regions) {
      String regionName = info.getEncodedName();
      Path regionDir = new Path(snapshotDir, regionName);
      HRegionInfo snapshotRegionInfo = HRegion.loadDotRegionInfoFileContent(fs, regionDir);
      assertEquals(info, snapshotRegionInfo);
      // check to make sure we have the family
      Path familyDir = new Path(regionDir, Bytes.toString(testFamily));
      assertTrue("Expected to find: " + familyDir + ", but it doesn't exist", fs.exists(familyDir));
      // make sure we have some files references
      assertTrue(fs.listStatus(familyDir).length > 0);
    }
  }

  /**
   * Helper method for testing async snapshot operations. Just waits for the given snapshot to
   * complete on the server by repeatedly checking the master.
   * @param master running the snapshot
   * @param snapshot to check
   * @param sleep amount to sleep between checks to see if the snapshot is done
   * @throws IOException if the snapshot fails
   */
  public static void waitForSnapshotToComplete(HMaster master, HSnapshotDescription snapshot,
      long sleep) throws IOException {
    boolean done = false;
    while (!done) {
      done = master.isSnapshotDone(snapshot);
      try {
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
  }

  public static void cleanupSnapshot(HBaseAdmin admin, byte[] tableName) throws IOException {
    SnapshotTestingUtils.cleanupSnapshot(admin, Bytes.toString(tableName));
  }

  public static void cleanupSnapshot(HBaseAdmin admin, String snapshotName) throws IOException {
    // delete the taken snapshot
    admin.deleteSnapshot(snapshotName);
    assertNoSnapshots(admin);
  }

  /**
   * Expect the snapshot to throw an error when checking if the snapshot is complete
   * @param master master to check
   * @param snapshot the {@link HSnapshotDescription} request to pass to the master
   * @param clazz expected exception from the master
   */
  public static void expectSnapshotDoneException(HMaster master, HSnapshotDescription snapshot,
      Class<? extends HBaseSnapshotException> clazz) {
    try {
      boolean res = master.isSnapshotDone(snapshot);
      Assert.fail("didn't fail to lookup a snapshot: res=" + res);
    } catch (HBaseSnapshotException e) {
      assertEquals("Threw wrong snapshot exception!", clazz, e.getClass());
    } catch (Throwable t) {
      Assert.fail("Threw an unexpected exception:" + t);
    }
  }

  /**
   * List all the HFiles in the given table
   * @param fs FileSystem where the table lives
   * @param tableDir directory of the table
   * @return array of the current HFiles in the table (could be a zero-length array)
   * @throws IOException on unexecpted error reading the FS
   */
  public static FileStatus[] listHFiles(final FileSystem fs, Path tableDir) throws IOException {
    // setup the filters we will need based on the filesystem
    PathFilter regionFilter = new FSUtils.RegionDirFilter(fs);
    PathFilter familyFilter = new FSUtils.FamilyDirFilter(fs);
    final PathFilter fileFilter = new PathFilter() {
      @Override
      public boolean accept(Path file) {
        try {
          return fs.isFile(file);
        } catch (IOException e) {
          return false;
        }
      }
    };

    FileStatus[] regionDirs = FSUtils.listStatus(fs, tableDir, regionFilter);
    // if no regions, then we are done
    if (regionDirs == null || regionDirs.length == 0) return new FileStatus[0];

    // go through each of the regions, and add al the hfiles under each family
    List<FileStatus> regionFiles = new ArrayList<FileStatus>(regionDirs.length);
    for (FileStatus regionDir : regionDirs) {
      FileStatus[] fams = FSUtils.listStatus(fs, regionDir.getPath(), familyFilter);
      // if no families, then we are done again
      if (fams == null || fams.length == 0) continue;
      // add all the hfiles under the family
      regionFiles.addAll(SnapshotTestingUtils.getHFilesInRegion(fams, fs, fileFilter));
    }
    FileStatus[] files = new FileStatus[regionFiles.size()];
    regionFiles.toArray(files);
    return files;
  }

  /**
   * Get all the hfiles in the region, under the passed set of families
   * @param families all the family directories under the region
   * @param fs filesystem where the families live
   * @param fileFilter filter to only include files
   * @return collection of all the hfiles under all the passed in families (non-null)
   * @throws IOException on unexecpted error reading the FS
   */
  public static Collection<FileStatus> getHFilesInRegion(FileStatus[] families, FileSystem fs,
      PathFilter fileFilter) throws IOException {
    Set<FileStatus> files = new TreeSet<FileStatus>();
    for (FileStatus family : families) {
      // get all the hfiles in the family
      FileStatus[] hfiles = FSUtils.listStatus(fs, family.getPath(), fileFilter);
      // if no hfiles, then we are done with this family
      if (hfiles == null || hfiles.length == 0) continue;
      files.addAll(Arrays.asList(hfiles));
    }
    return files;
  }
}
