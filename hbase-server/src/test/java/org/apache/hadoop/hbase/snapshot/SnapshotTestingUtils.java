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
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.IsSnapshotDoneRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.IsSnapshotDoneResponse;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.server.snapshot.TakeSnapshotUtils;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.junit.Assert;

import com.google.protobuf.ServiceException;

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
  public static void confirmSnapshotValid(SnapshotDescription snapshotDescriptor, byte[] tableName,
      byte[] testFamily, Path rootDir, HBaseAdmin admin, FileSystem fs, boolean requireLogs,
      Path logsDir, Set<String> snapshotServers) throws IOException {
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
   * @throws ServiceException if the snapshot fails
   */
  public static void waitForSnapshotToComplete(HMaster master, SnapshotDescription snapshot,
      long sleep) throws ServiceException {
    final IsSnapshotDoneRequest request = IsSnapshotDoneRequest.newBuilder().setSnapshot(snapshot)
        .build();
    IsSnapshotDoneResponse done = IsSnapshotDoneResponse.newBuilder().buildPartial();
    while (!done.getDone()) {
      done = master.isSnapshotDone(null, request);
      try {
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
        throw new ServiceException(e);
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
   * @param snapshot the {@link SnapshotDescription} request to pass to the master
   * @param clazz expected exception from the master
   */
  public static void expectSnapshotDoneException(HMaster master, IsSnapshotDoneRequest snapshot,
      Class<? extends HBaseSnapshotException> clazz) {
    try {
      master.isSnapshotDone(null, snapshot);
      Assert.fail("didn't fail to lookup a snapshot");
    } catch (ServiceException se) {
      try {
        throw ProtobufUtil.getRemoteException(se);
      } catch (HBaseSnapshotException e) {
        assertEquals("Threw wrong snapshot exception!", clazz, e.getClass());
      } catch (Throwable t) {
        Assert.fail("Threw an unexpected exception:" + t);
      }
    }
  }
}
