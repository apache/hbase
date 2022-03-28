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
package org.apache.hadoop.hbase.master.region;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.HRegion.FlushResult;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestMasterRegionInitialize extends MasterRegionTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMasterRegionInitialize.class);

  @Test
  public void testUpgrade() throws IOException {
    Path rootDir = new Path(htu.getDataTestDir(), REGION_DIR_NAME);
    Path tableDir =
      CommonFSUtils.getTableDir(rootDir, region.region.getTableDescriptor().getTableName());
    Path initializingFlag = new Path(tableDir, MasterRegion.INITIALIZING_FLAG);
    Path initializedFlag = new Path(tableDir, MasterRegion.INITIALIZED_FLAG);
    HRegionFileSystem hfs = region.region.getRegionFileSystem();
    assertFalse(hfs.getFileSystem().exists(initializingFlag));
    assertTrue(hfs.getFileSystem().exists(initializedFlag));
    byte[] row = Bytes.toBytes("row");
    byte[] cf = CF1;
    byte[] cq = Bytes.toBytes("qual");
    byte[] value = Bytes.toBytes("value");
    region.update(r -> r.put(new Put(row).addColumn(cf, cq, value)));
    assertEquals(FlushResult.Result.FLUSHED_NO_COMPACTION_NEEDED, region.flush(true).getResult());
    // delete initialized flag to simulate old implementation
    hfs.getFileSystem().delete(initializedFlag, true);
    FSTableDescriptors.deleteTableDescriptors(hfs.getFileSystem(), tableDir);
    assertNull(FSTableDescriptors.getTableDescriptorFromFs(hfs.getFileSystem(), tableDir));
    // reopen, with new file tracker
    region.close(false);
    htu.getConfiguration().set(StoreFileTrackerFactory.TRACKER_IMPL,
      StoreFileTrackerFactory.Trackers.FILE.name());
    createMasterRegion();

    // make sure we successfully upgrade to new implementation without data loss
    hfs = region.region.getRegionFileSystem();
    assertFalse(hfs.getFileSystem().exists(initializingFlag));
    assertTrue(hfs.getFileSystem().exists(initializedFlag));
    TableDescriptor td = FSTableDescriptors.getTableDescriptorFromFs(hfs.getFileSystem(), tableDir);
    assertEquals(StoreFileTrackerFactory.Trackers.FILE.name(),
      td.getValue(StoreFileTrackerFactory.TRACKER_IMPL));
    assertArrayEquals(value, region.get(new Get(row)).getValue(cf, cq));
  }

  @Test
  public void testInitializingCleanup() throws IOException {
    Path rootDir = new Path(htu.getDataTestDir(), REGION_DIR_NAME);
    Path tableDir =
      CommonFSUtils.getTableDir(rootDir, region.region.getTableDescriptor().getTableName());
    Path initializingFlag = new Path(tableDir, MasterRegion.INITIALIZING_FLAG);
    Path initializedFlag = new Path(tableDir, MasterRegion.INITIALIZED_FLAG);
    HRegionFileSystem hfs = region.region.getRegionFileSystem();
    assertFalse(hfs.getFileSystem().exists(initializingFlag));
    assertTrue(hfs.getFileSystem().exists(initializedFlag));
    byte[] row = Bytes.toBytes("row");
    byte[] cf = CF1;
    byte[] cq = Bytes.toBytes("qual");
    byte[] value = Bytes.toBytes("value");
    region.update(r -> r.put(new Put(row).addColumn(cf, cq, value)));
    // delete initialized flag and touch a initializing flag, to simulate initializing in progress
    hfs.getFileSystem().delete(initializedFlag, true);
    if (!hfs.getFileSystem().mkdirs(initializingFlag)) {
      throw new IOException("can not touch " + initializedFlag);
    }

    region.close(false);
    createMasterRegion();
    hfs = region.region.getRegionFileSystem();
    assertFalse(hfs.getFileSystem().exists(initializingFlag));
    assertTrue(hfs.getFileSystem().exists(initializedFlag));

    // but the data should have been cleaned up
    assertTrue(region.get(new Get(row)).isEmpty());
  }
}
