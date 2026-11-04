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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.ReaderContext;
import org.apache.hadoop.hbase.io.hfile.ReaderContext.ReaderType;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerForTest;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Test HStoreFile
 */
@Tag(RegionServerTests.TAG)
@Tag(SmallTests.TAG)
public class TestStoreFileInfo {

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private static final String PLAIN_HFILE_NAME = "abc0123456789abcdef0123456789ab";

  private static FileSystem fileSystemCountingGetFileStatus(FileSystem base,
    AtomicInteger statCalls) {
    return new FilterFileSystem(base) {
      @Override
      public FileStatus getFileStatus(Path path) throws IOException {
        statCalls.incrementAndGet();
        return super.getFileStatus(path);
      }
    };
  }

  private static Path createPlainHFileOnTestFs(Path dir) throws IOException {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    fs.mkdirs(dir);
    Path file = new Path(dir, PLAIN_HFILE_NAME);
    try (FSDataOutputStream out = fs.create(file)) {
      out.write(new byte[128]);
    }
    return file;
  }

  private static StoreContext newStoreContextForTest(FileSystem fs, Path regionDir)
    throws IOException {
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(TableName.valueOf("table1")).build();
    return StoreContext.getBuilder()
      .withRegionFileSystem(HRegionFileSystem.create(TEST_UTIL.getConfiguration(), fs, regionDir,
        regionInfo))
      .withColumnFamilyDescriptor(
        ColumnFamilyDescriptorBuilder.newBuilder("cf1".getBytes()).build())
      .build();
  }

  /**
   * Validate that we can handle valid tables with '.', '_', and '-' chars.
   */
  @Test
  public void testStoreFileNames() {
    String[] legalHFileLink = { "MyTable_02=abc012-def345", "MyTable_02.300=abc012-def345",
      "MyTable_02-400=abc012-def345", "MyTable_02-400.200=abc012-def345",
      "MyTable_02=abc012-def345_SeqId_1_", "MyTable_02=abc012-def345_SeqId_20_" };
    for (String name : legalHFileLink) {
      assertTrue(HFileLink.isHFileLink(name), "should be a valid link: " + name);
      assertTrue(StoreFileInfo.validateStoreFileName(name), "should be a valid StoreFile" + name);
      assertFalse(StoreFileInfo.isReference(name), "should not be a valid reference: " + name);

      String refName = name + ".6789";
      assertTrue(StoreFileInfo.isReference(refName),
        "should be a valid link reference: " + refName);
      assertTrue(StoreFileInfo.validateStoreFileName(refName),
        "should be a valid StoreFile" + refName);
    }

    String[] illegalHFileLink = { ".MyTable_02=abc012-def345", "-MyTable_02.300=abc012-def345",
      "MyTable_02-400=abc0_12-def345", "MyTable_02-400.200=abc012-def345...." };
    for (String name : illegalHFileLink) {
      assertFalse(HFileLink.isHFileLink(name), "should not be a valid link: " + name);
    }
  }

  @Test
  public void testEqualsWithLink() throws IOException {
    Path origin = new Path("/origin");
    Path tmp = TEST_UTIL.getDataTestDir();
    Path mob = new Path("/mob");
    Path archive = new Path("/archive");
    HFileLink link1 = new HFileLink(new Path(origin, "f1"), new Path(tmp, "f1"),
      new Path(mob, "f1"), new Path(archive, "f1"));
    HFileLink link2 = new HFileLink(new Path(origin, "f1"), new Path(tmp, "f1"),
      new Path(mob, "f1"), new Path(archive, "f1"));

    StoreFileInfo info1 = new StoreFileInfo(TEST_UTIL.getConfiguration(),
      TEST_UTIL.getTestFileSystem(), new Path(archive, "f1"), link1);
    StoreFileInfo info2 = new StoreFileInfo(TEST_UTIL.getConfiguration(),
      TEST_UTIL.getTestFileSystem(), new Path(archive, "f1"), link2);

    assertEquals(info1, info2);
    assertEquals(info1.hashCode(), info2.hashCode());
  }

  /**
   * Plain HFiles should not call {@code getFileStatus} again when opening a reader context if size
   * and status were established at {@link StoreFileInfo} construction.
   */
  @Test
  public void testCreateReaderContextSkipsRestatForPlainHFile() throws IOException {
    Path dir = TEST_UTIL.getDataTestDirOnTestFS("plain-hfile-no-restat");
    Path file = createPlainHFileOnTestFs(dir);
    FileSystem baseFs = TEST_UTIL.getTestFileSystem();
    FileStatus listedStatus = baseFs.getFileStatus(file);
    AtomicInteger statCalls = new AtomicInteger();
    FileSystem fs = fileSystemCountingGetFileStatus(baseFs, statCalls);
    StoreFileInfo info = new StoreFileInfo(TEST_UTIL.getConfiguration(), fs,
      listedStatus.getModificationTime(), file, listedStatus.getLen(), null, null, false);
    assertEquals(0, statCalls.get(), "construction should not stat when size is supplied");
    statCalls.set(0);
    ReaderContext context =
      info.createReaderContext(false, 1000, ReaderType.PREAD, null, null, null);
    assertEquals(0, statCalls.get(),
      "createReaderContext should reuse cached status for plain HFiles");
    assertEquals(listedStatus.getLen(), context.getFileSize());
    context.getInputStreamWrapper().close();
  }

  @Test
  public void testGetReferencedFileStatusSkipsRestatForPlainHFile() throws IOException {
    Path regionDir = TEST_UTIL.getDataTestDirOnTestFS("plain-hfile-referenced-status");
    Path file = createPlainHFileOnTestFs(regionDir);
    FileSystem baseFs = TEST_UTIL.getTestFileSystem();
    FileStatus listedStatus = baseFs.getFileStatus(file);
    AtomicInteger statCalls = new AtomicInteger();
    FileSystem fs = fileSystemCountingGetFileStatus(baseFs, statCalls);
    StoreFileInfo info = new StoreFileInfo(TEST_UTIL.getConfiguration(), fs,
      listedStatus.getModificationTime(), file, listedStatus.getLen(), null, null, false,
      listedStatus);
    statCalls.set(0);
    assertEquals(listedStatus.getLen(), info.getReferencedFileStatus(fs).getLen());
    assertEquals(0, statCalls.get(),
      "getReferencedFileStatus should reuse cached status that does not need refresh");
    statCalls.set(0);
    assertEquals(listedStatus.getLen(), info.getFileStatus().getLen());
    assertEquals(0, statCalls.get());
  }

  @Test
  public void testPlainHFileStatusNeedsRefreshForBlockDistribution() throws IOException {
    Path dir = TEST_UTIL.getDataTestDirOnTestFS("plain-hfile-needs-refresh");
    Path file = createPlainHFileOnTestFs(dir);
    FileSystem baseFs = TEST_UTIL.getTestFileSystem();
    FileStatus listedStatus = baseFs.getFileStatus(file);
    AtomicInteger statCalls = new AtomicInteger();
    FileSystem fs = fileSystemCountingGetFileStatus(baseFs, statCalls);
    StoreFileInfo info = new StoreFileInfo(TEST_UTIL.getConfiguration(), fs,
      listedStatus.getModificationTime(), file, listedStatus.getLen(), null, null, false);
    statCalls.set(0);
    info.getReferencedFileStatus(fs);
    assertEquals(1, statCalls.get(),
      "inferred status must be refreshed for block locality");
    statCalls.set(0);
    info.getReferencedFileStatus(fs);
    assertEquals(0, statCalls.get(), "second getReferencedFileStatus should use refreshed status");
    statCalls.set(0);
    info.computeHDFSBlocksDistribution(fs);
    assertEquals(0, statCalls.get(), "block distribution should use refreshed status");
    statCalls.set(0);
    ReaderContext context =
      info.createReaderContext(false, 1000, ReaderType.PREAD, null, null, null);
    assertEquals(0, statCalls.get(), "reader open should still avoid restat");
    context.getInputStreamWrapper().close();
  }

  @Test
  public void testTrackerGetStoreFileInfoWithStatusSkipsRestatOnOpen() throws IOException {
    Path regionDir = TEST_UTIL.getDataTestDirOnTestFS("tracker-with-status-no-restat");
    Path file = createPlainHFileOnTestFs(regionDir);
    FileSystem baseFs = TEST_UTIL.getTestFileSystem();
    FileStatus listedStatus = baseFs.getFileStatus(file);
    AtomicInteger statCalls = new AtomicInteger();
    FileSystem fs = fileSystemCountingGetFileStatus(baseFs, statCalls);
    StoreFileTrackerForTest tracker =
      new StoreFileTrackerForTest(TEST_UTIL.getConfiguration(), true,
        newStoreContextForTest(fs, regionDir));
    statCalls.set(0);
    StoreFileInfo info = tracker.getStoreFileInfo(listedStatus, file, true);
    assertEquals(0, statCalls.get(), "tracker should not stat when fileStatus is supplied");
    statCalls.set(0);
    info.getReferencedFileStatus(fs);
    ReaderContext context =
      info.createReaderContext(false, 1000, ReaderType.PREAD, null, null, null);
    assertEquals(0, statCalls.get(),
      "open and referenced status should reuse listed fileStatus");
    assertEquals(listedStatus.getLen(), context.getFileSize());
    context.getInputStreamWrapper().close();
  }

  @Test
  public void testTrackerGetStoreFileInfoWithoutStatusSkipsRestatOnOpen() throws IOException {
    Path regionDir = TEST_UTIL.getDataTestDirOnTestFS("tracker-no-status-no-restat");
    Path file = createPlainHFileOnTestFs(regionDir);
    FileSystem baseFs = TEST_UTIL.getTestFileSystem();
    AtomicInteger statCalls = new AtomicInteger();
    FileSystem fs = fileSystemCountingGetFileStatus(baseFs, statCalls);
    StoreFileTrackerForTest tracker =
      new StoreFileTrackerForTest(TEST_UTIL.getConfiguration(), true,
        newStoreContextForTest(fs, regionDir));
    statCalls.set(0);
    StoreFileInfo info = tracker.getStoreFileInfo(file, true);
    assertEquals(1, statCalls.get(), "tracker should stat once when fileStatus is absent");
    statCalls.set(0);
    ReaderContext context =
      info.createReaderContext(false, 1000, ReaderType.PREAD, null, null, null);
    assertEquals(0, statCalls.get(),
      "createReaderContext should not restat after tracker registration");
    assertEquals(128, context.getFileSize());
    context.getInputStreamWrapper().close();
    statCalls.set(0);
    info.getReferencedFileStatus(fs);
    assertEquals(1, statCalls.get(),
      "getReferencedFileStatus should refresh inferred status once");
    statCalls.set(0);
    info.getReferencedFileStatus(fs);
    assertEquals(0, statCalls.get());
  }

  @Test
  public void testOpenErrorMessageReference() throws IOException {
    // Test file link exception
    // Try to open nonsense hfilelink. Make sure exception is from HFileLink.
    Path p = new Path(TEST_UTIL.getDataTestDirOnTestFS(), "4567.abcd");
    FileSystem fs = FileSystem.get(TEST_UTIL.getConfiguration());
    fs.mkdirs(p.getParent());
    Reference r = Reference.createBottomReference(HConstants.EMPTY_START_ROW);
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(TableName.valueOf("table1")).build();
    StoreContext storeContext = StoreContext.getBuilder()
      .withRegionFileSystem(HRegionFileSystem.create(TEST_UTIL.getConfiguration(), fs,
        TEST_UTIL.getDataTestDirOnTestFS(), regionInfo))
      .withColumnFamilyDescriptor(
        ColumnFamilyDescriptorBuilder.newBuilder("cf1".getBytes()).build())
      .build();
    StoreFileTrackerForTest storeFileTrackerForTest =
      new StoreFileTrackerForTest(TEST_UTIL.getConfiguration(), true, storeContext);
    storeFileTrackerForTest.createReference(r, p);
    StoreFileInfo sfi = storeFileTrackerForTest.getStoreFileInfo(p, true);
    try {
      ReaderContext context =
        sfi.createReaderContext(false, 1000, ReaderType.PREAD, null, null, null);
      sfi.createReader(context, null);
      throw new IllegalStateException();
    } catch (FileNotFoundException fnfe) {
      assertTrue(fnfe.getMessage().contains("->"));
    }
  }
}
