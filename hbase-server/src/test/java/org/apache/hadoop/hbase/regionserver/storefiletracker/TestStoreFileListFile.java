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
package org.apache.hadoop.hbase.regionserver.storefiletracker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtil;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.io.ByteStreams;

import org.apache.hadoop.hbase.shaded.protobuf.generated.StoreFileTrackerProtos.StoreFileEntry;
import org.apache.hadoop.hbase.shaded.protobuf.generated.StoreFileTrackerProtos.StoreFileList;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestStoreFileListFile {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestStoreFileListFile.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestStoreFileListFile.class);

  private static final HBaseCommonTestingUtil UTIL = new HBaseCommonTestingUtil();

  private Path testDir;

  private StoreFileListFile storeFileListFile;

  @Rule
  public TestName name = new TestName();

  private StoreFileListFile create() throws IOException {
    HRegionFileSystem hfs = mock(HRegionFileSystem.class);
    when(hfs.getFileSystem()).thenReturn(FileSystem.get(UTIL.getConfiguration()));
    StoreContext ctx = StoreContext.getBuilder().withFamilyStoreDirectoryPath(testDir)
      .withRegionFileSystem(hfs).build();
    return new StoreFileListFile(ctx);
  }

  @Before
  public void setUp() throws IOException {
    testDir = UTIL.getDataTestDir(name.getMethodName());
    storeFileListFile = create();
  }

  @AfterClass
  public static void tearDown() {
    UTIL.cleanupTestDir();
  }

  @Test
  public void testEmptyLoad() throws IOException {
    assertNull(storeFileListFile.load(false));
  }

  private FileStatus getOnlyTrackerFile(FileSystem fs) throws IOException {
    return fs.listStatus(new Path(testDir, StoreFileListFile.TRACK_FILE_DIR))[0];
  }

  private byte[] readAll(FileSystem fs, Path file) throws IOException {
    try (FSDataInputStream in = fs.open(file)) {
      return ByteStreams.toByteArray(in);
    }
  }

  private void write(FileSystem fs, Path file, byte[] buf, int off, int len) throws IOException {
    try (FSDataOutputStream out = fs.create(file, true)) {
      out.write(buf, off, len);
    }
  }

  @Test
  public void testLoadPartial() throws IOException {
    StoreFileList.Builder builder = StoreFileList.newBuilder();
    storeFileListFile.update(builder);
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    FileStatus trackerFileStatus = getOnlyTrackerFile(fs);
    // truncate it so we do not have enough data
    LOG.info("Truncate file {} with size {} to {}", trackerFileStatus.getPath(),
      trackerFileStatus.getLen(), trackerFileStatus.getLen() / 2);
    byte[] content = readAll(fs, trackerFileStatus.getPath());
    write(fs, trackerFileStatus.getPath(), content, 0, content.length / 2);
    assertNull(storeFileListFile.load(false));
  }

  private void writeInt(byte[] buf, int off, int value) {
    byte[] b = Bytes.toBytes(value);
    for (int i = 0; i < 4; i++) {
      buf[off + i] = b[i];
    }
  }

  @Test
  public void testZeroFileLength() throws IOException {
    StoreFileList.Builder builder = StoreFileList.newBuilder();
    storeFileListFile.update(builder);
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    FileStatus trackerFileStatus = getOnlyTrackerFile(fs);
    // write a zero length
    byte[] content = readAll(fs, trackerFileStatus.getPath());
    writeInt(content, 0, 0);
    write(fs, trackerFileStatus.getPath(), content, 0, content.length);
    assertThrows(IOException.class, () -> storeFileListFile.load(false));
  }

  @Test
  public void testBigFileLength() throws IOException {
    StoreFileList.Builder builder = StoreFileList.newBuilder();
    storeFileListFile.update(builder);
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    FileStatus trackerFileStatus = getOnlyTrackerFile(fs);
    // write a large length
    byte[] content = readAll(fs, trackerFileStatus.getPath());
    writeInt(content, 0, 128 * 1024 * 1024);
    write(fs, trackerFileStatus.getPath(), content, 0, content.length);
    assertThrows(IOException.class, () -> storeFileListFile.load(false));
  }

  @Test
  public void testChecksumMismatch() throws IOException {
    StoreFileList.Builder builder = StoreFileList.newBuilder();
    storeFileListFile.update(builder);
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    FileStatus trackerFileStatus = getOnlyTrackerFile(fs);
    // flip one byte
    byte[] content = readAll(fs, trackerFileStatus.getPath());
    content[5] = (byte) ~content[5];
    write(fs, trackerFileStatus.getPath(), content, 0, content.length);
    assertThrows(IOException.class, () -> storeFileListFile.load(false));
  }

  @Test
  public void testLoadNewerTrackFiles() throws IOException, InterruptedException {
    StoreFileList.Builder builder = StoreFileList.newBuilder();
    storeFileListFile.update(builder);

    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    FileStatus trackFileStatus = getOnlyTrackerFile(fs);

    builder.addStoreFile(StoreFileEntry.newBuilder().setName("hehe").setSize(10).build());
    storeFileListFile = create();
    storeFileListFile.update(builder);

    // should load the list we stored the second time
    storeFileListFile = create();
    StoreFileList list = storeFileListFile.load(true);
    assertEquals(1, list.getStoreFileCount());
    // since read only is true, we should not delete the old track file
    // the deletion is in background, so we will test it multiple times through HTU.waitFor and make
    // sure that it is still there after timeout, i.e, the waitFor method returns -1
    assertTrue(UTIL.waitFor(2000, 100, false, () -> !fs.exists(testDir)) < 0);

    // this time read only is false, we should delete the old track file
    list = storeFileListFile.load(false);
    assertEquals(1, list.getStoreFileCount());
    UTIL.waitFor(5000, () -> !fs.exists(trackFileStatus.getPath()));
  }

  // This is to simulate the scenario where a 'dead' RS perform flush or compaction on a region
  // which has already been reassigned to another RS. This is possible in real world, usually caused
  // by a long STW GC.
  @Test
  public void testConcurrentUpdate() throws IOException {
    storeFileListFile.update(StoreFileList.newBuilder());

    StoreFileListFile storeFileListFile2 = create();
    storeFileListFile2.update(StoreFileList.newBuilder()
      .addStoreFile(StoreFileEntry.newBuilder().setName("hehe").setSize(10).build()));

    // let's update storeFileListFile several times
    for (int i = 0; i < 10; i++) {
      storeFileListFile.update(StoreFileList.newBuilder()
        .addStoreFile(StoreFileEntry.newBuilder().setName("haha-" + i).setSize(100 + i).build()));
    }

    // create a new list file, make sure we load the list generate by storeFileListFile2.
    StoreFileListFile storeFileListFile3 = create();
    StoreFileList fileList = storeFileListFile3.load(true);
    assertEquals(1, fileList.getStoreFileCount());
    StoreFileEntry entry = fileList.getStoreFile(0);
    assertEquals("hehe", entry.getName());
    assertEquals(10, entry.getSize());
  }
}
