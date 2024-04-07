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
package org.apache.hadoop.hbase.regionserver.storefiletracker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
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

import org.apache.hadoop.hbase.shaded.protobuf.generated.StoreFileTrackerProtos.StoreFileList;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestStoreFileListFile {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestStoreFileListFile.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestStoreFileListFile.class);

  private static final HBaseCommonTestingUtility UTIL = new HBaseCommonTestingUtility();

  private Path testDir;

  private StoreFileListFile storeFileListFile;

  @Rule
  public TestName name = new TestName();

  @Before
  public void setUp() throws IOException {
    testDir = UTIL.getDataTestDir(name.getMethodName());
    HRegionFileSystem hfs = mock(HRegionFileSystem.class);
    when(hfs.getFileSystem()).thenReturn(FileSystem.get(UTIL.getConfiguration()));
    StoreContext ctx = StoreContext.getBuilder().withFamilyStoreDirectoryPath(testDir)
      .withRegionFileSystem(hfs).build();
    storeFileListFile = new StoreFileListFile(ctx);
  }

  @AfterClass
  public static void tearDown() {
    UTIL.cleanupTestDir();
  }

  @Test
  public void testEmptyLoad() throws IOException {
    assertNull(storeFileListFile.load());
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
    assertNull(storeFileListFile.load());
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
    assertThrows(IOException.class, () -> storeFileListFile.load());
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
    assertThrows(IOException.class, () -> storeFileListFile.load());
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
    assertThrows(IOException.class, () -> storeFileListFile.load());
  }

  @Test
  public void testLoadHigherVersion() throws IOException {
    // write a fake StoreFileList file with higher version
    StoreFileList storeFileList =
      StoreFileList.newBuilder().setVersion(StoreFileListFile.VERSION + 1)
        .setTimestamp(EnvironmentEdgeManager.currentTime()).build();
    Path trackFileDir = new Path(testDir, StoreFileListFile.TRACK_FILE_DIR);
    StoreFileListFile.write(FileSystem.get(UTIL.getConfiguration()),
      new Path(trackFileDir, StoreFileListFile.TRACK_FILE), storeFileList);
    IOException error = assertThrows(IOException.class, () -> storeFileListFile.load());
    assertEquals("Higher store file list version detected, expected " + StoreFileListFile.VERSION
      + ", got " + (StoreFileListFile.VERSION + 1), error.getMessage());
  }
}
