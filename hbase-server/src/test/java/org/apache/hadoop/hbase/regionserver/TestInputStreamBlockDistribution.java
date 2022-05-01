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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.io.FileLink;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestInputStreamBlockDistribution {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestInputStreamBlockDistribution.class);

  private Configuration conf;
  private FileSystem fs;
  private Path testPath;

  @Before
  public void setUp() throws Exception {
    HBaseTestingUtility testUtil = new HBaseTestingUtility();
    conf = testUtil.getConfiguration();
    conf.setInt("dfs.blocksize", 1024 * 1024);
    conf.setInt("dfs.client.read.prefetch.size", 2 * 1024 * 1024);

    testUtil.startMiniDFSCluster(1);
    MiniDFSCluster cluster = testUtil.getDFSCluster();
    fs = cluster.getFileSystem();

    testPath = new Path(testUtil.getDefaultRootDirPath(), "test.file");

    writeSomeData(fs, testPath, 256 << 20, (byte) 2);
  }

  @After
  public void tearDown() throws Exception {
    fs.delete(testPath, false);
    fs.close();
  }

  @Test
  public void itDerivesLocalityFromHFileInputStream() throws Exception {
    try (FSDataInputStream stream = fs.open(testPath)) {
      HDFSBlocksDistribution initial = new HDFSBlocksDistribution();
      InputStreamBlockDistribution test =
        new InputStreamBlockDistribution(stream, getMockedStoreFileInfo(initial, false));

      assertSame(initial, test.getHDFSBlockDistribution());

      test.setLastCachedAt(test.getCachePeriodMs() + 1);

      assertNotSame(initial, test.getHDFSBlockDistribution());
    }

  }

  @Test
  public void itDerivesLocalityFromFileLinkInputStream() throws Exception {
    List<Path> files = new ArrayList<Path>();
    files.add(testPath);

    FileLink link = new FileLink(files);
    try (FSDataInputStream stream = link.open(fs)) {

      HDFSBlocksDistribution initial = new HDFSBlocksDistribution();

      InputStreamBlockDistribution test =
        new InputStreamBlockDistribution(stream, getMockedStoreFileInfo(initial, true));

      assertSame(initial, test.getHDFSBlockDistribution());

      test.setLastCachedAt(test.getCachePeriodMs() + 1);

      assertNotSame(initial, test.getHDFSBlockDistribution());
    }
  }

  @Test
  public void itFallsBackOnLastKnownValueWhenUnsupported() {
    FSDataInputStream fakeStream = mock(FSDataInputStream.class);
    HDFSBlocksDistribution initial = new HDFSBlocksDistribution();

    InputStreamBlockDistribution test =
      new InputStreamBlockDistribution(fakeStream, getMockedStoreFileInfo(initial, false));

    assertSame(initial, test.getHDFSBlockDistribution());
    test.setLastCachedAt(test.getCachePeriodMs() + 1);

    // fakeStream is not an HdfsDataInputStream or FileLink, so will fail to resolve
    assertSame(initial, test.getHDFSBlockDistribution());
    assertTrue(test.isStreamUnsupported());
  }

  @Test
  public void itFallsBackOnLastKnownValueOnException() throws IOException {
    HdfsDataInputStream fakeStream = mock(HdfsDataInputStream.class);
    when(fakeStream.getAllBlocks()).thenThrow(new IOException("test"));

    HDFSBlocksDistribution initial = new HDFSBlocksDistribution();

    InputStreamBlockDistribution test =
      new InputStreamBlockDistribution(fakeStream, getMockedStoreFileInfo(initial, false));

    assertSame(initial, test.getHDFSBlockDistribution());
    test.setLastCachedAt(test.getCachePeriodMs() + 1);

    // fakeStream throws an exception, so falls back on original
    assertSame(initial, test.getHDFSBlockDistribution());

    assertFalse(test.isStreamUnsupported());
  }

  /**
   * Write up to 'size' bytes with value 'v' into a new file called 'path'.
   */
  private void writeSomeData(FileSystem fs, Path path, long size, byte v) throws IOException {
    byte[] data = new byte[4096];
    for (int i = 0; i < data.length; i++) {
      data[i] = v;
    }

    FSDataOutputStream stream = fs.create(path);
    try {
      long written = 0;
      while (written < size) {
        stream.write(data, 0, data.length);
        written += data.length;
      }
    } finally {
      stream.close();
    }
  }

  private StoreFileInfo getMockedStoreFileInfo(HDFSBlocksDistribution distribution,
    boolean isFileLink) {
    StoreFileInfo mock = mock(StoreFileInfo.class);
    when(mock.getHDFSBlockDistribution()).thenReturn(distribution);
    when(mock.getConf()).thenReturn(conf);
    when(mock.isLink()).thenReturn(isFileLink);
    return mock;
  }
}
