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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.AsyncFSWALProvider;
import org.apache.hadoop.hbase.wal.AsyncFSWALProvider.AsyncWriter;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestCombinedAsyncWriter {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCombinedAsyncWriter.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static EventLoopGroup EVENT_LOOP_GROUP;

  private static Class<? extends Channel> CHANNEL_CLASS;

  private static WALFactory WALS;

  @Rule
  public final TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    EVENT_LOOP_GROUP = new NioEventLoopGroup();
    CHANNEL_CLASS = NioSocketChannel.class;
    UTIL.startMiniDFSCluster(3);
    UTIL.getTestFileSystem().mkdirs(UTIL.getDataTestDirOnTestFS());
    WALS = new WALFactory(UTIL.getConfiguration(), TestCombinedAsyncWriter.class.getSimpleName());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (WALS != null) {
      WALS.close();
    }
    EVENT_LOOP_GROUP.shutdownGracefully().syncUninterruptibly();
    UTIL.shutdownMiniDFSCluster();
  }

  @Test
  public void testWithTrailer() throws IOException {
    doTest(true);
  }

  @Test
  public void testWithoutTrailer() throws IOException {
    doTest(false);
  }

  private Path getPath(int index) throws IOException {
    String methodName = name.getMethodName().replaceAll("[^A-Za-z0-9_-]", "_");
    return new Path(UTIL.getDataTestDirOnTestFS(), methodName + "-" + index);
  }

  private void doTest(boolean withTrailer) throws IOException {
    int columnCount = 5;
    int recordCount = 5;
    TableName tableName = TableName.valueOf("tablename");
    byte[] row = Bytes.toBytes("row");
    long timestamp = System.currentTimeMillis();
    Path path1 = getPath(1);
    Path path2 = getPath(2);
    FileSystem fs = UTIL.getTestFileSystem();
    Configuration conf = UTIL.getConfiguration();
    try (
      AsyncWriter writer1 = AsyncFSWALProvider.createAsyncWriter(conf, fs, path1, false,
        EVENT_LOOP_GROUP.next(), CHANNEL_CLASS);
      AsyncWriter writer2 = AsyncFSWALProvider.createAsyncWriter(conf, fs, path2, false,
        EVENT_LOOP_GROUP.next(), CHANNEL_CLASS);
      CombinedAsyncWriter writer = CombinedAsyncWriter.create(writer1, writer2)) {
      ProtobufLogTestHelper.doWrite(new WriterOverAsyncWriter(writer), withTrailer, tableName,
        columnCount, recordCount, row, timestamp);
      try (ProtobufLogReader reader = (ProtobufLogReader) WALS.createReader(fs, path1)) {
        ProtobufLogTestHelper.doRead(reader, withTrailer, tableName, columnCount, recordCount, row,
          timestamp);
      }
      try (ProtobufLogReader reader = (ProtobufLogReader) WALS.createReader(fs, path2)) {
        ProtobufLogTestHelper.doRead(reader, withTrailer, tableName, columnCount, recordCount, row,
          timestamp);
      }
    }
  }
}
