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
package org.apache.hadoop.hbase.wal;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ByteBufferKeyValue;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.io.crypto.KeyProviderForTesting;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogReader;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogWriter;
import org.apache.hadoop.hbase.regionserver.wal.SecureAsyncProtobufLogWriter;
import org.apache.hadoop.hbase.regionserver.wal.SecureProtobufLogReader;
import org.apache.hadoop.hbase.regionserver.wal.SecureProtobufLogWriter;
import org.apache.hadoop.hbase.regionserver.wal.SecureWALCellCodec;
import org.apache.hadoop.hbase.regionserver.wal.WALCellCodec;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Test that verifies WAL written by SecureProtobufLogWriter is not readable by ProtobufLogReader
 */
@Tag(RegionServerTests.TAG)
@Tag(SmallTests.TAG)
public class TestWALReaderOnSecureWAL {

  static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  final byte[] value = Bytes.toBytes("Test value");

  private static final String WAL_ENCRYPTION = "hbase.regionserver.wal.encryption";

  private String methodName;

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, KeyProviderForTesting.class.getName());
    conf.set(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, "hbase");
    conf.setBoolean(WALSplitter.SPLIT_SKIP_ERRORS_KEY, true);
    conf.setBoolean(HConstants.ENABLE_WAL_ENCRYPTION, true);
    CommonFSUtils.setRootDir(conf, TEST_UTIL.getDataTestDir());
  }

  @BeforeEach
  public void setUp(TestInfo testInfo) {
    methodName = testInfo.getTestMethod().get().getName();
  }

  private Path writeWAL(final WALFactory wals, final String tblName, boolean offheap)
    throws IOException {
    Configuration conf = TEST_UTIL.getConfiguration();
    String clsName = conf.get(WALCellCodec.WAL_CELL_CODEC_CLASS_KEY, WALCellCodec.class.getName());
    conf.setClass(WALCellCodec.WAL_CELL_CODEC_CLASS_KEY, SecureWALCellCodec.class,
      WALCellCodec.class);
    try {
      TableName tableName = TableName.valueOf(tblName);
      NavigableMap<byte[], Integer> scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      scopes.put(tableName.getName(), 0);
      RegionInfo regionInfo = RegionInfoBuilder.newBuilder(tableName).build();
      final int total = 10;
      final byte[] row = Bytes.toBytes("row");
      final byte[] family = Bytes.toBytes("family");
      final MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl(1);

      // Write the WAL
      WAL wal = wals.getWAL(regionInfo);
      for (int i = 0; i < total; i++) {
        WALEdit kvs = new WALEdit();
        KeyValue kv = new KeyValue(row, family, Bytes.toBytes(i), value);
        if (offheap) {
          ByteBuffer bb = ByteBuffer.allocateDirect(kv.getBuffer().length);
          bb.put(kv.getBuffer());
          ByteBufferKeyValue offheapKV = new ByteBufferKeyValue(bb, 0, kv.getLength());
          kvs.add(offheapKV);
        } else {
          kvs.add(kv);
        }
        wal.appendData(regionInfo, new WALKeyImpl(regionInfo.getEncodedNameAsBytes(), tableName,
          EnvironmentEdgeManager.currentTime(), mvcc, scopes), kvs);
      }
      wal.sync();
      final Path walPath = AbstractFSWALProvider.getCurrentFileName(wal);
      wal.shutdown();

      return walPath;
    } finally {
      // restore the cell codec class
      conf.set(WALCellCodec.WAL_CELL_CODEC_CLASS_KEY, clsName);
    }
  }

  @Test()
  public void testWALReaderOnSecureWALWithKeyValues() throws Exception {
    testSecureWALInternal(false);
  }

  @Test()
  public void testWALReaderOnSecureWALWithOffheapKeyValues() throws Exception {
    testSecureWALInternal(true);
  }

  private void testSecureWALInternal(boolean offheap) throws IOException, FileNotFoundException {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setClass("hbase.regionserver.hlog.reader.impl", ProtobufLogReader.class, WAL.Reader.class);
    conf.setClass("hbase.regionserver.hlog.writer.impl", SecureProtobufLogWriter.class,
      WALProvider.Writer.class);
    conf.setClass("hbase.regionserver.hlog.async.writer.impl", SecureAsyncProtobufLogWriter.class,
      WALProvider.AsyncWriter.class);
    conf.setBoolean(WAL_ENCRYPTION, true);
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    final WALFactory wals = new WALFactory(conf, methodName);
    Path walPath = writeWAL(wals, methodName, offheap);

    // Insure edits are not plaintext
    long length = fs.getFileStatus(walPath).getLen();
    FSDataInputStream in = fs.open(walPath);
    byte[] fileData = new byte[(int) length];
    IOUtils.readFully(in, fileData);
    in.close();
    assertFalse(Bytes.contains(fileData, value), "Cells appear to be plaintext");

    // Confirm the WAL cannot be read back by ProtobufLogReader
    try {
      wals.createReader(TEST_UTIL.getTestFileSystem(), walPath);
      assertFalse(true);
    } catch (IOException ioe) {
      System.out.println("Expected ioe " + ioe.getMessage());
    }

    FileStatus[] listStatus = fs.listStatus(walPath.getParent());
    Path rootdir = CommonFSUtils.getRootDir(conf);
    WALSplitter s = new WALSplitter(wals, conf, rootdir, fs, rootdir, fs, null, null, null);
    WALSplitter.SplitWALResult swr = s.splitWAL(listStatus[0], null);
    assertTrue(swr.isCorrupt());
    wals.close();
  }

  @Test()
  public void testSecureWALReaderOnWAL() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setClass("hbase.regionserver.hlog.reader.impl", SecureProtobufLogReader.class,
      WAL.Reader.class);
    conf.setClass("hbase.regionserver.hlog.writer.impl", ProtobufLogWriter.class,
      WALProvider.Writer.class);
    conf.setBoolean(WAL_ENCRYPTION, false);
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    final WALFactory wals = new WALFactory(conf,
      ServerName.valueOf(methodName, 16010, EnvironmentEdgeManager.currentTime()).toString());
    Path walPath = writeWAL(wals, methodName, false);

    // Ensure edits are plaintext
    long length = fs.getFileStatus(walPath).getLen();
    FSDataInputStream in = fs.open(walPath);
    byte[] fileData = new byte[(int) length];
    IOUtils.readFully(in, fileData);
    in.close();
    assertTrue(Bytes.contains(fileData, value), "Cells should be plaintext");

    // Confirm the WAL can be read back by SecureProtobufLogReader
    try {
      WAL.Reader reader = wals.createReader(TEST_UTIL.getTestFileSystem(), walPath);
      reader.close();
    } catch (IOException ioe) {
      assertFalse(true);
    }

    FileStatus[] listStatus = fs.listStatus(walPath.getParent());
    Path rootdir = CommonFSUtils.getRootDir(conf);
    try {
      WALSplitter s = new WALSplitter(wals, conf, rootdir, fs, rootdir, fs, null, null, null);
      s.splitWAL(listStatus[0], null);
      Path file =
        new Path(ZKSplitLog.getSplitLogDir(rootdir, listStatus[0].getPath().getName()), "corrupt");
      assertTrue(!fs.exists(file));
    } catch (IOException ioe) {
      fail("WAL should have been processed");
    }
    wals.close();
  }
}
