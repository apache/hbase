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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.crypto.KeyProviderForTesting;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.SplitLogTask.RecoveryMode;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;
import org.apache.log4j.Level;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/*
 * Test that verifies WAL written by SecureProtobufLogWriter is not readable by ProtobufLogReader
 */
@Category(MediumTests.class)
public class TestHLogReaderOnSecureHLog {
  static final Log LOG = LogFactory.getLog(TestHLogReaderOnSecureHLog.class);
  static {
    ((Log4JLogger)LogFactory.getLog("org.apache.hadoop.hbase.regionserver.wal"))
      .getLogger().setLevel(Level.ALL);
  };
  static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  final byte[] value = Bytes.toBytes("Test value");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, KeyProviderForTesting.class.getName());
    conf.set(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, "hbase");
    conf.setBoolean("hbase.hlog.split.skip.errors", true);
    conf.setBoolean(HConstants.ENABLE_WAL_ENCRYPTION, true);
  }

  private Path writeWAL(String tblName, boolean encrypt) throws IOException {
    Configuration conf = TEST_UTIL.getConfiguration();
    String clsName = conf.get(WALCellCodec.WAL_CELL_CODEC_CLASS_KEY, WALCellCodec.class.getName());
    conf.setClass(WALCellCodec.WAL_CELL_CODEC_CLASS_KEY, SecureWALCellCodec.class,
      WALCellCodec.class);
    if (encrypt) {
      conf.set("hbase.regionserver.wal.encryption", "true");
    } else {
      conf.set("hbase.regionserver.wal.encryption", "false");
    }
    TableName tableName = TableName.valueOf(tblName);
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(tableName.getName()));
    HRegionInfo regioninfo = new HRegionInfo(tableName,
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, false);
    final int total = 10;
    final byte[] row = Bytes.toBytes("row");
    final byte[] family = Bytes.toBytes("family");
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path logDir = TEST_UTIL.getDataTestDir(tblName);
    final AtomicLong sequenceId = new AtomicLong(1);

    // Write the WAL
    FSHLog wal = new FSHLog(fs, TEST_UTIL.getDataTestDir(), logDir.toString(), conf);
    for (int i = 0; i < total; i++) {
      WALEdit kvs = new WALEdit();
      kvs.add(new KeyValue(row, family, Bytes.toBytes(i), value));
      wal.append(regioninfo, tableName, kvs, System.currentTimeMillis(), htd, sequenceId);
    }
    final Path walPath = ((FSHLog) wal).computeFilename();
    wal.close();
    // restore the cell codec class
    conf.set(WALCellCodec.WAL_CELL_CODEC_CLASS_KEY, clsName);
    
    return walPath;
  }
  
  @Test()
  public void testHLogReaderOnSecureHLog() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    HLogFactory.resetLogReaderClass();
    HLogFactory.resetLogWriterClass();
    conf.setClass("hbase.regionserver.hlog.reader.impl", ProtobufLogReader.class,
      HLog.Reader.class);
    conf.setClass("hbase.regionserver.hlog.writer.impl", SecureProtobufLogWriter.class,
      HLog.Writer.class);
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path walPath = writeWAL("testHLogReaderOnSecureHLog", true);

    // Insure edits are not plaintext
    long length = fs.getFileStatus(walPath).getLen();
    FSDataInputStream in = fs.open(walPath);
    byte[] fileData = new byte[(int)length];
    IOUtils.readFully(in, fileData);
    in.close();
    assertFalse("Cells appear to be plaintext", Bytes.contains(fileData, value));

    // Confirm the WAL cannot be read back by ProtobufLogReader
    try {
      HLog.Reader reader = HLogFactory.createReader(TEST_UTIL.getTestFileSystem(), walPath, conf);
      assertFalse(true);
    } catch (IOException ioe) {
      // expected IOE
    }
    
    FileStatus[] listStatus = fs.listStatus(walPath.getParent());
    RecoveryMode mode = (conf.getBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, false) ? 
        RecoveryMode.LOG_REPLAY : RecoveryMode.LOG_SPLITTING);
    Path rootdir = FSUtils.getRootDir(conf);
    try {
      HLogSplitter s = new HLogSplitter(conf, rootdir, fs, null, null, mode);
      s.splitLogFile(listStatus[0], null);
      Path file = new Path(ZKSplitLog.getSplitLogDir(rootdir, listStatus[0].getPath().getName()),
        "corrupt");
      assertTrue(fs.exists(file));
      // assertFalse("log splitting should have failed", true);
    } catch (IOException ioe) {
      assertTrue("WAL should have been sidelined", false);
    }
  }
  
  @Test()
  public void testSecureHLogReaderOnHLog() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    HLogFactory.resetLogReaderClass();
    HLogFactory.resetLogWriterClass();
    conf.setClass("hbase.regionserver.hlog.reader.impl", SecureProtobufLogReader.class,
      HLog.Reader.class);
    conf.setClass("hbase.regionserver.hlog.writer.impl", ProtobufLogWriter.class,
      HLog.Writer.class);
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path walPath = writeWAL("testSecureHLogReaderOnHLog", false);

    // Ensure edits are plaintext
    long length = fs.getFileStatus(walPath).getLen();
    FSDataInputStream in = fs.open(walPath);
    byte[] fileData = new byte[(int)length];
    IOUtils.readFully(in, fileData);
    in.close();
    assertTrue("Cells should be plaintext", Bytes.contains(fileData, value));

    // Confirm the WAL can be read back by ProtobufLogReader
    try {
      HLog.Reader reader = HLogFactory.createReader(TEST_UTIL.getTestFileSystem(), walPath, conf);
    } catch (IOException ioe) {
      assertFalse(true);
    }
    
    FileStatus[] listStatus = fs.listStatus(walPath.getParent());
    RecoveryMode mode = (conf.getBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, false) ? 
        RecoveryMode.LOG_REPLAY : RecoveryMode.LOG_SPLITTING);
    Path rootdir = FSUtils.getRootDir(conf);
    try {
      HLogSplitter s = new HLogSplitter(conf, rootdir, fs, null, null, mode);
      s.splitLogFile(listStatus[0], null);
      Path file = new Path(ZKSplitLog.getSplitLogDir(rootdir, listStatus[0].getPath().getName()),
        "corrupt");
      assertTrue(!fs.exists(file));
    } catch (IOException ioe) {
      assertTrue("WAL should have been processed", false);
    }
  }
}
