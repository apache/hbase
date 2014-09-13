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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.crypto.KeyProviderForTesting;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestSecureHLog {
  static final Log LOG = LogFactory.getLog(TestSecureHLog.class);
  static {
    ((Log4JLogger)LogFactory.getLog("org.apache.hadoop.hbase.regionserver.wal"))
      .getLogger().setLevel(Level.ALL);
  };
  static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, KeyProviderForTesting.class.getName());
    conf.set(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, "hbase");
    conf.setClass("hbase.regionserver.hlog.reader.impl", SecureProtobufLogReader.class,
      HLog.Reader.class);
    conf.setClass("hbase.regionserver.hlog.writer.impl", SecureProtobufLogWriter.class,
      HLog.Writer.class);
    conf.setBoolean(HConstants.ENABLE_WAL_ENCRYPTION, true);
  }

  @Test
  public void testSecureHLog() throws Exception {
    TableName tableName = TableName.valueOf("TestSecureHLog");
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(tableName.getName()));
    HRegionInfo regioninfo = new HRegionInfo(tableName,
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, false);
    final int total = 10;
    final byte[] row = Bytes.toBytes("row");
    final byte[] family = Bytes.toBytes("family");
    final byte[] value = Bytes.toBytes("Test value");
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path logDir = TEST_UTIL.getDataTestDir("log");
    final AtomicLong sequenceId = new AtomicLong(1);

    // Write the WAL
    HLog wal = new FSHLog(fs, TEST_UTIL.getDataTestDir(), logDir.toString(),
      TEST_UTIL.getConfiguration());
    for (int i = 0; i < total; i++) {
      WALEdit kvs = new WALEdit();
      kvs.add(new KeyValue(row, family, Bytes.toBytes(i), value));
      wal.append(regioninfo, tableName, kvs, System.currentTimeMillis(), htd, sequenceId);
    }
    final Path walPath = ((FSHLog) wal).computeFilename();
    wal.close();

    // Insure edits are not plaintext
    long length = fs.getFileStatus(walPath).getLen();
    FSDataInputStream in = fs.open(walPath);
    byte[] fileData = new byte[(int)length];
    IOUtils.readFully(in, fileData);
    in.close();
    assertFalse("Cells appear to be plaintext", Bytes.contains(fileData, value));

    // Confirm the WAL can be read back
    HLog.Reader reader = HLogFactory.createReader(TEST_UTIL.getTestFileSystem(), walPath,
      TEST_UTIL.getConfiguration());
    int count = 0;
    HLog.Entry entry = new HLog.Entry();
    while (reader.next(entry) != null) {
      count++;
      List<KeyValue> kvs = entry.getEdit().getKeyValues();
      assertTrue("Should be one KV per WALEdit", kvs.size() == 1);
      for (KeyValue kv: kvs) {
        byte[] thisRow = kv.getRow();
        assertTrue("Incorrect row", Bytes.equals(thisRow, row));
        byte[] thisFamily = kv.getFamily();
        assertTrue("Incorrect family", Bytes.equals(thisFamily, family));
        byte[] thisValue = kv.getValue();
        assertTrue("Incorrect value", Bytes.equals(thisValue, value));
      }
    }
    assertEquals("Should have read back as many KVs as written", total, count);
    reader.close();
  }

}
