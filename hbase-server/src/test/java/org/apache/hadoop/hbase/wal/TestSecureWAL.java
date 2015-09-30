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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.crypto.KeyProviderForTesting;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.log4j.Level;

// imports for things that haven't moved from regionserver.wal yet.
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.regionserver.wal.SecureProtobufLogReader;
import org.apache.hadoop.hbase.regionserver.wal.SecureProtobufLogWriter;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestSecureWAL {
  private static final Log LOG = LogFactory.getLog(TestSecureWAL.class);
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
      WAL.Reader.class);
    conf.setClass("hbase.regionserver.hlog.writer.impl", SecureProtobufLogWriter.class,
      WALProvider.Writer.class);
    conf.setBoolean(HConstants.ENABLE_WAL_ENCRYPTION, true);
    FSUtils.setRootDir(conf, TEST_UTIL.getDataTestDir());
  }

  @Test
  public void testSecureWAL() throws Exception {
    TableName tableName = TableName.valueOf("TestSecureWAL");
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(tableName.getName()));
    HRegionInfo regioninfo = new HRegionInfo(tableName,
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, false);
    final int total = 10;
    final byte[] row = Bytes.toBytes("row");
    final byte[] family = Bytes.toBytes("family");
    final byte[] value = Bytes.toBytes("Test value");
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    final WALFactory wals = new WALFactory(TEST_UTIL.getConfiguration(), null, "TestSecureWAL");

    // Write the WAL
    final WAL wal = wals.getWAL(regioninfo.getEncodedNameAsBytes());

    for (int i = 0; i < total; i++) {
      WALEdit kvs = new WALEdit();
      kvs.add(new KeyValue(row, family, Bytes.toBytes(i), value));
      wal.append(htd, regioninfo, new WALKey(regioninfo.getEncodedNameAsBytes(), tableName,
          System.currentTimeMillis()), kvs, true);
    }
    wal.sync();
    final Path walPath = DefaultWALProvider.getCurrentFileName(wal);
    wals.shutdown();

    // Insure edits are not plaintext
    long length = fs.getFileStatus(walPath).getLen();
    FSDataInputStream in = fs.open(walPath);
    byte[] fileData = new byte[(int)length];
    IOUtils.readFully(in, fileData);
    in.close();
    assertFalse("Cells appear to be plaintext", Bytes.contains(fileData, value));

    // Confirm the WAL can be read back
    WAL.Reader reader = wals.createReader(TEST_UTIL.getTestFileSystem(), walPath);
    int count = 0;
    WAL.Entry entry = new WAL.Entry();
    while (reader.next(entry) != null) {
      count++;
      List<Cell> cells = entry.getEdit().getCells();
      assertTrue("Should be one KV per WALEdit", cells.size() == 1);
      for (Cell cell: cells) {
        byte[] thisRow = cell.getRow();
        assertTrue("Incorrect row", Bytes.equals(thisRow, row));
        byte[] thisFamily = cell.getFamily();
        assertTrue("Incorrect family", Bytes.equals(thisFamily, family));
        byte[] thisValue = cell.getValue();
        assertTrue("Incorrect value", Bytes.equals(thisValue, value));
      }
    }
    assertEquals("Should have read back as many KVs as written", total, count);
    reader.close();
  }

}
