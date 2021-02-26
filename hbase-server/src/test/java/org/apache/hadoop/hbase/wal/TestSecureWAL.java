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
package org.apache.hadoop.hbase.wal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.io.crypto.KeyProviderForTesting;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.wal.SecureAsyncProtobufLogWriter;
import org.apache.hadoop.hbase.regionserver.wal.SecureProtobufLogReader;
import org.apache.hadoop.hbase.regionserver.wal.SecureProtobufLogWriter;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category({ RegionServerTests.class, MediumTests.class })
public class TestSecureWAL {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSecureWAL.class);

  static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Rule
  public TestName name = new TestName();

  @Parameter
  public String walProvider;

  @Parameters(name = "{index}: provider={0}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[] { "defaultProvider" }, new Object[] { "asyncfs" });
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, KeyProviderForTesting.class.getName());
    conf.set(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, "hbase");
    conf.setClass("hbase.regionserver.hlog.reader.impl", SecureProtobufLogReader.class,
      WAL.Reader.class);
    conf.setClass("hbase.regionserver.hlog.writer.impl", SecureProtobufLogWriter.class,
      WALProvider.Writer.class);
    conf.setClass("hbase.regionserver.hlog.async.writer.impl", SecureAsyncProtobufLogWriter.class,
      WALProvider.AsyncWriter.class);
    conf.setBoolean(HConstants.ENABLE_WAL_ENCRYPTION, true);
    CommonFSUtils.setRootDir(conf, TEST_UTIL.getDataTestDirOnTestFS());
    TEST_UTIL.startMiniDFSCluster(3);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() {
    TEST_UTIL.getConfiguration().set(WALFactory.WAL_PROVIDER, walProvider);
  }

  @Test
  public void testSecureWAL() throws Exception {
    TableName tableName = TableName.valueOf(name.getMethodName().replaceAll("[^a-zA-Z0-9]", "_"));
    NavigableMap<byte[], Integer> scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    scopes.put(tableName.getName(), 0);
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(tableName).build();
    final int total = 10;
    final byte[] row = Bytes.toBytes("row");
    final byte[] family = Bytes.toBytes("family");
    final byte[] value = Bytes.toBytes("Test value");
    FileSystem fs = TEST_UTIL.getDFSCluster().getFileSystem();
    final WALFactory wals =
        new WALFactory(TEST_UTIL.getConfiguration(), tableName.getNameAsString());

    // Write the WAL
    final WAL wal = wals.getWAL(regionInfo);

    MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();

    for (int i = 0; i < total; i++) {
      WALEdit kvs = new WALEdit();
      kvs.add(new KeyValue(row, family, Bytes.toBytes(i), value));
      wal.appendData(regionInfo, new WALKeyImpl(regionInfo.getEncodedNameAsBytes(), tableName,
        System.currentTimeMillis(), mvcc, scopes), kvs);
    }
    wal.sync();
    final Path walPath = AbstractFSWALProvider.getCurrentFileName(wal);
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
        assertTrue("Incorrect row", Bytes.equals(cell.getRowArray(), cell.getRowOffset(),
          cell.getRowLength(), row, 0, row.length));
        assertTrue("Incorrect family", Bytes.equals(cell.getFamilyArray(), cell.getFamilyOffset(),
          cell.getFamilyLength(), family, 0, family.length));
        assertTrue("Incorrect value", Bytes.equals(cell.getValueArray(), cell.getValueOffset(),
          cell.getValueLength(), value, 0, value.length));
      }
    }
    assertEquals("Should have read back as many KVs as written", total, count);
    reader.close();
  }
}
