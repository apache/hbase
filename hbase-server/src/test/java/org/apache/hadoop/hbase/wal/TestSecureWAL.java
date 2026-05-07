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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.io.crypto.MockAesKeyProvider;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.params.provider.Arguments;

@Tag(RegionServerTests.TAG)
@Tag(MediumTests.TAG)
@HBaseParameterizedTestTemplate
public class TestSecureWAL {

  static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private String testMethodName;

  public String walProvider;

  public TestSecureWAL(String walProvider) {
    this.walProvider = walProvider;
  }

  public static Stream<Arguments> parameters() {
    return Stream.of(Arguments.of("defaultProvider"), Arguments.of("asyncfs"));
  }

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, MockAesKeyProvider.class.getName());
    conf.set(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, "hbase");
    conf.setBoolean(HConstants.ENABLE_WAL_ENCRYPTION, true);
    CommonFSUtils.setRootDir(conf, TEST_UTIL.getDataTestDirOnTestFS());
    TEST_UTIL.startMiniDFSCluster(3);
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @BeforeEach
  public void setUp(TestInfo testInfo) {
    testMethodName = testInfo.getTestMethod().get().getName();
    TEST_UTIL.getConfiguration().set(WALFactory.WAL_PROVIDER, walProvider);
  }

  @TestTemplate
  public void testSecureWAL() throws Exception {
    TableName tableName = TableName.valueOf(testMethodName.replaceAll("[^a-zA-Z0-9]", "_"));
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
        EnvironmentEdgeManager.currentTime(), mvcc, scopes), kvs);
    }
    wal.sync();
    final Path walPath = AbstractFSWALProvider.getCurrentFileName(wal);
    wals.shutdown();

    // Insure edits are not plaintext
    long length = fs.getFileStatus(walPath).getLen();
    FSDataInputStream in = fs.open(walPath);
    byte[] fileData = new byte[(int) length];
    IOUtils.readFully(in, fileData);
    in.close();
    assertFalse(Bytes.contains(fileData, value), "Cells appear to be plaintext");

    // Confirm the WAL can be read back
    int count = 0;
    try (WALStreamReader reader = wals.createStreamReader(TEST_UTIL.getTestFileSystem(), walPath)) {
      WAL.Entry entry = new WAL.Entry();
      while (reader.next(entry) != null) {
        count++;
        List<Cell> cells = entry.getEdit().getCells();
        assertTrue(cells.size() == 1, "Should be one KV per WALEdit");
        for (Cell cell : cells) {
          assertTrue(Bytes.equals(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), row,
            0, row.length), "Incorrect row");
          assertTrue(Bytes.equals(cell.getFamilyArray(), cell.getFamilyOffset(),
            cell.getFamilyLength(), family, 0, family.length), "Incorrect family");
          assertTrue(Bytes.equals(cell.getValueArray(), cell.getValueOffset(),
            cell.getValueLength(), value, 0, value.length), "Incorrect value");
        }
      }
      assertEquals(total, count, "Should have read back as many KVs as written");
    }
  }
}
