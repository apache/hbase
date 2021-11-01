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
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.util.Bytes;

@SuppressWarnings("checkstyle:innerassignment")
public class CompressedWALTestBase {

  protected final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  static final byte[] VALUE;
  static {
    // 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597
    VALUE = new byte[1+1+2+3+5+8+13+21+34+55+89+144+233+377+610+987+1597];
    int off = 0;
    Arrays.fill(VALUE, off, (off+=1), (byte)'A');
    Arrays.fill(VALUE, off, (off+=1), (byte)'B');
    Arrays.fill(VALUE, off, (off+=2), (byte)'C');
    Arrays.fill(VALUE, off, (off+=3), (byte)'D');
    Arrays.fill(VALUE, off, (off+=5), (byte)'E');
    Arrays.fill(VALUE, off, (off+=8), (byte)'F');
    Arrays.fill(VALUE, off, (off+=13), (byte)'G');
    Arrays.fill(VALUE, off, (off+=21), (byte)'H');
    Arrays.fill(VALUE, off, (off+=34), (byte)'I');
    Arrays.fill(VALUE, off, (off+=55), (byte)'J');
    Arrays.fill(VALUE, off, (off+=89), (byte)'K');
    Arrays.fill(VALUE, off, (off+=144), (byte)'L');
    Arrays.fill(VALUE, off, (off+=233), (byte)'M');
    Arrays.fill(VALUE, off, (off+=377), (byte)'N');
    Arrays.fill(VALUE, off, (off+=610), (byte)'O');
    Arrays.fill(VALUE, off, (off+=987), (byte)'P');
    Arrays.fill(VALUE, off, (off+=1597), (byte)'Q');
  }

  public void doTest(TableName tableName) throws Exception {
    NavigableMap<byte[], Integer> scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    scopes.put(tableName.getName(), 0);
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(tableName).build();
    final int total = 1000;
    final byte[] row = Bytes.toBytes("row");
    final byte[] family = Bytes.toBytes("family");
    final byte[] value = VALUE;
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
