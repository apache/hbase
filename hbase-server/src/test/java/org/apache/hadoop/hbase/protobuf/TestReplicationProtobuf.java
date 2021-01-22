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
package org.apache.hadoop.hbase.protobuf;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALKey;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(SmallTests.class)
public class TestReplicationProtobuf {
  /**
   * Little test to check we can basically convert list of a list of KVs into a CellScanner
   * @throws IOException
   */
  @Test
  public void testGetCellScanner() throws IOException {
    List<Cell> a = new ArrayList<Cell>();
    KeyValue akv = new KeyValue(Bytes.toBytes("a"), -1L);
    a.add(akv);
    // Add a few just to make it less regular.
    a.add(new KeyValue(Bytes.toBytes("aa"), -1L));
    a.add(new KeyValue(Bytes.toBytes("aaa"), -1L));
    List<Cell> b = new ArrayList<Cell>();
    KeyValue bkv = new KeyValue(Bytes.toBytes("b"), -1L);
    a.add(bkv);
    List<Cell> c = new ArrayList<Cell>();
    KeyValue ckv = new KeyValue(Bytes.toBytes("c"), -1L);
    c.add(ckv);
    List<List<? extends Cell>> all = new ArrayList<List<? extends Cell>>();
    all.add(a);
    all.add(b);
    all.add(c);
    CellScanner scanner = ReplicationProtbufUtil.getCellScanner(all, 0);
    testAdvanceHasSameRow(scanner, akv);
    // Skip over aa
    scanner.advance();
    // Skip over aaa
    scanner.advance();
    testAdvanceHasSameRow(scanner, bkv);
    testAdvanceHasSameRow(scanner, ckv);
    assertFalse(scanner.advance());
  }

  private void testAdvanceHasSameRow(CellScanner scanner, final KeyValue kv) throws IOException {
    scanner.advance();
    assertTrue(Bytes.equals(scanner.current().getRowArray(), scanner.current().getRowOffset(),
        scanner.current().getRowLength(),
      kv.getRowArray(), kv.getRowOffset(), kv.getRowLength()));
  }

  @Test
  public void testWALEntryProtobufConstruction() throws Exception {
    byte[] encodedRegionName = Bytes.toBytes("region");
    TableName tableName = TableName.valueOf("table");
    long ts =  EnvironmentEdgeManager.currentTime();
    MultiVersionConcurrencyControl mvcc = null;
    Map<String, byte[]> extendedAttributes = new HashMap<String, byte[]>(1);
    String attrKey = "attr";
    byte[] attrValue = Bytes.toBytes("attrVal");
    extendedAttributes.put(attrKey, attrValue);
    WALKey key = new WALKey(encodedRegionName, tableName, ts, mvcc, extendedAttributes);
    Cell cell = CellUtil.createCell(Bytes.toBytes("row"), Bytes.toBytes("f"), Bytes.toBytes("q"),
      ts, KeyValue.Type.Put.getCode(), Bytes.toBytes("val"));
    WALEdit edit = new WALEdit(1);
    edit.add(cell);
    WAL.Entry entry = new WAL.Entry(key, edit);

    Pair<AdminProtos.ReplicateWALEntryRequest, CellScanner> pair =
      ReplicationProtbufUtil.buildReplicateWALEntryRequest(new WAL.Entry[] {entry});
    AdminProtos.ReplicateWALEntryRequest request = pair.getFirst();
    AdminProtos.WALEntry protoWALEntry = request.getEntry(0);
    WALProtos.WALKey protoWALKey = protoWALEntry.getKey();
    assertArrayEquals(encodedRegionName, protoWALKey.getEncodedRegionName().toByteArray());
    assertEquals(tableName, TableName.valueOf(protoWALKey.getTableName().toByteArray()));
    assertEquals(ts, protoWALKey.getWriteTime());
    assertEquals(extendedAttributes.size(), protoWALKey.getExtendedAttributesCount());
    for (WALProtos.Attribute attr : protoWALKey.getExtendedAttributesList()) {
      assertArrayEquals(extendedAttributes.get(attr.getKey()), attr.getValue().toByteArray());
    }
    //the WALEdit's cells don't go on the protobuf; they're in a cell block
    // elsewhere in the RPC protocol. We just mark the cell count
    assertEquals(edit.size(), protoWALEntry.getAssociatedCellCount());
  }
}
