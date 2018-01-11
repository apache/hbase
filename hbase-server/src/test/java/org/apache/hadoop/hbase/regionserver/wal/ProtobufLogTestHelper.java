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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.stream.IntStream;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.hbase.wal.WALProvider;

/**
 * Helper class for testing protobuf log.
 */
final class ProtobufLogTestHelper {

  private ProtobufLogTestHelper() {
  }

  private static byte[] toValue(int prefix, int suffix) {
    return Bytes.toBytes(prefix + "-" + suffix);
  }

  private static RegionInfo toRegionInfo(TableName tableName) {
    return RegionInfoBuilder.newBuilder(tableName).setRegionId(1024).build();
  }

  public static void doWrite(WALProvider.Writer writer, boolean withTrailer, TableName tableName,
      int columnCount, int recordCount, byte[] row, long timestamp) throws IOException {
    RegionInfo hri = toRegionInfo(tableName);
    for (int i = 0; i < recordCount; i++) {
      WALKeyImpl key = new WALKeyImpl(hri.getEncodedNameAsBytes(), tableName, i, timestamp,
          HConstants.DEFAULT_CLUSTER_ID);
      WALEdit edit = new WALEdit();
      int prefix = i;
      IntStream.range(0, columnCount).mapToObj(j -> toValue(prefix, j))
          .map(value -> new KeyValue(row, row, row, timestamp, value)).forEachOrdered(edit::add);
      writer.append(new WAL.Entry(key, edit));
    }
    writer.sync(false);
    if (withTrailer) {
      writer.close();
    }
  }

  public static void doRead(ProtobufLogReader reader, boolean withTrailer, TableName tableName,
      int columnCount, int recordCount, byte[] row, long timestamp) throws IOException {
    if (withTrailer) {
      assertNotNull(reader.trailer);
    } else {
      assertNull(reader.trailer);
    }
    RegionInfo hri = toRegionInfo(tableName);
    for (int i = 0; i < recordCount; ++i) {
      WAL.Entry entry = reader.next();
      assertNotNull(entry);
      assertEquals(columnCount, entry.getEdit().size());
      assertArrayEquals(hri.getEncodedNameAsBytes(), entry.getKey().getEncodedRegionName());
      assertEquals(tableName, entry.getKey().getTableName());
      int idx = 0;
      for (Cell val : entry.getEdit().getCells()) {
        assertTrue(Bytes.equals(row, 0, row.length, val.getRowArray(), val.getRowOffset(),
          val.getRowLength()));
        assertArrayEquals(toValue(i, idx), CellUtil.cloneValue(val));
        idx++;
      }
    }
    assertNull(reader.next());
  }
}
