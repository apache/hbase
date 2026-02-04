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
package org.apache.hadoop.hbase.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class KeyOnlyCellComparable implements WritableComparable<KeyOnlyCellComparable> {

  static {
    WritableComparator.define(KeyOnlyCellComparable.class, new KeyOnlyCellComparator());
  }

  private ExtendedCell cell = null;

  public KeyOnlyCellComparable() {
  }

  public KeyOnlyCellComparable(ExtendedCell cell) {
    this.cell = cell;
  }

  public ExtendedCell getCell() {
    return cell;
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EQ_COMPARETO_USE_OBJECT_EQUALS",
      justification = "This is wrong, yes, but we should be purging Writables, not fixing them")
  public int compareTo(KeyOnlyCellComparable o) {
    return CellComparator.getInstance().compare(cell, o.cell);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    int keyLen = PrivateCellUtil.estimatedSerializedSizeOfKey(cell);
    int valueLen = 0; // We avoid writing value here. So just serialize as if an empty value.
    out.writeInt(keyLen + valueLen + KeyValue.KEYVALUE_INFRASTRUCTURE_SIZE);
    out.writeInt(keyLen);
    out.writeInt(valueLen);
    PrivateCellUtil.writeFlatKey(cell, out);
    out.writeLong(cell.getSequenceId());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    cell = KeyValue.create(in);
    long seqId = in.readLong();
    cell.setSequenceId(seqId);
  }

  public static class KeyOnlyCellComparator extends WritableComparator {

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      try (DataInputStream d1 = new DataInputStream(new ByteArrayInputStream(b1, s1, l1));
        DataInputStream d2 = new DataInputStream(new ByteArrayInputStream(b2, s2, l2))) {
        KeyOnlyCellComparable kv1 = new KeyOnlyCellComparable();
        kv1.readFields(d1);
        KeyOnlyCellComparable kv2 = new KeyOnlyCellComparable();
        kv2.readFields(d2);
        return compare(kv1, kv2);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
