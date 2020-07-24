/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hbase.ccsmap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;

@InterfaceAudience.Private
public class CompactedCellHelper implements CompactedTypeHelper<Cell, Cell> {

  private final KVComparator comparator;

  public CompactedCellHelper(KVComparator c) {
    this.comparator = c;
  }

  @Override
  public int getCompactedSize(Cell key, Cell value) {
    long sz = KeyValue.getKeyValueDataStructureSize(key.getRowLength(),
      key.getFamilyLength(), key.getQualifierLength(),
      key.getValueLength(), key.getTagsLength()) + Bytes.SIZEOF_LONG; // mvcc
    if (sz > Integer.MAX_VALUE) {
      throw new RuntimeException("Too big cell");
    }
    return (int) sz;
  }

  @Override
  public void compact(Cell key, Cell value, byte[] data, int offset, int len) {
    KeyValue kv = new KeyValue(key);
    System.arraycopy(kv.getRowArray(), kv.getOffset(), data, offset, kv.getLength());
    CCSMapUtils.writeSeqId(data, offset, len, key.getSequenceId());
  }

  @Override
  public KVPair<Cell, Cell> decomposite(byte[] data, int offset, int len) {
    KeyValue kv = new OnPageKeyValue(data, offset, len);
    return new KVPair<Cell, Cell>(kv, kv);
  }

  @Override
  public int compare(byte[] ldata, int loffset, int llen, byte[] rdata,
    int roffset, int rlen) {
    KVPair<Cell, Cell> lc = decomposite(ldata, loffset, llen);
    KVPair<Cell, Cell> rc = decomposite(rdata, roffset, rlen);
    return comparator.compare(lc.key(), rc.key());
  }

  @Override
  public int compare(Cell key, byte[] data, int offset, int len) {
    KVPair<Cell, Cell> rc = decomposite(data, offset, len);
    return comparator.compare(key, rc.key());
  }

  @Override
  public int compare(Cell lkey, Cell rkey) {
    return comparator.compare(lkey, rkey);
  }

}

