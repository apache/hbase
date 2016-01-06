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
package org.apache.hadoop.hbase.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, SmallTests.class})
public class TestByteRangeWithKVSerialization {

  static void writeCell(PositionedByteRange pbr, KeyValue kv) throws Exception {
    pbr.putInt(kv.getKeyLength());
    pbr.putInt(kv.getValueLength());
    pbr.put(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength());
    pbr.put(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
    int tagsLen = kv.getTagsLength();
    pbr.put((byte) (tagsLen >> 8 & 0xff));
    pbr.put((byte) (tagsLen & 0xff));
    pbr.put(kv.getTagsArray(), kv.getTagsOffset(), tagsLen);
    pbr.putVLong(kv.getSequenceId());
  }

  static KeyValue readCell(PositionedByteRange pbr) throws Exception {
    int kvStartPos = pbr.getPosition();
    int keyLen = pbr.getInt();
    int valLen = pbr.getInt();
    pbr.setPosition(pbr.getPosition() + keyLen + valLen); // Skip the key and value section
    int tagsLen = ((pbr.get() & 0xff) << 8) ^ (pbr.get() & 0xff);
    pbr.setPosition(pbr.getPosition() + tagsLen); // Skip the tags section
    long mvcc = pbr.getVLong();
    KeyValue kv = new KeyValue(pbr.getBytes(), kvStartPos,
        (int) KeyValue.getKeyValueDataStructureSize(keyLen, valLen, tagsLen));
    kv.setSequenceId(mvcc);
    return kv;
  }

  @Test
  public void testWritingAndReadingCells() throws Exception {
    final byte[] FAMILY = Bytes.toBytes("f1");
    final byte[] QUALIFIER = Bytes.toBytes("q1");
    final byte[] VALUE = Bytes.toBytes("v");
    int kvCount = 1000000;
    List<KeyValue> kvs = new ArrayList<KeyValue>(kvCount);
    int totalSize = 0;
    Tag[] tags = new Tag[] { new ArrayBackedTag((byte) 1, "tag1") };
    for (int i = 0; i < kvCount; i++) {
      KeyValue kv = new KeyValue(Bytes.toBytes(i), FAMILY, QUALIFIER, i, VALUE, tags);
      kv.setSequenceId(i);
      kvs.add(kv);
      totalSize += kv.getLength() + Bytes.SIZEOF_LONG;
    }
    PositionedByteRange pbr = new SimplePositionedMutableByteRange(totalSize);
    for (KeyValue kv : kvs) {
      writeCell(pbr, kv);
    }

    PositionedByteRange pbr1 = new SimplePositionedMutableByteRange(pbr.getBytes(), 0,
        pbr.getPosition());
    for (int i = 0; i < kvCount; i++) {
      KeyValue kv = readCell(pbr1);
      KeyValue kv1 = kvs.get(i);
      Assert.assertTrue(kv.equals(kv1));
      Assert.assertTrue(Bytes.equals(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength(),
          kv1.getValueArray(), kv1.getValueOffset(), kv1.getValueLength()));
      Assert.assertTrue(Bytes.equals(kv.getTagsArray(), kv.getTagsOffset(),
          kv.getTagsLength(), kv1.getTagsArray(), kv1.getTagsOffset(),
          kv1.getTagsLength()));
      Assert.assertEquals(kv1.getSequenceId(), kv.getSequenceId());
    }
  }
}