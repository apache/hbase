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

package org.apache.hadoop.hbase.io;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.OffheapKeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.ByteBufferedCell;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.io.util.LRUDictionary;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, SmallTests.class})
public class TestTagCompressionContext {

  private static final byte[] ROW = Bytes.toBytes("r1");
  private static final byte[] CF = Bytes.toBytes("f");
  private static final byte[] Q = Bytes.toBytes("q");
  private static final byte[] V = Bytes.toBytes("v");

  @Test
  public void testCompressUncompressTags1() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    TagCompressionContext context = new TagCompressionContext(LRUDictionary.class, Byte.MAX_VALUE);
    KeyValue kv1 = createKVWithTags(2);
    int tagsLength1 = kv1.getTagsLength();
    ByteBuffer ib = ByteBuffer.wrap(kv1.getTagsArray());
    context.compressTags(baos, ib, kv1.getTagsOffset(), tagsLength1);
    KeyValue kv2 = createKVWithTags(3);
    int tagsLength2 = kv2.getTagsLength();
    ib = ByteBuffer.wrap(kv2.getTagsArray());
    context.compressTags(baos, ib, kv2.getTagsOffset(), tagsLength2);

    context.clear();

    byte[] dest = new byte[tagsLength1];
    ByteBuffer ob = ByteBuffer.wrap(baos.toByteArray());
    context.uncompressTags(new SingleByteBuff(ob), dest, 0, tagsLength1);
    assertTrue(Bytes.equals(kv1.getTagsArray(), kv1.getTagsOffset(), tagsLength1, dest, 0,
        tagsLength1));
    dest = new byte[tagsLength2];
    context.uncompressTags(new SingleByteBuff(ob), dest, 0, tagsLength2);
    assertTrue(Bytes.equals(kv2.getTagsArray(), kv2.getTagsOffset(), tagsLength2, dest, 0,
        tagsLength2));
  }

  @Test
  public void testCompressUncompressTagsWithOffheapKeyValue1() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream daos = new ByteBufferSupportDataOutputStream(baos);
    TagCompressionContext context = new TagCompressionContext(LRUDictionary.class, Byte.MAX_VALUE);
    ByteBufferedCell kv1 = (ByteBufferedCell)createOffheapKVWithTags(2);
    int tagsLength1 = kv1.getTagsLength();
    context.compressTags(daos, kv1.getTagsByteBuffer(), kv1.getTagsPosition(), tagsLength1);
    ByteBufferedCell kv2 = (ByteBufferedCell)createOffheapKVWithTags(3);
    int tagsLength2 = kv2.getTagsLength();
    context.compressTags(daos, kv2.getTagsByteBuffer(), kv2.getTagsPosition(), tagsLength2);

    context.clear();

    byte[] dest = new byte[tagsLength1];
    ByteBuffer ob = ByteBuffer.wrap(baos.getBuffer());
    context.uncompressTags(new SingleByteBuff(ob), dest, 0, tagsLength1);
    assertTrue(Bytes.equals(kv1.getTagsArray(), kv1.getTagsOffset(), tagsLength1, dest, 0,
        tagsLength1));
    dest = new byte[tagsLength2];
    context.uncompressTags(new SingleByteBuff(ob), dest, 0, tagsLength2);
    assertTrue(Bytes.equals(kv2.getTagsArray(), kv2.getTagsOffset(), tagsLength2, dest, 0,
        tagsLength2));
  }

  @Test
  public void testCompressUncompressTags2() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    TagCompressionContext context = new TagCompressionContext(LRUDictionary.class, Byte.MAX_VALUE);
    KeyValue kv1 = createKVWithTags(1);
    int tagsLength1 = kv1.getTagsLength();
    context.compressTags(baos, kv1.getTagsArray(), kv1.getTagsOffset(), tagsLength1);
    KeyValue kv2 = createKVWithTags(3);
    int tagsLength2 = kv2.getTagsLength();
    context.compressTags(baos, kv2.getTagsArray(), kv2.getTagsOffset(), tagsLength2);

    context.clear();

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.getBuffer());
    byte[] dest = new byte[tagsLength1];
    context.uncompressTags(bais, dest, 0, tagsLength1);
    assertTrue(Bytes.equals(kv1.getTagsArray(), kv1.getTagsOffset(), tagsLength1, dest, 0,
        tagsLength1));
    dest = new byte[tagsLength2];
    context.uncompressTags(bais, dest, 0, tagsLength2);
    assertTrue(Bytes.equals(kv2.getTagsArray(), kv2.getTagsOffset(), tagsLength2, dest, 0,
        tagsLength2));
  }

  @Test
  public void testCompressUncompressTagsWithOffheapKeyValue2() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream daos = new ByteBufferSupportDataOutputStream(baos);
    TagCompressionContext context = new TagCompressionContext(LRUDictionary.class, Byte.MAX_VALUE);
    ByteBufferedCell kv1 = (ByteBufferedCell)createOffheapKVWithTags(1);
    int tagsLength1 = kv1.getTagsLength();
    context.compressTags(daos, kv1.getTagsByteBuffer(), kv1.getTagsPosition(), tagsLength1);
    ByteBufferedCell kv2 = (ByteBufferedCell)createOffheapKVWithTags(3);
    int tagsLength2 = kv2.getTagsLength();
    context.compressTags(daos, kv2.getTagsByteBuffer(), kv2.getTagsPosition(), tagsLength2);

    context.clear();

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.getBuffer());
    byte[] dest = new byte[tagsLength1];
    context.uncompressTags(bais, dest, 0, tagsLength1);
    assertTrue(Bytes.equals(kv1.getTagsArray(), kv1.getTagsOffset(), tagsLength1, dest, 0,
        tagsLength1));
    dest = new byte[tagsLength2];
    context.uncompressTags(bais, dest, 0, tagsLength2);
    assertTrue(Bytes.equals(kv2.getTagsArray(), kv2.getTagsOffset(), tagsLength2, dest, 0,
        tagsLength2));
  }

  private KeyValue createKVWithTags(int noOfTags) {
    List<Tag> tags = new ArrayList<Tag>();
    for (int i = 0; i < noOfTags; i++) {
      tags.add(new ArrayBackedTag((byte) i, "tagValue" + i));
    }
    KeyValue kv = new KeyValue(ROW, CF, Q, 1234L, V, tags);
    return kv;
  }

  private Cell createOffheapKVWithTags(int noOfTags) {
    List<Tag> tags = new ArrayList<Tag>();
    for (int i = 0; i < noOfTags; i++) {
      tags.add(new ArrayBackedTag((byte) i, "tagValue" + i));
    }
    KeyValue kv = new KeyValue(ROW, CF, Q, 1234L, V, tags);
    ByteBuffer dbb = ByteBuffer.allocateDirect(kv.getBuffer().length);
    ByteBufferUtils.copyFromArrayToBuffer(dbb, kv.getBuffer(), 0, kv.getBuffer().length);
    OffheapKeyValue offheapKV = new OffheapKeyValue(dbb, 0, kv.getBuffer().length, true, 0);
    return offheapKV;
  }
}
