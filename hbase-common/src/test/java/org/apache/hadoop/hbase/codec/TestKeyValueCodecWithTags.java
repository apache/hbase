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
package org.apache.hadoop.hbase.codec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.io.CountingInputStream;
import org.apache.hbase.thirdparty.com.google.common.io.CountingOutputStream;

@Category({MiscTests.class, SmallTests.class})
public class TestKeyValueCodecWithTags {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestKeyValueCodecWithTags.class);

  @Test
  public void testKeyValueWithTag() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    CountingOutputStream cos = new CountingOutputStream(baos);
    DataOutputStream dos = new DataOutputStream(cos);
    Codec codec = new KeyValueCodecWithTags();
    Codec.Encoder encoder = codec.getEncoder(dos);
    final KeyValue kv1 = new KeyValue(Bytes.toBytes("r"), Bytes.toBytes("f"), Bytes.toBytes("1"),
        HConstants.LATEST_TIMESTAMP, Bytes.toBytes("1"), new Tag[] {
            new ArrayBackedTag((byte) 1, Bytes.toBytes("teststring1")),
            new ArrayBackedTag((byte) 2, Bytes.toBytes("teststring2")) });
    final KeyValue kv2 = new KeyValue(Bytes.toBytes("r"), Bytes.toBytes("f"), Bytes.toBytes("2"),
        HConstants.LATEST_TIMESTAMP, Bytes.toBytes("2"), new Tag[] { new ArrayBackedTag((byte) 1,
            Bytes.toBytes("teststring3")), });
    final KeyValue kv3 = new KeyValue(Bytes.toBytes("r"), Bytes.toBytes("f"), Bytes.toBytes("3"),
        HConstants.LATEST_TIMESTAMP, Bytes.toBytes("3"), new Tag[] {
            new ArrayBackedTag((byte) 2, Bytes.toBytes("teststring4")),
            new ArrayBackedTag((byte) 2, Bytes.toBytes("teststring5")),
            new ArrayBackedTag((byte) 1, Bytes.toBytes("teststring6")) });

    encoder.write(kv1);
    encoder.write(kv2);
    encoder.write(kv3);
    encoder.flush();
    dos.close();
    long offset = cos.getCount();
    CountingInputStream cis = new CountingInputStream(new ByteArrayInputStream(baos.toByteArray()));
    DataInputStream dis = new DataInputStream(cis);
    Codec.Decoder decoder = codec.getDecoder(dis);
    assertTrue(decoder.advance());
    Cell c = decoder.current();
    assertTrue(CellUtil.equals(c, kv1));
    List<Tag> tags =
        PrivateCellUtil.getTags(c);
    assertEquals(2, tags.size());
    Tag tag = tags.get(0);
    assertEquals(1, tag.getType());
    assertTrue(Bytes.equals(Bytes.toBytes("teststring1"), Tag.cloneValue(tag)));
    tag = tags.get(1);
    assertEquals(2, tag.getType());
    assertTrue(Bytes.equals(Bytes.toBytes("teststring2"), Tag.cloneValue(tag)));
    assertTrue(decoder.advance());
    c = decoder.current();
    assertTrue(CellUtil.equals(c, kv2));
    tags = PrivateCellUtil.getTags(c);
    assertEquals(1, tags.size());
    tag = tags.get(0);
    assertEquals(1, tag.getType());
    assertTrue(Bytes.equals(Bytes.toBytes("teststring3"), Tag.cloneValue(tag)));
    assertTrue(decoder.advance());
    c = decoder.current();
    assertTrue(CellUtil.equals(c, kv3));
    tags = PrivateCellUtil.getTags(c);
    assertEquals(3, tags.size());
    tag = tags.get(0);
    assertEquals(2, tag.getType());
    assertTrue(Bytes.equals(Bytes.toBytes("teststring4"), Tag.cloneValue(tag)));
    tag = tags.get(1);
    assertEquals(2, tag.getType());
    assertTrue(Bytes.equals(Bytes.toBytes("teststring5"), Tag.cloneValue(tag)));
    tag = tags.get(2);
    assertEquals(1, tag.getType());
    assertTrue(Bytes.equals(Bytes.toBytes("teststring6"), Tag.cloneValue(tag)));
    assertFalse(decoder.advance());
    dis.close();
    assertEquals(offset, cis.getCount());
  }
}
