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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.io.CountingInputStream;
import com.google.common.io.CountingOutputStream;

@Category(SmallTests.class)
public class TestCellCodecWithTags {

  @Test
  public void testCellWithTag() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    CountingOutputStream cos = new CountingOutputStream(baos);
    DataOutputStream dos = new DataOutputStream(cos);
    Codec codec = new CellCodecWithTags();
    Codec.Encoder encoder = codec.getEncoder(dos);
    final Cell cell1 = new KeyValue(Bytes.toBytes("r"), Bytes.toBytes("f"), Bytes.toBytes("1"),
        HConstants.LATEST_TIMESTAMP, Bytes.toBytes("1"), new Tag[] {
            new Tag((byte) 1, Bytes.toBytes("teststring1")),
            new Tag((byte) 2, Bytes.toBytes("teststring2")) });
    final Cell cell2 = new KeyValue(Bytes.toBytes("r"), Bytes.toBytes("f"), Bytes.toBytes("2"),
        HConstants.LATEST_TIMESTAMP, Bytes.toBytes("2"), new Tag[] { new Tag((byte) 1,
            Bytes.toBytes("teststring3")), });
    final Cell cell3 = new KeyValue(Bytes.toBytes("r"), Bytes.toBytes("f"), Bytes.toBytes("3"),
        HConstants.LATEST_TIMESTAMP, Bytes.toBytes("3"), new Tag[] {
            new Tag((byte) 2, Bytes.toBytes("teststring4")),
            new Tag((byte) 2, Bytes.toBytes("teststring5")),
            new Tag((byte) 1, Bytes.toBytes("teststring6")) });

    encoder.write(cell1);
    encoder.write(cell2);
    encoder.write(cell3);
    encoder.flush();
    dos.close();
    long offset = cos.getCount();
    CountingInputStream cis = new CountingInputStream(new ByteArrayInputStream(baos.toByteArray()));
    DataInputStream dis = new DataInputStream(cis);
    Codec.Decoder decoder = codec.getDecoder(dis);
    assertTrue(decoder.advance());
    Cell c = decoder.current();
    assertTrue(CellComparator.equals(c, cell1));
    List<Tag> tags = Tag.asList(c.getTagsArray(), c.getTagsOffset(), c.getTagsLengthUnsigned());
    assertEquals(2, tags.size());
    Tag tag = tags.get(0);
    assertEquals(1, tag.getType());
    assertTrue(Bytes.equals(Bytes.toBytes("teststring1"), tag.getValue()));
    tag = tags.get(1);
    assertEquals(2, tag.getType());
    assertTrue(Bytes.equals(Bytes.toBytes("teststring2"), tag.getValue()));
    assertTrue(decoder.advance());
    c = decoder.current();
    assertTrue(CellComparator.equals(c, cell2));
    tags = Tag.asList(c.getTagsArray(), c.getTagsOffset(), c.getTagsLengthUnsigned());
    assertEquals(1, tags.size());
    tag = tags.get(0);
    assertEquals(1, tag.getType());
    assertTrue(Bytes.equals(Bytes.toBytes("teststring3"), tag.getValue()));
    assertTrue(decoder.advance());
    c = decoder.current();
    assertTrue(CellComparator.equals(c, cell3));
    tags = Tag.asList(c.getTagsArray(), c.getTagsOffset(), c.getTagsLengthUnsigned());
    assertEquals(3, tags.size());
    tag = tags.get(0);
    assertEquals(2, tag.getType());
    assertTrue(Bytes.equals(Bytes.toBytes("teststring4"), tag.getValue()));
    tag = tags.get(1);
    assertEquals(2, tag.getType());
    assertTrue(Bytes.equals(Bytes.toBytes("teststring5"), tag.getValue()));
    tag = tags.get(2);
    assertEquals(1, tag.getType());
    assertTrue(Bytes.equals(Bytes.toBytes("teststring6"), tag.getValue()));
    assertFalse(decoder.advance());
    dis.close();
    assertEquals(offset, cis.getCount());
  }
}