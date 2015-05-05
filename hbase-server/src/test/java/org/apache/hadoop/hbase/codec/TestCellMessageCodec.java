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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.io.CountingInputStream;
import com.google.common.io.CountingOutputStream;

@Category({MiscTests.class, SmallTests.class})
public class TestCellMessageCodec {
  private static final Log LOG = LogFactory.getLog(TestCellMessageCodec.class);

  @Test
  public void testEmptyWorks() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    CountingOutputStream cos = new CountingOutputStream(baos);
    DataOutputStream dos = new DataOutputStream(cos);
    MessageCodec cmc = new MessageCodec();
    Codec.Encoder encoder = cmc.getEncoder(dos);
    encoder.flush();
    dos.close();
    long offset = cos.getCount();
    assertEquals(0, offset);
    CountingInputStream cis = new CountingInputStream(new ByteArrayInputStream(baos.toByteArray()));
    DataInputStream dis = new DataInputStream(cis);
    Codec.Decoder decoder = cmc.getDecoder(dis);
    assertFalse(decoder.advance());
    dis.close();
    assertEquals(0, cis.getCount());
  }

  @Test
  public void testOne() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    CountingOutputStream cos = new CountingOutputStream(baos);
    DataOutputStream dos = new DataOutputStream(cos);
    MessageCodec cmc = new MessageCodec();
    Codec.Encoder encoder = cmc.getEncoder(dos);
    final KeyValue kv =
      new KeyValue(Bytes.toBytes("r"), Bytes.toBytes("f"), Bytes.toBytes("q"), Bytes.toBytes("v"));
    encoder.write(kv);
    encoder.flush();
    dos.close();
    long offset = cos.getCount();
    CountingInputStream cis = new CountingInputStream(new ByteArrayInputStream(baos.toByteArray()));
    DataInputStream dis = new DataInputStream(cis);
    Codec.Decoder decoder = cmc.getDecoder(dis);
    assertTrue(decoder.advance()); // First read should pull in the KV
    assertFalse(decoder.advance()); // Second read should trip over the end-of-stream  marker and return false
    dis.close();
    assertEquals(offset, cis.getCount());
  }

  @Test
  public void testThree() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    CountingOutputStream cos = new CountingOutputStream(baos);
    DataOutputStream dos = new DataOutputStream(cos);
    MessageCodec cmc = new MessageCodec();
    Codec.Encoder encoder = cmc.getEncoder(dos);
    final KeyValue kv1 = new KeyValue(Bytes.toBytes("r"), Bytes.toBytes("f"), Bytes.toBytes("1"), Bytes.toBytes("1"));
    final KeyValue kv2 = new KeyValue(Bytes.toBytes("r"), Bytes.toBytes("f"), Bytes.toBytes("2"), Bytes.toBytes("2"));
    final KeyValue kv3 = new KeyValue(Bytes.toBytes("r"), Bytes.toBytes("f"), Bytes.toBytes("3"), Bytes.toBytes("3"));
    encoder.write(kv1);
    encoder.write(kv2);
    encoder.write(kv3);
    encoder.flush();
    dos.close();
    long offset = cos.getCount();
    CountingInputStream cis = new CountingInputStream(new ByteArrayInputStream(baos.toByteArray()));
    DataInputStream dis = new DataInputStream(cis);
    Codec.Decoder decoder = cmc.getDecoder(dis);
    assertTrue(decoder.advance());
    Cell c = decoder.current();
    assertTrue(CellUtil.equals(c, kv1));
    assertTrue(decoder.advance());
    c = decoder.current();
    assertTrue(CellUtil.equals(c, kv2));
    assertTrue(decoder.advance());
    c = decoder.current();
    assertTrue(CellUtil.equals(c, kv3));
    assertFalse(decoder.advance());
    dis.close();
    assertEquals(offset, cis.getCount());
  }
}