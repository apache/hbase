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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.io.CountingInputStream;
import com.google.common.io.CountingOutputStream;

@Category(SmallTests.class)
public class TestKeyValueCodec {
  @Test
  public void testEmptyWorks() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    CountingOutputStream cos = new CountingOutputStream(baos);
    DataOutputStream dos = new DataOutputStream(cos);
    KeyValueCodec kvc = new KeyValueCodec();
    Codec.Encoder encoder = kvc.getEncoder(dos);
    encoder.flush();
    dos.close();
    long offset = cos.getCount();
    assertEquals(0, offset);
    CountingInputStream cis =
      new CountingInputStream(new ByteArrayInputStream(baos.toByteArray()));
    DataInputStream dis = new DataInputStream(cis);
    Codec.Decoder decoder = kvc.getDecoder(dis);
    assertFalse(decoder.advance());
    dis.close();
    assertEquals(0, cis.getCount());
  }

  @Test
  public void testOne() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    CountingOutputStream cos = new CountingOutputStream(baos);
    DataOutputStream dos = new DataOutputStream(cos);
    KeyValueCodec kvc = new KeyValueCodec();
    Codec.Encoder encoder = kvc.getEncoder(dos);
    final KeyValue kv =
      new KeyValue(Bytes.toBytes("r"), Bytes.toBytes("f"), Bytes.toBytes("q"), Bytes.toBytes("v"));
    final long length = kv.getLength() + Bytes.SIZEOF_INT; 
    encoder.write(kv);
    encoder.flush();
    dos.close();
    long offset = cos.getCount();
    assertEquals(length, offset);
    CountingInputStream cis =
      new CountingInputStream(new ByteArrayInputStream(baos.toByteArray()));
    DataInputStream dis = new DataInputStream(cis);
    Codec.Decoder decoder = kvc.getDecoder(dis);
    assertTrue(decoder.advance()); // First read should pull in the KV
    // Second read should trip over the end-of-stream  marker and return false
    assertFalse(decoder.advance());
    dis.close();
    assertEquals(length, cis.getCount());
  }

  @Test
  public void testThree() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    CountingOutputStream cos = new CountingOutputStream(baos);
    DataOutputStream dos = new DataOutputStream(cos);
    KeyValueCodec kvc = new KeyValueCodec();
    Codec.Encoder encoder = kvc.getEncoder(dos);
    final KeyValue kv1 =
      new KeyValue(Bytes.toBytes("r"), Bytes.toBytes("f"), Bytes.toBytes("1"), Bytes.toBytes("1"));
    final KeyValue kv2 =
      new KeyValue(Bytes.toBytes("r"), Bytes.toBytes("f"), Bytes.toBytes("2"), Bytes.toBytes("2"));
    final KeyValue kv3 =
      new KeyValue(Bytes.toBytes("r"), Bytes.toBytes("f"), Bytes.toBytes("3"), Bytes.toBytes("3"));
    final long length = kv1.getLength() + Bytes.SIZEOF_INT; 
    encoder.write(kv1);
    encoder.write(kv2);
    encoder.write(kv3);
    encoder.flush();
    dos.close();
    long offset = cos.getCount();
    assertEquals(length * 3, offset);
    CountingInputStream cis =
      new CountingInputStream(new ByteArrayInputStream(baos.toByteArray()));
    DataInputStream dis = new DataInputStream(cis);
    Codec.Decoder decoder = kvc.getDecoder(dis);
    assertTrue(decoder.advance());
    KeyValue kv = (KeyValue)decoder.current();
    assertTrue(kv1.equals(kv));
    assertTrue(decoder.advance());
    kv = (KeyValue)decoder.current();
    assertTrue(kv2.equals(kv));
    assertTrue(decoder.advance());
    kv = (KeyValue)decoder.current();
    assertTrue(kv3.equals(kv));
    assertFalse(decoder.advance());
    dis.close();
    assertEquals((length * 3), cis.getCount());
  }
}