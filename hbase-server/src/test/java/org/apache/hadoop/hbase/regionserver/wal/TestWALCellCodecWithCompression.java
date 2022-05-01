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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.ByteBufferKeyValue;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.codec.Codec.Decoder;
import org.apache.hadoop.hbase.codec.Codec.Encoder;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.util.LRUDictionary;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@Category({ RegionServerTests.class, SmallTests.class })
@RunWith(Parameterized.class)
public class TestWALCellCodecWithCompression {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestWALCellCodecWithCompression.class);

  private Compression.Algorithm compression;

  public TestWALCellCodecWithCompression(Compression.Algorithm algo) {
    this.compression = algo;
  }

  @Parameters
  public static List<Object[]> params() {
    return HBaseTestingUtility.COMPRESSION_ALGORITHMS_PARAMETERIZED;
  }

  @Test
  public void testEncodeDecodeKVsWithTags() throws Exception {
    doTest(false, false);
  }

  @Test
  public void testEncodeDecodeKVsWithTagsWithTagsCompression() throws Exception {
    doTest(true, false);
  }

  @Test
  public void testEncodeDecodeOffKVsWithTagsWithTagsCompression() throws Exception {
    doTest(true, false);
  }

  @Test
  public void testValueCompressionEnabled() throws Exception {
    doTest(false, true);
  }

  @Test
  public void testValueCompression() throws Exception {
    final byte[] row_1 = Bytes.toBytes("row_1");
    final byte[] value_1 = new byte[20];
    Bytes.zero(value_1);
    final byte[] row_2 = Bytes.toBytes("row_2");
    final byte[] value_2 = new byte[Bytes.SIZEOF_LONG];
    Bytes.random(value_2);
    final byte[] row_3 = Bytes.toBytes("row_3");
    final byte[] value_3 = new byte[100];
    Bytes.random(value_3);
    final byte[] row_4 = Bytes.toBytes("row_4");
    final byte[] value_4 = new byte[128];
    fillBytes(value_4, Bytes.toBytes("DEADBEEF"));
    final byte[] row_5 = Bytes.toBytes("row_5");
    final byte[] value_5 = new byte[64];
    fillBytes(value_5, Bytes.toBytes("CAFEBABE"));

    Configuration conf = new Configuration(false);
    WALCellCodec codec = new WALCellCodec(conf,
      new CompressionContext(LRUDictionary.class, false, true, true, compression));
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Encoder encoder = codec.getEncoder(bos);
    encoder.write(createKV(row_1, value_1, 0));
    encoder.write(createKV(row_2, value_2, 0));
    encoder.write(createKV(row_3, value_3, 0));
    encoder.write(createKV(row_4, value_4, 0));
    encoder.write(createKV(row_5, value_5, 0));
    encoder.flush();
    try (InputStream is = new ByteArrayInputStream(bos.toByteArray())) {
      Decoder decoder = codec.getDecoder(is);
      decoder.advance();
      KeyValue kv = (KeyValue) decoder.current();
      assertTrue(Bytes.equals(row_1, 0, row_1.length, kv.getRowArray(), kv.getRowOffset(),
        kv.getRowLength()));
      assertTrue(Bytes.equals(value_1, 0, value_1.length, kv.getValueArray(), kv.getValueOffset(),
        kv.getValueLength()));
      decoder.advance();
      kv = (KeyValue) decoder.current();
      assertTrue(Bytes.equals(row_2, 0, row_2.length, kv.getRowArray(), kv.getRowOffset(),
        kv.getRowLength()));
      assertTrue(Bytes.equals(value_2, 0, value_2.length, kv.getValueArray(), kv.getValueOffset(),
        kv.getValueLength()));
      decoder.advance();
      kv = (KeyValue) decoder.current();
      assertTrue(Bytes.equals(row_3, 0, row_3.length, kv.getRowArray(), kv.getRowOffset(),
        kv.getRowLength()));
      assertTrue(Bytes.equals(value_3, 0, value_3.length, kv.getValueArray(), kv.getValueOffset(),
        kv.getValueLength()));
      decoder.advance();
      kv = (KeyValue) decoder.current();
      assertTrue(Bytes.equals(row_4, 0, row_4.length, kv.getRowArray(), kv.getRowOffset(),
        kv.getRowLength()));
      assertTrue(Bytes.equals(value_4, 0, value_4.length, kv.getValueArray(), kv.getValueOffset(),
        kv.getValueLength()));
      decoder.advance();
      kv = (KeyValue) decoder.current();
      assertTrue(Bytes.equals(row_5, 0, row_5.length, kv.getRowArray(), kv.getRowOffset(),
        kv.getRowLength()));
      assertTrue(Bytes.equals(value_5, 0, value_5.length, kv.getValueArray(), kv.getValueOffset(),
        kv.getValueLength()));
    }
  }

  static void fillBytes(byte[] buffer, byte[] fill) {
    int offset = 0;
    int remaining = buffer.length;
    while (remaining > 0) {
      int len = remaining < fill.length ? remaining : fill.length;
      System.arraycopy(fill, 0, buffer, offset, len);
      offset += len;
      remaining -= len;
    }
  }

  private void doTest(boolean compressTags, boolean offheapKV) throws Exception {
    final byte[] key = Bytes.toBytes("myRow");
    final byte[] value = Bytes.toBytes("myValue");
    Configuration conf = new Configuration(false);
    conf.setBoolean(CompressionContext.ENABLE_WAL_TAGS_COMPRESSION, compressTags);
    WALCellCodec codec =
      new WALCellCodec(conf, new CompressionContext(LRUDictionary.class, false, compressTags));
    ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
    Encoder encoder = codec.getEncoder(bos);
    if (offheapKV) {
      encoder.write(createOffheapKV(key, value, 1));
      encoder.write(createOffheapKV(key, value, 0));
      encoder.write(createOffheapKV(key, value, 2));
    } else {
      encoder.write(createKV(key, value, 1));
      encoder.write(createKV(key, value, 0));
      encoder.write(createKV(key, value, 2));
    }

    InputStream is = new ByteArrayInputStream(bos.toByteArray());
    Decoder decoder = codec.getDecoder(is);
    decoder.advance();
    KeyValue kv = (KeyValue) decoder.current();
    List<Tag> tags = PrivateCellUtil.getTags(kv);
    assertEquals(1, tags.size());
    assertEquals("tagValue1", Bytes.toString(Tag.cloneValue(tags.get(0))));
    decoder.advance();
    kv = (KeyValue) decoder.current();
    tags = PrivateCellUtil.getTags(kv);
    assertEquals(0, tags.size());
    decoder.advance();
    kv = (KeyValue) decoder.current();
    tags = PrivateCellUtil.getTags(kv);
    assertEquals(2, tags.size());
    assertEquals("tagValue1", Bytes.toString(Tag.cloneValue(tags.get(0))));
    assertEquals("tagValue2", Bytes.toString(Tag.cloneValue(tags.get(1))));
  }

  private KeyValue createKV(byte[] row, byte[] value, int noOfTags) {
    byte[] cf = Bytes.toBytes("myCF");
    byte[] q = Bytes.toBytes("myQualifier");
    List<Tag> tags = new ArrayList<>(noOfTags);
    for (int i = 1; i <= noOfTags; i++) {
      tags.add(new ArrayBackedTag((byte) i, Bytes.toBytes("tagValue" + i)));
    }
    return new KeyValue(row, cf, q, HConstants.LATEST_TIMESTAMP, value, tags);
  }

  private ByteBufferKeyValue createOffheapKV(byte[] row, byte[] value, int noOfTags) {
    byte[] cf = Bytes.toBytes("myCF");
    byte[] q = Bytes.toBytes("myQualifier");
    List<Tag> tags = new ArrayList<>(noOfTags);
    for (int i = 1; i <= noOfTags; i++) {
      tags.add(new ArrayBackedTag((byte) i, Bytes.toBytes("tagValue" + i)));
    }
    KeyValue kv = new KeyValue(row, cf, q, HConstants.LATEST_TIMESTAMP, value, tags);
    ByteBuffer dbb = ByteBuffer.allocateDirect(kv.getBuffer().length);
    dbb.put(kv.getBuffer());
    return new ByteBufferKeyValue(dbb, 0, kv.getBuffer().length);
  }
}
