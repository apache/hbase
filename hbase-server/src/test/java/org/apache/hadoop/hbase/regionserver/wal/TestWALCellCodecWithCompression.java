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

import static org.junit.Assert.assertEquals;

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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.codec.Codec.Decoder;
import org.apache.hadoop.hbase.codec.Codec.Encoder;
import org.apache.hadoop.hbase.io.util.LRUDictionary;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, SmallTests.class})
public class TestWALCellCodecWithCompression {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestWALCellCodecWithCompression.class);

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
    doTest(true, true);
  }

  private void doTest(boolean compressTags, boolean offheapKV) throws Exception {
    Configuration conf = new Configuration(false);
    conf.setBoolean(CompressionContext.ENABLE_WAL_TAGS_COMPRESSION, compressTags);
    WALCellCodec codec = new WALCellCodec(conf, new CompressionContext(LRUDictionary.class, false,
        compressTags));
    ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
    Encoder encoder = codec.getEncoder(bos);
    if (offheapKV) {
      encoder.write(createOffheapKV(1));
      encoder.write(createOffheapKV(0));
      encoder.write(createOffheapKV(2));
    } else {
      encoder.write(createKV(1));
      encoder.write(createKV(0));
      encoder.write(createKV(2));
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

  private KeyValue createKV(int noOfTags) {
    byte[] row = Bytes.toBytes("myRow");
    byte[] cf = Bytes.toBytes("myCF");
    byte[] q = Bytes.toBytes("myQualifier");
    byte[] value = Bytes.toBytes("myValue");
    List<Tag> tags = new ArrayList<>(noOfTags);
    for (int i = 1; i <= noOfTags; i++) {
      tags.add(new ArrayBackedTag((byte) i, Bytes.toBytes("tagValue" + i)));
    }
    return new KeyValue(row, cf, q, HConstants.LATEST_TIMESTAMP, value, tags);
  }

  private ByteBufferKeyValue createOffheapKV(int noOfTags) {
    byte[] row = Bytes.toBytes("myRow");
    byte[] cf = Bytes.toBytes("myCF");
    byte[] q = Bytes.toBytes("myQualifier");
    byte[] value = Bytes.toBytes("myValue");
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
