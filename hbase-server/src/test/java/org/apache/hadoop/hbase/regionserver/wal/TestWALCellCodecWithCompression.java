/**
 *
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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.codec.Codec.Decoder;
import org.apache.hadoop.hbase.codec.Codec.Encoder;
import org.apache.hadoop.hbase.io.util.LRUDictionary;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestWALCellCodecWithCompression {

  @Test
  public void testEncodeDecodeKVsWithTags() throws Exception {
    doTest(false);
  }

  @Test
  public void testEncodeDecodeKVsWithTagsWithTagsCompression() throws Exception {
    doTest(true);
  }

  private void doTest(boolean compressTags) throws Exception {
    Configuration conf = new Configuration(false);
    conf.setBoolean(CompressionContext.ENABLE_WAL_TAGS_COMPRESSION, compressTags);
    WALCellCodec codec = new WALCellCodec(conf, new CompressionContext(LRUDictionary.class, false,
        compressTags));
    ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
    Encoder encoder = codec.getEncoder(bos);
    encoder.write(createKV(1));
    encoder.write(createKV(0));
    encoder.write(createKV(2));

    InputStream is = new ByteArrayInputStream(bos.toByteArray());
    Decoder decoder = codec.getDecoder(is);
    decoder.advance();
    KeyValue kv = (KeyValue) decoder.current();
    List<Tag> tags = kv.getTags();
    assertEquals(1, tags.size());
    assertEquals("tagValue1", Bytes.toString(tags.get(0).getValue()));
    decoder.advance();
    kv = (KeyValue) decoder.current();
    tags = kv.getTags();
    assertEquals(0, tags.size());
    decoder.advance();
    kv = (KeyValue) decoder.current();
    tags = kv.getTags();
    assertEquals(2, tags.size());
    assertEquals("tagValue1", Bytes.toString(tags.get(0).getValue()));
    assertEquals("tagValue2", Bytes.toString(tags.get(1).getValue()));
  }

  private KeyValue createKV(int noOfTags) {
    byte[] row = Bytes.toBytes("myRow");
    byte[] cf = Bytes.toBytes("myCF");
    byte[] q = Bytes.toBytes("myQualifier");
    byte[] value = Bytes.toBytes("myValue");
    List<Tag> tags = new ArrayList<Tag>(noOfTags);
    for (int i = 1; i <= noOfTags; i++) {
      tags.add(new Tag((byte) i, Bytes.toBytes("tagValue" + i)));
    }
    return new KeyValue(row, cf, q, HConstants.LATEST_TIMESTAMP, value, tags);
  }
}
