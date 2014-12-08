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
 * limitations under the License
 */
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.io.util.LRUDictionary;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import com.google.common.collect.Lists;

@Category(SmallTests.class)
public class TestKeyValueCompression {
  private static final byte[] VALUE = Bytes.toBytes("fake value");
  private static final int BUF_SIZE = 256*1024;
  
  @Test
  public void testCountingKVs() throws Exception {
    List<KeyValue> kvs = Lists.newArrayList();
    for (int i = 0; i < 400; i++) {
      byte[] row = Bytes.toBytes("row" + i);
      byte[] fam = Bytes.toBytes("fam" + i);
      byte[] qual = Bytes.toBytes("qual" + i);
      kvs.add(new KeyValue(row, fam, qual, 12345L, VALUE));
    }
    
    runTestCycle(kvs);
  }
  
  @Test
  public void testRepeatingKVs() throws Exception {
    List<KeyValue> kvs = Lists.newArrayList();
    for (int i = 0; i < 400; i++) {
      byte[] row = Bytes.toBytes("row" + (i % 10));
      byte[] fam = Bytes.toBytes("fam" + (i % 127));
      byte[] qual = Bytes.toBytes("qual" + (i % 128));
      kvs.add(new KeyValue(row, fam, qual, 12345L, VALUE));
    }
    
    runTestCycle(kvs);
  }

  private void runTestCycle(List<KeyValue> kvs) throws Exception {
    CompressionContext ctx = new CompressionContext(LRUDictionary.class, false, false);
    DataOutputBuffer buf = new DataOutputBuffer(BUF_SIZE);
    for (KeyValue kv : kvs) {
      KeyValueCompression.writeKV(buf, kv, ctx);
    }

    ctx.clear();
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(
        buf.getData(), 0, buf.getLength()));
    for (KeyValue kv : kvs) {
      KeyValue readBack = KeyValueCompression.readKV(in, ctx);
      assertEquals(kv, readBack);
    }
  }

  @Test
  public void testKVWithTags() throws Exception {
    CompressionContext ctx = new CompressionContext(LRUDictionary.class, false, false);
    DataOutputBuffer buf = new DataOutputBuffer(BUF_SIZE);
    KeyValueCompression.writeKV(buf, createKV(1), ctx);
    KeyValueCompression.writeKV(buf, createKV(0), ctx);
    KeyValueCompression.writeKV(buf, createKV(2), ctx);
    
    ctx.clear();
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(
        buf.getData(), 0, buf.getLength()));
    
    KeyValue readBack = KeyValueCompression.readKV(in, ctx);
    List<Tag> tags = readBack.getTags();
    assertEquals(1, tags.size());
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
