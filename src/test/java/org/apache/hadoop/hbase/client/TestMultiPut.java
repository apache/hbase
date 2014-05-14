/*
 * Copyright 2013 The Apache Software Foundation
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
package org.apache.hadoop.hbase.client;

import com.facebook.swift.codec.ThriftCodec;
import com.facebook.swift.codec.ThriftCodecManager;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static junit.framework.Assert.assertEquals;

@Category(SmallTests.class)
public class TestMultiPut {
  /**
   * Test the serialization and deserialization of the MultiPut object using
   * Swift.
   */
  @Test
  public void testSwiftSerDe() throws Exception {
    ThriftCodec<MultiPut> codec =
      new ThriftCodecManager().getCodec(MultiPut.class);
    TMemoryBuffer transport = new TMemoryBuffer(100*1024);
    TCompactProtocol protocol = new TCompactProtocol(transport);

    Map<byte[], List<Put>> puts = createDummyMultiPut();
    MultiPut multiPut = new MultiPut(puts);
    codec.write(multiPut, protocol);
    MultiPut multiPutCopy = codec.read(protocol);
    assertEquals(multiPut, multiPutCopy);
  }

  Map<byte[], List<Put>> createDummyMultiPut() {
    Map<byte[], List<Put>> puts =
      new TreeMap<byte[], List<Put>>(Bytes.BYTES_COMPARATOR);
    for (int i = 0; i < 10; i++) {
      byte[] regionName = Bytes.toBytes("region" + i);
      List<Put> putList = createDummyPuts(10);
      puts.put(regionName, putList);
    }
    return puts;
  }

  List<Put> createDummyPuts(int n) {
    List<Put> putList = new ArrayList<Put>();
    for (int i = 0; i < n; i++) {
      Put p = new Put(Bytes.toBytes("row" + i), System.currentTimeMillis());
      p.add(Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("myValue"));
      putList.add(p);
    }
    return putList;
  }
}
