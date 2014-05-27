/*
 * Copyright 2010 The Apache Software Foundation
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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import junit.framework.Assert;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.Test;

import com.facebook.swift.codec.ThriftCodec;
import com.facebook.swift.codec.ThriftCodecManager;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestPut {

  /**
   * Test if the serialization and deserialization of Put works when using
   * Swift.
   * @throws Exception
   */
  @Test
  public void testSwiftSerDe() throws Exception {

    ThriftCodec<Put> codec = new ThriftCodecManager().getCodec(Put.class);
    TMemoryBuffer transport = new TMemoryBuffer(1000 * 1024);
    TCompactProtocol protocol = new TCompactProtocol(transport);

    Map<byte[], List<KeyValue>> fMap =
      new TreeMap<byte[], List<KeyValue>>(Bytes.BYTES_COMPARATOR);
    byte[] row = Bytes.toBytes("row");
    fMap.put(row, createDummyKVs(row));
    long lockId = 5;
    boolean writeToWAL = true;
    Put put = new Put(row, fMap, lockId, writeToWAL);

    codec.write(put, protocol);
    Put putCopy = codec.read(protocol);
    assertEquals(put, putCopy);
  }

  @Test
  public void testIsDummy() throws Exception {
    Assert.assertTrue("Put.isDummy", new Put().isDummy());
    Assert.assertTrue("Put.isDummy", new Put(new byte[0]).isDummy());
  }

  public static List<KeyValue> createDummyKVs(byte[] row) {
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    for (int i = 0; i < 10; i++) {
      byte[] family = Bytes.toBytes("fam" + i);
      byte[] qualifier = Bytes.toBytes("q" + i);
      byte[] value = Bytes.toBytes("v" + i);
      kvs.add(new KeyValue(row, family, qualifier, System.currentTimeMillis(),
        KeyValue.Type.Delete, value));
    }
    return kvs;
  }

  /**
   * Test if the Builder is functionally equivalent to the constructor.
   * @throws Exception
   */
  @Test
  public void testBuilder() throws Exception {
    byte[] row = Bytes.toBytes("row");
    Map<byte[], List<KeyValue>> familyMap =
      new TreeMap<byte[], List<KeyValue>>(Bytes.BYTES_COMPARATOR);
    familyMap.put(row, createDummyKVs(row));
    long lockId = 5;
    boolean writeToWAL = true;
    // Create a Put the standard way
    Put put1 = new Put(row, familyMap, lockId, writeToWAL);

    // Now use a builder to create one, with the same parameters.
    Put put2 = new Put.Builder()
      .setRow(row)
      .setFamilyMap(familyMap)
      .setLockId(lockId)
      .setWriteToWAL(writeToWAL)
      .create();

    // Check if they match.
    Assert.assertEquals(put1, put2);
  }

  /**
   * Utility method that can create n dummy puts
   *
   * @param n
   * @return
   */
  public static List<Put> createDummyPuts(int n) {
    List<Put> puts = new ArrayList<Put>();
    String row = "row";
    for (int i = 0; i < n; i++) {
      Map<byte[], List<KeyValue>> fMap = new TreeMap<byte[], List<KeyValue>>(
          Bytes.BYTES_COMPARATOR);
      byte[] rowBytes = Bytes.toBytes(row + i);
      fMap.put(rowBytes, createDummyKVs(rowBytes));
      Put put = new Put.Builder().setRow(rowBytes).setFamilyMap(fMap)
          .setLockId(1).setWriteToWAL(false).create();
      puts.add(put);
    }
    return puts;
  }

}
