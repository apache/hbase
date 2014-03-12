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


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.Test;

import com.facebook.swift.codec.ThriftCodec;
import com.facebook.swift.codec.ThriftCodecManager;

public class TestDelete extends TestCase {

  /**
   * Test to verify if the serialization and deserialization of Delete objects
   * works fine when using Swift.
   * @throws Exception
   */
  @Test
  public void testSwiftSerDe() throws Exception {
    ThriftCodec<Delete> codec = new ThriftCodecManager().getCodec(Delete.class);
    TMemoryBuffer transport = new TMemoryBuffer(100*1024);
    TCompactProtocol protocol = new TCompactProtocol(transport);

    byte[] row = Bytes.toBytes("row");
    Map<byte[], List<KeyValue>> familyMap =
      new TreeMap<byte[], List<KeyValue>>(Bytes.BYTES_COMPARATOR);
    familyMap.put(row, createDummyKVs(row));
    long lockId = 5;
    boolean writeToWAL = true;
    Delete del = new Delete(row, System.currentTimeMillis(),
                            familyMap, lockId, writeToWAL);
    codec.write(del, protocol);
    Delete delCopy = codec.read(protocol);
    Assert.assertEquals(del, delCopy);
  }

  @Test
  public void testIsDummy() throws Exception {
    Assert.assertTrue("Delete.isDummy", new Delete().isDummy());
    Assert.assertTrue("Delete.isDummy", new Delete(new byte[0]).isDummy());
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
    long currentMs = System.currentTimeMillis();
    long lockId = 5;
    boolean writeToWAL = true;
    // Create a Delete the standard way
    Delete del = new Delete(row, currentMs, familyMap, lockId, writeToWAL);

    // Now use a builder to create one, with the same parameters.
    Delete del2 = new Delete.Builder()
                        .setRow(row)
                        .setFamilyMap(familyMap)
                        .setTimeStamp(currentMs)
                        .setLockId(lockId)
                        .setWriteToWAL(writeToWAL)
                        .create();

    // Check if they match.
    Assert.assertEquals(del, del2);
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
   * Utility method that can create n dummy deletes
   *
   * @param n
   * @return List of Delete
   */
  public static List<Delete> createDummyDeletes(int n) {
    List<Delete> deletes = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      byte[] row = Bytes.toBytes("row" + i);
      Map<byte[], List<KeyValue>> familyMap =
          new TreeMap<byte[], List<KeyValue>>(Bytes.BYTES_COMPARATOR);
      familyMap.put(row, createDummyKVs(row));
      Delete del = new Delete.Builder().setRow(row).setFamilyMap(familyMap)
          .setTimeStamp(System.currentTimeMillis()).setLockId(1)
          .setWriteToWAL(false).create();
      deletes.add(del);
    }
    return deletes;
  }
}
