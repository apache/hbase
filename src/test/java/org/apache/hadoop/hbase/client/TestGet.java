/*
 * Copyright The Apache Software Foundation
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.Assert;
import org.junit.Test;

import com.facebook.swift.codec.ThriftCodec;
import com.facebook.swift.codec.ThriftCodecManager;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestGet {

  /**
   * Tests if the object is correctly serialized & deserialized
   *
   * @throws Exception
   */
  @Test
  public void testSwiftSerDe() throws Exception {
    ThriftCodec<Get> codec = new ThriftCodecManager()
        .getCodec(Get.class);
    TMemoryBuffer transport = new TMemoryBuffer(1000 * 1024);
    TCompactProtocol protocol = new TCompactProtocol(transport);

    byte[] rowArray = Bytes.toBytes("myRow");
    Map<byte[], Set<byte[]>> familyMap = new HashMap<byte[], Set<byte[]>>();
    Set<byte[]> cols = createColumns(100);
    for (int i = 1; i < 10; i++) {
      familyMap.put(Bytes.toBytes("myFam" + i), cols);
    }
    // get with all attributes specified
    Get get = new Get.Builder(rowArray).setMaxVersions(1)
        .setEffectiveTS(System.currentTimeMillis()).setLockId(1L)
        .setFamilyMap(familyMap).setStoreLimit(5)
        .setTr(new TimeRange(0L, 100L)).create();
    codec.write(get, protocol);
    Get getCopy = codec.read(protocol);
    Assert.assertEquals(get, getCopy);

    // get with specifying just a row key
    Get get2 = new Get(rowArray);
    codec.write(get2, protocol);
    Get getCopy2 = codec.read(protocol);
    Assert.assertEquals(get2, getCopy2);

    // test add family and add column
    Get get3 = new Get.Builder(rowArray).addFamily(Bytes.toBytes("myFam1"))
        .addColumn(Bytes.toBytes("myFam2"), Bytes.toBytes("col1")).create();
    codec.write(get3, protocol);
    Get getCopy3 = codec.read(protocol);
    Assert.assertEquals(get3, getCopy3);
  }

  /**
   * Returns a set of columns with prefix col
   *
   * @param n - the number of columns we want to obtain
   * @return
   */
  public static Set<byte[]> createColumns(int n) {
    Set<byte[]> cols = new HashSet<byte[]>();
    String column = "col";
    for (int i = n - 1; i >= 0; i--) {
      cols.add(Bytes.toBytes(column + i));
    }
    return cols;
  }

  /**
   * Utility method that can create n dummy gets
   *
   * @param n
   * @return List of Get
   */

  public static List<Get> createDummyGets(int n) {
    List<Get> gets = new ArrayList<>();
    Map<byte[], Set<byte[]>> familyMap = new HashMap<byte[], Set<byte[]>>();
    Set<byte[]> cols = createColumns(100);
    for (int i = 1; i < 10; i++) {
      familyMap.put(Bytes.toBytes("myFam" + i), cols);
    }
    for (int i = 0; i < n; i++) {
      byte[] rowArray = Bytes.toBytes("myRow" + i);
      Get get = new Get.Builder(rowArray).setFamilyMap(familyMap).create();
      gets.add(get);
    }
    return gets;
  }
}
