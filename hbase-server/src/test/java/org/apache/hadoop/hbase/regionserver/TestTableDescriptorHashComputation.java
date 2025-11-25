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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import java.security.MessageDigest;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.TableSchema;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestTableDescriptorHashComputation {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTableDescriptorHashComputation.class);

  private String computeHash(TableDescriptor tableDescriptor) throws Exception {
    TableSchema tableSchema = ProtobufUtil.toTableSchema(tableDescriptor);
    byte[] bytes = tableSchema.toByteArray();
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    byte[] hash = digest.digest(bytes);
    return Bytes.toHex(hash);
  }

  @Test
  public void testHashLength() throws Exception {
    TableDescriptor td = TableDescriptorBuilder.newBuilder(TableName.valueOf("testTable"))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf")).build();

    String hash = computeHash(td);
    assertNotNull(hash);
    assertEquals(64, hash.length());
  }

  @Test
  public void testIdenticalDescriptorsProduceSameHash() throws Exception {
    TableDescriptor td1 = TableDescriptorBuilder.newBuilder(TableName.valueOf("testTable"))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf")).build();

    TableDescriptor td2 = TableDescriptorBuilder.newBuilder(TableName.valueOf("testTable"))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf")).build();

    String hash1 = computeHash(td1);
    String hash2 = computeHash(td2);

    assertEquals(hash1, hash2);
  }

  @Test
  public void testDifferentDescriptorsProduceDifferentHashes() throws Exception {
    TableDescriptor td1 = TableDescriptorBuilder.newBuilder(TableName.valueOf("testTable"))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf")).build();

    TableDescriptor td2 = TableDescriptorBuilder.newBuilder(TableName.valueOf("testTable"))
      .setColumnFamily(
        ColumnFamilyDescriptorBuilder.newBuilder("cf".getBytes()).setTimeToLive(86400).build())
      .build();

    String hash1 = computeHash(td1);
    String hash2 = computeHash(td2);

    assertNotEquals(hash1, hash2);
  }

  @Test
  public void testDifferentCompressionProducesDifferentHash() throws Exception {
    TableDescriptor td1 = TableDescriptorBuilder.newBuilder(TableName.valueOf("testTable"))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("cf".getBytes())
        .setCompressionType(org.apache.hadoop.hbase.io.compress.Compression.Algorithm.NONE).build())
      .build();

    TableDescriptor td2 = TableDescriptorBuilder.newBuilder(TableName.valueOf("testTable"))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("cf".getBytes())
        .setCompressionType(org.apache.hadoop.hbase.io.compress.Compression.Algorithm.SNAPPY)
        .build())
      .build();

    String hash1 = computeHash(td1);
    String hash2 = computeHash(td2);

    assertNotEquals(hash1, hash2);
  }

  @Test
  public void testMultipleColumnFamilies() throws Exception {
    TableDescriptor td1 = TableDescriptorBuilder.newBuilder(TableName.valueOf("testTable"))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf1"))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf2")).build();

    TableDescriptor td2 = TableDescriptorBuilder.newBuilder(TableName.valueOf("testTable"))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf1")).build();

    String hash1 = computeHash(td1);
    String hash2 = computeHash(td2);

    assertNotEquals(hash1, hash2);
  }
}
