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
package org.apache.hadoop.hbase.mob;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSetMultimap;

@Category(SmallTests.class)
public class TestMobUtils {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMobUtils.class);
  public static final TableName TEST_TABLE_1 = TableName.valueOf("testTable1");
  public static final TableName TEST_TABLE_2 = TableName.valueOf("testTable2");
  public static final TableName TEST_TABLE_3 = TableName.valueOf("testTable3");

  @Test
  public void serializeSingleMobFileRefs() {
    ImmutableSetMultimap<TableName, String> mobRefSet =
      ImmutableSetMultimap.<TableName, String> builder().putAll(TEST_TABLE_1, "file1a").build();
    byte[] result = MobUtils.serializeMobFileRefs(mobRefSet);
    assertEquals("testTable1/file1a", Bytes.toString(result));
  }

  @Test
  public void serializeMultipleMobFileRefs() {
    ImmutableSetMultimap<TableName, String> mobRefSet =
      ImmutableSetMultimap.<TableName, String> builder().putAll(TEST_TABLE_1, "file1a", "file1b")
        .putAll(TEST_TABLE_2, "file2a").putAll(TEST_TABLE_3, "file3a", "file3b").build();
    byte[] result = MobUtils.serializeMobFileRefs(mobRefSet);
    assertEquals("testTable1/file1a,file1b//testTable2/file2a//testTable3/file3a,file3b",
      Bytes.toString(result));
  }

  @Test
  public void deserializeSingleMobFileRefs() {
    ImmutableSetMultimap<TableName, String> mobRefSet =
      MobUtils.deserializeMobFileRefs(Bytes.toBytes("testTable1/file1a")).build();
    assertEquals(1, mobRefSet.size());
    ImmutableSet<String> testTable1Refs = mobRefSet.get(TEST_TABLE_1);
    assertEquals(1, testTable1Refs.size());
    assertTrue(testTable1Refs.contains("file1a"));
  }

  @Test
  public void deserializeMultipleMobFileRefs() {
    ImmutableSetMultimap<TableName,
      String> mobRefSet = MobUtils
        .deserializeMobFileRefs(
          Bytes.toBytes("testTable1/file1a,file1b//testTable2/file2a//testTable3/file3a,file3b"))
        .build();
    assertEquals(5, mobRefSet.size());
    ImmutableSet<String> testTable1Refs = mobRefSet.get(TEST_TABLE_1);
    ImmutableSet<String> testTable2Refs = mobRefSet.get(TEST_TABLE_2);
    ImmutableSet<String> testTable3Refs = mobRefSet.get(TEST_TABLE_3);
    assertEquals(2, testTable1Refs.size());
    assertEquals(1, testTable2Refs.size());
    assertEquals(2, testTable3Refs.size());
    assertTrue(testTable1Refs.contains("file1a"));
    assertTrue(testTable1Refs.contains("file1b"));
    assertTrue(testTable2Refs.contains("file2a"));
    assertTrue(testTable3Refs.contains("file3a"));
    assertTrue(testTable3Refs.contains("file3b"));
  }

  public static String getTableName(TestName test) {
    return test.getMethodName().replace("[", "-").replace("]", "");
  }
}
