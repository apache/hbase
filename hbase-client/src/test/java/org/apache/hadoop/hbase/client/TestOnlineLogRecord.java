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
package org.apache.hadoop.hbase.client;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSet;

@Category({ ClientTests.class, SmallTests.class })
public class TestOnlineLogRecord {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestOnlineLogRecord.class);

  @Test
  public void itSerializesScan() {
    Scan scan = new Scan();
    scan.withStartRow(Bytes.toBytes(123));
    scan.withStopRow(Bytes.toBytes(456));
    String expectedOutput = "{\n" + "  \"startTime\": 1,\n" + "  \"processingTime\": 2,\n"
      + "  \"queueTime\": 3,\n" + "  \"responseSize\": 4,\n" + "  \"blockBytesScanned\": 5,\n"
      + "  \"fsReadTime\": 6,\n" + "  \"multiGetsCount\": 6,\n" + "  \"multiMutationsCount\": 7,\n"
      + "  \"scan\": {\n" + "    \"totalColumns\": 0,\n" + "    \"maxResultSize\": -1,\n"
      + "    \"caching\": -1,\n" + "    \"includeStopRow\": false,\n"
      + "    \"consistency\": \"STRONG\",\n" + "    \"maxVersions\": 1,\n"
      + "    \"mvccReadPoint\": -1,\n" + "    \"includeStartRow\": true,\n"
      + "    \"stopRow\": \"\\\\x00\\\\x00\\\\x01\\\\xC8\",\n" + "    \"limit\": -1,\n"
      + "    \"timeRange\": [\n" + "      0,\n" + "      9223372036854775807\n" + "    ],\n"
      + "    \"startRow\": \"\\\\x00\\\\x00\\\\x00{\",\n" + "    \"targetReplicaId\": -1,\n"
      + "    \"batch\": -1,\n" + "    \"families\": {},\n" + "    \"priority\": -1,\n"
      + "    \"storeOffset\": 0,\n" + "    \"queryMetricsEnabled\": false,\n"
      + "    \"needCursorResult\": false,\n" + "    \"storeLimit\": -1,\n"
      + "    \"cacheBlocks\": true,\n" + "    \"readType\": \"DEFAULT\",\n"
      + "    \"allowPartialResults\": false,\n" + "    \"reversed\": false\n" + "  }\n" + "}";
    OnlineLogRecord o = new OnlineLogRecord(1, 2, 3, 4, 5, 6, null, null, null, null, null, null,
      null, 6, 7, 0, scan, Collections.emptyMap(), Collections.emptyMap());
    String actualOutput = o.toJsonPrettyPrint();
    System.out.println(actualOutput);
    Assert.assertEquals(expectedOutput, actualOutput);
  }

  @Test
  public void itSerializesRequestAttributes() {
    Map<String, byte[]> requestAttributes = ImmutableMap.<String, byte[]> builder()
      .put("r", Bytes.toBytes("1")).put("2", Bytes.toBytes(0.0)).build();
    Set<String> expectedOutputs =
      ImmutableSet.<String> builder().add("requestAttributes").add("\"r\": \"1\"")
        .add("\"2\": \"\\\\x00\\\\x00\\\\x00\\\\x00\\\\x00\\\\x00\\\\x00\\\\x00\"").build();
    OnlineLogRecord o = new OnlineLogRecord(1, 2, 3, 4, 5, 6, null, null, null, null, null, null,
      null, 6, 7, 0, null, requestAttributes, Collections.emptyMap());
    String actualOutput = o.toJsonPrettyPrint();
    System.out.println(actualOutput);
    expectedOutputs.forEach(expected -> Assert.assertTrue(actualOutput.contains(expected)));
  }

  @Test
  public void itOmitsEmptyRequestAttributes() {
    OnlineLogRecord o = new OnlineLogRecord(1, 2, 3, 4, 5, 6, null, null, null, null, null, null,
      null, 6, 7, 0, null, Collections.emptyMap(), Collections.emptyMap());
    String actualOutput = o.toJsonPrettyPrint();
    System.out.println(actualOutput);
    Assert.assertFalse(actualOutput.contains("requestAttributes"));
  }

  @Test
  public void itSerializesConnectionAttributes() {
    Map<String, byte[]> connectionAttributes = ImmutableMap.<String, byte[]> builder()
      .put("c", Bytes.toBytes("1")).put("2", Bytes.toBytes(0.0)).build();
    Set<String> expectedOutputs =
      ImmutableSet.<String> builder().add("connectionAttributes").add("\"c\": \"1\"")
        .add("\"2\": \"\\\\x00\\\\x00\\\\x00\\\\x00\\\\x00\\\\x00\\\\x00\\\\x00\"").build();
    OnlineLogRecord o = new OnlineLogRecord(1, 2, 3, 4, 5, 6, null, null, null, null, null, null,
      null, 6, 7, 0, null, Collections.emptyMap(), connectionAttributes);
    String actualOutput = o.toJsonPrettyPrint();
    System.out.println(actualOutput);
    expectedOutputs.forEach(expected -> Assert.assertTrue(actualOutput.contains(expected)));
  }

  @Test
  public void itOmitsEmptyConnectionAttributes() {
    OnlineLogRecord o = new OnlineLogRecord(1, 2, 3, 4, 5, 6, null, null, null, null, null, null,
      null, 6, 7, 0, null, Collections.emptyMap(), Collections.emptyMap());
    String actualOutput = o.toJsonPrettyPrint();
    System.out.println(actualOutput);
    Assert.assertFalse(actualOutput.contains("connectionAttributes"));
  }
}
