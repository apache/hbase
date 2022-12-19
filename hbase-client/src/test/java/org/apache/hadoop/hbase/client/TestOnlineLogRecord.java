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

import com.google.common.collect.ImmutableList;
import java.util.Optional;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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
    String expectedOutput = "{\n" + "  \"startTime\": \"1\",\n" + "  \"processingTime\": 2,\n"
      + "  \"queueTime\": 3,\n" + "  \"responseSize\": \"4\",\n" + "  \"multiGetsCount\": 5,\n"
      + "  \"multiMutationsCount\": 6,\n" + "  \"multiServiceCalls\": 7,\n" + "  \"scan\": {\n"
      + "    \"startRow\": [\n" + "      0,\n" + "      0,\n" + "      0,\n" + "      123\n"
      + "    ],\n" + "    \"includeStartRow\": true,\n" + "    \"stopRow\": [\n" + "      0,\n"
      + "      0,\n" + "      1,\n" + "      -56\n" + "    ],\n"
      + "    \"includeStopRow\": false,\n" + "    \"maxVersions\": 1,\n" + "    \"batch\": -1,\n"
      + "    \"allowPartialResults\": false,\n" + "    \"storeLimit\": -1,\n"
      + "    \"storeOffset\": 0,\n" + "    \"caching\": -1,\n" + "    \"maxResultSize\": \"-1\",\n"
      + "    \"cacheBlocks\": true,\n" + "    \"reversed\": false,\n" + "    \"tr\": {\n"
      + "      \"minStamp\": \"0\",\n" + "      \"maxStamp\": \"9223372036854775807\",\n"
      + "      \"allTime\": true\n" + "    },\n" + "    \"familyMap\": {},\n"
      + "    \"mvccReadPoint\": \"-1\",\n" + "    \"limit\": -1,\n"
      + "    \"readType\": \"DEFAULT\",\n" + "    \"needCursorResult\": false,\n"
      + "    \"targetReplicaId\": -1,\n" + "    \"consistency\": \"STRONG\",\n"
      + "    \"colFamTimeRangeMap\": {},\n" + "    \"priority\": -1\n" + "  }\n" + "}";
    OnlineLogRecord o = new OnlineLogRecord(1, 2, 3, 4, null, null, null, null, null, null, null, 5,
      6, 7, Optional.of(scan), Optional.empty(), Optional.empty(), Optional.empty());
    Assert.assertEquals(o.toJsonPrettyPrint(), expectedOutput);
  }

  @Test
  public void itSerializesMulti() {
    Get get1 = new Get(Bytes.toBytes(123));
    Get get2 = new Get(Bytes.toBytes(456));
    get2.setFilter(new FirstKeyOnlyFilter());
    String expectedOutput = "{\n" + "  \"startTime\": \"1\",\n" + "  \"processingTime\": 2,\n"
      + "  \"queueTime\": 3,\n" + "  \"responseSize\": \"4\",\n" + "  \"multiGetsCount\": 5,\n"
      + "  \"multiMutationsCount\": 6,\n" + "  \"multiServiceCalls\": 7,\n" + "  \"multi\": [\n"
      + "    {\n" + "      \"row\": [\n" + "        0,\n" + "        0,\n" + "        0,\n"
      + "        123\n" + "      ],\n" + "      \"maxVersions\": 1,\n"
      + "      \"cacheBlocks\": true,\n" + "      \"storeLimit\": -1,\n"
      + "      \"storeOffset\": 0,\n" + "      \"tr\": {\n" + "        \"minStamp\": \"0\",\n"
      + "        \"maxStamp\": \"9223372036854775807\",\n" + "        \"allTime\": true\n"
      + "      },\n" + "      \"checkExistenceOnly\": false,\n" + "      \"familyMap\": {},\n"
      + "      \"targetReplicaId\": -1,\n" + "      \"consistency\": \"STRONG\",\n"
      + "      \"colFamTimeRangeMap\": {},\n" + "      \"priority\": -1\n" + "    },\n" + "    {\n"
      + "      \"row\": [\n" + "        0,\n" + "        0,\n" + "        1,\n" + "        -56\n"
      + "      ],\n" + "      \"maxVersions\": 1,\n" + "      \"cacheBlocks\": true,\n"
      + "      \"storeLimit\": -1,\n" + "      \"storeOffset\": 0,\n" + "      \"tr\": {\n"
      + "        \"minStamp\": \"0\",\n" + "        \"maxStamp\": \"9223372036854775807\",\n"
      + "        \"allTime\": true\n" + "      },\n" + "      \"checkExistenceOnly\": false,\n"
      + "      \"familyMap\": {},\n" + "      \"filter\": {\n" + "        \"foundKV\": false\n"
      + "      },\n" + "      \"targetReplicaId\": -1,\n" + "      \"consistency\": \"STRONG\",\n"
      + "      \"colFamTimeRangeMap\": {},\n" + "      \"priority\": -1\n" + "    }\n" + "  ]\n"
      + "}";
    OnlineLogRecord o = new OnlineLogRecord(1, 2, 3, 4, null, null, null, null, null, null, null, 5,
      6, 7, Optional.empty(),
      Optional.of(ImmutableList.<Operation> builder().add(get1).add(get2).build()),
      Optional.empty(), Optional.empty());
    Assert.assertEquals(o.toJsonPrettyPrint(), expectedOutput);
  }

  @Test
  public void itSerializesGet() {
    Get get = new Get(Bytes.toBytes(123));
    String expectedOutput = "{\n" + "  \"startTime\": \"1\",\n" + "  \"processingTime\": 2,\n"
      + "  \"queueTime\": 3,\n" + "  \"responseSize\": \"4\",\n" + "  \"multiGetsCount\": 5,\n"
      + "  \"multiMutationsCount\": 6,\n" + "  \"multiServiceCalls\": 7,\n" + "  \"get\": {\n"
      + "    \"row\": [\n" + "      0,\n" + "      0,\n" + "      0,\n" + "      123\n" + "    ],\n"
      + "    \"maxVersions\": 1,\n" + "    \"cacheBlocks\": true,\n" + "    \"storeLimit\": -1,\n"
      + "    \"storeOffset\": 0,\n" + "    \"tr\": {\n" + "      \"minStamp\": \"0\",\n"
      + "      \"maxStamp\": \"9223372036854775807\",\n" + "      \"allTime\": true\n" + "    },\n"
      + "    \"checkExistenceOnly\": false,\n" + "    \"familyMap\": {},\n"
      + "    \"targetReplicaId\": -1,\n" + "    \"consistency\": \"STRONG\",\n"
      + "    \"colFamTimeRangeMap\": {},\n" + "    \"priority\": -1\n" + "  }\n" + "}";
    OnlineLogRecord o = new OnlineLogRecord(1, 2, 3, 4, null, null, null, null, null, null, null, 5,
      6, 7, Optional.empty(), Optional.empty(), Optional.of(get), Optional.empty());
    Assert.assertEquals(o.toJsonPrettyPrint(), expectedOutput);
  }

  @Test
  public void itSerializesMutate() {
    Mutation mutate = new Put(Bytes.toBytes(123)).addColumn(Bytes.toBytes(456), Bytes.toBytes(789),
      Bytes.toBytes(0));
    String expectedOutput = "{\n" + "  \"startTime\": \"1\",\n" + "  \"processingTime\": 2,\n"
      + "  \"queueTime\": 3,\n" + "  \"responseSize\": \"4\",\n" + "  \"multiGetsCount\": 5,\n"
      + "  \"multiMutationsCount\": 6,\n" + "  \"multiServiceCalls\": 7,\n" + "  \"mutate\": {\n"
      + "    \"row\": [\n" + "      0,\n" + "      0,\n" + "      0,\n" + "      123\n" + "    ],\n"
      + "    \"ts\": \"9223372036854775807\",\n" + "    \"durability\": \"USE_DEFAULT\",\n"
      + "    \"familyMap\": {\n" + "      \"[B@1a6e431\": [\n" + "        {\n"
      + "          \"bytes\": [\n" + "            0,\n" + "            0,\n" + "            0,\n"
      + "            24,\n" + "            0,\n" + "            0,\n" + "            0,\n"
      + "            4,\n" + "            0,\n" + "            4,\n" + "            0,\n"
      + "            0,\n" + "            0,\n" + "            123,\n" + "            4,\n"
      + "            0,\n" + "            0,\n" + "            1,\n" + "            -56,\n"
      + "            0,\n" + "            0,\n" + "            3,\n" + "            21,\n"
      + "            127,\n" + "            -1,\n" + "            -1,\n" + "            -1,\n"
      + "            -1,\n" + "            -1,\n" + "            -1,\n" + "            -1,\n"
      + "            4,\n" + "            0,\n" + "            0,\n" + "            0,\n"
      + "            0\n" + "          ],\n" + "          \"offset\": 0,\n"
      + "          \"length\": 36,\n" + "          \"seqId\": \"0\"\n" + "        }\n" + "      ]\n"
      + "    },\n" + "    \"priority\": -1\n" + "  }\n" + "}";
    OnlineLogRecord o = new OnlineLogRecord(1, 2, 3, 4, null, null, null, null, null, null, null, 5,
      6, 7, Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(mutate));
    Assert.assertEquals(o.toJsonPrettyPrint(), expectedOutput);
  }
}
