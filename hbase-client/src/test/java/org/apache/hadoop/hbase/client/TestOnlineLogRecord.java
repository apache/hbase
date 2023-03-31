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

import java.util.Optional;
import org.apache.hadoop.hbase.HBaseClassTestRule;
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
    String expectedOutput = "{\n" + "  \"startTime\": 1,\n" + "  \"processingTime\": 2,\n"
      + "  \"queueTime\": 3,\n" + "  \"responseSize\": 4,\n" + "  \"blockBytesScanned\": 5,\n"
      + "  \"multiGetsCount\": 6,\n" + "  \"multiMutationsCount\": 7,\n" + "  \"scan\": {\n"
      + "    \"startRow\": \"\\\\x00\\\\x00\\\\x00{\",\n"
      + "    \"stopRow\": \"\\\\x00\\\\x00\\\\x01\\\\xC8\",\n" + "    \"batch\": -1,\n"
      + "    \"cacheBlocks\": true,\n" + "    \"totalColumns\": 0,\n"
      + "    \"maxResultSize\": \"-1\",\n" + "    \"families\": {},\n" + "    \"caching\": -1,\n"
      + "    \"maxVersions\": 1,\n" + "    \"timeRange\": [\n" + "      \"0\",\n"
      + "      \"9223372036854775807\"\n" + "    ]\n" + "  }\n" + "}";
    OnlineLogRecord o = new OnlineLogRecord(1, 2, 3, 4, 5, null, null, null, null, null, null, null,
      6, 7, 0, Optional.of(scan));
    String actualOutput = o.toJsonPrettyPrint();
    Assert.assertEquals(actualOutput, expectedOutput);
  }
}
