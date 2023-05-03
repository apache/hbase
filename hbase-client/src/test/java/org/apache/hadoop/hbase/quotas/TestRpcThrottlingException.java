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
package org.apache.hadoop.hbase.quotas;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

@Category({ SmallTests.class })
public class TestRpcThrottlingException {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRpcThrottlingException.class);

  private static final Map<String, Long> STR_TO_MS_NEW_FORMAT =
    ImmutableMap.<String, Long> builder().put("0ms", 0L).put("50ms", 50L).put("1sec, 1ms", 1001L)
      .put("1min, 5sec, 15ms", 65_015L).put("5mins, 2sec, 0ms", 302000L)
      .put("1hr, 3mins, 5sec, 1ms", 3785001L).put("1hr, 5sec, 1ms", 3605001L)
      .put("1hr, 0ms", 3600000L).put("1hr, 1min, 1ms", 3660001L).build();
  private static final Map<String,
    Long> STR_TO_MS_LEGACY_FORMAT = ImmutableMap.<String, Long> builder().put("0sec", 0L)
      .put("1sec", 1000L).put("2sec", 2000L).put("1mins, 5sec", 65_000L).put("5mins, 2sec", 302000L)
      .put("1hrs, 3mins, 5sec", 3785000L).build();

  @Test
  public void itConvertsMillisToNewString() {
    for (Map.Entry<String, Long> strAndMs : STR_TO_MS_NEW_FORMAT.entrySet()) {
      String output = RpcThrottlingException.stringFromMillis(strAndMs.getValue());
      assertEquals(strAndMs.getKey(), output);
    }
  }

  @Test
  public void itConvertsNewStringToMillis() {
    for (Map.Entry<String, Long> strAndMs : STR_TO_MS_NEW_FORMAT.entrySet()) {
      Long output = RpcThrottlingException.timeFromString(strAndMs.getKey());
      assertEquals(strAndMs.getValue(), output);
    }
  }

  @Test
  public void itConvertsLegacyStringToMillis() {
    for (Map.Entry<String, Long> strAndMs : STR_TO_MS_LEGACY_FORMAT.entrySet()) {
      Long output = RpcThrottlingException.timeFromString(strAndMs.getKey());
      assertEquals(strAndMs.getValue(), output);
    }
  }

}
