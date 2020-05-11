/**
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

package org.apache.hadoop.hbase.ipc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category(SmallTests.class)
public class TestRpcServerTraceLogging {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule
      .forClass(TestRpcServerTraceLogging.class);

  static org.apache.log4j.Logger rpcServerLog = org.apache.log4j.Logger.getLogger(RpcServer.class);

  static final String TRACE_LOG_MSG =
      "This is dummy message for testing:: region { type: REGION_NAME value: \"hbase:meta,,1\" }"
          + " scan { column { family: \"info\" } time_range { from: 0 to: 9223372036854775807 } "
      + "max_versions: 1 cache_blocks: true max_result_size: 2097152 caching: 2147483647 } "
      + "number_of_rows: 2147483647 close_scanner: false client_handles_partials: "
      + "true client_handles_heartbeats: true track_scan_metrics: false";

  static final int TRACE_LOG_LENGTH = TRACE_LOG_MSG.length();

  static final RpcServer mockRpcServer = Mockito.mock(RpcServer.class);

  static final Configuration conf = new Configuration(false);

  @BeforeClass
  public static void setUp() {
    Mockito.when(mockRpcServer.getConf()).thenReturn(conf);
    Mockito.when(mockRpcServer.truncateTraceLog(Mockito.any(String.class))).thenCallRealMethod();
  }

  @Test
  public void testLoggingWithTraceOff() {
    conf.setInt("hbase.ipc.trace.log.max.length", 250);
    rpcServerLog.setLevel(org.apache.log4j.Level.DEBUG);
    String truncatedString = mockRpcServer.truncateTraceLog(TRACE_LOG_MSG);

    assertEquals(150 + RpcServer.KEY_WORD_TRUNCATED.length(), truncatedString.length());
    assertTrue(truncatedString.contains(RpcServer.KEY_WORD_TRUNCATED));
  }

  @Test
  public void testLoggingWithTraceOn() {
    conf.setInt("hbase.ipc.trace.log.max.length", 250);
    rpcServerLog.setLevel(org.apache.log4j.Level.TRACE);
    String truncatedString = mockRpcServer.truncateTraceLog(TRACE_LOG_MSG);

    assertEquals(250 + RpcServer.KEY_WORD_TRUNCATED.length(), truncatedString.length());
    assertTrue(truncatedString.contains(RpcServer.KEY_WORD_TRUNCATED));
  }

  @Test
  public void testLoggingWithTraceOnLargeMax() {
    conf.setInt("hbase.ipc.trace.log.max.length", 2000);
    rpcServerLog.setLevel(org.apache.log4j.Level.TRACE);
    String truncatedString = mockRpcServer.truncateTraceLog(TRACE_LOG_MSG);

    assertEquals(TRACE_LOG_LENGTH, truncatedString.length());
    assertFalse(
      mockRpcServer.truncateTraceLog(TRACE_LOG_MSG).contains(RpcServer.KEY_WORD_TRUNCATED));
  }
}
