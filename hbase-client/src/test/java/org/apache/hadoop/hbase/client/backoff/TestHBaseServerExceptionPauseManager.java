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
package org.apache.hadoop.hbase.client.backoff;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseServerException;
import org.apache.hadoop.hbase.quotas.RpcThrottlingException;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ClientTests.class, SmallTests.class })
public class TestHBaseServerExceptionPauseManager {

  private static final long WAIT_INTERVAL_MILLIS = 1L;
  private static final long WAIT_INTERVAL_NANOS =
    TimeUnit.MILLISECONDS.toNanos(WAIT_INTERVAL_MILLIS);
  private static final long PAUSE_NANOS_FOR_SERVER_OVERLOADED = WAIT_INTERVAL_NANOS * 3;

  private static final long PAUSE_NANOS = WAIT_INTERVAL_NANOS * 2;

  private final RpcThrottlingException RPC_THROTTLING_EXCEPTION = new RpcThrottlingException(
    RpcThrottlingException.Type.NumRequestsExceeded, WAIT_INTERVAL_MILLIS, "doot");
  private final Throwable OTHER_EXCEPTION = new RuntimeException("");
  private final HBaseServerException SERVER_OVERLOADED_EXCEPTION = new HBaseServerException(true);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHBaseServerExceptionPauseManager.class);

  @Test
  public void itSupportsRpcThrottlingNanos() {
    OptionalLong pauseNanos = HBaseServerExceptionPauseManager.getPauseNsFromException(
      RPC_THROTTLING_EXCEPTION, PAUSE_NANOS, PAUSE_NANOS_FOR_SERVER_OVERLOADED, Long.MAX_VALUE);
    assertTrue(pauseNanos.isPresent());
    assertEquals(pauseNanos.getAsLong(), WAIT_INTERVAL_NANOS);
  }

  @Test
  public void itSupportsServerOverloadedExceptionNanos() {
    OptionalLong pauseNanos = HBaseServerExceptionPauseManager.getPauseNsFromException(
      SERVER_OVERLOADED_EXCEPTION, PAUSE_NANOS, PAUSE_NANOS_FOR_SERVER_OVERLOADED, Long.MAX_VALUE);
    assertTrue(pauseNanos.isPresent());
    assertEquals(pauseNanos.getAsLong(), PAUSE_NANOS_FOR_SERVER_OVERLOADED);
  }

  @Test
  public void itSupportsOtherExceptionNanos() {
    OptionalLong pauseNanos = HBaseServerExceptionPauseManager.getPauseNsFromException(
      OTHER_EXCEPTION, PAUSE_NANOS, PAUSE_NANOS_FOR_SERVER_OVERLOADED, Long.MAX_VALUE);
    assertTrue(pauseNanos.isPresent());
    assertEquals(pauseNanos.getAsLong(), PAUSE_NANOS);
  }

  @Test
  public void itThrottledTimeoutFastFail() {
    OptionalLong pauseNanos = HBaseServerExceptionPauseManager.getPauseNsFromException(
      RPC_THROTTLING_EXCEPTION, PAUSE_NANOS, PAUSE_NANOS_FOR_SERVER_OVERLOADED, 0L);
    assertFalse(pauseNanos.isPresent());
  }

}
