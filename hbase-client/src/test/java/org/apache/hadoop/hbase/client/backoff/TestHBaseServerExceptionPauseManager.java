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

import java.util.concurrent.TimeUnit;
import java.util.function.Function;
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

  private static final long PAUSE_MILLIS_FOR_SERVER_OVERLOADED = 2L;
  private static final long PAUSE_NANOS_FOR_SERVER_OVERLOADED =
    TimeUnit.MILLISECONDS.toNanos(PAUSE_MILLIS_FOR_SERVER_OVERLOADED);

  private static final long PAUSE_MILLIS = 3L;
  private static final long PAUSE_NANOS = TimeUnit.MILLISECONDS.toNanos(PAUSE_MILLIS);

  private final RpcThrottlingException RPC_THROTTLING_EXCEPTION = new RpcThrottlingException(
    RpcThrottlingException.Type.NumRequestsExceeded, WAIT_INTERVAL_MILLIS, "doot");
  private final Throwable OTHER_EXCEPTION = new RuntimeException("");
  private final HBaseServerException SERVER_OVERLOADED_EXCEPTION = new HBaseServerException(true);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHBaseServerExceptionPauseManager.class);

  @Test
  public void itSupportsRpcThrottlingNanos() {
    long pauseNanos = HBaseServerExceptionPauseManager.getPauseNanos(RPC_THROTTLING_EXCEPTION,
      PAUSE_NANOS_FOR_SERVER_OVERLOADED, PAUSE_NANOS);
    assertEquals(pauseNanos, WAIT_INTERVAL_NANOS);
  }

  @Test
  public void itSupportsRpcThrottlingMillis() {
    long pauseMillis = HBaseServerExceptionPauseManager.getPauseMillis(RPC_THROTTLING_EXCEPTION,
      r -> r, PAUSE_MILLIS_FOR_SERVER_OVERLOADED, PAUSE_MILLIS);
    assertEquals(pauseMillis, WAIT_INTERVAL_MILLIS);
  }

  @Test
  public void itSupportsRetryFunction() {
    Function<Long, Long> retryFunction = r -> 2 * r;
    Long pauseMillis = HBaseServerExceptionPauseManager.getPauseMillis(OTHER_EXCEPTION,
      retryFunction, PAUSE_MILLIS_FOR_SERVER_OVERLOADED, PAUSE_MILLIS);
    assertEquals(pauseMillis, retryFunction.apply(PAUSE_MILLIS));
  }

  @Test
  public void itSupportsServerOverloadedExceptionMillis() {
    long pauseMillis = HBaseServerExceptionPauseManager.getPauseMillis(SERVER_OVERLOADED_EXCEPTION,
      r -> r, PAUSE_MILLIS_FOR_SERVER_OVERLOADED, PAUSE_MILLIS);
    assertEquals(pauseMillis, PAUSE_MILLIS_FOR_SERVER_OVERLOADED);
  }

  @Test
  public void itSupportsServerOverloadedExceptionNanos() {
    long pauseNanos = HBaseServerExceptionPauseManager.getPauseNanos(SERVER_OVERLOADED_EXCEPTION,
      PAUSE_NANOS_FOR_SERVER_OVERLOADED, PAUSE_NANOS);
    assertEquals(pauseNanos, PAUSE_NANOS_FOR_SERVER_OVERLOADED);
  }

}
