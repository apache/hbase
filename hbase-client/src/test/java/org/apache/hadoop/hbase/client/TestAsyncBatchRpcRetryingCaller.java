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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncBatchRpcRetryingCaller.RegionRequest;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

@Category({ ClientTests.class, SmallTests.class })
public class TestAsyncBatchRpcRetryingCaller {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncBatchRpcRetryingCaller.class);

  private org.apache.logging.log4j.core.Appender mockAppender;

  @Before
  public void setUp() {
    mockAppender = mock(org.apache.logging.log4j.core.Appender.class);
    when(mockAppender.getName()).thenReturn("mockAppender");
    when(mockAppender.isStarted()).thenReturn(true);
    ((org.apache.logging.log4j.core.Logger) org.apache.logging.log4j.LogManager
      .getLogger(AsyncBatchRpcRetryingCaller.class)).addAppender(mockAppender);

  }

  @After
  public void tearDown() {
    ((org.apache.logging.log4j.core.Logger) org.apache.logging.log4j.LogManager
      .getLogger(AsyncBatchRpcRetryingCaller.class)).removeAppender(mockAppender);
  }

  @Test
  public void testLogAction() {
    AtomicReference<org.apache.logging.log4j.Level> level = new AtomicReference<>();
    AtomicReference<String> msg = new AtomicReference<String>();
    doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        org.apache.logging.log4j.core.LogEvent logEvent =
          invocation.getArgument(0, org.apache.logging.log4j.core.LogEvent.class);
        level.set(logEvent.getLevel());
        msg.set(logEvent.getMessage().getFormattedMessage());
        return null;
      }
    }).when(mockAppender).append(any(org.apache.logging.log4j.core.LogEvent.class));
    TableName tn = TableName.valueOf("async");
    ServerName sn = ServerName.valueOf("host", 12345, EnvironmentEdgeManager.currentTime());
    RegionRequest request =
      new RegionRequest(new HRegionLocation(RegionInfoBuilder.newBuilder(tn).build(), sn));
    Action put = new Action(new Put(Bytes.toBytes("a")), 0);
    Action get = new Action(new Get(Bytes.toBytes("b")), 1);
    Action incr = new Action(new Increment(Bytes.toBytes("c")), 2);
    Action del = new Action(new Delete(Bytes.toBytes("d")), 3);
    request.actions.add(put);
    request.actions.add(get);
    request.actions.add(incr);
    request.actions.add(del);
    IdentityHashMap<Action, Throwable> action2Error = new IdentityHashMap<>();
    AsyncBatchRpcRetryingCaller.logActionsException(1, 2, request, action2Error, sn);
    verify(mockAppender, never()).append(any());
    AsyncBatchRpcRetryingCaller.logActionsException(5, 4, request, action2Error, sn);
    verify(mockAppender, never()).append(any());

    action2Error.put(get, new IOException("get error"));
    action2Error.put(incr, new IOException("incr error"));
    AsyncBatchRpcRetryingCaller.logActionsException(5, 4, request, action2Error, sn);
    verify(mockAppender, times(1)).append(any());
    assertEquals(org.apache.logging.log4j.Level.WARN, level.get());

    String logMsg = msg.get();
    assertThat(logMsg,
      startsWith("Process batch for " + request.loc.getRegion().getRegionNameAsString() + " on "
        + sn.toString() + ", 2/4 actions failed, tries=5, sampled 2 errors:"));
    assertThat(logMsg, containsString("=> java.io.IOException: get error"));
    assertThat(logMsg, containsString("=> java.io.IOException: incr error"));
  }
}
