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
package org.apache.hadoop.hbase.logging;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * This should be in the hbase-logging module but the {@link HBaseClassTestRule} is in hbase-common
 * so we can only put the class in hbase-common module for now...
 */
@Category({ MiscTests.class, SmallTests.class })
public class TestJul2Slf4j {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestJul2Slf4j.class);

  static {
    System.setProperty("java.util.logging.config.class", JulToSlf4jInitializer.class.getName());
  }

  private String loggerName = getClass().getName();

  private org.apache.logging.log4j.core.Appender mockAppender;

  @Before
  public void setUp() {
    mockAppender = mock(org.apache.logging.log4j.core.Appender.class);
    when(mockAppender.getName()).thenReturn("mockAppender");
    when(mockAppender.isStarted()).thenReturn(true);
    ((org.apache.logging.log4j.core.Logger) org.apache.logging.log4j.LogManager
      .getLogger(loggerName)).addAppender(mockAppender);
  }

  @After
  public void tearDown() {
    ((org.apache.logging.log4j.core.Logger) org.apache.logging.log4j.LogManager
      .getLogger(loggerName)).removeAppender(mockAppender);
  }

  @Test
  public void test() throws IOException {
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
    java.util.logging.Logger logger = java.util.logging.Logger.getLogger(loggerName);
    logger.info(loggerName);
    verify(mockAppender, times(1)).append(any(org.apache.logging.log4j.core.LogEvent.class));
    assertEquals(org.apache.logging.log4j.Level.INFO, level.get());
    assertEquals(loggerName, msg.get());
  }
}
